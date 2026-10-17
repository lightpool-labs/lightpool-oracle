// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

/// Latest HL last-trade price per coin (from `trades` WS).
#[derive(Clone, Default)]
pub struct MidBook {
    inner: Arc<RwLock<HashMap<String, f64>>>,
}

impl MidBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn get(&self, coin: &str) -> Option<f64> {
        self.inner.read().await.get(coin).copied()
    }

    async fn set_price(&self, coin: &str, price: f64) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        self.inner.write().await.insert(coin.to_string(), price);
    }
}

pub fn to_chain_price(mid: f64, price_scale: u64, tick_size: u64) -> Option<u64> {
    if !mid.is_finite() || mid <= 0.0 || price_scale == 0 {
        return None;
    }
    let raw = (mid * price_scale as f64).round();
    if raw <= 0.0 || raw >= u64::MAX as f64 {
        return None;
    }
    let mut price = raw as u64;
    if tick_size > 0 {
        price = price / tick_size * tick_size;
    }
    if price == 0 {
        None
    } else {
        Some(price)
    }
}

/// Subscribe Hyperliquid `trades` for each configured coin (one shared WS).
/// Last trade `px` becomes the book entry used for `ora_commit`.
pub fn spawn_trades_feed(
    ws_url: String,
    info_url: String,
    coins: Vec<String>,
    book: MidBook,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let coins: Vec<String> = coins
            .into_iter()
            .map(|c| c.trim().to_uppercase())
            .filter(|c| !c.is_empty())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if coins.is_empty() {
            warn!("hyperliquid trades feed: no coins configured");
            return;
        }
        // HTTP allMids first: respects HTTPS_PROXY / ALL_PROXY (WS often does not).
        if let Err(err) = poll_all_mids_seed(&info_url, &coins, &book).await {
            warn!("hyperliquid info seed failed: {err}");
        } else {
            info!("hyperliquid allMids seed ok coins={}", coins.len());
        }
        loop {
            if cancel.is_cancelled() {
                break;
            }
            if let Err(err) = run_trades_session(&ws_url, &coins, &book, &cancel).await {
                warn!("hyperliquid trades ws session ended: {err}");
            }
            if cancel.is_cancelled() {
                break;
            }
            if let Err(err) = poll_all_mids_seed(&info_url, &coins, &book).await {
                warn!("hyperliquid info seed failed: {err}");
            }
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
            }
        }
    });
}

/// Backward-compatible name used by older call sites.
pub fn spawn_all_mids_feed(
    ws_url: String,
    info_url: String,
    book: MidBook,
    cancel: CancellationToken,
) {
    spawn_trades_feed(ws_url, info_url, Vec::new(), book, cancel);
}

async fn poll_all_mids_seed(
    info_url: &str,
    coins: &[String],
    book: &MidBook,
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let value: Value = client
        .post(info_url)
        .json(&json!({"type": "allMids"}))
        .send()
        .await?
        .json()
        .await?;
    let mids = parse_mids_object(&value)?;
    for coin in coins {
        if let Some(px) = mids.get(coin) {
            book.set_price(coin, *px).await;
        }
    }
    Ok(())
}

async fn run_trades_session(
    ws_url: &str,
    coins: &[String],
    book: &MidBook,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let (ws, _) = connect_async(ws_url).await?;
    let (mut write, mut read) = ws.split();
    for coin in coins {
        let sub = json!({
            "method": "subscribe",
            "subscription": { "type": "trades", "coin": coin }
        });
        write.send(Message::Text(sub.to_string())).await?;
        info!("subscribed hyperliquid trades coin={coin}");
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            msg = read.next() => {
                let Some(msg) = msg else { break; };
                let msg = msg?;
                let text = match msg {
                    Message::Text(text) => text,
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await?;
                        continue;
                    }
                    Message::Close(_) => break,
                    _ => continue,
                };
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    apply_trades_message(book, &value).await;
                }
            }
        }
    }
    Ok(())
}

async fn apply_trades_message(book: &MidBook, value: &Value) {
    if value.get("channel").and_then(|v| v.as_str()) != Some("trades") {
        return;
    }
    let Some(data) = value.get("data") else {
        return;
    };
    let trades = if let Some(arr) = data.as_array() {
        arr.as_slice()
    } else {
        return;
    };
    // Use the last trade in the batch as the latest print.
    for trade in trades {
        let Some(coin) = trade.get("coin").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(px) = parse_px(trade.get("px")) else {
            continue;
        };
        debug!("hyperliquid trade coin={coin} px={px}");
        book.set_price(&coin.to_uppercase(), px).await;
    }
}

fn parse_px(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::String(s) => s.parse().ok(),
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn parse_mids_object(value: &Value) -> anyhow::Result<HashMap<String, f64>> {
    let obj = if let Some(mids) = value.get("mids") {
        mids.as_object()
            .ok_or_else(|| anyhow::anyhow!("mids is not object"))?
    } else {
        value
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("allMids payload is not object"))?
    };
    let mut out = HashMap::new();
    for (coin, mid_val) in obj {
        let mid = match mid_val {
            Value::String(s) => s.parse::<f64>()?,
            Value::Number(n) => n.as_f64().ok_or_else(|| anyhow::anyhow!("bad mid number"))?,
            _ => continue,
        };
        out.insert(coin.to_uppercase(), mid);
    }
    Ok(out)
}
