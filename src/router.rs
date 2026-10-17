// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use lightpool_crypto::{Digest, PublicKey, SecretKey, Signature};
use lightpool_types::contract::ContractAddress;
use lightpool_types::effects::ExecutionStatus;
use lightpool_types::module_types::spot::{OracleQuotePayload, SignedOracleQuote};
use lightpool_types::Committee;
use log::{info, warn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::config::{MarketConfig, OracleConfig};
use crate::hyperliquid::{to_chain_price, MidBook};
use crate::lp::LightpoolClient;
use crate::peers::broadcast_quotes;

/// Sentinel: no live override; fall back to config / consensus committee epoch.
const EPOCH_UNSET: u64 = u64::MAX;

struct QuoteBucket {
    quotes: HashMap<PublicKey, SignedOracleQuote>,
    submitted: bool,
}

pub struct OracleRouter {
    config: OracleConfig,
    name: PublicKey,
    secret_key: SecretKey,
    client: LightpoolClient,
    book: MidBook,
    committee: RwLock<Committee>,
    pending: RwLock<HashMap<(u64, ContractAddress), QuoteBucket>>,
    /// Live staking RunningCommittee.epoch (auto-healed from ora_commit errors).
    quote_epoch_live: AtomicU64,
}

impl OracleRouter {
    pub fn new(
        config: OracleConfig,
        name: PublicKey,
        secret_key: SecretKey,
        book: MidBook,
    ) -> Self {
        let client = LightpoolClient::new(config.lightpool_rpc_url.clone());
        let quote_epoch_live = AtomicU64::new(config.quote_epoch.unwrap_or(EPOCH_UNSET));
        Self {
            config,
            name,
            secret_key,
            client,
            book,
            committee: RwLock::new(Committee::new(Vec::new(), 0)),
            pending: RwLock::new(HashMap::new()),
            quote_epoch_live,
        }
    }

    async fn quote_epoch(&self) -> u64 {
        let live = self.quote_epoch_live.load(Ordering::Relaxed);
        if live != EPOCH_UNSET {
            return live;
        }
        let committee = self.committee.read().await;
        committee.epoch as u64
    }

    fn learn_epoch_from_failure(msg: &str) -> Option<u64> {
        // "... epoch mismatch: quote=4, committee=6"
        let key = "committee=";
        let idx = msg.find(key)?;
        let digits: String = msg[idx + key.len()..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        digits.parse().ok()
    }

    pub async fn refresh_committee(&self) {
        match self.client.fetch_committee().await {
            Ok(committee) => {
                let mut guard = self.committee.write().await;
                *guard = committee;
            }
            Err(err) => warn!("fetch committee failed: {err}"),
        }
    }

    pub async fn ingest_quote(&self, quote: SignedOracleQuote) {
        let round = quote.payload.round;
        let market = quote.payload.market;
        let mut pending = self.pending.write().await;
        let bucket = pending.entry((round, market)).or_insert_with(|| QuoteBucket {
            quotes: HashMap::new(),
            submitted: false,
        });
        if bucket.submitted {
            return;
        }
        bucket.quotes.insert(quote.validator, quote);
    }

    async fn is_leader(&self, round: u64) -> bool {
        let committee = self.committee.read().await;
        let keys = committee.voting_public_keys();
        if keys.is_empty() {
            return true;
        }
        let idx = (round as usize) % keys.len();
        keys[idx] == self.name
    }

    fn current_round(&self) -> u64 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now_ms / self.config.round_ms.max(1)
    }

    fn sign_quote(&self, payload: OracleQuotePayload) -> SignedOracleQuote {
        let digest = Digest::from_data(&payload);
        let signature = Signature::new(&digest, &self.secret_key);
        SignedOracleQuote {
            payload,
            validator: self.name,
            signature,
        }
    }

    async fn produce_round_quotes(&self, round: u64) -> Vec<SignedOracleQuote> {
        // On-chain ora_commit checks quote.epoch against staking RunningCommittee.epoch.
        let epoch = self.quote_epoch().await;
        let mut out = Vec::new();
        for market_cfg in &self.config.markets {
            let Ok(market) = market_cfg.market_contract() else {
                warn!("skip bad market config {}", market_cfg.spot_market);
                continue;
            };
            let Some(mid) = self.book.get(&market_cfg.hl_coin).await else {
                warn!("no hyperliquid mid for {}", market_cfg.hl_coin);
                continue;
            };
            let Some(price) =
                to_chain_price(mid, market_cfg.price_scale, market_cfg.tick_size)
            else {
                warn!(
                    "failed to convert mid={} for {}",
                    mid, market_cfg.hl_coin
                );
                continue;
            };
            let quote = self.sign_quote(OracleQuotePayload {
                market,
                round,
                price,
                epoch,
            });
            out.push(quote);
        }
        out
    }

    async fn try_leader_commit(&self, round: u64) {
        if !self.is_leader(round).await {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(
            self.config.collect_wait_ms,
        ))
        .await;

        let committee = self.committee.read().await;
        let threshold = committee.quorum_threshold() as u64;
        let markets: Vec<ContractAddress> = self
            .config
            .markets
            .iter()
            .filter_map(|m| m.market_contract().ok())
            .collect();

        let mut batch = Vec::new();
        let mut keys_to_mark = Vec::new();
        {
            let pending = self.pending.read().await;
            for market in markets {
                let key = (round, market);
                let Some(bucket) = pending.get(&key) else {
                    continue;
                };
                if bucket.submitted {
                    continue;
                }
                let mut power = 0u64;
                let mut quotes = Vec::new();
                for (pk, quote) in &bucket.quotes {
                    let stake = committee.stake(pk) as u64;
                    if stake == 0 {
                        continue;
                    }
                    power = power.saturating_add(stake);
                    quotes.push(quote.clone());
                }
                if power < threshold && !(committee.voting_public_keys().is_empty() && !quotes.is_empty())
                {
                    info!(
                        "round {} market {} waiting quorum power={} threshold={}",
                        round, market, power, threshold
                    );
                    continue;
                }
                if quotes.is_empty() {
                    continue;
                }
                batch.extend(quotes);
                keys_to_mark.push(key);
            }
        }

        if batch.is_empty() {
            return;
        }

        match self
            .client
            .submit_ora_commit(batch.clone(), self.name, &self.secret_key)
            .await
        {
            Ok(receipt) => {
                if receipt.is_success() {
                    info!(
                        "ora_commit ok round={} quotes={} events={}",
                        round,
                        batch.len(),
                        receipt.events.len()
                    );
                    let mut pending = self.pending.write().await;
                    for key in keys_to_mark {
                        if let Some(bucket) = pending.get_mut(&key) {
                            bucket.submitted = true;
                        }
                    }
                } else {
                    if let ExecutionStatus::Failure(msg) = &receipt.status {
                        if let Some(epoch) = Self::learn_epoch_from_failure(msg) {
                            let prev = self.quote_epoch_live.swap(epoch, Ordering::Relaxed);
                            if prev != epoch {
                                warn!(
                                    "ora_commit epoch mismatch; quote_epoch {} -> {}",
                                    if prev == EPOCH_UNSET {
                                        "unset".into()
                                    } else {
                                        prev.to_string()
                                    },
                                    epoch
                                );
                            }
                        }
                    }
                    warn!(
                        "ora_commit rejected round={} quotes={} status={:?} events={}",
                        round,
                        batch.len(),
                        receipt.status,
                        receipt.events.len()
                    );
                }
            }
            Err(err) => warn!("ora_commit failed round={round}: {err}"),
        }
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut last_round = 0u64;
        while !cancel.is_cancelled() {
            self.refresh_committee().await;
            let round = self.current_round();
            if round != last_round {
                last_round = round;
                let quotes = self.produce_round_quotes(round).await;
                for quote in &quotes {
                    self.ingest_quote(quote.clone()).await;
                }
                broadcast_quotes(&self.config.peers, &quotes).await;
                let router = Arc::clone(&self);
                tokio::spawn(async move {
                    router.try_leader_commit(round).await;
                });
            }
            let poll_ms = (self.config.round_ms / 4).clamp(10, 50);
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_millis(poll_ms)) => {}
            }
        }
    }
}

pub fn spawn_oracle_router(
    config: OracleConfig,
    name: PublicKey,
    secret_key: SecretKey,
    book: MidBook,
    cancel: CancellationToken,
) -> Arc<OracleRouter> {
    let router = Arc::new(OracleRouter::new(config, name, secret_key, book));
    let runner = Arc::clone(&router);
    tokio::spawn(async move {
        runner.run(cancel).await;
    });
    router
}

#[allow(dead_code)]
fn market_label(cfg: &MarketConfig) -> &str {
    &cfg.hl_coin
}
