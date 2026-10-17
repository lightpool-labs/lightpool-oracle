// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use lightpool_oracle::{spawn_oracle_router, spawn_trades_feed, MidBook, OracleConfig};
use log::info;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Parser)]
#[command(name = "lightpool-oracle", about = "Off-chain LightPool oracle worker")]
struct Args {
    #[arg(long, value_name = "FILE")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let config = OracleConfig::read(&args.config)
        .with_context(|| format!("failed to read config {}", args.config.display()))?;
    let (name, secret_key) = config.load_wallet().context("failed to load wallet")?;
    let peer_listen: SocketAddr = config
        .peer_listen
        .parse()
        .context("invalid peer_listen address")?;

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received Ctrl-C, shutting down oracle");
        cancel_clone.cancel();
    });

    let book = MidBook::new();
    let coins: Vec<String> = config
        .markets
        .iter()
        .map(|m| m.hl_coin.clone())
        .collect();
    spawn_trades_feed(
        config.hyperliquid_ws_url.clone(),
        config.hyperliquid_info_url.clone(),
        coins,
        book.clone(),
        cancel.clone(),
    );

    let router = spawn_oracle_router(
        config.clone(),
        name,
        secret_key,
        book,
        cancel.clone(),
    );

    let peer_router = Arc::clone(&router);
    let peer_cancel = cancel.clone();
    tokio::spawn(async move {
        if let Err(err) =
            lightpool_oracle::peers::spawn_peer_server(peer_listen, peer_router, peer_cancel).await
        {
            log::error!("peer server exited: {err}");
        }
    });

    info!(
        "lightpool-oracle started markets={} peers={} round_ms={} collect_wait_ms={}",
        config.markets.len(),
        config.peers.len(),
        config.round_ms,
        config.collect_wait_ms
    );
    cancel.cancelled().await;
    Ok(())
}
