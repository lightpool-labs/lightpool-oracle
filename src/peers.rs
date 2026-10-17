// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use lightpool_types::module_types::spot::SignedOracleQuote;
use log::{debug, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::router::OracleRouter;

pub async fn spawn_peer_server(
    listen: SocketAddr,
    router: Arc<OracleRouter>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/vote", post(ingest_vote))
        .with_state(router);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    log::info!("oracle peer listen on {}", listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel.cancelled().await;
        })
        .await?;
    Ok(())
}

async fn ingest_vote(
    State(router): State<Arc<OracleRouter>>,
    Json(quote): Json<SignedOracleQuote>,
) -> Json<&'static str> {
    router.ingest_quote(quote).await;
    Json("ok")
}

pub async fn broadcast_quotes(peers: &[String], quotes: &[SignedOracleQuote]) {
    if peers.is_empty() || quotes.is_empty() {
        return;
    }
    let client = reqwest::Client::new();
    for quote in quotes {
        for peer in peers {
            let url = format!("{}/vote", peer.trim_end_matches('/'));
            match client.post(&url).json(quote).send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!("gossip quote to {} ok", url);
                }
                Ok(resp) => warn!("gossip quote to {} status={}", url, resp.status()),
                Err(err) => warn!("gossip quote to {} failed: {}", url, err),
            }
        }
    }
}
