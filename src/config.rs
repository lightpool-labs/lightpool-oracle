// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use lightpool_crypto::{derive_public_key_from_secret, PublicKey, SecretKey};
use lightpool_types::contract::ContractAddress;
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct MarketConfig {
    pub spot_market: String,
    pub hl_coin: String,
    #[serde(default = "default_price_scale")]
    pub price_scale: u64,
    #[serde(default = "default_tick_size")]
    pub tick_size: u64,
}

impl MarketConfig {
    pub fn market_contract(&self) -> anyhow::Result<ContractAddress> {
        let hex = self.spot_market.trim().trim_start_matches("0x");
        let bytes = hex::decode(hex)?;
        if bytes.len() != ContractAddress::CONTRACT_ADDRESS_LENGTH {
            anyhow::bail!(
                "spot_market must be {} bytes, got {} ({})",
                ContractAddress::CONTRACT_ADDRESS_LENGTH,
                bytes.len(),
                self.spot_market
            );
        }
        let mut arr = [0u8; ContractAddress::CONTRACT_ADDRESS_LENGTH];
        arr.copy_from_slice(&bytes);
        Ok(ContractAddress::from_bytes(arr))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OracleConfig {
    pub wallet_path: String,
    #[serde(default = "default_rpc")]
    pub lightpool_rpc_url: String,
    #[serde(default = "default_hl_ws")]
    pub hyperliquid_ws_url: String,
    #[serde(default = "default_hl_info")]
    pub hyperliquid_info_url: String,
    /// Oracle round length in milliseconds. Must be in (0, 200).
    #[serde(default = "default_round_ms")]
    pub round_ms: u64,
    #[serde(default = "default_collect_wait_ms")]
    pub collect_wait_ms: u64,
    #[serde(default = "default_peer_listen")]
    pub peer_listen: String,
    #[serde(default)]
    pub peers: Vec<String>,
    /// Override quote `epoch` for `ora_commit` (staking RunningCommittee epoch).
    /// When unset, uses getCommitteeInfo consensus epoch (may mismatch staking).
    #[serde(default)]
    pub quote_epoch: Option<u64>,
    pub markets: Vec<MarketConfig>,
}

#[derive(Debug, Deserialize)]
struct WalletFile {
    private_key: String,
}

fn default_rpc() -> String {
    "http://127.0.0.1:26300".to_string()
}
fn default_hl_ws() -> String {
    "wss://api.hyperliquid.xyz/ws".to_string()
}
fn default_hl_info() -> String {
    "https://api.hyperliquid.xyz/info".to_string()
}
fn default_round_ms() -> u64 {
    100
}
fn default_collect_wait_ms() -> u64 {
    50
}
fn default_peer_listen() -> String {
    "0.0.0.0:9100".to_string()
}
fn default_price_scale() -> u64 {
    1_000_000
}
fn default_tick_size() -> u64 {
    1
}

impl OracleConfig {
    pub fn read(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let data = fs::read(path.as_ref())?;
        let config: Self = serde_json::from_slice(&data)?;
        if config.markets.is_empty() {
            anyhow::bail!("oracle config markets must not be empty");
        }
        if config.round_ms == 0 || config.round_ms >= 200 {
            anyhow::bail!("round_ms must be in (0, 200), got {}", config.round_ms);
        }
        if config.collect_wait_ms >= config.round_ms {
            anyhow::bail!(
                "collect_wait_ms ({}) must be < round_ms ({})",
                config.collect_wait_ms,
                config.round_ms
            );
        }
        Ok(config)
    }

    pub fn load_wallet(&self) -> anyhow::Result<(PublicKey, SecretKey)> {
        let data = fs::read(&self.wallet_path)?;
        let wallet: WalletFile = serde_json::from_slice(&data)?;
        let bytes = hex::decode(wallet.private_key.trim())?;
        if bytes.len() != 32 {
            anyhow::bail!("private_key must be 32 bytes, got {}", bytes.len());
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        let encoded = base64::encode(arr);
        let secret_key = SecretKey::decode_base64(&encoded)
            .map_err(|e| anyhow::anyhow!("invalid secret key: {}", e))?;
        let public_key = derive_public_key_from_secret(&secret_key)
            .map_err(|e| anyhow::anyhow!("derive public key: {}", e))?;
        Ok((public_key, secret_key))
    }
}
