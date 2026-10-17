// Copyright (c) LightPool Labs
// Author: xiaoyu1998

use lightpool_crypto::{PublicKey, SecretKey, Signature};
use lightpool_types::address_type::Address;
use lightpool_types::contract::ContractAddress;
use lightpool_types::effects::TransactionReceipt;
use lightpool_types::module::Module;
use lightpool_types::module_types::spot::SignedOracleQuote;
use lightpool_types::transaction::{Action, SignedTransaction, Transaction};
use lightpool_types::{name, Committee, Name};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;

pub const ORA_COMMIT_ACTION: Name = name!("ora_commit");

pub fn spot_module_contract() -> ContractAddress {
    ContractAddress::new(Module::SPOT, [0u8; 7])
}

#[derive(Clone)]
pub struct LightpoolClient {
    rpc_url: String,
    http: reqwest::Client,
}

impl LightpoolClient {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn fetch_committee(&self) -> anyhow::Result<Committee> {
        #[derive(Deserialize)]
        struct CommitteeMemberInfo {
            consensus_pubkey: PublicKey,
            stake: u64,
            #[serde(default)]
            mempool_address: String,
            #[serde(default)]
            consensus_address: String,
        }

        #[derive(Deserialize)]
        struct CommitteeInfo {
            epoch: u128,
            members: Vec<CommitteeMemberInfo>,
        }

        let body = json!({
            "jsonrpc": "2.0",
            "method": "getCommitteeInfo",
            "params": [],
            "id": 1,
        });
        let value = self.post_json(&body).await?;
        let result = value
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("getCommitteeInfo missing result"))?;
        let parsed: CommitteeInfo = serde_json::from_value(result)?;
        let placeholder: SocketAddr = "0.0.0.0:0".parse()?;
        let info = parsed
            .members
            .into_iter()
            .map(|m| {
                let consensus = m
                    .consensus_address
                    .parse::<SocketAddr>()
                    .unwrap_or(placeholder);
                let mempool = m
                    .mempool_address
                    .parse::<SocketAddr>()
                    .unwrap_or(placeholder);
                (
                    m.consensus_pubkey,
                    m.stake,
                    consensus,
                    placeholder,
                    mempool,
                )
            })
            .collect();
        Ok(Committee::new(info, parsed.epoch))
    }

    pub async fn submit_ora_commit(
        &self,
        quotes: Vec<SignedOracleQuote>,
        sender: PublicKey,
        secret_key: &SecretKey,
    ) -> anyhow::Result<TransactionReceipt> {
        #[derive(Serialize)]
        struct CommitOraclePriceParams {
            quotes: Vec<SignedOracleQuote>,
        }
        let params = bincode::serialize(&CommitOraclePriceParams { quotes })?;
        let action = Action::new(spot_module_contract(), ORA_COMMIT_ACTION, params);
        let sender_addr = Address::from_public_key(&sender);
        let tx = Transaction::new(sender_addr, u64::MAX, vec![action]);
        let digest = tx.digest();
        let signature = Signature::new(&digest, secret_key);
        let signed = SignedTransaction::new(tx, signature);

        #[derive(Serialize)]
        struct SubmitParams {
            tx: SignedTransaction,
        }
        #[derive(Deserialize)]
        struct SubmitResponse {
            receipt: TransactionReceipt,
        }

        let body = json!({
            "jsonrpc": "2.0",
            "method": "submitTransaction",
            "params": [SubmitParams { tx: signed }],
            "id": 1,
        });
        let value = self.post_json(&body).await?;
        let result = value
            .get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("submitTransaction missing result"))?;
        let parsed: SubmitResponse = serde_json::from_value(result)?;
        Ok(parsed.receipt)
    }

    async fn post_json(&self, body: &Value) -> anyhow::Result<Value> {
        let response = self.http.post(&self.rpc_url).json(body).send().await?;
        let status = response.status();
        let value: Value = response.json().await?;
        if !status.is_success() {
            anyhow::bail!("rpc http {}: {}", status, value);
        }
        if let Some(err) = value.get("error") {
            anyhow::bail!("rpc error: {}", err);
        }
        Ok(value)
    }
}
