# lightpool-oracle

Off-chain oracle worker for LightPool spot marks. Each committee validator runs one
instance: subscribe Hyperliquid `trades` and use the last trade `px`, sign per-market quotes, gossip votes to
peers, and let the **time-rotated leader** submit a single `ora_commit` with the
quorum-signed batch (same collect/leader pattern as `lightpool-bridge`).

## Flow

```text
Hyperliquid trades (last px) ──► each oracle signs OracleQuotePayload
                              │
                              ▼ gossip /vote
                     leader collects stake ≥ quorum
                              │
                              ▼ 1 tx
                     spot `ora_commit` → stake-weighted median → oracle_price
```

Round length is **`round_ms` (default 100, must be &lt; 200)**. Leader waits
`collect_wait_ms` (default 50) inside the round before submitting `ora_commit`.

## Run

```bash
cd lightpool-oracle
cp oracle.config.example.json oracle.config.json
# edit wallet_path, markets, peers; keep round_ms < 200
cargo run --release -- --config oracle.config.json
```

Wallet file format (same as bridge): `{"private_key":"<32-byte hex>"}`.
