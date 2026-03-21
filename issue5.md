# feat(exex): add example block indexer ExEx for explorers

## Summary

Create an example ExEx that indexes blocks and transactions to a local SQLite database, demonstrating how a **block explorer backend** could be built as an ExEx plugin. This is a more advanced example showing database persistence, historical replay on restart, and the full ExEx lifecycle.

## Background

Block explorers are one of the primary use cases for ExEx. Instead of polling RPC endpoints, an ExEx receives a push-based stream of all committed blocks with full execution data (receipts, state changes). This example stores the data in SQLite so it can be queried by a frontend.

This example also demonstrates the **replay** feature from Issue 3 — on restart, the indexer checks its last indexed block and replays from there to catch up before switching to live notifications.

## Scope

### New files
- `examples/exex-indexer/Cargo.toml`
- `examples/exex-indexer/src/main.rs`

### Implementation

**Schema:**
```sql
CREATE TABLE IF NOT EXISTS blocks (
    number INTEGER PRIMARY KEY,
    hash TEXT NOT NULL,
    parent_hash TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    gas_used INTEGER NOT NULL,
    gas_limit INTEGER NOT NULL,
    tx_count INTEGER NOT NULL,
    base_fee_per_gas INTEGER
);

CREATE TABLE IF NOT EXISTS transactions (
    hash TEXT PRIMARY KEY,
    block_number INTEGER NOT NULL,
    tx_index INTEGER NOT NULL,
    from_addr TEXT NOT NULL,
    to_addr TEXT,
    value TEXT NOT NULL,
    gas_used INTEGER NOT NULL,
    FOREIGN KEY (block_number) REFERENCES blocks(number)
);
```

**ExEx function:**
```rust
async fn indexer_exex(ctx: TnExExContext) -> eyre::Result<()> {
    // 1. Open/create SQLite database
    let db = open_or_create_db("exex_indexer.db")?;

    // 2. Check last indexed block
    let last_indexed = get_last_indexed_block(&db)?;

    // 3. Replay from last indexed + 1, then switch to live notifications
    let mut stream = ctx.replay_and_subscribe(last_indexed.map(|n| n + 1).unwrap_or(0));

    // 4. Process notifications (identical for replay and live)
    while let Some(notification) = stream.next().await {
        let notification = notification?;
        match notification {
            TnExExNotification::ChainCommitted { new } => {
                // Insert blocks and transactions in a single SQLite transaction
                insert_chain_data(&db, &new)?;

                if let Some(tip) = new.tip().number {
                    // Report finished height
                    ctx.events.send(TnExExEvent::FinishedHeight(tip))?;
                }
            }
        }
    }
    Ok(())
}
```

**Dependencies:**
```toml
[dependencies]
tn-exex = { path = "../../crates/exex" }
rusqlite = { version = "0.31", features = ["bundled"] }
tokio = { workspace = true }
eyre = { workspace = true }
futures = { workspace = true }
```

### Documentation
Include comments explaining:
1. How `replay_and_subscribe` handles catch-up seamlessly
2. Why the processing code is identical for replay vs live notifications
3. How to extend the schema for a production block explorer
4. How to add a REST API layer on top of the SQLite data

## Acceptance criteria

- [ ] Example compiles as a workspace member
- [ ] Correctly indexes blocks and transactions to SQLite on first run
- [ ] On restart, replays from last indexed block (no duplicates, no gaps)
- [ ] Handles epoch boundaries without interruption (ExEx survives epochs)
- [ ] `FinishedHeight` reported after each batch insert
- [ ] Code is well-documented as a block explorer starting point
- [ ] `cargo check --workspace` passes

## References

- [reth ExEx examples](https://github.com/paradigmxyz/reth/tree/main/examples/exex) — reth's example ExExes
- [reth backfill](https://github.com/paradigmxyz/reth/tree/main/crates/exex/exex/src/backfill) — reth's catch-up mechanism (our replay is simpler)

## Dependencies

- Issue 0 (core ExEx types)
- Issue 1 (ExEx manager)
- Issue 2 (node integration / registration API)
- Issue 3 (historical replay)
