# feat(exex): add example metrics dashboard ExEx

## Summary

Create an example ExEx that tracks block execution metrics and exposes them via Prometheus, serving as both a useful feature for **node operator dashboards** and a reference implementation for ExEx developers.

## Background

Node operators need visibility into chain activity — blocks per second, gas utilization, transaction throughput, etc. Currently this requires custom RPC polling. An ExEx can push these metrics in real-time as blocks are committed, with zero additional load on the RPC layer.

This example also serves as a template showing the minimal ExEx pattern: receive notifications, process data, report `FinishedHeight`.

## Scope

### New files
- `examples/exex-metrics/Cargo.toml`
- `examples/exex-metrics/src/main.rs`

### Implementation

```rust
use tn_exex::{TnExExContext, TnExExNotification, TnExExEvent};
use metrics::{counter, gauge, histogram};

/// Example ExEx that exposes block execution metrics via Prometheus.
///
/// Tracked metrics:
/// - `tn_exex_blocks_processed_total` — counter of blocks processed
/// - `tn_exex_transactions_processed_total` — counter of transactions processed
/// - `tn_exex_gas_used_total` — counter of total gas consumed
/// - `tn_exex_latest_block_number` — gauge of the latest processed block number
/// - `tn_exex_latest_block_timestamp` — gauge of the latest block's timestamp
async fn metrics_exex(mut ctx: TnExExContext) -> eyre::Result<()> {
    while let Some(notification) = ctx.notifications.recv().await {
        match notification {
            TnExExNotification::ChainCommitted { new } => {
                for block in new.blocks() {
                    counter!("tn_exex_blocks_processed_total").increment(1);
                    counter!("tn_exex_transactions_processed_total")
                        .increment(block.body().transactions().len() as u64);
                    counter!("tn_exex_gas_used_total")
                        .increment(block.header().gas_used);
                    gauge!("tn_exex_latest_block_number")
                        .set(block.header().number as f64);
                    gauge!("tn_exex_latest_block_timestamp")
                        .set(block.header().timestamp as f64);
                }

                // Report finished height for pruning coordination
                if let Some(tip) = new.tip().number {
                    ctx.events.send(TnExExEvent::FinishedHeight(tip))?;
                }
            }
        }
    }
    Ok(())
}
```

### Registration example (in docs/comments):
```rust
// In node setup:
builder.install_exex("metrics-dashboard", |ctx| {
    Box::pin(metrics_exex(ctx))
});
```

### Documentation
Include doc comments explaining:
1. How to write a custom ExEx (the notification loop pattern)
2. How `FinishedHeight` works and why it matters
3. How to register an ExEx on the node
4. How to view the metrics (Prometheus endpoint)

## Acceptance criteria

- [ ] Example compiles as a workspace member
- [ ] Metrics are correctly incremented for each committed block
- [ ] `FinishedHeight` is reported after processing each notification
- [ ] Code is well-documented as a template for contributors
- [ ] README comments explain the ExEx pattern for new developers
- [ ] `cargo check --workspace` passes

## References

- [reth ExEx examples](https://github.com/paradigmxyz/reth/tree/main/examples/exex) — reth's example ExExes
- `crates/tn-reth/src/txn_pool.rs:127-140` — existing `CanonStateNotification` subscriber pattern

## Dependencies

- Issue 0 (core ExEx types)
- Issue 1 (ExEx manager)
- Issue 2 (node integration / registration API)
