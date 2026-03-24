# Make `RethEnv` Thread-Safe with `Arc<RethEnvInner>`

## Summary

Refactor `RethEnv` to wrap its fields in an `Arc<RethEnvInner>` so that cloning is cheap (just an `Arc` bump) and the type is trivially `Send + Sync`.

## Current State

`RethEnv` currently holds its fields directly:

```rust
#[derive(Clone)]
pub struct RethEnv {
    node_config: NodeConfig<RethChainSpec>,
    blockchain_provider: BlockchainProvider<TelcoinNode>,
    evm_config: TnEvmConfig,
    task_spawner: TaskSpawner,
}
```

Cloning duplicates all inner fields. While some of these types may already be internally reference-counted, the struct as a whole does not guarantee cheap cloning or shared ownership semantics.

## Proposed Change

Introduce a `RethEnvInner` struct that holds the existing fields and wrap it in an `Arc` inside `RethEnv`:

```rust
pub struct RethEnv {
    inner: Arc<RethEnvInner>,
}

struct RethEnvInner {
    node_config: NodeConfig<RethChainSpec>,
    blockchain_provider: BlockchainProvider<TelcoinNode>,
    evm_config: TnEvmConfig,
    task_spawner: TaskSpawner,
}
```

- `Clone` for `RethEnv` becomes a simple `Arc::clone`.
- All existing public methods on `RethEnv` should continue to work by accessing `self.inner.<field>` instead of `self.<field>`.
- The `Debug` impl should be updated to delegate through `self.inner`.

## Requirements

1. `RethEnv` must remain `Clone`, `Send`, and `Sync`.
2. All existing public methods must keep their current signatures and behavior.
3. `RethEnvInner` should be private (not `pub`).
4. No changes to external callers should be required.

## Files to Modify

- `crates/tn-reth/src/lib.rs` — struct definition, constructor, all method impls, `Debug` impl.

## Testing

All existing tests should pass without modification since the public API does not change.
