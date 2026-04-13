# Creating Custom Node Binaries with ExExs

This guide explains how to create custom Telcoin Network node binaries that include your own ExExs (Execution Extensions).

## Overview

The default `telcoin-network` binary has no ExExs built-in. To run ExExs, you need to:

1. Create a custom node binary that depends on your ExEx crates
2. Register your ExExs with a `TnExExLauncher`
3. Pass the launcher to `launch_node()`
4. Optionally use config files or CLI flags to control which ExExs run

## Step-by-Step Guide

### 1. Create Your ExEx

First, create a crate for your ExEx:

```rust
// my-indexer-exex/src/lib.rs
use eyre::Result;
use tn_exex::{TnExExContext, TnExExEvent};
use tn_reth::BlockReader;
use tn_types::BlockNumHash;

/// Transaction indexer ExEx.
///
/// Receives committed blocks and indexes transactions to a database.
pub async fn indexer_exex<P>(mut ctx: TnExExContext<P>) -> Result<()>
where
    P: BlockReader + Clone + Unpin + 'static,
{
    tracing::info!("Transaction indexer ExEx started");

    while let Some(notification) = ctx.notifications.recv().await {
        if let Some(chain) = notification.committed_chain() {
            let tip = chain.tip();
            
            // Index transactions in this block
            for block in chain.blocks() {
                index_block(&block).await?;
            }
            
            // Report progress to the ExEx manager
            ctx.events.send(TnExExEvent::FinishedHeight(
                BlockNumHash::new(tip.number, tip.hash())
            ))?;
        }
    }

    tracing::info!("Transaction indexer ExEx stopped");
    Ok(())
}

async fn index_block(block: &reth_primitives::SealedBlock) -> Result<()> {
    // Your indexing logic here
    Ok(())
}
```

### 2. Create a Custom Node Binary

Create a new binary crate that depends on your ExEx:

```toml
# my-custom-node/Cargo.toml
[package]
name = "my-custom-node"
version = "0.1.0"
edition = "2021"

[dependencies]
telcoin-network-cli = { path = "../../crates/telcoin-network-cli" }
tn-node = { path = "../../crates/node" }
tn-exex = { path = "../../crates/tn-exex" }
my-indexer-exex = { path = "../my-indexer-exex" }
# Add other ExEx dependencies here

clap = { version = "4", features = ["derive"] }
eyre = "0.6"
```

### 3. Register Your ExExs

In your custom node's `main.rs`, create a launcher and register your ExExs:

```rust
// my-custom-node/src/main.rs
use clap::Parser as _;
use telcoin_network_cli::cli::{Cli, NoArgs};
use tn_node::launch_node;
use tn_exex::TnExExLauncher;

fn main() {
    let cli = Cli::<NoArgs>::parse();

    if let Err(err) = cli.run(None, |builder, _, tn_datadir, key_config| {
        // Create launcher and install ExExs
        let mut launcher = TnExExLauncher::new();
        
        // Install your ExExs with unique IDs
        launcher.install("indexer", Box::new(|ctx| {
            Box::pin(my_indexer_exex::indexer_exex(ctx))
        }));
        
        launcher.install("metrics", Box::new(|ctx| {
            Box::pin(my_metrics_exex::metrics_exex(ctx))
        }));
        
        // Launch the node with your ExExs
        launch_node(builder, tn_datadir, key_config, Some(launcher))
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
```

### 4. Run Your Custom Node

Build and run your custom node:

```bash
cargo build --release --bin my-custom-node
./target/release/my-custom-node node --observer
```

All installed ExExs will run automatically. The ExEx manager will coordinate them and handle backpressure to prevent overwhelming the system.

## Configuration-Based ExEx Control

You can make your node respect the config file's `exex` field to selectively enable ExExs:

### Config File Example

```yaml
# config.yaml
node_info:
  name: "my-node"

# Enable specific ExExs
exex:
  - id: "indexer"
    enabled: true
  - id: "metrics"
    enabled: false
```

### Conditional Installation

Modify your `main.rs` to check the config:

```rust
use tn_config::Config;

fn main() {
    let cli = Cli::<NoArgs>::parse();

    if let Err(err) = cli.run(None, |builder, _, tn_datadir, key_config| {
        let mut launcher = TnExExLauncher::new();
        
        // Always install all available ExExs
        launcher.install("indexer", Box::new(|ctx| {
            Box::pin(my_indexer_exex::indexer_exex(ctx))
        }));
        
        launcher.install("metrics", Box::new(|ctx| {
            Box::pin(my_metrics_exex::metrics_exex(ctx))
        }));
        
        // The launcher will only run ExExs listed in builder.tn_config.exex
        // with enabled=true
        launch_node(builder, tn_datadir, key_config, Some(launcher))
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
```

**Note**: The launcher runs **all** installed ExExs by default. The config's `exex` field is available for your custom logic, but you'll need to implement filtering yourself if desired. Future versions may add automatic filtering.

## CLI Flag Support

The `--exex` flag is available but requires custom handling:

```bash
my-custom-node node --observer --exex indexer --exex metrics
```

The CLI flag populates `builder.tn_config.exex` which you can use to filter which ExExs to install.

## Why Observer Nodes for ExExs?

We **strongly recommend** running ExExs on observer nodes:

- ✅ **Zero consensus impact**: ExExs can't affect validator consensus
- ✅ **Resource isolation**: Heavy ExEx processing doesn't compete with consensus
- ✅ **Flexible deployment**: Scale ExEx nodes independently from validators
- ✅ **Identical execution**: Observers receive the exact same blocks as validators

## ExEx Development Tips

### 1. Use Structured Logging

```rust
use tracing::{info, warn, error, debug};

pub async fn my_exex<P>(mut ctx: TnExExContext<P>) -> Result<()> {
    info!(target: "exex::my_exex", "Starting ExEx");
    
    while let Some(notification) = ctx.notifications.recv().await {
        debug!(target: "exex::my_exex", ?notification, "Received notification");
        // ...
    }
    
    Ok(())
}
```

### 2. Report Progress Regularly

```rust
// After processing each block
ctx.events.send(TnExExEvent::FinishedHeight(
    BlockNumHash::new(block_number, block_hash)
))?;
```

This prevents the ExEx manager from thinking your ExEx is stalled.

### 3. Handle Graceful Shutdown

```rust
pub async fn my_exex<P>(mut ctx: TnExExContext<P>) -> Result<()> {
    while let Some(notification) = ctx.notifications.recv().await {
        // Process notification...
        
        // Check for shutdown
        if ctx.notifications.is_closed() {
            info!("Shutting down gracefully");
            break;
        }
    }
    
    Ok(())
}
```

### 4. Use Provider for State Queries

```rust
use tn_reth::BlockReader;

pub async fn my_exex<P>(mut ctx: TnExExContext<P>) -> Result<()>
where
    P: BlockReader + Clone + Unpin + 'static,
{
    while let Some(notification) = ctx.notifications.recv().await {
        if let Some(chain) = notification.committed_chain() {
            let tip = chain.tip();
            
            // Query historical state
            if let Some(header) = ctx.provider.header_by_number(tip.number - 1)? {
                // Access previous block data
            }
        }
    }
    
    Ok(())
}
```

## Testing Your ExEx

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tn_exex::TnExExContext;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_indexer_exex() {
        let (notification_tx, notification_rx) = mpsc::channel(32);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        
        // Create mock provider
        let provider = (); // Use mock
        
        let ctx = TnExExContext {
            notifications: notification_rx,
            events: event_tx,
            provider,
        };
        
        // Spawn ExEx
        let handle = tokio::spawn(indexer_exex(ctx));
        
        // Send test notifications
        // notification_tx.send(...).await?;
        
        // Assert on events
        // let event = event_rx.recv().await.unwrap();
        
        drop(notification_tx);
        handle.await.unwrap().unwrap();
    }
}
```

### Integration Tests

Run your custom node in a local testnet using the scripts in `etc/`:

```bash
# Copy and modify etc/local-testnet.sh to use your custom binary
./etc/local-testnet.sh
```

## Example: Real-World ExEx

See the README.md for complete examples of:
- Transaction indexer with database
- Bridge relayer with event monitoring
- Metrics exporter

## Troubleshooting

### ExEx not receiving notifications

- Ensure you're calling `ctx.events.send()` to report progress
- Check logs for backpressure warnings
- Verify the node is actually processing blocks

### Compile errors about `Send` or `Sync`

- Make sure your ExEx function is `async` and returns `Result<()>`
- Use `Box::pin()` when installing: `Box::pin(my_exex(ctx))`

### ExEx falls behind

- Run ExExs on observer nodes with more resources
- Optimize your processing logic
- Report progress more frequently

## Next Steps

- Read the [main README](./README.md) for ExEx architecture details
- Check the [implementation plan](../../.github/.plans/tn-exex-implementation-plan.md)
- Look at Reth's ExEx examples for more patterns
