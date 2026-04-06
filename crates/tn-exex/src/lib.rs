// SPDX-License-Identifier: MIT OR Apache-2.0
//! # Telcoin Network Execution Extensions (TN ExEx)
//!
//! A simplified execution extension system for Telcoin Network that feeds real-time chain state
//! transitions to extension tasks on observer and validator nodes.
//!
//! Unlike traditional blockchain indexers that require re-processing historical data, TN ExEx
//! enables dapp developers to consume every executed block directly as it's committed.
//!
//! ## Key Simplifications vs Reth ExEx
//!
//! - **No Write-Ahead Log (WAL)**: Bullshark consensus provides finality, eliminating reorgs
//! - **No Reorg Notifications**: Only `ChainCommitted` events are needed
//! - **Simplified Manager**: No pipeline vs blockchain-tree distinction
//!
//! ## Observer Nodes - Primary Use Case
//!
//! **Observer nodes are the recommended deployment target for ExExes.** Observers:
//!
//! - Follow consensus without participating in committee voting
//! - Execute all blocks identically to validators (same `ExecutorEngine` → `finish_executing_output()` path)
//! - Receive ExEx notifications for every committed chain state transition
//! - Do not affect consensus performance (ExEx backpressure only impacts the observer)
//! - Can run custom indexing, bridges, or analytics without validator hardware requirements
//!
//! **Deployment pattern:**
//! ```text
//! ┌──────────────────────────────────────────────────────┐
//! │ Validator Committee (consensus + execution)          │
//! │ - No ExExes installed (minimal overhead)             │
//! └──────────────────────────────────────────────────────┘
//!                       │
//!                       │ Consensus output
//!                       ▼
//! ┌──────────────────────────────────────────────────────┐
//! │ Observer Node (execution + ExEx)                     │
//! │ - Follows consensus via state-sync                   │
//! │ - Runs custom ExExes (indexer, bridge, analytics)    │
//! │ - Exposes custom RPC endpoints for dapp queries      │
//! └──────────────────────────────────────────────────────┘
//! ```
//!
//! **Configuration:** Launch observer with `--observer` flag. ExExes receive identical
//! chain state transitions as validators without affecting network consensus.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────┐
//! │ ExecutorEngine  │
//! └────────┬────────┘
//!          │ CanonStateNotification
//!          ▼
//! ┌─────────────────┐
//! │  TnExExManager  │ ◄──── Backpressure signals
//! └────────┬────────┘
//!          │ TnExExNotification
//!          ▼
//! ┌─────────────────┐
//! │   ExEx Tasks    │ ──┐
//! │  (indexers,     │   │ TnExExEvent
//! │   bridges, etc) │ ◄─┘
//! └─────────────────┘
//! ```

#![warn(missing_docs, unreachable_pub)]
#![deny(unused_must_use, rust_2018_idioms)]

mod context;
mod event;
mod launcher;
mod manager;
mod notification;

pub use context::TnExExContext;
pub use event::TnExExEvent;
pub use launcher::{TnExExInstallFn, TnExExLauncher};
pub use manager::{TnExExHandle, TnExExManager, TnExExManagerHandle};
pub use notification::TnExExNotification;
