//! Execution Extensions (ExEx) plugin system for Telcoin Network.
//!
//! ExEx provides a full-lifecycle plugin system that allows external modules to
//! react to chain state changes in real-time. Unlike reth's execution-only ExEx,
//! TN's version covers the entire transaction lifecycle:
//!
//! 1. **Certificate accepted** — a header is certified (own or peer)
//! 2. **Consensus committed** — a sub-DAG is committed by Bullshark
//! 3. **Chain executed** — blocks are executed and finalized
//!
//! This enables bridges, wallets, and dapps to track transactions from consensus
//! inclusion through finality to execution.
//!
//! TN uses Bullshark BFT consensus with immediate finality. There are no reorgs,
//! no WAL, and no reorg handling needed.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]

mod context;
mod event;
/// ExEx manager for notification fan-out.
pub mod manager;
mod notification;
/// Historical block replay for ExEx catch-up.
pub mod replay;

pub use context::TnExExContext;
pub use event::TnExExEvent;
pub use manager::{exex_channel_capacity, TnExExManager, TnExExManagerHandle};
pub use notification::{Chain, TnExExNotification};
pub use replay::ReplayStream;

use std::{future::Future, pin::Pin};

/// Type for ExEx install functions.
///
/// The function receives a [`TnExExContext`] and returns a future that runs
/// for the lifetime of the ExEx. The outer function handles initialization,
/// the returned future is the long-running ExEx task.
///
/// Reference: reth's `LaunchExEx` trait uses the same pattern.
pub type ExExInstallFn = Box<
    dyn FnOnce(TnExExContext) -> Pin<Box<dyn Future<Output = eyre::Result<()>> + Send>>
        + Send
        + Sync,
>;
