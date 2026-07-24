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
//!
//! # Building an ExEx
//!
//! An ExEx is an `async fn(`[`TnExExContext`]`) -> eyre::Result<()>` registered
//! with `TnBuilder::install_exex` (or `install_exex_with_capacity` to size its
//! notification buffer for a heavyweight consumer). It runs as an isolated,
//! non-critical task: a slow, stuck, or panicking ExEx can never stall consensus,
//! delay execution, or crash the node — at worst it stops receiving live
//! notifications and must reconcile via replay.
//!
//! Two worked examples ship in the `examples/` directory:
//!
//! - **`exex-lifecycle`** — the "hello world": logs every notification type.
//! - **`exex-indexer`** — a stateful skeleton modelling the full contract: replay-on-startup,
//!   uniform replay/live processing, [`Lagged`](TnExExNotification::Lagged) reconciliation, and
//!   progress reporting. Start here for an indexer or block explorer, a rollup data-availability
//!   feed, an MEV or analytics pipeline, or a bridge monitor.
//!
//! ## Reading chain state
//!
//! Beyond the notification payload, an ExEx has two read-only handles into the
//! node's databases:
//!
//! - [`reth_env`](TnExExContext::reth_env) — EVM chain state and history: historical blocks,
//!   headers, receipts, and account/storage state.
//! - [`consensus_chain`](TnExExContext::consensus_chain) — the consensus DB: consensus headers,
//!   epoch records, and committed sub-DAGs by number or digest.
//!
//! Both handles are for reads only, and **neither** is mutation-free at the type
//! level: `reth_env` exposes state-writing methods (e.g. `finish_executing_output`,
//! `finalize_block`) and `consensus_chain` exposes consensus-DB writers. An ExEx
//! must treat both as read handles and never call their writing methods — doing
//! so would corrupt the follower's state. The read-only contract is by
//! convention, not enforced by the type (see [`TnExExContext::reth_env`] and
//! [`TnExExContext::consensus_chain`]).
//!
//! # Replay vs. live: state-diff fidelity
//!
//! The authoritative catch-up path is [replay](TnExExContext::replay_from): on
//! startup — and after a [`Lagged`](TnExExNotification::Lagged) gap — a stateful
//! ExEx replays historical blocks from its last processed height, then follows
//! live notifications. Both paths deliver
//! [`ChainExecuted`](TnExExNotification::ChainExecuted), but with one asymmetry a
//! stateful consumer must account for:
//!
//! - **Live** `ChainExecuted` carries the full `BundleState` (the account/storage diffs produced by
//!   execution).
//! - **Replayed** `ChainExecuted` carries block data and **receipts** but an **empty
//!   `BundleState`**: that state is already committed to the DB and is not re-derived during
//!   replay.
//!
//! Block- and receipt-level consumers (transaction counts, gas, logs, events)
//! therefore process both paths identically. A consumer that needs account or
//! storage *diffs* must read them from [`reth_env`](TnExExContext::reth_env) by
//! block number on the replay path. Letting the replay and live paths diverge
//! here is the most common source of "works live, corrupts on catch-up" bugs.

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
pub use manager::{
    exex_channel_capacity, resolve_exex_channel_capacity, TnExExManager, TnExExManagerHandle,
};
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
