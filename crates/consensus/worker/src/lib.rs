// SPDX-License-Identifier: Apache-2.0
//! Worker components to create and sync batches.

#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]

mod block_fetcher;
mod block_provider;
mod network;
pub mod quorum_waiter;
mod worker;

pub mod metrics;

pub use crate::{
    block_provider::BlockProvider,
    worker::{Worker, CHANNEL_CAPACITY},
};

/// The number of shutdown receivers to create on startup. We need one per component loop.
pub const NUM_SHUTDOWN_RECEIVERS: u64 = 26;
