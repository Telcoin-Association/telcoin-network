// SPDX-License-Identifier: Apache-2.0
//! Worker components to create and sync batches.

#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]

mod batch_fetcher;
mod network;
mod worker;
pub use network::{WorkerNetwork, WorkerNetworkHandle, WorkerRequest, WorkerResponse};
pub mod quorum_waiter;

pub mod metrics;

pub use crate::worker::{new_worker, Worker, CHANNEL_CAPACITY};

/// The number of shutdown receivers to create on startup. We need one per component loop.
pub const NUM_SHUTDOWN_RECEIVERS: u64 = 26;

#[cfg(feature = "test-utils")]
pub mod test_utils;
