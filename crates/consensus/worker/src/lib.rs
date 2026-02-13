// SPDX-License-Identifier: Apache-2.0
//! Worker components to create and sync batches.

#![allow(unused_crate_dependencies, missing_docs)]

mod batch_fetcher;
mod network;
mod worker;
pub use network::{WorkerNetwork, WorkerNetworkHandle, WorkerRequest, WorkerResponse};
pub mod quorum_waiter;

pub use crate::{
    network::{
        error::WorkerNetworkError,
        handler::RequestHandler,
        message::{WorkerGossip, WorkerRPCError},
    },
    worker::{new_worker, Worker, CHANNEL_CAPACITY},
};

/// The number of shutdown receivers to create on startup. We need one per component loop.
pub const NUM_SHUTDOWN_RECEIVERS: u64 = 26;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
