// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]

mod batch_fetcher;
mod batch_maker;
mod client;
mod handlers;
mod quorum_waiter;
// mod transactions_server;
// mod tx_validator;
mod worker;

pub mod metrics;

pub use crate::{
    batch_maker::BatchMaker,
    client::LocalNarwhalClient,
    worker::{Worker, CHANNEL_CAPACITY},
    // tx_validator::{TransactionValidator, TrivialTransactionValidator},
};

/// The number of shutdown receivers to create on startup. We need one per component loop.
pub const NUM_SHUTDOWN_RECEIVERS: u64 = 26;
