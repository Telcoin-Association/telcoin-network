// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]

mod aggregators;
mod certificate_fetcher;
mod certifier;
pub mod consensus;
mod network;
mod primary;
mod proposer;
mod state_handler;
mod synchronizer;

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;

#[cfg(test)]
#[path = "tests/certificate_tests.rs"]
mod certificate_tests;

#[cfg(test)]
#[path = "tests/rpc_tests.rs"]
mod rpc_tests;

#[cfg(test)]
#[path = "tests/certificate_order_test.rs"]
mod certificate_order_test;

pub use crate::primary::{Primary, CHANNEL_CAPACITY, NUM_SHUTDOWN_RECEIVERS};
