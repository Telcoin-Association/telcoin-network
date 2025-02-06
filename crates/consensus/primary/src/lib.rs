// SPDX-License-Identifier: Apache-2.0
//! Primary actors

#![warn(future_incompatible, nonstandard_style, rust_2018_idioms, rust_2021_compatibility)]
// TEMPORARY UNTIL LIBP2P INTEGRATED
#![allow(dead_code)]

mod aggregators;
mod anemo_network;
mod certificate_fetcher;
mod certifier;
pub mod consensus;
mod error;
pub mod network;
mod primary;
mod proposer;
mod state_handler;
mod state_sync;
mod synchronizer;

#[cfg(test)]
#[path = "tests/certificate_tests.rs"]
mod certificate_tests;

#[cfg(test)]
#[path = "tests/rpc_tests.rs"]
mod rpc_tests;

pub use crate::primary::Primary;

mod consensus_bus;
pub use consensus_bus::*;

mod recent_blocks;
pub use recent_blocks::*;
