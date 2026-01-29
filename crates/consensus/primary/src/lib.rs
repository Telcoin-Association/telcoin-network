// SPDX-License-Identifier: Apache-2.0
//! Primary actors

#![allow(missing_docs)]

mod aggregators;
mod certificate_fetcher;
mod certifier;
pub mod consensus;
mod error;
pub mod network;
mod primary;
mod proposer;
mod state_handler;
mod state_sync;

pub use state_sync::StateSynchronizer;

#[cfg(test)]
#[path = "tests/certificate_tests.rs"]
mod certificate_tests;

#[cfg(test)]
#[path = "tests/consensus_bus_tests.rs"]
mod consensus_bus_tests;

pub use crate::primary::Primary;

mod consensus_bus;
pub use consensus_bus::*;

mod recent_blocks;
pub use recent_blocks::*;

#[cfg(feature = "test-utils")]
pub mod test_utils;
