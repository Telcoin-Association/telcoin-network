// SPDX-License-Identifier: Apache-2.0
//! Process consensus output and execute every transaction.

mod errors;
pub mod subscriber;
pub use errors::{SubscriberError, SubscriberResult};

#[cfg(test)]
use tempfile as _;

#[cfg(test)]
mod clippy {
    use eyre as _;
    use tn_network_libp2p as _;
    use tn_reth as _;
    use tn_test_utils as _;
}
