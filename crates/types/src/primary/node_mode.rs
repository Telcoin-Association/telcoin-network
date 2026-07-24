// SPDX-License-Identifier: MIT or Apache-2.0
//! Serializable consensus participation mode of a node, for use over the RPC boundary.

use serde::{Deserialize, Serialize};

/// The consensus participation mode of a node, in a form suitable for the wire (RPC/JSON).
///
/// This mirrors the consensus-layer node mode but lives in `tn-types` so it can cross the RPC
/// boundary: `tn-rpc` depends only on `tn-types`, not on the consensus crate. The consensus crate
/// provides an exhaustive `From` conversion into this type at the boundary. Variants match the
/// consensus enum one-for-one; [`NodeMode::CvvActive`] is the default.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeMode {
    /// Fully-synced CVV actively voting in the current committee.
    #[default]
    CvvActive,
    /// Staked CVV following consensus output while catching up to rejoin the committee after a
    /// failure during the epoch (the follow/catch-up path). This mode is transient: the node
    /// transitions back to [`NodeMode::CvvActive`] once it has synced past the garbage-collection
    /// window.
    CvvInactive,
    /// Node that only follows consensus output and is not in the current committee (may or may not
    /// be staked).
    Observer,
}
