// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod certificate_store;
mod consensus_store;
mod node_store;
mod payload_store;
mod proposer_store;
mod vote_digest_store;

pub use certificate_store::*;
pub use consensus_store::*;
pub use node_store::*;
pub use payload_store::*;
pub use proposer_store::*;
pub use vote_digest_store::*;

/// Convenience type to propagate store errors.
/// Use eyre- just YOLO these errors for now...
pub type StoreResult<T> = eyre::Result<T>;
