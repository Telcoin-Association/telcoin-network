// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types for the output of consensus.
#![allow(clippy::mutable_key_type)]

mod reputation;
pub use reputation::*;
mod output;
pub use output::*;
mod execution;
pub use execution::*;
pub use reth_consensus::{Consensus, ConsensusError};
use tokio::sync::{mpsc::Sender, oneshot};

use crate::{error::BlockSealError, WorkerBlock};

/// A global sequence number assigned to every CommittedSubDag.
pub type SequenceNumber = u64;

/// Type for the channel sender to submit worker block to the block provider.
pub type WorkerBlockSender = Sender<(WorkerBlock, oneshot::Sender<Result<(), BlockSealError>>)>;
