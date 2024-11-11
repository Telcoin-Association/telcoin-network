//! Worker types.

use tokio::sync::{mpsc::Sender, oneshot};
#[allow(clippy::mutable_key_type)]
mod info;
pub use info::*;
mod pending_block;
use crate::{error::BlockSealError, WorkerBlock};
pub use pending_block::*;

/// Type for the channel sender to submit worker block to the block provider.
pub type WorkerBlockSender = Sender<(WorkerBlock, oneshot::Sender<Result<(), BlockSealError>>)>;
