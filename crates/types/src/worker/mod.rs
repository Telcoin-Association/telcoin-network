//! Worker types.

use tokio::sync::{mpsc::Sender, oneshot};
#[allow(clippy::mutable_key_type)]
mod info;
pub use info::*;
mod sealed_block;
pub use sealed_block::*;
mod pending_block;
use crate::error::BlockSealError;
pub use pending_block::*;

/// Type for the channel sender to submit worker block to the block provider.
pub type WorkerBlockSender = Sender<(WorkerBlock, oneshot::Sender<Result<(), BlockSealError>>)>;
