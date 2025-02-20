//! Worker types.

use tokio::sync::{mpsc::Sender, oneshot};
#[allow(clippy::mutable_key_type)]
mod info;
pub use info::*;
mod sealed_batch;
pub use sealed_batch::*;
mod pending_batch;
use crate::error::BlockSealError;
pub use pending_batch::*;

/// Type for the channel sender to submit sealed batches to the block provider.
///
/// The sending half (EL) pulls transactions from the public RPC transaction pool and seals a block
/// that extends the canonical tip.
///
/// The receiving half (CL) broadcasts to peers and tries to reach quorum.
pub type BatchSender = Sender<(SealedBatch, oneshot::Sender<Result<(), BlockSealError>>)>;

pub const DEFAULT_WORKER_PORT: u16 = 44895;
pub const DEFAULT_PRIMARY_PORT: u16 = 44894;
