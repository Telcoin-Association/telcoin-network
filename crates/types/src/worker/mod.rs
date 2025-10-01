//! Worker types.

use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};
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
pub type BatchReceiver = Receiver<(SealedBatch, oneshot::Sender<Result<(), BlockSealError>>)>;

/// The default worker udp port for consensus messages.
pub const DEFAULT_WORKER_PORT: u16 = 44895;

/// The unique identifier for a worker (per primary).
///
/// Workers communicate with peers of the same `WorkerId`.
pub type WorkerId = u16;
