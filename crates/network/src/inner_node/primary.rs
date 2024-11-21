//! Inner-node network impl for the primary.

use tn_network_types::{FetchBlocksRequest, WorkerSynchronizeMessage};
use tokio::sync::mpsc;

use super::{EngineToPrimaryMessage, WorkerToPrimaryMessage};

/// Primary message types to worker and engine.
pub enum FromPrimaryMessage {
    /// Primary to worker network messages.
    PrimaryToWorker(PrimaryToWorkerMessage),
    /// Primary to engine network messages.
    PrimaryToEngine(PrimaryToEngineMessage),
}

/// Messages to the primary from worker and engine.
pub enum ToPrimaryMessage {
    /// Worker to primary network messages.
    WorkerToPrimary(WorkerToPrimaryMessage),
    /// Engine to primary network messages.
    EngineToPrimary(EngineToPrimaryMessage),
}

/// Primary message to the worker.
///
/// TODO: can these be consolidated?
///
/// DO NOT MERGE UNTIL ADDRESSED
/// @@@@@
///
/// !!!!!!----------
///
/// ???
///
///
/// asdf
pub enum PrimaryToWorkerMessage {
    /// Synchronize with other workers to retrieve missing batches.
    Synchronize(WorkerSynchronizeMessage),
    /// Fetch blocks missing in certificates.
    FetchBlocks(FetchBlocksRequest),
}

/// Primary message to the engine.
pub enum PrimaryToEngineMessage {
    /// Verify a peer's execution result.
    VerifyExecution,
}

/// The primary's handle to the inner-node network.
pub struct PrimaryInnerNetworkHandle {
    /// Sending half to the inner-node network for primary to worker messages.
    pub to_network: mpsc::Sender<FromPrimaryMessage>,
    /// Receiver for inner-node network messages.
    pub from_network: mpsc::Receiver<ToPrimaryMessage>,
}
