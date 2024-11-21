//! Inner-node network impl for the primary.

use consensus_network_types::{FetchBlocksRequest, WorkerSynchronizeMessage};
use tokio::sync::mpsc;

/// Primary message types to worker and engine.
pub enum LocalPrimaryMessage {
    /// Primary to worker network messages.
    PrimaryToWorker(PrimaryToWorkerMessage),
    /// Primary to engine network messages.
    PrimaryToEngine(PrimaryToEngineMessage),
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
    pub to_network: mpsc::Sender<LocalPrimaryMessage>,
    /// Receiver for inner-node network messages.
    pub from_network: mpsc::Receiver<()>,
}
