//! Inner-node network impl for the worker.

use super::PrimaryToWorkerMessage;
use tn_network_types::{WorkerOthersBlockMessage, WorkerOwnBlockMessage};
use tokio::sync::mpsc;

/// Worker to Primary message types.
pub enum WorkerToPrimaryMessage {
    /// Worker reporting own block.
    OwnBlock(WorkerOwnBlockMessage),
    /// Worker reporting others blocks.
    OtherBlock(WorkerOthersBlockMessage),
}

/// The engine's handle to the inner-node network.
pub struct WorkerInnerNetworkHandle {
    /// Sending half to the inner-node network for worker to primary messages.
    pub to_network: mpsc::Sender<WorkerToPrimaryMessage>,
    /// Receiver for inner-node network messages.
    pub from_network: mpsc::Receiver<PrimaryToWorkerMessage>,
}
