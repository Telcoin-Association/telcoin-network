//! Local network implementation.
//!
//! This is used for inner-node communication within the same process.
//! Channels are used instead of binding ports on the host.

use tn_types::CHANNEL_CAPACITY;
use tokio::sync::mpsc;
use tracing::error;

/// Handle to the network.
pub struct InnerNodeNetwork {
    /// For primary.
    primary_handle: PrimaryInnerNetworkHandle,
    /// For worker.
    worker_handle: WorkerInnerNetworkHandle,
    /// For engine.
    engine_handle: EngineInnerNetworkHandle,
}

impl InnerNodeNetwork {
    /// Deconstruct self for separate nodes to take ownership of handles.
    pub fn into_parts(
        self,
    ) -> (PrimaryInnerNetworkHandle, WorkerInnerNetworkHandle, EngineInnerNetworkHandle) {
        let Self { primary_handle, worker_handle, engine_handle } = self;
        (primary_handle, worker_handle, engine_handle)
    }
}

/// Message types for inner-node communication.
pub enum InnerNetworkMessage {
    /// From primary to the worker.
    PrimaryToWorker,
    /// From primary to the engine.
    PrimaryToEngine,
    /// From worker to the primary.
    WorkerToPrimary,
    /// From engine to the primary.
    EngineToPrimary,
}

/// The primary's handle to the inner-node network.
pub struct PrimaryInnerNetworkHandle {
    /// Sending half to the inner-node network for primary to worker messages.
    pub to_network: mpsc::Sender<InnerNetworkMessage>,
    /// Receiver for inner-node network messages.
    pub from_network: mpsc::Receiver<()>,
}

/// The engine's handle to the inner-node network.
pub struct WorkerInnerNetworkHandle {
    /// Sending half to the inner-node network for worker to primary messages.
    pub to_network: mpsc::Sender<InnerNetworkMessage>,
    /// Receiver for inner-node network messages.
    pub from_network: mpsc::Receiver<()>,
}

/// The engine's handle to the inner-node network.
pub struct EngineInnerNetworkHandle {
    /// Sending half to the inner-node network for engine to primary messages.
    pub to_network: mpsc::Sender<InnerNetworkMessage>,
    /// Receiver for inner-node network messages.
    pub from_network: mpsc::Receiver<()>,
}

impl InnerNodeNetwork {
    /// Spawn the local network for inner-node communication.
    ///
    /// Returns local network handles for worker, primary, and engine.
    ///
    /// TODO: this only supports one local worker.
    pub fn spawn() -> Self {
        // channels for primary
        let (for_primary_tx, mut primary_router) = mpsc::channel(CHANNEL_CAPACITY);
        let (inner_worker_to_primary, for_primary_rx) = mpsc::channel(CHANNEL_CAPACITY);
        // channels for worker
        let (for_worker_tx, mut worker_router) = mpsc::channel(CHANNEL_CAPACITY);
        let (inner_primary_to_worker, for_worker_rx) = mpsc::channel(CHANNEL_CAPACITY);
        // channels for engine
        let (for_engine_tx, mut engine_router) = mpsc::channel(CHANNEL_CAPACITY);
        let (inner_primary_to_engine, for_engine_rx) = mpsc::channel(CHANNEL_CAPACITY);

        // spawn primary router
        //
        // process message FROM primary and forward them to the correct channels
        tokio::spawn(async move {
            while let Some(msg) = primary_router.recv().await {
                match msg {
                    InnerNetworkMessage::PrimaryToWorker => {
                        if let Err(e) = inner_primary_to_worker.send(()).await {
                            error!(target: "inner-node-network", ?e, "primary to worker:")
                        }
                    }
                    InnerNetworkMessage::PrimaryToEngine => {
                        if let Err(e) = inner_primary_to_engine.send(()).await {
                            error!(target: "inner-node-network", ?e, "primary to engine:")
                        }
                    }
                    _ => {
                        panic!("wrong message type")
                    }
                }
            }
        });

        // obtain copy of "to primary" for engine before passing to worker inner router
        let inner_engine_to_primary = inner_worker_to_primary.clone();

        // spawn worker router
        //
        // process message FROM worker and forward to the primary
        tokio::spawn(async move {
            while let Some(msg) = worker_router.recv().await {
                match msg {
                    InnerNetworkMessage::WorkerToPrimary => {
                        if let Err(e) = inner_worker_to_primary.send(()).await {
                            error!(target: "inner-node-network", ?e, "worker to primary")
                        }
                    }
                    _ => {
                        panic!("wrong message type")
                    }
                }
            }
        });

        // spawn engine router
        //
        // process message FROM engine and forward to the primary
        tokio::spawn(async move {
            while let Some(msg) = engine_router.recv().await {
                match msg {
                    InnerNetworkMessage::EngineToPrimary => {
                        if let Err(e) = inner_engine_to_primary.send(()).await {
                            error!(target: "inner-node-network", ?e, "engine to primary")
                        }
                    }
                    _ => {
                        panic!("wrong message type")
                    }
                }
            }
        });

        let primary_handle =
            PrimaryInnerNetworkHandle { to_network: for_primary_tx, from_network: for_primary_rx };
        let worker_handle =
            WorkerInnerNetworkHandle { to_network: for_worker_tx, from_network: for_worker_rx };
        let engine_handle =
            EngineInnerNetworkHandle { to_network: for_engine_tx, from_network: for_engine_rx };

        Self { primary_handle, worker_handle, engine_handle }
    }
}
