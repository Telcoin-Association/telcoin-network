//! The worker's local client to the primary.
//!
//! The client reports blocks to the primary.

use anemo::async_trait;
use consensus_network_types::{WorkerOthersBlockMessage, WorkerOwnBlockMessage, WorkerToPrimary};

/// The client for local worker to primary messages.
///
/// This only uses a channel not a WAN.
#[derive(Clone)]
pub struct WorkersLocalClient {
    sender: tokio::sync::mpsc::Sender<()>,
}

impl WorkersLocalClient {
    /// Create a new instance of Self.
    pub fn new(sender: tokio::sync::mpsc::Sender<()>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl WorkerToPrimary for WorkersLocalClient {
    async fn report_own_block(
        &self,
        request: anemo::Request<WorkerOwnBlockMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        if self.sender.send(()).await.is_ok() {
            Ok(anemo::Response::new(()))
        } else {
            Err(anemo::rpc::Status::new(anemo::types::response::StatusCode::InternalServerError))
        }
    }

    async fn report_others_block(
        &self,
        request: anemo::Request<WorkerOthersBlockMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        if self.sender.send(()).await.is_ok() {
            Ok(anemo::Response::new(()))
        } else {
            Err(anemo::rpc::Status::new(anemo::types::response::StatusCode::InternalServerError))
        }
    }
}
