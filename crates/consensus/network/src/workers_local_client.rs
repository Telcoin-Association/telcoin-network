//! The worker's local client to the primary.
//!
//! The client reports blocks to the primary.

use anemo::async_trait;
use consensus_network_types::WorkerToPrimary;

/// The client for local worker to primary messages.
///
/// This only uses a channel not a WAN.
#[derive(Clone)]
pub struct WorkersLocalClient {
    sender: tokio::sync::mpsc::Sender<()>,
}

#[async_trait]
impl WorkerToPrimary for WorkersLocalClient {
    async fn report_own_block(
        &self,
        request: consensus_network_types::WorkerOwnBlockMessage,
    ) -> eyre::Result<(), crate::error::LocalClientError> {
        self.sender.send(());
        todo!()
    }

    async fn report_others_block(
        &self,
        request: consensus_network_types::WorkerOthersBlockMessage,
    ) -> eyre::Result<(), crate::error::LocalClientError> {
        self.sender.send(());
        todo!()
    }
}
