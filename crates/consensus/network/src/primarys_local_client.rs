//! The primary's local client to the worker.
//!
//! The client requests actions from worker.

pub struct WorkersLocalClient {
    sender: tokio::sync::mpsc::Sender<()>,
}

impl WorkersLocalClient {
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
