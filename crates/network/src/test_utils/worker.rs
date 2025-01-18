//! Mock implementations for Worker to Primary/Worker.
use anemo::async_trait;
use tn_network_types::{
    BatchMessage, RequestBatchesRequest, RequestBatchesResponse, WorkerToWorker,
    WorkerToWorkerServer,
};
use tn_types::{traits::KeyPair as _, Multiaddr, NetworkKeypair};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::info;

pub struct WorkerToWorkerMockServer {
    batch_sender: Sender<BatchMessage>,
}

impl WorkerToWorkerMockServer {
    pub fn spawn(
        keypair: NetworkKeypair,
        address: Multiaddr,
    ) -> (Receiver<BatchMessage>, anemo::Network) {
        let addr = address.to_anemo_address().unwrap();
        // Channel size 1000, should be big enough for testing even if ignoring the receiver..
        let (batch_sender, batch_receiver) = channel(1000);
        let service = WorkerToWorkerServer::new(Self { batch_sender });

        let routes = anemo::Router::new().add_rpc_service(service);
        let network = anemo::Network::bind(addr)
            .server_name("consensus-test")
            .private_key(keypair.private().0.to_bytes())
            .start(routes)
            .unwrap();
        info!("starting network on: {}", network.local_addr());
        (batch_receiver, network)
    }
}

#[async_trait]
impl WorkerToWorker for WorkerToWorkerMockServer {
    async fn report_batch(
        &self,
        request: anemo::Request<BatchMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();

        self.batch_sender.send(message).await.unwrap();

        Ok(anemo::Response::new(()))
    }

    async fn request_batches(
        &self,
        _request: anemo::Request<RequestBatchesRequest>,
    ) -> Result<anemo::Response<RequestBatchesResponse>, anemo::rpc::Status> {
        tracing::error!("Not implemented WorkerToWorkerMockServer::request_batches");
        Err(anemo::rpc::Status::internal("Unimplemented"))
    }
}
