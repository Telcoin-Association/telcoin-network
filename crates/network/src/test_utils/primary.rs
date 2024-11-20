//! Mock implementations for Primary to Primary/Worker.
use anemo::async_trait;
use std::collections::HashMap;
use tn_network_types::{
    FetchBlocksRequest, FetchBlocksResponse, FetchCertificatesRequest, FetchCertificatesResponse,
    PrimaryToPrimary, PrimaryToPrimaryServer, PrimaryToWorker, PrimaryToWorkerServer,
    RequestVoteRequest, RequestVoteResponse, SendCertificateRequest, SendCertificateResponse,
    WorkerSynchronizeMessage,
};
use tn_types::{traits::KeyPair as _, Multiaddr, NetworkKeypair};

use tokio::sync::mpsc::{channel, Receiver, Sender};
use tracing::info;

#[derive(Clone)]
pub struct PrimaryToPrimaryMockServer {
    sender: Sender<SendCertificateRequest>,
}

impl PrimaryToPrimaryMockServer {
    pub fn spawn(
        network_keypair: NetworkKeypair,
        address: Multiaddr,
    ) -> (Receiver<SendCertificateRequest>, anemo::Network) {
        let addr = address.to_anemo_address().unwrap();
        let (sender, receiver) = channel(1);
        let service = PrimaryToPrimaryServer::new(Self { sender });

        let routes = anemo::Router::new().add_rpc_service(service);
        let network = anemo::Network::bind(addr)
            .server_name("narwhal")
            .private_key(network_keypair.private().0.to_bytes())
            .start(routes)
            .unwrap();
        info!("starting network on: {}", network.local_addr());
        (receiver, network)
    }
}

#[async_trait]
impl PrimaryToPrimary for PrimaryToPrimaryMockServer {
    async fn send_certificate(
        &self,
        request: anemo::Request<SendCertificateRequest>,
    ) -> Result<anemo::Response<SendCertificateResponse>, anemo::rpc::Status> {
        let message = request.into_body();

        self.sender.send(message).await.unwrap();

        Ok(anemo::Response::new(SendCertificateResponse { accepted: true }))
    }

    async fn request_vote(
        &self,
        _request: anemo::Request<RequestVoteRequest>,
    ) -> Result<anemo::Response<RequestVoteResponse>, anemo::rpc::Status> {
        unimplemented!()
    }

    async fn fetch_certificates(
        &self,
        _request: anemo::Request<FetchCertificatesRequest>,
    ) -> Result<anemo::Response<FetchCertificatesResponse>, anemo::rpc::Status> {
        unimplemented!()
    }
}

pub struct PrimaryToWorkerMockServer {
    // TODO: refactor tests to use mockall for this.
    synchronize_sender: Sender<WorkerSynchronizeMessage>,
}

impl PrimaryToWorkerMockServer {
    pub fn spawn(
        keypair: NetworkKeypair,
        address: Multiaddr,
    ) -> (Receiver<WorkerSynchronizeMessage>, anemo::Network) {
        let addr = address.to_anemo_address().unwrap();
        let (synchronize_sender, synchronize_receiver) = channel(1);
        let service = PrimaryToWorkerServer::new(Self { synchronize_sender });

        let routes = anemo::Router::new().add_rpc_service(service);
        let network = anemo::Network::bind(addr)
            .server_name("narwhal")
            .private_key(keypair.private().0.to_bytes())
            .start(routes)
            .unwrap();
        info!("starting network on: {}", network.local_addr());
        (synchronize_receiver, network)
    }
}

#[async_trait]
impl PrimaryToWorker for PrimaryToWorkerMockServer {
    async fn synchronize(
        &self,
        request: anemo::Request<WorkerSynchronizeMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let message = request.into_body();
        self.synchronize_sender.send(message).await.unwrap();
        Ok(anemo::Response::new(()))
    }

    async fn fetch_blocks(
        &self,
        _request: anemo::Request<FetchBlocksRequest>,
    ) -> Result<anemo::Response<FetchBlocksResponse>, anemo::rpc::Status> {
        Ok(anemo::Response::new(FetchBlocksResponse { blocks: HashMap::new() }))
    }
}
