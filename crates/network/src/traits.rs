use crate::{error::LocalClientError, CancelOnDropHandler};
use eyre::Result;
use std::future::Future;
use tn_network_types::{
    FetchBatchResponse, FetchBatchesRequest, FetchCertificatesRequest, FetchCertificatesResponse,
    RequestBatchesRequest, RequestBatchesResponse, WorkerOthersBatchMessage, WorkerOwnBatchMessage,
    WorkerSynchronizeMessage,
};
use tn_types::NetworkPublicKey;

pub trait ReliableNetwork<Request: Clone + Send + Sync> {
    type Response: Clone + Send + Sync;

    fn send(
        &self,
        peer: NetworkPublicKey,
        message: &Request,
    ) -> CancelOnDropHandler<Result<anemo::Response<Self::Response>>>;

    fn broadcast(
        &self,
        peers: Vec<NetworkPublicKey>,
        message: &Request,
    ) -> Vec<CancelOnDropHandler<Result<anemo::Response<Self::Response>>>> {
        let mut handlers = Vec::new();
        for peer in peers {
            let handle = self.send(peer, message);
            handlers.push(handle);
        }
        handlers
    }
}

pub trait PrimaryToPrimaryRpc {
    fn fetch_certificates(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<FetchCertificatesRequest> + Send,
    ) -> impl Future<Output = Result<FetchCertificatesResponse>>;
}

pub trait PrimaryToWorkerClient {
    fn synchronize(
        &self,
        worker_name: NetworkPublicKey,
        request: WorkerSynchronizeMessage,
    ) -> impl Future<Output = Result<(), LocalClientError>>;

    fn fetch_batches(
        &self,
        worker_name: NetworkPublicKey,
        request: FetchBatchesRequest,
    ) -> impl Future<Output = Result<FetchBatchResponse, LocalClientError>>;
}

pub trait WorkerToPrimaryClient {
    fn report_own_batch(
        &self,
        request: WorkerOwnBatchMessage,
    ) -> impl Future<Output = Result<(), LocalClientError>>;

    fn report_others_batch(
        &self,
        request: WorkerOthersBatchMessage,
    ) -> impl Future<Output = Result<(), LocalClientError>>;
}

pub trait WorkerRpc {
    fn request_batches(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<RequestBatchesRequest> + Send,
    ) -> impl Future<Output = Result<RequestBatchesResponse>>;
}
