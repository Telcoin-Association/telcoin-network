use std::future::Future;

// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{error::LocalClientError, CancelOnDropHandler};
use eyre::Result;
use narwhal_network_types::{
    FetchBlocksRequest, FetchBlocksResponse, FetchCertificatesRequest, FetchCertificatesResponse,
    RequestBlocksRequest, RequestBlocksResponse, WorkerOthersBlockMessage, WorkerOwnBlockMessage,
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
        request: FetchBlocksRequest,
    ) -> impl Future<Output = Result<FetchBlocksResponse, LocalClientError>>;
}

pub trait WorkerToPrimaryClient {
    fn report_own_block(
        &self,
        request: WorkerOwnBlockMessage,
    ) -> impl Future<Output = Result<(), LocalClientError>>;

    fn report_others_block(
        &self,
        request: WorkerOthersBlockMessage,
    ) -> impl Future<Output = Result<(), LocalClientError>>;
}

pub trait WorkerRpc {
    fn request_blocks(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<RequestBlocksRequest> + Send,
    ) -> impl Future<Output = Result<RequestBlocksResponse>>;
}
