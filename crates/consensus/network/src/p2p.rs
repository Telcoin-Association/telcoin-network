// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    traits::{PrimaryToPrimaryRpc, ReliableNetwork, WorkerRpc},
    CancelOnDropHandler, RetryConfig,
};
use anemo::PeerId;
use eyre::{format_err, Result};
use narwhal_network_types::{
    FetchCertificatesRequest, FetchCertificatesResponse, PrimaryToPrimaryClient,
    RequestBlocksRequest, RequestBlocksResponse, WorkerBlockMessage, WorkerToWorkerClient,
};
use std::time::Duration;
use tn_types::NetworkPublicKey;

fn send<F, R, Fut>(
    network: anemo::Network,
    peer: NetworkPublicKey,
    f: F,
) -> CancelOnDropHandler<Result<anemo::Response<R>>>
where
    F: Fn(anemo::Peer) -> Fut + Send + Sync + 'static + Clone,
    R: Send + Sync + 'static + Clone,
    Fut: std::future::Future<Output = Result<anemo::Response<R>, anemo::rpc::Status>> + Send,
{
    // Safety
    // Since this spawns an unbounded task, this should be called in a time-restricted fashion.

    let peer_id = PeerId(peer.0.to_bytes());
    let message_send = move || {
        let network = network.clone();
        let f = f.clone();

        async move {
            if let Some(peer) = network.peer(peer_id) {
                f(peer).await.map_err(|e| {
                    // this returns a backoff::Error::Transient
                    // so that if anemo::Status is returned, we retry
                    backoff::Error::transient(eyre::eyre!("RPC error: {e:?}"))
                })
            } else {
                Err(backoff::Error::transient(eyre::eyre!("not connected to peer {peer_id}")))
            }
        }
    };

    let retry_config = RetryConfig {
        retrying_max_elapsed_time: None, // retry forever
        ..Default::default()
    };
    let task = tokio::spawn(retry_config.retry(message_send));

    CancelOnDropHandler(task)
}

//
// Primary-to-Primary
//

impl PrimaryToPrimaryRpc for anemo::Network {
    async fn fetch_certificates(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<FetchCertificatesRequest> + Send,
    ) -> Result<FetchCertificatesResponse> {
        let peer_id = PeerId(peer.0.to_bytes());
        let peer = self
            .peer(peer_id)
            .ok_or_else(|| format_err!("Network has no connection with peer {peer_id}"))?;
        let response = PrimaryToPrimaryClient::new(peer)
            .fetch_certificates(request)
            .await
            .map_err(|e| format_err!("Network error {:?}", e))?;
        Ok(response.into_body())
    }
}

impl ReliableNetwork<WorkerBlockMessage> for anemo::Network {
    type Response = ();
    fn send(
        &self,
        peer: NetworkPublicKey,
        message: &WorkerBlockMessage,
    ) -> CancelOnDropHandler<Result<anemo::Response<()>>> {
        let message = message.to_owned();
        let f = move |peer| {
            // Timeout will be retried in send().
            let req = anemo::Request::new(message.clone()).with_timeout(Duration::from_secs(15));
            async move { WorkerToWorkerClient::new(peer).report_block(req).await }
        };

        send(self.clone(), peer, f)
    }
}

impl WorkerRpc for anemo::Network {
    async fn request_blocks(
        &self,
        peer: &NetworkPublicKey,
        request: impl anemo::types::request::IntoRequest<RequestBlocksRequest> + Send,
    ) -> Result<RequestBlocksResponse> {
        let peer_id = PeerId(peer.0.to_bytes());
        let peer = self
            .peer(peer_id)
            .ok_or_else(|| format_err!("Network has no connection with peer {peer_id}"))?;
        let response = WorkerToWorkerClient::new(peer)
            .request_blocks(request)
            .await
            .map_err(|e| format_err!("Network error {:?}", e))?;
        Ok(response.into_body())
    }
}
