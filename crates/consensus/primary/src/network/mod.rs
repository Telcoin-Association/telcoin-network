//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use handler::RequestHandler;
pub use message::{MissingCertificatesRequest, PrimaryRequest, PrimaryResponse};
use tn_network_libp2p::{
    types::{IntoResponse as _, NetworkEvent, NetworkHandle},
    PeerId, ResponseChannel,
};
use tn_storage::traits::Database;
use tn_types::{Certificate, Header};
use tokio::sync::mpsc;
mod handler;
mod message;

#[cfg(test)]
#[path = "../tests/network_tests.rs"]
mod network_tests;

/// Convenience type for Primary network.
type Req = PrimaryRequest;
/// Convenience type for Primary network.
type Res = PrimaryResponse;

/// Handle inter-node communication between primaries.
pub struct PrimaryNetwork<DB> {
    /// Receiver for network events.
    network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: NetworkHandle<Req, Res>,
    /// Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
}

impl<DB> PrimaryNetwork<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(crate) fn new(
        network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
        network_handle: NetworkHandle<Req, Res>,
        request_handler: RequestHandler<DB>,
    ) -> Self {
        Self { network_events, network_handle, request_handler }
    }

    /// Run the network.
    async fn spawn(mut self) {
        tokio::select! {
            event = self.network_events.recv() => {
                match event {
                    Some(e) => self.process_network_event(e),
                    None => todo!(),
                }
            }
        }
    }

    /// Handle events concurrently.
    fn process_network_event(&self, event: NetworkEvent<Req, Res>) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();

        // match event
        match event {
            NetworkEvent::Request { peer, request, channel } => match request {
                PrimaryRequest::Vote { header, parents } => {
                    self.process_vote_request(
                        request_handler,
                        network_handle,
                        peer,
                        header,
                        parents,
                        channel,
                    );
                }
                PrimaryRequest::MissingCertificates { inner } => self
                    .process_request_for_missing_certs(
                        request_handler,
                        network_handle,
                        peer,
                        inner,
                        channel,
                    ),
            },
            NetworkEvent::Gossip(msg) => {
                self.process_gossip(request_handler, network_handle, msg);
            }
        }
    }

    /// Process vote request.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    fn process_vote_request(
        &self,
        request_handler: RequestHandler<DB>,
        network_handle: NetworkHandle<Req, Res>,
        peer: PeerId,
        header: Header,
        parents: Vec<Certificate>,
        channel: ResponseChannel<PrimaryResponse>,
    ) {
        tokio::spawn(async move {
            let response = request_handler.vote(peer, header, parents).await.into_response();
            let _ = network_handle.send_response(response, channel).await;
        });
    }

    /// Attempt to retrieve certificates for a peer that's missing them.
    fn process_request_for_missing_certs(
        &self,
        request_handler: RequestHandler<DB>,
        network_handle: NetworkHandle<Req, Res>,
        peer: PeerId,
        request: MissingCertificatesRequest,
        channel: ResponseChannel<PrimaryResponse>,
    ) {
        tokio::spawn(async move {
            let response = request_handler
                .process_request_for_missing_certs(peer, request)
                .await
                .into_response();
            let _ = network_handle.send_response(response, channel).await;
        });
    }

    /// Process gossip from committee.
    fn process_gossip(
        &self,
        // peerId: PeerId,
        request_handler: RequestHandler<DB>,
        network_handle: NetworkHandle<Req, Res>,
        msg: Vec<u8>,
    ) {
        tokio::spawn(async move {
            if let Err(e) = request_handler.process_gossip(msg).await {
                // network_handle.set_application_score(peer_id, new_score).await;
                //
                //
                // This should pass the entire gossip message, not just the data

                // match on error to lower peer score
                todo!();
            }
        });
    }
}
