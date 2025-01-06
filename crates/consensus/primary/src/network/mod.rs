// Copyright (c) Telcoin, LLC
//! Network contains logic for type that sends and receives messages between other primaries.

use crate::{anemo_network::PrimaryReceiverHandler, ConsensusBus};
pub use message::{PrimaryRequest, PrimaryResponse};
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    types::{IntoResponse as _, NetworkEvent, NetworkHandle, NetworkResult},
    ConsensusNetwork, PeerId, ResponseChannel,
};
use tn_primary_metrics::PrimaryMetrics;
use tn_storage::traits::Database;
use tn_types::{
    ensure,
    error::{HeaderError, HeaderResult},
    Certificate, Header,
};
use tokio::sync::mpsc;
mod message;

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
    pub fn new(
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
                    Some(e) => match e {
                        NetworkEvent::Request { peer, request, channel } => {
                            // match request and send to actor
                            // - mpsc::Sender<_>::send(request, oneshot);
                            // let res = oneshot.await?;
                            // self.network_handle.send_response(res, channel).await?;
                            match request {
                                PrimaryRequest::NewCertificate { certificate } => todo!(),
                                PrimaryRequest::Vote { header, parents } => {
                                    self.process_vote_request(peer, header, parents, channel);
                                },
                                PrimaryRequest::MissingCertificates { inner } => todo!(),
                            }
                        }
                        NetworkEvent::Gossip(bytes) => {
                            // match gossip and send to actor
                            todo!()
                        }
                    },
                    None => todo!(),
                }
            }
        }
    }

    /// Process vote request.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    fn process_vote_request(
        &self,
        peer: PeerId,
        header: Header,
        parents: Vec<Certificate>,
        channel: ResponseChannel<PrimaryResponse>,
    ) {
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        tokio::spawn(async move {
            let response = request_handler.vote(peer, header, parents).into_response();
            let _ = network_handle.send_response(response, channel).await;
        });
    }
}

/// The type that handles requests from peers.
#[derive(Clone)]
struct RequestHandler<DB> {
    /// Consensus config with access to database.
    consensus_config: ConsensusConfig<DB>,
    /// Inner-processs channel bus.
    consensus_bus: ConsensusBus,
}

impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(consensus_config: ConsensusConfig<DB>, consensus_bus: ConsensusBus) -> Self {
        Self { consensus_config, consensus_bus }
    }

    /// Evaluate request to possibly issue a vote in support of peer's header.
    fn vote(
        &self,
        peer: PeerId,
        header: Header,
        parents: Vec<Certificate>,
    ) -> HeaderResult<PrimaryResponse> {
        // current committee
        let committee = self.consensus_config.committee();

        // validate header
        header.validate(committee, &self.consensus_config.worker_cache())?;

        // validate parents
        let num_parents = parents.len();
        ensure!(
            num_parents <= committee.size(),
            HeaderError::TooManyParents(num_parents, committee.size())
        );
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_in_votes
            .inc_by(num_parents as u64);

        // TODO: DO NOT MERGE - remove this once config updated
        let converted_network_key =
            self.consensus_config.ed25519_libp2p_to_fastcrypto(&peer).ok_or(HeaderError::PeerId)?;
        // !!^^^^^^end

        // ensure request for vote came from the header's author
        let committee_peer = committee
            .authority_by_network_key(&converted_network_key)
            .ok_or(HeaderError::AuthorityByNetworkKey)?;
        ensure!(header.author() == committee_peer.id(), HeaderError::PeerNotAuthor);

        // TODO: ensure peer's header isn't too far in the past
        //  - peer can't propose a block from round 1 when this node is on 100
        // ^^^^^^^^^^^^^^^^^^^^^^^^^^ TODO: check if header is too old
        //
        //

        // logic:
        // - ensure block header isn't too far in the past
        // - ensure block header isn't too far in the future
        //      - if block header is ahead, but within bounds, then wait for EL results

        // check watch channel that latest block num is within bounds
        // proposed headers must be within a few blocks of this header's block number
        let latest_block_num = self.consensus_bus.recent_blocks().borrow().latest_block_num_hash();
        ensure!(
            header.latest_execution_block_num <= latest_block_num.number + 3,
            HeaderError::AdvancedExecution(
                header.latest_execution_block_num,
                latest_block_num.number
            )
        );
        // then, if within limits, wait for execution updates

        todo!()
    }
}
