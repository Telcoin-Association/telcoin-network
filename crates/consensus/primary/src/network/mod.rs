// Copyright (c) Telcoin, LLC
//! Network contains logic for type that sends and receives messages between other primaries.

use crate::{anemo_network::PrimaryReceiverHandler, synchronizer::Synchronizer, ConsensusBus};
pub use message::{PrimaryRequest, PrimaryResponse};
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc};
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
    AuthorityIdentifier, Certificate, CertificateDigest, Header, Round,
};
use tokio::sync::mpsc;
use tracing::{debug, error};
mod message;

/// The maximum number of rounds that a proposed header can be behind.
const HEADER_AGE_LIMIT: Round = 3;

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
            let response = request_handler.vote(peer, header, parents).await.into_response();
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
    /// Synchronizer has ability to fetch missing data from peers.
    synchronizer: Arc<Synchronizer<DB>>,
    /// The digests of parents that are currently being requested from peers.
    ///
    /// Missing parents are requested from peers. This is a local map to track in-flight requests for missing parents. The values are associated with the first authority that proposed a header with these parents.
    requested_parents: Arc<Mutex<BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>>>,
}

impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        synchronizer: Arc<Synchronizer<DB>>,
    ) -> Self {
        Self {
            consensus_config,
            consensus_bus,
            synchronizer,
            requested_parents: Default::default(),
        }
    }

    /// Evaluate request to possibly issue a vote in support of peer's header.
    async fn vote(
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
        // let mut watch_execution_results = self.consensus_bus.recent_blocks().subscribe();
        let mut latest_block_num_hash =
            self.consensus_bus.recent_blocks().borrow().latest_block_num_hash();

        // TODO: update watch channel to map consensus round with block numhash?
        //
        // peer built off round 1 <= we're on round 100 + 3
        // however, the block number doesn't equate to the actual round of the block
        // which would be more appropriate to check in this case

        // if peer is ahead, wait for execution to catch up
        //
        // NOTE: this doesn't hurt anything since this node shouldn't vote until execution is caught up
        let mut watch_execution_result = self.consensus_bus.recent_blocks().subscribe();
        while header.latest_execution_block_num > latest_block_num_hash.number {
            watch_execution_result.changed().await.map_err(|_| HeaderError::ClosedWatchChannel)?;
            latest_block_num_hash =
                self.consensus_bus.recent_blocks().borrow().latest_block_num_hash();
        }

        // ensure execution results match. execution happens in waves per round, so the latest block number is likely to increase by more than 1
        //
        // NOTE: it's expected to be nearly a 0% chance that a recent block hash would match and have the wrong block number
        let recent_blocks = self.consensus_bus.recent_blocks().borrow();
        if !recent_blocks.contains_hash(header.latest_execution_block) {
            error!(
                target: "primary",
                peer_num = header.latest_execution_block_num,
                peer_hash = ?header.latest_execution_block,
                expected = ?recent_blocks.latest_block(),
                "unexpected execution result received"
            );
            return Err(HeaderError::UnknownExecutionResult(
                header.latest_execution_block_num,
                header.latest_execution_block,
            ));
        }

        // drop read lock for watch channel
        drop(recent_blocks);

        debug!(target: "primary", ?header, round = header.round(), "Processing vote request from peer");

        // certifier optimistically sends header without parents
        // however, peers may request missing certificates from the a proposer
        // when this happens, the proposer sends a new vote request with the missing parents requested by this peer
        //
        // NOTE: this is a latency optimization and is not required for liveness
        if parents.is_empty() {
            // check if any parents missing
            // let missing_digests = self
            todo!()
        } else {
            // validate parents included with new proposal
            todo!()
        }
    }

    /// Helper method to retrieve parents for header.
    ///
    /// Certificates are considered "known" if they are in local storage, suspended, or already requested from a peer.
    async fn check_for_missing_parents(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<CertificateDigest>> {
        // check synchronizer state for parents
        let mut unknown_certs = self.synchronizer.get_unknown_parent_digests(header).await?;

        // ensure header is not too old
        let current_round = self.consensus_bus.narwhal_round_updates().borrow();
        let limit = current_round.saturating_sub(HEADER_AGE_LIMIT);
        ensure!(limit <= header.round(), HeaderError::TooOld(header.round(), limit));

        // drop borrow
        drop(current_round);

        // lock to ensure consistency between limit_round and where parent_digests are gc'ed
        let mut current_requests = self.requested_parents.lock();

        // remove entries that are past the limit
        //
        // NOTE: the minimum parent round is the limit - 1
        while let Some(((round, _), _)) = current_requests.first_key_value() {
            if round < &limit.saturating_sub(1) {
                current_requests.pop_first();
            } else {
                break;
            }
        }

        // filter out parents that were already requested and new ones
        unknown_certs.retain(|digest| {
            let key = (header.round() - 1, *digest);
            if !current_requests.contains_key(&key) {
                current_requests.insert(key, header.author());
                true
            } else {
                false
            }
        });

        Ok(unknown_certs)
    }
}
