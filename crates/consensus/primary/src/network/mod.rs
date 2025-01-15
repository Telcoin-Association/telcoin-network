// Copyright (c) Telcoin, LLC
//! Network contains logic for type that sends and receives messages between other primaries.

use crate::{anemo_network::PrimaryReceiverHandler, synchronizer::Synchronizer, ConsensusBus};
use fastcrypto::hash::Hash;
pub use message::{PrimaryRequest, PrimaryResponse};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    types::{IntoResponse as _, NetworkEvent, NetworkHandle, NetworkResult},
    ConsensusNetwork, PeerId, ResponseChannel,
};
use tn_primary_metrics::PrimaryMetrics;
use tn_storage::traits::Database;
use tn_types::{
    ensure,
    error::{CertificateError, HeaderError, HeaderResult},
    now, AuthorityIdentifier, Certificate, CertificateDigest, Header, Round,
    SignatureVerificationState, Vote,
};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};
mod message;

/// The maximum number of rounds that a proposed header can be behind.
const HEADER_AGE_LIMIT: Round = 3;

/// The tolerable amount of time to wait if a header is proposed before the current time. This
/// accounts for small drifts in time keeping between nodes. The timestamp for headers is currently
/// measured in secs.
const MAX_HEADER_TIME_DRIFT_TOLERANCE: u64 = 1;

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
    /// Missing parents are requested from peers. This is a local map to track in-flight requests
    /// for missing parents. The values are associated with the first authority that proposed a
    /// header with these parents. The node keeps track of requested Certificates to prevent
    /// unsolicited certificate attacks.
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
        // NOTE: this doesn't hurt anything since this node shouldn't vote until execution is caught
        // up
        let mut watch_execution_result = self.consensus_bus.recent_blocks().subscribe();
        while header.latest_execution_block_num > latest_block_num_hash.number {
            watch_execution_result.changed().await.map_err(|_| HeaderError::ClosedWatchChannel)?;
            latest_block_num_hash =
                self.consensus_bus.recent_blocks().borrow().latest_block_num_hash();
        }

        // ensure execution results match. execution happens in waves per round, so the latest block
        // number is likely to increase by more than 1
        //
        // NOTE: it's expected to be nearly a 0% chance that a recent block hash would match and
        // have the wrong block number
        if !self.consensus_bus.recent_blocks().borrow().contains_hash(header.latest_execution_block)
        {
            error!(
                target: "primary",
                peer_num = header.latest_execution_block_num,
                peer_hash = ?header.latest_execution_block,
                expected = ?self.consensus_bus.recent_blocks().borrow().latest_block(),
                "unexpected execution result received"
            );
            return Err(HeaderError::UnknownExecutionResult(
                header.latest_execution_block_num,
                header.latest_execution_block,
            ));
        }

        debug!(target: "primary", ?header, round = header.round(), "Processing vote request from peer");

        // certifier optimistically sends header without parents
        // however, peers may request missing certificates from the a proposer
        // when this happens, the proposer sends a new vote request with the missing parents
        // requested by this peer
        //
        // NOTE: this is a latency optimization and is not required for liveness
        if parents.is_empty() {
            // check if any parents missing
            let missing_parents = self.check_for_missing_parents(&header).await?;
            if !missing_parents.is_empty() {
                // return request for missing parents
                debug!(
                    "Received vote request for {:?} with unknown parents {:?}",
                    header, missing_parents
                );
                return Ok(PrimaryResponse::MissingParents(missing_parents));
            }
        } else {
            // validate parent signatures are present and set verification state to unverified
            let verified = parents
                .into_iter()
                .map(|mut cert| {
                    let sig = cert
                        .aggregated_signature()
                        .cloned()
                        .ok_or(HeaderError::ParentMissingSignature)?;
                    cert.set_signature_verification_state(SignatureVerificationState::Unverified(
                        sig,
                    ));
                    Ok(cert)
                })
                .collect::<HeaderResult<Vec<Certificate>>>()?;

            // try to accept parent certificates
            self.try_accept_unknown_certs(&header, verified).await?;
        }

        // Confirm all parents are accepted. If any are missing, this call will wait until they are
        // stored in the db. Eventually, this method will timeout or get cancelled for certificates
        // that never arrive.
        //
        // NOTE: this check is necessary for correctness.
        let parents = self.synchronizer.notify_read_parent_certificates(&header).await?;

        // Verify parent certs. Ensure the parents:
        // - are from the previous round
        // - created before the header
        // - are from unique authorities
        // - form a quorum through staked weight
        let mut parent_authorities = BTreeSet::new();
        let mut stake = 0;
        for parent in parents.iter() {
            ensure!(
                parent.round() + 1 == header.round(),
                HeaderError::InvalidParent(
                    "Certificate is not from the previous round".to_string()
                )
            );

            // @Steve - can you double check me here?
            //
            // confirm header created_at must always be larger than parent
            //
            // this deviates from original:
            // header.created_at() >= parent.header().created_at(),
            // Old logic was >= - but this seems wrong
            // - note: this is always in secs, so this would prevent sub-sec block production which
            //   is a goal
            ensure!(
                header.created_at() > parent.header().created_at(),
                HeaderError::InvalidParent(format!(
                    "Header was not created after parent. Header timestamp: {0} - parent timestamp: {1}",
                    header.created_at(),
                    parent.header.created_at()
                ))
            );

            ensure!(
                parent_authorities.insert(parent.header().author()),
                HeaderError::InvalidParent("Parent authors are not unique".to_string())
            );

            stake += committee.stake_by_id(parent.origin());
        }

        // verify aggregate signatures form quorum
        ensure!(
            stake >= committee.quorum_threshold(),
            HeaderError::InvalidParent(CertificateError::Inquorate.to_string())
        );

        // parents valid - now verify batches
        //
        // TODO: can this be parallelized?
        // Need to ensure an invalid parent attack shuts down batch sync
        //
        // TODO: this is called during Synchronizer::process_certificate_internal
        // - does this need to be called again?
        self.synchronizer.sync_header_batches(&header, 0).await?;

        // verify header was created in the past
        let now = now();
        if &now < header.created_at() {
            // wait if the difference is small enough
            if *header.created_at() - now <= MAX_HEADER_TIME_DRIFT_TOLERANCE {
                tokio::time::sleep(Duration::from_secs(*header.created_at() - now)).await;
            } else {
                // created_at is too far in the future
                warn!(
                    "Rejected header {:?} due to timestamp {} newer than {now}",
                    header,
                    *header.created_at()
                );

                return Err(HeaderError::InvalidTimestamp(*header.created_at(), now));
            }
        }

        // Check if node should vote for this header:
        // 1. when there is no existing vote for this public key for the epoch/round
        // 2. when there is a vote for this public key & epoch/round, and the vote is the same
        //
        // The only time the node shouldn't vote is:
        // - there is a digest for the public key for this epoch/round and it does not match the
        //   vote digest
        // - if this header is older than the previously voted on header matching the epoch/round
        //
        // check storage for a previous vote
        //
        // if a vote already exists for this author:
        // - ensure correct epoch
        // - ensure previous vote is older than current header round
        // - check if digests match to avoid voting twice for header in the same round
        let previous_vote = self
            .consensus_config
            .node_storage()
            .vote_digest_store
            .read(&header.author())
            .map_err(HeaderError::Storage)?;
        if let Some(vote_info) = previous_vote {
            ensure!(
                header.epoch() == vote_info.epoch(),
                HeaderError::InvalidEpoch { expected: header.epoch(), received: vote_info.epoch() }
            );
            ensure!(
                header.round() >= vote_info.round(),
                HeaderError::AlreadyVotedForLaterRound(header.round(), vote_info.round(),)
            );
            if header.round() == vote_info.round() {
                // Make sure we don't vote twice for the same authority in the same epoch/round.
                let vote = Vote::new(
                    &header,
                    &self.consensus_config.authority().id(),
                    self.consensus_config.key_config(),
                )
                .await;
                if vote.digest() != vote_info.vote_digest() {
                    warn!(
                        "Authority {} submitted different header {:?} for voting",
                        header.author(),
                        header,
                    );

                    // metrics
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .votes_dropped_equivocation_protection
                        .inc();

                    return Err(HeaderError::AlreadyVoted(header.digest(), header.round()));
                }

                debug!("Resending vote {vote:?} for {} at round {}", header, header.round());
                return Ok(PrimaryResponse::Vote(vote));
            }
        }

        // this node hasn't voted yet
        let vote = Vote::new(
            &header,
            &self.consensus_config.authority().id(),
            self.consensus_config.key_config(),
        )
        .await;

        debug!(target: "primary", "Created vote {vote:?} for {} at round {}", header, header.round());

        // Update the vote digest store with the vote we just sent.
        self.consensus_config.node_storage().vote_digest_store.write(&vote)?;

        Ok(PrimaryResponse::Vote(vote))
    }

    /// Helper method to retrieve parents for header.
    ///
    /// Certificates are considered "known" if they are in local storage, suspended, or already
    /// requested from a peer.
    async fn check_for_missing_parents(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<CertificateDigest>> {
        // check synchronizer state for parents
        let mut unknown_certs = self.synchronizer.get_unknown_parent_digests(header).await?;

        // ensure header is not too old
        let limit =
            self.consensus_bus.narwhal_round_updates().borrow().saturating_sub(HEADER_AGE_LIMIT);
        ensure!(limit <= header.round(), HeaderError::TooOld(header.round(), limit));

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

    /// Try to accept parents included with peer's request for vote.
    ///
    /// Parents are expected with a vote request after this node rejects a proposed header due to
    /// missing parents. The certificates are only processed if this node has requested them.
    async fn try_accept_unknown_certs(
        &self,
        header: &Header,
        mut parents: Vec<Certificate>,
    ) -> HeaderResult<()> {
        // sanitize request
        let requested_parents = self.requested_parents.lock();
        parents.retain(|cert| {
            let req = (cert.round(), cert.digest());
            if let Some(authority) = requested_parents.get(&req) {
                *authority == header.author()
            } else {
                false
            }
        });

        // try to accept
        for parent in parents {
            self.synchronizer
                .try_accept_certificate(parent)
                .await
                .map_err(|e| HeaderError::InvalidParent(e.to_string()))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        network::{PrimaryNetwork, PrimaryRequest, RequestHandler},
        synchronizer::Synchronizer,
        ConsensusBus,
    };
    use std::sync::Arc;
    use tn_network_libp2p::{
        types::{NetworkEvent, NetworkHandle},
        ResponseChannel,
    };
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{TaskManager, TnSender as _};
    use tokio::sync::mpsc;

    /// Helper function to create an instance of [PrimaryNetwork].
    fn primary_network_for_test() -> PrimaryNetwork<MemDatabase> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
        let primary = fixture.authorities().last().unwrap();
        let config = primary.consensus_config();
        let certificate_store = config.node_storage().certificate_store.clone();
        let cb = ConsensusBus::new();

        // Make a synchronizer.
        let synchronizer = Arc::new(Synchronizer::new(config.clone(), &cb));
        let task_manager = TaskManager::default();
        synchronizer.spawn(&task_manager);

        // create primary network
        let (wan, network_events) = mpsc::channel(1);
        let (network_handle_tx, network_commands) = mpsc::channel(1);
        let network_handle = NetworkHandle::new(network_handle_tx);
        let request_handler = RequestHandler::new(config, cb, synchronizer);
        PrimaryNetwork::new(network_events, network_handle, request_handler)
    }

    #[tokio::test]
    async fn test_vote_succeeds() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
        let primary0 = fixture.authorities().next().expect("4 authorities in committee fixture");
        let config = primary0.consensus_config();
        let certificate_store = config.node_storage().certificate_store.clone();
        let cb = ConsensusBus::new();
        // Make a synchronizer.
        let synchronizer = Arc::new(Synchronizer::new(config.clone(), &cb));
        let task_manager = TaskManager::default();
        synchronizer.spawn(&task_manager);

        // create primary network
        let (wan, network_events) = mpsc::channel(1);
        let (network_handle_tx, network_commands) = mpsc::channel(1);
        let network_handle = NetworkHandle::new(network_handle_tx);
        let request_handler = RequestHandler::new(config.clone(), cb, synchronizer);
        let mut primary_network =
            PrimaryNetwork::new(network_events, network_handle, request_handler);

        // spawn primary network
        task_manager.spawn_task("primary-test-network", primary_network.spawn());

        // simulate vote request from last peer in committee fixture
        let primary_peer =
            fixture.authorities().last().expect("4 authorities in committee fixture");
        let header = fixture.header_from_last_authority();
        // sanity check
        assert_eq!(primary_peer.id(), header.author());
        let parents = vec![];
        let request = PrimaryRequest::Vote { header, parents };
        let peer_network_key = primary_peer.primary_network_public_key();
        let peer_id = config
            .ed25519_fastcrypto_to_libp2p(&peer_network_key)
            .expect("fastcrypto to libp2p PeerId");
        // let response_channel = ResponseChannel::new();
        let _ = wan.send(NetworkEvent::Request { peer: peer_id, request, channel: todo!() }).await;
    }
}
