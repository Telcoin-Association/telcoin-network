//! Handle specific request types received from the network.

use super::{
    message::{ConsensusResult, MissingCertificatesRequest},
    PrimaryResponse,
};
use crate::{
    error::{CertManagerError, PrimaryNetworkError, PrimaryNetworkResult},
    network::message::PrimaryGossip,
    state_sync::{CertificateCollector, StateSynchronizer},
    ConsensusBus, NodeMode,
};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network_libp2p::GossipMessage;
use tn_storage::{tables::ConsensusBlocks, ConsensusStore, EpochStore, VoteDigestStore};
use tn_types::{
    ensure,
    error::{CertificateError, HeaderError, HeaderResult},
    now, to_intent_message, try_decode, AuthorityIdentifier, BlockHash, BlsPublicKey, Certificate,
    CertificateDigest, ConsensusHeader, Database, Epoch, EpochCertificate, EpochRecord, Hash as _,
    Header, ProtocolSignature, Round, SignatureVerificationState, TnSender as _, Vote,
};
use tokio::sync::oneshot;
use tracing::{debug, error, warn};

/// The type that handles requests from peers.
#[derive(Clone)]
pub(crate) struct RequestHandler<DB> {
    /// Consensus config with access to database.
    consensus_config: ConsensusConfig<DB>,
    /// Inner-processs channel bus.
    consensus_bus: ConsensusBus,
    /// Synchronize state between peers.
    state_sync: StateSynchronizer<DB>,
    /// The digests of parents that are currently being requested from peers.
    ///
    /// Missing parents are requested from peers. This is a local map to track in-flight requests
    /// for missing parents. The values are associated with the first authority that proposed a
    /// header with these parents. The node keeps track of requested Certificates to prevent
    /// unsolicited certificate attacks.
    requested_parents: Arc<Mutex<BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>>>,
    /// Track consensus headers until we hit a simple quorum then send on.
    consensus_certs: Arc<Mutex<HashMap<BlockHash, u32>>>,
}

impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(crate) fn new(
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        state_sync: StateSynchronizer<DB>,
    ) -> Self {
        Self {
            consensus_config,
            consensus_bus,
            state_sync,
            requested_parents: Default::default(),
            consensus_certs: Default::default(),
        }
    }

    /// Process gossip from the committee.
    ///
    /// Peers gossip the CertificateDigest so peers can request the Certificate. This waits until
    /// the certificate can be retrieved and timesout after some time. It's important to give up
    /// after enough time to limit the DoS attack surface. Peers who timeout must lose reputation.
    pub(super) async fn process_gossip(&self, msg: &GossipMessage) -> PrimaryNetworkResult<()> {
        // deconstruct message
        let GossipMessage { data, topic, .. } = msg;

        // gossip is uncompressed
        let gossip = try_decode(data)?;

        match gossip {
            PrimaryGossip::Certificate(cert) => {
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::primary_topic()),
                    PrimaryNetworkError::InvalidTopic
                );
                // process certificate
                let unverified_cert = cert.validate_received().map_err(CertManagerError::from)?;
                self.state_sync.process_peer_certificate(unverified_cert).await?;
            }
            PrimaryGossip::Consenus(result) => {
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::consensus_output_topic()),
                    PrimaryNetworkError::InvalidTopic
                );
                let ConsensusResult { epoch, number, hash, validator: key, signature } = *result;
                let (old_number, old_hash) =
                    *self.consensus_bus.last_published_consensus_num_hash().borrow();
                if hash == old_hash || old_number >= number {
                    return Ok(()); // We have already dealt with this hash.
                }
                if let Some(committee) =
                    self.consensus_config.node_storage().get_committee_keys(epoch)
                {
                    // If we do not have the committee to verify this message then just ignore for
                    // now. Another one will be along soon and we should be
                    // syncing epochs in the background.
                    ensure!(
                        committee.contains(&key),
                        PrimaryNetworkError::PeerNotInCommittee(Box::new(key))
                    );
                    ensure!(
                        signature.verify_secure(&to_intent_message(hash), &key),
                        PrimaryNetworkError::UnknownConsensusHeaderCert(hash)
                    );
                    // Once we have seen 1/3 + 1 committe members have signed this it should be
                    // valid.
                    let enough_sigs = (committee.len() / 3) + 1;
                    let mut consensus_certs = self.consensus_certs.lock();
                    let sigs = consensus_certs.get(&hash).copied();
                    if let Some(sigs) = sigs {
                        if (sigs + 1) as usize >= enough_sigs {
                            // Last consensus block we have executed, us this to determine if we are
                            // too far behind.
                            let (exec_number, exec_epoch) = self
                                .consensus_config
                                .node_storage()
                                .last_record::<ConsensusBlocks>()
                                .map(|(n, h)| (n, h.sub_dag.leader_epoch()))
                                .unwrap_or((0, 0));
                            // Use GC depth to estimate how many blocks we can be behind (roughly
                            // one block every two rounds).
                            let allowed_behind =
                                (self.consensus_config.parameters().gc_depth / 2) - 2;
                            if matches!(
                                *self.consensus_bus.node_mode().borrow(),
                                NodeMode::CvvActive
                            ) && ((exec_number + allowed_behind as u64) < number
                                || (epoch > exec_epoch && exec_number + 1 < number))
                            {
                                // We seem to be too far behind to be an active CVV, try to go
                                // inactive to catch up.
                                let _ = self.consensus_bus.node_mode().send(NodeMode::CvvInactive);
                                self.consensus_config.shutdown().notify();
                                consensus_certs.clear();
                                return Ok(());
                            }
                            // Make sure we don't get old gossip and go backwards.
                            if number > old_number {
                                // Only send this when we are sure it is valid.
                                // Receivers will count on this being verified.
                                let _ = self
                                    .consensus_bus
                                    .last_published_consensus_num_hash()
                                    .send((number, hash));
                            }
                            consensus_certs.clear();
                        } else {
                            consensus_certs.insert(hash, sigs + 1);
                        }
                    } else {
                        consensus_certs.insert(hash, 1);
                    }
                } else {
                    let latest_missing = *self.consensus_bus.requested_missing_epoch().borrow();
                    if epoch > latest_missing {
                        // Not sure we can sanity check this epoch.  However if it is bogus the code
                        // to handle it should be fine and will reset requested_missing_epoch to
                        // sanity.
                        let _ = self.consensus_bus.requested_missing_epoch().send(epoch);
                    }
                }
            }
            PrimaryGossip::EpochVote(vote) => {
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::epoch_vote_topic()),
                    PrimaryNetworkError::InvalidTopic
                );
                let (tx, rx) = oneshot::channel();
                let _ = self.consensus_bus.new_epoch_votes().send((*vote, tx)).await;
                match rx.await {
                    // Propogate any errors so the peer can be punished.
                    Ok(res) => res?,
                    // Don't punish the peer for an internal channel issue...
                    Err(e) => error!(target: "primary", "error waiting on epoch vote result: {e}"),
                }
            }
        }

        Ok(())
    }

    /// Evaluate request to possibly issue a vote in support of peer's header.
    pub(crate) async fn vote(
        &self,
        peer: BlsPublicKey,
        header: Header,
        parents: Vec<Certificate>,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        // current committee
        let committee = self.consensus_config.committee();

        // validate header
        header.validate(committee)?;

        // validate parents
        let num_parents = parents.len();
        ensure!(
            num_parents <= committee.size(),
            HeaderError::TooManyParents(num_parents, committee.size()).into()
        );
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_in_votes
            .inc_by(num_parents as u64);

        let committee_peer = header.author.clone();
        ensure!(
            self.consensus_config.in_committee(&committee_peer),
            HeaderError::UnknownNetworkKey(Box::new(peer)).into()
        );
        let auth_id: AuthorityIdentifier = peer.into();
        if let Some(auth) = self.consensus_config.committee().authority(&committee_peer) {
            // We err on the side of caution here, if auths peer id is not known fail but we
            // should know it (got a vote request from them).
            ensure!(auth_id == auth.id(), HeaderError::PeerNotAuthor.into());
        } else {
            // The committee check above passed so this should not happen, but just in case.
            return Err(HeaderError::UnknownNetworkKey(Box::new(peer)).into());
        }

        // if peer is ahead, wait for execution to catch up
        // NOTE: this doesn't hurt since this node shouldn't vote until execution is caught up
        // ensure execution results match if this succeeds.
        if self.consensus_bus.wait_for_execution(header.latest_execution_block).await.is_err() {
            error!(
                target: "primary",
                peer_hash = ?header.latest_execution_block,
                expected = ?self.consensus_bus.recent_blocks().borrow().latest_block(),
                "unexpected execution result received"
            );
            return Err(HeaderError::UnknownExecutionResult(header.latest_execution_block).into());
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
                    let sig =
                        cert.aggregated_signature().ok_or(HeaderError::ParentMissingSignature)?;
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
        let parents = self.state_sync.notify_read_parent_certificates(&header).await?;

        // Verify parent certs. Ensure the parents:
        // - are from the previous round
        // - created before the header
        // - are from unique authorities
        // - form a quorum through staked weight
        let mut parent_authorities = BTreeSet::new();
        let mut stake = 0;
        for parent in parents.iter() {
            ensure!(parent.round() + 1 == header.round(), HeaderError::InvalidParentRound.into());

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
                header.created_at() >= parent.header().created_at(),
                HeaderError::InvalidParentTimestamp {
                    header: *header.created_at(),
                    parent: *parent.header.created_at()
                }
                .into()
            );

            ensure!(
                parent_authorities.insert(parent.header().author()),
                HeaderError::DuplicateParents.into()
            );

            stake += committee.voting_power_by_id(parent.origin());
        }

        // verify aggregate signatures form quorum
        let threshold = committee.quorum_threshold();
        ensure!(
            stake >= threshold,
            CertManagerError::from(CertificateError::Inquorate { stake, threshold }).into()
        );

        // parents valid - now verify batches
        // NOTE: this blocks until batches become available
        self.state_sync.sync_header_batches(&header, false, 0).await?;

        // verify header was created in the past
        let now = now();
        if &now < header.created_at() {
            // wait if the difference is small enough
            if *header.created_at() - now
                <= self
                    .consensus_config
                    .network_config()
                    .sync_config()
                    .max_header_time_drift_tolerance
            {
                tokio::time::sleep(Duration::from_secs(*header.created_at() - now)).await;
            } else {
                // created_at is too far in the future
                warn!(
                    "Rejected header {:?} due to timestamp {} newer than {now}",
                    header,
                    *header.created_at()
                );

                return Err(HeaderError::InvalidTimestamp {
                    created: *header.created_at(),
                    received: now,
                }
                .into());
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
            .read_vote_info(header.author())
            .map_err(HeaderError::Storage)?;
        if let Some(vote_info) = previous_vote {
            ensure!(
                header.epoch() == vote_info.epoch(),
                HeaderError::InvalidEpoch { theirs: header.epoch(), ours: vote_info.epoch() }
                    .into()
            );
            ensure!(
                header.round() >= vote_info.round(),
                HeaderError::AlreadyVotedForLaterRound {
                    theirs: header.round(),
                    ours: vote_info.round()
                }
                .into()
            );
            if header.round() == vote_info.round() {
                // Make sure we don't vote twice for the same authority in the same epoch/round.
                let vote = Vote::new(
                    &header,
                    self.consensus_config.authority_id().expect("only validators can vote"),
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

                    return Err(HeaderError::AlreadyVoted(header.digest(), header.round()).into());
                }

                debug!("Resending vote {vote:?} for {} at round {}", header, header.round());
                return Ok(PrimaryResponse::Vote(vote));
            }
        }

        // this node hasn't voted yet
        let vote = Vote::new(
            &header,
            self.consensus_config.authority_id().expect("only validators can vote"),
            self.consensus_config.key_config(),
        )
        .await;

        debug!(target: "primary", "Created vote {vote:?} for {} at round {}", header, header.round());

        // Update the vote digest store with the vote we just sent.
        self.consensus_config.node_storage().write_vote(&vote)?;

        Ok(PrimaryResponse::Vote(vote))
    }

    /// Helper method to retrieve parents for header.
    ///
    /// Certificates are considered "known" if they are in local storage, pending, or already
    /// requested from a peer.
    async fn check_for_missing_parents(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<CertificateDigest>> {
        // identify parents that are neither in storage nor pending
        let mut unknown_certs = self.state_sync.identify_unkown_parents(header).await?;

        // ensure header is not too old
        let limit = self.consensus_bus.primary_round_updates().borrow().saturating_sub(
            self.consensus_config.network_config().sync_config().max_proposed_header_age_limit,
        );
        ensure!(
            limit <= header.round(),
            HeaderError::TooOld {
                digest: header.digest(),
                header_round: header.round(),
                max_round: limit,
            }
        );

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
            if let std::collections::btree_map::Entry::Vacant(e) = current_requests.entry(key) {
                e.insert(header.author().clone());
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
    ) -> PrimaryNetworkResult<()> {
        // sanitize request
        {
            let requested_parents = self.requested_parents.lock();
            parents.retain(|cert| {
                let req = (cert.round(), cert.digest());
                if let Some(authority) = requested_parents.get(&req) {
                    authority == header.author()
                } else {
                    false
                }
            });
        }

        // try to accept
        for parent in parents {
            self.state_sync.process_peer_certificate(parent).await?;
        }

        Ok(())
    }

    /// Process a request from a peer for missing certificates.
    ///
    /// This method efficiently retrieves certificates that the requesting peer is missing while
    /// protecting against malicious requests through:
    /// - limiting total processing time
    /// - processing certificates in chunks
    /// - validating request parameters
    pub(crate) async fn retrieve_missing_certs(
        &self,
        request: MissingCertificatesRequest,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        // validates request is within limits
        let mut collector = CertificateCollector::new(request, self.consensus_config.clone())?;

        // Create a time-bounded iter for collecting certificates
        let mut missing = Vec::new();

        // Collect certificates from the stream
        for cert in collector.by_ref() {
            missing.push(cert?);

            // yield occassionally to allow the request handler shutdown during network timeout
            if missing.len() % 10 == 0 {
                tokio::task::yield_now().await;
            }
        }

        debug!(
            target: "cert-collector",
            "Collected {} certificates in {}ms",
            missing.len(),
            collector.start_time().elapsed().as_millis(),
        );

        Ok(PrimaryResponse::RequestedCertificates(missing))
    }

    /// Retrieve a consensus header from local storage.
    pub(super) async fn retrieve_consensus_header(
        &self,
        number: Option<u64>,
        hash: Option<BlockHash>,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        let header = match (number, hash) {
            (_, Some(hash)) => self.get_header_by_hash(hash)?,
            (Some(number), _) => self.get_header_by_number(number)?,
            (None, None) => self.get_latest_output()?,
        };

        Ok(PrimaryResponse::ConsensusHeader(Arc::new(header)))
    }

    /// Retrieve an epoch record from local storage.
    pub(super) async fn retrieve_epoch_record(
        &self,
        epoch: Option<Epoch>,
        hash: Option<BlockHash>,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        let (record, certificate) = match (epoch, hash) {
            (_, Some(hash)) => self.get_epoch_by_hash(hash).await?,
            (Some(epoch), _) => self.get_epoch_by_number(epoch).await?,
            (None, None) => return Err(PrimaryNetworkError::InvalidEpochRequest),
        };

        Ok(PrimaryResponse::EpochRecord { record, certificate })
    }

    /// Retrieve the consensus header by number.
    fn get_header_by_number(&self, number: u64) -> PrimaryNetworkResult<ConsensusHeader> {
        match self.consensus_config.node_storage().get_consensus_by_number(number) {
            Some(header) => Ok(header),
            None => Err(PrimaryNetworkError::UnknownConsensusHeaderNumber(number)),
        }
    }

    /// Retrieve the consensus header by hash
    fn get_header_by_hash(&self, hash: BlockHash) -> PrimaryNetworkResult<ConsensusHeader> {
        match self.consensus_config.node_storage().get_consensus_by_hash(hash) {
            Some(header) => Ok(header),
            None => Err(PrimaryNetworkError::UnknownConsensusHeaderDigest(hash)),
        }
    }

    /// Retrieve the last record in consensus blocks table.
    fn get_latest_output(&self) -> PrimaryNetworkResult<ConsensusHeader> {
        self.consensus_config
            .node_storage()
            .last_record::<ConsensusBlocks>()
            .map(|(_, header)| header)
            .ok_or(PrimaryNetworkError::InvalidRequest("Consensus headers unavailable".to_string()))
    }

    /// Retrieve the consensus header by number.
    async fn get_epoch_by_number(
        &self,
        epoch: Epoch,
    ) -> PrimaryNetworkResult<(EpochRecord, EpochCertificate)> {
        match self.consensus_config.node_storage().get_epoch_by_number(epoch) {
            Some((record, Some(cert))) => Ok((record, cert)),
            Some((_record, None)) => {
                // If we have the record but not the cert then wait a beat for it to show up.
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Some((record, Some(cert))) =
                        self.consensus_config.node_storage().get_epoch_by_number(epoch)
                    {
                        return Ok((record, cert));
                    }
                }
                Err(PrimaryNetworkError::UnavailableEpoch(epoch))
            }
            None => Err(PrimaryNetworkError::UnavailableEpoch(epoch)),
        }
    }

    /// Retrieve the consensus header by hash
    async fn get_epoch_by_hash(
        &self,
        hash: BlockHash,
    ) -> PrimaryNetworkResult<(EpochRecord, EpochCertificate)> {
        match self.consensus_config.node_storage().get_epoch_by_hash(hash) {
            Some((record, Some(cert))) => Ok((record, cert)),
            Some((_record, None)) => {
                // If we have the record but not the cert then wait a beat for it to show up.
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Some((record, Some(cert))) =
                        self.consensus_config.node_storage().get_epoch_by_hash(hash)
                    {
                        return Ok((record, cert));
                    }
                }
                Err(PrimaryNetworkError::UnavailableEpochDigest(hash))
            }
            None => Err(PrimaryNetworkError::UnavailableEpochDigest(hash)),
        }
    }
}
