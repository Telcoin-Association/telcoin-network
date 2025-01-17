//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use crate::{synchronizer::Synchronizer, ConsensusBus};
use fastcrypto::hash::Hash;
use parking_lot::Mutex;
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network_types::{RequestVoteRequest, RequestVoteResponse};
use tn_primary_metrics::PrimaryMetrics;
use tn_storage::{traits::Database, PayloadStore};
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    now,
    traits::ToFromBytes as _,
    validate_received_certificate_version, AuthorityIdentifier, Certificate, CertificateDigest,
    Header, NetworkPublicKey, Round, Vote,
};
use tracing::{debug, error, warn};
mod primary;
mod worker;

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
pub(super) struct WorkerReceiverHandler<DB> {
    consensus_bus: ConsensusBus,
    payload_store: PayloadStore<DB>,
}

impl<DB: Database> WorkerReceiverHandler<DB> {
    /// Create a new instance of Self.
    pub fn new(consensus_bus: ConsensusBus, payload_store: PayloadStore<DB>) -> Self {
        Self { consensus_bus, payload_store }
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
pub(super) struct PrimaryReceiverHandler<DB> {
    consensus_config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    synchronizer: Arc<Synchronizer<DB>>,
    /// Known parent digests that are being fetched from header proposers.
    /// Values are where the digests are first known from.
    /// TODO: consider limiting maximum number of digests from one authority, allow timeout
    /// and retries from other authorities.
    parent_digests: Arc<Mutex<BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>>>,
    metrics: Arc<PrimaryMetrics>,
}

#[allow(clippy::result_large_err)]
impl<DB: Database> PrimaryReceiverHandler<DB> {
    /// Create a new instance of Self.
    pub fn new(
        consensus_config: ConsensusConfig<DB>,
        synchronizer: Arc<Synchronizer<DB>>,
        consensus_bus: ConsensusBus,
        parent_digests: Arc<Mutex<BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>>>,
    ) -> Self {
        let metrics = consensus_bus.primary_metrics().node_metrics.clone();
        Self { consensus_config, consensus_bus, synchronizer, parent_digests, metrics }
    }

    fn find_next_round(
        &self,
        origin: AuthorityIdentifier,
        current_round: Round,
        skip_rounds: &BTreeSet<Round>,
    ) -> Result<Option<Round>, anemo::rpc::Status> {
        let mut current_round = current_round;
        while let Some(round) = self
            .consensus_config
            .node_storage()
            .certificate_store
            .next_round_number(origin, current_round)
            .map_err(|e| anemo::rpc::Status::unknown(format!("unknown error: {e}")))?
        {
            if !skip_rounds.contains(&round) {
                return Ok(Some(round));
            }
            current_round = round;
        }
        Ok(None)
    }

    #[allow(clippy::mutable_key_type)]
    async fn process_request_vote(
        &self,
        request: anemo::Request<RequestVoteRequest>,
    ) -> DagResult<RequestVoteResponse> {
        let header = &request.body().header;
        let committee = self.consensus_config.committee().clone();
        header.validate(&committee, self.consensus_config.worker_cache())?;

        let num_parents = request.body().parents.len();
        ensure!(
            num_parents <= committee.size(),
            DagError::TooManyParents(num_parents, committee.size())
        );
        self.metrics.certificates_in_votes.inc_by(num_parents as u64);

        // Vote request must come from the Header's author.
        let peer_id = request
            .peer_id()
            .ok_or_else(|| DagError::NetworkError("Unable to access remote peer ID".to_owned()))?;
        let peer_network_key = NetworkPublicKey::from_bytes(&peer_id.0).map_err(|e| {
            DagError::NetworkError(format!(
                "Unable to interpret remote peer ID {peer_id:?} as a NetworkPublicKey: {e:?}"
            ))
        })?;
        let peer_authority =
            committee.authority_by_network_key(&peer_network_key).ok_or_else(|| {
                DagError::NetworkError(format!(
                    "Unable to find authority with network key {peer_network_key:?}"
                ))
            })?;
        ensure!(
            header.author() == peer_authority.id(),
            DagError::NetworkError(format!(
                "Header author {:?} must match requesting peer {peer_authority:?}",
                header.author()
            ))
        );
        let mut rx_recent_blocks = self.consensus_bus.recent_blocks().subscribe();
        let mut latest_block_num =
            self.consensus_bus.recent_blocks().borrow().latest_block_num_hash();
        // Make sure we are within a few blocks of this headers block number.
        // Using half the GC window should roughly cover the blocks from the current window.
        let allowed_previous_blocks = self.consensus_config.config().parameters.gc_depth as u64 / 2;
        ensure!(
            header.latest_execution_block_num <= latest_block_num.number + allowed_previous_blocks,
            DagError::NetworkError(format!(
                "Header latest execution block {}/{} is too far ahead of us, we are at block {}",
                header.latest_execution_block_num,
                header.latest_execution_block,
                latest_block_num.number
            ))
        );
        // If we are a little behind then wait to catch up...
        while header.latest_execution_block_num > latest_block_num.number {
            rx_recent_blocks.changed().await.map_err(|e| {
                DagError::ClosedChannel(format!("recent blocks changed failed: {e}"))
            })?;
            latest_block_num = self.consensus_bus.recent_blocks().borrow().latest_block_num_hash();
        }
        ensure!(
            self.consensus_bus
                .recent_blocks()
                .borrow()
                .contains_hash(header.latest_execution_block),
            // Note, errors are currently "flattened" but if/when we return better errors may want
            // to make this specific in case we want to take action on this (i.e. are
            // we on our own fork?).
            DagError::NetworkError(format!(
                "Header latest execution block {}/{} must be a block we have executed recently",
                header.latest_execution_block_num, header.latest_execution_block
            ))
        );

        debug!("Processing vote request for {:?} round:{:?}", header, header.round());

        // Request missing parent certificates from the header proposer, to reduce voting latency
        // when some certificates are not broadcasted to many primaries.
        // This is only a latency optimization, and not required for liveness.
        let parents = request.body().parents.clone();
        if parents.is_empty() {
            // If any parent is still unknown, ask the header proposer to include them with another
            // vote request.
            let unknown_digests = self.get_unknown_parent_digests(header).await?;
            if !unknown_digests.is_empty() {
                debug!(
                    "Received vote request for {:?} with unknown parents {:?}",
                    header, unknown_digests
                );
                return Ok(RequestVoteResponse { vote: None, missing: unknown_digests });
            }
        } else {
            let mut validated_received_parents = vec![];
            for parent in parents {
                validated_received_parents.push(
                    validate_received_certificate_version(parent).map_err(|err| {
                        error!("request vote parents processing error: {err}");
                        DagError::InvalidCertificateVersion
                    })?,
                );
            }
            // If requester has provided parent certificates, try to accept them.
            // It is ok to not check for additional unknown digests, because certificates can
            // become available asynchronously from broadcast or certificate fetching.
            self.try_accept_unknown_parents(header, validated_received_parents).await?;
        }

        // Ensure the header has all parents accepted. If some are missing, waits until they become
        // available from broadcast or certificate fetching. If no certificate becomes available
        // for a digest, this request will time out or get cancelled by the requestor eventually.
        // This check is necessary for correctness.
        let parents = self.synchronizer.notify_read_parent_certificates(header).await?;

        // Check the parent certificates. Ensure the parents:
        // - form a quorum
        // - are all from the previous round
        // - are from unique authorities
        let mut parent_authorities = BTreeSet::new();
        let mut stake = 0;
        for parent in parents.iter() {
            ensure!(
                parent.round() + 1 == header.round(),
                DagError::HeaderHasInvalidParentRoundNumbers(header.digest())
            );
            ensure!(
                header.created_at() >= parent.header().created_at(),
                DagError::HeaderHasInvalidParentTimestamp(header.digest())
            );
            ensure!(
                parent_authorities.insert(parent.header().author()),
                DagError::HeaderHasDuplicateParentAuthorities(header.digest())
            );
            stake += committee.stake_by_id(parent.origin());
        }
        ensure!(
            stake >= committee.quorum_threshold(),
            DagError::HeaderRequiresQuorum(header.digest())
        );

        // Synchronize all batches referenced in the header.
        self.synchronizer.sync_header_batches(header, /* max_age */ 0).await?;

        // Check that the time of the header is smaller than the current time. If not but the
        // difference is small, just wait. Otherwise reject with an error.
        const TOLERANCE_SEC: u64 = 1;
        let current_time = now();
        if current_time < *header.created_at() {
            if *header.created_at() - current_time <= TOLERANCE_SEC {
                // for a small difference we simply wait
                tokio::time::sleep(Duration::from_secs(*header.created_at() - current_time)).await;
            } else {
                // For larger differences return an error, and log it
                warn!(
                    "Rejected header {:?} due to timestamp {} newer than {current_time}",
                    header,
                    *header.created_at()
                );
                return Err(DagError::InvalidTimestamp {
                    created_time: *header.created_at(),
                    local_time: current_time,
                });
            }
        }

        // Check if we can vote for this header.
        // Send the vote when:
        // 1. when there is no existing vote for this publicKey & epoch/round
        // 2. when there is a vote for this publicKey & epoch/round, and the vote is the same
        // Taking the inverse of these two, the only time we don't want to vote is when:
        // there is a digest for the publicKey & epoch/round, and it does not match the digest
        // of the vote we create for this header.
        // Also when the header is older than one we've already voted for, it is useless to vote,
        // so we don't.
        let result = self
            .consensus_config
            .node_storage()
            .vote_digest_store
            .read(&header.author())
            .map_err(DagError::StoreError)?;

        if let Some(vote_info) = result {
            ensure!(
                header.epoch() == vote_info.epoch(),
                DagError::InvalidEpoch { expected: header.epoch(), received: vote_info.epoch() }
            );
            ensure!(
                header.round() >= vote_info.round(),
                DagError::AlreadyVotedNewerHeader(
                    header.digest(),
                    header.round(),
                    vote_info.round(),
                )
            );
            if header.round() == vote_info.round() {
                // Make sure we don't vote twice for the same authority in the same epoch/round.
                let vote = Vote::new(
                    header,
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
                    self.metrics.votes_dropped_equivocation_protection.inc();
                    return Err(DagError::AlreadyVoted(
                        vote_info.vote_digest(),
                        header.digest(),
                        header.round(),
                    ));
                }
                debug!("Resending vote {vote:?} for {} at round {}", header, header.round());
                return Ok(RequestVoteResponse { vote: Some(vote), missing: Vec::new() });
            }
        }

        // Make a vote and send it to the header's creator.
        let vote = Vote::new(
            header,
            &self.consensus_config.authority().id(),
            self.consensus_config.key_config(),
        )
        .await;
        debug!("Created vote {vote:?} for {} at round {}", header, header.round());

        // Update the vote digest store with the vote we just sent.
        self.consensus_config.node_storage().vote_digest_store.write(&vote)?;

        Ok(RequestVoteResponse { vote: Some(vote), missing: Vec::new() })
    }

    // Tries to accept certificates if they have been requested from the header author.
    // The filtering is to avoid overload from unrequested certificates. It is ok that this
    // filter may result in a certificate never arriving via header proposals, because
    // liveness is guaranteed by certificate fetching.
    async fn try_accept_unknown_parents(
        &self,
        header: &Header,
        mut parents: Vec<Certificate>,
    ) -> DagResult<()> {
        {
            let parent_digests = self.parent_digests.lock();
            parents.retain(|cert| {
                let Some(from) = parent_digests.get(&(cert.round(), cert.digest())) else {
                    return false;
                };
                // Only process a certificate from the primary where it is first known.
                *from == header.author()
            });
        }
        for parent in parents {
            self.synchronizer.try_accept_certificate(parent).await?;
        }
        Ok(())
    }

    /// Gets parent certificate digests not known before.
    /// Digests that are in storage, suspended, or being requested from other proposers
    /// are considered to be known.
    async fn get_unknown_parent_digests(
        &self,
        header: &Header,
    ) -> DagResult<Vec<CertificateDigest>> {
        // Get digests not known by the synchronizer, in storage or among suspended certificates.
        let mut digests = self.synchronizer.get_unknown_parent_digests(header).await?;

        // Maximum header age is chosen to strike a balance between allowing for slightly older
        // certificates to still have a chance to be included in the DAG while not wasting
        // resources on very old vote requests. This value affects performance but not correctness
        // of the algorithm.
        const HEADER_AGE_LIMIT: Round = 3;

        // Lock to ensure consistency between limit_round and where parent_digests are gc'ed.
        let mut parent_digests = self.parent_digests.lock();

        // Check that the header is not too old.
        let narwhal_round = *self.consensus_bus.narwhal_round_updates().borrow();
        let limit_round = narwhal_round.saturating_sub(HEADER_AGE_LIMIT);
        ensure!(
            limit_round <= header.round(),
            DagError::TooOld(header.digest().into(), header.round(), narwhal_round)
        );

        // Drop old entries from parent_digests.
        while let Some(((round, _digest), _authority)) = parent_digests.first_key_value() {
            // Minimum header round is limit_round, so minimum parent round is limit_round - 1.
            if *round < limit_round.saturating_sub(1) {
                parent_digests.pop_first();
            } else {
                break;
            }
        }

        // Filter out digests that are already requested from other header proposers.
        digests.retain(|digest| match parent_digests.entry((header.round() - 1, *digest)) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(header.author());
                true
            }
        });

        Ok(digests)
    }
}
