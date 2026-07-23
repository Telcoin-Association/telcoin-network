//! Handle specific request types received from the network.

use super::PrimaryResponse;
use crate::{
    error::{CertManagerError, PrimaryNetworkError, PrimaryNetworkResult},
    network::message::PrimaryGossip,
    state_sync::{CertificateCollector, StateSynchronizer},
    ConsensusBusApp, NodeMode,
};
use futures::AsyncWriteExt as _;
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    write_frame, GossipMessage, PrimarySyncRequest, Stream, SyncFrame, SyncFrameError,
};
use tn_storage::{consensus::ConsensusChain, tables::Votes, CertificateStore, VoteDigestStore};
use tn_types::{
    ensure,
    error::{CertificateError, HeaderError, HeaderResult},
    now, to_intent_message, try_decode, AuthorityIdentifier, BlsPublicKey, Certificate,
    ConsensusResult, ConsensusResultDigest, Database, Epoch, EpochCertificate, EpochDigest,
    EpochRecord, Hash as _, Header, HeaderDigest, ProtocolSignature, Round,
    SignatureVerificationState, TnSender as _, Vote,
};
use tokio::{sync::Mutex as TokioMutex, time::timeout};
use tracing::{debug, error, info, warn};

/// Total timeout for sending a 16kb buffer of pack file data.
/// Prevents slow-reader attacks where a peer accepts a stream but never reads.
/// Set to an arbitrary 10 seconds to read 16kb buffer.
const SEND_STREAM_BUFFER_TIMEOUT: Duration = Duration::from_secs(10);

/// Total timeout for serving an entire epoch pack over the sync protocol.
///
/// A backstop above the per-frame [`SEND_STREAM_BUFFER_TIMEOUT`]: a peer that
/// drip-reads just fast enough to keep resetting the per-frame timer cannot pin
/// the responder task (and the admission slot it holds) indefinitely.
const SEND_SYNC_PACK_TIMEOUT: Duration = Duration::from_secs(200);

/// Total timeout for serving a missing-certificates response over the sync protocol.
///
/// The streaming collector is already bounded by its DB-read time limit, but the
/// number of frames and a slow reader are not, so this backstops the whole serve
/// (and the admission slot it holds) the same way [`SEND_SYNC_PACK_TIMEOUT`] does
/// for an epoch pack.
const SEND_SYNC_CERTS_TIMEOUT: Duration = Duration::from_secs(200);

/// Map to hold vote info to detect invalid votes, equivocation and cache responses in case of
/// rerequests.
type AuthEquivocationMap = HashMap<
    AuthorityIdentifier,
    TokioMutex<Option<(Epoch, Round, HeaderDigest, Option<PrimaryResponse>)>>,
>;

/// A pending consensus-result signature tally.
///
/// See the `consensus_certs` field, GHSA-2r5c-c4h7-gp5h, and GHSA-pvhw-9pmg-q2hg.
#[derive(Default, Debug)]
struct ConsensusCertTally {
    /// Monotonic recency sequence, bumped every time a new distinct signer is added. Used for
    /// least-recently-updated (LRU) eviction.
    seq: u64,
    /// The consensus number this tally is for. Used to rate-limit equivocation per
    /// `(signer, number)`: an honest validator signs exactly one result per consensus number.
    number: u64,
    /// The committee members that have signed this result.
    signers: HashSet<BlsPublicKey>,
}

/// Dedup key for an epoch vote at ingress: `(author, epoch, epoch-record hash)`. See the
/// `epoch_votes_seen` field and issue #898.
type EpochVoteKey = (BlsPublicKey, Epoch, EpochDigest);

/// The type that handles requests from peers.
#[derive(Clone, Debug)]
pub(crate) struct RequestHandler<DB> {
    /// Consensus config with access to database.
    consensus_config: ConsensusConfig<DB>,
    /// Inner-processs channel bus.
    consensus_bus: ConsensusBusApp,
    /// Synchronize state between peers.
    state_sync: StateSynchronizer<DB>,
    /// The digests of parents that are currently being requested from peers.
    ///
    /// Missing parents are requested from peers. This is a local map to track in-flight requests
    /// for missing parents. The values are associated with the first authority that proposed a
    /// header with these parents. The node keeps track of requested Certificates to prevent
    /// unsolicited certificate attacks.
    requested_parents: Arc<Mutex<BTreeMap<(Round, HeaderDigest), AuthorityIdentifier>>>,
    /// Map of the last epoch and round each authority requested a vote for.
    /// Used to stop validator equivocation early.
    auth_last_vote: Arc<AuthEquivocationMap>,
    /// Track consensus headers until we hit a simple quorum then send on.
    consensus_certs: Arc<Mutex<HashMap<ConsensusResultDigest, ConsensusCertTally>>>,
    /// Deduplicate epoch votes at ingress, keyed on `(author, epoch, epoch-record hash)`.
    ///
    /// The value is a monotonic recency sequence used to evict the least-recently-seen entry
    /// once the map reaches [`MAX_EPOCH_VOTES`](super::MAX_EPOCH_VOTES), bounding memory
    /// against a vote flood. A key is inserted only after its signature verifies, so this gate
    /// skips repeated verifies of an already-accepted vote without letting a bad-signature vote
    /// poison the slot of the real author. The `epoch` field is part of the key even though it is
    /// not covered by the vote signature: this ensures a replay that tampers with `vote.epoch`
    /// lands on a different key and cannot evict/pre-empt the real vote's slot. See the
    /// `PrimaryGossip::EpochVote` handler and issue #898.
    epoch_votes_seen: Arc<Mutex<HashMap<EpochVoteKey, u64>>>,
    /// Access to the consensus chain data.
    consensus_chain: ConsensusChain,
}

impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub(crate) fn new(
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBusApp,
        state_sync: StateSynchronizer<DB>,
        consensus_chain: ConsensusChain,
    ) -> Self {
        let mut auth_last_vote: AuthEquivocationMap = Default::default();
        // Preload with each committee auth id.
        // This is so we can lock each authority individually when processing votes.
        for auth in consensus_config.committee().authorities() {
            auth_last_vote.insert(auth.id(), TokioMutex::new(None));
        }
        Self {
            consensus_config,
            consensus_bus,
            state_sync,
            requested_parents: Default::default(),
            auth_last_vote: Arc::new(auth_last_vote),
            consensus_certs: Default::default(),
            epoch_votes_seen: Default::default(),
            consensus_chain,
        }
    }

    /// Expose the handlers consensus chain- useful for testing.
    #[cfg(test)]
    pub(super) fn consensus_chain(&self) -> &ConsensusChain {
        &self.consensus_chain
    }

    /// Detect if we are too far behind the given epoch, round and switch to CVV inactive if we are
    /// active. This will put us in a "catch up" mode until we have caught up enough to rejoin
    /// consensus.
    pub(crate) async fn behind_consensus(
        &self,
        epoch: Epoch,
        round: Round,
        number: Option<u64>,
    ) -> bool {
        // Need to be an active cvv to have fallen behind.
        if !self.consensus_bus.is_active_cvv() {
            return false;
        }
        // Last consensus block we have executed, use this to determine if we are
        // too far behind.
        let (exec_number, exec_epoch, exec_round) = self
            .consensus_bus
            .last_consensus_block(&self.consensus_chain)
            .await
            .map(|h| (h.number, h.sub_dag.leader_epoch(), h.sub_dag.leader_round()))
            .unwrap_or((0, 0, 0));
        // When empty outputs are skipped by execution, no new EVM block is produced.
        // In that case, rely on the most recent consensus round processed by the engine.
        let processed_consensus_round = self.consensus_bus.last_consensus_round();
        // The committed round is updated synchronously by the consensus core (Bullshark)
        // when it commits certificates. This happens before the async engine pipeline processes
        // them. Including it prevents false-positive "behind" detection when the engine
        // lags behind a burst of empty consensus rounds.
        let committed_round = self.consensus_bus.committed_round();
        let effective_exec_round = exec_round.max(processed_consensus_round).max(committed_round);
        let (_last_consensus_epoch, last_consensus_number, _) =
            self.consensus_bus.published_consensus_num_hash();
        // Use GC depth to estimate how many rounds we can be behind.
        // Subtract `GC_ACTIVITY_BUFFER` here so if we are right on the GC depth we will still go
        // inactive (small safety buffer).  The buffer is arbitrary but should make sure we are
        // comfortably within the current DAG. Trying to ride the GC window exactly can lead to
        // subtle races (allow some time to get going).  On a production node
        // `Parameters::validate_operational_floors` keeps the configured `gc_depth` above this
        // buffer, so the window is positive.  `saturating_sub` still guards the test-fixture path,
        // where that floor is intentionally not enforced and a small `gc_depth` can zero the
        // window.
        let gc_depth = self
            .consensus_config
            .parameters()
            .gc_depth
            .saturating_sub(tn_types::GC_ACTIVITY_BUFFER);
        // is our round outside the GC window
        // Will be false when not the same epoch (can't compare rounds) but
        // epoch_behind will work in that case.
        let outside_gc_window = epoch == exec_epoch && (effective_exec_round + gc_depth) < round;
        // are we on older epoch?
        // note, we need to make sure we are not at the epoch boundary otherwise
        // we can get false positives
        let epoch_behind = if let Some(number) = number {
            // Throw this max() in so we can avoid races when execution is behind consensus at an
            // epoch boundary.
            epoch > exec_epoch && exec_number.max(last_consensus_number) + 1 < number
        } else if exec_epoch + 1 == epoch {
            // This check is a little hand-wavy, basically if we don't have the number (i.e.
            // checking a cert) then we let the next epoch early rounds through. Having the
            // number is better.  Note we will seen 1/3 + 1 (min) certs to here.
            // Also, these checks for certs are probably not 100% needed anyway...
            round > gc_depth.max(6)
        } else {
            epoch > exec_epoch
        };
        if outside_gc_window || epoch_behind {
            // We seem to be too far behind to be an active CVV, try to go
            // inactive to catch up.
            warn!(
                target: "primary",
                epoch,
                exec_epoch,
                ?number,
                exec_number,
                exec_round,
                processed_consensus_round,
                committed_round,
                effective_exec_round,
                "we are behind, go to catchup mode!"
            );
            self.consensus_bus.node_mode().send_replace(NodeMode::CvvInactive);
            self.consensus_config.shutdown().notify();
            true
        } else {
            false
        }
    }

    async fn get_committee(&self, epoch: Epoch) -> Option<BTreeSet<BlsPublicKey>> {
        if epoch == self.consensus_config.committee().epoch() {
            Some(self.consensus_config.committee().bls_keys())
        } else if epoch == self.consensus_config.committee().epoch() + 1 {
            Some(self.consensus_config.next_committee_keys().iter().copied().collect())
        } else {
            self.consensus_chain.epochs().get_committee_keys(epoch).await
        }
    }

    /// True if a vote for this `(author, epoch, epoch-record)` has already been verified and
    /// forwarded.
    ///
    /// Cheap read used as a pre-verify gate: an accepted vote is re-gossiped by its author on a
    /// timer and duplicated by the gossip mesh, so this caps repeated BLS verifies of the same
    /// valid vote. See [`RequestHandler::record_epoch_vote`] and issue #898.
    fn epoch_vote_seen(&self, author: BlsPublicKey, epoch: Epoch, epoch_hash: EpochDigest) -> bool {
        self.epoch_votes_seen.lock().contains_key(&(author, epoch, epoch_hash))
    }

    /// Record a verified `(author, epoch, epoch-record)` vote so later replays are dropped before
    /// the signature check.
    ///
    /// Callers MUST invoke this only after [`EpochVote::check_signature`] succeeds: recording a
    /// bad-signature vote would let a Byzantine committee publisher poison the slot of the real
    /// author and suppress their vote. Eviction is least-recently-seen once the map reaches
    /// [`MAX_EPOCH_VOTES`](super::MAX_EPOCH_VOTES), so a flood of distinct keys cannot grow it
    /// without bound; a false negative after eviction only costs one extra verify (the collector
    /// dedups per signer downstream), never a dropped honest vote.
    fn record_epoch_vote(&self, author: BlsPublicKey, epoch: Epoch, epoch_hash: EpochDigest) {
        let key = (author, epoch, epoch_hash);
        let mut guard = self.epoch_votes_seen.lock();
        let next_seq = guard.values().copied().max().unwrap_or(0) + 1;
        if guard.len() >= super::MAX_EPOCH_VOTES && !guard.contains_key(&key) {
            let evict = guard.iter().min_by_key(|entry| *entry.1).map(|entry| *entry.0);
            if let Some(evict) = evict {
                guard.remove(&evict);
            }
        }
        guard.insert(key, next_seq);
    }

    /// Process gossip from the committee.
    ///
    /// Peers gossip the HeaderDigest so peers can request the Certificate. This waits until
    /// the certificate can be retrieved and timesout after some time. It's important to give up
    /// after enough time to limit the DoS attack surface. Peers who timeout must lose reputation.
    pub(super) async fn process_gossip(&self, msg: &GossipMessage) -> PrimaryNetworkResult<()> {
        // deconstruct message
        let GossipMessage { data, topic, .. } = msg;

        // gossip is uncompressed
        let gossip = try_decode(data)?;

        match gossip {
            PrimaryGossip::Certificate(mut cert) => {
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::primary_topic(
                        self.consensus_config.chain_id()
                    )),
                    PrimaryNetworkError::InvalidTopic
                );
                // process certificate
                cert.validate_received().map_err(CertManagerError::from)?;

                let epoch = cert.header().epoch();
                // Early verify so we can detect we are behind.
                // The verification is cached in the cert so this should not be too expensive.
                if let Some(committee) = self.get_committee(epoch).await {
                    match cert.verify_cert(&committee) {
                        Ok(()) => {
                            if self.consensus_bus.is_cvv() {
                                if self.behind_consensus(epoch, cert.header().round(), None).await {
                                    warn!(target: "primary", "certificate indicates we are behind, go to catchup mode!");
                                    return Ok(());
                                }
                                self.state_sync.process_peer_certificate(&mut cert).await?;
                            }
                            if self.consensus_bus.is_cvv_inactive()
                                && self.consensus_config.committee().epoch() == cert.epoch()
                            {
                                // If we are catching up and this is for our current epoch save in
                                // cache so we will be able
                                // to rejoin consensus later when caught up.
                                let _ = self.consensus_config.node_storage().write((*cert).clone());
                            }
                            // ExEx delivery runs on consensus-following nodes
                            // (Observer + inactive CVV), never on the active
                            // validator hot path. The certificate verified against
                            // its committee just above, so emit it as the earliest
                            // lifecycle signal for any installed ExEx.
                            if !self.consensus_bus.is_active_cvv() {
                                self.consensus_bus.notify_exex_certificate(&cert);
                            }
                        }
                        Err(e) => warn!(target: "primary", "Recieved invalid cert {e}"),
                    }
                } else {
                    // If we can't find this cert's committee then it's bogus or we are
                    // catching up. Ignore for now otherwise if we go inactive that could open
                    // an attack surface since we can not verify the cert without the
                    // committee.
                    warn!(target: "primary", "failed to get committee for epoch {epoch}, ignoring certificate!", );
                }
            }
            PrimaryGossip::Consensus(result) => {
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::consensus_output_topic(
                        self.consensus_config.chain_id()
                    )),
                    PrimaryNetworkError::InvalidTopic
                );
                // We want to confirm all the data (including but not limited to the consensus
                // header hash) against the signature.
                // Used to track signature counts as well to avoid any mismatched data errors.
                let consensus_result_hash = result.digest();
                let ConsensusResult { epoch, round, number, hash, validator: key, signature } =
                    *result;
                let (_old_epoch, old_number, old_hash) =
                    self.consensus_bus.published_consensus_num_hash();
                if hash == old_hash || old_number >= number {
                    // We have already dealt with this hash or we are past this output.
                    return Ok(());
                }
                if let Some(committee) = self.get_committee(epoch).await {
                    // If we do not have the committee to verify this message then just ignore for
                    // now. Another one will be along soon and we should be
                    // syncing epochs in the background.
                    ensure!(
                        committee.contains(&key),
                        PrimaryNetworkError::PeerNotInCommittee(Box::new(key))
                    );
                    let sigs;
                    let enough_sigs;
                    {
                        // Trying to walk the line on keeping this lock non-async, held as short as
                        // possible and don't want to do the cert verify if we can
                        // skip it.  So this seems like the place.  And of course
                        // close any windows to double counting a signature.
                        let guard = self.consensus_certs.lock();
                        if guard
                            .get(&consensus_result_hash)
                            .and_then(|tally| tally.signers.get(&key))
                            .is_some()
                        {
                            // We have already counted this signature so ignore.
                            return Ok(());
                        }
                        drop(guard);
                        // Drop the lock for the expensive verify op. If a dup gets through the Set
                        // will still only count it once.
                        ensure!(
                            signature
                                .verify_secure(&to_intent_message(consensus_result_hash), &key),
                            PrimaryNetworkError::UnknownConsensusHeaderCert(hash)
                        );
                        // Once we have seen 1/3 + 1 committe members have signed this it should be
                        // valid.
                        enough_sigs = (committee.len() / 3) + 1;
                        // The tally map is hard-capped so a flood of validly-signed but non-quorum
                        // results cannot grow it without bound. The cap SCALES with committee size
                        // (with `MAX_CONSENSUS_CERTS` as a floor): the number of Byzantine members
                        // `f` grows with `n`, and the per-number equivocation limit below lets
                        // those `f` members occupy at most `f *
                        // MAX_TALLIES_PER_SIGNER_PER_NUMBER` tallies
                        // for any one consensus number, which stays below `max_certs` for every
                        // committee size. A fixed cap would be exceeded once `f` passed
                        // `cap / MAX_TALLIES_PER_SIGNER_PER_NUMBER`, reactivating eviction and
                        // reopening the GHSA-pvhw liveness residual.
                        let max_certs = committee.len().max(super::MAX_CONSENSUS_CERTS);
                        let mut guard = self.consensus_certs.lock();
                        // Per-publisher equivocation limit (GHSA-pvhw-9pmg-q2hg). The cap above
                        // bounds the map's memory; it does not protect liveness. A Byzantine member
                        // can sign many distinct hashes for the SAME consensus number, and a flood
                        // of those fresh digests can evict an honest tally still climbing to
                        // quorum; honest validators gossip each result only
                        // once, so an evicted honest signature is lost for
                        // good and that result can never reach quorum.
                        //
                        // An honest validator signs exactly ONE result (one hash) per consensus
                        // number. A signer already present in `MAX_TALLIES_PER_SIGNER_PER_NUMBER`
                        // distinct live tallies for THIS number is equivocating; drop its further
                        // fresh digests for the number. The limit is per `(signer, number)`, NOT
                        // per signer, so an honest validator that is the
                        // first to gossip several consecutive un-quorumed
                        // numbers is never throttled. Joining an existing
                        // tally is always allowed (it never grows the map).
                        //
                        // Residual (documented follow-up): a single member fabricating one hash
                        // each for many distinct FUTURE numbers is not
                        // bounded here and is indistinguishable from an
                        // honest validator racing ahead. See
                        // GHSA-pvhw-9pmg-q2hg.
                        if !guard.contains_key(&consensus_result_hash) {
                            let signer_tallies_for_number = guard
                                .values()
                                .filter(|tally| {
                                    tally.number == number && tally.signers.contains(&key)
                                })
                                .count();
                            if signer_tallies_for_number >= super::MAX_TALLIES_PER_SIGNER_PER_NUMBER
                            {
                                return Ok(());
                            }
                        }
                        // Eviction is by least-recently-updated (LRU), NOT by signer count. A real
                        // consensus result is signed once by each honest validator in a burst as
                        // the network commits it, so its tally is bumped to most-recently-used on
                        // every distinct signer and is never the eviction victim while it climbs to
                        // quorum. Evicting by fewest-signers instead would be steerable. See
                        // GHSA-2r5c-c4h7-gp5h.
                        let next_seq = guard.values().map(|tally| tally.seq).max().unwrap_or(0) + 1;
                        if guard.len() >= max_certs && !guard.contains_key(&consensus_result_hash) {
                            let evict = guard
                                .iter()
                                .min_by_key(|(_, tally)| tally.seq)
                                .map(|(digest, _)| *digest);
                            if let Some(evict) = evict {
                                guard.remove(&evict);
                            }
                        }
                        let tally = guard.entry(consensus_result_hash).or_default();
                        tally.seq = next_seq;
                        tally.number = number;
                        tally.signers.insert(key);
                        sigs = tally.signers.len();
                    }
                    if sigs >= enough_sigs {
                        if self.behind_consensus(epoch, round, Some(number)).await {
                            warn!(target: "primary", "consensus result indicates we are behind, go to catchup mode!");
                            self.consensus_certs.lock().clear();
                            return Ok(());
                        }

                        // Make sure we don't get old gossip and go backwards.
                        // number has to be greater than old_number due to an early check so
                        // this is safe Only send this when we are
                        // sure it is valid. Receivers will count on
                        // this being verified.
                        info!(target: "primary", "got new consensus {number}/{hash}");
                        self.consensus_bus.publish_consensus_num_hash_if_newer(epoch, number, hash);
                        self.consensus_certs.lock().clear();
                    }
                } else {
                    // Not sure we can sanity check this epoch.  However if it is bogus the code
                    // to handle it should be fine, it stops when out of epochs.
                    self.consensus_bus.set_request_missing_epoch_if_newer(epoch);
                }
            }
            PrimaryGossip::EpochVote(vote) => {
                // Uniform gate ordering (issue #898): run the cheap, attacker-independent checks
                // before the expensive BLS verify so unauthorized or duplicate votes are dropped
                // without consuming verification resources.

                // 1. Topic must match.
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::epoch_vote_topic(
                        self.consensus_config.chain_id()
                    )),
                    PrimaryNetworkError::InvalidTopic
                );
                // 2. Committee membership BEFORE crypto. An epoch vote is signed by a member of the
                //    committee for `vote.epoch` (see `EpochVote` and `manage_epoch_votes`), so a
                //    non-member is rejected without a signature check. `get_committee` resolves the
                //    current/next committee synchronously (the live-voting window) and falls back
                //    to stored epoch records for older epochs. Membership is by epoch *number*, not
                //    `epoch_hash`, so a member's vote for a forked/alternative record is still
                //    admitted (the collector's equivocation path needs it). A non-member yields the
                //    benign, non-penalizing `PeerNotInCommittee` rather than the `Fatal`
                //    `PeerNotAuthor`, which on the gossip path is charged to the honest relayer
                //    rather than the author (GHSA-j2g4-553f-875r). Mirrors the `Consensus` arm.
                if let Some(committee) = self.get_committee(vote.epoch).await {
                    ensure!(
                        committee.contains(&vote.public_key),
                        PrimaryNetworkError::PeerNotInCommittee(Box::new(vote.public_key))
                    );
                    // 3. Drop votes already verified and forwarded for this (author, record).
                    if !self.epoch_vote_seen(vote.public_key, vote.epoch, vote.epoch_hash) {
                        // 4. Signature check LAST — the most expensive gate.
                        ensure!(
                            vote.check_signature(),
                            PrimaryNetworkError::InvalidHeader(HeaderError::PeerNotAuthor)
                        );
                        // Record only AFTER a valid signature so a bad-signature vote cannot
                        // poison the dedup slot for the real author.
                        self.record_epoch_vote(vote.public_key, vote.epoch, vote.epoch_hash);
                        // Fire-and-forget: no oneshot, no blocking.
                        let _ = self.consensus_bus.new_epoch_votes().send(*vote).await;
                    }
                } else {
                    // Committee for this epoch is not known yet (we are behind, or the epoch is
                    // bogus). Drop without penalty and without writing request state from an
                    // unverified, attacker-controlled epoch number; the author re-gossips its
                    // vote and the epoch is learned via consensus / epoch-record sync.
                    debug!(
                        target: "primary",
                        epoch = vote.epoch,
                        author = ?vote.public_key,
                        "dropping epoch vote for unknown committee epoch"
                    );
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
        // Sanity check the peer is the author and bounce quick if not.
        // This should keep a malicious validator from corrupting another
        // nodes vote cache.
        // This relies on libp2p to manage peer ids that are used to get the bls key.
        let committee_peer = header.author().clone();
        let auth_id: AuthorityIdentifier = peer.into();
        if let Some(auth) = self.consensus_config.committee().authority(&committee_peer) {
            // We err on the side of caution here, if auths peer id is not known fail but we
            // should know it (got a vote request from them).
            ensure!(auth_id == auth.id(), HeaderError::PeerNotAuthor.into());
        } else {
            return Err(HeaderError::UnknownAuthority(committee_peer.to_string()).into());
        }
        if let Ok(Some(vote_info)) = self.consensus_config.node_storage().read_vote_info(&auth_id) {
            // If we have already cast a vote for this header then just recast it quickly.
            if vote_info.vote_digest == header.digest().into() {
                let vote = Vote::new(
                    &header,
                    self.consensus_config.authority_id().ok_or(HeaderError::NotCommitteeMember)?,
                    self.consensus_config.key_config(),
                );

                info!(target: "primary", "Recast vote {vote:?} for {} at round {}", header, header.round());
                return Ok(PrimaryResponse::Vote(vote));
            }
        }
        // We have already verified committee membership with the peer check but this will double
        // check. Each committee member has it's own lock so different authorities can be
        // voted on in parallel but only one vote for an authority can proceed at a time.
        if let Some(auth_lock) = self.auth_last_vote.get(header.author()) {
            // Check for validator equivocation early and reject if so
            let mut auth_last_vote = auth_lock.lock().await;
            if let Some((last_epoch, last_round, last_digest, last_response)) =
                auth_last_vote.take()
            {
                if last_digest == header.digest() {
                    match last_response {
                        None | Some(PrimaryResponse::RecoverableError(_)) => {}
                        Some(PrimaryResponse::MissingParents(missing)) => {
                            if parents.is_empty() {
                                // Proposer retried with a fresh request (no parents provided).
                                // Re-issue the missing parents request so the proposer knows
                                // what to provide on the next attempt. This avoids a deadlock
                                // where the proposer's certifier restarts (losing the missing
                                // parent hint) and the cached state here causes a fatal
                                // WrongNumberOfParents error on every subsequent attempt.
                                *auth_last_vote = Some((
                                    last_epoch,
                                    last_round,
                                    last_digest,
                                    Some(PrimaryResponse::MissingParents(missing.clone())),
                                ));
                                return Ok(PrimaryResponse::MissingParents(missing));
                            }
                            // A proper response to missing parents will include exactly the missing
                            // parents.
                            if parents.len() == missing.len() {
                                for parent in parents.iter().map(|p| p.digest()) {
                                    if !missing.contains(&parent) {
                                        *auth_last_vote = Some((
                                            last_epoch,
                                            last_round,
                                            last_digest,
                                            Some(PrimaryResponse::MissingParents(missing)),
                                        ));
                                        return Err(HeaderError::InvalidParents.into());
                                    }
                                }
                            } else {
                                let missing_len = missing.len();
                                *auth_last_vote = Some((
                                    last_epoch,
                                    last_round,
                                    last_digest,
                                    Some(PrimaryResponse::MissingParents(missing)),
                                ));
                                return Err(HeaderError::WrongNumberOfParents(
                                    missing_len,
                                    parents.len(),
                                )
                                .into());
                            }
                        }
                        Some(res) => return Ok(res),
                    }
                } else if header.epoch() < last_epoch
                    || (last_epoch == header.epoch() && last_round >= header.round())
                {
                    *auth_last_vote = Some((last_epoch, last_round, last_digest, None));
                    return Err(HeaderError::AlreadyVotedForLaterRound {
                        theirs: header.round(),
                        ours: last_round,
                    }
                    .into());
                }
            }
            let epoch = header.epoch();
            let round = header.round();
            let digest = header.digest();
            // Use a timeout for a sanity check.  Should be able to process a vote request within
            // header delay...
            let res = tokio::time::timeout(
                self.consensus_config.config().parameters.max_header_delay,
                self.vote_inner(header, parents),
            )
            .await?;
            // Do this to cache the "full" response.
            // If pulling from the cache it is fine to already be converted
            // but sometimes we want the full error (basically tests) so return
            // res when we have a non-converted error.
            let cached_res: PrimaryResponse = match &res {
                Ok(msg) => msg.clone(),
                Err(e) => PrimaryResponse::into_error_ref(e),
            };
            *auth_last_vote = Some((epoch, round, digest, Some(cached_res)));
            res
        } else {
            Err(HeaderError::UnknownAuthority(header.author().to_string()).into())
        }
    }

    /// Evaluate request to possibly issue a vote in support of peer's header.
    async fn vote_inner(
        &self,
        header: Header,
        parents: Vec<Certificate>,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        // current committee
        let committee = self.consensus_config.committee();

        // validate header
        header.validate(committee)?;

        // A proposed header is always at round >= 1; round 0 is reserved for genesis certificates
        // and is never voted on. Reject a round-0 header here, before parent tracking, so
        // `check_for_missing_parents` never evaluates `header.round() - 1` at round 0 (see #789).
        ensure!(header.round() != 0, HeaderError::InvalidRound(header.digest()).into());

        let max_round =
            self.consensus_bus.committed_round() + self.consensus_config.parameters().gc_depth;
        // Make sure the header is not unreasonable in the future.
        ensure!(
            header.round() <= max_round,
            HeaderError::TooNew {
                digest: header.digest(),
                header_round: header.round(),
                max_round,
            }
            .into()
        );

        // validate parents
        let num_parents = parents.len();
        ensure!(
            num_parents <= committee.size(),
            HeaderError::TooManyParents(num_parents, committee.size()).into()
        );

        // if peer is ahead, wait for execution to catch up
        // NOTE: this doesn't hurt since this node shouldn't vote until execution is caught up
        // ensure execution results match if this succeeds.
        if self.consensus_bus.wait_for_execution(header.latest_execution_block()).await.is_err() {
            error!(
                target: "primary",
                peer_hash = ?header.latest_execution_block(),
                expected = ?self.consensus_bus.latest_execution_block_num_hash(),
                "unexpected execution result received"
            );
            return Err(HeaderError::UnknownExecutionResult(header.latest_execution_block()).into());
        }
        debug!(target: "primary", ?header, round = header.round(), "Processing vote request from peer");

        // certifier optimistically sends header without parents
        // however, peers may request missing certificates from a proposer
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
            ensure!(
                parent.epoch() == header.epoch(),
                HeaderError::InvalidEpoch { theirs: parent.epoch(), ours: header.epoch() }.into()
            );
            ensure!(parent.round() + 1 == header.round(), HeaderError::InvalidParentRound.into());

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
                    parent: *parent.header().created_at()
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
                header.epoch() >= vote_info.epoch(),
                HeaderError::InvalidEpoch { theirs: header.epoch(), ours: vote_info.epoch() }
                    .into()
            );
            if header.epoch() == vote_info.epoch() {
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
                        self.consensus_config
                            .authority_id()
                            .ok_or(HeaderError::NotCommitteeMember)?,
                        self.consensus_config.key_config(),
                    );
                    if vote.digest() != vote_info.vote_digest() {
                        // We have already signed a *different* vote for this author at
                        // this (epoch, round); refuse to sign a second one.
                        //
                        // The previous behavior re-voted whenever `read_by_index` found no
                        // certificate for the earlier vote. That probe is unsound across a
                        // crash: the certificate tables are `TableHint::Epoch`, written
                        // non-durably through the layered epoch DB, so a certificate that
                        // *was* formed can be lost on restart and read back as absent,
                        // letting the node sign a distinct vote for a slot already
                        // aggregated into a live certificate. (`.unwrap_or(None)` also
                        // failed a storage error open into the same branch.) A `false` or
                        // errored read cannot prove the earlier vote was never certified,
                        // so the only safe action is to refuse. See #934, #963.
                        warn!(
                            "Authority {} submitted a different header {:?} for a round it was \
                             already voted on; refusing to re-vote to avoid equivocation",
                            header.author(),
                            header,
                        );
                        return Err(
                            HeaderError::AlreadyVoted(header.digest(), header.round()).into()
                        );
                    } else {
                        debug!(
                            "Resending vote {vote:?} for {} at round {}",
                            header,
                            header.round()
                        );
                        return Ok(PrimaryResponse::Vote(vote));
                    }
                }
            }
        }

        // this node hasn't voted yet
        let vote = Vote::new(
            &header,
            self.consensus_config.authority_id().ok_or(HeaderError::NotCommitteeMember)?,
            self.consensus_config.key_config(),
        );

        debug!(target: "primary", "Created vote {vote:?} for {} at round {}", header, header.round());

        // Update the vote digest store with the vote we just sent.
        self.consensus_config.node_storage().write_vote(&vote)?;

        // Wait for the `Votes` record to be durable before returning the vote to the peer. The
        // epoch DB persists asynchronously, so `write_vote` returns before the record hits disk;
        // returning the vote first would let a crash in that window lose the record. On restart the
        // recast guard would then read nothing and could sign a *different* vote for the same
        // author/round while the pre-crash vote is already held by the peer: equivocation from an
        // ordinary crash. See issue #934.
        self.consensus_config.node_storage().persist_durable::<Votes>().await;

        Ok(PrimaryResponse::Vote(vote))
    }

    /// Helper method to retrieve parents for header.
    ///
    /// Certificates are considered "known" if they are in local storage, pending, or already
    /// requested from a peer.
    async fn check_for_missing_parents(&self, header: &Header) -> HeaderResult<Vec<HeaderDigest>> {
        // identify parents that are neither in storage nor pending
        let mut unknown_certs = self.state_sync.identify_unkown_parents(header).await?;

        // ensure header is not too old
        let limit = self.consensus_bus.primary_round().saturating_sub(
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
            // `saturating_sub` matches the style used for `limit` above and keeps this helper
            // underflow-safe on its own; `vote_inner` already rejects round 0 (see #789).
            let key = (header.round().saturating_sub(1), *digest);
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
        for parent in parents.iter_mut() {
            self.state_sync.process_peer_certificate(parent).await?;
        }

        Ok(())
    }

    /// Retrieve the raw (serialized) consensus output bytes from local storage.
    ///
    /// Returns the full pack-file encoded output for `number` so a peer can reconstruct the
    /// [`tn_types::ConsensusOutput`] (batches + consensus header) without a separate batch fetch.
    /// The bytes are streamed to the requesting peer over the typed sync protocol (see
    /// `process_sync_consensus_output_stream`), because a single output can exceed the
    /// request/response message-size limit.
    pub(super) async fn consensus_output_bytes(
        &self,
        number: u64,
    ) -> PrimaryNetworkResult<Vec<u8>> {
        let mut my_number = self.consensus_chain.latest_consensus_number();
        // If we are behind then wait up to two seconds to catch up.
        let mut count = 0;
        if number.saturating_sub(my_number) < 4 {
            // Only wait if we are close.
            while my_number < number {
                tokio::time::sleep(Duration::from_millis(100)).await;
                my_number = self.consensus_chain.latest_consensus_number();
                if count >= 20 {
                    // Don't wait more than 2 seconds for this to show up.
                    return Err(PrimaryNetworkError::UnknownConsensusOutput(number));
                }
                count += 1;
            }
        }
        match self.consensus_chain.consensus_output_bytes_by_number(number).await {
            Ok(Some(bytes)) => Ok(bytes),
            // Missing locally, or the requested number is outside the pack's range.
            Ok(None) => Err(PrimaryNetworkError::UnknownConsensusOutput(number)),
            Err(e) => Err(PrimaryNetworkError::ConsensusChainError(e)),
        }
    }

    /// Retrieve an epoch record from local storage.
    pub(super) async fn retrieve_epoch_record(
        &self,
        epoch: Option<Epoch>,
        hash: Option<EpochDigest>,
    ) -> PrimaryNetworkResult<PrimaryResponse> {
        let (record, certificate) = match (epoch, hash) {
            (_, Some(hash)) => self.get_epoch_by_hash(hash).await?,
            (Some(epoch), _) => self.get_epoch_by_number(epoch).await?,
            (None, None) => return Err(PrimaryNetworkError::InvalidEpochRequest),
        };

        Ok(PrimaryResponse::EpochRecord { record, certificate })
    }

    /// Retrieve the consensus header by number.
    async fn get_epoch_by_number(
        &self,
        epoch: Epoch,
    ) -> PrimaryNetworkResult<(EpochRecord, EpochCertificate)> {
        match self.consensus_chain.epochs().get_epoch_by_number(epoch).await {
            Some((record, Some(cert))) => Ok((record, cert)),
            Some((_record, None)) => {
                // If we have the record but not the cert then wait a beat for it to show up.
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Some((record, Some(cert))) =
                        self.consensus_chain.epochs().get_epoch_by_number(epoch).await
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
        hash: EpochDigest,
    ) -> PrimaryNetworkResult<(EpochRecord, EpochCertificate)> {
        match self.consensus_chain.epochs().get_epoch_by_hash(hash).await {
            Some((record, Some(cert))) => Ok((record, cert)),
            Some((_record, None)) => {
                // If we have the record but not the cert then wait a beat for it to show up.
                for _ in 0..5 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Some((record, Some(cert))) =
                        self.consensus_chain.epochs().get_epoch_by_hash(hash).await
                    {
                        return Ok((record, cert));
                    }
                }
                Err(PrimaryNetworkError::UnavailableEpochDigest(hash))
            }
            None => Err(PrimaryNetworkError::UnavailableEpochDigest(hash)),
        }
    }

    /// Number of tracked consensus-result digests. Test-only accessor used to assert the
    /// `consensus_certs` eviction bound (see the `PrimaryGossip::Consensus` handler).
    #[cfg(test)]
    pub(crate) fn consensus_certs_len(&self) -> usize {
        self.consensus_certs.lock().len()
    }

    /// Whether a tally for the given consensus-result digest is currently live. Test-only
    /// accessor used to assert per-(signer, number) equivocation behaviour and eviction.
    #[cfg(test)]
    pub(crate) fn consensus_certs_has(&self, digest: &ConsensusResultDigest) -> bool {
        self.consensus_certs.lock().contains_key(digest)
    }

    /// Serve an inbound epoch-pack sync exchange.
    ///
    /// The exchange has already been admitted against the concurrency caps and its
    /// opening request frame read by the caller. This streams the pack via
    /// [`send_sync_epoch_pack_over_stream`] under a total timeout. A send failure is
    /// logged and best-effort signalled with [`SyncFrame::Err`] so the requester
    /// stops waiting; it is not a peer fault, so no penalty is returned (metrics-only
    /// during the item-6 rollout, like the legacy responder).
    ///
    /// [`send_sync_epoch_pack_over_stream`]: crate::network::sync_codec::send_sync_epoch_pack_over_stream
    pub(super) async fn process_sync_epoch_pack_stream(
        &self,
        peer: BlsPublicKey,
        mut stream: Stream,
        epoch: Epoch,
        stop_number: Option<u64>,
        consensus_chain: &ConsensusChain,
    ) -> PrimaryNetworkResult<()> {
        debug!(target: "primary::network", %peer, epoch, ?stop_number, "serving inbound sync epoch pack stream");
        let max_frame = crate::network::sync_codec::MAX_SYNC_PACK_FRAME_SIZE;

        // bound the whole serve; flatten the timeout's outer Result into the send's
        let served = timeout(
            SEND_SYNC_PACK_TIMEOUT,
            crate::network::sync_codec::send_sync_epoch_pack_over_stream(
                &mut stream,
                consensus_chain,
                epoch,
                stop_number,
                SEND_STREAM_BUFFER_TIMEOUT,
                peer,
            ),
        )
        .await
        .map_err(PrimaryNetworkError::from)
        .and_then(|served| served);

        // a send failure or timeout is logged and best-effort signalled so the
        // requester stops waiting; it is not a peer fault, so no penalty
        if let Err(e) = served {
            warn!(target: "primary::network", %peer, ?e, "failed to serve sync epoch pack stream");
            // bound the best-effort error write: a peer that read the `Ack` then
            // stopped reading would otherwise pin this responder task.
            let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
            let _ = timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, async {
                let _ = write_frame(
                    &mut stream,
                    &SyncFrame::<PrimarySyncRequest>::Err(SyncFrameError::Internal),
                    &mut encode_buffer,
                    &mut compressed_buffer,
                    max_frame,
                )
                .await;
            })
            .await;
        }

        // attempt to close the stream gracefully, bounded so a peer that stops
        // reading cannot pin this responder task on the FIN flush
        let _ = timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;

        Ok(())
    }

    /// Serve an inbound missing-certificates sync exchange.
    ///
    /// The exchange has already been admitted against the concurrency caps and its
    /// opening `Req(MissingCertificates { .. })` frame read by the caller. A streaming
    /// [`CertificateCollector`] (no aggregate response-size cap, only its DB-read time
    /// limit) drives [`send_sync_certificates_over_stream`], which writes [`SyncFrame::Ack`]
    /// then the matching certificates as batched `Data` frames terminated by
    /// [`SyncFrame::End`]. This is the streamed replacement for the legacy
    /// request-response `RequestedCertificates` reply, without its `max_response_size`
    /// coupling.
    ///
    /// A request whose `skip_rounds` is out of bounds fails the collector build and is
    /// declined with [`SyncFrame::Err`]`(`[`SyncFrameError::Malformed`]`)` before any
    /// `Ack`; any later serve failure is logged and best-effort signalled with
    /// `SyncFrame::Err`. Errors are metrics-only during the rollout (no penalty),
    /// matching the legacy responder and the epoch-pack sync path.
    ///
    /// [`send_sync_certificates_over_stream`]: crate::network::sync_codec::send_sync_certificates_over_stream
    pub(super) async fn process_sync_missing_certs_stream(
        &self,
        peer: BlsPublicKey,
        mut stream: Stream,
        exclusive_lower_bound: Round,
        skip_rounds: Vec<(AuthorityIdentifier, Vec<u8>)>,
    ) -> PrimaryNetworkResult<()> {
        debug!(target: "primary::network", %peer, exclusive_lower_bound, "serving inbound sync missing certificates stream");
        let max_frame = crate::network::sync_codec::MAX_SYNC_CERT_FRAME_SIZE;
        let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

        // build the streaming collector. An out-of-bounds `skip_rounds` (or an
        // undecodable bitmap) is a malformed request: decline with `Err(Malformed)`
        // before any `Ack` and drop (metrics-only, no penalty during rollout).
        let Ok(collector) = CertificateCollector::new(
            exclusive_lower_bound,
            skip_rounds,
            self.consensus_config.clone(),
        ) else {
            warn!(target: "primary::network", %peer, "rejecting malformed sync missing certificates request");
            let _ = timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, async {
                let _ = write_frame(
                    &mut stream,
                    &SyncFrame::<PrimarySyncRequest>::Err(SyncFrameError::Malformed),
                    &mut encode_buffer,
                    &mut compressed_buffer,
                    max_frame,
                )
                .await;
                stream.close().await
            })
            .await;
            return Ok(());
        };

        // bound the whole serve; flatten the timeout's outer Result into the send's
        let served = timeout(
            SEND_SYNC_CERTS_TIMEOUT,
            crate::network::sync_codec::send_sync_certificates_over_stream(
                &mut stream,
                collector,
                SEND_STREAM_BUFFER_TIMEOUT,
            ),
        )
        .await
        .map_err(PrimaryNetworkError::from)
        .and_then(|served| served);

        // a serve failure or timeout is logged and best-effort signalled so the
        // requester stops waiting; it is not a peer fault, so no penalty
        if let Err(e) = served {
            warn!(target: "primary::network", %peer, ?e, "failed to serve sync missing certificates stream");
            // bound the best-effort error write: a peer that read the `Ack` then
            // stopped reading would otherwise pin this responder task.
            let _ = timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, async {
                let _ = write_frame(
                    &mut stream,
                    &SyncFrame::<PrimarySyncRequest>::Err(SyncFrameError::Internal),
                    &mut encode_buffer,
                    &mut compressed_buffer,
                    max_frame,
                )
                .await;
            })
            .await;
        }

        // attempt to close the stream gracefully, bounded so a peer that stops
        // reading cannot pin this responder task on the FIN flush
        let _ = timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;

        Ok(())
    }

    /// Serve an inbound single-consensus-output sync exchange.
    ///
    /// The exchange has already been admitted against the concurrency caps and its
    /// opening `Req(ConsensusOutput { number })` frame read by the caller. This
    /// resolves the output bytes (with the same brief catch-up wait as before, via
    /// [`Self::consensus_output_bytes`]) and streams them with
    /// [`send_sync_consensus_output_over_stream`]: an available output is sent as
    /// `Ack` + `Data`* + `End`; a benign miss (unknown number, or one not yet built
    /// during catch-up) sheds with a [`SyncFrame::Deny`]`(Unavailable)` frame so the
    /// requester retries elsewhere. A send failure is logged and best-effort signalled
    /// with [`SyncFrame::Err`]; it is not a peer fault, so no penalty is returned
    /// (metrics-only during the item-9b rollout, like the epoch-pack sync path).
    ///
    /// [`send_sync_consensus_output_over_stream`]: crate::network::sync_codec::send_sync_consensus_output_over_stream
    pub(super) async fn process_sync_consensus_output_stream(
        &self,
        peer: BlsPublicKey,
        mut stream: Stream,
        number: u64,
    ) -> PrimaryNetworkResult<()> {
        debug!(target: "primary::network", %peer, number, "serving inbound sync consensus output stream");
        let max_frame = crate::network::sync_codec::MAX_SYNC_PACK_FRAME_SIZE;

        // resolve the output bytes; a benign miss is shed with `Deny(Unavailable)`
        // inside the serve helper (not a peer fault), so drop the error into `None`.
        let bytes = self.consensus_output_bytes(number).await.ok();

        // bound the whole serve; flatten the timeout's outer Result into the send's
        let served = timeout(
            SEND_SYNC_PACK_TIMEOUT,
            crate::network::sync_codec::send_sync_consensus_output_over_stream(
                &mut stream,
                bytes,
                SEND_STREAM_BUFFER_TIMEOUT,
                peer,
                number,
            ),
        )
        .await
        .map_err(PrimaryNetworkError::from)
        .and_then(|served| served);

        // a send failure or timeout is logged and best-effort signalled so the
        // requester stops waiting; it is not a peer fault, so no penalty
        if let Err(e) = served {
            warn!(target: "primary::network", %peer, ?e, "failed to serve sync consensus output stream");
            // bound the best-effort error write: a peer that read the `Ack` then
            // stopped reading would otherwise pin this responder task.
            let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
            let _ = timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, async {
                let _ = write_frame(
                    &mut stream,
                    &SyncFrame::<PrimarySyncRequest>::Err(SyncFrameError::Internal),
                    &mut encode_buffer,
                    &mut compressed_buffer,
                    max_frame,
                )
                .await;
            })
            .await;
        }

        // attempt to close the stream gracefully, bounded so a peer that stops
        // reading cannot pin this responder task on the FIN flush
        let _ = timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;

        Ok(())
    }
}
