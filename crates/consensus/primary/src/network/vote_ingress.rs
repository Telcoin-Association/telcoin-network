//! Node-lifetime ingress for epoch-record votes.
//!
//! Epoch votes travel over the `tn-epoch-vote-{chain_id}` gossip topic, but their historical
//! consumer — the epoch-scoped [`PrimaryNetwork`](super::PrimaryNetwork) event loop — is aborted
//! at every epoch boundary, milliseconds after the node publishes its own vote for the closing
//! epoch. Peer votes arrive at least one network round-trip later and landed in
//! `primary_network_events` with no consumer: on a live close they queued until the next epoch's
//! loop spawned (certifying the record late), and on an entry that parks on the certified-anchor
//! wait they were never consumed at all — no consumer, no quorum, no certificate, and the park
//! never resolves.
//!
//! The fix is sender-side routing. [`PrimaryEventRouter`] wraps the primary swarm's event sink
//! and diverts gossip on the epoch-vote topic to a dedicated always-subscribed channel, consumed
//! by the node-lifetime task spawned in [`spawn_epoch_vote_ingress`]; every other event passes
//! through to `primary_network_events` with identical send semantics. Routing must happen on the
//! sender side because `QueChannel` is strictly single-consumer (`subscribe` panics while a
//! subscription is live), so a second drainer of the primary event queue is structurally
//! impossible.
//!
//! The ingress runs the same gate ordering as the (now vestigial) handler arm — topic is
//! guaranteed by the router, then committee membership, dedup, and the BLS signature check last —
//! and forwards verified votes to `new_epoch_votes` for the node-lifetime vote collector.
//! Committee membership resolves from stored epoch records only (`get_committee_keys`: the
//! epoch's own record, else the previous record's `next_committee`), so the ingress needs no
//! epoch-scoped `ConsensusConfig` and keeps working while the node is parked between epochs. A
//! vote whose committee cannot be resolved yet is dropped without penalty: its author republishes
//! on a timer, and the vote is accepted once the record is stored.

use std::collections::HashMap;

use parking_lot::Mutex;
use tn_network_libp2p::types::NetworkEvent;
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    try_decode, BlsPublicKey, Epoch, EpochDigest, Noticer, SendError, TaskSpawner, TnReceiver as _,
    TnSender, TrySendError,
};
use tracing::debug;

use super::{message::PrimaryGossip, Req, Res, MAX_EPOCH_VOTES};
use crate::{ConsensusBusApp, QueChannel};

/// Dedup key for an epoch vote at ingress: `(author, epoch, epoch-record hash)`.
///
/// The `epoch` field is part of the key even though it is not covered by the vote signature:
/// this ensures a replay that tampers with `vote.epoch` lands on a different key and cannot
/// evict/pre-empt the real vote's slot. See issue #898.
type EpochVoteKey = (BlsPublicKey, Epoch, EpochDigest);

/// Bounded seen-set for verified epoch votes, shared in shape by the node-lifetime ingress and
/// the epoch-scoped request handler.
///
/// An accepted vote is re-gossiped by its author on a timer and duplicated by the gossip mesh,
/// so this caps repeated BLS verifies of the same valid vote. The value is a monotonic recency
/// sequence used to evict the least-recently-seen entry once the map reaches
/// [`MAX_EPOCH_VOTES`], bounding memory against a flood of distinct keys; a false negative after
/// eviction only costs one extra verify (the collector dedups per signer downstream), never a
/// dropped honest vote.
#[derive(Debug, Default)]
pub(super) struct EpochVotesSeen {
    /// Verified `(author, epoch, epoch-record)` keys with their recency sequence.
    seen: Mutex<HashMap<EpochVoteKey, u64>>,
}

impl EpochVotesSeen {
    /// True if a vote for this `(author, epoch, epoch-record)` has already been verified and
    /// forwarded. Cheap read used as a pre-verify gate.
    pub(super) fn contains(
        &self,
        author: BlsPublicKey,
        epoch: Epoch,
        epoch_hash: EpochDigest,
    ) -> bool {
        self.seen.lock().contains_key(&(author, epoch, epoch_hash))
    }

    /// Record a verified `(author, epoch, epoch-record)` vote so later replays are dropped
    /// before the signature check.
    ///
    /// Callers MUST invoke this only after [`EpochVote::check_signature`] succeeds: recording a
    /// bad-signature vote would let a Byzantine committee publisher poison the slot of the real
    /// author and suppress their vote.
    ///
    /// [`EpochVote::check_signature`]: tn_types::EpochVote::check_signature
    pub(super) fn record(&self, author: BlsPublicKey, epoch: Epoch, epoch_hash: EpochDigest) {
        let key = (author, epoch, epoch_hash);
        let mut guard = self.seen.lock();
        let next_seq = guard.values().copied().max().unwrap_or(0) + 1;
        if guard.len() >= MAX_EPOCH_VOTES && !guard.contains_key(&key) {
            let evict = guard.iter().min_by_key(|entry| *entry.1).map(|entry| *entry.0);
            if let Some(evict) = evict {
                guard.remove(&evict);
            }
        }
        guard.insert(key, next_seq);
    }
}

/// Sender-side router installed as the primary swarm's event sink.
///
/// Gossip whose topic is exactly the chain's epoch-vote topic diverts to the internal
/// always-subscribed vote channel; every other [`NetworkEvent`] — all request/response traffic,
/// inbound streams, and gossip on any other topic — passes through to `primary_network_events`
/// via the same `send`/`try_send` method that was called on the router, preserving the swarm's
/// non-blocking `try_send` semantics bit for bit.
#[derive(Debug, Clone)]
pub struct PrimaryEventRouter {
    /// Epoch-vote gossip, consumed by the node-lifetime ingress task.
    vote_events: QueChannel<NetworkEvent<Req, Res>>,
    /// The pass-through target: the application bus's `primary_network_events` channel, consumed
    /// by the epoch-scoped `PrimaryNetwork`.
    primary_events: QueChannel<NetworkEvent<Req, Res>>,
    /// Precomputed `tn-epoch-vote-{chain_id}` topic string compared against gossip topics.
    epoch_vote_topic: String,
}

impl PrimaryEventRouter {
    /// Wrap `primary_events` (the bus's `primary_network_events` channel) for `chain_id`.
    pub fn new(primary_events: QueChannel<NetworkEvent<Req, Res>>, chain_id: u64) -> Self {
        Self {
            vote_events: QueChannel::new_always_subscribed(),
            primary_events,
            epoch_vote_topic: tn_config::LibP2pConfig::epoch_vote_topic(chain_id),
        }
    }

    /// True only for gossip on the epoch-vote topic; every other event passes through.
    fn diverts(&self, event: &NetworkEvent<Req, Res>) -> bool {
        matches!(
            event,
            NetworkEvent::Gossip(payload)
                if payload.message.topic.as_str() == self.epoch_vote_topic
        )
    }
}

impl TnSender<NetworkEvent<Req, Res>> for PrimaryEventRouter {
    async fn send(
        &self,
        value: NetworkEvent<Req, Res>,
    ) -> Result<(), SendError<NetworkEvent<Req, Res>>> {
        if self.diverts(&value) {
            self.vote_events.send(value).await
        } else {
            self.primary_events.send(value).await
        }
    }

    fn try_send(
        &self,
        value: NetworkEvent<Req, Res>,
    ) -> Result<(), TrySendError<NetworkEvent<Req, Res>>> {
        if self.diverts(&value) {
            self.vote_events.try_send(value)
        } else {
            self.primary_events.try_send(value)
        }
    }
}

/// Spawn the node-lifetime task that consumes the router's diverted epoch-vote gossip.
///
/// Runs until node shutdown on the node-lifetime spawner. Each event is decoded, gated
/// (committee membership from stored epoch records, dedup, BLS signature), and forwarded to
/// `new_epoch_votes` for the vote collector. Must be called exactly once per router: the
/// vote channel is single-consumer.
pub fn spawn_epoch_vote_ingress(
    router: &PrimaryEventRouter,
    consensus_chain: ConsensusChain,
    consensus_bus: ConsensusBusApp,
    task_spawner: &TaskSpawner,
    shutdown: Noticer,
) {
    let mut events = router.vote_events.subscribe();
    task_spawner.spawn_critical_task("Epoch Vote Ingress", async move {
        let seen = EpochVotesSeen::default();
        loop {
            tokio::select! {
                _ = &shutdown => return Ok(()),
                event = events.recv() => {
                    let Some(event) = event else { return Ok(()) };
                    ingest_epoch_vote_event(event, &consensus_chain, &consensus_bus, &seen).await;
                }
            }
        }
    });
}

/// Verify one diverted event and forward the vote to the collector.
///
/// Mirrors the gate ordering of the handler's `PrimaryGossip::EpochVote` arm (issue #898): the
/// cheap, attacker-independent checks run before the expensive BLS verify. Failures are dropped
/// at `debug!` without peer penalties — a vote reaching this task already passed the swarm's
/// authorized-publisher check for the committee-restricted epoch-vote topic, so penalty
/// attribution adds nothing here.
async fn ingest_epoch_vote_event(
    event: NetworkEvent<Req, Res>,
    consensus_chain: &ConsensusChain,
    consensus_bus: &ConsensusBusApp,
    seen: &EpochVotesSeen,
) {
    // Router invariant: only gossip on the epoch-vote topic is diverted here.
    let NetworkEvent::Gossip(payload) = event else {
        debug!(target: "primary", "epoch-vote ingress dropping unexpected non-gossip event");
        return;
    };
    let vote = match try_decode(&payload.message.data) {
        Ok(PrimaryGossip::EpochVote(vote)) => vote,
        Ok(other) => {
            debug!(
                target: "primary",
                ?other,
                "epoch-vote ingress dropping non-vote gossip on the epoch-vote topic"
            );
            return;
        }
        Err(e) => {
            debug!(target: "primary", ?e, "epoch-vote ingress failed to decode gossip");
            return;
        }
    };
    // Committee membership BEFORE crypto, resolved from stored epoch records only. Membership is
    // by epoch *number*, not `epoch_hash`, so a member's vote for a forked/alternative record is
    // still admitted (the collector's equivocation path needs it).
    let Some(committee) = consensus_chain.epochs().get_committee_keys(vote.epoch).await else {
        debug!(
            target: "primary",
            epoch = vote.epoch,
            author = ?vote.public_key,
            "epoch-vote ingress dropping vote for unknown committee epoch"
        );
        return;
    };
    if !committee.contains(&vote.public_key) {
        debug!(
            target: "primary",
            epoch = vote.epoch,
            author = ?vote.public_key,
            "epoch-vote ingress dropping non-committee vote"
        );
        return;
    }
    // Drop votes already verified and forwarded for this (author, record).
    if seen.contains(vote.public_key, vote.epoch, vote.epoch_hash) {
        return;
    }
    // Signature check LAST — the most expensive gate.
    if !vote.check_signature() {
        debug!(
            target: "primary",
            epoch = vote.epoch,
            author = ?vote.public_key,
            "epoch-vote ingress dropping vote with invalid signature"
        );
        return;
    }
    // Record only AFTER a valid signature so a bad-signature vote cannot poison the dedup slot
    // for the real author.
    seen.record(vote.public_key, vote.epoch, vote.epoch_hash);
    debug!(
        target: "primary",
        epoch = vote.epoch,
        author = ?vote.public_key,
        "epoch-vote ingress forwarded vote"
    );
    // Fire-and-forget: no oneshot, no blocking consumer dependency.
    let _ = consensus_bus.new_epoch_votes().send(*vote).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConsensusBus;
    use rand::SeedableRng as _;
    use tempfile::TempDir;
    use tn_config::{KeyConfig, LibP2pConfig};
    use tn_network_libp2p::{types::GossipPayload, GossipMessage, TopicHash};
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils_committee::CommitteeFixture;
    use tn_types::{encode, BlsKeypair, EpochRecord, EpochVote, TryRecvError};

    /// Build a gossip event on `topic` carrying raw `data`.
    fn gossip_event(topic: &str, data: Vec<u8>) -> NetworkEvent<Req, Res> {
        NetworkEvent::Gossip(Box::new(GossipPayload {
            message: GossipMessage {
                source: None,
                data,
                sequence_number: None,
                topic: TopicHash::from_raw(topic),
            },
            relayer: None,
            author: None,
        }))
    }

    /// Byte marker carried by a routed event, for asserting which channel received it.
    fn marker(event: NetworkEvent<Req, Res>) -> u8 {
        match event {
            NetworkEvent::Gossip(payload) => payload.message.data[0],
            other => panic!("expected gossip event, got {other:?}"),
        }
    }

    /// Gossip on the epoch-vote topic diverts to the vote channel; gossip on every other topic
    /// passes through to the primary channel, for both `send` and `try_send`.
    ///
    /// The non-gossip variants (`Request`, `Error`, `InboundStream`) cannot be constructed
    /// outside a live swarm (their `ResponseChannel`/`Stream` constructors are private to
    /// `tn-network-libp2p`), and take the pass-through arm by construction: `diverts` matches
    /// only `NetworkEvent::Gossip` with the exact epoch-vote topic string. The e2e suites
    /// exercise the full variant set through a real swarm.
    #[tokio::test]
    async fn router_diverts_only_epoch_vote_topic_gossip() {
        let chain_id = 2017;
        let primary_events = QueChannel::new_always_subscribed();
        let mut primary_rx = primary_events.subscribe();
        let router = PrimaryEventRouter::new(primary_events, chain_id);
        let mut vote_rx = router.vote_events.subscribe();
        let vote_topic = LibP2pConfig::epoch_vote_topic(chain_id);

        router.send(gossip_event(&vote_topic, vec![1])).await.expect("send diverted");
        router.try_send(gossip_event(&vote_topic, vec![2])).expect("try_send diverted");
        router
            .send(gossip_event(&LibP2pConfig::primary_topic(chain_id), vec![3]))
            .await
            .expect("send passed through");
        router
            .try_send(gossip_event(&LibP2pConfig::consensus_output_topic(chain_id), vec![4]))
            .expect("try_send passed through");
        // A vote topic for a DIFFERENT chain id must not divert.
        router
            .send(gossip_event(&LibP2pConfig::epoch_vote_topic(chain_id + 1), vec![5]))
            .await
            .expect("foreign-chain vote topic passed through");

        assert_eq!(marker(vote_rx.recv().await.expect("first diverted event")), 1);
        assert_eq!(marker(vote_rx.recv().await.expect("second diverted event")), 2);
        assert_eq!(marker(primary_rx.recv().await.expect("first pass-through")), 3);
        assert_eq!(marker(primary_rx.recv().await.expect("second pass-through")), 4);
        assert_eq!(marker(primary_rx.recv().await.expect("third pass-through")), 5);
        assert!(matches!(vote_rx.try_recv(), Err(TryRecvError::Empty)));
        assert!(matches!(primary_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    /// Shared scaffolding for the ingest tests: a stored epoch-0 record with a 4-key committee
    /// (the record db only accepts sequential epochs, so epoch 0 is the first storable record).
    struct IngestSetup {
        consensus_chain: ConsensusChain,
        consensus_bus: ConsensusBus,
        record: EpochRecord,
        signers: Vec<BlsKeypair>,
        vote_topic: String,
        // Held for the test's lifetime so the epoch db files survive.
        _temp_dir: TempDir,
    }

    async fn ingest_setup(save_record: bool) -> IngestSetup {
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let signers: Vec<BlsKeypair> = (0..4).map(|_| BlsKeypair::generate(&mut rng)).collect();
        let committee: Vec<_> = signers.iter().map(|kp| *kp.public()).collect();
        let record = EpochRecord {
            epoch: 0,
            committee: committee.clone(),
            next_committee: committee,
            ..Default::default()
        };
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::new().expect("tempdir");
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .expect("consensus chain");
        if save_record {
            consensus_chain.epochs().save_record(record.clone()).await.expect("save record");
        }
        IngestSetup {
            consensus_chain,
            consensus_bus: ConsensusBus::new(),
            record,
            signers,
            vote_topic: LibP2pConfig::epoch_vote_topic(2017),
            _temp_dir: temp_dir,
        }
    }

    /// Encode a vote the way the wire carries it.
    fn vote_event(topic: &str, vote: EpochVote) -> NetworkEvent<Req, Res> {
        gossip_event(topic, encode(&PrimaryGossip::EpochVote(Box::new(vote))))
    }

    /// A valid committee vote is verified and forwarded to `new_epoch_votes` exactly once;
    /// a replay of the same vote is dropped by the dedup gate.
    #[tokio::test]
    async fn ingest_forwards_valid_vote_and_dedups_replay() {
        let setup = ingest_setup(true).await;
        let mut votes_rx = setup.consensus_bus.app().subscribe_new_epoch_votes();
        let seen = EpochVotesSeen::default();
        let key_config = KeyConfig::new_with_testing_key(setup.signers[1].copy());
        let vote = setup.record.sign_vote(&key_config);

        ingest_epoch_vote_event(
            vote_event(&setup.vote_topic, vote),
            &setup.consensus_chain,
            setup.consensus_bus.app(),
            &seen,
        )
        .await;
        let forwarded = votes_rx.recv().await.expect("vote forwarded");
        assert_eq!(forwarded, vote);

        // Replay: dropped before re-verification, nothing forwarded.
        ingest_epoch_vote_event(
            vote_event(&setup.vote_topic, vote),
            &setup.consensus_chain,
            setup.consensus_bus.app(),
            &seen,
        )
        .await;
        assert!(matches!(votes_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    /// A vote from a key outside the record committee is dropped.
    #[tokio::test]
    async fn ingest_drops_non_committee_vote() {
        let setup = ingest_setup(true).await;
        let mut votes_rx = setup.consensus_bus.app().subscribe_new_epoch_votes();
        let seen = EpochVotesSeen::default();
        let mut rng = rand::rngs::StdRng::seed_from_u64(99);
        let outsider = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut rng));
        let vote = setup.record.sign_vote(&outsider);

        ingest_epoch_vote_event(
            vote_event(&setup.vote_topic, vote),
            &setup.consensus_chain,
            setup.consensus_bus.app(),
            &seen,
        )
        .await;
        assert!(matches!(votes_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    /// A committee vote whose signature does not verify is dropped and does not poison the
    /// dedup slot: the author's genuine vote still gets through afterwards.
    #[tokio::test]
    async fn ingest_drops_invalid_signature_without_poisoning_dedup() {
        let setup = ingest_setup(true).await;
        let mut votes_rx = setup.consensus_bus.app().subscribe_new_epoch_votes();
        let seen = EpochVotesSeen::default();
        let key_config = KeyConfig::new_with_testing_key(setup.signers[2].copy());
        let genuine = setup.record.sign_vote(&key_config);
        let mut forged = genuine;
        forged.epoch_hash = EpochDigest::default();

        ingest_epoch_vote_event(
            vote_event(&setup.vote_topic, forged),
            &setup.consensus_chain,
            setup.consensus_bus.app(),
            &seen,
        )
        .await;
        assert!(matches!(votes_rx.try_recv(), Err(TryRecvError::Empty)));

        ingest_epoch_vote_event(
            vote_event(&setup.vote_topic, genuine),
            &setup.consensus_chain,
            setup.consensus_bus.app(),
            &seen,
        )
        .await;
        assert_eq!(votes_rx.recv().await.expect("genuine vote forwarded"), genuine);
    }

    /// A vote for an epoch with no stored record (and no previous record) is dropped; once the
    /// record is saved, the author's republished vote is accepted — the self-healing path relied
    /// on during recovery.
    #[tokio::test]
    async fn ingest_accepts_vote_after_record_is_stored() {
        let setup = ingest_setup(false).await;
        let mut votes_rx = setup.consensus_bus.app().subscribe_new_epoch_votes();
        let seen = EpochVotesSeen::default();
        let key_config = KeyConfig::new_with_testing_key(setup.signers[0].copy());
        let vote = setup.record.sign_vote(&key_config);

        ingest_epoch_vote_event(
            vote_event(&setup.vote_topic, vote),
            &setup.consensus_chain,
            setup.consensus_bus.app(),
            &seen,
        )
        .await;
        assert!(matches!(votes_rx.try_recv(), Err(TryRecvError::Empty)));

        setup
            .consensus_chain
            .epochs()
            .save_record(setup.record.clone())
            .await
            .expect("save record");
        ingest_epoch_vote_event(
            vote_event(&setup.vote_topic, vote),
            &setup.consensus_chain,
            setup.consensus_bus.app(),
            &seen,
        )
        .await;
        assert_eq!(votes_rx.recv().await.expect("republished vote forwarded"), vote);
    }

    /// With no record stored for the vote's epoch, membership falls back to the previous
    /// record's `next_committee` — the live-close case where peers vote on an epoch this node
    /// has not persisted yet.
    #[tokio::test]
    async fn ingest_resolves_committee_from_prev_record_next_committee() {
        let setup = ingest_setup(true).await;
        let mut votes_rx = setup.consensus_bus.app().subscribe_new_epoch_votes();
        let seen = EpochVotesSeen::default();
        // The epoch-1 record exists only on the wire (never saved locally); membership must
        // resolve through the stored epoch-0 record's next_committee.
        let next_record = EpochRecord {
            epoch: 1,
            committee: setup.record.committee.clone(),
            next_committee: setup.record.committee.clone(),
            parent_hash: setup.record.digest(),
            ..Default::default()
        };
        let key_config = KeyConfig::new_with_testing_key(setup.signers[3].copy());
        let vote = next_record.sign_vote(&key_config);

        ingest_epoch_vote_event(
            vote_event(&setup.vote_topic, vote),
            &setup.consensus_chain,
            setup.consensus_bus.app(),
            &seen,
        )
        .await;
        assert_eq!(votes_rx.recv().await.expect("fallback-committee vote forwarded"), vote);
    }
}
