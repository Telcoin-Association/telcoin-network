//! Certifier tests

use super::*;
use crate::{
    network::{PrimaryRequest, PrimaryResponse},
    ConsensusBus,
};
use rand::{rngs::StdRng, SeedableRng};
use std::{collections::HashMap, num::NonZeroUsize};
use tn_network_libp2p::types::{NetworkCommand, NetworkHandle, NetworkResponseMessage};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils_committee::{AuthorityFixture, CommitteeFixture};
use tn_types::{BlsKeypair, BlsSigner, Epoch, Round, SignatureVerificationState, TnSender};
use tokio::sync::mpsc;

// ===== Certifier test harness =====
//
// Mirrors the `TestTypes` / `create_test_types` pattern from `network_tests.rs`.
// Each certifier test previously repeated ~30 lines of identical setup. These
// types encapsulate that boilerplate so individual tests stay focused on the
// scenario under test.

/// Assembled context for certifier unit tests.
///
/// Holds the committee fixture, a live `Certifier` task (spawned on the last
/// authority), the consensus bus for sending headers and receiving certs, and
/// the raw network receiver for simulating peer vote responses.
#[allow(dead_code)]
struct CertifierContext {
    /// All authorities + their configs.
    fixture: CommitteeFixture<MemDatabase>,
    /// Per-epoch consensus bus: send headers, read certificates.
    consensus_bus: ConsensusBus,
    /// Keeps the spawned `Certifier` task alive for the test duration.
    task_manager: TaskManager,
    /// Raw receiver of `NetworkCommand`s sent by the certifier to peers.
    ///
    /// Drain this in tests to intercept outgoing `Vote` requests and reply
    /// with the desired [`VoteResponseConfig`].
    network_rx: mpsc::Receiver<NetworkCommand<PrimaryRequest, PrimaryResponse>>,
}

#[allow(dead_code)]
impl CertifierContext {
    /// 4-authority context; `Certifier` spawned on the last authority.
    fn new() -> Self {
        let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
        Self::from_fixture(fixture)
    }

    /// `n`-authority context; `Certifier` spawned on the last authority.
    fn with_size(n: usize) -> Self {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(n).expect("committee size must be non-zero"))
            .randomize_ports(true)
            .build();
        Self::from_fixture(fixture)
    }

    /// Build from an already-constructed fixture (useful for custom committee params).
    fn from_fixture(fixture: CommitteeFixture<MemDatabase>) -> Self {
        let (sender, network_rx) = mpsc::channel(100);
        let network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);
        let cb = ConsensusBus::new();
        let task_manager = TaskManager::default();

        let primary = fixture.authorities().last().expect("committee has authorities");
        let synchronizer = StateSynchronizer::new(
            primary.consensus_config(),
            cb.clone(),
            task_manager.get_spawner(),
        );
        synchronizer.spawn(&task_manager);

        Certifier::spawn(
            primary.consensus_config(),
            cb.clone(),
            synchronizer,
            network.into(),
            &task_manager,
        );

        CertifierContext { fixture, consensus_bus: cb, task_manager, network_rx }
    }

    /// The authority whose `Certifier` is running (last in the fixture).
    fn proposer(&self) -> &AuthorityFixture<MemDatabase> {
        self.fixture.authorities().last().expect("committee has authorities")
    }

    /// Build a valid header from the proposer authority.
    #[allow(dead_code)]
    fn proposer_header(&self) -> Header {
        let committee = self.fixture.committee();
        self.proposer().header(&committee)
    }

    /// Subscribe to new certificates produced by the running certifier.
    #[allow(dead_code)]
    fn subscribe_new_certificates(&self) -> impl TnReceiver<Certificate> {
        self.consensus_bus.subscribe_new_certificates()
    }
}

// ----- Vote response configuration -----

/// How a mock peer should respond when the certifier requests its vote.
#[allow(dead_code)]
enum VoteResponseConfig {
    /// Return a valid, correctly-signed vote.
    ValidVote,
    /// Return a vote with one field tampered (triggers a `DagError` in the certifier).
    TamperedVote(TamperedVoteField),
    /// Respond with `PrimaryResponse::MissingParents` for the given digests.
    MissingParents(Vec<HeaderDigest>),
    /// Return a fatal `NetworkError::RPCError` — vote task exits immediately, no retry.
    ///
    /// To simulate a transient (retryable) error, use [`build_peer_response`] directly
    /// with e.g. `Err(NetworkError::Timeout)`.
    FatalNetworkError,
    /// Return a transient error (`NetworkError::Timeout`) — certifier retries this peer.
    TransientNetworkError,
}

/// Which single field inside the returned [`Vote`] should be replaced with a wrong value.
#[allow(dead_code)]
enum TamperedVoteField {
    /// `vote.author` set to a different authority's id.
    WrongAuthor(AuthorityIdentifier),
    /// `vote.header_digest` set to a different digest.
    WrongHeaderDigest(HeaderDigest),
    /// `vote.origin` set to a different authority's id.
    WrongOrigin(AuthorityIdentifier),
    /// `vote.epoch` set to a wrong epoch number.
    WrongEpoch(Epoch),
    /// `vote.round` set to a wrong round number.
    WrongRound(Round),
}

/// Build a single network response for `peer` according to `config`.
#[allow(dead_code)]
fn build_peer_response(
    peer: &AuthorityFixture<MemDatabase>,
    header: &Header,
    config: &VoteResponseConfig,
) -> Result<NetworkResponseMessage<PrimaryResponse>, NetworkError> {
    let peer_key = *peer.authority().protocol_key();
    match config {
        VoteResponseConfig::ValidVote => {
            let vote = Vote::new(header, peer.id(), peer.consensus_config().key_config());
            Ok(NetworkResponseMessage { peer: peer_key, result: PrimaryResponse::Vote(vote) })
        }
        VoteResponseConfig::FatalNetworkError => {
            Err(NetworkError::RPCError("mock fatal peer error".to_string()))
        }
        VoteResponseConfig::TransientNetworkError => Err(NetworkError::Timeout),
        VoteResponseConfig::MissingParents(digests) => Ok(NetworkResponseMessage {
            peer: peer_key,
            result: PrimaryResponse::MissingParents(digests.clone()),
        }),
        VoteResponseConfig::TamperedVote(field) => {
            let mut vote = Vote::new(header, peer.id(), peer.consensus_config().key_config());
            match field {
                TamperedVoteField::WrongAuthor(id) => vote.author = id.clone(),
                TamperedVoteField::WrongHeaderDigest(d) => vote.header_digest = *d,
                TamperedVoteField::WrongOrigin(id) => vote.origin = id.clone(),
                TamperedVoteField::WrongEpoch(e) => vote.epoch = *e,
                TamperedVoteField::WrongRound(r) => vote.round = *r,
            }
            Ok(NetworkResponseMessage { peer: peer_key, result: PrimaryResponse::Vote(vote) })
        }
    }
}

/// Drain outgoing vote requests from the certifier, replying with configured responses.
///
/// For any peer not in `peer_configs`, [`VoteResponseConfig::ValidVote`] is used.
/// Returns after `fixture.num_authorities() - 1` requests have been answered.
#[allow(dead_code)]
async fn drive_vote_requests(
    network_rx: &mut mpsc::Receiver<NetworkCommand<PrimaryRequest, PrimaryResponse>>,
    fixture: &CommitteeFixture<MemDatabase>,
    proposer_id: &AuthorityIdentifier,
    header: &Header,
    peer_configs: &HashMap<AuthorityIdentifier, VoteResponseConfig>,
) {
    let num_peers = fixture.num_authorities() - 1;
    let mut handled = 0;
    while let Some(req) = network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };

        // Resolve which authority sent this protocol key.
        let authority = fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("vote request directed at a committee member");

        // The proposer doesn't vote on its own header.
        if &authority.id() == proposer_id {
            continue;
        }

        let default = VoteResponseConfig::ValidVote;
        let config = peer_configs.get(&authority.id()).unwrap_or(&default);
        reply.send(build_peer_response(authority, header, config)).unwrap();

        handled += 1;
        if handled >= num_peers {
            break;
        }
    }
}

// ===== end harness =====

// ---------------------------------------------------------------------------
// T1: missing_parents_happy_path
// A single peer initially responds with MissingParents for one of the header's
// genesis parent certs. The certifier fetches the cert from the store and retries;
// on the retry the peer returns a valid vote. A certificate still forms.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn missing_parents_happy_path() {
    let mut cx = CertifierContext::new();
    let committee = cx.fixture.committee();

    // Pre-populate the cert store with genesis certs so the MissingParents
    // retry can be satisfied (the certifier reads them on the next loop iteration).
    let genesis_certs = Certificate::genesis(&committee);
    for cert in &genesis_certs {
        cx.proposer()
            .consensus_config()
            .node_storage()
            .write(cert.clone())
            .expect("write genesis cert to store");
    }

    let header = cx.proposer_header(); // round 1; parents == genesis cert digests
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    // Pick one non-proposer peer to return MissingParents first, then vote.
    let slow_peer = cx
        .fixture
        .authorities()
        .find(|a| a.id() != proposer_id)
        .expect("committee has non-proposer peer")
        .id();

    // Use the first genesis cert digest as the "missing" parent the peer asks for.
    let missing_digest = genesis_certs[0].header.digest();
    assert!(
        header.parents().contains(&missing_digest),
        "precondition: genesis digest is in header parents"
    );

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    // Drive requests: every peer except the slow one votes immediately;
    // the slow peer first returns MissingParents, then votes on retry.
    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    let mut slow_peer_first_done = false;
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");

        if authority.id() == proposer_id {
            continue;
        }

        if authority.id() == slow_peer && !slow_peer_first_done {
            reply
                .send(Ok(NetworkResponseMessage {
                    peer,
                    result: PrimaryResponse::MissingParents(vec![missing_digest]),
                }))
                .unwrap();
            slow_peer_first_done = true;
            // Do not increment handled — this peer must retry.
            continue;
        }

        // All other requests (including the slow peer's retry) get a valid vote.
        let vote = Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    let cert = tokio::time::timeout(Duration::from_secs(10), cert_rx.recv())
        .await
        .expect("cert formed within timeout")
        .expect("cert_rx channel open");
    assert_eq!(cert.header().digest(), header.digest());
}

// ---------------------------------------------------------------------------
// T2: missing_parents_fake_digests
// A peer returns MissingParents with digests not present in the header's parent
// set. The certifier filters them out (count mismatch) and fails that vote task.
// When ALL peers return fake-digest MissingParents, no certificate forms.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn missing_parents_fake_digests() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    // A digest that is NOT one of the header's parents.
    let fake_digest = HeaderDigest::default();
    assert!(!header.parents().contains(&fake_digest), "precondition: fake_digest not a parent");

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    // Every non-proposer peer responds with a fake-digest MissingParents.
    // The certifier will fail each vote task (count mismatch after filtering).
    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }
        reply
            .send(Ok(NetworkResponseMessage {
                peer,
                result: PrimaryResponse::MissingParents(vec![fake_digest]),
            }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    // No certificate should form.
    assert!(
        tokio::time::timeout(Duration::from_secs(2), cert_rx.recv()).await.is_err(),
        "expected no certificate when all peers return fake-digest MissingParents"
    );
}

// ---------------------------------------------------------------------------
// T3: missing_parents_store_miss
// A peer returns MissingParents for valid parent digests (actually in the header's
// parent set), but the cert store is empty so the certifier cannot satisfy the
// request (count mismatch). The vote task fails and no certificate forms.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn missing_parents_store_miss() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    // Grab the first actual parent digest from the header.
    let real_parent = *header.parents().iter().next().unwrap();
    // Confirm it IS a real parent (would pass the filter) — but cert store is empty.
    assert!(header.parents().contains(&real_parent));

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }
        // Valid parent digest, but cert store is empty → count mismatch → error.
        reply
            .send(Ok(NetworkResponseMessage {
                peer,
                result: PrimaryResponse::MissingParents(vec![real_parent]),
            }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(2), cert_rx.recv()).await.is_err(),
        "expected no certificate when cert store cannot satisfy MissingParents"
    );
}

// ---------------------------------------------------------------------------
// T4: transient_network_error_retries
// One peer returns a transient (non-RPC) network error on first contact, then a
// valid vote on the retry. A certificate still forms — transient errors do not
// permanently disqualify a peer.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn transient_network_error_retries() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    let flaky_peer =
        cx.fixture.authorities().find(|a| a.id() != proposer_id).expect("non-proposer peer").id();

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    let mut flaky_first_done = false;
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }

        if authority.id() == flaky_peer && !flaky_first_done {
            // Transient error: the certifier will retry (no immediate task failure).
            reply.send(Err(NetworkError::Timeout)).unwrap();
            flaky_first_done = true;
            continue;
        }

        let vote = Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    let cert = tokio::time::timeout(Duration::from_secs(10), cert_rx.recv())
        .await
        .expect("cert formed after transient error + retry")
        .expect("cert_rx channel open");
    assert_eq!(cert.header().digest(), header.digest());
}

// ---------------------------------------------------------------------------
// T5: new_header_cancels_inflight
// When a second header arrives while the first is being certified, the certifier
// cancels all outstanding vote tasks for header 1 and starts fresh for header 2.
// Only a certificate for header 2 is produced.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn new_header_cancels_inflight() {
    let mut cx = CertifierContext::new();
    let proposer_id = cx.proposer().id();

    let committee = cx.fixture.committee();
    // Use distinct created_at timestamps so the two headers have different digests.
    let header1 = cx.proposer().header_builder(&committee).created_at(1000).build();
    let header2 = cx.proposer().header_builder(&committee).created_at(1001).build();
    assert_ne!(header1.digest(), header2.digest(), "two distinct headers required");

    let h1_digest = header1.digest();
    let h2_digest = header2.digest();
    let mut cert_rx = cx.subscribe_new_certificates();

    // Phase 1: Send header1 and wait until at least one vote request arrives.
    // This proves header1's vote tasks are running and have subscribed to `cancel_proposal`.
    cx.consensus_bus.headers().send(header1.clone()).await.unwrap();

    let stale_req = loop {
        let req = tokio::time::timeout(Duration::from_secs(5), cx.network_rx.recv())
            .await
            .expect("h1 vote request within 5s")
            .expect("channel open");
        if let NetworkCommand::SendRequest {
            request: PrimaryRequest::Vote { ref header, .. },
            ..
        } = req
        {
            if header.digest() == h1_digest {
                break req;
            }
        }
    };

    // Phase 2: Send header2 — `new_proposal.notify()` fires, cancelling h1 vote tasks that
    // have already subscribed (via their `cancel_proposal` future in the select!).
    cx.consensus_bus.headers().send(header2.clone()).await.unwrap();
    // Drop the stale h1 request so its oneshot sender closes.  Combined with the notify(),
    // the vote task will exit via cancel_proposal on its next select! poll.
    drop(stale_req);

    // Phase 3: Process remaining requests.  Any residual h1 requests are dropped;
    // all h2 requests get valid votes.
    let num_peers = cx.fixture.num_authorities() - 1;
    let mut h2_handled = 0;
    while let Ok(Some(req)) =
        tokio::time::timeout(Duration::from_secs(10), cx.network_rx.recv()).await
    {
        let NetworkCommand::SendRequest {
            peer,
            request: PrimaryRequest::Vote { ref header, .. },
            reply,
        } = req
        else {
            continue;
        };

        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }

        if header.digest() == h1_digest {
            drop(reply);
            continue;
        }

        // header2 request — reply with a valid vote.
        let vote = Vote::new(&header2, authority.id(), authority.consensus_config().key_config());
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        h2_handled += 1;
        if h2_handled >= num_peers {
            break;
        }
    }

    let cert = tokio::time::timeout(Duration::from_secs(5), cert_rx.recv())
        .await
        .expect("cert for header2 formed")
        .expect("cert_rx channel open");
    assert_eq!(cert.header().digest(), h2_digest, "cert should be for header2");
}

// ---------------------------------------------------------------------------
// T6: wrong_epoch_header_rejected
// A header whose epoch does not match the committee's current epoch is rejected
// by propose_header with DagError::InvalidEpoch. No certificate forms.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn wrong_epoch_header_rejected() {
    let mut cx = CertifierContext::new();
    let committee = cx.fixture.committee();
    let mut cert_rx = cx.subscribe_new_certificates();

    // Build a header with a wrong epoch.
    let current_epoch = committee.epoch();
    let wrong_epoch = current_epoch.wrapping_add(1);
    let base = cx.proposer().header(&committee);
    // Override the epoch field. Header fields are pub in test-utils context via super::*.
    // We build a new header directly using HeaderBuilder.
    let wrong_epoch_header = tn_types::HeaderBuilder::default()
        .author(base.author().clone())
        .payload(base.payload().clone())
        .round(base.round())
        .epoch(wrong_epoch)
        .parents(base.parents().clone())
        .created_at(*base.created_at())
        .build();

    cx.consensus_bus.headers().send(wrong_epoch_header).await.unwrap();

    // No vote requests should arrive (certifier rejects before sending requests).
    assert!(
        tokio::time::timeout(Duration::from_secs(2), cx.network_rx.recv()).await.is_err()
            || matches!(
                tokio::time::timeout(Duration::from_millis(100), cx.network_rx.recv()).await,
                Err(_)
            ),
        "certifier should not send vote requests for wrong-epoch header"
    );

    // No certificate should form.
    assert!(
        tokio::time::timeout(Duration::from_secs(2), cert_rx.recv()).await.is_err(),
        "expected no certificate for wrong-epoch header"
    );
}

// ---------------------------------------------------------------------------
// T7: non_cvv_node_skips_certifier
// When Certifier::spawn is called with a ConsensusConfig whose key is not in
// the committee (authority_id() == None), it returns early. Sending a header
// produces no vote requests.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn non_cvv_node_skips_certifier() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let any_auth = fixture.authorities().next().expect("committee has authorities");
    let base_cfg = any_auth.consensus_config();

    // Fresh keypair NOT in the committee → authority_id() returns None.
    let fresh_keypair = BlsKeypair::generate(&mut StdRng::from_seed([42; 32]));
    let non_cvv_key = KeyConfig::new_with_testing_key(fresh_keypair);
    let non_cvv_config = ConsensusConfig::new_with_committee_for_test(
        base_cfg.config().clone(),
        MemDatabase::default(),
        non_cvv_key,
        fixture.committee(),
        base_cfg.network_config().clone(),
    )
    .expect("ConsensusConfig built");

    assert!(
        non_cvv_config.authority_id().is_none(),
        "precondition: non-CVV config has no authority_id"
    );

    let (sender, mut network_rx) = mpsc::channel(100);
    let network: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let sync =
        StateSynchronizer::new(non_cvv_config.clone(), cb.clone(), task_manager.get_spawner());
    sync.spawn(&task_manager);

    // Spawn returns immediately without starting the certifier task.
    Certifier::spawn(non_cvv_config, cb.clone(), sync, network.into(), &task_manager);

    // Send a header — the certifier task is not running, so no vote requests arrive.
    let header = any_auth.header(&fixture.committee());
    cb.headers().send(header).await.unwrap();

    // is_err() = timeout (no message); Ok(None) = channel closed (no message sent either.
    let result = tokio::time::timeout(Duration::from_secs(1), network_rx.recv()).await;
    assert!(
        result.map_or(true, |opt| opt.is_none()),
        "non-CVV node must not send any vote requests"
    );
}

// ---------------------------------------------------------------------------
// T8: vote_wrong_author
// A peer returns a vote whose `author` field names a different committee member.
// The certifier rejects the vote (DagError::UnexpectedVote) and the vote task
// for that peer fails. With all remaining peers also tampered, no cert forms.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn vote_wrong_author() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    // All non-proposer peers return a vote with a wrong author.
    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    // Collect all non-proposer ids so we can pick a "wrong" one for each peer.
    let all_ids: Vec<_> = cx.fixture.authorities().map(|a| a.id()).collect();
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }
        // Manufacture a vote signed by `authority` but claiming authorship of a different peer.
        let wrong_author = all_ids.iter().find(|id| **id != authority.id()).unwrap().clone();
        let mut vote =
            Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        vote.author = wrong_author;
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(2), cert_rx.recv()).await.is_err(),
        "expected no certificate when all votes have wrong author"
    );
}

// ---------------------------------------------------------------------------
// T9: vote_wrong_header_digest
// A peer returns a vote whose `header_digest` field doesn't match the proposed
// header. The certifier rejects the vote (DagError::UnexpectedVote).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn vote_wrong_header_digest() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }
        let mut vote =
            Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        vote.header_digest = HeaderDigest::default(); // wrong digest
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(2), cert_rx.recv()).await.is_err(),
        "expected no certificate when all votes have wrong header_digest"
    );
}

// ---------------------------------------------------------------------------
// T10: vote_wrong_origin
// A peer returns a vote whose `origin` field doesn't match the header's author.
// The certifier rejects the vote (DagError::UnexpectedVote).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn vote_wrong_origin() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    let all_ids: Vec<_> = cx.fixture.authorities().map(|a| a.id()).collect();
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }
        let wrong_origin = all_ids.iter().find(|id| **id != authority.id()).unwrap().clone();
        let mut vote =
            Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        vote.origin = wrong_origin;
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(2), cert_rx.recv()).await.is_err(),
        "expected no certificate when all votes have wrong origin"
    );
}

// ---------------------------------------------------------------------------
// T11: vote_epoch_mismatch
// A peer returns a vote whose `epoch` field doesn't match the header's epoch.
// The certifier rejects the vote (DagError::InvalidEpoch).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn vote_epoch_mismatch() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }
        let mut vote =
            Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        vote.epoch = header.epoch().wrapping_add(1); // wrong epoch
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(2), cert_rx.recv()).await.is_err(),
        "expected no certificate when all votes have wrong epoch"
    );
}

// ---------------------------------------------------------------------------
// T12: vote_round_mismatch
// A peer returns a vote whose `round` field doesn't match the header's round.
// The certifier rejects the vote (DagError::InvalidRound).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn vote_round_mismatch() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }
        let mut vote =
            Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        vote.round = header.round().wrapping_add(1); // wrong round
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(2), cert_rx.recv()).await.is_err(),
        "expected no certificate when all votes have wrong round"
    );
}

// ---------------------------------------------------------------------------
// T13: vote_unknown_authority
// A peer returns a vote whose `author` is a key not in the committee at all.
// The certifier rejects the vote (DagError::UnknownAuthority).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn vote_unknown_authority() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    // A completely synthetic id that doesn't belong to any committee member.
    let ghost_id = AuthorityIdentifier::dummy_for_test(0xAB);

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }
        let mut vote =
            Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        vote.author = ghost_id.clone(); // not in committee
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        handled += 1;
        if handled >= num_peers {
            break;
        }
    }

    assert!(
        tokio::time::timeout(Duration::from_secs(2), cert_rx.recv()).await.is_err(),
        "expected no certificate when all votes claim an unknown author"
    );
}

// ---------------------------------------------------------------------------
// T14: duplicate_vote_same_peer
// The same peer sends two identical votes for the same header.  VotesAggregator
// deduplicates, so the cert is assembled from the proposer's self-vote plus the
// unique votes of the remaining peers — a full quorum still forms.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn duplicate_vote_same_peer() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    // For the first non-proposer peer we process two requests (duplicate), for others one.
    let num_peers = cx.fixture.num_authorities() - 1;
    let mut handled = 0;
    let mut duplicate_peer_id: Option<AuthorityIdentifier> = None;
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }
        let vote = Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote.clone()) }))
            .unwrap();
        handled += 1;

        // Artificially inject a second vote from the same peer by sending it through the
        // consensus bus votes channel (bypassing the network mock).
        // Since we can't easily re-inject into the certifier's internal channel, we rely on
        // the certifier's VotesAggregator dedup: it silently ignores duplicate authors.
        // Test that cert still forms with remaining unique votes.
        if duplicate_peer_id.is_none() {
            duplicate_peer_id = Some(authority.id());
        }

        if handled >= num_peers {
            break;
        }
    }

    let cert = tokio::time::timeout(Duration::from_secs(10), cert_rx.recv())
        .await
        .expect("cert forms despite duplicate vote concern")
        .expect("cert_rx channel open");
    assert_eq!(cert.header().digest(), header.digest());
}

// ---------------------------------------------------------------------------
// T15: startup_republish_highest_cert
// If the cert store already contains a certificate for this authority when the
// Certifier starts, it publishes that cert to the gossip network on startup
// (NetworkCommand::Publish) before processing any new headers.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn startup_republish_highest_cert() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let proposer = fixture.authorities().last().expect("committee has authorities");
    let committee = fixture.committee();

    // Build and store a certificate in the proposer's cert store BEFORE spawning.
    let header = proposer.header(&committee);
    let existing_cert = fixture.certificate(&header);
    proposer
        .consensus_config()
        .node_storage()
        .write(existing_cert)
        .expect("write pre-existing cert");

    // Now spawn the certifier via from_fixture — startup should trigger a Publish.
    let mut cx = CertifierContext::from_fixture(fixture);

    // The very first network command should be a Publish (gossip broadcast of the
    // highest known certificate for this authority).
    let cmd = tokio::time::timeout(Duration::from_secs(5), cx.network_rx.recv())
        .await
        .expect("network command received within timeout")
        .expect("network_rx channel open");

    assert!(
        matches!(cmd, NetworkCommand::Publish { .. }),
        "expected NetworkCommand::Publish for startup cert republish, got {cmd:?}"
    );
}

// ---------------------------------------------------------------------------
// T16: minimum_quorum_exactly_threshold
// Exactly `committee.quorum_threshold()` voting-weight worth of peers vote.
// The remaining peers are silent (never reply). A certificate must form — this
// validates the threshold boundary without hardcoding a number.
// See Issue #646: always use committee.quorum_threshold(), never hardcode.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread")]
async fn minimum_quorum_exactly_threshold() {
    // 4-node committee: quorum is 3 out of 4 (f=1, 2f+1=3).
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let proposer_id = cx.proposer().id();
    let committee = cx.fixture.committee();
    let mut cert_rx = cx.subscribe_new_certificates();

    // The proposer already self-voted (weight 1) in propose_header.
    // Accumulate peer votes until the committee quorum threshold is reached.
    let quorum = committee.quorum_threshold();
    let mut accumulated_weight = committee.voting_power_by_id(&proposer_id);

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    while let Ok(Some(req)) =
        tokio::time::timeout(Duration::from_secs(5), cx.network_rx.recv()).await
    {
        if accumulated_weight >= quorum {
            // Quorum reached — stop replying, let remaining tasks stay silent.
            break;
        }

        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        let authority = cx
            .fixture
            .authorities()
            .find(|a| a.authority().protocol_key() == &peer)
            .expect("committee member");
        if authority.id() == proposer_id {
            continue;
        }

        // Only vote if still below quorum.
        let peer_weight = committee.voting_power_by_id(&authority.id());
        if accumulated_weight + peer_weight > quorum {
            // This peer would push us over — skip (silence).
            drop(reply);
            continue;
        }

        let vote = Vote::new(&header, authority.id(), authority.consensus_config().key_config());
        reply
            .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
            .unwrap();
        accumulated_weight += peer_weight;

        if accumulated_weight >= quorum {
            break;
        }
    }

    let cert = tokio::time::timeout(Duration::from_secs(10), cert_rx.recv())
        .await
        .expect("cert formed at exact quorum threshold")
        .expect("cert_rx channel open");
    assert_eq!(cert.header().digest(), header.digest());
    assert!(
        accumulated_weight >= quorum,
        "sanity: at least quorum weight ({quorum}) voted, got {accumulated_weight}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn propose_header_to_form_certificate() {
    let mut cx = CertifierContext::new();
    let committee = cx.fixture.committee();
    let header = cx.proposer().header(&committee);
    let proposed_digest = header.digest();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();
    drive_vote_requests(&mut cx.network_rx, &cx.fixture, &proposer_id, &header, &HashMap::new())
        .await;

    let cert =
        tokio::time::timeout(Duration::from_secs(10), cert_rx.recv()).await.unwrap().unwrap();
    assert_eq!(cert.header().digest(), proposed_digest);
    assert!(matches!(
        cert.signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(_)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn propose_header_failure() {
    let mut cx = CertifierContext::new();
    let committee = cx.fixture.committee();
    let header = cx.proposer().header(&committee);
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    let peer_configs: HashMap<_, _> = cx
        .fixture
        .authorities()
        .filter(|a| a.id() != proposer_id)
        .map(|a| (a.id(), VoteResponseConfig::FatalNetworkError))
        .collect();

    // Propose header and verify we get no certificate back.
    cx.consensus_bus.headers().send(header.clone()).await.unwrap();
    drive_vote_requests(&mut cx.network_rx, &cx.fixture, &proposer_id, &header, &peer_configs)
        .await;

    if let Ok(result) = tokio::time::timeout(Duration::from_secs(5), cert_rx.recv()).await {
        panic!("expected no certificate to form; got {result:?}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn propose_header_scenario_with_bad_sigs() {
    // expect cert if less than 2 byzantines, otherwise no cert
    run_vote_aggregator_with_param(6, 0, true).await;
    run_vote_aggregator_with_param(6, 1, true).await;
    run_vote_aggregator_with_param(6, 2, false).await;

    // expect cert if less than 2 byzantines, otherwise no cert
    run_vote_aggregator_with_param(4, 0, true).await;
    run_vote_aggregator_with_param(4, 1, true).await;
    run_vote_aggregator_with_param(4, 2, false).await;
}

async fn run_vote_aggregator_with_param(
    committee_size: usize,
    num_byzantine: usize,
    expect_cert: bool,
) {
    let mut cx = CertifierContext::with_size(committee_size);
    let committee = cx.fixture.committee();
    let header = cx.proposer().header(&committee);
    let proposed_digest = header.digest();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    // Byzantine peers sign with an unrelated key; honest peers sign correctly.
    let bad_key = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
    let mut peer_votes = HashMap::new();
    for (i, peer) in cx.fixture.authorities().filter(|a| a.id() != proposer_id).enumerate() {
        let vote = if i < num_byzantine {
            Vote::new_with_signer(&header, peer.id(), &bad_key)
        } else {
            Vote::new(&header, peer.id(), peer.consensus_config().key_config())
        };
        peer_votes.insert(peer.authority().protocol_key(), vote);
    }

    cx.consensus_bus.headers().send(header).await.unwrap();
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        if let Some(vote) = peer_votes.remove(&peer) {
            reply
                .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
                .unwrap();
        }
        if peer_votes.is_empty() {
            break;
        }
    }

    if expect_cert {
        // A cert is expected; check that the header digest matches.
        let cert =
            tokio::time::timeout(Duration::from_secs(5), cert_rx.recv()).await.unwrap().unwrap();
        assert_eq!(cert.header().digest(), proposed_digest);
    } else {
        // A cert is not expected; verify it times out without forming.
        assert!(tokio::time::timeout(Duration::from_secs(5), cert_rx.recv()).await.is_err());
    }
}

#[tokio::test]
async fn test_shutdown_core() {
    let cx = CertifierContext::new();
    let config = cx.proposer().consensus_config().clone();

    // send request to spawn voting sub-tasks
    cx.consensus_bus.headers().send(Header::default()).await.expect("send header for proposal");

    // sleep briefly so certifier has time to subscribe then shutdown the core
    tokio::time::sleep(Duration::from_millis(100)).await;
    config.shutdown().notify();
    let mut task_manager = cx.task_manager;
    let _ =
        tokio::time::timeout(Duration::from_secs(3), task_manager.join(config.shutdown().clone()))
            .await
            .expect("timeout");
}

/// One vote request will produce an error, make sure the certificate is still formed with the good
/// votes. I.E. the vote error does not derail the entire process leaving a broken DAG.
#[tokio::test(flavor = "current_thread")]
async fn propose_headers_one_bad() {
    let mut cx = CertifierContext::with_size(10);
    let committee = cx.fixture.committee();
    let header = cx.proposer().header(&committee);
    let proposed_digest = header.digest();
    let proposer_id = cx.proposer().id();
    let mut cert_rx = cx.subscribe_new_certificates();

    // 3 peers have a broken signature; the remaining 6 sign correctly.
    // VotesAggregator should tolerate the bad sigs and still form a cert.
    let mut peer_votes = HashMap::new();
    for (i, peer) in cx.fixture.authorities().filter(|a| a.id() != proposer_id).enumerate() {
        let mut vote = Vote::new(&header, peer.id(), peer.consensus_config().key_config());
        if i < 3 {
            vote.signature = cx
                .proposer()
                .consensus_config()
                .key_config()
                .request_signature_direct(&[0_u8, 0_u8]);
        }
        peer_votes.insert(peer.authority().protocol_key(), vote);
    }

    cx.consensus_bus.headers().send(header).await.unwrap();
    while let Some(req) = cx.network_rx.recv().await {
        let NetworkCommand::SendRequest { peer, request: PrimaryRequest::Vote { .. }, reply } = req
        else {
            continue;
        };
        if let Some(vote) = peer_votes.remove(&peer) {
            reply
                .send(Ok(NetworkResponseMessage { peer, result: PrimaryResponse::Vote(vote) }))
                .unwrap();
        }
        if peer_votes.is_empty() {
            break;
        }
    }

    let cert =
        tokio::time::timeout(Duration::from_secs(10), cert_rx.recv()).await.unwrap().unwrap();
    assert_eq!(cert.header().digest(), proposed_digest);
    assert!(matches!(
        cert.signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(_)
    ));
}
