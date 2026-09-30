//! Certifier tests

use super::*;
use crate::{
    aggregators::VotesAggregator,
    network::{PrimaryRequest, PrimaryResponse},
    ConsensusBus,
};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;
use std::{collections::HashMap, num::NonZeroUsize};
use tn_network_libp2p::types::{
    NetworkCommand, NetworkHandle, NetworkResponseMessage, NetworkResponseSender,
};
use tn_storage::{mem_db::MemDatabase, tables::ProposedCertificates};
use tn_test_utils_committee::{AuthorityFixture, CommitteeFixture};
use tn_types::{
    encode, error::DagError, BlsKeypair, BlsSigner, HeaderBuilder, SignatureVerificationState,
    TnSender,
};
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
    fn proposer_header(&self) -> Header {
        let committee = self.fixture.committee();
        self.proposer().header(&committee)
    }

    /// Subscribe to new certificates produced by the running certifier.
    fn subscribe_new_certificates(&self) -> impl TnReceiver<Certificate> {
        self.consensus_bus.subscribe_new_certificates()
    }
}

// ----- Vote response configuration -----

/// How a mock peer should respond when the certifier requests its vote.
enum VoteResponseConfig {
    /// Return a valid, correctly-signed vote.
    ValidVote,
    /// Return a fatal `NetworkError::RPCError` — vote task exits immediately, no retry.
    ///
    /// To simulate a transient (retryable) error, use [`build_peer_response`] directly
    /// with e.g. `Err(NetworkError::Timeout)`.
    FatalNetworkError,
}

/// Build a single network response for `peer` according to `config`.
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
    }
}

/// Upper bound on every wait for the certifier's next network command.
///
/// Every test runs on tokio's paused clock, so this is virtual time. The clock auto-advances only
/// while every task is idle: the wait costs no wall-clock time and expires only once the certifier
/// genuinely has nothing left to send. It must exceed the certifier's 10s vote-retry backoff
/// ceiling; otherwise a wait could tie with a backoff tick and fail a test that is only waiting for
/// a retry.
const STEP_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a test watches for something that must NOT happen, such as a certificate forming.
///
/// Virtual time on the paused clock, like [`STEP_TIMEOUT`]. It is longer than the certifier's 10s
/// vote-retry ceiling, so any retry that was going to fire has fired before the window closes.
const QUIET_WINDOW: Duration = Duration::from_secs(30);

/// Receive the certifier's next network command, panicking if none arrives within
/// [`STEP_TIMEOUT`].
///
/// The certifier task holds the channel's sender for the whole test, so a bare `recv()` never
/// returns `None`: a certifier that sends fewer commands than a test expects would hang the test
/// instead of failing it. `context` names what the caller was waiting for.
async fn next_command(
    network_rx: &mut mpsc::Receiver<NetworkCommand<PrimaryRequest, PrimaryResponse>>,
    context: &str,
) -> NetworkCommand<PrimaryRequest, PrimaryResponse> {
    match tokio::time::timeout(STEP_TIMEOUT, network_rx.recv()).await {
        Ok(Some(command)) => command,
        Ok(None) => panic!("{context}: certifier network channel closed"),
        Err(_) => panic!("{context}: certifier sent no network command within {STEP_TIMEOUT:?}"),
    }
}

/// Drain outgoing vote requests from the certifier, replying with configured responses.
///
/// For any peer not in `peer_configs`, [`VoteResponseConfig::ValidVote`] is used.
/// Returns after `fixture.num_authorities() - 1` requests have been answered.
async fn drive_vote_requests(
    network_rx: &mut mpsc::Receiver<NetworkCommand<PrimaryRequest, PrimaryResponse>>,
    fixture: &CommitteeFixture<MemDatabase>,
    proposer_id: &AuthorityIdentifier,
    header: &Header,
    peer_configs: &HashMap<AuthorityIdentifier, VoteResponseConfig>,
) {
    let num_peers = fixture.num_authorities() - 1;
    let mut handled = 0;
    loop {
        let req = next_command(
            network_rx,
            &format!("drive_vote_requests: got {handled} of {num_peers} vote requests"),
        )
        .await;
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

/// Stands in for the libp2p network under the code being tested.
///
/// Owns the receiving end of the network command channel. Every receive is bounded by
/// [`STEP_TIMEOUT`] and panics with the caller's context string, so a test that expects a request
/// the code never sends fails instead of hanging.
struct MockNetwork {
    /// Receiver of the `NetworkCommand`s sent through the handle returned by [`Self::new`].
    rx: mpsc::Receiver<NetworkCommand<PrimaryRequest, PrimaryResponse>>,
}

impl MockNetwork {
    /// A mock network and the primary network handle whose commands it receives.
    fn new() -> (Self, PrimaryNetworkHandle) {
        let (sender, rx) = mpsc::channel(100);
        let handle: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);
        (Self { rx }, handle.into())
    }

    /// Receive the next network command. See the free [`next_command`].
    async fn next_command(
        &mut self,
        context: &str,
    ) -> NetworkCommand<PrimaryRequest, PrimaryResponse> {
        next_command(&mut self.rx, context).await
    }

    /// Answer the next `count` vote requests, each with the reply `reply_for` picks for it.
    ///
    /// `reply_for` receives the peer the request is addressed to and the request. A request
    /// answered with [`Reply::Hold`] is not answered at all: its reply channel is handed back in
    /// [`Responses::held`]. Panics, naming `context`, if a vote request does not arrive within
    /// [`STEP_TIMEOUT`], if any other command arrives instead, or if the requester is gone before
    /// its reply is delivered.
    async fn respond(
        &mut self,
        count: usize,
        context: &str,
        mut reply_for: impl FnMut(&BlsPublicKey, &PrimaryRequest) -> Reply,
    ) -> Responses {
        let mut responses = Responses { held: Vec::new() };
        for ordinal in 1..=count {
            let command = self
                .next_command(&format!("{context}: waiting for vote request {ordinal} of {count}"))
                .await;
            let (peer, request, reply) = match command {
                NetworkCommand::SendRequest {
                    peer,
                    request: request @ PrimaryRequest::Vote { .. },
                    reply,
                } => (peer, request, reply),
                other => {
                    panic!("{context}: expected vote request {ordinal} of {count}, got {other:?}")
                }
            };
            let result = match reply_for(&peer, &request) {
                Reply::Vote(vote) => Ok(PrimaryResponse::Vote(vote)),
                Reply::Response(response) => Ok(response),
                Reply::Fail(error) => Err(error),
                Reply::Hold => {
                    responses.held.push(reply);
                    continue;
                }
            };
            assert!(
                reply.send(result.map(|result| NetworkResponseMessage { peer, result })).is_ok(),
                "{context}: requester dropped vote request {ordinal} of {count} before its reply"
            );
        }
        responses
    }
}

/// How a [`MockNetwork`] peer answers one vote request.
enum Reply {
    /// Answer with this vote.
    Vote(Vote),
    /// Answer with this raw response, such as [`PrimaryResponse::MissingParents`].
    Response(PrimaryResponse),
    /// Fail the request with this network error, as if the exchange with the peer failed.
    Fail(NetworkError),
    /// Leave the request unanswered and hand its reply channel back to the test.
    Hold,
}

/// What [`MockNetwork::respond`] hands back once it has seen all of its requests.
struct Responses {
    /// The reply channels of the requests answered with [`Reply::Hold`], in arrival order.
    ///
    /// The requester waits on each until the test answers it or drops it; a channel whose
    /// requester stopped waiting reports `is_closed()`.
    held: Vec<NetworkResponseSender<PrimaryResponse>>,
}

// ===== end harness =====

// ---------------------------------------------------------------------------
// T1: missing_parents_happy_path
// A single peer initially responds with MissingParents for one of the header's
// genesis parent certs. The certifier fetches the cert from the store and retries;
// on the retry the peer returns a valid vote. A certificate still forms.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread", start_paused = true)]
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
    let missing_digest = genesis_certs[0].header().digest();
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
    loop {
        let req = next_command(
            &mut cx.network_rx,
            &format!(
                "MissingParents then retry: got {handled} of {num_peers} votes \
                 (MissingParents sent: {slow_peer_first_done})"
            ),
        )
        .await;
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
// T4: transient_network_error_retries
// One peer returns a transient (non-RPC) network error on first contact, then a
// valid vote on the retry. A certificate still forms — transient errors do not
// permanently disqualify a peer.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread", start_paused = true)]
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
    loop {
        let req = next_command(
            &mut cx.network_rx,
            &format!(
                "transient error then retry: got {handled} of {num_peers} votes \
                 (transient error sent: {flaky_first_done})"
            ),
        )
        .await;
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
#[tokio::test(flavor = "current_thread", start_paused = true)]
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
        let req = next_command(&mut cx.network_rx, "phase 1: first header1 vote request").await;
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
    loop {
        let req = next_command(
            &mut cx.network_rx,
            &format!("phase 3: got {h2_handled} of {num_peers} header2 vote requests"),
        )
        .await;
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
#[tokio::test(flavor = "current_thread", start_paused = true)]
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
        tokio::time::timeout(QUIET_WINDOW, cx.network_rx.recv()).await.is_err(),
        "certifier should not send vote requests for wrong-epoch header"
    );

    // No certificate should form.
    assert!(
        tokio::time::timeout(QUIET_WINDOW, cert_rx.recv()).await.is_err(),
        "expected no certificate for wrong-epoch header"
    );
}

// ---------------------------------------------------------------------------
// T7: non_cvv_node_skips_certifier
// When Certifier::spawn is called with a ConsensusConfig whose key is not in
// the committee (authority_id() == None), it returns early. Sending a header
// produces no vote requests.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread", start_paused = true)]
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
    let result = tokio::time::timeout(QUIET_WINDOW, network_rx.recv()).await;
    assert!(
        result.map_or(true, |opt| opt.is_none()),
        "non-CVV node must not send any vote requests"
    );
}

/// `request_vote` returns exactly the result each peer reply calls for.
///
/// Every row calls `Certifier::request_vote` directly, so neither a later check (the aggregator's
/// digest and signature checks) nor a quorum that never forms can stand in for the check a row
/// targets. Each row runs as its own task and the test lists every row that failed.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn request_vote_outcomes() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let epoch = committee.epoch();
    let proposer = fixture.authorities().last().expect("committee has authorities");
    let mut others = fixture.authorities().filter(|a| a.id() != proposer.id());
    let peer = others.next().expect("committee has a non-proposer peer");
    let bystander = others.next().expect("committee has a second non-proposer peer");
    let peer_keys = peer.consensus_config().key_config().clone();

    let header = proposer.header(&committee);
    let honest_vote = Vote::new(&header, peer.id(), &peer_keys);

    // same author, round and epoch as `header`: a vote for it differs from the honest vote only in
    // its header digest
    let sibling = proposer.header_builder(&committee).created_at(1_000).build();
    assert_ne!(sibling.digest(), header.digest(), "precondition: sibling header is distinct");

    // a header one epoch ahead of the committee, so the two epoch checks can be told apart
    let next_epoch_header = HeaderBuilder::from_header(&header).epoch(epoch + 1).build();

    // a node outside the committee
    let ghost_id = AuthorityIdentifier::dummy_for_test(0xAB);
    let ghost_key = BlsKeypair::generate(&mut StdRng::from_seed([7; 32]));
    assert_eq!(committee.voting_power_by_id(&ghost_id), 0, "precondition: ghost is not a member");

    // a stored certificate that is not a parent of `header`, and a parent that is not stored
    let non_parent = fixture.certificate(&peer.header(&committee));
    let non_parent_digest = non_parent.header().digest();
    assert!(!header.parents().contains(&non_parent_digest), "precondition: not a parent");
    let store_with_non_parent = MemDatabase::default();
    store_with_non_parent.write(non_parent).expect("write non-parent certificate");
    let absent_parent = *header.parents().iter().next().expect("header has parents");

    // a request to `peer` for its vote on `header`, against an empty certificate store; the row
    // expects one vote request per reply
    let row = |name: &'static str, replies: Vec<Reply>, expected: DagResult<Vote>| VoteRequestRow {
        name,
        authority: peer.id(),
        peer_id: *peer.authority().protocol_key(),
        header: header.clone(),
        store: MemDatabase::default(),
        replies,
        cancel_after_replies: false,
        expected,
    };

    let mut rows = vec![
        row("valid vote", vec![Reply::Vote(honest_vote.clone())], Ok(honest_vote.clone())),
        row(
            "wrong header digest",
            vec![Reply::Vote(Vote::new(&sibling, peer.id(), &peer_keys))],
            Err(DagError::UnexpectedVote(sibling.digest())),
        ),
        row(
            "wrong origin",
            vec![Reply::Vote(Vote { origin: bystander.id(), ..honest_vote.clone() })],
            Err(DagError::UnexpectedVote(header.digest())),
        ),
        row(
            "wrong author",
            vec![Reply::Vote(Vote::new(
                &header,
                bystander.id(),
                bystander.consensus_config().key_config(),
            ))],
            Err(DagError::UnexpectedVote(header.digest())),
        ),
        // a non-member author trips the author clause before the voting-power check can run
        row(
            "ghost author",
            vec![Reply::Vote(Vote { author: ghost_id.clone(), ..honest_vote.clone() })],
            Err(DagError::UnexpectedVote(header.digest())),
        ),
        // the only way to reach the voting-power check is to request the vote from a non-member
        VoteRequestRow {
            authority: ghost_id.clone(),
            peer_id: *ghost_key.public(),
            ..row(
                "unknown authority",
                vec![Reply::Vote(Vote::new_with_signer(&header, ghost_id.clone(), &ghost_key))],
                Err(DagError::UnknownAuthority(ghost_id.to_string())),
            )
        },
        // the vote matches the committee epoch, so only the header-vs-vote check can reject it
        VoteRequestRow {
            header: next_epoch_header.clone(),
            ..row(
                "header epoch != vote epoch",
                vec![Reply::Vote(Vote {
                    epoch,
                    ..Vote::new(&next_epoch_header, peer.id(), &peer_keys)
                })],
                Err(DagError::InvalidEpoch { expected: epoch + 1, received: epoch }),
            )
        },
        // the vote matches the header epoch, so only the committee check can reject it
        VoteRequestRow {
            header: next_epoch_header.clone(),
            ..row(
                "vote epoch != committee epoch",
                vec![Reply::Vote(Vote::new(&next_epoch_header, peer.id(), &peer_keys))],
                Err(DagError::InvalidEpoch { expected: epoch, received: epoch + 1 }),
            )
        },
        row(
            "round mismatch",
            vec![Reply::Vote(Vote { round: header.round() + 1, ..honest_vote.clone() })],
            Err(DagError::InvalidRound { expected: header.round(), received: header.round() + 1 }),
        ),
        // the store could serve the certificate, but it is not a parent of the header
        VoteRequestRow {
            store: store_with_non_parent,
            ..row(
                "missing parents: stored non-parent",
                vec![Reply::Response(PrimaryResponse::MissingParents(vec![non_parent_digest]))],
                Err(DagError::ProposedHeaderMissingCertificates),
            )
        },
        // a real parent the store cannot serve
        row(
            "missing parents: absent parent",
            vec![Reply::Response(PrimaryResponse::MissingParents(vec![absent_parent]))],
            Err(DagError::ProposedHeaderMissingCertificates),
        ),
        // the peer rejected the request on its merits, so asking again is pointless
        row(
            "RPCError is fatal",
            vec![Reply::Fail(NetworkError::RPCError("mock permanent rejection".to_string()))],
            Err(DagError::NetworkError(format!(
                "irrecoverable error requesting vote for {header}: mock permanent rejection"
            ))),
        ),
        // the header is superseded while the peer has not answered: the call gives up on it
        VoteRequestRow {
            cancel_after_replies: true,
            ..row("canceled while held", vec![Reply::Hold], Err(DagError::Canceled))
        },
    ];
    // every other network error is transient: the request is sent again and the peer's second
    // answer decides the result
    rows.extend(
        [
            ("Timeout retries", NetworkError::Timeout),
            (
                "AckChannelClosed retries",
                NetworkError::AckChannelClosed("mock reply channel closed".to_string()),
            ),
            (
                "RPCRetryable retries",
                NetworkError::RPCRetryable("mock transient rejection".to_string()),
            ),
            ("PeerUnresolved retries", NetworkError::PeerUnresolved),
        ]
        .into_iter()
        .map(|(name, error)| {
            row(
                name,
                vec![Reply::Fail(error), Reply::Vote(honest_vote.clone())],
                Ok(honest_vote.clone()),
            )
        }),
    );

    let row_count = rows.len();
    let mut failures = Vec::new();
    for row in rows {
        let name = row.name;
        if let Err(error) = tokio::spawn(run_vote_request_row(row, committee.clone())).await {
            failures.push(row_failure(name, error));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {row_count} request_vote rows failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// One `request_vote` call in [`request_vote_outcomes`] and the exact result it must return.
struct VoteRequestRow {
    /// Names the row in every failure message.
    name: &'static str,
    /// The authority whose vote is requested.
    authority: AuthorityIdentifier,
    /// The network key the request is addressed to.
    peer_id: BlsPublicKey,
    /// The header the vote is requested for.
    header: Header,
    /// The certificate store `request_vote` reads missing parents from.
    store: MemDatabase,
    /// The peer's replies, in order, one per vote request the row expects.
    replies: Vec<Reply>,
    /// Fire the proposal's cancel notifier once every request has been seen.
    cancel_after_replies: bool,
    /// What `request_vote` must return.
    expected: DagResult<Vote>,
}

/// Run one row against its own mock network, panicking with the row's name on any mismatch.
///
/// The row expects exactly one vote request per reply. A `request_vote` that sends fewer drops its
/// network handle on return, so the mock sees its channel close while waiting for the next
/// request; one that sends more waits on a request nobody answers until [`STEP_TIMEOUT`].
async fn run_vote_request_row(row: VoteRequestRow, committee: Committee) {
    let VoteRequestRow {
        name,
        authority,
        peer_id,
        header,
        store,
        replies,
        cancel_after_replies,
        expected,
    } = row;
    let request_count = replies.len();
    let mut replies = replies.into_iter();
    let (mut network, handle) = MockNetwork::new();
    let cancel_proposal = Notifier::new();
    let call = Certifier::request_vote(
        authority,
        header,
        peer_id,
        store,
        handle,
        committee,
        cancel_proposal.subscribe(),
    );
    let (result, responses) = tokio::join!(
        async {
            tokio::time::timeout(STEP_TIMEOUT, call).await.unwrap_or_else(|_| {
                panic!(
                    "{name}: request_vote did not return within {STEP_TIMEOUT:?} (a held request, \
                     or a request after the row's last reply, goes unanswered)"
                )
            })
        },
        async {
            let responses = network
                .respond(request_count, name, |_, _| {
                    replies.next().expect("respond asks for one reply per request")
                })
                .await;
            if cancel_after_replies {
                cancel_proposal.notify();
            }
            responses
        },
    );
    assert!(same_outcome(&result, &expected), "{name}: expected {expected:?}, got {result:?}");
    assert!(
        responses.held.iter().all(NetworkResponseSender::is_closed),
        "{name}: request_vote returned but still holds the reply channel of a held request"
    );
}

/// Whether two results are the same outcome.
///
/// `DagError` has no `PartialEq`. Its derived `Debug` prints the variant and every field, so equal
/// `Debug` text means the same variant carrying the same values. `Ok` values are compared by their
/// encoded bytes: `Vote`'s `PartialEq` and `Debug` both leave out fields (the signature among
/// them), and the bytes of a `Certificate` cover its signers and aggregated signature.
fn same_outcome<T: Serialize>(got: &DagResult<T>, want: &DagResult<T>) -> bool {
    match (got, want) {
        (Ok(got), Ok(want)) => encode(got) == encode(want),
        (Err(got), Err(want)) => format!("{got:?}") == format!("{want:?}"),
        _ => false,
    }
}

/// The failure message of a table row whose task panicked.
fn row_failure(name: &str, error: tokio::task::JoinError) -> String {
    let Ok(payload) = error.try_into_panic() else {
        return format!("{name}: row task was cancelled");
    };
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|message| message.to_string()))
        .unwrap_or_else(|| format!("{name}: row panicked with a non-string payload"))
}

/// `VotesAggregator::append` returns exactly the result each vote calls for.
///
/// Every row appends its votes, in order, to a fresh aggregator and checks each result, so the
/// aggregator's own checks are pinned without a certifier or a network in the way. A row stops at
/// its first mismatch, since later results depend on the aggregator's state; the test lists every
/// row that failed.
#[test]
fn votes_aggregator_append_outcomes() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let proposer = fixture.authorities().last().expect("committee has authorities");
    let header = proposer.header(&committee);
    let mut members = fixture.authorities();
    let mut member = || members.next().expect("committee has three members");
    let (a, b, c) = (member(), member(), member());
    assert_eq!(committee.quorum_threshold(), 3, "precondition: three votes reach quorum");

    let vote = |voter: &AuthorityFixture<MemDatabase>| {
        Vote::new(&header, voter.id(), voter.consensus_config().key_config())
    };
    // the verified certificate over exactly these voters' votes
    let certificate = |voters: &[&AuthorityFixture<MemDatabase>]| {
        let votes = voters.iter().map(|voter| (voter.id(), *vote(voter).signature())).collect();
        let mut certificate = Certificate::new_unverified(&committee, header.clone(), votes)
            .expect("the voters reach quorum");
        certificate.verify_cert(&committee.bls_keys()).expect("expected certificate verifies");
        certificate
    };

    // same author, round and epoch as `header`, but a different digest
    let sibling = proposer.header_builder(&committee).created_at(1_000).build();
    assert_ne!(sibling.digest(), header.digest(), "precondition: sibling header is distinct");

    // a node outside the committee
    let ghost_id = AuthorityIdentifier::dummy_for_test(0xAB);
    let ghost_key = BlsKeypair::generate(&mut StdRng::from_seed([7; 32]));
    assert_eq!(committee.voting_power_by_id(&ghost_id), 0, "precondition: ghost is not a member");

    // a key that belongs to no member
    let wrong_key = BlsKeypair::generate(&mut StdRng::from_seed([8; 32]));

    let rows = vec![
        AppendRow {
            name: "votes below quorum, then the vote that reaches it",
            steps: vec![
                (vote(a), Ok(None)),
                (vote(b), Ok(None)),
                (vote(c), Ok(Some(certificate(&[a, b, c])))),
            ],
        },
        AppendRow {
            name: "duplicate vote",
            steps: vec![
                (vote(a), Ok(None)),
                (vote(a), Err(DagError::AuthorityReuse(a.id().to_string()))),
                // the duplicate added no weight: two distinct voters are still short of quorum
                (vote(b), Ok(None)),
                // and the certificate is the one over the three distinct votes
                (vote(c), Ok(Some(certificate(&[a, b, c])))),
            ],
        },
        AppendRow {
            name: "vote for another header",
            steps: vec![(
                Vote::new(&sibling, a.id(), a.consensus_config().key_config()),
                Err(DagError::InvalidHeaderDigest),
            )],
        },
        AppendRow {
            name: "vote from a non-member",
            steps: vec![(
                Vote::new_with_signer(&header, ghost_id.clone(), &ghost_key),
                Err(DagError::UnknownAuthority(ghost_id.to_string())),
            )],
        },
        AppendRow {
            name: "invalid signature",
            steps: vec![(
                Vote::new_with_signer(&header, a.id(), &wrong_key),
                Err(DagError::InvalidSignature),
            )],
        },
    ];

    let row_count = rows.len();
    let mut failures = Vec::new();
    for AppendRow { name, steps } in rows {
        let mut aggregator = VotesAggregator::new();
        for (ordinal, (vote, expected)) in (1..).zip(steps) {
            let result = aggregator.append(vote, &committee, &header);
            if !same_outcome(&result, &expected) {
                failures
                    .push(format!("{name}: vote {ordinal}: expected {expected:?}, got {result:?}"));
                break;
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {row_count} VotesAggregator::append rows failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// One aggregator in [`votes_aggregator_append_outcomes`] and the votes appended to it.
struct AppendRow {
    /// Names the row in every failure message.
    name: &'static str,
    /// Each vote, in append order, with the exact result `append` must return for it.
    steps: Vec<(Vote, DagResult<Option<Certificate>>)>,
}

// ---------------------------------------------------------------------------
// T15: startup_republish_highest_cert
// If the cert store already contains a certificate for this authority when the
// Certifier starts, it publishes that cert to the gossip network on startup
// (NetworkCommand::Publish) before processing any new headers.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "current_thread", start_paused = true)]
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
    let cmd = next_command(&mut cx.network_rx, "startup republish: first network command").await;

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
#[tokio::test(flavor = "current_thread", start_paused = true)]
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

    loop {
        let req = next_command(
            &mut cx.network_rx,
            &format!("quorum drain: voting weight {accumulated_weight} of quorum {quorum}"),
        )
        .await;
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
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

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn propose_header_failure() {
    let mut cx = CertifierContext::new();
    let committee = cx.fixture.committee();
    let header = cx.proposer().header(&committee);
    let proposed_digest = header.digest();
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

    // Fatal peer errors should cause proposal failure without publishing a cert.
    // The paused clock only reaches the end of the quiet window once every task is idle,
    // so the certifier has processed all vote task results before this can pass.
    if let Ok(result) = tokio::time::timeout(QUIET_WINDOW, cert_rx.recv()).await {
        panic!("expected no certificate to form; got {result:?}");
    }

    let stored = cx
        .proposer()
        .consensus_config()
        .node_storage()
        .get::<ProposedCertificates>(&proposed_digest)
        .expect("reading proposed certificates should succeed");
    assert!(
        stored.is_none(),
        "failed proposal should not persist a proposed certificate for {proposed_digest:?}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
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
    let num_peers = peer_votes.len();
    loop {
        let req = next_command(
            &mut cx.network_rx,
            &format!(
                "bad-sig votes ({committee_size} authorities, {num_byzantine} byzantine): \
                 got {} of {num_peers} vote requests",
                num_peers - peer_votes.len()
            ),
        )
        .await;
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
        assert!(tokio::time::timeout(QUIET_WINDOW, cert_rx.recv()).await.is_err());
    }
}

#[tokio::test(start_paused = true)]
async fn test_shutdown_core() {
    let cx = CertifierContext::new();
    let config = cx.proposer().consensus_config().clone();

    // send request to spawn voting sub-tasks
    cx.consensus_bus.headers().send(Header::default()).await.expect("send header for proposal");

    // on the paused clock this sleep ends only once every task is idle, so the certifier has
    // subscribed before the core is shut down
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
#[tokio::test(flavor = "current_thread", start_paused = true)]
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
    let num_peers = peer_votes.len();
    loop {
        let req = next_command(
            &mut cx.network_rx,
            &format!(
                "three bad-sig votes: got {} of {num_peers} vote requests",
                num_peers - peer_votes.len()
            ),
        )
        .await;
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
