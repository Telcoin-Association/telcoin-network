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
    MessageId, NetworkCommand, NetworkHandle, NetworkResponseMessage, NetworkResponseSender,
};
use tn_storage::{mem_db::MemDatabase, tables::ProposedCertificates};
use tn_test_utils_committee::{AuthorityFixture, CommitteeFixture};
use tn_types::{encode, error::DagError, BlsKeypair, BlsSigner, HeaderBuilder, TnSender};
use tokio::{sync::mpsc, task::JoinHandle};

// ===== Certifier test harness =====

/// The `Certifier` of the last authority of a committee, on a [`MockNetwork`].
///
/// In the spawned mode ([`Self::from_fixture`]) the certifier runs as its own task, as in
/// production: tests send headers through the consensus bus, answer the certifier's vote requests
/// through [`Self::network`], and read the certificates it forms from the bus. In the unspawned
/// mode ([`Self::unspawned_from_fixture`]) the test gets the `Certifier` itself and calls its
/// methods directly, so it can match the exact result of one proposal.
struct CertifierContext<DB> {
    /// All authorities and their configs.
    fixture: CommitteeFixture<DB>,
    /// Per-epoch consensus bus: send headers, read certificates.
    consensus_bus: ConsensusBus,
    /// Owns the certifier's tasks for the test duration: the certifier task itself (spawned mode
    /// only), the state synchronizer, and the vote tasks of every proposal.
    task_manager: TaskManager,
    /// The network the certifier sends through. It knows the proposer's key, so
    /// [`MockNetwork::respond`] fails a test in which the proposer is asked for its own vote.
    network: MockNetwork,
}

impl CertifierContext<MemDatabase> {
    /// 4-authority context; `Certifier` spawned on the last authority.
    fn new() -> Self {
        let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
        Self::from_fixture(fixture)
    }

    /// `n`-authority context and the last authority's `Certifier`, built but not spawned.
    fn unspawned(n: usize) -> (Self, Certifier<MemDatabase>) {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(n).expect("committee size must be non-zero"))
            .randomize_ports(true)
            .build();
        Self::unspawned_from_fixture(fixture)
    }
}

impl<DB: Database> CertifierContext<DB> {
    /// Spawn the `Certifier` on the last authority of an already-built fixture (useful for custom
    /// committee params or a store filled before the certifier starts).
    fn from_fixture(fixture: CommitteeFixture<DB>) -> Self {
        let (cx, handle, state_sync) = Self::assemble(fixture);
        Certifier::spawn(
            cx.proposer().consensus_config(),
            cx.consensus_bus.clone(),
            state_sync,
            handle,
            &cx.task_manager,
        );
        cx
    }

    /// Build, without spawning, the `Certifier` of the last authority of an already-built fixture,
    /// with the fields `Certifier::spawn` gives it.
    ///
    /// Nothing runs until the test calls a method on the returned certifier, so the startup
    /// republish of `Certifier::spawn` never happens. Vote tasks are spawned on
    /// [`Self::task_manager`], and every proposal on the returned certifier or its clones shares
    /// one `new_proposal` notifier and one proposal lock, as the proposals of a running certifier
    /// do.
    fn unspawned_from_fixture(fixture: CommitteeFixture<DB>) -> (Self, Certifier<DB>) {
        let (cx, network, state_sync) = Self::assemble(fixture);
        let config = cx.proposer().consensus_config();
        let certifier = Certifier {
            authority_id: config.authority_id().expect("the proposer is a committee member"),
            committee: config.committee().clone(),
            certificate_store: config.node_storage().clone(),
            state_sync,
            signature_service: config.key_config().clone(),
            network,
            task_spawner: cx.task_manager.get_spawner(),
            new_proposal: Notifier::new(),
            proposal_lock: Arc::new(Mutex::new(())),
            metrics: cx.consensus_bus.app().metrics().clone(),
            config,
        };
        (cx, certifier)
    }

    /// The context both modes share, and the network handle and running state synchronizer the
    /// certifier is built with.
    fn assemble(
        fixture: CommitteeFixture<DB>,
    ) -> (Self, PrimaryNetworkHandle, StateSynchronizer<DB>) {
        let consensus_bus = ConsensusBus::new();
        let task_manager = TaskManager::default();

        let primary = fixture.authorities().last().expect("committee has authorities");
        let (network, handle) = MockNetwork::for_node(*primary.authority().protocol_key());
        let state_sync = StateSynchronizer::new(
            primary.consensus_config(),
            consensus_bus.clone(),
            task_manager.get_spawner(),
        );
        state_sync.spawn(&task_manager);

        (Self { fixture, consensus_bus, task_manager, network }, handle, state_sync)
    }

    /// The authority whose `Certifier` is under test (last in the fixture).
    fn proposer(&self) -> &AuthorityFixture<DB> {
        self.fixture.authorities().last().expect("committee has authorities")
    }

    /// Every authority except the proposer, in fixture order.
    fn peers(&self) -> impl Iterator<Item = &AuthorityFixture<DB>> {
        let proposer = self.proposer().id();
        self.fixture.authorities().filter(move |authority| authority.id() != proposer)
    }

    /// Each peer's valid vote for `header`, keyed by the network key its vote request is sent to.
    fn peer_votes(&self, header: &Header) -> HashMap<BlsPublicKey, Vote> {
        self.peers().map(|peer| (*peer.authority().protocol_key(), peer.vote(header))).collect()
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

/// Upper bound on every wait for the next network command.
///
/// Every test runs on tokio's paused clock, so this is virtual time. The clock auto-advances only
/// while every task is idle: the wait costs no wall-clock time and expires only once the code under
/// test genuinely has nothing left to send. It must exceed the certifier's 10s vote-retry backoff
/// ceiling; otherwise a wait could tie with a backoff tick and fail a test that is only waiting for
/// a retry.
const STEP_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a test watches for something that must NOT happen, such as a certificate forming.
///
/// Virtual time on the paused clock, like [`STEP_TIMEOUT`]. It is longer than the certifier's 10s
/// vote-retry ceiling, so any retry that was going to fire has fired before the window closes.
const QUIET_WINDOW: Duration = Duration::from_secs(30);

/// Stands in for the libp2p network under the code being tested.
///
/// Owns the receiving end of the network command channel. Every receive is bounded by
/// [`STEP_TIMEOUT`] (or [`QUIET_WINDOW`], when nothing may arrive) and panics with the caller's
/// context string, so a test that expects a request the code never sends fails instead of hanging.
struct MockNetwork {
    /// Receiver of the `NetworkCommand`s sent through the handle returned with this network.
    rx: mpsc::Receiver<NetworkCommand<PrimaryRequest, PrimaryResponse>>,
    /// The network key of the node that sends through this network, if known. That node must never
    /// be asked for its own vote.
    node: Option<BlsPublicKey>,
}

impl MockNetwork {
    /// A mock network and the primary network handle whose commands it receives.
    fn new() -> (Self, PrimaryNetworkHandle) {
        let (sender, rx) = mpsc::channel(100);
        let handle: NetworkHandle<PrimaryRequest, PrimaryResponse> = NetworkHandle::new(sender);
        (Self { rx, node: None }, handle.into())
    }

    /// Like [`Self::new`], for the network of `node`: [`Self::respond`] panics if a vote request is
    /// addressed to `node` itself.
    fn for_node(node: BlsPublicKey) -> (Self, PrimaryNetworkHandle) {
        let (network, handle) = Self::new();
        (Self { node: Some(node), ..network }, handle)
    }

    /// Receive the next network command, panicking if none arrives within [`STEP_TIMEOUT`].
    ///
    /// The code under test holds the channel's sender, so a bare `recv()` never returns `None`
    /// while it runs: code that sends fewer commands than a test expects would hang the test
    /// instead of failing it. `context` names what the caller was waiting for.
    async fn next_command(
        &mut self,
        context: &str,
    ) -> NetworkCommand<PrimaryRequest, PrimaryResponse> {
        match tokio::time::timeout(STEP_TIMEOUT, self.rx.recv()).await {
            Ok(Some(command)) => command,
            Ok(None) => panic!("{context}: network channel closed"),
            Err(_) => panic!("{context}: no network command within {STEP_TIMEOUT:?}"),
        }
    }

    /// Receive the next vote request: the peer it is addressed to, the request, and its reply
    /// channel.
    ///
    /// Gossip publishes that arrive first are acknowledged and skipped. The certifier waits for
    /// each publish to be acknowledged, and at startup it republishes its highest certificate
    /// before it reads any header, so an unanswered publish would stall it. Panics, naming
    /// `context`, on any other command.
    async fn next_vote_request(
        &mut self,
        context: &str,
    ) -> (BlsPublicKey, PrimaryRequest, NetworkResponseSender<PrimaryResponse>) {
        loop {
            match self.next_command(context).await {
                NetworkCommand::SendRequest {
                    peer,
                    request: request @ PrimaryRequest::Vote { .. },
                    reply,
                } => return (peer, request, reply),
                NetworkCommand::Publish { reply, .. } => {
                    // the certifier only logs a failed publish, so a publisher that stopped
                    // waiting changes nothing
                    let _ = reply.send(Ok(MessageId::new(&[])));
                }
                other => panic!("{context}: expected a vote request, got {other:?}"),
            }
        }
    }

    /// Receive the next network command, which must be a gossip publish, acknowledge it, and
    /// return the bytes it publishes.
    async fn next_publish(&mut self, context: &str) -> Vec<u8> {
        match self.next_command(context).await {
            NetworkCommand::Publish { msg, reply, .. } => {
                // the certifier only logs a failed publish, so a publisher that stopped waiting
                // changes nothing
                let _ = reply.send(Ok(MessageId::new(&[])));
                msg
            }
            other => panic!("{context}: expected a gossip publish, got {other:?}"),
        }
    }

    /// Panic, naming `context`, unless the network channel is closed and nothing was sent on it.
    ///
    /// The channel closes once every handle to this network has been dropped. Unlike
    /// [`Self::assert_quiet`], an open channel fails the check even if nothing is sent on it: it
    /// means something, such as a running certifier task, still holds a handle.
    async fn assert_closed(&mut self, context: &str) {
        match tokio::time::timeout(STEP_TIMEOUT, self.rx.recv()).await {
            Ok(None) => {}
            Ok(Some(command)) => {
                panic!("{context}: expected a closed network channel, got {command:?}")
            }
            Err(_) => panic!(
                "{context}: network channel still open after {STEP_TIMEOUT:?}; something holds a \
                 handle to it"
            ),
        }
    }

    /// Panic, naming `context`, if any network command arrives within [`QUIET_WINDOW`].
    ///
    /// A closed channel counts as quiet: nothing can arrive on it.
    async fn assert_quiet(&mut self, context: &str) {
        if let Ok(Some(command)) = tokio::time::timeout(QUIET_WINDOW, self.rx.recv()).await {
            panic!(
                "{context}: expected no network command within {QUIET_WINDOW:?}, got {command:?}"
            );
        }
    }

    /// Answer the next `count` vote requests, each with the reply `reply_for` picks for it.
    ///
    /// `reply_for` receives the peer the request is addressed to and the request. A request
    /// answered with [`Reply::Hold`] is not answered at all: its reply channel is handed back in
    /// [`Responses::held`]. Every request is logged in [`Responses::requests`]. Gossip publishes in
    /// between are acknowledged and skipped (see [`Self::next_vote_request`]). Panics, naming
    /// `context`, if a vote request does not arrive within [`STEP_TIMEOUT`], if any other command
    /// arrives instead, if a request is addressed to this network's own node, or if the requester
    /// is gone before its reply is delivered.
    async fn respond(
        &mut self,
        count: usize,
        context: &str,
        mut reply_for: impl FnMut(&BlsPublicKey, &PrimaryRequest) -> Reply,
    ) -> Responses {
        let mut responses = Responses { held: Vec::new(), requests: Vec::new() };
        for ordinal in 1..=count {
            let (peer, request, reply) = self
                .next_vote_request(&format!(
                    "{context}: waiting for vote request {ordinal} of {count}"
                ))
                .await;
            assert!(
                self.node != Some(peer),
                "{context}: vote request {ordinal} of {count} asks the proposer for its own vote"
            );
            let PrimaryRequest::Vote { header, parents } = &request else {
                unreachable!("next_vote_request returns only vote requests");
            };
            responses.requests.push(LoggedRequest {
                peer,
                header: header.digest(),
                parents: parents.iter().map(|parent| parent.header().digest()).collect(),
            });
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
    /// Every vote request seen, in arrival order, however it was answered.
    requests: Vec<LoggedRequest>,
}

impl Responses {
    /// How many of the logged requests were addressed to each peer.
    fn requests_per_peer(&self) -> HashMap<BlsPublicKey, usize> {
        let mut counts = HashMap::new();
        for request in &self.requests {
            *counts.entry(request.peer).or_default() += 1;
        }
        counts
    }
}

/// One vote request seen by [`MockNetwork::respond`].
struct LoggedRequest {
    /// The peer the request is addressed to.
    peer: BlsPublicKey,
    /// The digest of the header the request asks a vote for.
    header: HeaderDigest,
    /// The digests of the parent certificates the request carries, in request order.
    parents: Vec<HeaderDigest>,
}

/// Run `certifier.propose_header(header)` as its own task, so the test can answer the vote
/// requests it sends. Collect the result with [`proposal_result`].
fn start_proposal<DB: Database>(
    certifier: &Certifier<DB>,
    header: Header,
) -> JoinHandle<DagResult<Certificate>> {
    let certifier = certifier.clone();
    tokio::spawn(async move { certifier.propose_header(header).await })
}

/// The result of a proposal started with [`start_proposal`].
///
/// Panics, naming `context`, if the proposal does not return within [`STEP_TIMEOUT`] (it is still
/// waiting on a vote no peer will send) or if its task panicked.
async fn proposal_result(
    proposal: JoinHandle<DagResult<Certificate>>,
    context: &str,
) -> DagResult<Certificate> {
    match tokio::time::timeout(STEP_TIMEOUT, proposal).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => panic!("{context}: proposal task failed: {error}"),
        Err(_) => panic!("{context}: propose_header did not return within {STEP_TIMEOUT:?}"),
    }
}

/// The verified certificate for `header` over exactly the votes of `voters`: what
/// `VotesAggregator` forms when those votes, and no others, reach quorum.
fn certificate_over<'a, DB: Database>(
    committee: &Committee,
    header: &Header,
    voters: impl IntoIterator<Item = &'a AuthorityFixture<DB>>,
) -> Certificate {
    let votes =
        voters.into_iter().map(|voter| (voter.id(), *voter.vote(header).signature())).collect();
    let mut certificate = Certificate::new_unverified(committee, header.clone(), votes)
        .expect("the voters reach quorum");
    certificate.verify_cert(&committee.bls_keys()).expect("expected certificate verifies");
    certificate
}

/// The bytes a certifier gossips for `certificate`: what
/// `PrimaryNetworkHandle::publish_certificate` publishes, captured on a scratch [`MockNetwork`].
async fn certificate_gossip(certificate: Certificate) -> Vec<u8> {
    let (mut network, handle) = MockNetwork::new();
    let (published, msg) = tokio::join!(
        handle.publish_certificate(certificate),
        network.next_publish("gossip of the expected certificate")
    );
    published.expect("the scratch network acknowledges the publish");
    msg
}

// ===== end harness =====

/// A peer that answers `MissingParents` is asked again with exactly the parent certificate it
/// named, and its vote on the retry counts toward the certificate.
///
/// The header is a round-2 header whose parents are the round-1 certificates of every authority,
/// stored where the proposer reads the parents a peer asks for. One other peer's request is held
/// unanswered, so quorum needs the retried vote.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn missing_parents_happy_path() {
    let (mut cx, certifier) = CertifierContext::unspawned(4);
    let committee = cx.fixture.committee();

    let round1: Vec<Certificate> = cx
        .fixture
        .authorities()
        .map(|authority| {
            cx.fixture.certificate(&authority.header_builder_at_round(&committee, 1).build())
        })
        .collect();
    let store = cx.proposer().consensus_config().node_storage().clone();
    for certificate in &round1 {
        store.write(certificate.clone()).expect("write round-1 certificate");
    }
    let header = cx
        .proposer()
        .header_builder_at_round(&committee, 2)
        .parents(round1.iter().map(|certificate| certificate.header().digest()).collect())
        .build();
    // the proposer's own round-1 certificate, which the slow peer has not seen
    let missing = round1.last().expect("committee has authorities").header().digest();

    let votes = cx.peer_votes(&header);
    let (slow, held, voter) = {
        let mut peers = cx.peers();
        let mut peer = || peers.next().expect("committee has three peers");
        (peer(), peer(), peer())
    };
    let expected = Ok(certificate_over(&committee, &header, [cx.proposer(), slow, voter]));
    let (slow, held) = (*slow.authority().protocol_key(), *held.authority().protocol_key());

    let proposal = start_proposal(&certifier, header);
    // the slow peer's vote comes on its second request: one request more than there are peers
    let mut asked_for_parents = false;
    let responses = cx
        .network
        .respond(votes.len() + 1, "the slow peer names a missing parent", |peer, _| {
            if *peer == held {
                Reply::Hold
            } else if *peer == slow && !asked_for_parents {
                asked_for_parents = true;
                Reply::Response(PrimaryResponse::MissingParents(vec![missing]))
            } else {
                Reply::Vote(votes[peer].clone())
            }
        })
        .await;
    let result = proposal_result(proposal, "the slow peer names a missing parent").await;

    let slow_requests: Vec<&[HeaderDigest]> = responses
        .requests
        .iter()
        .filter(|request| request.peer == slow)
        .map(|request| request.parents.as_slice())
        .collect();
    assert_eq!(
        slow_requests,
        [&[][..], &[missing][..]],
        "the slow peer's first request must carry no parents, and its retry exactly the parent it \
         named"
    );
    assert!(
        same_outcome(&result, &expected),
        "expected {expected:?} over the proposer's, the slow peer's and the voter's votes, got \
         {result:?}"
    );
}

/// A peer whose first request fails with a retryable network error is asked again, and its vote
/// on the retry counts toward the certificate.
///
/// One other peer's request is held unanswered, so quorum needs the retried vote.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn transient_network_error_retries() {
    let (mut cx, certifier) = CertifierContext::unspawned(4);
    let committee = cx.fixture.committee();
    let header = cx.proposer_header();
    let votes = cx.peer_votes(&header);

    let (flaky, held, voter) = {
        let mut peers = cx.peers();
        let mut peer = || peers.next().expect("committee has three peers");
        (peer(), peer(), peer())
    };
    let expected = Ok(certificate_over(&committee, &header, [cx.proposer(), flaky, voter]));
    let [flaky, held, voter] = [flaky, held, voter].map(|peer| *peer.authority().protocol_key());

    let proposal = start_proposal(&certifier, header);
    // the flaky peer's vote comes on its second request: one request more than there are peers
    let mut failed_once = false;
    let responses = cx
        .network
        .respond(
            votes.len() + 1,
            "the flaky peer's first request fails, so it must be asked again",
            |peer, _| {
                if *peer == held {
                    Reply::Hold
                } else if *peer == flaky && !failed_once {
                    failed_once = true;
                    Reply::Fail(NetworkError::Timeout)
                } else {
                    Reply::Vote(votes[peer].clone())
                }
            },
        )
        .await;
    let result = proposal_result(proposal, "the flaky peer fails once").await;

    assert_eq!(
        responses.requests_per_peer(),
        HashMap::from([(flaky, 2), (held, 1), (voter, 1)]),
        "the flaky peer must be asked exactly twice, every other peer once"
    );
    assert!(
        same_outcome(&result, &expected),
        "expected {expected:?} over the proposer's, the flaky peer's and the voter's votes, got \
         {result:?}"
    );
}

/// A new header cancels the proposal in flight: every vote request for the old header is dropped,
/// the new header is certified, and the old one never is.
///
/// Header 1's requests are held unanswered, so its vote tasks stay parked on the network until
/// something cancels them. Receiving header 2 fires `new_proposal`, which ends header 1's proposal
/// (releasing `proposal_lock`, so header 2's proposal can start) and each of header 1's vote tasks
/// (dropping its pending request, which closes the reply channel the test holds). Holding rather
/// than dropping the requests matters: a dropped request fails with a retryable error, so a vote
/// task that is never cancelled would retry forever instead of leaving its channel open.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn new_header_cancels_inflight() {
    let mut cx = CertifierContext::new();

    let committee = cx.fixture.committee();
    // distinct creation times give the two headers distinct digests
    let header1 = cx.proposer().header_builder(&committee).created_at(1000).build();
    let header2 = cx.proposer().header_builder(&committee).created_at(1001).build();
    let h2_digest = header2.digest();
    assert_ne!(header1.digest(), h2_digest, "precondition: two distinct headers");
    let votes = cx.peer_votes(&header2);
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(header1).await.unwrap();
    let mut header1_requests =
        cx.network.respond(votes.len(), "header 1: every request held", |_, _| Reply::Hold).await;

    cx.consensus_bus.headers().send(header2).await.unwrap();
    let all_cancelled = async {
        for reply in &mut header1_requests.held {
            reply.closed().await;
        }
    };
    if tokio::time::timeout(STEP_TIMEOUT, all_cancelled).await.is_err() {
        let open = header1_requests.held.iter().filter(|reply| !reply.is_closed()).count();
        panic!(
            "header 2 did not cancel header 1's vote tasks: {open} of {} header-1 requests still \
             pending after {STEP_TIMEOUT:?}",
            header1_requests.held.len()
        );
    }

    let header2_requests = cx
        .network
        .respond(votes.len(), "header 2: every peer votes", |peer, _| {
            Reply::Vote(votes[peer].clone())
        })
        .await;
    assert!(
        header2_requests.requests.iter().all(|request| request.header == h2_digest),
        "every vote request after header 2 must be for header 2"
    );

    let cert = tokio::time::timeout(STEP_TIMEOUT, cert_rx.recv())
        .await
        .expect("header 2 certified")
        .expect("cert_rx channel open");
    assert_eq!(cert.header().digest(), h2_digest, "the certificate must be for header 2");
    if let Ok(result) = tokio::time::timeout(QUIET_WINDOW, cert_rx.recv()).await {
        panic!("expected no certificate after header 2's, got {result:?}");
    }
}

/// `propose_header` rejects a header from another epoch with exactly `InvalidEpoch`, naming the
/// committee's epoch and the header's, before it sends anything on the network.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn wrong_epoch_header_rejected() {
    let (mut cx, certifier) = CertifierContext::unspawned(4);
    let epoch = cx.fixture.committee().epoch();
    let wrong_epoch = epoch + 1;
    let header = HeaderBuilder::from_header(&cx.proposer_header()).epoch(wrong_epoch).build();

    let proposal = start_proposal(&certifier, header);
    cx.network.assert_quiet("wrong-epoch header: propose_header must send nothing").await;
    let result = proposal_result(proposal, "wrong-epoch header").await;
    let expected = Err(DagError::InvalidEpoch { expected: epoch, received: wrong_epoch });
    assert!(
        same_outcome(&result, &expected),
        "wrong-epoch header: expected {expected:?}, got {result:?}"
    );
}

/// A rejected wrong-epoch header does not stall the running certifier: the next valid header is
/// certified, and no certificate ever forms for the rejected one.
///
/// The valid header is sent only once the certifier has gone idle after the wrong-epoch one, so
/// the certifier has already processed and rejected it.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn wrong_epoch_header_does_not_stall_certifier() {
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let wrong_epoch = cx.fixture.committee().epoch() + 1;
    let wrong_epoch_header = HeaderBuilder::from_header(&header).epoch(wrong_epoch).build();
    let digest = header.digest();
    let votes = cx.peer_votes(&header);
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(wrong_epoch_header).await.unwrap();
    cx.network.assert_quiet("wrong-epoch header: the certifier must send nothing").await;

    cx.consensus_bus.headers().send(header).await.unwrap();
    let responses = cx
        .network
        .respond(votes.len(), "valid header after the wrong-epoch one", |peer, _| {
            Reply::Vote(votes[peer].clone())
        })
        .await;
    assert!(
        responses.requests.iter().all(|request| request.header == digest),
        "every vote request must be for the valid header"
    );

    let certificate = tokio::time::timeout(STEP_TIMEOUT, cert_rx.recv())
        .await
        .expect("the valid header after a wrong-epoch one is certified")
        .expect("certificate channel open");
    assert_eq!(certificate.header().digest(), digest, "the certificate is for the valid header");
    if let Ok(result) = tokio::time::timeout(QUIET_WINDOW, cert_rx.recv()).await {
        panic!("expected no certificate after the valid header's, got {result:?}");
    }
}

/// `Certifier::spawn` on a node whose key is not in the committee (`authority_id()` is `None`)
/// returns without spawning a certifier task.
///
/// `spawn` owns the network handle it is given, so when it returns early it drops the handle and
/// the network channel closes. A certifier task, even an idle one, would keep the handle and the
/// channel open.
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

    let (mut network, handle) = MockNetwork::new();
    let cb = ConsensusBus::new();
    let task_manager = TaskManager::default();
    let sync =
        StateSynchronizer::new(non_cvv_config.clone(), cb.clone(), task_manager.get_spawner());
    sync.spawn(&task_manager);

    Certifier::spawn(non_cvv_config, cb, sync, handle, &task_manager);

    network.assert_closed("non-CVV node: Certifier::spawn must not spawn a certifier task").await;
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

/// A certifier that starts with its own certificates in the store first gossips exactly the
/// highest-round one, before it does anything else.
///
/// The store holds the proposer's round-1 and round-2 certificates, written highest round first,
/// so neither the lowest-round nor the last-written certificate is the right one.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn startup_republish_highest_cert() {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let committee = fixture.committee();
    let proposer = fixture.authorities().last().expect("committee has authorities");
    let [round1, round2] = [1, 2].map(|round| {
        fixture.certificate(&proposer.header_builder_at_round(&committee, round).build())
    });
    let store = proposer.consensus_config().node_storage().clone();
    for certificate in [&round2, &round1] {
        store.write(certificate.clone()).expect("write an own certificate before startup");
    }

    let mut cx = CertifierContext::from_fixture(fixture);

    let published = cx.network.next_publish("startup republish: first network command").await;
    assert!(
        published == certificate_gossip(round2).await,
        "startup must republish the proposer's round-2 certificate, its highest; published the \
         round-1 one instead: {}",
        published == certificate_gossip(round1).await
    );
}

/// Exactly `committee.quorum_threshold()` voting-weight worth of peers vote. The remaining peers
/// are silent (never reply). A certificate must form — this validates the threshold boundary
/// without hardcoding a number. See Issue #646: always use committee.quorum_threshold(), never
/// hardcode.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn minimum_quorum_exactly_threshold() {
    // 4-node committee: quorum is 3 out of 4 (f=1, 2f+1=3).
    let mut cx = CertifierContext::new();
    let header = cx.proposer_header();
    let votes = cx.peer_votes(&header);
    let committee = cx.fixture.committee();
    let mut cert_rx = cx.subscribe_new_certificates();

    // The proposer already self-voted (weight 1) in propose_header.
    // Accumulate peer votes until the committee quorum threshold is reached.
    let quorum = committee.quorum_threshold();
    let mut accumulated_weight = committee.voting_power_by_id(&cx.proposer().id());

    cx.consensus_bus.headers().send(header.clone()).await.unwrap();

    // a peer whose vote would push the weight past quorum stays silent, and so does every peer once
    // quorum is reached; their requests are held unanswered until the test ends
    let _silent = cx
        .network
        .respond(votes.len(), "quorum drain", |peer, _| {
            let vote = votes[peer].clone();
            let peer_weight = committee.voting_power_by_id(vote.author());
            if accumulated_weight + peer_weight > quorum {
                return Reply::Hold;
            }
            accumulated_weight += peer_weight;
            Reply::Vote(vote)
        })
        .await;

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

/// The running certifier turns a header into exactly the certificate its quorum's votes support,
/// records it in `ProposedCertificates` under the header digest, and gossips it.
///
/// Every vote request is answered. The first peers asked, in fixture order, vote until they and
/// the proposer are a quorum; the other requests are held until the certificate is gossiped and
/// then answered with late votes, which must change nothing. Gossip is the next network command
/// after the vote requests, so a request the proposer sends to itself fails the test wherever it
/// arrives.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn propose_header_to_form_certificate() {
    let mut cx = CertifierContext::new();
    let committee = cx.fixture.committee();
    let header = cx.proposer_header();
    let digest = header.digest();
    let votes = cx.peer_votes(&header);
    let quorum = committee.quorum_threshold();
    // every authority has one vote, so the proposer's own vote needs `quorum - 1` peers' votes
    let (expected, voters) = {
        let voters: Vec<_> = cx.peers().take(quorum as usize - 1).collect();
        let expected =
            certificate_over(&committee, &header, voters.iter().copied().chain([cx.proposer()]));
        let voters: Vec<_> = voters.iter().map(|voter| *voter.authority().protocol_key()).collect();
        (expected, voters)
    };
    let store = cx.proposer().consensus_config().node_storage().clone();
    let mut cert_rx = cx.subscribe_new_certificates();

    cx.consensus_bus.headers().send(header).await.unwrap();
    let responses = cx
        .network
        .respond(votes.len(), "every peer is asked", |peer, _| {
            if voters.contains(peer) {
                Reply::Vote(votes[peer].clone())
            } else {
                Reply::Hold
            }
        })
        .await;

    let certificate = tokio::time::timeout(STEP_TIMEOUT, cert_rx.recv())
        .await
        .expect("a certificate forms from a quorum of votes")
        .expect("certificate channel open");
    assert!(
        encode(&certificate) == encode(&expected),
        "expected the certificate over the proposer's and its voters' votes {expected:?}, got \
         {certificate:?}"
    );
    certificate.clone().verify_cert(&committee.bls_keys()).expect("the certificate verifies");
    assert_eq!(certificate.signed_authorities().len(), quorum, "a quorum signs the certificate");

    let proposed = store
        .get::<ProposedCertificates>(&digest)
        .expect("read ProposedCertificates")
        .expect("ProposedCertificates holds the certificate under the header digest");
    assert!(
        encode(&proposed) == encode(&expected),
        "ProposedCertificates holds {proposed:?} for the header, not its certificate"
    );

    let gossip = cx.network.next_publish("gossip of the new certificate").await;
    assert!(
        gossip == certificate_gossip(expected).await,
        "the gossip after certification must carry exactly the new certificate"
    );

    // the held requests, in arrival order, are those of the peers that did not vote
    let late_voters =
        responses.requests.iter().map(|request| request.peer).filter(|peer| !voters.contains(peer));
    for (reply, peer) in responses.held.into_iter().zip(late_voters) {
        let vote = PrimaryResponse::Vote(votes[&peer].clone());
        assert!(
            reply.send(Ok(NetworkResponseMessage { peer, result: vote })).is_ok(),
            "a held vote request stopped waiting before its late vote"
        );
    }
    cx.network.assert_quiet("late votes after the certificate is gossiped").await;
}

/// `propose_header` returns exactly `CouldNotFormCertificate` for the header when every peer fails
/// its vote request with the fatal `NetworkError::RPCError`, and asks each peer exactly once.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn propose_header_failure() {
    let (mut cx, certifier) = CertifierContext::unspawned(4);
    let header = cx.proposer_header();
    let peers: HashMap<_, _> =
        cx.peers().map(|peer| (*peer.authority().protocol_key(), 1)).collect();

    let proposal = start_proposal(&certifier, header.clone());
    let responses = cx
        .network
        .respond(peers.len(), "every peer fails fatally", |_, _| {
            Reply::Fail(NetworkError::RPCError("mock fatal peer error".to_string()))
        })
        .await;
    let result = proposal_result(proposal, "every peer fails fatally").await;

    let expected = Err(DagError::CouldNotFormCertificate(header.digest()));
    assert!(same_outcome(&result, &expected), "expected {expected:?}, got {result:?}");
    assert_eq!(responses.requests_per_peer(), peers, "each peer is asked exactly once");
    cx.network.assert_quiet("a fatal error is not retried").await;
}

/// `propose_header` returns exactly the certificate the valid votes support when some peers sign
/// their votes badly.
///
/// Every row calls `Certifier::propose_header` directly, on a committee of the row's size, and
/// every peer answers. The bad signers are the first peers asked, so their votes reach the
/// aggregator before any valid one. Each row sits at the quorum boundary. Where the proposer and
/// the valid voters are exactly a quorum, the result must be the certificate over exactly their
/// votes: it needs every valid vote and carries no bad one. Where they are one short, the call must
/// return `CouldNotFormCertificate` once every vote is in. Each row runs as its own task and the
/// test lists every row that failed.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn propose_header_bad_signature_outcomes() {
    let row = |name, committee_size, bad_signers, forgery, certifies| BadSignatureRow {
        name,
        committee_size,
        bad_signers,
        forgery,
        certifies,
    };
    let rows = [
        row("4 authorities, 1 foreign-key signature", 4, 1, Forgery::ForeignKey, true),
        row("4 authorities, 2 foreign-key signatures", 4, 2, Forgery::ForeignKey, false),
        row("6 authorities, 1 foreign-key signature", 6, 1, Forgery::ForeignKey, true),
        row("6 authorities, 2 foreign-key signatures", 6, 2, Forgery::ForeignKey, false),
        row("10 authorities, 3 wrong-message signatures", 10, 3, Forgery::WrongMessage, true),
        row("10 authorities, 4 wrong-message signatures", 10, 4, Forgery::WrongMessage, false),
    ];

    let row_count = rows.len();
    let mut failures = Vec::new();
    for row in rows {
        let name = row.name;
        if let Err(error) = tokio::spawn(run_bad_signature_row(row)).await {
            failures.push(row_failure(name, error));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {row_count} bad-signature rows failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// One committee in [`propose_header_bad_signature_outcomes`] and the proposal it must certify or
/// fail.
struct BadSignatureRow {
    /// Names the row in every failure message.
    name: &'static str,
    /// Number of authorities, the proposer among them.
    committee_size: usize,
    /// Number of peers whose vote carries a bad signature.
    bad_signers: usize,
    /// How the bad signatures are made.
    forgery: Forgery,
    /// Whether the proposer and the valid voters are exactly a quorum (else one short of it).
    certifies: bool,
}

/// How a bad signer's vote signature is made. The vote is otherwise the signer's honest vote.
enum Forgery {
    /// The vote for the header, signed with a key that belongs to no member.
    ForeignKey,
    /// The signer's own key, over a message other than the header digest.
    WrongMessage,
}

/// Run one row on its own unspawned certifier, panicking with the row's name on any mismatch.
async fn run_bad_signature_row(row: BadSignatureRow) {
    let BadSignatureRow { name, committee_size, bad_signers, forgery, certifies } = row;
    let (mut cx, certifier) = CertifierContext::unspawned(committee_size);
    let committee = cx.fixture.committee();
    let quorum = committee.quorum_threshold();
    let valid_voters = (committee_size - bad_signers) as u64;
    assert_eq!(
        valid_voters,
        if certifies { quorum } else { quorum - 1 },
        "{name}: precondition: the row must sit at the quorum boundary"
    );

    let header = cx.proposer_header();
    let honest = cx.peer_votes(&header);
    let foreign_key = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
    let forged: HashMap<BlsPublicKey, Vote> = cx
        .peers()
        .map(|peer| {
            let vote = match forgery {
                Forgery::ForeignKey => Vote::new_with_signer(&header, peer.id(), &foreign_key),
                Forgery::WrongMessage => Vote {
                    signature: peer
                        .consensus_config()
                        .key_config()
                        .request_signature_direct(&[0_u8, 0_u8]),
                    ..peer.vote(&header)
                },
            };
            (*peer.authority().protocol_key(), vote)
        })
        .collect();

    let proposal = start_proposal(&certifier, header.clone());
    let mut bad = Vec::new();
    cx.network
        .respond(honest.len(), name, |peer, _| {
            if bad.len() < bad_signers {
                bad.push(*peer);
                Reply::Vote(forged[peer].clone())
            } else {
                Reply::Vote(honest[peer].clone())
            }
        })
        .await;
    let result = proposal_result(proposal, name).await;

    let expected = if certifies {
        // the proposer and every peer that signed validly
        let valid = cx
            .fixture
            .authorities()
            .filter(|authority| !bad.contains(authority.authority().protocol_key()));
        Ok(certificate_over(&committee, &header, valid))
    } else {
        Err(DagError::CouldNotFormCertificate(header.digest()))
    };
    let signers = |outcome: &DagResult<Certificate>| {
        outcome
            .as_ref()
            .map(|certificate| certificate.signed_authorities_with_committee(&committee))
            .unwrap_or_default()
    };
    let got = signers(&result);
    assert!(
        same_outcome(&result, &expected),
        "{name}: expected {expected:?} with {} signers, none bad; got {result:?} with {} signers, \
         {} of them bad",
        signers(&expected).len(),
        got.len(),
        got.iter().filter(|signer| bad.contains(signer)).count()
    );
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
