//! Tests of QUIC Retry on the listener and of the per-poll outcome cap.
//!
//! quinn gives a validated dialer a NEW_TOKEN token, and a later dial with that token skips
//! Retry. So every dial below uses a fresh dialer transport with a fresh key.

use crate::quic_incoming::QuicIncomingLimits;
use futures::{future::poll_fn, task::ArcWake, StreamExt as _};
use libp2p::{
    core::{
        transport::{DialOpts, ListenerId, PortUse, Transport as _},
        Endpoint,
    },
    identity::Keypair,
    quic::{tokio::Transport as QuicTransport, Config as QuicTransportConfig, IncomingStats},
    Multiaddr,
};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::Context,
    time::Duration,
};
use tn_config::NetworkConfig;
use tn_test_utils::wait_until;

/// Upper bound on the time one test waits to converge.
const WAIT: Duration = Duration::from_secs(20);

/// Default `max_priority_peers` of the node.
const PEERS: usize = 45;

/// Established connections allowed per peer by the node.
const CONNECTIONS_PER_PEER: u32 = 8;

/// Per-poll outcome cap of the scheduling test.
const CAP: u64 = 1;

/// Concurrent unvalidated attempts in the scheduling test (more than `CAP`).
const ATTEMPTS: u64 = 4;

/// Node limits with the default peer and connection values.
fn node_limits() -> QuicIncomingLimits {
    QuicIncomingLimits::new(PEERS, CONNECTIONS_PER_PEER)
}

/// The Retry switch of the node's default network config, so that a test that uses it also
/// covers the shipped default.
fn node_retry_switch() -> bool {
    NetworkConfig::default().quic_config().retry_unvalidated_incoming
}

/// A listener config with the node limits, the Retry switch and fresh shared counters.
fn listener_config(
    keypair: &Keypair,
    retry: bool,
    limits: QuicIncomingLimits,
) -> (QuicTransportConfig, Arc<IncomingStats>) {
    let stats = Arc::new(IncomingStats::default());
    let mut config = QuicTransportConfig::new(keypair);
    limits.apply(&mut config, retry, Arc::clone(&stats));
    (config, stats)
}

/// Listen on a loopback port and give back the transport and its listen address.
async fn listen(config: QuicTransportConfig) -> eyre::Result<(QuicTransport, Multiaddr)> {
    let mut transport = QuicTransport::new(config);
    transport.listen_on(ListenerId::next(), "/ip4/127.0.0.1/udp/0/quic-v1".parse()?)?;
    let event = poll_fn(|cx| Pin::new(&mut transport).poll(cx)).await;
    event
        .into_new_address()
        .map(|addr| (transport, addr))
        .ok_or_else(|| eyre::eyre!("first listener event is not a new address"))
}

/// Drive a listener in the background and complete every inbound handshake.
fn serve(mut transport: QuicTransport) -> tokio::task::JoinHandle<()> {
    let events = futures::stream::poll_fn(move |cx| Pin::new(&mut transport).poll(cx).map(Some));
    tokio::spawn(events.for_each(|event| {
        let _handshake = event.into_incoming().map(|(upgrade, _addr)| {
            tokio::spawn(async move {
                let _connection = upgrade.await;
            })
        });
        futures::future::ready(())
    }))
}

/// Dial `addr` from a fresh transport with a fresh key (no NEW_TOKEN token).
async fn dial_fresh(addr: Multiaddr) -> eyre::Result<()> {
    let keypair = Keypair::generate_ed25519();
    let mut dialer = QuicTransport::new(QuicTransportConfig::new(&keypair));
    let opts = DialOpts { role: Endpoint::Dialer, port_use: PortUse::New };
    let dial = dialer.dial(addr, opts)?;
    tokio::time::timeout(WAIT, dial).await?.map(|_| ()).map_err(eyre::Report::from)
}

/// Wait until the listener counts `n` accepted attempts.
async fn wait_accepted(stats: &Arc<IncomingStats>, n: u64) -> eyre::Result<()> {
    wait_until(WAIT, "listener accepts the dial", || {
        let stats = Arc::clone(stats);
        async move { Ok(stats.accepted() >= n) }
    })
    .await
}

/// The queue bounds derived from the default node limits have the documented values.
#[test]
fn queue_bounds_follow_node_limits() {
    assert_eq!(node_limits().queue_bounds(), (720, 5888, 4_239_360));
}

/// With the shipped Retry switch, a fresh dial gets a Retry and then one accept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_applies_to_unvalidated_dial() -> eyre::Result<()> {
    let (config, stats) =
        listener_config(&Keypair::generate_ed25519(), node_retry_switch(), node_limits());
    let (listener, addr) = listen(config).await?;
    let server = serve(listener);
    dial_fresh(addr).await?;
    wait_accepted(&stats, 1).await?;
    assert!(stats.retried() >= 1, "the unvalidated attempt gets a Retry");
    assert_eq!(stats.accepted(), 1);
    assert_eq!(stats.refused() + stats.ignored(), 0);
    server.abort();
    Ok(())
}

/// With the Retry switch off, the listener accepts a fresh dial and sends no Retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_off_accepts_without_retry() -> eyre::Result<()> {
    let (config, stats) = listener_config(&Keypair::generate_ed25519(), false, node_limits());
    let (listener, addr) = listen(config).await?;
    let server = serve(listener);
    dial_fresh(addr).await?;
    wait_accepted(&stats, 1).await?;
    assert_eq!(stats.retried(), 0, "with the switch off the listener sends no Retry");
    assert_eq!(stats.accepted(), 1);
    server.abort();
    Ok(())
}

/// Two Retry-on listeners dial each other from their listen endpoints (port reuse): each
/// dial completes through the Retry of the other listener, and each listener counts at least
/// one Retry and one accept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patched_peers_interoperate_in_both_directions() -> eyre::Result<()> {
    let (config_a, stats_a) = listener_config(&Keypair::generate_ed25519(), true, node_limits());
    let (config_b, stats_b) = listener_config(&Keypair::generate_ed25519(), true, node_limits());
    let (mut transport_a, addr_a) = listen(config_a).await?;
    let (mut transport_b, addr_b) = listen(config_b).await?;
    let dial_a_to_b =
        transport_a.dial(addr_b, DialOpts { role: Endpoint::Dialer, port_use: PortUse::Reuse })?;
    let dial_b_to_a =
        transport_b.dial(addr_a, DialOpts { role: Endpoint::Dialer, port_use: PortUse::Reuse })?;
    let server_a = serve(transport_a);
    let server_b = serve(transport_b);
    let (a_to_b, b_to_a) =
        tokio::time::timeout(WAIT, futures::future::join(dial_a_to_b, dial_b_to_a)).await?;
    let _connections = (a_to_b?, b_to_a?);
    wait_until(WAIT, "both listeners retry and accept the peer dial", || {
        let both = [Arc::clone(&stats_a), Arc::clone(&stats_b)];
        async move { Ok(both.iter().all(|stats| stats.retried() >= 1 && stats.accepted() >= 1)) }
    })
    .await?;
    assert_eq!(stats_a.refused() + stats_a.ignored(), 0, "listener a refuses nothing");
    assert_eq!(stats_b.refused() + stats_b.ignored(), 0, "listener b refuses nothing");
    server_a.abort();
    server_b.abort();
    Ok(())
}

/// A Retry token past its lifetime does not validate the address: the dial fails after the
/// Retry and the listener accepts nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_retry_token_gets_no_handshake() -> eyre::Result<()> {
    let (mut config, stats) = listener_config(&Keypair::generate_ed25519(), true, node_limits());
    // quinn encodes the issue time in whole UNIX seconds and rejects a token once
    // `issued + lifetime` is in the past, so a zero lifetime expires every Retry token.
    config.retry_token_lifetime = Some(Duration::ZERO);
    let (listener, addr) = listen(config).await?;
    let server = serve(listener);
    assert!(dial_fresh(addr).await.is_err(), "an expired Retry token must not validate");
    wait_until(WAIT, "listener counts the Retry", || {
        let stats = Arc::clone(&stats);
        async move { Ok(stats.retried() >= 1) }
    })
    .await?;
    assert_eq!(stats.accepted(), 0);
    server.abort();
    Ok(())
}

/// A waker that counts its wakes.
#[derive(Default)]
struct CountingWaker {
    /// Number of wakes.
    wakes: AtomicU64,
}

impl ArcWake for CountingWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

/// Retry, Refuse and Ignore outcomes so far.
fn outcomes(stats: &IncomingStats) -> u64 {
    stats.retried() + stats.refused() + stats.ignored()
}

/// Observations of the scheduling test.
#[derive(Default)]
struct Probe {
    /// Polls that broke the cap or hit it without a yield and a wake.
    violations: AtomicU64,
    /// Polls that handled exactly `CAP` outcomes.
    cap_hits: AtomicU64,
}

/// Poll the listener transport once with a counting waker, check the cap, and tell whether
/// the poll produced an event.
fn checked_poll(transport: &mut QuicTransport, stats: &IncomingStats, probe: &Probe) -> bool {
    let counter = Arc::new(CountingWaker::default());
    let waker = futures::task::waker(Arc::clone(&counter));
    let mut cx = Context::from_waker(&waker);
    let before = outcomes(stats);
    let yields_before = stats.budget_yields();
    let poll = Pin::new(&mut *transport).poll(&mut cx);
    let delta = outcomes(stats) - before;
    let cap_hit = delta == CAP;
    let yielded = poll.is_pending()
        && counter.wakes.load(Ordering::SeqCst) >= 1
        && stats.budget_yields() == yields_before + 1;
    let ok = delta <= CAP && (!cap_hit || yielded);
    probe.violations.fetch_add(u64::from(!ok), Ordering::SeqCst);
    probe.cap_hits.fetch_add(u64::from(cap_hit), Ordering::SeqCst);
    poll.is_ready()
}

/// With a per-poll cap of `CAP`, no listener poll handles more than `CAP` Retry outcomes,
/// and every poll that reaches the cap yields and wakes its task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_outcomes_yield_after_per_poll_cap() -> eyre::Result<()> {
    let limits = node_limits().with_outcomes_per_poll(usize::try_from(CAP)?);
    let (config, stats) = listener_config(&Keypair::generate_ed25519(), true, limits);
    let (listener, addr) = listen(config).await?;
    let transport = Mutex::new(listener);
    let probe = Probe::default();
    let dialers: Vec<_> = (0..ATTEMPTS).map(|_| tokio::spawn(dial_fresh(addr.clone()))).collect();
    wait_until(WAIT, "listener retries every attempt", || {
        let drained = transport
            .lock()
            .map(|mut guard| {
                std::iter::repeat_with(|| checked_poll(&mut guard, &stats, &probe))
                    .take(1024)
                    .take_while(|ready| *ready)
                    .count()
            })
            .map_err(|_| eyre::eyre!("listener lock poisoned"));
        let done = stats.retried() >= ATTEMPTS;
        async move { drained.map(|_| done) }
    })
    .await?;
    assert_eq!(probe.violations.load(Ordering::SeqCst), 0, "every capped poll yields and wakes");
    assert!(probe.cap_hits.load(Ordering::SeqCst) >= ATTEMPTS);
    assert!(stats.budget_yields() >= ATTEMPTS);
    dialers.iter().for_each(|dialer| dialer.abort());
    Ok(())
}
