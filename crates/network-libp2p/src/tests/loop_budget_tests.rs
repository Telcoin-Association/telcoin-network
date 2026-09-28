//! Regression tests for GHSA-v6gc-qm2v-q2f4: the network loop must hand control back to the tokio
//! scheduler while swarm events stay continuously ready.
//!
//! The swarm stream spends no tokio coop budget (libp2p events, quinn accepts and futures channels
//! are not budget-aware), so a flood of ready swarm events alone never makes the loop yield. These
//! tests drive the production [`next_loop_event`] with an always-ready event source and check that
//! sibling tasks, the command channel and the intervals keep making progress.

use super::{next_loop_event, LoopEvent};
use futures::{stream::FusedStream, Stream, StreamExt as _};
use std::{
    future::Future as _,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{interval_at, Instant, Interval, MissedTickBehavior},
};

/// Swarm events processed before the harness may stop, far past one coop budget.
const FLOOD_EVENTS: u64 = 100_000;
/// Hard cap on the flood while the harness waits for both intervals to fire.
const FLOOD_CAP: u64 = 2_000_000;
/// The tokio coop budget: the most budget units a task spends in one scheduler poll.
const COOP_BUDGET: u64 = 128;
/// Period of the record-refresh analog and the in-stream (heartbeat and sweep) analog.
const TICK: Duration = Duration::from_millis(1);
/// Capacity of the command channel, as in production.
const COMMAND_CAPACITY: usize = 100;

/// An interval that first fires one period from now and skips missed ticks, like the production
/// record refresh.
fn ticker() -> Interval {
    let mut interval = interval_at(Instant::now() + TICK, TICK);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval
}

/// An event source that is always ready and never spends coop budget, like a swarm flooded with
/// QUIC accepts. Each poll also polls its own interval, like the peer-manager heartbeat and the
/// stream sweep inside the swarm poll.
#[derive(Debug)]
struct FloodSource {
    /// The in-stream interval.
    ticker: Interval,
    /// How many times the in-stream interval fired.
    ticks: u64,
}

impl Stream for FloodSource {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<()>> {
        let fired = self.ticker.poll_tick(cx).is_ready();
        self.ticks += u64::from(fired);
        Poll::Ready(Some(()))
    }
}

impl FusedStream for FloodSource {
    fn is_terminated(&self) -> bool {
        false
    }
}

/// Progress counters observed when the flood stops.
#[derive(Debug, Default, Clone, Copy)]
struct Progress {
    /// Swarm events returned by [`next_loop_event`].
    events: u64,
    /// Commands returned by [`next_loop_event`].
    commands: u64,
    /// Record-refresh ticks returned by [`next_loop_event`].
    refresh_ticks: u64,
    /// Ticks of the interval polled inside the event source.
    stream_ticks: u64,
    /// Scheduler polls of the task that runs the loop.
    loop_polls: u64,
    /// Polls of a probe task spawned from the loop task (multi-thread: the LIFO slot).
    local_probe: u64,
    /// Polls of a probe task spawned from the test body.
    remote_probe: u64,
}

/// State of the loop harness.
#[derive(Debug)]
struct Harness {
    /// The record-refresh analog.
    refresh: Interval,
    /// The always-ready swarm analog.
    source: FloodSource,
    /// The command channel receiver.
    commands: mpsc::Receiver<u64>,
    /// Keeps the command channel open when no producer runs.
    _sender: Option<mpsc::Sender<u64>>,
    /// Counters so far.
    progress: Progress,
    /// Poll counter of the loop task.
    loop_polls: Arc<AtomicU64>,
    /// Poll counter of the local probe.
    local_probe: Arc<AtomicU64>,
    /// Poll counter of the remote probe.
    remote_probe: Arc<AtomicU64>,
}

impl Harness {
    /// Continue until [`FLOOD_EVENTS`] events are processed and both intervals fired, or until
    /// [`FLOOD_CAP`] events.
    fn running(&self) -> bool {
        let timers_fired = self.progress.refresh_ticks > 0 && self.source.ticks > 0;
        let events = self.progress.events;
        events < FLOOD_EVENTS || (!timers_fired && events < FLOOD_CAP)
    }

    /// The counters, including the shared ones, at this instant.
    fn snapshot(&self) -> Progress {
        Progress {
            stream_ticks: self.source.ticks,
            loop_polls: self.loop_polls.load(Ordering::Relaxed),
            local_probe: self.local_probe.load(Ordering::Relaxed),
            remote_probe: self.remote_probe.load(Ordering::Relaxed),
            ..self.progress
        }
    }
}

/// Spawn a task that counts its own polls and yields after each one.
fn spawn_probe(polls: Arc<AtomicU64>) -> JoinHandle<()> {
    tokio::spawn(futures::stream::repeat(()).for_each(move |()| {
        polls.fetch_add(1, Ordering::Relaxed);
        tokio::task::yield_now()
    }))
}

/// Spawn a task that sends commands until the receiver is gone.
fn spawn_producer(sender: mpsc::Sender<u64>) -> JoinHandle<()> {
    tokio::spawn(
        futures::stream::iter(0u64..)
            .then(move |n| {
                let sender = sender.clone();
                async move { sender.send(n).await.is_ok() }
            })
            .take_while(|sent| futures::future::ready(*sent))
            .for_each(|_| futures::future::ready(())),
    )
}

/// Drive [`next_loop_event`] the way [`super::ConsensusNetwork::run`] does until the harness
/// stops, returning the last snapshot.
async fn drive(harness: Harness) -> Progress {
    futures::stream::unfold(harness, |mut h| async move {
        h.running().then_some(())?;
        match next_loop_event(&mut h.refresh, &mut h.source, &mut h.commands).await {
            LoopEvent::Refresh => h.progress.refresh_ticks += 1,
            LoopEvent::Swarm(()) => h.progress.events += 1,
            LoopEvent::Command(_) => h.progress.commands += 1,
            LoopEvent::CommandsClosed => {}
        }
        let snapshot = h.snapshot();
        Some((snapshot, h))
    })
    .fold(Progress::default(), |_, snapshot| futures::future::ready(snapshot))
    .await
}

/// Run the flood on the current runtime, with or without a command producer.
async fn flood(with_producer: bool, label: &str) -> Progress {
    let (sender, commands) = mpsc::channel(COMMAND_CAPACITY);
    let remote_polls = Arc::new(AtomicU64::new(0));
    let remote = spawn_probe(remote_polls.clone());
    let producer = with_producer.then(|| spawn_producer(sender.clone()));
    let harness = Harness {
        refresh: ticker(),
        source: FloodSource { ticker: ticker(), ticks: 0 },
        commands,
        _sender: (!with_producer).then_some(sender),
        progress: Progress::default(),
        loop_polls: Arc::new(AtomicU64::new(0)),
        local_probe: Arc::new(AtomicU64::new(0)),
        remote_probe: remote_polls,
    };
    let loop_task = tokio::spawn(async move {
        let local = spawn_probe(harness.local_probe.clone());
        let loop_polls = harness.loop_polls.clone();
        let mut driven = Box::pin(drive(harness));
        let progress = std::future::poll_fn(move |cx| {
            loop_polls.fetch_add(1, Ordering::Relaxed);
            driven.as_mut().poll(cx)
        })
        .await;
        local.abort();
        progress
    });
    let progress = loop_task.await.expect("loop task completes");
    remote.abort();
    producer.iter().for_each(JoinHandle::abort);
    eprintln!("{label}: {progress:?}");
    progress
}

/// Assert that the loop yielded at least once per coop budget and that every other participant
/// made progress during the flood.
fn assert_progress(progress: Progress) {
    assert!(progress.events >= FLOOD_EVENTS, "flood too short: {progress:?}");
    assert!(
        progress.loop_polls * COOP_BUDGET >= progress.events,
        "loop served more than one coop budget of events per poll: {progress:?}"
    );
    assert!(progress.local_probe > 0, "task spawned by the loop starved: {progress:?}");
    assert!(progress.remote_probe > 0, "sibling task starved: {progress:?}");
    assert!(progress.refresh_ticks > 0, "record refresh never fired: {progress:?}");
    assert!(progress.stream_ticks > 0, "in-stream interval never fired: {progress:?}");
}

/// On a `current_thread` runtime with a command producer, the flooded loop yields once per coop
/// budget, so the producer, both probes and both intervals make progress.
#[tokio::test(flavor = "current_thread")]
async fn test_flood_yields_current_thread_with_commands() {
    let progress = flood(true, "current_thread+commands").await;
    assert_progress(progress);
    assert!(
        usize::try_from(progress.commands).is_ok_and(|served| served > COMMAND_CAPACITY),
        "commands starved: {progress:?}"
    );
}

/// On a `current_thread` runtime with no command traffic (no budget-aware arm is ready), the
/// flooded loop still yields once per coop budget.
#[tokio::test(flavor = "current_thread")]
async fn test_flood_yields_current_thread_idle_commands() {
    assert_progress(flood(false, "current_thread+idle").await);
}

/// On a `multi_thread` runtime with a command producer, the flooded loop yields once per coop
/// budget, not only when received commands happen to spend the budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_flood_yields_multi_thread_with_commands() {
    let progress = flood(true, "multi_thread+commands").await;
    assert_progress(progress);
    assert!(
        usize::try_from(progress.commands).is_ok_and(|served| served > COMMAND_CAPACITY),
        "commands starved: {progress:?}"
    );
}

/// On a `multi_thread` runtime with no command traffic, the flooded loop yields once per coop
/// budget, not only when interval ticks happen to spend the budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_flood_yields_multi_thread_idle_commands() {
    assert_progress(flood(false, "multi_thread+idle").await);
}
