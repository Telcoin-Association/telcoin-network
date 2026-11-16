//! Notify subscribers - useful for shutdown.

use parking_lot::Mutex;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll, Waker},
};

/// A Noticer's state: whether it was noticed, and the waker of a poll that is waiting.
type NoticerState = Mutex<(bool, Option<Waker>)>;

/// A Noticer is a future that will resolve when the Notifier it is subscribed to is notified.
/// Used for simple notification schemes (like shutdown signals).
/// Does not implement Clone on purpose- you will have race conditions with the Waker if it is
/// cloned.
#[derive(Debug)]
pub struct Noticer {
    lock: Arc<NoticerState>,
}

/// Simple notifier.
///
/// Will hand out a future on a subscribe() call that will resolve after
/// notify() if called.  Can manage any number of "subscribers".
/// Once notify() if called the resolved subscribers will be cleared.
#[derive(Clone, Debug)]
pub struct Notifier {
    noticers: Arc<Mutex<Vec<Noticer>>>,
}

impl Notifier {
    /// Create a new empty Notifier.
    pub fn new() -> Self {
        Self { noticers: Arc::new(Mutex::new(Vec::new())) }
    }

    /// Get a Noticer that will resolve once this Notifier is notified.
    pub fn subscribe(&self) -> Noticer {
        let lock = Arc::new(Mutex::new((false, None)));
        let noticer = Noticer { lock: lock.clone() };
        self.noticers.lock().push(Noticer { lock });
        noticer
    }

    /// Resolve all the subscribed Noticers.
    pub fn notify(&self) {
        let mut wakers = vec![];
        let mut noticers = self.noticers.lock();
        for n in noticers.drain(..) {
            let mut guard = n.lock.lock();
            if let Some(wake) = guard.1.take() {
                wakers.push(wake);
            }
            guard.0 = true;
        }
        // Wake everyone up after all flags set to true to avoid races in noticiers.
        for wake in wakers {
            wake.wake();
        }
    }
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}

/// One-shot variant of [`Notifier`] for signals that fire at most once, like shutdown or
/// cancellation.
///
/// The notified state is sticky: once notify() is called, every later subscribe() returns a
/// [`Noticer`] that resolves immediately. This closes the orphan path where a subscriber that
/// arrives after the signal would wait forever. Use [`Notifier`] instead when the signal
/// re-arms after each notify() (like the certifier's per-proposal cancel).
#[derive(Clone, Debug)]
pub struct ShutdownNotifier {
    /// The sticky notified flag and the Noticers subscribed before notify().
    inner: Arc<Mutex<ShutdownState>>,
}

/// [`ShutdownNotifier::subscribe`] prunes dropped subscribers once the list holds at least this
/// many entries.
pub(crate) const MIN_PRUNE_LEN: usize = 64;

/// The state a [`ShutdownNotifier`] shares with its clones.
#[derive(Debug)]
struct ShutdownState {
    /// Set by the first notify() and never cleared.
    notified: bool,
    /// The Noticers subscribed before notify(), held weakly. A long-lived notifier, like the one
    /// in a node's task manager, sees a subscribe() for every task it spawns. Held strongly, each
    /// finished task's Noticer and stored waker would stay here until shutdown. Held weakly, a
    /// dropped Noticer frees its waker at once, and subscribe() prunes its dead entry later.
    noticers: Vec<Weak<NoticerState>>,
    /// subscribe() prunes the dead entries from `noticers` once it reaches this length.
    prune_at: usize,
}

impl ShutdownNotifier {
    /// Create a new un-notified ShutdownNotifier.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ShutdownState {
                notified: false,
                noticers: Vec::new(),
                prune_at: MIN_PRUNE_LEN,
            })),
        }
    }

    /// Get a Noticer that will resolve once this ShutdownNotifier is notified.
    /// A subscribe() after notify() returns a Noticer that resolves immediately.
    pub fn subscribe(&self) -> Noticer {
        let mut state = self.inner.lock();
        let lock = Arc::new(Mutex::new((state.notified, None)));
        if !state.notified {
            if state.noticers.len() >= state.prune_at {
                // An entry with no strong reference left belongs to a dropped Noticer. It can
                // never be upgraded again, so nothing waits on it and notify() would skip it.
                state.noticers.retain(|noticer| noticer.strong_count() > 0);
                // Prune next when the list has doubled from what survived, or reaches
                // MIN_PRUNE_LEN. Each prune is then paid for by the subscribes since the last
                // one (O(1) per subscribe on average), and the list never holds more than
                // MIN_PRUNE_LEN or twice the Noticers live at the last prune.
                state.prune_at = MIN_PRUNE_LEN.max(2 * state.noticers.len());
            }
            state.noticers.push(Arc::downgrade(&lock));
        }
        Noticer { lock }
    }

    /// Resolve all the subscribed Noticers and latch the notified state so any later
    /// subscribe() resolves immediately.
    pub fn notify(&self) {
        let mut state = self.inner.lock();
        state.notified = true;
        let wakers: Vec<Waker> = state
            .noticers
            .drain(..)
            // An entry that does not upgrade belongs to a dropped Noticer: nothing to wake.
            .filter_map(|noticer| noticer.upgrade())
            .filter_map(|noticer| {
                let mut guard = noticer.lock();
                guard.0 = true;
                guard.1.take()
            })
            .collect();
        drop(state);
        // Wake everyone up after all flags are set to true to avoid races in noticers.
        wakers.into_iter().for_each(Waker::wake);
    }

    /// Return true once notify() has been called.
    ///
    /// Reads the sticky flag without subscribing, so a frequent check costs one lock and no
    /// allocation.
    pub fn is_notified(&self) -> bool {
        self.inner.lock().notified
    }

    /// The number of entries in the subscriber list, dead ones not yet pruned included.
    #[cfg(test)]
    pub(crate) fn registered_len(&self) -> usize {
        self.inner.lock().noticers.len()
    }

    /// The number of entries in the subscriber list whose Noticer is still alive.
    #[cfg(test)]
    pub(crate) fn live_noticers(&self) -> usize {
        self.inner.lock().noticers.iter().filter(|noticer| noticer.strong_count() > 0).count()
    }
}

impl Default for ShutdownNotifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Noticer {
    /// Return true of this Noticer has been noticed.
    /// I.e. the future has or will resolve.
    pub fn noticed(&self) -> bool {
        let guard = self.lock.lock();
        guard.0
    }

    fn poll_int(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut guard = self.lock.lock();
        if guard.0 {
            guard.1 = None;
            Poll::Ready(())
        } else {
            guard.1 = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Future for Noticer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.poll_int(cx)
    }
}

impl Future for &Noticer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.poll_int(cx)
    }
}

#[cfg(test)]
mod test {
    use super::MIN_PRUNE_LEN;
    use crate::{Noticer, Notifier, ShutdownNotifier};
    use futures::task::ArcWake;
    use parking_lot::Mutex;
    use std::{
        future::Future as _,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Barrier,
        },
        task::Context,
        time::Duration,
    };

    /// A waker that counts its wakes. Its Arc's strong count also shows whether anything still
    /// holds a clone of it.
    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl WakeCount {
        fn wakes(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl ArcWake for WakeCount {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Poll `noticer` once with a waker that counts into `count`. Return true if it was ready.
    fn poll_once(noticer: &mut Noticer, count: &Arc<WakeCount>) -> bool {
        let waker = futures::task::waker(count.clone());
        let mut cx = Context::from_waker(&waker);
        Pin::new(noticer).poll(&mut cx).is_ready()
    }

    /// Regression: the notifier used to keep every Noticer, with the waker of whatever last
    /// polled it, until notify(). A long-lived notifier (a node's task manager subscribes once
    /// per spawned task) grew with every task it ever ran. A dropped Noticer must free its waker
    /// at once, and the list must stay bounded however many Noticers came and went.
    #[test]
    fn test_shutdown_notifier_releases_dropped_noticers() {
        let notifier = ShutdownNotifier::new();
        let count = Arc::new(WakeCount::default());
        for _ in 0..10_000 {
            let mut noticer = notifier.subscribe();
            assert!(!poll_once(&mut noticer, &count));
            assert!(Arc::strong_count(&count) > 1, "the pending noticer holds the waker");
            drop(noticer);
            assert_eq!(Arc::strong_count(&count), 1, "the dropped noticer freed its waker");
            assert!(notifier.registered_len() <= MIN_PRUNE_LEN, "dead entries are pruned");
        }
        assert_eq!(notifier.live_noticers(), 0);
        notifier.notify();
        assert_eq!(count.wakes(), 0, "a dropped noticer is not woken");
    }

    /// Pruning removes only dropped Noticers: notify() still wakes each live one parked among
    /// thousands of dropped ones, and the list stays within twice the live count.
    #[test]
    fn test_shutdown_notifier_prune_keeps_live_noticers() {
        const LIVE: usize = 200;
        const CHURN: usize = 50;
        let notifier = ShutdownNotifier::new();
        let count = Arc::new(WakeCount::default());
        let mut live = Vec::with_capacity(LIVE);
        for i in 0..LIVE * CHURN {
            let mut noticer = notifier.subscribe();
            assert!(!poll_once(&mut noticer, &count));
            if i % CHURN == 0 {
                live.push(noticer);
            }
            assert!(notifier.registered_len() <= MIN_PRUNE_LEN.max(2 * LIVE));
        }
        assert_eq!(notifier.live_noticers(), LIVE);
        notifier.notify();
        assert_eq!(count.wakes(), LIVE, "notify woke each live noticer once");
        assert_eq!(notifier.registered_len(), 0);
        for mut noticer in live {
            assert!(noticer.noticed());
            assert!(poll_once(&mut noticer, &count));
        }
    }

    /// Threads subscribe, poll and drop Noticers while notify() runs. Every Noticer that was
    /// pending when notify() ran is woken exactly once, every subscribe after notify() resolves
    /// at once, and pruning dropped Noticers concurrently never costs a live one its wake.
    #[test]
    fn test_shutdown_notifier_no_lost_wake_under_races() {
        const THREADS: usize = 4;
        const PER_THREAD: usize = 20_000;
        for round in 0..5 {
            let notifier = ShutdownNotifier::new();
            let start = Arc::new(Barrier::new(THREADS + 1));
            let progress = Arc::new(AtomicUsize::new(0));
            let threads: Vec<_> = (0..THREADS)
                .map(|_| {
                    let notifier = notifier.clone();
                    let start = start.clone();
                    let progress = progress.clone();
                    std::thread::spawn(move || {
                        start.wait();
                        let mut kept = Vec::new();
                        for i in 0..PER_THREAD {
                            let mut noticer = notifier.subscribe();
                            let count = Arc::new(WakeCount::default());
                            let ready = poll_once(&mut noticer, &count);
                            // Keep one in three; drop the rest so subscribe() prunes as it goes.
                            if i % 3 == 0 {
                                kept.push((noticer, count, ready));
                            }
                            progress.fetch_add(1, Ordering::Relaxed);
                        }
                        kept
                    })
                })
                .collect();
            start.wait();
            // Notify midway, while the threads are still subscribing, polling and dropping.
            while progress.load(Ordering::Relaxed) < THREADS * PER_THREAD / 2 {
                std::hint::spin_loop();
            }
            notifier.notify();
            let mut parked = 0;
            for thread in threads {
                for (noticer, count, ready) in thread.join().expect("thread ran") {
                    assert!(noticer.noticed(), "round {round}: a live noticer missed notify");
                    let expected = usize::from(!ready);
                    assert_eq!(count.wakes(), expected, "round {round}: wakes for ready={ready}");
                    parked += expected;
                }
            }
            assert!(parked > 0, "round {round}: notify ran before any noticer parked");
            assert_eq!(notifier.registered_len(), 0, "nothing registers after notify");
            let mut late = notifier.subscribe();
            assert!(late.noticed());
            assert!(poll_once(&mut late, &Arc::new(WakeCount::default())));
        }
    }

    #[tokio::test]
    async fn test_shutdown_notifier_late_subscribe_resolves() {
        let notifier = ShutdownNotifier::new();
        let early = notifier.subscribe();
        let (early_done_tx, early_done_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            early.await;
            let _ = early_done_tx.send(());
        });
        // Let the spawned task park on the noticer so notify() must wake it
        // (covers the waker path, not just the sticky flag).
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        notifier.notify();
        tokio::time::timeout(Duration::from_secs(1), early_done_rx)
            .await
            .expect("notify wakes the parked noticer")
            .expect("early subscriber task completes");
        let late = notifier.subscribe();
        assert!(late.noticed());
        tokio::time::timeout(Duration::from_secs(1), late)
            .await
            .expect("noticer subscribed after notify resolves");
    }

    #[tokio::test]
    async fn test_notifier_rearms_after_notify() {
        // Pin the re-arm behavior the certifier depends on: a subscribe after a notify
        // starts un-noticed and resolves on the next notify.
        let notifier = Notifier::new();
        let first = notifier.subscribe();
        notifier.notify();
        tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .expect("first subscription resolves on first notify");
        let second = notifier.subscribe();
        assert!(!second.noticed());
        notifier.notify();
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("second subscription resolves on second notify");
    }

    #[tokio::test]
    async fn test_notifier() {
        let b1 = Arc::new(Mutex::new(false));
        let b1_clone = b1.clone();
        let b2 = Arc::new(Mutex::new(false));
        let b2_clone = b2.clone();
        let b3 = Arc::new(Mutex::new(false));
        let b3_clone = b3.clone();
        let notifier = Notifier::new();
        let n1 = notifier.subscribe();
        let n2 = notifier.subscribe();
        let n3 = notifier.subscribe();
        tokio::spawn(async move {
            n1.await;
            *b1_clone.lock() = true;
        });
        tokio::spawn(async move {
            n2.await;
            *b2_clone.lock() = true;
        });
        tokio::spawn(async move {
            n3.await;
            *b3_clone.lock() = true;
        });
        assert!(!(*b1.lock()));
        assert!(!(*b2.lock()));
        assert!(!(*b3.lock()));
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!(*b1.lock()));
        assert!(!(*b2.lock()));
        assert!(!(*b3.lock()));
        notifier.notify();
        // Make sure the background tasks get a chance to run.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(*b1.lock());
        assert!(*b2.lock());
        assert!(*b3.lock());
    }
}
