//! Notify subscribers - useful for shutdown.

use parking_lot::Mutex;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

/// A Noticer is a future that will resolve when the Notifier it is subscribed to is notified.
/// Used for simple notification schemes (like shutdown signals).
/// Does not implement Clone on purpose- you will have race conditions with the Waker if it is
/// cloned.
#[derive(Debug)]
pub struct Noticer {
    lock: Arc<Mutex<(bool, Option<Waker>)>>,
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
    inner: Arc<Mutex<(bool, Vec<Noticer>)>>,
}

impl ShutdownNotifier {
    /// Create a new un-notified ShutdownNotifier.
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new((false, Vec::new()))) }
    }

    /// Get a Noticer that will resolve once this ShutdownNotifier is notified.
    /// A subscribe() after notify() returns a Noticer that resolves immediately.
    pub fn subscribe(&self) -> Noticer {
        let mut inner = self.inner.lock();
        let notified = inner.0;
        let lock = Arc::new(Mutex::new((notified, None)));
        let noticer = Noticer { lock: lock.clone() };
        if !notified {
            inner.1.push(Noticer { lock });
        }
        noticer
    }

    /// Resolve all the subscribed Noticers and latch the notified state so any later
    /// subscribe() resolves immediately.
    pub fn notify(&self) {
        let mut inner = self.inner.lock();
        inner.0 = true;
        let wakers: Vec<Waker> = inner
            .1
            .drain(..)
            .filter_map(|noticer| {
                let mut guard = noticer.lock.lock();
                guard.0 = true;
                guard.1.take()
            })
            .collect();
        drop(inner);
        // Wake everyone up after all flags are set to true to avoid races in noticers.
        wakers.into_iter().for_each(Waker::wake);
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
    use crate::{Notifier, ShutdownNotifier};
    use parking_lot::Mutex;
    use std::{sync::Arc, time::Duration};

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
