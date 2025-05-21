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
#[derive(Clone, Debug)]
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
        let noticer = Noticer { lock: Arc::new(Mutex::new((false, None))) };
        self.noticers.lock().push(noticer.clone());
        noticer
    }

    /// Resolve all the subscribed Noticers.
    pub fn notify(&self) {
        let mut noticers = self.noticers.lock();
        for n in noticers.iter_mut() {
            let mut guard = n.lock.lock();
            let wake = guard.1.take();
            guard.0 = true;
            if let Some(wake) = wake {
                wake.wake();
            }
        }
        // Done with these, they can all resolve now.
        noticers.clear();
    }
}

impl Default for Notifier {
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
    use crate::Notifier;
    use parking_lot::Mutex;
    use std::{sync::Arc, time::Duration};

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
