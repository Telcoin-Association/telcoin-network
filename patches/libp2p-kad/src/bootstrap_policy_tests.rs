//! Deterministic tests for periodic and automatic bootstrap suspension.

use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Count wakeups when an idle bootstrap scheduler is re-enabled.
#[derive(Default)]
struct WakeCount(AtomicUsize);

impl std::task::Wake for WakeCount {
    /// Record a request to poll the scheduler again.
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Automatic work queued before closure and contacts arriving during closure cannot bootstrap.
#[test]
fn closed_automatic_bootstrap_discards_pending_work_and_wakes_on_recovery() {
    let mut status = Status::new(None, Some(Duration::ZERO));
    status.trigger();
    status.set_enabled(false);
    status.trigger();
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(status.poll_next_bootstrap(&mut cx).is_pending());
    status.set_enabled(true);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert!(status.poll_next_bootstrap(&mut cx).is_pending());
    status.trigger();
    assert!(status.poll_next_bootstrap(&mut cx).is_ready());
}

/// Even a ready periodic timer cannot bootstrap in Closed, and recovery keeps its configuration.
#[test]
fn closed_periodic_bootstrap_stays_suspended_after_timer_ready() -> Result<(), &'static str> {
    let mut status = Status::new(Some(Duration::ZERO), None);
    status.set_enabled(false);
    let delay = &mut status
        .interval_and_delay
        .as_mut()
        .ok_or("missing periodic timer")?
        .1;
    futures::executor::block_on(futures::future::poll_fn(|cx| delay.poll_unpin(cx)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(status.poll_next_bootstrap(&mut cx).is_pending());
    status.set_enabled(true);
    futures::executor::block_on(futures::future::poll_fn(|cx| {
        status.poll_next_bootstrap(cx)
    }));
    Ok(())
}
