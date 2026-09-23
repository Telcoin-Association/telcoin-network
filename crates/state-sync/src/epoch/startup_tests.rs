//! Deterministic startup sync scheduling tests using Tokio's paused clock.

use super::{
    drive_epoch_record_sync, STARTUP_PEER_POLL_INTERVAL, STARTUP_SYNC_PASS_TIMEOUT,
    STARTUP_SYNC_TIMEOUT,
};
use std::{
    cell::{Cell, RefCell},
    future::{pending, ready},
    time::Duration,
};
use tokio::time::Instant;

/// A repair can move backward; the initial cursor and an unequal pass never prove quiescence.
#[tokio::test(start_paused = true)]
async fn startup_sync_requires_equal_consecutive_passes() {
    let mut results = [4, 3, 5, 5].into_iter();
    let requested = RefCell::new(Vec::new());
    drive_epoch_record_sync(
        || ready(1),
        |epoch| {
            requested.borrow_mut().push(epoch);
            ready(results.next().unwrap_or(5))
        },
        || ready(0),
        pending(),
    )
    .await;
    assert_eq!(*requested.borrow(), [0, 4, 3, 5]);
}

/// A singleton polls until the deadline without collection or database-progress work.
#[tokio::test(start_paused = true)]
async fn startup_sync_waits_without_established_peers() {
    let start = Instant::now();
    let polls = Cell::new(0);
    let work = Cell::new(0);
    drive_epoch_record_sync(
        || {
            polls.set(polls.get() + 1);
            ready(0)
        },
        |epoch| {
            work.set(work.get() + 1);
            ready(epoch)
        },
        || {
            work.set(work.get() + 1);
            ready(0)
        },
        pending(),
    )
    .await;
    assert_eq!(work.get(), 0);
    assert!(polls.get() > 1);
    assert_eq!(start.elapsed(), STARTUP_SYNC_TIMEOUT);
}

/// A connection established after two empty polls still triggers collection before startup ends.
#[tokio::test(start_paused = true)]
async fn startup_sync_collects_after_delayed_connection() {
    let start = Instant::now();
    let polls = Cell::new(0);
    let passes = Cell::new(0);
    drive_epoch_record_sync(
        || {
            polls.set(polls.get() + 1);
            ready(usize::from(polls.get() > 2))
        },
        |epoch| {
            assert_eq!(polls.get(), 3);
            passes.set(passes.get() + 1);
            ready(epoch)
        },
        || ready(0),
        pending(),
    )
    .await;
    assert_eq!(passes.get(), 2);
    assert_eq!(start.elapsed(), STARTUP_PEER_POLL_INTERVAL * 2);
}

/// Waiting for a peer consumes the same deadline as subsequent productive collection passes.
#[tokio::test(start_paused = true)]
async fn startup_sync_peer_wait_does_not_renew_deadline() {
    let start = Instant::now();
    let prefix = Cell::new(0);
    drive_epoch_record_sync(
        || async {
            tokio::time::sleep(STARTUP_SYNC_TIMEOUT - STARTUP_SYNC_PASS_TIMEOUT).await;
            1
        },
        |_| {
            prefix.set(prefix.get() + 1);
            pending()
        },
        || ready(prefix.get()),
        pending(),
    )
    .await;
    assert_eq!(prefix.get(), 1);
    assert_eq!(start.elapsed(), STARTUP_SYNC_TIMEOUT);
}

/// Shutdown remains responsive when bootstrap dials have not connected yet.
#[tokio::test(start_paused = true)]
async fn startup_sync_shutdown_cancels_peer_wait() {
    let start = Instant::now();
    let passes = Cell::new(0);
    drive_epoch_record_sync(
        || ready(0),
        |epoch| {
            passes.set(passes.get() + 1);
            ready(epoch)
        },
        || ready(0),
        tokio::time::sleep(STARTUP_PEER_POLL_INTERVAL),
    )
    .await;
    assert_eq!(passes.get(), 0);
    assert_eq!(start.elapsed(), STARTUP_PEER_POLL_INTERVAL);
}

/// Connected cold peers that cannot serve certificates consume one pass cap, not the full deadline.
#[tokio::test(start_paused = true)]
async fn startup_sync_caps_a_stalled_cohort() {
    let start = Instant::now();
    let passes = Cell::new(0);
    drive_epoch_record_sync(
        || ready(4),
        |_| {
            passes.set(passes.get() + 1);
            pending()
        },
        || ready(0),
        pending(),
    )
    .await;
    assert_eq!(passes.get(), 1);
    assert_eq!(start.elapsed(), STARTUP_SYNC_PASS_TIMEOUT);
}

/// Backfilling certificates counts as progress even when the highest stored record is unchanged.
#[tokio::test(start_paused = true)]
async fn startup_sync_resumes_after_a_capped_pass_with_progress() {
    let prefix = Cell::new(5);
    let requested = RefCell::new(Vec::new());
    drive_epoch_record_sync(
        || ready(1),
        |epoch| {
            requested.borrow_mut().push(epoch);
            let first_pass = requested.borrow().len() == 1;
            let prefix = &prefix;
            async move {
                if first_pass {
                    prefix.set(6);
                    pending().await
                } else {
                    5
                }
            }
        },
        || ready(prefix.get()),
        pending(),
    )
    .await;
    assert_eq!(*requested.borrow(), [0, 5, 5]);
}

/// Continuous certified-prefix progress cannot renew the overall startup deadline.
#[tokio::test(start_paused = true)]
async fn startup_sync_deadline_bounds_continuous_progress() {
    let start = Instant::now();
    let prefix = Cell::new(0);
    drive_epoch_record_sync(
        || ready(1),
        |_| {
            prefix.set(prefix.get() + 1);
            pending()
        },
        || ready(prefix.get()),
        pending(),
    )
    .await;
    assert_eq!(start.elapsed(), STARTUP_SYNC_TIMEOUT);
    assert!(prefix.get() > 1, "the gate must continue after a productive capped pass");
}

/// The deadline also covers an unresponsive network command used to count established peers.
#[tokio::test(start_paused = true)]
async fn startup_sync_deadline_bounds_peer_count() {
    let start = Instant::now();
    drive_epoch_record_sync(pending, ready, || ready(0), pending()).await;
    assert_eq!(start.elapsed(), STARTUP_SYNC_TIMEOUT);
}

/// Shutdown cancels an in-flight collection before its pass cap.
#[tokio::test(start_paused = true)]
async fn startup_sync_shutdown_cancels_collection() {
    let start = Instant::now();
    let passes = Cell::new(0);
    let shutdown_after = Duration::from_secs(1);
    drive_epoch_record_sync(
        || ready(1),
        |_| {
            passes.set(passes.get() + 1);
            pending()
        },
        || ready(0),
        tokio::time::sleep(shutdown_after),
    )
    .await;
    assert_eq!(passes.get(), 1);
    assert_eq!(start.elapsed(), shutdown_after);
}

/// A shutdown already requested wins before any network or collection work starts.
#[tokio::test(start_paused = true)]
async fn startup_sync_honors_existing_shutdown() {
    let work = Cell::new(0);
    drive_epoch_record_sync(
        || async {
            work.set(work.get() + 1);
            1
        },
        ready,
        || ready(0),
        ready(()),
    )
    .await;
    assert_eq!(work.get(), 0);
}
