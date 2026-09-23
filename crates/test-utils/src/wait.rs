// SPDX-License-Identifier: Apache-2.0

//! Bounded condition-polling helpers for tests.
//!
//! A self-synchronizing replacement for a fixed `sleep(N)` "wait for
//! convergence" call, which only guesses at how long an asynchronous event
//! (a store write becoming visible, a swarm connection settling, a subscription
//! propagating) takes. Instead of sleeping a fixed pad and hoping, poll a cheap
//! predicate on a bounded ~10ms cadence until it holds, failing with a
//! descriptive message once the deadline passes.
//!
//! [`wait_until`] is for `async` call sites; [`wait_until_blocking`] is the
//! synchronous analogue, for helpers that poll with `std::thread::sleep` /
//! `block_on` rather than `.await`.

use std::time::{Duration, Instant};

/// Cadence between poll attempts. Small enough to keep the settle latency low,
/// large enough not to busy-spin.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Number of `POLL_INTERVAL` attempts that fit in `timeout` (at least one).
fn attempts_for(timeout: Duration) -> u128 {
    (timeout.as_millis() / POLL_INTERVAL.as_millis()).max(1)
}

/// Poll an async `condition` until it returns `true`, or fail once `timeout` elapses.
///
/// A bounded, self-synchronizing replacement for a fixed `sleep` that only
/// guesses at how long an asynchronous event takes. It polls on a bounded sequence of ~10ms
/// intervals, stopping at the first resolved verdict:
/// `Some(Ok(()))` once the condition holds, `Some(Err(_))` if a poll itself
/// errors, and `None` while still waiting. A Tokio deadline also bounds time spent inside each
/// condition, cancelling an in-flight poll when the budget expires. Under
/// `tokio::test(start_paused = true)` the interval
/// sleeps advance virtual time between attempts, so the spawned work being waited
/// on makes progress without burning real wall-clock.
pub async fn wait_until<F, Fut>(
    timeout: Duration,
    description: &str,
    condition: F,
) -> eyre::Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = eyre::Result<bool>>,
{
    use futures::StreamExt as _;

    let condition = &condition;

    let polls =
        futures::stream::iter(0..attempts_for(timeout)).filter_map(move |attempt| async move {
            if attempt > 0 {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            condition().await.map(|met| met.then_some(())).transpose()
        });
    let mut polls = std::pin::pin!(polls);
    tokio::time::timeout(timeout, polls.next())
        .await
        .unwrap_or(None)
        .unwrap_or_else(|| Err(eyre::eyre!("timed out waiting for condition: {description}")))
}

/// Poll a synchronous `condition` until it returns `true`, or fail once `timeout` elapses.
///
/// The blocking analogue of [`wait_until`], for helpers that observe state with
/// `std::thread::sleep` / `block_on` rather than `.await` (the `e2e-tests`
/// deadline checks). It searches the same bounded attempt sequence for the first resolved verdict,
/// sleeping the current thread `POLL_INTERVAL`
/// between attempts. Predicate execution time counts against the deadline, and a result arriving
/// after the deadline is rejected. A synchronous predicate cannot be preempted, so callers must
/// bound any blocking I/O inside it; an in-flight call can exceed the budget by its own timeout.
pub fn wait_until_blocking<F>(
    timeout: Duration,
    description: &str,
    condition: F,
) -> eyre::Result<()>
where
    F: Fn() -> eyre::Result<bool>,
{
    let start = Instant::now();
    wait_until_blocking_with_clock(
        timeout,
        description,
        condition,
        || start.elapsed(),
        std::thread::sleep,
    )
}

/// Poll with an injected elapsed clock and sleeper so deadline boundaries are deterministic in
/// tests.
fn wait_until_blocking_with_clock(
    timeout: Duration,
    description: &str,
    condition: impl Fn() -> eyre::Result<bool>,
    elapsed: impl Fn() -> Duration,
    sleep: impl Fn(Duration),
) -> eyre::Result<()> {
    let timed_out = || eyre::eyre!("timed out waiting for condition: {description}");
    (0..attempts_for(timeout))
        .find_map(|attempt| {
            if attempt > 0 {
                sleep(POLL_INTERVAL.min(timeout.saturating_sub(elapsed())));
            }
            if elapsed() >= timeout {
                Some(Err(timed_out()))
            } else {
                let result = condition().map(|met| met.then_some(())).transpose();
                if elapsed() >= timeout {
                    Some(Err(timed_out()))
                } else {
                    result
                }
            }
        })
        .unwrap_or_else(|| Err(timed_out()))
}

#[cfg(test)]
mod tests {
    //! Deadline tests using virtual clocks rather than wall-clock timing assertions.

    use super::*;
    use std::cell::Cell;

    /// A slow async predicate is cancelled at the deadline instead of multiplying the attempt
    /// budget.
    #[tokio::test(start_paused = true)]
    async fn wait_until_bounds_slow_conditions() {
        let start = tokio::time::Instant::now();
        let timeout = Duration::from_millis(50);
        let result = wait_until(timeout, "slow predicate", || async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(false)
        })
        .await;
        assert!(result.is_err());
        assert_eq!(start.elapsed(), timeout);
    }

    /// Both late success and continued unavailability consume the blocking predicate's time budget.
    #[test]
    fn wait_until_blocking_counts_condition_time() {
        [false, true].into_iter().for_each(|met| {
            let elapsed = Cell::new(Duration::ZERO);
            let calls = Cell::new(0);
            let timeout = Duration::from_millis(100);
            let result = wait_until_blocking_with_clock(
                timeout,
                "slow blocking predicate",
                || {
                    calls.set(calls.get() + 1);
                    elapsed.set(elapsed.get() + timeout);
                    Ok(met)
                },
                || elapsed.get(),
                |duration| elapsed.set(elapsed.get() + duration),
            );
            assert!(result.is_err());
            assert_eq!(calls.get(), 1);
        });
    }

    /// The final interval is capped to the remaining budget and cannot launch another predicate.
    #[test]
    fn wait_until_blocking_does_not_poll_after_final_sleep() {
        let elapsed = Cell::new(Duration::ZERO);
        let calls = Cell::new(0);
        let timeout = Duration::from_millis(100);
        let result = wait_until_blocking_with_clock(
            timeout,
            "final interval",
            || {
                calls.set(calls.get() + 1);
                elapsed.set(elapsed.get() + Duration::from_millis(95));
                Ok(false)
            },
            || elapsed.get(),
            |duration| elapsed.set(elapsed.get() + duration),
        );
        assert!(result.is_err());
        assert_eq!(calls.get(), 1);
        assert_eq!(elapsed.get(), timeout);
    }

    /// Both helpers preserve success and predicate errors without polling a resolved condition
    /// again.
    #[tokio::test(start_paused = true)]
    async fn wait_helpers_preserve_resolved_conditions() -> eyre::Result<()> {
        let timeout = Duration::from_secs(1);
        let calls = Cell::new(0);
        wait_until(timeout, "ready", || {
            calls.set(calls.get() + 1);
            std::future::ready(Ok(true))
        })
        .await?;
        assert_eq!(calls.get(), 1);
        let result = wait_until(timeout, "error", || {
            std::future::ready(Err(eyre::eyre!("predicate error")))
        })
        .await;
        assert_eq!(result.err().map(|error| error.to_string()), Some("predicate error".to_owned()));
        let calls = Cell::new(0);
        wait_until_blocking_with_clock(
            timeout,
            "ready",
            || {
                calls.set(calls.get() + 1);
                Ok(true)
            },
            || Duration::ZERO,
            |_| {},
        )?;
        assert_eq!(calls.get(), 1);
        let result = wait_until_blocking_with_clock(
            timeout,
            "error",
            || Err(eyre::eyre!("predicate error")),
            || Duration::ZERO,
            |_| {},
        );
        assert_eq!(result.err().map(|error| error.to_string()), Some("predicate error".to_owned()));
        Ok(())
    }
}
