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

use std::time::Duration;

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
/// guesses at how long an asynchronous event takes. It folds over a bounded
/// sequence of ~10ms poll attempts, carrying the first resolved verdict forward:
/// `Some(Ok(()))` once the condition holds, `Some(Err(_))` if a poll itself
/// errors, and `None` while still waiting. An exhausted fold (still `None`) means
/// the deadline passed. Under `tokio::test(start_paused = true)` the interval
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

    futures::stream::iter(0..attempts_for(timeout))
        .fold(None, move |resolved: Option<eyre::Result<()>>, attempt| async move {
            if resolved.is_some() {
                resolved
            } else {
                if attempt > 0 {
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
                condition().await.map(|met| met.then_some(())).transpose()
            }
        })
        .await
        .unwrap_or_else(|| Err(eyre::eyre!("timed out waiting for condition: {description}")))
}

/// Poll a synchronous `condition` until it returns `true`, or fail once `timeout` elapses.
///
/// The blocking analogue of [`wait_until`], for helpers that observe state with
/// `std::thread::sleep` / `block_on` rather than `.await` (the `e2e-tests`
/// deadline loops). It folds over the same bounded attempt sequence, carrying the
/// first resolved verdict forward and sleeping the current thread `POLL_INTERVAL`
/// between attempts. An exhausted fold means the deadline passed.
pub fn wait_until_blocking<F>(
    timeout: Duration,
    description: &str,
    condition: F,
) -> eyre::Result<()>
where
    F: Fn() -> eyre::Result<bool>,
{
    (0..attempts_for(timeout))
        .fold(None, |resolved: Option<eyre::Result<()>>, attempt| {
            if resolved.is_some() {
                resolved
            } else {
                if attempt > 0 {
                    std::thread::sleep(POLL_INTERVAL);
                }
                condition().map(|met| met.then_some(())).transpose()
            }
        })
        .unwrap_or_else(|| Err(eyre::eyre!("timed out waiting for condition: {description}")))
}
