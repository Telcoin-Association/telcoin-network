//! Bounded peer dial retries that preserve recovery while a node is isolated.

use std::{fmt::Display, future::Future, time::Duration};

use crate::network_readiness::{probe, NETWORK_COMMAND_TIMEOUT};
use futures::StreamExt as _;
use tracing::warn;

/// Result of a dial request, distinguishing established connectivity from an in-flight dial.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DialOutcome {
    /// The dial succeeded or the peer was already connected.
    Connected,
    /// A dial is still in flight and may subsequently fail.
    Pending,
    /// The request failed or its outcome could not be obtained within the command bound.
    Failed,
}

/// Retry a peer until connected, or until retries are exhausted with another established peer.
///
/// The command bound covers admission and the dial outcome, not the transport's lifetime.
/// Timing out drops the reply receiver without cancelling the swarm's in-flight dial. A later
/// pending outcome therefore continues retrying. Backoff doubles up to 120 seconds, and an
/// unavailable or timed-out established-peer probe never permits abandoning an isolated node.
pub(crate) async fn retry_peer_dial<D, Dial, P, Peers, E>(
    peer: impl Display,
    dial: D,
    established_peers: P,
) where
    D: Fn() -> Dial,
    Dial: Future<Output = DialOutcome>,
    P: Fn() -> Peers,
    Peers: Future<Output = Result<usize, E>>,
{
    futures::stream::unfold((1_u64, 0_u32), |(backoff, retries)| {
        let peer = &peer;
        let dial = &dial;
        let established_peers = &established_peers;
        async move {
            let outcome = tokio::time::timeout(NETWORK_COMMAND_TIMEOUT, dial())
                .await
                .unwrap_or(DialOutcome::Failed);
            if matches!(outcome, DialOutcome::Connected) {
                None
            } else {
                warn!(target: "dial_peer", %peer, ?outcome, "peer is not connected; retrying");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                let reachable = probe(established_peers()).await.is_reachable();
                if retries >= 10 && reachable {
                    warn!(target: "dial_peer", %peer, "failed to reach peer, giving up");
                    None
                } else {
                    Some(((), ((backoff * 2).min(120), retries.saturating_add(1))))
                }
            }
        }
    })
    .for_each(|()| std::future::ready(()))
    .await;
}

#[cfg(test)]
mod tests {
    //! Deterministic coverage of slow failed dials and isolation beyond the retry budget.

    use super::{retry_peer_dial, DialOutcome};
    use std::{cell::Cell, time::Duration};
    use tokio::time::Instant;

    /// A slow failed transport dial must leave retries alive until a later dial connects.
    #[tokio::test(start_paused = true)]
    async fn slow_failed_dial_recovers_after_pending_replies() {
        let started = Instant::now();
        let attempts = Cell::new(0_u32);
        let connected = Cell::new(false);
        retry_peer_dial(
            "bootstrap-peer",
            || {
                let attempt = attempts.get();
                attempts.set(attempt + 1);
                let connected = &connected;
                async move {
                    if attempt == 0 {
                        // The receiver times out, but the simulated transport remains in flight.
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        DialOutcome::Failed
                    } else if started.elapsed() < Duration::from_secs(10) {
                        DialOutcome::Pending
                    } else {
                        connected.set(true);
                        DialOutcome::Connected
                    }
                }
            },
            || std::future::ready(Ok::<_, ()>(0)),
        )
        .await;
        assert!(connected.get(), "pending dials must not end reconnect work");
        assert!(attempts.get() >= 5);
    }

    /// The retry budget cannot terminate reconnect work without an established peer.
    #[tokio::test(start_paused = true)]
    async fn isolated_node_keeps_retrying_beyond_budget() {
        let attempts = Cell::new(0_u32);
        retry_peer_dial(
            "bootstrap-peer",
            || {
                attempts.set(attempts.get() + 1);
                std::future::ready(if attempts.get() > 12 {
                    DialOutcome::Connected
                } else {
                    DialOutcome::Pending
                })
            },
            || std::future::ready(Ok::<_, ()>(0)),
        )
        .await;
        assert_eq!(attempts.get(), 13);
    }
}
