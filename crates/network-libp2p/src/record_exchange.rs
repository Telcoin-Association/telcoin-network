//! Bounded, authenticated retrieval of a connected peer's current signed record.
//!
//! The request is BCS unit, and the response is an optional `(BLS key, NodeRecord)`.
//! Negotiation is the capability check. Unsupported peers keep using Kademlia.
//! The authenticated transport peer supplies the publisher, and responses pass
//! through the ordinary inbound record pipeline.

use crate::{codec::TNCodec, types::NodeRecord};
use libp2p::{request_response::OutboundRequestId, PeerId};
use lru::LruCache;
use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    time::Duration,
};
use tn_types::BlsPublicKey;
use tokio::time::Instant;

/// A signed self-record, or a small refusal when the serving budget is exhausted.
pub(crate) type RecordResponse = Option<(BlsPublicKey, NodeRecord)>;

/// Bounded snappy/BCS framing without peer-exchange or consensus messages.
pub(crate) type RecordCodec = TNCodec<(), RecordResponse>;

/// Per-peer cooldowns and a connection-budget-sized set of live retrievals.
pub(crate) struct RecordExchange {
    /// Outbound attempts, including failures, retained in a capacity-bounded LRU.
    attempts: LruCache<PeerId, Instant>,
    /// Inbound responses, independent of the outbound cooldown.
    served: LruCache<PeerId, Instant>,
    /// One live request per peer, retained until response or timeout.
    pending: HashMap<PeerId, OutboundRequestId>,
    /// Reconnects deferred by the cooldown or live-request budget, also capacity-bounded.
    deferred: HashSet<PeerId>,
    /// Maximum simultaneous requests, derived from the configured live-peer budget.
    max_pending: usize,
    /// Minimum interval between attempts or full responses to the same peer.
    interval: Duration,
}

#[cfg(test)]
#[path = "tests/record_exchange_tests.rs"]
mod tests;

impl RecordExchange {
    /// Use bounded history and the peer manager's connection and heartbeat budgets.
    pub(crate) fn new(capacity: NonZeroUsize, max_pending: usize, interval: Duration) -> Self {
        Self {
            attempts: LruCache::new(capacity),
            served: LruCache::new(capacity),
            pending: HashMap::new(),
            deferred: HashSet::new(),
            max_pending,
            interval,
        }
    }

    /// Reserve an attempt only when neither a live request nor a recent attempt exists.
    pub(crate) fn allow_request(&mut self, peer: PeerId) -> bool {
        !self.pending.contains_key(&peer)
            && self.pending.len() < self.max_pending
            && Self::allow(&mut self.attempts, peer, self.interval)
    }

    /// Track the request immediately after an allowed attempt is dispatched.
    pub(crate) fn track(&mut self, peer: PeerId, request: OutboundRequestId) {
        self.pending.insert(peer, request);
        self.deferred.remove(&peer);
    }

    /// Coalesce a budget-limited reconnect, returning whether a new retry was queued.
    pub(crate) fn defer(&mut self, peer: PeerId) -> bool {
        if !self.pending.contains_key(&peer) && self.deferred.len() < self.max_pending {
            self.deferred.insert(peer)
        } else {
            false
        }
    }

    /// Current live requests and queued retries, for the network gauges.
    pub(crate) fn counts(&self) -> (usize, usize) {
        (self.pending.len(), self.deferred.len())
    }

    /// Drain bounded deferred work for the next heartbeat, dropping disconnected peers upstream.
    pub(crate) fn take_deferred(&mut self) -> HashSet<PeerId> {
        std::mem::take(&mut self.deferred)
    }

    /// Accept only a response or failure matching the peer's currently tracked request.
    pub(crate) fn finish(&mut self, peer: PeerId, request: OutboundRequestId) -> bool {
        if self.pending.get(&peer) == Some(&request) {
            self.pending.remove(&peer);
            true
        } else {
            false
        }
    }

    /// Permit one full response per interval, without refreshing it on a refusal.
    pub(crate) fn allow_response(&mut self, peer: PeerId) -> bool {
        Self::allow(&mut self.served, peer, self.interval)
    }

    /// Apply a cooldown before allocating a response or dispatching a request.
    fn allow(history: &mut LruCache<PeerId, Instant>, peer: PeerId, interval: Duration) -> bool {
        let now = Instant::now();
        if history.get(&peer).is_some_and(|last| now.duration_since(*last) < interval) {
            false
        } else {
            history.put(peer, now);
            true
        }
    }
}
