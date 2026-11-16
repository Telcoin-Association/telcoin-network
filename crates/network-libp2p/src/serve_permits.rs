//! Per-class serve permits: the application's bounded budget for inbound requests in service.
//!
//! The swarm forwards each inbound request to the application through the event queue. A serve
//! permit covers the application side of one such request. It starts when the request handler
//! calls [`ServePermits::try_acquire`], after it dequeues the forwarded request and before it does
//! any work for it. It ends when the [`ServePermit`] drops. Release is a destructor, so it is
//! uniform across every exit: the response is sent, the handler returns an error, the swarm
//! cancels the request after a timeout or a closed stream, or an epoch teardown drops the task.
//!
//! The permit window is distinct from `inbound_requests_pending`, which the swarm measures from
//! the forward to the response, so that gauge also spans the event-queue wait and the response
//! send. It is also distinct from the event queue itself, which holds requests that no handler
//! has dequeued yet.
//!
//! A class capacity of zero means unbounded: every offer is admitted and the series only observe.
//! Zero is the default for every class, so a node without a configured budget admits exactly what
//! it admitted before. A `serve_budget` shed drops the response channel without a reply, so the
//! swarm also counts an `omitted` failure for the same request. That is the same overlap that an
//! `admission` shed has. Two cases are not offers and stay uncounted here: a closed event channel
//! at the epoch boundary is not overload, and the swarm answers peer-exchange requests itself, so
//! they never reach a handler.
//!
//! An enforcing class also bounds each peer: one peer holds at most [`MAX_PERMITS_PER_PEER`]
//! permits of the class. An offer from a peer at its share is shed under `serve_budget` even when
//! the pool has free permits, so one peer cannot hold a whole class. The per-peer counts keep only
//! peers that hold a permit. An unbounded class keeps no per-peer counts.
//!
//! A permit is held for the whole task. For `vote` that includes a wait on the author's vote lock
//! in the primary's request handler, behind an earlier vote request of the same peer. The per-peer
//! share bounds that wait to one task per peer.
//!
//! Every series carries the `network` and `class` labels only, so the cardinality is the six
//! classes per swarm. The pools register all six classes at construction, so `gossip` and
//! `certificate_sync` export zero series rather than missing ones.

use crate::{
    metrics::{network_label, shed_counter, PerClass},
    service_class::{ServiceClass, ShedReason},
    types::NetworkType,
};
use reth_metrics::metrics::{Counter, Gauge};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, PoisonError,
    },
};
use tn_config::ServeBudget;
use tn_types::BlsPublicKey;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[cfg(test)]
#[path = "tests/serve_permits_tests.rs"]
mod serve_permits_tests;

/// The most permits of one enforcing class that one peer can hold at once.
///
/// It matches the primary's per-peer epoch record admission cap. Without it, one peer could hold
/// every permit of a class, and the node would shed the requests of every other peer in that
/// class. Committee peers are exempt from penalties, so peer scoring does not stop a committee
/// peer that does this. An unbounded class has no per-peer share.
pub const MAX_PERMITS_PER_PEER: u32 = 2;

/// The permit capacity of `class` under `budget`. Zero is unbounded.
///
/// Gossip has no response and takes no handler permit, so its capacity is always zero.
pub(crate) fn capacity_for(class: ServiceClass, budget: &ServeBudget) -> u32 {
    match class {
        ServiceClass::Vote => budget.vote(),
        ServiceClass::EpochRecord => budget.epoch_record(),
        ServiceClass::CertificateSync => budget.certificate_sync(),
        ServiceClass::Batch => budget.batch(),
        ServiceClass::Gossip => 0,
        ServiceClass::Other => budget.other(),
    }
}

/// The semaphore size for `capacity`: the capacity itself, or the largest pool for zero.
fn pool_size(capacity: u32) -> usize {
    usize::try_from(capacity)
        .ok()
        .filter(|permits| *permits > 0)
        .map_or(Semaphore::MAX_PERMITS, |permits| permits.min(Semaphore::MAX_PERMITS))
}

/// The pool and the metric handles of one service class.
struct ClassPermits {
    /// The class this pool serves.
    class: ServiceClass,
    /// The configured capacity in permits. Zero is unbounded.
    capacity: u32,
    /// The pool. An unbounded class holds [`Semaphore::MAX_PERMITS`].
    semaphore: Arc<Semaphore>,
    /// The permits held right now.
    held: AtomicU32,
    /// The permits each peer holds right now. Only an enforcing class uses it, and a peer leaves
    /// the map with its last permit, so it holds at most `capacity` peers.
    peers: Mutex<HashMap<BlsPublicKey, u32>>,
    /// `serve_permits_held` for this class.
    held_gauge: Gauge,
    /// `serve_requests_offered_total` for this class.
    offered: Counter,
    /// `serve_requests_admitted_total` for this class.
    admitted: Counter,
    /// `inbound_requests_shed_total` with reason `serve_budget` for this class.
    shed: Counter,
}

impl ClassPermits {
    /// Resolve the series for `network` and `class`, register each at zero, and size the pool.
    fn new(network: &str, class: ServiceClass, capacity: u32) -> Self {
        metrics::gauge!(
            "tn_network.serve_permit_capacity",
            "network" => network.to_owned(),
            "class" => class.label(),
        )
        .set(f64::from(capacity));
        let pool = Self {
            class,
            capacity,
            semaphore: Arc::new(Semaphore::new(pool_size(capacity))),
            held: AtomicU32::new(0),
            peers: Mutex::new(HashMap::new()),
            held_gauge: metrics::gauge!(
                "tn_network.serve_permits_held",
                "network" => network.to_owned(),
                "class" => class.label(),
            ),
            offered: metrics::counter!(
                "tn_network.serve_requests_offered_total",
                "network" => network.to_owned(),
                "class" => class.label(),
            ),
            admitted: metrics::counter!(
                "tn_network.serve_requests_admitted_total",
                "network" => network.to_owned(),
                "class" => class.label(),
            ),
            shed: shed_counter(network, class, ShedReason::ServeBudget),
        };
        // Register the held gauge without setting it: at an epoch boundary the previous pool
        // generation can still hold permits on the same series, and its drops subtract from the
        // sum. A set to zero here would make that sum go negative.
        pool.held_gauge.increment(0.0);
        pool.offered.increment(0);
        pool.admitted.increment(0);
        pool.shed.increment(0);
        pool
    }

    /// An unbounded pool for `class` with no-op handles, so nothing is registered.
    fn unmetered(class: ServiceClass) -> Self {
        Self {
            class,
            capacity: 0,
            semaphore: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            held: AtomicU32::new(0),
            peers: Mutex::new(HashMap::new()),
            held_gauge: Gauge::noop(),
            offered: Counter::noop(),
            admitted: Counter::noop(),
            shed: Counter::noop(),
        }
    }

    /// Offer one request from `peer` to the pool: admit it with a permit, or shed it.
    ///
    /// The decision is one counter and one semaphore try, before any work for the request. An
    /// enforcing class first checks the share of `peer` and counts the new permit under the same
    /// lock, so two offers of one peer cannot both pass the check. A shed counts exactly once
    /// under `serve_budget` and changes nothing else.
    fn try_acquire(self: &Arc<Self>, peer: BlsPublicKey) -> Option<ServePermit> {
        self.offered.increment(1);
        let permit = if self.capacity > 0 {
            let mut peers = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
            let held_by_peer = peers.get(&peer).copied().unwrap_or(0);
            (held_by_peer < MAX_PERMITS_PER_PEER)
                .then(|| self.semaphore.clone().try_acquire_owned().ok())
                .flatten()
                .inspect(|_| *peers.entry(peer).or_insert(0) += 1)
        } else {
            self.semaphore.clone().try_acquire_owned().ok()
        };
        permit.map_or_else(
            || {
                self.shed.increment(1);
                None
            },
            |permit| {
                self.admitted.increment(1);
                self.held.fetch_add(1, Ordering::Relaxed);
                self.held_gauge.increment(1.0);
                Some(ServePermit { _permit: permit, pool: Arc::clone(self), peer })
            },
        )
    }

    /// Return one permit of the share of `peer`. The peer leaves the map with its last permit.
    fn release_peer(&self, peer: &BlsPublicKey) {
        let mut peers = self.peers.lock().unwrap_or_else(PoisonError::into_inner);
        let remaining = peers.get(peer).map_or(0, |held| held.saturating_sub(1));
        if remaining == 0 {
            peers.remove(peer);
        } else {
            peers.insert(*peer, remaining);
        }
    }
}

/// The per-class serve permit pools of one swarm.
///
/// Clones share the pools, so every handle of the swarm offers to the same budget.
#[derive(Clone)]
pub struct ServePermits {
    /// One pool per class.
    pools: PerClass<Arc<ClassPermits>>,
}

impl fmt::Debug for ServePermits {
    /// Print `(capacity, held)` for every class.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ServePermits");
        ServiceClass::ALL.iter().for_each(|class| {
            debug.field(class.label(), &(self.capacity(*class), self.held(*class)));
        });
        debug.finish()
    }
}

impl ServePermits {
    /// Build the pools of `network_type` from `budget` and register every series at zero.
    pub fn new(network_type: &NetworkType, budget: ServeBudget) -> Self {
        let network = network_label(network_type);
        Self {
            pools: PerClass::new(|class| {
                Arc::new(ClassPermits::new(&network, class, capacity_for(class, &budget)))
            }),
        }
    }

    /// Unbounded pools without metric series, for a handle that no swarm built.
    ///
    /// [`NetworkHandle::new`](crate::types::NetworkHandle::new) uses this, so a handle built
    /// without a swarm never registers a series under a network label that no swarm owns. Every
    /// offer is admitted.
    pub fn unmetered() -> Self {
        Self { pools: PerClass::new(|class| Arc::new(ClassPermits::unmetered(class))) }
    }

    /// Offer one request of `class` from `peer`: a permit to hold while serving it, or `None` to
    /// shed it.
    ///
    /// An enforcing class sheds the offer when `peer` already holds [`MAX_PERMITS_PER_PEER`]
    /// permits of it, even when the pool has free permits. On `None` the caller drops the response
    /// channel and does no work for the request. The shed is already counted.
    pub fn try_acquire(&self, class: ServiceClass, peer: BlsPublicKey) -> Option<ServePermit> {
        self.pools.get(class).try_acquire(peer)
    }

    /// The permits of `class` held right now.
    pub fn held(&self, class: ServiceClass) -> u32 {
        self.pools.get(class).held.load(Ordering::Relaxed)
    }

    /// The configured capacity of `class` in permits. Zero is unbounded.
    pub fn capacity(&self, class: ServiceClass) -> u32 {
        self.pools.get(class).capacity
    }
}

/// One admitted request's permit. Dropping it releases the pool slot and the held count.
pub struct ServePermit {
    /// The semaphore permit. Dropping it frees the pool slot.
    _permit: OwnedSemaphorePermit,
    /// The pool to release on drop.
    pool: Arc<ClassPermits>,
    /// The peer whose share this permit counts against in an enforcing pool.
    peer: BlsPublicKey,
}

impl fmt::Debug for ServePermit {
    /// Print the class of the pool that issued this permit.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServePermit").field("class", &self.pool.class).finish_non_exhaustive()
    }
}

impl Drop for ServePermit {
    /// Release the held count, the gauge and the peer's share. The semaphore permit releases
    /// itself.
    fn drop(&mut self) {
        // the update closure returns `Some` for every input, so it never fails
        let _ = self.pool.held.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
            Some(held.saturating_sub(1))
        });
        self.pool.held_gauge.decrement(1.0);
        if self.pool.capacity > 0 {
            self.pool.release_peer(&self.peer);
        }
    }
}
