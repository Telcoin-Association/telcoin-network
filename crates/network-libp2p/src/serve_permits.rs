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
    fmt,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use tn_config::ServeBudget;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[cfg(test)]
#[path = "tests/serve_permits_tests.rs"]
mod serve_permits_tests;

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
            held_gauge: Gauge::noop(),
            offered: Counter::noop(),
            admitted: Counter::noop(),
            shed: Counter::noop(),
        }
    }

    /// Offer one request to the pool: admit it with a permit, or shed it.
    ///
    /// The decision is one counter and one semaphore try, before any work for the request. A
    /// shed counts exactly once under `serve_budget` and changes nothing else.
    fn try_acquire(self: &Arc<Self>) -> Option<ServePermit> {
        self.offered.increment(1);
        self.semaphore.clone().try_acquire_owned().map_or_else(
            |_no_permit| {
                self.shed.increment(1);
                None
            },
            |permit| {
                self.admitted.increment(1);
                self.held.fetch_add(1, Ordering::Relaxed);
                self.held_gauge.increment(1.0);
                Some(ServePermit { _permit: permit, pool: Arc::clone(self) })
            },
        )
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

    /// Offer one request of `class`: a permit to hold while serving it, or `None` to shed it.
    ///
    /// On `None` the caller drops the response channel and does no work for the request. The
    /// shed is already counted.
    pub fn try_acquire(&self, class: ServiceClass) -> Option<ServePermit> {
        self.pools.get(class).try_acquire()
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
}

impl fmt::Debug for ServePermit {
    /// Print the class of the pool that issued this permit.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServePermit").field("class", &self.pool.class).finish_non_exhaustive()
    }
}

impl Drop for ServePermit {
    /// Release the held count and gauge. The semaphore permit releases itself.
    fn drop(&mut self) {
        // the update closure returns `Some` for every input, so it never fails
        let _ = self.pool.held.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
            Some(held.saturating_sub(1))
        });
        self.pool.held_gauge.decrement(1.0);
    }
}
