//! Per-call-site warning/error budgets for network diagnostics.
//!
//! Each `bounded_warn!` or `bounded_error!` call site owns one window that admits four detailed
//! events per minute across all swarms. One call site cannot use the budget of another, so a
//! remotely driven site cannot suppress an unrelated local failure. Suppressed fields are never
//! evaluated. Summaries carry a fixed class name and the call site, and an event-loop timer
//! drains quiet tails. See `docs/network-diagnostics.md` for the site and label audit.

#[path = "diagnostics/window.rs"]
mod window;

use std::{
    sync::{Mutex, Once, PoisonError},
    time::Instant,
};
use window::{Summary, Window};

pub(crate) use window::INTERVAL;

/// Fixed event classes. A class names the kind of failure; the budget belongs to the call site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    /// Outer swarm event failures and external address reports.
    Swarm,
    /// Local command processing failures, including remotely induced application replies.
    Command,
    /// Gossip rejection, validation reporting, and forwarding failures.
    Gossip,
    /// Request/response protocol and forwarding failures.
    Request,
    /// Inbound bulk streams and application forwarding failures.
    Stream,
    /// Kademlia query, publication, and retained-record validation failures.
    Kademlia,
    /// Invalid or rejected inbound peer records and provider announcements.
    Record,
    /// Kademlia database read, write, and decode failures.
    Store,
    /// Admission failures and peer-manager connection invariants.
    PeerAdmission,
    /// Penalties, bans, reputation, and peer lifecycle inconsistencies.
    PeerState,
    /// Peer identity, inventory, and confirmed-record inconsistencies.
    PeerRecord,
    /// Undeliverable application or dial replies.
    Reply,
}

impl Class {
    /// The fixed metric label for this class.
    const fn label(self) -> &'static str {
        match self {
            Self::Swarm => "swarm",
            Self::Command => "command",
            Self::Gossip => "gossip",
            Self::Request => "request",
            Self::Stream => "stream",
            Self::Kademlia => "kademlia",
            Self::Record => "record",
            Self::Store => "store",
            Self::PeerAdmission => "peer_admission",
            Self::PeerState => "peer_state",
            Self::PeerRecord => "peer_record",
            Self::Reply => "reply",
        }
    }
}

/// The budget of one call site. Every `bounded_*!` expansion owns exactly one static `Site`.
#[derive(Debug)]
pub(crate) struct Site {
    /// The class reported in summaries and metrics.
    class: Class,
    /// The call site as `file:line`, fixed at compile time.
    location: &'static str,
    /// The burst window of this call site only.
    window: Mutex<Window>,
    /// Adds the site to the flush registry once, on its first event.
    registered: Once,
}

impl Site {
    /// Construct a call-site budget without allocation.
    pub(crate) const fn new(class: Class, location: &'static str) -> Self {
        Self { class, location, window: Mutex::new(Window::new()), registered: Once::new() }
    }

    /// Record an occurrence without inspecting its diagnostic payload.
    fn observe(&self, now: Instant) -> (bool, Option<Summary>) {
        self.window.lock().unwrap_or_else(PoisonError::into_inner).observe(now)
    }

    /// Drain an elapsed suppression window.
    fn flush(&self, now: Instant) -> Option<Summary> {
        self.window.lock().unwrap_or_else(PoisonError::into_inner).flush(now)
    }
}

/// Sites that emitted at least once. Only compile-time call sites can register, so the length is
/// bounded by the number of `bounded_*!` calls in this crate. No remote data adds an entry.
type Registry = Mutex<Vec<&'static Site>>;

/// Process-wide registry; short locks protect counters only, never formatting or a tracing sink.
static SITES: Registry = Mutex::new(Vec::new());

/// Emit one summary and count the suppressed events with fixed class and site labels.
fn report(site: &Site, summary: Summary) {
    metrics::counter!(
        "tn_network.diagnostics_suppressed_total",
        "class" => site.class.label(),
        "site" => site.location,
    )
    .increment(summary.suppressed());
    tracing::warn!(
        target: "network::diagnostics",
        event_class = ?site.class,
        site = %site.location,
        emitted = summary.emitted(),
        suppressed = summary.suppressed(),
        window_seconds = summary.elapsed().as_secs_f64(),
        "repeated network diagnostics suppressed"
    );
}

/// Emit a diagnostic using a supplied clock and registry, with sinks outside every lock.
fn emit_at(registry: &Registry, site: &'static Site, now: Instant, event: impl FnOnce()) {
    site.registered.call_once(|| {
        registry.lock().unwrap_or_else(PoisonError::into_inner).push(site);
    });
    let (emit, summary) = site.observe(now);
    summary.into_iter().for_each(|summary| report(site, summary));
    if emit {
        event();
    }
}

/// Admit a detailed diagnostic before any field or error formatting is evaluated.
pub(crate) fn emit(site: &'static Site, event: impl FnOnce()) {
    emit_at(&SITES, site, Instant::now(), event);
}

/// Drain summaries using a supplied clock, including quiet tails.
fn flush_at(registry: &Registry, now: Instant) {
    let summaries: Vec<(&'static Site, Summary)> = registry
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .filter_map(|site| site.flush(now).map(|summary| (*site, summary)))
        .collect();
    summaries.into_iter().for_each(|(site, summary)| report(site, summary));
}

/// Report suppressed tails even when the offending input has stopped arriving.
pub(crate) fn flush() {
    flush_at(&SITES, Instant::now());
}

/// Gate all tracing arguments so a suppressed event cannot format its payload.
macro_rules! bounded_warn {
    ($class:ident, $($fields:tt)*) => {{
        static SITE: $crate::diagnostics::Site = $crate::diagnostics::Site::new(
            $crate::diagnostics::Class::$class,
            concat!(file!(), ":", line!()),
        );
        $crate::diagnostics::emit(&SITE, || {
            tracing::warn!($($fields)*);
        });
    }};
}

/// Preserve error severity with the same per-call-site budget as warnings.
macro_rules! bounded_error {
    ($class:ident, $($fields:tt)*) => {{
        static SITE: $crate::diagnostics::Site = $crate::diagnostics::Site::new(
            $crate::diagnostics::Class::$class,
            concat!(file!(), ":", line!()),
        );
        $crate::diagnostics::emit(&SITE, || {
            tracing::error!($($fields)*);
        });
    }};
}

/// Gate the exported `send_or_log_error!` macro's caller-resolved `error!` at network call sites.
macro_rules! bounded_reply_error {
    ($($fields:tt)*) => {
        $crate::diagnostics::bounded_error!(Reply, $($fields)*)
    };
}

pub(crate) use bounded_error;
pub(crate) use bounded_reply_error;
pub(crate) use bounded_warn;

#[cfg(test)]
#[path = "tests/diagnostics.rs"]
pub(crate) mod emission_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A saturated call site cannot suppress another call site in the same class.
    #[test]
    fn call_sites_have_independent_windows() {
        static REMOTE: Site = Site::new(Class::Store, "remote:1");
        static LOCAL: Site = Site::new(Class::Store, "local:2");
        let registry = Registry::new(Vec::new());
        let start = Instant::now();
        let admitted = AtomicUsize::new(0);
        (0..10_000).for_each(|_| emit_at(&registry, &REMOTE, start, || {}));
        emit_at(&registry, &LOCAL, start, || {
            admitted.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(admitted.load(Ordering::Relaxed), 1, "the local site keeps its first detail");
        assert_eq!(registry.lock().unwrap_or_else(PoisonError::into_inner).len(), 2);
        assert!(REMOTE.flush(start + INTERVAL).is_some_and(|summary| {
            (summary.emitted(), summary.suppressed()) == (window::BURST, 10_000 - window::BURST)
        }));
        assert!(LOCAL.flush(start + INTERVAL).is_none());
    }
}
