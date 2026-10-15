//! Process-wide warning/error budgets for fixed network event classes.
//!
//! Each class admits four detailed events per minute across all swarms. Suppressed fields are
//! never evaluated. Summaries carry only a fixed class name and counts, and an event-loop timer
//! drains quiet tails. See `docs/network-diagnostics.md` for the site and label audit.

#[path = "diagnostics/window.rs"]
mod window;

use std::{sync::Mutex, time::Instant};
use window::{Summary, Window};

pub(crate) use window::INTERVAL;

/// Fixed event classes. No remote data can create another class or another limiter entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Class {
    /// Recoverable transport listener errors.
    Listener,
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
    /// Every class, in the same order as its storage slot.
    const ALL: [Self; 13] = [
        Self::Listener,
        Self::Swarm,
        Self::Command,
        Self::Gossip,
        Self::Request,
        Self::Stream,
        Self::Kademlia,
        Self::Record,
        Self::Store,
        Self::PeerAdmission,
        Self::PeerState,
        Self::PeerRecord,
        Self::Reply,
    ];

    /// Resolve a compile-time class to its fixed storage slot.
    const fn index(self) -> usize {
        match self {
            Self::Listener => 0,
            Self::Swarm => 1,
            Self::Command => 2,
            Self::Gossip => 3,
            Self::Request => 4,
            Self::Stream => 5,
            Self::Kademlia => 6,
            Self::Record => 7,
            Self::Store => 8,
            Self::PeerAdmission => 9,
            Self::PeerState => 10,
            Self::PeerRecord => 11,
            Self::Reply => 12,
        }
    }
}

/// Fixed storage shared across primary and worker swarms and their peer/record helpers.
#[derive(Debug)]
struct Diagnostics {
    /// One window per class, never per peer, address, message, or error string.
    windows: [Window; Class::ALL.len()],
}

impl Diagnostics {
    /// Construct every window without allocation.
    const fn new() -> Self {
        Self { windows: [const { Window::new() }; Class::ALL.len()] }
    }

    /// Record an occurrence without inspecting its diagnostic payload.
    fn observe(&mut self, class: Class, now: Instant) -> (bool, Option<Summary>) {
        self.windows.get_mut(class.index()).map_or((false, None), |window| window.observe(now))
    }

    /// Drain elapsed suppression windows into fixed-size storage before calling a tracing sink.
    fn flush(&mut self, now: Instant) -> [Option<Summary>; Class::ALL.len()] {
        std::array::from_fn(|index| {
            self.windows.get_mut(index).and_then(|window| window.flush(now))
        })
    }
}

/// Process-wide budget; a short lock protects counters only, never formatting or a tracing sink.
static DIAGNOSTICS: Mutex<Diagnostics> = Mutex::new(Diagnostics::new());

/// Emit one summary with a bounded class vocabulary and the actual observation window.
fn report(class: Class, summary: Summary) {
    tracing::warn!(
        target: "network::diagnostics",
        event_class = ?class,
        emitted = summary.emitted,
        suppressed = summary.suppressed,
        window_seconds = summary.elapsed.as_secs_f64(),
        "repeated network diagnostics suppressed"
    );
}

/// Emit a diagnostic using a supplied clock and fixed storage, with sinks outside the lock.
fn emit_at(state: &Mutex<Diagnostics>, class: Class, now: Instant, event: impl FnOnce()) {
    let (emit, summary) =
        state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).observe(class, now);
    summary.into_iter().for_each(|summary| report(class, summary));
    if emit {
        event();
    }
}

/// Admit a detailed diagnostic before any field or error formatting is evaluated.
pub(crate) fn emit(class: Class, event: impl FnOnce()) {
    emit_at(&DIAGNOSTICS, class, Instant::now(), event);
}

/// Drain summaries using a supplied clock, including quiet tails.
fn flush_at(state: &Mutex<Diagnostics>, now: Instant) {
    let summaries = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).flush(now);
    Class::ALL.into_iter().zip(summaries).for_each(|(class, summary)| {
        summary.into_iter().for_each(|summary| report(class, summary));
    });
}

/// Report suppressed tails even when the offending input has stopped arriving.
pub(crate) fn flush() {
    flush_at(&DIAGNOSTICS, Instant::now());
}

/// Gate all tracing arguments so a suppressed event cannot format its payload.
macro_rules! bounded_warn {
    ($class:ident, $($fields:tt)*) => {{
        $crate::diagnostics::emit($crate::diagnostics::Class::$class, || {
            tracing::warn!($($fields)*);
        });
    }};
}

/// Preserve error severity while sharing the same fixed per-class budget as warnings.
macro_rules! bounded_error {
    ($class:ident, $($fields:tt)*) => {{
        $crate::diagnostics::emit($crate::diagnostics::Class::$class, || {
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

    /// Every fixed class has an independent burst, and churn cannot change storage size.
    #[test]
    fn classes_have_fixed_independent_windows() {
        let start = Instant::now();
        let mut diagnostics = Diagnostics::new();
        let bytes = std::mem::size_of_val(&diagnostics);
        Class::ALL.into_iter().enumerate().for_each(|(index, class)| {
            assert_eq!(class.index(), index);
            (0..window::BURST)
                .for_each(|_| assert_eq!(diagnostics.observe(class, start), (true, None)));
            (0..10_000).for_each(|_| assert_eq!(diagnostics.observe(class, start), (false, None)));
        });
        assert_eq!(std::mem::size_of_val(&diagnostics), bytes);
        assert!(diagnostics.flush(start + INTERVAL).into_iter().all(|summary| {
            summary
                == Some(Summary { emitted: window::BURST, suppressed: 10_000, elapsed: INTERVAL })
        }));
        assert!(diagnostics.flush(start + INTERVAL).into_iter().all(|summary| summary.is_none()));
        Class::ALL.into_iter().for_each(|class| {
            assert_eq!(diagnostics.observe(class, start + INTERVAL), (true, None));
        });
    }
}
