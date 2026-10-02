//! Disposable libp2p-tls instrumentation, never linked into the node.

use std::{cell::RefCell, collections::BTreeMap, time::Instant};

use rustls::{crypto::CryptoProvider, NamedGroup};

thread_local! {
    /// Provider override during benchmark configuration construction only.
    static GROUPS: RefCell<Option<Vec<NamedGroup>>> = const { RefCell::new(None) };
    /// Thread-local call counts and elapsed nanoseconds for verification phases.
    static STATS: RefCell<BTreeMap<&'static str, (u64, u128)>> = const { RefCell::new(BTreeMap::new()) };
}

/// Select a benchmark configuration's groups; `None` preserves provider defaults.
pub fn set_groups(groups: Option<Vec<NamedGroup>>) {
    GROUPS.with(|value| *value.borrow_mut() = groups);
}

/// Construct the stock provider, optionally selecting benchmark-only membership and order.
pub fn provider() -> CryptoProvider {
    let mut provider = rustls::crypto::aws_lc_rs::default_provider();
    GROUPS.with(|value| {
        value.borrow().iter().for_each(|groups| {
            provider.kx_groups = groups
                .iter()
                .filter_map(|name| {
                    rustls::crypto::aws_lc_rs::ALL_KX_GROUPS
                        .iter()
                        .find(|group| group.name() == *name)
                        .copied()
                })
                .collect();
        });
    });
    provider
}

/// Take and clear this thread's phase counts and elapsed nanoseconds.
pub fn take_stats() -> BTreeMap<&'static str, (u64, u128)> {
    STATS.with(|stats| std::mem::take(&mut *stats.borrow_mut()))
}

/// An elapsed-time measurement that also records unsuccessful verification calls.
pub struct Span {
    /// Verification phase being measured.
    phase: &'static str,
    /// Monotonic start time, including profiling overhead.
    start: Instant,
}

impl Span {
    /// Start a phase without changing its return value or error handling.
    pub fn new(phase: &'static str) -> Self {
        Self { phase, start: Instant::now() }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos();
        STATS.with(|stats| {
            let mut stats = stats.borrow_mut();
            let entry = stats.entry(self.phase).or_default();
            entry.0 += 1;
            entry.1 += elapsed;
        });
    }
}

/// Time an unchanged expression, preserving its complete result and error semantics.
pub fn measure<T>(phase: &'static str, operation: impl FnOnce() -> T) -> T {
    let _span = Span::new(phase);
    operation()
}
