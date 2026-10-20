//! Deterministic checks of the production emission path and its structured tracing output.

use super::*;
use std::{
    fmt,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Event, Metadata, Subscriber,
};

/// Captured structured fields for one tracing event.
#[derive(Debug, Default)]
struct Fields(Vec<(String, String)>);

impl Fields {
    /// Read one field without relying on tracing's field ordering.
    fn value(&self, name: &str) -> Option<&str> {
        self.0.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
    }
}

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }
}

/// Minimal synchronous tracing sink, local to each test.
#[derive(Clone, Debug, Default)]
pub(crate) struct Capture(Arc<Mutex<Vec<Fields>>>);

impl Capture {
    /// Count captured messages matching a fixed diagnostic text.
    pub(crate) fn message_count(&self, text: &str) -> usize {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|fields| fields.value("message").is_some_and(|message| message.contains(text)))
            .count()
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(fields);
    }
}

/// A changing remote error whose formatting work is observable.
struct RemoteError<'a> {
    /// Counter incremented only when the tracing sink formats the error.
    formatted: &'a AtomicUsize,
    /// Remote identity/error variation, never a limiter key.
    identity: u64,
}

impl fmt::Debug for RemoteError<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.formatted.fetch_add(1, Ordering::Relaxed);
        write!(formatter, "malformed input from changing identity {}", self.identity)
    }
}

/// Controlled time proves bounded output, lazy formatting, idle summaries, and recovery.
#[test]
fn emission_is_bounded_and_suppressed_payloads_are_lazy() {
    let start = Instant::now();
    static SITE: Site = Site::new(Class::Swarm, "tests/diagnostics.rs:1");
    let registry = Registry::new(Vec::new());
    let capture = Capture::default();
    let formatted = AtomicUsize::new(0);
    tracing::subscriber::with_default(capture.clone(), || {
        (0..10_004).for_each(|identity| {
            emit_at(&registry, &SITE, start, || {
                assert!(SITE.window.try_lock().is_ok(), "diagnostic sinks run outside the lock");
                tracing::error!(error = ?RemoteError { formatted: &formatted, identity }, "listener error");
            });
        });
        assert_eq!(formatted.load(Ordering::Relaxed), 4);
        flush_at(&registry, start + INTERVAL);
        flush_at(&registry, start + INTERVAL);
        let events = capture.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(events.len(), 5, "four details and exactly one idle summary");
        assert!(events
            .first()
            .and_then(|fields| fields.value("error"))
            .is_some_and(|error| error.contains("identity 0")));
        assert_eq!(events.last().and_then(|fields| fields.value("event_class")), Some("Swarm"));
        assert_eq!(
            events.last().and_then(|fields| fields.value("site")),
            Some("tests/diagnostics.rs:1")
        );
        assert_eq!(events.last().and_then(|fields| fields.value("emitted")), Some("4"));
        assert_eq!(events.last().and_then(|fields| fields.value("suppressed")), Some("10000"));
        assert_eq!(events.last().and_then(|fields| fields.value("window_seconds")), Some("60.0"));
        drop(events);
        emit_at(&registry, &SITE, start + INTERVAL, || {
            tracing::error!(error = ?RemoteError { formatted: &formatted, identity: 10_004 }, "listener error");
        });
        assert_eq!(
            formatted.load(Ordering::Relaxed),
            5,
            "recovery admits useful detail immediately"
        );
    });
}
