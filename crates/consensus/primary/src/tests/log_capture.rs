//! Test-only capture of the `consensus::metrics` stage lines, for tests that pin a line's fields.
//!
//! The crate has no `tracing-subscriber` dependency, so this is a minimal [`Subscriber`] that keeps
//! the events of one target and nothing else. Install it with [`tracing::subscriber::set_default`]
//! on a current-thread runtime so every task the test spawns logs through it.

use parking_lot::Mutex;
use std::{fmt, sync::Arc};
use tracing::{
    field::{Field, Visit},
    span,
    subscriber::Interest,
    Event, Metadata, Subscriber,
};

/// One captured event: its message and its other fields, in the order the macro lists them.
#[derive(Clone, Debug)]
pub(crate) struct CapturedEvent {
    /// The event's message text.
    pub(crate) message: String,
    /// `(name, value)` per field other than the message, each value as `Debug` formats it.
    pub(crate) fields: Vec<(&'static str, String)>,
}

impl CapturedEvent {
    /// The formatted value of field `name`, if the event has it.
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields.iter().find(|(field, _)| *field == name).map(|(_, value)| value.as_str())
    }

    /// The value of field `name` parsed as an unsigned integer, if the event has it.
    pub(crate) fn u64_field(&self, name: &str) -> Option<u64> {
        self.field(name).and_then(|value| value.parse().ok())
    }
}

/// A subscriber that records every event of one target and disables everything else.
#[derive(Clone)]
pub(crate) struct LogCapture {
    /// The only target whose events are kept.
    target: &'static str,
    /// The events captured so far, oldest first.
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl LogCapture {
    /// A capture of the events logged under `target`.
    pub(crate) fn new(target: &'static str) -> Self {
        Self { target, events: Arc::default() }
    }

    /// The captured events whose message is `message`, oldest first.
    pub(crate) fn events(&self, message: &str) -> Vec<CapturedEvent> {
        self.events.lock().iter().filter(|event| event.message == message).cloned().collect()
    }
}

impl Subscriber for LogCapture {
    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
        // ask per event: the callsite cache is process-wide and other subscribers may exist
        Interest::sometimes()
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event() && metadata.target() == self.target
    }

    fn new_span(&self, _span: &span::Attributes<'_>) -> span::Id {
        // spans are never enabled, so no id is ever looked up
        span::Id::from_u64(1)
    }

    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}

    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        self.events.lock().push(CapturedEvent { message: visitor.message, fields: visitor.fields });
    }

    fn enter(&self, _span: &span::Id) {}

    fn exit(&self, _span: &span::Id) {}
}

/// Collects one event's message and fields.
#[derive(Default)]
struct FieldVisitor {
    /// The `message` field.
    message: String,
    /// Every other field.
    fields: Vec<(&'static str, String)>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        // plain text, so a test compares label values without the quotes `Debug` adds
        if field.name() == "message" {
            self.message = value.to_owned();
        } else {
            self.fields.push((field.name(), value.to_owned()));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.fields.push((field.name(), format!("{value:?}")));
        }
    }
}
