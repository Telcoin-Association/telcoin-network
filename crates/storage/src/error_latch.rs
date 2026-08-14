//! First-error-wins writes for the background-thread error latches.

use tokio::sync::watch;

/// Latch `error` into `slot` only when the slot is empty.
///
/// The store loops latch each failed background write into a `watch` slot. A plain
/// `send_replace` is last-write-wins: a later failure overwrites an earlier one before a
/// reader can observe it, so the first failure of a cascade is lost. This helper keeps the
/// first error. The pack itself also replays the root cause of its failed state on every
/// later append and commit (flush is not guarded), so in that cascade both layers report
/// the same root cause; the latch stays as defense in depth for failures that do not share
/// one root cause. A reader that drains the slot still acknowledges exactly one failure;
/// first-write-wins only changes which failure that is. Callers log every failure, so the
/// errors the latch does not keep stay visible.
pub(crate) fn latch_first_error<E>(slot: &watch::Sender<Option<E>>, error: E) {
    slot.send_if_modified(|current| {
        let vacant = current.is_none();
        if vacant {
            *current = Some(error);
        }
        vacant
    });
}

#[cfg(test)]
mod test {
    use super::latch_first_error;

    #[test]
    fn keeps_the_first_error() {
        let (slot, mut reader) = tokio::sync::watch::channel(None);
        latch_first_error(&slot, "root cause");
        assert!(reader.has_changed().expect("sender alive"), "the first latch notifies");
        assert_eq!(*reader.borrow_and_update(), Some("root cause"));
        latch_first_error(&slot, "follow-on failure");
        assert!(
            !reader.has_changed().expect("sender alive"),
            "a suppressed follow-on write does not notify"
        );
        assert_eq!(*reader.borrow(), Some("root cause"));
    }
}
