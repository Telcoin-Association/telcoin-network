//! The environment's failure latch: the first failure of a write, a sync, a commit or a growth,
//! or damage found by a read, stops every later write and commit, which return
//! [`Error::Failed`] with the first cause. Reads continue. The latch is never cleared: only a
//! reopen, which recovers from the meta pages, clears it.

use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use crate::error::{Error, Result};

/// The failure latch (see the module docs). The first cause wins and only it is logged.
#[derive(Debug, Default)]
pub(crate) struct Latch {
    /// The first failure's cause (never cleared).
    cause: Mutex<Option<String>>,
    /// Set with `cause`: the lock-free check every write makes.
    failed: AtomicBool,
}

impl Latch {
    /// Latch an I/O, growth or commit failure, or a panic part way through an operation: the
    /// outcome of the failed operation is unknown until the file is reopened.
    pub(crate) fn fail(&self, cause: String) {
        self.latch(cause, "a write, sync or commit failed");
    }

    /// Latch damage a read found: the file is as it was (a reopen reads the same damage), but
    /// nothing may be built on it.
    pub(crate) fn fail_damaged(&self, cause: String) {
        self.latch(cause, "damage found");
    }

    fn latch(&self, cause: String, kind: &str) {
        let mut first = self.cause.lock();
        if first.is_some() {
            return;
        }
        tracing::error!(target: "tndb_b", "{kind}: {cause}; every later write and commit fails");
        *first = Some(cause);
        self.failed.store(true, Ordering::Release);
    }

    /// Latch the failure `error` stands for, if it is one: damage ([`Error::Corrupt`]) through
    /// [`Self::fail_damaged`]; an I/O error or a broken invariant ([`Error::Io`],
    /// [`Error::Failed`]) through [`Self::fail`]. Validation errors (an entry over the limits, a
    /// table error) change nothing and do not latch.
    pub(crate) fn latch_error(&self, error: &Error) {
        match error {
            Error::Corrupt(cause) => self.fail_damaged(cause.clone()),
            Error::Io(_) | Error::Failed(_) => self.fail(error.to_string()),
            Error::KeyTooLarge { .. }
            | Error::ValueTooLarge { .. }
            | Error::TableNotFound(_)
            | Error::StaleTable(_)
            | Error::InvalidTableName
            | Error::NotTndbB
            | Error::UnsupportedVersion(_)
            | Error::InvalidPageSize(_)
            | Error::Locked => {}
        }
    }

    /// True once anything has failed.
    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// `Err(Failed(first cause))` once anything has failed.
    pub(crate) fn check(&self) -> Result<()> {
        if !self.is_failed() {
            return Ok(());
        }
        Err(Error::Failed(self.failure().unwrap_or_default()))
    }

    /// The first failure's cause, if anything has failed.
    pub(crate) fn failure(&self) -> Option<String> {
        self.cause.lock().clone()
    }
}

/// Latches a failure (with the given cause) if dropped while unwinding: a panic in the step it
/// covers leaves state no later write or commit may build on.
pub(crate) struct UnwindLatch<'a>(pub(crate) &'a Latch, pub(crate) &'static str);

impl Drop for UnwindLatch<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.fail(self.1.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first cause wins, `check` reports it as `Failed`, and validation errors do not latch.
    #[test]
    fn the_first_failure_wins() {
        let latch = Latch::default();
        assert!(!latch.is_failed() && latch.check().is_ok() && latch.failure().is_none());
        latch.latch_error(&Error::KeyTooLarge { len: 2, max: 1 });
        latch.latch_error(&Error::StaleTable("t".to_string()));
        assert!(!latch.is_failed(), "validation errors change nothing");

        latch.latch_error(&Error::Corrupt("page 7 holds page 9".to_string()));
        assert!(latch.is_failed());
        latch.fail("msync failed".to_string());
        assert_eq!(latch.failure().as_deref(), Some("page 7 holds page 9"));
        assert!(matches!(latch.check(), Err(Error::Failed(c)) if c == "page 7 holds page 9"));
    }

    /// A panic inside a guarded step latches; a guarded step that returns does not.
    #[test]
    fn a_panic_under_the_guard_latches() {
        let latch = Latch::default();
        drop(UnwindLatch(&latch, "unused"));
        assert!(!latch.is_failed());
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = UnwindLatch(&latch, "panic during commit");
            panic!("boom");
        }));
        assert!(caught.is_err());
        assert_eq!(latch.failure().as_deref(), Some("panic during commit"));
    }
}
