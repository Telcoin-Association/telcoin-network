//! The environment's failure latch: the first failure of a write, a sync, a commit or a growth,
//! or damage found by a read, stops every later write and commit, which return
//! [`Error::Failed`] with the first cause. Reads continue. The latch is never cleared: only a
//! reopen, which recovers from the meta pages, clears it. A read-only environment has no writer:
//! its `begin_rw` returns [`Error::ReadOnly`] whether it latched or not, and the latch only
//! records the first damage its reads found.

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
        tracing::error!(target: "tn_lmdb", "{kind}: {cause}; every later write and commit fails");
        *first = Some(cause);
        self.failed.store(true, Ordering::Release);
    }

    /// Latch the failure `error` stands for, if it is one ([`Error::latches`]): damage
    /// ([`Error::Corrupt`]) through [`Self::fail_damaged`], any other through [`Self::fail`].
    /// Validation errors (an entry over the limits, an append out of order, an insert of a present
    /// key, a table error, a write asked of a read-only environment) change nothing and do not
    /// latch.
    pub(crate) fn latch_error(&self, error: &Error) {
        match error {
            Error::Corrupt(cause) => self.fail_damaged(cause.clone()),
            e if e.latches() => self.fail(e.to_string()),
            _ => {}
        }
    }

    /// True once anything has failed.
    #[inline]
    pub(crate) fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// `Err(Failed(first cause))` once anything has failed. Every write checks it first, so the
    /// error is built out of line.
    #[inline]
    pub(crate) fn check(&self) -> Result<()> {
        if !self.is_failed() {
            return Ok(());
        }
        Err(self.failed_error())
    }

    /// [`Self::check`]'s error.
    #[cold]
    #[inline(never)]
    fn failed_error(&self) -> Error {
        Error::Failed(self.failure().unwrap_or_default())
    }

    /// The first failure's cause, if anything has failed.
    pub(crate) fn failure(&self) -> Option<String> {
        self.cause.lock().clone()
    }
}

/// Latches a failure (with the given cause) if dropped while unwinding: a panic in the step it
/// covers leaves state no later write or commit may build on. The step ends with
/// [`Self::disarm`], so a guard made while an unrelated panic unwinds (a commit or sync run by
/// a destructor) latches only if its own step panics.
pub(crate) struct UnwindLatch<'a>(pub(crate) &'a Latch, pub(crate) &'static str);

impl UnwindLatch<'_> {
    /// The covered step finished (returning `Ok` or `Err`): drop the guard without its check.
    #[inline]
    pub(crate) fn disarm(self) {
        std::mem::forget(self);
    }
}

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
        latch.latch_error(&Error::AppendOutOfOrder);
        latch.latch_error(&Error::KeyExists);
        latch.latch_error(&Error::ReadOnly);
        latch.latch_error(&Error::WrongTxn);
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

    /// A guarded step a destructor runs while an unrelated panic unwinds latches only if it
    /// panics itself: one that finishes disarms its guard.
    #[test]
    fn a_step_run_during_an_unrelated_unwind_latches_only_on_its_own_panic() {
        /// Runs a guarded step when dropped; the step panics (caught here) if `.1`.
        struct StepOnDrop<'a>(&'a Latch, bool);
        impl Drop for StepOnDrop<'_> {
            fn drop(&mut self) {
                let (latch, panics) = (self.0, self.1);
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let guard = UnwindLatch(latch, "the step panicked");
                    assert!(!panics, "the step's own panic");
                    guard.disarm();
                }));
            }
        }
        let latch = Latch::default();
        for panics in [false, true] {
            let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _step = StepOnDrop(&latch, panics);
                panic!("unrelated");
            }));
            assert!(caught.is_err());
            assert_eq!(latch.is_failed(), panics, "the step panicked: {panics}");
        }
        assert_eq!(latch.failure().as_deref(), Some("the step panicked"));
    }
}
