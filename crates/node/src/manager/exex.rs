//! Exex helper functions for EpochManager.

use futures::FutureExt as _;
use std::panic::AssertUnwindSafe;
use tn_types::TaskError;
use tracing::{error, warn};

/// Run an ExEx future as an isolated unit of work.
///
/// Contains panics via `catch_unwind` and translates every outcome (finished /
/// errored / panicked) into a structured log. **Always resolves `Ok(())`** so
/// the surrounding NON-critical task never triggers a node-wide shutdown: ExExes
/// are optional, possibly third-party extensions and must not be able to take
/// the node down. See finding #1 in the ExEx remediation plan.
pub(super) async fn run_isolated_exex_future<F>(label: String, fut: F) -> Result<(), TaskError>
where
    F: std::future::Future<Output = eyre::Result<()>> + Send,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(Ok(())) => {
            warn!(target: "exex", %label, "ExEx future finished; delivery for it stopped");
        }
        Ok(Err(e)) => {
            error!(target: "exex", %label, ?e, "ExEx future failed; delivery for it stopped");
        }
        Err(panic) => {
            error!(
                target: "exex",
                %label,
                panic = %exex_panic_message(panic.as_ref()),
                "ExEx future panicked; delivery for it stopped",
            );
        }
    }
    Ok(())
}

/// Run an ExEx future as a CRITICAL unit of work.
///
/// Used only when `Config::exex_critical` is set — i.e. the operator declared
/// this ExEx load-bearing and wants its failure to stop the node. This is the
/// opposite of [`run_isolated_exex_future`]:
///
/// - it does **not** `catch_unwind`, so a panic propagates to the task manager as a critical join
///   error (node-wide shutdown);
/// - a returned error maps to a [`TaskError`] (critical exit → shutdown);
/// - a clean finish returns `Ok(())`; the task manager treats a resolved critical task as a
///   shutdown trigger too (`CriticalExitOk`), unless the node is already shutting down.
pub(super) async fn run_critical_exex_future<F>(label: String, fut: F) -> Result<(), TaskError>
where
    F: std::future::Future<Output = eyre::Result<()>> + Send,
{
    match fut.await {
        Ok(()) => {
            warn!(target: "exex", %label, "critical ExEx future finished; node will shut down");
            Ok(())
        }
        Err(e) => {
            error!(target: "exex", %label, ?e, "critical ExEx future failed; node will shut down");
            Err(TaskError::from_message(format!("critical exex {label} failed: {e}")))
        }
    }
}

/// Extract a human-readable message from a caught panic payload.
///
/// Panic payloads are `&'static str` (from `panic!("literal")`) or `String`
/// (from formatted panics); anything else is reported generically.
fn exex_panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

#[cfg(test)]
mod exex_isolation_tests {
    //! ExEx task spawning behavior. By default a buggy or finished ExEx is
    //! isolated and must never shut the node down (remediation finding #1). When
    //! `Config::exex_critical` is set, the operator has declared the ExEx
    //! load-bearing and a failing one instead shuts the node down.
    use super::{run_critical_exex_future, run_isolated_exex_future};
    use std::time::Duration;
    use tn_types::{Notifier, TaskError, TaskJoinError, TaskManager};

    #[tokio::test]
    async fn isolated_exex_future_contains_panic_error_and_completion() {
        // A panicking ExEx is contained and reported as a clean `Ok(())`, so the
        // surrounding non-critical task resolves normally — no propagated unwind.
        assert!(run_isolated_exex_future("panic".to_string(), async {
            panic!("buggy exex");
        })
        .await
        .is_ok());

        // An ExEx that returns an error is likewise contained.
        assert!(run_isolated_exex_future("error".to_string(), async {
            Err(eyre::eyre!("exex failed"))
        })
        .await
        .is_ok());

        // A normally-finishing ExEx resolves `Ok(())`.
        assert!(run_isolated_exex_future("done".to_string(), async { Ok(()) }).await.is_ok());
    }

    #[tokio::test]
    async fn panicking_exex_does_not_shut_down_node() {
        // End-to-end: ExExes are spawned NON-critical and wrapped by
        // `run_isolated_exex_future`, exactly as the node does. A panicking or
        // finished ExEx must not trigger the critical-task shutdown path; only
        // the critical "node" task may.
        let mut task_manager = TaskManager::default();

        // Buggy ExEx that panics immediately.
        task_manager.spawn_task(
            "exex-panic",
            run_isolated_exex_future("exex-panic".to_string(), async {
                panic!("buggy exex");
            }),
        );
        // ExEx that simply finishes its job.
        task_manager.spawn_task(
            "exex-done",
            run_isolated_exex_future("exex-done".to_string(), async { Ok(()) }),
        );

        // Critical sentinel standing in for the node: stays up until signaled.
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        task_manager.spawn_critical_task("node-alive", async move {
            let _ = stop_rx.await;
            Err(TaskError::from_message("node-stopped-by-test"))
        });

        // Let the non-critical ExEx tasks panic/finish.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The node must still be up: only now do we stop the sentinel. If the
        // panicking/finished ExExes had shut the node down, `join` would have
        // returned with one of THEIR names instead of "node-alive".
        stop_tx.send(()).expect("sentinel still running");
        match task_manager.join(Notifier::default()).await {
            Err(TaskJoinError::CriticalExitError(name, _)) => {
                assert_eq!(name, "node-alive", "shutdown must originate from the critical task");
            }
            other => panic!("unexpected join result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn critical_exex_future_propagates_error_and_clean_finish() {
        // The critical runner is the opposite of the isolated one: a failing
        // ExEx surfaces as `Err(TaskError)` (which the task manager turns into a
        // node-wide shutdown), while a clean finish returns `Ok(())` (the task
        // manager treats a resolved critical task as a shutdown trigger too).
        assert!(run_critical_exex_future("error".to_string(), async {
            Err(eyre::eyre!("load-bearing exex failed"))
        })
        .await
        .is_err());

        assert!(run_critical_exex_future("done".to_string(), async { Ok(()) }).await.is_ok());
    }

    #[tokio::test]
    async fn critical_exex_failure_shuts_down_node() {
        // End-to-end counterpart to `panicking_exex_does_not_shut_down_node`: an
        // ExEx spawned CRITICAL (exactly as `exex_critical = true` does) and
        // wrapped by `run_critical_exex_future` MUST shut the node down when it
        // fails. The join reports the critical exex as the shutdown origin.
        let mut task_manager = TaskManager::default();

        task_manager.spawn_critical_task(
            "exex-critical",
            run_critical_exex_future("exex-critical".to_string(), async {
                Err(eyre::eyre!("load-bearing exex failed"))
            }),
        );

        match task_manager.join(Notifier::default()).await {
            Err(TaskJoinError::CriticalExitError(name, _)) => {
                assert_eq!(name, "exex-critical", "shutdown must originate from the critical exex");
            }
            other => panic!("unexpected join result: {other:?}"),
        }
    }
}
