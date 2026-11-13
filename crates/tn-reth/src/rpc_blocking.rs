//! A blocking pool of RPC's own.
//!
//! reth's RPC runs its blocking work (state reads, `eth_call`, replays, traces, cache misses)
//! through a task spawner's `spawn_blocking_task`, which by default lands on the tokio runtime's
//! one blocking pool. Block execution and certificate validation run their blocking work in the
//! same pool, so RPC load could take the threads they need: every worker's RPC server bounds its
//! own work with its own permits, and those bounds add up across workers.
//!
//! [`RpcTaskSpawner`] sends the blocking work to a separate runtime's pool instead, and passes
//! every other spawn to the spawner it wraps unchanged. The runtime is created once per process
//! and shared by every worker's RPC server, each built once when its worker first starts and
//! kept across epochs. It is never dropped: a tokio runtime cannot be dropped from async code.

use futures::future::BoxFuture;
use parking_lot::Mutex;
use reth_tasks::TaskSpawner;
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
};

/// The most threads RPC's blocking work may use at once: tokio's default for a runtime's
/// blocking pool, the bound RPC shared with the node before it had a pool of its own.
const MAX_BLOCKING_THREADS: usize = 512;

/// The runtime whose blocking pool runs RPC's blocking work.
///
/// Built on first use. One worker thread drives the timers and I/O of the futures the blocking
/// threads run; the work itself runs on the blocking threads.
fn rpc_runtime() -> std::io::Result<&'static Runtime> {
    static RUNTIME: Mutex<Option<&'static Runtime>> = Mutex::new(None);
    let mut runtime = RUNTIME.lock();
    if let Some(runtime) = *runtime {
        return Ok(runtime);
    }
    let built = Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(MAX_BLOCKING_THREADS)
        .thread_name("tn-rpc-blocking")
        .enable_all()
        .build()?;
    // Leaked on purpose: see the module docs.
    let built: &'static Runtime = Box::leak(Box::new(built));
    *runtime = Some(built);
    Ok(built)
}

/// A [`TaskSpawner`] that runs blocking tasks on RPC's own pool and passes every other task to
/// the spawner it wraps.
///
/// Critical blocking tasks also stay with the wrapped spawner, so a spawner that tracks critical
/// tasks (TN's [`tn_types::TaskSpawner`]) keeps tracking them. reth's RPC spawns none.
#[derive(Clone, Debug)]
pub(crate) struct RpcTaskSpawner<S> {
    /// Takes every task that is not plain blocking work.
    inner: S,
    /// The runtime whose blocking pool runs the blocking work.
    blocking: &'static Runtime,
}

impl<S> RpcTaskSpawner<S> {
    /// Wrap `inner`, creating RPC's blocking pool on first use.
    pub(crate) fn new(inner: S) -> std::io::Result<Self> {
        Ok(Self { inner, blocking: rpc_runtime()? })
    }
}

impl<S: TaskSpawner + Clone + 'static> TaskSpawner for RpcTaskSpawner<S> {
    fn spawn_task(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.inner.spawn_task(fut)
    }

    fn spawn_critical_task(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        self.inner.spawn_critical_task(name, fut)
    }

    fn spawn_blocking_task(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        let handle = self.blocking.handle().clone();
        self.blocking.spawn_blocking(move || handle.block_on(fut))
    }

    fn spawn_critical_blocking_task(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        self.inner.spawn_critical_blocking_task(name, fut)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_tasks::TokioTaskExecutor;
    use std::{sync::mpsc, time::Duration};

    /// The name of the thread a blocking task runs on.
    async fn blocking_thread_name(spawner: &impl TaskSpawner) -> String {
        let (tx, rx) = tokio::sync::oneshot::channel();
        spawner.spawn_blocking_task(Box::pin(async move {
            let _ = tx.send(std::thread::current().name().unwrap_or_default().to_string());
        }));
        rx.await.expect("the blocking task ran")
    }

    #[tokio::test]
    async fn blocking_work_runs_on_the_rpc_pool() {
        let spawner = RpcTaskSpawner::new(TokioTaskExecutor::default()).expect("rpc runtime");
        assert_eq!(blocking_thread_name(&spawner).await, "tn-rpc-blocking");
        // the stock executor uses the caller's runtime, as RPC did before
        assert_ne!(blocking_thread_name(&TokioTaskExecutor::default()).await, "tn-rpc-blocking");
    }

    /// With the node's blocking pool full, RPC's blocking work still runs, and timers inside it
    /// still fire.
    #[test]
    fn blocking_work_runs_while_the_node_pool_is_full() {
        let node = Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("node runtime");
        let (release, released) = mpsc::channel::<()>();
        let occupied = node.spawn_blocking(move || {
            let _ = released.recv();
        });
        let ran = node.block_on(async {
            let spawner = RpcTaskSpawner::new(TokioTaskExecutor::default()).expect("rpc runtime");
            let (tx, rx) = tokio::sync::oneshot::channel();
            spawner.spawn_blocking_task(Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let _ = tx.send(());
            }));
            tokio::time::timeout(Duration::from_secs(10), rx).await
        });
        assert!(matches!(ran, Ok(Ok(()))), "rpc blocking work waited for the node's pool");
        drop(release);
        node.block_on(occupied).expect("the node's blocking task ends");
    }
}
