//! A blocking pool for each worker's RPC server.
//!
//! reth's RPC runs its blocking work (state reads, `eth_call`, replays, traces, log scans, cache
//! misses) through a task spawner's `spawn_blocking_task`, which by default lands on the tokio
//! runtime's one blocking pool. Block execution and certificate validation run their blocking
//! work in that pool too, so RPC load could take the threads they need.
//!
//! [`RpcTaskSpawner`] sends the blocking work to an [`RpcBlockingPool`] instead, and passes every
//! other spawn to the spawner it wraps unchanged. `get_rpc_server` builds one pool for each
//! server it returns, and both of that server's spawners use it: the RPC modules' (log scans) and
//! the eth API's (calls, state reads, traces, cache misses). A node builds one server per worker,
//! when the worker first starts, and keeps it across epochs.
//!
//! Some blocking work waits for other blocking work on the same pool. A log scan holds one of
//! its server's blocking-IO permits (`max_blocking_io_requests`, 256 by default) while it waits
//! for its cache misses and its parallel chunks. A pool has 256 more threads than its server has
//! permits (512 by default), so permit holders never hold all of its threads, and the work they
//! wait for gets a thread once one frees. With one pool for every server, the permits of two
//! servers would add up to the pool's threads: log scans flooding two workers could take every
//! thread while the work they wait for queues, and RPC would stall on every worker.
//!
//! With `--rpc.pending-block` set (the default is `none`), a state read at `pending` holds a
//! thread without a permit while it waits for the pending-block build on the same pool, so enough
//! of those reads held open can still take every thread of their own server's pool.
//!
//! tokio starts a pool's blocking threads on demand and ends each after 10 seconds idle, so an
//! idle pool keeps one thread: its runtime's worker, which drives the timers and I/O of the
//! futures the blocking threads run.
//!
//! A pool lives while a spawner holds it (the server's modules, its eth API and its state-cache
//! task), then shuts down without waiting for its threads. Dropping a runtime the usual way waits
//! for its blocking threads, which panics in async code and, on one of the pool's own threads,
//! would wait for itself.

use futures::future::BoxFuture;
use reth_tasks::TaskSpawner;
use std::sync::Arc;
use tokio::{
    runtime::{Builder, Handle, Runtime},
    task::JoinHandle,
};

/// The threads a pool has beyond its server's blocking-IO permits, so permit holders never hold
/// all of its threads (see the module docs). With reth's default of 256 permits a pool has 512
/// threads, tokio's default for a runtime's blocking pool.
const THREADS_BEYOND_PERMITS: usize = 256;

/// One RPC server's blocking pool: the blocking threads of a runtime of its own.
///
/// One worker thread drives the timers and I/O of the futures the blocking threads run; the work
/// itself runs on the blocking threads.
#[derive(Debug)]
pub(crate) struct RpcBlockingPool {
    /// Spawns the blocking tasks and runs their futures.
    handle: Handle,
    /// `None` only while the pool drops.
    runtime: Option<Runtime>,
}

impl RpcBlockingPool {
    /// Build the pool of a server whose eth API has `blocking_io_permits` blocking-IO permits.
    pub(crate) fn new(blocking_io_permits: usize) -> std::io::Result<Arc<Self>> {
        Self::with_max_threads(blocking_io_permits.saturating_add(THREADS_BEYOND_PERMITS))
    }

    /// Build a pool of at most `max_threads` blocking threads (at least 1).
    fn with_max_threads(max_threads: usize) -> std::io::Result<Arc<Self>> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(max_threads)
            .thread_name("tn-rpc-blocking")
            .enable_all()
            .build()?;
        Ok(Arc::new(Self { handle: runtime.handle().clone(), runtime: Some(runtime) }))
    }

    /// A spawner that runs blocking tasks on this pool and passes every other task to `inner`.
    pub(crate) fn spawner<S>(self: &Arc<Self>, inner: S) -> RpcTaskSpawner<S> {
        RpcTaskSpawner { inner, pool: Arc::clone(self) }
    }
}

impl Drop for RpcBlockingPool {
    fn drop(&mut self) {
        // Does not wait for the blocking threads: see the module docs.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// A [`TaskSpawner`] that runs blocking tasks on its server's [`RpcBlockingPool`] and passes every
/// other task to the spawner it wraps.
///
/// Critical blocking tasks also stay with the wrapped spawner, so a spawner that tracks critical
/// tasks (TN's [`tn_types::TaskSpawner`]) keeps tracking them. reth's RPC spawns none.
#[derive(Clone, Debug)]
pub(crate) struct RpcTaskSpawner<S> {
    /// Takes every task that is not plain blocking work.
    inner: S,
    /// Runs the blocking work.
    pool: Arc<RpcBlockingPool>,
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
        let handle = self.pool.handle.clone();
        self.pool.handle.spawn_blocking(move || handle.block_on(fut))
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
    use tokio::{sync::oneshot, time::timeout};

    /// A spawner on a new pool of `max_threads` threads.
    fn new_spawner(max_threads: usize) -> RpcTaskSpawner<TokioTaskExecutor> {
        RpcBlockingPool::with_max_threads(max_threads)
            .expect("rpc runtime")
            .spawner(TokioTaskExecutor::default())
    }

    /// The name of the thread a blocking task runs on.
    async fn blocking_thread_name(spawner: &impl TaskSpawner) -> String {
        let (tx, rx) = oneshot::channel();
        spawner.spawn_blocking_task(Box::pin(async move {
            let _ = tx.send(std::thread::current().name().unwrap_or_default().to_string());
        }));
        rx.await.expect("the blocking task ran")
    }

    /// Whether a blocking task spawned on `spawner` runs within 10 seconds.
    async fn runs(spawner: &impl TaskSpawner) -> bool {
        let (tx, rx) = oneshot::channel();
        spawner.spawn_blocking_task(Box::pin(async move {
            let _ = tx.send(());
        }));
        matches!(timeout(Duration::from_secs(10), rx).await, Ok(Ok(())))
    }

    #[tokio::test]
    async fn blocking_work_runs_on_the_rpc_pool() {
        let pool = RpcBlockingPool::new(1).expect("rpc runtime");
        let spawner = pool.spawner(TokioTaskExecutor::default());
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
            let spawner = new_spawner(1);
            let (tx, rx) = oneshot::channel();
            spawner.spawn_blocking_task(Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let _ = tx.send(());
            }));
            timeout(Duration::from_secs(10), rx).await
        });
        assert!(matches!(ran, Ok(Ok(()))), "rpc blocking work waited for the node's pool");
        drop(release);
        node.block_on(occupied).expect("the node's blocking task ends");
    }

    /// Two servers' pools, as `get_rpc_server` builds them for two workers: with every thread of
    /// one taken by work that does not end, the other still runs its blocking work.
    #[tokio::test]
    async fn a_full_pool_does_not_hold_up_another_servers_work() {
        let flooded = new_spawner(1);
        let other = new_spawner(1);

        // take the flooded pool's only thread until released
        let (release, released) = mpsc::channel::<()>();
        let (started_tx, started) = oneshot::channel();
        flooded.spawn_blocking_task(Box::pin(async move {
            let _ = started_tx.send(());
            let _ = released.recv();
        }));
        started.await.expect("the flooding task runs");
        let (queued_tx, mut queued) = oneshot::channel();
        flooded.spawn_blocking_task(Box::pin(async move {
            let _ = queued_tx.send(());
        }));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(queued.try_recv().is_err(), "the flooded pool still had a free thread");

        assert!(runs(&other).await, "the other server's blocking work waited for the flooded pool");

        drop(release);
        let queued = timeout(Duration::from_secs(10), queued).await;
        assert!(matches!(queued, Ok(Ok(()))), "the flooded pool's queued work never ran");
    }

    /// The last spawner may drop its pool in async code or on one of the pool's own blocking
    /// threads: the pool shuts down there without panicking or waiting for itself.
    #[tokio::test]
    async fn a_pool_shuts_down_where_its_last_spawner_drops() {
        // in async code
        let spawner = new_spawner(2);
        assert!(runs(&spawner).await);
        drop(spawner);

        // on the pool's own blocking thread
        let spawner = new_spawner(2);
        let last = spawner.clone();
        let (outer_dropped, wait_outer) = mpsc::channel::<()>();
        let (done_tx, done) = oneshot::channel();
        spawner.spawn_blocking_task(Box::pin(async move {
            let _ = wait_outer.recv();
            drop(last);
            let _ = done_tx.send(());
        }));
        drop(spawner);
        outer_dropped.send(()).expect("the blocking task waits");
        let done = timeout(Duration::from_secs(10), done).await;
        assert!(matches!(done, Ok(Ok(()))), "dropping the pool on its own thread did not return");
    }
}
