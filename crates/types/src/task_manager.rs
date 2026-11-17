//! Task manager interface to spawn tasks to the tokio runtime.

use crate::{error::NodeForked, ShutdownNotifier};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{
    fmt::{Debug, Display},
    future::Future,
    pin::pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinHandle},
};

/// Trait that encompasses the required traits for a TaskError.
pub trait TaskErrorTrait: std::error::Error + Send + Sync + 'static {}

impl<E: std::error::Error + Send + Sync + 'static> TaskErrorTrait for E {}

/// Used internally to wrap a simple message error when there is nothing better to wrap.
#[derive(Clone, Debug)]
struct TaskStrErr(String);

impl std::error::Error for TaskStrErr {}
impl Display for TaskStrErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error type for tasks.
/// Reports an error happened and gets it's string.
/// Wraps the original error.
#[derive(Clone, Debug)]
pub struct TaskError {
    error: Arc<dyn TaskErrorTrait>,
}

impl TaskError {
    /// Retrun a reference to the contained error.
    pub fn error(&self) -> &dyn TaskErrorTrait {
        &*self.error
    }

    /// Produce a TaskError from a String.
    /// We can not just implement From because of the
    /// generic implementation (even though it does not work for a String...).
    pub fn from_message(message: impl ToString) -> Self {
        let err = TaskStrErr(message.to_string());
        err.into()
    }

    /// Produce a TaskError from an `eyre::Report`: its message followed by the messages of its
    /// causes (`{report:#}`). A fork stop ([`NodeForked::is_in`]) stays one, its message wrapped in
    /// [`NodeForked`] again, so the fork crosses the task boundary as a type.
    pub fn from_report(report: eyre::Report) -> Self {
        let message = format!("{report:#}");
        if NodeForked::is_in(&report) {
            NodeForked::new(message).into()
        } else {
            Self::from_message(message)
        }
    }

    /// Whether this is a fork stop: the error, or an error it was caused by, is a [`NodeForked`].
    pub fn is_fork(&self) -> bool {
        let error: &(dyn std::error::Error + 'static) = &*self.error;
        std::iter::successors(Some(error), |error| error.source())
            .any(|error| error.is::<NodeForked>())
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for TaskError {
    fn from(error: E) -> Self {
        Self { error: Arc::new(error) }
    }
}

impl Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

/// Result type for all tasks.
pub type TaskResult = Result<(), TaskError>;

/// Used for the futures that will resolve when tasks do.
/// Allows us to hold a FuturesUnordered directly in the TaskManager struct.
struct TaskHandle {
    /// The  owned permission to join on a task (await its termination).
    handle: JoinHandle<TaskResult>,
    /// The name for the task.
    info: TaskInfo,
}

impl TaskHandle {
    /// Create a new instance of `Self`.
    fn new(name: String, handle: JoinHandle<TaskResult>, critical: bool) -> Self {
        Self { handle, info: TaskInfo { name, critical } }
    }
}

/// The information for task results.
#[derive(Clone, Debug)]
struct TaskInfo {
    /// The name of the task.
    name: String,
    /// Bool indicating if the task is critical. Critical tasks cause the loop to break and force
    /// shutdown.
    critical: bool,
}

impl Future for TaskHandle {
    // Return the `name` and `critical` status for task.
    type Output = Result<TaskInfo, (TaskInfo, TaskJoinError)>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match this.handle.poll_unpin(cx) {
            Poll::Ready(res) => match res {
                Ok(r) => match r {
                    Ok(_) => Poll::Ready(Ok(this.info.clone())),
                    Err(e) => Poll::Ready(Err((
                        this.info.clone(),
                        TaskJoinError::CriticalExitError(this.info.name.clone(), e),
                    ))),
                },
                Err(err) => Poll::Ready(Err((
                    this.info.clone(),
                    TaskJoinError::CriticalJoinError(this.info.name.clone(), err),
                ))),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A basic task manager.
///
/// Allows new tasks to be be started on the tokio runtime and tracks
/// there JoinHandles.
pub struct TaskManager {
    tasks: FuturesUnordered<TaskHandle>,
    name: String,
    new_task_rx: mpsc::Receiver<TaskHandle>,
    new_task_tx: mpsc::Sender<TaskHandle>,
    /// This is used to notify any spawned tasks to exit when task manager is dropped.
    /// Otherwise we will end up with orphaned tasks when epochs change.
    local_shutdown: ShutdownNotifier,
    /// How long to wait for joins to complete.  This will be used twice (so double it).
    join_wait_millis: u64,
}

impl Drop for TaskManager {
    fn drop(&mut self) {
        self.local_shutdown.notify();
    }
}

/// The type that can spawn tasks for a parent `TaskManager`.
///
/// The TaskSpawner is clone-able and forwards tasks to the task manager
/// to track. This type lives with other types to spawn short-lived tasks.
#[derive(Clone, Debug)]
pub struct TaskSpawner {
    /// The channel to forward task handles to the parent [TaskManager].
    new_task_tx: mpsc::Sender<TaskHandle>,
    local_shutdown: ShutdownNotifier,
}

impl TaskSpawner {
    /// Spawns a non-critical task on tokio and records it's JoinHandle and name. Other tasks are
    /// unaffected when this task resolves.
    pub fn spawn_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        self.create_task(name, future, false);
    }

    /// Spawns a critical task on tokio and records it's JoinHandle and name.
    ///
    /// The task is tracked as "critical". When the task resolves, other tasks
    /// will shutdown.
    pub fn spawn_critical_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        self.create_task(name, future, true);
    }

    /// The main function to spawn tasks on the `tokio` runtime. These tasks are tracked by the
    /// manager.
    fn create_task<F, S: ToString>(&self, name: S, future: F, critical: bool)
    where
        F: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        let name = name.to_string();
        let rx_shutdown = self.local_shutdown.subscribe();
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = rx_shutdown => {
                    Ok(())
                }
                res = future => {
                    res
                }
            }
        });
        match self.new_task_tx.try_send(TaskHandle::new(name.clone(), handle, critical)) {
            Ok(()) => {}
            // The manager has ended (dropped, which notifies its shutdown): the task subscribed to
            // that notice above, so it ends at once and nothing is left to track. A spawn racing a
            // node stop lands here.
            Err(mpsc::error::TrySendError::Closed(_)) if self.local_shutdown.is_notified() => {
                tracing::debug!(target: "tn::tasks", "task {name} spawned after its task manager ended; it ends at once");
            }
            Err(err) => {
                tracing::error!(target: "tn::tasks", "Failed to track spawned task {name}: {err}. The task is already spawned but untracked: it can never be joined or aborted, and only its shutdown notice can end it.");
            }
        }
    }

    /// Spawns a non-critical, blocking task on tokio and records the JoinHandle and name.
    ///
    /// Other tasks are unaffected when this task resolves.
    /// Note: this essencially spawns a thread on the tokio blocking thread pool.
    /// It WILL NOT be ended early when it's task manager is dropped so use wisely.
    pub fn spawn_blocking_task<F, S: ToString>(&self, name: S, task: F)
    where
        F: FnOnce() -> TaskResult + Send + 'static,
    {
        let name = name.to_string();
        let handle = tokio::task::spawn_blocking(task);
        if let Err(err) = self.new_task_tx.try_send(TaskHandle::new(name.clone(), handle, false)) {
            tracing::error!(target: "tn::tasks", "Task error sending joiner for {name}: {err}");
        }
    }

    /// Internal method to spawn and manage tasks. Critical and blocking bools are used to spawn and
    /// track the correct type.
    fn spawn_reth_task(
        &self,
        name: &str,
        fut: BoxFuture<'static, ()>,
        critical: bool,
        blocking: bool,
    ) -> JoinHandle<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // Need two join handles so do this channel dance to get them.
        // Required because the task manager needs one and this foreign Reth interface return one.
        let rx_shutdown = self.local_shutdown.subscribe();
        let f = async move {
            tokio::select! {
                _ = rx_shutdown => {
                    let _ = tx.send(Ok::<(), TaskError>(()));
                }
                _ = fut => {
                    let _ = tx.send(Ok::<(), TaskError>(()));
                }
            }
            Ok::<(), TaskError>(())
        };
        let rx_shutdown = self.local_shutdown.subscribe();
        let join = tokio::spawn(async move {
            tokio::select! {
                _ = rx_shutdown => { }
                _ = rx => { }
            }
        });

        match (critical, blocking) {
            // critical, blocking
            (true, true) => {
                let handle = tokio::runtime::Handle::current();
                let join_handle = tokio::task::spawn_blocking(move || handle.block_on(f));
                if let Err(err) =
                    self.new_task_tx.try_send(TaskHandle::new(name.to_string(), join_handle, true))
                {
                    tracing::error!(target: "tn::tasks", "Task error sending joiner for critical, blocking task: {err}");
                }
            }
            // critical, non-blocking
            (true, false) => {
                self.spawn_critical_task(name, f);
            }
            // non-critical, blocking
            (false, true) => {
                let handle = tokio::runtime::Handle::current();
                let join_handle = tokio::task::spawn_blocking(move || handle.block_on(f));
                if let Err(err) =
                    self.new_task_tx.try_send(TaskHandle::new(name.to_string(), join_handle, false))
                {
                    tracing::error!(target: "tn::tasks", "Task error sending joiner for non-critical, blocking task: {err}");
                }
            }
            // non-critical, non-blocking
            (false, false) => {
                self.spawn_task(name, f);
            }
        }

        // return join
        join
    }
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new("Default (test) Task Manager")
    }
}

impl TaskManager {
    /// Create a new empty TaskManager.
    pub fn new<S: ToString>(name: S) -> Self {
        let (new_task_tx, new_task_rx) = mpsc::channel(1000);
        Self {
            tasks: FuturesUnordered::new(),
            name: name.to_string(),
            new_task_rx,
            new_task_tx,
            local_shutdown: ShutdownNotifier::default(),
            join_wait_millis: 2000,
        }
    }

    /// Sets the amount of time this task manager will wait for its tasks to complete
    /// during shutdown (the timeout for [`Self::wait_for_task_shutdown`]).
    pub fn set_join_wait(&mut self, millis: u64) {
        self.join_wait_millis = millis;
    }

    /// Spawns a non-critical task on tokio and records it's JoinHandle and name. Other tasks are
    /// unaffected when this task resolves.
    pub fn spawn_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        self.create_task(name, future, false);
    }

    /// Spawns a critical task on tokio and records it's JoinHandle and name.
    ///
    /// The task is tracked as "critical". When the task resolves, other tasks
    /// will shutdown.
    pub fn spawn_critical_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        self.create_task(name, future, true);
    }

    /// The main function to spawn tasks on the `tokio` runtime. These tasks are tracked by the
    /// manager.
    fn create_task<F, S: ToString>(&self, name: S, future: F, critical: bool)
    where
        F: Future<Output = Result<(), TaskError>> + Send + 'static,
    {
        let name = name.to_string();
        let rx_shutdown = self.local_shutdown.subscribe();
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = rx_shutdown => { Ok(()) }
                res = future => { res }
            }
        });
        self.tasks.push(TaskHandle::new(name, handle, critical));
    }

    /// Return a clonable spawner (also implements Reth's TaskSpawner trait).
    pub fn get_spawner(&self) -> TaskSpawner {
        TaskSpawner {
            new_task_tx: self.new_task_tx.clone(),
            local_shutdown: self.local_shutdown.clone(),
        }
    }

    /// Will resolve once one of the tasks for the manager resolves.
    ///
    /// The manager tracks critical and non-critical tasks. Critical tasks
    /// that stop force the process to shutdown.
    pub async fn join(&mut self, shutdown: ShutdownNotifier) -> Result<(), TaskJoinError> {
        self.join_internal(shutdown, false).await
    }

    /// Will resolve once one of the tasks for the manager resolves.
    ///
    /// Note the manager is based on the assumption that all tasks added via spawn_task
    /// are critical and and one stopping is problem.
    /// Also will end if the user hits ctrl-c or sends a SIGTERM to the app.
    pub async fn join_until_exit(
        &mut self,
        shutdown: ShutdownNotifier,
    ) -> Result<(), TaskJoinError> {
        self.join_internal(shutdown, true).await
    }

    /// Will resolve once one of the tasks for the manager resolves, shutdown is notified or the
    /// node recieves SIGTERM.
    ///
    /// Note the manager is based on the assumption that all tasks added via spawn_task
    /// are critical and and one stopping is problem.
    /// Also will end if the user hits ctrl-c or sends a SIGTERM to the app.
    /// This will not join the tasks but resolves as soon as shutdown is indicated.
    pub async fn until_exit(&mut self, shutdown: ShutdownNotifier) -> Result<(), TaskJoinError> {
        self.until_exit_internal(shutdown, true).await
    }

    /// Will resolve once one of the tasks for the manager resolves or shutdown is notified.
    ///
    /// Note the manager is based on the assumption that all tasks added via spawn_task
    /// are critical and and one stopping is problem.
    /// This will not join the tasks but resolves as soon as shutdown is indicated.
    pub async fn until_task_ends(
        &mut self,
        shutdown: ShutdownNotifier,
    ) -> Result<(), TaskJoinError> {
        self.until_exit_internal(shutdown, false).await
    }

    /// Will resolve once all tasks have shutdown or timeout is reached.
    /// Can be used after until_exit to split up the join.
    pub async fn wait_for_task_shutdown(&mut self) {
        let _ = self.wait_for_task_shutdown_internal().await;
    }

    /// Give the tasks up to `grace` to exit on their own (after their shutdown was signalled), so
    /// each can run its graceful exit, and report whether all of them did. Unlike
    /// [`Self::wait_for_task_shutdown`], tasks still running when the grace lapses are not
    /// reported as a failure: the caller is expected to abort them next.
    pub async fn wait_for_exit(&mut self, grace: Duration) -> bool {
        self.join_tasks_within(grace).await.all_exited
    }

    /// Signal every task this manager spawned to exit, as dropping the manager does, and keep the
    /// manager so [`Self::wait_for_exit`] can wait for them. Reth's blocking tasks
    /// (`spawn_reth_task` on the blocking pool) end only on this notice: [`Self::abort`] cannot
    /// stop a blocking task.
    pub fn signal_shutdown(&self) {
        self.local_shutdown.notify();
    }

    /// Stop and join every task once the shutdown the tasks follow has been notified.
    ///
    /// The tasks first get up to `grace` to exit on their own, so their graceful exits run. Then
    /// the manager signals its own shutdown ([`Self::signal_shutdown`]), which ends the tasks
    /// that follow only this manager (reth's tasks), and joins them for up to the join wait. Only
    /// a task still running after both is logged as an error. The result says whether every task
    /// exited and names the first critical task that failed as it stopped: a stop is where a task
    /// that holds work (the engine's drain) finishes it, so its error is the stop's.
    pub async fn shutdown_tasks(&mut self, grace: Duration) -> TasksStopped {
        self.update_tasks();
        let first = self.join_tasks_within(grace).await;
        if first.all_exited {
            return first;
        }
        self.signal_shutdown();
        let rest = self.wait_for_task_shutdown_internal().await;
        TasksStopped { all_exited: rest.all_exited, failed: first.failed.or(rest.failed) }
    }

    /// Abort all of our direct tasks (not sub task managers though).
    pub fn abort(&self) {
        for task in self.tasks.iter() {
            tracing::debug!(target: "tn::tasks", task_name=?task.info.name, "aborting task");
            task.handle.abort();
        }
    }

    /// Abort all of this manager's tasks.
    ///
    /// This is used to close epoch-related tasks.
    pub fn abort_all_tasks(&mut self) {
        self.abort();
    }

    /// Take any tasks on the new task queue and put them in the task list.
    ///
    /// Use this using the Reth spawner interface to update the task list.
    /// For instance to correctly print all the tasks with Display before
    /// calling join.
    pub fn update_tasks(&mut self) {
        while let Ok(task) = self.new_task_rx.try_recv() {
            self.tasks.push(task);
        }
    }

    /// Resolves when the task manager should exit.
    async fn until_exit_internal(
        &mut self,
        shutdown: ShutdownNotifier,
        do_exit: bool,
    ) -> Result<(), TaskJoinError> {
        let rx_shutdown = shutdown.subscribe();
        // One signal listener for the whole wait. A listener made inside the loop is dropped
        // whenever another branch wins the select, and a signal that arrives before the next
        // one is made is lost: the node then ignores a SIGTERM sent while tasks come and go.
        let mut exit = pin!(Self::exit(do_exit));
        let mut result = Ok(());
        loop {
            tokio::select! {
                _ = &mut exit => {
                    tracing::info!(target: "tn::tasks", "{}: Node exiting", self.name);
                    break;
                },
                _ = &rx_shutdown => {
                    tracing::info!(target: "tn::tasks", "{}: exiting, received shutdown notification", self.name);
                    break;
                },
                Some(task) = self.new_task_rx.recv() => {
                    self.tasks.push(task);
                    continue;
                },
                Some(res) = self.tasks.next() => {
                    // If any task self.tasks exits then this could indicate an error.
                    //
                    // Some tasks are expected to exit graceful at the epoch boundary.
                    match res {
                        Ok(info) => {
                            // ignore short-lived, non-critical tasks that resolve
                            if !info.critical {
                                continue;
                            }
                            tracing::info!(target: "tn::tasks", "{}: {} returned Ok, exiting", self.name, info.name);
                            // Ok exit is fine if we are shutting down. Ask the notifier, not
                            // `rx_shutdown`: `notify` sets its noticers one at a time, so a task
                            // can see its own set and exit before `rx_shutdown`'s is, while
                            // `is_notified` waits for `notify` to finish.
                            if !shutdown.is_notified() {
                                result = Err(TaskJoinError::CriticalExitOk(info.name));
                            }
                        }
                        Err((info, join_err)) => {
                            // ignore short-lived, non-critical tasks that resolve
                            if !info.critical {
                                continue;
                            }
                            if !shutdown.is_notified() {
                                tracing::error!(target: "tn::tasks", "{}: {} returned error {join_err}, exiting", self.name, info.name);
                                result = Err(join_err);
                            } else {
                                // Already shutting down: the shutdown decides the exit, but the
                                // error is still the task's, so it is logged.
                                tracing::warn!(target: "tn::tasks", "{}: {} returned error {join_err} while shutting down", self.name, info.name);
                            }
                        }
                    }
                    break;
                }
            }
        }
        // No matter how we exit notify shutdown and allow a chance for other tasks to exit
        // cleanly.
        shutdown.notify();
        result
    }

    /// Attempt to wait for all the tasks to complete or timeout waiting on them. When some did
    /// not, the ones still running are logged.
    async fn wait_for_task_shutdown_internal(&mut self) -> TasksStopped {
        let task_name = self.name.clone();
        let join_wait = Duration::from_millis(self.join_wait_millis);
        let stopped = self.join_tasks_within(join_wait).await;
        if !stopped.all_exited {
            let running: Vec<&str> =
                self.tasks.iter().map(|task| task.info.name.as_str()).collect();
            tracing::error!(target:"tn::tasks", ?running, "{}: All tasks NOT shutdown", task_name);
        }
        stopped
    }

    /// Join the tasks as they finish, logging each, for up to `wait`.
    async fn join_tasks_within(&mut self, wait: Duration) -> TasksStopped {
        let mut failed = None;
        let all_exited = tokio::time::timeout(wait, async {
            tracing::debug!(target: "tn::tasks", "awaiting shutdown for task manager\n{self:?}");
            while let Some(res) = self.tasks.next().await {
                match res {
                    Ok(info) => {
                        tracing::info!(
                            target: "tn::tasks",
                            "{}: {} shutdown successfully",
                            self.name,
                            info.name,
                        )
                    }
                    Err((info, TaskJoinError::CriticalJoinError(_, err))) if err.is_cancelled() => {
                        tracing::info!(
                            target: "tn::tasks",
                            "{}: {} was cancelled",
                            self.name,
                            info.name,
                        )
                    }
                    Err((info, err)) => {
                        if !info.critical {
                            continue;
                        }
                        tracing::error!(
                            target: "tn::tasks",
                            "{}: {} shutdown with error {err}",
                            self.name,
                            info.name,
                        );
                        failed.get_or_insert(err);
                    }
                }
            }
            tracing::info!(target: "tn::tasks", "{}: All tasks shutdown", self.name);
        })
        .await
        .is_ok();
        TasksStopped { all_exited, failed }
    }

    /// Implements the join logic for the manager.
    async fn join_internal(
        &mut self,
        shutdown: ShutdownNotifier,
        do_exit: bool,
    ) -> Result<(), TaskJoinError> {
        let result = self.until_exit_internal(shutdown, do_exit).await;
        let _ = self.wait_for_task_shutdown_internal().await;
        result
    }

    /// Will resolve when ctrl-c is pressed or a SIGTERM is received.
    async fn exit(do_exit: bool) {
        if !do_exit {
            futures::future::pending::<()>().await;
        }
        #[cfg(unix)]
        {
            let mut stream =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("could not config sigterm");
            let sigterm = stream.recv();
            let sigterm = pin!(sigterm);
            let ctrl_c = pin!(tokio::signal::ctrl_c());

            tokio::select! {
                _ = ctrl_c => {
                    tracing::info!(target: "tn::tasks", "Received ctrl-c");
                },
                _ = sigterm => {
                    tracing::info!(target: "tn::tasks", "Received SIGTERM");
                },
            }
        }

        #[cfg(not(unix))]
        {
            let _ = ctrl_c().await;
        }
    }
}

impl Display for TaskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.name)?;
        for task in self.tasks.iter() {
            let critical = if task.info.critical { "critical" } else { "not critical" };
            writeln!(f, "Task: {} ({critical})", task.info.name)?;
        }
        Ok(())
    }
}

impl Debug for TaskManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl reth_tasks::TaskSpawner for TaskSpawner {
    fn spawn_task(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.spawn_reth_task("reth-task", fut, false, false)
    }

    fn spawn_critical_task(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        self.spawn_reth_task(name, fut, true, false)
    }

    fn spawn_blocking_task(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.spawn_reth_task("reth-blocking-task", fut, false, true)
    }

    fn spawn_critical_blocking_task(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        self.spawn_reth_task(name, fut, true, true)
    }
}

/// A reth [`reth_tasks::TaskSpawner`] over a [`TaskSpawner`] that spawns reth's critical tasks as
/// non-critical TN tasks: tracked by the manager, ended by its shutdown signal, but never ending
/// the node when they exit or panic. For reth code whose "critical" tasks only serve RPC (its eth
/// state cache, fee-history task and transaction batcher): on reth's own `TokioTaskExecutor` they
/// ran untracked, and a panic left the RPC cache dead with the node running; tracked, they
/// release what they hold (the provider, so the database) when the node's tasks shut down, and
/// a panic still only ends them. [`TaskSpawner::non_critical`] makes one.
#[derive(Clone, Debug)]
pub struct NonCriticalTaskSpawner(TaskSpawner);

impl TaskSpawner {
    /// A reth spawner on this manager whose critical tasks are tracked as non-critical
    /// ([`NonCriticalTaskSpawner`]).
    pub fn non_critical(&self) -> NonCriticalTaskSpawner {
        NonCriticalTaskSpawner(self.clone())
    }
}

impl reth_tasks::TaskSpawner for NonCriticalTaskSpawner {
    fn spawn_task(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.0.spawn_reth_task("reth-task", fut, false, false)
    }

    fn spawn_critical_task(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        self.0.spawn_reth_task(name, fut, false, false)
    }

    fn spawn_blocking_task(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.0.spawn_reth_task("reth-blocking-task", fut, false, true)
    }

    fn spawn_critical_blocking_task(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        self.0.spawn_reth_task(name, fut, false, true)
    }
}

/// How a manager's tasks stopped ([`TaskManager::shutdown_tasks`]).
#[derive(Debug)]
#[must_use = "a critical task that failed as it stopped is an error of the stop"]
pub struct TasksStopped {
    /// Every task ended within the waits.
    pub all_exited: bool,
    /// The first critical task that ended with an error (or panicked) as it stopped; each is also
    /// logged. A task ended by the manager's own shutdown ends `Ok`, and a cancelled one is not
    /// counted.
    pub failed: Option<TaskJoinError>,
}

/// Indicate a non-normal exit on a a taskmanager join.
#[derive(Debug, Error)]
pub enum TaskJoinError {
    #[error("Critical task {0} has exited unexpectedly: OK")]
    CriticalExitOk(String),
    #[error("Critical task {0} has exited unexpectedly (join error): {1}")]
    CriticalJoinError(String, JoinError),
    #[error("Critical task {0} has exited unexpectedly: {1}")]
    CriticalExitError(String, TaskError),
}

impl TaskJoinError {
    /// Whether the task ended with a fork stop as its error ([`TaskError::is_fork`]).
    pub fn is_fork(&self) -> bool {
        matches!(self, Self::CriticalExitError(_, error) if error.is_fork())
    }

    /// This error as the error of a stop, its message after `context` (`"{context}: {self}"`). A
    /// fork stop ([`Self::is_fork`]) stays one: the report is a [`NodeForked`], so the process
    /// exits with the fork's status.
    #[track_caller]
    pub fn to_report(&self, context: &str) -> eyre::Report {
        let message = format!("{context}: {self}");
        if self.is_fork() {
            NodeForked::new(message).into()
        } else {
            eyre::eyre!(message)
        }
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::mpsc::{self, Receiver, Sender};

    use crate::{error::NodeForked, ShutdownNotifier, TaskError, TaskJoinError, TaskManager};

    struct Ping {
        ping_rx: Receiver<u32>,
        pong_tx: Sender<u32>,
    }

    struct Pong {
        ping_tx: Sender<u32>,
        pong_rx: Receiver<u32>,
    }

    fn new_ping_pong() -> (Ping, Pong) {
        let (ping_tx, ping_rx) = mpsc::channel(10);
        let (pong_tx, pong_rx) = mpsc::channel(10);
        (Ping { ping_rx, pong_tx }, Pong { ping_tx, pong_rx })
    }

    impl Ping {
        async fn run(mut self) {
            while let Some(p) = self.ping_rx.recv().await {
                let _ = self.pong_tx.send(p).await;
            }
        }

        fn run_blocking(mut self) {
            while let Some(p) = self.ping_rx.blocking_recv() {
                let _ = self.pong_tx.try_send(p);
            }
        }
    }

    impl Pong {
        async fn ping(&mut self, p: u32) -> eyre::Result<u32> {
            self.ping_tx.send(p).await?;
            self.pong_rx.recv().await.map_or_else(|| Err(eyre::eyre!("No Pong!")), Ok)
        }
    }

    /// A critical task that exits because the shutdown was notified is a normal exit, even when it
    /// exits while `notify` is still setting the other noticers (it sets them one at a time):
    /// `until_task_ends` must not report it as an unexpected `CriticalExitOk`. The noticers between
    /// the task's and the manager's own widen that window enough to hit it every time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn critical_exit_mid_notify_is_a_shutdown() {
        for round in 0..10 {
            let mut task_manager = TaskManager::default();
            let shutdown = ShutdownNotifier::new();
            let task_rx = shutdown.subscribe();
            let _between: Vec<_> = (0..200_000).map(|_| shutdown.subscribe()).collect();
            task_manager.spawn_critical_task("watcher", async move {
                while !task_rx.noticed() {
                    tokio::task::yield_now().await;
                }
                Ok(())
            });
            let notifier = shutdown.clone();
            let notify = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                notifier.notify();
            });
            let result = task_manager.until_task_ends(shutdown).await;
            notify.join().unwrap();
            assert!(result.is_ok(), "round {round}: {result:?}");
        }
    }

    /// Test that the various spawns work and that the spawned tasks are dropped when the spawning
    /// TaskManager is dropped. Except for Spawner.spawn_block_task(), it does not spawn a
    /// future and will not be killed forcefully on drop.
    #[tokio::test]
    async fn test_task_manager() {
        let task_manager = TaskManager::default();
        let (ping_crit, mut pong_crit) = new_ping_pong();
        task_manager.spawn_critical_task("Crit", async move {
            ping_crit.run().await;
            Ok(())
        });
        assert_eq!(pong_crit.ping(1).await.unwrap(), 1);
        assert_eq!(pong_crit.ping(2).await.unwrap(), 2);

        let (ping_norm, mut pong_norm) = new_ping_pong();
        task_manager.spawn_task("task", async move {
            ping_norm.run().await;
            Ok(())
        });
        assert_eq!(pong_norm.ping(1).await.unwrap(), 1);
        assert_eq!(pong_norm.ping(2).await.unwrap(), 2);

        let spawner = task_manager.get_spawner();
        let (sping_crit, mut spong_crit) = new_ping_pong();
        spawner.spawn_critical_task("Crit", async move {
            sping_crit.run().await;
            Ok(())
        });
        assert_eq!(spong_crit.ping(1).await.unwrap(), 1);
        assert_eq!(spong_crit.ping(2).await.unwrap(), 2);

        let (sping_norm, mut spong_norm) = new_ping_pong();
        spawner.spawn_task("task", async move {
            sping_norm.run().await;
            Ok(())
        });
        assert_eq!(spong_norm.ping(1).await.unwrap(), 1);
        assert_eq!(spong_norm.ping(2).await.unwrap(), 2);

        let (sping_block, mut spong_block) = new_ping_pong();
        spawner.spawn_blocking_task("SBlock", move || {
            sping_block.run_blocking();
            Ok(())
        });
        assert_eq!(spong_block.ping(1).await.unwrap(), 1);
        assert_eq!(spong_block.ping(2).await.unwrap(), 2);

        // Test the reth interface to the TaskSpawner.
        use reth_tasks::TaskSpawner as RethTaskSpawner;
        let (rsping_crit, mut rspong_crit) = new_ping_pong();
        spawner.spawn_critical_task(
            "Crit",
            Box::pin(async move {
                rsping_crit.run().await;
                Ok(())
            }),
        );
        assert_eq!(rspong_crit.ping(1).await.unwrap(), 1);
        assert_eq!(rspong_crit.ping(2).await.unwrap(), 2);

        let (rsping_norm, mut rspong_norm) = new_ping_pong();
        RethTaskSpawner::spawn_task(
            &spawner,
            Box::pin(async move {
                rsping_norm.run().await;
            }),
        );
        assert_eq!(rspong_norm.ping(1).await.unwrap(), 1);
        assert_eq!(rspong_norm.ping(2).await.unwrap(), 2);

        let (rsping_block, mut rspong_block) = new_ping_pong();
        RethTaskSpawner::spawn_blocking_task(
            &spawner,
            Box::pin(async move {
                rsping_block.run().await;
            }),
        );
        assert_eq!(rspong_block.ping(1).await.unwrap(), 1);
        assert_eq!(rspong_block.ping(2).await.unwrap(), 2);

        let (rsping_crit_block, mut rspong_crit_block) = new_ping_pong();
        spawner.spawn_critical_blocking_task(
            "Crit block",
            Box::pin(async move {
                rsping_crit_block.run().await;
            }),
        );
        assert_eq!(rspong_crit_block.ping(1).await.unwrap(), 1);
        assert_eq!(rspong_crit_block.ping(2).await.unwrap(), 2);

        drop(task_manager);

        // Dropping the manager notifies local_shutdown; each killed task drops its
        // Ping on exit, closing the ping channel. Poll for that instead of a fixed
        // 1s sleep, bounded so a shutdown regression still fails loudly.
        let killed = [
            &pong_crit.ping_tx,
            &pong_norm.ping_tx,
            &spong_crit.ping_tx,
            &spong_norm.ping_tx,
            &rspong_crit.ping_tx,
            &rspong_norm.ping_tx,
            &rspong_block.ping_tx,
            &rspong_crit_block.ping_tx,
        ];
        tokio::time::timeout(Duration::from_secs(5), async {
            while killed.iter().any(|tx| !tx.is_closed()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("spawned tasks still alive 5s after TaskManager drop");
        assert!(pong_crit.ping(2).await.is_err());
        assert!(pong_norm.ping(2).await.is_err());

        assert!(spong_crit.ping(2).await.is_err());
        assert!(spong_norm.ping(2).await.is_err());
        // Note this blocking task is NOT killed when task manager is dropped..
        assert_eq!(spong_block.ping(3).await.unwrap(), 3);

        assert!(rspong_crit.ping(2).await.is_err());
        assert!(rspong_norm.ping(2).await.is_err());
        assert!(rspong_block.ping(2).await.is_err());
        assert!(rspong_crit_block.ping(2).await.is_err());
    }

    /// A reth "critical" task spawned through `TaskSpawner::non_critical` ends, or panics,
    /// without ending the node: `join` keeps waiting, where the spawner's own reth critical
    /// spawn reports the exit. Both are tracked: the manager's shutdown signal ends them.
    #[tokio::test]
    async fn non_critical_reth_tasks_never_end_the_node() {
        use reth_tasks::TaskSpawner as RethTaskSpawner;
        let mut task_manager = TaskManager::default();
        let spawner = task_manager.get_spawner().non_critical();
        spawner.spawn_critical_task("rpc cache", Box::pin(async { panic!("dead cache") }));
        spawner.spawn_critical_task("fee history", Box::pin(async {}));
        spawner.spawn_critical_blocking_task("batcher", Box::pin(async {}));
        let waited = tokio::time::timeout(
            Duration::from_millis(300),
            task_manager.join(ShutdownNotifier::default()),
        )
        .await;
        assert!(waited.is_err(), "the node keeps running: {waited:?}");
        task_manager.signal_shutdown();
        assert!(task_manager.wait_for_exit(Duration::from_secs(5)).await, "tracked: they exit");

        let mut task_manager = TaskManager::default();
        let spawner = task_manager.get_spawner();
        RethTaskSpawner::spawn_critical_task(&spawner, "rpc cache", Box::pin(async {}));
        match task_manager.join(ShutdownNotifier::default()).await {
            Err(TaskJoinError::CriticalExitOk(name)) => assert_eq!(name, "rpc cache"),
            other => panic!("a reth critical task's exit ends the node: {other:?}"),
        }
    }

    /// Test that an error produced in a task makes it up to join.
    #[tokio::test]
    async fn test_task_manager_join() {
        let mut task_manager = TaskManager::default();
        task_manager.spawn_critical_task("Crit 1", async move { Ok(()) });
        match task_manager.join(ShutdownNotifier::default()).await {
            Ok(_) => {}
            Err(TaskJoinError::CriticalExitOk(name)) => assert!(name.eq("Crit 1")),
            Err(TaskJoinError::CriticalJoinError(_name, _err)) => panic!("wrong error"),
            Err(TaskJoinError::CriticalExitError(_name, _err)) => panic!("wrong error"),
        }
        let mut task_manager = TaskManager::default();
        task_manager.spawn_critical_task("Crit 1", async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        });
        task_manager
            .spawn_critical_task("Crit 2", async move { Err(TaskError::from_message("BOOM!")) });
        match task_manager.join(ShutdownNotifier::default()).await {
            Ok(_) => {}
            Err(TaskJoinError::CriticalExitOk(_name)) => panic!("should not be OK"),
            Err(TaskJoinError::CriticalJoinError(_name, _err)) => panic!("wrong error"),
            Err(TaskJoinError::CriticalExitError(name, _err)) => assert!(name.eq("Crit 2")),
        }
    }

    /// `wait_for_exit` lets tasks that honour their shutdown signal finish on their own (their
    /// graceful exit runs), returns as soon as they have, and reports a task that ignores the
    /// signal by returning `false` after the grace, leaving it running for the caller to abort.
    #[tokio::test]
    async fn test_wait_for_exit_lets_tasks_finish_before_an_abort() {
        let mut task_manager = TaskManager::default();
        let shutdown = ShutdownNotifier::default();
        let (done_tx, mut done_rx) = mpsc::channel::<()>(1);
        let rx_shutdown = shutdown.subscribe();
        task_manager.spawn_task("graceful", async move {
            rx_shutdown.await;
            let _ = done_tx.send(()).await; // the graceful exit's own work
            Ok(())
        });
        task_manager.update_tasks();
        shutdown.notify();
        let started = std::time::Instant::now();
        assert!(task_manager.wait_for_exit(Duration::from_secs(5)).await, "the task exits");
        assert!(started.elapsed() < Duration::from_secs(5), "returns once every task exited");
        assert!(done_rx.try_recv().is_ok(), "the graceful exit ran");

        let mut task_manager = TaskManager::default();
        task_manager.spawn_task("stubborn", async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        });
        task_manager.update_tasks();
        assert!(
            !task_manager.wait_for_exit(Duration::from_millis(50)).await,
            "a task that does not exit is reported"
        );
        task_manager.abort_all_tasks();
        task_manager.wait_for_task_shutdown().await;
    }

    /// A reth blocking task outlives an abort and ends on `signal_shutdown`, which leaves the
    /// manager in place so `wait_for_exit` sees it go and what it held is released.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_signal_shutdown_ends_reth_blocking_tasks() {
        let mut task_manager = TaskManager::default();
        let held = Arc::new(());
        let task_held = held.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        reth_tasks::TaskSpawner::spawn_blocking_task(
            &task_manager.get_spawner(),
            Box::pin(async move {
                let _held = task_held;
                let _ = started_tx.send(());
                std::future::pending::<()>().await;
            }),
        );
        task_manager.update_tasks();
        // a blocking task that has not started yet is cancelled by an abort; this one is running
        started_rx.await.expect("the task starts");
        task_manager.abort();
        assert!(
            !task_manager.wait_for_exit(Duration::from_millis(200)).await,
            "an abort does not stop a blocking task"
        );
        task_manager.signal_shutdown();
        assert!(task_manager.wait_for_exit(Duration::from_secs(5)).await, "the task exits");
        assert_eq!(Arc::strong_count(&held), 1, "the task released what it held");
    }

    /// After the node's shutdown is notified, `shutdown_tasks` lets the tasks that follow it run
    /// their graceful exits, then ends the tasks that follow only the manager (reth's, blocking
    /// or not) and joins every one within the shutdown grace, as the node's clean stop needs.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_tasks_ends_tasks_that_follow_only_the_manager() {
        use reth_tasks::TaskSpawner as RethTaskSpawner;
        let mut task_manager = TaskManager::default();
        task_manager.set_join_wait(500);
        let node_shutdown = ShutdownNotifier::default();
        let rx_shutdown = node_shutdown.subscribe();
        let (done_tx, mut done_rx) = mpsc::channel::<()>(1);
        task_manager.spawn_critical_task("follows the node shutdown", async move {
            rx_shutdown.await;
            // the graceful exit's own work
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = done_tx.send(()).await;
            Ok(())
        });
        let spawner = task_manager.get_spawner();
        RethTaskSpawner::spawn_critical_task(&spawner, "reth", Box::pin(std::future::pending()));
        RethTaskSpawner::spawn_blocking_task(&spawner, Box::pin(std::future::pending()));

        node_shutdown.notify();
        let stopped = task_manager.shutdown_tasks(Duration::from_millis(300)).await;
        assert!(stopped.all_exited, "every task exits within the grace and the join wait");
        assert!(stopped.failed.is_none(), "a task the manager ends is no failure: {stopped:?}");
        assert!(done_rx.try_recv().is_ok(), "the graceful exit ran");
    }

    /// A critical task whose graceful exit fails (the engine failing to persist what it drains)
    /// is named in `shutdown_tasks`' result, so the node's stop can exit with it; a non-critical
    /// task's failure is not.
    #[tokio::test]
    async fn shutdown_tasks_names_a_critical_task_that_failed_as_it_stopped() {
        let mut task_manager = TaskManager::default();
        let node_shutdown = ShutdownNotifier::default();
        let rx_drain = node_shutdown.subscribe();
        task_manager.spawn_critical_task("drains on the node shutdown", async move {
            rx_drain.await;
            Err(TaskError::from_message("the drain could not persist"))
        });
        let rx_side = node_shutdown.subscribe();
        task_manager.spawn_task("not critical", async move {
            rx_side.await;
            Err(TaskError::from_message("ignored"))
        });

        node_shutdown.notify();
        let stopped = task_manager.shutdown_tasks(Duration::from_secs(5)).await;
        assert!(stopped.all_exited);
        let Some(TaskJoinError::CriticalExitError(name, err)) = stopped.failed else {
            panic!("expected the draining task's error, got {:?}", stopped.failed);
        };
        assert_eq!(name, "drains on the node shutdown");
        assert!(err.to_string().contains("the drain could not persist"), "{err}");
    }

    /// A critical task's fork stop reaches both ways a manager reports a failed critical task as a
    /// fork, the join that ends on it and the join of a stop, and so does the stop's error built
    /// from either, with the task's message kept. Another error, even one that reads like a fork,
    /// is none.
    #[tokio::test]
    async fn a_fork_stop_stays_a_fork_across_the_task_boundary() {
        // a fork found under more context, as a collection reports it
        let fork = || {
            TaskError::from_report(
                eyre::Report::new(std::io::Error::other("refused"))
                    .wrap_err(NodeForked::new("epoch 2: this node has forked"))
                    .wrap_err("collection failed"),
            )
        };
        let mut task_manager = TaskManager::default();
        task_manager.spawn_critical_task("forked", async move { Err(fork()) });
        let ended = task_manager.until_task_ends(ShutdownNotifier::default()).await;
        let Err(err) = ended else {
            panic!("expected the forked task's error, got {ended:?}");
        };
        assert!(err.is_fork(), "{err}");
        let stop = err.to_report("Node task shutdown");
        assert!(NodeForked::is_in(&stop), "{stop:#}");
        assert_eq!(
            stop.to_string(),
            "Node task shutdown: Critical task forked has exited unexpectedly: collection failed: \
             epoch 2: this node has forked: refused",
        );

        let mut task_manager = TaskManager::default();
        let node_shutdown = ShutdownNotifier::default();
        let rx_forked = node_shutdown.subscribe();
        task_manager.spawn_critical_task("forked as it stopped", async move {
            rx_forked.await;
            Err(fork())
        });
        node_shutdown.notify();
        let stopped = task_manager.shutdown_tasks(Duration::from_secs(5)).await;
        let Some(err) = stopped.failed else {
            panic!("expected the forked task's error");
        };
        assert!(err.is_fork(), "{err}");
        let stop = err.to_report("a node task failed as the node stopped");
        assert!(NodeForked::is_in(&stop), "{stop:#}");

        let other = TaskJoinError::CriticalExitError(
            "engine".to_string(),
            TaskError::from_report(eyre::eyre!("this node has forked")),
        );
        assert!(!other.is_fork());
        assert!(!NodeForked::is_in(&other.to_report("Node task shutdown")));
    }

    /// A task spawned through a spawner whose manager has ended (a spawn racing a node stop) ends
    /// at once: the manager's drop notified its shutdown, which the task's select sees, so a
    /// closed tracker there leaves nothing running and is not an error.
    #[tokio::test]
    async fn a_task_spawned_after_its_manager_ended_ends_at_once() {
        let manager = TaskManager::new("ended");
        let spawner = manager.get_spawner();
        drop(manager);
        let (ended_tx, ended_rx) = tokio::sync::oneshot::channel::<()>();
        spawner.spawn_task("late", async move {
            // never resolves on its own; only the shutdown notice ends the task, dropping
            // `ended_tx`
            let _ended = ended_tx;
            std::future::pending::<()>().await;
            Ok(())
        });
        tokio::time::timeout(Duration::from_secs(5), ended_rx)
            .await
            .expect("the late task ended on the manager's shutdown")
            .expect_err("the task ended without finishing its future");
    }

    /// Regression: each spawn subscribes to the manager's shutdown notifier (a reth task three
    /// times, a reth blocking task twice), and the notifier used to keep every Noticer until the
    /// manager shut down. A node's manager lives as long as the node and reth spawns through it
    /// for blocking RPC calls and subscriptions, so the list grew with RPC traffic: these 300
    /// finished tasks left 600 entries. Finished tasks must leave no live Noticer behind, and the
    /// list must stay bounded however many tasks have run.
    #[tokio::test(flavor = "multi_thread")]
    async fn finished_tasks_release_their_shutdown_noticers() {
        use crate::notifier::MIN_PRUNE_LEN;
        use reth_tasks::TaskSpawner as RethTaskSpawner;
        const N: usize = 100;
        let manager = TaskManager::new("noticers");
        let spawner = manager.get_spawner();
        for _ in 0..N {
            RethTaskSpawner::spawn_task(&spawner, Box::pin(async {})).await.expect("task joins");
            RethTaskSpawner::spawn_blocking_task(&spawner, Box::pin(async {}))
                .await
                .expect("blocking task joins");
            let (ran_tx, ran_rx) = tokio::sync::oneshot::channel();
            manager.spawn_task("finished", async move {
                let _ = ran_tx.send(());
                Ok(())
            });
            ran_rx.await.expect("task ran");
        }
        // A reth join handle resolves when the task signals its end, which can be just before
        // the task finishes and drops its Noticers, so give the last ones a moment.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while manager.local_shutdown.live_noticers() > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(manager.local_shutdown.live_noticers(), 0, "finished tasks left live noticers");
        let registered = manager.local_shutdown.registered_len();
        assert!(
            registered <= MIN_PRUNE_LEN,
            "{registered} entries left by {} finished tasks",
            3 * N
        );
    }

    /// Regression: `until_exit` made a new SIGTERM listener on every turn of its loop, so a
    /// signal that arrived while a task was being added or reaped (between two listeners) was
    /// lost and the node kept running. With tasks coming and going the whole time, every SIGTERM
    /// must end the wait.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sigterm_ends_until_exit_while_tasks_come_and_go() {
        use tokio::signal::unix::{signal, SignalKind};
        // keeps the process's SIGTERM handler installed, so a signal never kills the test
        let _installed = signal(SignalKind::terminate()).expect("sigterm handler");
        for round in 0..40 {
            let mut manager = TaskManager::new("sigterm");
            let spawner = manager.get_spawner();
            let churn = tokio::spawn(async move {
                loop {
                    spawner.spawn_task("short", async { Ok(()) });
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            });
            let kill = tokio::spawn(async {
                // after `until_exit` began its wait
                tokio::time::sleep(Duration::from_millis(5)).await;
                let status = std::process::Command::new("kill")
                    .args(["-TERM", &std::process::id().to_string()])
                    .status()
                    .expect("kill runs");
                assert!(status.success(), "kill -TERM failed");
            });
            let ended = tokio::time::timeout(
                Duration::from_secs(5),
                manager.until_exit(ShutdownNotifier::new()),
            )
            .await;
            churn.abort();
            kill.await.expect("kill task");
            assert!(ended.is_ok(), "round {round}: the SIGTERM did not end until_exit");
        }
    }
}
