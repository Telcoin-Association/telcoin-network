//! Task manager interface to spawn tasks to the tokio runtime.

use crate::Notifier;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    future::Future,
    pin::pin,
    task::Poll,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinHandle},
};

/// Used for the futures that will resolve when tasks do.
/// Allows us to hold a FuturesUnordered directly in the TaskManager struct.
struct TaskHandle {
    /// The  owned permission to join on a task (await its termination).
    handle: JoinHandle<()>,
    /// The name for the task.
    info: TaskInfo,
}

impl TaskHandle {
    /// Create a new instance of `Self`.
    fn new(name: String, handle: JoinHandle<()>, critical: bool) -> Self {
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
    type Output = Result<TaskInfo, (TaskInfo, JoinError)>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match this.handle.poll_unpin(cx) {
            Poll::Ready(res) => match res {
                Ok(_) => Poll::Ready(Ok(this.info.clone())),
                Err(err) => Poll::Ready(Err((this.info.clone(), err))),
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
    submanagers: HashMap<String, TaskManager>,
    name: String,
    new_task_rx: mpsc::Receiver<TaskHandle>,
    new_task_tx: mpsc::Sender<TaskHandle>,
}

/// The type that can spawn tasks for a parent `TaskManager`.
///
/// The TaskSpawner is clone-able and forwards tasks to the task manager
/// to track. This type lives with other types to spawn short-lived tasks.
#[derive(Clone, Debug)]
pub struct TaskSpawner {
    /// The channel to forward task handles to the parent [TaskManager].
    new_task_tx: mpsc::Sender<TaskHandle>,
}

impl TaskSpawner {
    /// Spawns a non-critical task on tokio and records it's JoinHandle and name. Other tasks are
    /// unaffected when this task resolves.
    pub fn spawn_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, false);
    }

    /// Spawns a critical task on tokio and records it's JoinHandle and name.
    ///
    /// The task is tracked as "critical". When the task resolves, other tasks
    /// will shutdown.
    pub fn spawn_critical_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, true);
    }

    /// The main function to spawn tasks on the `tokio` runtime. These tasks are tracked by the
    /// manager.
    fn create_task<F, S: ToString>(&self, name: S, future: F, critical: bool)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = name.to_string();
        let handle = tokio::spawn(async move {
            future.await;
        });
        if let Err(err) = self.new_task_tx.try_send(TaskHandle::new(name.clone(), handle, critical))
        {
            tracing::error!(target: "tn::tasks", "Task error sending joiner for {name}: {err}");
        }
    }

    /// Spawns a non-critical, blocking task on tokio and records the JoinHandle and name.
    ///
    /// Other tasks are unaffected when this task resolves.
    pub fn spawn_blocking_task<F, S: ToString>(&self, name: S, task: F)
    where
        F: FnOnce() + Send + 'static,
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
        let (tx, mut rx) = tokio::sync::broadcast::channel(1);
        // Need two join handles so do this channel dance to get them.
        // Required because the task manager needs one and this foreign Reth interface return one.
        let f = async move {
            let value = fut.await;
            let _ = tx.send(value);
        };
        let join = tokio::spawn(async move {
            let _ = rx.recv().await;
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
        let (new_task_tx, new_task_rx) = mpsc::channel(100);
        Self {
            tasks: FuturesUnordered::new(),
            submanagers: HashMap::new(),
            name: name.to_string(),
            new_task_rx,
            new_task_tx,
        }
    }

    /// Spawns a non-critical task on tokio and records it's JoinHandle and name. Other tasks are
    /// unaffected when this task resolves.
    pub fn spawn_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, false);
    }

    /// Spawns a critical task on tokio and records it's JoinHandle and name.
    ///
    /// The task is tracked as "critical". When the task resolves, other tasks
    /// will shutdown.
    pub fn spawn_critical_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_task(name, future, true);
    }

    /// The main function to spawn tasks on the `tokio` runtime. These tasks are tracked by the
    /// manager.
    fn create_task<F, S: ToString>(&self, name: S, future: F, critical: bool)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = name.to_string();
        let handle = tokio::spawn(async move {
            future.await;
        });
        self.tasks.push(TaskHandle::new(name, handle, critical));
    }

    /// Return a clonable spawner (also implements Reth's TaskSpawner trait).
    pub fn get_spawner(&self) -> TaskSpawner {
        TaskSpawner { new_task_tx: self.new_task_tx.clone() }
    }

    /// Return a mutable reference to a submanager.
    pub fn get_submanager(&mut self, name: &str) -> Option<&mut TaskManager> {
        self.submanagers.get_mut(name)
    }

    /// Adds a subtask manager by name to this TaskManager.  Allows building a heirarchy of tasks.
    pub fn add_task_manager(&mut self, manager: TaskManager) {
        self.submanagers.insert(manager.name.clone(), manager);
    }

    /// Will resolve once one of the tasks for the manager resolves.
    ///
    /// The manager tracks critical and non-critical tasks. Critical tasks
    /// that stop force the process to shutdown.
    pub async fn join(&mut self, shutdown: Notifier) -> Result<(), TaskJoinError> {
        self.join_internal(shutdown, false).await
    }

    /// Will resolve once one of the tasks for the manager resolves.
    ///
    /// Note the manager is based on the assumption that all tasks added via spawn_task
    /// are critical and and one stopping is problem.
    /// Also will end if the user hits ctrl-c or sends a SIGTERM to the app.
    pub async fn join_until_exit(&mut self, shutdown: Notifier) -> Result<(), TaskJoinError> {
        self.join_internal(shutdown, true).await
    }

    /// Abort all of our direct tasks (not sub task managers though).
    pub fn abort(&self) {
        for task in self.tasks.iter() {
            tracing::debug!(target: "tn::tasks", task_name=?task.info.name, "aborting task");
            task.handle.abort();
        }
    }

    /// Abort all tasks including submanagers.
    ///
    /// This is used to close epoch-related tasks.
    pub fn abort_all_tasks(&mut self) {
        self.abort();

        // abort submanager tasks as well
        for manager in self.submanagers.values_mut() {
            manager.abort_all_tasks();
        }
    }

    /// Spawn blocking on tokio.  Here mostly for compat with old Reth interface.
    pub fn spawn_blocking(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || handle.block_on(fut))
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

    /// Implements the join logic for the manager.
    async fn join_internal(
        &mut self,
        shutdown: Notifier,
        do_exit: bool,
    ) -> Result<(), TaskJoinError> {
        let shutdown_ref = &shutdown;
        let mut future_managers: FuturesUnordered<_> = self
            .submanagers
            .drain()
            .map(|(name, mut sub)| async move { (sub.join(shutdown_ref.clone()).await, name) })
            .collect();
        let rx_shutdown = shutdown.subscribe();
        let mut result = Ok(());
        loop {
            tokio::select! {
                _ = Self::exit(do_exit) => {
                    tracing::info!(target: "tn::tasks", "{}: Node exiting", self.name);
                    break;
                },
                _ = &rx_shutdown => {
                    tracing::info!(target: "tn::tasks", "{}: Node exiting, received shutdown notification", self.name);
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
                            tracing::info!(target: "tn::tasks", "{}: {} returned Ok, node exiting", self.name, info.name);
                            // Ok exit is fine if we are shutting down.
                            if !rx_shutdown.noticed() {
                                result = Err(TaskJoinError::CriticalExitOk(info.name));
                            }
                        }
                        Err((info, join_err)) => {
                            // ignore short-lived, non-critical tasks that resolve
                            if !info.critical {
                                continue;
                            }
                            tracing::error!(target: "tn::tasks", "{}: {} returned error {join_err}, node exiting", self.name, info.name);
                            // Ok exit is fine if we are shutting down.
                            if !rx_shutdown.noticed() {
                                result = Err(TaskJoinError::CriticalExitError(info.name, join_err));
                            }
                        }
                    }
                    break;
                }
                Some((res, name)) = future_managers.next() => {
                    tracing::error!(target: "tn::tasks", "{}: Sub-Task Manager {name} returned exited, node exiting", self.name);
                    result = res;
                    break;
                }
            }
        }
        // No matter how we exit notify shutdown and allow a chance for other tasks to exit
        // cleanly.
        shutdown.notify();
        let task_name = self.name.clone();
        // wait some time for shutdown...
        // 2 seconds for our tasks to end...
        if tokio::time::timeout(Duration::from_secs(2), async move {
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
                    Err((info, err)) => tracing::error!(
                        target: "tn::tasks",
                        "{}: {} shutdown with error {err}",
                        self.name,
                        info.name,
                    ),
                }
            }
            tracing::info!(target: "tn::tasks", "{}: All tasks shutdown", self.name);
        })
        .await
        .is_err()
        {
            tracing::error!(target:"tn::tasks", "{}: All tasks NOT shutdown", task_name);
        }

        // Another 2 seconds for any of our sub tasks to end...
        let task_name_clone = task_name.clone();
        if tokio::time::timeout(Duration::from_secs(2), async move {
            while let Some((_, name)) = future_managers.next().await {
                tracing::info!(
                    target: "tn::tasks",
                    "{}: TaskManager {name} shutdown successfully",
                    task_name_clone
                )
            }
            tracing::info!(
                target: "tn::tasks",
                "{}: All tasks managers shutdown",
                task_name_clone
            );
        })
        .await
        .is_err()
        {
            tracing::error!(target: "tn::tasks", "{}: All tasks managers NOT shutdown", task_name);
        }
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
        for sub in self.submanagers.values() {
            writeln!(f, "++++++++++++++++++++++++++++++++++++++++++++++++++++")?;
            writeln!(f, "{sub}")?;
            writeln!(f, "++++++++++++++++++++++++++++++++++++++++++++++++++++")?;
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
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.spawn_reth_task("reth-task", fut, false, false)
    }

    fn spawn_critical(&self, name: &'static str, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.spawn_reth_task(name, fut, true, false)
    }

    fn spawn_blocking(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        self.spawn_reth_task("reth-blocking-task", fut, false, true)
    }

    fn spawn_critical_blocking(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
    ) -> JoinHandle<()> {
        self.spawn_reth_task(name, fut, true, true)
    }
}

/// Indicate a non-normal exit on a a taskmanager join.
#[derive(Debug, Error)]
pub enum TaskJoinError {
    #[error("Critical task {0} has exited unexpectedly: OK")]
    CriticalExitOk(String),
    #[error("Critical task {0} has exited unexpectedly: {1}")]
    CriticalExitError(String, JoinError),
}
