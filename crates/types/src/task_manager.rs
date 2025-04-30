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
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinHandle},
};

/// Used for the futures that will resolve when tasks do.
/// Allows us to hold a FuturesUnordered in directly in the TaskManager struct.
struct TaskHandle {
    handle: JoinHandle<()>,
    name: String,
}

impl Future for TaskHandle {
    type Output = Result<String, (String, JoinError)>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match this.handle.poll_unpin(cx) {
            Poll::Ready(res) => match res {
                Ok(_) => Poll::Ready(Ok(this.name.clone())),
                Err(err) => Poll::Ready(Err((this.name.clone(), err))),
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

#[derive(Clone, Debug)]
pub struct TaskManagerClone {
    new_task_tx: mpsc::Sender<TaskHandle>,
}

impl TaskManagerClone {
    /// Spawns a task on tokio and records it's JoinHandle and name.
    pub fn spawn_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = name.to_string();
        let handle = tokio::spawn(async move {
            future.await;
        });
        if let Err(err) = self.new_task_tx.try_send(TaskHandle { name, handle }) {
            tracing::error!(target: "tn::tasks", "Task error sending joiner: {err}");
        }
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

    /// Spawns a task on tokio and records it's JoinHandle and name.
    pub fn spawn_task<F, S: ToString>(&self, name: S, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let name = name.to_string();
        let handle = tokio::spawn(async move {
            future.await;
        });
        self.tasks.push(TaskHandle { name, handle });
    }

    /// Return a clonable spawner (also implements Reth's TaskSpawner trait).
    pub fn get_spawner(&self) -> TaskManagerClone {
        TaskManagerClone { new_task_tx: self.new_task_tx.clone() }
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
    /// Note the manager is based on the assumption that all tasks added via spawn_task
    /// are critical and and one stopping is problem.
    pub async fn join(&mut self, shutdown: Notifier) {
        self.join_internal(shutdown, false).await;
    }

    /// Will resolve once one of the tasks for the manager resolves.
    ///
    /// Note the manager is based on the assumption that all tasks added via spawn_task
    /// are critical and and one stopping is problem.
    /// Also will end if the user hits ctrl-c or sends a SIGTERM to the app.
    pub async fn join_until_exit(&mut self, shutdown: Notifier) {
        self.join_internal(shutdown, true).await;
    }

    /// Abort all of our direct tasks (not sub task managers though).
    /// This is included for some tests, should not use in real code.
    pub fn abort(&self) {
        for task in self.tasks.iter() {
            task.handle.abort();
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
    async fn join_internal(&mut self, shutdown: Notifier, do_exit: bool) {
        let shutdown_ref = &shutdown;
        let mut future_managers: FuturesUnordered<_> = self
            .submanagers
            .drain()
            .map(|(name, mut sub)| async move { (sub.join(shutdown_ref.clone()).await, name) })
            .collect();
        let rx_shutdown = shutdown.subscribe();
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
                res = self.tasks.next() => {
                    // If any task self.tasks exits then this could indicate an error.
                    //
                    // Some tasks are expected to exit graceful at the epoch boundary.
                    match res {
                        Some(Ok(name)) => {
                            tracing::info!(target: "tn::tasks", "{}: {name} returned Ok, node exiting", self.name);
                        }
                        Some(Err((name, join_err))) => {
                            tracing::error!(target: "tn::tasks", "{}: {name} returned error {join_err}, node exiting", self.name);
                        }
                        None => {
                            tracing::error!(target: "tn::tasks", "{}: Out of tasks! node exiting", self.name);
                        }
                    }
                    break;
                }
                Some((_, name)) = future_managers.next() => {
                    tracing::error!(target: "tn::tasks", "{}: Sub-Task Manager {name} returned exited, node exiting", self.name);
                    break;
                }
            }
        }
        // No matter how we exit notify shutdown and allow a chance for other tasks to exit
        // cleanly.
        shutdown.notify();
        let task_name = self.name.clone();
        // wait some time for shutdown...
        // Two seconds for our tasks to end...
        if tokio::time::timeout(Duration::from_secs(2), async move {
            while let Some(res) = self.tasks.next().await {
                match res {
                    Ok(name) => {
                        tracing::info!(
                            target = "tn::tasks",
                            "{}: {name} shutdown successfully",
                            self.name
                        )
                    }
                    Err((name, err)) => tracing::error!(
                        target = "tn::tasks",
                        "{}: {name} shutdown with error {err}",
                        self.name
                    ),
                }
            }
            tracing::info!(target = "tn::tasks", "{}: All tasks shutdown", self.name);
        })
        .await
        .is_err()
        {
            tracing::error!(target = "tn::tasks", "{}: All tasks NOT shutdown", task_name);
        }

        // Another two seconds for any of our sub tasks to end...
        let task_name_clone = task_name.clone();
        if tokio::time::timeout(Duration::from_secs(2), async move {
            while let Some((_, name)) = future_managers.next().await {
                tracing::info!(
                    target = "tn::tasks",
                    "{}: TaskManager {name} shutdown successfully",
                    task_name_clone
                )
            }
            tracing::info!(
                target = "tn::tasks",
                "{}: All tasks managers shutdown",
                task_name_clone
            );
        })
        .await
        .is_err()
        {
            tracing::error!(target = "tn::tasks", "{}: All tasks managers NOT shutdown", task_name);
        }
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
            writeln!(f, "Task: {}", task.name)?;
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

impl reth_tasks::TaskSpawner for TaskManagerClone {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        tokio::spawn(fut)
    }

    fn spawn_critical(&self, name: &'static str, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
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
        self.spawn_task(name.to_string(), f);
        join
    }

    fn spawn_blocking(&self, fut: BoxFuture<'static, ()>) -> JoinHandle<()> {
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || handle.block_on(fut))
    }

    fn spawn_critical_blocking(
        &self,
        name: &'static str,
        fut: BoxFuture<'static, ()>,
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
        let handle = tokio::runtime::Handle::current();
        let join_handle = tokio::task::spawn_blocking(move || handle.block_on(f));
        if let Err(err) =
            self.new_task_tx.try_send(TaskHandle { name: name.to_string(), handle: join_handle })
        {
            tracing::error!(target: "tn::tasks", "Task error sending joiner: {err}");
        }
        join
    }
}
