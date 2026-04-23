//! Task manager interface to spawn tasks to the tokio runtime.

use crate::Notifier;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{
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

/// The error type for tasks.
pub type TaskError = eyre::Report;
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
    local_shutdown: Notifier,
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
    local_shutdown: Notifier,
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
        if let Err(err) = self.new_task_tx.try_send(TaskHandle::new(name.clone(), handle, critical))
        {
            tracing::error!(target: "tn::tasks", "Task error sending joiner for {name}: {err}");
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
            local_shutdown: Notifier::default(),
            join_wait_millis: 2000,
        }
    }

    /// Sets the amount of time this task manager will wait on tasks/submanagers to complete.
    /// Will be used twice in join, once for tasks and once for subtasks so max wait will
    /// 2x this value (min).
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

    /// Will resolve once one of the tasks for the manager resolves, shutdown is notified or the
    /// node recieves SIGTERM.
    ///
    /// Note the manager is based on the assumption that all tasks added via spawn_task
    /// are critical and and one stopping is problem.
    /// Also will end if the user hits ctrl-c or sends a SIGTERM to the app.
    /// This will not join the tasks but resolves as soon as shutdown is indicated.
    pub async fn until_exit(&mut self, shutdown: Notifier) -> Result<(), TaskJoinError> {
        self.until_exit_internal(shutdown, true).await
    }

    /// Will resolve once one of the tasks for the manager resolves or shutdown is notified.
    ///
    /// Note the manager is based on the assumption that all tasks added via spawn_task
    /// are critical and and one stopping is problem.
    /// This will not join the tasks but resolves as soon as shutdown is indicated.
    pub async fn until_task_ends(&mut self, shutdown: Notifier) -> Result<(), TaskJoinError> {
        self.until_exit_internal(shutdown, false).await
    }

    /// Will resolve once all tasks have shutdown or timeout is reached.
    /// Can be used after until_exit to split up the join.
    pub async fn wait_for_task_shutdown(&mut self) {
        self.wait_for_task_shutdown_internal().await
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
        shutdown: Notifier,
        do_exit: bool,
    ) -> Result<(), TaskJoinError> {
        let rx_shutdown = shutdown.subscribe();
        let mut result = Ok(());
        loop {
            tokio::select! {
                _ = Self::exit(do_exit) => {
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
                            // Ok exit is fine if we are shutting down.
                            if !rx_shutdown.noticed() {
                                tracing::error!(target: "tn::tasks", "{}: {} returned error {join_err}, exiting", self.name, info.name);
                                result = Err(join_err);
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

    /// Attempt to wait for all the tasks to complete or timeout waiting on them.
    async fn wait_for_task_shutdown_internal(&mut self) {
        let task_name = self.name.clone();
        let join_wait = Duration::from_millis(self.join_wait_millis);
        // wait some time for shutdown...
        // 2 seconds for our tasks to end...
        if tokio::time::timeout(join_wait, async move {
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
                        )
                    }
                }
            }
            tracing::info!(target: "tn::tasks", "{}: All tasks shutdown", self.name);
        })
        .await
        .is_err()
        {
            tracing::error!(target:"tn::tasks", "{}: All tasks NOT shutdown", task_name);
        }
    }

    /// Implements the join logic for the manager.
    async fn join_internal(
        &mut self,
        shutdown: Notifier,
        do_exit: bool,
    ) -> Result<(), TaskJoinError> {
        let result = self.until_exit_internal(shutdown, do_exit).await;
        self.wait_for_task_shutdown_internal().await;
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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::sync::mpsc::{self, Receiver, Sender};

    use crate::{Notifier, TaskJoinError, TaskManager};

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

        tokio::time::sleep(Duration::from_secs(1)).await;
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

    /// Test that an error produced in a task makes it up to join.
    #[tokio::test]
    async fn test_task_manager_join() {
        let mut task_manager = TaskManager::default();
        task_manager.spawn_critical_task("Crit 1", async move { Ok(()) });
        match task_manager.join(Notifier::default()).await {
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
        task_manager.spawn_critical_task("Crit 2", async move { Err(eyre::eyre!("BOOM!")) });
        match task_manager.join(Notifier::default()).await {
            Ok(_) => {}
            Err(TaskJoinError::CriticalExitOk(_name)) => panic!("should not be OK"),
            Err(TaskJoinError::CriticalJoinError(_name, _err)) => panic!("wrong error"),
            Err(TaskJoinError::CriticalExitError(name, _err)) => assert!(name.eq("Crit 2")),
        }
    }
}
