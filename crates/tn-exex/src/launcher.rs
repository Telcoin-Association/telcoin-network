// SPDX-License-Identifier: MIT OR Apache-2.0
//! ExEx launcher for registering and starting execution extensions.

use crate::{TnExExContext, TnExExHandle, TnExExManager, TnExExManagerHandle};
use eyre::Result;
use std::{future::Future, pin::Pin, sync::Arc};
use tn_types::{BlockNumHash, Certificate, CommittedSubDag, TaskSpawner};
use tokio::sync::broadcast;

/// Type alias for an ExEx installation function.
///
/// This function receives a [`TnExExContext`] and returns a pinned future that:
/// - Runs for the lifetime of the ExEx
/// - Consumes notifications from `context.notifications`
/// - Sends progress events via `context.events`
/// - Has access to blockchain state via `context.provider`
///
/// # Example
///
/// ```ignore
/// fn install_my_exex<P>(ctx: TnExExContext<P>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
///     Box::pin(async move {
///         while let Some(notification) = ctx.notifications.recv().await {
///             // Process the notification
///             if let Some(chain) = notification.committed_chain() {
///                 let tip = chain.tip();
///                 // ... do work ...
///                 
///                 // Report progress
///                 ctx.events.send(TnExExEvent::FinishedHeight(
///                     BlockNumHash::new(tip.number, tip.hash())
///                 ))?;
///             }
///         }
///         Ok(())
///     })
/// }
/// ```
pub type TnExExInstallFn<Provider> = Box<
    dyn FnOnce(TnExExContext<Provider>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send,
>;

/// Builder for registering and launching execution extensions.
///
/// The launcher collects ExEx installation functions during node configuration,
/// then spawns them all at once during node startup. Each ExEx runs as an
/// independent task that receives chain state notifications.
///
/// # Lifecycle
///
/// 1. **Registration Phase**: Call [`install`](Self::install) multiple times to register ExExes
/// 2. **Launch Phase**: Call [`launch`](Self::launch) once to start all ExExes
/// 3. **Execution Phase**: ExExes run independently, manager coordinates them
///
/// # Example
///
/// ```ignore
/// let mut launcher = TnExExLauncher::new();
/// launcher.install("indexer", Box::new(install_indexer));
/// launcher.install("metrics", Box::new(install_metrics_exporter));
///
/// let (manager, handle) = launcher.launch(
///     chain_head,
///     provider,
///     task_spawner,
/// ).await?;
///
/// // Spawn manager as background task
/// task_spawner.spawn_critical("exex-manager", manager);
///
/// // Use handle to send notifications from engine
/// handle.send(notification);
/// ```
pub struct TnExExLauncher<
    Provider = reth_provider::providers::BlockchainProvider<tn_reth::traits::TelcoinNode>,
> {
    /// Registered ExExes to be launched.
    /// Tuple of (id, install_function).
    exexs: Vec<(String, TnExExInstallFn<Provider>)>,
    _phantom: std::marker::PhantomData<Provider>,
}

impl<Provider> Default for TnExExLauncher<Provider> {
    fn default() -> Self {
        Self { exexs: Vec::new(), _phantom: std::marker::PhantomData }
    }
}

impl<Provider> TnExExLauncher<Provider>
where
    Provider: Clone + Send + Sync + 'static,
{
    /// Creates a new empty launcher.
    pub fn new() -> Self {
        Self { exexs: Vec::new(), _phantom: std::marker::PhantomData }
    }

    /// Registers an ExEx to be launched.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for this ExEx (used in logs and metrics)
    /// * `install_fn` - Function that creates the ExEx future when given a context
    ///
    /// # Panics
    ///
    /// Panics if an ExEx with the same `id` is already registered.
    pub fn install(&mut self, id: impl Into<String>, install_fn: TnExExInstallFn<Provider>) {
        let id = id.into();

        // Ensure ID is unique
        if self.exexs.iter().any(|(existing_id, _)| existing_id == &id) {
            panic!("ExEx with id '{}' is already registered", id);
        }

        self.exexs.push((id, install_fn));
    }

    /// Returns the number of registered ExExes.
    pub fn len(&self) -> usize {
        self.exexs.len()
    }

    /// Returns whether any ExExes are registered.
    pub fn is_empty(&self) -> bool {
        self.exexs.is_empty()
    }

    /// Launches all registered ExExes and returns the manager and handle.
    ///
    /// This method:
    /// 1. Creates a handle for each registered ExEx
    /// 2. Creates a context with channels for each ExEx
    /// 3. Calls each install function to get the ExEx future
    /// 4. Spawns each ExEx future as a task
    /// 5. Creates the manager and handle for coordination
    ///
    /// # Arguments
    ///
    /// * `node_head` - Current chain head (ExExes start from here)
    /// * `config` - Node configuration to provide to each ExEx
    /// * `provider` - Blockchain provider for state access
    /// * `task_spawner` - Task spawner for launching ExEx tasks
    /// * `exex_certificates_rx` - Optional broadcast receiver for certificate notifications from ConsensusBus
    /// * `exex_committed_sub_dags_rx` - Optional broadcast receiver for committed sub-DAG notifications from ConsensusBus
    ///
    /// # Returns
    ///
    /// Tuple of:
    /// - `TnExExManager` - Must be spawned as a background task
    /// - `TnExExManagerHandle` - Used by engine to send notifications
    ///
    /// # Errors
    ///
    /// Returns an error if any ExEx installation function fails.
    pub async fn launch(
        mut self,
        node_head: BlockNumHash,
        config: tn_config::Config,
        provider: Provider,
        task_spawner: TaskSpawner,
        exex_certificates_rx: Option<broadcast::Receiver<Arc<Certificate>>>,
        exex_committed_sub_dags_rx: Option<broadcast::Receiver<Arc<CommittedSubDag>>>,
    ) -> Result<(TnExExManager, TnExExManagerHandle)> {
        if self.exexs.is_empty() {
            tracing::info!("No ExExes registered, using empty handle");
            return Ok((
                TnExExManager::new(vec![], None, None, None).0,
                TnExExManagerHandle::empty(),
            ));
        }

        tracing::info!("Launching {} ExEx(s)", self.exexs.len());

        let mut handles = Vec::with_capacity(self.exexs.len());

        for (id, install_fn) in self.exexs.drain(..) {
            tracing::info!(exex_id = %id, "Installing ExEx");

            // Create communication channels
            let (handle, event_tx, notification_rx) = TnExExHandle::new(id.clone(), node_head);

            // Create context for this ExEx
            let context = TnExExContext {
                head: node_head,
                config: config.clone(),
                events: event_tx,
                notifications: notification_rx,
                provider: provider.clone(),
                task_executor: task_spawner.clone(),
            };

            // Call the install function to get the ExEx future
            let exex_future = install_fn(context);

            // Spawn the ExEx as a non-critical task (if it fails, node continues)
            task_spawner.spawn_task(
                format!("exex-{}", id),
                Box::pin(async move {
                    if let Err(e) = exex_future.await {
                        tracing::error!(exex_id = %id, error = ?e, "ExEx task failed");
                    } else {
                        tracing::info!(exex_id = %id, "ExEx task completed successfully");
                    }
                    Ok(())
                }),
            );

            handles.push(handle);
        }

        // Create the manager with all handles and optional consensus receivers
        let num_exexs = handles.len();
        let (manager, manager_handle) =
            TnExExManager::new(handles, None, exex_certificates_rx, exex_committed_sub_dags_rx);

        tracing::info!(num_exexs, "ExEx manager created");

        Ok((manager, manager_handle))
    }
}

impl<Provider> std::fmt::Debug for TnExExLauncher<Provider> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TnExExLauncher").field("num_exexs", &self.exexs.len()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TnExExNotification;
    use reth_provider::Chain;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tn_types::TaskManager;
    use tokio::time::{sleep, Duration};

    #[test]
    fn test_launcher_registration() {
        let mut launcher = TnExExLauncher::<()>::new();
        assert!(launcher.is_empty());
        assert_eq!(launcher.len(), 0);

        launcher.install("exex1", Box::new(|_ctx| Box::pin(async { Ok(()) })));
        assert!(!launcher.is_empty());
        assert_eq!(launcher.len(), 1);

        launcher.install("exex2", Box::new(|_ctx| Box::pin(async { Ok(()) })));
        assert_eq!(launcher.len(), 2);
    }

    #[test]
    #[should_panic(expected = "ExEx with id 'duplicate' is already registered")]
    fn test_launcher_duplicate_panics() {
        let mut launcher = TnExExLauncher::<()>::new();
        launcher.install("duplicate", Box::new(|_ctx| Box::pin(async { Ok(()) })));
        launcher.install("duplicate", Box::new(|_ctx| Box::pin(async { Ok(()) })));
    }

    #[tokio::test]
    async fn test_empty_launcher() {
        let launcher = TnExExLauncher::<()>::new();
        let task_manager = TaskManager::new("test".to_string());
        let node_head = BlockNumHash::new(0, Default::default());

        let (_manager, handle) = launcher
            .launch(
                node_head,
                tn_config::Config::default_for_test(),
                (),
                task_manager.get_spawner(),
                None,
                None,
            )
            .await
            .unwrap();

        assert!(!handle.has_exexs());
    }

    #[tokio::test]
    async fn test_install_and_launch() {
        let mut launcher = TnExExLauncher::<()>::new();
        let notification_count = Arc::new(AtomicUsize::new(0));
        let notification_count_clone = Arc::clone(&notification_count);

        // Register a simple ExEx that counts notifications
        launcher.install(
            "counter",
            Box::new(move |mut ctx| {
                let count = Arc::clone(&notification_count_clone);
                Box::pin(async move {
                    while let Some(_notification) = ctx.notifications.recv().await {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(())
                })
            }),
        );

        let task_manager = TaskManager::new("test".to_string());
        let node_head = BlockNumHash::new(0, Default::default());

        let (manager, handle) = launcher
            .launch(
                node_head,
                tn_config::Config::default_for_test(),
                (),
                task_manager.get_spawner(),
                None,
                None,
            )
            .await
            .unwrap();

        assert!(handle.has_exexs());

        // Spawn manager
        let mut manager = Box::pin(manager);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(10)).await;
                std::future::poll_fn(|cx| {
                    let _ = Pin::new(&mut manager).poll(cx);
                    std::task::Poll::Ready(())
                })
                .await;
            }
        });

        // Send a few notifications
        for _ in 0..3 {
            handle.send(TnExExNotification::ChainCommitted { new: Arc::new(Chain::default()) });
        }

        // Give it time to process
        sleep(Duration::from_millis(100)).await;

        // Verify ExEx received them
        assert_eq!(notification_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_multiple_exexs() {
        let mut launcher = TnExExLauncher::<()>::new();

        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        let count1_clone = Arc::clone(&count1);
        let count2_clone = Arc::clone(&count2);

        // Register two ExExes
        launcher.install(
            "exex1",
            Box::new(move |mut ctx| {
                let count = Arc::clone(&count1_clone);
                Box::pin(async move {
                    while let Some(_notification) = ctx.notifications.recv().await {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(())
                })
            }),
        );

        launcher.install(
            "exex2",
            Box::new(move |mut ctx| {
                let count = Arc::clone(&count2_clone);
                Box::pin(async move {
                    while let Some(_notification) = ctx.notifications.recv().await {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(())
                })
            }),
        );

        let task_manager = TaskManager::new("test".to_string());
        let node_head = BlockNumHash::new(0, Default::default());

        let (manager, handle) = launcher
            .launch(
                node_head,
                tn_config::Config::default_for_test(),
                (),
                task_manager.get_spawner(),
                None,
                None,
            )
            .await
            .unwrap();
        let mut manager = Box::pin(manager);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(10)).await;
                std::future::poll_fn(|cx| {
                    let _ = Pin::new(&mut manager).poll(cx);
                    std::task::Poll::Ready(())
                })
                .await;
            }
        });

        // Send notifications
        for _ in 0..2 {
            handle.send(TnExExNotification::ChainCommitted { new: Arc::new(Chain::default()) });
        }

        sleep(Duration::from_millis(150)).await;

        // Both ExExes should receive the same notifications
        assert_eq!(count1.load(Ordering::Relaxed), 2);
        assert_eq!(count2.load(Ordering::Relaxed), 2);
    }
}
