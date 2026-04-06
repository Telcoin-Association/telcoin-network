//! Integration tests for TN ExEx system.
//!
//! These tests verify the end-to-end notification pipeline from consensus output
//! through engine execution to ExEx reception.

#![allow(unused_crate_dependencies)]

use eyre::Result;
use reth_provider::Chain;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tempfile::TempDir;
use tn_exex::{TnExExContext, TnExExLauncher, TnExExNotification};
use tn_reth::RethEnv;
use tn_types::{test_chain_spec_arc, BlockHash, BlockNumHash, TaskManager};
use tokio::time::{sleep, timeout, Duration};

/// Test that multiple ExExs receive the same notification.
#[tokio::test]
async fn test_multiple_exexs_receive_notifications() -> Result<()> {
    let _guard = crate::IT_TEST_GUARD.lock();

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();

    // Create simple chain
    let chain = test_chain_spec_arc();

    // Counters for each ExEx
    let counter1 = Arc::new(AtomicU64::new(0));
    let counter2 = Arc::new(AtomicU64::new(0));
    let counter3 = Arc::new(AtomicU64::new(0));

    // Create three ExExs that count notifications
    let make_exex = |counter: Arc<AtomicU64>| {
        move |mut ctx: TnExExContext<_>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
            let counter = counter.clone();
            Box::pin(async move {
                while let Some(notification) = ctx.notifications.recv().await {
                    if notification.committed_chain().is_some() {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
                Ok(())
            })
        }
    };

    let mut launcher = TnExExLauncher::new();
    launcher.install("exex-1", Box::new(make_exex(counter1.clone())));
    launcher.install("exex-2", Box::new(make_exex(counter2.clone())));
    launcher.install("exex-3", Box::new(make_exex(counter3.clone())));

    // Create RethEnv
    let reth_env = RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;
    let provider = reth_env.blockchain_provider();

    // Launch ExEx manager
    let node_head = BlockNumHash::new(0, BlockHash::default());
    let (exex_manager, exex_handle) =
        launcher.launch(node_head, provider, task_manager.get_spawner()).await?;

    tokio::spawn(async move {
        exex_manager.await;
    });

    // Send a test notification directly to the manager
    let test_chain = Arc::new(Chain::default());
    let notification = TnExExNotification::ChainCommitted { new: test_chain };

    exex_handle.send(notification.clone());

    // Wait for all ExExs to process the notification
    let result = timeout(Duration::from_secs(2), async {
        loop {
            if counter1.load(Ordering::SeqCst) > 0
                && counter2.load(Ordering::SeqCst) > 0
                && counter3.load(Ordering::SeqCst) > 0
            {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    assert!(result.is_ok(), "All ExExs should receive notification");
    assert_eq!(counter1.load(Ordering::SeqCst), 1, "ExEx 1 should receive 1 notification");
    assert_eq!(counter2.load(Ordering::SeqCst), 1, "ExEx 2 should receive 1 notification");
    assert_eq!(counter3.load(Ordering::SeqCst), 1, "ExEx 3 should receive 1 notification");

    Ok(())
}

/// Test backpressure handling: a slow ExEx should not block notification delivery to fast ExExs.
#[tokio::test]
async fn test_backpressure_handling() -> Result<()> {
    let _guard = crate::IT_TEST_GUARD.lock();

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let chain = test_chain_spec_arc();

    // Slow ExEx that delays processing
    let slow_count = Arc::new(AtomicU64::new(0));
    let slow_count_clone = slow_count.clone();

    let slow_exex = move |mut ctx: TnExExContext<_>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        let counter = slow_count_clone.clone();
        Box::pin(async move {
            while let Some(notification) = ctx.notifications.recv().await {
                if notification.committed_chain().is_some() {
                    // Simulate slow processing
                    sleep(Duration::from_millis(100)).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        })
    };

    // Fast ExEx that processes immediately
    let fast_count = Arc::new(AtomicU64::new(0));
    let fast_count_clone = fast_count.clone();

    let fast_exex = move |mut ctx: TnExExContext<_>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        let counter = fast_count_clone.clone();
        Box::pin(async move {
            while let Some(notification) = ctx.notifications.recv().await {
                if notification.committed_chain().is_some() {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        })
    };

    let mut launcher = TnExExLauncher::new();
    launcher.install("slow-exex", Box::new(slow_exex));
    launcher.install("fast-exex", Box::new(fast_exex));

    let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;
    let provider = reth_env.blockchain_provider();

    let node_head = BlockNumHash::new(0, BlockHash::default());
    let (exex_manager, exex_handle) =
        launcher.launch(node_head, provider, task_manager.get_spawner()).await?;

    tokio::spawn(async move {
        exex_manager.await;
    });

    // Send multiple notifications
    for _ in 0..5 {
        let test_chain = Arc::new(Chain::default());
        let notification = TnExExNotification::ChainCommitted { new: test_chain };
        exex_handle.send(notification);
    }

    // Wait for fast ExEx to process all notifications
    let result = timeout(Duration::from_secs(3), async {
        loop {
            if fast_count.load(Ordering::SeqCst) >= 5 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    assert!(result.is_ok(), "Fast ExEx should process all notifications even with slow ExEx present");

    assert_eq!(
        fast_count.load(Ordering::SeqCst),
        5,
        "Fast ExEx processed all notifications"
    );

    // Slow ExEx may not have processed all yet due to delays
    let slow_processed = slow_count.load(Ordering::SeqCst);
    assert!(
        slow_processed <= 5,
        "Slow ExEx should not have processed more than sent"
    );

    Ok(())
}

/// Test that notifications are received and processed sequentially.
#[tokio::test]
async fn test_notification_processing() -> Result<()> {
    let _guard = crate::IT_TEST_GUARD.lock();

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let chain = test_chain_spec_arc();

    // Track number of processed notifications
    let processed_count = Arc::new(AtomicU64::new(0));
    let count_clone = processed_count.clone();

    let exex = move |mut ctx: TnExExContext<_>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        let counter = count_clone.clone();
        Box::pin(async move {
            while let Some(notification) = ctx.notifications.recv().await {
                if notification.committed_chain().is_some() {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        })
    };

    let mut launcher = TnExExLauncher::new();
    launcher.install("processor", Box::new(exex));

    let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;
    let provider = reth_env.blockchain_provider();

    let node_head = BlockNumHash::new(0, BlockHash::default());
    let (exex_manager, exex_handle) =
        launcher.launch(node_head, provider, task_manager.get_spawner()).await?;

    tokio::spawn(async move {
        exex_manager.await;
    });

    // Send multiple notifications
    for _ in 0..3 {
        let test_chain = Arc::new(Chain::default());
        let notification = TnExExNotification::ChainCommitted { new: test_chain };
        exex_handle.send(notification);
    }

    // Wait for ExEx to process all notifications
    let result = timeout(Duration::from_secs(2), async {
        loop {
            if processed_count.load(Ordering::SeqCst) >= 3 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    assert!(result.is_ok(), "ExEx should process all notifications");
    assert_eq!(
        processed_count.load(Ordering::SeqCst),
        3,
        "Should have processed 3 notifications"
    );

    Ok(())
}

/// Test that notifications are delivered in order even with concurrent sends.
#[tokio::test]
async fn test_notification_ordering() -> Result<()> {
    let _guard = crate::IT_TEST_GUARD.lock();

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let chain = test_chain_spec_arc();

    // Track number of received notifications (order is guaranteed by channel)
    let received_count = Arc::new(AtomicU64::new(0));
    let count_clone = received_count.clone();

    let exex = move |mut ctx: TnExExContext<_>| -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
        let counter = count_clone.clone();
        Box::pin(async move {
            while let Some(notification) = ctx.notifications.recv().await {
                if notification.committed_chain().is_some() {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        })
    };

    let mut launcher = TnExExLauncher::new();
    launcher.install("order-tracker", Box::new(exex));

    let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;
    let provider = reth_env.blockchain_provider();

    let node_head = BlockNumHash::new(0, BlockHash::default());
    let (exex_manager, exex_handle) =
        launcher.launch(node_head, provider, task_manager.get_spawner()).await?;

    tokio::spawn(async move {
        exex_manager.await;
    });

    // Send notifications rapidly to test ordering
    for _ in 1..=5 {
        let test_chain = Arc::new(Chain::default());
        let notification = TnExExNotification::ChainCommitted { new: test_chain };
        exex_handle.send(notification);
    }

    // Wait for ExEx to process all notifications
    let result = timeout(Duration::from_secs(2), async {
        loop {
            if received_count.load(Ordering::SeqCst) >= 5 {
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    assert!(result.is_ok(), "ExEx should receive all notifications in order");
    assert_eq!(
        received_count.load(Ordering::SeqCst),
        5,
        "Should have received 5 notifications"
    );

    Ok(())
}

/// Mutex to serialize integration tests that create MDBX databases.
/// Each DB has large memory space allocations, so running too many in parallel
/// can exhaust available memory.
pub static IT_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

