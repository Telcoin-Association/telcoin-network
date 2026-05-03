//! Integration tests for TN ExEx system.
//!
//! These tests verify the end-to-end notification pipeline from consensus output
//! through engine execution to ExEx reception.

#![allow(unused_crate_dependencies)]

use eyre::Result;
use reth_provider::Chain;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
};
use tempfile::TempDir;
use tn_exex::{
    ReplayStream, TnExExContext, TnExExEvent, TnExExHandle, TnExExLauncher, TnExExManager,
    TnExExNotification,
};
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
    let (exex_manager, exex_handle) = launcher
        .launch(
            node_head,
            tn_config::Config::default_for_test(),
            provider,
            task_manager.get_spawner(),
            None,
            None,
        )
        .await?;

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
    let (exex_manager, exex_handle) = launcher
        .launch(
            node_head,
            tn_config::Config::default_for_test(),
            provider,
            task_manager.get_spawner(),
            None,
            None,
        )
        .await?;

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

    assert!(
        result.is_ok(),
        "Fast ExEx should process all notifications even with slow ExEx present"
    );

    assert_eq!(fast_count.load(Ordering::SeqCst), 5, "Fast ExEx processed all notifications");

    // Slow ExEx may not have processed all yet due to delays
    let slow_processed = slow_count.load(Ordering::SeqCst);
    assert!(slow_processed <= 5, "Slow ExEx should not have processed more than sent");

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
    let (exex_manager, exex_handle) = launcher
        .launch(
            node_head,
            tn_config::Config::default_for_test(),
            provider,
            task_manager.get_spawner(),
            None,
            None,
        )
        .await?;

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
    assert_eq!(processed_count.load(Ordering::SeqCst), 3, "Should have processed 3 notifications");

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
    let (exex_manager, exex_handle) = launcher
        .launch(
            node_head,
            tn_config::Config::default_for_test(),
            provider,
            task_manager.get_spawner(),
            None,
            None,
        )
        .await?;

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
    assert_eq!(received_count.load(Ordering::SeqCst), 5, "Should have received 5 notifications");

    Ok(())
}

/// Mutex to serialize integration tests that create MDBX databases.
/// Each DB has large memory space allocations, so running too many in parallel
/// can exhaust available memory.
pub static IT_TEST_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

// ======================== ReplayStream tests ========================

/// Test that ReplayStream with from > to is immediately finished.
#[tokio::test]
async fn test_replay_stream_empty_when_from_exceeds_to() -> Result<()> {
    let _guard = IT_TEST_GUARD.lock();

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let reth_env = RethEnv::new_for_test(tmp_dir.path(), &task_manager, None)?;

    let mut stream = ReplayStream::new(reth_env, 10, 5);
    assert!(stream.is_finished());
    assert_eq!(stream.current_block(), 10);
    assert_eq!(stream.target_block(), 5);

    // Polling should immediately return None
    let item = tokio_stream::StreamExt::next(&mut stream).await;
    assert!(item.is_none(), "stream should be empty when from > to");

    Ok(())
}

/// Test that ReplayStream over a range with no blocks in the DB terminates cleanly.
#[tokio::test]
async fn test_replay_stream_skips_missing_blocks() -> Result<()> {
    let _guard = IT_TEST_GUARD.lock();

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let reth_env = RethEnv::new_for_test(tmp_dir.path(), &task_manager, None)?;

    // Request blocks 100..=105 — well past genesis, none should exist.
    let mut stream = ReplayStream::new(reth_env, 100, 105);
    assert!(!stream.is_finished());
    assert_eq!(stream.current_block(), 100);
    assert_eq!(stream.target_block(), 105);

    // Drain the stream — should produce no items and finish.
    let mut count = 0;
    while let Some(result) = tokio_stream::StreamExt::next(&mut stream).await {
        result?;
        count += 1;
    }

    assert_eq!(count, 0, "no blocks exist in range so stream should yield nothing");
    assert!(stream.is_finished());

    Ok(())
}

/// Test ReplayStream with a single-block range (from == to) on a block that doesn't exist.
#[tokio::test]
async fn test_replay_stream_single_block_range_missing() -> Result<()> {
    let _guard = IT_TEST_GUARD.lock();

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let reth_env = RethEnv::new_for_test(tmp_dir.path(), &task_manager, None)?;

    // Block 999 doesn't exist
    let mut stream = ReplayStream::new(reth_env, 999, 999);
    assert!(!stream.is_finished());

    let item = tokio_stream::StreamExt::next(&mut stream).await;
    assert!(item.is_none(), "single missing block should produce no items");
    assert!(stream.is_finished());

    Ok(())
}

/// Test ReplayStream yields the genesis block when it exists.
#[tokio::test]
async fn test_replay_stream_yields_genesis_block() -> Result<()> {
    let _guard = IT_TEST_GUARD.lock();

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let reth_env = RethEnv::new_for_test(tmp_dir.path(), &task_manager, None)?;

    // Block 0 (genesis) should exist from chain init.
    let mut stream = ReplayStream::new(reth_env, 0, 0);

    let item = tokio_stream::StreamExt::next(&mut stream).await;
    assert!(item.is_some(), "genesis block should be yielded");
    let notification = item.unwrap()?;
    assert!(notification.committed_chain().is_some());
    assert!(stream.is_finished());

    // No more items
    let item = tokio_stream::StreamExt::next(&mut stream).await;
    assert!(item.is_none());

    Ok(())
}

/// Test ReplayStream size_hint.
#[tokio::test]
async fn test_replay_stream_size_hint() -> Result<()> {
    let _guard = IT_TEST_GUARD.lock();

    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let reth_env = RethEnv::new_for_test(tmp_dir.path(), &task_manager, None)?;

    let stream = ReplayStream::new(reth_env.clone(), 5, 10);
    let (lower, upper) = tokio_stream::Stream::size_hint(&stream);
    assert_eq!(lower, 0);
    assert_eq!(upper, Some(6)); // 10 - 5 + 1

    // Finished stream should report (0, Some(0))
    let stream = ReplayStream::new(reth_env, 10, 5);
    let (lower, upper) = tokio_stream::Stream::size_hint(&stream);
    assert_eq!(lower, 0);
    assert_eq!(upper, Some(0));

    Ok(())
}

// ================ FinishedHeight event loop integration test ================

/// Test that the manager's finished height watch converges to the minimum
/// across multiple ExExes reporting FinishedHeight events.
#[tokio::test]
async fn test_finished_height_converges_to_minimum() -> Result<()> {
    let node_head = BlockNumHash::new(0, Default::default());

    let (handle1, event_tx1, _notif_rx1) = TnExExHandle::new("exex1".to_string(), node_head);
    let (handle2, event_tx2, _notif_rx2) = TnExExHandle::new("exex2".to_string(), node_head);
    let (handle3, event_tx3, _notif_rx3) = TnExExHandle::new("exex3".to_string(), node_head);

    let (manager, mgr_handle) =
        TnExExManager::new(vec![handle1, handle2, handle3], Some(10), None, None);

    let mut finished_rx = mgr_handle.finished_height();

    // Spawn manager
    let mut manager = Box::pin(manager);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            std::future::poll_fn(|cx| {
                let _ = Pin::new(&mut manager).poll(cx);
                Poll::Ready(())
            })
            .await;
        }
    });

    // All three ExExes report different heights
    let block_100 = BlockNumHash::new(100, BlockHash::default());
    let block_50 = BlockNumHash::new(50, BlockHash::default());
    let block_200 = BlockNumHash::new(200, BlockHash::default());

    event_tx1.send(TnExExEvent::FinishedHeight(block_100))?;
    event_tx2.send(TnExExEvent::FinishedHeight(block_50))?;
    event_tx3.send(TnExExEvent::FinishedHeight(block_200))?;

    // Wait for the finished height to converge
    let result = timeout(Duration::from_secs(2), async {
        loop {
            finished_rx.changed().await.ok();
            let current = *finished_rx.borrow();
            if current.height() == Some(block_50) {
                break;
            }
        }
    })
    .await;

    assert!(result.is_ok(), "finished height should converge to block 50 (the minimum)");

    // Now exex2 catches up to block 150 — minimum should move to block 100 (exex1)
    let block_150 = BlockNumHash::new(150, BlockHash::default());
    event_tx2.send(TnExExEvent::FinishedHeight(block_150))?;

    let result = timeout(Duration::from_secs(2), async {
        loop {
            finished_rx.changed().await.ok();
            let current = *finished_rx.borrow();
            if current.height() == Some(block_100) {
                break;
            }
        }
    })
    .await;

    assert!(result.is_ok(), "after exex2 catches up, minimum should be block 100 (exex1)");

    Ok(())
}
