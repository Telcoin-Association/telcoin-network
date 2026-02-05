//! Unit tests for metered channels

use super::{channel, channel_sender, channel_with_total};
use futures::{
    task::{noop_waker, Context, Poll},
    FutureExt,
};
use prometheus::{IntCounter, IntGauge};
use tn_types::{TnReceiver, TnSender, TrySendError};

#[tokio::test]
async fn test_send() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = channel(8, &counter);

    assert_eq!(counter.get(), 0);
    let item = 42;
    tx.send(item).await.unwrap();
    assert_eq!(counter.get(), 1);
    let received_item = rx.recv().await.unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);
}

#[tokio::test]
async fn test_total() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let counter_total = IntCounter::new("TEST_TOTAL", "test_total").unwrap();
    let (tx, mut rx) = channel_with_total(8, &counter, &counter_total);

    assert_eq!(counter.get(), 0);
    let item = 42;
    tx.send(item).await.unwrap();
    assert_eq!(counter.get(), 1);
    let received_item = rx.recv().await.unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);
    assert_eq!(counter_total.get(), 1);
}

#[tokio::test]
async fn test_empty_closed_channel() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = channel(8, &counter);

    assert_eq!(counter.get(), 0);
    let item = 42;
    tx.send(item).await.unwrap();
    assert_eq!(counter.get(), 1);

    let received_item = rx.recv().await.unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);

    // channel is empty
    let res = rx.try_recv();
    assert!(res.is_err());
    assert_eq!(counter.get(), 0);

    // channel is closed
    rx.close();
    let res2 = rx.recv().now_or_never().unwrap();
    assert!(res2.is_none());
    assert_eq!(counter.get(), 0);
}

#[tokio::test]
async fn test_send_backpressure() {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = channel(1, &counter);

    assert_eq!(counter.get(), 0);
    tx.send(1).await.unwrap();
    assert_eq!(counter.get(), 1);

    let mut task = Box::pin(tx.send(2));
    assert!(matches!(task.poll_unpin(&mut cx), Poll::Pending));
    let item = rx.recv().await.unwrap();
    assert_eq!(item, 1);
    assert_eq!(counter.get(), 0);
    assert!(task.now_or_never().is_some());
    assert_eq!(counter.get(), 1);
}

#[tokio::test]
async fn test_send_backpressure_multi_senders() {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx1, mut rx) = channel(1, &counter);

    assert_eq!(counter.get(), 0);
    tx1.send(1).await.unwrap();
    assert_eq!(counter.get(), 1);

    let tx2 = tx1;
    let mut task = Box::pin(tx2.send(2));
    assert!(matches!(task.poll_unpin(&mut cx), Poll::Pending));
    let item = rx.recv().await.unwrap();
    assert_eq!(item, 1);
    assert_eq!(counter.get(), 0);
    assert!(task.now_or_never().is_some());
    assert_eq!(counter.get(), 1);
}

#[tokio::test]
async fn test_try_send() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = channel(1, &counter);

    assert_eq!(counter.get(), 0);
    let item = 42;
    tx.try_send(item).unwrap();
    assert_eq!(counter.get(), 1);
    let received_item = rx.recv().await.unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);
}

#[tokio::test]
async fn test_try_send_full() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = channel(2, &counter);

    assert_eq!(counter.get(), 0);
    let item = 42;
    tx.try_send(item).unwrap();
    assert_eq!(counter.get(), 1);
    tx.try_send(item).unwrap();
    assert_eq!(counter.get(), 2);
    if let Err(e) = tx.try_send(item) {
        assert!(matches!(e, TrySendError::Full(_)));
    } else {
        panic!("Expect try_send return channel being full error");
    }

    let received_item = rx.recv().await.unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 1);
    let received_item = rx.recv().await.unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);
}

#[tokio::test]
async fn test_send_before_subscribe_is_noop() {
    // Create a channel via channel_sender (no receiver handed out yet).
    // Sends should be no-ops since subscribed starts false.
    let counter = IntGauge::new("TEST_COUNTER_NO_SUB", "test").unwrap();
    let tx = channel_sender::<i32>(8, &counter);

    // send() should succeed but be a no-op (counter stays 0)
    let result = tx.send(42).await;
    assert!(result.is_ok());
    assert_eq!(counter.get(), 0);

    // try_send() should also succeed but be a no-op
    let result = tx.try_send(99);
    assert!(result.is_ok());
    assert_eq!(counter.get(), 0);

    // After subscribe, sends should go through
    let mut rx = tx.subscribe();
    let result = tx.send(7).await;
    assert!(result.is_ok());
    assert_eq!(counter.get(), 1);
    let received = rx.recv().await.unwrap();
    assert_eq!(received, 7);
}

#[tokio::test]
async fn test_send_no_op_after_subscribe_drop() {
    // Create a channel via channel_sender, subscribe and immediately drop.
    // After the receiver is dropped, sends become no-ops.
    let counter = IntGauge::new("TEST_COUNTER_SUB_THEN_DROP", "test").unwrap();
    let tx = channel_sender::<i32>(8, &counter);

    // Subscribe and immediately drop to set subscribed = false
    drop(tx.subscribe());

    // send() should be a no-op
    let result = tx.send(42).await;
    assert!(result.is_ok());
    assert_eq!(counter.get(), 0);

    // try_send() should also be a no-op
    let result = tx.try_send(99);
    assert!(result.is_ok());
    assert_eq!(counter.get(), 0);
}

#[tokio::test]
async fn test_send_after_subscribe() {
    // Create a channel via channel_sender, subscribe, then send.
    // The message should arrive at the receiver.
    let counter = IntGauge::new("TEST_COUNTER_AFTER_SUB", "test").unwrap();
    let tx = channel_sender::<i32>(8, &counter);

    let mut rx = tx.subscribe();

    let item = 42;
    tx.send(item).await.unwrap();
    assert_eq!(counter.get(), 1);

    let received = rx.recv().await.unwrap();
    assert_eq!(received, item);
    assert_eq!(counter.get(), 0);
}

#[tokio::test]
async fn test_send_after_subscribe_then_drop() {
    // Create a channel via channel_sender, subscribe, drop the receiver,
    // then send. The send should be a no-op and return Ok(()).
    let counter = IntGauge::new("TEST_COUNTER_SUB_DROP", "test").unwrap();
    let tx = channel_sender::<i32>(8, &counter);

    let rx = tx.subscribe();
    // Drop the receiver - this should set subscribed to false
    drop(rx);

    // send() should now be a no-op
    let result = tx.send(42).await;
    assert!(result.is_ok());
    assert_eq!(counter.get(), 0);

    // try_send() should also be a no-op
    let result = tx.try_send(99);
    assert!(result.is_ok());
    assert_eq!(counter.get(), 0);
}
