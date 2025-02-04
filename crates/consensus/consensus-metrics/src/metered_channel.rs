//! Wrapper around mpsc channel that also captures useful metrics.

use futures::{FutureExt, Stream, TryFutureExt};
use parking_lot::Mutex;
use prometheus::{IntCounter, IntGauge};
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tn_types::{TnReceiver, TnSender};
use tokio::sync::mpsc::{self};

#[cfg(test)]
#[path = "tests/metered_channel_tests.rs"]
mod metered_channel_tests;

/// An [`mpsc::Sender`] with an [`IntGauge`]
/// counting the number of currently queued items.
#[derive(Debug)]
pub struct MeteredMpscChannel<T> {
    inner: mpsc::Sender<T>,
    gauge: IntGauge,
    receiver: Arc<Mutex<Option<Receiver<T>>>>,
}

impl<T> Clone for MeteredMpscChannel<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            gauge: self.gauge.clone(),
            receiver: self.receiver.clone(),
        }
    }
}

/// An [`mpsc::Receiver`] with an [`IntGauge`]
/// counting the number of currently queued items.
#[derive(Debug)]
pub struct Receiver<T> {
    inner: mpsc::Receiver<T>,
    gauge: IntGauge,
    total: Option<IntCounter>,
}

impl<T> Receiver<T> {
    /// Closes the receiving half of a channel without dropping it.
    pub fn close(&mut self) {
        self.inner.close()
    }
}

impl<T: Send> TnReceiver<T> for Receiver<T> {
    fn recv(&mut self) -> impl std::future::Future<Output = Option<T>> + Send {
        self.inner.recv().inspect(|opt| {
            if opt.is_some() {
                self.gauge.dec();
                if let Some(total_gauge) = &self.total {
                    total_gauge.inc();
                }
            }
        })
    }

    fn try_recv(&mut self) -> Result<T, tn_types::TryRecvError> {
        Ok(self.inner.try_recv().inspect(|_| {
            self.gauge.dec();
            if let Some(total_gauge) = &self.total {
                total_gauge.inc();
            }
        })?)
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.inner.poll_recv(cx) {
            res @ Poll::Ready(Some(_)) => {
                self.gauge.dec();
                if let Some(total_gauge) = &self.total {
                    total_gauge.inc();
                }
                res
            }
            s => s,
        }
    }
}

impl<T> Unpin for Receiver<T> {}

impl<T> MeteredMpscChannel<T> {
    /// Completes when the receiver has dropped.
    pub async fn closed(&self) {
        self.inner.closed().await
    }

    /// Checks if the channel has been closed. This happens when the
    /// [`Receiver`] is dropped, or when the [`Receiver::close`] method is
    /// called.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Returns the current capacity of the channel.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Returns a reference to the underlying gauge.
    pub fn gauge(&self) -> &IntGauge {
        &self.gauge
    }
}

impl<T: Send + 'static> TnSender<T> for MeteredMpscChannel<T> {
    /// Sends a value, waiting until there is capacity.
    /// Increments the gauge in case of a successful `send`.
    fn send(
        &self,
        value: T,
    ) -> impl std::future::Future<Output = Result<(), tn_types::SendError<T>>> + Send {
        self.inner.send(value).inspect_ok(|_| self.gauge.inc()).map_err(|e| e.into())
    }

    /// Attempts to immediately send a message on this `Sender`
    /// Increments the gauge in case of a successful `try_send`.
    fn try_send(&self, message: T) -> Result<(), tn_types::TrySendError<T>> {
        Ok(self
            .inner
            .try_send(message)
            // remove this unsightly hack once https://github.com/rust-lang/rust/issues/91345 is resolved
            .inspect(|_| {
                self.gauge.inc();
            })?)
    }

    fn subscribe(&self) -> impl TnReceiver<T> + 'static {
        self.receiver.lock().take().expect("No receiver to subscribe, can only subscribe once!")
    }
}

////////////////////////////////
// Stream API Wrappers!
////////////////////////////////

/// A wrapper around [`crate::metered_channel::Receiver`] that implements [`Stream`].
#[derive(Debug)]
pub struct ReceiverStream<T> {
    inner: Receiver<T>,
}

impl<T> ReceiverStream<T> {
    /// Create a new `ReceiverStream`.
    pub fn new(recv: Receiver<T>) -> Self {
        Self { inner: recv }
    }

    /// Get back the inner `Receiver`.
    pub fn into_inner(self) -> Receiver<T> {
        self.inner
    }

    /// Closes the receiving half of a channel without dropping it.
    pub fn close(&mut self) {
        self.inner.close()
    }
}

impl<T: Send> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

impl<T> AsRef<Receiver<T>> for ReceiverStream<T> {
    fn as_ref(&self) -> &Receiver<T> {
        &self.inner
    }
}

impl<T> AsMut<Receiver<T>> for ReceiverStream<T> {
    fn as_mut(&mut self) -> &mut Receiver<T> {
        &mut self.inner
    }
}

impl<T> From<Receiver<T>> for ReceiverStream<T> {
    fn from(recv: Receiver<T>) -> Self {
        Self::new(recv)
    }
}

////////////////////////////////////////////////////////////////
// Constructor
////////////////////////////////////////////////////////////////

/// Similar to `mpsc::channel`, `channel` creates a pair of `Sender` and `Receiver`
#[track_caller]
pub fn channel<T>(size: usize, gauge: &IntGauge) -> (MeteredMpscChannel<T>, Receiver<T>) {
    gauge.set(0);
    let (sender, receiver) = mpsc::channel(size);
    (
        MeteredMpscChannel {
            inner: sender,
            gauge: gauge.clone(),
            receiver: Arc::new(Mutex::new(None)),
        },
        Receiver { inner: receiver, gauge: gauge.clone(), total: None },
    )
}

#[track_caller]
pub fn channel_with_total<T>(
    size: usize,
    gauge: &IntGauge,
    total_gauge: &IntCounter,
) -> (MeteredMpscChannel<T>, Receiver<T>) {
    gauge.set(0);
    let (sender, receiver) = mpsc::channel(size);
    (
        MeteredMpscChannel {
            inner: sender,
            gauge: gauge.clone(),
            receiver: Arc::new(Mutex::new(None)),
        },
        Receiver { inner: receiver, gauge: gauge.clone(), total: Some(total_gauge.clone()) },
    )
}

/// Similar to `mpsc::channel`, `channel` creates a pair of `Sender` and `Receiver`
/// This version will save the reciever in the sender for one time subscribtion.
pub fn channel_sender<T>(size: usize, gauge: &IntGauge) -> MeteredMpscChannel<T> {
    gauge.set(0);
    let (sender, receiver) = mpsc::channel(size);
    let rx = Receiver { inner: receiver, gauge: gauge.clone(), total: None };
    MeteredMpscChannel {
        inner: sender,
        gauge: gauge.clone(),
        receiver: Arc::new(Mutex::new(Some(rx))),
    }
}

pub fn channel_with_total_sender<T>(
    size: usize,
    gauge: &IntGauge,
    total_gauge: &IntCounter,
) -> MeteredMpscChannel<T> {
    gauge.set(0);
    let (sender, receiver) = mpsc::channel(size);
    let rx = Receiver { inner: receiver, gauge: gauge.clone(), total: Some(total_gauge.clone()) };
    MeteredMpscChannel {
        inner: sender,
        gauge: gauge.clone(),
        receiver: Arc::new(Mutex::new(Some(rx))),
    }
}
