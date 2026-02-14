//! Provide abstractions over sync chanel code.
//! This will allow us to insulate from specific implementations and more easily swap
//! as needed (for instance moving from MPSC to Broadcast).

use std::{
    error::Error,
    fmt::Display,
    future::Future,
    task::{Context, Poll},
};
use tokio::sync::{broadcast, mpsc};

/// The default channel capacity for each channel.
pub const CHANNEL_CAPACITY: usize = 10_000;

/// Error returned by `try_recv`.
/// This is just a trivial abstraction over the tokio version.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TryRecvError {
    /// This **channel** is currently empty, but the **Sender**(s) have not yet
    /// disconnected, so data may yet become available.
    Empty,
    /// The **channel**'s sending half has become disconnected, and there will
    /// never be any more data received on it.
    Disconnected,
    /// If the underlying channel is a broadcast it has lagged and some messages were not received.
    Lagged,
}

impl Error for TryRecvError {}

impl Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "recv error: Empty"),
            TryRecvError::Disconnected => write!(f, "recv error: Disconnected"),
            TryRecvError::Lagged => write!(f, "recv error: Lagged"),
        }
    }
}

impl From<mpsc::error::TryRecvError> for TryRecvError {
    fn from(value: mpsc::error::TryRecvError) -> Self {
        match value {
            tokio::sync::mpsc::error::TryRecvError::Empty => Self::Empty,
            tokio::sync::mpsc::error::TryRecvError::Disconnected => Self::Disconnected,
        }
    }
}

impl From<broadcast::error::TryRecvError> for TryRecvError {
    fn from(value: broadcast::error::TryRecvError) -> Self {
        match value {
            broadcast::error::TryRecvError::Empty => Self::Empty,
            broadcast::error::TryRecvError::Closed => Self::Disconnected,
            broadcast::error::TryRecvError::Lagged(_) => Self::Lagged,
        }
    }
}

/// Error returned by the `TnSender`.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SendError<T>(pub T);

impl<T: std::fmt::Debug> Error for SendError<T> {}

impl<T> Display for SendError<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "send error: {:?}", self.0)
    }
}

impl<T> std::fmt::Debug for SendError<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SendError!: {:?}", self.0)
    }
}

impl<T> From<mpsc::error::SendError<T>> for SendError<T> {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> SendError<T> {
        SendError(value.0)
    }
}

impl<T> From<broadcast::error::SendError<T>> for SendError<T> {
    fn from(value: broadcast::error::SendError<T>) -> SendError<T> {
        SendError(value.0)
    }
}

/// This enumeration is the list of the possible error outcomes for the
/// [`try_send`](TnSender::try_send) method.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum TrySendError<T> {
    /// The data could not be sent on the channel because the channel is
    /// currently full and sending would require blocking.
    Full(T),

    /// The receive half of the channel was explicitly closed or has been
    /// dropped.
    Closed(T),

    /// Broadcast channel error.
    Broadcast(T),
}

impl<T> Error for TrySendError<T> {}

impl<T> Display for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "Send Error: Full"),
            TrySendError::Closed(_) => write!(f, "Send Error: Closed"),
            TrySendError::Broadcast(_) => write!(f, "Send Error: Broadcast"),
        }
    }
}

impl<T> std::fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "Send Error: Full"),
            TrySendError::Closed(_) => write!(f, "Send Error: Closed"),
            TrySendError::Broadcast(_) => write!(f, "Send Error: Broadcast"),
        }
    }
}

impl<T> From<mpsc::error::TrySendError<T>> for TrySendError<T> {
    fn from(value: mpsc::error::TrySendError<T>) -> TrySendError<T> {
        match value {
            tokio::sync::mpsc::error::TrySendError::Full(t) => TrySendError::Full(t),
            tokio::sync::mpsc::error::TrySendError::Closed(t) => TrySendError::Closed(t),
        }
    }
}

impl<T> From<broadcast::error::SendError<T>> for TrySendError<T> {
    fn from(value: broadcast::error::SendError<T>) -> TrySendError<T> {
        TrySendError::Broadcast(value.0)
    }
}

pub trait TnReceiver<T>: Send + Unpin {
    /// Receives the next value for this channel.
    /// Signature is desugared async fn recv(&mut self) -> Option<T> with Send added.
    fn recv(&mut self) -> impl Future<Output = Option<T>> + Send;

    /// Attempts to receive the next value for this channel.
    fn try_recv(&mut self) -> Result<T, TryRecvError>;

    /// Polls to receive the next message on this channel.
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>>;
}

pub trait TnSender<T>: Unpin + Clone {
    /// Sends a value, waiting until there is capacity.
    /// Signature is desugared async fn send(&self, value: T) -> Result<(), SendError<T>> with Send
    /// added.
    fn send(&self, value: T) -> impl Future<Output = Result<(), SendError<T>>> + Send;

    /// Attempts to immediately send a message on this `Sender`
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>>;
}

impl<T: Send + Clone + 'static> TnSender<T> for broadcast::Sender<T> {
    async fn send(&self, value: T) -> Result<(), SendError<T>> {
        // This will only fail if there are no open receivers.
        // We are not worried about that, if no code is interested
        // then that is fine, it might be later
        let _ = self.send(value);
        Ok(())
    }

    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        // This will only fail if there are no open receivers.
        // We are not worried about that, if no code is interested
        // then that is fine, it might be later
        let _ = self.send(value);
        Ok(())
    }
}

impl<T: Send + 'static> TnSender<T> for mpsc::Sender<T> {
    async fn send(&self, value: T) -> Result<(), SendError<T>> {
        Ok(mpsc::Sender::send(self, value).await?)
    }

    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        Ok(mpsc::Sender::try_send(self, value)?)
    }
}

impl<T: Send + Clone> TnReceiver<T> for broadcast::Receiver<T> {
    async fn recv(&mut self) -> Option<T> {
        loop {
            match broadcast::Receiver::recv(self).await {
                Ok(value) => return Some(value),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    // Log the lag event and continue receiving.
                    // After Lagged, the internal cursor is updated and the next recv
                    // will return the oldest message still in the buffer.
                    // Previously this was silently converted to None via .ok(),
                    // which caused observers to exit their recv loop and stop following
                    // consensus entirely.
                    tracing::warn!(
                        target: "tn::observer",
                        messages_lost = n,
                        "broadcast channel lagged - receiver lost messages, resuming"
                    );
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        Ok(broadcast::Receiver::try_recv(self)?)
    }

    fn poll_recv(&mut self, _cx: &mut Context<'_>) -> Poll<Option<T>> {
        panic!("poll_recv not implemented for tokio broadcast channels!")
    }
}

impl<T: Send> TnReceiver<T> for mpsc::Receiver<T> {
    async fn recv(&mut self) -> Option<T> {
        mpsc::Receiver::recv(self).await
    }

    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        Ok(mpsc::Receiver::try_recv(self)?)
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        mpsc::Receiver::poll_recv(self, cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that broadcast lag does NOT cause recv() to return None.
    /// This was the root cause of observers silently stopping: the old
    /// implementation converted RecvError::Lagged to None via .ok(),
    /// which caused `while let Some(x) = rx.recv().await` loops to exit.
    #[tokio::test]
    async fn test_broadcast_lag_does_not_return_none() {
        let (tx, _default_rx) = broadcast::channel::<u64>(16);
        let mut slow_rx = tx.subscribe();

        // Overflow the channel: send 32 messages into a capacity-16 channel
        for i in 0..32 {
            let _ = tx.send(i);
        }

        // The receiver has lagged. With the fix, TnReceiver::recv() should NOT
        // return None. Instead, it should skip the lagged messages and return the
        // oldest available message (which is 16, since 0..15 were dropped).
        let value = TnReceiver::recv(&mut slow_rx).await;
        assert!(value.is_some(), "recv() must not return None on lag");
        // After lag, the next available message should be 16 (first non-dropped)
        assert_eq!(value.unwrap(), 16);
    }

    /// Verify that recv() returns None only when the channel is actually closed.
    #[tokio::test]
    async fn test_broadcast_closed_returns_none() {
        let (tx, _default_rx) = broadcast::channel::<u64>(16);
        let mut rx = tx.subscribe();

        // Send a message and then drop the sender
        let _ = tx.send(42);
        drop(tx);

        // First recv gets the message
        let value = TnReceiver::recv(&mut rx).await;
        assert_eq!(value, Some(42));

        // Second recv: channel closed, should return None
        let value = TnReceiver::recv(&mut rx).await;
        assert!(value.is_none(), "closed channel must return None");
    }

    /// Verify that try_recv correctly reports Lagged errors.
    #[test]
    fn test_broadcast_try_recv_lagged() {
        let (tx, _default_rx) = broadcast::channel::<u64>(16);
        let mut slow_rx: broadcast::Receiver<u64> = tx.subscribe();

        // Overflow the channel
        for i in 0..32 {
            let _ = tx.send(i);
        }

        // try_recv should report the lag
        let result: Result<u64, TryRecvError> = TnReceiver::try_recv(&mut slow_rx);
        assert_eq!(result, Err(TryRecvError::Lagged));

        // After acknowledging the lag, next try_recv should succeed
        let result: Result<u64, TryRecvError> = TnReceiver::try_recv(&mut slow_rx);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 16);
    }
}
