//! Stream manager for managing long-lived streams per peer.
//!
//! The StreamManager is generic over request and response types, following the same
//! pattern as ConsensusNetwork with TNMessage. Application-layer types (like epoch sync)
//! implement the TNStreamMessage trait.
//!
//! The StreamManager handles:
//! - Opening and closing streams to peers
//! - Routing requests/responses through the correct stream
//! - Tracking pending requests and handling timeouts
//! - Managing raw byte streaming for large transfers

use super::{
    codec::StreamCodec,
    handler::{spawn_stream_tasks, ReadEvent, StreamHandle, StreamHandlerConfig},
    protocol::{FrameHeader, StreamError, StreamErrorCode, TN_STREAM_PROTOCOL},
    TNStreamMessage,
};
use libp2p::{bytes::Bytes, swarm::Stream as NegotiatedStream, PeerId};
use libp2p_stream as stream;
use std::{collections::HashMap, marker::PhantomData, time::Instant};
use tn_config::StreamConfig;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

/// Handle for controlling an active raw byte stream.
///
/// Used when streaming large data (e.g., epoch pack files).
/// The application layer decides when to use background streaming.
#[derive(Debug)]
pub struct RawStreamHandle {
    /// Cancel the stream early.
    pub cancel: oneshot::Sender<()>,
    /// Stream completion notification.
    pub done: oneshot::Receiver<Result<(), StreamNetworkError>>,
}

/// State of a peer's stream connection.
#[derive(Debug)]
enum PeerStreamState {
    /// Stream is being established.
    Connecting,
    /// Stream is active.
    Active {
        /// Handle for sending to the stream.
        handle: StreamHandle,
        /// Receiver for events from the stream.
        events_rx: mpsc::Receiver<ReadEvent>,
    },
    /// Stream is closing.
    Closing,
}

/// Manages streams for all connected peers.
///
/// Generic over request and response types, following the same pattern
/// as ConsensusNetwork with TNMessage.
pub struct StreamManager<Req, Res>
where
    Req: TNStreamMessage,
    Res: TNStreamMessage,
{
    /// libp2p-stream control handle for opening new streams.
    control: stream::Control,
    /// Active streams by peer ID.
    streams: HashMap<PeerId, PeerStreamState>,
    /// Pending typed requests awaiting responses.
    /// Key: (PeerId, request_id), Value: response channel
    pending_requests: HashMap<(PeerId, u64), PendingTypedRequest>,
    /// Active raw streams by (PeerId, request_id).
    active_raw_streams: HashMap<(PeerId, u64), ActiveRawStream>,
    /// Configuration.
    config: StreamConfig,
    /// Handler configuration derived from StreamConfig.
    handler_config: StreamHandlerConfig,
    /// Phantom data for generic types.
    _phantom: PhantomData<(Req, Res)>,
}

/// A pending typed request awaiting a response.
struct PendingTypedRequest {
    /// Channel to send the response payload.
    reply: oneshot::Sender<Result<Vec<u8>, StreamNetworkError>>,
    /// When the request was sent.
    sent_at: Instant,
}

/// An active raw byte stream transfer.
struct ActiveRawStream {
    /// Channel to send received bytes to the application.
    data_tx: mpsc::Sender<Bytes>,
    /// Channel to receive cancellation signal.
    #[allow(dead_code)]
    cancel_rx: oneshot::Receiver<()>,
    /// Channel to notify completion.
    done_tx: Option<oneshot::Sender<Result<(), StreamNetworkError>>>,
}

/// Network-level error type for stream operations.
///
/// Application-specific errors (like "epoch not found") should be defined
/// in the application layer, not here.
#[derive(Debug, thiserror::Error)]
pub enum StreamNetworkError {
    /// No stream to the peer.
    #[error("no stream to peer")]
    NoStream,

    /// Failed to open stream.
    #[error("failed to open stream: {0}")]
    OpenFailed(String),

    /// Stream was closed.
    #[error("stream closed")]
    StreamClosed,

    /// Request timed out.
    #[error("request timed out")]
    Timeout,

    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// Peer sent an error response.
    #[error("peer error: code={code:?}, message={message}")]
    PeerError { code: StreamErrorCode, message: String },

    /// Channel was closed.
    #[error("channel closed")]
    ChannelClosed,

    /// Stream was cancelled.
    #[error("cancelled")]
    Cancelled,

    /// Connection to peer was lost.
    #[error("disconnected")]
    Disconnected,
}

impl<Req, Res> StreamManager<Req, Res>
where
    Req: TNStreamMessage,
    Res: TNStreamMessage,
{
    /// Create a new stream manager.
    pub fn new(control: stream::Control, config: StreamConfig) -> Self {
        let handler_config = StreamHandlerConfig {
            max_frame_size: config.max_frame_size,
            read_timeout: config.request_timeout,
            write_buffer_size: 64,
        };

        Self {
            control,
            streams: HashMap::new(),
            pending_requests: HashMap::new(),
            active_raw_streams: HashMap::new(),
            config,
            handler_config,
            _phantom: PhantomData,
        }
    }

    /// Get or create a stream to a peer.
    ///
    /// If a stream doesn't exist, opens a new one.
    /// Returns a cloned handle to avoid borrow checker issues.
    pub async fn get_or_open_stream(
        &mut self,
        peer_id: PeerId,
    ) -> Result<StreamHandle, StreamNetworkError> {
        // Check if we already have an active stream
        let needs_open =
            !matches!(self.streams.get(&peer_id), Some(PeerStreamState::Active { .. }));

        if needs_open {
            // Need to open a new stream
            self.open_stream(peer_id).await?;
        }

        // Now get the handle (clone it to avoid borrow issues)
        match self.streams.get(&peer_id) {
            Some(PeerStreamState::Active { handle, .. }) => Ok(handle.clone()),
            _ => Err(StreamNetworkError::OpenFailed("stream not in active state".to_string())),
        }
    }

    /// Open a new stream to a peer.
    async fn open_stream(&mut self, peer_id: PeerId) -> Result<(), StreamNetworkError> {
        debug!(target: "stream-manager", ?peer_id, "opening stream to peer");

        // Mark as connecting
        self.streams.insert(peer_id, PeerStreamState::Connecting);

        // Open the stream
        let stream = self
            .control
            .open_stream(peer_id, TN_STREAM_PROTOCOL)
            .await
            .map_err(|e| StreamNetworkError::OpenFailed(e.to_string()))?;

        // Spawn read/write tasks
        let (handle, events_rx) = spawn_stream_tasks(stream, peer_id, self.handler_config.clone());

        // Store the active stream
        self.streams.insert(peer_id, PeerStreamState::Active { handle, events_rx });

        debug!(target: "stream-manager", ?peer_id, "stream opened successfully");
        Ok(())
    }

    /// Handle an incoming stream from a peer.
    pub fn handle_incoming_stream(&mut self, peer_id: PeerId, stream: NegotiatedStream) {
        debug!(target: "stream-manager", ?peer_id, "handling incoming stream");

        // Spawn read/write tasks
        let (handle, events_rx) = spawn_stream_tasks(stream, peer_id, self.handler_config.clone());

        // Store the stream (may replace existing)
        self.streams.insert(peer_id, PeerStreamState::Active { handle, events_rx });
    }

    /// Send a typed request and wait for response.
    ///
    /// This is the replacement for the request-response protocol.
    /// Returns the raw response payload bytes for the caller to deserialize.
    pub async fn send_request(
        &mut self,
        peer_id: PeerId,
        request_payload: Vec<u8>,
    ) -> Result<oneshot::Receiver<Result<Vec<u8>, StreamNetworkError>>, StreamNetworkError> {
        // Get or open stream
        let handle = self.get_or_open_stream(peer_id).await?;

        // Generate request ID
        let request_id = handle.next_request_id();

        // Create response channel
        let (reply_tx, reply_rx) = oneshot::channel();

        // Track the pending request
        self.pending_requests.insert(
            (peer_id, request_id),
            PendingTypedRequest { reply: reply_tx, sent_at: Instant::now() },
        );

        // Create and send the frame
        let header = FrameHeader::typed_request(request_id, request_payload.len() as u32);
        handle.send_frame(header, request_payload).await.map_err(|e| {
            StreamNetworkError::Io(std::io::Error::other(e.to_string()))
        })?;

        Ok(reply_rx)
    }

    /// Send a typed response to a request.
    pub async fn send_response(
        &mut self,
        peer_id: PeerId,
        request_id: u64,
        response_payload: Vec<u8>,
    ) -> Result<(), StreamNetworkError> {
        let handle = self.get_stream_handle(&peer_id)?;

        let header = FrameHeader::typed_response(request_id, response_payload.len() as u32);
        handle.send_frame(header, response_payload).await.map_err(|e| {
            StreamNetworkError::Io(std::io::Error::other(e.to_string()))
        })?;

        Ok(())
    }

    /// Send an error response.
    pub async fn send_error(
        &mut self,
        peer_id: PeerId,
        request_id: u64,
        error: StreamError,
    ) -> Result<(), StreamNetworkError> {
        let handle = self.get_stream_handle(&peer_id)?;

        let mut codec = StreamCodec::new(self.config.max_frame_size);
        let payload = codec
            .encode_payload(&error)
            .map_err(|e| StreamNetworkError::Serialization(e.to_string()))?;

        let header = FrameHeader::error(request_id, payload.len() as u32);
        handle.send_frame(header, payload).await.map_err(|e| {
            StreamNetworkError::Io(std::io::Error::other(e.to_string()))
        })?;

        Ok(())
    }

    /// Start a raw byte stream to a peer.
    ///
    /// This is the generic version for streaming large data. The request payload
    /// should be pre-serialized by the application layer.
    ///
    /// Returns a handle for controlling the stream and a receiver for the data bytes.
    pub async fn start_raw_stream(
        &mut self,
        peer_id: PeerId,
        request_payload: Vec<u8>,
    ) -> Result<(RawStreamHandle, mpsc::Receiver<Bytes>), StreamNetworkError> {
        // Get or open stream
        let handle = self.get_or_open_stream(peer_id).await?;

        // Generate request ID
        let request_id = handle.next_request_id();

        // Create channels
        let (data_tx, data_rx) = mpsc::channel(64); // Buffer some chunks
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();

        // Track the active stream
        self.active_raw_streams.insert(
            (peer_id, request_id),
            ActiveRawStream { data_tx, cancel_rx, done_tx: Some(done_tx) },
        );

        // Send the request with RawStreamBegin (has_more = true since we expect data)
        let header = FrameHeader::raw_stream_begin(request_id, request_payload.len() as u32, true);
        handle.send_frame(header, request_payload).await.map_err(|e| {
            StreamNetworkError::Io(std::io::Error::other(e.to_string()))
        })?;

        let raw_handle = RawStreamHandle { cancel: cancel_tx, done: done_rx };

        Ok((raw_handle, data_rx))
    }

    /// Process events from all streams.
    ///
    /// This should be called in the main event loop to handle incoming data.
    pub async fn poll_streams(&mut self) -> Option<StreamEvent<Req, Res>> {
        // Collect peers with active streams
        let peers: Vec<PeerId> = self
            .streams
            .iter()
            .filter_map(|(peer_id, state)| {
                matches!(state, PeerStreamState::Active { .. }).then_some(*peer_id)
            })
            .collect();

        for peer_id in peers {
            // Try to receive an event from this peer's stream
            if let Some(PeerStreamState::Active { events_rx, .. }) = self.streams.get_mut(&peer_id)
            {
                match events_rx.try_recv() {
                    Ok(event) => {
                        return Some(self.process_read_event(peer_id, event));
                    }
                    Err(mpsc::error::TryRecvError::Empty) => continue,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        // Stream closed
                        self.handle_stream_closed(peer_id);
                        return Some(StreamEvent::StreamClosed { peer_id });
                    }
                }
            }
        }

        None
    }

    /// Process a read event from a peer's stream.
    fn process_read_event(&mut self, peer_id: PeerId, event: ReadEvent) -> StreamEvent<Req, Res> {
        match event {
            ReadEvent::Request { request_id, payload } => {
                StreamEvent::InboundRequest { peer_id, request_id, payload }
            }
            ReadEvent::Response { request_id, payload } => {
                // Find and complete the pending request
                if let Some(pending) = self.pending_requests.remove(&(peer_id, request_id)) {
                    let _ = pending.reply.send(Ok(payload.clone()));
                }
                StreamEvent::ResponseReceived { peer_id, request_id, payload }
            }
            ReadEvent::RawStreamBegin { request_id, payload, has_more } => {
                StreamEvent::RawStreamBegin { peer_id, request_id, payload, has_more }
            }
            ReadEvent::RawStreamEnd { request_id, payload } => {
                // Complete the raw stream
                if let Some(mut active) = self.active_raw_streams.remove(&(peer_id, request_id)) {
                    if let Some(done_tx) = active.done_tx.take() {
                        let _ = done_tx.send(Ok(()));
                    }
                }
                StreamEvent::RawStreamEnd { peer_id, request_id, payload }
            }
            ReadEvent::RawData { data } => {
                // Forward to any active raw stream for this peer
                // Note: In raw mode, we don't have request_id, so we forward to all active streams
                for ((pid, _), active) in self.active_raw_streams.iter() {
                    if *pid == peer_id {
                        let _ = active.data_tx.try_send(data.clone());
                    }
                }
                StreamEvent::RawDataReceived { peer_id, data }
            }
            ReadEvent::Error { request_id, error } => {
                // Complete pending request with error
                if let Some(pending) = self.pending_requests.remove(&(peer_id, request_id)) {
                    let _ = pending.reply.send(Err(StreamNetworkError::PeerError {
                        code: error.code,
                        message: error.message.clone(),
                    }));
                }
                // Complete raw stream with error
                if let Some(mut active) = self.active_raw_streams.remove(&(peer_id, request_id)) {
                    if let Some(done_tx) = active.done_tx.take() {
                        let _ = done_tx.send(Err(StreamNetworkError::PeerError {
                            code: error.code,
                            message: error.message.clone(),
                        }));
                    }
                }
                StreamEvent::ErrorReceived { peer_id, request_id, error }
            }
            ReadEvent::Cancelled { request_id } => {
                StreamEvent::RequestCancelled { peer_id, request_id }
            }
            ReadEvent::Closed => {
                self.handle_stream_closed(peer_id);
                StreamEvent::StreamClosed { peer_id }
            }
            ReadEvent::ReadError { error } => {
                warn!(target: "stream-manager", ?peer_id, ?error, "read error on stream");
                self.handle_stream_closed(peer_id);
                StreamEvent::StreamError { peer_id, error: error.to_string() }
            }
        }
    }

    /// Handle a stream being closed.
    fn handle_stream_closed(&mut self, peer_id: PeerId) {
        // Remove the stream
        self.streams.remove(&peer_id);

        // Fail all pending requests to this peer
        let to_remove: Vec<_> =
            self.pending_requests.keys().filter(|(pid, _)| *pid == peer_id).cloned().collect();

        for key in to_remove {
            if let Some(pending) = self.pending_requests.remove(&key) {
                let _ = pending.reply.send(Err(StreamNetworkError::StreamClosed));
            }
        }

        // Fail all active raw streams to this peer
        let raw_to_remove: Vec<_> =
            self.active_raw_streams.keys().filter(|(pid, _)| *pid == peer_id).cloned().collect();

        for key in raw_to_remove {
            if let Some(mut active) = self.active_raw_streams.remove(&key) {
                if let Some(done_tx) = active.done_tx.take() {
                    let _ = done_tx.send(Err(StreamNetworkError::Disconnected));
                }
            }
        }
    }

    /// Close stream to a peer.
    pub async fn close_stream(&mut self, peer_id: &PeerId) {
        if let Some(PeerStreamState::Active { handle, .. }) = self.streams.get(peer_id) {
            let _ = handle.close().await;
        }
        self.handle_stream_closed(*peer_id);
    }

    /// Called when a peer disconnects.
    pub fn on_peer_disconnected(&mut self, peer_id: &PeerId) {
        self.handle_stream_closed(*peer_id);
    }

    /// Check for timed out requests and clean them up.
    pub fn check_timeouts(&mut self) {
        let timeout = self.config.request_timeout;
        let now = Instant::now();

        // Check pending requests
        let timed_out: Vec<_> = self
            .pending_requests
            .iter()
            .filter(|(_, pending)| now.duration_since(pending.sent_at) > timeout)
            .map(|(key, _)| *key)
            .collect();

        for key in timed_out {
            if let Some(pending) = self.pending_requests.remove(&key) {
                let _ = pending.reply.send(Err(StreamNetworkError::Timeout));
            }
        }
    }

    /// Get the stream handle for a peer, if it exists.
    fn get_stream_handle(&self, peer_id: &PeerId) -> Result<StreamHandle, StreamNetworkError> {
        match self.streams.get(peer_id) {
            Some(PeerStreamState::Active { handle, .. }) => Ok(handle.clone()),
            _ => Err(StreamNetworkError::NoStream),
        }
    }

    /// Check if we have a stream to a peer.
    pub fn has_stream(&self, peer_id: &PeerId) -> bool {
        matches!(self.streams.get(peer_id), Some(PeerStreamState::Active { .. }))
    }

    /// Get the number of active streams.
    pub fn active_stream_count(&self) -> usize {
        self.streams.values().filter(|s| matches!(s, PeerStreamState::Active { .. })).count()
    }

    /// Get the number of pending requests.
    pub fn pending_request_count(&self) -> usize {
        self.pending_requests.len()
    }
}

/// Events emitted by the stream manager for the network layer to process.
///
/// Generic over request and response types, following the same pattern as ConsensusNetwork.
#[derive(Debug)]
pub enum StreamEvent<Req, Res>
where
    Req: TNStreamMessage,
    Res: TNStreamMessage,
{
    /// Received an inbound typed request.
    /// The payload should be deserialized by the application layer.
    InboundRequest { peer_id: PeerId, request_id: u64, payload: Vec<u8> },
    /// Received a response to our request.
    /// The payload should be deserialized by the application layer.
    ResponseReceived { peer_id: PeerId, request_id: u64, payload: Vec<u8> },
    /// Received raw stream begin message.
    /// The payload contains application-defined metadata about the stream.
    RawStreamBegin { peer_id: PeerId, request_id: u64, payload: Vec<u8>, has_more: bool },
    /// Received raw stream end message.
    /// The payload may contain final metadata.
    RawStreamEnd { peer_id: PeerId, request_id: u64, payload: Vec<u8> },
    /// Received raw data during streaming.
    RawDataReceived { peer_id: PeerId, data: Bytes },
    /// Received an error response.
    ErrorReceived { peer_id: PeerId, request_id: u64, error: StreamError },
    /// A request was cancelled by the peer.
    RequestCancelled { peer_id: PeerId, request_id: u64 },
    /// Stream to peer was closed.
    StreamClosed { peer_id: PeerId },
    /// Stream error occurred.
    StreamError { peer_id: PeerId, error: String },
    /// Phantom data to satisfy type constraints.
    #[doc(hidden)]
    _Phantom(PhantomData<(Req, Res)>),
}
