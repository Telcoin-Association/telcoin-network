//! Per-peer stream handling.
//!
//! Manages the read/write tasks for a single peer's stream connection.

use super::{
    codec::StreamCodec,
    protocol::{
        EpochStreamRequest, EpochStreamResponse, FrameHeader, StreamError, StreamRequestType,
    },
};
use futures::{AsyncRead, AsyncWrite};
use libp2p::bytes::Bytes;
use libp2p::PeerId;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

/// Commands sent to the stream write task.
#[derive(Debug)]
pub enum WriteCommand {
    /// Send a typed request/response frame.
    SendFrame { header: FrameHeader, payload: Vec<u8> },
    /// Send raw bytes (for epoch streaming).
    SendRaw { data: Bytes },
    /// Flush the write buffer.
    Flush,
    /// Close the stream gracefully.
    Close,
}

/// Events from the stream read task.
#[derive(Debug)]
pub enum ReadEvent {
    /// Received a typed request.
    Request { request_id: u64, payload: Vec<u8> },
    /// Received a typed response.
    Response { request_id: u64, payload: Vec<u8> },
    /// Received an epoch stream request.
    EpochStreamRequest { request_id: u64, request: EpochStreamRequest },
    /// Received epoch stream metadata.
    EpochStreamMeta { request_id: u64, meta: EpochStreamResponse, has_more: bool },
    /// Received raw bytes (epoch stream data).
    RawData { data: Bytes },
    /// Received an error response.
    Error { request_id: u64, error: StreamError },
    /// Request was cancelled.
    Cancelled { request_id: u64 },
    /// Stream was closed.
    Closed,
    /// Read error occurred.
    ReadError { error: std::io::Error },
}

/// Configuration for stream handlers.
#[derive(Debug, Clone)]
pub struct StreamHandlerConfig {
    /// Maximum frame size.
    pub max_frame_size: usize,
    /// Read timeout for individual frames.
    pub read_timeout: Duration,
    /// Write buffer size.
    pub write_buffer_size: usize,
}

impl Default for StreamHandlerConfig {
    fn default() -> Self {
        Self {
            max_frame_size: 10 * 1024 * 1024, // 10 MiB
            read_timeout: Duration::from_secs(30),
            write_buffer_size: 64,
        }
    }
}

/// Handle for controlling a peer's stream.
#[derive(Debug, Clone)]
pub struct StreamHandle {
    /// Channel to send write commands.
    write_tx: mpsc::Sender<WriteCommand>,
    /// Peer ID.
    peer_id: PeerId,
    /// Next request ID counter.
    next_request_id: Arc<AtomicU64>,
}

impl StreamHandle {
    /// Create a new stream handle.
    pub fn new(write_tx: mpsc::Sender<WriteCommand>, peer_id: PeerId) -> Self {
        Self { write_tx, peer_id, next_request_id: Arc::new(AtomicU64::new(1)) }
    }

    /// Get the peer ID.
    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Generate the next request ID.
    pub fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a frame to the peer.
    pub async fn send_frame(
        &self,
        header: FrameHeader,
        payload: Vec<u8>,
    ) -> Result<(), StreamHandlerError> {
        self.write_tx
            .send(WriteCommand::SendFrame { header, payload })
            .await
            .map_err(|_| StreamHandlerError::ChannelClosed)
    }

    /// Send raw bytes to the peer.
    pub async fn send_raw(&self, data: Bytes) -> Result<(), StreamHandlerError> {
        self.write_tx
            .send(WriteCommand::SendRaw { data })
            .await
            .map_err(|_| StreamHandlerError::ChannelClosed)
    }

    /// Flush the stream.
    pub async fn flush(&self) -> Result<(), StreamHandlerError> {
        self.write_tx.send(WriteCommand::Flush).await.map_err(|_| StreamHandlerError::ChannelClosed)
    }

    /// Close the stream.
    pub async fn close(&self) -> Result<(), StreamHandlerError> {
        self.write_tx.send(WriteCommand::Close).await.map_err(|_| StreamHandlerError::ChannelClosed)
    }
}

/// Error type for stream handler operations.
#[derive(Debug, thiserror::Error)]
pub enum StreamHandlerError {
    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Channel was closed.
    #[error("channel closed")]
    ChannelClosed,

    /// Stream was closed by peer.
    #[error("stream closed by peer")]
    StreamClosed,

    /// Read timeout.
    #[error("read timeout")]
    ReadTimeout,

    /// Invalid frame.
    #[error("invalid frame: {0}")]
    InvalidFrame(String),
}

/// Spawns read and write tasks for a stream.
///
/// Returns a handle for sending commands and a receiver for events.
pub fn spawn_stream_tasks<S>(
    stream: S,
    peer_id: PeerId,
    config: StreamHandlerConfig,
) -> (StreamHandle, mpsc::Receiver<ReadEvent>)
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (read_half, write_half) = futures::io::AsyncReadExt::split(stream);

    // Create channels
    let (write_tx, write_rx) = mpsc::channel(config.write_buffer_size);
    let (read_tx, read_rx) = mpsc::channel(config.write_buffer_size);

    // Spawn write task
    let write_config = config.clone();
    tokio::spawn(async move {
        if let Err(e) = write_task(write_half, write_rx, write_config).await {
            debug!(target: "stream", ?peer_id, ?e, "write task ended with error");
        }
    });

    // Spawn read task
    let read_peer_id = peer_id;
    tokio::spawn(async move {
        if let Err(e) = read_task(read_half, read_tx, config, read_peer_id).await {
            debug!(target: "stream", ?read_peer_id, ?e, "read task ended with error");
        }
    });

    let handle = StreamHandle::new(write_tx, peer_id);
    (handle, read_rx)
}

/// Write task that processes commands and writes to the stream.
async fn write_task<W>(
    mut writer: W,
    mut commands: mpsc::Receiver<WriteCommand>,
    config: StreamHandlerConfig,
) -> Result<(), StreamHandlerError>
where
    W: AsyncWrite + Unpin,
{
    let mut codec = StreamCodec::new(config.max_frame_size);

    while let Some(cmd) = commands.recv().await {
        match cmd {
            WriteCommand::SendFrame { header, payload } => {
                codec.write_frame(&mut writer, &header, &payload).await?;
            }
            WriteCommand::SendRaw { data } => {
                codec.write_raw(&mut writer, &data).await?;
            }
            WriteCommand::Flush => {
                codec.flush(&mut writer).await?;
            }
            WriteCommand::Close => {
                codec.flush(&mut writer).await?;
                break;
            }
        }
    }

    Ok(())
}

/// Read task that reads frames and sends events.
async fn read_task<R>(
    mut reader: R,
    events: mpsc::Sender<ReadEvent>,
    config: StreamHandlerConfig,
    peer_id: PeerId,
) -> Result<(), StreamHandlerError>
where
    R: AsyncRead + Unpin,
{
    let mut codec = StreamCodec::new(config.max_frame_size);
    let mut in_raw_mode = false;
    let mut raw_buffer = vec![0u8; 64 * 1024]; // 64KB read buffer for raw mode

    loop {
        if in_raw_mode {
            // In raw mode, just forward bytes
            match codec.read_raw(&mut reader, &mut raw_buffer).await {
                Ok(0) => {
                    // EOF
                    let _ = events.send(ReadEvent::Closed).await;
                    break;
                }
                Ok(n) => {
                    let data = Bytes::copy_from_slice(&raw_buffer[..n]);
                    if events.send(ReadEvent::RawData { data }).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = events.send(ReadEvent::ReadError { error: e }).await;
                    break;
                }
            }
        } else {
            // Normal framed mode
            match codec.read_frame(&mut reader).await {
                Ok((header, payload)) => {
                    let event = match header.request_type {
                        StreamRequestType::TypedRequest => {
                            ReadEvent::Request { request_id: header.request_id, payload }
                        }
                        StreamRequestType::TypedResponse => {
                            ReadEvent::Response { request_id: header.request_id, payload }
                        }
                        StreamRequestType::EpochStreamRequest => {
                            match codec.decode_payload::<EpochStreamRequest>(&payload) {
                                Ok(request) => ReadEvent::EpochStreamRequest {
                                    request_id: header.request_id,
                                    request,
                                },
                                Err(e) => ReadEvent::ReadError {
                                    error: std::io::Error::other(format!(
                                        "failed to decode epoch request: {e}"
                                    )),
                                },
                            }
                        }
                        StreamRequestType::EpochStreamMeta => {
                            match codec.decode_payload::<EpochStreamResponse>(&payload) {
                                Ok(meta) => {
                                    // Switch to raw mode after metadata if has_more is set
                                    if header.flags.has_more() {
                                        in_raw_mode = true;
                                    }
                                    ReadEvent::EpochStreamMeta {
                                        request_id: header.request_id,
                                        meta,
                                        has_more: header.flags.has_more(),
                                    }
                                }
                                Err(e) => ReadEvent::ReadError {
                                    error: std::io::Error::other(format!(
                                        "failed to decode epoch meta: {e}"
                                    )),
                                },
                            }
                        }
                        StreamRequestType::ErrorResponse => {
                            match codec.decode_payload::<StreamError>(&payload) {
                                Ok(error) => {
                                    ReadEvent::Error { request_id: header.request_id, error }
                                }
                                Err(e) => ReadEvent::ReadError {
                                    error: std::io::Error::other(format!(
                                        "failed to decode error: {e}"
                                    )),
                                },
                            }
                        }
                        StreamRequestType::Cancel => {
                            ReadEvent::Cancelled { request_id: header.request_id }
                        }
                    };

                    if events.send(event).await.is_err() {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    let _ = events.send(ReadEvent::Closed).await;
                    break;
                }
                Err(e) => {
                    let _ = events.send(ReadEvent::ReadError { error: e }).await;
                    break;
                }
            }
        }
    }

    Ok(())
}

/// State for tracking a pending request.
#[derive(Debug)]
pub struct PendingRequest<Res> {
    /// Channel to send the response.
    pub reply: oneshot::Sender<Result<Res, StreamHandlerError>>,
    /// When the request was sent.
    pub sent_at: Instant,
    /// Request timeout.
    pub timeout: Duration,
}

impl<Res> PendingRequest<Res> {
    /// Create a new pending request.
    pub fn new(reply: oneshot::Sender<Result<Res, StreamHandlerError>>, timeout: Duration) -> Self {
        Self { reply, sent_at: Instant::now(), timeout }
    }

    /// Check if the request has timed out.
    pub fn is_timed_out(&self) -> bool {
        self.sent_at.elapsed() > self.timeout
    }

    /// Complete the request with a response.
    pub fn complete(self, response: Result<Res, StreamHandlerError>) {
        let _ = self.reply.send(response);
    }
}
