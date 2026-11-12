//! Frame codec for the primary's typed epoch-pack sync protocol.
//!
//! Step 6 of #739 cuts the full epoch-pack transfer over to the typed
//! [`SyncFrame`] layer: the request travels in the opening
//! [`SyncFrame::Req`](tn_network_libp2p::SyncFrame::Req) of a `/tn-primary-sync`
//! stream and the responder answers [`SyncFrame::Ack`] then streams the raw pack
//! bytes as a sequence of [`SyncFrame::Data`] frames terminated by
//! [`SyncFrame::End`] (or a [`SyncFrame::Deny`] when the pack is not held).
//!
//! Unlike the worker's batch path — where each `Data` frame is one whole
//! decodable [`Batch`](tn_types::Batch) — an epoch pack is a single opaque byte
//! stream (`EpochMeta` followed by interleaved batch and consensus records). The
//! responder therefore chunks the pack file into fixed-size `Data` frames and the
//! requester reassembles them into a contiguous [`tokio::io::AsyncRead`] so the
//! existing [`ConsensusChain::stream_import`](tn_storage::consensus::ConsensusChain::stream_import)
//! verifier consumes it unchanged.

use crate::error::PrimaryNetworkResult;
use futures::{
    io::{AsyncRead as FuturesAsyncRead, AsyncWrite as FuturesAsyncWrite},
    AsyncWriteExt as _, FutureExt as _, Stream, StreamExt as _, TryFutureExt as _,
    TryStreamExt as _,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tn_network_libp2p::{read_frame, write_frame, DenyReason, PrimarySyncRequest, SyncFrame};
use tn_storage::consensus::ConsensusChain;
use tn_types::{encode, encoded_size, try_decode, BlsPublicKey, Certificate, Epoch};
use tokio::io::{AsyncRead as TokioAsyncRead, AsyncReadExt as _};
use tracing::debug;

/// The raw pack bytes carried by a single [`SyncFrame::Data`] frame.
///
/// A larger chunk means fewer frames (and fewer flushes) at the cost of a larger
/// per-frame buffer on each side; 256 KiB keeps a full 512 MiB pack to a few
/// thousand frames.
const SYNC_PACK_CHUNK_SIZE: usize = 256 * 1024;

/// Headroom added to [`SYNC_PACK_CHUNK_SIZE`] for the `SyncFrame` envelope (the
/// enum tag and the `Data` length prefix) when bounding a decoded frame.
const SYNC_PACK_FRAME_OVERHEAD: usize = 1024;

/// The largest sync frame accepted on the primary epoch-pack stream: one `Data`
/// frame carrying a pack chunk, plus envelope headroom. Unlike the worker's
/// epoch-keyed bound, the pack is chunked at a fixed size, so this is constant.
pub(crate) const MAX_SYNC_PACK_FRAME_SIZE: usize = SYNC_PACK_CHUNK_SIZE + SYNC_PACK_FRAME_OVERHEAD;

/// Per-frame read timeout while reassembling the pack on the requester side. Guards a single
/// stalled frame (a peer that sends nothing); the throughput floor ([`RateWatch`]) guards a peer
/// that dribbles small frames just inside this timeout.
const EPOCH_PACK_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

/// Rolling window over which the epoch-pack transfer's throughput floor is evaluated (both the
/// responder's writes and the requester's reads).
const SYNC_RATE_WINDOW: Duration = Duration::from_secs(30);

/// Minimum bytes an epoch-pack transfer must carry per [`SYNC_RATE_WINDOW`]: 1 MiB per 30 s
/// sustained; a window stretched by a late chunk must carry proportionally more. Below this a
/// transfer is treated as a drip/stall and aborted, so a pack finishes at any sustained rate
/// at/above the floor, and a below-floor peer is cut within roughly one window. Both the
/// requester's reads and the responder's writes check this floor. The responder additionally
/// applies a fixed `PUBLIC_EPOCH_PACK_SERVE_TIMEOUT` (200 s) to public-tier epoch-pack serves. The
/// bound is not scaled by pack length, so a public requester must read a pack of L bytes at
/// L / 200 s or faster on each stream.
const SYNC_RATE_MIN_BYTES: u64 = 1 << 20;

/// How many floors ([`SYNC_RATE_MIN_BYTES`]) a fresh peer must deliver per window: the ratio of
/// [`SYNC_TARGET_MIN_BYTES`] to the floor.
const SYNC_TARGET_RATE_MULTIPLE: u64 = 8;

/// Minimum bytes per [`SYNC_RATE_WINDOW`] a fresh peer must sustain for the requester to keep
/// reading: 8 MiB per 30 s window, about 273 KiB/s or 2.24 Mbit/s per stream. A node with three
/// epoch fetchers needs about 6.7 Mbit/s to meet it on every stream. The target is well above the
/// floor, below the rate the responder's own `PUBLIC_EPOCH_PACK_SERVE_TIMEOUT` already demands of
/// a 100 MiB public pack (512 KiB/s), and far below an honest link.
///
/// A fresh peer is read at the target ([`SyncRateBound::Target`]). A peer cut at the target in a
/// probe walk is probed again at the floor in the same walk, with no sleep
/// (`walk_with_floor_fallback`). A de-prioritised peer (one marked slow for that epoch) is read at
/// the floor ([`SyncRateBound::Floor`]), so a network whose only peers are slow still syncs. A
/// peer that serves at or above the target becomes the preferred sync server. It keeps the
/// probe-first slot until it fails a probe, until another peer serves an exploration walk
/// (`SYNC_PREFERENCE_EXPLORE_EVERY` in `network/mod.rs`, which probes the holder last) at the
/// target, or until the epoch rotates. The last two bump a generation on the slot, so a serve by
/// the old holder that completes afterwards does not put it back. A holder that serves at
/// exactly the target is therefore displaced by the first exploration walk, at most
/// `SYNC_PREFERENCE_EXPLORE_EVERY` walks away, that any other peer serves at the target (#1787).
///
/// A requester whose own link is below the target pays for it once per node epoch (#1787, Gap
/// 4): the first walk that cuts two distinct peers at the target (about one window each, ~31 s
/// at 190 KiB/s) stops its target pass there, reads the rest of that walk at the floor, and sets
/// the handle's floor-only flag (`floor_only_this_epoch` in `network/mod.rs`), so every later walk
/// of that node epoch reads every peer at the floor with no target probe; `clear_sync_capability`
/// resets the flag at rotation. The worst case is two cut probes per node epoch on top of the
/// parent's always-floor behaviour.
pub(crate) const SYNC_TARGET_MIN_BYTES: u64 = SYNC_RATE_MIN_BYTES * SYNC_TARGET_RATE_MULTIPLE;

/// Wall-clock allowance for the latency part of one exchange (stream open, request write, `Ack`
/// and first frame) on top of its bytes at the target rate ([`SyncServeMeter::speed`]). It is the
/// protocol's own "ample on a working link" bound for one small frame
/// (`PUBLIC_SYNC_REQUEST_READ_TIMEOUT`, 1 s).
pub(crate) const SYNC_SERVE_LATENCY_ALLOWANCE: Duration = super::PUBLIC_SYNC_REQUEST_READ_TIMEOUT;

/// How long one pack record may take to arrive over a sync stream: the time the largest record a
/// pack may hold needs at the throughput floor, plus one window of slack. A shorter per-record
/// timeout would cut an honest transfer the floor admits (a 1 MB batch needs ~30 s at the floor),
/// while a stalled or dribbling peer is still cut by the per-frame timeout and the floor itself.
pub(crate) const SYNC_RECORD_TIMEOUT: Duration = Duration::from_secs(
    ((tn_storage::archive::pack_iter::MAX_RECORD_SIZE as u64).div_ceil(SYNC_RATE_MIN_BYTES) + 1)
        * SYNC_RATE_WINDOW.as_secs(),
);

/// A coarse throughput floor over a tumbling time window. Each completed window (at least
/// `window` since the last reset) must have carried at least `min_bytes`, prorated by the real
/// window length, else the transfer is below the floor.
/// Pure so it is unit-testable with an injected clock; see [`RateWatch::record`].
struct RateWatch {
    window: Duration,
    min_bytes: u64,
    window_start: Instant,
    bytes: u64,
}

impl RateWatch {
    fn new(window: Duration, min_bytes: u64, now: Instant) -> Self {
        Self { window, min_bytes, window_start: now, bytes: 0 }
    }

    /// Record `n` bytes transferred as of `now`. Returns `false` when a just-completed window
    /// carried fewer than `min_bytes` prorated by its real length (below the floor; the caller
    /// aborts); otherwise `true`. A window completes once `elapsed = now - window_start` reaches
    /// `window`, and it is ok iff `bytes * window >= min_bytes * elapsed` (in milliseconds): a
    /// window stretched by a late chunk must carry proportionally more. A completed window (below
    /// floor or not) resets the counter for the next window.
    fn record(&mut self, now: Instant, n: u64) -> bool {
        self.bytes = self.bytes.saturating_add(n);
        let elapsed = now.duration_since(self.window_start);
        if elapsed >= self.window {
            let carried = u128::from(self.bytes).saturating_mul(self.window.as_millis());
            let owed = u128::from(self.min_bytes).saturating_mul(elapsed.as_millis());
            let ok = carried >= owed;
            self.window_start = now;
            self.bytes = 0;
            return ok;
        }
        true
    }
}

/// The throughput bound the requester's reader holds a peer to over each [`SYNC_RATE_WINDOW`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncRateBound {
    /// The floor, [`SYNC_RATE_MIN_BYTES`] per window: for a peer already de-prioritised this
    /// epoch, so a network whose only peers are slow still syncs.
    Floor,
    /// The target, [`SYNC_TARGET_MIN_BYTES`] per window: for a fresh peer.
    Target,
}

impl SyncRateBound {
    /// The bound for one probe: [`Self::Floor`] when the peer is `deprioritised` (it already
    /// failed, timed out or served below the target rate this epoch), else [`Self::Target`].
    pub(crate) fn for_probe(deprioritised: bool) -> Self {
        if deprioritised {
            Self::Floor
        } else {
            Self::Target
        }
    }

    /// Minimum bytes per [`SYNC_RATE_WINDOW`] under this bound.
    fn min_bytes(self) -> u64 {
        match self {
            Self::Floor => SYNC_RATE_MIN_BYTES,
            Self::Target => SYNC_TARGET_MIN_BYTES,
        }
    }
}

/// How fast a completed serve delivered its bytes, as judged by [`SyncServeMeter::speed`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncServeSpeed {
    /// The serve completed within [`SYNC_SERVE_LATENCY_ALLOWANCE`] plus the time its bytes need
    /// at the target rate: the peer may become the preferred sync server.
    Target,
    /// The serve completed, but slower than that: the peer is de-prioritised for the epoch and
    /// never holds the probe-first preference.
    Slow,
}

/// Counts the pack bytes the requester's reader ([`sync_pack_reader`]) delivers for one exchange,
/// and notes whether the reader cut the exchange for falling below [`SyncRateBound::Target`].
///
/// The probe walk keeps one handle and gives a clone to the reader, so after the exchange it can
/// judge the serve's rate ([`Self::speed`]) from the bytes and the wall-clock time it took, and
/// tell a cut at the target ([`Self::cut_at_target`]) from any other failure (#1787).
#[derive(Clone, Debug, Default)]
pub(crate) struct SyncServeMeter(
    /// The byte count and the cut flag, shared by every clone.
    Arc<SyncServeMeterState>,
);

/// The shared state behind a [`SyncServeMeter`] and all of its clones.
#[derive(Debug, Default)]
struct SyncServeMeterState {
    /// Bytes delivered so far ([`SyncServeMeter::record`]).
    bytes: AtomicU64,
    /// Whether the reader cut the exchange for falling below [`SyncRateBound::Target`]
    /// ([`SyncServeMeter::note_cut`]).
    cut_at_target: AtomicBool,
}

impl SyncServeMeter {
    /// Add `n` delivered bytes.
    pub(crate) fn record(&self, n: u64) {
        self.0.bytes.fetch_add(n, Ordering::Relaxed);
    }

    /// The bytes delivered so far.
    pub(crate) fn bytes(&self) -> u64 {
        self.0.bytes.load(Ordering::Relaxed)
    }

    /// Record that the reader ([`sync_pack_reader`]) cut the stream for falling below `bound`.
    /// Only a cut at [`SyncRateBound::Target`] is remembered, because only that cut is worth a
    /// retry at the floor in the same walk; see `walk_with_floor_fallback` in `network/mod.rs`
    /// (#1787).
    pub(crate) fn note_cut(&self, bound: SyncRateBound) {
        match bound {
            SyncRateBound::Target => self.0.cut_at_target.store(true, Ordering::Relaxed),
            SyncRateBound::Floor => {}
        }
    }

    /// Whether the reader cut the exchange for falling below [`SyncRateBound::Target`]
    /// ([`Self::note_cut`]). A cut at the floor, a clean serve and any other failure leave it
    /// `false` (#1787).
    pub(crate) fn cut_at_target(&self) -> bool {
        self.0.cut_at_target.load(Ordering::Relaxed)
    }

    /// Judge a serve that took `elapsed`: [`SyncServeSpeed::Target`] when `elapsed` is at most
    /// [`SYNC_SERVE_LATENCY_ALLOWANCE`] plus the time the delivered bytes need at
    /// [`SYNC_TARGET_MIN_BYTES`] per [`SYNC_RATE_WINDOW`], else [`SyncServeSpeed::Slow`].
    pub(crate) fn speed(&self, elapsed: Duration) -> SyncServeSpeed {
        let transfer_ms = u128::from(self.bytes()).saturating_mul(SYNC_RATE_WINDOW.as_millis())
            / u128::from(SYNC_TARGET_MIN_BYTES);
        let budget_ms = SYNC_SERVE_LATENCY_ALLOWANCE.as_millis().saturating_add(transfer_ms);
        if elapsed.as_millis() <= budget_ms {
            SyncServeSpeed::Target
        } else {
            SyncServeSpeed::Slow
        }
    }
}

/// Target encoded size of one missing-certificates [`SyncFrame::Data`] frame: the
/// responder accumulates certificates into a batch and flushes a frame once the
/// batch reaches this size, so a large catch-up is a few hundred frames rather
/// than one frame per certificate. A certificate is ~0.3-3.5 KiB, so a batch holds
/// many.
const SYNC_CERT_BATCH_TARGET_SIZE: usize = 256 * 1024;

/// The largest missing-certificates sync frame accepted on either side: one batch
/// `Data` frame plus headroom for the certificate that pushed the batch over the
/// target and the `SyncFrame` envelope. Bounds the per-frame allocation a peer can
/// force.
pub(crate) const MAX_SYNC_CERT_FRAME_SIZE: usize = SYNC_CERT_BATCH_TARGET_SIZE * 2;

/// Honest-responder cap on the total encoded certificate bytes streamed in one
/// missing-certificates exchange. The responder stops streaming (writes `End`) once
/// it has emitted this many bytes; the streaming collector is otherwise bounded only
/// by its processing-time limit, so this keeps a single exchange to a sane size
/// (64x the 1 MiB default legacy single-response cap) while still delivering far
/// more than one request-response message per round.
const MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Requester-side cap on the total encoded certificate bytes accepted in one
/// missing-certificates exchange. One [`MAX_SYNC_CERT_FRAME_SIZE`] above the honest
/// responder cap so the batch that carries an honest responder past
/// [`MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES`] is still accepted, while a responder
/// that ignores `End` and streams unboundedly is cut off.
///
/// This bounds a *single* exchange. The requester (`fetch_from_peers`) staggers several
/// fetches and cancels only on the first success, so a catch-up round can hold multiple
/// exchanges open at once; the process-wide requester peak is that concurrency times this
/// cap. See `fetch_from_peers` for the aggregate envelope (issue #822, finding 2).
const MAX_SYNC_MISSING_CERTS_ACCEPT_BYTES: usize =
    MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES + MAX_SYNC_CERT_FRAME_SIZE;

/// Per-frame read timeout while reading the missing-certificates response on the
/// requester side. The whole exchange is also bounded by the caller's fetch
/// timeout; this guards a single stalled frame.
const MISSING_CERTS_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

/// Serve an accepted epoch-pack sync exchange over `stream`.
///
/// `stop_number` selects the transfer: `None` serves the complete epoch pack,
/// `Some(last_consensus_number)` serves the verifiable PREFIX up to that consensus
/// output (the partial in-progress epoch, [`ConsensusChain::get_partial_epoch_stream`]).
/// Either way the transfer is all-or-nothing: if the requested pack (or its prefix)
/// is not held, the responder sheds with
/// [`SyncFrame::Deny`]`(`[`DenyReason::Unavailable`]`)` so the requester retries
/// another peer immediately instead of waiting out its ack timeout. Otherwise it
/// writes [`SyncFrame::Ack`], streams the pack as [`SyncFrame::Data`] frames, and
/// ends with [`SyncFrame::End`]. A partial transfer bounds the source reader to the
/// prefix length so it never streams past the verifiable point. Each frame write is
/// bounded by `buffer_timeout` so a peer that stops reading cannot stall the
/// responder task on an unbounded write. The whole data stream is also bounded by
/// [`pack_serve_deadline`] of the pack length, so a requester cannot hold the serve
/// longer than an honest link at the throughput floor needs. The caller has already
/// admitted the exchange against the concurrency caps and read the opening request frame.
pub(crate) async fn send_sync_epoch_pack_over_stream<S>(
    stream: &mut S,
    consensus_chain: &ConsensusChain,
    epoch: Epoch,
    stop_number: Option<u64>,
    buffer_timeout: Duration,
    peer: BlsPublicKey,
) -> PrimaryNetworkResult<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

    // resolve the source reader and its byte cap through the `Option`'s combinator
    // rather than a Some/None branch: a full pack is capped at its logical `[0, data_len)`
    // length, a partial prefix at its `[0, end)` verifiable length. Both `get_*_epoch_stream`
    // calls now return `(reader, cap)`, so `map_or_else` picks either directly; a missing pack
    // (or an out-of-range partial stop point) sheds cleanly so the requester retries elsewhere.
    let source = stop_number
        .map_or_else(
            || consensus_chain.get_epoch_stream(epoch).map_err(drop).boxed(),
            |number| consensus_chain.get_partial_epoch_stream(epoch, number).map_err(drop).boxed(),
        )
        .await;
    let Ok((epoch_stream, cap)) = source else {
        debug!(target: "primary::network", %peer, epoch, ?stop_number, "epoch pack unavailable; denying sync request");
        write_one_frame(
            stream,
            &SyncFrame::Deny(DenyReason::Unavailable),
            &mut encode_buffer,
            &mut compressed_buffer,
            MAX_SYNC_PACK_FRAME_SIZE,
            buffer_timeout,
        )
        .await?;
        return Ok(());
    };

    // accept and flush immediately so the requester sees the `Ack` within its ack
    // timeout even when the first pack reads are slow
    write_one_frame(
        stream,
        &SyncFrame::Ack,
        &mut encode_buffer,
        &mut compressed_buffer,
        MAX_SYNC_PACK_FRAME_SIZE,
        buffer_timeout,
    )
    .await?;

    // `cap` bounds the reader to the pack's logical length in both cases (a full transfer to
    // `[0, data_len)`, a partial prefix to `[0, end)`), so `take(cap)` stops the stream there
    // and the trailing mmap capacity padding is never sent. The same length sizes the total
    // serve deadline.
    let mut capped = epoch_stream.take(cap);
    write_pack_data_frames_within_deadline(&mut capped, cap, stream, buffer_timeout).await?;
    Ok(())
}

/// The total time an epoch-pack serve of `pack_bytes` bytes may take: one
/// [`SYNC_RATE_WINDOW`] for each started [`SYNC_RATE_MIN_BYTES`] of pack, plus one window of
/// slack. A requester that reads at the throughput floor finishes inside it, so the deadline
/// never cuts a link the floor admits; a 100 MiB pack gets 50.5 minutes.
pub(crate) fn pack_serve_deadline(pack_bytes: u64) -> Duration {
    let windows = pack_bytes.div_ceil(SYNC_RATE_MIN_BYTES).saturating_add(1);
    SYNC_RATE_WINDOW.saturating_mul(u32::try_from(windows).unwrap_or(u32::MAX))
}

/// Stream a pack reader as [`write_pack_data_frames`] does, bounded by the total deadline
/// [`pack_serve_deadline`] of `pack_bytes`.
///
/// The per-frame write timeout and the throughput floor are checked frame by frame. The deadline
/// is a hard upper bound on the whole serve that holds whatever those checks measure, so a
/// requester that keeps each frame just inside its timeout still releases the serve slot. A
/// missed deadline is an [`std::io::ErrorKind::TimedOut`] error, which the admission layer
/// classifies as a stalled serve.
pub(crate) async fn write_pack_data_frames_within_deadline<R, S>(
    reader: &mut R,
    pack_bytes: u64,
    stream: &mut S,
    buffer_timeout: Duration,
) -> std::io::Result<()>
where
    R: TokioAsyncRead + Unpin + Send,
    S: FuturesAsyncWrite + Unpin + Send,
{
    tokio::time::timeout(
        pack_serve_deadline(pack_bytes),
        write_pack_data_frames(reader, stream, buffer_timeout),
    )
    .await
    .map_err(|_elapsed| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "epoch pack serve missed its deadline")
    })?
}

/// Stream a pack reader as [`SyncFrame::Data`] frames terminated by
/// [`SyncFrame::End`]. Does not write the opening `Ack` (the caller decides
/// whether to accept); split out so the wire framing can be round-tripped in
/// tests against [`sync_pack_reader`] without a [`ConsensusChain`] fixture.
pub(crate) async fn write_pack_data_frames<R, S>(
    reader: &mut R,
    stream: &mut S,
    buffer_timeout: Duration,
) -> std::io::Result<()>
where
    R: TokioAsyncRead + Unpin + Send,
    S: FuturesAsyncWrite + Unpin + Send,
{
    // Chunk the reader into fixed-capacity `Data` frames by folding a `ReaderStream`
    // (yielding `Ok(<=SYNC_PACK_CHUNK_SIZE bytes)`, ending at EOF, surfacing a read
    // error). The writer and the two scratch buffers ride the accumulator: this
    // reuses the buffers across frames and keeps the writer's `&mut` out of the
    // `FnMut` closure, whose returned future cannot borrow per-call captures. Each
    // chunk's backing allocation is reclaimed into the `Data` frame without a copy.
    let watch = RateWatch::new(SYNC_RATE_WINDOW, SYNC_RATE_MIN_BYTES, Instant::now());
    let (stream, mut encode_buffer, mut compressed_buffer, _watch) =
        tokio_util::io::ReaderStream::with_capacity(reader, SYNC_PACK_CHUNK_SIZE)
            .try_fold(
                (stream, Vec::new(), Vec::new(), watch),
                |(stream, mut encode_buffer, mut compressed_buffer, mut watch), chunk| async move {
                    let len = chunk.len() as u64;
                    write_one_frame(
                        stream,
                        &SyncFrame::Data(chunk.into()),
                        &mut encode_buffer,
                        &mut compressed_buffer,
                        MAX_SYNC_PACK_FRAME_SIZE,
                        buffer_timeout,
                    )
                    .await?;
                    // Enforce the throughput floor on bytes the requester actually accepted (a
                    // completed `write_one_frame` means the frame was flushed): a drip-reading peer
                    // that keeps resetting the per-frame timer is cut here, within about one
                    // window, rather than holding the responder task (and its admission slot)
                    // until a whole-serve bound expires.
                    if !watch.record(Instant::now(), len) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                "epoch pack serve throughput below floor ({} bytes / {}s)",
                                SYNC_RATE_MIN_BYTES,
                                SYNC_RATE_WINDOW.as_secs(),
                            ),
                        ));
                    }
                    Ok((stream, encode_buffer, compressed_buffer, watch))
                },
            )
            .await?;

    write_one_frame(
        stream,
        &SyncFrame::End,
        &mut encode_buffer,
        &mut compressed_buffer,
        MAX_SYNC_PACK_FRAME_SIZE,
        buffer_timeout,
    )
    .await
}

/// Write a single frame and flush, bounding both on `buffer_timeout` so a
/// non-reading peer cannot pin the writer on an unbounded write. `max_frame_size`
/// bounds the encoded frame (pack chunks and cert batches use different caps).
async fn write_one_frame<S>(
    stream: &mut S,
    frame: &SyncFrame<PrimarySyncRequest>,
    encode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    max_frame_size: usize,
    buffer_timeout: Duration,
) -> std::io::Result<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    tokio::time::timeout(buffer_timeout, async {
        write_frame(stream, frame, encode_buffer, compressed_buffer, max_frame_size).await?;
        stream.flush().await
    })
    .await
    .map_err(|_elapsed| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout writing sync frame")
    })?
}

/// The requester's read clock for its rate bound: tokio's clock, so a paused test runtime drives
/// it, and the same instant as [`Instant::now`] on a live runtime.
fn reader_now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// Reassemble the [`SyncFrame::Data`] frames of an accepted epoch-pack exchange
/// into a contiguous [`tokio::io::AsyncRead`] terminating at [`SyncFrame::End`].
///
/// The caller has already read the opening [`SyncFrame::Ack`]; this reads the
/// `Data`/`End` stream. An [`SyncFrame::Err`] frame or any out-of-place frame
/// surfaces as a read error so the importer aborts. The returned reader is fed
/// directly to
/// [`ConsensusChain::stream_import`](tn_storage::consensus::ConsensusChain::stream_import).
///
/// `bound` is the rate the peer must sustain over each [`SYNC_RATE_WINDOW`], prorated over the
/// real window length: [`SyncRateBound::Target`] for a fresh peer, [`SyncRateBound::Floor`] for
/// one already de-prioritised this epoch. `meter` counts every delivered `Data` byte, so the
/// caller can judge the completed serve's rate ([`SyncServeMeter::speed`]). The bound is also
/// checked when the `End` frame arrives: otherwise a peer that dribbles a few bytes and sends
/// `End` after the window would finish as a clean serve, because only a `Data` chunk ran the
/// check.
pub(crate) fn sync_pack_reader<S>(
    stream: S,
    bound: SyncRateBound,
    meter: SyncServeMeter,
) -> impl TokioAsyncRead + Unpin + Send
where
    S: FuturesAsyncRead + Unpin + Send + 'static,
{
    let watch = RateWatch::new(SYNC_RATE_WINDOW, bound.min_bytes(), reader_now());
    let frames = futures::stream::unfold(
        (stream, Vec::new(), Vec::new(), watch, meter),
        move |(mut stream, mut decode_buffer, mut compressed_buffer, mut watch, meter)| async move {
            let chunk =
                next_pack_chunk(&mut stream, &mut decode_buffer, &mut compressed_buffer).await;
            // Enforce the rate bound on delivered bytes: a peer that dribbles small frames
            // just inside the per-frame timeout (never tripping it) is cut here instead
            // of pinning the import for a fixed wall-clock cap. The `End` frame runs the same
            // check with no bytes, so a dribble that ends after the window is cut too. A
            // below-bound window yields one error item, which unwinds `stream_import` so its
            // `ImportPath` sentinel clears and the epoch is retryable.
            let now = reader_now();
            let chunk = chunk.and_then(|frame| {
                let delivered = frame
                    .as_ref()
                    .map_or(0, |bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                meter.record(delivered);
                if watch.record(now, delivered) {
                    Ok(frame)
                } else {
                    meter.note_cut(bound);
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "epoch pack sync throughput below {bound:?} bound ({min} bytes / {secs}s)",
                            min = bound.min_bytes(),
                            secs = SYNC_RATE_WINDOW.as_secs(),
                        ),
                    ))
                }
            });
            let state = (stream, decode_buffer, compressed_buffer, watch, meter);
            // Result<Option<bytes>> -> Option<Result<bytes>>: `End` stops the
            // stream, `Data` yields a chunk, an error yields one error item
            chunk.transpose().map(|item| (item.map(std::io::Cursor::new), state))
        },
    );
    tokio_util::io::StreamReader::new(Box::pin(frames))
}

/// Read the next pack chunk from an accepted exchange: `Ok(Some(bytes))` for a
/// `Data` frame, `Ok(None)` for `End`, `Err` for an aborted or malformed stream.
async fn next_pack_chunk<S>(
    stream: &mut S,
    decode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
) -> std::io::Result<Option<Vec<u8>>>
where
    S: FuturesAsyncRead + Unpin + Send,
{
    let frame = tokio::time::timeout(
        EPOCH_PACK_FRAME_TIMEOUT,
        read_frame::<_, PrimarySyncRequest>(
            stream,
            decode_buffer,
            compressed_buffer,
            MAX_SYNC_PACK_FRAME_SIZE,
        ),
    )
    .await
    .map_err(|_elapsed| {
        std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout reading epoch pack sync frame")
    })?
    // A substream that closes without an `End` frame reads as EOF inside a frame. Left as
    // `UnexpectedEof`, the pack decoder on top of this reader would take an EOF that lands on a
    // record boundary for a clean end of the pack, and the importer would then charge the peer
    // for a short pack. It is a transport failure: report it as one.
    .map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "epoch pack sync stream closed before its End frame",
            )
        } else {
            e
        }
    })?;

    match frame {
        SyncFrame::Data(bytes) => Ok(Some(bytes)),
        SyncFrame::End => Ok(None),
        SyncFrame::Err(err) => {
            Err(std::io::Error::other(format!("peer aborted epoch pack sync stream: {err:?}")))
        }
        // a well-behaved responder never sends these once streaming
        SyncFrame::Ack | SyncFrame::Deny(_) | SyncFrame::Req(_) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected sync frame during epoch pack stream",
        )),
    }
}

/// Serve an accepted single-consensus-output sync exchange over `stream`.
///
/// `bytes` is the output's pack-file-encoded bytes when the responder holds it, or
/// `None` when it is unavailable (an unknown number, or one not yet built during
/// catch-up). An unavailable output sheds with
/// [`SyncFrame::Deny`]`(`[`DenyReason::Unavailable`]`)` so the requester retries
/// another peer immediately instead of waiting out its ack timeout. Otherwise it
/// writes [`SyncFrame::Ack`], streams the bytes as [`SyncFrame::Data`] frames, and
/// ends with [`SyncFrame::End`]. Reuses the epoch-pack chunk framing
/// ([`write_pack_data_frames`]) and its [`MAX_SYNC_PACK_FRAME_SIZE`] bound, so the
/// requester reassembles it with [`sync_pack_reader`]. Each frame write
/// is bounded by `buffer_timeout` so a peer that stops reading cannot pin the
/// responder task. The caller has already admitted the exchange against the
/// concurrency caps and read the opening request frame.
pub(crate) async fn send_sync_consensus_output_over_stream<S>(
    stream: &mut S,
    bytes: Option<Vec<u8>>,
    buffer_timeout: Duration,
    peer: BlsPublicKey,
    number: u64,
) -> PrimaryNetworkResult<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

    // an unavailable output sheds with `Deny(Unavailable)` so the requester retries
    // elsewhere without waiting out its ack timeout, exactly as the epoch-pack serve
    // does on a `get_epoch_stream` miss.
    let Some(bytes) = bytes else {
        debug!(target: "primary::network", %peer, number, "consensus output unavailable; denying sync request");
        write_one_frame(
            stream,
            &SyncFrame::Deny(DenyReason::Unavailable),
            &mut encode_buffer,
            &mut compressed_buffer,
            MAX_SYNC_PACK_FRAME_SIZE,
            buffer_timeout,
        )
        .await?;
        return Ok(());
    };

    // accept and flush immediately so the requester sees the `Ack` within its ack
    // timeout, then stream the output bytes as fixed-size `Data` frames.
    write_one_frame(
        stream,
        &SyncFrame::Ack,
        &mut encode_buffer,
        &mut compressed_buffer,
        MAX_SYNC_PACK_FRAME_SIZE,
        buffer_timeout,
    )
    .await?;
    write_pack_data_frames(&mut std::io::Cursor::new(bytes), stream, buffer_timeout).await?;
    Ok(())
}

/// Serve an accepted missing-certificates sync exchange over `stream`.
///
/// Writes [`SyncFrame::Ack`], then streams the certificates yielded by `certs` as
/// batched [`SyncFrame::Data`] frames (each one BCS-encoded `Vec<Certificate>`
/// targeting [`SYNC_CERT_BATCH_TARGET_SIZE`]), and ends with [`SyncFrame::End`].
///
/// Unlike the legacy request-response reply there is no aggregate cap declared by
/// the requester: the responder streams until `certs` is exhausted, the stream
/// stops yielding (the streaming collector's processing-time limit), or the honest
/// byte cap [`MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES`] is reached. Each frame write
/// is bounded by `buffer_timeout` so a peer that stops reading cannot pin the
/// responder task. The caller has already admitted the exchange against the
/// concurrency caps and read the opening request frame.
///
/// A storage read error yielded by `certs` aborts with that error; the caller then
/// signals [`SyncFrame::Err`]. Certificates already written stay valid on the wire
/// because the requester only commits the response on [`SyncFrame::End`].
pub(crate) async fn send_sync_certificates_over_stream<S, I>(
    stream: &mut S,
    certs: I,
    buffer_timeout: Duration,
) -> PrimaryNetworkResult<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
    I: Stream<Item = PrimaryNetworkResult<Certificate>>,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

    // accept and flush immediately so the requester sees the `Ack` within its ack timeout
    write_one_frame(
        stream,
        &SyncFrame::Ack,
        &mut encode_buffer,
        &mut compressed_buffer,
        MAX_SYNC_CERT_FRAME_SIZE,
        buffer_timeout,
    )
    .await?;

    // Fold the certificates into target-sized `Data` frames: each step appends the
    // certificate to the in-progress batch and flushes a frame once the batch reaches
    // `SYNC_CERT_BATCH_TARGET_SIZE`; `finish` then writes the trailing partial batch
    // and the closing `End`.
    //
    // `scan` enforces the honest response cap on the cumulative encoded size, ending
    // the stream once `MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES` is reached, including the
    // certificate that crosses it, matching the requester's one-frame acceptance
    // margin. Bounding the stream rather than the fold stops the fold from draining the
    // time-bounded collector past what it will send (at the cost of one boundary read).
    // A storage error yielded by `certs` short-circuits the fold and propagates so the
    // caller signals `SyncFrame::Err`.
    certs
        .map(|cert| {
            cert.and_then(|cert| encoded_size(&cert).map(|size| (size, cert)).map_err(Into::into))
        })
        .scan(0usize, |sent, sized| {
            let within_cap = *sent < MAX_SYNC_MISSING_CERTS_RESPONSE_BYTES;
            futures::future::ready(within_cap.then(|| sized.inspect(|(size, _)| *sent += *size)))
        })
        .try_fold(
            CertBatch::new(stream, encode_buffer, compressed_buffer, buffer_timeout),
            |batch, (size, cert)| batch.push(cert, size),
        )
        .await?
        .finish()
        .await
}

/// Encode `batch` into one [`SyncFrame::Data`] frame (a BCS `Vec<Certificate>`) and
/// write it, bounded by `buffer_timeout`. Serializing the slice by reference
/// produces the same bytes the requester decodes as a `Vec<Certificate>`.
async fn write_cert_batch_frame<S>(
    stream: &mut S,
    batch: &[Certificate],
    encode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    buffer_timeout: Duration,
) -> std::io::Result<()>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    write_one_frame(
        stream,
        &SyncFrame::Data(encode(&batch)),
        encode_buffer,
        compressed_buffer,
        MAX_SYNC_CERT_FRAME_SIZE,
        buffer_timeout,
    )
    .await
}

/// The in-progress state threaded through [`send_sync_certificates_over_stream`]'s
/// fold: the certificates gathered for the next [`SyncFrame::Data`] frame and the
/// reusable encode buffers, carried alongside the stream so each fold step can flush
/// a full frame in place.
struct CertBatch<'s, S> {
    stream: &'s mut S,
    encode_buffer: Vec<u8>,
    compressed_buffer: Vec<u8>,
    batch: Vec<Certificate>,
    batch_bytes: usize,
    buffer_timeout: Duration,
}

impl<'s, S> CertBatch<'s, S>
where
    S: FuturesAsyncWrite + Unpin + Send,
{
    fn new(
        stream: &'s mut S,
        encode_buffer: Vec<u8>,
        compressed_buffer: Vec<u8>,
        buffer_timeout: Duration,
    ) -> Self {
        Self {
            stream,
            encode_buffer,
            compressed_buffer,
            batch: Vec::new(),
            batch_bytes: 0,
            buffer_timeout,
        }
    }

    /// Append one certificate (encoded size `size`), flushing a full
    /// [`SyncFrame::Data`] frame once the batch reaches [`SYNC_CERT_BATCH_TARGET_SIZE`].
    async fn push(mut self, cert: Certificate, size: usize) -> PrimaryNetworkResult<Self> {
        self.batch.push(cert);
        self.batch_bytes += size;
        match () {
            () if self.batch_bytes >= SYNC_CERT_BATCH_TARGET_SIZE => self.flush().await?,
            () => (),
        }
        Ok(self)
    }

    /// Write the gathered batch as one [`SyncFrame::Data`] frame and reset.
    async fn flush(&mut self) -> std::io::Result<()> {
        write_cert_batch_frame(
            self.stream,
            &self.batch,
            &mut self.encode_buffer,
            &mut self.compressed_buffer,
            self.buffer_timeout,
        )
        .await?;
        self.batch.clear();
        self.batch_bytes = 0;
        Ok(())
    }

    /// Flush the trailing partial batch, if any, then close the exchange with
    /// [`SyncFrame::End`].
    async fn finish(mut self) -> PrimaryNetworkResult<()> {
        match () {
            () if !self.batch.is_empty() => self.flush().await?,
            () => (),
        }
        write_one_frame(
            self.stream,
            &SyncFrame::End,
            &mut self.encode_buffer,
            &mut self.compressed_buffer,
            MAX_SYNC_CERT_FRAME_SIZE,
            self.buffer_timeout,
        )
        .await
        .map_err(Into::into)
    }
}

/// Read the certificate [`SyncFrame::Data`] frames of an accepted
/// missing-certificates exchange into a `Vec`, terminating at [`SyncFrame::End`].
///
/// The caller has already read the opening [`SyncFrame::Ack`]; this reads the
/// `Data`/`End` stream. Each `Data` frame is a BCS-encoded `Vec<Certificate>` — an
/// untrusted peer payload, so it is decoded fallibly. An [`SyncFrame::Err`] frame,
/// any out-of-place frame, a frame larger than [`MAX_SYNC_CERT_FRAME_SIZE`], a
/// malformed payload, a stalled frame, or a response whose total encoded size
/// exceeds [`MAX_SYNC_MISSING_CERTS_ACCEPT_BYTES`] surfaces as an error so the
/// caller drops the exchange and tries another peer. The returned certificates are
/// unverified; the caller validates them.
pub(crate) async fn read_sync_certificates<S>(stream: &mut S) -> std::io::Result<Vec<Certificate>>
where
    S: FuturesAsyncRead + Unpin + Send,
{
    let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());

    // Unfold the `Data`/`End` stream into the payload of each `Data` frame: the
    // generator reads one frame per step (each bounded by `MISSING_CERTS_FRAME_TIMEOUT`),
    // yields the `Data` bytes, ends the stream on `End`, and surfaces an aborted (`Err`)
    // or out-of-place frame as an error that short-circuits the fold below. The stream
    // threads the `&mut` stream and reusable decode buffers through its state so each
    // step reuses them, mirroring the loop's single buffer pair.
    let data_frames = futures::stream::try_unfold(
        (stream, &mut decode_buffer, &mut compressed_buffer),
        |(stream, decode_buffer, compressed_buffer)| async move {
            let frame = tokio::time::timeout(
                MISSING_CERTS_FRAME_TIMEOUT,
                read_frame::<_, PrimarySyncRequest>(
                    stream,
                    decode_buffer,
                    compressed_buffer,
                    MAX_SYNC_CERT_FRAME_SIZE,
                ),
            )
            .await
            .map_err(|_elapsed| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timeout reading missing certificates sync frame",
                )
            })??;

            match frame {
                SyncFrame::Data(bytes) => {
                    Ok(Some((bytes, (stream, decode_buffer, compressed_buffer))))
                }
                SyncFrame::End => Ok(None),
                SyncFrame::Err(err) => Err(std::io::Error::other(format!(
                    "peer aborted missing certificates sync stream: {err:?}"
                ))),
                // a well-behaved responder never sends these once streaming
                SyncFrame::Ack | SyncFrame::Deny(_) | SyncFrame::Req(_) => {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "unexpected sync frame during missing certificates stream",
                    ))
                }
            }
        },
    );

    // Fold the `Data` payloads into the certificate vector: each step decodes one
    // untrusted frame and extends the gathered certificates. The running byte total
    // bounds the cumulative encoded size by `MAX_SYNC_MISSING_CERTS_ACCEPT_BYTES`
    // (counting the frame that crosses it, as the loop's `total_bytes` check did) and
    // short-circuits with an error so the caller drops the exchange and tries another
    // peer.
    data_frames
        .try_fold((Vec::new(), 0usize), |(mut certificates, total_bytes), bytes| async move {
            let total_bytes = total_bytes + bytes.len();
            (total_bytes <= MAX_SYNC_MISSING_CERTS_ACCEPT_BYTES).then_some(()).ok_or_else(
                || std::io::Error::other("missing certificates sync response exceeded size cap"),
            )?;
            let batch: Vec<Certificate> = try_decode(&bytes).map_err(std::io::Error::other)?;
            certificates.extend(batch);
            Ok((certificates, total_bytes))
        })
        .await
        .map(|(certificates, _total_bytes)| certificates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tn_network_libp2p::SyncFrameError;

    /// Four throughput-floor units of pack: sixteen full `Data` frames.
    const DRIP_PACK_LEN: usize = 4 << 20;

    /// A requester that takes `delay` to accept each frame flush, so every frame lands inside the
    /// per-frame write timeout while the whole serve runs as long as the frame count allows.
    /// Writes and close complete at once.
    struct DripSink {
        /// How long each flush waits.
        delay: Duration,
        /// The flush in progress, if any.
        pending: Option<Pin<Box<tokio::time::Sleep>>>,
    }

    impl FuturesAsyncWrite for DripSink {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let delay = self.delay;
            let pending = &mut self.pending;
            let ready = std::future::Future::poll(
                pending.get_or_insert_with(|| Box::pin(tokio::time::sleep(delay))).as_mut(),
                cx,
            );
            ready.map(|()| {
                *pending = None;
                Ok(())
            })
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// The length of the drip pack as the deadline sees it.
    fn drip_pack_bytes() -> u64 {
        u64::try_from(DRIP_PACK_LEN).unwrap_or(u64::MAX)
    }

    /// Serve a [`DRIP_PACK_LEN`] pack of zeros to a [`DripSink`] that takes `delay` to accept each
    /// frame, with a 10 s per-frame write timeout. Returns the serve result and the (paused) time
    /// it took.
    async fn drip_serve(delay: Duration) -> (std::io::Result<()>, Duration) {
        let mut reader = std::io::Cursor::new(vec![0u8; DRIP_PACK_LEN]);
        let mut sink = DripSink { delay, pending: None };
        let start = tokio::time::Instant::now();
        let served = write_pack_data_frames_within_deadline(
            &mut reader,
            drip_pack_bytes(),
            &mut sink,
            Duration::from_secs(10),
        )
        .await;
        (served, start.elapsed())
    }

    /// The total epoch-pack serve deadline is one throughput-floor window per started MiB of
    /// pack, plus one window, and saturates instead of overflowing.
    #[test]
    fn pack_serve_deadline_is_one_window_per_floor_plus_one() {
        assert_eq!(pack_serve_deadline(0), Duration::from_secs(30));
        assert_eq!(pack_serve_deadline(SYNC_RATE_MIN_BYTES), Duration::from_secs(60));
        assert_eq!(
            pack_serve_deadline(SYNC_RATE_MIN_BYTES.saturating_add(1)),
            Duration::from_secs(90)
        );
        assert_eq!(
            pack_serve_deadline(SYNC_RATE_MIN_BYTES.saturating_mul(100)),
            Duration::from_secs(3030)
        );
        assert_eq!(pack_serve_deadline(u64::MAX), SYNC_RATE_WINDOW.saturating_mul(u32::MAX));
    }

    /// A requester that keeps every frame inside the per-frame write timeout, but reads so slowly
    /// that the whole serve would outlast its deadline, is cut at the deadline with a `TimedOut`
    /// error. Paused time leaves the wall-clock throughput floor idle, so only the deadline can
    /// end this serve.
    #[tokio::test(start_paused = true)]
    async fn drip_reader_is_cut_at_the_pack_serve_deadline() {
        let deadline = pack_serve_deadline(drip_pack_bytes());
        let (served, elapsed) = drip_serve(Duration::from_millis(9_900)).await;
        let err = served.expect_err("a drip reader must not finish the serve");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "got {err:?}");
        assert!(err.to_string().contains("deadline"), "got {err:?}");
        assert!(elapsed >= deadline, "cut at {elapsed:?}, before the {deadline:?} deadline");
        assert!(
            elapsed < deadline.saturating_add(Duration::from_secs(1)),
            "cut at {elapsed:?}, well after the {deadline:?} deadline"
        );
    }

    /// A requester that accepts each frame after one second finishes the same serve well inside
    /// its deadline.
    #[tokio::test(start_paused = true)]
    async fn steady_reader_finishes_inside_the_pack_serve_deadline() {
        let deadline = pack_serve_deadline(drip_pack_bytes());
        let (served, elapsed) = drip_serve(Duration::from_secs(1)).await;
        assert!(served.is_ok(), "got {served:?}");
        assert!(elapsed < deadline, "took {elapsed:?}, past the {deadline:?} deadline");
    }

    /// The throughput floor trips only on a COMPLETED window that carried fewer than
    /// `min_bytes`; an at/above-floor window resets and does not trip, and a partial
    /// (not-yet-elapsed) window keeps accumulating. Uses a manual clock so the tumbling-window
    /// logic is deterministic.
    /// A record of the largest size a pack may hold, delivered at exactly the throughput floor,
    /// must arrive inside the per-record timeout: otherwise the timeout, not the floor, decides
    /// which honest links can sync.
    #[test]
    fn record_timeout_admits_the_largest_record_at_the_floor() {
        let max_record = tn_storage::archive::pack_iter::MAX_RECORD_SIZE as f64;
        let floor_bytes_per_sec = SYNC_RATE_MIN_BYTES as f64 / SYNC_RATE_WINDOW.as_secs_f64();
        assert!(SYNC_RECORD_TIMEOUT.as_secs_f64() > max_record / floor_bytes_per_sec);
    }

    #[test]
    fn rate_watch_trips_only_below_floor_per_window() {
        let t0 = Instant::now();
        let window = Duration::from_secs(30);
        let mut w = RateWatch::new(window, 1000, t0);

        // Partial window: bytes accumulate, never trips even when below floor so far.
        assert!(w.record(t0 + Duration::from_secs(10), 100));
        assert!(w.record(t0 + Duration::from_secs(20), 100));
        // First completed window (>= 30s) carried 200 < 1000 -> trips.
        assert!(!w.record(t0 + Duration::from_secs(31), 0), "below-floor window must trip");

        // Fresh window (reset at 31s); deliver >= floor before it elapses -> no trip, and it
        // resets.
        let mut w = RateWatch::new(window, 1000, t0);
        assert!(w.record(t0 + Duration::from_secs(5), 5000));
        assert!(w.record(t0 + Duration::from_secs(31), 0), "at/above-floor window must not trip");
        // Next window below floor trips again (counter reset after the previous completed window).
        assert!(!w.record(t0 + Duration::from_secs(62), 10));
    }

    /// A window stretched past `window` by a late chunk must carry `min_bytes` prorated by its
    /// real length: 1000 bytes over 40 s is below a 1000-per-30 s floor, 1334 bytes is not, and a
    /// window that completes at exactly 30 s owes exactly `min_bytes`.
    #[test]
    fn rate_watch_prorates_a_stretched_window() {
        let t0 = Instant::now();
        let window = Duration::from_secs(30);

        let mut w = RateWatch::new(window, 1000, t0);
        assert!(!w.record(t0 + Duration::from_secs(40), 1000), "1000 bytes over 40 s must trip");

        let mut w = RateWatch::new(window, 1000, t0);
        assert!(w.record(t0 + Duration::from_secs(40), 1334), "1334 bytes over 40 s must pass");

        let mut w = RateWatch::new(window, 1000, t0);
        assert!(w.record(t0 + Duration::from_secs(30), 1000), "1000 bytes over 30 s must pass");
    }

    /// The target bound is eight floors, and a probe reads a de-prioritised peer at the floor and
    /// a fresh peer at the target.
    #[test]
    fn sync_rate_bound_target_is_eight_floors() {
        assert_eq!(SyncRateBound::Target.min_bytes(), 8 * SyncRateBound::Floor.min_bytes());
        assert_eq!(SyncRateBound::for_probe(true), SyncRateBound::Floor);
        assert_eq!(SyncRateBound::for_probe(false), SyncRateBound::Target);
    }

    /// Every `Data` byte the reader delivers is counted on the caller's meter clone.
    #[tokio::test]
    async fn sync_serve_meter_counts_delivered_bytes() {
        // a full chunk plus a partial one: two `Data` frames
        let original = vec![7u8; SYNC_PACK_CHUNK_SIZE + 1234];
        let framed = frame_pack(&original).await;
        let meter = SyncServeMeter::default();
        let mut reader =
            sync_pack_reader(futures::io::Cursor::new(framed), SyncRateBound::Floor, meter.clone());
        let mut received = Vec::new();
        reader.read_to_end(&mut received).await.expect("read back");
        assert_eq!(received, original);
        assert_eq!(meter.bytes(), u64::try_from(original.len()).unwrap_or(u64::MAX));
    }

    /// A small output that completes inside the latency allowance is a target-rate serve.
    #[test]
    fn serve_speed_small_output_within_allowance_is_target() {
        let meter = SyncServeMeter::default();
        meter.record(10 * 1024);
        assert_eq!(meter.speed(Duration::from_millis(300)), SyncServeSpeed::Target);
    }

    /// An output dribbled over 25 s is far slower than the target rate.
    #[test]
    fn serve_speed_dribbled_output_is_slow() {
        let meter = SyncServeMeter::default();
        meter.record(500 * 1024);
        assert_eq!(meter.speed(Duration::from_secs(25)), SyncServeSpeed::Slow);
    }

    /// A 100 MiB pack delivered at exactly the target rate is a target-rate serve inside the 1 s
    /// latency allowance, and a slow one past it.
    #[test]
    fn serve_speed_pack_at_target_rate_is_target() {
        let meter = SyncServeMeter::default();
        meter.record(100 << 20);
        // 100 MiB at 8 MiB per 30 s takes 100 MiB * 30 s / 8 MiB = 375 s.
        let at_target = Duration::from_secs(375);
        assert_eq!(
            meter.speed(at_target.saturating_add(Duration::from_secs(1))),
            SyncServeSpeed::Target
        );
        assert_eq!(
            meter.speed(at_target.saturating_add(Duration::from_secs(2))),
            SyncServeSpeed::Slow
        );
    }

    /// A peer that sends one small `Data` frame and then ends only after the window elapsed is cut
    /// at the `End` frame with a `TimedOut` read error, not finished as a clean serve: the bound
    /// is checked on `End` too. Paused time drives the reader's clock.
    #[tokio::test(start_paused = true)]
    async fn sync_pack_reader_cuts_a_dribble_that_ends_after_the_window() {
        let framed = frame_pack(&[7u8; 1024]).await;
        let mut reader = sync_pack_reader(
            futures::io::Cursor::new(framed),
            SyncRateBound::Floor,
            SyncServeMeter::default(),
        );
        let mut first = vec![0u8; 4096];
        let read = reader.read(&mut first).await.expect("read the data frame");
        assert_eq!(read, 1024);

        tokio::time::advance(SYNC_RATE_WINDOW.saturating_add(Duration::from_secs(1))).await;

        let mut rest = Vec::new();
        let err = reader.read_to_end(&mut rest).await.expect_err("a dribble must not end cleanly");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "got {err:?}");
        assert!(err.to_string().contains("Floor"), "got {err:?}");
    }

    /// Drive the dribble of `sync_pack_reader_cuts_a_dribble_that_ends_after_the_window` at
    /// `bound` with `meter`: one small `Data` frame, then `End` after the window. Returns the
    /// result of reading the rest of the stream after the window.
    async fn read_dribble_at(
        bound: SyncRateBound,
        meter: SyncServeMeter,
    ) -> std::io::Result<usize> {
        let payload = [7u8; 1024];
        let framed = frame_pack(&payload).await;
        let mut reader = sync_pack_reader(futures::io::Cursor::new(framed), bound, meter);
        let mut first = vec![0u8; payload.len()];
        reader.read_exact(&mut first).await?;
        tokio::time::advance(SYNC_RATE_WINDOW.saturating_add(Duration::from_secs(1))).await;
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest).await
    }

    /// A reader cut for falling below the target bound marks the cut on the caller's meter clone,
    /// so the probe walk can retry the peer at the floor (`walk_with_floor_fallback`, #1787). A
    /// cut at the floor and a clean read at the target leave the mark unset. Paused time drives
    /// the reader's clock.
    #[tokio::test(start_paused = true)]
    async fn sync_pack_reader_marks_a_target_cut_on_the_meter() {
        let target_cut = SyncServeMeter::default();
        let err = read_dribble_at(SyncRateBound::Target, target_cut.clone())
            .await
            .expect_err("a dribble must not end cleanly at the target");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "got {err:?}");
        assert!(err.to_string().contains("Target"), "got {err:?}");
        assert!(target_cut.cut_at_target(), "a cut at the target must be marked");

        let floor_cut = SyncServeMeter::default();
        let err = read_dribble_at(SyncRateBound::Floor, floor_cut.clone())
            .await
            .expect_err("a dribble must not end cleanly at the floor");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "got {err:?}");
        assert!(!floor_cut.cut_at_target(), "a cut at the floor must not be marked");

        let clean = SyncServeMeter::default();
        let framed = frame_pack(&[7u8; 1024]).await;
        let mut reader = sync_pack_reader(
            futures::io::Cursor::new(framed),
            SyncRateBound::Target,
            clean.clone(),
        );
        let mut received = Vec::new();
        reader.read_to_end(&mut received).await.expect("a clean read at the target");
        assert_eq!(received.len(), 1024);
        assert!(!clean.cut_at_target(), "a clean read must not be marked");
        assert!(clean.bytes() > 0, "a clean read must count its bytes");
    }

    /// Frame `bytes` through [`write_pack_data_frames`] into an in-memory buffer.
    async fn frame_pack(bytes: &[u8]) -> Vec<u8> {
        let mut reader = std::io::Cursor::new(bytes.to_vec());
        let mut out = Vec::new();
        write_pack_data_frames(&mut reader, &mut out, Duration::from_secs(5))
            .await
            .expect("frame pack");
        out
    }

    /// Reassemble framed bytes through [`sync_pack_reader`].
    async fn read_back(framed: Vec<u8>) -> std::io::Result<Vec<u8>> {
        let mut reader = sync_pack_reader(
            futures::io::Cursor::new(framed),
            SyncRateBound::Floor,
            SyncServeMeter::default(),
        );
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await?;
        Ok(out)
    }

    /// The responder's `Data`+`End` framing round-trips through the requester's
    /// reassembly reader across multiple chunk boundaries: the reconstructed pack
    /// bytes are byte-for-byte the original. This is the cross-version wire
    /// contract between an item-6 responder and an item-6 requester.
    #[tokio::test]
    async fn sync_pack_round_trip_multi_chunk() {
        // two full chunks plus a partial one, so the reader spans frame boundaries
        let original: Vec<u8> =
            (0..(SYNC_PACK_CHUNK_SIZE * 2 + 1234)).map(|i| (i % 251) as u8).collect();
        let framed = frame_pack(&original).await;
        let received = read_back(framed).await.expect("read back");
        assert_eq!(received, original);
    }

    /// An empty pack streams only `End`; the reader yields zero bytes cleanly.
    #[tokio::test]
    async fn sync_pack_round_trip_empty() {
        let framed = frame_pack(&[]).await;
        let received = read_back(framed).await.expect("read back");
        assert!(received.is_empty());
    }

    /// An `Err` frame mid-stream surfaces as a read error so `stream_import` aborts.
    #[tokio::test]
    async fn sync_pack_reader_rejects_err_frame() {
        let mut framed = Vec::new();
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        write_frame(
            &mut framed,
            &SyncFrame::<PrimarySyncRequest>::Data(vec![1, 2, 3]),
            &mut enc,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("write data");
        write_frame(
            &mut framed,
            &SyncFrame::<PrimarySyncRequest>::Err(SyncFrameError::Internal),
            &mut enc,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("write err");
        assert!(read_back(framed).await.is_err(), "an Err frame must surface as a read error");
    }

    /// A stream that ends without an `End` frame is a transport failure, never a clean end of
    /// the pack: the reader must surface an error (and not `UnexpectedEof`, which the pack decoder
    /// above it treats as a clean end on a record boundary).
    #[tokio::test]
    async fn sync_pack_reader_rejects_eof_without_end_frame() {
        let mut framed = Vec::new();
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        write_frame(
            &mut framed,
            &SyncFrame::<PrimarySyncRequest>::Data(vec![1, 2, 3]),
            &mut enc,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("write data");
        let err = read_back(framed).await.expect_err("EOF without End must be an error");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionAborted, "got {err:?}");
    }

    /// An out-of-place control frame (e.g. a second `Ack`) is a protocol
    /// violation and surfaces as a read error.
    #[tokio::test]
    async fn sync_pack_reader_rejects_unexpected_frame() {
        let mut framed = Vec::new();
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        write_frame(
            &mut framed,
            &SyncFrame::<PrimarySyncRequest>::Ack,
            &mut enc,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("write ack");
        assert!(
            read_back(framed).await.is_err(),
            "an unexpected control frame mid-stream must surface as a read error"
        );
    }

    /// The consensus-output serve writes `Ack` then the exact output bytes as
    /// `Data`+`End`, and `read_sync_consensus_output` reassembles them
    /// byte-for-byte across chunk boundaries. This is the wire contract between an
    /// item-9b responder and an item-9b requester.
    #[tokio::test]
    async fn sync_consensus_output_round_trip() {
        // one full chunk plus a partial one, so the reader spans a frame boundary
        let original: Vec<u8> =
            (0..(SYNC_PACK_CHUNK_SIZE + 777)).map(|i| (i % 251) as u8).collect();
        let mut wire = Vec::new();
        send_sync_consensus_output_over_stream(
            &mut wire,
            Some(original.clone()),
            Duration::from_secs(5),
            BlsPublicKey::default(),
            7,
        )
        .await
        .expect("serve consensus output");

        // consume the opening `Ack`, then reassemble the remaining `Data`/`End` frames
        let mut cursor = futures::io::Cursor::new(wire);
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let ack = read_frame::<_, PrimarySyncRequest>(
            &mut cursor,
            &mut dec,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("read ack");
        assert!(matches!(ack, SyncFrame::Ack), "first frame must be Ack");
        let mut received = Vec::new();
        sync_pack_reader(cursor, SyncRateBound::Floor, SyncServeMeter::default())
            .read_to_end(&mut received)
            .await
            .expect("reassemble output");
        assert_eq!(received, original, "reassembled bytes must equal the served output");
    }

    /// An empty served output streams `Ack` then only `End`; the reader yields zero
    /// bytes cleanly (distinct from the `None`/`Deny` unavailable case below).
    #[tokio::test]
    async fn sync_consensus_output_round_trip_empty() {
        let mut wire = Vec::new();
        send_sync_consensus_output_over_stream(
            &mut wire,
            Some(Vec::new()),
            Duration::from_secs(5),
            BlsPublicKey::default(),
            7,
        )
        .await
        .expect("serve empty consensus output");
        let mut cursor = futures::io::Cursor::new(wire);
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let ack = read_frame::<_, PrimarySyncRequest>(
            &mut cursor,
            &mut dec,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("read ack");
        assert!(matches!(ack, SyncFrame::Ack), "first frame must be Ack");
        let mut received = Vec::new();
        sync_pack_reader(cursor, SyncRateBound::Floor, SyncServeMeter::default())
            .read_to_end(&mut received)
            .await
            .expect("reassemble output");
        assert!(received.is_empty(), "an empty served output reassembles to zero bytes");
    }

    /// An unavailable output (`None`) is shed with `Deny(Unavailable)` and no `Ack`,
    /// so a sync requester retries another peer immediately.
    #[tokio::test]
    async fn sync_consensus_output_unavailable_denies() {
        let mut wire = Vec::new();
        send_sync_consensus_output_over_stream(
            &mut wire,
            None,
            Duration::from_secs(5),
            BlsPublicKey::default(),
            7,
        )
        .await
        .expect("serving an unavailable output sheds cleanly");
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let frame = read_frame::<_, PrimarySyncRequest>(
            &mut futures::io::Cursor::new(wire),
            &mut dec,
            &mut comp,
            MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("read deny");
        assert!(
            matches!(frame, SyncFrame::Deny(DenyReason::Unavailable)),
            "an unavailable output must deny"
        );
    }

    /// Serve `certs` through [`send_sync_certificates_over_stream`] (`Ack` + `Data`* +
    /// `End`), consume the opening `Ack`, then reassemble through
    /// [`read_sync_certificates`]. Returns the reconstructed certificates.
    async fn round_trip_certs(certs: Vec<Certificate>) -> std::io::Result<Vec<Certificate>> {
        let mut wire = Vec::new();
        send_sync_certificates_over_stream(
            &mut wire,
            futures::stream::iter(certs.into_iter().map(Ok)),
            Duration::from_secs(5),
        )
        .await
        .expect("serve certs");

        let mut reader = futures::io::Cursor::new(wire);
        let (mut dec, mut comp) = (Vec::new(), Vec::new());
        let ack: SyncFrame<PrimarySyncRequest> =
            read_frame(&mut reader, &mut dec, &mut comp, MAX_SYNC_CERT_FRAME_SIZE)
                .await
                .expect("read ack");
        assert!(matches!(ack, SyncFrame::Ack), "responder opens with Ack");
        read_sync_certificates(&mut reader).await
    }

    /// A handful of certificates round-trip through one `Data` batch frame.
    #[tokio::test]
    async fn sync_certs_round_trip_single_batch() {
        let certs = vec![Certificate::default(), Certificate::default(), Certificate::default()];
        let received = round_trip_certs(certs.clone()).await.expect("round trip");
        assert_eq!(received, certs);
    }

    /// An empty response streams only `Ack` + `End`; the reader yields zero certs cleanly.
    #[tokio::test]
    async fn sync_certs_round_trip_empty() {
        let received = round_trip_certs(Vec::new()).await.expect("round trip");
        assert!(received.is_empty());
    }

    /// Enough certificates to cross the batch-flush threshold, so the response spans
    /// multiple `Data` frames and the reader reassembles across frame boundaries. This
    /// is the cross-version wire contract between an item-7 responder and requester.
    #[tokio::test]
    async fn sync_certs_round_trip_multi_batch() {
        let one = encode(&Certificate::default()).len().max(1);
        // strictly more than two full batches so at least three `Data` frames are sent
        let count = (SYNC_CERT_BATCH_TARGET_SIZE * 2) / one + 5;
        let certs = vec![Certificate::default(); count];
        let received = round_trip_certs(certs.clone()).await.expect("round trip");
        assert_eq!(received.len(), count);
        assert_eq!(received, certs);
    }

    /// An `Err` frame after some `Data` surfaces as a read error so the fetch aborts.
    #[tokio::test]
    async fn read_sync_certificates_rejects_err_frame() {
        let mut wire = Vec::new();
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        write_frame(
            &mut wire,
            &SyncFrame::<PrimarySyncRequest>::Data(encode(&vec![Certificate::default()])),
            &mut enc,
            &mut comp,
            MAX_SYNC_CERT_FRAME_SIZE,
        )
        .await
        .expect("write data");
        write_frame(
            &mut wire,
            &SyncFrame::<PrimarySyncRequest>::Err(SyncFrameError::Internal),
            &mut enc,
            &mut comp,
            MAX_SYNC_CERT_FRAME_SIZE,
        )
        .await
        .expect("write err");
        let mut reader = futures::io::Cursor::new(wire);
        assert!(
            read_sync_certificates(&mut reader).await.is_err(),
            "an Err frame must surface as a read error"
        );
    }

    /// An out-of-place control frame (a stray `Ack`) mid-stream is a protocol violation
    /// and surfaces as a read error.
    #[tokio::test]
    async fn read_sync_certificates_rejects_unexpected_frame() {
        let mut wire = Vec::new();
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        write_frame(
            &mut wire,
            &SyncFrame::<PrimarySyncRequest>::Ack,
            &mut enc,
            &mut comp,
            MAX_SYNC_CERT_FRAME_SIZE,
        )
        .await
        .expect("write stray ack");
        let mut reader = futures::io::Cursor::new(wire);
        assert!(
            read_sync_certificates(&mut reader).await.is_err(),
            "an unexpected control frame mid-stream must surface as a read error"
        );
    }
}
