//! A background-thread-free, drop-in twin of [`ConsensusPack`](crate::consensus_pack::ConsensusPack).
//!
//! [`ConsensusPack`](crate::consensus_pack::ConsensusPack) runs one OS thread per pack: every public
//! `async fn` sends a message over an mpsc channel and awaits a `oneshot` reply while a background
//! thread performs the IO. [`ConsensusPackDirect`] holds the same [`Inner`] state behind an
//! `Arc<Mutex<..>>` and makes every call **inline on the caller's task** — no channel, no thread, no
//! context switch. The two front-ends share the exact same `Inner` IO and the same decode helpers
//! ([`decode_output_bytes`]/[`serve_output_bytes`]), so it exists to isolate the background-thread
//! overhead in [`crate::pack_bench`]: a per-op timing delta between the two is the thread/channel
//! cost. Not used in production.
//!
//! The lock is only ever held for the synchronous `Inner` call and is dropped before any `.await`,
//! so no lock is held across a suspension point.

// Methods mirror `ConsensusPack`'s `async fn` signatures for drop-in fidelity even though the direct
// implementation does no `.await` of its own (it calls `Inner` inline), so allow the pedantic lint
// about async fns without an await.
#![allow(clippy::unused_async)]

use std::{
    collections::HashMap,
    io::Cursor,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use parking_lot::Mutex;
use tn_types::{
    gas_accumulator::RewardsCounter, AuthorityIdentifier, Batch, BlockHash, CommittedSubDag,
    Committee, ConsensusHeader, ConsensusHeaderDigest, ConsensusOutput, Epoch, EpochRecord, Round,
};
use tokio::io::{AsyncRead, BufReader};

use crate::{
    archive::pack::{FileBackend, PackCompression},
    consensus_pack::{
        bytes_to_output, bytes_to_verified_output, decode_output_bytes, serve_output_bytes, Inner,
        PackError, PACK_VERSION,
    },
};

/// A [`ConsensusPack`](crate::consensus_pack::ConsensusPack) that performs all its IO directly
/// (inline on the caller's task) instead of on a background thread. Same public API and behaviour;
/// used to measure the background-thread/channel overhead (see the module docs).
#[derive(Debug, Clone)]
pub struct ConsensusPackDirect {
    /// The real pack state, shared (`Arc`) and mutable (`Mutex`) — no background thread owns it.
    inner: Arc<Mutex<Inner>>,
    epoch: Epoch,
    committee: Committee,
    compression: PackCompression,
    is_static: bool,
    version: u16,
}

impl ConsensusPackDirect {
    /// Opens a new epoch pack for append, creating the epoch files if they do not exist.
    pub fn open_append(
        path: impl Into<PathBuf>,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> Result<Self, PackError> {
        Self::open_append_inner(path, previous_epoch, committee, PACK_VERSION, FileBackend::default())
    }

    /// Like [`Self::open_append`] but selecting the on-disk file `backend` (buffered vs mmap).
    pub fn open_append_with_backend(
        path: impl Into<PathBuf>,
        previous_epoch: EpochRecord,
        committee: Committee,
        backend: FileBackend,
    ) -> Result<Self, PackError> {
        Self::open_append_inner(path, previous_epoch, committee, PACK_VERSION, backend)
    }

    /// Shared append-open body stamping the given on-disk data `version`.
    fn open_append_inner(
        path: impl Into<PathBuf>,
        previous_epoch: EpochRecord,
        committee: Committee,
        version: u16,
        backend: FileBackend,
    ) -> Result<Self, PackError> {
        let path: PathBuf = path.into();
        let epoch = committee.epoch();
        let inner = Inner::open_append(path, &previous_epoch, committee.clone(), version, backend)?;
        let version = inner.version();
        let compression = inner.compression();
        Ok(Self::from_inner(inner, epoch, committee, compression, false, version))
    }

    /// Open the files for a previous epoch in append mode. Fails if the files do not exist.
    pub fn open_append_exists(path: impl Into<PathBuf>, epoch: Epoch) -> Result<Self, PackError> {
        let inner = Inner::open_append_exists(path.into(), epoch, FileBackend::default())?;
        let version = inner.version();
        let compression = inner.compression();
        let committee = inner.epoch_meta().committee.clone();
        Ok(Self::from_inner(inner, epoch, committee, compression, false, version))
    }

    /// Open the static (read-only) files for a previous epoch.
    pub fn open_static(path: impl Into<PathBuf>, epoch: Epoch) -> Result<Self, PackError> {
        Self::open_static_with_backend(path, epoch, FileBackend::default())
    }

    /// Like [`Self::open_static`] but selecting the on-disk file `backend` (buffered vs mmap).
    pub fn open_static_with_backend(
        path: impl Into<PathBuf>,
        epoch: Epoch,
        backend: FileBackend,
    ) -> Result<Self, PackError> {
        let inner = Inner::open_static(path.into(), epoch, backend)?;
        let version = inner.version();
        let compression = inner.compression();
        let committee = inner.epoch_meta().committee.clone();
        Ok(Self::from_inner(inner, epoch, committee, compression, true, version))
    }

    /// Create a new set of epoch static files, replaying `stream` into them.
    pub async fn stream_import<R: AsyncRead + Unpin>(
        path: impl Into<PathBuf>,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        final_consensus_number: u64,
        timeout: Duration,
    ) -> Result<Self, PackError> {
        Self::stream_import_with_backend(
            path,
            stream,
            epoch,
            previous_epoch,
            final_consensus_number,
            timeout,
            FileBackend::default(),
        )
        .await
    }

    /// Like [`Self::stream_import`] but selecting the on-disk file `backend` (buffered vs mmap).
    #[allow(clippy::too_many_arguments)]
    pub async fn stream_import_with_backend<R: AsyncRead + Unpin>(
        path: impl Into<PathBuf>,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        final_consensus_number: u64,
        timeout: Duration,
        backend: FileBackend,
    ) -> Result<Self, PackError> {
        let inner = Inner::stream_import(
            path.into(),
            stream,
            epoch,
            previous_epoch,
            final_consensus_number,
            timeout,
            backend,
        )
        .await?;
        let version = inner.version();
        let compression = inner.compression();
        let committee = inner.epoch_meta().committee.clone();
        Ok(Self::from_inner(inner, epoch, committee, compression, true, version))
    }

    /// Assemble the handle from an opened [`Inner`] and its cached metadata.
    fn from_inner(
        inner: Inner,
        epoch: Epoch,
        committee: Committee,
        compression: PackCompression,
        is_static: bool,
        version: u16,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            epoch,
            committee,
            compression,
            is_static,
            version,
        }
    }

    /// Is this packfile static — i.e. complete and read only.
    pub fn is_static(&self) -> bool {
        self.is_static
    }

    /// Return the epoch for this pack file.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Save all batches and the consensus header from `consensus`, returning the bytes it took.
    pub async fn save_consensus_output(
        &self,
        consensus: ConsensusOutput,
    ) -> Result<u64, PackError> {
        self.inner.lock().save_consensus_output(&consensus)
    }

    /// Load and return the consensus output for `number` from this epoch.
    pub async fn get_consensus_output(&self, number: u64) -> Result<ConsensusOutput, PackError> {
        let bytes = { self.inner.lock().bytes_for_consensus(number)? };
        decode_output_bytes(bytes, self.version, self.compression, &self.committee).await
    }

    /// Decode pack-file `bytes` into a [`ConsensusOutput`] using this pack's committee/compression.
    pub async fn decode_output(&self, bytes: Vec<u8>) -> Result<ConsensusOutput, PackError> {
        let cursor = Cursor::new(bytes);
        let reader = BufReader::new(cursor);
        bytes_to_output(reader, self.compression, Duration::from_secs(5), &self.committee).await
    }

    /// Stream-decode a v1 pack-encoded [`ConsensusOutput`] from `reader`, verifying the header's
    /// digest equals `expected_digest` before any batch record is buffered.
    pub async fn decode_output_stream<R: AsyncRead + Unpin>(
        &self,
        reader: R,
        expected_digest: ConsensusHeaderDigest,
    ) -> Result<ConsensusOutput, PackError> {
        bytes_to_verified_output(
            reader,
            self.compression,
            Duration::from_secs(5),
            &self.committee,
            expected_digest,
        )
        .await
    }

    /// Load and return the raw pack-file bytes for `number` (the serve-to-peer path).
    pub async fn get_consensus_output_bytes(&self, number: u64) -> Result<Vec<u8>, PackError> {
        let bytes = { self.inner.lock().bytes_for_consensus(number)? };
        serve_output_bytes(bytes, self.version, self.compression, &self.committee).await
    }

    /// Byte offset just past the end of the consensus output for `number`.
    pub async fn consensus_output_end(&self, number: u64) -> Result<u64, PackError> {
        self.inner.lock().output_end_for_consensus(number)
    }

    /// True if the pack contains a consensus header for `number`.
    pub async fn contains_consensus_header_number(&self, number: u64) -> Result<bool, PackError> {
        Ok(self.inner.lock().contains_consensus_header_number(number))
    }

    /// True if the pack contains the consensus header for `digest`.
    pub async fn contains_consensus_header(&self, digest: ConsensusHeaderDigest) -> bool {
        self.inner.lock().contains_consensus_header(digest)
    }

    /// Retrieve a consensus header by digest.
    pub async fn consensus_header_by_digest(
        &self,
        digest: ConsensusHeaderDigest,
    ) -> Option<ConsensusHeader> {
        self.inner.lock().consensus_header_by_digest(digest)
    }

    /// Retrieve a consensus header by number.
    pub async fn consensus_header_by_number(
        &self,
        number: u64,
    ) -> Result<ConsensusHeader, PackError> {
        self.inner.lock().consensus_header_by_number(number)
    }

    /// Flush and durably persist all buffered writes.
    pub async fn persist(&self) -> Result<(), PackError> {
        self.inner.lock().persist()
    }

    /// Make buffered writes visible to a separate reader without a durability barrier.
    pub async fn flush_data(&self) -> Result<(), PackError> {
        self.inner.lock().flush_data()
    }

    /// Read the last committed rounds for authorities from the epoch.
    pub async fn read_last_committed(
        &self,
    ) -> Result<HashMap<AuthorityIdentifier, Round>, PackError> {
        self.inner.lock().read_last_committed()
    }

    /// Read the latest commit sub-dag whose reputation scores are marked final, if any.
    pub async fn read_latest_commit_with_final_reputation_scores(
        &self,
    ) -> Result<Option<CommittedSubDag>, PackError> {
        self.inner.lock().read_latest_commit_with_final_reputation_scores()
    }

    /// Return the latest consensus header by reading directly from the pack index.
    pub async fn latest_consensus_header(&self) -> Result<Option<ConsensusHeader>, PackError> {
        self.inner.lock().latest_consensus_header()
    }

    /// True if the pack contains the batch for `digest`.
    pub async fn contains_batch(&self, digest: BlockHash) -> bool {
        self.inner.lock().contains_batch(digest)
    }

    /// Return the batch for `digest` if found.
    pub async fn batch(&self, digest: BlockHash) -> Option<Batch> {
        self.inner.lock().batch(digest)
    }

    /// Count leaders (into `rewards_counter`) lower than `last_executed_round`.
    pub async fn count_leaders(
        &self,
        last_executed_round: Round,
        rewards_counter: RewardsCounter,
    ) -> Result<(), PackError> {
        self.inner.lock().count_leaders(last_executed_round, &rewards_counter)
    }
}

impl Drop for ConsensusPackDirect {
    fn drop(&mut self) {
        // Mirror ConsensusPack: the last live handle persists on close. Underlying Pack/index Drops
        // also flush, so this is best-effort (errors — e.g. on a read-only pack — are ignored).
        if Arc::strong_count(&self.inner) == 1 {
            let _ = self.inner.lock().persist();
        }
    }
}
