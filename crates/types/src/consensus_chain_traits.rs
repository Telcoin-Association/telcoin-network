// SPDX-License-Identifier: MIT or Apache-2.0
//! Facet traits for the consensus-side storage surface.
//!
//! The node-level storage has two impedance-mismatched layers:
//!
//! - The sync, generic-over-`Table`, KV-shaped [`Database`] trait, used for ephemeral epoch data,
//!   the KAD store, and per-epoch caches.
//! - An async, domain-typed, log-shaped consensus-chain handle, used for the durable
//!   consensus-chain pack files and the surrounding epoch metadata.
//!
//! These traits expose the consensus-chain side behind facet interfaces so call
//! sites can depend on what they actually use ([`ConsensusChainReader`] for read
//! paths, [`ConsensusChainWriter`] for write paths) instead of the concrete
//! handle.  A [`UnifiedStore`] blanket marries both layers for places that want a
//! single handle.
//!
//! Error handling follows the [`Database`] trait convention of `eyre::Result`,
//! so these interfaces compose cleanly with the rest of the workspace.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use tokio::io::{AsyncRead, AsyncSeek};

use crate::{
    gas_accumulator::RewardsCounter, AuthorityIdentifier, Batch, BlockHash, CommittedSubDag,
    Committee, ConsensusHeader, ConsensusOutput, Database, Epoch, EpochRecord, Round, B256,
};

/// Marker trait for an async readable+seekable stream over an epoch's pack
/// data file.  Returned boxed from [`ConsensusChainReader::get_epoch_stream`]
/// for use in cross-peer sync.
///
/// Auto-implemented for any type that satisfies the underlying tokio I/O
/// supertraits, so callers can `Box::new(my_async_file) as Box<dyn ReadStream>`
/// without needing an explicit impl per concrete stream type.
pub trait ReadStream: AsyncRead + AsyncSeek + Send + Unpin {}

impl<T: AsyncRead + AsyncSeek + Send + Unpin> ReadStream for T {}

/// Read-only async view of the consensus chain.
///
/// Models the queryable surface of the per-epoch pack files plus the
/// in-process "latest consensus" cursor.  Implementors are expected to be
/// cheap to clone (typically `Arc`-shared internally) so that handles can be
/// threaded through tasks without serialised access.
pub trait ConsensusChainReader: Send + Sync + Clone + 'static {
    /// Retrieve a consensus header from `epoch`'s pack file by digest.
    fn consensus_header_by_digest(
        &self,
        epoch: Epoch,
        digest: B256,
    ) -> impl Future<Output = eyre::Result<Option<ConsensusHeader>>> + Send;

    /// Retrieve a consensus header by global number.
    fn consensus_header_by_number(
        &self,
        number: u64,
    ) -> impl Future<Output = eyre::Result<Option<ConsensusHeader>>> + Send;

    /// Retrieve the most recent consensus header that was executed.
    fn consensus_header_latest(
        &self,
    ) -> impl Future<Output = eyre::Result<Option<ConsensusHeader>>> + Send;

    /// Read the latest consensus header directly from `epoch`'s pack index,
    /// bypassing the slot files.  Used during startup recovery so that the
    /// answer stays consistent with [`Self::read_last_committed`].
    fn latest_consensus_header_from_pack(
        &self,
        epoch: Epoch,
    ) -> impl Future<Output = eyre::Result<Option<ConsensusHeader>>> + Send;

    /// Number of the last consensus output that was processed.
    fn latest_consensus_number(&self) -> u64;

    /// Epoch of the last consensus output that was processed.
    fn latest_consensus_epoch(&self) -> Epoch;

    /// Read the last committed round per authority for `epoch`.
    fn read_last_committed(
        &self,
        epoch: Epoch,
    ) -> impl Future<Output = HashMap<AuthorityIdentifier, Round>> + Send;

    /// Read the final committed sub dag of `epoch` together with its
    /// finalised reputation scores.
    fn read_latest_commit_with_final_reputation_scores(
        &self,
        epoch: Epoch,
    ) -> impl Future<Output = Option<Arc<CommittedSubDag>>> + Send;

    /// Load a consensus output from the current epoch by number.
    fn get_consensus_output_current(
        &self,
        number: u64,
    ) -> impl Future<Output = eyre::Result<ConsensusOutput>> + Send;

    /// Return true if the pack file for `epoch_record` is complete.
    fn is_epoch_complete(&self, epoch_record: &EpochRecord) -> impl Future<Output = bool> + Send;

    /// Return true if the current epoch pack contains the batch for `digest`.
    fn contains_current_batch(&self, digest: BlockHash) -> impl Future<Output = bool> + Send;

    /// Return the batches in `epoch` matching any of the provided `digests`,
    /// in the order they are found.
    fn get_batches<'a>(
        &'a self,
        epoch: Epoch,
        digests: impl Iterator<Item = &'a BlockHash> + Send + 'a,
    ) -> impl Future<Output = Vec<Batch>> + Send + 'a;

    /// Count leaders in the current pack's rewards counter that are below
    /// `last_executed_round`.
    fn count_leaders(
        &self,
        last_executed_round: Round,
        rewards_counter: RewardsCounter,
    ) -> impl Future<Output = eyre::Result<()>> + Send;

    /// Open a verified read stream over the data file for `epoch`.
    fn get_epoch_stream(
        &self,
        epoch: Epoch,
    ) -> impl Future<Output = eyre::Result<Box<dyn ReadStream>>> + Send;

    /// Return true if this process is already streaming `epoch`.
    fn already_streaming_epoch(&self, epoch: Epoch) -> bool;
}

/// Read + write async view of the consensus chain.
///
/// Extends [`ConsensusChainReader`] with the mutating operations that advance
/// the chain, rotate the current epoch pack, or absorb a streamed epoch from
/// a peer.
pub trait ConsensusChainWriter: ConsensusChainReader {
    /// Append a [`ConsensusOutput`] to the current epoch's pack file and
    /// advance the in-process latest-consensus cursor.
    fn save_consensus_output(
        &self,
        consensus: ConsensusOutput,
    ) -> impl Future<Output = eyre::Result<()>> + Send;

    /// Rotate the current pack file to a new epoch.  The previous pack is
    /// persisted and moved into the recent-packs cache before the new pack
    /// is opened.
    fn new_epoch(
        &self,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> impl Future<Output = eyre::Result<()>> + Send;

    /// Import an epoch's pack file from a peer stream.  Returns once the
    /// stream is fully consumed and the resulting pack file has been moved
    /// into the canonical epoch directory.
    fn stream_import<R: AsyncRead + Unpin + Send>(
        &self,
        stream: R,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        timeout: Duration,
    ) -> impl Future<Output = eyre::Result<()>> + Send;

    /// Resolve once the current epoch is fully persisted to storage.
    fn persist_current(&self) -> impl Future<Output = eyre::Result<()>> + Send;
}

/// Composite handle combining the table-KV [`Database`] surface and the
/// consensus-chain log surface.
///
/// Acts as a blanket marker: any type that implements both [`Database`] and
/// [`ConsensusChainWriter`] is automatically a `UnifiedStore`.  Call sites
/// that need both layers can take `U: UnifiedStore` as a single bound
/// instead of carrying two type parameters.
pub trait UnifiedStore: Database + ConsensusChainWriter {}

impl<T> UnifiedStore for T where T: Database + ConsensusChainWriter {}
