#![deny(missing_docs)]

//! Module to maintain an index of digests for a pack file.

pub mod bloom;
pub mod bucket_iter;
pub mod index;
/// On-demand benchmark: direct-IO (`HdxIndex`) vs mmap (`HdxIndexMmap`) index (see
/// `index_bench.rs`).
#[cfg(test)]
mod index_bench;
pub mod index_mmap;
pub mod odx_header;

use std::{
    hash::{BuildHasher, BuildHasherDefault},
    path::Path,
};

use tn_types::B256;

use crate::archive::{
    error::{
        commit::CommitError, fetch::FetchError, insert::AppendError, load_header::LoadHeaderError,
    },
    fxhasher::FxHasher,
    index::Index,
    pack::{DataHeader, FileBackend},
};

use self::{index::HdxIndex, index_mmap::HdxIndexMmap};

/// A digest index (256-bit digest -> u64 record position) whose on-disk file backend is chosen at
/// open time.
///
/// Both variants share the exact same on-disk format, so a directory written by one opens with the
/// other; this enum just lets a caller hold either behind one type, driven by the pack's
/// [`FileBackend`]:
/// - [`Buffered`](Self::Buffered): [`HdxIndex`] on a raw random-access `File`, with an in-memory
///   bucket cache. The default, and the long-standing production path.
/// - [`Mmap`](Self::Mmap): the cache-free, memory-mapped [`HdxIndexMmap`], which reads and writes
///   hash buckets directly through the mapping.
///
/// Dispatch is a single `match` (no vtable), so the hot [`Index::load`] path pays no dynamic-call
/// cost over using either type directly.
#[derive(Debug)]
pub enum DigestIndex<
    const KSIZE: usize = 32,
    S: BuildHasher + Default = BuildHasherDefault<FxHasher>,
> {
    /// Buffered backend (the default): [`HdxIndex`] on a raw random-access `File`.
    Buffered(HdxIndex<KSIZE, S>),
    /// Memory-mapped, cache-free backend: [`HdxIndexMmap`].
    Mmap(HdxIndexMmap<KSIZE, S>),
}

impl<const KSIZE: usize, S: BuildHasher + Default> DigestIndex<KSIZE, S> {
    /// Open (creating if empty) the digest index in directory `dir`, selecting the file `backend`.
    ///
    /// You MUST supply a stable hasher (e.g. fxhasher); the default Rust hasher is not stable
    /// across instances and would invalidate the index.
    pub fn open_hdx_file<P: AsRef<Path>>(
        dir: P,
        data_header: &DataHeader,
        hasher_builder: S,
        read_only: bool,
        backend: FileBackend,
    ) -> Result<Self, LoadHeaderError> {
        match backend {
            FileBackend::Buffered => Ok(Self::Buffered(HdxIndex::open_hdx_file_with_backend(
                dir,
                data_header,
                hasher_builder,
                read_only,
                FileBackend::Buffered,
            )?)),
            FileBackend::Mmap => Ok(Self::Mmap(HdxIndexMmap::open_hdx_file(
                dir,
                data_header,
                hasher_builder,
                read_only,
            )?)),
        }
    }

    /// Number of keys hashed in this index.
    pub fn len(&self) -> usize {
        match self {
            Self::Buffered(i) => i.len(),
            Self::Mmap(i) => i.len(),
        }
    }

    /// True if there are no keys stored in this index.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Buffered(i) => i.is_empty(),
            Self::Mmap(i) => i.is_empty(),
        }
    }

    /// Set the tracked data-file length (metadata about the paired pack data file; does not affect
    /// the index itself).
    pub fn set_data_file_length(&mut self, data_file_length: u64) {
        match self {
            Self::Buffered(i) => i.set_data_file_length(data_file_length),
            Self::Mmap(i) => i.set_data_file_length(data_file_length),
        }
    }

    /// Get the tracked data-file length.
    pub fn data_file_length(&self) -> u64 {
        match self {
            Self::Buffered(i) => i.data_file_length(),
            Self::Mmap(i) => i.data_file_length(),
        }
    }
}

impl<const KSIZE: usize, S: BuildHasher + Default> Index<B256, u64> for DigestIndex<KSIZE, S> {
    fn save(&mut self, key: B256, record_pos: u64) -> Result<(), AppendError> {
        match self {
            Self::Buffered(i) => i.save(key, record_pos),
            Self::Mmap(i) => i.save(key, record_pos),
        }
    }

    fn load(&mut self, key: B256) -> Result<u64, FetchError> {
        match self {
            Self::Buffered(i) => i.load(key),
            Self::Mmap(i) => i.load(key),
        }
    }

    fn sync(&mut self) -> Result<(), CommitError> {
        match self {
            Self::Buffered(i) => i.sync(),
            Self::Mmap(i) => i.sync(),
        }
    }
}
