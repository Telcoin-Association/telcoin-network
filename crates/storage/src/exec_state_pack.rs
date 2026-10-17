//! Pack file capturing a complete EVM execution-state snapshot: the authoritative
//! state root, one or more block headers (the snapshot header first, then recent
//! ancestors), and the full account / storage / bytecode set.
//!
//! This is the *storage format* only. It is a one-shot, write-once artifact used to
//! export execution state from reth and later reimport it so a node can start without
//! replaying from genesis. Because it is written once in a batch — not concurrently
//! during live consensus like [`crate::consensus_pack`] / [`crate::certificate_pack`]
//! — the API here is plain synchronous, with no background writer thread.
//!
//! Record stream layout (insert order, enforced on read):
//!
//! ```text
//! Meta -> header_count x Header -> N x (Account -> Storage*) -> End
//! ```
//!
//! An account's storage is split into bounded `Storage` chunk records that follow its `Account`
//! header (sentinel-terminated by the next `Account`/`End`), so an account with arbitrarily large
//! storage never exceeds the container's per-record limit and can be read one chunk at a time.
//!
//! ## Encoding
//!
//! Records go through the container's BCS codec, which is *not* self-describing. Alloy's
//! `Header` and `GenesisAccount` cannot survive it — their serde derives use
//! `skip_serializing_if` and `alloy_serde::quantity`, which only round-trip through
//! self-describing formats. So headers are stored in their canonical **RLP** form and
//! accounts in a small primitive wire struct (`AccountRecord`); the public API still
//! speaks in [`ExecHeader`] / [`GenesisAccount`], converting at the boundary.
//!
//! ## Verification scope
//!
//! [`ExecStatePackReader::verify`] checks only *structural / self-consistency*
//! invariants (meta first, version, header/account counts, meta<->header agreement,
//! trailing footer present, per-record CRC32). Cryptographic verification that the
//! account set actually hashes to `state_root` requires reth's trie machinery and is
//! performed at import time, not here — `tn-storage` deliberately takes no reth/trie
//! dependency.

use std::{collections::BTreeMap, error::Error, fmt, io, path::Path};

use alloy_rlp::{Decodable, Encodable};
use serde::{Deserialize, Serialize};
use tn_types::{Address, Bytes, ExecHeader, GenesisAccount, B256, U256};

use crate::{
    archive::{
        error::{fetch::FetchError, load_header::LoadHeaderError, open::OpenError},
        pack::{Pack, PackCompression, RawIter},
    },
    consensus_pack::PACK_VERSION,
};

/// Schema version stamped into the pack file's `DataHeader` (via [`Pack::open`]) and
/// verified when the pack is reopened. Bump it when the record layout changes.
///
/// This is the pack's own version field — there is no separate version inside
/// [`ExecStateMeta`]. It must stay `<= PACK_VERSION`, the global container-framing
/// version every read is gated on (guaranteed by the assertion below).
pub const EXEC_STATE_PACK_VERSION: u16 = 1;

const _: () = assert!(
    EXEC_STATE_PACK_VERSION <= PACK_VERSION,
    "EXEC_STATE_PACK_VERSION must not exceed the container framing version PACK_VERSION"
);

/// Name of the data file inside the pack directory.
const DATA_NAME: &str = "state_data";

/// Maximum storage slots per `Storage` chunk record. Each slot is 64 bytes, so 64k
/// slots is ~4 MiB decompressed — comfortably under the container's per-record limit
/// (`MAX_RECORD_SIZE` = 16 MiB, `crate::archive::pack_iter`), leaving margin for BCS overhead.
///
/// Public so streaming exporters can flush at exactly this bound — reproducing the chunk layout
/// [`ExecStatePackWriter::append_account`] emits, byte for byte — and so the per-account
/// working-set ceiling such an exporter must hold in memory is stated in one place.
///
/// The bound is enforced on both ends: the writer rejects a longer chunk at
/// [`ExecStatePackWriter::append_storage_chunk_owned`], and the reader rejects one on every read
/// path (as [`ExecStatePackError::OversizedStorageChunk`]) before any consumer sizes an
/// allocation to it, so a corrupt or hand-crafted pack cannot push chunks past the documented
/// working-set ceiling through a restore.
pub const STORAGE_CHUNK_SLOTS: usize = 64 * 1024;

/// Depth of the EVM `BLOCKHASH` lookback window: the opcode resolves the 256 most recent block
/// hashes. The export side embeds this many real ancestor headers below the snapshot header
/// (`gather_headers` in `tn-node`), and the restore side refuses a pack (and a scaffold window,
/// `SnapshotRestorer::import_chain_scaffold` in `tn-reth`) that does not cover this lookback
/// below its tip. One shared constant keeps the export bound and the restore floor from drifting
/// apart (issue #1174).
pub const BLOCKHASH_ANCESTORS: u64 = 256;

/// Upper bound on the header count a pack may carry: the snapshot header plus its
/// [`BLOCKHASH_ANCESTORS`] (256) ancestors, the most the exporter embeds (`gather_headers` in
/// `tn-node`). Enforced at export ([`ExecStatePackWriter::create`]) and, on read, before any
/// allocation sized by the untrusted `header_count` (a crafted/corrupt count would otherwise
/// request a multi-TB `Vec::with_capacity` in [`ExecStatePackReader::open`] before any
/// per-record CRC/size guard runs).
const MAX_HEADER_COUNT: u32 = BLOCKHASH_ANCESTORS as u32 + 1;

/// Upper bound on one `Header` record's RLP length, enforced at export and on read before the
/// record is decoded. A real header is under 1 KiB (`extra_data` is empty or 32 bytes), but a
/// record may decompress to the container's 16 MiB limit and RLP has no size cap, while
/// [`ExecStatePackReader::open`] keeps every header it decodes: without this bound a few-MB
/// crafted pack could make `open` hold `MAX_HEADER_COUNT` x 16 MiB.
const MAX_HEADER_RECORD_BYTES: usize = 64 * 1024;

/// First record in the pack. Minimal and fixed: it carries the authoritative state
/// root and describes the shape of the stream that follows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecStateMeta {
    /// The EVM state root this snapshot represents. Equal to the snapshot block
    /// header's `state_root`; the import step must reproduce this value from the
    /// account set.
    pub state_root: B256,
    /// Block number of the snapshot (the first / canonical header).
    pub block_number: u64,
    /// Block hash of the snapshot (the first / canonical header).
    pub block_hash: B256,
    /// Number of header records that immediately follow (>= 1). [`ExecStatePackReader::open`]
    /// also holds it to the `BLOCKHASH` lookback floor: at least
    /// `min(block_number, BLOCKHASH_ANCESTORS)` headers.
    pub header_count: u32,
}

/// A single account plus its storage and code, mirroring the genesis-account shape so
/// the import side can feed reth's genesis machinery directly. This is the public
/// account type; on disk it is stored as a BCS-safe `AccountRecord` header followed by
/// `ExecStateRecord::Storage` chunks, so storage of any size round-trips.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecStateAccount {
    /// Account address.
    pub address: Address,
    /// Account state: nonce, balance, code, and storage.
    pub account: GenesisAccount,
}

/// Trailing summary record. Lets a reader detect truncation and obtain tallies without
/// a side channel. Written by [`ExecStatePackWriter::finish`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecStateStats {
    /// Number of account records written.
    pub account_count: u64,
    /// Total non-zero storage slots across all accounts.
    pub storage_slots: u64,
    /// Number of accounts carrying contract code.
    pub bytecodes: u64,
}

/// On-disk wire form of an account *header* — only primitive types that round-trip through the
/// container's BCS codec. Balance is the account's [`U256`] as 32 big-endian bytes. Storage is NOT
/// inline: it follows as zero or more `ExecStateRecord::Storage` chunks, so an account with
/// arbitrarily large storage never exceeds the container's per-record limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AccountRecord {
    address: Address,
    nonce: u64,
    balance: B256,
    code: Option<Bytes>,
}

impl AccountRecord {
    /// Rebuild a full account from this header plus its collected storage slots.
    fn into_account_with_storage(self, storage: Vec<(B256, B256)>) -> ExecStateAccount {
        // Empty storage collapses back to `None`, mirroring how the exporter emits it.
        let storage =
            (!storage.is_empty()).then(|| storage.into_iter().collect::<BTreeMap<_, _>>());
        ExecStateAccount {
            address: self.address,
            account: GenesisAccount {
                nonce: Some(self.nonce),
                balance: U256::from_be_bytes(self.balance.0),
                code: self.code,
                storage,
                private_key: None,
            },
        }
    }

    /// The account header as public metadata (no storage).
    fn into_meta(self) -> ExecStateAccountMeta {
        ExecStateAccountMeta {
            address: self.address,
            nonce: self.nonce,
            balance: U256::from_be_bytes(self.balance.0),
            code: self.code,
        }
    }
}

/// An account header without storage, yielded by the chunked read API
/// ([`ExecStatePackReader::next_entry`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecStateAccountMeta {
    /// Account address.
    pub address: Address,
    /// Account nonce.
    pub nonce: u64,
    /// Account balance.
    pub balance: U256,
    /// Contract code, if any.
    pub code: Option<Bytes>,
}

/// One item of the chunked read stream ([`ExecStatePackReader::next_entry`]): an account header, or
/// a bounded chunk of the most-recently-yielded account's storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateEntry {
    /// The start of an account.
    Account(ExecStateAccountMeta),
    /// A chunk of storage slots belonging to the current account.
    Storage(Vec<(B256, B256)>),
}

/// The record types stored, in order, in an exec-state pack.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ExecStateRecord {
    /// Snapshot metadata; always the first record.
    Meta(ExecStateMeta),
    /// An RLP-encoded [`ExecHeader`] (canonical, lossless form). Encoded as one byte string
    /// (byte-identical to a sequence of `u8`), not byte by byte.
    Header(#[serde(with = "tn_types::byte_vec")] Vec<u8>),
    /// An account header (address/nonce/balance/code); storage follows as `Storage` chunks.
    Account(AccountRecord),
    /// A chunk of the preceding account's storage slots (bounded by [`STORAGE_CHUNK_SLOTS`]).
    Storage(Vec<(B256, B256)>),
    /// Trailing summary; always the last record.
    End(ExecStateStats),
}

/// RLP-encode a header for storage.
fn encode_header(header: &ExecHeader) -> Vec<u8> {
    let mut buf = Vec::new();
    header.encode(&mut buf);
    buf
}

/// Enforce [`MAX_HEADER_RECORD_BYTES`] on one header record's RLP length. Checked at export and
/// on read before [`decode_header`], so a crafted pack cannot drive an oversized decode.
fn check_header_bound(len: usize) -> Result<(), ExecStatePackError> {
    (len <= MAX_HEADER_RECORD_BYTES)
        .then_some(())
        .ok_or(ExecStatePackError::OversizedHeader { len, max: MAX_HEADER_RECORD_BYTES })
}

/// Decode a header from its stored RLP bytes.
fn decode_header(bytes: &[u8]) -> Result<ExecHeader, ExecStatePackError> {
    ExecHeader::decode(&mut &bytes[..]).map_err(|e| ExecStatePackError::HeaderRlp(e.to_string()))
}

/// Enforce the documented [`STORAGE_CHUNK_SLOTS`] bound on one storage chunk's slot count.
///
/// A conforming writer keeps the bound by construction ([`ExecStatePackWriter::append_account`]
/// splits at exactly this size), so a longer chunk can only come from a non-conforming writer or
/// a corrupt / hand-crafted pack. Checked at both ends: on append (so a buggy exporter fails at
/// write time, not at restore time) and on every record pulled by the reader (so the read side
/// never trusts the writer-side convention).
fn check_chunk_bound(len: usize) -> Result<(), ExecStatePackError> {
    (len <= STORAGE_CHUNK_SLOTS)
        .then_some(())
        .ok_or(ExecStatePackError::OversizedStorageChunk { len, max: STORAGE_CHUNK_SLOTS })
}

/// Summary returned by a successful [`ExecStatePackReader::verify`] pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifyReport {
    /// Number of header records in the pack.
    pub header_count: u32,
    /// Number of account records observed.
    pub account_count: u64,
    /// Total non-zero storage slots observed.
    pub storage_slots: u64,
    /// Number of accounts carrying code.
    pub bytecodes: u64,
}

/// Write-once builder for an exec-state pack.
///
/// Dropped before [`finish`](Self::finish) succeeds (an export that failed or was cancelled
/// mid-way), it removes its data file instead of sealing it, so a torn export never looks like a
/// cleanly closed pack and a retry can `create` afresh.
#[derive(Debug)]
pub struct ExecStatePackWriter {
    data: Pack<ExecStateRecord>,
    stats: ExecStateStats,
    /// Set once `finish` has committed the footer; until then `Drop` removes the data file.
    finished: bool,
}

impl Drop for ExecStatePackWriter {
    fn drop(&mut self) {
        if !self.finished {
            self.data.set_remove_on_drop();
        }
    }
}

impl ExecStatePackWriter {
    /// Create a new pack in directory `path` (created if missing) and write the
    /// [`ExecStateMeta`] followed by `headers`. The snapshot header must be first and
    /// its `state_root` must equal `state_root` (the invariant the reader re-checks).
    ///
    /// `headers` must also fit the reader's bounds (at most the snapshot header plus
    /// [`BLOCKHASH_ANCESTORS`] ancestors, each a plausibly sized header), so an export the reader
    /// would refuse fails here rather than at import.
    pub fn create<P: AsRef<Path>>(
        path: P,
        state_root: B256,
        headers: &[ExecHeader],
    ) -> Result<Self, ExecStatePackError> {
        let snapshot = headers.first().ok_or(ExecStatePackError::MissingHeaders)?;
        if snapshot.state_root != state_root {
            return Err(ExecStatePackError::StateRootMismatch);
        }
        if headers.len() > MAX_HEADER_COUNT as usize {
            return Err(ExecStatePackError::TooManyHeaders {
                declared: u32::try_from(headers.len()).unwrap_or(u32::MAX),
                max: MAX_HEADER_COUNT,
            });
        }
        let encoded: Vec<Vec<u8>> = headers.iter().map(encode_header).collect();
        encoded.iter().try_for_each(|rlp| check_header_bound(rlp.len()))?;

        let base = path.as_ref();
        std::fs::create_dir_all(base)?;
        let data_path = base.join(DATA_NAME);
        // `Pack::open` appends to an existing data file, so creating over a leftover pack (from an
        // earlier or interrupted export) would publish a doubled bundle. Refuse it; an empty file
        // (a crash before the first write) holds nothing and is reused.
        if std::fs::metadata(&data_path).is_ok_and(|meta| meta.len() > 0) {
            return Err(ExecStatePackError::Io(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("exec state pack already exists at {}", data_path.display()),
            )));
        }
        let data: Pack<ExecStateRecord> =
            Pack::open(&data_path, 0, false, PackCompression::ZStd, EXEC_STATE_PACK_VERSION)?;
        // Own the pack from here, so a failed append below removes the file (see `Drop`).
        let mut writer = Self { data, stats: ExecStateStats::default(), finished: false };

        let meta = ExecStateMeta {
            state_root,
            block_number: snapshot.number,
            block_hash: snapshot.hash_slow(),
            header_count: headers.len() as u32,
        };
        Self::append(&mut writer.data, &ExecStateRecord::Meta(meta))?;
        for rlp in encoded {
            Self::append(&mut writer.data, &ExecStateRecord::Header(rlp))?;
        }

        Ok(writer)
    }

    /// Append one account (header + storage chunks) to the pack, updating the running tallies.
    ///
    /// The account's storage is split into [`STORAGE_CHUNK_SLOTS`]-sized
    /// `ExecStateRecord::Storage` records so an account with arbitrarily large storage always
    /// stays under the container's per-record limit. Callers that must also bound *write*
    /// memory can drive [`append_account_header`](Self::append_account_header) +
    /// [`append_storage_chunk`](Self::append_storage_chunk) directly.
    pub fn append_account(&mut self, account: &ExecStateAccount) -> Result<(), ExecStatePackError> {
        self.append_account_header(
            account.address,
            account.account.nonce.unwrap_or_default(),
            account.account.balance,
            account.account.code.clone(),
        )?;
        if let Some(storage) = &account.account.storage {
            // BTreeMap iterates in ascending key order — a stable, deterministic chunk order. Build
            // each chunk straight from the iterator and move it into its record, so an account's
            // storage is never copied into a full intermediate Vec and then again per chunk.
            let mut chunk: Vec<(B256, B256)> =
                Vec::with_capacity(STORAGE_CHUNK_SLOTS.min(storage.len()));
            for (k, v) in storage.iter() {
                chunk.push((*k, *v));
                if chunk.len() == STORAGE_CHUNK_SLOTS {
                    self.append_storage_chunk_owned(std::mem::replace(
                        &mut chunk,
                        Vec::with_capacity(STORAGE_CHUNK_SLOTS),
                    ))?;
                }
            }
            self.append_storage_chunk_owned(chunk)?;
        }
        Ok(())
    }

    /// Append an account header (no storage) and start its storage run. Follow with zero or more
    /// [`append_storage_chunk`](Self::append_storage_chunk) calls, then the next account or
    /// [`finish`](Self::finish). This is the streaming path — the caller never needs to hold an
    /// account's full storage in memory.
    pub fn append_account_header(
        &mut self,
        address: Address,
        nonce: u64,
        balance: U256,
        code: Option<Bytes>,
    ) -> Result<(), ExecStatePackError> {
        self.stats.account_count += 1;
        if code.as_ref().is_some_and(|code| !code.is_empty()) {
            self.stats.bytecodes += 1;
        }
        let record = AccountRecord {
            address,
            nonce,
            balance: B256::from(balance.to_be_bytes::<32>()),
            code,
        };
        Self::append(&mut self.data, &ExecStateRecord::Account(record))
    }

    /// Append one chunk of storage slots for the current account. Empty chunks are ignored. A chunk
    /// must hold at most [`STORAGE_CHUNK_SLOTS`] slots to stay under the container's record
    /// limit; a longer chunk is rejected as [`ExecStatePackError::OversizedStorageChunk`].
    pub fn append_storage_chunk(
        &mut self,
        slots: &[(B256, B256)],
    ) -> Result<(), ExecStatePackError> {
        self.append_storage_chunk_owned(slots.to_vec())
    }

    /// Append one owned chunk of storage slots for the current account, moving it into its record
    /// (no extra copy) and bumping the tally. Empty chunks are ignored. A chunk must hold at
    /// most [`STORAGE_CHUNK_SLOTS`] slots to stay under the container's record limit (a longer
    /// chunk is rejected as [`ExecStatePackError::OversizedStorageChunk`]); streaming
    /// callers that flush at exactly that bound reproduce
    /// [`append_account`](Self::append_account)'s chunk layout byte for byte.
    pub fn append_storage_chunk_owned(
        &mut self,
        slots: Vec<(B256, B256)>,
    ) -> Result<(), ExecStatePackError> {
        if slots.is_empty() {
            return Ok(());
        }
        check_chunk_bound(slots.len())?;
        self.stats.storage_slots += slots.len() as u64;
        Self::append(&mut self.data, &ExecStateRecord::Storage(slots))
    }

    /// Write the trailing [`ExecStateStats`] footer, commit the pack to disk, and
    /// return the tallies.
    pub fn finish(mut self) -> Result<ExecStateStats, ExecStatePackError> {
        Self::append(&mut self.data, &ExecStateRecord::End(self.stats))?;
        self.data.commit().map_err(|e| ExecStatePackError::Persist(e.to_string()))?;
        // The write-once export is complete and committed; mark it consistent so the clean `Drop`
        // seals it. `create` only ever starts from an absent or empty data file, which opens
        // clean, so this is defensive. A failed append or commit returns early above, before
        // `finished` is set, so the writer's `Drop` removes the file: a torn export is never
        // sealed.
        self.data.mark_consistent();
        self.finished = true;
        Ok(self.stats)
    }

    fn append(
        data: &mut Pack<ExecStateRecord>,
        record: &ExecStateRecord,
    ) -> Result<(), ExecStatePackError> {
        data.append(record).map(|_| ()).map_err(|e| ExecStatePackError::Append(e.to_string()))
    }
}

/// Read-only view over an exec-state pack.
///
/// [`open`](Self::open) eagerly reads the meta and every header; accounts are then
/// streamed lazily via [`accounts`](Self::accounts) / [`next_account`](Self::next_account)
/// so that a snapshot with millions of accounts is never materialized in memory.
#[derive(Debug)]
pub struct ExecStatePackReader {
    meta: ExecStateMeta,
    headers: Vec<ExecHeader>,
    iter: RawIter<ExecStateRecord>,
    /// One-record lookahead: the record that terminated the previous account's storage run.
    pending: Option<ExecStateRecord>,
    done: bool,
}

impl ExecStatePackReader {
    /// Open the pack in directory `path` read-only and read its meta + headers.
    ///
    /// Errors if the first record is not the meta, the schema version is unexpected, the
    /// declared headers are missing/short, or the declared header count does not cover the
    /// `BLOCKHASH` lookback floor for the pack's tip block (see [`BLOCKHASH_ANCESTORS`]).
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, ExecStatePackError> {
        let base = path.as_ref();
        let data: Pack<ExecStateRecord> = Pack::open(
            base.join(DATA_NAME),
            0,
            true,
            PackCompression::ZStd,
            EXEC_STATE_PACK_VERSION,
        )?;
        // `Pack::open` verifies the file-header version against `EXEC_STATE_PACK_VERSION`,
        // rejecting any pack written by a newer build.
        let mut iter = data.raw_iter()?;

        let meta = match iter.next() {
            Some(Ok(ExecStateRecord::Meta(meta))) => meta,
            Some(Ok(_)) | None => return Err(ExecStatePackError::MetaNotFirst),
            Some(Err(e)) => return Err(e.into()),
        };
        if meta.header_count == 0 {
            return Err(ExecStatePackError::MissingHeaders);
        }
        // Reject an implausible count BEFORE the capacity reservation below: `header_count` is read
        // straight from the (untrusted) meta, and a crafted value would otherwise make
        // `with_capacity` request a huge allocation before any per-record guard runs.
        if meta.header_count > MAX_HEADER_COUNT {
            return Err(ExecStatePackError::TooManyHeaders {
                declared: meta.header_count,
                max: MAX_HEADER_COUNT,
            });
        }
        // Defense-in-depth floor (issue #1174): a genuine pack covers the full `BLOCKHASH`
        // lookback below its tip (or reaches block 1), so a shorter pack is truncated or
        // corrupt. Refusing it here keeps the restore path from ever scaffolding zero-hash
        // ancestors inside the lookback; `import_chain_scaffold` re-checks the same floor on
        // the assembled window.
        let floor = meta.block_number.min(BLOCKHASH_ANCESTORS);
        (u64::from(meta.header_count) >= floor).then_some(()).ok_or(
            ExecStatePackError::InsufficientHeaders {
                declared: meta.header_count,
                floor,
                block: meta.block_number,
            },
        )?;

        let mut headers = Vec::with_capacity(meta.header_count as usize);
        for _ in 0..meta.header_count {
            match iter.next() {
                Some(Ok(ExecStateRecord::Header(bytes))) => {
                    check_header_bound(bytes.len())?;
                    headers.push(decode_header(&bytes)?);
                }
                Some(Ok(_)) => return Err(ExecStatePackError::CorruptPack),
                Some(Err(e)) => return Err(e.into()),
                None => return Err(ExecStatePackError::MissingHeaders),
            }
        }

        Ok(Self { meta, headers, iter, pending: None, done: false })
    }

    /// The snapshot metadata.
    pub fn meta(&self) -> &ExecStateMeta {
        &self.meta
    }

    /// All embedded block headers, snapshot header first.
    pub fn headers(&self) -> &[ExecHeader] {
        &self.headers
    }

    /// The snapshot (canonical) block header — the one whose `state_root` equals
    /// [`ExecStateMeta::state_root`].
    pub fn snapshot_header(&self) -> &ExecHeader {
        // `open` guarantees `header_count >= 1`, so this never panics.
        &self.headers[0]
    }

    /// Pull the next record, honoring the one-record lookahead buffer.
    ///
    /// Every record enters the reader through here (the lookahead buffer only ever re-yields a
    /// record this method already vetted), so this is also where the read path re-checks the
    /// writer-side [`STORAGE_CHUNK_SLOTS`] bound: an oversized `Storage` record is rejected as
    /// [`ExecStatePackError::OversizedStorageChunk`] before any consumer sees it.
    fn pull(&mut self) -> Option<Result<ExecStateRecord, ExecStatePackError>> {
        if let Some(record) = self.pending.take() {
            return Some(Ok(record));
        }
        match self.iter.next() {
            Some(Ok(ExecStateRecord::Storage(chunk))) => {
                Some(check_chunk_bound(chunk.len()).map(|()| ExecStateRecord::Storage(chunk)))
            }
            Some(Ok(record)) => Some(Ok(record)),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    /// Pull the next account — its header plus all of its storage chunks — from the stream, or
    /// `None` once the trailing footer is reached. A truncated pack yields
    /// `Some(Err(MissingFooter))`.
    ///
    /// This reassembles the account's full storage in memory; consumers that must bound memory for
    /// pathologically large accounts should use [`next_entry`](Self::next_entry) instead.
    pub fn next_account(&mut self) -> Option<Result<ExecStateAccount, ExecStatePackError>> {
        if self.done {
            return None;
        }
        let header = match self.pull() {
            Some(Ok(ExecStateRecord::Account(record))) => record,
            Some(Ok(ExecStateRecord::End(_))) => {
                self.done = true;
                return None;
            }
            Some(Ok(_)) => {
                self.done = true;
                return Some(Err(ExecStatePackError::CorruptPack));
            }
            Some(Err(e)) => {
                self.done = true;
                return Some(Err(e));
            }
            None => {
                self.done = true;
                return Some(Err(ExecStatePackError::MissingFooter));
            }
        };

        // Gather this account's storage chunks up to the next non-Storage record, which is buffered
        // for the following call.
        let mut storage: Vec<(B256, B256)> = Vec::new();
        loop {
            match self.pull() {
                Some(Ok(ExecStateRecord::Storage(chunk))) => storage.extend(chunk),
                Some(Ok(other)) => {
                    self.pending = Some(other);
                    break;
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Some(Err(e));
                }
                None => {
                    self.done = true;
                    return Some(Err(ExecStatePackError::MissingFooter));
                }
            }
        }
        Some(Ok(header.into_account_with_storage(storage)))
    }

    /// Pull the next entry of the chunked read stream: an account header, or a bounded chunk of the
    /// current account's storage. Returns `None` at the trailing footer. Unlike
    /// [`next_account`](Self::next_account) this never materializes a whole account's storage — a
    /// consumer processes one [`STORAGE_CHUNK_SLOTS`]-bounded chunk at a time.
    pub fn next_entry(&mut self) -> Option<Result<StateEntry, ExecStatePackError>> {
        if self.done {
            return None;
        }
        match self.pull() {
            Some(Ok(ExecStateRecord::Account(record))) => {
                Some(Ok(StateEntry::Account(record.into_meta())))
            }
            Some(Ok(ExecStateRecord::Storage(chunk))) => Some(Ok(StateEntry::Storage(chunk))),
            Some(Ok(ExecStateRecord::End(_))) => {
                self.done = true;
                None
            }
            Some(Ok(_)) => {
                self.done = true;
                Some(Err(ExecStatePackError::CorruptPack))
            }
            Some(Err(e)) => {
                self.done = true;
                Some(Err(e))
            }
            None => {
                self.done = true;
                Some(Err(ExecStatePackError::MissingFooter))
            }
        }
    }

    /// Stream the remaining account records. Stops cleanly at the trailing footer.
    pub fn accounts(
        &mut self,
    ) -> impl Iterator<Item = Result<ExecStateAccount, ExecStatePackError>> + '_ {
        std::iter::from_fn(move || self.next_account())
    }

    /// Run a full structural / self-consistency pass over the pack at `path`.
    ///
    /// This opens its own reader (it does not disturb any reader you are streaming
    /// from) and checks: meta first + version, `header_count >= 1` plus the `BLOCKHASH`
    /// lookback floor (see [`BLOCKHASH_ANCESTORS`]), snapshot header's
    /// `state_root`/number/hash agree with the meta, every storage chunk is within the
    /// [`STORAGE_CHUNK_SLOTS`] bound, a trailing footer is present, and its declared
    /// `account_count` matches the records actually read. Per-record CRC32 is enforced
    /// by the container during the walk.
    ///
    /// Note: this does *not* recompute the Merkle state root from the accounts — see
    /// the module docs.
    pub fn verify<P: AsRef<Path>>(path: P) -> Result<VerifyReport, ExecStatePackError> {
        let mut reader = Self::open(path)?;

        {
            let snapshot = &reader.headers[0];
            if snapshot.state_root != reader.meta.state_root {
                return Err(ExecStatePackError::StateRootMismatch);
            }
            if snapshot.number != reader.meta.block_number
                || snapshot.hash_slow() != reader.meta.block_hash
            {
                return Err(ExecStatePackError::BlockIdentityMismatch);
            }
        }

        let mut counted = ExecStateStats::default();
        let mut saw_account = false;
        // Walk through `pull` (not the raw container iterator) so the walk enforces the same
        // per-chunk [`STORAGE_CHUNK_SLOTS`] bound the streaming read paths do: a pack `verify`
        // accepts is a pack the import side will not reject on structural grounds.
        let footer = loop {
            match reader.pull() {
                Some(Ok(ExecStateRecord::Account(record))) => {
                    counted.account_count += 1;
                    if record.code.as_ref().is_some_and(|code| !code.is_empty()) {
                        counted.bytecodes += 1;
                    }
                    saw_account = true;
                }
                Some(Ok(ExecStateRecord::Storage(chunk))) => {
                    // Storage may only follow an account.
                    if !saw_account {
                        return Err(ExecStatePackError::CorruptPack);
                    }
                    counted.storage_slots += chunk.len() as u64;
                }
                Some(Ok(ExecStateRecord::End(stats))) => break stats,
                Some(Ok(_)) => return Err(ExecStatePackError::CorruptPack),
                Some(Err(e)) => return Err(e),
                None => return Err(ExecStatePackError::MissingFooter),
            }
        };

        if footer.account_count != counted.account_count {
            return Err(ExecStatePackError::AccountCountMismatch {
                expected: footer.account_count,
                got: counted.account_count,
            });
        }

        Ok(VerifyReport {
            header_count: reader.meta.header_count,
            account_count: counted.account_count,
            storage_slots: counted.storage_slots,
            bytecodes: counted.bytecodes,
        })
    }
}

/// Errors produced when reading or writing an exec-state pack.
#[derive(Debug)]
pub enum ExecStatePackError {
    /// An underlying IO error.
    Io(io::Error),
    /// Failed to open the pack data file.
    Open(OpenError),
    /// Failed to open the raw record iterator (bad/foreign file header).
    LoadHeader(LoadHeaderError),
    /// Failed to read/decode a record (includes CRC32 failures).
    Read(FetchError),
    /// Failed to RLP-decode a stored block header.
    HeaderRlp(String),
    /// Failed to append a record.
    Append(String),
    /// Failed to commit the pack to disk.
    Persist(String),
    /// A record was structurally out of place.
    CorruptPack,
    /// The first record was not the meta record.
    MetaNotFirst,
    /// The pack declared zero headers, or ended before all declared headers were read.
    MissingHeaders,
    /// The pack declared more headers than any legitimate pack contains. Bounds the up-front
    /// allocation on the untrusted read path.
    TooManyHeaders {
        /// Header count declared by the pack meta.
        declared: u32,
        /// Maximum accepted count (`MAX_HEADER_COUNT`).
        max: u32,
    },
    /// A `Header` record is larger than any real header's RLP. Bounds what a crafted pack can make
    /// the reader decode and keep.
    OversizedHeader {
        /// Byte length of the offending record's RLP.
        len: usize,
        /// Maximum accepted length (`MAX_HEADER_RECORD_BYTES`).
        max: usize,
    },
    /// The pack declares fewer headers than the `BLOCKHASH` lookback floor for its tip block. A
    /// genuine pack covers the full lookback (or reaches block 1); a shorter pack is truncated
    /// or corrupt, and restoring from it would scaffold zero-hash ancestors inside the lookback
    /// of the first post-restore block.
    InsufficientHeaders {
        /// Header count declared by the pack meta.
        declared: u32,
        /// Minimum accepted count: `min(block, BLOCKHASH_ANCESTORS)`.
        floor: u64,
        /// The pack's tip block number ([`ExecStateMeta::block_number`]).
        block: u64,
    },
    /// A `Storage` record carried more slots than the writer-side chunk bound
    /// ([`STORAGE_CHUNK_SLOTS`]), so the pack is corrupt or was not produced by a conforming
    /// writer. Bounds what a crafted pack can drive through consumers that size allocations to a
    /// chunk's length.
    OversizedStorageChunk {
        /// Slot count carried by the offending record.
        len: usize,
        /// Maximum accepted count ([`STORAGE_CHUNK_SLOTS`]).
        max: usize,
    },
    /// The snapshot header's `state_root` does not match the meta's `state_root`.
    StateRootMismatch,
    /// The snapshot header's number/hash do not match the meta.
    BlockIdentityMismatch,
    /// The account stream ended without a trailing footer record.
    MissingFooter,
    /// The footer's declared account count did not match the records read.
    AccountCountMismatch {
        /// Count declared by the footer.
        expected: u64,
        /// Count actually observed.
        got: u64,
    },
}

impl Error for ExecStatePackError {}

impl fmt::Display for ExecStatePackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io error: {e}"),
            Self::Open(e) => write!(f, "open error: {e}"),
            Self::LoadHeader(e) => write!(f, "load header error: {e}"),
            Self::Read(e) => write!(f, "read error: {e}"),
            Self::HeaderRlp(e) => write!(f, "header rlp decode error: {e}"),
            Self::Append(e) => write!(f, "append error: {e}"),
            Self::Persist(e) => write!(f, "persist error: {e}"),
            Self::CorruptPack => write!(f, "corrupt pack: record out of place"),
            Self::MetaNotFirst => write!(f, "first record was not the meta record"),
            Self::MissingHeaders => write!(f, "pack is missing one or more declared headers"),
            Self::TooManyHeaders { declared, max } => {
                write!(f, "pack declares {declared} headers, exceeding the maximum of {max}")
            }
            Self::OversizedHeader { len, max } => {
                write!(f, "header record is {len} bytes, exceeding the maximum of {max}")
            }
            Self::InsufficientHeaders { declared, floor, block } => {
                write!(
                    f,
                    "pack declares {declared} headers for tip block {block}, below the \
                     BLOCKHASH lookback floor of {floor}"
                )
            }
            Self::OversizedStorageChunk { len, max } => {
                write!(f, "storage chunk holds {len} slots, exceeding the maximum of {max}")
            }
            Self::StateRootMismatch => {
                write!(f, "snapshot header state_root does not match meta state_root")
            }
            Self::BlockIdentityMismatch => {
                write!(f, "snapshot header number/hash does not match meta")
            }
            Self::MissingFooter => write!(f, "account stream ended without a footer record"),
            Self::AccountCountMismatch { expected, got } => {
                write!(f, "footer account count {expected} does not match {got} accounts read")
            }
        }
    }
}

impl From<io::Error> for ExecStatePackError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<OpenError> for ExecStatePackError {
    fn from(value: OpenError) -> Self {
        Self::Open(value)
    }
}

impl From<LoadHeaderError> for ExecStatePackError {
    fn from(value: LoadHeaderError) -> Self {
        Self::LoadHeader(value)
    }
}

impl From<FetchError> for ExecStatePackError {
    fn from(value: FetchError) -> Self {
        Self::Read(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::TempDir;

    /// The `Header` record is a byte string, byte-identical to the plain `Vec<u8>` it was before,
    /// so existing exec-state packs read unchanged (and the mirror shows the variant indices
    /// hold).
    #[test]
    fn header_record_encodes_as_before() {
        #[derive(Serialize, Deserialize)]
        enum OldRecord {
            Meta(ExecStateMeta),
            Header(Vec<u8>),
        }
        let meta = ExecStateMeta {
            state_root: B256::from([1_u8; 32]),
            block_number: 7,
            block_hash: B256::from([2_u8; 32]),
            header_count: 1,
        };
        let rlp = encode_header(&header(7, B256::from([3_u8; 32])));
        let old_meta = tn_types::encode(&OldRecord::Meta(meta.clone()));
        let old_header = tn_types::encode(&OldRecord::Header(rlp.clone()));
        assert_eq!(tn_types::encode(&ExecStateRecord::Meta(meta)), old_meta);
        assert_eq!(tn_types::encode(&ExecStateRecord::Header(rlp.clone())), old_header);
        match tn_types::decode::<ExecStateRecord>(&old_header) {
            ExecStateRecord::Header(bytes) => assert_eq!(bytes, rlp),
            other => panic!("decoded {other:?}"),
        }
    }

    fn header(number: u64, state_root: B256) -> ExecHeader {
        ExecHeader { number, state_root, ..Default::default() }
    }

    fn account(seed: u8, slots: usize, with_code: bool) -> ExecStateAccount {
        let storage = (slots > 0).then(|| {
            (0..slots)
                .map(|i| (B256::from([i as u8 + 1; 32]), B256::from([seed; 32])))
                .collect::<BTreeMap<_, _>>()
        });
        let code = with_code.then(|| Bytes::from(vec![0x60, 0x00, seed]));
        ExecStateAccount {
            address: Address::from([seed; 20]),
            account: GenesisAccount {
                nonce: Some(seed as u64),
                balance: U256::from(seed as u64) * U256::from(1_000_000u64),
                code,
                storage,
                private_key: None,
            },
        }
    }

    /// Open the raw data file read-write so a test can craft a deliberately malformed
    /// pack (bypassing the writer's up-front validation).
    fn open_raw(dir: &Path) -> Pack<ExecStateRecord> {
        Pack::open(dir.join(DATA_NAME), 0, false, PackCompression::ZStd, EXEC_STATE_PACK_VERSION)
            .expect("open raw pack")
    }

    #[test]
    fn round_trip_and_verify() {
        let dir = TempDir::with_prefix("exec_state_pack").expect("temp dir");
        let root = B256::from([7u8; 32]);
        // tip block 3 with all 3 headers down to block 1, so the pack covers the whole chain
        // below its tip and satisfies `open`'s BLOCKHASH lookback floor
        let headers = vec![
            header(3, root),
            header(2, B256::from([9u8; 32])),
            header(1, B256::from([8u8; 32])),
        ];

        // Write: meta + 3 headers + 100 varied accounts + footer.
        let mut writer =
            ExecStatePackWriter::create(dir.path(), root, &headers).expect("create writer");
        let mut expected = Vec::new();
        let mut expected_slots = 0u64;
        let mut expected_code = 0u64;
        for i in 0..100u8 {
            let slots = (i % 4) as usize;
            let with_code = i % 3 == 0;
            expected_slots += slots as u64;
            if with_code {
                expected_code += 1;
            }
            let acc = account(i, slots, with_code);
            expected.push(acc.clone());
            writer.append_account(&acc).expect("append account");
        }
        let stats = writer.finish().expect("finish");
        assert_eq!(stats.account_count, 100);
        assert_eq!(stats.storage_slots, expected_slots);
        assert_eq!(stats.bytecodes, expected_code);

        // Read back: meta + headers round-trip, accounts stream byte-equal.
        let mut reader = ExecStatePackReader::open(dir.path()).expect("open reader");
        assert_eq!(reader.meta().state_root, root);
        assert_eq!(reader.meta().block_number, 3);
        assert_eq!(reader.meta().header_count, 3);
        assert_eq!(reader.headers().len(), 3);
        assert_eq!(reader.snapshot_header().state_root, root);
        assert_eq!(reader.headers(), headers.as_slice());
        let got: Vec<_> = reader.accounts().collect::<Result<_, _>>().expect("stream accounts");
        assert_eq!(got, expected);

        // Structural verification agrees with the writer's tallies.
        let report = ExecStatePackReader::verify(dir.path()).expect("verify");
        assert_eq!(report.header_count, 3);
        assert_eq!(report.account_count, stats.account_count);
        assert_eq!(report.storage_slots, stats.storage_slots);
        assert_eq!(report.bytecodes, stats.bytecodes);
    }

    #[test]
    fn round_trip_large_account_storage() {
        let dir = TempDir::with_prefix("exec_state_pack_large").expect("temp dir");
        let root = B256::from([5u8; 32]);
        let mut writer =
            ExecStatePackWriter::create(dir.path(), root, &[header(1, root)]).expect("create");

        // As a single inline record, this account's storage (>262k slots * 64 bytes) would exceed
        // the container's 16 MiB decompress cap; chunking keeps every record small.
        let n = 300_000usize;
        let big: BTreeMap<B256, B256> = (0..n)
            .map(|i| (B256::from(U256::from(i as u64).to_be_bytes::<32>()), B256::from([1u8; 32])))
            .collect();
        let acc = ExecStateAccount {
            address: Address::from([1u8; 20]),
            account: GenesisAccount {
                nonce: Some(1),
                balance: U256::from(42u64),
                code: Some(Bytes::from_static(&[0x60, 0x00])),
                storage: Some(big),
                private_key: None,
            },
        };
        writer.append_account(&acc).expect("append");
        let stats = writer.finish().expect("finish");
        assert_eq!(stats.account_count, 1);
        assert_eq!(stats.storage_slots, n as u64);

        // Full-assembly read reconstructs the account byte-for-byte.
        let mut reader = ExecStatePackReader::open(dir.path()).expect("open");
        let got: Vec<_> = reader.accounts().collect::<Result<_, _>>().expect("accounts");
        assert_eq!(got, vec![acc]);

        // Chunked read yields one Account header then several bounded Storage chunks.
        let mut reader = ExecStatePackReader::open(dir.path()).expect("open");
        let mut account_headers = 0usize;
        let mut chunks: Vec<Vec<(B256, B256)>> = Vec::new();
        while let Some(entry) = reader.next_entry() {
            match entry.expect("entry") {
                StateEntry::Account(_) => account_headers += 1,
                StateEntry::Storage(chunk) => chunks.push(chunk),
            }
        }
        assert_eq!(account_headers, 1);
        assert_eq!(chunks.len(), n.div_ceil(STORAGE_CHUNK_SLOTS));
        assert!(chunks.iter().all(|c| c.len() <= STORAGE_CHUNK_SLOTS));
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), n);
    }

    /// One `(slot, value)` pair with a unique key derived from `i`.
    fn slot(i: usize) -> (B256, B256) {
        (B256::from(U256::from(i).to_be_bytes::<32>()), B256::from([1u8; 32]))
    }

    /// Craft a raw pack whose single account carries ONE storage chunk of exactly `slots` slots,
    /// bypassing the writer's append-time bound check. An oversized chunk built this way stays
    /// under the container's 16 MiB record cap, so only the [`STORAGE_CHUNK_SLOTS`] re-check can
    /// reject it.
    fn craft_pack_with_chunk_len(slots: usize) -> TempDir {
        let dir = TempDir::with_prefix("exec_state_pack_chunk_len").expect("temp dir");
        let root = B256::from([4u8; 32]);
        // tip block 1, so the single-header pack reaches block 1 and satisfies `open`'s
        // BLOCKHASH lookback floor
        let snapshot = header(1, root);
        let mut pack = open_raw(dir.path());
        let meta = ExecStateMeta {
            state_root: root,
            block_number: 1,
            block_hash: snapshot.hash_slow(),
            header_count: 1,
        };
        pack.append(&ExecStateRecord::Meta(meta)).unwrap();
        pack.append(&ExecStateRecord::Header(encode_header(&snapshot))).unwrap();
        pack.append(&ExecStateRecord::Account(AccountRecord {
            address: Address::from([1u8; 20]),
            nonce: 1,
            balance: B256::ZERO,
            code: None,
        }))
        .unwrap();
        pack.append(&ExecStateRecord::Storage((0..slots).map(slot).collect())).unwrap();
        pack.append(&ExecStateRecord::End(ExecStateStats {
            account_count: 1,
            storage_slots: slots as u64,
            bytecodes: 0,
        }))
        .unwrap();
        pack.commit().unwrap();
        dir
    }

    /// The read path must not trust the writer-side [`STORAGE_CHUNK_SLOTS`] convention: a
    /// hand-crafted pack carrying an oversized storage chunk is rejected on every read path
    /// (`next_account`, `next_entry`, `verify`), while the same fixture with the chunk at exactly
    /// the bound is accepted by all three, so the rejections are attributable to the bound
    /// check, not to the raw-crafted fixture.
    #[test]
    fn reader_rejects_oversized_storage_chunk() {
        // Positive control: a chunk of exactly STORAGE_CHUNK_SLOTS streams through every path.
        let dir = craft_pack_with_chunk_len(STORAGE_CHUNK_SLOTS);
        let mut reader = ExecStatePackReader::open(dir.path()).expect("open");
        let accounts: Vec<_> =
            reader.accounts().collect::<Result<_, _>>().expect("exact-bound chunk must stream");
        let slot_count =
            accounts.first().and_then(|a| a.account.storage.as_ref()).map(BTreeMap::len);
        assert_eq!(slot_count, Some(STORAGE_CHUNK_SLOTS));

        let mut reader = ExecStatePackReader::open(dir.path()).expect("open");
        let chunk_lens: Vec<usize> = std::iter::from_fn(|| reader.next_entry())
            .map(|entry| entry.expect("exact-bound entries must read"))
            .filter_map(|entry| match entry {
                StateEntry::Account(_) => None,
                StateEntry::Storage(chunk) => Some(chunk.len()),
            })
            .collect();
        assert_eq!(chunk_lens, vec![STORAGE_CHUNK_SLOTS]);

        let report = ExecStatePackReader::verify(dir.path()).expect("exact-bound pack must verify");
        assert_eq!(report.storage_slots, STORAGE_CHUNK_SLOTS as u64);

        // One slot past the bound: every read path rejects with the dedicated error.
        let dir = craft_pack_with_chunk_len(STORAGE_CHUNK_SLOTS + 1);
        let mut reader = ExecStatePackReader::open(dir.path()).expect("open");
        let err = reader
            .accounts()
            .collect::<Result<Vec<_>, _>>()
            .expect_err("next_account must reject an oversized chunk");
        assert!(matches!(
            err,
            ExecStatePackError::OversizedStorageChunk { len, max }
                if len == STORAGE_CHUNK_SLOTS + 1 && max == STORAGE_CHUNK_SLOTS
        ));

        let mut reader = ExecStatePackReader::open(dir.path()).expect("open");
        let entries: Vec<_> = std::iter::from_fn(|| reader.next_entry()).collect();
        let (oks, errs): (Vec<_>, Vec<_>) = entries.into_iter().partition(Result::is_ok);
        assert_eq!(oks.len(), 1, "only the account header precedes the oversized chunk");
        assert!(matches!(errs.as_slice(), [Err(ExecStatePackError::OversizedStorageChunk { .. })]));

        let err = ExecStatePackReader::verify(dir.path())
            .expect_err("verify must reject an oversized chunk");
        assert!(matches!(err, ExecStatePackError::OversizedStorageChunk { .. }));
    }

    /// The writer enforces the bound it documents: an append at exactly [`STORAGE_CHUNK_SLOTS`]
    /// succeeds (positive control), one slot more is rejected before anything is written or
    /// tallied, and the surviving pack still round-trips.
    #[test]
    fn writer_rejects_oversized_storage_chunk() {
        let dir = TempDir::with_prefix("exec_state_pack_writer_bound").expect("temp dir");
        let root = B256::from([6u8; 32]);
        // tip block 1, so the single-header pack reaches block 1 and satisfies the
        // BLOCKHASH lookback floor on `verify`
        let mut writer =
            ExecStatePackWriter::create(dir.path(), root, &[header(1, root)]).expect("create");
        writer
            .append_account_header(Address::from([1u8; 20]), 1, U256::from(1u64), None)
            .expect("account header");

        let oversized: Vec<(B256, B256)> = (0..=STORAGE_CHUNK_SLOTS).map(slot).collect();
        let err = writer
            .append_storage_chunk(&oversized)
            .expect_err("oversized chunk must be rejected at append time");
        assert!(matches!(
            err,
            ExecStatePackError::OversizedStorageChunk { len, max }
                if len == STORAGE_CHUNK_SLOTS + 1 && max == STORAGE_CHUNK_SLOTS
        ));

        let exact: Vec<(B256, B256)> = (0..STORAGE_CHUNK_SLOTS).map(slot).collect();
        writer.append_storage_chunk(&exact).expect("exact-bound chunk must append");
        let stats = writer.finish().expect("finish");
        assert_eq!(
            stats.storage_slots, STORAGE_CHUNK_SLOTS as u64,
            "the rejected chunk must not have been tallied"
        );

        let report = ExecStatePackReader::verify(dir.path()).expect("verify");
        assert_eq!(report.storage_slots, STORAGE_CHUNK_SLOTS as u64);
    }

    #[test]
    fn create_rejects_state_root_mismatch() {
        let dir = TempDir::with_prefix("exec_state_pack_create_srm").expect("temp dir");
        let headers = vec![header(1, B256::from([2u8; 32]))];
        let err = ExecStatePackWriter::create(dir.path(), B256::from([1u8; 32]), &headers)
            .expect_err("mismatched root must be rejected");
        assert!(matches!(err, ExecStatePackError::StateRootMismatch));
    }

    #[test]
    fn create_rejects_empty_headers() {
        let dir = TempDir::with_prefix("exec_state_pack_no_headers").expect("temp dir");
        let err = ExecStatePackWriter::create(dir.path(), B256::ZERO, &[])
            .expect_err("empty headers must be rejected");
        assert!(matches!(err, ExecStatePackError::MissingHeaders));
    }

    #[test]
    fn create_refuses_an_existing_pack() {
        let dir = TempDir::with_prefix("exec_state_create_twice").expect("temp dir");
        let root = B256::from([1u8; 32]);
        let mut writer =
            ExecStatePackWriter::create(dir.path(), root, &[header(1, root)]).expect("create");
        writer.append_account(&account(0xAA, 1, false)).expect("append");
        writer.finish().expect("finish");
        let before = std::fs::read(dir.path().join(DATA_NAME)).expect("read pack");

        let err = ExecStatePackWriter::create(dir.path(), root, &[header(1, root)])
            .expect_err("a second create over a finished pack must be refused");
        assert!(
            matches!(&err, ExecStatePackError::Io(e) if e.kind() == io::ErrorKind::AlreadyExists),
            "expected AlreadyExists, got {err}"
        );
        assert_eq!(
            std::fs::read(dir.path().join(DATA_NAME)).expect("reread pack"),
            before,
            "a refused create must leave the existing pack untouched"
        );
    }

    #[test]
    fn reader_stops_at_first_footer_ignoring_appended_bytes() {
        // Safety net behind `create`'s refusal to write over an existing pack: bytes appended
        // after a pack's `End` footer (here a whole second pack, the "doubled" shape an
        // append-mode writer over a dirty dir would leave) must be inert. The reader terminates at
        // the FIRST footer, so they can never contribute wrong state to a recomputed root.
        let dir = TempDir::with_prefix("exec_state_doubled").expect("temp dir");

        // First (real) pack: meta + one header + account A + footer.
        let root1 = B256::from([1u8; 32]);
        let acct_a = account(0xAA, 1, false);
        let mut w1 =
            ExecStatePackWriter::create(dir.path(), root1, &[header(1, root1)]).expect("create 1");
        w1.append_account(&acct_a).expect("append A");
        w1.finish().expect("finish 1");

        // A whole second pack appended after the first's footer, written through a raw
        // append-mode handle (`create` itself refuses to write over an existing pack).
        let root2 = B256::from([2u8; 32]);
        let acct_b = account(0xBB, 1, false);
        let snapshot2 = header(1, root2);
        let mut w2 = ExecStatePackWriter {
            data: open_raw(dir.path()),
            stats: ExecStateStats::default(),
            finished: false,
        };
        let meta2 = ExecStateMeta {
            state_root: root2,
            block_number: snapshot2.number,
            block_hash: snapshot2.hash_slow(),
            header_count: 1,
        };
        ExecStatePackWriter::append(&mut w2.data, &ExecStateRecord::Meta(meta2)).expect("meta 2");
        ExecStatePackWriter::append(
            &mut w2.data,
            &ExecStateRecord::Header(encode_header(&snapshot2)),
        )
        .expect("header 2");
        w2.append_account(&acct_b).expect("append B");
        w2.finish().expect("finish 2");

        // The reader reads the FIRST pack only (meta1, its header, account A) and stops at End1.
        let mut reader = ExecStatePackReader::open(dir.path()).expect("open");
        assert_eq!(reader.meta().state_root, root1, "reader must read the first pack's meta");
        let got: Vec<_> = reader.accounts().collect::<Result<_, _>>().expect("accounts");
        assert_eq!(
            got,
            vec![acct_a],
            "reader must return only the first pack's account; appended bytes are inert"
        );
    }

    #[test]
    fn verify_detects_state_root_mismatch() {
        let dir = TempDir::with_prefix("exec_state_pack_srm").expect("temp dir");
        let mut pack = open_raw(dir.path());
        // Meta claims root A, but the snapshot header carries root B.
        let meta = ExecStateMeta {
            state_root: B256::from([1u8; 32]),
            block_number: 1,
            block_hash: B256::ZERO,
            header_count: 1,
        };
        pack.append(&ExecStateRecord::Meta(meta)).unwrap();
        pack.append(&ExecStateRecord::Header(encode_header(&header(1, B256::from([2u8; 32])))))
            .unwrap();
        pack.append(&ExecStateRecord::End(ExecStateStats::default())).unwrap();
        pack.commit().unwrap();
        drop(pack);

        let err = ExecStatePackReader::verify(dir.path()).expect_err("must detect mismatch");
        assert!(matches!(err, ExecStatePackError::StateRootMismatch));
    }

    #[test]
    fn verify_detects_missing_footer() {
        let dir = TempDir::with_prefix("exec_state_pack_mf").expect("temp dir");
        let root = B256::from([3u8; 32]);
        let snapshot = header(1, root);
        let mut pack = open_raw(dir.path());
        let meta = ExecStateMeta {
            state_root: root,
            block_number: 1,
            block_hash: snapshot.hash_slow(),
            header_count: 1,
        };
        pack.append(&ExecStateRecord::Meta(meta)).unwrap();
        pack.append(&ExecStateRecord::Header(encode_header(&snapshot))).unwrap();
        pack.append(&ExecStateRecord::Account(AccountRecord {
            address: Address::from([1u8; 20]),
            nonce: 1,
            balance: B256::ZERO,
            code: None,
        }))
        .unwrap();
        // Deliberately omit the End footer.
        pack.commit().unwrap();
        drop(pack);

        let err = ExecStatePackReader::verify(dir.path()).expect_err("must detect truncation");
        assert!(matches!(err, ExecStatePackError::MissingFooter));

        // Streaming reader surfaces the same truncation.
        let mut reader = ExecStatePackReader::open(dir.path()).expect("open reader");
        let results: Vec<_> = reader.accounts().collect();
        assert!(matches!(results.last(), Some(Err(ExecStatePackError::MissingFooter))));
    }

    #[test]
    fn open_rejects_newer_pack_version() {
        let dir = TempDir::with_prefix("exec_state_pack_ver").expect("temp dir");
        // Stamp the file header with a newer version than this build understands.
        let mut pack: Pack<ExecStateRecord> = Pack::open(
            dir.path().join(DATA_NAME),
            0,
            false,
            PackCompression::ZStd,
            EXEC_STATE_PACK_VERSION + 1,
        )
        .expect("open raw pack");
        let meta = ExecStateMeta {
            state_root: B256::ZERO,
            block_number: 0,
            block_hash: B256::ZERO,
            header_count: 1,
        };
        pack.append(&ExecStateRecord::Meta(meta)).unwrap();
        pack.commit().unwrap();
        drop(pack);

        // The container's version gate (fed EXEC_STATE_PACK_VERSION) rejects it on open.
        let err = ExecStatePackReader::open(dir.path()).expect_err("must reject newer version");
        assert!(matches!(err, ExecStatePackError::Open(_)));
    }

    #[test]
    fn open_rejects_implausible_header_count() {
        let dir = TempDir::with_prefix("exec_state_pack_thc").expect("temp dir");
        let mut pack = open_raw(dir.path());
        // A crafted meta declaring far more headers than any legitimate pack. `open` must reject it
        // up front rather than eagerly reserving a `Vec` sized by the untrusted count (a would-be
        // multi-TB allocation), before reading any header record.
        let meta = ExecStateMeta {
            state_root: B256::ZERO,
            block_number: 0,
            block_hash: B256::ZERO,
            header_count: u32::MAX,
        };
        pack.append(&ExecStateRecord::Meta(meta)).unwrap();
        pack.commit().unwrap();
        drop(pack);

        let err = ExecStatePackReader::open(dir.path())
            .expect_err("must reject implausible header_count");
        assert!(
            matches!(err, ExecStatePackError::TooManyHeaders { declared, .. } if declared == u32::MAX)
        );
    }

    #[test]
    fn open_rejects_header_count_below_blockhash_floor() {
        // Boundary pair around the `BLOCKHASH` lookback floor (issue #1174) for a tip deep
        // enough that the full 256-header floor applies: a pack one header short of the floor
        // must be refused, and a pack exactly at the floor must open, proving the refusal is
        // specific rather than a blanket reject.
        let block = 300u64;
        let root = B256::from([4u8; 32]);
        let snapshot = header(block, root);

        let build = |header_count: u32| {
            let dir = TempDir::with_prefix("exec_state_pack_floor").expect("temp dir");
            let mut pack = open_raw(dir.path());
            let meta = ExecStateMeta {
                state_root: root,
                block_number: block,
                block_hash: snapshot.hash_slow(),
                header_count,
            };
            pack.append(&ExecStateRecord::Meta(meta)).expect("append meta");
            // snapshot header first, then ancestors newest-first (the exporter's shape)
            ((block + 1 - u64::from(header_count))..=block)
                .rev()
                .try_for_each(|number| {
                    pack.append(&ExecStateRecord::Header(encode_header(&header(number, root))))
                        .map(|_| ())
                })
                .expect("append headers");
            pack.append(&ExecStateRecord::End(ExecStateStats::default())).expect("append end");
            pack.commit().expect("commit");
            drop(pack);
            dir
        };

        // 255 headers for tip 300 is one short of the floor (min(300, 256) = 256)
        let short = build(255);
        let err = ExecStatePackReader::open(short.path())
            .expect_err("a pack short of the BLOCKHASH lookback floor must be refused");
        assert!(
            matches!(
                err,
                ExecStatePackError::InsufficientHeaders { declared: 255, floor: 256, block: 300 }
            ),
            "unexpected error: {err:?}"
        );

        // exactly the floor opens, and the declared headers round-trip
        let at_floor = build(256);
        let reader = ExecStatePackReader::open(at_floor.path()).expect("at-floor pack must open");
        assert_eq!(reader.meta().header_count, 256);
        assert_eq!(reader.headers().len(), 256);
    }

    #[test]
    fn account_code_bytes_encodes_like_vec() {
        // `AccountRecord.code` is stored as `Option<Bytes>` (avoiding a per-account `to_vec`),
        // where it used to be `Option<Vec<u8>>`. Under the pack's BCS codec both encode
        // identically — (Option tag) + (ULEB128 len) + (raw bytes) — so the wire format is
        // unchanged and `EXEC_STATE_PACK_VERSION` need not bump. This test locks that
        // invariant.
        let raw = vec![0x60u8, 0x00, 0x2a, 0xff, 0x01];
        let as_bytes: Option<Bytes> = Some(Bytes::from(raw.clone()));
        let as_vec: Option<Vec<u8>> = Some(raw);
        assert_eq!(tn_types::encode(&as_bytes), tn_types::encode(&as_vec));

        let none_bytes: Option<Bytes> = None;
        let none_vec: Option<Vec<u8>> = None;
        assert_eq!(tn_types::encode(&none_bytes), tn_types::encode(&none_vec));
    }

    /// A header whose RLP exceeds [`MAX_HEADER_RECORD_BYTES`]; a real one is under 1 KiB
    /// (`extra_data` is empty or 32 bytes).
    fn oversized_header(number: u64, state_root: B256) -> ExecHeader {
        ExecHeader {
            extra_data: Bytes::from(vec![0xAB; MAX_HEADER_RECORD_BYTES]),
            ..header(number, state_root)
        }
    }

    /// A raw pack at `dir`: a meta for tip `block` declaring `header_count`, then `headers` and a
    /// footer (no accounts).
    fn write_raw_headers(dir: &Path, block: u64, header_count: u32, headers: &[ExecHeader]) {
        let root = headers[0].state_root;
        let mut pack = open_raw(dir);
        let meta = ExecStateMeta {
            state_root: root,
            block_number: block,
            block_hash: headers[0].hash_slow(),
            header_count,
        };
        pack.append(&ExecStateRecord::Meta(meta)).expect("append meta");
        for header in headers {
            pack.append(&ExecStateRecord::Header(encode_header(header))).expect("append header");
        }
        pack.append(&ExecStateRecord::End(ExecStateStats::default())).expect("append end");
        pack.commit().expect("commit");
    }

    /// `count` headers for tip `block`, snapshot first then ancestors newest-first (the
    /// exporter's shape).
    fn header_window(block: u64, count: u64, root: B256) -> Vec<ExecHeader> {
        ((block + 1 - count)..=block).rev().map(|number| header(number, root)).collect()
    }

    #[test]
    fn open_rejects_an_oversized_header_record() {
        // Each record may decompress to the container's 16 MiB limit, and the reader keeps every
        // header it decodes, so an unbounded header record would let a few-MB crafted pack drive
        // a multi-GiB allocation at `open`. The record is refused before it is decoded.
        let dir = TempDir::with_prefix("exec_state_pack_big_header").expect("temp dir");
        let root = B256::from([5u8; 32]);
        write_raw_headers(dir.path(), 1, 1, &[oversized_header(1, root)]);

        let err = ExecStatePackReader::open(dir.path())
            .expect_err("an oversized header record must be refused");
        assert!(
            matches!(
                err,
                ExecStatePackError::OversizedHeader { len, max: MAX_HEADER_RECORD_BYTES }
                    if len > MAX_HEADER_RECORD_BYTES
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn open_bounds_the_header_count_by_the_lookback_window() {
        // A genuine pack holds the snapshot header plus at most BLOCKHASH_ANCESTORS ancestors.
        let block = 300u64;
        let root = B256::from([6u8; 32]);
        let full = BLOCKHASH_ANCESTORS + 1;

        let at_cap = TempDir::with_prefix("exec_state_pack_count_cap").expect("temp dir");
        write_raw_headers(at_cap.path(), block, full as u32, &header_window(block, full, root));
        let reader = ExecStatePackReader::open(at_cap.path()).expect("a full window must open");
        assert_eq!(reader.headers().len() as u64, full);

        let over = TempDir::with_prefix("exec_state_pack_count_over").expect("temp dir");
        let declared = full as u32 + 1;
        write_raw_headers(over.path(), block, declared, &header_window(block, full + 1, root));
        let err = ExecStatePackReader::open(over.path())
            .expect_err("more headers than the lookback window must be refused");
        assert!(
            matches!(
                err,
                ExecStatePackError::TooManyHeaders { declared: d, max } if d == declared && max == full as u32
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn create_enforces_the_reader_header_bounds() {
        // An export the reader would refuse must fail at export, not later at import.
        let block = 300u64;
        let root = B256::from([7u8; 32]);
        let full = BLOCKHASH_ANCESTORS + 1;

        let over = TempDir::with_prefix("exec_state_create_count").expect("temp dir");
        let err =
            ExecStatePackWriter::create(over.path(), root, &header_window(block, full + 1, root))
                .expect_err("too many headers must be refused at export");
        assert!(
            matches!(err, ExecStatePackError::TooManyHeaders { declared, max }
                if u64::from(declared) == full + 1 && u64::from(max) == full),
            "unexpected error: {err:?}"
        );
        assert!(!over.path().join(DATA_NAME).exists(), "a refused export must not leave a pack");

        let big = TempDir::with_prefix("exec_state_create_big").expect("temp dir");
        let err = ExecStatePackWriter::create(big.path(), root, &[oversized_header(1, root)])
            .expect_err("an oversized header must be refused at export");
        assert!(
            matches!(err, ExecStatePackError::OversizedHeader { .. }),
            "unexpected error: {err:?}"
        );
        assert!(!big.path().join(DATA_NAME).exists(), "a refused export must not leave a pack");

        // A full window is exported and reads back.
        let ok = TempDir::with_prefix("exec_state_create_full").expect("temp dir");
        ExecStatePackWriter::create(ok.path(), root, &header_window(block, full, root))
            .expect("a full window exports")
            .finish()
            .expect("finish");
        ExecStatePackReader::verify(ok.path()).expect("the export verifies");
    }

    #[test]
    fn abandoned_writer_leaves_no_pack() {
        // A writer dropped before `finish` (an export that failed or was cancelled mid-way) must
        // not leave a cleanly sealed partial pack behind; a retry can then create afresh.
        let dir = TempDir::with_prefix("exec_state_abandoned").expect("temp dir");
        let root = B256::from([8u8; 32]);
        let mut writer =
            ExecStatePackWriter::create(dir.path(), root, &[header(1, root)]).expect("create");
        writer.append_account(&account(0xAA, 3, true)).expect("append");
        drop(writer);
        assert!(!dir.path().join(DATA_NAME).exists(), "an abandoned export must be removed");

        let mut writer =
            ExecStatePackWriter::create(dir.path(), root, &[header(1, root)]).expect("retry");
        writer.append_account(&account(0xAA, 3, true)).expect("append");
        writer.finish().expect("finish");
        assert_eq!(ExecStatePackReader::verify(dir.path()).expect("verify").account_count, 1);
    }
}
