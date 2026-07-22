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
//! accounts in a small primitive wire struct ([`AccountRecord`]); the public API still
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

use std::{collections::BTreeMap, error::Error, fmt, fs::File, io, path::Path};

use alloy_rlp::{Decodable, Encodable};
use serde::{Deserialize, Serialize};
use tn_types::{Address, Bytes, ExecHeader, GenesisAccount, B256, U256};

use crate::archive::{
    error::{fetch::FetchError, load_header::LoadHeaderError, open::OpenError},
    pack::{Pack, PackCompression},
    pack_iter::PackIter,
};

/// Schema version stamped into the pack file's `DataHeader` (via [`Pack::open`]) and
/// verified when the pack is reopened. Bump it when the record layout changes.
///
/// This is the pack's own version field — there is no separate version inside
/// [`ExecStateMeta`]. It must stay `<= PACK_VERSION`, the global container-framing
/// version every read is gated on (guaranteed by the assertion below).
pub const EXEC_STATE_PACK_VERSION: u16 = 1;

/// Name of the data file inside the pack directory.
const DATA_NAME: &str = "state_data";

/// Maximum storage slots per [`ExecStateRecord::Storage`] chunk. Each slot is 64 bytes, so 64k
/// slots is ~4 MiB decompressed — comfortably under the container's per-record limit
/// (`MAX_RECORD_SIZE` = 16 MiB, [`crate::archive::pack_iter`]), leaving margin for BCS overhead.
const STORAGE_CHUNK_SLOTS: usize = 64 * 1024;

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
    /// Number of header records that immediately follow (>= 1).
    pub header_count: u32,
}

/// A single account plus its storage and code, mirroring the genesis-account shape so
/// the import side can feed reth's genesis machinery directly. This is the public
/// account type; on disk it is stored as a BCS-safe [`AccountRecord`] header followed by
/// [`ExecStateRecord::Storage`] chunks, so storage of any size round-trips.
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
/// inline: it follows as zero or more [`ExecStateRecord::Storage`] chunks, so an account with
/// arbitrarily large storage never exceeds the container's per-record limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AccountRecord {
    address: Address,
    nonce: u64,
    balance: B256,
    code: Option<Vec<u8>>,
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
                code: self.code.map(Bytes::from),
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
            code: self.code.map(Bytes::from),
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
    /// An RLP-encoded [`ExecHeader`] (canonical, lossless form).
    Header(Vec<u8>),
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

/// Decode a header from its stored RLP bytes.
fn decode_header(bytes: &[u8]) -> Result<ExecHeader, ExecStatePackError> {
    ExecHeader::decode(&mut &bytes[..]).map_err(|e| ExecStatePackError::HeaderRlp(e.to_string()))
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
#[derive(Debug)]
pub struct ExecStatePackWriter {
    data: Pack<ExecStateRecord>,
    stats: ExecStateStats,
}

impl ExecStatePackWriter {
    /// Create a new pack in directory `path` (created if missing) and write the
    /// [`ExecStateMeta`] followed by `headers`. The snapshot header must be first and
    /// its `state_root` must equal `state_root` (the invariant the reader re-checks).
    pub fn create<P: AsRef<Path>>(
        path: P,
        state_root: B256,
        headers: &[ExecHeader],
    ) -> Result<Self, ExecStatePackError> {
        let snapshot = headers.first().ok_or(ExecStatePackError::MissingHeaders)?;
        if snapshot.state_root != state_root {
            return Err(ExecStatePackError::StateRootMismatch);
        }

        let base = path.as_ref();
        std::fs::create_dir_all(base)?;
        let mut data: Pack<ExecStateRecord> = Pack::open(
            base.join(DATA_NAME),
            0,
            false,
            PackCompression::ZStd,
            EXEC_STATE_PACK_VERSION,
        )?;

        let meta = ExecStateMeta {
            state_root,
            block_number: snapshot.number,
            block_hash: snapshot.hash_slow(),
            header_count: headers.len() as u32,
        };
        Self::append(&mut data, &ExecStateRecord::Meta(meta))?;
        for header in headers {
            Self::append(&mut data, &ExecStateRecord::Header(encode_header(header)))?;
        }

        Ok(Self { data, stats: ExecStateStats::default() })
    }

    /// Append one account (header + storage chunks) to the pack, updating the running tallies.
    ///
    /// The account's storage is split into [`STORAGE_CHUNK_SLOTS`]-sized
    /// [`ExecStateRecord::Storage`] records so an account with arbitrarily large storage always
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
            // BTreeMap iterates in ascending key order — a stable, deterministic chunk order.
            let slots: Vec<(B256, B256)> = storage.iter().map(|(k, v)| (*k, *v)).collect();
            for chunk in slots.chunks(STORAGE_CHUNK_SLOTS) {
                self.append_storage_chunk(chunk)?;
            }
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
            code: code.map(|code| code.to_vec()),
        };
        Self::append(&mut self.data, &ExecStateRecord::Account(record))
    }

    /// Append one chunk of storage slots for the current account. Empty chunks are ignored. A chunk
    /// should hold at most [`STORAGE_CHUNK_SLOTS`] slots to stay under the container's record
    /// limit.
    pub fn append_storage_chunk(
        &mut self,
        slots: &[(B256, B256)],
    ) -> Result<(), ExecStatePackError> {
        if slots.is_empty() {
            return Ok(());
        }
        self.stats.storage_slots += slots.len() as u64;
        Self::append(&mut self.data, &ExecStateRecord::Storage(slots.to_vec()))
    }

    /// Write the trailing [`ExecStateStats`] footer, commit the pack to disk, and
    /// return the tallies.
    pub fn finish(mut self) -> Result<ExecStateStats, ExecStatePackError> {
        Self::append(&mut self.data, &ExecStateRecord::End(self.stats))?;
        self.data.commit().map_err(|e| ExecStatePackError::Persist(e.to_string()))?;
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
    iter: PackIter<ExecStateRecord, File>,
    /// One-record lookahead: the record that terminated the previous account's storage run.
    pending: Option<ExecStateRecord>,
    done: bool,
}

impl ExecStatePackReader {
    /// Open the pack in directory `path` read-only and read its meta + headers.
    ///
    /// Errors if the first record is not the meta, the schema version is unexpected, or
    /// the declared headers are missing/short.
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

        let mut headers = Vec::with_capacity(meta.header_count as usize);
        for _ in 0..meta.header_count {
            match iter.next() {
                Some(Ok(ExecStateRecord::Header(bytes))) => headers.push(decode_header(&bytes)?),
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
    fn pull(&mut self) -> Option<Result<ExecStateRecord, ExecStatePackError>> {
        if let Some(record) = self.pending.take() {
            return Some(Ok(record));
        }
        match self.iter.next() {
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
    /// from) and checks: meta first + version, `header_count >= 1`, snapshot header's
    /// `state_root`/number/hash agree with the meta, a trailing footer is present, and
    /// its declared `account_count` matches the records actually read. Per-record CRC32
    /// is enforced by the container during the walk.
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
        let footer = loop {
            match reader.iter.next() {
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
                Some(Err(e)) => return Err(e.into()),
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
        let headers = vec![
            header(100, root),
            header(99, B256::from([9u8; 32])),
            header(98, B256::from([8u8; 32])),
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
        assert_eq!(reader.meta().block_number, 100);
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
    fn verify_detects_state_root_mismatch() {
        let dir = TempDir::with_prefix("exec_state_pack_srm").expect("temp dir");
        let mut pack = open_raw(dir.path());
        // Meta claims root A, but the snapshot header carries root B.
        let meta = ExecStateMeta {
            state_root: B256::from([1u8; 32]),
            block_number: 5,
            block_hash: B256::ZERO,
            header_count: 1,
        };
        pack.append(&ExecStateRecord::Meta(meta)).unwrap();
        pack.append(&ExecStateRecord::Header(encode_header(&header(5, B256::from([2u8; 32])))))
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
        let snapshot = header(7, root);
        let mut pack = open_raw(dir.path());
        let meta = ExecStateMeta {
            state_root: root,
            block_number: 7,
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
}
