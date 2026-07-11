//! Snapshot export and restore for reth's plain EVM state.
//!
//! This module has two sides. The export side ([`PinnedStateView`]) streams a state-only dump out
//! of a live node; the restore side ([`SnapshotRestorer`]) rebuilds that dump into a fresh datadir.
//! The [restore documentation](SnapshotRestorer) covers the rebuild algorithm; the rest of these
//! module docs describe the export side.
//!
//! # Export: pinned, read-consistent view over reth's plain EVM state
//!
//! [`RethEnv::pin_state_view`] opens a single reth `Tx<RO>` on the MDBX environment and calls
//! `disable_long_read_transaction_safety()` on it. MDBX (via reth's default [`DatabaseArgs`])
//! enforces a reader timeout (~5 minutes) that kills long-lived read transactions to keep the
//! free list from stalling; a full-state export can easily run longer than that. Disabling the
//! guard exempts *only this one transaction* — it does not touch any env-wide configuration, so
//! other readers and the writer keep their normal safety limits.
//!
//! The returned [`PinnedStateView`] owns the transaction (which itself keeps the MDBX environment
//! alive) and is [`Send`], so an uploader can move it into a background task and stream the dump
//! off the hot path.
//!
//! # Output format is a compatibility contract
//!
//! [`PinnedStateView::export_state_jsonl`] writes reth "init-state" JSONL that the restore side
//! (a later commit) feeds to `reth_db_common::init::init_from_state_dump` (reth v1.11.3,
//! `crates/storage/db-common/src/init.rs`). That parser reads the first line into a private
//! `StateRoot { root: B256 }` (`parse_state_root`) and every subsequent line into a private
//! `GenesisAccountWithAddress` — a `#[serde(flatten)]` [`GenesisAccount`] plus an `address` field
//! (~line 889). Both reth types are private, so we mirror their two shapes here and serialize the
//! real [`GenesisAccount`]. Because the account body is the genuine alloy type, its hex encodings
//! and its `skip_serializing_if` field omissions come straight from alloy's serde and cannot drift
//! from what the parser expects.
//!
//! # Why the state root is not recomputed here
//!
//! [`PinnedStateView::export_state_jsonl`] takes the expected state root as a parameter and simply
//! echoes it on line 1. It deliberately does not recompute the root from the pinned transaction:
//! a from-tx recompute on a live database just re-reads the already-cached trie tables and proves
//! nothing about the plain-state rows we actually stream. The restore side is the real check — it
//! rebuilds the trie from scratch out of the accounts in this dump and hard-fails on any mismatch.
//! Making the caller supply the root keeps that single authoritative check on the restore side.
//!
//! # Rejected alternative
//!
//! `mdbx_env_copy` with `MDBX_CP_COMPACT` (reachable via `Environment::with_raw_env_ptr` +
//! `reth_libmdbx::ffi`) would hand us a compacted copy of the whole database. It was rejected as
//! the primary path: it pays full-database read+write I/O and needs 1–2× the disk for tables we do
//! not ship (hashed state, history, trie, receipts — all of which the restore side rebuilds), so
//! it is kept only as a fallback. Note also that while a pin is held, MVCC page retention grows the
//! main database file by roughly the write volume that lands during the export; that space is
//! reclaimed by MDBX once the pinned transaction drops.

use crate::{
    error::{TnRethError, TnRethResult},
    DatabaseEnv, RethConfig, RethDatabaseT, RethDb, RethEnv,
};
use alloy::consensus::constants::KECCAK_EMPTY;
use eyre::{eyre, WrapErr as _};
use reth_config::config::EtlConfig;
use reth_db::{
    cursor::DbCursorRO,
    tables::{
        AccountsTrie, BlockBodyIndices, Bytecodes, HashedAccounts, HashedStorages, HeaderNumbers,
        PlainAccountState, PlainStorageState, StoragesTrie,
    },
    transaction::{DbTx, DbTxMut},
};
use reth_db_common::init::init_from_state_dump;
use reth_provider::{
    BlockWriter, ChainStateBlockWriter, DBProvider, DatabaseProviderFactory, ProviderError,
    StageCheckpointWriter, StaticFileProviderFactory, StaticFileSegment, StaticFileWriter,
};
use reth_stages_types::{StageCheckpoint, StageId};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    io::{self, BufRead, Write},
    path::PathBuf,
};
use tn_types::{
    gas_accumulator::{RewardsCounter, WorkerFeeConfig},
    Address, BlockBody, BlockNumHash, Bytes, Epoch, ExecHeader, GenesisAccount, SealedBlock,
    SealedHeader, TaskManager, WorkerId, B256,
};
use tracing::debug;

/// The pinned read-only MDBX transaction type — reth's `Tx<RO>` with long-read safety disabled.
type PinnedTx = <DatabaseEnv as RethDatabaseT>::TX;

/// A read-consistent view of reth's plain EVM state pinned to the tip at open time.
///
/// Wraps a single reth `Tx<RO>` that has been exempted from the long-read-transaction reader-kill
/// (see the module docs). The view reads only MDBX-resident tables (`PlainAccountState`,
/// `PlainStorageState`, `Bytecodes`, `HeaderNumbers`, `BlockBodyIndices`), so the transaction alone
/// is enough to keep everything it needs alive; no static-file or provider handle is required.
///
/// The transaction keeps its MVCC snapshot for as long as this value lives, so callers should drop
/// it promptly once the export finishes to let MDBX reclaim retained pages.
pub struct PinnedStateView {
    /// The pinned read-only transaction. `Send`, and self-sufficient for keeping the env alive.
    tx: PinnedTx,
}

impl PinnedStateView {
    /// Sanity-check that the pin observes the boundary tip the caller expects.
    ///
    /// Performs two point reads against the pinned transaction: `HeaderNumbers[expected.hash]` must
    /// equal `expected.number`, and the highest key in `BlockBodyIndices` (the last executed block)
    /// must also equal `expected.number`. Returns `Ok(false)` on any mismatch so the caller can
    /// skip a stale or unexpected snapshot rather than shipping the wrong state; `Ok(true)` only
    /// when both reads agree with `expected`.
    pub fn verify_tip(&self, expected: BlockNumHash) -> TnRethResult<bool> {
        // the header hash must resolve to the expected number
        match self.tx.get::<HeaderNumbers>(expected.hash).map_err(ProviderError::from)? {
            Some(number) if number == expected.number => {}
            _ => return Ok(false),
        }

        // the highest block-body-indices key is the last executed block, i.e. the pinned tip
        let mut cursor = self.tx.cursor_read::<BlockBodyIndices>().map_err(ProviderError::from)?;
        match cursor.last().map_err(ProviderError::from)? {
            Some((number, _)) if number == expected.number => Ok(true),
            _ => Ok(false),
        }
    }

    /// Stream the full plain EVM state at the pinned view as reth init-state JSONL into `out`.
    ///
    /// Line 1 is `{"root":"0x<state_root>"}` using the caller-supplied `state_root` (this function
    /// does not recompute it; see the module docs). Every subsequent line is one account, emitted
    /// in ascending address order (the `PlainAccountState` cursor order). Accounts and their
    /// storage are produced by a merge join of the `PlainAccountState` and dup-sorted
    /// `PlainStorageState` cursors, walked in lockstep so each table is scanned once; bytecode
    /// is resolved through a lookup memo keyed by code hash. Zero-valued storage slots are
    /// omitted (an absent slot is already zero in the trie, so keeping a zeroed row would only
    /// bloat the dump). Only plain state is written — the restore side rebuilds the hashed,
    /// history, and trie tables itself.
    ///
    /// Returns [`StateExportStats`] describing what was written.
    pub fn export_state_jsonl<W: Write>(
        &self,
        state_root: B256,
        out: W,
    ) -> TnRethResult<StateExportStats> {
        let tx = &self.tx;
        let mut out = CountingWriter::new(out);
        let mut stats = StateExportStats::default();

        // line 1: the caller-supplied expected state root
        serde_json::to_writer(&mut out, &StateRootLine { root: state_root })
            .map_err(|e| write_err(&e))?;
        out.write_all(b"\n").map_err(|e| write_err(&e))?;

        // lookup-only memo: many accounts share bytecode (e.g. proxies). it is only ever point
        // queried, never iterated, so it has no bearing on output order — that comes solely from
        // the account cursor walk below.
        let mut code_cache: HashMap<B256, Bytes> = HashMap::new();

        let mut account_cursor =
            tx.cursor_read::<PlainAccountState>().map_err(ProviderError::from)?;
        let mut storage_cursor =
            tx.cursor_dup_read::<PlainStorageState>().map_err(ProviderError::from)?;

        // merge-join drivers: both tables are keyed by address in the same order, so a single
        // forward pass over each suffices.
        let mut pending_storage = storage_cursor.first().map_err(ProviderError::from)?;
        let mut pending_account = account_cursor.first().map_err(ProviderError::from)?;

        while let Some((address, account)) = pending_account {
            // drain the storage cursor up to and including this account's rows. the pending tuple
            // is Copy, so matching it here does not consume the cursor position we reassign below.
            let mut storage: BTreeMap<B256, B256> = BTreeMap::new();
            while let Some((storage_address, entry)) = pending_storage {
                match storage_address.cmp(&address) {
                    Ordering::Less => {
                        // storage for an address with no plain-account row (db inconsistency); it
                        // contributes no genesis account, so drop it
                        pending_storage = storage_cursor.next().map_err(ProviderError::from)?;
                    }
                    Ordering::Equal => {
                        if !entry.value.is_zero() {
                            storage.insert(entry.key, B256::from(entry.value.to_be_bytes::<32>()));
                            stats.storage_slots += 1;
                        }
                        pending_storage = storage_cursor.next().map_err(ProviderError::from)?;
                    }
                    // storage for a later account; leave it pending
                    Ordering::Greater => break,
                }
            }

            let code = match account.bytecode_hash {
                Some(hash) if hash != KECCAK_EMPTY => {
                    let bytes = match code_cache.get(&hash) {
                        Some(bytes) => bytes.clone(),
                        None => {
                            let bytecode = tx
                                .get::<Bytecodes>(hash)
                                .map_err(ProviderError::from)?
                                .ok_or_else(|| {
                                    TnRethError::Snapshot(format!(
                                        "snapshot export: bytecode {hash} referenced by account \
                                         {address} is missing from the Bytecodes table"
                                    ))
                                })?;
                            let bytes = bytecode.original_bytes();
                            code_cache.insert(hash, bytes.clone());
                            stats.bytecodes += 1;
                            bytes
                        }
                    };
                    Some(bytes)
                }
                _ => None,
            };

            let line = GenesisAccountWithAddress {
                genesis_account: GenesisAccount {
                    nonce: Some(account.nonce),
                    balance: account.balance,
                    code,
                    storage: (!storage.is_empty()).then_some(storage),
                    private_key: None,
                },
                address,
            };
            serde_json::to_writer(&mut out, &line).map_err(|e| write_err(&e))?;
            out.write_all(b"\n").map_err(|e| write_err(&e))?;
            stats.accounts += 1;

            pending_account = account_cursor.next().map_err(ProviderError::from)?;
        }

        stats.bytes_written = out.bytes_written();
        Ok(stats)
    }
}

impl std::fmt::Debug for PinnedStateView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedStateView").finish_non_exhaustive()
    }
}

impl RethEnv {
    /// Open a read-only MDBX transaction pinned to the current tip for snapshot export.
    ///
    /// The transaction is exempted from the long-read-transaction reader-kill (see the module docs)
    /// so a full-state export can run without racing MDBX's reader timeout. The returned
    /// [`PinnedStateView`] is `Send` and self-contained, ready to be handed to a background task.
    pub fn pin_state_view(&self) -> TnRethResult<PinnedStateView> {
        // one RO transaction, exempted from the reader-kill, then unwrapped to the raw tx so the
        // view only carries what it needs (the plain-state tables all live in mdbx, not static
        // files, so no provider/static-file handle has to be kept alive alongside it)
        let tx = self
            .inner
            .blockchain_provider
            .database_provider_ro()?
            .disable_long_read_transaction_safety()
            .into_tx();
        Ok(PinnedStateView { tx })
    }
}

/// Summary of a single [`PinnedStateView::export_state_jsonl`] run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StateExportStats {
    /// Number of accounts written (one JSONL line each, not counting the state-root header line).
    pub accounts: u64,
    /// Number of non-zero storage slots written across all accounts (zero slots are omitted).
    pub storage_slots: u64,
    /// Number of distinct bytecodes written, deduplicated by code hash.
    pub bytecodes: u64,
    /// Total bytes written to `out`, including the state-root header line and every newline.
    pub bytes_written: u64,
}

/// The first JSONL line: the expected state root.
///
/// Structurally mirrors reth's private `StateRoot` so the output parses via that crate's
/// `parse_state_root`.
#[derive(Debug, Serialize, Deserialize)]
struct StateRootLine {
    /// The expected state root, echoed from the caller.
    root: B256,
}

/// One account per JSONL line.
///
/// Structurally mirrors reth's private `GenesisAccountWithAddress`: a flattened [`GenesisAccount`]
/// plus its `address`. Serializing the real [`GenesisAccount`] keeps hex encodings and empty-field
/// omission identical to what `init_from_state_dump` deserializes.
#[derive(Debug, Serialize, Deserialize)]
struct GenesisAccountWithAddress {
    /// The account's nonce, balance, code, and storage.
    #[serde(flatten)]
    genesis_account: GenesisAccount,
    /// The account's address.
    address: Address,
}

/// Map a serialization/IO failure while writing the dump to a [`TnRethError`].
fn write_err(e: &dyn std::fmt::Display) -> TnRethError {
    TnRethError::Snapshot(format!("snapshot export: failed writing state dump: {e}"))
}

/// An [`io::Write`] wrapper that tallies bytes written, so the export can report an exact byte
/// count without a second pass over the output.
struct CountingWriter<W> {
    /// The wrapped writer.
    inner: W,
    /// Running total of bytes forwarded to `inner`.
    bytes: u64,
}

impl<W: Write> CountingWriter<W> {
    /// Wrap `inner` with a zeroed byte counter.
    fn new(inner: W) -> Self {
        Self { inner, bytes: 0 }
    }

    /// The number of bytes written so far.
    fn bytes_written(&self) -> u64 {
        self.bytes
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Rebuilds a state-only snapshot into a fresh, empty datadir.
///
/// The restore side is the inverse of [`PinnedStateView`]: it takes the JSONL state dump produced
/// by [`PinnedStateView::export_state_jsonl`] plus a contiguous window of real headers ending at
/// the snapshot's final block `B`, and reconstructs enough of the reth database that the node can
/// resume at `B` from consensus. It restores STATE only — the pre-`B` block bodies, receipts, and
/// transaction history are never shipped (the node does not re-execute them; it follows consensus
/// forward from `B`).
///
/// # Why this reimplements reth's `setup_without_evm`
///
/// reth ships `reth_cli_commands::init_state::setup_without_evm`, which does almost exactly what
/// [`import_chain_scaffold`](Self::import_chain_scaffold) does, but it is unusable here for two
/// reasons: it fills every dummy header below the tip with a `B256::ZERO` hash, which would break
/// the `BLOCKHASH` opcode and the base-fee walk for blocks inside the shipped window; and pulling
/// it in drags the whole `reth-cli-commands` dependency tree. So the scaffold is reimplemented
/// with real hashes for the window headers and zero-hash dummies only strictly below it.
///
/// # Restore algorithm
///
/// A full restore drives the methods in order:
///
/// 1. [`open`](Self::open) builds the provider factory from the local config and REFUSES a datadir
///    that already holds chain data (restore never overwrites, and genesis — the trust root —
///    always comes from the local chain spec, never the snapshot).
/// 2. [`import_chain_scaffold`](Self::import_chain_scaffold) clears the genesis alloc from the
///    state tables and writes a header-only chain up to `B` (real hashes in the window, zero-hash
///    dummies below it), so the state import has a real `header(B)` to check its recomputed root
///    against.
/// 3. [`import_state`](Self::import_state) streams the dump through reth's
///    [`init_from_state_dump`], which recomputes the state root from scratch and hard-fails on any
///    mismatch with `header(B).state_root`.
/// 4. [`derive_fee_precondition`](Self::derive_fee_precondition) verifies the shipped window is
///    self-sufficient for the node's first epoch-entry base-fee derivation, so the restored node
///    will not walk below the window into history the snapshot omitted and halt.
/// 5. [`finish`](Self::finish) persists the finalized/safe markers at `B` and sanity-checks the
///    reconstructed tip.
#[derive(Debug)]
pub struct SnapshotRestorer {
    /// The execution environment opened over the (initially empty) restore datadir.
    ///
    /// Built once in [`SnapshotRestorer::open`]; its in-memory canonical state stays empty for the
    /// life of the restore (nothing is committed through the canonical path), so every read falls
    /// through to the committed database tip — which is exactly what the post-import fee read at
    /// block `B` needs.
    reth_env: RethEnv,
}

impl SnapshotRestorer {
    /// Open a restorer over an empty datadir, refusing any datadir that already holds chain data.
    ///
    /// Builds the [`RethEnv`] the same way the node does (which initializes genesis from the local
    /// chain spec), then refuses to continue if the canonical tip is above genesis. `init_genesis`
    /// is non-destructive — it early-returns when a matching genesis is already present and errors
    /// on a genesis mismatch — so running it before this emptiness check can never overwrite chain
    /// data; the datadir is untouched at the point this refuses.
    ///
    /// Must be called from within a tokio runtime (the provider factory captures the current
    /// runtime handle).
    pub fn open(
        reth_config: &RethConfig,
        db: RethDb,
        task_manager: &TaskManager,
    ) -> eyre::Result<Self> {
        let reth_env =
            RethEnv::new(reth_config, task_manager, db, None, RewardsCounter::default())?;

        // a fresh or genesis-only datadir sits at block 0; any higher tip means the datadir
        // already holds chain data that restore must not clobber.
        let tip = reth_env.last_block_number()?;
        if tip > 0 {
            return Err(eyre!(
                "refusing to restore into a non-empty datadir: canonical tip is block {tip} \
                 (restore requires an empty datadir)"
            ));
        }

        Ok(Self { reth_env })
    }

    /// Write a header-only chain scaffold up to the snapshot's final block `B`.
    ///
    /// `window` is a contiguous, ascending run of the real headers that end at `final_state`
    /// (block `B`); it must not include genesis. This method, run once before
    /// [`import_state`](Self::import_state):
    ///
    /// - Clears the genesis alloc from `PlainAccountState`, `PlainStorageState`, `HashedAccounts`,
    ///   `HashedStorages`, `AccountsTrie`, and `StoragesTrie`. `init_genesis` (run in
    ///   [`open`](Self::open)) wrote the genesis alloc into these tables, but the dump OMITS
    ///   zero-valued storage slots; a genesis slot later zeroed on-chain would otherwise survive
    ///   the import and diverge the recomputed state root. `Bytecodes` and the genesis header are
    ///   deliberately kept.
    /// - Appends headers `1..=B-1` to the Headers static file: the real header (with its real hash)
    ///   for every block inside `window`, and a zero-hash dummy for every block below it. Real
    ///   hashes inside the window keep `BLOCKHASH` and the base-fee walk resolvable.
    /// - Advances the transactions/receipts/senders static-file segments to `B-1`, inserts block
    ///   `B` itself with an empty body (advancing headers and transactions to `B`), and advances
    ///   receipts to `B` — mirroring reth's own dummy-chain shaping so the segments stay internally
    ///   consistent.
    /// - Writes `HeaderNumbers` (hash → number) for every window header so `BLOCKHASH` and the fee
    ///   walk resolve by hash, and sets every stage checkpoint to `B`.
    ///
    /// Static files are committed BEFORE the database provider: the refuse-non-empty check in
    /// [`open`](Self::open) keys on the Headers static-file height, so committing static files
    /// first means a crash mid-scaffold leaves a datadir the next `open` correctly rejects.
    pub fn import_chain_scaffold(
        &self,
        window: &[SealedHeader],
        final_state: BlockNumHash,
    ) -> eyre::Result<()> {
        let header_b = window
            .last()
            .ok_or_else(|| eyre!("snapshot restore: cannot scaffold an empty header window"))?;
        let b = final_state.number;

        // cheap invariant checks (the window is validated upstream, but a mis-shaped window here
        // would silently corrupt the scaffold)
        if b == 0 {
            return Err(eyre!("snapshot restore: cannot scaffold at genesis (block 0)"));
        }
        if header_b.number != b || header_b.hash() != final_state.hash {
            return Err(eyre!(
                "snapshot restore: window tip {}:{} does not match final_state {b}:{}",
                header_b.number,
                header_b.hash(),
                final_state.hash
            ));
        }
        let window_start = window[0].number;
        if window_start == 0 {
            return Err(eyre!("snapshot restore: window must not include genesis (block 0)"));
        }
        for (i, header) in window.iter().enumerate() {
            let expected = window_start + i as u64;
            if header.number != expected {
                return Err(eyre!(
                    "snapshot restore: window is not contiguous at index {i}: expected block \
                     {expected}, got {}",
                    header.number
                ));
            }
        }

        let provider_rw = self.reth_env.inner.blockchain_provider.database_provider_rw()?;

        // drop the genesis alloc so an on-chain-zeroed genesis slot cannot survive the import
        {
            let tx = provider_rw.tx_ref();
            tx.clear::<PlainAccountState>()?;
            tx.clear::<PlainStorageState>()?;
            tx.clear::<HashedAccounts>()?;
            tx.clear::<HashedStorages>()?;
            tx.clear::<AccountsTrie>()?;
            tx.clear::<StoragesTrie>()?;
        }

        let sf = provider_rw.static_file_provider();

        // resolve the real header for a block number inside the window, if any
        let header_at = |number: u64| -> Option<&SealedHeader> {
            (window_start..=b).contains(&number).then(|| &window[(number - window_start) as usize])
        };

        // headers 1..=B-1: real headers with real hashes inside the window, zero-hash dummies
        // below it. the dummies are never referenced by BLOCKHASH within the shipped window, so
        // a zero hash there is inert.
        {
            let mut headers_writer = sf.latest_writer(StaticFileSegment::Headers)?;
            for number in 1..b {
                match header_at(number) {
                    Some(header) => {
                        headers_writer.append_header(header.header(), &header.hash())?
                    }
                    None => {
                        let dummy = ExecHeader { number, ..Default::default() };
                        headers_writer.append_header(&dummy, &B256::ZERO)?;
                    }
                }
            }
        }

        // advance the body-bearing segments to B-1 so insert_block(B) and the receipt increment
        // below land on the right block. only touch segments that exist (senders may live in mdbx
        // rather than a static file, in which case it is skipped here).
        for segment in [
            StaticFileSegment::Transactions,
            StaticFileSegment::Receipts,
            StaticFileSegment::TransactionSenders,
        ] {
            if sf.get_highest_static_file_block(segment).is_none() {
                continue;
            }
            let mut writer = sf.latest_writer(segment)?;
            for number in 1..b {
                writer.increment_block(number)?;
            }
        }

        // block B: the real header with an empty body. advances headers and transactions to B and
        // writes HeaderNumbers[hash_B]=B and BlockBodyIndices[B].
        let sealed_b: SealedBlock =
            SealedBlock::from_sealed_parts(header_b.clone(), BlockBody::default());
        let block_b = sealed_b
            .try_recover()
            .map_err(|e| eyre!("snapshot restore: failed to recover scaffold block {b}: {e:?}"))?;
        provider_rw.insert_block(&block_b)?;

        // receipts to B (insert_block skips receipts in blocks-only mode)
        if sf.get_highest_static_file_block(StaticFileSegment::Receipts).is_some() {
            sf.latest_writer(StaticFileSegment::Receipts)?.increment_block(b)?;
        }

        // HeaderNumbers for every window header so BLOCKHASH and the fee walk resolve by hash
        // (insert_block already covered block B; the rest are written here)
        {
            let tx = provider_rw.tx_ref();
            for header in window {
                tx.put::<HeaderNumbers>(header.hash(), header.number)?;
            }
        }

        // set every stage checkpoint to B so the datadir is shaped like a normally-synced one
        for stage in StageId::ALL {
            provider_rw.save_stage_checkpoint(stage, StageCheckpoint::new(b))?;
        }

        // static files first (see the refuse-non-empty rationale in the doc comment), then the db
        sf.commit()?;
        provider_rw.commit()?;

        Ok(())
    }

    /// Import the plain-state dump into the scaffolded datadir and return the recomputed state
    /// root.
    ///
    /// Delegates to reth's [`init_from_state_dump`], which reads the tip (`B`) written by
    /// [`import_chain_scaffold`](Self::import_chain_scaffold), checks the dump's declared root
    /// against `header(B).state_root`, ETL-sorts the accounts to disk, writes them, then recomputes
    /// the state root FROM SCRATCH and hard-fails on any mismatch. `etl_dir` is the scratch
    /// directory for the ETL sort. The returned root is `header(B).state_root` — equal to the
    /// recomputed root by the function's success contract.
    pub fn import_state(&self, dump: impl BufRead, etl_dir: PathBuf) -> eyre::Result<B256> {
        let provider_rw = self.reth_env.inner.blockchain_provider.database_provider_rw()?;
        let etl_config = EtlConfig::new(Some(etl_dir), EtlConfig::default_file_size());

        // returns block B's hash; the recomputed root it validated equals header(B).state_root
        let block_hash = init_from_state_dump(dump, &provider_rw, etl_config)?;
        provider_rw.commit()?;

        let header = self.reth_env.header(block_hash)?.ok_or_else(|| {
            eyre!("snapshot restore: header for imported block {block_hash} not found after commit")
        })?;
        Ok(header.state_root)
    }

    /// Verify the shipped window can seed the restored node's first epoch-entry base fees.
    ///
    /// At epoch entry the node derives each worker's base fee from the previous epoch's genuine
    /// blocks; a worker with an `Eip1559` config that produced no genuine block in that epoch has
    /// no chain-observable fee anchor and the node walks BACKWARD through earlier epochs to find
    /// one. A restored node only has the shipped `window`, so such a walk would run off the bottom
    /// of the window into pre-snapshot history the snapshot omitted, and halt.
    ///
    /// This mirrors that attribution: it reads the worker fee configs at block `B` (the closing
    /// block of the epoch below `entered`, which defines `entered`'s configuration) against the
    /// post-import state, then requires every `Eip1559` worker to have produced at least one
    /// genuine batch block within `window`. `Static` workers need no anchor — their fee is pinned
    /// by config. Errors name the offending worker. Must run AFTER
    /// [`import_state`](Self::import_state), since it reads contract state at `B`.
    pub fn derive_fee_precondition(
        &self,
        entered: Epoch,
        window: &[SealedHeader],
    ) -> eyre::Result<()> {
        let closing = window
            .last()
            .ok_or_else(|| eyre!("snapshot restore: cannot check fees over an empty window"))?;

        // worker strategies at the closing block define the entered epoch's configuration
        let (num_workers, configs) =
            self.reth_env.get_worker_fee_configs_at_block(closing.hash()).wrap_err(
                "snapshot restore: reading worker fee configs at the snapshot's final block",
            )?;

        // workers with a genuine batch block inside the shipped window
        let mut produced: BTreeSet<WorkerId> = BTreeSet::new();
        for header in window {
            if is_worker_batch_block(header) {
                produced.insert(worker_id_from_header(header));
            }
        }

        let prior = entered.saturating_sub(1);
        for (worker_id, config) in configs.iter().enumerate() {
            let worker_id = worker_id as WorkerId;
            if matches!(config, WorkerFeeConfig::Eip1559 { .. }) && !produced.contains(&worker_id) {
                return Err(eyre!(
                    "snapshot restore: worker {worker_id} has an EIP-1559 fee config but produced \
                     no genuine block in the shipped epoch-{prior} window; the restored node would \
                     walk into pre-snapshot history deriving its base fee when entering epoch \
                     {entered} and halt"
                ));
            }
        }

        debug!(
            target: "tn::reth",
            entered,
            num_workers,
            produced = produced.len(),
            "snapshot restore: fee precondition satisfied"
        );
        Ok(())
    }

    /// Persist the finalized/safe markers at `B` and sanity-check the reconstructed tip.
    ///
    /// The restored node reads these markers on startup to place its finalized/safe blocks (the
    /// node persists them the same way after finalizing a block), so the restore is not complete
    /// until they point at `B`. After committing them, this verifies the persisted tip and its
    /// hash match `final_state`; a mismatch means the scaffold or import produced a chain other
    /// than the one the snapshot claimed.
    pub fn finish(self, final_state: BlockNumHash) -> eyre::Result<()> {
        let b = final_state.number;

        {
            let provider = self.reth_env.inner.blockchain_provider.database_provider_rw()?;
            provider.save_finalized_block_number(b)?;
            provider.save_safe_block_number(b)?;
            provider.commit()?;
        }

        let tip = self.reth_env.last_block_number()?;
        if tip != b {
            return Err(eyre!("snapshot restore: reconstructed tip is block {tip}, expected {b}"));
        }
        let sealed = self.reth_env.sealed_header_by_number(b)?.ok_or_else(|| {
            eyre!("snapshot restore: no sealed header at final block {b} after restore")
        })?;
        if sealed.hash() != final_state.hash {
            return Err(eyre!(
                "snapshot restore: reconstructed block {b} has hash {}, expected {}",
                sealed.hash(),
                final_state.hash
            ));
        }

        Ok(())
    }
}

/// The worker id encoded in a header's `difficulty` (low 16 bits).
///
/// Mirrors the canonical `worker_id_from_header` in `tn_node::manager::node`; kept in sync with the
/// encoding used by the gas accumulator's block attribution. Public so the snapshot uploader's
/// fee-derivability precheck attributes headers with the exact same rule the restore-side
/// [`SnapshotRestorer::derive_fee_precondition`] uses — one implementation, no drift.
pub fn worker_id_from_header(header: &SealedHeader) -> WorkerId {
    (header.difficulty.into_limbs()[0] & 0xffff) as u16
}

/// True when `header` is a genuine worker batch block.
///
/// Mirrors the canonical `is_worker_batch_block` in `tn_node::manager::node`: excludes genesis
/// (`number == 0`) and the synthetic empty-close block, whose `ommers_hash` (the batch-digest slot)
/// is zero. Only genuine batch blocks carry a non-zero batch digest. Public so the snapshot
/// uploader's fee-derivability precheck shares the exact attribution rule the restore-side
/// [`SnapshotRestorer::derive_fee_precondition`] applies.
pub fn is_worker_batch_block(header: &SealedHeader) -> bool {
    header.number != 0 && header.ommers_hash != B256::ZERO
}

#[cfg(test)]
mod tests {
    use super::{
        is_worker_batch_block, worker_id_from_header, GenesisAccountWithAddress, PinnedStateView,
        SnapshotRestorer, StateRootLine,
    };
    use crate::{MaybePlatformPath, RethChainSpec, RethConfig, RethDb, RethEnv};
    use alloy::hex;
    use reth::{args::DatadirArgs, builder::NodeConfig};
    use reth_db::{cursor::DbCursorRW, tables::PlainStorageState, transaction::DbTxMut};
    use reth_primitives_traits::StorageEntry;
    use reth_provider::{AccountReader, DBProvider, DatabaseProviderFactory, StateProvider};
    use std::{
        collections::{BTreeMap, BTreeSet},
        io::BufReader,
        path::Path,
        sync::Arc,
    };
    use tempfile::TempDir;
    use tn_types::{
        gas_accumulator::RewardsCounter, test_genesis, Address, BlockNumHash, Bytes, ExecHeader,
        GenesisAccount, SealedHeader, TaskManager, B256, U256,
    };

    /// Compile-time proof that a pinned view can be moved into a background upload task.
    fn assert_send<T: Send>() {}

    /// A 32-byte storage word holding the integer `n`.
    fn word(n: u64) -> B256 {
        B256::from(U256::from(n).to_be_bytes::<32>())
    }

    /// Some non-trivial contract bytecode (`PUSH1 0 PUSH1 0 STOP`).
    const CODE: &[u8] = &[0x60, 0x00, 0x60, 0x00, 0x00];

    /// Directly upsert a single `(address, slot)` storage row so tests can plant rows that genesis
    /// init would not (notably a zero-valued slot, which `insert_state` treats as a no-op change).
    fn upsert_storage(
        reth_env: &RethEnv,
        address: Address,
        slot: B256,
        value: U256,
    ) -> eyre::Result<()> {
        let provider = reth_env.inner.blockchain_provider.database_provider_rw()?;
        {
            let tx = provider.tx_ref();
            let mut cursor = tx.cursor_dup_write::<PlainStorageState>()?;
            cursor.upsert(address, &StorageEntry::new(slot, value))?;
        }
        provider.commit()?;
        Ok(())
    }

    #[tokio::test]
    async fn export_roundtrips_accounts_storage_and_code() -> eyre::Result<()> {
        let eoa = Address::from([0x0a; 20]);
        let contract = Address::from([0x0c; 20]);
        // shares the same code as `contract`, exercising the bytecode memo
        let contract_twin = Address::from([0x0d; 20]);
        let code: Bytes = Bytes::from_static(CODE);

        let (slot_a, slot_b, slot_zero) =
            (B256::from([0x01; 32]), B256::from([0x02; 32]), B256::from([0x03; 32]));

        let genesis = test_genesis().extend_accounts([
            (
                eoa,
                GenesisAccount {
                    nonce: Some(7),
                    balance: U256::from(1_000u64),
                    code: None,
                    storage: None,
                    private_key: None,
                },
            ),
            (
                contract,
                GenesisAccount {
                    nonce: Some(1),
                    balance: U256::from(42u64),
                    code: Some(code.clone()),
                    // slot_b listed before slot_a to prove output is sorted by slot, not insertion
                    storage: Some(BTreeMap::from([(slot_b, word(222)), (slot_a, word(111))])),
                    private_key: None,
                },
            ),
            (
                contract_twin,
                GenesisAccount {
                    nonce: Some(2),
                    balance: U256::from(84u64),
                    code: Some(code.clone()),
                    storage: Some(BTreeMap::from([(slot_a, word(999))])),
                    private_key: None,
                },
            ),
        ]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Snapshot Export Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // plant a zero-valued slot on `contract`; genesis would not persist one
        upsert_storage(&reth_env, contract, slot_zero, U256::ZERO)?;

        let expected_root = B256::repeat_byte(0xab);
        let view = reth_env.pin_state_view()?;
        let mut buf = Vec::new();
        let stats = view.export_state_jsonl(expected_root, &mut buf)?;

        let text = String::from_utf8(buf.clone())?;
        let mut lines = text.lines();

        // line 1: the caller-supplied root, echoed verbatim
        let root_line: StateRootLine = serde_json::from_str(lines.next().expect("root line"))?;
        assert_eq!(root_line.root, expected_root);

        // remaining lines parse back through the reth-shaped account type
        let mut accounts: BTreeMap<Address, GenesisAccount> = BTreeMap::new();
        let mut raw_lines: BTreeMap<Address, String> = BTreeMap::new();
        for line in lines {
            let parsed: GenesisAccountWithAddress = serde_json::from_str(line)?;
            raw_lines.insert(parsed.address, line.to_string());
            assert!(
                accounts.insert(parsed.address, parsed.genesis_account).is_none(),
                "each account must appear exactly once"
            );
        }

        // eoa round-trips: balance and nonce preserved, no code, no storage
        let eoa_account = accounts.get(&eoa).expect("eoa exported");
        assert_eq!(eoa_account.balance, U256::from(1_000u64));
        assert_eq!(eoa_account.nonce, Some(7));
        assert_eq!(eoa_account.code, None);
        assert_eq!(eoa_account.storage, None);

        // contract round-trips: code, balance, nonce, and only the non-zero slots (sorted)
        let contract_account = accounts.get(&contract).expect("contract exported");
        assert_eq!(contract_account.balance, U256::from(42u64));
        assert_eq!(contract_account.nonce, Some(1));
        assert_eq!(contract_account.code, Some(code.clone()));
        assert_eq!(
            contract_account.storage,
            Some(BTreeMap::from([(slot_a, word(111)), (slot_b, word(222))])),
            "the zero slot must be omitted; the rest survive"
        );

        // storage is written in ascending slot order (dup-sort walk order)
        let contract_line = raw_lines.get(&contract).expect("contract line");
        let pos_a = contract_line.find(&hex::encode(slot_a)).expect("slot_a present");
        let pos_b = contract_line.find(&hex::encode(slot_b)).expect("slot_b present");
        assert!(pos_a < pos_b, "slots must be emitted in ascending order");
        assert!(
            !contract_line.contains(&hex::encode(slot_zero)),
            "zero-valued slot must not appear in the output"
        );

        // twin shares bytecode with `contract`: the memo resolves it to the same code
        let twin_account = accounts.get(&contract_twin).expect("twin exported");
        assert_eq!(twin_account.code, Some(code.clone()));

        // stats are self-consistent with the parsed output
        assert_eq!(stats.accounts as usize, accounts.len());
        let total_slots: usize =
            accounts.values().map(|a| a.storage.as_ref().map_or(0, |s| s.len())).sum();
        assert_eq!(stats.storage_slots as usize, total_slots);
        let distinct_codes: BTreeSet<Bytes> =
            accounts.values().filter_map(|a| a.code.clone()).collect();
        assert_eq!(stats.bytecodes as usize, distinct_codes.len());
        assert_eq!(stats.bytes_written as usize, buf.len());

        Ok(())
    }

    #[tokio::test]
    async fn verify_tip_matches_only_the_real_tip() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Snapshot Verify Tip Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // a fresh temp chain sits at genesis (block 0)
        let genesis_hash = reth_env.sealed_header_by_number(0)?.expect("genesis header").hash();
        let view = reth_env.pin_state_view()?;

        assert!(view.verify_tip(BlockNumHash::new(0, genesis_hash))?, "real tip matches");
        assert!(
            !view.verify_tip(BlockNumHash::new(0, B256::repeat_byte(0xff)))?,
            "wrong hash is rejected"
        );
        assert!(
            !view.verify_tip(BlockNumHash::new(999, genesis_hash))?,
            "wrong number is rejected"
        );

        Ok(())
    }

    #[test]
    fn pinned_state_view_is_send() {
        // the uploader moves the view into a background task, so it must be Send
        assert_send::<PinnedStateView>();
    }

    /// Build the `(RethConfig, RethDb)` pair a [`SnapshotRestorer`] needs over a temp datadir,
    /// mirroring how `RethEnv::new_for_temp_chain` shapes a node config.
    fn temp_config_and_db(
        chain: Arc<RethChainSpec>,
        path: &Path,
    ) -> eyre::Result<(RethConfig, RethDb)> {
        let node_config = NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(path.to_path_buf()),
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            ..NodeConfig::default()
        };
        // RethConfig's inner NodeConfig is private, but this test module is a descendant of the
        // crate root, so the tuple constructor is in scope.
        let reth_config = RethConfig(node_config);
        let db = RethEnv::new_database(&reth_config, path)?;
        Ok((reth_config, db))
    }

    /// A sealed header for a synthetic window block: `state_root` is set to the snapshot's exported
    /// root so the import's from-scratch recompute matches, and a non-zero `ommers_hash` marks it a
    /// genuine batch block attributed to `worker_id`.
    fn synthetic_header(
        number: u64,
        parent: B256,
        state_root: B256,
        worker_id: u16,
        genuine: bool,
    ) -> SealedHeader {
        let header = ExecHeader {
            number,
            parent_hash: parent,
            state_root,
            difficulty: U256::from(worker_id as u64),
            ommers_hash: if genuine { B256::repeat_byte(0xbb) } else { B256::ZERO },
            ..Default::default()
        };
        SealedHeader::seal_slow(header)
    }

    #[tokio::test]
    async fn restore_roundtrips_state_from_export() -> eyre::Result<()> {
        let eoa = Address::from([0x0a; 20]);
        let contract = Address::from([0x0c; 20]);
        let code: Bytes = Bytes::from_static(CODE);
        let (slot_a, slot_b) = (B256::from([0x01; 32]), B256::from([0x02; 32]));

        let genesis = test_genesis().extend_accounts([
            (
                eoa,
                GenesisAccount {
                    nonce: Some(7),
                    balance: U256::from(1_000u64),
                    code: None,
                    storage: None,
                    private_key: None,
                },
            ),
            (
                contract,
                GenesisAccount {
                    nonce: Some(1),
                    balance: U256::from(42u64),
                    code: Some(code.clone()),
                    storage: Some(BTreeMap::from([(slot_a, word(111)), (slot_b, word(222))])),
                    private_key: None,
                },
            ),
        ]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // source: export the genesis state at its real root
        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Restore Roundtrip Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;
        let genesis_header = source.sealed_header_by_number(0)?.expect("genesis header");
        let state_root = genesis_header.state_root;
        let mut dump = Vec::new();
        source.pin_state_view()?.export_state_jsonl(state_root, &mut dump)?;

        // destination: restore into a fresh datadir with the same genesis
        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Restore Roundtrip Dest");
        let (reth_config, db) = temp_config_and_db(chain.clone(), dst_dir.path())?;
        let etl_dir = TempDir::new()?;

        // a contiguous window of real headers ending at block B, all pinning the exported root
        let b = 3u64;
        let h1 = synthetic_header(1, genesis_header.hash(), state_root, 0, true);
        let h2 = synthetic_header(2, h1.hash(), state_root, 0, true);
        let h3 = synthetic_header(3, h2.hash(), state_root, 0, true);
        let window = vec![h1, h2, h3];
        let final_state = BlockNumHash::new(b, window[2].hash());

        let restorer = SnapshotRestorer::open(&reth_config, db.clone(), &dst_tm)?;
        restorer.import_chain_scaffold(&window, final_state)?;
        let root =
            restorer.import_state(BufReader::new(dump.as_slice()), etl_dir.path().to_path_buf())?;
        assert_eq!(root, state_root, "recomputed root must equal header(B).state_root");
        restorer.finish(final_state)?;

        // read the restored state back through a fresh env over the destination datadir
        let reader = RethEnv::new(&reth_config, &dst_tm, db, None, RewardsCounter::default())?;
        assert_eq!(reader.last_block_number()?, b);
        assert_eq!(
            reader.sealed_header_by_number(b)?.expect("tip header").hash(),
            final_state.hash,
            "block_hash(B) must match the snapshot's final block"
        );

        let state = reader.latest()?;
        assert_eq!(state.account_balance(&eoa)?, Some(U256::from(1_000u64)));
        assert_eq!(state.basic_account(&eoa)?.expect("eoa restored").nonce, 7);
        assert_eq!(state.storage(contract, slot_a)?, Some(U256::from(111u64)));
        assert_eq!(state.storage(contract, slot_b)?, Some(U256::from(222u64)));
        assert_eq!(
            state.account_code(&contract)?.map(|c| c.original_bytes()),
            Some(code),
            "deployed code must round-trip"
        );

        Ok(())
    }

    #[tokio::test]
    async fn restore_clears_genesis_zeroed_slots() -> eyre::Result<()> {
        let contract = Address::from([0x0c; 20]);
        let code: Bytes = Bytes::from_static(CODE);
        let slot_a = B256::from([0x01; 32]);
        // nonzero in the genesis alloc, but zeroed on-chain by block B (so the dump omits it)
        let slot_s = B256::from([0x05; 32]);

        // source genesis holds the block-B shape (WITHOUT slot_s); its root is the dump's root
        let source_genesis = test_genesis().extend_accounts([(
            contract,
            GenesisAccount {
                nonce: Some(1),
                balance: U256::from(42u64),
                code: Some(code.clone()),
                storage: Some(BTreeMap::from([(slot_a, word(111))])),
                private_key: None,
            },
        )]);
        let source_chain: Arc<RethChainSpec> = Arc::new(source_genesis.into());
        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Genesis Zeroed Slot Source");
        let source =
            RethEnv::new_for_temp_chain(source_chain.clone(), src_dir.path(), &src_tm, None)?;
        let root_without_s = source.sealed_header_by_number(0)?.expect("genesis").state_root;
        let mut dump = Vec::new();
        source.pin_state_view()?.export_state_jsonl(root_without_s, &mut dump)?;

        // destination genesis holds slot_s nonzero: init_genesis writes it, and the scaffold must
        // clear it or it would survive the import (the dump omits it) and diverge the root
        let dest_genesis = test_genesis().extend_accounts([(
            contract,
            GenesisAccount {
                nonce: Some(1),
                balance: U256::from(42u64),
                code: Some(code.clone()),
                storage: Some(BTreeMap::from([(slot_a, word(111)), (slot_s, word(999))])),
                private_key: None,
            },
        )]);
        let dest_chain: Arc<RethChainSpec> = Arc::new(dest_genesis.into());
        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Genesis Zeroed Slot Dest");
        let (reth_config, db) = temp_config_and_db(dest_chain.clone(), dst_dir.path())?;
        let etl_dir = TempDir::new()?;

        let b = 1u64;
        let parent = dest_chain.sealed_genesis_header().hash();
        let header_b = synthetic_header(b, parent, root_without_s, 0, true);
        let window = vec![header_b];
        let final_state = BlockNumHash::new(b, window[0].hash());

        let restorer = SnapshotRestorer::open(&reth_config, db.clone(), &dst_tm)?;
        restorer.import_chain_scaffold(&window, final_state)?;
        // import succeeds ONLY because the scaffold cleared slot_s: otherwise the recompute would
        // see {slot_a, slot_s} and fail the root check against root_without_s.
        let root =
            restorer.import_state(BufReader::new(dump.as_slice()), etl_dir.path().to_path_buf())?;
        assert_eq!(root, root_without_s);
        restorer.finish(final_state)?;

        let reader = RethEnv::new(&reth_config, &dst_tm, db, None, RewardsCounter::default())?;
        let state = reader.latest()?;
        assert!(
            state.storage(contract, slot_s)?.unwrap_or_default().is_zero(),
            "the genesis-zeroed slot must read as zero after restore"
        );
        assert_eq!(
            state.storage(contract, slot_a)?,
            Some(U256::from(111u64)),
            "the surviving slot must still be present"
        );

        Ok(())
    }

    #[test]
    fn worker_attribution_helpers_match_header_encoding() {
        // genesis (block 0) is never a genuine batch block
        let genesis = synthetic_header(0, B256::ZERO, B256::ZERO, 0, true);
        assert!(!is_worker_batch_block(&genesis));

        // the synthetic empty-close block carries a zero ommers_hash (batch-digest slot)
        let empty_close = synthetic_header(5, B256::ZERO, B256::ZERO, 2, false);
        assert!(!is_worker_batch_block(&empty_close));

        // a real batch block has a non-zero ommers_hash and a worker id in difficulty's low bits
        let genuine = synthetic_header(5, B256::ZERO, B256::ZERO, 3, true);
        assert!(is_worker_batch_block(&genuine));
        assert_eq!(worker_id_from_header(&genuine), 3);
    }

    #[tokio::test]
    async fn open_refuses_non_empty_datadir() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let dir = TempDir::new()?;
        let tm = TaskManager::new("Refuse Non-Empty");
        let (reth_config, db) = temp_config_and_db(chain.clone(), dir.path())?;

        // scaffold one block so the datadir holds chain data above genesis
        let genesis_header = chain.sealed_genesis_header();
        let header_b =
            synthetic_header(1, genesis_header.hash(), genesis_header.state_root, 0, true);
        let window = vec![header_b];
        let final_state = BlockNumHash::new(1, window[0].hash());
        {
            let restorer = SnapshotRestorer::open(&reth_config, db.clone(), &tm)?;
            restorer.import_chain_scaffold(&window, final_state)?;
        }

        // a second open over the now-populated datadir must refuse rather than overwrite
        let err = SnapshotRestorer::open(&reth_config, db, &tm)
            .expect_err("open must refuse a datadir that already holds chain data");
        assert!(err.to_string().contains("non-empty"), "unexpected error: {err}");

        Ok(())
    }
}
