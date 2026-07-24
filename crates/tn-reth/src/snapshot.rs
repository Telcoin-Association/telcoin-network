//! Snapshot export and restore for reth's plain EVM state.
//!
//! This module has two sides. The export side ([`PinnedStateView`]) streams a state-only dump out
//! of a live node into an [exec state pack](tn_storage::exec_state_pack); the restore side
//! ([`SnapshotRestorer`]) rebuilds that pack into a fresh datadir. The
//! [restore documentation](SnapshotRestorer) covers the rebuild algorithm; the rest of these module
//! docs describe the export side.
//!
//! # Export: pinned, read-consistent view over reth's plain EVM state
//!
//! [`RethEnv::pin_state_view`] opens a single reth `Tx<RO>` on the MDBX environment and calls
//! `disable_long_read_transaction_safety()` on it. MDBX enforces a reader timeout (~5 minutes) that
//! kills long-lived read transactions to keep the free list from stalling; a full-state export can
//! easily run longer than that. Disabling the guard exempts *only this one transaction* — it does
//! not touch any env-wide configuration, so other readers and the writer keep their normal safety
//! limits.
//!
//! The returned [`PinnedStateView`] owns the transaction (which itself keeps the MDBX environment
//! alive) and is [`Send`], so an uploader can move it into a background task and stream the export
//! off the hot path.
//!
//! # Storage format is the exec state pack (not JSONL)
//!
//! [`PinnedStateView::export_state_pack`] writes an [`ExecStatePackWriter`]: the caller-supplied
//! state root and block header(s), then one [`ExecStateAccount`] per plain-state account. The pack
//! is a compact, CRC32- and zstd-framed binary artifact (see `tn_storage::exec_state_pack`) — it
//! does not go through reth's private JSONL `init_from_state_dump` path at all. The restore side
//! rebuilds state from the pack directly using reth's *public* state-insertion and trie building
//! blocks.
//!
//! # Why the state root is not recomputed on export
//!
//! [`PinnedStateView::export_state_pack`] takes the expected state root as a parameter and simply
//! records it. It deliberately does not recompute the root from the pinned transaction: a from-tx
//! recompute on a live database just re-reads the already-cached trie tables and proves nothing
//! about the plain-state rows we actually stream. The restore side is the real check — it rebuilds
//! the trie from scratch out of the accounts in the pack and hard-fails on any mismatch.

use crate::{
    error::{TnRethError, TnRethResult},
    DatabaseEnv, RethConfig, RethDatabaseT, RethDb, RethEnv,
};
use alloy::consensus::constants::KECCAK_EMPTY;
use eyre::{eyre, WrapErr as _};
use reth_db::{
    cursor::DbCursorRO,
    tables::{
        AccountsTrie, BlockBodyIndices, Bytecodes, HashedAccounts, HashedStorages, HeaderNumbers,
        PlainAccountState, PlainStorageState, StoragesTrie,
    },
    transaction::{DbTx, DbTxMut},
};
use reth_db_common::init::{insert_genesis_hashes, insert_history, insert_state};
use reth_provider::{
    BlockWriter, ChainStateBlockWriter, DBProvider, DatabaseProviderFactory, ProviderError,
    StageCheckpointWriter, StaticFileProviderFactory, StaticFileSegment, StaticFileWriter,
    TrieWriter,
};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_trie::{IntermediateStateRootState, StateRoot, StateRootProgress};
use reth_trie_db::DatabaseStateRoot;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
};
use tn_storage::exec_state_pack::{
    ExecStateAccount, ExecStatePackReader, ExecStatePackWriter, ExecStateStats,
};
use tn_types::{
    gas_accumulator::{RewardsCounter, WorkerFeeConfig},
    Address, BlockBody, BlockNumHash, Bytes, Epoch, ExecHeader, GenesisAccount, SealedBlock,
    SealedHeader, TaskManager, WorkerId, B256,
};
use tracing::{debug, warn};

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

    /// Stream the full plain EVM state at the pinned view into an exec state pack in `out_dir`.
    ///
    /// The pack records the caller-supplied `state_root` and `headers` (snapshot header first, then
    /// recent ancestors — the snapshot header's `state_root` must equal `state_root`), followed by
    /// one account each, emitted in ascending address order (the `PlainAccountState` cursor order).
    /// Accounts and their storage are produced by a merge join of the `PlainAccountState` and
    /// dup-sorted `PlainStorageState` cursors, walked in lockstep so each table is scanned once;
    /// bytecode is resolved through a lookup memo keyed by code hash. Zero-valued storage slots are
    /// omitted (an absent slot is already zero in the trie, so keeping a zeroed row would only
    /// bloat the pack). Only plain state is written — the restore side rebuilds the hashed,
    /// history, and trie tables itself.
    ///
    /// Returns the [`ExecStateStats`] the writer accumulated.
    pub fn export_state_pack(
        &self,
        state_root: B256,
        headers: &[ExecHeader],
        out_dir: &Path,
    ) -> TnRethResult<ExecStateStats> {
        let tx = &self.tx;
        let mut writer = ExecStatePackWriter::create(out_dir, state_root, headers)
            .map_err(|e| TnRethError::Snapshot(format!("failed to create state pack: {e}")))?;

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
                        // contributes no account, so drop it
                        pending_storage = storage_cursor.next().map_err(ProviderError::from)?;
                    }
                    Ordering::Equal => {
                        if !entry.value.is_zero() {
                            storage.insert(entry.key, B256::from(entry.value.to_be_bytes::<32>()));
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
                                        "bytecode {hash} referenced by account {address} is \
                                         missing from the Bytecodes table"
                                    ))
                                })?;
                            let bytes = bytecode.original_bytes();
                            code_cache.insert(hash, bytes.clone());
                            bytes
                        }
                    };
                    Some(bytes)
                }
                _ => None,
            };

            let account = ExecStateAccount {
                address,
                account: GenesisAccount {
                    nonce: Some(account.nonce),
                    balance: account.balance,
                    code,
                    storage: (!storage.is_empty()).then_some(storage),
                    private_key: None,
                },
            };
            writer.append_account(&account).map_err(|e| {
                TnRethError::Snapshot(format!("failed to append account {}: {e}", account.address))
            })?;

            pending_account = account_cursor.next().map_err(ProviderError::from)?;
        }

        writer
            .finish()
            .map_err(|e| TnRethError::Snapshot(format!("failed to finish state pack: {e}")))
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

/// Number of accounts [`SnapshotRestorer::import_state`] buffers before flushing a chunk to reth's
/// state tables.
///
/// Mirrors reth's `AVERAGE_COUNT_ACCOUNTS_PER_GB_STATE_DUMP` (its `dump_state` batches on the same
/// bound): roughly one gigabyte of state per chunk, so the resident account set and the reth
/// `BundleState` mirror stay near that ceiling instead of scaling with the whole chain.
const IMPORT_ACCOUNT_CHUNK: usize = 285_228;

/// Rebuilds a state-only snapshot into a fresh, empty datadir.
///
/// The restore side is the inverse of [`PinnedStateView`]: it takes the exec state pack produced by
/// [`PinnedStateView::export_state_pack`] plus a contiguous window of real headers ending at the
/// snapshot's final block `B`, and reconstructs enough of the reth database that the node can
/// resume at `B` from consensus. It restores STATE only — the pre-`B` block bodies, receipts, and
/// transaction history are never shipped (the node does not re-execute them; it follows consensus
/// forward from `B`).
///
/// # Why restore does not use reth's `init_from_state_dump`
///
/// reth's `init_from_state_dump` is private and JSONL-only. Instead,
/// [`import_state`](Self::import_state) streams the pack's accounts through reth's *public*
/// state-insertion building blocks ([`insert_state`], [`insert_genesis_hashes`],
/// [`insert_history`]) in bounded chunks and recomputes the state root from scratch with
/// [`StateRoot`] — the same tables and the same from-scratch check `init_from_state_dump` performs
/// internally, without the JSONL parser.
///
/// # Why this reimplements reth's `setup_without_evm`
///
/// reth ships `reth_cli_commands::init_state::setup_without_evm`, which does almost exactly what
/// [`import_chain_scaffold`](Self::import_chain_scaffold) does, but it is unusable here for two
/// reasons: it fills every dummy header below the tip with a `B256::ZERO` hash, which would break
/// the `BLOCKHASH` opcode and the base-fee walk for blocks inside the shipped window; and pulling
/// it in drags the whole `reth-cli-commands` dependency tree. So the scaffold is reimplemented with
/// real hashes for the window headers and zero-hash dummies only strictly below it.
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
/// 3. [`import_state`](Self::import_state) streams the pack's accounts into reth's state tables in
///    bounded chunks, recomputes the state root from scratch, and hard-fails on any mismatch with
///    `header(B).state_root`.
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
    ///   [`open`](Self::open)) wrote the genesis alloc into these tables, but the pack OMITS
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

        // headers 1..=B-1: real headers with real hashes inside the window, zero-hash dummies below
        // it. the dummies are never referenced by BLOCKHASH within the shipped window, so a zero
        // hash there is inert.
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

    /// Import the pack's plain-state accounts into the scaffolded datadir and return the recomputed
    /// state root.
    ///
    /// Streams every [`ExecStateAccount`] out of `reader` in bounded chunks of
    /// [`IMPORT_ACCOUNT_CHUNK`] accounts, writing each chunk into reth's state tables with the
    /// public building blocks [`insert_genesis_hashes`], [`insert_history`], and [`insert_state`]
    /// (mirroring what reth's private `dump_state` does, but reading from the pack, not JSONL). The
    /// state root is then recomputed FROM SCRATCH with [`StateRoot::from_tx`], driven incrementally
    /// via [`StateRoot::root_with_progress`] so the in-flight `TrieUpdates` are flushed to disk as
    /// they accumulate rather than pinned in RAM. The import hard-fails unless the recomputed root
    /// equals both the pack's declared root and `header(B).state_root` (written by
    /// [`import_chain_scaffold`](Self::import_chain_scaffold)). No JSONL, no ETL.
    ///
    /// # Memory profile
    ///
    /// Only one chunk of accounts (and the reth `BundleState` mirror for that chunk) is resident at
    /// a time, and the trie-update flushing bounds the recompute's memory. The output is
    /// byte-identical to a single-shot import: the plain-state, hashed, and history writes are
    /// order-independent `upsert`s, and the per-block change-set `append_dup`s land under the same
    /// block key regardless of how the accounts are batched — see the ascending-address invariant.
    ///
    /// # Ascending-address invariant
    ///
    /// The per-chunk [`insert_state`] is correct ONLY if accounts arrive strictly ascending by
    /// address across the whole stream. Every chunk's change sets are appended under the same block
    /// `B` with `append_dup` keyed by address, so the subkeys must be globally ascending or the
    /// underlying MDBX append rejects them. The exporter
    /// ([`PinnedStateView::export_state_pack`]) guarantees this ordering — it walks the
    /// `PlainAccountState` cursor in ascending, unique address order — so a well-formed pack always
    /// satisfies it. This method still verifies the order explicitly and returns a clear error on
    /// any violation, turning a corrupt, crafted, or out-of-order pack into a loud failure instead
    /// of a cryptic append_dup error.
    pub fn import_state(&self, reader: &mut ExecStatePackReader) -> eyre::Result<B256> {
        self.import_state_with_chunk_size(reader, IMPORT_ACCOUNT_CHUNK)
    }

    /// [`import_state`](Self::import_state) with an explicit account-chunk size.
    ///
    /// Factored out so tests can force multiple chunks with a tiny size; production always calls
    /// [`import_state`], which passes [`IMPORT_ACCOUNT_CHUNK`].
    fn import_state_with_chunk_size(
        &self,
        reader: &mut ExecStatePackReader,
        chunk_size: usize,
    ) -> eyre::Result<B256> {
        let expected_root = reader.meta().state_root;
        let b = reader.meta().block_number;

        let provider = self.reth_env.inner.blockchain_provider.database_provider_rw()?;

        // stream the pack's accounts in bounded chunks, writing plain + hashed + history state per
        // chunk, mirroring reth's private `dump_state` sequence but driven from the pack (not
        // JSONL). only one chunk is resident at a time, which bounds both the account set and the
        // reth BundleState `insert_state` builds. the ascending-address check enforces the
        // append_dup ordering the per-block change-set writes require.
        //
        // cap the preallocation at IMPORT_ACCOUNT_CHUNK so a very large chunk_size (e.g. a
        // single-shot import) cannot overflow the allocation; the vec still grows if more than that
        // many accounts land in one chunk.
        let mut batch: Vec<(Address, GenesisAccount)> =
            Vec::with_capacity(chunk_size.min(IMPORT_ACCOUNT_CHUNK));
        let mut prev_address: Option<Address> = None;

        let flush_batch = |batch: &mut Vec<(Address, GenesisAccount)>| -> eyre::Result<()> {
            if batch.is_empty() {
                return Ok(());
            }
            insert_genesis_hashes(&provider, batch.iter().map(|(a, g)| (a, g)))?;
            insert_history(&provider, batch.iter().map(|(a, g)| (a, g)), b)?;
            insert_state(&provider, batch.iter().map(|(a, g)| (a, g)), b)?;
            batch.clear();
            Ok(())
        };

        while let Some(account) = reader.next_account() {
            let account = account?;
            let address = account.address;

            // strictly ascending, unique addresses are required for the per-block append_dup
            // change-set writes above; the exporter guarantees this, so a violation means the pack
            // is corrupt or was not produced by the exporter.
            if let Some(prev) = prev_address {
                if address <= prev {
                    return Err(eyre!(
                        "snapshot restore: pack accounts are not in strictly ascending address \
                         order at {address}; the pack is corrupt or was not produced by the \
                         exporter"
                    ));
                }
            }
            prev_address = Some(address);

            batch.push((address, account.account));
            if batch.len() >= chunk_size {
                flush_batch(&mut batch)?;
            }
        }
        // flush the final partial chunk
        flush_batch(&mut batch)?;

        // recompute the state root from scratch out of the just-written hashed state and persist
        // the trie nodes. the scaffold cleared the trie tables, so this is a full rebuild with no
        // prefix sets. drive it incrementally: `root_with_progress` yields the trie updates in
        // batches, which are written and released each iteration so they are never all pinned in
        // RAM, resuming from the returned intermediate state until it completes.
        let root = {
            let tx = provider.tx_ref();
            let mut intermediate: Option<IntermediateStateRootState> = None;
            loop {
                let computer = StateRoot::from_tx(tx).with_intermediate_state(intermediate.take());
                match computer
                    .root_with_progress()
                    .map_err(|e| eyre!("snapshot restore: state root computation failed: {e}"))?
                {
                    StateRootProgress::Progress(state, _, updates) => {
                        provider.write_trie_updates(updates)?;
                        intermediate = Some(*state);
                    }
                    StateRootProgress::Complete(root, _, updates) => {
                        provider.write_trie_updates(updates)?;
                        break root;
                    }
                }
            }
        };

        // authoritative check: the recomputed root must match the pack's declared root and the
        // scaffolded header(B). a mismatch means the shipped accounts do not hash to the claimed
        // state.
        if root != expected_root {
            return Err(eyre!(
                "snapshot restore: recomputed state root {root} does not match the pack's declared \
                 root {expected_root}"
            ));
        }
        let header_b = self.reth_env.sealed_header_by_number(b)?.ok_or_else(|| {
            eyre!("snapshot restore: no scaffolded header at block {b} to check the state root")
        })?;
        if header_b.state_root != root {
            return Err(eyre!(
                "snapshot restore: recomputed state root {root} does not match header({b}) \
                 state_root {}",
                header_b.state_root
            ));
        }

        provider.commit()?;
        Ok(root)
    }

    /// Restore-side driver for [`check_fee_precondition`]: verifies the shipped window can seed the
    /// restored node's first epoch-entry base fees, reading contract state through this restorer's
    /// env. Must run AFTER [`import_state`](Self::import_state), since it reads contract state at
    /// `B`.
    pub fn derive_fee_precondition(
        &self,
        entered: Epoch,
        window: &[SealedHeader],
    ) -> eyre::Result<()> {
        check_fee_precondition(&self.reth_env, entered, window)
    }

    /// Restore-side driver for [`check_fees_resumable`]: reads the entered epoch from the restored
    /// state at `B` and verifies fee derivability. Must run AFTER
    /// [`import_state`](Self::import_state) (it reads contract state at `B`) and before
    /// [`finish`](Self::finish).
    pub fn check_resumable_fees(&self, window: &[SealedHeader]) -> eyre::Result<()> {
        check_fees_resumable(&self.reth_env, window)
    }

    /// Persist the finalized/safe markers at `B` and sanity-check the reconstructed tip.
    ///
    /// The restored node reads these markers on startup to place its finalized/safe blocks (the
    /// node persists them the same way after finalizing a block), so the restore is not
    /// complete until they point at `B`. After committing them, this verifies the persisted tip
    /// and its hash match `final_state`; a mismatch means the scaffold or import produced a
    /// chain other than the one the snapshot claimed.
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

/// Verify a snapshot's shipped `window` can seed the restored node's first epoch-entry base fees,
/// reading contract state at `B` (= `window.last()`) through `reth_env`.
///
/// At epoch entry the node derives each worker's base fee from the previous epoch's genuine blocks;
/// a worker with an `Eip1559` config that produced no genuine block in that epoch has no
/// chain-observable fee anchor and the node walks BACKWARD through earlier epochs to find one. A
/// node holding only the shipped `window` (a snapshot-restored node bootstrapped from an idle
/// epoch) would run off the bottom of the window into pre-snapshot history and halt. This reads the
/// worker fee configs at `B` (the closing block of the epoch below `entered`, which defines
/// `entered`'s configuration) and requires every `Eip1559` worker to have produced at least one
/// genuine batch block within `window`; `Static` workers need no anchor. Errors name the offending
/// worker.
///
/// Shared by the restore side ([`SnapshotRestorer::derive_fee_precondition`]) and the export side
/// (which runs it against a candidate bundle's window before writing it) — one implementation, no
/// drift.
pub fn check_fee_precondition(
    reth_env: &RethEnv,
    entered: Epoch,
    window: &[SealedHeader],
) -> eyre::Result<()> {
    let closing = window
        .last()
        .ok_or_else(|| eyre!("snapshot restore: cannot check fees over an empty window"))?;

    // worker strategies at the closing block define the entered epoch's configuration
    let (num_workers, configs) = reth_env
        .get_worker_fee_configs_at_block(closing.hash())
        .wrap_err("snapshot restore: reading worker fee configs at the snapshot's final block")?;

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

/// Verify a snapshot `window` is fee-resumable without being told the entered epoch: read it from
/// the state at `B` (the window's last header, whose registry state — stamped by `concludeEpoch` at
/// that closing block — names the epoch a node would enter) and defer to
/// [`check_fee_precondition`].
///
/// A no-op when the node would enter epoch 0 or 1: entering epoch 1 stops at the epoch-0 base case
/// of the base-fee walk (`derive_idle_worker_fee_at`) and never reads state below `B`, so there is
/// nothing to guard (and an epoch-0 snapshot is not consensus-resumable regardless). The
/// entered-epoch read is best-effort: a read fault logs a warning and returns `Ok` rather than
/// blocking on a transient/absent read (the restore side reads it again at startup; the export side
/// proceeds). An actual idle-epoch violation is a hard error naming the offending worker.
///
/// On the restore side, run AFTER [`SnapshotRestorer::import_state`] (it reads contract state at
/// `B`). On the export side, run against a candidate bundle's window before writing the pack.
pub fn check_fees_resumable(reth_env: &RethEnv, window: &[SealedHeader]) -> eyre::Result<()> {
    let closing = window.last().ok_or_else(|| {
        eyre!("snapshot restore: cannot check fee resumability over an empty window")
    })?;
    let entered = match reth_env.get_current_epoch_info_at_header(closing) {
        Ok((entered, _info)) => entered,
        Err(e) => {
            warn!(
                target: "tn::reth",
                error = %e,
                "snapshot restore: could not read the entered epoch at the snapshot block; \
                 skipping the fee-derivability precheck"
            );
            return Ok(());
        }
    };
    // Entering epoch 0 or 1 never walks below B (the walk stops at the epoch-0 base case), so there
    // is nothing to verify.
    if entered < 2 {
        return Ok(());
    }
    check_fee_precondition(reth_env, entered, window)
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
    use super::{is_worker_batch_block, worker_id_from_header, PinnedStateView, SnapshotRestorer};
    use crate::{MaybePlatformPath, RethChainSpec, RethConfig, RethDb, RethEnv};
    use reth::{args::DatadirArgs, builder::NodeConfig};
    use reth_db::{cursor::DbCursorRW, tables::PlainStorageState, transaction::DbTxMut};
    use reth_primitives_traits::StorageEntry;
    use reth_provider::{AccountReader, DBProvider, DatabaseProviderFactory, StateProvider};
    use std::{collections::BTreeMap, path::Path, sync::Arc};
    use tempfile::TempDir;
    use tn_storage::exec_state_pack::{ExecStateAccount, ExecStatePackReader, ExecStatePackWriter};
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

    /// Collect every account out of a pack into a map for assertions.
    fn read_pack_accounts(dir: &Path) -> BTreeMap<Address, GenesisAccount> {
        let mut reader = ExecStatePackReader::open(dir).expect("open pack");
        reader
            .accounts()
            .map(|a| a.expect("account"))
            .map(|a: ExecStateAccount| (a.address, a.account))
            .collect()
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

        // export at the genuine genesis root, embedding the genesis header (its state_root matches)
        let genesis_header = reth_env.sealed_header_by_number(0)?.expect("genesis header");
        let state_root = genesis_header.state_root;
        let pack_dir = TempDir::new()?;
        let view = reth_env.pin_state_view()?;
        let stats = view.export_state_pack(
            state_root,
            &[genesis_header.header().clone()],
            pack_dir.path(),
        )?;

        // the pack records the root and the snapshot header
        let reader_meta = ExecStatePackReader::open(pack_dir.path())?;
        assert_eq!(reader_meta.meta().state_root, state_root);
        assert_eq!(reader_meta.snapshot_header().number, 0);
        drop(reader_meta);

        let accounts = read_pack_accounts(pack_dir.path());

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

        // twin shares bytecode with `contract`: the memo resolves it to the same code
        let twin_account = accounts.get(&contract_twin).expect("twin exported");
        assert_eq!(twin_account.code, Some(code.clone()));

        // stats are self-consistent with the exported accounts
        assert_eq!(stats.account_count as usize, accounts.len());
        let total_slots: u64 =
            accounts.values().map(|a| a.storage.as_ref().map_or(0, |s| s.len()) as u64).sum();
        assert_eq!(stats.storage_slots, total_slots);
        let total_codes: Vec<Bytes> = accounts.values().filter_map(|a| a.code.clone()).collect();
        assert_eq!(stats.bytecodes as usize, total_codes.len());

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

    /// A window header built on top of the real genesis header, so it carries the Cancun/Prague
    /// fields (`excess_blob_gas`, base fee, gas limit, timestamp, ...) that `evm_env` requires for
    /// the post-import contract reads — `synthetic_header`'s `Default`-filled header omits them,
    /// which is fine for tests that only read state but not for ones that issue EVM calls.
    /// `state_root` pins the exported (genesis) root; `worker_id`/`genuine` drive worker
    /// attribution.
    fn window_header_on_genesis(
        genesis: &SealedHeader,
        number: u64,
        parent: B256,
        state_root: B256,
        worker_id: u16,
        genuine: bool,
    ) -> SealedHeader {
        let mut header = genesis.header().clone();
        header.number = number;
        header.parent_hash = parent;
        header.state_root = state_root;
        header.timestamp = genesis.timestamp + number;
        header.difficulty = U256::from(worker_id as u64);
        header.ommers_hash = if genuine { B256::repeat_byte(0xbb) } else { B256::ZERO };
        SealedHeader::seal_slow(header)
    }

    /// Export the source's plain state into a pack, embedding `headers` (snapshot header first).
    fn export_pack(source: &RethEnv, state_root: B256, headers: &[ExecHeader], dir: &Path) {
        source
            .pin_state_view()
            .expect("pin")
            .export_state_pack(state_root, headers, dir)
            .expect("export pack");
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

        // a contiguous window of real headers ending at block B, all pinning the exported root
        // (empty blocks 1..=3 keep the state equal to genesis, so header(B).state_root == root)
        let b = 3u64;
        let h1 = synthetic_header(1, genesis_header.hash(), state_root, 0, true);
        let h2 = synthetic_header(2, h1.hash(), state_root, 0, true);
        let h3 = synthetic_header(3, h2.hash(), state_root, 0, true);
        let window = vec![h1.clone(), h2.clone(), h3.clone()];
        let final_state = BlockNumHash::new(b, h3.hash());

        // export embeds the window headers, snapshot (tip) header first
        let pack_dir = TempDir::new()?;
        export_pack(
            &source,
            state_root,
            &[h3.header().clone(), h2.header().clone(), h1.header().clone()],
            pack_dir.path(),
        );

        // destination: restore into a fresh datadir with the same genesis
        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Restore Roundtrip Dest");
        let (reth_config, db) = temp_config_and_db(chain.clone(), dst_dir.path())?;

        let restorer = SnapshotRestorer::open(&reth_config, db.clone(), &dst_tm)?;
        restorer.import_chain_scaffold(&window, final_state)?;
        let mut reader = ExecStatePackReader::open(pack_dir.path())?;
        let root = restorer.import_state(&mut reader)?;
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
        // nonzero in the genesis alloc, but zeroed on-chain by block B (so the pack omits it)
        let slot_s = B256::from([0x05; 32]);

        // source genesis holds the block-B shape (WITHOUT slot_s); its root is the pack's root
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

        let b = 1u64;
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
        let parent = dest_chain.sealed_genesis_header().hash();
        let header_b = synthetic_header(b, parent, root_without_s, 0, true);
        let window = vec![header_b.clone()];
        let final_state = BlockNumHash::new(b, header_b.hash());

        // export the source (slot_s-free) state, embedding header_b (state_root == root_without_s)
        let pack_dir = TempDir::new()?;
        export_pack(&source, root_without_s, &[header_b.header().clone()], pack_dir.path());

        // destination genesis holds slot_s nonzero: init_genesis writes it, and the scaffold must
        // clear it or it would survive the import (the pack omits it) and diverge the root
        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Genesis Zeroed Slot Dest");
        let (reth_config, db) = temp_config_and_db(dest_chain.clone(), dst_dir.path())?;

        let restorer = SnapshotRestorer::open(&reth_config, db.clone(), &dst_tm)?;
        restorer.import_chain_scaffold(&window, final_state)?;
        // import succeeds ONLY because the scaffold cleared slot_s: otherwise the recompute would
        // see {slot_a, slot_s} and fail the root check against root_without_s.
        let mut reader = ExecStatePackReader::open(pack_dir.path())?;
        let root = restorer.import_state(&mut reader)?;
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
        let window = vec![header_b.clone()];
        let final_state = BlockNumHash::new(1, header_b.hash());
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

    /// The fee-derivability precheck rejects a snapshot whose EIP-1559 worker produced no genuine
    /// block in the shipped window (the node would walk below the snapshot and halt), and accepts
    /// one that did. Uses a consensus-registry genesis (worker 0 is EIP-1559 by default) and passes
    /// `entered = 2` explicitly so the check runs regardless of the genesis registry epoch.
    #[tokio::test]
    async fn fee_precondition_gates_idle_snapshot_epochs() -> eyre::Result<()> {
        let genesis = crate::test_utils::test_genesis_with_consensus_registry(4);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // source: export the registry genesis state at its real root
        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Fee Precondition Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;
        let genesis_header = source.sealed_header_by_number(0)?.expect("genesis header");
        let state_root = genesis_header.state_root;
        let parent0 = genesis_header.hash();

        // Restore a 1-block window (block 1, parent = genesis, pinning the genesis root) into a
        // fresh datadir and run the precheck. `genuine` controls whether worker 0's window block is
        // a genuine batch block.
        let run = |genuine: bool| -> eyre::Result<()> {
            let h1 = window_header_on_genesis(&genesis_header, 1, parent0, state_root, 0, genuine);
            let window = vec![h1.clone()];
            let final_state = BlockNumHash::new(1, h1.hash());
            let pack_dir = TempDir::new()?;
            export_pack(&source, state_root, &[h1.header().clone()], pack_dir.path());
            let dst_dir = TempDir::new()?;
            let dst_tm = TaskManager::new("Fee Precondition Dest");
            let (reth_config, db) = temp_config_and_db(chain.clone(), dst_dir.path())?;
            let restorer = SnapshotRestorer::open(&reth_config, db, &dst_tm)?;
            restorer.import_chain_scaffold(&window, final_state)?;
            let mut reader = ExecStatePackReader::open(pack_dir.path())?;
            restorer.import_state(&mut reader)?;
            // The precheck reads worker configs at B (= the restored genesis state) and scans the
            // window for genuine blocks; returns its verdict (datadir/guards drop after it runs).
            restorer.derive_fee_precondition(2, &window)
        };

        let err = run(false).expect_err("an idle EIP-1559 window must be rejected");
        assert!(
            err.to_string().contains("produced no genuine block"),
            "expected an idle-worker rejection, got: {err:?}"
        );
        run(true).expect("a window with a genuine worker block must satisfy the precondition");

        Ok(())
    }

    /// `check_resumable_fees` is a no-op for a snapshot rooted at the genesis block: the node would
    /// enter epoch <= 1, whose base-fee derivation never walks below B, so even an idle window is
    /// tolerated (either the entered-epoch read returns < 2, or a read fault best-effort skips).
    #[tokio::test]
    async fn check_resumable_fees_tolerates_epoch_zero_snapshot() -> eyre::Result<()> {
        let genesis = crate::test_utils::test_genesis_with_consensus_registry(4);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Resumable Fees Skip Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;
        let genesis_header = source.sealed_header_by_number(0)?.expect("genesis header");
        let state_root = genesis_header.state_root;

        // an idle 1-block window that WOULD be rejected if the entered epoch were >= 2
        let h1 = window_header_on_genesis(
            &genesis_header,
            1,
            genesis_header.hash(),
            state_root,
            0,
            false,
        );
        let window = vec![h1.clone()];
        let final_state = BlockNumHash::new(1, h1.hash());
        let pack_dir = TempDir::new()?;
        export_pack(&source, state_root, &[h1.header().clone()], pack_dir.path());

        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Resumable Fees Skip Dest");
        let (reth_config, db) = temp_config_and_db(chain.clone(), dst_dir.path())?;
        let restorer = SnapshotRestorer::open(&reth_config, db, &dst_tm)?;
        restorer.import_chain_scaffold(&window, final_state)?;
        let mut reader = ExecStatePackReader::open(pack_dir.path())?;
        restorer.import_state(&mut reader)?;

        restorer
            .check_resumable_fees(&window)
            .expect("an epoch-<=1 snapshot must skip the fee-derivability precheck");

        Ok(())
    }

    /// One more slot than the exporter packs into a single storage record, so the account below is
    /// emitted across more than one `STORAGE_CHUNK_SLOTS`-bounded storage chunk and the chunked
    /// import reassembles it across that boundary. Kept local (the pack constant is private) and
    /// deliberately just past the boundary to keep the heavy multi-slot env cheap.
    const MULTI_CHUNK_SLOTS: u64 = 64 * 1024 + 1;

    /// Restore twice from one pack — once streaming in tiny two-account chunks, once in a single
    /// chunk — and prove the two agree.
    ///
    /// The source holds several accounts in ascending address order (forcing multiple chunks at
    /// chunk size 2) and one contract whose storage spans more than one `STORAGE_CHUNK_SLOTS` pack
    /// record (forcing the chunked import to reassemble an account across storage-chunk
    /// boundaries). Both imports must recompute the SAME root, that root must equal the pack's
    /// declared root (the dual hard-fail check passes), and the chunked restore must read the state
    /// back intact — including a probe slot from deep inside the large account. Byte-identical
    /// output between chunked and single-shot is the correctness proof for the streaming rewrite.
    #[tokio::test]
    async fn import_streams_in_chunks_matches_single_shot() -> eyre::Result<()> {
        let code: Bytes = Bytes::from_static(CODE);

        // several accounts in ascending address order so chunk size 2 spans multiple chunks
        let eoa_a = Address::from([0x0a; 20]);
        let eoa_b = Address::from([0x0b; 20]);
        let contract = Address::from([0x0c; 20]);
        let big = Address::from([0x0d; 20]);
        let eoa_e = Address::from([0x0e; 20]);
        let eoa_f = Address::from([0x0f; 20]);
        let (slot_a, slot_b) = (B256::from([0x01; 32]), B256::from([0x02; 32]));

        // a probe slot deep inside the large account's storage, to prove the multi-chunk reassembly
        // round-trips a slot beyond the first storage record
        let probe_slot = word(MULTI_CHUNK_SLOTS - 7);
        // well above every `word(i + 1)` filler below, so the probe value is unambiguous
        let probe_value = word(0x7_0000_0000);

        // build the large account's storage: MULTI_CHUNK_SLOTS non-zero slots keyed by index, with
        // the probe slot carrying a distinctive value
        let big_storage: BTreeMap<B256, B256> = (0..MULTI_CHUNK_SLOTS)
            .map(|i| {
                let slot = word(i);
                let value = if slot == probe_slot { probe_value } else { word(i + 1) };
                (slot, value)
            })
            .collect();

        let genesis = test_genesis().extend_accounts([
            (
                eoa_a,
                GenesisAccount {
                    nonce: Some(1),
                    balance: U256::from(100u64),
                    code: None,
                    storage: None,
                    private_key: None,
                },
            ),
            (
                eoa_b,
                GenesisAccount {
                    nonce: Some(2),
                    balance: U256::from(200u64),
                    code: None,
                    storage: None,
                    private_key: None,
                },
            ),
            (
                contract,
                GenesisAccount {
                    nonce: Some(3),
                    balance: U256::from(300u64),
                    code: Some(code.clone()),
                    storage: Some(BTreeMap::from([(slot_a, word(111)), (slot_b, word(222))])),
                    private_key: None,
                },
            ),
            (
                big,
                GenesisAccount {
                    nonce: Some(4),
                    balance: U256::from(400u64),
                    code: Some(code.clone()),
                    storage: Some(big_storage),
                    private_key: None,
                },
            ),
            (
                eoa_e,
                GenesisAccount {
                    nonce: Some(5),
                    balance: U256::from(500u64),
                    code: None,
                    storage: None,
                    private_key: None,
                },
            ),
            (
                eoa_f,
                GenesisAccount {
                    nonce: Some(6),
                    balance: U256::from(600u64),
                    code: None,
                    storage: None,
                    private_key: None,
                },
            ),
        ]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // source: export the genesis state at its real root
        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Chunked Import Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;
        let genesis_header = source.sealed_header_by_number(0)?.expect("genesis header");
        let state_root = genesis_header.state_root;

        // a contiguous 1-block window pinning the exported root (block 1 is empty, so its state
        // equals genesis and header(B).state_root == state_root)
        let b = 1u64;
        let h1 = synthetic_header(b, genesis_header.hash(), state_root, 0, true);
        let window = vec![h1.clone()];
        let final_state = BlockNumHash::new(b, h1.hash());

        let pack_dir = TempDir::new()?;
        export_pack(&source, state_root, &[h1.header().clone()], pack_dir.path());

        // restore #1: stream in tiny two-account chunks (6 accounts => at least three chunks)
        let chunked_dir = TempDir::new()?;
        let chunked_tm = TaskManager::new("Chunked Import Dest Chunked");
        let (chunked_config, chunked_db) = temp_config_and_db(chain.clone(), chunked_dir.path())?;
        let chunked_root = {
            let restorer =
                SnapshotRestorer::open(&chunked_config, chunked_db.clone(), &chunked_tm)?;
            restorer.import_chain_scaffold(&window, final_state)?;
            let mut reader = ExecStatePackReader::open(pack_dir.path())?;
            let root = restorer.import_state_with_chunk_size(&mut reader, 2)?;
            restorer.finish(final_state)?;
            root
        };
        assert_eq!(chunked_root, state_root, "chunked import must recompute the declared root");

        // restore #2: single chunk (one flush) from the same pack
        let single_dir = TempDir::new()?;
        let single_tm = TaskManager::new("Chunked Import Dest Single");
        let (single_config, single_db) = temp_config_and_db(chain.clone(), single_dir.path())?;
        let single_root = {
            let restorer = SnapshotRestorer::open(&single_config, single_db, &single_tm)?;
            restorer.import_chain_scaffold(&window, final_state)?;
            let mut reader = ExecStatePackReader::open(pack_dir.path())?;
            let root = restorer.import_state_with_chunk_size(&mut reader, usize::MAX)?;
            restorer.finish(final_state)?;
            root
        };

        // the whole point: chunked output is byte-identical to single-shot at the root level
        assert_eq!(
            chunked_root, single_root,
            "chunked import must produce the same state root as a single-shot import"
        );

        // and the chunked datadir reads the state back intact, including the deep probe slot
        let reader = RethEnv::new(
            &chunked_config,
            &chunked_tm,
            chunked_db,
            None,
            RewardsCounter::default(),
        )?;
        assert_eq!(reader.last_block_number()?, b);
        let state = reader.latest()?;
        assert_eq!(state.account_balance(&eoa_a)?, Some(U256::from(100u64)));
        assert_eq!(state.account_balance(&eoa_f)?, Some(U256::from(600u64)));
        assert_eq!(state.storage(contract, slot_a)?, Some(U256::from(111u64)));
        assert_eq!(
            state.storage(big, probe_slot)?,
            Some(U256::from_be_bytes(probe_value.0)),
            "a slot past the first storage chunk must survive the chunked reassembly"
        );

        Ok(())
    }

    /// A pack whose accounts are NOT in strictly ascending address order is rejected with a clear
    /// error before any state root is computed.
    ///
    /// The exporter can never emit such a pack, so this crafts one directly with the writer
    /// (two accounts in descending address order) to prove the guard turns a corrupt or
    /// hand-crafted pack into a loud, specific failure rather than a cryptic MDBX append_dup error.
    #[tokio::test]
    async fn import_rejects_out_of_order_pack() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());

        // a fresh source only to obtain a real genesis root/header for the window + pack meta
        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Out Of Order Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;
        let genesis_header = source.sealed_header_by_number(0)?.expect("genesis header");
        let state_root = genesis_header.state_root;

        let b = 1u64;
        let h1 = synthetic_header(b, genesis_header.hash(), state_root, 0, true);
        let window = vec![h1.clone()];
        let final_state = BlockNumHash::new(b, h1.hash());

        // hand-write a corrupt pack: the snapshot header pins the exported root (so `create`
        // accepts it), then two accounts appended in DESCENDING address order.
        let pack_dir = TempDir::new()?;
        let higher = Address::from([0xcc; 20]);
        let lower = Address::from([0x11; 20]);
        {
            let mut writer =
                ExecStatePackWriter::create(pack_dir.path(), state_root, &[h1.header().clone()])?;
            let acct = |address: Address| ExecStateAccount {
                address,
                account: GenesisAccount {
                    nonce: Some(1),
                    balance: U256::from(1u64),
                    code: None,
                    storage: None,
                    private_key: None,
                },
            };
            writer.append_account(&acct(higher))?;
            writer.append_account(&acct(lower))?;
            writer.finish()?;
        }

        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Out Of Order Dest");
        let (reth_config, db) = temp_config_and_db(chain.clone(), dst_dir.path())?;
        let restorer = SnapshotRestorer::open(&reth_config, db, &dst_tm)?;
        restorer.import_chain_scaffold(&window, final_state)?;
        let mut reader = ExecStatePackReader::open(pack_dir.path())?;

        let err = restorer
            .import_state(&mut reader)
            .expect_err("a descending-address pack must be rejected");
        assert!(
            err.to_string().contains("strictly ascending address order"),
            "expected an ascending-order rejection, got: {err:?}"
        );

        Ok(())
    }
}
