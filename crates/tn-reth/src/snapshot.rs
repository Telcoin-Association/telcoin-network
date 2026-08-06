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
//!
//! # Trust boundary: what restore verifies, and what it trusts
//!
//! Restore proves internal consistency, not provenance. It verifies that the pack's accounts
//! rebuild from scratch to both the pack's declared state root and `header(B).state_root`, that
//! the header window is contiguous by block number, hash-linked from each header to its
//! predecessor, and tipped by a header matching the claimed `final_state` number and hash, and
//! (in [`SnapshotRestorer::finish`]) that the reconstructed tip matches `final_state`. It does
//! NOT verify that `final_state` itself belongs to the canonical chain: no consensus provenance
//! or signature check happens in this module, so `final_state` is the trusted input here: a
//! restore against a forged window that is internally consistent and tipped by the claimed final
//! block would succeed. Callers are expected to supply a `final_state` they have verified: `tn db
//! load-state` pins it to an epoch record checked against that epoch's super-quorum certificate.
//! Because a header hash commits to every field of its header, including `parent_hash`, the
//! linkage walk carries such a caller's guarantee from the tip down through the whole window.
//! Genesis is never taken from the snapshot; it always comes from the local chain spec, which
//! remains the trust root.
//!
//! The linkage walk's own consumer set has narrowed: nothing in the restore reads the CONTENT of a
//! window header below `B` any more. The entry-readiness precondition is a single pinned state read
//! at `B` (no prior-epoch header scan), so the `BLOCKHASH` opcode — which resolves ancestor hashes
//! for blocks inside the window — is now the sole consumer of the sub-`B` headers. They are still
//! scaffolded with real hashes and still walked for linkage: `BLOCKHASH` returning a hash that does
//! not belong to the chain the tip commits to would diverge execution from the fleet.

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
use reth_primitives_traits::{Account, StorageEntry};
use reth_provider::{
    BlockWriter, ChainStateBlockWriter, DBProvider, DatabaseProviderFactory, HashingWriter,
    HistoryWriter, ProviderError, StageCheckpointWriter, StateWriter, StaticFileProviderFactory,
    StaticFileSegment, StaticFileWriter, StorageSettingsCache, TrieWriter,
};
use reth_revm::{
    bytecode::Bytecode,
    db::states::{PlainStorageChangeset, StateChangeset},
};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_trie::{IntermediateStateRootState, StateRoot, StateRootProgress};
use reth_trie_db::DatabaseStateRoot;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    path::Path,
};
use tn_storage::exec_state_pack::{
    ExecStateAccount, ExecStateAccountMeta, ExecStatePackReader, ExecStatePackWriter,
    ExecStateStats, StateEntry,
};
use tn_types::{
    gas_accumulator::{entry_fee_for_worker, GasAccumulator},
    Address, BlockBody, BlockNumHash, Bytes, ExecHeader, GenesisAccount, SealedBlock, SealedHeader,
    TaskManager, WorkerId, B256, U256,
};
use tracing::{debug, info};

/// The pinned read-only MDBX transaction type — reth's `Tx<RO>` with long-read safety disabled.
type PinnedTx = <DatabaseEnv as RethDatabaseT>::TX;

/// Emit a scaffold-progress log every this many blocks. The chain scaffold's cost scales with chain
/// height (not state size), so on a mature chain the header/body loops churn through millions of
/// static-file writes; periodic logging makes a long scaffold observable instead of silent.
const SCAFFOLD_LOG_INTERVAL: u64 = 500_000;

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
            .blockchain_provider()
            .database_provider_ro()?
            .disable_long_read_transaction_safety()
            .into_tx();
        Ok(PinnedStateView { tx })
    }
}

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
/// state-insertion building blocks ([`StateWriter::write_state_changes`] for plain state and
/// bytecode, [`HashingWriter`] for the hashed tables, [`HistoryWriter`] for the history indices)
/// one account header and one bounded storage chunk at a time, then recomputes the state root from
/// scratch with [`StateRoot`] — the same hashed/history tables and the same from-scratch check
/// `init_from_state_dump` performs internally, without the JSONL parser.
///
/// # Why this reimplements reth's `setup_without_evm`
///
/// reth ships `reth_cli_commands::init_state::setup_without_evm`, which does almost exactly what
/// [`import_chain_scaffold`](Self::import_chain_scaffold) does, but it is unusable here for two
/// reasons: it fills every dummy header below the tip with a `B256::ZERO` hash, which would break
/// the `BLOCKHASH` opcode for blocks inside the shipped window; and pulling it in drags the whole
/// `reth-cli-commands` dependency tree. So the scaffold is reimplemented with real hashes for the
/// window headers and zero-hash dummies only strictly below it.
///
/// # Epoch-boundary requirement (validated, not assumed)
///
/// Snapshots are taken at epoch boundaries: `B` must be the block that closed an epoch. The
/// restored node's first epoch entry seeds its worker count and per-worker base fees from ONE
/// pinned state read at the previous epoch's closing block — which, for a restored node, is `B`
/// itself — and its accumulator catchup scans forward from the entered epoch's first block
/// (`B + 1`). [`entry_readiness_precondition`](Self::entry_readiness_precondition) VALIDATES this
/// shape against the imported state instead of trusting the operator: a mid-epoch `B` would pin
/// the fee read to a block the close-time write path never touched and leave catchup a scan range
/// whose earlier blocks the snapshot omitted.
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
/// 3. [`import_state`](Self::import_state) streams the pack's accounts into reth's state tables one
///    account header and one bounded storage chunk at a time, recomputes the state root from
///    scratch, and hard-fails on any mismatch with `header(B).state_root`.
/// 4. [`entry_readiness_precondition`](Self::entry_readiness_precondition) verifies `B` is an
///    epoch-closing block and that the imported state at `B` can seed the node's first epoch entry
///    (worker count and per-worker base fees from one pinned read).
/// 5. [`finish`](Self::finish) persists the finalized/safe markers at `B` and sanity-checks the
///    reconstructed tip.
#[derive(Debug)]
pub struct SnapshotRestorer {
    /// The execution environment opened over the (initially empty) restore datadir.
    ///
    /// Built once in [`SnapshotRestorer::open`]; its in-memory canonical state stays empty for the
    /// life of the restore (nothing is committed through the canonical path), so every read falls
    /// through to the committed database tip — which is exactly what the post-import
    /// entry-readiness reads at block `B` need.
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
            RethEnv::new(reth_config, task_manager, db, None, GasAccumulator::default())?;

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
    /// `window` is a contiguous, ascending, parent-hash-linked run of the real headers that end at
    /// `final_state` (block `B`); it must not include genesis. Those properties are re-checked
    /// here rather than assumed, so every caller of this `pub` API gets them. This method, run
    /// once before [`import_state`](Self::import_state):
    ///
    /// - Clears the genesis alloc from `PlainAccountState`, `PlainStorageState`, `HashedAccounts`,
    ///   `HashedStorages`, `AccountsTrie`, and `StoragesTrie`. `init_genesis` (run in
    ///   [`open`](Self::open)) wrote the genesis alloc into these tables, but the pack OMITS
    ///   zero-valued storage slots; a genesis slot later zeroed on-chain would otherwise survive
    ///   the import and diverge the recomputed state root. `Bytecodes` and the genesis header are
    ///   deliberately kept.
    /// - Appends headers `1..=B-1` to the Headers static file: the real header (with its real hash)
    ///   for every block inside `window`, and a zero-hash dummy for every block below it. Real
    ///   hashes inside the window keep `BLOCKHASH` resolvable.
    /// - Advances the transactions/receipts/senders static-file segments to `B-1`, inserts block
    ///   `B` itself with an empty body (advancing headers and transactions to `B`), and advances
    ///   receipts to `B` — mirroring reth's own dummy-chain shaping so the segments stay internally
    ///   consistent.
    /// - Writes `HeaderNumbers` (hash → number) for every window header so `BLOCKHASH` resolves by
    ///   hash, and sets every stage checkpoint to `B`.
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

        // cheap invariant checks. nothing upstream re-checks the window's shape or its linkage
        // (`scaffold_window` only normalizes, and the pack-vs-record check sees the tip alone), so
        // these run here or nowhere, and a mis-shaped window would silently corrupt the scaffold
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
        // parent-hash linkage. `final_state.hash` is pinned upstream to the certificate-verified
        // `EpochRecord.final_state`, and a header hash commits to every field of that header,
        // including its `parent_hash`. Walking the links therefore carries the certificate's
        // coverage from the tip down through every ancestor this scaffold writes, instead of
        // trusting the bundle's producer for them. Number contiguity alone accepts a header that
        // sits at the right height with arbitrary ancestry.
        window.iter().zip(window.iter().skip(1)).try_for_each(|(parent, child)| {
            (child.parent_hash == parent.hash()).then_some(()).ok_or_else(|| {
                eyre!(
                    "snapshot restore: window is not hash-linked at block {}: parent_hash {} does \
                     not match block {}'s hash {}",
                    child.number,
                    child.parent_hash,
                    parent.number,
                    parent.hash()
                )
            })
        })?;

        info!(
            target: "tn::reth",
            blocks = b,
            "snapshot restore: writing header-only chain scaffold up to block {b}"
        );

        let provider_rw = self.reth_env.blockchain_provider().database_provider_rw()?;

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
                if number.is_multiple_of(SCAFFOLD_LOG_INTERVAL) {
                    info!(
                        target: "tn::reth",
                        block = number,
                        total = b,
                        "snapshot restore: scaffolding headers"
                    );
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
                if number.is_multiple_of(SCAFFOLD_LOG_INTERVAL) {
                    info!(
                        target: "tn::reth",
                        ?segment,
                        block = number,
                        total = b,
                        "snapshot restore: advancing body segments"
                    );
                }
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

        // HeaderNumbers for every window header so BLOCKHASH resolves by hash (insert_block
        // already covered block B; the rest are written here)
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

        info!(target: "tn::reth", blocks = b, "snapshot restore: chain scaffold written");
        Ok(())
    }

    /// Import the pack's plain-state accounts into the scaffolded datadir and return the recomputed
    /// state root.
    ///
    /// Drives the pack's chunked read stream ([`ExecStatePackReader::next_entry`]): each
    /// [`StateEntry::Account`] writes one account header (`PlainAccountState`, `HashedAccounts`,
    /// `AccountsHistory`, and its `Bytecodes` row) and each [`StateEntry::Storage`] writes one
    /// bounded slot chunk (`PlainStorageState`, `HashedStorages`, `StoragesHistory`) for the
    /// current account. Plain state and bytecode go through
    /// [`StateWriter::write_state_changes`], the hashed tables through [`HashingWriter`], and
    /// the history indices through [`HistoryWriter`] — the same hashed/history/plain rows
    /// reth's private `dump_state` writes, but never the `AccountChangeSets`/
    /// `StorageChangeSets` those helpers also fabricate (see the change-set section). The state
    /// root is then recomputed FROM SCRATCH with [`StateRoot::from_tx`], driven incrementally
    /// via [`StateRoot::root_with_progress`] so the in-flight `TrieUpdates` are flushed to disk
    /// as they accumulate rather than pinned in RAM. The import hard-fails unless
    /// the recomputed root equals both the pack's declared root and `header(B).state_root` (written
    /// by [`import_chain_scaffold`](Self::import_chain_scaffold)). No JSONL, no ETL.
    ///
    /// # Memory profile
    ///
    /// At most one account header plus one `STORAGE_CHUNK_SLOTS`-bounded storage chunk is resident
    /// at a time — a whale account's storage is never fully materialized, unlike
    /// [`ExecStatePackReader::next_account`] (which reassembles it into one `Vec`) or the reth
    /// `write_state`/`insert_state` path (which builds a whole-account `BundleState`). The
    /// trie-update flushing bounds the recompute's memory the same way.
    ///
    /// # Why the block-`B` change sets are skipped
    ///
    /// `insert_state` (and every reth `write_state` variant) records a per-block revert into
    /// `AccountChangeSets`/`StorageChangeSets`, and there is no public reth API that writes plain
    /// state per storage chunk WITHOUT also bundling those change sets and materializing the whole
    /// account. This import deliberately writes neither table for block `B`. That is sound because
    /// those rows would be fictional anyway (the scaffold fabricates "all state created at `B`"
    /// over dummy blocks) and nothing a snapshot-bootstrapped node does ever reads them: the
    /// state root is recomputed from the HASHED tables (not change sets); the node never
    /// unwinds a persisted canonical block (`TN reorgs are impossible`) and restore finalizes
    /// `B`, so `B` and below are never rolled back; every registry/fee read pins the canonical
    /// tip (`= B`), which reth serves from a `LatestStateProvider` (plain state, no change-set
    /// read); and the node runs archive mode with the pruner fully disabled, so a historical
    /// read below `B` resolves through `HistoryInfo::NotYetWritten` (the single-`[B]` history
    /// index + absent prune boundary) rather than a change-set lookup. Blocks executed forward
    /// from `B` write their change sets normally through `save_blocks`.
    ///
    /// # Ascending-address invariant
    ///
    /// The exporter ([`PinnedStateView::export_state_pack`]) emits accounts in strictly ascending,
    /// unique address order (it walks the `PlainAccountState` cursor). The per-account
    /// [`HashingWriter::insert_storage_for_hashing`] MERGES a chunk's slots into any existing
    /// `HashedStorages` dup-list for the address, so re-visiting an address across the stream would
    /// silently fold two accounts' storage together; the per-account/per-chunk history-index writes
    /// likewise assume each `(address, slot)` sharded key is touched once. This method verifies the
    /// ordering explicitly and returns a clear error on any violation, turning a corrupt, crafted,
    /// or out-of-order pack into a loud failure instead of silent state corruption.
    pub fn import_state(&self, reader: &mut ExecStatePackReader) -> eyre::Result<B256> {
        let expected_root = reader.meta().state_root;
        let b = reader.meta().block_number;

        let provider = self.reth_env.blockchain_provider().database_provider_rw()?;

        // TN keeps EVM state in the PLAIN tables (the exporter reads PlainAccountState /
        // PlainStorageState, and the account-header / storage-chunk writers below target the plain
        // tables). reth's `write_state_changes` silently SKIPS plain writes when the db is in
        // hashed-state mode, so a hashed-mode db here would leave the plain tables empty and the
        // node unable to serve state. assert the mode up front rather than importing a
        // half-populated db.
        if provider.cached_storage_settings().use_hashed_state() {
            return Err(eyre!(
                "snapshot restore: reth db is in hashed-state mode, but the import writes plain \
                 state; refusing to import a db whose plain tables would be left empty"
            ));
        }

        // drive the pack's chunked read stream, writing one account header or one bounded storage
        // chunk at a time. only the current chunk (and one header) is ever resident, so a whale
        // account's storage is never fully materialized. the ascending-address check enforces the
        // unique-address assumption the per-account HashedStorages merge and history-index writes
        // rely on.
        let mut current: Option<Address> = None;
        let mut prev_address: Option<Address> = None;

        while let Some(entry) = reader.next_entry() {
            match entry? {
                StateEntry::Account(meta) => {
                    let address = meta.address;

                    // strictly ascending, unique addresses: the exporter guarantees this, so a
                    // violation means the pack is corrupt or was not produced by the exporter.
                    if let Some(prev) = prev_address {
                        if address <= prev {
                            return Err(eyre!(
                                "snapshot restore: pack accounts are not in strictly ascending \
                                 address order at {address}; the pack is corrupt or was not \
                                 produced by the exporter"
                            ));
                        }
                    }
                    prev_address = Some(address);
                    current = Some(address);

                    Self::write_account_header(&provider, &meta, b)?;
                }
                StateEntry::Storage(chunk) => {
                    // a Storage record can only follow an Account record; the pack layout
                    // guarantees this, but guard so a malformed pack fails loudly here rather than
                    // silently dropping storage.
                    let address = current.ok_or_else(|| {
                        eyre!(
                            "snapshot restore: pack has a storage chunk before any account; the \
                             pack is corrupt or was not produced by the exporter"
                        )
                    })?;
                    Self::write_storage_chunk(&provider, address, chunk, b)?;
                }
            }
        }

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

    /// Restore-side driver for [`check_entry_readiness`]: resolves the snapshot's final block `B`
    /// from `final_state` and verifies the imported state at it can seed the restored node's first
    /// epoch entry.
    ///
    /// Must run AFTER [`import_state`](Self::import_state), since the check reads contract state at
    /// `B`, and before [`finish`](Self::finish). Failure handling is deliberately stricter than the
    /// live node's: both [`StateReadError`](crate::error::StateReadError) classes fail the restore,
    /// including `ChainGlobal` failures a running fleet may fail open on — a restoring node has no
    /// fleet to stay in lockstep with, so strictness is free.
    pub fn entry_readiness_precondition(&self, final_state: BlockNumHash) -> eyre::Result<()> {
        let b = final_state.number;
        let header_b = self.reth_env.sealed_header_by_hash(final_state.hash)?.ok_or_else(|| {
            eyre!(
                "snapshot restore: no header for final block {b} ({}) to check entry readiness \
                 against (run the precondition after the scaffold and state import)",
                final_state.hash
            )
        })?;
        if header_b.number != b {
            return Err(eyre!(
                "snapshot restore: final block hash {} resolves to block {}, expected {b}",
                final_state.hash,
                header_b.number
            ));
        }

        check_entry_readiness(&self.reth_env, &header_b)
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
            let provider = self.reth_env.blockchain_provider().database_provider_rw()?;
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

    /// Write one account header (nonce/balance/code) into the plain, hashed, history, and bytecode
    /// tables at block `b`, WITHOUT any change-set row.
    ///
    /// This is the account-only half of what reth's `insert_state` + `insert_genesis_hashes` +
    /// `insert_history` write for a single account: `write_state_changes` upserts
    /// `PlainAccountState` and (via `contracts`) `Bytecodes` but no `AccountChangeSets`;
    /// `insert_account_for_hashing` upserts `HashedAccounts`; `insert_account_history_index`
    /// appends the single `[b]` transition to `AccountsHistory`. The account's storage arrives
    /// separately as [`Self::write_storage_chunk`] calls, so this never touches storage.
    fn write_account_header<Provider>(
        provider: &Provider,
        meta: &ExecStateAccountMeta,
        b: u64,
    ) -> eyre::Result<()>
    where
        Provider: StateWriter + HashingWriter + HistoryWriter,
    {
        // resolve the bytecode hash once and reuse it across the plain account row, the hashed
        // account row, and the Bytecodes entry, mirroring reth's genesis-account handling. an empty
        // code slice is treated as no code (hash left None) so the account keeps KECCAK_EMPTY.
        let (bytecode_hash, contracts) = match &meta.code {
            Some(code) if !code.is_empty() => {
                let bytecode = Bytecode::new_raw_checked(code.clone()).map_err(|e| {
                    eyre!("snapshot restore: invalid bytecode for account {}: {e}", meta.address)
                })?;
                let hash = bytecode.hash_slow();
                (Some(hash), vec![(hash, bytecode)])
            }
            _ => (None, Vec::new()),
        };
        let account = Account { nonce: meta.nonce, balance: meta.balance, bytecode_hash };

        // plain account + bytecode only: `write_state_changes` writes NO change sets.
        provider.write_state_changes(StateChangeset {
            accounts: vec![(meta.address, Some(account.into()))],
            storage: Vec::new(),
            contracts,
        })?;
        // hashed account row
        provider.insert_account_for_hashing([(meta.address, Some(account))])?;
        // single history transition at block `b`
        provider.insert_account_history_index([(meta.address, [b])])?;

        Ok(())
    }

    /// Write one bounded storage chunk for `address` into the plain, hashed, and history tables at
    /// block `b`, WITHOUT any change-set row.
    ///
    /// `chunk` is a slice of the account's `(slot, value)` pairs (at most `STORAGE_CHUNK_SLOTS`),
    /// already ascending by slot and free of zero values (the exporter filters them). This is the
    /// storage half of reth's `insert_state`/`insert_genesis_hashes`/`insert_history`:
    /// `write_state_changes` upserts `PlainStorageState` (no `StorageChangeSets`);
    /// `insert_storage_for_hashing` MERGES the chunk into the address's `HashedStorages` dup-list;
    /// `insert_storage_history_index` appends the single `[b]` transition per slot to
    /// `StoragesHistory`. Merging is why the caller must never re-visit an address (see the
    /// ascending-address invariant on [`import_state`](Self::import_state)).
    fn write_storage_chunk<Provider>(
        provider: &Provider,
        address: Address,
        chunk: Vec<(B256, B256)>,
        b: u64,
    ) -> eyre::Result<()>
    where
        Provider: StateWriter + HashingWriter + HistoryWriter,
    {
        if chunk.is_empty() {
            return Ok(());
        }

        // plain storage: `write_state_changes` upserts PlainStorageState (values are U256; the
        // exporter already dropped zero-valued slots) and writes NO change sets. `wipe_storage` is
        // false — the scaffold cleared the tables, so there is nothing to wipe and every account is
        // visited once.
        let plain_storage: Vec<(U256, U256)> = chunk
            .iter()
            .map(|(slot, value)| (U256::from_be_bytes(slot.0), U256::from_be_bytes(value.0)))
            .collect();
        provider.write_state_changes(StateChangeset {
            accounts: Vec::new(),
            storage: vec![PlainStorageChangeset {
                address,
                wipe_storage: false,
                storage: plain_storage,
            }],
            contracts: Vec::new(),
        })?;

        // hashed storage: merges this chunk's slots into the address's existing dup-list.
        let hashed_entries = chunk
            .iter()
            .map(|(slot, value)| StorageEntry { key: *slot, value: U256::from_be_bytes(value.0) });
        provider.insert_storage_for_hashing([(address, hashed_entries)])?;

        // storage history: one `[b]` transition per slot, keyed by (address, slot).
        let history = chunk.iter().map(|(slot, _)| ((address, *slot), [b]));
        provider.insert_storage_history_index(history)?;

        Ok(())
    }
}

/// Verify the state at `header_b` — the snapshot's final block `B` — can seed a restored node's
/// first epoch entry: ONE pinned state read, no prior-epoch headers.
///
/// At epoch entry the node reads the entered epoch's worker count and per-worker base fees from ONE
/// pinned block: the previous epoch's closing block, whose own system call recorded each `Eip1559`
/// worker's next-epoch fee in its `WorkerConfigs.data` word (`read_base_fees_for_entered_epoch` in
/// `tn_node::manager`, one row per worker through [`entry_fee_for_worker`]). A restored node pins
/// that first entry read to `B`, so this precondition re-issues the same reads against the
/// post-import state and fails on anything the entry would trip over. Three checks:
///
/// 1. **`B` closed an epoch.** The `ConsensusRegistry` at `B` must report the entered epoch
///    beginning at `B.number + 1` — `concludeEpoch` runs INSIDE a closing block, so only an
///    epoch-closing block satisfies this. This check upgrades "snapshots are taken at epoch
///    boundaries" from a scaffold assumption to a VALIDATED requirement: with a mid-epoch final
///    block, the restored node's entry would pin its fee read to a block the close-time write path
///    never touched (a wrong-block-pinned fee), and its accumulator catchup would scan the entered
///    epoch's range from a start below `B` whose blocks the snapshot omitted — either way the
///    rebuilt per-worker state diverges from the fleet and forks the state root once the node
///    produces.
/// 2. **Fee readiness.** The `WorkerConfigs` read at `B` must succeed, and every `Eip1559` row's
///    `data` word must pass [`entry_fee_for_worker`]. A word wider than `u64` (only reachable
///    through a foreign governance write) fails HERE — naming the worker and word — instead of
///    halting the node at its first epoch entry later. `Static` rows keep their fee in the config's
///    value and ignore `data` entirely, so a garbage word on a `Static` row is accepted by design.
/// 3. **At least one worker.** A zero-worker count cannot seed any entry. Unlike (1) and (2) this
///    one is unreachable through any in-repo path — see the comment at the check itself for the
///    searches that establish that, and for why it is kept anyway.
///
/// Shared by the restore side ([`SnapshotRestorer::entry_readiness_precondition`]) and the export
/// side (which runs it against a candidate bundle's anchor block before writing the pack) — one
/// implementation, no drift.
pub fn check_entry_readiness(reth_env: &RethEnv, header_b: &SealedHeader) -> eyre::Result<()> {
    let b = header_b.number;

    // (1) B must be an epoch-closing block: concludeEpoch runs inside the closing block, so the
    // registry pinned at B already reports the entered epoch, and that epoch's first block must be
    // the very next one.
    let (entered, epoch_info) = reth_env.get_current_epoch_info_at_header(header_b).wrap_err(
        "snapshot restore: reading the registry epoch record at the snapshot's final block",
    )?;
    if epoch_info.blockHeight != b + 1 {
        return Err(eyre!(
            "snapshot restore: final block {b} is not an epoch-closing block — the registry \
             at this block reports epoch {entered} with first block {}, expected {} — \
             snapshots must end at an epoch boundary (the block that closed the previous \
             epoch), or the restored node's epoch entry and catchup would run against a \
             mid-epoch state",
            epoch_info.blockHeight,
            b + 1
        ));
    }

    // (2)+(3) the entry fee read must succeed for every worker
    let (num_workers, entries) = reth_env
        .get_worker_fee_configs_at_block(header_b.hash())
        .wrap_err("snapshot restore: reading WorkerConfigs at the snapshot's final block")?;
    // (3) UNREACHABLE BY CONSTRUCTION, and therefore deliberately untested: there is no honest
    // fixture for this branch, only one that pokes storage the contract would never write.
    // `numWorkers` has exactly two writers and both floor it at 1 — `WorkerConfigs.sol`'s
    // constructor and `setNumWorkers`, each opening with `revert NumWorkersBelowMinimum()` on
    // zero. `rg -n 'numWorkers\s*=[^=]' tn-contracts/src/` returns only those two assignments, so
    // that pair is exhaustive. Upstream of the contract, `GenesisArgs::parse_worker_fee_configs`
    // rejects an empty config list, and a reverted constructor fails genesis creation outright
    // (`ensure_pre_genesis_create_success`, pinned by
    // `genesis_ceremony_rejects_empty_worker_configs`) instead of committing runtime code over
    // empty storage — the one state that would read back as zero here. An absent contract is a
    // different failure that never reaches this line: `decode_worker_fee_configs` cannot decode
    // empty return data and errors out of the read above. Kept regardless, because a snapshot
    // pack's provenance is outside restore's trust boundary: if a hand-built genesis or a
    // future contract revision ever does produce zero, it must name itself here rather than
    // surface later as an empty accumulator at epoch entry.
    if num_workers == 0 {
        return Err(eyre!(
            "snapshot restore: WorkerConfigs at final block {b} reports zero workers; the \
             restored node cannot enter epoch {entered} without at least one worker"
        ));
    }
    for (worker_id, entry) in entries.iter().enumerate() {
        entry_fee_for_worker(worker_id as WorkerId, entry).map_err(|e| {
            eyre!(
                "snapshot restore: epoch {entered}'s entry base fee is unreadable at the \
                 final block {b}: {e}"
            )
        })?;
    }

    debug!(
        target: "tn::reth",
        entered,
        final_block = b,
        num_workers,
        "snapshot restore: entry-readiness precondition satisfied"
    );
    Ok(())
}

// The canonical worker-attribution helper lives in `tn-types` (one implementation shared with
// `tn_node`'s epoch-entry read — no drift). Re-exported so in-crate consumers and tests keep
// referring to it via the `tn_reth::snapshot::` path.
pub use tn_types::gas_accumulator::worker_id_from_header;

#[cfg(test)]
mod tests {
    use super::{worker_id_from_header, PinnedStateView, SnapshotRestorer, StateEntry};
    use crate::{
        payload::TNPayload,
        system_calls::WorkerConfigs,
        test_utils::{
            consensus_output_for_tests, execute_payload_and_update_canonical_chain,
            governance_owner_factory, read_worker_config_entries_at,
            test_genesis_with_consensus_registry_and_workers,
        },
        MaybePlatformPath, RethChainSpec, RethConfig, RethDb, RethEnv,
    };
    use alloy::{eips::eip7685::EMPTY_REQUESTS_HASH, primitives::aliases::U184};
    use reth::{args::DatadirArgs, builder::NodeConfig};
    use reth_db::{
        cursor::{DbCursorRW, DbDupCursorRO},
        tables::{HashedAccounts, HashedStorages, PlainAccountState, PlainStorageState},
        transaction::{DbTx, DbTxMut},
    };
    use reth_primitives_traits::StorageEntry;
    use reth_provider::{
        AccountReader, ChangeSetReader, DBProvider, DatabaseProviderFactory, StateProvider,
        StorageChangeSetReader,
    };
    use std::{collections::BTreeMap, path::Path, sync::Arc};
    use tempfile::TempDir;
    use tn_config::WORKER_CONFIGS_ADDRESS;
    use tn_storage::exec_state_pack::{ExecStateAccount, ExecStatePackReader, ExecStatePackWriter};
    use tn_types::{
        gas_accumulator::{next_base_fee_for_config, GasAccumulator, WorkerFeeConfig},
        keccak256, test_genesis, Address, BlockNumHash, Bytes, ExecHeader, GenesisAccount,
        SealedHeader, SolCall as _, TaskManager, B256, EMPTY_WITHDRAWALS, MIN_PROTOCOL_BASE_FEE,
        U256,
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
        let provider = reth_env.blockchain_provider().database_provider_rw()?;
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
    /// root so the import's from-scratch recompute matches. The optional post-London fields carry
    /// the values every real TN block sets (see the header assembly in `evm/block.rs`) — the full
    /// contiguous suffix keeps the header RLP round-trippable through the pack, and the blob-gas
    /// fields let an EVM pinned to this header (the entry-readiness precondition reads contract
    /// state at the final block) pass header validation.
    fn synthetic_header(number: u64, parent: B256, state_root: B256) -> SealedHeader {
        let header = ExecHeader {
            number,
            parent_hash: parent,
            state_root,
            base_fee_per_gas: Some(MIN_PROTOCOL_BASE_FEE),
            withdrawals_root: Some(EMPTY_WITHDRAWALS),
            parent_beacon_block_root: Some(B256::ZERO),
            blob_gas_used: Some(0),
            excess_blob_gas: Some(0),
            requests_hash: Some(EMPTY_REQUESTS_HASH),
            ..Default::default()
        };
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
        let h1 = synthetic_header(1, genesis_header.hash(), state_root);
        let h2 = synthetic_header(2, h1.hash(), state_root);
        let h3 = synthetic_header(3, h2.hash(), state_root);
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
        let reader = RethEnv::new(&reth_config, &dst_tm, db, None, GasAccumulator::default())?;
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
        let header_b = synthetic_header(b, parent, root_without_s);
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

        let reader = RethEnv::new(&reth_config, &dst_tm, db, None, GasAccumulator::default())?;
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

    #[tokio::test]
    async fn import_rejects_truncated_pack() -> eyre::Result<()> {
        // A valid pack whose `state_data` file is then truncated on disk so the trailing records
        // (the End footer and part of the account run) are lost. meta + headers at the front
        // survive, so the reader opens, but the restore must reject the damaged stream rather than
        // import a partial, footerless state.
        let a = Address::from([0x0a; 20]);
        let b_addr = Address::from([0x0b; 20]);
        let contract = Address::from([0x0c; 20]);
        let code: Bytes = Bytes::from_static(CODE);
        let genesis = test_genesis().extend_accounts([
            (
                a,
                GenesisAccount {
                    nonce: Some(1),
                    balance: U256::from(100u64),
                    code: None,
                    storage: None,
                    private_key: None,
                },
            ),
            (
                b_addr,
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
                    storage: Some(BTreeMap::from([(B256::from([0x01; 32]), word(111))])),
                    private_key: None,
                },
            ),
        ]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Truncated Pack Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;
        let genesis_header = source.sealed_header_by_number(0)?.expect("genesis header");
        let state_root = genesis_header.state_root;

        let b = 1u64;
        let header_b = synthetic_header(b, genesis_header.hash(), state_root);
        let window = vec![header_b.clone()];
        let final_state = BlockNumHash::new(b, header_b.hash());

        let pack_dir = TempDir::new()?;
        export_pack(&source, state_root, &[header_b.header().clone()], pack_dir.path());

        // Corrupt the pack: drop the tail of the `state_data` stream (the End footer and part of
        // the account run). The meta + header records at the front are untouched.
        let data_path = pack_dir.path().join("state_data");
        let len = std::fs::metadata(&data_path)?.len();
        assert!(len > 24, "pack data should be larger than the truncation amount");
        std::fs::OpenOptions::new().write(true).open(&data_path)?.set_len(len - 24)?;

        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Truncated Pack Dest");
        let (reth_config, db) = temp_config_and_db(chain.clone(), dst_dir.path())?;
        let restorer = SnapshotRestorer::open(&reth_config, db, &dst_tm)?;
        restorer.import_chain_scaffold(&window, final_state)?;
        let mut reader = ExecStatePackReader::open(pack_dir.path())?;
        let err = restorer
            .import_state(&mut reader)
            .expect_err("a truncated pack must be rejected, not imported as partial state");
        // MissingFooter / CorruptPack / a CRC or short-read error are all acceptable — the point is
        // the restore fails loudly instead of committing a partial state.
        assert!(!err.to_string().is_empty(), "expected a descriptive error");

        Ok(())
    }

    #[test]
    fn worker_id_mask_matches_header_encoding() {
        // the payload builder writes `batch_index << 16 | worker_id` into difficulty; only the
        // low 16 bits attribute the worker
        let header = SealedHeader::seal_slow(ExecHeader {
            number: 5,
            difficulty: U256::from((7u64 << 16) | 3),
            ..Default::default()
        });
        assert_eq!(worker_id_from_header(&header), 3);
    }

    #[tokio::test]
    async fn open_refuses_non_empty_datadir() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let dir = TempDir::new()?;
        let tm = TaskManager::new("Refuse Non-Empty");
        let (reth_config, db) = temp_config_and_db(chain.clone(), dir.path())?;

        // scaffold one block so the datadir holds chain data above genesis
        let genesis_header = chain.sealed_genesis_header();
        let header_b = synthetic_header(1, genesis_header.hash(), genesis_header.state_root);
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

    /// Block-number contiguity alone accepts a window whose headers carry the right heights but
    /// descend from different ancestors, so the scaffold also walks the parent-hash links. The
    /// break sits in the middle of the window and the header above it re-links to the mutated
    /// header, so only a per-pair walk catches it; checking the ends of the window would not.
    #[tokio::test]
    async fn import_rejects_a_window_that_is_not_hash_linked() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let genesis_header = chain.sealed_genesis_header();
        let state_root = genesis_header.state_root;

        // scaffold a 3-block window into a fresh datadir; `linked` decides whether block 2
        // descends from block 1 or from a stranger. every other check the scaffold makes
        // (non-empty, non-genesis, number contiguity, tip vs `final_state`) passes either way, so
        // the two runs differ in the parent-hash link and nothing else.
        let run = |linked: bool| -> eyre::Result<()> {
            let h1 = synthetic_header(1, genesis_header.hash(), state_root);
            let h2_parent = if linked { h1.hash() } else { B256::repeat_byte(0xee) };
            let h2 = synthetic_header(2, h2_parent, state_root);
            let h3 = synthetic_header(3, h2.hash(), state_root);
            let window = vec![h1, h2, h3.clone()];
            let final_state = BlockNumHash::new(3, h3.hash());

            let dir = TempDir::new()?;
            let tm = TaskManager::new("Window Linkage");
            let (reth_config, db) = temp_config_and_db(chain.clone(), dir.path())?;
            let restorer = SnapshotRestorer::open(&reth_config, db, &tm)?;
            restorer.import_chain_scaffold(&window, final_state)
        };

        // positive control: the same fixture, honestly linked, is accepted, so the rejection
        // below is attributable to the broken link rather than to the synthetic headers
        run(true)?;

        let err = run(false).expect_err("a window that is not hash-linked must be rejected");
        assert!(
            err.to_string().contains("not hash-linked at block 2"),
            "expected a parent-hash linkage rejection, got: {err:?}"
        );

        Ok(())
    }

    /// Several slots more than the exporter packs into a single storage record, so the whale
    /// account below is emitted across MANY `STORAGE_CHUNK_SLOTS`-bounded storage chunks and the
    /// streaming import must write each chunk without ever materializing the whole account. Kept
    /// local (the pack constant is private) and only a few chunks past the boundary to keep the
    /// heavy multi-slot env cheap.
    const MULTI_CHUNK_SLOTS: u64 = 4 * 64 * 1024 + 7;

    /// Import a pack containing a WHALE account whose storage spans many storage chunks and prove
    /// the streaming, per-chunk import is exactly correct.
    ///
    /// The source holds several accounts in ascending address order plus one contract (`big`) with
    /// `MULTI_CHUNK_SLOTS` slots — several times the pack's per-record `STORAGE_CHUNK_SLOTS` bound,
    /// so the pack emits `big` as one account header followed by many `Storage` chunks and the
    /// import writes them one chunk at a time (never holding the whole account). The import must:
    ///
    /// - recompute the SAME root the pack declares (the dual hard-fail check passes) — this is the
    ///   byte-exact equivalence proof: the declared root came from the source node's genuine trie
    ///   built by a full-account write, so matching it proves the streamed hashed state is
    ///   identical to a whole-account import;
    /// - read every account back intact — balances, code, and multiple storage slots INCLUDING a
    ///   probe slot deep past the first storage chunk;
    /// - leave `AccountChangeSets`/`StorageChangeSets` EMPTY (the deliberate skip that lets the
    ///   storage stream per chunk); and
    /// - populate the hashed and plain tables with the same rows a whole-account import would (spot
    ///   checked against the source's keccak256 keys).
    #[tokio::test]
    async fn import_streams_whale_storage_in_chunks() -> eyre::Result<()> {
        let code: Bytes = Bytes::from_static(CODE);

        // several accounts in ascending address order, spanning many storage chunks for `big`
        let eoa_a = Address::from([0x0a; 20]);
        let eoa_b = Address::from([0x0b; 20]);
        let contract = Address::from([0x0c; 20]);
        let big = Address::from([0x0d; 20]);
        let eoa_e = Address::from([0x0e; 20]);
        let eoa_f = Address::from([0x0f; 20]);
        let (slot_a, slot_b) = (B256::from([0x01; 32]), B256::from([0x02; 32]));

        // a probe slot deep inside the whale's storage (past several chunk boundaries), to prove
        // the per-chunk writes round-trip a slot far beyond the first storage record
        let probe_slot = word(MULTI_CHUNK_SLOTS - 7);
        // well above every `word(i + 1)` filler below, so the probe value is unambiguous
        let probe_value = word(0x7_0000_0000);

        // build the whale's storage: MULTI_CHUNK_SLOTS non-zero slots keyed by index, with the
        // probe slot carrying a distinctive value
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
        let src_tm = TaskManager::new("Whale Import Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;
        let genesis_header = source.sealed_header_by_number(0)?.expect("genesis header");
        let state_root = genesis_header.state_root;

        // sanity: the pack really does emit `big` across more than one storage chunk (otherwise the
        // whale test would not exercise the per-chunk path at all)
        let b = 1u64;
        let h1 = synthetic_header(b, genesis_header.hash(), state_root);
        let window = vec![h1.clone()];
        let final_state = BlockNumHash::new(b, h1.hash());
        let pack_dir = TempDir::new()?;
        export_pack(&source, state_root, &[h1.header().clone()], pack_dir.path());
        {
            let mut reader = ExecStatePackReader::open(pack_dir.path())?;
            let mut big_chunks = 0usize;
            let mut in_big = false;
            while let Some(entry) = reader.next_entry() {
                match entry? {
                    StateEntry::Account(meta) => in_big = meta.address == big,
                    StateEntry::Storage(_) => {
                        if in_big {
                            big_chunks += 1;
                        }
                    }
                }
            }
            assert!(
                big_chunks > 1,
                "the whale account must span more than one storage chunk (got {big_chunks})"
            );
        }

        // restore: single streaming import (the per-entry path never materializes the whole whale)
        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Whale Import Dest");
        let (dst_config, dst_db) = temp_config_and_db(chain.clone(), dst_dir.path())?;
        let root = {
            let restorer = SnapshotRestorer::open(&dst_config, dst_db.clone(), &dst_tm)?;
            restorer.import_chain_scaffold(&window, final_state)?;
            let mut reader = ExecStatePackReader::open(pack_dir.path())?;
            let root = restorer.import_state(&mut reader)?;
            restorer.finish(final_state)?;
            root
        };
        // matching the pack's declared root proves the streamed state is byte-exactly what a
        // full-account import produces
        assert_eq!(
            root, state_root,
            "streaming import must recompute the pack's declared (genuine full-trie) root"
        );

        // open a fresh env over the restored datadir for both the table-level assertions and the
        // state readback
        let reader =
            RethEnv::new(&dst_config, &dst_tm, dst_db.clone(), None, GasAccumulator::default())?;

        // change sets at block B MUST be empty: the skip is what lets storage stream per chunk.
        // genesis (block 0) legitimately keeps its own change sets (init_genesis wrote them and the
        // scaffold does not clear those tables), so this checks block B specifically rather than
        // global emptiness — and confirms block 0 still has change sets, proving the test measures
        // the right thing and the import, not the scaffold, is the differentiator.
        {
            let provider = reader.blockchain_provider().database_provider_ro()?;
            let tx = provider.tx_ref();

            assert!(
                !provider.account_block_changeset(0)?.is_empty(),
                "genesis account change sets should still be present (only block B is skipped)"
            );
            assert!(
                provider.account_block_changeset(b)?.is_empty(),
                "import must write NO account change sets for block B"
            );
            assert!(
                provider.storage_changeset(b)?.is_empty(),
                "import must write NO storage change sets for block B"
            );

            // the plain/hashed tables, in contrast, are fully populated. the exact storage-row
            // count also includes `test_genesis`'s own system-contract slots (dumped by the
            // exporter), so assert the meaningful invariant — plain and hashed hold the same number
            // of rows — plus a floor of the whale slots and the contract's two slots.
            assert!(tx.entries::<PlainAccountState>()? >= 6, "plain accounts must be populated");
            assert!(tx.entries::<HashedAccounts>()? >= 6, "hashed accounts must be populated");
            let plain_storage_rows = tx.entries::<PlainStorageState>()?;
            assert!(
                plain_storage_rows >= MULTI_CHUNK_SLOTS as usize + 2,
                "plain storage must hold at least the whale slots plus the contract's two slots \
                 (got {plain_storage_rows})"
            );
            assert_eq!(
                tx.entries::<HashedStorages>()?,
                plain_storage_rows,
                "hashed storage rows must match plain storage rows exactly"
            );

            // spot-check the hashed tables directly: the whale's deep probe slot lives under
            // keccak256(address)/keccak256(slot) with its value, exactly as a whole-account import
            // would write it
            let hashed_addr = keccak256(big);
            let hashed_slot = keccak256(probe_slot);
            let mut cursor = tx.cursor_dup_read::<HashedStorages>()?;
            let entry = cursor
                .seek_by_key_subkey(hashed_addr, hashed_slot)?
                .filter(|e| e.key == hashed_slot)
                .expect("whale probe slot must be present in HashedStorages");
            assert_eq!(
                entry.value,
                U256::from_be_bytes(probe_value.0),
                "hashed storage value for the deep probe slot must match"
            );
        }

        // read the state back: balances, code, and deep storage slots intact
        assert_eq!(reader.last_block_number()?, b);
        let state = reader.latest()?;
        assert_eq!(state.account_balance(&eoa_a)?, Some(U256::from(100u64)));
        assert_eq!(state.account_balance(&eoa_f)?, Some(U256::from(600u64)));
        assert_eq!(state.basic_account(&big)?.expect("whale restored").nonce, 4);
        assert_eq!(
            state.account_code(&big)?.map(|c| c.original_bytes()),
            Some(code.clone()),
            "whale code must round-trip"
        );
        assert_eq!(state.storage(contract, slot_a)?, Some(U256::from(111u64)));
        assert_eq!(state.storage(contract, slot_b)?, Some(U256::from(222u64)));
        assert_eq!(
            state.storage(big, word(0))?,
            Some(U256::from(1u64)),
            "the whale's first slot (first chunk) must survive"
        );
        assert_eq!(
            state.storage(big, probe_slot)?,
            Some(U256::from_be_bytes(probe_value.0)),
            "a slot several chunks deep must survive the per-chunk import"
        );
        assert_eq!(
            state.storage(big, word(MULTI_CHUNK_SLOTS - 1))?,
            Some(U256::from(MULTI_CHUNK_SLOTS)),
            "the whale's last slot (last chunk) must survive"
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
        let h1 = synthetic_header(b, genesis_header.hash(), state_root);
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

    /// Drive the restore pipeline through the state import (open → scaffold → import) over a
    /// fresh datadir, returning the restorer ready for the entry-readiness precondition.
    fn restore_through_import(
        chain: Arc<RethChainSpec>,
        dst_dir: &Path,
        task_manager: &TaskManager,
        window: &[SealedHeader],
        final_state: BlockNumHash,
        pack_dir: &Path,
    ) -> eyre::Result<SnapshotRestorer> {
        let (reth_config, db) = temp_config_and_db(chain, dst_dir)?;
        let restorer = SnapshotRestorer::open(&reth_config, db, task_manager)?;
        restorer.import_chain_scaffold(window, final_state)?;
        let mut reader = ExecStatePackReader::open(pack_dir)?;
        restorer.import_state(&mut reader)?;
        Ok(restorer)
    }

    /// A genuine epoch-boundary snapshot passes the entry-readiness precondition end to end: the
    /// final block `B` closed epoch 0 (so the registry at `B` reports epoch 1 beginning at
    /// `B + 1`), and `B`'s own system call recorded the EIP-1559 worker's next-epoch base fee in
    /// its `WorkerConfigs.data` word — one state read at `B` seeds the restored node's first
    /// entry, no prior-epoch headers required.
    #[tokio::test]
    async fn entry_readiness_accepts_epoch_boundary_snapshot() -> eyre::Result<()> {
        const TARGET_GAS: u64 = 1_000_000;
        const START_FEE: u64 = 1_000_000;
        const EPOCH_GAS: u64 = 1_500_000;

        let genesis = test_genesis_with_consensus_registry_and_workers(5, vec![(0u8, TARGET_GAS)]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // source: a moved fee and mid-epoch gas so the close records a non-trivial word
        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Entry Readiness Accept Source");
        let acc = GasAccumulator::new(1);
        acc.base_fee(0).set_base_fee(START_FEE);
        acc.inc_block(0, EPOCH_GAS, 30_000_000);
        let source =
            RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, Some(acc))?;

        // block 1 closes epoch 0: concludeEpoch seats epoch 1 and records worker 0's fee
        let consensus_output = consensus_output_for_tests(2, 0, 1, true);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block = execute_payload_and_update_canonical_chain(&source, payload, vec![])?;
        let closing = block.recovered_block.clone_sealed_header();

        // fixture guard: the close wrote a genuine fee word (oracle value, away from MIN)
        let expected = next_base_fee_for_config(
            WorkerFeeConfig::Eip1559 { target_gas: TARGET_GAS },
            START_FEE,
            EPOCH_GAS,
        );
        assert_ne!(expected, MIN_PROTOCOL_BASE_FEE, "fixture fee must not pin at MIN");
        let (num_workers, entries) = read_worker_config_entries_at(&source, closing.hash())?;
        assert_eq!(num_workers, 1);
        assert_eq!(
            entries[0].data,
            U184::from(expected),
            "the closing block must record worker 0's next-epoch fee"
        );

        // export the boundary state; the closing block is the snapshot's final block B
        let final_state = BlockNumHash::new(closing.number, closing.hash());
        let pack_dir = TempDir::new()?;
        export_pack(&source, closing.state_root, &[closing.header().clone()], pack_dir.path());

        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Entry Readiness Accept Dest");
        let restorer = restore_through_import(
            chain,
            dst_dir.path(),
            &dst_tm,
            std::slice::from_ref(&closing),
            final_state,
            pack_dir.path(),
        )?;
        restorer.entry_readiness_precondition(final_state)?;
        restorer.finish(final_state)?;

        Ok(())
    }

    /// A snapshot whose final block did NOT close an epoch is refused, and the error names the
    /// boundary requirement: block 1 here is a mid-epoch-0 block, so the registry at `B` still
    /// reports epoch 0 with first block 0 — not an entered epoch beginning at `B + 1`.
    #[tokio::test]
    async fn entry_readiness_rejects_non_boundary_final_block() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry_and_workers(5, vec![(0u8, 1_000_000)]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Entry Readiness Non Boundary Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;

        // block 1 stays mid-epoch: no concludeEpoch, no boundary
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block = execute_payload_and_update_canonical_chain(&source, payload, vec![])?;
        let mid_epoch = block.recovered_block.clone_sealed_header();

        let final_state = BlockNumHash::new(mid_epoch.number, mid_epoch.hash());
        let pack_dir = TempDir::new()?;
        export_pack(&source, mid_epoch.state_root, &[mid_epoch.header().clone()], pack_dir.path());

        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Entry Readiness Non Boundary Dest");
        let restorer = restore_through_import(
            chain,
            dst_dir.path(),
            &dst_tm,
            &[mid_epoch],
            final_state,
            pack_dir.path(),
        )?;

        let err = restorer
            .entry_readiness_precondition(final_state)
            .expect_err("a mid-epoch final block must fail the restore");
        assert!(
            err.to_string().contains("not an epoch-closing block"),
            "error must name the boundary requirement: {err}"
        );

        Ok(())
    }

    /// An EIP-1559 row whose `data` word was governance-poisoned above `u64::MAX` fails the
    /// RESTORE (not the node's first epoch entry later), naming the worker and the oversized
    /// word.
    ///
    /// A genuine close always rewrites every EIP-1559 row's word last (the boundary system calls
    /// run in `finish`, after user transactions), so the poison lands in a follow-up block and
    /// ships under a forged-but-self-consistent boundary header: number 1 — keeping the
    /// registry's `blockHeight == B + 1` boundary claim intact — over the poisoned block's real
    /// state root. Header provenance is outside restore's trust boundary (see the module docs),
    /// so the WORD check is what catches this shape.
    #[tokio::test]
    async fn entry_readiness_rejects_poisoned_eip1559_data_word() -> eyre::Result<()> {
        const TARGET_GAS: u64 = 1_000_000;
        let poison = U184::from((u64::MAX as u128) + 1);

        let genesis = test_genesis_with_consensus_registry_and_workers(5, vec![(0u8, TARGET_GAS)]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Entry Readiness Poison Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;

        // block 1 closes epoch 0 (the close itself records a sane word for worker 0)
        let consensus_output = consensus_output_for_tests(2, 0, 1, true);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&source, payload, vec![])?;
        let closing = block1.recovered_block.clone_sealed_header();

        // block 2 (mid-epoch-1): the owner rewrites worker 0's row with an oversized data word;
        // no close runs here, so nothing rewrites it back
        let mut governance = governance_owner_factory();
        let poison_tx = governance.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(WORKER_CONFIGS_ADDRESS),
            U256::ZERO,
            WorkerConfigs::setWorkerConfigCall {
                workerId: 0,
                strategy: 0,
                value: TARGET_GAS,
                data: poison,
            }
            .abi_encode()
            .into(),
        );
        let consensus_output = consensus_output_for_tests(2, 1, 2, false);
        let payload = TNPayload::new_for_test(closing.clone(), &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&source, payload, vec![poison_tx])?;
        let poisoned = block2.recovered_block.clone_sealed_header();

        // fixture guard: the oversized word stuck on an EIP-1559 row
        let (_, entries) = read_worker_config_entries_at(&source, poisoned.hash())?;
        assert_eq!(entries[0].data, poison, "the owner tx must land the oversized word");
        assert!(
            matches!(entries[0].config, WorkerFeeConfig::Eip1559 { .. }),
            "the poisoned row must still decode as EIP-1559"
        );

        // forge a self-consistent boundary header over the poisoned state: number 1 (so the
        // registry's blockHeight 2 still reads as B + 1) with block 2's real state root
        let forged = synthetic_header(1, chain.sealed_genesis_header().hash(), poisoned.state_root);
        let final_state = BlockNumHash::new(forged.number, forged.hash());
        let pack_dir = TempDir::new()?;
        export_pack(&source, poisoned.state_root, &[forged.header().clone()], pack_dir.path());

        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Entry Readiness Poison Dest");
        let restorer = restore_through_import(
            chain,
            dst_dir.path(),
            &dst_tm,
            &[forged],
            final_state,
            pack_dir.path(),
        )?;

        let err = restorer
            .entry_readiness_precondition(final_state)
            .expect_err("a poisoned EIP-1559 data word must fail the restore");
        let msg = err.to_string();
        assert!(msg.contains("worker 0"), "error must name the worker: {msg}");
        assert!(msg.contains("exceeds u64::MAX"), "error must name the oversized word: {msg}");

        Ok(())
    }

    /// A `Static` row's garbage `data` word is ACCEPTED: the fee lives in the config's value and
    /// the entry read never consults `data` for `Static` rows, so the precondition is
    /// deliberately non-strict here. The garbage rides the closing block itself — user
    /// transactions execute before the boundary system calls in `finish`, and the close never
    /// writes `Static` rows, so the word survives to `B` on a GENUINE boundary.
    #[tokio::test]
    async fn entry_readiness_accepts_static_worker_with_garbage_data() -> eyre::Result<()> {
        const STATIC_FEE: u64 = 777;
        let garbage = U184::MAX;

        let genesis = test_genesis_with_consensus_registry_and_workers(5, vec![(1u8, STATIC_FEE)]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let src_dir = TempDir::new()?;
        let src_tm = TaskManager::new("Entry Readiness Static Source");
        let source = RethEnv::new_for_temp_chain(chain.clone(), src_dir.path(), &src_tm, None)?;

        // the owner writes the garbage word in the closing block; the all-Static close records
        // nothing, so the word is still there when concludeEpoch seals the boundary
        let mut governance = governance_owner_factory();
        let garbage_tx = governance.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(WORKER_CONFIGS_ADDRESS),
            U256::ZERO,
            WorkerConfigs::setWorkerConfigCall {
                workerId: 0,
                strategy: 1,
                value: STATIC_FEE,
                data: garbage,
            }
            .abi_encode()
            .into(),
        );
        let consensus_output = consensus_output_for_tests(2, 0, 1, true);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block = execute_payload_and_update_canonical_chain(&source, payload, vec![garbage_tx])?;
        let closing = block.recovered_block.clone_sealed_header();

        // fixture guards: B is a genuine boundary AND the static row carries the garbage word
        let (num_workers, entries) = read_worker_config_entries_at(&source, closing.hash())?;
        assert_eq!(num_workers, 1);
        assert_eq!(entries[0].config, WorkerFeeConfig::Static { fee: STATIC_FEE });
        assert_eq!(
            entries[0].data, garbage,
            "the garbage word must survive the close on a Static row"
        );

        let final_state = BlockNumHash::new(closing.number, closing.hash());
        let pack_dir = TempDir::new()?;
        export_pack(&source, closing.state_root, &[closing.header().clone()], pack_dir.path());

        let dst_dir = TempDir::new()?;
        let dst_tm = TaskManager::new("Entry Readiness Static Dest");
        let restorer = restore_through_import(
            chain,
            dst_dir.path(),
            &dst_tm,
            &[closing],
            final_state,
            pack_dir.path(),
        )?;

        // accepted: data is ignored for Static rows
        restorer.entry_readiness_precondition(final_state)?;
        restorer.finish(final_state)?;

        Ok(())
    }
}
