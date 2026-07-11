//! Pinned, read-consistent view over reth's plain EVM state for snapshot export.
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
    DatabaseEnv, RethDatabaseT, RethEnv,
};
use alloy::consensus::constants::KECCAK_EMPTY;
use reth_db::{
    cursor::DbCursorRO,
    tables::{BlockBodyIndices, Bytecodes, HeaderNumbers, PlainAccountState, PlainStorageState},
    transaction::DbTx,
};
use reth_provider::{DBProvider, DatabaseProviderFactory, ProviderError};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    io::{self, Write},
};
use tn_types::{Address, BlockNumHash, Bytes, GenesisAccount, B256};

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
                                    TnRethError::EVMCustom(format!(
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
    TnRethError::EVMCustom(format!("snapshot export: failed writing state dump: {e}"))
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

#[cfg(test)]
mod tests {
    use super::{GenesisAccountWithAddress, PinnedStateView, StateRootLine};
    use crate::{RethChainSpec, RethEnv};
    use alloy::hex;
    use reth_db::{cursor::DbCursorRW, tables::PlainStorageState, transaction::DbTxMut};
    use reth_primitives_traits::StorageEntry;
    use reth_provider::{DBProvider, DatabaseProviderFactory};
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };
    use tempfile::TempDir;
    use tn_types::{
        test_genesis, Address, BlockNumHash, Bytes, GenesisAccount, TaskManager, B256, U256,
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
}
