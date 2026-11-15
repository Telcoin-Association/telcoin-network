//! Format-freeze tests: TN's on-disk execution formats, pinned by checked-in fixtures.
//!
//! A seeded generator ([`Plan`]) builds one small chain in three tn-lmdb datadirs ([`FIXTURES`]):
//! storage v1 and storage v2 storing their transactions inline (`v1`, `v2`: layout marker version
//! 1, the format of every datadir before transactions were stored once), and storage v2 storing
//! each transaction as its position in its consensus batch (`v2-ptr`: marker version 2, version-3
//! `Transactions` jars). The pointer chain is built from real batches under their digests, as
//! consensus commits them; they are checked in beside its datadir (`batches.bcs`) and stand in for
//! the consensus packs a node reads them from. Each datadir is checked in under
//! `tests/fixtures/format-freeze/` with its digests (`digests.txt`), and two tests hold them:
//!
//! - [`format_fixtures_read_back_unchanged`] (reader compatibility): opens a copy of each
//!   checked-in datadir with the current code, through `RethEnv::new`, and reads back every row of
//!   every reth table, every row of every static-file jar, the chain through the provider's readers
//!   (headers, bodies, transactions, receipts, senders, both changesets), the plan's accounts at
//!   every block through the historical state provider (account, code, slots 0 to 9), the
//!   registry's epoch state at every header, and the state-pack export. A pointer datadir's
//!   transactions resolve from its `batches.bcs`, decoded into a [`MemoryBatches`] of their own
//!   under each batch's recomputed digest (the `read batches` line). Each read is digested and
//!   compared with the `read` lines; the pointer fixture's transactions, senders, receipts, change
//!   sets and account history must also read as its inline twin's (`v2`, [`SAME_AS_INLINE`]). A
//!   binary that can no longer read today's bytes, or reads them as other values, fails here.
//! - [`format_writer_output_unchanged`] (writer stability): builds the chain again in a temp dir
//!   and compares the `write` lines: each tn-lmdb table's ordered raw (key, value) pairs, and each
//!   static file's bytes (data, `.off`, `.csoff`, `.conf`; the decompressed rows instead of the
//!   data and `.off` of a jar with a compressor), and a pointer fixture's batch encoding (`write
//!   batches.bcs`). The tn-lmdb file's own bytes are not pinned: transaction ids and free pages
//!   differ between runs. A writer change fails here.
//! - [`format_inline_fixtures_stay_inline`]: the current binary, asked for pointer rows as a new
//!   datadir is, opens `v1` and `v2` as inline datadirs, keeps their marker, and stores the
//!   transactions of a block it executes on them inline.
//!
//! The first two also check the format version markers each fixture records (`version` lines) and
//! the block hashes (`block` lines): a different block hash means execution changed, not storage.
//!
//! **The chain** ([`Plan`]), blocks 0 to [`TIP`], from a fixed genesis (`genesis.json`, checked in
//! so a later contract artifact cannot change the chain): legacy, EIP-2930, EIP-1559 and EIP-7702
//! transactions, with and without access lists; value transfers that create accounts; a genesis
//! slot zeroed, slots set and zeroed later; a contract created by a transaction and one created
//! and self-destructed in one transaction; an empty genesis account removed by a touch (EIP-161);
//! receipts with logs (`LOG0` to `LOG4`, so each such receipt is stored zstd-compressed); EIP-7702
//! delegations to a contract, one cleared later by an authorization to the zero address and one
//! held at the tip, and calls that run a delegated account's code; an epoch close, which rewrites
//! `ConsensusRegistry` slots; and an output that executes two blocks. Storage wipes of existing
//! slots cannot happen from Cancun on (EIP-6780), so none is covered. [`Plan::execute`] fails if
//! the plan stops producing the logs or the delegations. In the pointer chain, block 5's batch
//! also holds a copy of block 4's last transaction first (a transaction two producers packed),
//! which execution skips, so its block's transactions sit at batch positions 1 and 2.
//!
//! **Not pinned**: `VersionHistory` (each open's wall clock and client version), the tn-lmdb file
//! bytes, the compressed bytes of the headers jar, and anything outside the reth datadir (the
//! consensus DB and packs: `batches.bcs` pins the batches, not a pack's encoding of them). The
//! chain is short, so it also holds no history shard split past `NUM_OF_INDICES_IN_SHARD` (2,000
//! indices), no jar past block 499,999, no prune checkpoint or snapshot-restore scaffold
//! (`crate::snapshot`), and no datadir migrated from MDBX.
//!
//! **Regenerating** is explicit, never part of a test run, and names the fixtures it writes: see
//! [`regenerate_format_fixtures`] and the crate README, "On-disk format versions". It refuses to
//! rewrite a fixture whose `version` lines differ from the build's.

use super::{
    database::TN_LMDB_DB_VERSION,
    layout::{stored_marker, LAYOUT_VERSION_INLINE_TXS, TN_LAYOUT_KEY},
};
use crate::{
    init_reth_defaults,
    payload::TNPayload,
    test_utils::{
        consensus_output_for_tests_at, test_genesis_with_consensus_registry_and_workers,
        TransactionFactory,
    },
    DatadirFormat, MaybePlatformPath, MemoryBatches, NewCanonicalChain, OutputBuildScope,
    OutputTrieOverlay, RethChainSpec, RethConfig, RethDb, RethEnv, StorageLayout, TxBodies,
};
use alloy::{
    consensus::{TxEip2930, TxEip7702},
    eips::eip2930::{AccessList, AccessListItem},
    primitives::TxKind,
    sol_types::SolValue as _,
};
use rand::{rngs::StdRng, SeedableRng as _};
use reth_db::{
    cursor::DbCursorRO as _,
    static_file::iter_static_files,
    table::Table,
    tables::{TableViewer, Tables},
    transaction::DbTx,
    RawTable,
};
use reth_node_core::{args::DatadirArgs, node_config::NodeConfig};
use reth_provider::{
    providers::{JarFormat, TxRow},
    AccountReader as _, BlockBodyIndicesProvider as _, ChangeSetReader as _,
    DatabaseProviderFactory as _, HeaderProvider as _, MetadataProvider as _, ReceiptProvider as _,
    StateProviderFactory as _, StaticFileProviderFactory as _, StaticFileSegment,
    StorageChangeSetReader as _, StorageSettingsCache as _, TransactionsProvider as _,
};
use sha2::{Digest as _, Sha256};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    fmt::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::TempDir;
use tn_storage::exec_state_pack::EXEC_STATE_PACK_VERSION;
use tn_types::{
    gas_accumulator::GasAccumulator, Address, Batch, Bytes, CertifiedBatch, ConsensusHeader,
    ConsensusOutput, Decodable2718 as _, Encodable2718 as _, Genesis, GenesisAccount, Hash as _,
    SealedHeader, TaskManager, TransactionSigned, B256, U256,
};

/// The fixtures' directory.
fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format-freeze")
}

/// The fixtures: each one's directory under [`fixtures_dir`] and the datadir format it holds.
/// `v1` and `v2` store their transactions inline, as every datadir written before pointer rows
/// does; `v2-ptr` stores them by position in the batches beside it ([`BATCHES_FILE`]).
const FIXTURES: [(&str, DatadirFormat); 3] = [
    ("v1", DatadirFormat { layout: StorageLayout::V1, tx_bodies: TxBodies::Inline }),
    ("v2", DatadirFormat { layout: StorageLayout::V2, tx_bodies: TxBodies::Inline }),
    ("v2-ptr", DatadirFormat { layout: StorageLayout::V2, tx_bodies: TxBodies::Pointers }),
];

/// The fixture a pointer fixture must read back as, line for line, for the lines that do not
/// depend on where the transactions are stored ([`SAME_AS_INLINE`]).
const INLINE_TWIN: &str = "v2";

/// The `read` lines a pointer fixture shares with its [`INLINE_TWIN`]: every transaction (by id,
/// with its hash), sender and receipt, the change sets, and the plan's accounts and the registry's
/// epoch state at every block. Its headers name real batch digests, so its block hashes differ,
/// and with them the parent hashes the block-hash history contract (EIP-2935) stores: its headers,
/// hashed storage, account trie and state export differ.
const SAME_AS_INLINE: [&str; 8] = [
    "read chain/account_changesets",
    "read chain/body_indices",
    "read chain/receipts",
    "read chain/senders",
    "read chain/storage_changesets",
    "read chain/transactions",
    "read history/accounts",
    "read history/epochs",
];

/// The batches of a fixture that stores transactions by position, beside its datadir: the BCS
/// encoding of the `Vec<Batch>` its outputs carried, in output order.
const BATCHES_FILE: &str = "batches.bcs";

/// The env var that names the fixtures [`regenerate_format_fixtures`] writes.
const REGENERATE_VAR: &str = "TN_REGENERATE_FORMAT_FIXTURES";

/// What a failing test tells its reader to do.
const WHAT_TO_DO: &str = "\
If this change to an on-disk format is intended: bump the version marker that covers it \
(crates/tn-reth/README.md, \"On-disk format versions\"), keep this fixture with a test that the \
new binary still opens it or refuses it, and add a fixture for the new format (README: \
regenerating the fixtures). If it is not intended, it is a bug: a binary with this change would \
misread or rewrite mainnet datadirs.";

/// What a failing version check tells its reader to do.
const WHAT_TO_DO_VERSION: &str = "\
A version marker changed. Keep this fixture: move it to its own directory (e.g. v2 -> \
v2-layout1), with a test of what the new binary does with it (opens, converts or refuses it), and \
add a fixture for the new version (crates/tn-reth/README.md, \"On-disk format versions\": \
regenerating the fixtures). An unintended version change is a bug.";

/// The genesis timestamp, fixed (2026-01-01T00:00:00Z) so the chain is the same on every run.
const GENESIS_TIME: u64 = 1_767_225_600;

/// The highest block the plan executes.
const TIP: u64 = 9;

/// A contract that stores its second calldata word at the slot its first word names:
/// `SSTORE(calldataload(0), calldataload(32))`.
const STORE_CODE: &[u8] = &[0x60, 0x20, 0x35, 0x60, 0x00, 0x35, 0x55, 0x00];

/// A contract that logs its calldata five times: `LOG0` to `LOG4`, with topics 1 to 4.
const LOG_CODE: &[u8] = &[
    0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // CALLDATACOPY(0, 0, CALLDATASIZE)
    0x36, 0x60, 0x00, 0xa0, // LOG0(0, CALLDATASIZE)
    0x60, 0x01, 0x36, 0x60, 0x00, 0xa1, // LOG1(0, CALLDATASIZE, 1)
    0x60, 0x02, 0x60, 0x01, 0x36, 0x60, 0x00, 0xa2, // LOG2(.., 1, 2)
    0x60, 0x03, 0x60, 0x02, 0x60, 0x01, 0x36, 0x60, 0x00, 0xa3, // LOG3(.., 1, 2, 3)
    0x60, 0x04, 0x60, 0x03, 0x60, 0x02, 0x60, 0x01, 0x36, 0x60, 0x00,
    0xa4, // LOG4(.., 1, 2, 3, 4)
    0x00,
];

/// Init code deploying `code` (at most 255 bytes): `CODECOPY(0, 12, len)`, then `RETURN(0, len)`.
fn init_code(code: &[u8]) -> Bytes {
    let len = u8::try_from(code.len()).expect("init_code deploys at most 255 bytes");
    [&[0x60, len, 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, len, 0x60, 0x00, 0xf3], code].concat().into()
}

/// Init code that stores 1 at slot 0, then self-destructs in its own creation:
/// `SSTORE(0, 1)`, `SELFDESTRUCT(CALLER)`.
const DESTRUCT_INIT: &[u8] = &[0x60, 0x01, 0x60, 0x00, 0x55, 0x33, 0xff];

/// The genesis-allocated [`STORE_CODE`] contract, with slots 1, 2 and 3 set.
const STORE: Address = Address::repeat_byte(0x51);

/// An empty genesis account (no balance, nonce or code): a zero-value call removes it (EIP-161).
const EMPTY: Address = Address::repeat_byte(0xe0);

/// Accounts first created by a value transfer.
const FRESH: [Address; 2] = [Address::repeat_byte(0xf1), Address::repeat_byte(0xf2)];

/// A 32-byte word holding `n`.
fn word(n: u64) -> B256 {
    B256::from(U256::from(n))
}

/// The calldata storing `value` at `slot` in a [`STORE_CODE`] contract.
fn store(slot: u64, value: u64) -> Bytes {
    [word(slot).0, word(value).0].concat().into()
}

/// The genesis of a new fixture set: the test genesis with a fresh `ConsensusRegistry`, [`STORE`]
/// and [`EMPTY`], at [`GENESIS_TIME`]. Must run inside a tokio runtime (the registry deploys
/// through a temporary env). Fixtures are built from the checked-in copy ([`load_genesis`]).
fn new_genesis() -> Genesis {
    let mut genesis = test_genesis_with_consensus_registry_and_workers(4, vec![(0, 30_000_000)])
        .extend_accounts([
            (
                STORE,
                GenesisAccount {
                    code: Some(Bytes::from_static(STORE_CODE)),
                    storage: Some(BTreeMap::from([
                        (word(1), word(1)),
                        (word(2), word(2)),
                        (word(3), word(3)),
                    ])),
                    ..Default::default()
                },
            ),
            (EMPTY, GenesisAccount::default()),
        ]);
    genesis.timestamp = GENESIS_TIME;
    genesis
}

/// The checked-in genesis.
fn load_genesis() -> eyre::Result<Genesis> {
    let path = fixtures_dir().join("genesis.json");
    let text = std::fs::read_to_string(&path).map_err(|e| {
        eyre::eyre!("read {}: {e} (see regenerate_format_fixtures)", path.display())
    })?;
    Ok(serde_json::from_str(&text)?)
}

/// One consensus output: what it commits and the signed transactions of each of its batches, one
/// block a batch.
struct PlannedOutput {
    output: ConsensusOutput,
    batches: Vec<Vec<Vec<u8>>>,
}

/// The output whose second batch, in the pointer chain, starts with a copy of its first batch's
/// last transaction (blocks 4 and 5), as when two producers pack one transaction: execution skips
/// the copy (its nonce is used), so the block keeps batch positions 1 and 2.
const REPACKED_OUTPUT: usize = 3;

impl PlannedOutput {
    /// This output as consensus commits it, for a datadir that stores transactions by position:
    /// each block's transactions in a real [`Batch`] of the output's epoch, carried under its
    /// digest, with the copy of [`REPACKED_OUTPUT`] when `repack`. Also the batches, in order.
    fn with_batches(&self, repack: bool) -> eyre::Result<(ConsensusOutput, Vec<Batch>)> {
        let epoch = self.output.sub_dag().leader_epoch();
        let mut batches: Vec<Batch> = self
            .batches
            .iter()
            .map(|txs| Batch { transactions: txs.clone(), epoch, ..Default::default() })
            .collect();
        if repack {
            let copy = batches[0].transactions.last().cloned();
            let copy = copy.ok_or_else(|| eyre::eyre!("no transaction to repack"))?;
            batches[1].transactions.insert(0, copy);
        }
        let output = ConsensusOutput::new(
            self.output.sub_dag().clone(),
            ConsensusHeader::default().digest(),
            self.output.number(),
            self.output.close_epoch(),
            batches.iter().map(Batch::digest).collect(),
            vec![CertifiedBatch { address: Address::ZERO, batches: batches.clone() }],
        );
        // the consensus digest covers the sub-DAG, not the batches: the blocks keep the
        // inline chain's consensus fields
        eyre::ensure!(output.digest() == self.output.digest(), "the output's digest moved");
        Ok((output, batches))
    }
}

/// The chain the fixtures hold: its spec, its outputs in order, and the accounts it touches.
struct Plan {
    chain: Arc<RethChainSpec>,
    outputs: Vec<PlannedOutput>,
    /// The accounts the reader's historical reads cover.
    accounts: Vec<Address>,
    /// The two accounts block 8 and block 9 delegate (EIP-7702): the first is cleared in block 9,
    /// the second holds its delegation at the tip.
    delegated: [Address; 2],
}

impl Plan {
    /// The fixed plan over `genesis`. Every transaction is signed by the governance owner (seed
    /// 33, funded in the genesis), from nonce 0; the delegated accounts (seeds 34 and 35) sign
    /// only their EIP-7702 authorizations.
    fn new(genesis: Genesis) -> Self {
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let chain_id = chain.chain.id();
        let mut owner = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let created = owner.address().create(4);
        let mut call = |to: Option<Address>, value: u64, input: Bytes| {
            owner.create_eip1559_encoded(chain.clone(), None, 100, to, U256::from(value), input)
        };

        let block1 = vec![
            call(Some(STORE), 0, store(2, 0)),
            call(Some(STORE), 0, store(4, 44)),
            call(Some(EMPTY), 0, Bytes::new()),
            call(Some(FRESH[0]), 1, Bytes::new()),
            call(None, 0, init_code(STORE_CODE)),
            call(None, 0, Bytes::from_static(DESTRUCT_INIT)),
        ];
        let block2 = vec![
            call(Some(STORE), 0, store(4, 0)),
            call(Some(STORE), 0, store(5, 55)),
            call(Some(STORE), 0, store(1, 111)),
        ];
        let block4 =
            vec![call(Some(STORE), 0, store(1, 1111)), call(Some(FRESH[1]), 1, Bytes::new())];
        let block5 = vec![call(Some(created), 0, store(7, 77)), call(Some(STORE), 0, store(6, 66))];
        let mut block6 = vec![call(Some(STORE), 0, store(1, 11_111))];
        block6.push(
            owner
                .create_explicit_legacy_tx(
                    Some(chain_id),
                    None,
                    Some(100),
                    None,
                    Some(FRESH[0]),
                    Some(U256::from(2)),
                    None,
                )
                .encoded_2718(),
        );
        block6.push(owner.create_eip2930(chain_id, None, 100, STORE).encoded_2718());
        block6.push(owner.create_eip7702(chain_id, None, 100).encoded_2718());

        // block 6's authorization is the owner's own, and applying it bumped the owner's nonce
        owner.inc_nonce();
        let logger = owner.address().create(owner.nonce());
        let delegates = [34, 35]
            .map(|seed| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(seed)));
        let delegated = delegates.each_ref().map(TransactionFactory::address);
        let data = |len: u8| Bytes::from((0..len).collect::<Vec<u8>>());
        let access_list = |items: &[(Address, &[u64])]| {
            AccessList(
                items
                    .iter()
                    .map(|&(address, slots)| AccessListItem {
                        address,
                        storage_keys: slots.iter().map(|&slot| word(slot)).collect(),
                    })
                    .collect(),
            )
        };
        let block7 = vec![
            owner.create_eip1559_encoded(
                chain.clone(),
                None,
                100,
                None,
                U256::ZERO,
                init_code(LOG_CODE),
            ),
            owner
                .sign_transaction(TxEip2930 {
                    chain_id,
                    nonce: owner.nonce(),
                    gas_price: 100,
                    gas_limit: 1_000_000,
                    to: TxKind::Call(STORE),
                    value: U256::ZERO,
                    access_list: access_list(&[(STORE, &[1, 8]), (FRESH[0], &[])]),
                    input: store(8, 88),
                })
                .encoded_2718(),
        ];
        let block8 = vec![
            // the first account delegates to the logger, in a call to another contract
            owner
                .sign_transaction(TxEip7702 {
                    chain_id,
                    nonce: owner.nonce(),
                    gas_limit: 1_000_000,
                    max_fee_per_gas: 100,
                    max_priority_fee_per_gas: 0,
                    to: STORE,
                    value: U256::ZERO,
                    access_list: access_list(&[(STORE, &[9])]),
                    authorization_list: vec![delegates[0].authorize(chain_id, logger, 0)],
                    input: store(9, 99),
                })
                .encoded_2718(),
            // a call to the delegated account runs the logger's code
            owner.create_eip1559_encoded(
                chain.clone(),
                None,
                100,
                Some(delegated[0]),
                U256::ZERO,
                data(40),
            ),
            owner.create_eip1559_encoded(
                chain.clone(),
                None,
                100,
                Some(logger),
                U256::ZERO,
                data(100),
            ),
        ];
        // the first delegation is cleared and the second set, in a call to the second account
        let block9 = vec![owner
            .sign_transaction(TxEip7702 {
                chain_id,
                nonce: owner.nonce(),
                gas_limit: 1_000_000,
                max_fee_per_gas: 100,
                max_priority_fee_per_gas: 0,
                to: delegated[1],
                value: U256::ZERO,
                access_list: access_list(&[(logger, &[])]),
                authorization_list: vec![
                    delegates[0].authorize(chain_id, Address::ZERO, 1),
                    delegates[1].authorize(chain_id, logger, 0),
                ],
                input: data(64),
            })
            .encoded_2718()];

        let output = |round, epoch, index: u64, close| {
            consensus_output_for_tests_at(round, epoch, index, close, GENESIS_TIME + 10 * index)
        };
        let outputs = vec![
            PlannedOutput { output: output(2, 0, 1, false), batches: vec![block1] },
            PlannedOutput { output: output(4, 0, 2, false), batches: vec![block2] },
            // block 3 closes epoch 0
            PlannedOutput { output: output(6, 0, 3, true), batches: vec![Vec::new()] },
            // blocks 4 and 5: one output, two batches
            PlannedOutput { output: output(2, 1, 4, false), batches: vec![block4, block5] },
            PlannedOutput { output: output(4, 1, 5, false), batches: vec![block6] },
            PlannedOutput { output: output(6, 1, 6, false), batches: vec![block7] },
            PlannedOutput { output: output(8, 1, 7, false), batches: vec![block8] },
            PlannedOutput { output: output(10, 1, 8, false), batches: vec![block9] },
        ];
        let owner = owner.address();
        let mut accounts =
            vec![owner, STORE, EMPTY, created, owner.create(5), logger, Address::ZERO];
        accounts.extend(FRESH.into_iter().chain(delegated));
        Self { chain, outputs, accounts, delegated }
    }

    /// Execute every output on `env`, from its genesis, as the engine does: one trie overlay an
    /// output, each block made canonical in memory before the next builds, the output's blocks
    /// persisted together.
    ///
    /// With [`TxBodies::Inline`] each block names a placeholder batch digest (its number) and the
    /// persist gets no positions, as the inline fixtures were built. With [`TxBodies::Pointers`]
    /// each output carries its blocks' transactions as real batches
    /// ([`PlannedOutput::with_batches`]), signers are recovered with the batch digests checked,
    /// and the persist gets each block's batch positions; the batches are returned, in order.
    fn execute(&self, env: &RethEnv, tx_bodies: TxBodies) -> eyre::Result<Vec<Batch>> {
        let canonical = env.blockchain_provider().canonical_in_memory_state();
        let mut parent = header(env, 0)?;
        let mut logs = 0;
        let mut all_batches = Vec::new();
        for (index, planned) in self.outputs.iter().enumerate() {
            let (output, recovered) = match tx_bodies {
                TxBodies::Inline => (planned.output.clone(), Vec::new()),
                TxBodies::Pointers => {
                    let (output, batches) = planned.with_batches(index == REPACKED_OUTPUT)?;
                    all_batches.extend(batches);
                    let recovered = RethEnv::recover_output_signers(&output);
                    (output, recovered)
                }
            };
            let mut recovered = recovered.into_iter();
            let mut overlay = OutputTrieOverlay::new();
            let mut scope = OutputBuildScope::new();
            let (mut blocks, mut positions) = (Vec::new(), Vec::new());
            for (batch_index, txs) in planned.batches.iter().enumerate() {
                let number = parent.number + 1;
                let block = if tx_bodies == TxBodies::Inline {
                    let mut payload = TNPayload::new_for_test_with(
                        parent,
                        &output,
                        Address::ZERO,
                        B256::with_last_byte(number as u8),
                        B256::with_last_byte(number as u8),
                    );
                    payload.batch_index = batch_index;
                    env.build_block_from_batch_payload(payload, txs, &mut overlay)?
                } else {
                    let digest = output
                        .get_batch_digest(batch_index)
                        .ok_or_else(|| eyre::eyre!("block {number}: no batch digest"))?;
                    let mut payload = TNPayload::new_for_test_with(
                        parent,
                        &output,
                        Address::ZERO,
                        digest,
                        B256::with_last_byte(number as u8),
                    );
                    payload.batch_index = batch_index;
                    let recovered = recovered.next().ok_or_else(|| eyre::eyre!("no signers"))?;
                    let built =
                        env.build_block_from_recovered_payload(payload, recovered, &mut scope)?;
                    // each kept transaction's index in its batch, past the repacked copy
                    let first = u32::from(index == REPACKED_OUTPUT && batch_index == 1);
                    let expected: Vec<u32> = (first..first + txs.len() as u32).collect();
                    eyre::ensure!(
                        built.tx_positions.as_deref() == Some(expected.as_slice()),
                        "block {number}: positions {:?}, not {expected:?}",
                        built.tx_positions
                    );
                    positions.push(built.tx_positions);
                    built.block
                };
                let included = block.recovered_block.body().transactions.len();
                eyre::ensure!(
                    included == txs.len(),
                    "block {number} includes {included} of its {} transactions",
                    txs.len()
                );
                // a reverted transaction would not write what the plan covers
                let receipts = &block.execution_output.result.receipts;
                if let Some(at) = receipts.iter().position(|receipt| !receipt.success) {
                    eyre::bail!("block {number}: transaction {at} reverted");
                }
                logs += receipts.iter().map(|receipt| receipt.logs.len()).sum::<usize>();
                parent = block.recovered_block.clone_sealed_header();
                canonical.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
                canonical.set_canonical_head(parent.clone());
                blocks.push(block);
            }
            // `finish_executing_output`'s two halves, the engine's way: an inline persist gets
            // no positions, and the cache fill then has nothing to take
            env.persist_executed_output(&blocks, &positions)?;
            env.cache_persisted_batches(&output, &positions);
            env.announce_executed_output(blocks, None)?;
            env.finalize_block(parent.clone())?;
        }
        eyre::ensure!(parent.number == TIP, "the plan ends at block {}, not {TIP}", parent.number);
        // what a later edit of the plan must not lose: receipts with logs (each one compressed),
        // one delegation cleared and one kept
        eyre::ensure!(logs == 15, "the plan's receipts hold {logs} logs, not 15");
        let state = env.blockchain_provider().latest()?;
        let cleared = state.basic_account(&self.delegated[0])?.unwrap_or_default();
        eyre::ensure!(
            cleared.nonce == 2 && state.account_code(&self.delegated[0])?.is_none(),
            "the first delegation was not set and cleared: {cleared:?}"
        );
        eyre::ensure!(
            state.account_code(&self.delegated[1])?.is_some_and(|code| code.is_eip7702()),
            "the second account holds no delegation at the tip"
        );
        Ok(all_batches)
    }
}

/// The header of block `number`.
fn header(env: &RethEnv, number: u64) -> eyre::Result<SealedHeader> {
    env.sealed_header_by_number(number)?.ok_or_else(|| eyre::eyre!("no header {number}"))
}

/// An open env over a datadir, with what keeps it open.
struct Opened {
    env: RethEnv,
    db: RethDb,
    _tasks: TaskManager,
}

impl Opened {
    /// Open the datadir at `dir` with `chain`, which must be in `format` (a new datadir is
    /// created in it). The env reads the batches of a datadir storing transactions by position
    /// from `batches`; without them, a read of such a transaction fails.
    fn open(
        dir: &Path,
        chain: Arc<RethChainSpec>,
        format: DatadirFormat,
        batches: Option<Arc<MemoryBatches>>,
    ) -> eyre::Result<Self> {
        let opened = Self::open_asking(dir, chain, format, batches)?;
        let found = opened.env.datadir_format();
        eyre::ensure!(found == format, "the datadir at {} is {found}, not {format}", dir.display());
        Ok(opened)
    }

    /// Open the datadir at `dir` with `chain`, asking a new datadir for `asked`: an existing one
    /// keeps its own format. `batches` as for [`Self::open`].
    fn open_asking(
        dir: &Path,
        chain: Arc<RethChainSpec>,
        asked: DatadirFormat,
        batches: Option<Arc<MemoryBatches>>,
    ) -> eyre::Result<Self> {
        init_reth_defaults();
        let node_config = NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(dir.to_path_buf()),
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            ..NodeConfig::default()
        };
        let config = RethConfig::from_node_config(node_config)
            .with_new_datadir_layout(asked.layout)
            .with_new_datadir_tx_bodies(asked.tx_bodies);
        let config = match batches {
            Some(batches) => config.with_memory_batches_from(batches),
            None => config,
        };
        let tasks = TaskManager::new("format freeze");
        // where the node keeps it (`TelcoinDirs::reth_db_path`)
        let db = RethEnv::new_database(&config, dir.join("db"))?;
        let env = RethEnv::new(&config, &tasks, db.clone(), None, GasAccumulator::default())?;
        let running = env.blockchain_provider().database_provider_ro()?.cached_storage_settings();
        let layout = env.datadir_format().layout;
        eyre::ensure!(
            running.is_v2() == (layout == StorageLayout::V2),
            "the datadir at {} runs {running:?}, not {layout:?}",
            dir.display()
        );
        Ok(Self { env, db, _tasks: tasks })
    }

    /// Close cleanly: tn-lmdb trims its file's preallocated tail when its last handle drops.
    fn close(self) -> eyre::Result<()> {
        let Self { env, db, _tasks: tasks } = self;
        drop(env);
        drop(tasks);
        eyre::ensure!(Arc::strong_count(&db) == 1, "the database is still in use at close");
        drop(db);
        Ok(())
    }
}

/// A digest under construction: a SHA-256 over length-prefixed parts, and how many rows.
#[derive(Default)]
struct Digest {
    hasher: Sha256,
    rows: u64,
}

impl Digest {
    /// Add one row of `parts`.
    fn row(&mut self, parts: &[&[u8]]) {
        for part in parts {
            self.hasher.update((part.len() as u64).to_le_bytes());
            self.hasher.update(part);
        }
        self.rows += 1;
    }

    /// Add one row: `value` as JSON.
    fn json(&mut self, value: &impl serde::Serialize) -> eyre::Result<()> {
        self.row(&[&serde_json::to_vec(value)?]);
        Ok(())
    }

    /// The line value: rows and the hex digest.
    fn finish(self) -> String {
        format!("{} {:x}", self.rows, self.hasher.finalize())
    }

    /// The line value of a whole file's bytes.
    fn file(path: &Path) -> eyre::Result<String> {
        let bytes = std::fs::read(path).map_err(|e| eyre::eyre!("read {}: {e}", path.display()))?;
        Ok(Self::bytes(&bytes))
    }

    /// The line value of `bytes`, as of a file holding them.
    fn bytes(bytes: &[u8]) -> String {
        format!("{} {:x}", bytes.len(), Sha256::digest(bytes))
    }
}

/// A fixture's lines: `"<kind> <name>"` to the rest of the line.
type Lines = BTreeMap<String, String>;

/// Digest every reth table of `tx` into `lines` as `"<kind> table/<name>"`, in [`Tables::ALL`]'s
/// order, skipping `VersionHistory`; `raw` digests the stored bytes of each (key, value), else
/// their decoded values as JSON. A table the file lacks is `absent`. Each walk's row count must be
/// the engine's own.
fn digest_tables<TX: DbTx>(db: &RethDb, tx: &TX, raw: bool, lines: &mut Lines) -> eyre::Result<()> {
    struct Walk<'a, TX> {
        tx: &'a TX,
        raw: bool,
        out: RefCell<Digest>,
    }
    impl<TX: DbTx> TableViewer<()> for Walk<'_, TX> {
        type Error = eyre::Report;

        fn view<T: Table>(&self) -> Result<(), Self::Error> {
            let mut out = self.out.borrow_mut();
            if self.raw {
                let mut cursor = self.tx.cursor_read::<RawTable<T>>()?;
                for row in cursor.walk(None)? {
                    let (key, value) = row?;
                    out.row(&[key.raw_key(), value.raw_value()]);
                }
            } else {
                let mut cursor = self.tx.cursor_read::<T>()?;
                for row in cursor.walk(None)? {
                    out.json(&row?)?;
                }
            }
            Ok(())
        }
    }

    let kind = if raw { "write" } else { "read" };
    let entries: BTreeMap<_, _> =
        db.table_stats()?.into_iter().map(|stat| (stat.name, stat.entries)).collect();
    for &table in Tables::ALL {
        if table == Tables::VersionHistory {
            continue;
        }
        let key = format!("{kind} table/{}", table.name());
        let Some(&entries) = entries.get(table.name()) else {
            lines.insert(key, "absent".into());
            continue;
        };
        let walk = Walk { tx, raw, out: RefCell::new(Digest::default()) };
        table.view(&walk)?;
        let digest = walk.out.into_inner();
        eyre::ensure!(
            digest.rows == entries,
            "{}: the walk reads {} rows, the engine counts {entries}",
            table.name(),
            digest.rows
        );
        lines.insert(key, digest.finish());
    }
    Ok(())
}

/// Every static-file jar of `env`: its segment, data file and first block.
fn jars(env: &RethEnv) -> eyre::Result<Vec<(StaticFileSegment, PathBuf, u64)>> {
    let static_files = env.blockchain_provider().static_file_provider();
    let dir = static_files.directory().to_path_buf();
    let mut jars = Vec::new();
    for (segment, ranges) in iter_static_files(&dir)?.iter() {
        for (range, _) in ranges {
            let name = segment.filename(&static_files.find_fixed_range(segment, range.start()));
            jars.push((segment, dir.join(name), range.start()));
        }
    }
    jars.sort();
    Ok(jars)
}

/// The decompressed rows of the jar of `segment` holding `block`: each row's columns, each
/// value's checksum verified in a checksummed jar. Also whether the jar has a compressor.
fn jar_rows(env: &RethEnv, segment: StaticFileSegment, block: u64) -> eyre::Result<(Digest, bool)> {
    let static_files = env.blockchain_provider().static_file_provider();
    let jar = static_files.get_segment_provider_for_block(segment, block, None)?;
    let (rows, columns, compressed) = (jar.rows(), jar.columns(), jar.compressor().is_some());
    let mut cursor = jar.cursor()?;
    let mut digest = Digest::default();
    for row in 0..rows {
        let values = cursor
            .row_by_number_with_cols(row, (1 << columns) - 1)?
            .ok_or_else(|| eyre::eyre!("{segment} jar at {block}: no row {row} of {rows}"))?;
        digest.row(&values);
    }
    Ok((digest, compressed))
}

/// The name of `path` relative to `root`, with `/` separators.
fn relative(root: &Path, path: &Path) -> eyre::Result<String> {
    Ok(path.strip_prefix(root)?.to_string_lossy().replace('\\', "/"))
}

/// The `version` lines: each marker's value as `env`'s datadir and the running build hold it.
fn versions(env: &RethEnv, db_dir: &Path, lines: &mut Lines) -> eyre::Result<()> {
    // the datadir's marker, not this build's newest version: a datadir keeps the version it was
    // created with
    let provider = env.blockchain_provider().database_provider_ro()?;
    let marker = stored_marker(provider.get_metadata(TN_LAYOUT_KEY)?)?
        .ok_or_else(|| eyre::eyre!("the datadir has no {TN_LAYOUT_KEY} marker"))?;
    drop(provider);
    lines.insert("version tn_layout".into(), marker.version.to_string());
    lines.insert("version db_version_file".into(), TN_LMDB_DB_VERSION.to_string());
    lines.insert("version exec_state_pack".into(), EXEC_STATE_PACK_VERSION.to_string());
    // the meta record's format version and page size (crates/tn-lmdb/src/meta.rs)
    let meta = std::fs::read(db_dir.join("tn-lmdb.dat"))?;
    let field = |at: usize| u32::from_le_bytes(meta[at..at + 4].try_into().expect("4 bytes"));
    lines.insert("version tn_lmdb_format".into(), field(8).to_string());
    lines.insert("version tn_lmdb_page_size".into(), field(12).to_string());
    // a jar's format version is the first field of its configuration (`NippyJar::decode_config`)
    for (segment, path, _) in jars(env)? {
        let conf = std::fs::read(path.with_extension("conf"))?;
        let version = u64::from_le_bytes(conf[..8].try_into()?);
        lines.insert(format!("version jar/{segment}"), version.to_string());
    }
    Ok(())
}

/// The `block` lines: the hash of every block.
fn blocks(env: &RethEnv, lines: &mut Lines) -> eyre::Result<()> {
    for number in 0..=TIP {
        lines.insert(format!("block {number:02}"), header(env, number)?.hash().to_string());
    }
    Ok(())
}

/// The `write` lines of `opened`, whose datadir is `root`: every table's raw pairs and every
/// static file's bytes (a compressed jar's decompressed rows in place of its data and offsets).
fn write_lines(opened: &Opened, root: &Path, lines: &mut Lines) -> eyre::Result<()> {
    let provider = opened.env.blockchain_provider().database_provider_ro()?;
    digest_tables(&opened.db, provider.tx_ref(), true, lines)?;
    let version_file = root.join("db").join("database.version");
    lines.insert(
        format!("write file/{}", relative(root, &version_file)?),
        Digest::file(&version_file)?,
    );
    for (segment, path, start) in jars(&opened.env)? {
        let name = relative(root, &path)?;
        let (rows, compressed) = jar_rows(&opened.env, segment, start)?;
        let mut parts = vec!["conf"];
        if compressed {
            lines.insert(format!("write rows/{name}"), rows.finish());
        } else {
            lines.insert(format!("write file/{name}"), Digest::file(&path)?);
            parts.push("off");
        }
        if segment.is_change_based() {
            parts.push("csoff");
        }
        for ext in parts {
            lines.insert(
                format!("write file/{name}.{ext}"),
                Digest::file(&path.with_extension(ext))?,
            );
        }
    }
    Ok(())
}

/// The `read` lines of `opened`: every table decoded, every jar's rows, the chain through the
/// provider's readers, the historical state of `accounts` and the registry's epoch state at every
/// block, and the state-pack export at the tip.
fn read_lines(
    opened: &Opened,
    root: &Path,
    accounts: &[Address],
    lines: &mut Lines,
) -> eyre::Result<()> {
    let env = &opened.env;
    let provider = env.blockchain_provider().database_provider_ro()?;
    digest_tables(&opened.db, provider.tx_ref(), false, lines)?;
    for (segment, path, start) in jars(env)? {
        lines.insert(
            format!("read jar/{}", relative(root, &path)?),
            jar_rows(env, segment, start)?.0.finish(),
        );
    }

    let mut chain: BTreeMap<&str, Digest> = BTreeMap::new();
    for number in 0..=TIP {
        let sealed = provider
            .sealed_header(number)?
            .ok_or_else(|| eyre::eyre!("the provider reads no header {number}"))?;
        chain.entry("headers").or_default().json(&(sealed.hash(), sealed.header()))?;
        let indices = provider
            .block_body_indices(number)?
            .ok_or_else(|| eyre::eyre!("no body indices {number}"))?;
        chain.entry("body_indices").or_default().json(&indices)?;
        let txs = provider
            .transactions_by_block(number.into())?
            .ok_or_else(|| eyre::eyre!("no transactions {number}"))?;
        for tx in &txs {
            let id = provider.transaction_id(*tx.tx_hash())?;
            chain.entry("transactions").or_default().json(&(id, tx))?;
        }
        let receipts = provider
            .receipts_by_block(number.into())?
            .ok_or_else(|| eyre::eyre!("no receipts {number}"))?;
        chain.entry("receipts").or_default().json(&receipts)?;
        let senders = provider.senders_by_tx_range(indices.tx_num_range())?;
        chain.entry("senders").or_default().json(&senders)?;
        chain
            .entry("account_changesets")
            .or_default()
            .json(&provider.account_block_changeset(number)?)?;
        let storage = provider.storage_changeset(number)?;
        let storage: Vec<_> = storage
            .iter()
            .map(|(key, entry)| {
                let slot = entry.key;
                (key.block_number(), key.address(), slot.is_hashed(), slot.as_b256(), entry.value)
            })
            .collect();
        chain.entry("storage_changesets").or_default().json(&storage)?;
    }
    for (name, digest) in chain {
        lines.insert(format!("read chain/{name}"), digest.finish());
    }
    drop(provider);

    // each account's fields, code and slots 0 to 9 at every block, through the history indices
    // and change sets, and the epoch state the registry reports at every past header
    let mut history = Digest::default();
    let mut epochs = Digest::default();
    for number in 0..=TIP {
        let state = env.blockchain_provider().history_by_block_number(number)?;
        for address in accounts {
            let code = state.account_code(address)?.map(|code| code.original_bytes());
            history.json(&(number, address, state.basic_account(address)?, code))?;
            for slot in 0..10 {
                history.json(&(number, address, slot, state.storage(*address, word(slot))?))?;
            }
        }
        let epoch = env.epoch_state_at_header(&header(env, number)?)?;
        epochs.row(&[
            &epoch.epoch.to_le_bytes(),
            &epoch.epoch_info.abi_encode(),
            &epoch.validators.abi_encode(),
            &epoch.bls_pubkeys.concat(),
            &epoch.epoch_start.to_le_bytes(),
        ]);
    }
    lines.insert("read history/accounts".into(), history.finish());
    lines.insert("read history/epochs".into(), epochs.finish());

    let export = TempDir::new()?;
    let tip = header(env, TIP)?;
    let window = (1..=TIP)
        .rev()
        .map(|number| Ok(header(env, number)?.header().clone()))
        .collect::<eyre::Result<Vec<_>>>()?;
    env.pin_state_view()?.export_state_pack(tip.state_root, &window, export.path())?;
    let mut files: Vec<_> = std::fs::read_dir(export.path())?.collect::<Result<_, _>>()?;
    files.sort_by_key(|entry| entry.file_name());
    for entry in files {
        let name = entry.file_name().to_string_lossy().into_owned();
        lines.insert(format!("read export/{name}"), Digest::file(&entry.path())?);
    }
    Ok(())
}

/// Copy the directory tree at `from` into `to`, which must not exist.
fn copy_tree(from: &Path, to: &Path) -> eyre::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// The checked-in datadir of fixture `name`.
fn fixture_datadir(name: &str) -> PathBuf {
    fixtures_dir().join(name).join("datadir")
}

/// A temp copy of fixture `name`'s datadir, at `datadir` in the returned directory: an open
/// writes, so no test opens the checked-in files.
fn copy_fixture(name: &str) -> eyre::Result<TempDir> {
    let source = fixture_datadir(name);
    eyre::ensure!(
        source.is_dir(),
        "no fixture at {} (see regenerate_format_fixtures)",
        source.display()
    );
    let dir = TempDir::new()?;
    copy_tree(&source, &dir.path().join("datadir"))?;
    Ok(dir)
}

/// Open a temp copy of fixture `name`'s datadir, which holds `format`, reading its batches from
/// `batches` ([`fixture_batches`]).
fn open_fixture(
    name: &str,
    chain: Arc<RethChainSpec>,
    format: DatadirFormat,
    batches: Option<Arc<MemoryBatches>>,
) -> eyre::Result<(Opened, TempDir)> {
    let dir = copy_fixture(name)?;
    let opened = Opened::open(&dir.path().join("datadir"), chain, format, batches)?;
    Ok((opened, dir))
}

/// The bytes of [`BATCHES_FILE`] holding `batches`.
fn encode_batches(batches: &[Batch]) -> Vec<u8> {
    tn_types::encode(&batches)
}

/// The batches of fixture `name` in `format`, decoded from its [`BATCHES_FILE`] into a
/// [`MemoryBatches`] of their own, each under the digest recomputed from its decoded fields (the
/// digest a pointer row's block names), and the `read batches` line in `lines`: their count and a
/// digest of those digests, in order. `None` for a fixture storing transactions inline.
fn fixture_batches(
    name: &str,
    format: DatadirFormat,
    lines: &mut Lines,
) -> eyre::Result<Option<Arc<MemoryBatches>>> {
    if format.tx_bodies == TxBodies::Inline {
        return Ok(None);
    }
    let path = fixtures_dir().join(name).join(BATCHES_FILE);
    let bytes = std::fs::read(&path).map_err(|e| {
        eyre::eyre!("read {}: {e} (see regenerate_format_fixtures)", path.display())
    })?;
    let batches: Vec<Batch> =
        tn_types::try_decode(&bytes).map_err(|e| eyre::eyre!("decode {}: {e}", path.display()))?;
    let memory = Arc::new(MemoryBatches::default());
    let mut digests = Digest::default();
    for batch in &batches {
        let digest = batch.digest();
        memory.insert(digest, batch);
        digests.row(&[digest.as_slice()]);
    }
    lines.insert("read batches".into(), digests.finish());
    Ok(Some(memory))
}

/// Check that `env` stores each of transactions `0..count` as `tx_bodies` says: as its position in
/// its batch in a datadir storing them by position, as the transaction otherwise.
fn check_tx_rows(env: &RethEnv, tx_bodies: TxBodies, count: u64) -> eyre::Result<()> {
    let static_files = env.blockchain_provider().static_file_provider();
    let rows = static_files.tx_rows_by_tx_range(0..count)?;
    eyre::ensure!(rows.len() as u64 == count, "{} transaction rows, not {count}", rows.len());
    let positions = rows.iter().filter(|row| matches!(row, TxRow::Ref(_))).count();
    let expected = if tx_bodies == TxBodies::Pointers { rows.len() } else { 0 };
    eyre::ensure!(
        positions == expected,
        "{positions} of the {count} transaction rows are positions, not {expected}"
    );
    Ok(())
}

/// The number of transactions of blocks 0 to `tip`.
fn tx_count(env: &RethEnv, tip: u64) -> eyre::Result<u64> {
    let provider = env.blockchain_provider().database_provider_ro()?;
    let indices = provider.block_body_indices(tip)?;
    Ok(indices.ok_or_else(|| eyre::eyre!("no body indices {tip}"))?.next_tx_num())
}

/// The chain built fresh in a temp datadir in `format`, open, with its `block`, `version` and
/// `write` lines, and the batches it was built from (none for `format` storing transactions
/// inline): their encoding is the `write batches.bcs` line.
fn build(plan: &Plan, format: DatadirFormat) -> eyre::Result<(Opened, TempDir, Lines, Vec<Batch>)> {
    let dir = TempDir::new()?;
    let root = dir.path().join("datadir");
    let opened = Opened::open(&root, plan.chain.clone(), format, None)?;
    let batches = plan.execute(&opened.env, format.tx_bodies)?;
    let mut lines = Lines::new();
    blocks(&opened.env, &mut lines)?;
    versions(&opened.env, &root.join("db"), &mut lines)?;
    write_lines(&opened, &root, &mut lines)?;
    if format.tx_bodies == TxBodies::Pointers {
        lines.insert(format!("write {BATCHES_FILE}"), Digest::bytes(&encode_batches(&batches)));
    }
    Ok((opened, dir, lines, batches))
}

/// The lines of `lines` whose key is one of `keys`.
fn only(lines: &Lines, keys: &[&str]) -> Lines {
    lines
        .iter()
        .filter(|(key, _)| keys.contains(&key.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Parse a `digests.txt`.
fn parse_lines(text: &str) -> Lines {
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .filter_map(|line| {
            let mut words = line.splitn(3, ' ');
            let key = format!("{} {}", words.next()?, words.next()?);
            Some((key, words.next().unwrap_or_default().to_string()))
        })
        .collect()
}

/// Fixture `name`'s checked-in lines.
fn load_lines(name: &str) -> eyre::Result<Lines> {
    let path = fixtures_dir().join(name).join("digests.txt");
    let text = std::fs::read_to_string(&path).map_err(|e| {
        eyre::eyre!("read {}: {e} (see regenerate_format_fixtures)", path.display())
    })?;
    Ok(parse_lines(&text))
}

/// Where `found` differs from `expected` among the lines whose first word is one of `kinds`.
fn differences(expected: &Lines, found: &Lines, kinds: &[&str]) -> Vec<String> {
    let selected = |lines: &Lines| -> Lines {
        lines
            .iter()
            .filter(|(key, _)| kinds.iter().any(|kind| key.split(' ').next() == Some(kind)))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    };
    let (expected, found) = (selected(expected), selected(found));
    let mut out = Vec::new();
    for (key, value) in &expected {
        match found.get(key) {
            None => out.push(format!("{key}: pinned {value}, now missing")),
            Some(now) if now != value => out.push(format!("{key}: pinned {value}, now {now}")),
            Some(_) => {}
        }
    }
    for (key, value) in &found {
        if !expected.contains_key(key) {
            out.push(format!("{key}: new, {value}"));
        }
    }
    out
}

/// The error of a fixture check that could not run, explaining it.
fn failed(fixture: &str, what: &str, error: eyre::Report) -> eyre::Report {
    eyre::eyre!("format freeze, fixture {fixture}: {what}: {error:#}\n{WHAT_TO_DO}")
}

/// Fail with `differences`, if any, explaining them.
fn ensure_same(
    fixture: &str,
    what: &str,
    differences: Vec<String>,
    advice: &str,
) -> eyre::Result<()> {
    eyre::ensure!(
        differences.is_empty(),
        "format freeze, fixture {fixture}: {what}:\n  {}\n{advice}",
        differences.join("\n  ")
    );
    Ok(())
}

/// Reader compatibility: the current code opens each checked-in fixture and reads back exactly what
/// it held when it was written: every table row, every jar row, the chain through the provider's
/// readers and the state-pack export. A pointer fixture's transactions resolve from its checked-in
/// batches, and it reads as its inline twin wherever the storage of transactions makes no
/// difference ([`SAME_AS_INLINE`]). See the module docs.
#[tokio::test]
async fn format_fixtures_read_back_unchanged() -> eyre::Result<()> {
    let plan = Plan::new(load_genesis()?);
    for (name, format) in FIXTURES {
        let expected = load_lines(name)?;
        let mut found = Lines::new();
        let batches = fixture_batches(name, format, &mut found)
            .map_err(|e| failed(name, "the current code cannot load the checked-in batches", e))?;
        let (opened, dir) = open_fixture(name, plan.chain.clone(), format, batches)
            .map_err(|e| failed(name, "the current code cannot open the checked-in datadir", e))?;
        blocks(&opened.env, &mut found)?;
        check_tx_rows(&opened.env, format.tx_bodies, tx_count(&opened.env, TIP)?)
            .map_err(|e| failed(name, "the checked-in datadir stores other rows", e))?;
        read_lines(&opened, &dir.path().join("datadir"), &plan.accounts, &mut found)
            .map_err(|e| failed(name, "the current code cannot read the checked-in datadir", e))?;
        ensure_same(
            name,
            "the block hashes the current code reads differ",
            differences(&expected, &found, &["block"]),
            WHAT_TO_DO,
        )?;
        ensure_same(
            name,
            "the current code reads the checked-in datadir differently",
            differences(&expected, &found, &["read"]),
            WHAT_TO_DO,
        )?;
        if format.tx_bodies == TxBodies::Pointers {
            let twin = only(&load_lines(INLINE_TWIN)?, &SAME_AS_INLINE);
            ensure_same(
                name,
                &format!("the current code reads it otherwise than its inline twin {INLINE_TWIN}"),
                differences(&twin, &only(&found, &SAME_AS_INLINE), &["read"]),
                WHAT_TO_DO,
            )?;
        }
        opened.close()?;
        ensure_same(
            name,
            "opening and closing the checked-in datadir rewrote its files",
            rewritten_files(&fixture_datadir(name), &dir.path().join("datadir"))?,
            WHAT_TO_DO,
        )?;
    }
    Ok(())
}

/// The current binary, asked for transactions by position as a new datadir is, opens each inline
/// fixture (`v1`, `v2`: every datadir written before pointer rows) storing them inline, and keeps
/// it so: the open rewrites no file, its marker stays version 1, no `Transactions` jar is made for
/// tagged values, and a block executed from a real batch, its positions handed to the persist,
/// stores its transaction inline, read back by hash after a reopen.
#[tokio::test]
async fn format_inline_fixtures_stay_inline() -> eyre::Result<()> {
    assert_eq!(
        TxBodies::for_new_datadirs(),
        TxBodies::Pointers,
        "a new datadir on this build stores transactions by position: the test asks what a node \
         asks"
    );
    let plan = Plan::new(load_genesis()?);
    let owner = plan.accounts[0];
    for (name, format) in FIXTURES.into_iter().filter(|(_, f)| f.tx_bodies == TxBodies::Inline) {
        let asked = DatadirFormat { layout: format.layout, tx_bodies: TxBodies::Pointers };
        let dir = copy_fixture(name)?;
        let root = dir.path().join("datadir");
        let open = || Opened::open_asking(&root, plan.chain.clone(), asked, None);

        let opened = open()?;
        check_stays_inline(name, &opened.env, format, TIP)?;
        opened.close()?;
        ensure_same(
            name,
            "an open asking for transactions by position rewrote its files",
            rewritten_files(&fixture_datadir(name), &root)?,
            WHAT_TO_DO,
        )?;

        let opened = open()?;
        let (output, batch) = next_output(&opened.env, &plan, owner)?;
        let positions = execute_batched(&opened.env, header(&opened.env, TIP)?, &output)?;
        eyre::ensure!(positions == [Some(Arc::from([0u32]))], "{name}: positions {positions:?}");
        check_stays_inline(name, &opened.env, format, TIP + 1)?;
        opened.close()?;

        let opened = open()?;
        check_stays_inline(name, &opened.env, format, TIP + 1)?;
        let tx_hash = *TransactionSigned::decode_2718_exact(&batch.transactions[0])?.tx_hash();
        let provider = opened.env.blockchain_provider().database_provider_ro()?;
        let read = provider.transaction_by_hash(tx_hash)?;
        eyre::ensure!(
            read.is_some_and(|tx| *tx.tx_hash() == tx_hash),
            "{name}: the new block's transaction does not read back by hash"
        );
        drop(provider);
        opened.close()?;
    }
    Ok(())
}

/// Check that `env`, opened over inline fixture `name` in `format`, stores the transactions of
/// blocks 0 to `tip` inline and makes no jar for anything else: its format, its marker version,
/// the jar policy, each `Transactions` jar's format and each row.
fn check_stays_inline(
    name: &str,
    env: &RethEnv,
    format: DatadirFormat,
    tip: u64,
) -> eyre::Result<()> {
    eyre::ensure!(env.datadir_format() == format, "{name}: opened as {}", env.datadir_format());
    let provider = env.blockchain_provider().database_provider_ro()?;
    let marker = stored_marker(provider.get_metadata(TN_LAYOUT_KEY)?)?;
    drop(provider);
    eyre::ensure!(
        marker.is_some_and(|marker| marker.version == LAYOUT_VERSION_INLINE_TXS),
        "{name}: the layout marker is {marker:?}"
    );
    let static_files = env.blockchain_provider().static_file_provider();
    eyre::ensure!(
        !static_files.tx_pointer_jars(StaticFileSegment::Transactions),
        "{name}: a new Transactions jar would take tagged values"
    );
    let jars = static_files.jar_formats(StaticFileSegment::Transactions)?;
    let untagged = match format.layout {
        StorageLayout::V1 => JarFormat::Plain,
        StorageLayout::V2 => JarFormat::Checksummed,
    };
    eyre::ensure!(
        !jars.is_empty() && jars.iter().all(|(_, jar)| *jar == untagged),
        "{name}: Transactions jars {jars:?}, not {untagged:?}"
    );
    check_tx_rows(env, TxBodies::Inline, tx_count(env, tip)?)
}

/// The output after the plan's last one, carrying one real batch of one transaction of `owner`
/// (a value transfer at its next nonce), and that batch.
fn next_output(
    env: &RethEnv,
    plan: &Plan,
    owner: Address,
) -> eyre::Result<(ConsensusOutput, Batch)> {
    let nonce =
        env.blockchain_provider().latest()?.basic_account(&owner)?.unwrap_or_default().nonce;
    let mut factory = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    eyre::ensure!(factory.address() == owner, "the plan's owner is seed 33");
    factory.set_nonce(nonce);
    let tx = factory.create_eip1559_encoded(
        plan.chain.clone(),
        None,
        100,
        Some(FRESH[1]),
        U256::from(1),
        Bytes::new(),
    );
    let batch = Batch { transactions: vec![tx], epoch: 1, ..Default::default() };
    let base = consensus_output_for_tests_at(12, 1, TIP, false, GENESIS_TIME + 10 * TIP);
    let output = ConsensusOutput::new(
        base.sub_dag().clone(),
        ConsensusHeader::default().digest(),
        TIP,
        false,
        [batch.digest()].into(),
        vec![CertifiedBatch { address: Address::ZERO, batches: vec![batch.clone()] }],
    );
    Ok((output, batch))
}

/// Execute `output` on `parent`, as the engine does: signers recovered with the batch digests
/// checked, one block a batch under its digest, the persist given each block's batch positions.
/// Returns those positions.
fn execute_batched(
    env: &RethEnv,
    mut parent: SealedHeader,
    output: &ConsensusOutput,
) -> eyre::Result<Vec<Option<Arc<[u32]>>>> {
    let canonical = env.blockchain_provider().canonical_in_memory_state();
    let mut scope = OutputBuildScope::new();
    let (mut blocks, mut positions) = (Vec::new(), Vec::new());
    for (batch_index, recovered) in RethEnv::recover_output_signers(output).into_iter().enumerate()
    {
        let digest =
            output.get_batch_digest(batch_index).ok_or_else(|| eyre::eyre!("no batch digest"))?;
        let mut payload =
            TNPayload::new_for_test_with(parent, output, Address::ZERO, digest, digest);
        payload.batch_index = batch_index;
        let built = env.build_block_from_recovered_payload(payload, recovered, &mut scope)?;
        parent = built.block.recovered_block.clone_sealed_header();
        canonical.update_chain(NewCanonicalChain::Commit { new: vec![built.block.clone()] });
        canonical.set_canonical_head(parent.clone());
        blocks.push(built.block);
        positions.push(built.tx_positions);
    }
    env.persist_executed_output(&blocks, &positions)?;
    env.cache_persisted_batches(output, &positions);
    env.announce_executed_output(blocks, None)?;
    env.finalize_block(parent)?;
    Ok(positions)
}

/// The files of the checked-in datadir `fixture` that the open and close of its copy at `copy`
/// changed or removed, every file but `db/tn-lmdb.dat`: an open adds a `VersionHistory` row and
/// rewrites the layout records there, and its rows are compared through the reads. An open
/// rewrites each segment's latest `.conf` (the static-file writer's heal commits it), so a
/// release whose open changes a file's bytes fails here, before a one-release rollback meets it.
fn rewritten_files(fixture: &Path, copy: &Path) -> eyre::Result<Vec<String>> {
    let skipped = Path::new("db").join("tn-lmdb.dat");
    let mut rewritten = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(relative) = dirs.pop() {
        for entry in std::fs::read_dir(fixture.join(&relative))? {
            let entry = entry?;
            let path = relative.join(entry.file_name());
            if entry.file_type()?.is_dir() {
                dirs.push(path);
            } else if path != skipped {
                let opened = std::fs::read(copy.join(&path)).ok();
                if opened.as_deref() != Some(std::fs::read(entry.path())?.as_slice()) {
                    let what = if opened.is_some() { "rewritten" } else { "removed" };
                    rewritten.push(format!("file {}: {what}", path.display()));
                }
            }
        }
    }
    rewritten.sort();
    Ok(rewritten)
}

/// Writer stability: the current code, building the fixtures' chain again, writes the same version
/// markers, the same tn-lmdb rows and the same static-file bytes. See the module docs. The adiri
/// build's fork schedule executes the plan into other blocks, so this runs on the default build;
/// both builds share the storage code.
#[tokio::test]
#[cfg_attr(feature = "adiri", ignore = "adiri's fork schedule builds other blocks")]
async fn format_writer_output_unchanged() -> eyre::Result<()> {
    let plan = Plan::new(load_genesis()?);
    for (name, format) in FIXTURES {
        let expected = load_lines(name)?;
        let (opened, _dir, found, _batches) = build(&plan, format)
            .map_err(|e| failed(name, "the current code cannot build the fixture's chain", e))?;
        eyre::ensure!(
            differences(&expected, &found, &["block"]).is_empty(),
            "format freeze, fixture {name}: execution built other blocks than the fixture's:\n  {}\n\
             This is an execution change, not a storage-format one: check TN_*_FORK_EPOCH in the \
             environment first. An intended execution change regenerates the fixtures (README: \
             regenerating the fixtures).",
            differences(&expected, &found, &["block"]).join("\n  ")
        );
        ensure_same(
            name,
            "a format version marker differs from the fixture's",
            differences(&expected, &found, &["version"]),
            WHAT_TO_DO_VERSION,
        )?;
        ensure_same(
            name,
            "the writer's bytes differ from the fixture's",
            differences(&expected, &found, &["write"]),
            WHAT_TO_DO,
        )?;
        opened.close()?;
    }
    Ok(())
}

/// Write the fixtures `TN_REGENERATE_FORMAT_FIXTURES` names (comma-separated, from
/// [`FIXTURES`]): `genesis.json` (kept when present; delete it to rebuild it from the current
/// contracts) and, for each named fixture, `<name>/datadir`, `<name>/digests.txt` and, for one
/// storing transactions by position, `<name>/batches.bcs`. A fixture whose `version` lines differ
/// from the build's is refused, before anything is written: move it to its own directory first
/// (or delete it, for a format no release has written). Ignored, and a no-op unless the variable
/// is set, so no test run rewrites them; an unknown name is an error:
///
/// ```text
/// TN_REGENERATE_FORMAT_FIXTURES=v2-ptr cargo test -p tn-reth --features test-utils --lib \
///     env::format_freeze_tests::regenerate_format_fixtures -- --ignored --exact
/// ```
#[tokio::test]
#[ignore = "writes the checked-in fixtures; see the crate README"]
async fn regenerate_format_fixtures() -> eyre::Result<()> {
    let names = match std::env::var(REGENERATE_VAR) {
        Ok(names) => names,
        Err(std::env::VarError::NotPresent) => {
            eprintln!("{REGENERATE_VAR} not set: nothing written");
            return Ok(());
        }
        Err(e) => eyre::bail!("{REGENERATE_VAR}: {e}"),
    };
    let names: Vec<&str> = names.split(',').map(str::trim).collect();
    for name in &names {
        eyre::ensure!(
            FIXTURES.iter().any(|(fixture, _)| fixture == name),
            "{REGENERATE_VAR} names {name:?}, which is no fixture: name those to write, \
             comma-separated, from {:?}",
            FIXTURES.map(|(fixture, _)| fixture)
        );
    }
    eyre::ensure!(
        !cfg!(feature = "adiri"),
        "regenerate on the default build: adiri's fork schedule builds other blocks"
    );
    let root = fixtures_dir();
    std::fs::create_dir_all(&root)?;
    let genesis_path = root.join("genesis.json");
    if !genesis_path.exists() {
        std::fs::write(&genesis_path, serde_json::to_string_pretty(&new_genesis())? + "\n")?;
    }
    let plan = Plan::new(load_genesis()?);
    for (name, format) in FIXTURES.into_iter().filter(|(name, _)| names.contains(name)) {
        let (opened, built, mut lines, batches) = build(&plan, format)?;
        opened.close()?;

        // a fixture of another format version is kept, never rewritten in place
        if root.join(name).join("digests.txt").exists() {
            ensure_same(
                name,
                "the build's version markers differ from the checked-in fixture's, so it is not \
                 regenerated in place",
                differences(&load_lines(name)?, &lines, &["version"]),
                "Move the fixture to its own directory first (e.g. v2 -> v2-layout1), with a test \
                 of what the new binary does with it; delete it instead only if no release has \
                 written its format (crates/tn-reth/README.md, \"On-disk format versions\").",
            )?;
        }

        let target = fixture_datadir(name);
        if target.exists() {
            std::fs::remove_dir_all(&target)?;
        }
        copy_tree(&built.path().join("datadir"), &target)?;
        // the closed files are the ones the `write` lines describe
        for (key, value) in lines.iter().filter(|(key, _)| key.starts_with("write file/")) {
            let path = target.join(&key["write file/".len()..]);
            eyre::ensure!(&Digest::file(&path)? == value, "{} changed at close", path.display());
        }
        let batches_path = root.join(name).join(BATCHES_FILE);
        if format.tx_bodies == TxBodies::Pointers {
            std::fs::write(&batches_path, encode_batches(&batches))?;
        } else if batches_path.exists() {
            std::fs::remove_file(&batches_path)?;
        }

        let read_batches = fixture_batches(name, format, &mut lines)?;
        let (opened, dir) = open_fixture(name, plan.chain.clone(), format, read_batches)?;
        read_lines(&opened, &dir.path().join("datadir"), &plan.accounts, &mut lines)?;
        opened.close()?;

        let mut text = String::from(
            "# Format-freeze digests of this fixture (crates/tn-reth/src/env/format_freeze_tests.rs).\n\
             # Written by regenerate_format_fixtures; never edit by hand.\n",
        );
        for (key, value) in &lines {
            writeln!(text, "{key} {value}")?;
        }
        std::fs::write(root.join(name).join("digests.txt"), text)?;
    }
    Ok(())
}
