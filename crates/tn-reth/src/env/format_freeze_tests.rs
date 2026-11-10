//! Format-freeze tests: TN's on-disk execution formats, pinned by checked-in fixtures.
//!
//! A seeded generator ([`Plan`]) builds one small chain in a storage v1 and a storage v2 datadir
//! on tn-lmdb. Both datadirs are checked in under `tests/fixtures/format-freeze/` with their
//! digests (`digests.txt`), and two tests hold them:
//!
//! - [`format_fixtures_read_back_unchanged`] (reader compatibility): opens a copy of each
//!   checked-in datadir with the current code, through `RethEnv::new`, and reads back every row of
//!   every reth table, every row of every static-file jar, the chain through the provider's readers
//!   (headers, bodies, transactions, receipts, senders, both changesets) and the state-pack export.
//!   Each read is digested and compared with the `read` lines. A binary that can no longer read
//!   today's bytes, or reads them as other values, fails here.
//! - [`format_writer_output_unchanged`] (writer stability): builds the chain again in a temp dir
//!   and compares the `write` lines: each tn-lmdb table's ordered raw (key, value) pairs, and each
//!   static file's bytes (data, `.off`, `.csoff`, `.conf`; the decompressed rows instead of the
//!   data and `.off` of a jar with a compressor). The tn-lmdb file's own bytes are not pinned:
//!   transaction ids and free pages differ between runs. A writer change fails here.
//!
//! Both also check the format version markers each fixture records (`version` lines) and the block
//! hashes (`block` lines): a different block hash means execution changed, not storage.
//!
//! **The chain** ([`Plan`]), blocks 0 to [`TIP`], from a fixed genesis (`genesis.json`, checked in
//! so a later contract artifact cannot change the chain): legacy, EIP-2930, EIP-1559 and EIP-7702
//! transactions; value transfers that create accounts; a genesis slot zeroed, slots set and zeroed
//! later; a contract created by a transaction and one created and self-destructed in one
//! transaction; an empty genesis account removed by a touch (EIP-161); an epoch close, which
//! rewrites `ConsensusRegistry` slots; and an output that executes two blocks. Storage wipes of
//! existing slots cannot happen from Cancun on (EIP-6780), so none is covered.
//!
//! **Not pinned**: `VersionHistory` (each open's wall clock and client version), the tn-lmdb file
//! bytes, the compressed bytes of the headers jar, and anything outside the reth datadir (the
//! consensus DB and packs).
//!
//! **Regenerating** is explicit, never part of a test run: see [`regenerate_format_fixtures`] and
//! the crate README, "On-disk format versions".

use super::{database::TN_LMDB_DB_VERSION, layout::TN_LAYOUT_VERSION};
use crate::{
    init_reth_defaults,
    payload::TNPayload,
    test_utils::{
        consensus_output_for_tests_at, test_genesis_with_consensus_registry_and_workers,
        TransactionFactory,
    },
    MaybePlatformPath, NewCanonicalChain, OutputTrieOverlay, RethChainSpec, RethConfig, RethDb,
    RethEnv, StorageLayout,
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
    BlockBodyIndicesProvider as _, ChangeSetReader as _, DatabaseProviderFactory as _,
    HeaderProvider as _, ReceiptProvider as _, StaticFileProviderFactory as _, StaticFileSegment,
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
    gas_accumulator::GasAccumulator, Address, Bytes, ConsensusOutput, Encodable2718 as _, Genesis,
    GenesisAccount, SealedHeader, TaskManager, B256, U256,
};

/// The fixtures' directory.
fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format-freeze")
}

/// The fixtures: each one's directory under [`fixtures_dir`] and the layout it holds.
const FIXTURES: [(&str, StorageLayout); 2] = [("v1", StorageLayout::V1), ("v2", StorageLayout::V2)];

/// The env var that lets [`regenerate_format_fixtures`] write.
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
const TIP: u64 = 6;

/// A contract that stores its second calldata word at the slot its first word names:
/// `SSTORE(calldataload(0), calldataload(32))`.
const STORE_CODE: &[u8] = &[0x60, 0x20, 0x35, 0x60, 0x00, 0x35, 0x55, 0x00];

/// Init code deploying [`STORE_CODE`]: `CODECOPY(0, 12, 8)`, then `RETURN(0, 8)`.
const STORE_INIT: &[u8] = &[
    0x60, 0x08, 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, 0x08, 0x60, 0x00, 0xf3, 0x60, 0x20, 0x35, 0x60,
    0x00, 0x35, 0x55, 0x00,
];

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

/// The chain the fixtures hold: its spec and its outputs, in order.
struct Plan {
    chain: Arc<RethChainSpec>,
    outputs: Vec<PlannedOutput>,
}

impl Plan {
    /// The fixed plan over `genesis`. Every transaction is signed by the governance owner (seed
    /// 33, funded in the genesis), from nonce 0.
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
            call(None, 0, Bytes::from_static(STORE_INIT)),
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
        ];
        Self { chain, outputs }
    }

    /// Execute every output on `env`, from its genesis, as the engine does: one trie overlay an
    /// output, each block made canonical in memory before the next builds, the output's blocks
    /// persisted together.
    fn execute(&self, env: &RethEnv) -> eyre::Result<()> {
        let canonical = env.blockchain_provider().canonical_in_memory_state();
        let mut parent = header(env, 0)?;
        for planned in &self.outputs {
            let mut overlay = OutputTrieOverlay::new();
            let mut blocks = Vec::new();
            for (batch_index, txs) in planned.batches.iter().enumerate() {
                let number = parent.number + 1;
                let mut payload = TNPayload::new_for_test_with(
                    parent,
                    &planned.output,
                    Address::ZERO,
                    B256::with_last_byte(number as u8),
                    B256::with_last_byte(number as u8),
                );
                payload.batch_index = batch_index;
                let block = env.build_block_from_batch_payload(payload, txs, &mut overlay)?;
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
                parent = block.recovered_block.clone_sealed_header();
                canonical.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
                canonical.set_canonical_head(parent.clone());
                blocks.push(block);
            }
            env.finish_executing_output(blocks, None)?;
            env.finalize_block(parent.clone())?;
        }
        eyre::ensure!(parent.number == TIP, "the plan ends at block {}, not {TIP}", parent.number);
        Ok(())
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
    /// Open the datadir at `dir` with `chain`; a new datadir takes `layout`.
    fn open(dir: &Path, chain: Arc<RethChainSpec>, layout: StorageLayout) -> eyre::Result<Self> {
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
        let config = RethConfig::from_node_config(node_config).with_new_datadir_layout(layout);
        let tasks = TaskManager::new("format freeze");
        // where the node keeps it (`TelcoinDirs::reth_db_path`)
        let db = RethEnv::new_database(&config, dir.join("db"))?;
        let env = RethEnv::new(&config, &tasks, db.clone(), None, GasAccumulator::default())?;
        let running = env.blockchain_provider().database_provider_ro()?.cached_storage_settings();
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
        Ok(format!("{} {:x}", bytes.len(), Sha256::digest(&bytes)))
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
    lines.insert("version tn_layout".into(), TN_LAYOUT_VERSION.to_string());
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
/// provider's readers, and the state-pack export at the tip.
fn read_lines(opened: &Opened, root: &Path, lines: &mut Lines) -> eyre::Result<()> {
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

/// Open a temp copy of fixture `name`'s datadir, never the checked-in files: an open writes.
fn open_fixture(
    name: &str,
    chain: Arc<RethChainSpec>,
    layout: StorageLayout,
) -> eyre::Result<(Opened, TempDir)> {
    let source = fixture_datadir(name);
    eyre::ensure!(
        source.is_dir(),
        "no fixture at {} (see regenerate_format_fixtures)",
        source.display()
    );
    let dir = TempDir::new()?;
    copy_tree(&source, &dir.path().join("datadir"))?;
    let opened = Opened::open(&dir.path().join("datadir"), chain, layout)?;
    Ok((opened, dir))
}

/// The chain built fresh in a temp datadir in `layout`, open, with its `block`, `version` and
/// `write` lines.
fn build(plan: &Plan, layout: StorageLayout) -> eyre::Result<(Opened, TempDir, Lines)> {
    let dir = TempDir::new()?;
    let root = dir.path().join("datadir");
    let opened = Opened::open(&root, plan.chain.clone(), layout)?;
    plan.execute(&opened.env)?;
    let mut lines = Lines::new();
    blocks(&opened.env, &mut lines)?;
    versions(&opened.env, &root.join("db"), &mut lines)?;
    write_lines(&opened, &root, &mut lines)?;
    Ok((opened, dir, lines))
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
/// readers and the state-pack export. See the module docs.
#[tokio::test]
async fn format_fixtures_read_back_unchanged() -> eyre::Result<()> {
    let chain: Arc<RethChainSpec> = Arc::new(load_genesis()?.into());
    for (name, layout) in FIXTURES {
        let expected = load_lines(name)?;
        let (opened, dir) = open_fixture(name, chain.clone(), layout)
            .map_err(|e| failed(name, "the current code cannot open the checked-in datadir", e))?;
        let mut found = Lines::new();
        blocks(&opened.env, &mut found)?;
        read_lines(&opened, &dir.path().join("datadir"), &mut found)
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
        opened.close()?;
    }
    Ok(())
}

/// Writer stability: the current code, building the fixtures' chain again, writes the same version
/// markers, the same tn-lmdb rows and the same static-file bytes. See the module docs. The adiri
/// build's fork schedule executes the plan into other blocks, so this runs on the default build;
/// both builds share the storage code.
#[tokio::test]
#[cfg_attr(feature = "adiri", ignore = "adiri's fork schedule builds other blocks")]
async fn format_writer_output_unchanged() -> eyre::Result<()> {
    let plan = Plan::new(load_genesis()?);
    for (name, layout) in FIXTURES {
        let expected = load_lines(name)?;
        let (opened, _dir, found) = build(&plan, layout)
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

/// Write the fixtures: `genesis.json` (kept when present; delete it to rebuild it from the current
/// contracts) and, for each layout, `<name>/datadir` and `<name>/digests.txt`. Ignored, and a
/// no-op unless `TN_REGENERATE_FORMAT_FIXTURES=1`, so no test run rewrites them:
///
/// ```text
/// TN_REGENERATE_FORMAT_FIXTURES=1 cargo test -p tn-reth --features test-utils --lib \
///     env::format_freeze_tests::regenerate_format_fixtures -- --ignored --exact
/// ```
#[tokio::test]
#[ignore = "writes the checked-in fixtures; see the crate README"]
async fn regenerate_format_fixtures() -> eyre::Result<()> {
    if std::env::var(REGENERATE_VAR).as_deref() != Ok("1") {
        eprintln!("{REGENERATE_VAR}=1 not set: nothing written");
        return Ok(());
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
    for (name, layout) in FIXTURES {
        let (opened, built, mut lines) = build(&plan, layout)?;
        opened.close()?;

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

        let (opened, dir) = open_fixture(name, plan.chain.clone(), layout)?;
        read_lines(&opened, &dir.path().join("datadir"), &mut lines)?;
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
