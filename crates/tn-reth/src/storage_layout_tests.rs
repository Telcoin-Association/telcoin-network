//! One chain in a reth storage v1 and a storage v2 datadir: the TN-side readers and writers give
//! the same answers in both.
//!
//! The chain is built twice from the same genesis, consensus outputs and signed transactions, so
//! execution is identical and every difference is the layout's. It covers what the v2 export and
//! restore rely on: an account created with no storage, a contract created by a transaction, a
//! genesis slot changed and one set to zero, a slot set and zeroed in a later block, and the
//! `ConsensusRegistry` rewrites of an epoch close.
//!
//! Each datadir's layout is chosen with [`RethConfig::with_new_datadir_layout`], for v1 too, so
//! neither depends on the env's default for new datadirs. v2 runs on the tn-lmdb build only, so
//! this module builds there only.

use crate::{
    init_reth_defaults,
    payload::TNPayload,
    snapshot::SnapshotRestorer,
    test_utils::{
        consensus_output_for_tests_at, execute_payload_and_update_canonical_chain,
        governance_owner_factory, test_genesis_with_consensus_registry_and_workers,
    },
    worker::WorkerNetwork,
    MaybePlatformPath, RethChainSpec, RethConfig, RethDb, RethEnv, StorageLayout,
};
use jsonrpsee::{core::params::ArrayParams, Methods, RpcModule};
use reth_node_core::{
    args::{DatadirArgs, RpcServerArgs},
    node_config::NodeConfig,
};
use reth_provider::{DatabaseProviderFactory as _, StorageSettings, StorageSettingsCache as _};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::Path, sync::Arc};
use tempfile::TempDir;
use tn_storage::exec_state_pack::{ExecStateAccount, ExecStatePackReader};
use tn_types::{
    gas_accumulator::{BaseFeeContainer, GasAccumulator},
    keccak256, now, Address, BlockNumHash, Bytes, ConsensusOutput, GenesisAccount, NodeMode,
    SealedHeader, TaskManager, B256, U256,
};

/// The layouts under test.
fn layouts() -> [StorageSettings; 2] {
    [StorageLayout::V1.settings(), StorageLayout::V2.settings()]
}

/// A contract that stores its second calldata word at the slot its first word names:
/// `SSTORE(calldataload(0), calldataload(32))`.
const STORE_CODE: &[u8] = &[0x60, 0x20, 0x35, 0x60, 0x00, 0x35, 0x55, 0x00];

/// Init code deploying [`STORE_CODE`]: `CODECOPY(0, 12, 8)`, then `RETURN(0, 8)`.
const STORE_INIT: &[u8] = &[
    0x60, 0x08, 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, 0x08, 0x60, 0x00, 0xf3, 0x60, 0x20, 0x35, 0x60,
    0x00, 0x35, 0x55, 0x00,
];

/// The genesis-allocated [`STORE_CODE`] contract.
const STORE: Address = Address::repeat_byte(0x51);

/// Accounts first created by a value transfer in blocks 1, 2 and 4: no code, no storage.
const FRESH: [Address; 3] =
    [Address::repeat_byte(0xf1), Address::repeat_byte(0xf2), Address::repeat_byte(0xf4)];

/// The snapshot block: block 3 closes epoch 0.
const B: u64 = 3;

/// The highest block the plan executes (one past the snapshot).
const TIP: u64 = 4;

/// The label of `settings` for assertion messages.
fn name(settings: StorageSettings) -> &'static str {
    if settings.is_v2() {
        "v2"
    } else {
        "v1"
    }
}

/// A 32-byte word holding `n`.
fn word(n: u64) -> B256 {
    B256::from(U256::from(n))
}

/// The calldata storing `value` at `slot` in a [`STORE_CODE`] contract.
fn store(slot: u64, value: u64) -> Bytes {
    [word(slot).0, word(value).0].concat().into()
}

/// The blocks to execute: each consensus output with its signed transactions. Built once and
/// executed on every datadir, so every datadir builds the same chain.
struct Plan {
    /// The chain spec, with [`STORE`] in its genesis.
    chain: Arc<RethChainSpec>,
    /// Blocks `1..=TIP`, in order.
    blocks: Vec<(ConsensusOutput, Vec<Vec<u8>>)>,
    /// The address of the contract block 1's `CREATE` deploys.
    created: Address,
}

impl Plan {
    /// The fixed plan (must run inside a tokio runtime: the registry genesis deploys through a
    /// temporary env).
    fn new() -> Self {
        let genesis = test_genesis_with_consensus_registry_and_workers(5, vec![(0u8, 30_000_000)])
            .extend_accounts([(
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
            )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // the owner signs every transaction, from nonce 0: block 1's `CREATE` is its fourth
        let mut owner = governance_owner_factory();
        let created = owner.address().create(3);
        let mut tx = |to: Option<Address>, value: u64, input: Bytes| {
            owner.create_eip1559_encoded(chain.clone(), None, 100, to, U256::from(value), input)
        };
        let block1 = vec![
            tx(Some(FRESH[0]), 1, Bytes::new()),
            tx(Some(STORE), 0, store(2, 0)),
            tx(Some(STORE), 0, store(4, 44)),
            tx(None, 0, Bytes::from_static(STORE_INIT)),
        ];
        let block2 = vec![
            tx(Some(STORE), 0, store(4, 0)),
            tx(Some(STORE), 0, store(5, 55)),
            tx(Some(STORE), 0, store(1, 111)),
            tx(Some(FRESH[1]), 1, Bytes::new()),
        ];
        let block4 = vec![
            tx(Some(STORE), 0, store(1, 1111)),
            tx(Some(STORE), 0, store(6, 66)),
            tx(Some(FRESH[2]), 1, Bytes::new()),
        ];

        // one commit time for every datadir; blocks 1..=3 are epoch 0 and block 3 closes it
        let at = now();
        let output = |round, epoch, index, close| {
            consensus_output_for_tests_at(round, epoch, index, close, at + index)
        };
        let blocks = vec![
            (output(2, 0, 1, false), block1),
            (output(4, 0, 2, false), block2),
            (output(6, 0, 3, true), Vec::new()),
            (output(2, 1, 4, false), block4),
        ];
        Self { chain, blocks, created }
    }

    /// Execute blocks `from..=to` on `env`, each on the env's current tip.
    fn execute(&self, env: &RethEnv, from: u64, to: u64) -> eyre::Result<()> {
        for number in from..=to {
            let (output, txs) = &self.blocks[number as usize - 1];
            let parent = header(env, number - 1)?;
            let payload = TNPayload::new_for_test_with(
                parent,
                output,
                Address::ZERO,
                B256::with_last_byte(number as u8),
                B256::with_last_byte(number as u8),
            );
            let block = execute_payload_and_update_canonical_chain(env, payload, txs.clone())?;
            eyre::ensure!(block.recovered_block.number == number, "executed the wrong block");
        }
        Ok(())
    }

    /// The hashes of every planned transaction of `block`.
    fn tx_hashes(&self, block: u64) -> Vec<B256> {
        self.blocks[block as usize - 1].1.iter().map(keccak256).collect()
    }
}

/// The header of block `number`.
fn header(env: &RethEnv, number: u64) -> eyre::Result<SealedHeader> {
    env.sealed_header_by_number(number)?.ok_or_else(|| eyre::eyre!("no header {number}"))
}

/// A new temp datadir whose genesis is written in `settings`.
struct Datadir {
    config: RethConfig,
    db: RethDb,
    _dir: TempDir,
}

impl Datadir {
    fn new(chain: Arc<RethChainSpec>, settings: StorageSettings) -> eyre::Result<Self> {
        let dir = TempDir::new()?;
        init_reth_defaults();
        let node_config = NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(dir.path().to_path_buf()),
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            // historical `eth_getProof` reads (`rpc_reads_match_across_storage_layouts`)
            rpc: RpcServerArgs {
                http: true,
                ipcdisable: true,
                rpc_eth_proof_window: 16,
                ..Default::default()
            },
            ..NodeConfig::default()
        };
        let config = RethConfig::from_node_config(node_config)
            .with_new_datadir_layout(StorageLayout::of(settings));
        let db = RethEnv::new_database(&config, dir.path())?;
        Ok(Self { config, db, _dir: dir })
    }

    /// An env over this datadir, checked to run `settings`.
    fn open(&self, tasks: &TaskManager, settings: StorageSettings) -> eyre::Result<RethEnv> {
        let env =
            RethEnv::new(&self.config, tasks, self.db.clone(), None, GasAccumulator::default())?;
        let running = env.blockchain_provider().database_provider_ro()?.cached_storage_settings();
        eyre::ensure!(running == settings, "the env runs {running:?}, not {settings:?}");
        Ok(env)
    }
}

/// The plan's chain executed through block `to` in a new datadir in `settings`.
struct Chain {
    env: RethEnv,
    _datadir: Datadir,
    _tasks: TaskManager,
}

impl Chain {
    fn build(plan: &Plan, settings: StorageSettings, to: u64) -> eyre::Result<Self> {
        let tasks = TaskManager::new("storage layout chain");
        let datadir = Datadir::new(plan.chain.clone(), settings)?;
        let env = datadir.open(&tasks, settings)?;
        plan.execute(&env, 1, to)?;
        Ok(Self { env, _datadir: datadir, _tasks: tasks })
    }

    /// The hashes of blocks `0..=to`.
    fn hashes(&self, to: u64) -> eyre::Result<Vec<B256>> {
        (0..=to).map(|number| Ok(header(&self.env, number)?.hash())).collect()
    }
}

/// Export `env`'s state at its tip `B` into `dir`, with the headers of blocks `1..=B`.
fn export(env: &RethEnv, dir: &Path) -> eyre::Result<()> {
    let tip = header(env, B)?;
    let headers = (1..=B)
        .rev()
        .map(|number| Ok(header(env, number)?.header().clone()))
        .collect::<eyre::Result<Vec<_>>>()?;
    env.pin_state_view()?.export_state_pack(tip.state_root, &headers, dir)?;
    Ok(())
}

/// Every file of the pack in `dir`, by name, with its bytes.
fn pack_bytes(dir: &Path) -> eyre::Result<BTreeMap<String, Vec<u8>>> {
    std::fs::read_dir(dir)?
        .map(|entry| {
            let entry = entry?;
            Ok((entry.file_name().to_string_lossy().into_owned(), std::fs::read(entry.path())?))
        })
        .collect()
}

/// The pack's accounts, by address.
fn pack_accounts(dir: &Path) -> eyre::Result<BTreeMap<Address, GenesisAccount>> {
    ExecStatePackReader::open(dir)?
        .accounts()
        .map(|account| account.map(|ExecStateAccount { address, account }| (address, account)))
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

/// The same chain exports the same pack bytes from a v1 and a v2 datadir: the v2 export walks the
/// history indices for the plain keys and reads the hashed state, and finds exactly the accounts
/// and slots the v1 export reads from the plain tables.
#[tokio::test]
async fn snapshot_export_is_identical_in_both_storage_layouts() -> eyre::Result<()> {
    let plan = Plan::new();
    let mut packs = Vec::new();
    let mut hashes = Vec::new();
    for settings in layouts() {
        let chain = Chain::build(&plan, settings, B)?;
        hashes.push(chain.hashes(B)?);
        let dir = TempDir::new()?;
        export(&chain.env, dir.path())?;
        packs.push((pack_bytes(dir.path())?, pack_accounts(dir.path())?, dir));
    }
    assert_eq!(hashes[0], hashes[1], "both layouts must build the same blocks");
    assert_eq!(packs[0].0, packs[1].0, "the v1 and v2 exports must be byte-identical");

    // the fixture covers the cases the v2 walk must not miss
    let accounts = &packs[1].1;
    let store = accounts.get(&STORE).expect("the store contract is exported");
    assert_eq!(
        store.storage,
        Some(BTreeMap::from([(word(1), word(111)), (word(3), word(3)), (word(5), word(55))])),
        "changed and new slots exported; a genesis slot set to zero and a slot zeroed later not"
    );
    for fresh in &FRESH[..2] {
        let account = accounts.get(fresh).expect("an account created with no storage is exported");
        assert_eq!(
            (account.balance, account.code.as_ref(), account.storage.as_ref()),
            (U256::from(1), None, None)
        );
    }
    let created = accounts.get(&plan.created).expect("a contract created by a transaction");
    assert_eq!(created.code, Some(Bytes::from_static(STORE_CODE)));
    assert_eq!(created.storage, None);
    Ok(())
}

/// A key whose history spans several shards (a shard holds at most `NUM_OF_INDICES_IN_SHARD`
/// blocks) is exported once: the v2 walk meets the key once per shard.
#[tokio::test]
async fn snapshot_export_reads_each_key_once_across_history_shards() -> eyre::Result<()> {
    use reth_db::{cursor::DbCursorRO as _, tables::AccountsHistory, transaction::DbTx as _};
    use reth_provider::{DBProvider as _, HistoryWriter as _};

    let plan = Plan::new();
    let mut packs = Vec::new();
    for settings in layouts() {
        let tasks = TaskManager::new("storage layout shards");
        let datadir = Datadir::new(plan.chain.clone(), settings)?;
        let env = datadir.open(&tasks, settings)?;
        let provider = env.blockchain_provider().database_provider_rw()?;
        provider.insert_account_history_index([(STORE, 1..=2_001)])?;
        provider.insert_storage_history_index([((STORE, word(1)), 1..=2_001)])?;
        let shards = provider
            .tx_ref()
            .cursor_read::<AccountsHistory>()?
            .walk(None)?
            .filter(|row| row.as_ref().is_ok_and(|(key, _)| key.key == STORE))
            .count();
        assert_eq!(shards, 2, "the fixture must split the history into two shards");
        provider.commit()?;

        let genesis = header(&env, 0)?;
        let dir = TempDir::new()?;
        env.pin_state_view()?.export_state_pack(
            genesis.state_root,
            &[genesis.header().clone()],
            dir.path(),
        )?;
        packs.push((pack_bytes(dir.path())?, dir));
    }
    assert_eq!(packs[0].0, packs[1].0, "the v1 and v2 exports must be byte-identical");
    Ok(())
}

/// A pack from either layout restores into either: the recomputed root matches, the restored
/// datadir reopens (its static-file segments end at the stage checkpoints, which the startup
/// consistency check requires), re-exports the same pack, and continues the chain: the next block
/// executes to the source's block, and its changes read back at the snapshot block.
#[tokio::test]
async fn snapshot_restores_across_storage_layouts() -> eyre::Result<()> {
    let plan = Plan::new();
    for source_settings in layouts() {
        let source = Chain::build(&plan, source_settings, B)?;
        let pack = TempDir::new()?;
        export(&source.env, pack.path())?;
        let window =
            (1..=B).map(|number| header(&source.env, number)).collect::<eyre::Result<Vec<_>>>()?;
        let snapshot = header(&source.env, B)?;
        let final_state = BlockNumHash::new(B, snapshot.hash());
        plan.execute(&source.env, B + 1, TIP)?;
        let source_tip = header(&source.env, TIP)?.hash();

        for target_settings in layouts() {
            let case = format!("{} pack into {}", name(source_settings), name(target_settings));
            let tasks = TaskManager::new("storage layout restore");
            let target = Datadir::new(plan.chain.clone(), target_settings)?;
            {
                let restorer = SnapshotRestorer::open(&target.config, target.db.clone(), &tasks)?;
                restorer.import_chain_scaffold(&window, final_state)?;
                let mut reader = ExecStatePackReader::open(pack.path())?;
                assert_eq!(restorer.import_state(&mut reader)?, snapshot.state_root, "{case}");
                restorer.entry_readiness_precondition(final_state)?;
                restorer.finish(final_state)?;
            }

            let env = target.open(&tasks, target_settings)?;
            let again = TempDir::new()?;
            export(&env, again.path())?;
            assert_eq!(pack_bytes(again.path())?, pack_bytes(pack.path())?, "{case}: re-export");
            let state = env.latest()?;
            assert_eq!(state.storage(STORE, word(1))?, Some(U256::from(111)), "{case}");
            assert_eq!(state.storage(STORE, word(2))?.unwrap_or_default(), U256::ZERO, "{case}");
            assert_eq!(state.account_balance(&FRESH[0])?, Some(U256::from(1)), "{case}");
            assert_eq!(
                state.account_code(&plan.created)?.map(|code| code.original_bytes()),
                Some(Bytes::from_static(STORE_CODE)),
                "{case}"
            );
            drop(state);

            plan.execute(&env, B + 1, TIP)?;
            assert_eq!(header(&env, TIP)?.hash(), source_tip, "{case}: block {TIP}");
            let latest = env.latest()?;
            assert_eq!(latest.storage(STORE, word(1))?, Some(U256::from(1111)), "{case}");
            assert_eq!(latest.storage(STORE, word(6))?, Some(U256::from(66)), "{case}");
            let at_snapshot = env.state_by_block_hash(snapshot.hash())?;
            assert_eq!(at_snapshot.storage(STORE, word(1))?, Some(U256::from(111)), "{case}");
            assert_eq!(
                at_snapshot.storage(STORE, word(6))?.unwrap_or_default(),
                U256::ZERO,
                "{case}"
            );
            assert_eq!(at_snapshot.basic_account(&FRESH[2])?, None, "{case}");
            drop((latest, at_snapshot, env));

            // and reopens after persisting past the snapshot
            target.open(&tasks, target_settings)?;
        }
    }
    Ok(())
}

/// The consensus-critical registry reads pinned at past headers (the committee and epoch state,
/// the worker fee configs, a contract call) and plain storage reads at every block answer the
/// same in both layouts. Below the tip they resolve through the history: genesis slots never
/// rewritten (the genesis history, keyed by plain slot) and slots the epoch close rewrote (their
/// changesets).
#[tokio::test]
async fn registry_reads_at_past_headers_match_across_storage_layouts() -> eyre::Result<()> {
    use crate::system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS};
    use alloy::sol_types::SolCall as _;

    let plan = Plan::new();
    let mut answers = Vec::new();
    for settings in layouts() {
        let chain = Chain::build(&plan, settings, TIP)?;
        let env = &chain.env;
        let mut reads = Vec::new();
        for number in 0..=TIP {
            let header = header(env, number)?;
            let (epoch, info) = env.get_current_epoch_info_at_header(&header)?;
            reads.push(format!("{number} epoch {epoch} {info:?}"));
            reads.push(format!("{number} state {:?}", env.epoch_state_at_header(&header)?));
            reads.push(format!(
                "{number} fees {:?}",
                env.get_worker_fee_configs_at_block(header.hash())?
            ));
            let calldata = ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into();
            let output =
                env.read_contract_at_block(header.hash(), CONSENSUS_REGISTRY_ADDRESS, calldata)?;
            reads.push(format!("{number} call {output}"));
            let state = env.state_by_block_hash(header.hash())?;
            for slot in 1..=6 {
                reads.push(format!("{number} slot {slot} {:?}", state.storage(STORE, word(slot))?));
            }
        }
        // the reads see history: epoch 0 below the close, epoch 1 from the closing block
        let epochs = (0..=TIP)
            .map(|number| Ok(env.get_current_epoch_info_at_header(&header(env, number)?)?.0))
            .collect::<eyre::Result<Vec<_>>>()?;
        assert_eq!(epochs, [0, 0, 0, 1, 1], "{}", name(settings));
        // and the genesis state: the genesis committee, a genesis slot never rewritten (its
        // genesis history shard), and a genesis slot rewritten later (its changeset)
        let genesis = header(env, 0)?;
        assert_eq!(env.epoch_state_at_header(&genesis)?.validators.len(), 5, "{}", name(settings));
        for number in 0..=TIP {
            let state = env.state_by_block_hash(header(env, number)?.hash())?;
            assert_eq!(state.storage(STORE, word(3))?, Some(U256::from(3)), "{}", name(settings));
        }
        let at = |number| -> eyre::Result<_> {
            Ok(env.state_by_block_hash(header(env, number)?.hash())?.storage(STORE, word(1))?)
        };
        assert_eq!([at(0)?, at(2)?, at(4)?], [1, 111, 1111].map(|v| Some(U256::from(v))));
        answers.push(reads);
    }
    assert_eq!(answers[0], answers[1], "v1 and v2 must answer every pinned read the same");
    Ok(())
}

/// A damaged storage-v2 changeset fails the consensus-critical pinned reads closed. Every value of
/// the storage changeset file is changed (vendored TN-0120: each carries a CRC-32): the committee
/// and epoch reads pinned below the epoch close, which resolve the registry's rewritten slots
/// through that file, are `StateReadError::Provider` naming the checksum, and a plain storage read
/// below a rewrite errors too, never answering with another block's value. The operator response
/// is in the README's storage section.
#[tokio::test]
async fn damaged_v2_changeset_fails_pinned_reads_closed() -> eyre::Result<()> {
    use crate::error::StateReadError;
    use reth_provider::{StaticFileProviderFactory as _, StaticFileSegment};
    use std::io::Write as _;

    let plan = Plan::new();
    let chain = Chain::build(&plan, StorageSettings::v2(), TIP)?;
    let env = &chain.env;
    let genesis = header(env, 0)?;
    assert_eq!(env.epoch_state_at_header(&genesis)?.validators.len(), 5);

    let static_files = env.blockchain_provider().static_file_provider();
    let segment = StaticFileSegment::StorageChangeSets;
    let path =
        static_files.directory().join(segment.filename(&static_files.find_fixed_range(segment, 0)));
    let damaged: Vec<u8> = std::fs::read(&path)?.into_iter().map(|byte| byte ^ 0x01).collect();
    let mut file = std::fs::OpenOptions::new().write(true).open(&path)?;
    file.write_all(&damaged)?;
    file.sync_all()?;

    let checksum = |err: &dyn std::fmt::Display| err.to_string().contains("fails its checksum");
    for number in 0..3 {
        let header = header(env, number)?;
        match env.epoch_state_at_header(&header) {
            Err(err @ StateReadError::Provider(_)) => assert!(checksum(&err), "{number}: {err}"),
            other => panic!("epoch state at {number}: {other:?}"),
        }
        match env.get_current_epoch_info_at_header(&header) {
            Err(err @ StateReadError::Provider(_)) => assert!(checksum(&err), "{number}: {err}"),
            other => panic!("epoch info at {number}: {other:?}"),
        }
    }
    let err = env.state_by_block_hash(genesis.hash())?.storage(STORE, word(1)).unwrap_err();
    assert!(checksum(&err), "{err}");
    Ok(())
}

/// The format version of each static-file jar in `env`'s datadir, by segment: the first field of a
/// jar's configuration (`<jar>.conf`), a `u64` LE (`NippyJar::decode_config`). Version 1 is
/// upstream's format, version 2 the checksummed one (TN-0120).
fn jar_versions(
    env: &RethEnv,
) -> eyre::Result<BTreeMap<reth_provider::StaticFileSegment, Vec<u64>>> {
    use reth_provider::{StaticFileProviderFactory as _, StaticFileSegment};

    let mut versions = BTreeMap::<_, Vec<u64>>::new();
    for entry in std::fs::read_dir(env.blockchain_provider().static_file_provider().directory())? {
        let path = entry?.path();
        let name = path.file_name().map(|name| name.to_string_lossy().into_owned());
        // the data file's name; its configuration, offsets and sidecar files have extensions
        let Some((segment, _)) = name.as_deref().and_then(StaticFileSegment::parse_filename) else {
            continue;
        };
        let conf = std::fs::read(path.with_extension("conf"))?;
        let version = conf.get(..8).ok_or_else(|| eyre::eyre!("{} is short", path.display()))?;
        versions.entry(segment).or_default().push(u64::from_le_bytes(version.try_into()?));
    }
    Ok(versions)
}

/// A new datadir writes every static-file jar in its layout's format: version 1 in storage v1, for
/// the segments it keeps in static files (headers, transactions, receipts), and version 2
/// (checksummed) in storage v2, for every segment: those, senders and both changesets, except
/// `Transactions`, whose jars in a new v2 datadir (which stores transactions by position) are
/// version 3 (checksummed, tagged values). Checked
/// after genesis and after blocks persisted into every segment. Storage v2 also refuses a
/// version-1 jar of a checksummed segment at load (TN-0135); this reads the files themselves.
#[tokio::test]
async fn new_datadirs_write_every_jar_in_their_layout_format() -> eyre::Result<()> {
    use reth_provider::{StaticFileProviderFactory as _, StaticFileSegment};

    let plan = Plan::new();
    for settings in layouts() {
        let (version, segments) = if settings.is_v2() {
            (2, StaticFileSegment::iter().collect::<Vec<_>>())
        } else {
            (
                1,
                vec![
                    StaticFileSegment::Headers,
                    StaticFileSegment::Transactions,
                    StaticFileSegment::Receipts,
                ],
            )
        };
        let tasks = TaskManager::new("storage layout jar formats");
        let datadir = Datadir::new(plan.chain.clone(), settings)?;
        let env = datadir.open(&tasks, settings)?;
        let static_files = env.blockchain_provider().static_file_provider();
        for (when, tip) in [("genesis", 0), ("blocks", TIP)] {
            if tip > 0 {
                plan.execute(&env, 1, tip)?;
            }
            let versions = jar_versions(&env)?;
            let label = format!("{} after {when}", name(settings));
            assert_eq!(versions.keys().copied().collect::<Vec<_>>(), segments, "{label}");
            for (segment, found) in versions {
                // a new v2 datadir stores its transactions by position (`TxBodies`), in
                // `Transactions` jars with tagged values: version 3
                let version = if settings.is_v2() && segment == StaticFileSegment::Transactions {
                    3
                } else {
                    version
                };
                assert!(
                    found.iter().all(|found| *found == version),
                    "{label}: {segment} {found:?}"
                );
                assert_eq!(
                    static_files.get_highest_static_file_block(segment),
                    Some(tip),
                    "{label}: {segment}"
                );
            }
        }
    }
    Ok(())
}

/// `eth_getStorageAt`, `eth_getProof`, `eth_getBalance` at the tip and at past blocks, and
/// `eth_getTransactionByHash` and `eth_getTransactionReceipt`, answer the same in both layouts.
#[tokio::test(flavor = "multi_thread")]
async fn rpc_reads_match_across_storage_layouts() -> eyre::Result<()> {
    let plan = Plan::new();
    let mut answers = Vec::new();
    for settings in layouts() {
        let chain = Chain::build(&plan, settings, TIP)?;
        let methods = rpc_methods(&chain.env)?;
        let mut calls = Vec::new();
        for block in ["latest", "0x1", "0x2", "0x3"] {
            for slot in 1..=6 {
                calls.push((
                    "eth_getStorageAt",
                    vec![json!(STORE), json!(word(slot)), json!(block)],
                ));
            }
            calls.push(("eth_getBalance", vec![json!(FRESH[2]), json!(block)]));
            calls.push((
                "eth_getProof",
                vec![json!(STORE), json!([word(1), word(4), word(6)]), json!(block)],
            ));
        }
        for block in [1, 2, TIP] {
            for hash in plan.tx_hashes(block) {
                calls.push(("eth_getTransactionByHash", vec![json!(hash)]));
                calls.push(("eth_getTransactionReceipt", vec![json!(hash)]));
            }
        }
        let mut results = Vec::new();
        for (method, params) in calls {
            results.push((method, params.clone(), rpc(&methods, method, params).await));
        }
        answers.push(results);
    }
    assert_eq!(answers[0], answers[1], "v1 and v2 must answer every RPC read the same");

    // the answers are real: past blocks differ from the tip, and every transaction is found
    let answer = |method: &str, params: Vec<Value>| {
        answers[1]
            .iter()
            .find(|(m, p, _)| *m == method && *p == params)
            .map(|(_, _, result)| result.clone().map_err(|e| eyre::eyre!(e)))
            .expect("asked")
    };
    let slot1 =
        |block: &str| answer("eth_getStorageAt", vec![json!(STORE), json!(word(1)), json!(block)]);
    assert_eq!(slot1("0x1")?, json!(word(1)));
    assert_eq!(slot1("0x3")?, json!(word(111)));
    assert_eq!(slot1("latest")?, json!(word(1111)));
    let proof = answer(
        "eth_getProof",
        vec![json!(STORE), json!([word(1), word(4), word(6)]), json!("0x2")],
    )?;
    assert_eq!(proof["storageProof"][0]["value"], json!("0x6f"), "historical proof of slot 1");
    for (method, _, result) in &answers[1] {
        if method.starts_with("eth_getTransaction") {
            assert!(result.as_ref().is_ok_and(|value| !value.is_null()), "{method}: {result:?}");
        }
    }
    Ok(())
}

/// The env's HTTP RPC methods, served in process.
fn rpc_methods(env: &RethEnv) -> eyre::Result<Methods> {
    let pool = env.init_txn_pool(BaseFeeContainer::default())?;
    env.get_rpc_server(
        pool,
        WorkerNetwork::new_for_test(env.chainspec()),
        GasAccumulator::new(1).worker_base_fee(0),
        || NodeMode::CvvActive,
        RpcModule::new(()),
    )?
    .http_methods(|_| true)
    .ok_or_else(|| eyre::eyre!("the env serves HTTP"))
}

/// Call `method` with `params`, the error as text.
async fn rpc(methods: &Methods, method: &str, params: Vec<Value>) -> Result<Value, String> {
    let mut array = ArrayParams::new();
    for param in params {
        array.insert(param).map_err(|e| e.to_string())?;
    }
    methods.call::<_, Value>(method, array).await.map_err(|e| format!("{method}: {e}"))
}
