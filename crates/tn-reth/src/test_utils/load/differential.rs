//! The storage-layout differential: a seeded sample of past-block and by-hash RPC reads over a
//! finished chain, written with their answers to a file, and the comparison of two such files.
//!
//! Two runs of one load build the same chain whatever their storage layout or backend: execution
//! is deterministic. [`record`] draws its queries from an RNG seeded with `seed` and from the chain
//! itself (the tip, and the transaction hashes of drawn blocks), so two runs of one load draw the
//! same queries, and every answer must match. [`compare`] says where they do not.
//!
//! **Queries**, [`DiffKind::ALL`] in turn, each block drawn uniformly from the genesis to the tip:
//! - [`DiffKind::Header`]: `eth_getBlockByNumber(block, false)`, which carries the block's hash,
//!   state root and receipts root;
//! - [`DiffKind::BalanceAt`]: `eth_getBalance` of an address at a block;
//! - [`DiffKind::StorageAt`]: `eth_getStorageAt` of an address's balance slot on [`STABLECOIN_A`]
//!   or [`STABLECOIN_B`] at a block;
//! - [`DiffKind::Tx`] and [`DiffKind::Receipt`]: `eth_getTransactionByHash` and
//!   `eth_getTransactionReceipt` of a transaction of a block drawn from those after the genesis (up
//!   to [`TX_DRAWS`] draws for a block with one; a random hash after that);
//! - [`DiffKind::TxMiss`]: `eth_getTransactionByHash` of a random hash.
//!
//! An address is a keyed sender (whose balance and nonce change with every transaction it sends)
//! or a Zipf-drawn prefilled account, with even odds.
//!
//! **The file**: JSON lines. The first is a [`DiffHeader`]; then one [`DiffRecord`] per query, in
//! order. It is written to a temp file, synced and renamed into place.
//!
//! **Checks while recording**: an RPC error, a transaction of a drawn block that its hash does not
//! find (or finds in another block), and a random hash that answers, are [`DiffSummary::errors`];
//! the record keeps the error, so it compares too.

use super::{
    readers::{block_tag, call, number, runtime, tx_hash},
    state::{balance_slot, PrefillSpec, STABLECOIN_A, STABLECOIN_B},
    workload::{WorkloadConfig, Zipf},
};
use jsonrpsee::{core::params::ArrayParams, Methods};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead as _, BufReader, BufWriter, Write as _},
    path::Path,
    time::{Duration, Instant},
};
use tn_storage::disk::{fsync_directory, fsync_file};
use tn_types::{Address, B256};
use tokio::runtime::Handle;

/// The version of the differential file; [`compare`] refuses two of different versions.
pub const DIFF_VERSION: u32 = 1;

/// How many blocks a [`DiffKind::Tx`] or [`DiffKind::Receipt`] query draws, at most, for one with
/// a transaction.
pub const TX_DRAWS: usize = 8;

/// The mismatches [`compare`] describes; it counts them all.
pub const SHOWN_MISMATCHES: usize = 5;

/// What a differential query asks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffKind {
    /// A block's header fields, by number.
    Header,
    /// An address's balance at a block.
    BalanceAt,
    /// A token balance slot at a block.
    StorageAt,
    /// A transaction of the chain, by hash.
    Tx,
    /// A transaction's receipt, by hash.
    Receipt,
    /// A random hash, which must find no transaction.
    TxMiss,
}

impl DiffKind {
    /// Every kind, in the order the queries take them.
    pub const ALL: [Self; 6] =
        [Self::Header, Self::BalanceAt, Self::StorageAt, Self::Tx, Self::Receipt, Self::TxMiss];

    /// The kind's report label and JSON key.
    pub const fn key(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::BalanceAt => "balance_at",
            Self::StorageAt => "storage_at",
            Self::Tx => "tx",
            Self::Receipt => "receipt",
            Self::TxMiss => "tx_miss",
        }
    }
}

/// A differential file's first line. Two files compare when all but `layout` and `backend` match.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffHeader {
    /// [`DIFF_VERSION`].
    pub diff_version: u32,
    /// The chain's genesis hash.
    pub genesis_hash: B256,
    /// The tip the queries ran at.
    pub tip_number: u64,
    /// Its hash.
    pub tip_hash: B256,
    /// The queries' seed.
    pub seed: u64,
    /// How many queries.
    pub queries: usize,
    /// The run's storage layout, for the reader; not compared.
    pub layout: String,
    /// The run's database backend, for the reader; not compared.
    pub backend: String,
}

impl DiffHeader {
    /// The fields two files must share to compare, as text, or `None` when they do.
    fn mismatch(&self, other: &Self) -> Option<String> {
        let key = |h: &Self| (h.diff_version, h.genesis_hash, h.tip_number, h.tip_hash, h.seed);
        if key(self) != key(other) || self.queries != other.queries {
            return Some(format!(
                "the files do not record one load's queries: version {} genesis {} tip {} {} \
                 seed {} queries {} against version {} genesis {} tip {} {} seed {} queries {}",
                self.diff_version,
                self.genesis_hash,
                self.tip_number,
                self.tip_hash,
                self.seed,
                self.queries,
                other.diff_version,
                other.genesis_hash,
                other.tip_number,
                other.tip_hash,
                other.seed,
                other.queries
            ));
        }
        None
    }
}

/// One query and its answer: a line of the file after the header.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiffRecord {
    /// The query's place, from 0.
    pub index: usize,
    /// What it asks.
    pub kind: DiffKind,
    /// The RPC method.
    pub method: String,
    /// Its parameters.
    pub params: Value,
    /// The answer, or `null` with an error.
    pub answer: Value,
    /// The RPC's error, or the check's (see the module docs).
    pub error: Option<String>,
}

/// One kind's queries in a [`record`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DiffKindSamples {
    /// Each query's latency (its RPC call only).
    pub latencies: Vec<Duration>,
    /// The queries with an error.
    pub errors: u64,
}

/// What [`record`] wrote.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DiffSummary {
    /// The header written.
    pub header: Option<DiffHeader>,
    /// Each kind's queries.
    pub kinds: BTreeMap<DiffKind, DiffKindSamples>,
    /// The first error, with its query's index.
    pub first_error: Option<String>,
}

impl DiffSummary {
    /// The queries with an error, over every kind.
    pub fn errors(&self) -> u64 {
        self.kinds.values().map(|kind| kind.errors).sum()
    }
}

/// What [`compare`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiffComparison {
    /// The records compared.
    pub compared: usize,
    /// The records whose query or answer differ, plus one when the headers do not match.
    pub mismatches: usize,
    /// The first [`SHOWN_MISMATCHES`] mismatches, described.
    pub shown: Vec<String>,
}

/// The queries' plan: the RNG and the samplers, over the RPC at a fixed tip.
struct Planner<'a> {
    methods: &'a Methods,
    handle: &'a Handle,
    spec: PrefillSpec,
    zipf: Option<Zipf>,
    rng: StdRng,
    tip: u64,
}

impl Planner<'_> {
    /// A sender or a Zipf-drawn prefilled account, with even odds (whichever the spec has).
    fn address(&mut self) -> Address {
        let sender = self.spec.senders > 0 && (self.zipf.is_none() || self.rng.random());
        if sender {
            let i = self.rng.random_range(0..self.spec.senders);
            return self.spec.sender(i).address();
        }
        let i = self.zipf.as_ref().map_or(0, |zipf| zipf.sample(&mut self.rng));
        self.spec.account(i)
    }

    /// A block from the genesis to the tip.
    fn block(&mut self) -> u64 {
        self.rng.random_range(0..=self.tip)
    }

    /// A transaction of a block after the genesis and its block, drawn as the module docs say, or
    /// a random hash and `None`.
    fn chain_tx(&mut self) -> Result<(B256, Option<u64>), String> {
        for _ in 0..TX_DRAWS {
            if self.tip == 0 {
                break;
            }
            let block = self.rng.random_range(1..=self.tip);
            let params = json!([block_tag(block), false]);
            let value =
                call(self.methods, self.handle, "eth_getBlockByNumber", params_of(&params)?)?;
            let txs = value["transactions"].as_array().ok_or("a block without transactions")?;
            if !txs.is_empty() {
                let pick = self.rng.random_range(0..txs.len());
                return Ok((tx_hash(&txs[pick])?, Some(block)));
            }
        }
        Ok((B256::from(self.rng.random::<[u8; 32]>()), None))
    }

    /// The `index`-th query: its kind, method and parameters, and the block a by-hash query's
    /// transaction must be found in (`None` for a hash that must find none; unused otherwise).
    fn query(
        &mut self,
        index: usize,
    ) -> Result<(DiffKind, &'static str, Value, Option<u64>), String> {
        let kind = DiffKind::ALL[index % DiffKind::ALL.len()];
        Ok(match kind {
            DiffKind::Header => {
                (kind, "eth_getBlockByNumber", json!([block_tag(self.block()), false]), None)
            }
            DiffKind::BalanceAt => {
                let address = self.address();
                (kind, "eth_getBalance", json!([address, block_tag(self.block())]), None)
            }
            DiffKind::StorageAt => {
                let token = if self.rng.random() { STABLECOIN_A } else { STABLECOIN_B };
                let slot = balance_slot(self.address());
                (kind, "eth_getStorageAt", json!([token, slot, block_tag(self.block())]), None)
            }
            DiffKind::Tx | DiffKind::Receipt => {
                let (hash, block) = self.chain_tx()?;
                let method = if kind == DiffKind::Tx {
                    "eth_getTransactionByHash"
                } else {
                    "eth_getTransactionReceipt"
                };
                (kind, method, json!([hash]), block)
            }
            DiffKind::TxMiss => {
                let hash = B256::from(self.rng.random::<[u8; 32]>());
                (kind, "eth_getTransactionByHash", json!([hash]), None)
            }
        })
    }
}

/// `params`, a JSON array, as RPC parameters.
fn params_of(params: &Value) -> Result<ArrayParams, String> {
    let mut out = ArrayParams::new();
    for param in params.as_array().ok_or("parameters are not an array")? {
        out.insert(param).map_err(|e| format!("parameter {param}: {e}"))?;
    }
    Ok(out)
}

/// Check a by-hash `answer`: `expected` the block its transaction is in, or `None` for a hash
/// that must find none.
fn check_by_hash(hash: &Value, answer: &Value, expected: Option<u64>) -> Result<(), String> {
    match (expected, answer.is_null()) {
        (None, true) => Ok(()),
        (None, false) => Err(format!("{hash} is no transaction, and answered {answer}")),
        (Some(block), true) => {
            Err(format!("{hash}, a transaction of block {block}, has no answer"))
        }
        (Some(block), false) => {
            let found = number(&answer["blockNumber"])?;
            if found != block {
                return Err(format!(
                    "{hash}, a transaction of block {block}, answered block {found}"
                ));
            }
            let field =
                if answer.get("transactionHash").is_some() { "transactionHash" } else { "hash" };
            if tx_hash(&answer[field])? != tx_hash(hash)? {
                return Err(format!("{hash} answered transaction {}", answer[field]));
            }
            Ok(())
        }
    }
}

/// Run `header.queries` queries on `methods` at the tip `header` names (the env's tip, which
/// nothing advances meanwhile), drawing addresses of `spec`, and write them with their answers
/// to `path` (see the module docs). A query's error is recorded, counted and kept as the first;
/// only a failure to plan (a block fetch that fails) or to write ends the run early.
///
/// Call it off the runtime's worker threads (in `tokio::task::block_in_place`, say), within the
/// multi-thread tokio runtime the env's tasks run on.
pub fn record(
    methods: &Methods,
    spec: &PrefillSpec,
    header: DiffHeader,
    path: &Path,
) -> eyre::Result<DiffSummary> {
    let handle = runtime()?;
    let zipf = (spec.accounts > 0)
        .then(|| Zipf::new(spec.accounts, WorkloadConfig::default().zipf_exponent));
    eyre::ensure!(zipf.is_some() || spec.senders > 0, "the spec has no address to read");
    let mut planner = Planner {
        methods,
        handle: &handle,
        spec: *spec,
        zipf,
        rng: StdRng::seed_from_u64(header.seed),
        tip: header.tip_number,
    };
    let dir = path.parent().filter(|dir| !dir.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let name = path.file_name().ok_or_else(|| eyre::eyre!("{} names no file", path.display()))?;
    let tmp = dir.join(format!("{}.tmp", name.to_string_lossy()));
    let mut out = BufWriter::new(File::create(&tmp)?);
    serde_json::to_writer(&mut out, &header)?;
    out.write_all(b"\n")?;
    let mut summary = DiffSummary::default();
    for index in 0..header.queries {
        let (kind, method, params, expected) =
            planner.query(index).map_err(|e| eyre::eyre!("planning query {index}: {e}"))?;
        let rpc_params = params_of(&params).map_err(|e| eyre::eyre!("query {index}: {e}"))?;
        let started = Instant::now();
        let outcome = call(methods, &handle, method, rpc_params);
        let latency = started.elapsed();
        let (answer, error) = match outcome {
            Ok(answer) => {
                let check = match kind {
                    DiffKind::Tx | DiffKind::Receipt | DiffKind::TxMiss => {
                        check_by_hash(&params[0], &answer, expected)
                    }
                    _ => Ok(()),
                };
                (answer, check.err())
            }
            Err(e) => (Value::Null, Some(e)),
        };
        let samples = summary.kinds.entry(kind).or_default();
        samples.latencies.push(latency);
        if let Some(e) = &error {
            samples.errors += 1;
            summary.first_error.get_or_insert_with(|| format!("query {index} ({method}): {e}"));
        }
        let record = DiffRecord { index, kind, method: method.to_owned(), params, answer, error };
        serde_json::to_writer(&mut out, &record)?;
        out.write_all(b"\n")?;
    }
    let file = out.into_inner().map_err(|e| e.into_error())?;
    fsync_file(&file)?;
    drop(file);
    std::fs::rename(&tmp, path)?;
    fsync_directory(dir)?;
    summary.header = Some(header);
    Ok(summary)
}

/// A differential file's header and records.
fn read(path: &Path) -> eyre::Result<(DiffHeader, Vec<DiffRecord>)> {
    let file = File::open(path).map_err(|e| eyre::eyre!("opening {}: {e}", path.display()))?;
    let mut lines = BufReader::new(file).lines();
    let first = lines.next().ok_or_else(|| eyre::eyre!("{} is empty", path.display()))??;
    let header: DiffHeader = serde_json::from_str(&first)
        .map_err(|e| eyre::eyre!("{}: the header does not parse: {e}", path.display()))?;
    let mut records = Vec::with_capacity(header.queries);
    for (i, line) in lines.enumerate() {
        let record = serde_json::from_str(&line?)
            .map_err(|e| eyre::eyre!("{}: line {} does not parse: {e}", path.display(), i + 2))?;
        records.push(record);
    }
    Ok((header, records))
}

/// Compare the differential files `a` and `b` (see the module docs): their headers must record
/// one load's queries, then each record's query and answer (or error) must match. A file that
/// does not parse is an error; every difference is a mismatch.
pub fn compare(a: &Path, b: &Path) -> eyre::Result<DiffComparison> {
    let ((head_a, records_a), (head_b, records_b)) = (read(a)?, read(b)?);
    let mut out = DiffComparison::default();
    let mismatch = |out: &mut DiffComparison, text: String| {
        out.mismatches += 1;
        if out.shown.len() < SHOWN_MISMATCHES {
            out.shown.push(text);
        }
    };
    if let Some(text) = head_a.mismatch(&head_b) {
        mismatch(&mut out, text);
        return Ok(out);
    }
    for (name, head, records) in [("a", &head_a, &records_a), ("b", &head_b, &records_b)] {
        if records.len() != head.queries {
            mismatch(
                &mut out,
                format!("{name} holds {} records of its {} queries", records.len(), head.queries),
            );
        }
    }
    for (x, y) in records_a.iter().zip(&records_b) {
        out.compared += 1;
        let query = |r: &DiffRecord| (r.index, r.kind, r.method.clone(), r.params.clone());
        if query(x) != query(y) {
            mismatch(
                &mut out,
                format!(
                    "record {}: the queries differ: {} {} against {} {}",
                    x.index, x.method, x.params, y.method, y.params
                ),
            );
        } else if (&x.answer, &x.error) != (&y.answer, &y.error) {
            let text = |r: &DiffRecord| match &r.error {
                Some(e) => format!("error {e}"),
                None => r.answer.to_string(),
            };
            mismatch(
                &mut out,
                format!(
                    "record {} {} {}: {} against {}",
                    x.index,
                    x.method,
                    x.params,
                    text(x),
                    text(y)
                ),
            );
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{
            execute_payload_and_update_canonical_chain,
            load::{open_production_env, LoadOutput, OutputConfig, OutputFactory},
        },
        worker::WorkerNetwork,
        RethEnv,
    };
    use jsonrpsee::RpcModule;
    use std::sync::Arc;
    use tn_types::{
        gas_accumulator::{BaseFeeContainer, GasAccumulator},
        max_batch_gas, Hash as _, NodeMode, TaskManager,
    };

    /// A small prefill: 300 accounts, a few senders, two pools.
    fn spec() -> PrefillSpec {
        PrefillSpec { accounts: 300, senders: 6, pools: 2, seed: 5, registry: false }
    }

    /// An env with three executed outputs of two batches each, and its HTTP methods.
    struct Chain {
        env: RethEnv,
        methods: Methods,
        _tasks: TaskManager,
        _tmp: tempfile::TempDir,
    }

    impl Chain {
        /// Build the chain: the same on every call.
        fn build() -> eyre::Result<Self> {
            let tmp = tempfile::tempdir()?;
            let tasks = TaskManager::default();
            let env =
                open_production_env(tmp.path(), Arc::new(spec().genesis().into()), &[], &tasks)?;
            let pool = env.init_txn_pool(BaseFeeContainer::default())?;
            let methods = env
                .get_rpc_server(
                    pool,
                    WorkerNetwork::new_for_test(env.chainspec()),
                    GasAccumulator::new(1).worker_base_fee(0),
                    || NodeMode::CvvActive,
                    RpcModule::new(()),
                )?
                .http_methods(|_| true)
                .ok_or_else(|| eyre::eyre!("the production env serves HTTP"))?;
            let config = OutputConfig { batches: 2, batch_gas: 3_000_000, ..Default::default() };
            let mut factory = OutputFactory::new(&env, spec(), Default::default(), config)?;
            for load in factory.generate(3) {
                execute(&env, &load)?;
            }
            Ok(Self { env, methods, _tasks: tasks, _tmp: tmp })
        }

        /// The header of a differential of `queries` queries at the tip, seeded with `seed`.
        fn header(&self, seed: u64, queries: usize, layout: &str) -> eyre::Result<DiffHeader> {
            let tip = self.env.canonical_tip();
            let genesis = self.env.sealed_header_by_number(0)?.expect("a genesis").hash();
            Ok(DiffHeader {
                diff_version: DIFF_VERSION,
                genesis_hash: genesis,
                tip_number: tip.number,
                tip_hash: tip.hash(),
                seed,
                queries,
                layout: layout.to_owned(),
                backend: "test".to_owned(),
            })
        }

        /// Record a differential to `path`.
        fn record(&self, header: DiffHeader, path: &Path) -> eyre::Result<DiffSummary> {
            tokio::task::block_in_place(|| record(&self.methods, &spec(), header, path))
        }
    }

    /// Execute `load`, one block per batch.
    fn execute(env: &RethEnv, load: &LoadOutput) -> eyre::Result<()> {
        let output = &load.output;
        for index in 0..load.batches.len() {
            let batch = load.batch(index);
            let digest = output.get_batch_digest(index).expect("a digest per batch");
            let payload = TNPayload::new(
                env.canonical_tip(),
                batch.beneficiary,
                index,
                digest,
                output,
                output.digest().into(),
                batch.base_fee_per_gas,
                max_batch_gas(output.leader().epoch()),
                output.prev_randao(index, digest),
                batch.worker_id,
            );
            execute_payload_and_update_canonical_chain(env, payload, batch.transactions.clone())?;
        }
        Ok(())
    }

    /// Two chains built from one load record the same queries and answers, which compare
    /// without a mismatch; every kind ran, with no error. A changed answer, a changed query, a
    /// missing record and another seed are each caught.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_differential_records_and_compares() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let (a, b) = (dir.path().join("a.jsonl"), dir.path().join("b.jsonl"));
        let first = Chain::build()?;
        let summary = first.record(first.header(11, 60, "v1")?, &a)?;
        assert_eq!(summary.errors(), 0, "{:?}", summary.first_error);
        for kind in DiffKind::ALL {
            assert_eq!(summary.kinds.get(&kind).map(|k| k.latencies.len()), Some(10), "{kind:?}");
        }
        let (_, records) = read(&a)?;
        assert_eq!(records.len(), 60);
        let found = |kind| records.iter().filter(|r| r.kind == kind && !r.answer.is_null()).count();
        assert_eq!(found(DiffKind::Tx), 10, "the chain's transactions answer");
        assert_eq!(found(DiffKind::Receipt), 10);
        assert_eq!(found(DiffKind::TxMiss), 0);
        drop(first);

        let second = Chain::build()?;
        second.record(second.header(11, 60, "v2")?, &b)?;
        let same = compare(&a, &b)?;
        assert_eq!(same, DiffComparison { compared: 60, mismatches: 0, shown: vec![] });

        let text = std::fs::read_to_string(&b)?;
        let mut lines: Vec<String> = text.lines().map(str::to_owned).collect();
        let edit =
            |lines: &[String], i: usize, f: &dyn Fn(&mut DiffRecord)| -> eyre::Result<String> {
                let mut record: DiffRecord = serde_json::from_str(&lines[i])?;
                f(&mut record);
                let mut out = lines.to_vec();
                out[i] = serde_json::to_string(&record)?;
                Ok(out.join("\n"))
            };
        let changed = dir.path().join("changed.jsonl");
        // record 1 (a balance): another answer
        std::fs::write(&changed, edit(&lines, 2, &|r| r.answer = json!("0x1"))?)?;
        let diff = compare(&a, &changed)?;
        assert_eq!(diff.mismatches, 1, "{diff:?}");
        assert!(diff.shown[0].starts_with("record 1 eth_getBalance"), "{diff:?}");
        // record 3 (a transaction): an error in place of the answer
        std::fs::write(&changed, edit(&lines, 4, &|r| r.error = Some("gone".to_owned()))?)?;
        assert_eq!(compare(&a, &changed)?.mismatches, 1);
        // record 2: another query
        std::fs::write(&changed, edit(&lines, 3, &|r| r.params = json!([]))?)?;
        let diff = compare(&a, &changed)?;
        assert_eq!(diff.mismatches, 1, "{diff:?}");
        assert!(diff.shown[0].starts_with("record 2: the queries differ"), "{diff:?}");
        // a missing record
        lines.pop();
        std::fs::write(&changed, lines.join("\n"))?;
        let diff = compare(&a, &changed)?;
        assert_eq!((diff.compared, diff.mismatches), (59, 1), "{diff:?}");
        // another seed
        second.record(second.header(12, 60, "v2")?, &changed)?;
        let diff = compare(&a, &changed)?;
        assert_eq!((diff.compared, diff.mismatches), (0, 1), "{diff:?}");
        assert!(diff.shown[0].contains("seed 11"), "{diff:?}");
        Ok(())
    }

    /// The by-hash check: a found transaction must be the one asked for, in its block; a random
    /// hash must find none.
    #[test]
    fn test_check_by_hash() {
        let hash = json!(B256::repeat_byte(1));
        let tx = json!({ "hash": B256::repeat_byte(1), "blockNumber": "0x5" });
        let receipt = json!({ "transactionHash": B256::repeat_byte(1), "blockNumber": "0x5" });
        assert_eq!(check_by_hash(&hash, &tx, Some(5)), Ok(()));
        assert_eq!(check_by_hash(&hash, &receipt, Some(5)), Ok(()));
        assert_eq!(check_by_hash(&hash, &Value::Null, None), Ok(()));
        assert!(check_by_hash(&hash, &tx, Some(4)).is_err(), "another block");
        assert!(check_by_hash(&hash, &Value::Null, Some(5)).is_err(), "not found");
        assert!(check_by_hash(&hash, &tx, None).is_err(), "a random hash found");
        let other = json!({ "hash": B256::repeat_byte(2), "blockNumber": "0x5" });
        assert!(check_by_hash(&hash, &other, Some(5)).is_err(), "another transaction");
    }
}
