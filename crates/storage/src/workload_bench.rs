//! Production-shaped workloads for the node's [`Database`] backends.
//!
//! [`crate::db_bench`] times basic operations one at a time. This module replays how the node
//! drives its consensus DB, so the backends can be compared on the traffic they would carry. The
//! model, from the production callers:
//!
//! - **One DB per process**: the `CompositeDatabase<MdbxDatabase>` that `open_db` builds (`Durable`
//!   sync in a production binary). It routes each table to one of three `LayeredDatabase`s, each
//!   with one background thread applying writes in order (`db_run` in `layered_db.rs`): a write
//!   made outside a transaction is its own physical commit, and `persist` acks behind everything
//!   queued before it. The epoch and kad layers keep every row in memory (loaded at `open_table`);
//!   the cache layer keeps only rows not yet written, so its reads go to disk.
//! - **A consensus round** (committee N, [`BATCHES_PER_HEADER`] batches per header), all on the
//!   epoch layer:
//!   - N certificate writes, each one transaction of the certificate and its by-round and by-origin
//!     index rows, each followed by `gc_rounds`: a by-round scan from the start, then one
//!     transaction removing certificates more than [`ROUNDS_TO_KEEP`] rounds old
//!     (`CertificateStore::write` and `gc_rounds` in `stores/certificate_store.rs`).
//!   - N−1 votes: concurrent request tasks, each reading the header's parents and payload, then
//!     writing the vote and awaiting `persist` before replying (`VoteDigestStore::write_vote`).
//!   - The proposer and the certifier: a write plus an awaited `persist` each before externalizing
//!     (`Proposer::store_and_send_header`; the certifier's own-certificate write).
//!   - About (N−1)·P payload rows, single writes as worker batches are reported.
//!   - O(N²) point reads: parent `contains_key` and certificate `get` per vote request and per
//!     certificate accepted.
//! - **An epoch**: proposed certificates and payload rows are never trimmed within it, so a restart
//!   late in an 8-hour epoch (about one round a second) reloads tens of thousands of certificates
//!   and over a million payload rows into memory. At its end the epoch tables are cleared
//!   (`clear_consensus_db_for_next_epoch`).
//! - **The batch cache**: one single insert (its own commit) per batch of 1 KB to 1 MB, our own
//!   batches also kept in the own-batch table until committed; concurrent readers (`multi_get`,
//!   from the executor and peer streams) served from disk; and, once each consensus output is
//!   saved, one write transaction evicting its committed batches from both tables
//!   (`evict_committed_batches` in `run_epoch.rs`).
//!
//! ## Workloads (each an `#[ignore]`d test)
//!
//! ```text
//! cargo test --release -p tn-storage workload_ -- --ignored --nocapture --test-threads 1
//! ```
//!
//! - `workload_consensus_rounds`: [`CONSENSUS_ROUNDS`] rounds at N = 10 and N = 50, then the
//!   epoch-end clear. Reports time per round, the durable latency of votes and of our own header
//!   and certificate (write to `persist` ack, including the wait behind queued writes), and the
//!   disk the epoch leaves behind.
//! - `workload_batch_cache`: single batch inserts (16 KB and 256 KB), with a per-output eviction
//!   transaction, against [`CACHE_READERS`] concurrent `multi_get` readers of the live batches, on
//!   the cache-mode layer.
//! - `workload_startup_reload`: a restart late in an epoch: reopen time, raw and with the
//!   full-memory layer loading every row.
//! - `workload_compaction`: a long-lived table under churn (a sliding window of single inserts and
//!   removes, plus overwrites), tndb with its compaction off and on: what compaction saves on disk
//!   and what it costs the writer (step latency, including the switch at a commit).
//!
//! ## Columns
//!
//! `MemDb` (no disk; the floor), `TnDb`, `MDBX-prod` (production `Durable` sync and geometry; not
//! in the consensus workload, see the caveats) and each of the two disk backends behind a
//! `LayeredDatabase`: full-memory (`Layered<_>`, the epoch
//! layer) or cache-mode (`Layered-cache<_>`, the batch cache), as production configures them.
//!
//! ## Caveats
//!
//! - Values are sized [`ByteVec`] proxies, so reads skip the BCS decode (and header rehash) that
//!   real certificates pay on every `get`.
//! - The layered columns use `LayeredDatabase` directly. Production reaches it through
//!   `CompositeDatabase`, whose write transaction opens a layer's transaction only on its first
//!   write, so this replay opens a `gc_rounds` transaction only when there is something to remove.
//! - Raw MDBX (without the layer) is left out of the consensus workload. Its writers there run
//!   concurrently (vote tasks, payload reports), and reth-libmdbx's `begin_rw_txn` answers a busy
//!   writer lock by sleeping 250 ms and retrying: measured at about 1.5 s per round and a 0.5 s
//!   median vote, so the column took ten minutes to say only that. Production never has concurrent
//!   raw MDBX writers, since each layer applies its writes on one thread.
//! - tndb and `Durable` MDBX commit with `msync`. A macOS `msync` doesn't flush the drive's cache;
//!   on Linux (production) it does, so durable latencies there are higher for both.

use std::{
    os::unix::fs::MetadataExt as _,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tn_types::{ByteVec, Database, DbTxMut as _, Table, TableHint, B256};
use tokio::{runtime::Runtime, task::JoinSet};

#[cfg(feature = "reth-libmdbx")]
use crate::{
    db_bench::open_mdbx_prod,
    mdbx::database::{PROD_CACHE_GROWTH, PROD_CACHE_MAX, PROD_EPOCH_MAX, PROD_GROWTH},
};
use crate::{
    layered_db::LayeredDatabase,
    mem_db::MemDatabase,
    tndb::{CompactionConfig, TnDatabase},
    ROUNDS_TO_KEEP,
};

/// Define a bench table with a production key shape.
macro_rules! bench_table {
    ($name:ident, $key:ty, $value:ty, $hint:expr, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug)]
        struct $name;
        impl Table for $name {
            type Key = $key;
            type Value = $value;
            const NAME: &'static str = stringify!($name);
            const HINT: TableHint = $hint;
        }
    };
}

bench_table!(Certs, B256, ByteVec, TableHint::Epoch, "`Certificates`: digest to certificate.");
bench_table!(
    CertsByRound,
    (u32, B256),
    ByteVec,
    TableHint::Epoch,
    "`CertificateDigestByRound`: (round, origin) to digest."
);
bench_table!(
    CertsByOrigin,
    (B256, u32),
    ByteVec,
    TableHint::Epoch,
    "`CertificateDigestByOrigin`: (origin, round) to digest."
);
bench_table!(Votes, B256, ByteVec, TableHint::Epoch, "`Votes`: authority to its last vote.");
bench_table!(LastProposed, u32, ByteVec, TableHint::Epoch, "`LastProposed`: our last header.");
bench_table!(
    Proposed,
    B256,
    ByteVec,
    TableHint::Epoch,
    "`ProposedCertificates`: our certificates."
);
bench_table!(
    Payload,
    (B256, u16),
    u8,
    TableHint::Epoch,
    "`Payload`: (batch digest, worker) to a presence token."
);
bench_table!(Batches, B256, ByteVec, TableHint::Cache, "`NodeBatchesCache`: digest to batch.");
bench_table!(OurBatches, B256, ByteVec, TableHint::Cache, "`OurNodeBatchesCache`: our batches.");
bench_table!(
    Churn,
    u64,
    ByteVec,
    TableHint::Cache,
    "A long-lived table whose rows are replaced over time (as the batch caches' and kad tables')."
);

// ---- the production model's sizes and rates ----

/// Batches per header (the payload rows each header adds and each vote request checks).
const BATCHES_PER_HEADER: usize = 5;
/// Timed rounds in the consensus workload: with the warm-up, enough for `ROUNDS_TO_KEEP` rounds of
/// fill and then a long stretch of steady-state garbage collection.
const CONSENSUS_ROUNDS: u32 = 200;
/// Untimed rounds first, so the timed ones measure the steady state, not one-time setup (a table's
/// first write, a backend's background preparation after open).
const CONSENSUS_WARMUP_ROUNDS: u32 = 20;
/// An encoded vote (`VoteInfo`).
const VOTE_SIZE: usize = 41;
/// The payload table's presence token.
const PAYLOAD_TOKEN: u8 = 1;
/// Concurrent `multi_get` readers of the batch cache (executor and peer streams).
const CACHE_READERS: usize = 4;
/// Digests per cache `multi_get` (about an output's batches for one worker).
const CACHE_READ_CHUNK: usize = 32;
/// Batches one consensus output commits (N = 10 validators, [`BATCHES_PER_HEADER`] each).
const OUTPUT_BATCHES: u64 = 50;
/// Outputs between a batch's insert and its eviction (it is certified and committed meanwhile).
const EVICT_LAG: u64 = 4;
/// One batch in this many is our own, kept in the own-batch table too until it is committed.
const OUR_SHARE: u64 = 10;
/// The committee of the startup-reload workload.
const RELOAD_COMMITTEE: usize = 10;
/// Rounds before the restart: an 8-hour epoch at about one round a second.
const RELOAD_ROUNDS: u32 = 28_800;
/// Rows per bulk transaction when populating the startup-reload database.
const RELOAD_TXN_ROWS: usize = 10_000;

/// A quorum of an `n`-member committee: a header's parent count, and a certificate's signers.
fn quorum(n: usize) -> usize {
    2 * n / 3 + 1
}

/// An encoded header: fixed fields, a digest per batch, and a digest per parent.
fn header_size(n: usize) -> usize {
    142 + 35 * BATCHES_PER_HEADER + 33 * quorum(n)
}

/// An encoded certificate: its header, the aggregate signature and the signer bitmap.
fn cert_size(n: usize) -> usize {
    header_size(n) + 67 + 2 * quorum(n)
}

// ---- synthetic keys and values ----

/// Digest domains, so the synthetic digest streams never collide.
const DOMAIN_CERT: u64 = 1;
const DOMAIN_ORIGIN: u64 = 2;
const DOMAIN_PAYLOAD: u64 = 3;
const DOMAIN_BATCH: u64 = 4;

/// SplitMix64: a cheap, well-mixed stream.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// A synthetic 32-byte digest of `(domain, a, b)`: random-looking, as real hashes are, so rows
/// arrive in no key order.
fn digest(domain: u64, a: u64, b: u64) -> B256 {
    let seed = mix(domain ^ mix(a ^ mix(b)));
    let mut bytes = [0_u8; 32];
    for (lane, chunk) in bytes.chunks_mut(8).enumerate() {
        chunk.copy_from_slice(&mix(seed.wrapping_add(lane as u64)).to_le_bytes());
    }
    B256::from(bytes)
}

/// Authority `i`'s key; we are authority 0.
fn origin(i: usize) -> B256 {
    digest(DOMAIN_ORIGIN, i as u64, 0)
}

/// The digest of authority `i`'s certificate for `round`.
fn cert_digest(round: u32, i: usize) -> B256 {
    digest(DOMAIN_CERT, u64::from(round), i as u64)
}

/// The `j`th payload row reported in `round`.
fn payload_key(round: u32, j: usize) -> (B256, u16) {
    (digest(DOMAIN_PAYLOAD, u64::from(round), j as u64), 0)
}

/// A digest as an index value (a length-prefixed 32 bytes, as production stores it).
fn digest_value(d: &B256) -> ByteVec {
    ByteVec(d.to_vec())
}

/// A deterministic, non-trivial value of `size` bytes.
fn filler(size: usize, seed: u64) -> ByteVec {
    let s = seed.to_le_bytes();
    ByteVec((0..size).map(|i| s[i % 8].wrapping_add(i as u8)).collect())
}

// ---- measuring and reporting ----

/// Bytes allocated on disk under `dir` (blocks, not file lengths: mapped files are preallocated or
/// sparse).
fn disk_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| match entry.metadata() {
                    Ok(meta) if meta.is_dir() => disk_bytes(&entry.path()),
                    Ok(meta) => meta.blocks() * 512,
                    Err(_) => 0,
                })
                .sum()
        })
        .unwrap_or(0)
}

/// The `p` quantile (0 to 1) of `samples`, which this sorts.
fn quantile(samples: &mut [Duration], p: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples.sort_unstable();
    samples[((samples.len() - 1) as f64 * p).round() as usize]
}

/// Which way a report row's numbers improve, for coloring its best and worst backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Better {
    Lower,
    Higher,
    /// Not a measurement (e.g. a row count): never colored.
    Unranked,
}

/// One backend's value for one report row.
#[derive(Debug)]
struct Cell {
    label: String,
    text: String,
    better: Better,
}

impl Cell {
    fn new(label: &str, better: Better, text: String) -> Self {
        Self { label: label.to_string(), text, better }
    }

    /// A time in milliseconds.
    fn ms(label: &str, d: Duration) -> Self {
        Self::new(label, Better::Lower, format!("{:.2}", d.as_secs_f64() * 1e3))
    }

    /// A time in microseconds.
    fn us(label: &str, d: Duration) -> Self {
        Self::new(label, Better::Lower, format!("{:.0}", d.as_secs_f64() * 1e6))
    }

    /// Disk use in MiB (`-` for the in-memory backend).
    fn mb(label: &str, bytes: Option<u64>) -> Self {
        let text = bytes
            .map_or_else(|| "-".to_string(), |b| format!("{:.1}", b as f64 / (1024.0 * 1024.0)));
        Self::new(label, Better::Lower, text)
    }

    /// A throughput.
    fn rate(label: &str, per_sec: f64) -> Self {
        Self::new(label, Better::Higher, format!("{per_sec:.1}"))
    }

    /// A count, shown but not ranked.
    fn count(label: &str, n: usize) -> Self {
        Self::new(label, Better::Unranked, n.to_string())
    }
}

/// One report column: a backend name and its cells, one per row.
type Column = (String, Vec<Cell>);

/// The reference column: shown for scale, left out of the coloring (the comparison is tndb
/// against MDBX).
const REFERENCE: &str = "MemDb";

const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

/// Color the report only on a terminal, and not when `NO_COLOR` is set.
fn use_color() -> bool {
    use std::io::IsTerminal as _;
    std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
}

/// The best and worst displayed values of `row` across the ranked columns (every column but the
/// reference), or `None` when the row is unranked or they all tie. Compared as displayed, so
/// values that print the same rank the same.
fn row_extremes(cols: &[Column], row: usize) -> Option<(f64, f64)> {
    let better = cols.first()?.1[row].better;
    let values: Vec<f64> = cols
        .iter()
        .filter(|(name, _)| name != REFERENCE)
        .filter_map(|(_, cells)| cells[row].text.parse().ok())
        .collect();
    let min = values.iter().copied().fold(f64::INFINITY, f64::min);
    let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if values.len() < 2 || min == max {
        return None;
    }
    match better {
        Better::Lower => Some((min, max)),
        Better::Higher => Some((max, min)),
        Better::Unranked => None,
    }
}

/// Print the columns side by side (rows labeled from the first column). On a terminal, each row's
/// best value among the ranked columns is green and its worst red.
fn print_table(title: &str, legend: &str, cols: &[Column]) {
    let Some((_, first)) = cols.first() else { return };
    let color = use_color();
    let label_w = first.iter().map(|cell| cell.label.len()).max().unwrap_or(0);
    let cell_w = cols
        .iter()
        .map(|(name, cells)| cells.iter().map(|c| c.text.len()).fold(name.len(), usize::max))
        .max()
        .unwrap_or(0)
        .max(10);
    println!("\n=== {title} ===");
    println!("{legend}");
    if color {
        println!("{GREEN}green{RESET} / {RED}red{RESET}: each row's best / worst backend ({REFERENCE} is a reference, not ranked)");
    }
    print!("{:<label_w$}", "");
    for (name, _) in cols {
        print!(" {name:>cell_w$}");
    }
    println!();
    for (row, cell) in first.iter().enumerate() {
        print!("{:<label_w$}", cell.label);
        let extremes = if color { row_extremes(cols, row) } else { None };
        for (name, cells) in cols {
            let text = &cells[row].text;
            // Pad before coloring: the escape codes take no columns on screen.
            let padded = format!("{text:>cell_w$}");
            let shade = extremes.filter(|_| name != REFERENCE).and_then(|(best, worst)| {
                let value = text.parse::<f64>().ok()?;
                if value == best {
                    Some(GREEN)
                } else if value == worst {
                    Some(RED)
                } else {
                    None
                }
            });
            match shade {
                Some(shade) => print!(" {shade}{padded}{RESET}"),
                None => print!(" {padded}"),
            }
        }
        println!();
    }
}

// ---- running a workload on every backend ----

/// A workload replayed on each backend.
trait Workload {
    /// Open every table the workload uses.
    fn open_tables<DB: Database>(&self, db: &DB);

    /// Run on `db` (its tables open) and return the report's cells, one per row. `dir` is the
    /// backend's directory (`None` for the in-memory backend); the workload drops `db` before
    /// measuring the disk it used.
    fn run<DB: Database>(&mut self, rt: &Runtime, db: DB, dir: Option<&Path>) -> Vec<Cell>;
}

/// The layer mode of the layered columns: production's epoch layer keeps every row in memory, its
/// cache layer only rows not yet written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layer {
    FullMemory,
    Cache,
}

/// Run `w` on one backend and label the result.
fn column<W: Workload, DB: Database>(
    rt: &Runtime,
    w: &mut W,
    name: String,
    db: DB,
    dir: Option<&Path>,
) -> Column {
    println!("  running {name} ...");
    w.open_tables(&db);
    let cells = w.run(rt, db, dir);
    (name, cells)
}

/// Run `w` on every production-relevant backend, the layered ones in `layer` mode, each in its own
/// directory under `base`. `raw_mdbx` includes MDBX without the layer; a workload with concurrent
/// writers leaves it out (see the module caveats).
fn run_backends<W: Workload>(
    rt: &Runtime,
    w: &mut W,
    base: &Path,
    layer: Layer,
    raw_mdbx: bool,
) -> Vec<Column> {
    let full_memory = layer == Layer::FullMemory;
    let layered = if full_memory { "Layered" } else { "Layered-cache" };
    let mut cols = vec![column(rt, w, "MemDb".to_string(), MemDatabase::new(), None)];

    let dir = base.join("tndb");
    let db = TnDatabase::open(&dir).expect("open tndb");
    cols.push(column(rt, w, "TnDb".to_string(), db, Some(&dir)));
    let dir = base.join("tndb_layered");
    let db = LayeredDatabase::open(TnDatabase::open(&dir).expect("open tndb"), full_memory);
    cols.push(column(rt, w, format!("{layered}<TnDb>"), db, Some(&dir)));

    #[cfg(feature = "reth-libmdbx")]
    {
        // The production geometry of the environment that layer mode stands for.
        let (tables, max_size, growth) = match layer {
            Layer::FullMemory => (8, PROD_EPOCH_MAX, PROD_GROWTH),
            Layer::Cache => (4, PROD_CACHE_MAX, PROD_CACHE_GROWTH),
        };
        if raw_mdbx {
            let dir = base.join("mdbx_prod");
            let db = open_mdbx_prod(&dir, tables, max_size, growth);
            cols.push(column(rt, w, "MDBX-prod".to_string(), db, Some(&dir)));
        }
        let dir = base.join("mdbx_prod_layered");
        let db = LayeredDatabase::open(open_mdbx_prod(&dir, tables, max_size, growth), full_memory);
        cols.push(column(rt, w, format!("{layered}<MDBX-prod>"), db, Some(&dir)));
    }
    cols
}

// ---- workload 1: consensus rounds ----

/// Open the seven epoch tables.
fn open_epoch_tables<DB: Database>(db: &DB) {
    db.open_table::<Certs>().expect("open Certs");
    db.open_table::<CertsByRound>().expect("open CertsByRound");
    db.open_table::<CertsByOrigin>().expect("open CertsByOrigin");
    db.open_table::<Votes>().expect("open Votes");
    db.open_table::<LastProposed>().expect("open LastProposed");
    db.open_table::<Proposed>().expect("open Proposed");
    db.open_table::<Payload>().expect("open Payload");
}

/// What a round's concurrent task measured.
enum Durable {
    /// A vote: write to `persist` ack.
    Vote(Duration),
    /// Our header or certificate: write to `persist` ack.
    Own(Duration),
    /// Untimed (the payload writer).
    None,
}

/// The values one consensus workload writes.
struct RoundValues {
    header: ByteVec,
    cert: ByteVec,
    vote: ByteVec,
}

/// Rounds of consensus traffic on the epoch tables, then the epoch-end clear.
struct ConsensusRounds {
    committee: usize,
    rounds: u32,
}

impl ConsensusRounds {
    /// The concurrent part of round `r`: worker batch reports (payload rows), the other
    /// authorities' vote requests, and our proposer and certifier, each persisting before it
    /// externalizes. Returns what each task measured.
    async fn concurrent_tasks<DB: Database>(
        &self,
        db: &DB,
        r: u32,
        values: &Arc<RoundValues>,
    ) -> Vec<Durable> {
        let n = self.committee;
        let mut tasks = JoinSet::new();

        // Worker batch reports for this round's headers: single payload writes, no barrier.
        let payload_db = db.clone();
        tasks.spawn(async move {
            for j in 0..(n - 1) * BATCHES_PER_HEADER {
                payload_db.insert::<Payload>(&payload_key(r, j), &PAYLOAD_TOKEN).expect("payload");
            }
            Durable::None
        });

        // Vote requests from the other authorities: validate the header (its parents, the
        // previous round's certificates, and its payload), then vote and persist before replying.
        for author in 1..n {
            let (db, values) = (db.clone(), Arc::clone(values));
            tasks.spawn(async move {
                let _ = db.get::<Votes>(&origin(author)).expect("get vote");
                if r > 1 {
                    for p in 0..quorum(n) {
                        let _ = db.contains_key::<Certs>(&cert_digest(r - 1, p)).expect("parent");
                    }
                    for p in 0..quorum(n) {
                        let _ = db.get::<Certs>(&cert_digest(r - 1, p)).expect("read parent");
                    }
                    for b in 0..BATCHES_PER_HEADER {
                        let key = payload_key(r - 1, (author - 1) * BATCHES_PER_HEADER + b);
                        let _ = db.contains_key::<Payload>(&key).expect("payload check");
                    }
                }
                let start = Instant::now();
                db.insert::<Votes>(&origin(author), &values.vote).expect("insert vote");
                db.persist::<Votes>().await.expect("persist vote");
                Durable::Vote(start.elapsed())
            });
        }

        // Our proposer: store the header, durable before it is sent.
        let (proposer_db, proposer_values) = (db.clone(), Arc::clone(values));
        tasks.spawn(async move {
            let _ = proposer_db.get::<LastProposed>(&0).expect("get last proposed");
            let start = Instant::now();
            proposer_db.insert::<LastProposed>(&0, &proposer_values.header).expect("header");
            proposer_db.persist::<LastProposed>().await.expect("persist header");
            Durable::Own(start.elapsed())
        });

        // Our certifier: store our certificate, durable before it is gossiped.
        let (certifier_db, certifier_values) = (db.clone(), Arc::clone(values));
        tasks.spawn(async move {
            let ours = cert_digest(r, 0);
            let _ = certifier_db.get::<Proposed>(&ours).expect("get proposed");
            let start = Instant::now();
            certifier_db.insert::<Proposed>(&ours, &certifier_values.cert).expect("proposed");
            certifier_db.persist::<Proposed>().await.expect("persist proposed");
            Durable::Own(start.elapsed())
        });

        let mut measured = Vec::with_capacity(n + 2);
        while let Some(task) = tasks.join_next().await {
            measured.push(task.expect("round task"));
        }
        measured
    }

    /// The certificate manager's part of round `r`, one certificate at a time: validate it,
    /// store it with its index rows, then garbage-collect old rounds.
    fn store_certificates<DB: Database>(&self, db: &DB, r: u32, cert: &ByteVec) {
        let n = self.committee;
        for i in 0..n {
            let d = cert_digest(r, i);
            let _ = db.contains_key::<Certs>(&d).expect("known certificate");
            if r > 1 {
                for p in 0..quorum(n) {
                    let _ = db.contains_key::<Certs>(&cert_digest(r - 1, p)).expect("parent");
                }
            }
            for b in 0..BATCHES_PER_HEADER {
                let key = payload_key(r.saturating_sub(1), (i % (n - 1)) * BATCHES_PER_HEADER + b);
                let _ = db.contains_key::<Payload>(&key).expect("payload check");
            }

            let mut txn = db.write_txn().expect("write_txn");
            txn.insert::<Certs>(&d, cert).expect("insert certificate");
            txn.insert::<CertsByRound>(&(r, origin(i)), &digest_value(&d)).expect("by round");
            txn.insert::<CertsByOrigin>(&(origin(i), r), &digest_value(&d)).expect("by origin");
            txn.commit().expect("commit certificate");
            gc_rounds(db, r);
        }
    }
}

/// `gc_rounds` as production runs it after every certificate write: scan the by-round index from
/// the start for certificates more than `ROUNDS_TO_KEEP` rounds old, then remove them and their
/// index rows in one transaction (opened only when there is something to remove; see the module
/// caveats).
fn gc_rounds<DB: Database>(db: &DB, round: u32) {
    if round <= ROUNDS_TO_KEEP {
        return;
    }
    let target = round - ROUNDS_TO_KEEP;
    let old: Vec<((u32, B256), ByteVec)> =
        db.iter::<CertsByRound>().take_while(|((r, _), _)| *r < target).collect();
    if old.is_empty() {
        return;
    }
    let mut txn = db.write_txn().expect("write_txn");
    for ((r, origin), d) in old {
        txn.remove::<Certs>(&B256::from_slice(&d.0)).expect("remove certificate");
        txn.remove::<CertsByRound>(&(r, origin)).expect("remove by round");
        txn.remove::<CertsByOrigin>(&(origin, r)).expect("remove by origin");
    }
    txn.commit().expect("commit gc");
}

impl Workload for ConsensusRounds {
    fn open_tables<DB: Database>(&self, db: &DB) {
        open_epoch_tables(db);
    }

    fn run<DB: Database>(&mut self, rt: &Runtime, db: DB, dir: Option<&Path>) -> Vec<Cell> {
        let n = self.committee;
        let values = Arc::new(RoundValues {
            header: filler(header_size(n), 1),
            cert: filler(cert_size(n), 2),
            vote: filler(VOTE_SIZE, 3),
        });
        let (mut votes, mut own) = (Vec::new(), Vec::new());
        let mut cert_phase = Duration::ZERO;

        let warmup = CONSENSUS_WARMUP_ROUNDS;
        for r in 1..=warmup {
            rt.block_on(self.concurrent_tasks(&db, r, &values));
            self.store_certificates(&db, r, &values.cert);
        }
        let start = Instant::now();
        for r in warmup + 1..=warmup + self.rounds {
            for measured in rt.block_on(self.concurrent_tasks(&db, r, &values)) {
                match measured {
                    Durable::Vote(d) => votes.push(d),
                    Durable::Own(d) => own.push(d),
                    Durable::None => {}
                }
            }
            let phase = Instant::now();
            self.store_certificates(&db, r, &values.cert);
            cert_phase += phase.elapsed();
        }
        let elapsed = start.elapsed();

        // The certificates kept: the last `ROUNDS_TO_KEEP` rounds (plus the current one).
        let kept = db.iter::<Certs>().count();
        assert_eq!(kept, (ROUNDS_TO_KEEP as usize + 1) * n, "gc keeps the recent rounds");

        // Epoch end: clear the epoch tables (bare clears, as production does), durably.
        let clear_start = Instant::now();
        db.clear_table::<Certs>().expect("clear");
        db.clear_table::<CertsByRound>().expect("clear");
        db.clear_table::<CertsByOrigin>().expect("clear");
        db.clear_table::<Votes>().expect("clear");
        db.clear_table::<LastProposed>().expect("clear");
        db.clear_table::<Proposed>().expect("clear");
        db.clear_table::<Payload>().expect("clear");
        rt.block_on(db.persist::<Payload>()).expect("persist clear");
        let clear = clear_start.elapsed();
        drop(db);

        let rounds = self.rounds;
        vec![
            Cell::ms("ms / round", elapsed / rounds),
            Cell::ms("  cert phase ms / round", cert_phase / rounds),
            Cell::us("vote durable p50 us", quantile(&mut votes, 0.50)),
            Cell::us("vote durable p99 us", quantile(&mut votes, 0.99)),
            Cell::us("own header/cert durable p50 us", quantile(&mut own, 0.50)),
            Cell::us("own header/cert durable p99 us", quantile(&mut own, 0.99)),
            Cell::ms("epoch clear ms (durable)", clear),
            Cell::mb("disk MB after the epoch", dir.map(disk_bytes)),
        ]
    }
}

/// Consensus rounds on the epoch tables at two committee sizes.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tn-storage workload_consensus_rounds -- --ignored --nocapture
/// --test-threads 1`.
#[test]
#[ignore = "on-demand production-workload benchmark; run with --ignored --nocapture --test-threads 1"]
fn workload_consensus_rounds() {
    let rt = Runtime::new().expect("tokio runtime");
    for committee in [10, 50] {
        let tmp = tempfile::tempdir().expect("temp dir");
        let mut workload = ConsensusRounds { committee, rounds: CONSENSUS_ROUNDS };
        // Raw MDBX is left out: its concurrent writers stall (see the module caveats).
        let cols = run_backends(&rt, &mut workload, tmp.path(), Layer::FullMemory, false);
        print_table(
            &format!(
                "consensus rounds: N={committee}, {CONSENSUS_ROUNDS} rounds after \
                 {CONSENSUS_WARMUP_ROUNDS} warm-up rounds, cert {} B, header {} B",
                cert_size(committee),
                header_size(committee)
            ),
            "durable = write to persist ack (raw backends are durable at the write); disk = \
             allocated bytes after the epoch-end clear and close; raw MDBX omitted (concurrent \
             writers stall in reth-libmdbx's 250 ms busy backoff)",
            &cols,
        );
    }
}

// ---- workload 2: the batch cache ----

/// Batches into the cache, each a single insert, with concurrent `multi_get` readers.
struct BatchCache {
    size: usize,
    count: u64,
}

impl Workload for BatchCache {
    fn open_tables<DB: Database>(&self, db: &DB) {
        db.open_table::<Batches>().expect("open Batches");
        db.open_table::<OurBatches>().expect("open OurBatches");
    }

    fn run<DB: Database>(&mut self, rt: &Runtime, db: DB, dir: Option<&Path>) -> Vec<Cell> {
        let batch = filler(self.size, 9);
        let keys: Vec<B256> = (0..self.count).map(|i| digest(DOMAIN_BATCH, i, 0)).collect();
        let written = AtomicU64::new(0);
        // The lowest batch not yet evicted. Readers choose from two outputs above it, so a batch
        // they pick stays live for at least two more evictions.
        let floor = AtomicU64::new(0);
        let done = AtomicBool::new(false);

        let (write_time, gets) = std::thread::scope(|s| {
            let readers: Vec<_> = (0..CACHE_READERS as u64)
                .map(|reader| {
                    let (db, keys, written, floor, done) = (&db, &keys, &written, &floor, &done);
                    s.spawn(move || {
                        let mut x = mix(reader);
                        let mut gets = 0_u64;
                        while !done.load(Ordering::Acquire) {
                            let low = floor.load(Ordering::Acquire) + 2 * OUTPUT_BATCHES;
                            let available = written.load(Ordering::Acquire);
                            if available < low + CACHE_READ_CHUNK as u64 {
                                std::thread::yield_now();
                                continue;
                            }
                            let chunk: Vec<B256> = (0..CACHE_READ_CHUNK)
                                .map(|_| {
                                    x = mix(x);
                                    keys[(low + x % (available - low)) as usize]
                                })
                                .collect();
                            let got = db.multi_get::<Batches>(chunk.iter()).expect("multi_get");
                            assert!(got.iter().all(Option::is_some), "a written batch is readable");
                            gets += chunk.len() as u64;
                        }
                        gets
                    })
                })
                .collect();

            let start = Instant::now();
            for (i, key) in keys.iter().enumerate() {
                let i = i as u64;
                if i.is_multiple_of(OUR_SHARE) {
                    db.insert::<OurBatches>(key, &batch).expect("insert our batch");
                }
                db.insert::<Batches>(key, &batch).expect("insert batch");
                written.store(i + 1, Ordering::Release);
                // Each saved output evicts the batches it committed, from both tables, in one
                // transaction (present in the own-batch table only for our own).
                if (i + 1).is_multiple_of(OUTPUT_BATCHES)
                    && i + 1 >= (EVICT_LAG + 1) * OUTPUT_BATCHES
                {
                    let from = i + 1 - (EVICT_LAG + 1) * OUTPUT_BATCHES;
                    floor.store(from + OUTPUT_BATCHES, Ordering::Release);
                    let mut txn = db.write_txn().expect("evict txn");
                    for k in &keys[from as usize..(from + OUTPUT_BATCHES) as usize] {
                        txn.remove::<Batches>(k).expect("evict batch");
                        txn.remove::<OurBatches>(k).expect("evict our batch");
                    }
                    txn.commit().expect("evict commit");
                }
            }
            rt.block_on(db.persist::<Batches>()).expect("persist batches");
            let write_time = start.elapsed();
            done.store(true, Ordering::Release);
            let gets: u64 = readers.into_iter().map(|r| r.join().expect("reader")).sum();
            (write_time, gets)
        });
        let evicted = floor.load(Ordering::Acquire) as usize;
        let live = keys.iter().filter(|k| db.get::<Batches>(k).expect("get").is_some()).count();
        assert_eq!(live, keys.len() - evicted, "exactly the batches not evicted remain");

        // Epoch end: the batch cache is cleared.
        let clear_start = Instant::now();
        db.clear_table::<Batches>().expect("clear batches");
        rt.block_on(db.persist::<Batches>()).expect("persist clear");
        let clear = clear_start.elapsed();
        drop(db);

        let bytes = self.size as f64 * self.count as f64;
        let secs = write_time.as_secs_f64();
        vec![
            Cell::rate("write MB/s (to disk)", bytes / secs / 1e6),
            Cell::rate("reader K gets/s", gets as f64 / secs / 1e3),
            Cell::ms("epoch clear ms (durable)", clear),
            Cell::mb("disk MB after the epoch", dir.map(disk_bytes)),
        ]
    }
}

/// The batch cache at two batch sizes, on the cache-mode layer.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tn-storage workload_batch_cache -- --ignored --nocapture
/// --test-threads 1`.
#[test]
#[ignore = "on-demand production-workload benchmark; run with --ignored --nocapture --test-threads 1"]
fn workload_batch_cache() {
    let rt = Runtime::new().expect("tokio runtime");
    for (size, count) in [(16 * 1024, 4_000), (256 * 1024, 400)] {
        let tmp = tempfile::tempdir().expect("temp dir");
        let mut workload = BatchCache { size, count };
        let cols = run_backends(&rt, &mut workload, tmp.path(), Layer::Cache, true);
        print_table(
            &format!(
                "batch cache: {count} x {} KB single inserts, {CACHE_READERS} readers x \
                 {CACHE_READ_CHUNK}-key multi_get",
                size / 1024
            ),
            "write = inserts (+ an eviction txn per output) through the final persist ack; \
             readers run until then",
            &cols,
        );
    }
}

// ---- workload 3: startup reload ----

/// Writes rows in bulk transactions of [`RELOAD_TXN_ROWS`] rows.
struct BulkWriter<'a, DB: Database> {
    db: &'a DB,
    txn: Option<DB::TXMut<'a>>,
    in_txn: usize,
    rows: usize,
}

impl<'a, DB: Database> BulkWriter<'a, DB> {
    fn new(db: &'a DB) -> Self {
        Self { db, txn: None, in_txn: 0, rows: 0 }
    }

    /// The open transaction (opening one if needed).
    fn txn(&mut self) -> &mut DB::TXMut<'a> {
        let db = self.db;
        self.txn.get_or_insert_with(|| db.write_txn().expect("write_txn"))
    }

    /// Count `rows` written, committing once the transaction is full.
    fn wrote(&mut self, rows: usize) {
        self.rows += rows;
        self.in_txn += rows;
        if self.in_txn >= RELOAD_TXN_ROWS {
            self.commit();
        }
    }

    fn commit(&mut self) {
        if let Some(txn) = self.txn.take() {
            txn.commit().expect("commit");
        }
        self.in_txn = 0;
    }
}

/// Fill the epoch tables as they stand late in an epoch: every proposed certificate and payload
/// row of [`RELOAD_ROUNDS`] rounds, the certificates gc keeps, the votes and our last header.
/// Returns the row count.
fn populate_late_epoch<DB: Database>(db: &DB) -> usize {
    let n = RELOAD_COMMITTEE;
    let cert = filler(cert_size(n), 2);
    let mut w = BulkWriter::new(db);
    for r in 1..=RELOAD_ROUNDS {
        w.txn().insert::<Proposed>(&cert_digest(r, 0), &cert).expect("proposed");
        w.wrote(1);
        for j in 0..(n - 1) * BATCHES_PER_HEADER {
            w.txn().insert::<Payload>(&payload_key(r, j), &PAYLOAD_TOKEN).expect("payload");
            w.wrote(1);
        }
    }
    for r in RELOAD_ROUNDS - ROUNDS_TO_KEEP..=RELOAD_ROUNDS {
        for i in 0..n {
            let d = cert_digest(r, i);
            let txn = w.txn();
            txn.insert::<Certs>(&d, &cert).expect("certificate");
            txn.insert::<CertsByRound>(&(r, origin(i)), &digest_value(&d)).expect("by round");
            txn.insert::<CertsByOrigin>(&(origin(i), r), &digest_value(&d)).expect("by origin");
            w.wrote(3);
        }
    }
    for i in 1..n {
        w.txn().insert::<Votes>(&origin(i), &filler(VOTE_SIZE, 3)).expect("vote");
        w.wrote(1);
    }
    w.txn().insert::<LastProposed>(&0, &filler(header_size(n), 1)).expect("header");
    w.wrote(1);
    w.commit();
    w.rows
}

/// Rows in the seven epoch tables.
fn count_epoch_rows<DB: Database>(db: &DB) -> usize {
    db.iter::<Certs>().count()
        + db.iter::<CertsByRound>().count()
        + db.iter::<CertsByOrigin>().count()
        + db.iter::<Votes>().count()
        + db.iter::<LastProposed>().count()
        + db.iter::<Proposed>().count()
        + db.iter::<Payload>().count()
}

/// Populate a late-epoch database with `open`, close it, then time two reopens: the raw backend
/// (open plus `open_table`) and the full-memory layer over it (which loads every row into memory).
/// With `crash` (how to abandon an open handle as a crashed process would, so the next open
/// recovers), also time both reopens after a crash. Returns the raw and layered report columns.
fn reload<DB: Database>(
    name: &str,
    dir: &Path,
    open: impl Fn(&Path) -> DB,
    crash: Option<fn(DB)>,
) -> [Column; 2] {
    println!("  populating {name} ...");
    let rows = {
        let db = open(dir);
        open_epoch_tables(&db);
        populate_late_epoch(&db)
    };
    let disk = disk_bytes(dir);

    let start = Instant::now();
    let db = open(dir);
    open_epoch_tables(&db);
    let raw = start.elapsed();
    assert_eq!(count_epoch_rows(&db), rows, "{name}: every row readable after a reopen");
    drop(db);

    let start = Instant::now();
    let db = LayeredDatabase::open(open(dir), true);
    open_epoch_tables(&db);
    let layered = start.elapsed();
    assert_eq!(count_epoch_rows(&db), rows, "{name}: every row loaded into the memory layer");
    drop(db);

    // After a crash: the files are left unclosed (the handle is leaked), so each reopen recovers.
    let crashed = crash.map(|crash| {
        let db = open(dir);
        open_epoch_tables(&db);
        crash(db);
        let start = Instant::now();
        let db = open(dir);
        open_epoch_tables(&db);
        let raw = start.elapsed();
        assert_eq!(count_epoch_rows(&db), rows, "{name}: every row recovered after a crash");
        crash(db);
        let start = Instant::now();
        let db = LayeredDatabase::open(open(dir), true);
        open_epoch_tables(&db);
        let layered = start.elapsed();
        assert_eq!(count_epoch_rows(&db), rows, "{name}: every row recovered and loaded");
        drop(db);
        (raw, layered)
    });

    let cells = |time: Duration, crashed: Option<Duration>| {
        vec![
            Cell::ms("reopen ms", time),
            crashed.map_or_else(
                || Cell::new("crash reopen ms (rebuild)", Better::Lower, "-".to_string()),
                |d| Cell::ms("crash reopen ms (rebuild)", d),
            ),
            Cell::count("rows", rows),
            Cell::mb("disk MB", Some(disk)),
        ]
    };
    [
        (name.to_string(), cells(raw, crashed.map(|(raw, _)| raw))),
        (format!("Layered<{name}>"), cells(layered, crashed.map(|(_, layered)| layered))),
    ]
}

/// A restart late in an epoch: reopen time, raw and through the full-memory layer.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tn-storage workload_startup_reload -- --ignored --nocapture
/// --test-threads 1`.
#[test]
#[ignore = "on-demand production-workload benchmark; run with --ignored --nocapture --test-threads 1"]
fn workload_startup_reload() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let mut cols = Vec::new();
    cols.extend(reload(
        "TnDb",
        &tmp.path().join("tndb"),
        |dir| TnDatabase::open(dir).expect("open tndb"),
        // A leaked handle with its table locks released, as after a crashed process.
        Some(|db: TnDatabase| {
            db.release_locks_for_crash();
            std::mem::forget(db);
        }),
    ));
    #[cfg(feature = "reth-libmdbx")]
    cols.extend(reload(
        "MDBX-prod",
        &tmp.path().join("mdbx_prod"),
        |dir| open_mdbx_prod(dir, 8, PROD_EPOCH_MAX, PROD_GROWTH),
        None,
    ));
    print_table(
        &format!(
            "startup reload: N={RELOAD_COMMITTEE}, {RELOAD_ROUNDS} rounds into the epoch \
             (proposed certificates and payload rows are kept all epoch)"
        ),
        "reopen = open + open_table of the 7 epoch tables; the layered reopen also loads every \
         row into memory",
        &cols,
    );
}

// ---- workload 4: compaction ----

/// Live rows in the churn workload's sliding window.
const CHURN_WINDOW: u64 = 2_048;
/// The churn workload's value size.
const CHURN_VALUE: usize = 4 * 1024;
/// Steps: each inserts the next row and removes the one leaving the window; every fourth also
/// overwrites a live row.
const CHURN_STEPS: u64 = 40_000;

/// A long-lived table under churn: single (autocommit) inserts, removes and overwrites.
struct ChurnWorkload;

/// The compactions a tndb table has switched to: its generation number (the workload never
/// clears), or `None` for another backend.
fn tndb_compactions(dir: &Path) -> Option<usize> {
    std::fs::read_dir(dir.join(Churn::NAME))
        .ok()?
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.strip_prefix("gen-")?.parse().ok())
        .max()
}

impl Workload for ChurnWorkload {
    fn open_tables<DB: Database>(&self, db: &DB) {
        db.open_table::<Churn>().expect("open Churn");
    }

    fn run<DB: Database>(&mut self, rt: &Runtime, db: DB, dir: Option<&Path>) -> Vec<Cell> {
        let value = filler(CHURN_VALUE, 11);
        let mut steps = Vec::with_capacity(CHURN_STEPS as usize);
        let mut x = mix(7);
        let done = AtomicBool::new(false);
        let (total, peak) = std::thread::scope(|s| {
            // Disk use is sampled until the final persist (a layer writes in the background).
            let sampler = dir.map(|dir| {
                let done = &done;
                s.spawn(move || {
                    let mut peak = 0;
                    while !done.load(Ordering::Acquire) {
                        peak = peak.max(disk_bytes(dir));
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    peak.max(disk_bytes(dir))
                })
            });
            let start = Instant::now();
            for i in 0..CHURN_STEPS {
                let step = Instant::now();
                db.insert::<Churn>(&i, &value).expect("insert");
                if i >= CHURN_WINDOW {
                    db.remove::<Churn>(&(i - CHURN_WINDOW)).expect("remove");
                }
                if i % 4 == 3 {
                    x = mix(x);
                    let oldest = (i + 1).saturating_sub(CHURN_WINDOW);
                    let key = oldest + x % (i + 1 - oldest);
                    db.insert::<Churn>(&key, &value).expect("overwrite");
                }
                steps.push(step.elapsed());
            }
            rt.block_on(db.persist::<Churn>()).expect("persist");
            let total = start.elapsed();
            done.store(true, Ordering::Release);
            (total, sampler.map(|sampler| sampler.join().expect("sampler")))
        });
        assert_eq!(db.iter::<Churn>().count() as u64, CHURN_WINDOW, "the window's rows");
        drop(db);

        let max = steps.iter().copied().max().unwrap_or_default();
        let compactions = dir.and_then(tndb_compactions).map_or_else(
            || Cell::new("compactions", Better::Unranked, "-".to_string()),
            |n| Cell::count("compactions", n),
        );
        vec![
            Cell::rate("K steps/s", CHURN_STEPS as f64 / total.as_secs_f64() / 1e3),
            Cell::us("step p50 us", quantile(&mut steps, 0.5)),
            Cell::us("step p99 us", quantile(&mut steps, 0.99)),
            Cell::ms("step max ms", max),
            Cell::mb("peak disk MB", peak),
            Cell::mb("disk MB after close", dir.map(disk_bytes)),
            compactions,
        ]
    }
}

/// A long-lived table under churn, tndb with its compaction off and on (default config: at 64 MiB
/// of log and half its puts dead, copied at 64 MiB/s), raw and behind the cache-mode layer, with
/// MDBX (which reuses freed pages) for scale.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tn-storage workload_compaction -- --ignored --nocapture
/// --test-threads 1`.
#[test]
#[ignore = "on-demand production-workload benchmark; run with --ignored --nocapture --test-threads 1"]
fn workload_compaction() {
    let rt = Runtime::new().expect("tokio runtime");
    let tmp = tempfile::tempdir().expect("temp dir");
    let mut w = ChurnWorkload;
    let off = CompactionConfig { auto_min_bytes: None, ..Default::default() };
    let on = CompactionConfig::default();
    let tndb = |name: &str, config| {
        let dir = tmp.path().join(name);
        (TnDatabase::open_with_compaction(&dir, config).expect("open tndb"), dir)
    };
    let mut cols = Vec::new();
    for (name, config) in [("TnDb-nocompact", off), ("TnDb", on)] {
        let (db, dir) = tndb(name, config);
        cols.push(column(&rt, &mut w, name.to_string(), db, Some(&dir)));
    }
    for (name, config) in [("Layered-cache<TnDb-nocompact>", off), ("Layered-cache<TnDb>", on)] {
        let (db, dir) = tndb(name, config);
        let db = LayeredDatabase::open(db, false);
        cols.push(column(&rt, &mut w, name.to_string(), db, Some(&dir)));
    }
    #[cfg(feature = "reth-libmdbx")]
    {
        let dir = tmp.path().join("mdbx_prod");
        let db = open_mdbx_prod(&dir, 4, PROD_CACHE_MAX, PROD_CACHE_GROWTH);
        cols.push(column(&rt, &mut w, "MDBX-prod".to_string(), db, Some(&dir)));
        let dir = tmp.path().join("mdbx_prod_layered");
        let db = LayeredDatabase::open(
            open_mdbx_prod(&dir, 4, PROD_CACHE_MAX, PROD_CACHE_GROWTH),
            false,
        );
        cols.push(column(&rt, &mut w, "Layered-cache<MDBX-prod>".to_string(), db, Some(&dir)));
    }
    print_table(
        &format!(
            "compaction: {CHURN_STEPS} steps over a {CHURN_WINDOW}-row window of {} KB values \
             (live data {} MiB)",
            CHURN_VALUE / 1024,
            (CHURN_WINDOW * CHURN_VALUE as u64) >> 20
        ),
        "step = insert + remove (+ an overwrite every 4th), each its own commit; the layered \
         steps are not durable until the final persist; step max includes a compaction's switch",
        &cols,
    );
}
