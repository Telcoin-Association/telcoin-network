//! On-demand benchmark for consensus pack files ([`ConsensusPack`]): buffered vs mmap file backend.
//!
//! The sibling [`crate::db_bench`] compares the KV [`Database`](tn_types::Database) backends. Pack
//! files are a different storage path — append-only archive files (BCS + per-record ZStd) with
//! position/digest indexes, driven by one background thread per pack. This harness runs the same
//! battery of production read/write/stream patterns on both selectable file backends — the buffered
//! [`DataFile`](crate::archive::data_file::DataFile) (`fsync` barrier) and the memory-mapped
//! [`MmapDataFile`](crate::archive::data_file_mmap::MmapDataFile) (`msync` barrier) — and prints
//! one side-by-side table.
//!
//! Run it on demand (it is `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test -p tn-storage pack_file_bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! ## What it models (columns = backend × output width)
//!
//! The knob that drives pack cost in production is how many batches each consensus output carries —
//! a shallow leader vs a deep sub-DAG. So the report has a **column per `{backend} x{width}`**
//! (`buf`/`mmap` × narrow/typical/wide batches per output), with the two backends adjacent per
//! width; every row is the same production pattern, so you can read off both the backend delta and
//! how each pattern scales with output size. Each column writes [`NUM_OUTPUTS`] chained outputs
//! built with [`make_wide_test_output`], whose batches are genuinely distinct across outputs
//! (random transactions), matching production where each output commits its own batches. Only the
//! data file and position index switch backend; the digest index stays buffered+`fsync` in both.
//!
//! The rows, grouped, capture the real call sites traced through `consensus_pack.rs` /
//! `consensus.rs` / `state-sync/src/lib.rs`:
//!
//! - **writes** — `save_seq` is the executor hot path (append-only, no barrier); `save_durable`
//!   adds the async `persist()` barrier after every output (the state-sync `save_consensus` +
//!   persist pattern). Their delta is the per-output durability cost.
//! - **persist** — one bulk `persist()` flushing many un-persisted saves (the periodic-flush cost).
//! - **reads** — random access by number (`header_by_number`), full decode (`full_output`), the raw
//!   serve-to-peer bytes (`output_bytes`), digest-keyed header and batch lookups
//!   (`header_by_digest` / `batch_by_digest`, the `HdxIndex` path), and the reverse-scan metadata
//!   reads (`read_last_committed` / `latest_header`).
//! - **streams** — the partial catch-up prefix (`prefix_stream` = `consensus_output_end` + read
//!   `[0, end)`), the full state-sync serve (`full_stream` = sequential read to EOF), and the
//!   receive side (`stream_import` = replay a whole epoch into a fresh pack).
//! - **lifecycle** — `reopen_static` opens the finished pack cold (the pack-cache-miss + index-load
//!   cost).
//!
//! ## Caveats (printed with the results too)
//!
//! - Test batches carry **one transaction each** — this is the "representative & scalable" sizing:
//!   width (batches/output), not batch body size, is the lever here. Production batches can be far
//!   larger; scale [`NUM_OUTPUTS`] / [`WIDTHS`] up for heavier runs.
//! - `save`/`persist` timings include the background thread's disk IO (the async API awaits its
//!   ack), on a `tempfile` dir — so they reflect that filesystem, not a specific production disk.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tn_reth::RethChainSpec;
use tn_test_utils::CommitteeFixture;
use tn_types::{
    test_genesis, BlockHash, Committee, ConsensusHeader, ConsensusHeaderDigest, ConsensusOutput,
    Epoch, EpochRecord,
};
use tokio::io::AsyncReadExt as _;

use crate::{
    archive::pack::FileBackend,
    consensus_pack::{test::make_wide_test_output, ConsensusPack, DATA_NAME},
    mem_db::MemDatabase,
};

// ---- workload sizing (representative & scalable; this is an on-demand perf test) ----

/// Consensus outputs written per column. A few hundred keeps the on-demand run quick while giving
/// stable per-op signal; scale up for heavier samples.
const NUM_OUTPUTS: u64 = 200;

/// Output shapes: `(label, batches-per-output)`. These are the report columns — the single knob is
/// how many batches each consensus output carries (shallow leader vs deep sub-DAG), which is what
/// drives pack read/write/stream cost.
const WIDTHS: &[(&str, usize)] = &[("narrow x4", 4), ("typical x16", 16), ("wide x64", 64)];

/// Bounded sample for the digest-keyed batch lookups — there are `width * NUM_OUTPUTS` batches, so
/// a fixed sample keeps `batch_by_digest` comparable to the `header_by_digest` row across widths.
const BATCH_SAMPLE: usize = 512;

/// Generous ceiling for the state-sync `stream_import` path.
const IMPORT_TIMEOUT: Duration = Duration::from_secs(30);

/// Cheap-to-clone fixtures shared across every column.
struct Fixtures {
    committee: Committee,
    chain: Arc<RethChainSpec>,
    previous_epoch: EpochRecord,
}

impl Fixtures {
    fn new() -> Self {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        // Default epoch-0 record: keeps `start_consensus_number == 1`, so outputs number from 1.
        let previous_epoch = EpochRecord {
            epoch: 0,
            committee: committee.bls_keys().iter().copied().collect(),
            next_committee: committee.bls_keys().iter().copied().collect(),
            ..Default::default()
        };
        Self { committee, chain, previous_epoch }
    }

    /// On-disk data file for a pack opened at `path` for this epoch (`<path>/epoch-<e>/data`).
    fn data_path(&self, path: &Path) -> PathBuf {
        path.join(format!("epoch-{}", self.committee.epoch())).join(DATA_NAME)
    }
}

/// Build `NUM_OUTPUTS` chained outputs of the given width (untimed setup). Numbering and parent
/// chaining mirror `test_consensus_pack`; the batches are distinct per output (random txs).
fn build_outputs(fx: &Fixtures, width: usize) -> Vec<ConsensusOutput> {
    let mut outputs = Vec::with_capacity(NUM_OUTPUTS as usize);
    let mut parent = ConsensusHeader::default().digest();
    for number in 1..=NUM_OUTPUTS {
        let output = make_wide_test_output(&fx.committee, fx.chain.clone(), number, parent, width);
        parent = output.consensus_header_hash();
        outputs.push(output);
    }
    outputs
}

/// Open a fresh append pack and save every output, un-persisted (setup for the read/stream
/// battery).
async fn populate(
    fx: &Fixtures,
    backend: FileBackend,
    outputs: &[ConsensusOutput],
) -> (ConsensusPack, TempDir) {
    let dir = TempDir::with_prefix("pack_bench_read").expect("temp dir");
    let pack = ConsensusPack::open_append_with_backend(
        dir.path(),
        fx.previous_epoch.clone(),
        fx.committee.clone(),
        backend,
    )
    .expect("open pack");
    for output in outputs {
        pack.save_consensus_output(output.clone()).await.expect("save");
    }
    (pack, dir)
}

// ---- the battery: each returns the timed duration for its measured region ----

/// Save every output append-only, no durability barrier (the executor hot-path write).
async fn bench_save_seq(
    fx: &Fixtures,
    backend: FileBackend,
    outputs: &[ConsensusOutput],
) -> Duration {
    let dir = TempDir::with_prefix("pack_bench_save").expect("temp dir");
    let pack = ConsensusPack::open_append_with_backend(
        dir.path(),
        fx.previous_epoch.clone(),
        fx.committee.clone(),
        backend,
    )
    .expect("open pack");
    let start = Instant::now();
    for output in outputs {
        pack.save_consensus_output(output.clone()).await.expect("save");
    }
    let elapsed = start.elapsed();
    pack.persist().await.expect("persist"); // settle before drop (untimed)
    elapsed
}

/// Save every output, each followed by a `persist()` barrier (state-sync save + persist). The delta
/// vs [`bench_save_seq`] is the per-output durability cost.
async fn bench_save_durable(
    fx: &Fixtures,
    backend: FileBackend,
    outputs: &[ConsensusOutput],
) -> Duration {
    let dir = TempDir::with_prefix("pack_bench_durable").expect("temp dir");
    let pack = ConsensusPack::open_append_with_backend(
        dir.path(),
        fx.previous_epoch.clone(),
        fx.committee.clone(),
        backend,
    )
    .expect("open pack");
    let start = Instant::now();
    for output in outputs {
        pack.save_consensus_output(output.clone()).await.expect("save");
        pack.persist().await.expect("persist");
    }
    start.elapsed()
}

/// One bulk `persist()` flushing all the un-persisted saves left by [`populate`].
async fn bench_persist(pack: &ConsensusPack) -> Duration {
    let start = Instant::now();
    pack.persist().await.expect("persist");
    start.elapsed()
}

/// Random-access read of every header by number (index lookup + fetch + decode).
async fn bench_header_by_number(pack: &ConsensusPack) -> Duration {
    let start = Instant::now();
    for number in 1..=NUM_OUTPUTS {
        pack.consensus_header_by_number(number).await.expect("header by number");
    }
    start.elapsed()
}

/// Full decode of every output (range read + decode incl. all batches).
async fn bench_full_output(pack: &ConsensusPack) -> Duration {
    let start = Instant::now();
    for number in 1..=NUM_OUTPUTS {
        pack.get_consensus_output(number).await.expect("full output");
    }
    start.elapsed()
}

/// Raw pack-file bytes for every output (the serve-to-peer path: range read, no decode).
async fn bench_output_bytes(pack: &ConsensusPack) -> Duration {
    let start = Instant::now();
    for number in 1..=NUM_OUTPUTS {
        pack.get_consensus_output_bytes(number).await.expect("output bytes");
    }
    start.elapsed()
}

/// Digest-keyed header lookups over every header digest (the consensus `HdxIndex`).
async fn bench_header_by_digest(
    pack: &ConsensusPack,
    digests: &[ConsensusHeaderDigest],
) -> Duration {
    let start = Instant::now();
    for digest in digests {
        pack.consensus_header_by_digest(*digest).await.expect("header by digest");
    }
    start.elapsed()
}

/// Digest-keyed batch lookups over a bounded sample (the batch `HdxIndex`).
async fn bench_batch_by_digest(pack: &ConsensusPack, digests: &[BlockHash]) -> Duration {
    let start = Instant::now();
    for digest in digests {
        pack.batch(*digest).await.expect("batch by digest");
    }
    start.elapsed()
}

/// The `rev_iter(50)` reverse-scan behind `read_last_committed`.
async fn bench_read_last_committed(pack: &ConsensusPack) -> Duration {
    let start = Instant::now();
    pack.read_last_committed().await.expect("read last committed");
    start.elapsed()
}

/// Single index-tail read of the latest header.
async fn bench_latest_header(pack: &ConsensusPack) -> Duration {
    let start = Instant::now();
    pack.latest_consensus_header().await.expect("latest header");
    start.elapsed()
}

/// Partial catch-up serve: resolve the prefix byte offset for the mid output, then read `[0, end)`
/// of the data file (mirrors `get_partial_epoch_stream`).
async fn bench_prefix_stream(pack: &ConsensusPack, data_path: &Path) -> Duration {
    let mid = NUM_OUTPUTS / 2;
    let start = Instant::now();
    let end = pack.consensus_output_end(mid).await.expect("output end");
    let mut file = tokio::fs::File::open(data_path).await.expect("open data file");
    let mut buf = vec![0u8; end as usize];
    file.read_exact(&mut buf).await.expect("read prefix");
    start.elapsed()
}

/// Full state-sync serve: sequential read of the epoch's real data (bounded to `end`, so the mmap
/// backend's transient capacity padding is not counted).
async fn bench_full_stream(data_path: &Path, end: u64) -> Duration {
    let start = Instant::now();
    let file = tokio::fs::File::open(data_path).await.expect("open data file");
    let mut sink = Vec::new();
    file.take(end).read_to_end(&mut sink).await.expect("read all");
    start.elapsed()
}

/// Receive side of state sync: replay a whole epoch's data into a fresh pack. The source is bounded
/// to `end` (the real data length) so an open mmap pack's capacity padding is not streamed.
async fn bench_stream_import(
    fx: &Fixtures,
    backend: FileBackend,
    data_path: &Path,
    end: u64,
) -> Duration {
    let dir = TempDir::with_prefix("pack_bench_import").expect("temp dir");
    let stream = tokio::fs::File::open(data_path).await.expect("open data file").take(end);
    let start = Instant::now();
    let pack = ConsensusPack::stream_import_with_backend(
        dir.path(),
        stream,
        fx.committee.epoch(),
        &fx.previous_epoch,
        NUM_OUTPUTS,
        IMPORT_TIMEOUT,
        backend,
    )
    .await
    .expect("stream import");
    let elapsed = start.elapsed();
    // stream_import drains the whole stream before returning, so the epoch is already present.
    pack.get_consensus_output(NUM_OUTPUTS).await.expect("last output present after import");
    elapsed
}

/// Cold-open the finished pack read-only, touching the index (the pack-cache-miss open cost).
async fn bench_reopen_static(path: &Path, epoch: Epoch, backend: FileBackend) -> Duration {
    let start = Instant::now();
    let pack = ConsensusPack::open_static_with_backend(path, epoch, backend).expect("open static");
    pack.latest_consensus_header().await.expect("latest after reopen");
    start.elapsed()
}

/// One report column: the ordered rows plus the on-disk size of the populated pack.
struct Column {
    rows: Vec<(&'static str, Duration)>,
    data_len: u64,
}

/// Run the whole battery for one output width on one file `backend` and collect its column.
async fn run_battery(fx: &Fixtures, backend: FileBackend, width: usize) -> Column {
    let outputs = build_outputs(fx, width);
    let header_digests: Vec<ConsensusHeaderDigest> =
        outputs.iter().map(|o| o.consensus_header_hash()).collect();
    let batch_digests: Vec<BlockHash> =
        outputs.iter().flat_map(|o| o.batch_digests().iter().copied()).take(BATCH_SAMPLE).collect();

    // Writes each build (and drop) their own pack.
    let save_seq = bench_save_seq(fx, backend, &outputs).await;
    let save_durable = bench_save_durable(fx, backend, &outputs).await;

    // One populated pack shared by the read/stream battery.
    let (pack, dir) = populate(fx, backend, &outputs).await;
    let data_path = fx.data_path(dir.path());

    // Bulk-persist first so the un-persisted saves are flushed for the direct-file stream reads.
    let persist = bench_persist(&pack).await;
    // Real data length (past the last output). For mmap this excludes the open pack's capacity
    // padding, so it is comparable to the buffered backend and bounds the stream reads below.
    let data_len =
        pack.consensus_output_end(NUM_OUTPUTS).await.expect("output end for data length");

    let header_by_number = bench_header_by_number(&pack).await;
    let full_output = bench_full_output(&pack).await;
    let output_bytes = bench_output_bytes(&pack).await;
    let header_by_digest = bench_header_by_digest(&pack, &header_digests).await;
    let batch_by_digest = bench_batch_by_digest(&pack, &batch_digests).await;
    let read_last_committed = bench_read_last_committed(&pack).await;
    let latest_header = bench_latest_header(&pack).await;
    let prefix_stream = bench_prefix_stream(&pack, &data_path).await;
    let full_stream = bench_full_stream(&data_path, data_len).await;
    let stream_import = bench_stream_import(fx, backend, &data_path, data_len).await;

    // Reopen read-only after the writer is gone (index load / cache-miss open).
    drop(pack);
    let reopen_static = bench_reopen_static(dir.path(), fx.committee.epoch(), backend).await;
    drop(dir);

    Column {
        rows: vec![
            ("save_seq", save_seq),
            ("save_durable", save_durable),
            ("persist bulk", persist),
            ("header_by_number", header_by_number),
            ("full_output", full_output),
            ("output_bytes", output_bytes),
            ("header_by_digest", header_by_digest),
            ("batch_by_digest", batch_by_digest),
            ("read_last_committed", read_last_committed),
            ("latest_header", latest_header),
            ("prefix_stream", prefix_stream),
            ("full_stream", full_stream),
            ("stream_import", stream_import),
            ("reopen_static", reopen_static),
        ],
        data_len,
    }
}

/// Collects one column per output width and prints an aligned table (ms; single implementation).
struct Report {
    /// Row labels, captured from the first column and reused for alignment.
    order: Vec<&'static str>,
    /// `(width label, column)`.
    columns: Vec<(String, Column)>,
}

impl Report {
    fn new() -> Self {
        Self { order: Vec::new(), columns: Vec::new() }
    }

    fn push(&mut self, name: &str, col: Column) {
        if self.order.is_empty() {
            self.order = col.rows.iter().map(|(n, _)| *n).collect();
        }
        self.columns.push((name.to_string(), col));
    }

    fn print(&self) {
        let label_w =
            self.order.iter().map(|s| s.len()).max().unwrap_or(0).max("bytes/output".len());
        let cell_w = 14usize;

        println!("\n=== consensus pack-file benchmark: buffered (fsync) vs mmap (msync) (ms) ===");
        println!("legend: columns are {{backend}} x{{batches/output}}; mmap swaps the data file + position index to msync (digest index still fsync in both); save_durable adds a persist() barrier per output; test batches carry 1 tx each (representative, not production-sized).");

        // header
        print!("{:<label_w$}", "benchmark", label_w = label_w);
        for (name, _) in &self.columns {
            print!(" {:>cell_w$}", name, cell_w = cell_w);
        }
        println!();

        // timed rows
        for (row, label) in self.order.iter().enumerate() {
            print!("{:<label_w$}", label, label_w = label_w);
            for (_, col) in &self.columns {
                let ms = col.rows[row].1.as_secs_f64() * 1000.0;
                print!(" {:>cell_w$}", format!("{ms:.2}"), cell_w = cell_w);
            }
            println!();
        }

        // observational footer: pack size per column
        print!("{:<label_w$}", "data MiB", label_w = label_w);
        for (_, col) in &self.columns {
            let mib = col.data_len as f64 / (1024.0 * 1024.0);
            print!(" {:>cell_w$}", format!("{mib:.2}"), cell_w = cell_w);
        }
        println!();
        print!("{:<label_w$}", "bytes/output", label_w = label_w);
        for (_, col) in &self.columns {
            print!(" {:>cell_w$}", col.data_len / NUM_OUTPUTS, cell_w = cell_w);
        }
        println!("\n({NUM_OUTPUTS} outputs per column)\n");
    }
}

/// Observe the single consensus pack-file implementation across production usage patterns.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test -p tn-storage pack_file_bench -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand pack-file observation benchmark; run with --ignored --nocapture --test-threads 1"]
fn pack_file_bench() {
    // Mirror `db_bench`: a plain test driving the async pack API on a tokio runtime.
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    runtime.block_on(async {
        let fx = Fixtures::new();
        let mut report = Report::new();
        // Width-outer, backend-inner, so the two backends sit adjacent per width for easy
        // comparison.
        for &(_, width) in WIDTHS {
            for (backend, blabel) in [(FileBackend::Buffered, "buf"), (FileBackend::Mmap, "mmap")] {
                let label = format!("{blabel} x{width}");
                println!("  running battery for {label} ...");
                let col = run_battery(&fx, backend, width).await;
                report.push(&label, col);
            }
        }
        report.print();
    });
}
