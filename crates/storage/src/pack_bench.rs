//! On-demand benchmark for consensus pack files: **background thread vs direct IO**, on both file
//! backends.
//!
//! Pack files are append-only archive files (BCS + per-record ZStd) with position/digest indexes.
//! The production [`ConsensusPack`] drives them from **one background thread per pack** — every
//! public `async fn` sends a message over a channel and awaits a `oneshot` reply. Its twin
//! [`ConsensusPackDirect`] holds the same state behind an `Arc<Mutex<..>>` and does every call
//! **inline on the caller's task** (no channel, no thread). This harness runs the same battery on
//! both front-ends and both selectable file backends and prints one side-by-side table, so the
//! `thr − dir` delta per row is the background-thread/channel overhead.
//!
//! Run it on demand (it is `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test -p tn-storage pack_file_bench -- --ignored --nocapture --test-threads 1
//! ```
//!
//! ## What it models (columns = backend × transport × output width)
//!
//! The knob that drives pack cost in production is how many batches each consensus output carries —
//! a shallow leader vs a deep sub-DAG. So the report has a **column per `{backend}-{transport}
//! x{width}`** (`buf`/`mmap` × `thr`/`dir` × narrow/typical/wide batches per output), with `thr` and
//! `dir` adjacent per `{backend,width}` so their delta reads off directly. Each column writes
//! [`NUM_OUTPUTS`] chained outputs built with [`make_wide_test_output`], whose batches are genuinely
//! distinct across outputs (random transactions). All of the pack's files switch backend (data file
//! + position index + digest index), so the `mmap` columns are a fully-`msync` pack.
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
//!   (`header_by_digest` / `batch_by_digest`), and the reverse-scan metadata reads
//!   (`read_last_committed` / `latest_header`).
//! - **streams** — the partial catch-up prefix (`prefix_stream` = `consensus_output_end` + read
//!   `[0, end)`), the full state-sync serve (`full_stream` = sequential read to EOF), and the
//!   receive side (`stream_import` = replay a whole epoch into a fresh pack).
//! - **lifecycle** — `reopen_static` opens the finished pack cold (the pack-cache-miss + index-load
//!   cost).
//!
//! ## Caveats (printed with the results too)
//!
//! - Test batches carry **one transaction each** — width (batches/output), not batch body size, is
//!   the lever here. Scale [`NUM_OUTPUTS`] / [`WIDTHS`] up for heavier runs.
//! - Timings are on a `tempfile` dir — they reflect that filesystem, not a specific production disk.
//! - `thr` timings include the background thread's disk IO (the async API awaits its ack); `dir`
//!   runs the same IO inline behind an uncontended `parking_lot` lock (tens of ns), so `thr − dir`
//!   isolates the thread/channel round-trip.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use tn_reth::RethChainSpec;
use tn_test_utils::CommitteeFixture;
use tn_types::{
    test_genesis, AuthorityIdentifier, Batch, BlockHash, Committee, ConsensusHeader,
    ConsensusHeaderDigest, ConsensusOutput, Epoch, EpochRecord, Round,
};
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::{
    archive::pack::FileBackend,
    consensus_pack::{test::make_wide_test_output, ConsensusPack, PackError, DATA_NAME},
    consensus_pack_direct::ConsensusPackDirect,
    mem_db::MemDatabase,
};

/// The subset of the pack API the battery exercises, so the whole battery can run generically over
/// the threaded [`ConsensusPack`] and the direct [`ConsensusPackDirect`]. Both impls forward to
/// their identical inherent methods (static dispatch — no `dyn`).
trait BenchPack: Sized {
    fn open_append_with_backend(
        path: &Path,
        previous_epoch: EpochRecord,
        committee: Committee,
        backend: FileBackend,
    ) -> Result<Self, PackError>;

    fn open_static_with_backend(
        path: &Path,
        epoch: Epoch,
        backend: FileBackend,
    ) -> Result<Self, PackError>;

    #[allow(clippy::too_many_arguments)]
    async fn stream_import_with_backend<R: AsyncRead + Unpin>(
        path: &Path,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        final_consensus_number: u64,
        timeout: Duration,
        backend: FileBackend,
    ) -> Result<Self, PackError>;

    async fn save_consensus_output(&mut self, output: ConsensusOutput) -> Result<u64, PackError>;
    async fn persist(&mut self) -> Result<(), PackError>;
    async fn consensus_header_by_number(
        &mut self,
        number: u64,
    ) -> Result<ConsensusHeader, PackError>;
    async fn get_consensus_output(&mut self, number: u64) -> Result<ConsensusOutput, PackError>;
    async fn get_consensus_output_bytes(&mut self, number: u64) -> Result<Vec<u8>, PackError>;
    async fn consensus_header_by_digest(
        &mut self,
        digest: ConsensusHeaderDigest,
    ) -> Option<ConsensusHeader>;
    async fn batch(&mut self, digest: BlockHash) -> Option<Batch>;
    async fn read_last_committed(
        &mut self,
    ) -> Result<HashMap<AuthorityIdentifier, Round>, PackError>;
    async fn latest_consensus_header(&mut self) -> Result<Option<ConsensusHeader>, PackError>;
    async fn consensus_output_end(&mut self, number: u64) -> Result<u64, PackError>;
}

/// Forward every `BenchPack` method to the inherent method of the same name (UFCS avoids resolving
/// back to the trait method).
macro_rules! impl_bench_pack {
    ($ty:ty) => {
        impl BenchPack for $ty {
            fn open_append_with_backend(
                path: &Path,
                previous_epoch: EpochRecord,
                committee: Committee,
                backend: FileBackend,
            ) -> Result<Self, PackError> {
                <$ty>::open_append_with_backend(path, previous_epoch, committee, backend)
            }
            fn open_static_with_backend(
                path: &Path,
                epoch: Epoch,
                backend: FileBackend,
            ) -> Result<Self, PackError> {
                <$ty>::open_static_with_backend(path, epoch, backend)
            }
            async fn stream_import_with_backend<R: AsyncRead + Unpin>(
                path: &Path,
                stream: R,
                epoch: Epoch,
                previous_epoch: &EpochRecord,
                final_consensus_number: u64,
                timeout: Duration,
                backend: FileBackend,
            ) -> Result<Self, PackError> {
                <$ty>::stream_import_with_backend(
                    path,
                    stream,
                    epoch,
                    previous_epoch,
                    final_consensus_number,
                    timeout,
                    backend,
                )
                .await
            }
            async fn save_consensus_output(
                &mut self,
                output: ConsensusOutput,
            ) -> Result<u64, PackError> {
                <$ty>::save_consensus_output(self, output).await
            }
            async fn persist(&mut self) -> Result<(), PackError> {
                <$ty>::persist(self).await
            }
            async fn consensus_header_by_number(
                &mut self,
                number: u64,
            ) -> Result<ConsensusHeader, PackError> {
                <$ty>::consensus_header_by_number(self, number).await
            }
            async fn get_consensus_output(
                &mut self,
                number: u64,
            ) -> Result<ConsensusOutput, PackError> {
                <$ty>::get_consensus_output(self, number).await
            }
            async fn get_consensus_output_bytes(
                &mut self,
                number: u64,
            ) -> Result<Vec<u8>, PackError> {
                <$ty>::get_consensus_output_bytes(self, number).await
            }
            async fn consensus_header_by_digest(
                &mut self,
                digest: ConsensusHeaderDigest,
            ) -> Option<ConsensusHeader> {
                <$ty>::consensus_header_by_digest(self, digest).await
            }
            async fn batch(&mut self, digest: BlockHash) -> Option<Batch> {
                <$ty>::batch(self, digest).await
            }
            async fn read_last_committed(
                &mut self,
            ) -> Result<HashMap<AuthorityIdentifier, Round>, PackError> {
                <$ty>::read_last_committed(self).await
            }
            async fn latest_consensus_header(
                &mut self,
            ) -> Result<Option<ConsensusHeader>, PackError> {
                <$ty>::latest_consensus_header(self).await
            }
            async fn consensus_output_end(&mut self, number: u64) -> Result<u64, PackError> {
                <$ty>::consensus_output_end(self, number).await
            }
        }
    };
}

impl_bench_pack!(ConsensusPack);
impl_bench_pack!(ConsensusPackDirect);

// ---- workload sizing (representative & scalable; this is an on-demand perf test) ----

/// Consensus outputs written per column. A few hundred keeps the on-demand run quick while giving
/// stable per-op signal; scale up for heavier samples.
const NUM_OUTPUTS: u64 = 200;

/// Output shapes: `(label, batches-per-output)`. The single knob is how many batches each consensus
/// output carries (shallow leader vs deep sub-DAG), which drives pack read/write/stream cost.
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
async fn populate<P: BenchPack>(
    fx: &Fixtures,
    backend: FileBackend,
    outputs: &[ConsensusOutput],
) -> (P, TempDir) {
    let dir = TempDir::with_prefix("pack_bench_read").expect("temp dir");
    let mut pack = P::open_append_with_backend(
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
async fn bench_save_seq<P: BenchPack>(
    fx: &Fixtures,
    backend: FileBackend,
    outputs: &[ConsensusOutput],
) -> Duration {
    let dir = TempDir::with_prefix("pack_bench_save").expect("temp dir");
    let mut pack = P::open_append_with_backend(
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
async fn bench_save_durable<P: BenchPack>(
    fx: &Fixtures,
    backend: FileBackend,
    outputs: &[ConsensusOutput],
) -> Duration {
    let dir = TempDir::with_prefix("pack_bench_durable").expect("temp dir");
    let mut pack = P::open_append_with_backend(
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
async fn bench_persist<P: BenchPack>(pack: &mut P) -> Duration {
    let start = Instant::now();
    pack.persist().await.expect("persist");
    start.elapsed()
}

/// Random-access read of every header by number (index lookup + fetch + decode).
async fn bench_header_by_number<P: BenchPack>(pack: &mut P) -> Duration {
    let start = Instant::now();
    for number in 1..=NUM_OUTPUTS {
        pack.consensus_header_by_number(number).await.expect("header by number");
    }
    start.elapsed()
}

/// Full decode of every output (range read + decode incl. all batches).
async fn bench_full_output<P: BenchPack>(pack: &mut P) -> Duration {
    let start = Instant::now();
    for number in 1..=NUM_OUTPUTS {
        pack.get_consensus_output(number).await.expect("full output");
    }
    start.elapsed()
}

/// Raw pack-file bytes for every output (the serve-to-peer path: range read, no decode).
async fn bench_output_bytes<P: BenchPack>(pack: &mut P) -> Duration {
    let start = Instant::now();
    for number in 1..=NUM_OUTPUTS {
        pack.get_consensus_output_bytes(number).await.expect("output bytes");
    }
    start.elapsed()
}

/// Digest-keyed header lookups over every header digest (the consensus digest index).
async fn bench_header_by_digest<P: BenchPack>(
    pack: &mut P,
    digests: &[ConsensusHeaderDigest],
) -> Duration {
    let start = Instant::now();
    for digest in digests {
        pack.consensus_header_by_digest(*digest).await.expect("header by digest");
    }
    start.elapsed()
}

/// Digest-keyed batch lookups over a bounded sample (the batch digest index).
async fn bench_batch_by_digest<P: BenchPack>(pack: &mut P, digests: &[BlockHash]) -> Duration {
    let start = Instant::now();
    for digest in digests {
        pack.batch(*digest).await.expect("batch by digest");
    }
    start.elapsed()
}

/// The reverse-scan behind `read_last_committed`.
async fn bench_read_last_committed<P: BenchPack>(pack: &mut P) -> Duration {
    let start = Instant::now();
    pack.read_last_committed().await.expect("read last committed");
    start.elapsed()
}

/// Single index-tail read of the latest header.
async fn bench_latest_header<P: BenchPack>(pack: &mut P) -> Duration {
    let start = Instant::now();
    pack.latest_consensus_header().await.expect("latest header");
    start.elapsed()
}

/// Partial catch-up serve: resolve the prefix byte offset for the mid output, then read `[0, end)`
/// of the data file (mirrors `get_partial_epoch_stream`).
async fn bench_prefix_stream<P: BenchPack>(pack: &mut P, data_path: &Path) -> Duration {
    let mid = NUM_OUTPUTS / 2;
    let start = Instant::now();
    let end = pack.consensus_output_end(mid).await.expect("output end");
    let mut file = tokio::fs::File::open(data_path).await.expect("open data file");
    let mut buf = vec![0u8; end as usize];
    file.read_exact(&mut buf).await.expect("read prefix");
    start.elapsed()
}

/// Full state-sync serve: sequential read of the epoch's real data (bounded to `end`, so the mmap
/// backend's transient capacity padding is not counted). Not pack-typed (a plain file read).
async fn bench_full_stream(data_path: &Path, end: u64) -> Duration {
    let start = Instant::now();
    let file = tokio::fs::File::open(data_path).await.expect("open data file");
    let mut sink = Vec::new();
    file.take(end).read_to_end(&mut sink).await.expect("read all");
    start.elapsed()
}

/// Receive side of state sync: replay a whole epoch's data into a fresh pack. The source is bounded
/// to `end` (the real data length) so an open mmap pack's capacity padding is not streamed.
async fn bench_stream_import<P: BenchPack>(
    fx: &Fixtures,
    backend: FileBackend,
    data_path: &Path,
    end: u64,
) -> Duration {
    let dir = TempDir::with_prefix("pack_bench_import").expect("temp dir");
    let stream = tokio::fs::File::open(data_path).await.expect("open data file").take(end);
    let start = Instant::now();
    let mut pack = P::stream_import_with_backend(
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
async fn bench_reopen_static<P: BenchPack>(path: &Path, epoch: Epoch, backend: FileBackend) -> Duration {
    let start = Instant::now();
    let mut pack = P::open_static_with_backend(path, epoch, backend).expect("open static");
    pack.latest_consensus_header().await.expect("latest after reopen");
    start.elapsed()
}

/// One report column: the ordered rows plus the on-disk size of the populated pack.
struct Column {
    rows: Vec<(&'static str, Duration)>,
    data_len: u64,
}

/// Run the whole battery for one output width, one file `backend`, and one pack transport `P`.
async fn run_battery<P: BenchPack>(fx: &Fixtures, backend: FileBackend, width: usize) -> Column {
    let outputs = build_outputs(fx, width);
    let header_digests: Vec<ConsensusHeaderDigest> =
        outputs.iter().map(|o| o.consensus_header_hash()).collect();
    let batch_digests: Vec<BlockHash> =
        outputs.iter().flat_map(|o| o.batch_digests().iter().copied()).take(BATCH_SAMPLE).collect();

    // Writes each build (and drop) their own pack.
    let save_seq = bench_save_seq::<P>(fx, backend, &outputs).await;
    let save_durable = bench_save_durable::<P>(fx, backend, &outputs).await;

    // One populated pack shared by the read/stream battery.
    let (mut pack, dir) = populate::<P>(fx, backend, &outputs).await;
    let data_path = fx.data_path(dir.path());

    // Bulk-persist first so the un-persisted saves are flushed for the direct-file stream reads.
    let persist = bench_persist(&mut pack).await;
    // Real data length (past the last output). For mmap this excludes the open pack's capacity
    // padding, so it is comparable to the buffered backend and bounds the stream reads below.
    let data_len =
        pack.consensus_output_end(NUM_OUTPUTS).await.expect("output end for data length");

    let header_by_number = bench_header_by_number(&mut pack).await;
    let full_output = bench_full_output(&mut pack).await;
    let output_bytes = bench_output_bytes(&mut pack).await;
    let header_by_digest = bench_header_by_digest(&mut pack, &header_digests).await;
    let batch_by_digest = bench_batch_by_digest(&mut pack, &batch_digests).await;
    let read_last_committed = bench_read_last_committed(&mut pack).await;
    let latest_header = bench_latest_header(&mut pack).await;
    let prefix_stream = bench_prefix_stream(&mut pack, &data_path).await;
    let full_stream = bench_full_stream(&data_path, data_len).await;
    let stream_import = bench_stream_import::<P>(fx, backend, &data_path, data_len).await;

    // Reopen read-only after the writer is gone (index load / cache-miss open).
    drop(pack);
    let reopen_static = bench_reopen_static::<P>(dir.path(), fx.committee.epoch(), backend).await;
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

/// Collects one column per (backend, transport, width) and prints an aligned table (ms).
struct Report {
    /// Row labels, captured from the first column and reused for alignment.
    order: Vec<&'static str>,
    /// `(column label, column)`.
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
        // Columns are pushed 4 per width block (buf-thr, buf-dir, mmap-thr, mmap-dir); a vertical
        // rail after every block and a rule between row groups make a single row easy to follow
        // across all 12 columns.
        const GROUP: usize = 4;
        let label_w =
            self.order.iter().map(|s| s.len()).max().unwrap_or(0).max("bytes/output".len());
        let cell_w = 14usize;
        let n = self.columns.len();
        // Total printed width, including the 2-char " │" rails between blocks.
        let rails = n.saturating_sub(1) / GROUP;
        let total_w = label_w + n * (cell_w + 1) + rails * 2;
        let rule = "─".repeat(total_w);

        /// Print one row: left label then the cells, with a `│` rail between width-blocks.
        fn print_row(label: &str, cells: &[String], label_w: usize, cell_w: usize, group: usize) {
            print!("{label:<label_w$}");
            for (i, cell) in cells.iter().enumerate() {
                print!(" {cell:>cell_w$}");
                if (i + 1) % group == 0 && i + 1 < cells.len() {
                    print!(" │");
                }
            }
            println!();
        }

        // Row labels after which to draw a horizontal separator (the write / persist / read / stream
        // / lifecycle groups from the module docs).
        const GROUP_SEP_AFTER: &[&str] =
            &["save_durable", "persist bulk", "latest_header", "stream_import"];

        println!("\n=== consensus pack-file benchmark: background thread (thr) vs direct IO (dir), buffered (fsync) vs mmap (msync) (ms) ===");
        println!("legend: columns are {{backend}}-{{transport}} x{{batches/output}}; thr = background thread + channel, dir = inline &mut calls (no thread, no lock — the pure direct-IO baseline); thr - dir per row is the thread/channel overhead. Vertical rails group the 4 columns of each width; horizontal rules group the rows. save_durable adds a persist() barrier per output; test batches carry 1 tx each.");

        // header + underline rule
        let headers: Vec<String> = self.columns.iter().map(|(name, _)| name.clone()).collect();
        print_row("benchmark", &headers, label_w, cell_w, GROUP);
        println!("{rule}");

        // timed rows, with a rule between logical groups
        for (row, &label) in self.order.iter().enumerate() {
            let cells: Vec<String> = self
                .columns
                .iter()
                .map(|(_, col)| format!("{:.2}", col.rows[row].1.as_secs_f64() * 1000.0))
                .collect();
            print_row(label, &cells, label_w, cell_w, GROUP);
            if GROUP_SEP_AFTER.contains(&label) {
                println!("{rule}");
            }
        }

        // observational footer: pack size per column
        let mib: Vec<String> = self
            .columns
            .iter()
            .map(|(_, col)| format!("{:.2}", col.data_len as f64 / (1024.0 * 1024.0)))
            .collect();
        print_row("data MiB", &mib, label_w, cell_w, GROUP);
        let bytes_per_output: Vec<String> =
            self.columns.iter().map(|(_, col)| (col.data_len / NUM_OUTPUTS).to_string()).collect();
        print_row("bytes/output", &bytes_per_output, label_w, cell_w, GROUP);
        println!("\n({NUM_OUTPUTS} outputs per column)\n");
    }
}

/// Compare the background-thread [`ConsensusPack`] against the direct [`ConsensusPackDirect`] across
/// production usage patterns and both file backends.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test -p tn-storage pack_file_bench -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand pack-file thread-vs-direct benchmark; run with --ignored --nocapture --test-threads 1"]
fn pack_file_bench() {
    // Mirror `db_bench`: a plain test driving the async pack API on a tokio runtime.
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    runtime.block_on(async {
        let fx = Fixtures::new();
        let mut report = Report::new();
        // Width-outer, then backend, then transport — so `thr` and `dir` sit adjacent per
        // {backend, width} for an at-a-glance overhead read.
        for &(_, width) in WIDTHS {
            for (backend, blabel) in [(FileBackend::Buffered, "buf"), (FileBackend::Mmap, "mmap")] {
                let thr = format!("{blabel}-thr x{width}");
                println!("  running battery for {thr} ...");
                let col = run_battery::<ConsensusPack>(&fx, backend, width).await;
                report.push(&thr, col);

                let dir = format!("{blabel}-dir x{width}");
                println!("  running battery for {dir} ...");
                let col = run_battery::<ConsensusPackDirect>(&fx, backend, width).await;
                report.push(&dir, col);
            }
        }
        report.print();
    });
}
