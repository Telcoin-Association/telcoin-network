//! On-demand performance-regression benchmarks for the memory-mapped consensus pack file, across
//! production usage patterns.
//!
//! Pack files are append-only archive files (BCS + per-record ZStd) with position/digest indexes.
//! The production [`ConsensusPack`] shares one pack between its writer and its readers: reads run
//! under a read lock beside saves, and a save takes the write lock only to append and index its
//! already-encoded records. Two benches:
//!
//! - `pack_file_bench` runs a battery of those operations, one at a time on small synthetic
//!   outputs, and prints one column per output width, so runs can be compared over time to catch
//!   regressions.
//! - `consensus_pack_workload` drives a [`ConsensusChain`] with production-shaped outputs, each
//!   saved and made durable as state sync does, while readers query the pack concurrently (see its
//!   section below).
//!
//! Run them on demand (they are `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test --release -p tn-storage pack_file_bench -- --ignored --nocapture --test-threads 1
//! cargo test --release -p tn-storage --lib -- pack_bench::consensus_pack_workload --exact \
//!     --ignored --nocapture --test-threads 1
//! ```
//!
//! ## What it models (columns = output width)
//!
//! The knob that drives pack cost in production is how many batches each consensus output carries —
//! a shallow leader vs a deep sub-DAG. So the report has a **column per `x{width}`** (narrow /
//! typical / wide batches per output). Each column writes [`NUM_OUTPUTS`] chained outputs built
//! with [`make_wide_test_output`], whose batches are genuinely distinct across outputs (random
//! transactions). Every one of the pack's files (data + position index + digest index) is
//! memory-mapped; `persist()` msyncs the data log (the WAL, source of truth), while the derived
//! indexes are written in place and rebuilt from the log on recovery.
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
//! - **lifecycle** — `reopen_static` opens the finished pack (the pack-cache-miss + index-load
//!   cost), its files still in the page cache from being written.
//!
//! ## Caveats (printed with the results too)
//!
//! - Test batches carry **one transaction each** — width (batches/output), not batch body size, is
//!   the lever here. Scale [`NUM_OUTPUTS`] / [`WIDTHS`] up for heavier runs.
//! - Timings are on a `tempfile` dir — they reflect that filesystem, not a specific production
//!   disk.
//! - Every pack file is memory-mapped and uses `msync` durability barriers, driving the production
//!   `ConsensusPack`.
//!
//! ## `consensus_pack_workload`: production-shaped outputs beside readers
//!
//! One table per committee size N, one column per batch size. The model, from the production
//! callers:
//!
//! - **Outputs** as the subscriber assembles them (`Subscriber::fetch_batches`): a leader commits
//!   about two rounds of the DAG, so each output's sub-dag holds [`ROUNDS_PER_OUTPUT`] x N headers
//!   with the leader last. Each header carries [`WORKLOAD_BATCHES_PER_HEADER`] batch digests (the
//!   proposer seals a header at that many; the protocol cap is 10), a quorum of parents from the
//!   round before and its author's seed signature. The output carries those batches, one
//!   `CertifiedBatch` per header in sub-dag order. Batches are filled to the target size with
//!   distinct signed EIP-1559 transactions (transfers, ERC-20 transfers and larger contract calls
//!   from [`POOL_SENDERS`] accounts), sliced from a pool signed once. A thread builds the outputs
//!   ahead of the writer, outside the timed region.
//! - **Writes**: each output is `save_consensus_output` then `persist_current` (the pack's `msync`
//!   and the latest-consensus slot syncs, which run together and are also timed one by one), as
//!   state sync's `save_consensus` makes an output durable before forwarding it to execution. Their
//!   sum is the time to forward.
//! - **Reads**, on other tasks while the saves run, of the newest [`RECENT_OUTPUTS`] outputs:
//!   `with_consensus_header_by_digest` N times per output interval (one per gossiped certificate,
//!   reading the fields the primary's falling-behind check reads), `get_batches` for one header's
//!   batches N/2 times per interval with [`BATCH_MISS_PERCENT`]% of the digests absent (peer batch
//!   requests and cache misses), and `consensus_output_bytes_by_number` once per interval (a
//!   state-sync peer). Each call is its own task, started on schedule whether or not the earlier
//!   ones have returned, so a call that arrives during a save queues behind it as it would in
//!   production. The same reads then run alone, with no saves, for comparison.
//! - **Compression**: for a sample of outputs, every record is BCS-encoded and zstd-compressed
//!   outside the save, as the pack's append does, to show how much of a save is compression.
//!
//! Knobs (environment):
//!
//! - `TN_BENCH_PACK_RATE`: outputs saved a second (default 2). 0 saves as fast as the chain takes
//!   them; the readers then keep the default rate's schedule.
//! - `TN_BENCH_PACK_OUTPUTS`: outputs per configuration (default 200).
//! - `TN_BENCH_PACK_BATCH_KB`: batch sizes in KB, comma separated, one column each (default
//!   `16,256`; e.g. `16,256,1024`).
//! - `TN_BENCH_PACK_COMMITTEE`: committee sizes, comma separated, one table each (default `10,50`).
//! - `TN_BENCH_PACK_MAX_MB`: the most batch data one configuration writes (default 4096); a heavier
//!   configuration saves fewer outputs.
//! - `TN_BENCH_PACK_READ_SECS`: length of the readers-only phase (default 15).
//! - `TN_BENCH_PACK_READERS`: 0 runs no readers, to profile the writer alone (default 1).
//! - `TN_BENCH_PACK_NO_SLOTS`: 1 persists the pack alone, without the latest-consensus slot syncs
//!   that `persist_current` runs beside it: a measurement of what the slots add, never a production
//!   setting (default 0).
//! - `TN_BENCH_PACK_BATCH_REQ`: batches per `get_batches` request (default 5, one header's); a peer
//!   may ask for hundreds, served 200 at a time.
//!
//! The chain lives in a temp dir under `TMPDIR`. On Linux a tmpfs or ramfs `TMPDIR` is refused:
//! `msync` is free there, so the durable rows would mean nothing.
//!
//! Caveats:
//!
//! - The sub-dag stores only the certificates' headers, so certificates reuse one quorum-signed
//!   template per authority with each round's header swapped in; the headers are real.
//! - Reads arrive evenly spaced; production's arrive in bursts (a round's certificates together).
//! - Transactions are distinct within a batch but shared across batches. The pack compresses each
//!   record on its own, so the sharing does not change the compression ratio.
//! - The builder thread runs beside the writer. On a small machine it competes for CPU; the "writer
//!   waited on builder" row counts outputs that were not ready in time.
//! - A macOS `msync` does not flush the drive's write cache; on Linux (production) it does, so
//!   persist latencies there are higher.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::Write as _,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use rand::{rngs::StdRng, SeedableRng as _};
use tempfile::TempDir;
use tn_reth::{test_utils::TransactionFactory, RethChainSpec};
use tn_test_utils::{AuthorityFixture, CommitteeFixture};
use tn_types::{
    encode_into_buffer, encoded_size, forks, now_ms, test_genesis, Address, AuthorityIdentifier,
    Batch, BlockHash, BlockNumHash, Bytes, Certificate, CertifiedBatch, CommittedSubDag, Committee,
    ConsensusHeader, ConsensusHeaderDigest, ConsensusOutput, Epoch, EpochRecord,
    EpochSeedChainValue, Hash as _, HeaderBuilder, HeaderDigest, ReputationScores, Round,
    TimestampMs, B256, MIN_PROTOCOL_BASE_FEE, U256,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _},
    runtime::Runtime,
    sync::mpsc,
    task::JoinSet,
};

use crate::{
    consensus::ConsensusChain,
    consensus_pack::{
        test::make_wide_test_output, ConsensusPack, PackError, PackRecord, DATA_NAME,
    },
    mem_db::MemDatabase,
};

/// The subset of the pack API the battery exercises, kept as a small trait so the battery body
/// stays backend-agnostic. Its one implementation forwards to [`ConsensusPack`]'s inherent methods
/// (static dispatch — no `dyn`).
trait BenchPack: Sized {
    fn open_append(
        path: &Path,
        previous_epoch: EpochRecord,
        committee: Committee,
    ) -> Result<Self, PackError>;

    fn open_static(path: &Path, epoch: Epoch) -> Result<Self, PackError>;

    /// Async-seal and close the pack (the production shutdown path). Awaiting this before a reopen
    /// guarantees the pack is fully sealed, instead of racing the background drop-seal (which a
    /// multi-thread runtime defers via `spawn_blocking`).
    async fn close(self);

    async fn stream_import<R: AsyncRead + Unpin>(
        path: &Path,
        stream: R,
        epoch: Epoch,
        previous_epoch: &EpochRecord,
        final_consensus_number: u64,
        timeout: Duration,
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
            fn open_append(
                path: &Path,
                previous_epoch: EpochRecord,
                committee: Committee,
            ) -> Result<Self, PackError> {
                <$ty>::open_append(path, previous_epoch, committee)
            }
            fn open_static(path: &Path, epoch: Epoch) -> Result<Self, PackError> {
                <$ty>::open_static(path, epoch)
            }
            async fn stream_import<R: AsyncRead + Unpin>(
                path: &Path,
                stream: R,
                epoch: Epoch,
                previous_epoch: &EpochRecord,
                final_consensus_number: u64,
                timeout: Duration,
            ) -> Result<Self, PackError> {
                <$ty>::stream_import(
                    path,
                    stream,
                    epoch,
                    previous_epoch,
                    final_consensus_number,
                    timeout,
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
            async fn close(self) {
                <$ty>::close(self).await
            }
        }
    };
}

impl_bench_pack!(ConsensusPack);

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
async fn populate<P: BenchPack>(fx: &Fixtures, outputs: &[ConsensusOutput]) -> (P, TempDir) {
    let dir = TempDir::with_prefix("pack_bench_read").expect("temp dir");
    let mut pack = P::open_append(dir.path(), fx.previous_epoch.clone(), fx.committee.clone())
        .expect("open pack");
    for output in outputs {
        pack.save_consensus_output(output.clone()).await.expect("save");
    }
    (pack, dir)
}

// ---- the battery: each returns the timed duration for its measured region ----

/// Save every output append-only, no durability barrier (the executor hot-path write).
async fn bench_save_seq<P: BenchPack>(fx: &Fixtures, outputs: &[ConsensusOutput]) -> Duration {
    let dir = TempDir::with_prefix("pack_bench_save").expect("temp dir");
    let mut pack = P::open_append(dir.path(), fx.previous_epoch.clone(), fx.committee.clone())
        .expect("open pack");
    // The copies the saves take, made before the clock starts.
    let outputs = outputs.to_vec();
    let start = Instant::now();
    for output in outputs {
        pack.save_consensus_output(output).await.expect("save");
    }
    let elapsed = start.elapsed();
    pack.close().await; // seal synchronously before the TempDir drops (untimed), not a deferred drop
    elapsed
}

/// Save every output, each followed by a `persist()` barrier (state-sync save + persist). The delta
/// vs [`bench_save_seq`] is the per-output durability cost.
async fn bench_save_durable<P: BenchPack>(fx: &Fixtures, outputs: &[ConsensusOutput]) -> Duration {
    let dir = TempDir::with_prefix("pack_bench_durable").expect("temp dir");
    let mut pack = P::open_append(dir.path(), fx.previous_epoch.clone(), fx.committee.clone())
        .expect("open pack");
    let outputs = outputs.to_vec();
    let start = Instant::now();
    for output in outputs {
        pack.save_consensus_output(output).await.expect("save");
        pack.persist().await.expect("persist");
    }
    let elapsed = start.elapsed();
    pack.close().await; // seal synchronously before the TempDir drops (untimed)
    elapsed
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
async fn bench_stream_import<P: BenchPack>(fx: &Fixtures, data_path: &Path, end: u64) -> Duration {
    let dir = TempDir::with_prefix("pack_bench_import").expect("temp dir");
    let stream = tokio::fs::File::open(data_path).await.expect("open data file").take(end);
    let start = Instant::now();
    let mut pack = P::stream_import(
        dir.path(),
        stream,
        fx.committee.epoch(),
        &fx.previous_epoch,
        NUM_OUTPUTS,
        IMPORT_TIMEOUT,
    )
    .await
    .expect("stream import");
    let elapsed = start.elapsed();
    // stream_import drains the whole stream before returning, so the epoch is already present.
    pack.get_consensus_output(NUM_OUTPUTS).await.expect("last output present after import");
    pack.close().await; // seal synchronously before the TempDir drops (untimed)
    elapsed
}

/// Open the finished pack read-only, touching the index (the pack-cache-miss open cost). Its
/// files were just written, so they are in the page cache: this is the open's own work, not a
/// read from the drive.
async fn bench_reopen_static<P: BenchPack>(path: &Path, epoch: Epoch) -> Duration {
    let start = Instant::now();
    let mut pack = P::open_static(path, epoch).expect("open static");
    pack.latest_consensus_header().await.expect("latest after reopen");
    let elapsed = start.elapsed();
    pack.close().await; // before the TempDir drops (untimed)
    elapsed
}

/// One report column: the ordered rows plus the on-disk size of the populated pack.
struct Column {
    rows: Vec<(&'static str, Duration)>,
    data_len: u64,
}

/// Run the whole battery for one output width, over the pack type `P`.
async fn run_battery<P: BenchPack>(fx: &Fixtures, width: usize) -> Column {
    let outputs = build_outputs(fx, width);
    let header_digests: Vec<ConsensusHeaderDigest> =
        outputs.iter().map(|o| o.consensus_header_hash()).collect();
    let batch_digests: Vec<BlockHash> =
        outputs.iter().flat_map(|o| o.batch_digests().iter().copied()).take(BATCH_SAMPLE).collect();

    // Writes each build (and drop) their own pack.
    let save_seq = bench_save_seq::<P>(fx, &outputs).await;
    let save_durable = bench_save_durable::<P>(fx, &outputs).await;

    // One populated pack shared by the read/stream battery.
    let (mut pack, dir) = populate::<P>(fx, &outputs).await;
    let data_path = fx.data_path(dir.path());

    // Bulk-persist first so the un-persisted saves are flushed for the direct-file stream reads.
    let persist = bench_persist(&mut pack).await;
    // Real data length (past the last output). This excludes the open pack's capacity padding and
    // bounds the stream reads below.
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
    let stream_import = bench_stream_import::<P>(fx, &data_path, data_len).await;

    // Async-close (the production seal path) so the reopen sees a fully sealed pack. A plain
    // `drop(pack)` here would defer the seal to `spawn_blocking` on this multi-thread runtime and
    // return before it finished, so the read-only `open_static` below would race the unsealed pack
    // and fail.
    pack.close().await;
    let reopen_static = bench_reopen_static::<P>(dir.path(), fx.committee.epoch()).await;
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

/// Collects one column per output width and prints an aligned table (ms).
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
        // One column per width now (the buf/mmap A-B comparison was removed); a vertical rail
        // between width columns and a rule between row groups keep a single row easy to follow.
        const GROUP: usize = 1;
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

        // Row labels after which to draw a horizontal separator (the write / persist / read /
        // stream / lifecycle groups from the module docs).
        const GROUP_SEP_AFTER: &[&str] =
            &["save_durable", "persist bulk", "latest_header", "stream_import"];

        println!(
            "\n=== consensus pack-file benchmark: memory-mapped (msync) ConsensusPack (ms) ==="
        );
        println!("legend: columns are mmap x{{batches/output}} — the memory-mapped production ConsensusPack at each output width. Vertical rails group columns; horizontal rules group the rows. save_durable adds a persist() barrier per output; test batches carry 1 tx each.");

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

/// Benchmark the memory-mapped (`msync`) production [`ConsensusPack`] across its real usage
/// patterns, as a regression baseline.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tn-storage pack_file_bench -- --ignored --nocapture
/// --test-threads 1`.
#[test]
#[ignore = "on-demand pack-file (mmap) regression benchmark; run with --ignored --nocapture --test-threads 1"]
fn pack_file_bench() {
    crate::pack_bench::check_bench_temp_dir();
    // Mirror `db_bench`: a plain test driving the async pack API on a tokio runtime.
    let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
    runtime.block_on(async {
        let fx = Fixtures::new();
        let mut report = Report::new();
        // One column per output width, all on the memory-mapped file backend.
        for &(_, width) in WIDTHS {
            let name = format!("mmap x{width}");
            println!("  running battery for {name} ...");
            let col = run_battery::<ConsensusPack>(&fx, width).await;
            report.push(&name, col);
        }
        report.print();
    });
}

// ---- consensus_pack_workload: production-shaped outputs beside concurrent readers ----

/// Batches per header: the proposer seals a header at this many (the protocol cap,
/// `MAX_HEADER_NUM_OF_BATCHES`, is 10).
const WORKLOAD_BATCHES_PER_HEADER: usize = 5;
/// DAG rounds one output commits: a leader commits about its own round and its parents'.
const ROUNDS_PER_OUTPUT: u32 = 2;
/// How far header timestamps advance per round (two rounds per output at the default rate).
const ROUND_MS: u64 = 250;
/// The default output rate (outputs a second), and the readers' schedule when the writer is
/// unpaced.
const DEFAULT_OUTPUT_RATE: u32 = 2;
/// The newest outputs the readers pick from: gossip and peer requests ask about the tip.
const RECENT_OUTPUTS: usize = 8;
/// Percent of the batch digests a `get_batches` request asks for that the pack does not hold.
const BATCH_MISS_PERCENT: u64 = 10;
/// Outputs the builder thread keeps ready ahead of the writer.
const BUILD_AHEAD: usize = 4;
/// Accounts signing the transaction pool, so neighbouring transactions differ in sender and nonce.
const POOL_SENDERS: u64 = 64;
/// The smallest pool transaction (a plain transfer encodes to a little over this), for sizing the
/// pool against the largest batch.
const MIN_TX_BYTES: usize = 100;
/// An encoded batch's bytes besides its transactions: epoch, beneficiary, base fee, worker id and
/// the transaction count.
const BATCH_FIXED_BYTES: usize = 4 + 20 + 8 + 2 + 3;
/// Outputs per configuration whose records are encoded and compressed outside the save.
const COMPRESS_SAMPLES: u64 = 16;
/// Reader phases: while the writer saves, then reads alone, then stop.
const PHASE_WITH_SAVES: u8 = 0;
const PHASE_READS_ONLY: u8 = 1;
const PHASE_STOP: u8 = 2;

/// The workload's knobs (see the module docs), read from the environment.
#[derive(Debug)]
struct WorkloadKnobs {
    /// Outputs saved a second; 0 saves as fast as the chain takes them.
    rate: u32,
    /// Outputs per configuration, before the data cap.
    outputs: u64,
    /// Batch sizes in bytes, one column each.
    batch_sizes: Vec<usize>,
    /// Committee sizes, one table each.
    committees: Vec<usize>,
    /// The most batch data one configuration writes.
    max_bytes: u64,
    /// Length of the readers-only phase.
    read_only: Duration,
    /// Whether readers run at all (off for profiling the writer alone).
    readers: bool,
    /// Persist the pack alone, without the slot syncs `persist_current` runs beside it (a
    /// measurement only: production always syncs both).
    no_slots: bool,
    /// Batches per `get_batches` request (one header's by default; a peer may ask for hundreds).
    batch_request: usize,
}

impl WorkloadKnobs {
    fn from_env() -> Self {
        fn var<T: std::str::FromStr>(name: &str, default: T) -> T {
            std::env::var(name).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default)
        }
        fn list(name: &str, default: &str) -> Vec<usize> {
            let raw = std::env::var(name).unwrap_or_else(|_| default.to_string());
            raw.split(',').filter_map(|v| v.trim().parse().ok()).filter(|&v| v > 0).collect()
        }
        Self {
            rate: var("TN_BENCH_PACK_RATE", DEFAULT_OUTPUT_RATE),
            outputs: var("TN_BENCH_PACK_OUTPUTS", 200_u64).max(1),
            batch_sizes: list("TN_BENCH_PACK_BATCH_KB", "16,256")
                .into_iter()
                .map(|kb| kb << 10)
                .collect(),
            committees: list("TN_BENCH_PACK_COMMITTEE", "10,50"),
            max_bytes: var("TN_BENCH_PACK_MAX_MB", 4096_u64) << 20,
            read_only: Duration::from_secs(var("TN_BENCH_PACK_READ_SECS", 15_u64)),
            readers: var("TN_BENCH_PACK_READERS", 1_u8) != 0,
            no_slots: var("TN_BENCH_PACK_NO_SLOTS", 0_u8) != 0,
            batch_request: var("TN_BENCH_PACK_BATCH_REQ", WORKLOAD_BATCHES_PER_HEADER).max(1),
        }
    }

    /// Outputs `shape` saves: [`Self::outputs`], or fewer when that many would write more than
    /// [`Self::max_bytes`] of batches (the cap never goes below [`RECENT_OUTPUTS`]).
    fn outputs_for(&self, shape: OutputShape) -> u64 {
        let per_output = (shape.batches() * shape.batch_bytes) as u64;
        self.outputs.min((self.max_bytes / per_output).max(RECENT_OUTPUTS as u64))
    }

    /// The readers' output rate: the writer's, or the default when the writer is unpaced.
    fn read_rate(&self) -> u32 {
        if self.rate == 0 {
            DEFAULT_OUTPUT_RATE
        } else {
            self.rate
        }
    }

    /// The interval between a reader's calls when it makes `per_output` calls per output.
    fn reader_period(&self, per_output: usize) -> Duration {
        Duration::from_secs_f64(1.0 / (f64::from(self.read_rate()) * per_output as f64))
    }
}

/// One configuration's output shape.
#[derive(Clone, Copy, Debug)]
struct OutputShape {
    /// Committee size (N).
    committee: usize,
    /// Each batch is filled to about this many encoded bytes.
    batch_bytes: usize,
}

impl OutputShape {
    /// Certificates (headers) per output.
    fn certificates(self) -> usize {
        self.committee * ROUNDS_PER_OUTPUT as usize
    }

    /// Batches per output.
    fn batches(self) -> usize {
        self.certificates() * WORKLOAD_BATCHES_PER_HEADER
    }
}

/// SplitMix64: a cheap, well-mixed stream for the synthetic picks and fill bytes.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// `N` random-looking bytes from `seed`.
fn mixed_bytes<const N: usize>(seed: u64) -> [u8; N] {
    let mut bytes = [0_u8; N];
    for (lane, chunk) in bytes.chunks_mut(8).enumerate() {
        chunk.copy_from_slice(&mix(seed.wrapping_add(lane as u64)).to_le_bytes()[..chunk.len()]);
    }
    bytes
}

/// A 32-byte calldata word holding an address (left-padded with zeros, as the ABI encodes it).
fn address_word(seed: u64) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[12..].copy_from_slice(&mixed_bytes::<20>(seed));
    word
}

/// A 32-byte calldata word holding a token amount (a big-endian integer up to 2^64).
fn amount_word(seed: u64) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[24..].copy_from_slice(&mix(seed).to_be_bytes());
    word
}

/// ERC-20 `transfer(address,uint256)` calldata.
fn erc20_transfer(seed: u64) -> Bytes {
    let mut input = vec![0xa9, 0x05, 0x9c, 0xbb];
    input.extend_from_slice(&address_word(seed));
    input.extend_from_slice(&amount_word(seed ^ 1));
    input.into()
}

/// Calldata of a larger contract call (a swap or a multi-argument call): a selector, then 4 to 12
/// words of addresses, amounts, small integers and hashes.
fn contract_call(seed: u64) -> Bytes {
    let mut input = mixed_bytes::<4>(seed % 16).to_vec();
    let words = 4 + mix(seed) % 9;
    for word in 0..words {
        let word_seed = mix(seed ^ (word << 56));
        input.extend_from_slice(&match word % 4 {
            0 => address_word(word_seed),
            1 => amount_word(word_seed),
            2 => {
                let mut small = [0_u8; 32];
                small[31] = word_seed as u8;
                small
            }
            _ => mixed_bytes::<32>(word_seed),
        });
    }
    input.into()
}

/// `count` signed transactions from pool account `sender` (nonces 0 up): in turn a plain
/// transfer, an ERC-20 transfer to one of a few token contracts, and a larger contract call to a
/// router.
fn sign_sender_txs(chain: &Arc<RethChainSpec>, sender: u64, count: usize) -> Vec<Vec<u8>> {
    let mut rng = StdRng::seed_from_u64(sender);
    let mut factory = TransactionFactory::new_random_from_seed(&mut rng);
    let router = Address::from(mixed_bytes::<20>(0x5157_0000));
    (0..count as u64)
        .map(|nonce| {
            let seed = mix((sender << 32) | nonce);
            let gas_price = 7_000_000_000_u128 + u128::from(seed % 3_000_000_000);
            let (gas, to, value, input) = match (sender + nonce) % 3 {
                0 => (
                    21_000,
                    Address::from(mixed_bytes::<20>(seed)),
                    U256::from(seed % 1_000_000_000_000_000_000),
                    Bytes::new(),
                ),
                1 => (
                    65_000,
                    Address::from(mixed_bytes::<20>(0x70CE_0000 + seed % 4)),
                    U256::ZERO,
                    erc20_transfer(seed),
                ),
                _ => (250_000, router, U256::ZERO, contract_call(seed)),
            };
            factory.create_eip1559_encoded(
                chain.clone(),
                Some(gas),
                gas_price,
                Some(to),
                value,
                input,
            )
        })
        .collect()
}

/// Distinct signed transactions that batches are sliced from, signed once per run.
struct TxPool {
    txs: Vec<Vec<u8>>,
}

impl TxPool {
    /// Sign `len` transactions, [`POOL_SENDERS`] accounts on all cores, interleaved so that
    /// neighbours in the pool (and so in a batch) come from different accounts.
    fn new(len: usize) -> Self {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let per_sender = len.div_ceil(POOL_SENDERS as usize);
        let threads =
            std::thread::available_parallelism().map_or(4, |n| n.get()).min(POOL_SENDERS as usize);
        let mut by_sender: Vec<Vec<Vec<u8>>> = vec![Vec::new(); POOL_SENDERS as usize];
        std::thread::scope(|s| {
            let signers: Vec<_> = (0..threads)
                .map(|t| {
                    let chain = &chain;
                    s.spawn(move || {
                        (t as u64..POOL_SENDERS)
                            .step_by(threads)
                            .map(|sender| (sender, sign_sender_txs(chain, sender, per_sender)))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            for signer in signers {
                for (sender, txs) in signer.join().expect("pool signer") {
                    by_sender[sender as usize] = txs;
                }
            }
        });
        let mut txs = Vec::with_capacity(len);
        for nonce in 0..per_sender {
            for sender in &mut by_sender {
                txs.push(std::mem::take(&mut sender[nonce]));
            }
        }
        txs.truncate(len);
        Self { txs }
    }
}

/// The pool length for a run: longer than twice the largest batch's transaction count (so no batch
/// repeats a transaction) and than one authority's batches (so each starts at its own offset and
/// has its own digest). A prime, so any stride visits every offset.
fn pool_len(knobs: &WorkloadKnobs) -> usize {
    let largest = knobs.batch_sizes.iter().copied().max().unwrap_or(0);
    let per_authority =
        knobs.outputs as usize * ROUNDS_PER_OUTPUT as usize * WORKLOAD_BATCHES_PER_HEADER;
    let is_prime =
        |n: usize| n >= 2 && (2..).take_while(|d| d * d <= n).all(|d| !n.is_multiple_of(d));
    let mut len = (2 * largest / MIN_TX_BYTES).max(per_authority + 1).max(4_096);
    while !is_prime(len) {
        len += 1;
    }
    len
}

/// A batch from `beneficiary` of the pool's transactions from `offset` on (wrapping), filled until
/// the next one would take its encoding past `target` bytes (at least one transaction).
fn fill_batch(pool: &[Vec<u8>], offset: usize, target: usize, beneficiary: Address) -> Batch {
    let mut transactions = Vec::new();
    let mut bytes = BATCH_FIXED_BYTES;
    for i in 0..pool.len() {
        let tx = &pool[(offset + i) % pool.len()];
        // Each transaction is a length-prefixed byte string (a 2-byte prefix below 16 KB).
        let size = tx.len() + 2;
        if !transactions.is_empty() && bytes + size > target {
            break;
        }
        bytes += size;
        transactions.push(tx.clone());
    }
    Batch {
        transactions,
        epoch: 0,
        beneficiary,
        base_fee_per_gas: MIN_PROTOCOL_BASE_FEE,
        worker_id: 0,
        received_at: None,
    }
}

/// A saved output's keys, as the readers look it up.
#[derive(Debug)]
struct SavedOutput {
    number: u64,
    header: ConsensusHeaderDigest,
    /// Batch digests in sub-dag order: header `i`'s payload is the `i`th run of
    /// [`WORKLOAD_BATCHES_PER_HEADER`].
    batches: Vec<BlockHash>,
}

/// One built output, handed from the builder thread to the writer.
#[derive(Debug)]
struct BuiltOutput {
    output: ConsensusOutput,
    saved: Arc<SavedOutput>,
}

/// The builder's totals over the outputs it built, and its sampled encode and compress costs.
#[derive(Debug, Default)]
struct BuildTotals {
    outputs: u64,
    txs: u64,
    /// BCS-encoded batch bytes (what the pack's batch records hold before compression).
    batch_bytes: u64,
    /// Per sampled output: encoding every record, and compressing every record.
    encode: Vec<Duration>,
    compress: Vec<Duration>,
    /// Sampled outputs' record bytes, encoded and compressed.
    sampled_raw: u64,
    sampled_compressed: u64,
}

/// Builds a configuration's chained outputs (see the module docs for their shape).
struct OutputBuilder<'f> {
    shape: OutputShape,
    committee: Committee,
    /// The committee's authorities, in fixture order; authority `a` is index `a` throughout.
    authorities: Vec<&'f AuthorityFixture<MemDatabase>>,
    /// A certificate per authority with a real quorum of votes, its header swapped for each
    /// round's: the sub-dag keeps only the headers, and the leader's signature seeds the
    /// randomness of an epoch before the seed-signature fork.
    templates: Vec<Certificate>,
    quorum: usize,
    pool: Arc<TxPool>,
    /// Pool offset step between one authority's consecutive batches (a batch's worth).
    stride: usize,
    /// Batches each authority has made, its next batch's pool slot.
    batches_made: Vec<usize>,
    /// The last round's header digests, by authority (the genesis certificates' at first).
    previous_round: Vec<HeaderDigest>,
    round: Round,
    number: u64,
    parent: ConsensusHeaderDigest,
    previous_sub_dag: Option<CommittedSubDag>,
    /// Every batch digest so far, to prove they are distinct.
    digests: HashSet<BlockHash>,
    start_ms: u64,
}

impl<'f> OutputBuilder<'f> {
    fn new(
        fixture: &'f CommitteeFixture<MemDatabase>,
        shape: OutputShape,
        pool: Arc<TxPool>,
    ) -> Self {
        let committee = fixture.committee();
        let authorities: Vec<_> = fixture.authorities().collect();
        let n = authorities.len();
        let quorum = committee.quorum_threshold() as usize;
        let templates = (0..n)
            .map(|a| {
                let header = authorities[a].header(&committee);
                let votes = (0..quorum)
                    .map(|j| {
                        let vote = authorities[(a + j) % n].vote(&header);
                        (vote.author().clone(), *vote.signature())
                    })
                    .collect();
                Certificate::new_unverified(&committee, header, votes).expect("a quorum of votes")
            })
            .collect();
        let previous_round = Certificate::genesis(&committee).iter().map(|c| c.digest()).collect();
        let stride = (shape.batch_bytes / MIN_TX_BYTES).max(1) % pool.txs.len();
        Self {
            shape,
            committee,
            authorities,
            templates,
            quorum,
            stride: stride.max(1),
            pool,
            batches_made: vec![0; n],
            previous_round,
            round: 0,
            number: 0,
            parent: ConsensusHeader::default().digest(),
            previous_sub_dag: None,
            digests: HashSet::new(),
            start_ms: now_ms().as_millis(),
        }
    }

    /// Authority `a`'s next batch: its own offset into the pool, its execution address as the
    /// beneficiary.
    fn next_batch(&mut self, a: usize) -> Batch {
        let pool = &self.pool.txs;
        let slot = self.batches_made[a];
        self.batches_made[a] += 1;
        let offset = (slot * self.stride + a * (pool.len() / self.shape.committee)) % pool.len();
        let beneficiary = self.authorities[a].execution_address();
        let batch = fill_batch(pool, offset, self.shape.batch_bytes, beneficiary);
        assert!(self.digests.insert(batch.digest()), "every batch is distinct");
        batch
    }

    /// The next output, chained to the last, and its transaction count.
    fn next_output(&mut self) -> (ConsensusOutput, u64) {
        let n = self.shape.committee;
        let epoch = self.committee.epoch();
        self.number += 1;
        let leader_author = self.number as usize % n;
        let mut certificates = Vec::with_capacity(self.shape.certificates());
        let mut certified = Vec::with_capacity(self.shape.certificates());
        let mut batch_digests = VecDeque::with_capacity(self.shape.batches());
        let mut txs = 0;
        for step in 1..=ROUNDS_PER_OUTPUT {
            self.round += 1;
            let round = self.round;
            let leader_round = step == ROUNDS_PER_OUTPUT;
            let mut this_round = vec![HeaderDigest::default(); n];
            // The leader's certificate comes last in its round, and so in the sub-dag.
            let order = (0..n)
                .filter(|&a| !leader_round || a != leader_author)
                .chain(leader_round.then_some(leader_author));
            for a in order {
                let authority = self.authorities[a];
                let batches: Vec<Batch> =
                    (0..WORKLOAD_BATCHES_PER_HEADER).map(|_| self.next_batch(a)).collect();
                let payload: Vec<BlockHash> = batches.iter().map(Batch::digest).collect();
                txs += batches.iter().map(|b| b.transactions.len() as u64).sum::<u64>();
                let header = HeaderBuilder::default()
                    .author(authority.id())
                    .round(round)
                    .epoch(epoch)
                    .created_at_ms(TimestampMs::from_millis(
                        self.start_ms + u64::from(round) * ROUND_MS + a as u64,
                    ))
                    .payload(payload.iter().map(|digest| (*digest, 0)).collect())
                    .parents((0..self.quorum).map(|j| self.previous_round[(a + j) % n]).collect())
                    .latest_execution_block(BlockNumHash::new(
                        self.number,
                        B256::from(mixed_bytes::<32>(self.number)),
                    ))
                    .seed_signature(authority.seed_signature(epoch, round))
                    .build();
                this_round[a] = header.digest();
                batch_digests.extend(payload);
                let mut certificate = self.templates[a].clone();
                certificate.update_header_for_test(header);
                certificates.push(certificate);
                certified.push(CertifiedBatch { address: authority.execution_address(), batches });
            }
            self.previous_round = this_round;
        }
        let leader = certificates.last().expect("an output has certificates").clone();
        let sub_dag_index = leader.header().nonce();
        let sub_dag = CommittedSubDag::new(
            certificates,
            leader,
            sub_dag_index,
            ReputationScores::new(&self.committee),
            self.previous_sub_dag.take(),
            EpochSeedChainValue::genesis_placeholder(),
        );
        let output = ConsensusOutput::new(
            sub_dag.clone(),
            self.parent,
            self.number,
            false,
            batch_digests,
            certified,
        );
        self.parent = output.consensus_header_hash();
        self.previous_sub_dag = Some(sub_dag);
        (output, txs)
    }
}

/// Encode and compress every record of `output` as the pack's append does (BCS into a reused
/// buffer, then one zstd frame per record through a reused context at the pack's level), outside
/// any save. Returns the encode time, the compress time, and the bytes before and after.
fn measure_compression(
    output: &ConsensusOutput,
    ctx: &mut zstd::zstd_safe::CCtx<'static>,
    stage: &mut Vec<u8>,
    compressed: &mut Vec<u8>,
) -> (Duration, Duration, u64, u64) {
    let header = PackRecord::Consensus(Box::new(output.consensus_header()));
    let batches: Vec<PackRecord> = output
        .batches()
        .iter()
        .flat_map(|certified| certified.batches.iter().cloned().map(PackRecord::Batch))
        .collect();
    let (mut encode, mut compress, mut raw, mut packed) = (Duration::ZERO, Duration::ZERO, 0, 0);
    for record in std::iter::once(&header).chain(&batches) {
        let start = Instant::now();
        stage.clear();
        encode_into_buffer(stage, record).expect("encode a record");
        encode += start.elapsed();
        let start = Instant::now();
        compressed.clear();
        ctx.reset(zstd::zstd_safe::ResetDirective::SessionOnly).expect("reset the zstd context");
        let mut encoder = zstd::stream::write::Encoder::with_context(&mut *compressed, ctx);
        encoder.write_all(stage).expect("compress a record");
        encoder.finish().expect("finish a frame");
        compress += start.elapsed();
        raw += stage.len() as u64;
        packed += compressed.len() as u64;
    }
    (encode, compress, raw, packed)
}

/// The builder thread: make the committee (sent back first, for the chain), then build `outputs`
/// outputs into `outputs_tx`, at most [`BUILD_AHEAD`] ahead of the writer, measuring a sample's
/// compression.
fn build_workload_outputs(
    shape: OutputShape,
    outputs: u64,
    pool: Arc<TxPool>,
    committee_tx: std::sync::mpsc::Sender<Committee>,
    outputs_tx: mpsc::Sender<BuiltOutput>,
) -> BuildTotals {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(shape.committee).expect("a committee"))
        .build();
    committee_tx.send(fixture.committee()).expect("send the committee");
    let mut builder = OutputBuilder::new(&fixture, shape, pool);
    let mut totals = BuildTotals::default();
    let sample_every = (outputs / COMPRESS_SAMPLES).max(1);
    // Configured as the pack's own context (`ZstdCtx` in `archive/pack.rs`): level 0 is zstd's
    // default, 3.
    let mut ctx = zstd::zstd_safe::CCtx::create();
    ctx.set_parameter(zstd::zstd_safe::CParameter::CompressionLevel(0)).expect("zstd level");
    let (mut stage, mut compressed) = (Vec::new(), Vec::new());
    for i in 0..outputs {
        let (output, txs) = builder.next_output();
        totals.outputs += 1;
        totals.txs += txs;
        totals.batch_bytes += output
            .batches()
            .iter()
            .flat_map(|certified| &certified.batches)
            .map(|batch| encoded_size(batch).expect("batch size") as u64)
            .sum::<u64>();
        if i % sample_every == 0 {
            let (encode, compress, raw, packed) =
                measure_compression(&output, &mut ctx, &mut stage, &mut compressed);
            totals.encode.push(encode);
            totals.compress.push(compress);
            totals.sampled_raw += raw;
            totals.sampled_compressed += packed;
        }
        let saved = Arc::new(SavedOutput {
            number: output.number(),
            header: output.consensus_header_hash(),
            batches: output.batch_digests().iter().copied().collect(),
        });
        if outputs_tx.blocking_send(BuiltOutput { output, saved }).is_err() {
            break; // the writer stopped
        }
    }
    totals
}

/// The newest saved outputs, which the readers pick from.
type Recent = Arc<parking_lot::RwLock<VecDeque<Arc<SavedOutput>>>>;

/// The writer's per-output timings and totals.
#[derive(Debug, Default)]
struct WriterStats {
    save: Vec<Duration>,
    persist: Vec<Duration>,
    /// The two halves of each persist, which run together: the pack's sync and the slot syncs
    /// (none when they are skipped).
    persist_pack: Vec<Duration>,
    persist_slots: Vec<Duration>,
    /// The parts of each save: the pack's check, encode and append (see `SavePhases`), and the
    /// rest of the call (the blocking-pool dispatch, the latest-consensus update, waits).
    save_check: Vec<Duration>,
    save_encode: Vec<Duration>,
    save_append: Vec<Duration>,
    save_outside: Vec<Duration>,
    /// Save plus persist: when the output could be forwarded to execution.
    forward: Vec<Duration>,
    /// Outputs that were not built by their slot (the writer waited on the builder).
    builder_waits: usize,
    elapsed: Duration,
    /// Pack bytes the saves reported.
    pack_bytes: u64,
}

/// Save and persist `outputs` outputs as state sync's `save_consensus` does, `rate` a second (0:
/// back to back), publishing each to the readers once it is durable.
async fn run_writer(
    chain: &ConsensusChain,
    built: &mut mpsc::Receiver<BuiltOutput>,
    outputs: u64,
    rate: u32,
    no_slots: bool,
    recent: &Recent,
) -> WriterStats {
    let mut stats = WriterStats::default();
    let start = tokio::time::Instant::now();
    for i in 0..outputs {
        if rate > 0 {
            let slot = Duration::from_secs_f64(i as f64 / f64::from(rate));
            tokio::time::sleep_until(start + slot).await;
        }
        let BuiltOutput { output, saved } = match built.try_recv() {
            Ok(next) => next,
            Err(_) => {
                stats.builder_waits += 1;
                built.recv().await.expect("the builder sends every output")
            }
        };
        let call = Instant::now();
        stats.pack_bytes += chain.save_consensus_output(output).await.expect("save output");
        let saved_at = Instant::now();
        if no_slots {
            stats.persist_pack.push(chain.persist_pack_only().await.expect("persist the pack"));
        } else {
            let (pack, slots) = chain.persist_current_timed().await.expect("persist output");
            stats.persist_pack.push(pack);
            stats.persist_slots.push(slots);
        }
        let persisted_at = Instant::now();
        stats.save.push(saved_at - call);
        stats.persist.push(persisted_at - saved_at);
        for phases in chain.take_save_phases() {
            let inside = phases.check + phases.encode + phases.append;
            stats.save_check.push(phases.check);
            stats.save_encode.push(phases.encode);
            stats.save_append.push(phases.append);
            stats.save_outside.push((saved_at - call).saturating_sub(inside));
        }
        stats.forward.push(persisted_at - call);
        let mut recent = recent.write();
        if recent.len() == RECENT_OUTPUTS {
            recent.pop_front();
        }
        recent.push_back(saved);
    }
    stats.elapsed = start.elapsed();
    stats
}

/// The read kinds: each the chain call a production reader makes.
#[derive(Clone, Copy, Debug)]
enum ReadKind {
    /// `with_consensus_header_by_digest` of a header's number, leader epoch and round, as the
    /// primary reads them for every gossiped certificate.
    HeaderByDigest,
    /// `get_batches`, a request for one header's batches.
    Batches,
    /// `consensus_output_bytes_by_number`, a state-sync peer reading an output.
    OutputBytes,
}

impl ReadKind {
    const ALL: [Self; 3] = [Self::HeaderByDigest, Self::Batches, Self::OutputBytes];

    fn label(self) -> &'static str {
        match self {
            Self::HeaderByDigest => "header_by_digest",
            Self::Batches => "get_batches",
            Self::OutputBytes => "output_bytes",
        }
    }

    /// Calls per output interval at committee size `n`.
    fn per_output(self, n: usize) -> usize {
        match self {
            Self::HeaderByDigest => n,
            Self::Batches => (n / 2).max(1),
            Self::OutputBytes => 1,
        }
    }
}

/// One call of `kind` against `target`, asserting what it returns; `seed` picks the header and
/// the absent digests of a batch request.
async fn read_once(
    kind: ReadKind,
    chain: &ConsensusChain,
    epoch: Epoch,
    target: &SavedOutput,
    batch_request: usize,
    seed: u64,
) {
    match kind {
        ReadKind::HeaderByDigest => {
            // The fields the primary's falling-behind check reads, as it reads them.
            let fields = chain
                .with_consensus_header_by_digest(epoch, target.header, |h| {
                    (h.number, h.sub_dag.leader_epoch(), h.sub_dag.leader_round())
                })
                .await;
            assert!(
                matches!(fields, Ok(Some((number, _, _))) if number == target.number),
                "a saved header is found by digest: {fields:?}"
            );
        }
        ReadKind::Batches => {
            let mut x = seed;
            let mut next = || {
                x = mix(x);
                x
            };
            // `batch_request` digests from a header's first, wrapping round the output's batches
            // (one header's by default).
            let headers = target.batches.len() / WORKLOAD_BATCHES_PER_HEADER;
            let first = (next() as usize % headers) * WORKLOAD_BATCHES_PER_HEADER;
            let want = batch_request.min(target.batches.len());
            let mut digests: Vec<BlockHash> =
                (0..want).map(|i| target.batches[(first + i) % target.batches.len()]).collect();
            let mut held = digests.len();
            for digest in &mut digests {
                if next() % 100 < BATCH_MISS_PERCENT {
                    *digest = BlockHash::from(mixed_bytes::<32>(next()));
                    held -= 1;
                }
            }
            let batches = chain.get_batches(epoch, digests.iter()).await;
            assert_eq!(batches.len(), held, "every saved batch is found, and no other");
        }
        ReadKind::OutputBytes => {
            let bytes = chain.consensus_output_bytes_by_number(target.number).await;
            assert!(
                matches!(&bytes, Ok(Some(bytes)) if !bytes.is_empty()),
                "a saved output's bytes are found"
            );
        }
    }
}

/// One reader: a `kind` call every `period` against a random recent output, until the phase says
/// stop. Open loop, as independent gossip and request tasks arrive: each call is its own task,
/// started on schedule whether or not the earlier ones have returned, so a call queued behind a
/// save waits as long as it would in production. Each latency is recorded under the phase its
/// call started in.
#[allow(clippy::too_many_arguments)]
async fn run_reader(
    kind: ReadKind,
    chain: ConsensusChain,
    epoch: Epoch,
    recent: Recent,
    phase: Arc<AtomicU8>,
    period: Duration,
    batch_request: usize,
    seed: u64,
) -> [Vec<Duration>; 2] {
    let mut samples: [Vec<Duration>; 2] = Default::default();
    let mut calls = JoinSet::new();
    let mut x = seed;
    let start = tokio::time::Instant::now();
    for tick in 0_u32.. {
        tokio::time::sleep_until(start + period * tick).await;
        while let Some(done) = calls.try_join_next() {
            let (at, latency): (u8, Duration) = done.expect("read call");
            samples[usize::from(at)].push(latency);
        }
        let at = phase.load(Ordering::Acquire);
        if at == PHASE_STOP {
            break;
        }
        x = mix(x);
        let target = {
            let recent = recent.read();
            if recent.is_empty() {
                continue; // nothing saved yet
            }
            recent[x as usize % recent.len()].clone()
        };
        let (chain, seed) = (chain.clone(), mix(x ^ 1));
        calls.spawn(async move {
            let call = Instant::now();
            read_once(kind, &chain, epoch, &target, batch_request, seed).await;
            (at, call.elapsed())
        });
    }
    while let Some(done) = calls.join_next().await {
        let (at, latency) = done.expect("read call");
        samples[usize::from(at)].push(latency);
    }
    samples
}

/// [`check_bench_filesystem`] for the temp dir (`TMPDIR`) the benches make their files in: every
/// storage bench calls this first.
pub(crate) fn check_bench_temp_dir() {
    check_bench_filesystem(&std::env::temp_dir());
}

/// Refuse a RAM-backed temp dir on Linux, where `msync` and `fsync` are free and the durable rows
/// would mean nothing; note macOS's weaker `msync`.
fn check_bench_filesystem(dir: &Path) {
    println!("bench temp dir: {}", dir.display());
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt as _;
        const TMPFS_MAGIC: u32 = 0x0102_1994;
        const RAMFS_MAGIC: u32 = 0x8584_58f6;
        let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).expect("a path without NUL");
        // SAFETY: `path` is a NUL-terminated string that outlives the call, and `stat` is writable
        // memory of the type `statfs` fills in; it is read only after the call reported success
        // (0), which means every field was written.
        let fs_type = unsafe {
            let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
            if libc::statfs(path.as_ptr(), stat.as_mut_ptr()) != 0 {
                panic!("statfs {}: {}", dir.display(), std::io::Error::last_os_error());
            }
            stat.assume_init().f_type
        };
        // `f_type`'s integer type differs by architecture and libc; the magic numbers are 32 bits.
        let fs_type = fs_type as u32;
        assert!(
            fs_type != TMPFS_MAGIC && fs_type != RAMFS_MAGIC,
            "{} is on tmpfs or ramfs (RAM), where msync is free: set TMPDIR to a directory on the \
             disk under test",
            dir.display()
        );
    }
    #[cfg(target_os = "macos")]
    println!(
        "note: a macOS msync does not flush the drive's write cache, so persist latencies here \
         are lower than on Linux (production)"
    );
}

/// The `p` quantile (0 to 1) of `samples`, which this sorts.
fn quantile(samples: &mut [Duration], p: f64) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    samples.sort_unstable();
    samples[((samples.len() - 1) as f64 * p).round() as usize]
}

/// The p50, p99 and max of `samples`.
fn spread(samples: &mut [Duration]) -> [Duration; 3] {
    [quantile(samples, 0.50), quantile(samples, 0.99), quantile(samples, 1.0)]
}

/// The mean of `samples`.
fn mean(samples: &[Duration]) -> Duration {
    samples.iter().sum::<Duration>() / u32::try_from(samples.len().max(1)).expect("sample count")
}

/// The unit a latency row prints in.
#[derive(Clone, Copy, Debug)]
enum Unit {
    Ms,
    Us,
}

impl Unit {
    fn format(self, d: Duration) -> String {
        match self {
            Self::Ms => format!("{:.2}", d.as_secs_f64() * 1e3),
            Self::Us => format!("{:.0}", d.as_secs_f64() * 1e6),
        }
    }
}

/// One configuration (a batch size) of a workload table.
#[derive(Debug)]
struct WorkloadColumn {
    name: String,
    /// Latency rows: label, unit, then p50 / p99 / max.
    latencies: Vec<(String, Unit, [Duration; 3])>,
    /// Single-value rows.
    values: Vec<(String, String)>,
}

/// Row labels after which a rule separates the groups.
const WORKLOAD_RULE_AFTER: &[&str] = &["save+persist ms", "reads/s output_bytes (with / only)"];

/// Print the columns side by side: the latency rows as p50 / p99 / max per column, then the
/// single-value rows.
fn print_workload_table(title: &str, legend: &[String], cols: &[WorkloadColumn]) {
    let Some(first) = cols.first() else { return };
    const NUM_W: usize = 9;
    let block_w = 3 * NUM_W + 2;
    let label_w = first
        .latencies
        .iter()
        .map(|row| row.0.len())
        .chain(first.values.iter().map(|row| row.0.len()))
        .max()
        .unwrap_or(0);
    let rule = "─".repeat(label_w + cols.len() * (block_w + 3));
    println!("\n=== {title} ===");
    for line in legend {
        println!("{line}");
    }
    print!("{:label_w$}", "");
    for col in cols {
        print!(" │ {:>block_w$}", col.name);
    }
    println!();
    print!("{:label_w$}", "");
    for _ in cols {
        print!(" │ {:>NUM_W$} {:>NUM_W$} {:>NUM_W$}", "p50", "p99", "max");
    }
    println!();
    println!("{rule}");
    for (row, (label, ..)) in first.latencies.iter().enumerate() {
        print!("{label:<label_w$}");
        for col in cols {
            let (_, unit, [p50, p99, max]) = &col.latencies[row];
            let (p50, p99, max) = (unit.format(*p50), unit.format(*p99), unit.format(*max));
            print!(" │ {p50:>NUM_W$} {p99:>NUM_W$} {max:>NUM_W$}");
        }
        println!();
        if WORKLOAD_RULE_AFTER.contains(&label.as_str()) {
            println!("{rule}");
        }
    }
    println!("{rule}");
    for (row, (label, _)) in first.values.iter().enumerate() {
        print!("{label:<label_w$}");
        for col in cols {
            print!(" │ {:>block_w$}", col.values[row].1);
        }
        println!();
        if WORKLOAD_RULE_AFTER.contains(&label.as_str()) {
            println!("{rule}");
        }
    }
}

/// Run one configuration: build its outputs on a thread, save them on a fresh chain beside the
/// readers, then run the readers alone; return its report column.
fn run_configuration(
    rt: &Runtime,
    knobs: &WorkloadKnobs,
    shape: OutputShape,
    pool: &Arc<TxPool>,
) -> WorkloadColumn {
    let outputs = knobs.outputs_for(shape);
    let dir = tempfile::tempdir().expect("temp dir");
    let (outputs_tx, mut built) = mpsc::channel(BUILD_AHEAD);
    let (committee_tx, committee_rx) = std::sync::mpsc::channel();
    let builder = {
        let pool = pool.clone();
        // Named, so a profile can tell its work from the writer's.
        std::thread::Builder::new()
            .name("pack-build".to_string())
            .spawn(move || build_workload_outputs(shape, outputs, pool, committee_tx, outputs_tx))
            .expect("start the output builder")
    };
    let committee = committee_rx.recv().expect("the builder's committee");
    let epoch = committee.epoch();
    let (writer, reads, data_len) = rt.block_on(async {
        let chain = ConsensusChain::new_for_test(dir.path().to_path_buf(), committee)
            .await
            .expect("open the consensus chain");
        // Untimed: let the builder get ahead before the first slot.
        while built.len() < BUILD_AHEAD.min(outputs as usize) && !builder.is_finished() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let recent = Recent::default();
        let phase = Arc::new(AtomicU8::new(PHASE_WITH_SAVES));
        let kinds: &[ReadKind] = if knobs.readers { &ReadKind::ALL } else { &[] };
        let readers: Vec<_> = kinds
            .iter()
            .enumerate()
            .map(|(i, &kind)| {
                tokio::spawn(run_reader(
                    kind,
                    chain.clone(),
                    epoch,
                    recent.clone(),
                    phase.clone(),
                    knobs.reader_period(kind.per_output(shape.committee)),
                    knobs.batch_request,
                    mix(i as u64 + 1),
                ))
            })
            .collect();
        let writer =
            run_writer(&chain, &mut built, outputs, knobs.rate, knobs.no_slots, &recent).await;
        phase.store(PHASE_READS_ONLY, Ordering::Release);
        if knobs.readers {
            tokio::time::sleep(knobs.read_only).await;
        }
        phase.store(PHASE_STOP, Ordering::Release);
        let mut reads = Vec::with_capacity(readers.len());
        for reader in readers {
            reads.push(reader.await.expect("reader"));
        }
        let data_len = chain.current_data_len(epoch).await.expect("pack data length");
        chain.close().await;
        (writer, reads, data_len)
    });
    let totals = builder.join().expect("output builder");
    drop(dir);
    workload_column(knobs, shape, outputs, writer, reads, data_len, totals)
}

/// Turn one configuration's measurements into its report column.
fn workload_column(
    knobs: &WorkloadKnobs,
    shape: OutputShape,
    outputs: u64,
    mut writer: WriterStats,
    mut reads: Vec<[Vec<Duration>; 2]>,
    data_len: u64,
    totals: BuildTotals,
) -> WorkloadColumn {
    const MB: f64 = (1 << 20) as f64;
    let per_output = |total: u64| total as f64 / outputs as f64;
    let mut latencies = vec![
        ("save ms".to_string(), Unit::Ms, spread(&mut writer.save)),
        ("save: check ms".to_string(), Unit::Ms, spread(&mut writer.save_check)),
        ("save: encode ms".to_string(), Unit::Ms, spread(&mut writer.save_encode)),
        ("save: append+index ms".to_string(), Unit::Ms, spread(&mut writer.save_append)),
        ("save: outside the pack ms".to_string(), Unit::Ms, spread(&mut writer.save_outside)),
        ("persist ms".to_string(), Unit::Ms, spread(&mut writer.persist)),
        ("persist: pack sync ms".to_string(), Unit::Ms, spread(&mut writer.persist_pack)),
    ];
    if !knobs.no_slots {
        latencies.push((
            "persist: slot syncs ms".to_string(),
            Unit::Ms,
            spread(&mut writer.persist_slots),
        ));
    }
    latencies.push(("save+persist ms".to_string(), Unit::Ms, spread(&mut writer.forward)));
    let mut values = vec![
        (
            "outputs saved".to_string(),
            if outputs < knobs.outputs {
                format!("{outputs} (data cap)")
            } else {
                outputs.to_string()
            },
        ),
        (
            "outputs/s achieved".to_string(),
            format!("{:.2}", outputs as f64 / writer.elapsed.as_secs_f64()),
        ),
        ("writer waited on builder".to_string(), writer.builder_waits.to_string()),
    ];
    let phase_secs = [writer.elapsed.as_secs_f64(), knobs.read_only.as_secs_f64()];
    for (kind, [with_saves, reads_only]) in ReadKind::ALL.iter().zip(&mut reads) {
        let label = kind.label();
        let rates =
            [with_saves.len() as f64 / phase_secs[0], reads_only.len() as f64 / phase_secs[1]];
        latencies.push((format!("{label} us, with saves"), Unit::Us, spread(with_saves)));
        latencies.push((format!("{label} us, reads only"), Unit::Us, spread(reads_only)));
        values.push((
            format!("reads/s {label} (with / only)"),
            format!("{:.1} / {:.1}", rates[0], rates[1]),
        ));
    }
    let batches = (outputs * shape.batches() as u64).max(1);
    values.extend([
        (
            "certs / batches per output".to_string(),
            format!("{} / {}", shape.certificates(), shape.batches()),
        ),
        (
            "txs per output (per batch)".to_string(),
            format!("{:.0} ({:.0})", per_output(totals.txs), totals.txs as f64 / batches as f64),
        ),
        (
            "batch MB per output (BCS)".to_string(),
            format!("{:.2}", per_output(totals.batch_bytes) / MB),
        ),
        ("pack MB per output".to_string(), format!("{:.2}", per_output(writer.pack_bytes) / MB)),
        (
            "batch bytes / pack bytes".to_string(),
            format!("{:.2}", totals.batch_bytes as f64 / writer.pack_bytes.max(1) as f64),
        ),
        (
            "zstd ratio, sampled records".to_string(),
            format!("{:.2}", totals.sampled_raw as f64 / totals.sampled_compressed.max(1) as f64),
        ),
        ("bcs encode ms per output".to_string(), Unit::Ms.format(mean(&totals.encode))),
        ("zstd compress ms per output".to_string(), Unit::Ms.format(mean(&totals.compress))),
        ("pack data MB".to_string(), format!("{:.1}", data_len as f64 / MB)),
    ]);
    assert_eq!(totals.outputs, outputs, "the builder built every output");
    WorkloadColumn { name: format!("{} KB batches", shape.batch_bytes >> 10), latencies, values }
}

/// Production-shaped consensus pack traffic: outputs saved and made durable at the node's pace
/// while readers query the pack, then the same reads alone (see the module docs for the model and
/// the knobs).
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tn-storage --lib -- pack_bench::consensus_pack_workload --exact
/// --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand production-shaped pack benchmark; run with --release --ignored --nocapture --test-threads 1"]
fn consensus_pack_workload() {
    let knobs = WorkloadKnobs::from_env();
    {
        let probe = tempfile::tempdir().expect("temp dir");
        check_bench_filesystem(probe.path());
    }
    // The outputs are in epoch 0, whose header layout the build's fork points decide.
    let on = |active: bool| if active { "on" } else { "off" };
    let (seed, millis) = (forks::seed_signature_active(0), forks::subsecond_timestamp_active(0));
    println!(
        "epoch 0 headers: seed signature {}, millisecond timestamps {} (mainnet: both on)",
        on(seed),
        on(millis)
    );
    if !(seed && millis) {
        println!(
            "note: not the mainnet header layout; with the `test-utils` feature, \
             TN_SEED_SIGNATURE_FORK_EPOCH=0 TN_SUBSECOND_TIMESTAMP_FORK_EPOCH=0 select it"
        );
    }
    let rt =
        tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");
    let len = pool_len(&knobs);
    let signing = Instant::now();
    let pool = Arc::new(TxPool::new(len));
    let pool_bytes: usize = pool.txs.iter().map(Vec::len).sum();
    println!(
        "signed a pool of {len} transactions ({:.0} bytes each on average) in {:.1} s",
        pool_bytes as f64 / len as f64,
        signing.elapsed().as_secs_f64()
    );
    let pace = match knobs.rate {
        0 => format!("as fast as taken (readers paced at {DEFAULT_OUTPUT_RATE}/s)"),
        rate => format!("{rate}/s"),
    };
    for &n in &knobs.committees {
        let mut cols = Vec::with_capacity(knobs.batch_sizes.len());
        for &batch_bytes in &knobs.batch_sizes {
            let shape = OutputShape { committee: n, batch_bytes };
            println!(
                "  running N={n}, {} KB batches: {} outputs ...",
                batch_bytes >> 10,
                knobs.outputs_for(shape)
            );
            cols.push(run_configuration(&rt, &knobs, shape, &pool));
        }
        let persist = if knobs.no_slots {
            "the pack msync alone (TN_BENCH_PACK_NO_SLOTS: the slot syncs skipped, unlike \
             production)"
        } else {
            "persist_current (the pack msync and the latest-consensus slot syncs, together; each \
             half also timed alone)"
        };
        let mut legend = vec![
            format!(
                "outputs: {} headers ({ROUNDS_PER_OUTPUT} rounds x N), {WORKLOAD_BATCHES_PER_HEADER} \
                 batches each, saved at {pace}; save = save_consensus_output (its check, encode \
                 and append+index inside the pack, and the rest outside it), persist = {persist}, \
                 save+persist = the wait before an output is forwarded to execution",
                n * ROUNDS_PER_OUTPUT as usize
            ),
            format!(
                "reads per output interval: {} header_by_digest, {} get_batches of {} batches \
                 ({BATCH_MISS_PERCENT}% absent), {} output_bytes, of the newest {RECENT_OUTPUTS} \
                 outputs; \"reads only\" = the same reads for {} s after the last save",
                ReadKind::HeaderByDigest.per_output(n),
                ReadKind::Batches.per_output(n),
                knobs.batch_request,
                ReadKind::OutputBytes.per_output(n),
                knobs.read_only.as_secs()
            ),
            "compression: every record of a sample of outputs BCS-encoded and zstd-compressed \
             outside the save, as the pack's append does"
                .to_string(),
            "save: append+index includes the wait for the pack's write lock (behind running \
             reads); outputs/s and reads/s with saves divide by the writer's whole run (the \
             readers start with the first save), a slight undercount of both"
                .to_string(),
        ];
        if !knobs.readers {
            legend[1] = "reads: none (TN_BENCH_PACK_READERS=0)".to_string();
        }
        print_workload_table(&format!("consensus pack workload: N={n}"), &legend, &cols);
    }
}
