# tn-storage: a guide for working in this crate

`tn-storage` holds the node's consensus storage:
- **per-epoch pack files** of consensus output (`ConsensusPack`, behind `ConsensusChain`);
- **the epoch-record and certificate logs**;
- **execution-state snapshot packs**;
- **the `Database` key/value backends**, with tndb as the node's consensus DB.

**Read in this order:**
1. this file;
2. the README section for your topic (headings below);
3. the module docs at the top of the file.

**Three files are too big to read whole:**
- `consensus_pack.rs` is 12.5k lines;
- `consensus.rs` and `epoch_records.rs` are about 5k each.

Each starts with a "Where things are" list. Grep for the symbol, read with an offset and limit,
and remember that everything after `mod test` is tests (more than half of `consensus_pack.rs`).

**Design docs:**
- **`README.md`:**
  - "Layered architecture" §1–7: the byte, record and index layers, `ConsensusPack`, the recovery
    model (four invariants), `ConsensusChain`, and the key/value stores;
  - "On-disk layout", "Tooling" (`db validate/repair/migrate`), "Benchmarks";
  - "Intentional design decisions — please do not flag these".
- **`src/tndb/README.md`:** Layout, Records, Writes, Reads, Durability, Transactions, Recovery,
  Tests, Compaction, Production shape (`split.rs`), Against MDBX, Metrics, Inspection, Limits and
  choices, Not yet.
- **`src/archive/btree_index/README.md`:** the B+tree index (pages, CRC regime, snapshots, page
  reuse).

## Crate map

| file | role |
|---|---|
| `archive/data_file.rs` | `MmapDataFile`: the byte layer (mmap, growth, msync commits, the clean-close sentinel, the commit marker, `fsync_file`/`fdatasync_file`) |
| `archive/pack.rs` | `Pack<V>`: CRC-framed, optionally zstd-compressed records; `RecordEncoder` + `append_stored` |
| `archive/pack_iter.rs` | sequential record iterators (sync and async) over a data log |
| `archive/{position_index,digest_index,btree_index}/` | derived indexes: number → position, digest → position (`HdxIndex`, a hash index with a bloom), ordered keys (`BtreeIndex`, copy-on-write) |
| `archive/{crc,page_set,fxhasher,index}.rs` | CRC32 (aarch64 PMULL path), dirty-page sets, the stable hasher, the `Index` trait |
| `consensus_pack.rs` (**12.5k**) | `ConsensusPack`: one epoch's outputs: save, reads, recovery, legacy migration, import from peers, output decode |
| `consensus.rs` (**5.5k**) | `ConsensusChain`: the current pack and the static-pack cache, epoch handoff, import and staging, the `LatestConsensus` slot files |
| `epoch_records.rs` (**5.1k**) | `EpochRecordDb`: epoch records and certificates (an actor thread) |
| `certificate_pack.rs` | the per-epoch certificate pack (an actor thread) |
| `exec_state_pack.rs` | the EVM state snapshot pack |
| `pack_validate.rs` | read-only pack validation (`db validate`) |
| `tndb/` | the shared-log `Database` (see its README): `store.rs` (log + indexes, the one write lock), `commit.rs` (the commit thread), `database.rs` (`TnDatabase`), `split.rs` (`SplitDatabase`, one log per environment), `compact.rs`, `record.rs`, `layout.rs`, `manifest.rs`, `metrics.rs`, `inspect.rs`, `test.rs` |
| `lib.rs` | the tables (`mod tables`), `DatabaseType = tndb::SplitDatabase`, `open_db`, the MDBX → tndb migration (`migrate_from_mdbx`) |
| `stores/` | typed stores over `Database` (certificates, payload, proposer, votes) |
| `mdbx/`, `layered_db.rs`, `composite_db.rs`, `mem_db.rs` | the previous backend and its layers, kept only as the migration source and for tests and benches; `MemDatabase` is the test backend |
| `*_bench.rs`, `archive/index_bench.rs` | `#[ignore]`d benches (README "Benchmarks") |

## Production paths (the hot ones)
- **An output's critical path:**
  1. state sync calls `ConsensusChain::save_consensus_output`, which runs `Shared::save` in
     `consensus_pack.rs`;
  2. `persist_current` runs the pack msync and the slot syncs together;
  3. only then is the output forwarded to execution.
- **What a save does.** `Shared` has two locks: the `writer` mutex, which orders saves, persists
  and the close, and the `inner` `RwLock`, whose write side a save holds only to append and index.
  A save:
  1. checks the output under the read lock (`check_save`);
  2. encodes it holding only the `writer` mutex (`PreparedOutput::encode`, whose `OutputEncoder`
     lanes compress a large output's batches on several threads);
  3. appends and indexes under the `RwLock`'s write side (`append_prepared`).
- **Hot reads:**
  - the last executed header's number, leader epoch and round for every gossiped certificate
    (`with_consensus_header_by_digest`: a hit on the recent headers `ConsensusChain` keeps
    copies nothing);
  - `get_batches` for peers and cache misses;
  - output bytes for syncing peers.

  Reads take the read lock on the blocking pool, beside saves.
- **The consensus DB:** `open_db` opens a `SplitDatabase` with one tndb log per environment:
  - **epoch:** certificates, votes, payload and proposer state. Every round writes certificates,
    votes (each awaiting a `persist`), the proposer's header, and payload rows.
  - **kad:** peer records.
  - **cache:** the batch cache, one insert per batch, evicted after each output.

  Group commit: `persist` acknowledges once the writes before it are durable.

## Invariants: do not break these
- **Destructive recovery fails closed.** Validate the data log (the WAL) before deleting indexes
  (`recover_pack`, `check_recoverable`). An open that would initialise a file must first prove it
  holds nothing past its header. Never let a failed attempt or a corrupt proxy authorize erasing
  committed data.
- **The clean-close seal is gated.**
  - `MmapDataFile`'s `Drop` seals only when the file is not `opened_unclean`, not poisoned (any
    failed write, sync, grow or remap sets the latch), and not dropped during a panic.
  - Every recovery or heal must `mark_consistent` its data log and every index file, or the pack
    rebuilds on every open.
- **Persist on the hot path is the data log's msync, nothing more.**
  - Indexes are derived and rebuilt from the log.
  - Durable watermarks go in the commit marker (`stamp_commit_marker`, best effort), never as
    extra fsyncs per persist.
- **macOS:** never `File::sync_all` (it is `F_FULLFSYNC`, 10–20 ms). Use
  `archive::data_file::{fsync_file, fdatasync_file}`. A plain fsync is used on macOS; Linux is
  unchanged.
- **Mmap growth preallocates real blocks,** so a full disk is an `io::Error`, not SIGBUS. Open
  files carry up to 128 MiB of padding, trimmed at a clean close.
- **`ConsensusPack` locking:**
  - nothing may hold a reference into a mapping past the read lock (copy or decode under it;
    `seal_now` depends on this);
  - a panic inside the write lock (`Shared::mutate`) takes the state out and drops it unsealed,
    so the next open recovers it;
  - a failed save rolls back, and the next open rebuilds the indexes (README design decision 17).
  - `CertificatePack`, `EpochRecordDb` and `LatestConsensus` stay actor threads by decision: their
    traffic is too low for locks to pay, and an actor keeps their order, backpressure and panic
    containment. `EpochRecordDb`'s by-number routing (`final_numbers`) is updated on its thread,
    after a successful save.
- **Encode changes must keep the bytes on disk identical.** Prove it the way
  `stored_append_matches_append` and `test_split_encoding_matches_one_lane` do.
- **Sub-dag serde is fork-gated** (fields written only after their fork). Cache what a pack read
  returned, never a header built in memory.
- **Codecs:**
  - BCS can't carry alloy serde types (`Header`, genesis accounts): use RLP or plain wire
    structs;
  - a variable-length `Vec<u8>` must use `tn_types::byte_vec`/`ByteVec`, or bcs encodes it one
    byte at a time.
- **Formats and backends:** tndb is pre-release, so its formats and paths change freely, with no
  migration shims. MDBX remains only for the one-release migration (`migrate_from_mdbx`); delete
  it the release after.
- **Packs whose last handle drops on a multi-thread runtime seal on the blocking pool.** In tests
  and benches, `close().await` a pack before reopening its files.

## Reviewing and editing
- **Before flagging anything,** read the README's "Intentional design decisions — please do not
  flag these".
- **Comments and docs use durable anchors only** (function and test names, real issue numbers),
  never review finding ids or "the report".
- **When behaviour changes,** update the README section, `src/tndb/README.md` or the B+tree README
  that describes it.

## Testing
- **Storage tests:** `cargo test --release -p tn-storage --lib` (about 600 tests, seconds in
  release). One test: add `-- <path::to::test> --exact`.
- **Lanes:** `./etc/ci-lanes.sh clippy` (default and `--all-features`) and
  `./etc/ci-lanes.sh test-default` (nextest, the workspace).
- **End to end:** `make test-restarts` and `make test-epochs` (they build the e2e binary first).
  Filter a single e2e test with `--exact`: `test_epoch_sync` also matches the fork variants, which
  pin fork env vars for the whole process.
- **Pack test helpers** (in `consensus_pack.rs` `mod test`):
  - `pack_with_outputs`, `make_test_output`, `make_wide_test_output`, `batches_by_digest`;
  - `CommitteeFixture` and `MemDatabase` for committees, `TempDir` for files.
- **Pack test hooks:** `fail_next_save` (rollback), `panic_next_save` (the unsealed panic path),
  `set_encode_lanes` (force the split encode).
- **Check a new test with a mutation:** break the code it guards, see it fail, restore.

## Benchmarks
- **The catalog:** the README's "Benchmarks" section (what each bench measures, its command and
  its knobs).
- **How to run them:**
  - always `--release`, with `TMPDIR` on the disk under test (on Linux the workload and pack
    benches refuse tmpfs and ramfs);
  - macOS fsync and msync don't flush the drive, so durability numbers come from Linux.
- **Two scripts:**
  - `etc/storage-baseline-bench.sh <dir>` runs the workload suite plus
    `pack_bench::consensus_pack_workload`, with one report;
  - `etc/storage-ab-bench.sh <dir> <before-commit> [path]` runs one bench on a commit and on this
    checkout, before/after/after/before (separate builds, checked to differ, one report).
