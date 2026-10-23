# `tn-storage`

Persistent storage for telcoin-network. Two independent families live here:

1. **Append-only pack files** (`archive/`, `consensus_pack`, `epoch_records`, `certificate_pack`,
   `exec_state_pack`) — the mmap-backed, CRC-checked, compressed WALs that hold consensus output,
   the epoch record/certificate chain, and EVM state snapshots. This is the crate's centre of
   gravity and the focus of most of this document.
2. **Key/value stores** (`tndb`, `mdbx` with its `layered_db` and `composite_db`, `mem_db`,
   `stores/`) — a `Database` trait over tndb (the node's) or reth's MDBX (the release before, now
   only the migration's source and a test backend), used for the primary/worker's live consensus
   state (certificates, payloads, votes, proposer state) and libp2p/Kademlia records.

> **Reviewers & automated scanners:** several patterns below look like bugs but are deliberate. See
> [Intentional design decisions](#intentional-design-decisions--please-do-not-flag-these) before
> filing findings.

---

## Layered architecture (pack files → full consensus)

```
consensus.rs        ConsensusChain            epoch-aware store: current (writable) epoch,
                        │                      sealed past epochs (read-only), state-sync import,
                        │                      epoch handoff, the epochs record/cert chain
        ┌───────────────┼───────────────────────────────┐
        ▼               ▼                                ▼
consensus_pack     epoch_records                 certificate_pack / exec_state_pack
ConsensusPack      EpochRecordDb                 (one pack per epoch / per snapshot)
(one per epoch)    (epochs.pack + certs)
        │               │
        ▼               ▼
   archive::pack  ─────────────────────────────  Pack<V>: typed append-only record log
   ( +position_index .pdx, digest_index .hdx/.odx: derived byte-offset indexes )
        │
        ▼
   archive::data_file ─────────────────────────  MmapDataFile: the raw mmap-backed byte file
```

Each layer only knows about the one below it. The **data file is the source of truth**; everything
above it (indexes, the position/digest sidecars, the in-memory caches) is *derived* and rebuildable.

### 1. `archive::data_file` — `MmapDataFile` (the byte layer)

A single memory-mapped, append-only file. Reads/writes are `memcpy` against the mapping (no per-IO
syscalls, no read/write buffers). It exposes `Read`/`Write`/`Seek` plus `slice`, `set_len`,
`try_clone`, `sync_all`, and the growth/durability machinery.

- **Growth & transient padding.** mmap cannot write past EOF, so the physical file is sized *ahead*
  of the data (geometric growth). While appending, `capacity >= end` where `end` is the logical data
  length; the region `[end, capacity)` is zero padding. On a cleanly-closed file `end` excludes the
  padding, so ordinary reads (bounded to `end`) never observe it as data. The documented exceptions
  are the recovery/validation scan on an `opened_unclean` file — where `end` is left at physical EOF
  and the padding is deliberately read (as CRC-failing records) to find the trim point — and
  `AsyncPackIter` (intentional-design item 12). Growth preallocates real blocks for the new range
  (`allocate_range`: `fallocate` on Linux, `F_PREALLOCATE` on macOS), so a full disk fails the
  growing write with an `io::Error` instead of a later SIGBUS on a store into an unbacked page. On
  Linux that needs ext4 or xfs: where `fallocate` is unsupported (`EOPNOTSUPP`/`ENOSYS`: ext2/ext3,
  NFS, many FUSE filesystems) growth falls back to a sparse `set_len`, and a full disk can SIGBUS;
  copy-on-write filesystems (btrfs, ZFS) accept `fallocate` without guaranteeing a later write's
  space. Data already synced is kept either way.
- **Clean-close sentinel.** On a clean `Drop` the file is truncated to `end` and an 8-byte sentinel
  (`crc32(end)` ‖ `crc32` of those 4 bytes) is appended and fsync'd. On reopen the sentinel is
  validated against the physical size and stripped; a missing/invalid sentinel means the file was
  **not** cleanly closed (a crash) — `opened_unclean()` reports that and the logical end is left at
  physical EOF for the recovery scan to trim. Trailing zero padding can never masquerade as a
  sentinel (`crc32(0x00000000) != 0`).
- **Durability.** The default barrier is `msync` (flush dirty pages). A *size extension* is not
  `fsync`'d in the grow path: the next durability barrier fsyncs the size if the file grew since the
  last fsync (`sync_size_if_grown`), so a barrier still promises data `[0, end)` durable within a
  durable size, and one with no growth since stays msync-only. A derived index file
  (`MmapFileOptions::derived`: the position indexes and tndb's B+trees) skips that fsync and leaves
  its size to the clean-close seal. The digest indexes (`.hdx`/`.odx`) are not derived in this
  sense: the epoch records' and certificate pack's recoveries read an unsealed hdx's attested
  length as evidence to refuse a cut, so its size must not lag its header. `sync_disk` is the full
  msync+fsync.
- **Read-only handles** (`open(.., read_only=true)`) map a *sealed* file and clamp their read bound
  (`set_read_bound`) to the index-attested length as defense against a writer truncation.

### 2. `archive::pack` — `Pack<V>` (the record layer)

A typed append-only record log over an `MmapDataFile`. Each record on disk is
`u32 size ‖ payload ‖ u32 crc32`; payloads are the encoded `V` (via `tn-types`) and optionally `zstd`-compressed
(`MAX_RECORD_SIZE` caps both the framed size and the decompressed size — a decompression-bomb guard).
A 28-byte `DataHeader` (`DATA_HEADER_BYTES`: type, version, uid, appnum, compression, CRC32) leads every file and
is validated on open. `raw_iter` walks the log using **only** the data file (no indexes), bounded to
the clone-time logical `end` — this is the authoritative replay source for index rebuilds.

### 3. The derived indexes

- **`position_index` (`.pdx`)** — a fixed-stride array mapping a monotonic integer (consensus number)
  to byte offsets. A misaligned trailing record (torn write) is truncated back to record alignment on
  a writable open.
- **`digest_index` (`.hdx` + `.odx`)** — a memory-mapped hash index: `index.hdx` holds fixed-offset
  hash buckets (each CRC-trailed), `index.odx` is an append-only overflow log, and a bloom filter
  fronts negative lookups. It stores a `data_file_length` commit marker (written *last* on sync) used
  to detect a lagging/torn index.

- **`btree_index` (`index.btx`)** — a copy-on-write, memory-mapped B+tree over fixed-size keys
  (sorted lookup, ranges, prefixes; lock-free published snapshots), used by `tndb`. See
  [`archive/btree_index/README.md`](src/archive/btree_index/README.md).

All of these index types are **fully reconstructable from the data file** and are never trusted
over it.

### 4. `consensus_pack` — `ConsensusPack` (one epoch of consensus output)

An epoch's pack directory `epoch-{N}/` contains `data` (the WAL), `idx/` (position index),
`hash/` + `bhash/` (consensus-header and batch digest indexes), plus the per-epoch certificate pack
(`cert_data` + `cert_hash/`, a `CertificatePack`). Records are a leading
`EpochMeta` (committee, epoch-start linkage) followed by, per output, a `Consensus` header and its
`Batch` records. The public type is `Send + Sync + Clone`: a handle on the pack's shared state, with
no thread of its own.

**Reads and writes** (as tndb's readers taught: no hop and no wait behind a writer):
- **Reads** take the state's read lock, any number at once, and run on tokio's blocking pool, or on
  the caller for a constant-time lookup. They never wait for a save's encoding or a persist's sync,
  and each copies or decodes what it returns before letting go of the lock (see `seal_now`).
  `get_consensus_output` copies the output's bytes under the lock and decodes them after releasing
  it, still on the blocking pool. A cancelled `batches` request stops at the batch it is reading.
- **Writers** (a save, a persist, a flush, the close) take a writer lock, one at a time. A save,
  persist or flush that the OS failed returns `PackError::IO` with its `io::ErrorKind` kept (a full
  disk is `StorageFull`).
  - **A save** checks the output under the read lock, encodes and compresses its records under the
    writer lock alone, then takes the write lock only to append them (`Pack::append_stored`) and
    index the output. A reader sees an output whole or not at all, and a failed save rolls back
    under that same lock. The encoding (`OutputEncoder`) reuses its buffers from save to save (at
    most 16 MiB of them between saves, each lane's staging buffer counted, plus each lane's zstd
    context, about 3.5 MiB, fixed by the compression level), and
    splits a large output's batches, in order, across threads that compress at once (each record
    is compressed on its own, so the bytes are the same).
  - **A persist** syncs under the read lock and stamps the commit marker under a brief write lock.
  - **A panic** inside the write lock takes the state out, unsealed (the next open recovers it), and
    every later use of the pack fails.
- **Closing** takes the state out under the write lock and seals it; a read after that fails.
- **`ConsensusChain::with_consensus_header_by_digest`** remembers the last few headers it read,
  each with its epoch and digest, since the primary asks for the last executed one on every
  gossiped certificate. A hit runs the caller's function on the remembered header (the primary
  reads three fields), so it copies and re-hashes nothing; `consensus_header_by_digest` is the
  same lookup returning a copy.

**Open doors:**
| door | mode | on damage |
|------|------|-----------|
| `open_append` | writable, creates | header-only ⇒ write+fsync meta (a pre-v2 data log holding only its header is removed with its empty indexes and the epoch's pack started afresh); a legacy (pre-v2) data log that reaches the writable open unmigrated (its read-only check failed) is refused with `InvalidVersion` before anything is written, and the next open migrates it; an existing meta must match the chain-derived one on the fields `verify_epoch_meta` authenticates (epochs, start number, genesis states, committee BLS keys), else refuses without writing a meta; then recover. The rest of the on-disk committee is not compared, and the opened pack keeps the chain-derived meta |
| `open_append_exists` | writable, must exist | a legacy data log that reaches the writable open unmigrated is refused with `InvalidVersion`, as for `open_append`; recover (truncate torn tail + rebuild indexes); loads the on-disk meta, so after a restart an imported epoch's committee fields outside the BLS keys are the peer's copy again, even if `open_append` reopened it with the chain-derived one |
| `open_static` | read-only (sealed past epoch) | refuses; `ConsensusChain::get_static` then heals read-side (see below): rebuilds derived indexes from the WAL if the data log is clean, migrates a legacy pack; a heal refuses (`CorruptPack`, remembered by the back-off) a pack whose leading record is not this epoch's meta, since `open_static` would still refuse it; **refuses** (→ `db repair`) if the data log itself is torn |
| `stream_import` | writable, from a peer/byte stream | verify + append + fsync meta, then each output streamed record by record up to the certified `final_consensus`, whose digest is checked when its header is read (before its batches); earlier outputs are trusted through the parent links back from it (always written as v2, from a v1/v2 source; a v0 source is refused as `InvalidVersion`, no penalty). Only the stream reads run on the caller's task; each record's decompress, decode, digest and re-encode, and every write, run on the blocking pool. A failed or abandoned import is discarded (files removed, never sealed). Of the meta's committee only the BLS key set is authenticated; its other fields are the peer's and must not feed consensus-critical logic. A later `open_append` of the epoch compares only that authenticated subset, so the peer's other fields never block the reopen |

### 5. Recovery model — four invariants

The recovery paths (`recover_pack`, `files_consistent`, the open doors, `pack_validate`) uphold:

- **INV1 — recover from truncation, error on corruption.** In an *unclean* (crash-interrupted) log a
  torn/partial trailing record or trailing padding is truncated back to the last complete output and
  the node continues; in a *cleanly-sealed* log the clean-close sentinel proves the log is complete,
  so **any** CRC failure is at-rest corruption and a hard `CorruptPack`. `recover_pack` replays the
  WAL index-free, tracks `consistent_end`, and decides truncate-vs-error from the data alone: a torn
  tail is truncatable unless a later complete *output* decodes past it (`output_after_tear`) —
  committed data, so it errors — and a best-effort commit marker (written by `persist()` to the mmap
  capacity tail) catches at-rest corruption of the last committed output. A disk read error is never
  a torn tail: every walk (recovery, the legacy migration, the heal, `db validate`/`repair`) returns
  `ReadError` and cuts nothing, and `recover_pack`'s second pass must end where its first did. The
  zeros a cut writes over the tail are flushed before the open returns, so a power loss can't bring
  the cut bytes back.
- **INV2 — headers & meta are clean-or-error.** The `DataHeader` and the leading `EpochMeta` are
  expected present and correct; a corrupt/torn one is an **error** surfaced to the operator, **never
  repaired**. The meta is committed the instant it is written (`Pack::commit`: msync'd, its size
  extension fsync'd), so a torn meta is not a normal state.
- **INV3 — indexes are rebuildable.** Any index anomaly (corrupt/torn/missing index, stale marker,
  unclean seal) triggers a full rebuild from the WAL — including a corrupt index *header* that won't
  open (the writable doors discard and recreate the index that failed, then rebuild all of them; an
  environmental open failure such as fd/memory exhaustion is surfaced instead). Index problems never
  brick a writable open.
- **INV4 — the data file is the source of truth.** Recovery replays the data log to rebuild indexes;
  indexes never override the data file, no durably-acked data is dropped, and reads/serving are
  bounded to the logical length.

`pack_validate` (the `db validate` diagnostic) classifies a damaged data file as `TornTrailingTail`
(an unacked torn tail — truncated and repaired) or as needing a re-sync: `TornMetaEmpty` (a torn
epoch-meta is never repaired — INV2), `CorruptMetaWithData`, `MidLogCorruption`, `CorruptSealedRecord`
(data loss). Only a `TornTrailingTail` exits `db validate` with success. Its read-only recovery checks
(shared with `db repair`'s dry run) read the position index's entries straight from the file, so an
index a crash left unsealed attests the same output boundaries it does to the writable open.

### 6. `consensus.rs` — `ConsensusChain` (full consensus store)

Ties the epochs together under `<datadir>/consensus-db/epochs/`. It opens the **current** epoch
writable (`open_append_exists`, which runs recovery on restart), serves **sealed past** epochs
read-only via `open_static` (behind a small least-recently-used `recent_packs` cache; a legacy or index-damaged sealed epoch
is healed read-side first — built off the async runtime and outside `pack_install` into a side directory,
data log opened read-only, then swapped in by rename), imports a full epoch from peers with
`stream_import` (into an `import-{N}/` dir, then an install-locked rename to `epoch-{N}/`; a read-only
partial-prefix pack instead stages under its own `staging-{N}-{n}/` and is never renamed; either import
authenticates the record's final output itself and stops with a local `StorageFull` error, not charged
to the peer, before free space could drop below a floor (each check needs `IMPORT_FREE_HEADROOM` above
it for every floored import in flight in the process, for the growth their files may preallocate before
their next checks: the epoch fetchers and an observer's staging import can run several at once onto the
live stores' filesystem), since the peer's bytes
are authenticated only at the final header; `db load-state`'s restore of a local bundle has no floor;
the imported pack is sealed before `pack_install` is taken, and the live (writable) epoch is replaced
only with its stored record's chain, anything else, such as a prefix's partial final, being refused as
`LiveEpochImport`), and drives epoch handoff.
`LatestConsensus` persists the tip `(epoch, number)` in double-buffered, CRC-checked
`consensus_slot{1,2}` files — a non-authoritative hint; the pack files are ground truth.

`epoch_records` (`EpochRecordDb`) is the singleton chain of `EpochRecord`s (`epochs.pack`) and their
`EpochCertificate`s (`epoch_certs.pack`), auto-healed on open under the same rules: `persist()` stamps
each log's tail commit marker, a rebuild refuses to truncate below it (or below the digest index's
synced length), and an empty position index beside a populated digest index is rebuilt, not trusted.
The open scans every bucket of both (small) digest indexes and rebuilds on a damaged one, and a
failed save's index invalidation holds until a reopen rebuilds it. A log that holds no record beside
a digest index attesting acked ones was lost (absent counts as corrupt): the open and `assess`
refuse it (`CorruptLog`), judged by content so a retry refuses too. A read the disk fails during
recovery is an error (`Unreadable`, a read error rather than corruption), never a torn tail, and
nothing is cut. Recovery and save rollbacks flush the zeros they write over a cut tail.
Each write returns its own result to its caller (the handle is shared by concurrent writers, so a
failure is never reported to someone else), and closing persists before the files are sealed. A stored
epoch's record is never replaced: re-saving the same record is a no-op, and a different one is refused
(`ConflictingRecord`), since epoch records are identical on every node. A stored certificate counts
only if it reads back: one that does not is not certified (`first_missing_historical_cert` stops at
it), and a re-save reports it (`Unreadable`) rather than skipping or replacing it; the cure is a
re-sync. The by-number routing follows only what is stored: it is built at open and extended after
each save by the same checked read (`extend_routing`), and a stored record that does not read back
fails the open, naming its epoch. `certificate_pack` and `exec_state_pack` are per-epoch / per-snapshot packs for
certificate bundles and EVM state exports.

### 7. Key/value stores (the other family)

A `Database` trait (in `tn-types::database_traits`: tables, read/write txns, `get`/`insert`/`remove`/
`iter`/`skip`) with two persistent backends: **`tndb`** below, the node's (`DatabaseType`), and
**`MdbxDatabase`** (reth's MDBX, behind the default `reth-libmdbx` feature), the consensus DB of the
release before tndb, now kept only as the migration's source and for tests and benches (the next
release removes it). The layers that release put over MDBX live in the `mdbx` module and go with
it: `LayeredDatabase` (`mdbx::layered_db`) adds a write-through in-memory layer and a shared-txn
guard, and `CompositeDatabase` (`mdbx::composite_db`) splits the workload into `epoch` / `kad` /
`cache` sub-databases routed by a table hint. A `redb` backend was removed: it was the slowest, and tndb
replaces it. `MemDatabase` is an in-memory backend for tests. The typed `stores/` (`certificate_store`,
`payload_store`, `proposer_store`, `vote_digest_store`) wrap the trait for primary/worker state.

**`tndb`** (`TnDatabase`) is a `Database` backend built on this crate's own pack files
and B+tree index: every table of a database in one shared, CRC-framed log (one sync per commit,
transactions atomic across tables), lock-free snapshot reads, crash recovery by replaying the log,
and compaction into a new generation. It is the node's `DatabaseType` (`SplitDatabase`: one log
per `epoch` / `kad` / `cache` environment, routed by each table's hint), and the first open moves an
existing MDBX consensus DB into it (the epoch and kad tables and both batch caches;
`ConsensusCache` refills), then deletes the MDBX files. Every open then checks that the epoch
tables' headers decode with this binary (`epoch_db_recovery`, #1572): it drops the seconds-only
`LastProposed` header a late upgrader proposed for the sub-second fork epoch and refuses any other
undecodable row. It replaced
an earlier per-table engine of the same name, whose separate files cost a sync per table per
commit (this engine was called `tnlog` while the two were compared). It exports
`tn_storage_tndb_*` metrics, and `telcoin-network db tndb-stat` inspects it at rest. See
[`src/tndb/README.md`](src/tndb/README.md).

### 8. Write failures stop the node

**The rule.** When this node's own storage fails, the node exits non-zero and waits for an
operator. That covers an IO or sync error, a latched store, damage found on disk, and a store that
refuses writes. It never logs the failure and carries on. A node that keeps running on storage it
can't write either forgets what it acknowledged (a vote or proposal lost across a restart is
equivocation) or quietly stops making progress. An exit is visible: it prompts the operator to
free disk, replace a drive or re-sync. The restart then runs the open-time recovery, which heals
what can be healed and refuses what can't.

**The boundary.** Refusing a peer's bad input never stops the node; otherwise any peer could stop
it remotely. These are refused and logged, never fatal:
- an invalid, conflicting or oversized record, certificate or output from the network;
- a downloaded epoch record that fails `validate_downloaded_record`;
- a certificate that fails verification (checked before consensus sees it);
- an import that fails verification.

One known exception, which predates tndb and is tracked separately: a committee member can send a
flood of distinct, valid batches. Each is written to `NodeBatchesCache`, which has no byte budget
and keeps uncommitted batches until the epoch closes, so the flood can fill the disk. The cache
environment's write then fails, its `Durability` latches, and the node stops like any other
storage failure. (The MDBX cache of the release before stopped the node at its fixed 1 GiB
maximum.)

**How the stores report it.** A failure latches, so every later write fails the same way:
- **tndb:** `Durability`. A failed write or sync, a stopped writer, or damage a read found;
  `Database::failed()` resolves once any environment latches.
- **The packs:** `MmapDataFile`'s poison and `ConsensusPack`'s failed state.
- **`CertificatePack`:** its stored error, returned by every later `try_save`, `persist` and
  `shutdown`.
- **`EpochRecordDb`:** each write returns its own result.

**What stops the node:**
- **The consensus DB, mid-epoch:** `run_epoch` waits on `Database::failed()` (`store_failure`).
  The first environment (epoch, kad, cache) to latch ends the epoch with an error naming it.
- **The certificate pack:** any `try_save` error except `SendFull` (backpressure) ends the
  critical consensus task (`ConsensusError::CertificatePack`).
- **The epoch end (`finish_epoch`), after the epoch's tasks are joined:**
  1. The certificate pack is sealed. `CertificatePack::shutdown` reports a background save or a
     final flush that failed.
  2. The consensus pack is persisted. This comes before the leftover drain because a save
     advances `latest_consensus_number` before its persist, so an output whose persist failed is
     never forwarded to execution.
  3. `ensure_stores_durable` persists one table in each consensus DB environment.

  The state export's persist (`export_epoch_state`, at the boundary) returns its error too.
- **A failed output save or persist** ends the critical executor subscriber. A save that was rolled
  back leaves the pack usable, but every later persist of that handle fails (`Shared::persist`), so
  `finish_epoch`'s persist stops the node even for a save that failed in the subscriber's shutdown
  drain.
- **The epoch handoff (`new_epoch`):** the previous writable pack is sealed before the handoff
  returns. If its data log then reads back unsealed, `new_epoch` returns a `PersistError` naming
  the epoch and `db repair`, and `open_epoch_pack` stops the node. A check that cannot open the
  file for a reason the environment caused (descriptor or memory exhaustion) proves nothing about
  it: `new_epoch` returns that error, logged as "could not verify the seal of epoch N", which
  stops the node too but without the `db repair` remedy (a restart re-runs the handoff). The
  check reads the file back
  through the page cache, so it sees a seal that was never written but not a final fsync that
  failed: the seal (`MmapDataFile`'s `Drop`) only logs that. After a power loss such an epoch is
  refused at its next read (`db repair`); its committed data was msynced before the seal, so
  nothing is lost.
- **Epoch records and certificates:** `write_epoch_record` and the startup epoch-0 record save
  return their errors. A local failure is one `EpochDbError::is_local_storage_failure` says is our
  own (an exhaustive match: IO, append, index, open, unreadable, corrupt, a gone actor):
  - saving or persisting a quorum certificate or a recovered record: the vote round passes the
    error to the critical "Epoch Vote Collector";
  - saving or persisting a downloaded record: it ends the critical "Epoch Record Collector", or
    fails startup in `sync_epoch_records_to_tip`.

  A record or certificate the store refuses (a conflicting record, a certificate for another
  record, an out-of-order epoch) can come from a peer, so it is only logged and requested again
  later.
- **Startup:** a datadir that can't be opened aborts node start (decision 11).

Each error returns `Err` from `run_epoch`, then from `run_epochs`, `EpochManager::run` and
`launch_node`, and the binary exits 1. A critical epoch task's error ends the epoch's task manager
with `Err`, and a critical node task's (the epoch-vote and epoch-record collectors) ends the node's,
and either takes the same path. An epoch task's error that arrives after the epoch's shutdown is
notified is only logged, which is why `finish_epoch` re-checks the packs and stores itself.

**Two exceptions:**
- **The `LatestConsensus` slot files.** They are a non-authoritative hint; the packs are ground
  truth. A failed slot write or fsync is logged, and `persist_current` still returns Ok. A hint
  behind execution (both slots damaged fall back to epoch 0, or a lost epoch change) does not stop
  a restart either: mid-epoch, startup's `count_leaders` reopens the executed epoch's pack as the
  current one, with a warning; at a boundary, `new_epoch` opens the right epoch.
- **A pack import from a peer.** An import writes only its own directory, which is removed on any
  failure and never touches installed data. So a failed import is not installed, and the fetcher
  tries another peer, whether the fault was the peer's stream or a local write (an IO error there
  can be either). An imported pack that fails its seal check is one of these (the check has the
  handoff's limit: it reads through the page cache and cannot see a failed final fsync). A disk
  that fails here fails the node's own stores at their next write, and that stops it.

### On-disk layout

```
<datadir>/
  db/                          reth execution DB (MDBX)
  static_files/                reth static file segments
  consensus-db/                tndb/{epoch,kad,cache} (one tndb log per environment) + the epochs/ tree below
    epochs/
      epoch-{N}/               one ConsensusPack per epoch
        data                   the record WAL (source of truth)  ── + clean-close sentinel
        idx/index_pos.pdx      position index (derived)
        hash/{index.hdx,.odx}  consensus-header digest index (derived)
        bhash/{index.hdx,.odx} batch digest index (derived)
        cert_data              per-epoch certificate pack (CertificatePack)  ── + clean-close sentinel
        cert_hash/{index.hdx,.odx} certificate digest index (derived)
        cert_lock              CertificatePack writer lock (flock, held while a writable pack is open)
      epochs.pack / epoch_certs.pack + sidecars   the EpochRecordDb chain
      consensus_slot{1,2}      latest-consensus hint (double-buffered)
      import-{N}/              full-epoch import target (renamed to epoch-{N}/ on install)
      staging-{N}-{n}/         read-only partial-prefix import pack, one dir per import (never renamed)
    state_exports/             EVM state-export bundles
```

### Tooling

`telcoin-network db validate <path>` walks a pack's `data` stream read-only and reports integrity
issues. `telcoin-network db repair [--epoch N] [--force]` repairs epoch packs **at rest** (node
stopped): it truncates a torn tail and rebuilds indexes from the WAL for damaged epochs, dry-run by
default, skipping the current/latest epoch unless named. Meta/mid-log corruption is reported for
re-sync, never "fixed". A read the disk failed, or a failure the environment caused (descriptor or
memory exhaustion, a permission problem, a full disk), is no verdict on the epoch: it is reported as
an error to rerun once the cause is fixed, never as unrepairable. A legacy (pre-v2) pack's torn tail is judged by the migration to v2 (which
drops an unacked tail), so `db validate`, `db repair` and `db migrate` agree on it. Only the newest
epoch's legacy pack can drop an unacked tail: a past epoch (one with a later `epoch-N` directory) was
complete when the next epoch began, so a tail its replay cannot reach is refused as damage by the
read-side heal, `db migrate`, `db repair` and `db validate`, even when its indexes cannot attest its
length. A legacy v0
(batches-first) pack is not walked at all: `db validate` reports a dry run of its migration. `db repair`,
`db migrate`, `db load-state` and `db tndb-stat` refuse to run while a live node holds the
`<datadir>/telcoin.pid` lock (intentional-design item 9).

### Benchmarks

Every bench is an `#[ignore]`d test, run on demand:

```
cargo test --release -p tn-storage --lib -- <path> --exact --ignored --nocapture --test-threads 1
```

- **Always `--release`.**
- **The database files go under `TMPDIR`,** which must be on the disk under test. On Linux every
  bench refuses tmpfs and ramfs, where syncs are free (`pack_bench::check_bench_temp_dir`).
- **macOS `fsync` and `msync` don't flush the drive's cache,** so durability numbers come from
  Linux.
- **Paced runs leave the machine idle between operations,** so on a laptop they include CPU and
  NVMe power-state wake-ups. On the reference Linux laptop (i7-1360P, SK Hynix NVMe), a
  `consensus_pack_workload` persist took 9.3 ms paced at 2 outputs/s and 1.4 ms back to back
  (`TN_BENCH_PACK_RATE=0`), and a save 9.1 ms against 3.5. With NVMe power saving (APST) off and
  the performance CPU governor, the paced run came down to 2.3 ms and 4.2 ms. Compare runs of the
  same pacing, and note the machine's power settings.
- **MDBX columns** need the default `reth-libmdbx` feature.
- **`TN_BENCH_COLUMNS`** (comma separated) narrows the `workload_bench` benches to the named
  columns.
- **Two scripts:**
  - `etc/storage-baseline-bench.sh <dir>` runs the workload suite plus `consensus_pack_workload`
    into one report;
  - `etc/storage-ab-bench.sh <dir> <before-commit> [path]` runs one bench on a commit and on this
    checkout, in before/after/after/before order, with checked-distinct builds and one report.

| path | measures | knobs |
|---|---|---|
| `pack_bench::consensus_pack_workload` | production-shaped `ConsensusChain` traffic: real signed outputs saved and persisted at 2/s while readers look up headers, batches and output bytes; the save (and its parts: check, encode, append+index), the persist (each sync half) and read latencies; compression | `TN_BENCH_PACK_{RATE,OUTPUTS,BATCH_KB,COMMITTEE,MAX_MB,READ_SECS}`; `TN_BENCH_PACK_READERS=0` (the writer alone), `TN_BENCH_PACK_NO_SLOTS=1` (persist the pack without the slot syncs; a measurement only), `TN_BENCH_PACK_BATCH_REQ` (batches per request, default 5) |
| `pack_bench::pack_file_bench` | the `ConsensusPack` operations one at a time on small outputs, a regression baseline by output width | |
| `workload_bench::workload_consensus_rounds` | consensus rounds on the epoch tables at two committee sizes: ms per round, durable vote and header latency | `TN_BENCH_COLUMNS` |
| `workload_bench::workload_batch_cache` | the batch cache at two batch sizes: write MB/s, gets/s with 4 readers | `TN_BENCH_COLUMNS` |
| `workload_bench::workload_startup_reload` | a restart late in an epoch: the reopen time | `TN_BENCH_COLUMNS` |
| `workload_bench::workload_compaction` | a table under churn with tndb compaction off and on: disk, step latency, switch holds | `TN_BENCH_CHURN_SECS`, `TN_BENCH_COLUMNS` |
| `workload_bench::workload_mixed` | N=50 consensus beside a batch stream (256 KB unpaced; 1 MB at 25/s) | `TN_BENCH_COLUMNS` |
| `workload_bench::workload_mixed_compaction` | compaction's cost to consensus, as interleaved off/on pairs (ratios of vote p99 and ms per round) | `TN_BENCH_PAIRS`, `TN_BENCH_PAIR_{ROUNDS,RATE,BATCH_KB}` |
| `workload_bench::msync_concurrency_probe` | how this file system overlaps msyncs of different files | |
| `db_bench::db_backend_comparison` | every `Database` backend through the basic operations | |
| `pack_kv_bench::pack_vs_mdbx_bench` | the pack-file KV against MDBX on raw point KV | |
| `pack_kv_bench::kv_concurrent_read_bench` | concurrent point reads as threads are added, with and without a writer | |
| `archive::index_bench::index_bench` | `HdxIndex` against `BtreeIndex` across index sizes | `INDEX_BENCH_N` (comma list; the default's 8M needs a few GB) |

Each bench module's docs describe its model, columns and caveats in full.

---

## Intentional design decisions — please do not flag these

These recur in reviews and static/AI scans. They are deliberate; flagging them is noise. Where a
guard exists it is named so a reviewer can confirm it, not re-derive it.

1. **`unsafe { Mmap::map / MmapMut::map_mut }` (data_file.rs).** Sound under the single-writer pack
   model (one writable handle per file for its lifetime; read-only handles map only *sealed* files).
   Each block carries a `SAFETY:` comment. Not a memory-safety defect.
2. **Read-only mmap SIGBUS window.** A read-only handle mapping a file a writer could truncate is a
   known hazard, guarded by (a) opening read-only only on *sealed* packs and (b) `set_read_bound`
   clamping the read ceiling to the index-attested length. The `debug_assert_eq!` in `open_static`
   compiles out in release — the clamp is the real guard. Deliberate defense-in-depth.
3. **Trailing bytes past the logical end are not corruption.** mmap capacity padding and the 8-byte
   clean-close sentinel live in `[end, capacity]`; every read/slice/iterator is bounded to `end`. Do
   not flag "file is longer than its data" or "extra bytes after the last record".
4. **CRC32 (not a cryptographic hash) for headers/records/index integrity.** CRC32 detects
   *accidental* corruption (crashes, bit-rot, torn writes) only. Authenticity is enforced separately
   at the consensus layer (BLS/ECDSA signatures over the payloads). Using CRC32 here is correct and
   intentional; it is not a weak-hash/authentication finding.
5. **`recover_pack` truncates the "torn tail" / drops trailing records.** Dropping an *unacked*,
   incompletely-written trailing record is INV1, not data loss. The `output_after_tear` probe (a
   complete output decoding past the tear), the `committed_end` commit marker, and the
   `attested_record_survives` position-index check ensure a tear *below* durably-acked data becomes a
   hard `CorruptPack` error instead, and a read the disk fails is a `ReadError` that cuts nothing.
6. **A torn/corrupt `EpochMeta` (or `DataHeader`) is a hard error, never repaired (INV2).** Do not
   suggest "reconstructing" or "healing" the meta. It is deliberately refused: the committee (with
   network addresses) cannot be reconstructed from the epoch alone, so recovery is a re-sync, not a
   local rewrite. `db repair` reports these as `Unrepairable`.
7. **Indexes are wiped and rebuilt from the data log, never preserved.** `recover_pack`/
   `open_indexes_for_append`/`db repair` intentionally `remove_dir_all` the derived `idx/`,`hash/`,`bhash/`
   directories and replay the WAL. This never touches the `data` log or the chain-data dirs
   (`db`, `static_files`, `consensus-db`); rebuilding a derived index is not destructive.
8. **`msync` as the default write barrier (not `fsync`).** Durability comes from fsync'ing every size
   extension plus the msync default for data within an fsync'd size (see the module docs, incl. the
   macOS `F_FULLFSYNC` caveat). This is a deliberate performance choice, not a durability bug.
9. **Single-writer datadir, enforced by a `telcoin.pid` lockfile.** The node is the sole writer of
   its datadir and holds an exclusive advisory `flock` on `<datadir>/telcoin.pid` for its lifetime,
   recording its PID in it for operators (`tn_config::pid_lock`). Node startup and the at-rest
   writers (`db repair`, `db migrate`, `db load-state`) refuse to run while another process holds the
   lock; the kernel releases it when the holder exits or crashes, so a crash never blocks a restart.
   The file is never unlinked (a release just clears the PID) — deleting it would let two processes
   lock two different inodes at the same path. This guard is TN-owned; it does not depend on the
   execution engine's own database lock. `db repair`/`db migrate`/`db load-state` also take the lock
   for their run, so a node cannot start mid-repair or mid-import. `db repair`/`db migrate`
   stay dry-run by default (`--force` to apply). In all-mode `db repair` skips the current epoch
   (naming `--epoch N` explicitly — including the current/latest epoch — is intentionally allowed
   under the same node-stopped contract), while `db migrate` migrates every legacy epoch, the
   current one included: the same rewrite the node's own open of that epoch runs. The lock is advisory (only TN processes take it) and network
   filesystems with unreliable `flock` are out of scope, so the loud banner and the stop-the-node
   contract remain. Do not flag "`--epoch` bypasses the current-epoch skip".
10. **`db repair` / `open_append_exists` mutate pack files on open; a past-epoch read may rebuild
    derived indexes or migrate a legacy pack.** Truncating a torn tail and rebuilding derived indexes
    from the authoritative log is the *point* (`db repair`/`open_append_exists`, gated by the
    node-stopped contract above). `ConsensusChain::get_static` additionally heals a sealed past epoch
    whose data log is clean but whose index will not open (e.g. after an index-format change), and
    migrates a legacy (pre-v2) epoch whose indexes use the old key placement. The heal runs as its own
    task (a reader that stops waiting does not abandon it), is built on a blocking thread into a side
    directory (the data log opened read-only) that it removes if it is not installed, and is
    serialized per epoch and backed off after a failure; only the final renames run under
    `pack_install`. It never writes the
    data log of a v2 pack — a torn/unclean data log stays terminal (→ `db repair`). Not an "unsafe
    destructive operation".
11. **Fail-fast `expect`/`panic` in DB-open startup paths** (e.g. `open_db`). A datadir that cannot be
    opened is unrecoverable and must abort node start; this is intentional fail-fast, not a library
    `unwrap`.
12. **`AsyncPackIter` has no logical-end bound.** Its callers only feed it *sealed* files or framed,
    length-bounded network streams (state-sync bounds its copy to `data_file_len()`); it is
    documented never to be handed a live, capacity-padded mmap file. Not a "reads past end" bug.
13. **`PackIter::logical_position()` (whole-frame boundary).** Recovery truncates using
    `logical_position()`, advanced only by complete record frames; it is a valid boundary only after a
    successful `next()`, deliberately distinct from a raw byte offset.
14. **`refresh_data_file_end` clears a prior `set_read_bound` clamp.** A documented precondition; no
    production path refreshes a clamped read-only handle. Not a live SIGBUS.
15. **Trailing-CRC "dirty" (zero) sentinel in `crc.rs`.** A zeroed trailing CRC is a deliberate
    "written but not yet CRC'd" marker (`crc_state` distinguishes Dirty from Corrupt), not a missing
    checksum. A Dirty bucket is trusted only for a bucket the current handle actually wrote this
    sync-cycle (`HdxIndex::unsynced_buckets`); a Dirty bucket at rest on a clean/read-only index is
    treated as corruption (a lookup miss in it returns `CorruptIndex`, and a write to it is refused
    rather than laundering it valid).
16. **`eyre`-everywhere error style (`StoreResult<T> = eyre::Result<T>`).** Returning `eyre` errors
    (rather than a bespoke error enum per module) is an intentional, pre-existing convention for this
    crate; not a "define a proper error type" finding.
17. **A rolled-back consensus-output save invalidates the digest commit marker (forces a rebuild),
    it does not surgically restore overwritten index entries.** `rollback_output` rewinds the data
    log (flushing the zeros it writes over the cut, so a power loss cannot bring the failed save
    back; a failed flush poisons the log) + position index, then sets the digest indexes' `data_file_length` to a value that can never
    equal the rewound data length, so the next `open_append` fails `files_consistent` and
    `recover_pack` rebuilds every index from the data-log WAL. This deliberately does not try to
    restore an overwritten duplicate-key position (e.g. a batch already committed by an earlier
    output whose in-place index slot the failed output clobbered). The invalidation is sticky
    (`Inner::index_invalidated`): no later save on the same handle stamps the markers again, so the
    indexes stay untrusted until a reopen rebuilds them, even if the pack saves more and closes
    cleanly. A failed output save is fatal to the executor subscriber (a critical task), and every
    later `persist` of that handle fails (`Shared::persist`), so the epoch end stops the node even
    for a save that failed in the subscriber's shutdown drain; until the restart's reopen, reads stay
    safe because a digest read masks positions past the data end and re-hashes the record it fetches (a
    stale entry is at worst a miss). Corollary: a read-only `open_static` / `db validate` of an epoch
    where any save failed since the pack was last opened will correctly report it inconsistent
    (rebuild pending) rather than silently trust a stale mapping — that is the intended signal, not a
    bug. (Duplicate batches across outputs cannot arise under honest consensus — Bullshark commits
    each certificate once — so this path is defense-in-depth.)
18. **Clean close truncates to `end`, then appends and fsyncs the 8-byte sentinel.** A crash in that
    sub-millisecond window leaves the file at exactly `end` bytes with no sentinel, which the next open
    reads as unclean and recovers with no data loss (`[0, end)` is already durable). Writing the
    sentinel before the truncate was considered; the truncate-first order is intentional, and the CRC
    sentinel makes a padded file masquerading as clean a ~2⁻⁶⁴ event. Not a durability bug.
19. **Recovery reads the WAL through a cloned fd + `BufReader` (pread), not the mmap.** `recover_pack`
    and the index rebuilds walk the data log with syscall reads rather than faulting the whole file
    into the page cache through the mapping, and `PositionIndex::sync` issues a harmless extra
    `MS_ASYNC` (`flush`) before its `MS_SYNC` msync (`sync_all`); as a derived file it makes no size
    fsync (that waits for the clean-close seal). These are deliberate, low-priority performance choices. Not a bug.
20. **Digest-index bucket placement is stable across compiler upgrades by design.** Placement feeds
    the raw key bytes straight to the vendored `FxHasher` (`HdxIndex::stable_hash` → `Hasher::write`),
    deliberately bypassing `impl Hash for [u8]`, whose length-prefix encoding is not stable across Rust
    versions. The salt/pepper marker is derived with the same primitive, so the open-time drift check
    catches a change of hasher and routes it to a WAL rebuild (INV3). It cannot see a change in how
    key bytes are fed to the same hasher: hashing the 8-byte salt is identical either way, so an
    index built by an older release (placement via `Hash for [u8]`) passes it. What retires those
    indexes is the version-gated migration of every pre-v2 pack and the rebuild of any index file
    without a clean-close sentinel (which no older release's file has). Do not "simplify" this back to
    `hash_one`/`Hash for [u8]` — that would reintroduce toolchain-dependent placement and force an
    index rebuild on every compiler upgrade.
21. **Index rebuild adds an output's digests before it confirms the output is complete.** Recovery
    pass 2 indexes a header and its batch digests, then the completeness check trims a torn final
    output; the stale entries then point at or past the trimmed `end` and are never returned, because
    every read is bounded to the logical length (`pos >= file_len()` ⇒ miss). Deliberate, to keep the
    rebuild a single forward pass. Not a stale-index bug.
22. **`stream_import` replaces the entire `epoch-{N}/` directory as a unit, and always writes v2.** A
    full-epoch import (from a v1 or v2 source; v2 is the only writable format) installs a fresh
    `epoch-{N}/` by atomic rename, replacing whatever was there (any prior per-epoch
    cert files included). The per-epoch `CertificatePack` (`cert_data`/`cert_hash/`) is produced only
    by the live current-epoch (CVV) writer, not carried in the import — an imported past epoch simply
    has none, which is fine (past cert packs are not read). Not a cross-component ownership bug.
23. **Epoch install does synchronous filesystem work (rename, `remove_dir_all`) under `pack_install`.**
    These are fast local metadata operations, and holding `pack_install` across them scopes the
    install so a concurrent `get_static` waits out a half-installed epoch directory (it retries under
    the lock). Long work — a read-side index rebuild or legacy migration — is built outside the lock
    on a blocking thread; only its install renames take `pack_install`. Deliberate; not an
    async-blocking or lock-scope defect.
24. **`db repair` heals the shared EpochRecordDb but does not separately assess each per-epoch
    CertificatePack.** `db repair --force` opens `EpochRecordDb` (torn-tail truncate + WAL index
    rebuild) and repairs each epoch's consensus pack. The per-epoch `CertificatePack` is opened
    **writable for the current epoch only** (an active CVV); a writable open rebuilds its index from
    its own WAL (INV3). Past-epoch cert packs are not reopened in production, so `db repair` has
    nothing to assess there. (A read-only cert-pack open does not self-heal — but no production path
    takes one.) A writable open takes an exclusive `flock` on `epoch-{N}/cert_lock` and holds it until
    its data and index have sealed, so a same-epoch reopen waits for a previous writer that is still
    sealing (with a warning naming the epoch directory when the lock is not free at once). A
    writable open whose `cert_data` is missing starts a new log and discards a leftover
    `cert_hash/`, warning when that index attested certificates: the one exception to "an absent
    file beside an index that attests data counts as corrupt", since nothing reads the pack and a
    refusal would stop the node over an archive.
