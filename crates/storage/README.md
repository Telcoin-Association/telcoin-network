# `tn-storage`

Persistent storage for telcoin-network. Two independent families live here:

1. **Append-only pack files** (`archive/`, `consensus_pack`, `epoch_records`, `certificate_pack`,
   `exec_state_pack`) — the mmap-backed, CRC-checked, compressed WALs that hold consensus output,
   the epoch record/certificate chain, and EVM state snapshots. This is the crate's centre of
   gravity and the focus of most of this document.
2. **Key/value stores** (`redb`, `mdbx`, `layered_db`, `composite_db`, `mem_db`, `stores/`) — a
   `Database` trait over reth's MDBX (the default) or redb, used for the primary/worker's live
   consensus state (certificates, payloads, votes, proposer state) and libp2p/Kademlia records.

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
  length; the region `[end, capacity)` is zero padding. **All reads are bounded to `end`,** so the
  padding is never observed as data.
- **Clean-close sentinel.** On a clean `Drop` the file is truncated to `end` and an 8-byte sentinel
  (`crc32(end)` ‖ `crc32` of those 4 bytes) is appended and fsync'd. On reopen the sentinel is
  validated against the physical size and stripped; a missing/invalid sentinel means the file was
  **not** cleanly closed (a crash) — `opened_unclean()` reports that and the logical end is left at
  physical EOF for the recovery scan to trim. Trailing zero padding can never masquerade as a
  sentinel (`crc32(0x00000000) != 0`).
- **Durability.** The default barrier is `msync` (flush dirty pages); each *size extension* is
  `fsync`'d in the grow path, so data written within an already-fsync'd size is durable under the
  cheap msync default. `sync_disk` is the full msync+fsync.
- **Read-only handles** (`open(.., read_only=true)`) map a *sealed* file and clamp their read bound
  (`set_read_bound`) to the index-attested length as defense against a writer truncation.

### 2. `archive::pack` — `Pack<V>` (the record layer)

A typed append-only record log over an `MmapDataFile`. Each record on disk is
`u32 size ‖ payload ‖ u32 crc32`; payloads are the encoded `V` (via `tn-types`) and optionally `zstd`-compressed
(`MAX_RECORD_SIZE` caps both the framed size and the decompressed size — a decompression-bomb guard).
A 28-byte `DataHeader` (`DATA_HEADER_BYTES`, CRC + type + uid + version + appnum) leads every file and
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

Both index types are **fully reconstructable from the data file** and are never trusted over it.

### 4. `consensus_pack` — `ConsensusPack` (one epoch of consensus output)

An epoch's pack directory `epoch-{N}/` contains `data` (the WAL), `idx/` (position index),
`hash/` + `bhash/` (consensus-header and batch digest indexes). Records are a leading
`EpochMeta` (committee, epoch-start linkage) followed by, per output, a `Consensus` header and its
`Batch` records. A background thread (`run_pack_loop`) serializes writes behind a channel; the public
type is `Send + Sync + Clone`.

**Open doors:**
| door | mode | on damage |
|------|------|-----------|
| `open_append` | writable, creates | header-only ⇒ write+fsync meta; then recover |
| `open_append_exists` | writable, must exist | recover (truncate torn tail + rebuild indexes) |
| `open_static` | read-only (sealed past epoch) | **refuses** an inconsistent pack (cannot heal read-only) |
| `stream_import` | writable, from a peer/byte stream | verify + append + fsync meta, then per-output |

### 5. Recovery model — four invariants

The recovery paths (`recover_pack`, `files_consistent`, the open doors, `pack_validate`) uphold:

- **INV1 — recover from truncation.** A torn/partial trailing record or trailing padding is truncated
  back to the last complete output; the node continues. `recover_pack` replays the WAL, tracks
  `consistent_end`, and truncates the tail; `attested_end` + `tail_is_torn` distinguish an *unacked*
  torn tail (safe to drop) from a tear *below* durably-acked data (real corruption ⇒ error).
- **INV2 — headers & meta are clean-or-error.** The `DataHeader` and the leading `EpochMeta` are
  expected present and correct; a corrupt/torn one is an **error** surfaced to the operator, **never
  repaired**. The meta is fsync'd the instant it is written, so a torn meta is not a normal state.
- **INV3 — indexes are rebuildable.** Any index anomaly (corrupt/torn/missing index, stale marker,
  unclean seal) triggers a full rebuild from the WAL — including a corrupt index *header* that won't
  open (the writable doors wipe+recreate the index dirs). Index problems never brick a writable open.
- **INV4 — the data file is the source of truth.** Recovery replays the data log to rebuild indexes;
  indexes never override the data file, no durably-acked data is dropped, and reads/serving are
  bounded to the logical length.

`pack_validate` (the `db validate` diagnostic) classifies a damaged data file as `TornTrailingTail` /
`TornMetaEmpty` (truncatable) or `CorruptMetaWithData` / `MidLogCorruption` (data loss ⇒ re-sync).

### 6. `consensus.rs` — `ConsensusChain` (full consensus store)

Ties the epochs together under `<datadir>/consensus-db/epochs/`. It opens the **current** epoch
writable (`open_append_exists`, which runs recovery on restart), serves **sealed past** epochs
read-only via `open_static` (behind a small `recent_packs` cache), imports epochs from peers with
`stream_import` (into a `staging-{N}/` dir, then an install-locked rename), and drives epoch handoff.
`LatestConsensus` persists the tip `(epoch, number)` in double-buffered, CRC-checked
`consensus_slot{1,2}` files — a non-authoritative hint; the pack files are ground truth.

`epoch_records` (`EpochRecordDb`) is the singleton chain of `EpochRecord`s (`epochs.pack`) and their
`EpochCertificate`s (`epoch_certs.pack`), auto-healed on open. `certificate_pack` and
`exec_state_pack` are per-epoch / per-snapshot packs for certificate bundles and EVM state exports.

### 7. Key/value stores (the other family)

A `Database` trait (in `tn-types::database_traits`: tables, read/write txns, `get`/`insert`/`remove`/
`iter`/`skip`) with two backends: **`ReDB`** (redb — always compiled) and **`MdbxDatabase`** (reth's
MDBX — behind the default `reth-libmdbx` feature). `LayeredDatabase` adds a write-through in-memory
layer + a shared-txn guard; `CompositeDatabase` (the `DatabaseType`, backed by MDBX with the default
feature, else redb) splits the workload into `epoch` / `kad` / `cache` sub-databases routed by a table
hint. `MemDatabase` is an in-memory backend for tests. The typed `stores/` (`certificate_store`,
`payload_store`, `proposer_store`, `vote_digest_store`) wrap the trait for primary/worker state.

### On-disk layout

```
<datadir>/
  db/                          reth execution DB (MDBX) + epoch/kad/cache tables
  static_files/                reth static file segments
  consensus-db/
    epochs/
      epoch-{N}/               one ConsensusPack per epoch
        data                   the record WAL (source of truth)  ── + clean-close sentinel
        idx/index_pos.pdx      position index (derived)
        hash/{index.hdx,.odx}  consensus-header digest index (derived)
        bhash/{index.hdx,.odx} batch digest index (derived)
      epochs.pack / epoch_certs.pack + sidecars   the EpochRecordDb chain
      consensus_slot{1,2}      latest-consensus hint (double-buffered)
      staging-{N}/             transient state-sync import target
    state_exports/             EVM state-export bundles
```

### Tooling

`telcoin-network db validate <path>` walks a pack's `data` stream read-only and reports integrity
issues. `telcoin-network db repair [--epoch N] [--force]` repairs epoch packs **at rest** (node
stopped): it truncates a torn tail and rebuilds indexes from the WAL for damaged epochs, dry-run by
default, skipping the current/latest epoch unless named. Meta/mid-log corruption is reported for
re-sync, never "fixed".

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
   incompletely-written trailing record is INV1, not data loss. The `attested_end` + `tail_is_torn`
   guards ensure a tear *below* durably-acked data becomes a hard `CorruptPack` error instead.
6. **A torn/corrupt `EpochMeta` (or `DataHeader`) is a hard error, never repaired (INV2).** Do not
   suggest "reconstructing" or "healing" the meta. It is deliberately refused: the committee (with
   network addresses) cannot be reconstructed from the epoch alone, so recovery is a re-sync, not a
   local rewrite. `db repair` reports these as `Unrepairable`.
7. **Indexes are wiped and rebuilt from the data log, never preserved.** `recover_pack`/
   `reset_all_indexes`/`db repair` intentionally `remove_dir_all` the derived `idx/`,`hash/`,`bhash/`
   directories and replay the WAL. This never touches the `data` log or the chain-data dirs
   (`db`, `static_files`, `consensus-db`); rebuilding a derived index is not destructive.
8. **`msync` as the default write barrier (not `fsync`).** Durability comes from fsync'ing every size
   extension plus the msync default for data within an fsync'd size (see the module docs, incl. the
   macOS `F_FULLFSYNC` caveat). This is a deliberate performance choice, not a durability bug.
9. **No OS file lock on the datadir; single-writer by construction.** The node is the sole writer;
   the crate takes no advisory lock. `db repair` therefore cannot *detect* a running node and instead
   requires the operator to stop it (dry-run by default, `--force` to apply, current epoch skipped).
   Do not flag "missing file lock / TOCTOU".
10. **`db repair` / `open_append_exists` mutate pack files on open.** Truncating a torn tail and
    rebuilding derived indexes from the authoritative log is the *point*. Gated by the node-stopped
    contract above. Not an "unsafe destructive operation".
11. **Fail-fast `expect`/`panic` in DB-open startup paths** (e.g. `open_db`). A datadir that cannot be
    opened is unrecoverable and must abort node start; this is intentional fail-fast, not a library
    `unwrap`.
12. **`AsyncPackIter` has no logical-end bound.** Its callers only feed it *sealed* files or framed,
    length-bounded network streams (state-sync bounds its copy to `data_file_len()`); it is
    documented never to be handed a live, capacity-padded mmap file. Not a "reads past end" bug.
13. **`PackIter::position()` (physical stream position) vs `logical_position()`.** Recovery truncates
    using `logical_position()` (advanced only by whole record frames); `position()` is documented as
    physical and boundary-only. The two are deliberately distinct.
14. **`refresh_data_file_end` clears a prior `set_read_bound` clamp.** A documented precondition; no
    production path refreshes a clamped read-only handle. Not a live SIGBUS.
15. **Trailing-CRC "dirty" (zero) sentinel in `crc.rs`.** A zeroed trailing CRC is a deliberate
    "written but not yet CRC'd" marker (`crc_state` distinguishes Dirty from Corrupt), not a missing
    checksum.
16. **Mysten-derived / `#![allow(missing_docs)]` and `eyre`-everywhere error style.** Pre-existing
    conventions for this crate; not the target of this documentation pass.
