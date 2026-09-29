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
  length; the region `[end, capacity)` is zero padding. On a cleanly-closed file `end` excludes the
  padding, so ordinary reads (bounded to `end`) never observe it as data. The documented exceptions
  are the recovery/validation scan on an `opened_unclean` file — where `end` is left at physical EOF
  and the padding is deliberately read (as CRC-failing records) to find the trim point — and
  `AsyncPackIter` (intentional-design item 12).
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

Both index types are **fully reconstructable from the data file** and are never trusted over it.

### 4. `consensus_pack` — `ConsensusPack` (one epoch of consensus output)

An epoch's pack directory `epoch-{N}/` contains `data` (the WAL), `idx/` (position index),
`hash/` + `bhash/` (consensus-header and batch digest indexes), plus the per-epoch certificate pack
(`cert_data` + `cert_hash/`, a `CertificatePack`). Records are a leading
`EpochMeta` (committee, epoch-start linkage) followed by, per output, a `Consensus` header and its
`Batch` records. A background thread (`run_pack_loop`) serializes writes behind a channel; the public
type is `Send + Sync + Clone`.

**Open doors:**
| door | mode | on damage |
|------|------|-----------|
| `open_append` | writable, creates | header-only ⇒ write+fsync meta; then recover |
| `open_append_exists` | writable, must exist | recover (truncate torn tail + rebuild indexes) |
| `open_static` | read-only (sealed past epoch) | refuses; `ConsensusChain::get_static` then heals read-side (see below): rebuilds derived indexes from the WAL if the data log is clean, migrates a legacy pack; **refuses** (→ `db repair`) if the data log itself is torn |
| `stream_import` | writable, from a peer/byte stream | verify + append + fsync meta, then each output streamed record by record (always written as v2) |

### 5. Recovery model — four invariants

The recovery paths (`recover_pack`, `files_consistent`, the open doors, `pack_validate`) uphold:

- **INV1 — recover from truncation, error on corruption.** In an *unclean* (crash-interrupted) log a
  torn/partial trailing record or trailing padding is truncated back to the last complete output and
  the node continues; in a *cleanly-sealed* log the clean-close sentinel proves the log is complete,
  so **any** CRC failure is at-rest corruption and a hard `CorruptPack`. `recover_pack` replays the
  WAL index-free, tracks `consistent_end`, and decides truncate-vs-error from the data alone: a torn
  tail is truncatable unless a later complete *output* decodes past it (`output_after_tear`) —
  committed data, so it errors — and a best-effort commit marker (written by `persist()` to the mmap
  capacity tail) catches at-rest corruption of the last committed output.
- **INV2 — headers & meta are clean-or-error.** The `DataHeader` and the leading `EpochMeta` are
  expected present and correct; a corrupt/torn one is an **error** surfaced to the operator, **never
  repaired**. The meta is fsync'd the instant it is written, so a torn meta is not a normal state.
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
(data loss).

### 6. `consensus.rs` — `ConsensusChain` (full consensus store)

Ties the epochs together under `<datadir>/consensus-db/epochs/`. It opens the **current** epoch
writable (`open_append_exists`, which runs recovery on restart), serves **sealed past** epochs
read-only via `open_static` (behind a small `recent_packs` cache; a legacy or index-damaged sealed epoch
is healed read-side first — built off the async runtime and outside `pack_install` into a side directory,
data log opened read-only, then swapped in by rename), imports a full epoch from peers with
`stream_import` (into an `import-{N}/` dir, then an install-locked rename to `epoch-{N}/`; a read-only
partial-prefix pack instead stages under `staging-{N}/` and is never renamed), and drives epoch handoff.
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
  db/                          reth execution DB (MDBX)
  static_files/                reth static file segments
  consensus-db/                epoch/kad/cache sub-databases (redb or MDBX) + the epochs/ tree below
    epochs/
      epoch-{N}/               one ConsensusPack per epoch
        data                   the record WAL (source of truth)  ── + clean-close sentinel
        idx/index_pos.pdx      position index (derived)
        hash/{index.hdx,.odx}  consensus-header digest index (derived)
        bhash/{index.hdx,.odx} batch digest index (derived)
        cert_data              per-epoch certificate pack (CertificatePack)  ── + clean-close sentinel
        cert_hash/{index.hdx,.odx} certificate digest index (derived)
      epochs.pack / epoch_certs.pack + sidecars   the EpochRecordDb chain
      consensus_slot{1,2}      latest-consensus hint (double-buffered)
      import-{N}/              full-epoch import target (renamed to epoch-{N}/ on install)
      staging-{N}/             read-only partial-prefix import pack (never renamed)
    state_exports/             EVM state-export bundles
```

### Tooling

`telcoin-network db validate <path>` walks a pack's `data` stream read-only and reports integrity
issues. `telcoin-network db repair [--epoch N] [--force]` repairs epoch packs **at rest** (node
stopped): it truncates a torn tail and rebuilds indexes from the WAL for damaged epochs, dry-run by
default, skipping the current/latest epoch unless named. Meta/mid-log corruption is reported for
re-sync, never "fixed". `db repair` and `db migrate` refuse to run while a live node holds the
`<datadir>/telcoin.pid` lock (intentional-design item 9).

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
   hard `CorruptPack` error instead.
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
   recording its PID in it for operators (`tn_config::pid_lock`). Node startup and the at-rest writers
   (`db repair`, `db migrate`) refuse to run while another process holds the lock; the kernel releases
   it when the holder exits or crashes, so a crash never blocks a restart. The file is never unlinked
   (a release just clears the PID) — deleting it would let two processes lock two different inodes at
   the same path. This guard is TN-owned; it does not depend on the execution engine's own database
   lock. `db repair`/`db migrate` also take the lock for their run, so a node cannot start mid-repair.
   They stay dry-run by default (`--force` to apply, current epoch skipped in all-mode); naming
   `--epoch N` explicitly — including the current/latest epoch — is intentionally allowed under the
   same node-stopped contract. The lock is advisory (only TN processes take it) and network
   filesystems with unreliable `flock` are out of scope, so the loud banner and the stop-the-node
   contract remain. Do not flag "`--epoch` bypasses the current-epoch skip".
10. **`db repair` / `open_append_exists` mutate pack files on open; a past-epoch read may rebuild
    derived indexes or migrate a legacy pack.** Truncating a torn tail and rebuilding derived indexes
    from the authoritative log is the *point* (`db repair`/`open_append_exists`, gated by the
    node-stopped contract above). `ConsensusChain::get_static` additionally heals a sealed past epoch
    whose data log is clean but whose index will not open (e.g. after an index-format change), and
    migrates a legacy (pre-v2) epoch whose indexes use the old key placement. The heal is built on a
    blocking thread into a side directory (the data log opened read-only), serialized per epoch and
    backed off after a failure; only the final renames run under `pack_install`. It never writes the
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
    successful `next()`, deliberately distinct from a raw byte offset. (The former physical
    `position()` was removed — the recovery walk no longer does a per-frame `lseek`.)
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
    log + position index, then sets the digest indexes' `data_file_length` to a value that can never
    equal the rewound data length, so the next `open_append` fails `files_consistent` and
    `recover_pack` rebuilds every index from the data-log WAL. This deliberately does not try to
    restore an overwritten duplicate-key position (e.g. a batch already committed by an earlier
    output whose in-place index slot the failed output clobbered). It is safe because a failed output
    save is **fatal** (the executor subscriber is a critical task → node shutdown), so the epoch is
    reopened for append and rebuilt before it is served again. Corollary: a read-only `open_static` /
    `db validate` of an epoch whose most recent save failed will correctly report it inconsistent
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
    `MS_ASYNC` beside its fsync. These are deliberate, low-priority performance choices; the per-frame
    `lseek` that once made the cold-recovery walk expensive was already removed. Not a bug.
20. **Digest-index bucket placement is stable across compiler upgrades by design.** Placement feeds
    the raw key bytes straight to the vendored `FxHasher` (`HdxIndex::stable_hash` → `Hasher::write`),
    deliberately bypassing `impl Hash for [u8]`, whose length-prefix encoding is not stable across Rust
    versions. The salt/pepper marker is derived with the same primitive, so the open-time drift check
    exercises the exact placement hash and routes any mismatch to a WAL rebuild (INV3). Do not
    "simplify" this back to `hash_one`/`Hash for [u8]` — that would reintroduce toolchain-dependent
    placement and force an index rebuild on every compiler upgrade.
21. **Index rebuild adds an output's digests before it confirms the output is complete.** Recovery
    pass 2 indexes a header and its batch digests, then the completeness check trims a torn final
    output; the stale entries then point at or past the trimmed `end` and are never returned, because
    every read is bounded to the logical length (`pos >= file_len()` ⇒ miss). Deliberate, to keep the
    rebuild a single forward pass. Not a stale-index bug.
22. **`stream_import` replaces the entire `epoch-{N}/` directory as a unit, and always writes v2.** A
    full-epoch import (whatever the source stream's version: v2 is the only writable format)
    installs a fresh `epoch-{N}/` by atomic rename, replacing whatever was there (any prior per-epoch
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
    takes one.)
