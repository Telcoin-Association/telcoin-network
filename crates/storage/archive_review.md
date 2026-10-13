# `src/archive` review — performance, security, correctness

Scope: `crates/storage/src/archive` (the mmap-backed pack layer: `data_file`, `pack`, `pack_iter`,
`digest_index/*`, `position_index/*`, `crc`, `fxhasher`, `error/*`).

Method: read the crate `README.md` first and excluded all 16 documented "Intentional design
decisions" from findings (mmap `unsafe`, read-only SIGBUS window + `set_read_bound`, trailing
padding/sentinel, CRC32-not-crypto, torn-tail truncation, meta/header hard-error, index wipe+rebuild,
`msync` default, no file lock, mutate-on-open, fail-fast `expect`, `AsyncPackIter` unbounded,
`position` vs `logical_position`, `refresh_data_file_end` clears clamp, zero-CRC dirty sentinel,
`eyre` style). Then read each source file, focused on untrusted-input handling, memory safety,
arithmetic overflow, bounds, CRC/compression, durability, concurrency, and the crash/recovery model.

## Summary

`src/archive` is mature, defensively written, and unusually well-documented. Untrusted/on-disk inputs
are consistently bounds-checked (bucket element counts, overflow pointers, record sizes,
decompression), CRC integrity + WAL rebuild is coherent, arithmetic uses `checked_*` / `u128` /
`saturating_*`, and the crash/recovery model matches the README's INV1–INV4. **No high-severity
issues found.** Findings are low-severity robustness / defense-in-depth and minor performance notes.

---

## Findings

### F1 — Correctness / robustness (Low-Med): the write path does not enforce `MAX_RECORD_SIZE` (asymmetric with reads)

`write_value` (`pack.rs:615`) frames a record as `(buffer.len() as u32)` with **no upper-bound
check**, while every read path rejects `val_size > MAX_RECORD_SIZE` (16 MiB) —
`pack.rs:459` & `pack.rs:476`, `pack_iter.rs:140` & `pack_iter.rs:293`. Consequences:

- A payload in `(16 MiB, 4 GiB]` is written with a valid `u32` size prefix but **can never be read
  back** (`RequestedSizeTooLarge`); `raw_iter` / recovery then treats it as a torn tail and truncates
  it.
- A payload `> 4 GiB` silently **truncates** in `as u32`, writing a wrong (small) size prefix, so the
  outer CRC fails on read — silent corruption of that record.

Not a deliberate decision (the README does not cover it). In practice it is only reachable via an
internal bug: peer data arrives through `AsyncPackIter`, which already caps reads at `MAX_RECORD_SIZE`
before anything is appended, and `save_consensus_output` writes consensus-validated batches/headers.
So severity is low, but the guard is cheap and closes a silent-corruption footgun.

**Recommendation:** in `write_value`, return an error if `buffer.len() > MAX_RECORD_SIZE` (mirror the
read cap) — fail fast instead of writing an unreadable/truncated record.

### F2 — Correctness (Low): bloom size is a compile-time constant that differs by feature and is not recorded on disk

`BLOOM_SIZE_BYTES` is `64 KiB` under the `test-utils` feature (non-test) and `2 MiB` otherwise
(`digest_index/bloom.rs:20-24`), and it is **not** stored in the hdx header. On open, the bloom
region is read as a fixed `vec![0; BLOOM_SIZE_BYTES]` (`digest_index/index.rs:387-390`) and bucket
offsets are computed as `HEADER_SIZE + BLOOM_SIZE_BYTES + bucket*BUCKET_SIZE`
(`bucket_pos`, `digest_index/index.rs:461`). None of the existing open guards (uid/version/appnum,
`bucket_size`/`bucket_elements` geometry, salt/pepper) cover the bloom size. So opening a datadir
written by a *different* build config:

- small index (file `< HEADER + expected_bloom`): `read_exact` overruns → `LoadHeaderError` →
  rebuild (fails closed, fine); but
- large index: reads bloom+buckets-as-bloom, then every `bucket_pos` is off by the size delta →
  **silent misread → bloom/bucket false negatives (present digests report `NotFound`).**

The smaller test bloom is a deliberate test optimization; the *absence of a format guard* against a
cross-config open is not documented and is the reviewable part. Narrow (mixing prod / `test-utils`
datadirs is abnormal, and indexes are WAL-rebuildable), but undetected in the large-index case.

**Recommendation:** record the bloom size (or fold it into the geometry check / an index format flag)
in `HdxHeader` so a mismatch is rejected → rebuild, rather than silently misread.

### P1 — Performance (Low): read buffer is zero-filled before being fully overwritten

The fallback read path `self.value_buffer.resize(val_size as usize + 4, 0)` (`pack.rs:479`) and the
iterators' `buffer.resize(val_size as usize, 0)` (`pack_iter.rs:143`, `pack_iter.rs:296`) zero up to
~16 MiB and then immediately overwrite it with `read_exact`. The allocation is amortized by buffer
reuse, but the memset is redundant per fetched record. Minor, and only on the non-mmap / stream paths
(the hot pack read path is already zero-copy via `slice`).

**Recommendation (optional):** read into spare capacity to skip the fill, or leave as-is (simplicity
over a micro-optimization).

### P2 — Performance (Low): CRC bucket scans are O(total buckets), not O(dirty)

`crc_dirty_buckets` (`digest_index/index.rs:921`) and `bucket_crc_scan`
(`digest_index/index.rs:937`) iterate every main bucket on each `sync` / scan. Under the WAL model
`sync` is infrequent (drop/close) and each per-bucket check is a cheap 4-byte read, so this is
acceptable and already acknowledged in the comments; noted only as a scaling characteristic for very
large per-epoch indexes.

---

## Observations (no change recommended)

- **Overflow-pointer validation** (`find_in_bucket` / `collect_bucket_elements`,
  `digest_index/index.rs:578-598`, `:625-643`) enforces strictly-backwards + slice-covered, but not
  `>= odx header` or record alignment. A corrupt pointer stays memory-safe (bounds- and
  element-count-validated) and yields at worst a wrong/absent position, which is caught by the record
  CRC and the consensus-layer digest checks — consistent with the documented "reads don't verify a
  per-op CRC; corruption ⇒ rebuild" model.
- **`FxHasher` is architecture-dependent** (`fxhasher.rs`: `usize` width, `K`, and chunking differ
  32- vs 64-bit), but this is guarded — the `pepper == hash_one(salt)` check
  (`digest_index/index.rs:384`) fails → `InvalidHasher` → rebuild on an arch change. Nodes are
  64-bit. Fine.
- **`commit`** does `flush` (async msync) then `sync_all` (sync msync) (`pack.rs:397-398`); the async
  flush is redundant before the sync flush. Harmless.

---

## Coverage (reviewed, found clean)

- `data_file.rs` — growth/remap/`ensure_capacity`/`next_capacity`, `slice`/`slice_mut` (bounded to
  logical `end`), `set_len`/`ensure_len`, `flush_dirty`/`sync_all`/`sync_range`/`sync_disk`,
  `try_clone` (flush + logical end), `Read`/`Seek` (overflow-checked), clean-close sentinel
  (`detect_sentinel`/`clean_close_sentinel`), `refresh_data_file_end`, `rename`.
- `pack.rs` — record framing + CRC, `record_size_bytes` (size cap before slice), `read_record_into`
  (CRC-before-decode, decompression-bomb `take`), failed-state poisoning/replay, `DataHeader`
  load/write (fixed buffer, CRC, type/uid/version/appnum/compression validation), `raw_iter`.
- `pack_iter.rs` — `PackIter` logical-end bound; `AsyncPackIter` size + decompression caps.
- `digest_index/index.rs` — `HdxHeader` parse, bucket geometry/hasher guards, `bucket_elements`
  (count ≤ capacity), `scan_bucket`/`collect_from_buffer`, linear-hash `hash_to_bucket`, split +
  rollback (`split_one_bucket`/`redistribute_split`), overflow append, ordered commit
  (`ordered_sync`), `Drop`.
- `position_index/index.rs` — alignment/geometry guard + torn-tail heal, sequential-append
  enforcement, decode length checks (error not panic), iterator partial-entry normalization.
- `digest_index/{bloom.rs, odx_header.rs}`, `crc.rs`, `fxhasher.rs`, `error/*`, `index.rs`, `mod.rs`.

---

## Suggested follow-ups (if actioned later; not part of this review)

- **F1:** add a `buffer.len() > MAX_RECORD_SIZE` guard in `write_value`; unit-test that appending an
  over-cap value errors and leaves the pack unchanged. Existing decompression-bomb tests must still
  pass. `cargo test -p tn-storage --lib`.
- **F2:** add a bloom-size field to `HdxHeader` (or otherwise reject a mismatch on open); test that
  opening an index whose on-disk bloom size differs from the compiled one triggers a rebuild rather
  than a misread.
