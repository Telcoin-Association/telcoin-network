//! On-demand benchmarks: the tndb-b engine against MDBX (`reth-libmdbx`, opened the way the node
//! opens it), each through its own API: tndb-b's [`Env`], not the adapter.
//!
//! Run them with (they are `#[ignore]`d out of the default suite):
//!
//! ```text
//! cargo test --release -p tndb-b-reth tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
//! ```
//!
//! `engine-bench.md` beside this crate's README holds the runs, the tables and what they show.
//!
//! Every column gets a fresh temp dir and the same seeded key and value streams: 8-byte big-endian
//! sequential keys and 32-byte random keys, each with a 100-byte value. One column runs, in order:
//! a bulk sequential insert and a bulk random insert (one transaction each, timed as a whole and
//! split into the put loop and the commit), the file size, point gets that all hit and that all
//! miss, forward and reverse full scans, seeks each followed by a short range (at random targets,
//! then at the same targets sorted ascending, the way a walker seeks forward), small transactions
//! committed one by one, random updates and deletes over the random table, a `clear_table` of the
//! sequential table, and then the put loops of the API rows (an append put, a cursor put with and
//! without append, and the same through a detached cursor, `TxnCursor`, each of N sequential keys
//! into a fresh table of a fresh environment, so they compare with the sequential insert's put
//! loop; MDBX has no detached-cursor rows), then the delete rows, and last the
//! large-value rows. For values of 2, 8 and 32 KiB (overflow runs of 1, 3 and 9 pages at 4 KiB; at
//! 16 KiB the 2 KiB value is inline and the others take runs of 1 and 3 pages), they time a put
//! loop of 20k, 10k and 4k entries (8-byte keys in a scrambled order), its commit, and point gets
//! of every key in another scrambled order. Each value size runs in its own fresh environment, as
//! the API rows do, so no row starts from the file another row grew and synced. Their entry counts
//! are fixed: `TNDB_B_BENCH_N` does not scale them.
//!
//! The delete rows run in fresh environments too, so they change no table another row reads:
//!
//! - **delete 90%:** a random table of N entries is filled (untimed), 90% of its keys are deleted
//!   in a random order in one transaction (timed as a whole and split into the delete loop and the
//!   commit, as a bulk insert is), and the tenth left is scanned forward.
//! - **mixed 50/50:** `MIXED_OPS` operations over a fresh random table of N entries, in
//!   transactions of `MIXED_BATCH`: half are deletes of a random present key, half are puts, and
//!   each put re-inserts the key deleted longest ago (or updates a random key while none is
//!   deleted). So the table neither drains nor only grows: it stays within a few hundred entries of
//!   N, and almost every put is an insert.
//! - **churn:** ten rounds over a table of N/4 random keys, each a fill (one transaction: the first
//!   puts all the keys, a later one re-inserts the keys the last round deleted) and a delete of 90%
//!   of the keys at random (one transaction), timed as a whole; then, in bytes, the pages of the
//!   table's tree (tndb-b's `Env::check`, MDBX's table stat), the file's used span (pages times the
//!   page size, from the engine's stat) and its `st_size`.
//!
//! The `changesets` rows (tndb-b columns only; the MDBX columns print `-`) ask whether reth's
//! changeset tables, composite keys in this crate (`DUP_LAYOUTS`), would do better as packed dup
//! tables. Each of two shapes of reth's widths is loaded in two forms: **composite**, a plain
//! table keyed `k ‖ s`, and **packed**, a dup table of `DupOptions::packed(K)` (the state tables'
//! form). StorageChangeSets (K 28 = block ‖ address, s 32, values of 1..=32 bytes) changes
//! `CS_ADDRESSES` (200) addresses a block, each 1..=4 slots, so every key packs;
//! AccountChangeSets (K 8 = block, s 20, values of 0..=100 bytes) changes 200 accounts a block,
//! so every key nests at any bound. Each shape takes about N rows (the blocks are N divided by
//! its mean rows a block), seeded, appended in ascending order with a commit every
//! `CS_COMMIT_BLOCKS` (10) blocks. Per form, the rows are the load's time per row (commits
//! included), a `walk_range` as reth's walker takes it (a seek to a block's first row, then steps
//! across keys to the last row of the 100th block; `CS_WALKS` of them spread over the blocks)
//! per row read, point gets of every logical key in a scrambled order per get, the file's used
//! span and the table's pages (its `TableStat`'s branch, leaf and overflow pages, nested trees
//! included), each times the page size. A column runs each shape's forms in ABBA order (composite,
//! packed, packed, composite), each pass in a fresh environment removed after it, and reports a
//! cell as the mean of its form's two passes.
//!
//! The `rewrite` rows (columns `t4d` and `md4` only, `RW_ENGINES`; the others print `-`) price
//! reth's write stages per call: a positioning seek through a write cursor, then a write at its
//! row, in ascending key order, as reth writes. Each table is N rows, seeded, loaded in a random
//! order in one transaction (untimed), then rewritten by `RW_TXNS` (20) transactions of
//! `RW_KEYS` (10k) present rows each, picked at random (no row twice while N is at least 200k)
//! and sorted, through one write cursor a transaction:
//!
//! - **history** (reth's `append_history_index`, the open shard): a plain table of 28-byte keys (20
//!   random bytes ‖ `u64::MAX` big-endian) and values of 16..=160 random bytes. Per key the
//!   adapter's calls for `seek_exact` and `upsert`, `TxnCursor::seek_exact` and `TxnCursor::put` of
//!   the value grown by 8 bytes; MDBX as reth-db's cursor: `Cursor::set_key`, `Cursor::put` with
//!   `WriteFlags::UPSERT`.
//! - **dup** (reth's `write_hashed_state`, the storage loop): a dup table as this crate stores
//!   HashedStorages (its `DupLayout`'s `DupOptions`; 32-byte keys, subkeys and values), all its
//!   rows under `RW_DUP_KEYS` (3) keys, so each key holds a nested tree of N/3 rows (TN's state:
//!   two stablecoins and a router hold all the slots); MDBX: a `DUP_SORT` table of 64-byte values
//!   (subkey ‖ value). Per pair the adapter's engine calls for `seek_by_key_subkey`,
//!   `delete_current` and `upsert`: `DupTxnCursor::seek_dup_with_key_tail`, then at the exact row
//!   `delete_current`, `follower_shares_prefix` (and `prev_in_prefix` when that is false) and
//!   `insert` of key ‖ subkey with a new value; MDBX: `get_both_range`, `del` with
//!   `WriteFlags::CURRENT`, `put` with `WriteFlags::UPSERT`.
//!
//! Each table also runs **seek only**: the same transactions' seeks without the writes, aborted,
//! so a write's own cost is the rewrite's loop less the seek-only loop. Every pass (rewrite or
//! seek only, per table) is a fresh environment, removed after it. A loop cell is the median of
//! the 20 transactions' loops (from `begin` to just before `commit`) per key, in whole
//! nanoseconds; the commit cell is the median commit. `TNDB_B_BENCH_REPEAT` does not apply.
//!
//! tndb-b is a plain dependency of this crate, built without its test-only code, so these rows
//! time the engine as a release build of the node would. The rows that need the engine's
//! test-only switches (step 4 forced to one `fdatasync`, deletes with merging off) are the
//! engine's own bench, `tndb_b_sync_and_merge` in `crates/tndb-b/src/bench.rs`.
//!
//! ## Selecting what runs
//!
//! - `TNDB_B_BENCH_N` scales the bulk tables (default 1,000,000 entries each) and the rows of each
//!   `changesets` shape.
//! - `TNDB_B_BENCH_ENGINES` picks the columns, comma-separated (default all; printed in this
//!   order): `t4d` (tndb-b 4K Durable), `t16d` (16K Durable), `t4n` (4K NoSync), `md` (MDBX
//!   Durable), `md4` (MDBX Durable with 4 KiB pages), `mn` (MDBX SafeNoSync).
//! - `TNDB_B_BENCH_ROWS` picks the row groups, comma-separated (default all): `seq`, `rand`,
//!   `get-hit`, `get-miss`, `scan-fwd`, `scan-rev`, `seek`, `seek-fwd` (the seek row's targets
//!   sorted ascending, so each seek lands at or a little past the last one, often in the same leaf
//!   or the next), `small`, `mixed`, `clear`, `append`, `delete` (the delete rows), `big` (the
//!   large-value rows), `changesets` (the changeset forms' rows above, tndb-b columns only),
//!   `rewrite` (the rewrite rows above, `t4d` and `md4` only), and the aliases `bulk` (`seq,rand`),
//!   `get` (both gets) and `scan` (both scans). The file size rows come with `seq` or `rand`. A
//!   group that needs a bulk table (`clear` the sequential one; the gets, scans, both seeks and
//!   `mixed` the random one) builds it untimed when its own group is not selected. Only rows with a
//!   measured cell are printed.
//! - `TNDB_B_BENCH_REPEAT=k` (default 1) runs each selected read row (the gets, the scans and both
//!   seeks, and the `changesets` walks and gets) k times back to back and reports the median, so
//!   one read row can dominate a profile: for example `TNDB_B_BENCH_ENGINES=t16d
//!   TNDB_B_BENCH_ROWS=get-hit TNDB_B_BENCH_REPEAT=20`. To profile a put loop or a commit, select
//!   `seq` or `rand` and raise `TNDB_B_BENCH_N` instead.
//!
//! ## Caveats (printed with the results too)
//!
//! - MDBX opens like `MdbxDatabase::open_with_sync_mode` in tn-storage: a `write_map` environment
//!   with the OS page size (16 KiB on Apple Silicon, 4 KiB on x86_64), so the tndb-b column with
//!   the same page size is the like-for-like one. The `md4` column opens it the same way with 4 KiB
//!   pages (`PageSize::Set(4096)`): the like-for-like one for `t4d`, and what a Linux x86_64 node
//!   runs, though on Apple Silicon each 4 KiB page sits in a 16 KiB OS page. On x86_64 the two MDBX
//!   columns are the same configuration. The geometry is generous (no remap mid-run).
//! - tndb-b `Durable` syncs the data by one `msync(MS_SYNC)` of the dirty pages when they form one
//!   range, else by one `fdatasync` of the file, then the size if the file grew and the data sync
//!   did not cover it, then `msync` of the meta page. MDBX `Durable` syncs the data by `msync` of
//!   the whole used span, then the meta page, plus an `fsync` when the file grew. On macOS `msync`
//!   and `fsync` do not flush the drive's cache, so there the durable rows cost far less than on
//!   Linux, where every `msync(MS_SYNC)` takes the file system's fsync path. Linux numbers are the
//!   ones that decide production questions.
//! - tndb-b `NoSync` and MDBX `SafeNoSync` commits are not durable. tndb-b's "small commits" rows
//!   and bulk "commit" rows measure the commit path without a sync; MDBX's `SafeNoSync` small
//!   commits still took milliseconds each on Linux (the cause is not investigated).
//! - A bulk row's "put loop" runs from `begin` to just before `commit` (`begin` and the table's
//!   creation cost next to nothing); its "commit" is the rest. The two add up to the total.
//! - The API rows (append put, cursor put) time the put loop only; their commits match the
//!   sequential insert's. MDBX runs them with `WriteFlags::APPEND` on `Transaction::put` and with
//!   `Cursor::put`, with and without `WriteFlags::APPEND`; tndb-b with `RwTxn::append`,
//!   `RwCursor::put` and `RwCursor::append`, and with `TxnCursor::put` and `TxnCursor::append` (the
//!   "txn cursor" rows: the cost of a cursor that shares its transaction, against the `RwCursor`
//!   rows above them).
//! - With `TNDB_B_BENCH_REPEAT` above 1, a read row reports the median of its runs, not a single
//!   cold-ish run: later runs find the tree in the CPU caches a first run had to fill.
//! - A large-value get in tndb-b verifies the CRC of a run of more than one page, so it reads every
//!   byte of the value (a 1-page run, like a leaf, is checked by its header); an MDBX get returns a
//!   pointer to the value without reading it. Both gets find the values in the page cache, written
//!   moments before.

use std::{
    borrow::Cow,
    collections::VecDeque,
    hint::black_box,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use reth_db_api::{table::Table as _, tables::HashedStorages};
use reth_libmdbx::{
    DatabaseFlags, Environment, EnvironmentFlags, Geometry, Mode, PageSize, SyncMode as MdbxSync,
    Transaction, WriteFlags, RW,
};

use tndb_b::{
    DupOptions, DupRoCursor, Env, EnvOptions, ReadTxn as _, RoCursor, RoTxn, RwTxn, SyncMode, Table,
};

use crate::{
    bench::{announce_temp_dir, page_size},
    dup::dup_layout,
};

/// xorshift64: the seeded streams every column replays (the generator the engine's tests use, so
/// the data is the data the runs in `engine-bench.md` used).
struct Rng(u64);

impl Rng {
    /// The next value.
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// A value in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Entries in each bulk table unless `TNDB_B_BENCH_N` says otherwise.
const DEFAULT_N: usize = 1_000_000;
/// Value length of every entry.
const VALUE_LEN: usize = 100;
/// Distinct values cycled through (generating them costs nothing in the timed regions).
const VALUE_POOL: usize = 4096;
/// Small transactions committed one by one.
const SMALL_TXNS: usize = 2000;
/// Puts in each small transaction.
const SMALL_PUTS: usize = 3;
/// Seeks in the seek row, each followed by `SEEK_RANGE - 1` steps.
const SEEKS: usize = 100_000;
/// Entries each seek reads.
const SEEK_RANGE: usize = 10;
/// Operations in each mixed row, in transactions of `MIXED_BATCH`.
const MIXED_OPS: usize = 100_000;
/// Operations per transaction in the mixed rows.
const MIXED_BATCH: usize = 1000;
/// Rounds of fill and delete in the churn rows.
const CHURN_ROUNDS: usize = 10;
/// The churn rows' table holds `n / CHURN_DIV` keys when full.
const CHURN_DIV: usize = 4;
/// The large-value rows: (value length, entries, its rows).
const BIG: [(usize, usize, [Row; 3]); 3] = [
    (2 << 10, 20_000, [Row::BigPuts2, Row::BigCommit2, Row::BigGet2]),
    (8 << 10, 10_000, [Row::BigPuts8, Row::BigCommit8, Row::BigGet8]),
    (32 << 10, 4_000, [Row::BigPuts32, Row::BigCommit32, Row::BigGet32]),
];
/// The most entries of a large-value row.
const BIG_KEYS: usize = 20_000;
/// The large values are windows of one buffer at this many different offsets.
const BIG_SHIFTS: usize = 64;

/// The addresses a block of the `changesets` rows changes (StorageChangeSets), and the accounts
/// it changes (AccountChangeSets).
const CS_ADDRESSES: usize = 200;
/// The `changesets` rows commit their load once per this many blocks.
const CS_COMMIT_BLOCKS: usize = 10;
/// The blocks one `walk_range` of the `changesets` rows reads.
const CS_WALK_BLOCKS: usize = 100;
/// The `walk_range`s of a `changesets` row, at starts spread evenly over the blocks.
const CS_WALKS: usize = 10;
/// The point gets of a `changesets` row (fewer when the table holds fewer rows).
const CS_GETS: usize = 100_000;

/// The columns the `rewrite` rows run in (the others print `-`).
const RW_ENGINES: [&str; 2] = ["t4d", "md4"];
/// The transactions of a `rewrite` row.
const RW_TXNS: usize = 20;
/// The keys (pairs, in the dup row) a `rewrite` transaction rewrites, at most the table's rows.
const RW_KEYS: usize = 10_000;
/// The history row's key width: a 20-byte address ‖ the open shard's 8-byte `u64::MAX`.
const RW_HIST_KEY: usize = 28;
/// The history row's value lengths: `RW_HIST_MIN..=RW_HIST_MAX` bytes.
const RW_HIST_MIN: usize = 16;
const RW_HIST_MAX: usize = 160;
/// The bytes a history rewrite grows a value by (one block number more).
const RW_GROW: usize = 8;
/// The dup row's keys, which hold all its subkeys (TN's state: two stablecoins and a router hold
/// all the slots).
const RW_DUP_KEYS: usize = 3;
/// The dup row's key, subkey and value width (HashedStorages': a hashed address, a hashed slot, a
/// 32-byte value).
const RW_DUP_WIDTH: usize = 32;
/// The `rewrite` rows' values are windows of one random buffer at this many shifts.
const RW_SHIFTS: usize = 4096;

/// A changeset table's shape for the `changesets` rows: reth's widths, as `DUP_LAYOUTS` holds
/// them, and how many rows a block writes.
struct CsShape {
    /// The table, as reth names it.
    name: &'static str,
    /// The key width K: the 8-byte block number, then (StorageChangeSets) the 20-byte address.
    key_len: usize,
    /// The subkey width W: a storage slot (32) or an account's address (20).
    sub_len: usize,
    /// The keys a block writes: its changed addresses, or the block alone.
    keys_per_block: usize,
    /// The rows a key holds, inclusive: an address's slots, or a block's accounts.
    rows_per_key: (usize, usize),
    /// The length of a row's value after its subkey, inclusive: a slot's value, or an account's
    /// encoding (0 for an account the block created).
    value_len: (usize, usize),
    /// The rows of each form ([`CsForm::ALL`]'s order): append, walk, get, file used, table pages.
    rows: [[Row; 5]; 2],
}

impl CsShape {
    /// The blocks that hold about `n` rows (at least one).
    const fn blocks(&self, n: usize) -> usize {
        let per_block = self.keys_per_block * (self.rows_per_key.0 + self.rows_per_key.1) / 2;
        let blocks = n / per_block;
        if blocks == 0 {
            1
        } else {
            blocks
        }
    }
}

/// The `changesets` rows' shapes: StorageChangeSets (few slots per address, so every key's rows
/// fit a packed set) and AccountChangeSets (hundreds of accounts per block, so every key nests at
/// any bound).
const CS_SHAPES: [CsShape; 2] = [
    CsShape {
        name: "StorageChangeSets",
        key_len: 28,
        sub_len: 32,
        keys_per_block: CS_ADDRESSES,
        rows_per_key: (1, 4),
        value_len: (1, 32),
        rows: [
            [
                Row::CsStoreCompAppend,
                Row::CsStoreCompWalk,
                Row::CsStoreCompGet,
                Row::CsStoreCompUsed,
                Row::CsStoreCompTable,
            ],
            [
                Row::CsStorePackAppend,
                Row::CsStorePackWalk,
                Row::CsStorePackGet,
                Row::CsStorePackUsed,
                Row::CsStorePackTable,
            ],
        ],
    },
    CsShape {
        name: "AccountChangeSets",
        key_len: 8,
        sub_len: 20,
        keys_per_block: 1,
        rows_per_key: (CS_ADDRESSES, CS_ADDRESSES),
        value_len: (0, 100),
        rows: [
            [
                Row::CsAcctCompAppend,
                Row::CsAcctCompWalk,
                Row::CsAcctCompGet,
                Row::CsAcctCompUsed,
                Row::CsAcctCompTable,
            ],
            [
                Row::CsAcctPackAppend,
                Row::CsAcctPackWalk,
                Row::CsAcctPackGet,
                Row::CsAcctPackUsed,
                Row::CsAcctPackTable,
            ],
        ],
    },
];

/// The form a changeset table takes in the `changesets` rows.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CsForm {
    /// A plain table keyed `k ‖ s`, as the adapter stores the changesets.
    Composite,
    /// A dup table of `DupOptions::packed(K)`, as the adapter stores the state tables.
    Packed,
}

impl CsForm {
    /// The forms, in the order of [`CsShape::rows`].
    const ALL: [Self; 2] = [Self::Composite, Self::Packed];
    /// The order a column runs them in, so a drift over the run weighs on both forms alike.
    const ABBA: [Self; 4] = [Self::Composite, Self::Packed, Self::Packed, Self::Composite];

    const fn name(self) -> &'static str {
        match self {
            Self::Composite => "composite",
            Self::Packed => "packed",
        }
    }
}

/// One shape's seeded rows for the `changesets` rows, in ascending order of their logical keys.
struct CsData {
    /// The logical key width, K + W.
    stride: usize,
    /// Every row's logical key `block ‖ (address) ‖ subkey`, `stride` bytes each.
    keys: Vec<u8>,
    /// Every row's value: row `i`'s ends at `value_ends[i]` and starts where row `i - 1`'s ends.
    values: Vec<u8>,
    value_ends: Vec<u32>,
    /// The first row of each block, then the row count.
    block_starts: Vec<usize>,
    /// The first block of each `walk_range`, and the blocks each reads.
    walk_starts: Vec<usize>,
    walk_blocks: usize,
    /// The rows the point gets read, in a scrambled order.
    gets: Vec<u32>,
}

/// `len` bytes from `rng`.
fn random_bytes(rng: &mut Rng, out: &mut Vec<u8>, len: usize) {
    let start = out.len();
    out.resize(start + len, 0);
    for chunk in out[start..].chunks_mut(8) {
        chunk.copy_from_slice(&rng.next().to_le_bytes()[..chunk.len()]);
    }
}

impl CsData {
    /// About `n` rows of `shape`: block `b`'s keys start with `b` big-endian, its keys (random
    /// addresses) and each key's subkeys (random) sorted and distinct; each `walk_range` reads
    /// `walk_blocks` blocks (all of them if fewer).
    fn new(shape: &CsShape, n: usize, walk_blocks: usize) -> Self {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ shape.key_len as u64);
        let blocks = shape.blocks(n);
        let stride = shape.key_len + shape.sub_len;
        let pick =
            |rng: &mut Rng, (lo, hi): (usize, usize)| lo + rng.below((hi - lo + 1) as u64) as usize;
        let (mut keys, mut values, mut value_ends) = (Vec::new(), Vec::new(), Vec::new());
        let mut block_starts = Vec::with_capacity(blocks + 1);
        let random_set = |rng: &mut Rng, count: usize, len: usize| {
            let mut set: Vec<Vec<u8>> = (0..count)
                .map(|_| {
                    let mut v = Vec::with_capacity(len);
                    random_bytes(rng, &mut v, len);
                    v
                })
                .collect();
            set.sort_unstable();
            set.dedup();
            set
        };
        for b in 0..blocks as u64 {
            block_starts.push(value_ends.len());
            for k in random_set(&mut rng, shape.keys_per_block, shape.key_len - 8) {
                let rows = pick(&mut rng, shape.rows_per_key);
                for s in random_set(&mut rng, rows, shape.sub_len) {
                    keys.extend_from_slice(&b.to_be_bytes());
                    keys.extend_from_slice(&k);
                    keys.extend_from_slice(&s);
                    let len = pick(&mut rng, shape.value_len);
                    random_bytes(&mut rng, &mut values, len);
                    value_ends.push(u32::try_from(values.len()).expect("values within u32"));
                }
            }
        }
        let rows = value_ends.len();
        block_starts.push(rows);
        let walk_blocks = blocks.min(walk_blocks);
        let last = blocks - walk_blocks;
        let walk_starts = (0..CS_WALKS).map(|i| last * i / (CS_WALKS - 1)).collect();
        let gets = (0..CS_GETS.min(rows)).map(|_| rng.below(rows as u64) as u32).collect();
        Self { stride, keys, values, value_ends, block_starts, walk_starts, walk_blocks, gets }
    }

    fn rows(&self) -> usize {
        self.value_ends.len()
    }

    fn blocks(&self) -> usize {
        self.block_starts.len() - 1
    }

    fn key(&self, i: usize) -> &[u8] {
        &self.keys[i * self.stride..(i + 1) * self.stride]
    }

    fn value(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.value_ends[i - 1] as usize };
        &self.values[start..self.value_ends[i] as usize]
    }

    /// The rows of the `walk_range` from block `first`.
    fn walk_rows(&self, first: usize) -> usize {
        self.block_starts[first + self.walk_blocks] - self.block_starts[first]
    }
}

/// The cursor moves the `changesets` rows take, on a plain table's cursor and on a dup table's.
trait CsCursor {
    fn first_row(&mut self) -> tndb_b::Result<Option<(&[u8], &[u8])>>;
    fn seek_row(&mut self, key: &[u8]) -> tndb_b::Result<Option<(&[u8], &[u8])>>;
    fn next_row(&mut self) -> tndb_b::Result<Option<(&[u8], &[u8])>>;
}

impl CsCursor for RoCursor {
    fn first_row(&mut self) -> tndb_b::Result<Option<(&[u8], &[u8])>> {
        self.first()
    }
    fn seek_row(&mut self, key: &[u8]) -> tndb_b::Result<Option<(&[u8], &[u8])>> {
        self.seek(key)
    }
    fn next_row(&mut self) -> tndb_b::Result<Option<(&[u8], &[u8])>> {
        self.next()
    }
}

impl CsCursor for DupRoCursor {
    fn first_row(&mut self) -> tndb_b::Result<Option<(&[u8], &[u8])>> {
        self.first()
    }
    fn seek_row(&mut self, key: &[u8]) -> tndb_b::Result<Option<(&[u8], &[u8])>> {
        self.seek(key)
    }
    fn next_row(&mut self) -> tndb_b::Result<Option<(&[u8], &[u8])>> {
        self.next()
    }
}

/// Create `shape`'s table in `form` (committed, untimed), then append every row of `data` in
/// order, committing once per [`CS_COMMIT_BLOCKS`] blocks: the table and the load's wall time.
fn cs_load(env: &Env, shape: &CsShape, data: &CsData, form: CsForm) -> (Table, Duration) {
    let mut txn = env.begin_rw().expect("begin");
    let table = match form {
        CsForm::Composite => txn.create_table(shape.name),
        CsForm::Packed => {
            let key_len = u16::try_from(shape.key_len).expect("a narrow key");
            txn.create_dup_table(shape.name, DupOptions::packed(key_len))
        }
    }
    .expect("create the table");
    txn.commit().expect("commit");
    let start = Instant::now();
    for first in (0..data.blocks()).step_by(CS_COMMIT_BLOCKS) {
        let last = (first + CS_COMMIT_BLOCKS).min(data.blocks());
        let mut txn = env.begin_rw().expect("begin");
        for i in data.block_starts[first]..data.block_starts[last] {
            txn.append(&table, data.key(i), data.value(i)).expect("append");
        }
        txn.commit().expect("commit");
    }
    (table, start.elapsed())
}

/// One `walk_range` as reth's walker takes it: a seek to block `first`'s first row, then steps
/// to the last row before block `first + walk_blocks`, across keys. The rows read.
fn cs_walk(cursor: &mut impl CsCursor, data: &CsData, first: usize) -> usize {
    let end = ((first + data.walk_blocks) as u64).to_be_bytes();
    let (mut rows, mut bytes) = (0, 0);
    let mut row = cursor.seek_row(data.key(data.block_starts[first])).expect("seek");
    while let Some((k, v)) = row {
        if k[..8] >= end[..] {
            break;
        }
        rows += 1;
        bytes += v.len();
        row = cursor.next_row().expect("next");
    }
    black_box(bytes);
    rows
}

/// Every `walk_range` of `data` over `table` in `form`, each through a new cursor: the wall time.
fn cs_walks(txn: &Arc<RoTxn>, table: &Table, form: CsForm, data: &CsData) -> Duration {
    let start = Instant::now();
    for &first in &data.walk_starts {
        let rows = match form {
            CsForm::Composite => {
                cs_walk(&mut txn.cursor_owned(table).expect("cursor"), data, first)
            }
            CsForm::Packed => {
                cs_walk(&mut txn.dup_cursor_owned(table).expect("cursor"), data, first)
            }
        };
        assert_eq!(rows, data.walk_rows(first), "a walk_range reads its blocks' rows");
    }
    start.elapsed()
}

/// Every point get of `data` over `table`: the wall time.
fn cs_gets(txn: &RoTxn, table: &Table, data: &CsData) -> Duration {
    let start = Instant::now();
    for &i in &data.gets {
        black_box(txn.get(table, data.key(i as usize)).expect("get").expect("a hit"));
    }
    start.elapsed()
}

/// Every row of `table` in `form`, by a cursor from the first.
fn cs_scan(txn: &Arc<RoTxn>, table: &Table, form: CsForm) -> Vec<(Vec<u8>, Vec<u8>)> {
    fn scan(cursor: &mut impl CsCursor) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut row = cursor.first_row().expect("first");
        while let Some((k, v)) = row {
            out.push((k.to_vec(), v.to_vec()));
            row = cursor.next_row().expect("next");
        }
        out
    }
    match form {
        CsForm::Composite => scan(&mut txn.cursor_owned(table).expect("cursor")),
        CsForm::Packed => scan(&mut txn.dup_cursor_owned(table).expect("cursor")),
    }
}

/// The `rewrite` rows' seeded inputs (see the module docs).
struct RwData {
    /// The history table's keys (`RW_HIST_KEY` bytes each), in load order.
    hist_keys: Vec<u8>,
    /// The history table's value lengths, in load order.
    hist_lens: Vec<u8>,
    /// The dup table's keys (`RW_DUP_WIDTH` bytes each): row `i` is under key `i % RW_DUP_KEYS`.
    dup_keys: Vec<u8>,
    /// The dup table's subkeys (`RW_DUP_WIDTH` bytes each), in load order.
    dup_subkeys: Vec<u8>,
    /// The values: windows of this random buffer.
    buf: Vec<u8>,
    /// Per transaction of the history row, the rows it rewrites, sorted by key.
    hist_txns: Vec<Vec<u32>>,
    /// Per transaction of the dup row, the rows it rewrites, sorted by key ‖ subkey.
    dup_txns: Vec<Vec<u32>>,
}

impl RwData {
    /// `n` rows a table. A row's transactions draw distinct rows from a permutation of them, in
    /// turn (wrapping when 20 transactions take more than `n`), so no two rewrite one row while
    /// `n` is at least `RW_TXNS * RW_KEYS`.
    fn new(n: usize) -> Self {
        let mut rng = Rng(0x5eed_0000_0000_0010);
        let mut hist_keys = Vec::with_capacity(n * RW_HIST_KEY);
        let mut hist_lens = Vec::with_capacity(n);
        for _ in 0..n {
            random_bytes(&mut rng, &mut hist_keys, RW_HIST_KEY - 8);
            hist_keys.extend_from_slice(&u64::MAX.to_be_bytes());
            let spread = (RW_HIST_MAX - RW_HIST_MIN + 1) as u64;
            hist_lens.push((RW_HIST_MIN as u64 + rng.below(spread)) as u8);
        }
        let mut dup_keys = Vec::new();
        random_bytes(&mut rng, &mut dup_keys, RW_DUP_KEYS * RW_DUP_WIDTH);
        let mut dup_subkeys = Vec::with_capacity(n * RW_DUP_WIDTH);
        random_bytes(&mut rng, &mut dup_subkeys, n * RW_DUP_WIDTH);
        let mut buf = Vec::new();
        random_bytes(&mut rng, &mut buf, RW_SHIFTS + RW_HIST_MAX + RW_GROW);
        let this = Self {
            hist_keys,
            hist_lens,
            dup_keys,
            dup_subkeys,
            buf,
            hist_txns: Vec::new(),
            dup_txns: Vec::new(),
        };
        let mut hist_txns = rw_txns(&mut rng, n);
        for picks in &mut hist_txns {
            picks.sort_unstable_by(|&a, &b| this.hist_key(a).cmp(this.hist_key(b)));
        }
        let mut dup_txns = rw_txns(&mut rng, n);
        for picks in &mut dup_txns {
            picks.sort_unstable_by(|&a, &b| this.dup_row(a).cmp(&this.dup_row(b)));
        }
        Self { hist_txns, dup_txns, ..this }
    }

    fn rows(&self) -> usize {
        self.hist_lens.len()
    }

    fn hist_key(&self, i: u32) -> &[u8] {
        &self.hist_keys[i as usize * RW_HIST_KEY..][..RW_HIST_KEY]
    }

    /// History row `i`'s value as loaded, grown by `grow` bytes.
    fn hist_value(&self, i: u32, grow: usize) -> &[u8] {
        let at = i as usize % RW_SHIFTS;
        &self.buf[at..][..usize::from(self.hist_lens[i as usize]) + grow]
    }

    /// Dup row `i`'s key and subkey.
    fn dup_row(&self, i: u32) -> (&[u8], &[u8]) {
        let key = i as usize % RW_DUP_KEYS;
        (
            &self.dup_keys[key * RW_DUP_WIDTH..][..RW_DUP_WIDTH],
            &self.dup_subkeys[i as usize * RW_DUP_WIDTH..][..RW_DUP_WIDTH],
        )
    }

    /// Dup row `i`'s key ‖ subkey, an engine dup table's logical key.
    fn dup_stored(&self, i: u32) -> [u8; 2 * RW_DUP_WIDTH] {
        let (key, subkey) = self.dup_row(i);
        let mut stored = [0; 2 * RW_DUP_WIDTH];
        stored[..RW_DUP_WIDTH].copy_from_slice(key);
        stored[RW_DUP_WIDTH..].copy_from_slice(subkey);
        stored
    }

    /// Dup row `i`'s value as loaded (`round` 0) or as rewritten (1).
    fn dup_value(&self, i: u32, round: usize) -> &[u8] {
        &self.buf[(i as usize + round) % RW_SHIFTS..][..RW_DUP_WIDTH]
    }

    /// Dup row `i`'s subkey ‖ value (`round` as [`Self::dup_value`]'s), an MDBX dup value.
    fn dup_mdbx_value(&self, i: u32, round: usize) -> [u8; 2 * RW_DUP_WIDTH] {
        let mut value = [0; 2 * RW_DUP_WIDTH];
        value[..RW_DUP_WIDTH].copy_from_slice(self.dup_row(i).1);
        value[RW_DUP_WIDTH..].copy_from_slice(self.dup_value(i, round));
        value
    }
}

/// A `rewrite` row's transactions: `RW_TXNS` runs of `RW_KEYS` (at most `n`) rows of `0..n`,
/// taken in turn from a seeded permutation, unsorted.
fn rw_txns(rng: &mut Rng, n: usize) -> Vec<Vec<u32>> {
    let order = shuffled(rng, n);
    let keys = RW_KEYS.min(n);
    (0..RW_TXNS).map(|r| (0..keys).map(|j| order[(r * keys + j) % n]).collect()).collect()
}

/// The median of `times`.
fn median_time(mut times: Vec<Duration>) -> Duration {
    times.sort_unstable();
    times[times.len() / 2]
}

/// The seeded inputs every column replays.
struct Data {
    /// Entries in each bulk table.
    n: usize,
    /// The random table's keys, in insertion order.
    rand_keys: Vec<[u8; 32]>,
    /// Keys absent from the random table (also the seek targets).
    miss_keys: Vec<[u8; 32]>,
    /// The forward seek row's targets: the seek row's, sorted ascending.
    fwd_keys: Vec<[u8; 32]>,
    /// The values, picked by index modulo the pool.
    values: Vec<[u8; VALUE_LEN]>,
    /// The mixed row: (index into `rand_keys`, delete?).
    mixed: Vec<(usize, bool)>,
    /// The large-value rows' keys: `i` scrambled by a bijection of `u64`, so their order is not
    /// the insertion order.
    big_keys: Vec<[u8; 8]>,
    /// The large values: windows of this random buffer.
    big_buf: Vec<u8>,
    /// The delete row's order: a permutation of `0..n` (indexes into `rand_keys`), whose first
    /// `deleted(n)` are deleted.
    del_order: Vec<u32>,
    /// The mixed 50/50 row: (index into `rand_keys`, delete?).
    mixed_half: Vec<(usize, bool)>,
    /// Keys in the churn table when full: `rand_keys[..churn]`.
    churn: usize,
    /// The churn rounds' delete orders: `CHURN_ROUNDS` permutations of `0..churn`, one after
    /// the other (see [`Data::churn_order`]).
    churn_orders: Vec<u32>,
}

/// A seeded random permutation of `0..n`.
fn shuffled(rng: &mut Rng, n: usize) -> Vec<u32> {
    let mut v: Vec<u32> = (0..n as u32).collect();
    for i in (1..n).rev() {
        v.swap(i, rng.below(i as u64 + 1) as usize);
    }
    v
}

/// The mixed 50/50 row's operations over the random table's `n` keys (see the module docs):
/// (index into `rand_keys`, delete?). A delete always finds its key; a put re-inserts the key
/// deleted longest ago, or updates a random key while none is deleted.
fn mixed_half(rng: &mut Rng, n: usize) -> Vec<(usize, bool)> {
    let mut present = vec![true; n];
    let mut deleted = VecDeque::new();
    (0..MIXED_OPS)
        .map(|_| {
            if rng.below(2) == 0 && deleted.len() < n {
                let idx = loop {
                    let i = rng.below(n as u64) as usize;
                    if present[i] {
                        break i;
                    }
                };
                present[idx] = false;
                deleted.push_back(idx);
                (idx, true)
            } else if let Some(idx) = deleted.pop_front() {
                present[idx] = true;
                (idx, false)
            } else {
                (rng.below(n as u64) as usize, false)
            }
        })
        .collect()
}

/// The keys a delete of 90% of `count` keys removes.
fn deleted(count: usize) -> usize {
    count - count / 10
}

impl Data {
    fn new(n: usize) -> Self {
        let mut rng = Rng(0x7d1b_b0b5_eed0_0001);
        let mut key = || {
            let mut k = [0u8; 32];
            for chunk in k.chunks_exact_mut(8) {
                chunk.copy_from_slice(&rng.next().to_le_bytes());
            }
            k
        };
        let rand_keys: Vec<[u8; 32]> = (0..n).map(|_| key()).collect();
        let miss_keys: Vec<[u8; 32]> = (0..n).map(|_| key()).collect();
        let mut fwd_keys = miss_keys[..SEEKS.min(n)].to_vec();
        fwd_keys.sort_unstable();
        let mut rng = Rng(0x5eed_0000_0000_0002);
        let values = (0..VALUE_POOL)
            .map(|_| {
                let mut v = [0u8; VALUE_LEN];
                for b in v.iter_mut() {
                    *b = rng.next() as u8;
                }
                v
            })
            .collect();
        let mixed =
            (0..MIXED_OPS).map(|_| (rng.below(n as u64) as usize, rng.below(3) == 0)).collect();
        let big_keys = (0..BIG_KEYS as u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes())
            .collect();
        let big_buf = (0..BIG[2].0 + BIG_SHIFTS).map(|_| rng.next() as u8).collect();
        // The delete rows draw from their own stream, so the streams above stay as they were.
        let mut rng = Rng(0x5eed_0000_0000_0003);
        let del_order = shuffled(&mut rng, n);
        let mixed_half = mixed_half(&mut rng, n);
        let churn = n / CHURN_DIV;
        let churn_orders = (0..CHURN_ROUNDS).flat_map(|_| shuffled(&mut rng, churn)).collect();
        Self {
            n,
            rand_keys,
            miss_keys,
            fwd_keys,
            values,
            mixed,
            big_keys,
            big_buf,
            del_order,
            mixed_half,
            churn,
            churn_orders,
        }
    }

    /// Churn round `r`'s order: a permutation of `0..churn` (indexes into `rand_keys`), whose
    /// first `deleted(churn)` the round deletes.
    fn churn_order(&self, r: usize) -> &[u32] {
        &self.churn_orders[r * self.churn..][..self.churn]
    }

    fn value(&self, i: usize) -> &[u8] {
        &self.values[i % VALUE_POOL]
    }

    /// Entry `i`'s value in a large-value row of `len`-byte values.
    fn big_value(&self, i: usize, len: usize) -> &[u8] {
        let at = i % BIG_SHIFTS;
        &self.big_buf[at..at + len]
    }

    /// The `j`-th get of a large-value row of `count` entries: every key once, in an order that
    /// is neither the keys' nor the insertion order (7919 is a prime dividing no count).
    fn big_get_key(&self, j: usize, count: usize) -> &[u8; 8] {
        &self.big_keys[j * 7919 % count]
    }
}

/// A selectable group of rows (`TNDB_B_BENCH_ROWS`): one bit of [`Selection::rows`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Group {
    Seq,
    Rand,
    GetHit,
    GetMiss,
    ScanFwd,
    ScanRev,
    Seek,
    SeekFwd,
    Small,
    Mixed,
    Clear,
    Append,
    Delete,
    Big,
    ChangeSets,
    Rewrite,
}

/// The names `TNDB_B_BENCH_ROWS` accepts, each with the groups it selects.
const GROUP_NAMES: [(&str, &[Group]); 19] = [
    ("seq", &[Group::Seq]),
    ("rand", &[Group::Rand]),
    ("bulk", &[Group::Seq, Group::Rand]),
    ("get-hit", &[Group::GetHit]),
    ("get-miss", &[Group::GetMiss]),
    ("get", &[Group::GetHit, Group::GetMiss]),
    ("scan-fwd", &[Group::ScanFwd]),
    ("scan-rev", &[Group::ScanRev]),
    ("scan", &[Group::ScanFwd, Group::ScanRev]),
    ("seek", &[Group::Seek]),
    ("seek-fwd", &[Group::SeekFwd]),
    ("small", &[Group::Small]),
    ("mixed", &[Group::Mixed]),
    ("clear", &[Group::Clear]),
    ("append", &[Group::Append]),
    ("delete", &[Group::Delete]),
    ("big", &[Group::Big]),
    ("changesets", &[Group::ChangeSets]),
    ("rewrite", &[Group::Rewrite]),
];

/// The column a bench run measures.
#[derive(Clone, Copy, Debug)]
enum Engine {
    /// tndb-b with this page size and sync mode.
    Tndb(u32, SyncMode),
    /// MDBX with this sync mode and page size (`None`: the OS page size).
    Mdbx(MdbxSync, Option<usize>),
}

/// The columns `TNDB_B_BENCH_ENGINES` accepts, in print order: (selector, title, engine).
const ENGINES: [(&str, &str, Engine); 6] = [
    ("t4d", "tndb-b 4K Durable", Engine::Tndb(4096, SyncMode::Durable)),
    ("t16d", "tndb-b 16K Durable", Engine::Tndb(16384, SyncMode::Durable)),
    ("t4n", "tndb-b 4K NoSync", Engine::Tndb(4096, SyncMode::NoSync)),
    ("md", "MDBX Durable", Engine::Mdbx(MdbxSync::Durable, None)),
    ("md4", "MDBX 4K Durable", Engine::Mdbx(MdbxSync::Durable, Some(4096))),
    ("mn", "MDBX SafeNoSync", Engine::Mdbx(MdbxSync::SafeNoSync, None)),
];

/// What a bench run measures, from the environment (see the module docs).
#[derive(Debug)]
struct Selection {
    /// Entries in each bulk table.
    n: usize,
    /// The selected columns, as indexes into [`ENGINES`], in print order.
    engines: Vec<usize>,
    /// One bit per selected [`Group`].
    rows: u32,
    /// Runs of each selected read row.
    repeat: usize,
}

/// The comma-separated names of a selector variable.
fn names(v: &str) -> Vec<&str> {
    v.split(',').map(str::trim).filter(|s| !s.is_empty()).collect()
}

impl Selection {
    fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let number = |name: &str, default: usize| {
            var(name).map_or(default, |v| v.parse().unwrap_or_else(|_| panic!("{name} = {v:?}")))
        };
        let engines = match var("TNDB_B_BENCH_ENGINES") {
            None => (0..ENGINES.len()).collect(),
            Some(v) => {
                let picked = names(&v);
                let known = ENGINES.map(|(sel, ..)| sel);
                for p in &picked {
                    assert!(
                        known.contains(p),
                        "TNDB_B_BENCH_ENGINES: {p:?} is not one of {known:?}"
                    );
                }
                (0..ENGINES.len()).filter(|&e| picked.contains(&ENGINES[e].0)).collect()
            }
        };
        let rows = match var("TNDB_B_BENCH_ROWS") {
            None => u32::MAX,
            Some(v) => {
                let mut rows = 0;
                let picked = names(&v);
                for p in picked {
                    let Some((_, groups)) = GROUP_NAMES.iter().find(|(name, _)| *name == p) else {
                        let known = GROUP_NAMES.map(|(name, _)| name);
                        panic!("TNDB_B_BENCH_ROWS: {p:?} is not one of {known:?}");
                    };
                    rows |= groups.iter().fold(0, |bits, &g| bits | 1 << g as u32);
                }
                rows
            }
        };
        let repeat = number("TNDB_B_BENCH_REPEAT", 1);
        assert!(repeat >= 1, "TNDB_B_BENCH_REPEAT must be at least 1");
        Self { n: number("TNDB_B_BENCH_N", DEFAULT_N), engines, rows, repeat }
    }

    fn has(&self, group: Group) -> bool {
        self.rows & (1 << group as u32) != 0
    }

    /// Whether some selected group needs the sequential table.
    fn needs_seq(&self) -> bool {
        self.has(Group::Seq) || self.has(Group::Clear)
    }

    /// Whether some selected group needs the random table.
    fn needs_rand(&self) -> bool {
        use Group::*;
        [Rand, GetHit, GetMiss, ScanFwd, ScanRev, Seek, SeekFwd, Mixed]
            .into_iter()
            .any(|g| self.has(g))
    }

    /// Run a read row `repeat` times and return the median wall time.
    fn median(&self, mut run: impl FnMut() -> Duration) -> Duration {
        let mut times: Vec<Duration> = (0..self.repeat).map(|_| run()).collect();
        times.sort_unstable();
        times[times.len() / 2]
    }
}

/// One measured cell.
#[derive(Clone, Copy)]
enum Cell {
    /// Not measured for this column.
    None,
    /// Wall time of the whole row.
    Total(Duration),
    /// Wall time of the row divided by its operations.
    PerOp(Duration, usize),
    /// [`Cell::PerOp`] in whole nanoseconds, for the per-call rows that 0.1 us would round.
    Nanos(Duration, usize),
    /// A byte count.
    Bytes(u64),
}

impl Cell {
    fn render(self) -> String {
        match self {
            Cell::None => "-".to_string(),
            Cell::Total(d) => fmt_duration(d),
            Cell::PerOp(d, ops) => fmt_duration(d / ops.max(1) as u32),
            Cell::Nanos(d, ops) => format!("{} ns", d.as_nanos() / ops.max(1) as u128),
            Cell::Bytes(b) => format!("{:.1} MiB", b as f64 / (1024.0 * 1024.0)),
        }
    }
}

fn fmt_duration(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.1} us", ns as f64 / 1e3)
    } else if ns < 10_000_000_000 {
        format!("{:.1} ms", ns as f64 / 1e6)
    } else {
        format!("{:.2} s", ns as f64 / 1e9)
    }
}

/// The table's rows, in print order.
#[derive(Clone, Copy)]
enum Row {
    SeqInsert,
    SeqPuts,
    SeqCommit,
    RandInsert,
    RandPuts,
    RandCommit,
    FileSize,
    FileUsed,
    GetHit,
    GetMiss,
    ScanForward,
    ScanReverse,
    SeekRange,
    SeekFwdRange,
    SmallMedian,
    SmallP99,
    SmallTotal,
    Mixed,
    Clear,
    AppendPuts,
    CursorPuts,
    CursorAppendPuts,
    TxnCursorPuts,
    TxnCursorAppendPuts,
    DelTotal,
    DelLoop,
    DelCommit,
    DelScan,
    MixedHalf,
    ChurnTime,
    ChurnTable,
    ChurnUsed,
    ChurnSize,
    BigPuts2,
    BigCommit2,
    BigGet2,
    BigPuts8,
    BigCommit8,
    BigGet8,
    BigPuts32,
    BigCommit32,
    BigGet32,
    CsStoreCompAppend,
    CsStoreCompWalk,
    CsStoreCompGet,
    CsStoreCompUsed,
    CsStoreCompTable,
    CsStorePackAppend,
    CsStorePackWalk,
    CsStorePackGet,
    CsStorePackUsed,
    CsStorePackTable,
    CsAcctCompAppend,
    CsAcctCompWalk,
    CsAcctCompGet,
    CsAcctCompUsed,
    CsAcctCompTable,
    CsAcctPackAppend,
    CsAcctPackWalk,
    CsAcctPackGet,
    CsAcctPackUsed,
    CsAcctPackTable,
    RwHist,
    RwHistCommit,
    RwHistSeek,
    RwDup,
    RwDupCommit,
    RwDupSeek,
}

impl Row {
    /// Every row, in print order.
    const ALL: [Row; ROWS] = [
        Row::SeqInsert,
        Row::SeqPuts,
        Row::SeqCommit,
        Row::RandInsert,
        Row::RandPuts,
        Row::RandCommit,
        Row::FileSize,
        Row::FileUsed,
        Row::GetHit,
        Row::GetMiss,
        Row::ScanForward,
        Row::ScanReverse,
        Row::SeekRange,
        Row::SeekFwdRange,
        Row::SmallMedian,
        Row::SmallP99,
        Row::SmallTotal,
        Row::Mixed,
        Row::Clear,
        Row::AppendPuts,
        Row::CursorPuts,
        Row::CursorAppendPuts,
        Row::TxnCursorPuts,
        Row::TxnCursorAppendPuts,
        Row::DelTotal,
        Row::DelLoop,
        Row::DelCommit,
        Row::DelScan,
        Row::MixedHalf,
        Row::ChurnTime,
        Row::ChurnTable,
        Row::ChurnUsed,
        Row::ChurnSize,
        Row::BigPuts2,
        Row::BigCommit2,
        Row::BigGet2,
        Row::BigPuts8,
        Row::BigCommit8,
        Row::BigGet8,
        Row::BigPuts32,
        Row::BigCommit32,
        Row::BigGet32,
        Row::CsStoreCompAppend,
        Row::CsStoreCompWalk,
        Row::CsStoreCompGet,
        Row::CsStoreCompUsed,
        Row::CsStoreCompTable,
        Row::CsStorePackAppend,
        Row::CsStorePackWalk,
        Row::CsStorePackGet,
        Row::CsStorePackUsed,
        Row::CsStorePackTable,
        Row::CsAcctCompAppend,
        Row::CsAcctCompWalk,
        Row::CsAcctCompGet,
        Row::CsAcctCompUsed,
        Row::CsAcctCompTable,
        Row::CsAcctPackAppend,
        Row::CsAcctPackWalk,
        Row::CsAcctPackGet,
        Row::CsAcctPackUsed,
        Row::CsAcctPackTable,
        Row::RwHist,
        Row::RwHistCommit,
        Row::RwHistSeek,
        Row::RwDup,
        Row::RwDupCommit,
        Row::RwDupSeek,
    ];

    fn label(self, n: usize) -> String {
        let k = n / 1000;
        match self {
            Row::SeqInsert => format!("seq insert {k}k, 1 txn"),
            Row::RandInsert => format!("random insert {k}k, 1 txn"),
            Row::SeqPuts | Row::RandPuts => "  put loop".to_string(),
            Row::SeqCommit | Row::RandCommit | Row::DelCommit => "  commit".to_string(),
            Row::FileSize => "file size (st_size)".to_string(),
            Row::FileUsed => "file used (pages x P)".to_string(),
            Row::ChurnSize => "  file size (st_size)".to_string(),
            Row::ChurnUsed => "  file used (pages x P)".to_string(),
            Row::ChurnTable => "  table pages x P".to_string(),
            Row::GetHit => "get hit (per op)".to_string(),
            Row::GetMiss => "get miss (per op)".to_string(),
            Row::ScanForward => format!("scan forward {k}k"),
            Row::ScanReverse => format!("scan reverse {k}k"),
            Row::SeekRange => format!("seek + {SEEK_RANGE} entries (per seek)"),
            Row::SeekFwdRange => format!("seek fwd + {SEEK_RANGE} entries (per seek)"),
            Row::SmallMedian => format!("small txn ({SMALL_PUTS} puts) median"),
            Row::SmallP99 => "small txn p99".to_string(),
            Row::SmallTotal => format!("small txns total ({SMALL_TXNS})"),
            Row::Mixed => format!("mixed {}k upd/del ({MIXED_BATCH}/txn)", MIXED_OPS / 1000),
            Row::Clear => format!("clear_table {k}k + commit"),
            Row::AppendPuts => format!("append put {k}k (put loop)"),
            Row::CursorPuts => format!("cursor put {k}k (put loop)"),
            Row::CursorAppendPuts => format!("cursor put APPEND {k}k (put loop)"),
            Row::TxnCursorPuts => format!("txn cursor put {k}k (put loop)"),
            Row::TxnCursorAppendPuts => format!("txn cursor put APPEND {k}k (put loop)"),
            Row::DelTotal => format!("delete 90% of random {k}k, 1 txn"),
            Row::DelLoop => "  delete loop".to_string(),
            Row::DelScan => format!("  then scan forward {}k", (n - deleted(n)) / 1000),
            Row::MixedHalf => {
                format!("mixed {}k put/del 50/50 ({MIXED_BATCH}/txn)", MIXED_OPS / 1000)
            }
            Row::ChurnTime => {
                format!("churn {CHURN_ROUNDS} x fill + del 90% of {}k", n / CHURN_DIV / 1000)
            }
            Row::BigPuts2 | Row::BigPuts8 | Row::BigPuts32 => {
                let (len, count, _) =
                    BIG.into_iter().find(|b| b.2[0] as usize == self as usize).expect("a big row");
                format!("put {} KiB x {}k (per op)", len >> 10, count / 1000)
            }
            Row::BigCommit2 | Row::BigCommit8 | Row::BigCommit32 => "  commit".to_string(),
            Row::BigGet2 | Row::BigGet8 | Row::BigGet32 => {
                let (len, ..) =
                    BIG.into_iter().find(|b| b.2[2] as usize == self as usize).expect("a big row");
                format!("get {} KiB (per op)", len >> 10)
            }
            Row::CsStoreCompAppend
            | Row::CsStoreCompWalk
            | Row::CsStoreCompGet
            | Row::CsStoreCompUsed
            | Row::CsStoreCompTable
            | Row::CsStorePackAppend
            | Row::CsStorePackWalk
            | Row::CsStorePackGet
            | Row::CsStorePackUsed
            | Row::CsStorePackTable
            | Row::CsAcctCompAppend
            | Row::CsAcctCompWalk
            | Row::CsAcctCompGet
            | Row::CsAcctCompUsed
            | Row::CsAcctCompTable
            | Row::CsAcctPackAppend
            | Row::CsAcctPackWalk
            | Row::CsAcctPackGet
            | Row::CsAcctPackUsed
            | Row::CsAcctPackTable => {
                let (shape, form, measure) = CS_SHAPES
                    .iter()
                    .find_map(|shape| {
                        shape.rows.iter().enumerate().find_map(|(form, rows)| {
                            let at = rows.iter().position(|&r| r as usize == self as usize)?;
                            Some((shape, CsForm::ALL[form], at))
                        })
                    })
                    .expect("a changesets row");
                let walk = shape.blocks(n).min(CS_WALK_BLOCKS);
                match measure {
                    0 => format!(
                        "{} {} blocks, {}: append (per row)",
                        shape.name,
                        shape.blocks(n),
                        form.name()
                    ),
                    1 => format!("  walk_range {walk} blocks (per row)"),
                    2 => "  get (per op)".to_string(),
                    3 => "  file used (pages x P)".to_string(),
                    _ => "  table pages x P".to_string(),
                }
            }
            Row::RwHist => format!(
                "history {k}k, {RW_TXNS} txns x {}k: seek_exact + upsert (per key)",
                RW_KEYS.min(n) / 1000
            ),
            Row::RwDup => format!(
                "dup {k}k in {RW_DUP_KEYS} keys, {RW_TXNS} txns x {}k: seek_by_key_subkey + \
                 delete + upsert (per pair)",
                RW_KEYS.min(n) / 1000
            ),
            Row::RwHistCommit | Row::RwDupCommit => "  commit (median txn)".to_string(),
            Row::RwHistSeek => "  seek_exact only (per key)".to_string(),
            Row::RwDupSeek => "  seek_by_key_subkey only (per pair)".to_string(),
        }
    }
}

const ROWS: usize = Row::RwDupSeek as usize + 1;

/// One column of results.
struct Column {
    name: String,
    cells: [Cell; ROWS],
}

impl Column {
    fn new(name: &str) -> Self {
        Self { name: name.to_string(), cells: [Cell::None; ROWS] }
    }

    fn set(&mut self, row: Row, cell: Cell) {
        self.cells[row as usize] = cell;
    }

    /// Record a bulk insert: its total and its split into the put loop and the commit.
    fn set_bulk(&mut self, rows: [Row; 3], puts: Duration, total: Duration) {
        self.set(rows[0], Cell::Total(total));
        self.set(rows[1], Cell::Total(puts));
        self.set(rows[2], Cell::Total(total - puts));
    }

    /// Record per-transaction times as median, p99 and total.
    fn set_commits(&mut self, rows: [Row; 3], mut times: Vec<Duration>) {
        let total = times.iter().sum();
        times.sort_unstable();
        let at = |q: f64| times[((times.len() - 1) as f64 * q).round() as usize];
        self.set(rows[0], Cell::Total(at(0.5)));
        self.set(rows[1], Cell::Total(at(0.99)));
        self.set(rows[2], Cell::Total(total));
    }
}

fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).expect("stat the data file").len()
}

/// The options every tndb-b environment of a column opens with.
fn tndb_options(page_size: u32, sync: SyncMode) -> EnvOptions {
    EnvOptions { page_size, map_reserve: 8 << 30, sync, ..EnvOptions::default() }
}

/// Run the selected rows on a fresh tndb-b file.
fn run_tndb(
    dir: &Path,
    name: &str,
    page_size: u32,
    sync: SyncMode,
    data: &Data,
    cs: &[CsData],
    sel: &Selection,
) -> Column {
    let mut col = Column::new(name);
    let path = dir.join("data.tndb-b");
    let options = tndb_options(page_size, sync);
    let env = Env::open(&path, options).expect("open tndb-b");
    let n = data.n;

    let seq = sel.needs_seq().then(|| {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        let seq = txn.create_table("seq").expect("create seq");
        for i in 0..n {
            txn.put(&seq, &(i as u64).to_be_bytes(), data.value(i)).expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        if sel.has(Group::Seq) {
            col.set_bulk([Row::SeqInsert, Row::SeqPuts, Row::SeqCommit], puts, start.elapsed());
        }
        seq
    });

    let rand = sel.needs_rand().then(|| {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        let rand = txn.create_table("rand").expect("create rand");
        for (i, k) in data.rand_keys.iter().enumerate() {
            txn.put(&rand, k, data.value(i)).expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        if sel.has(Group::Rand) {
            col.set_bulk([Row::RandInsert, Row::RandPuts, Row::RandCommit], puts, start.elapsed());
        }
        rand
    });

    if sel.has(Group::Seq) || sel.has(Group::Rand) {
        let stat = env.stat();
        col.set(Row::FileSize, Cell::Bytes(file_size(&path)));
        col.set(Row::FileUsed, Cell::Bytes(u64::from(stat.page_count) * u64::from(stat.page_size)));
    }

    if let Some(rand) = &rand {
        tndb_reads(&env, rand, data, sel, &mut col);
    }

    if sel.has(Group::Small) {
        tndb_small(&env, data, &mut col);
    }

    if let (true, Some(rand)) = (sel.has(Group::Mixed), &rand) {
        let start = Instant::now();
        for (b, batch) in data.mixed.chunks(MIXED_BATCH).enumerate() {
            let mut txn = env.begin_rw().expect("begin");
            for (j, &(idx, delete)) in batch.iter().enumerate() {
                let k = &data.rand_keys[idx];
                if delete {
                    txn.delete(rand, k).expect("delete");
                } else {
                    txn.put(rand, k, data.value(b + j)).expect("put");
                }
            }
            txn.commit().expect("commit");
        }
        col.set(Row::Mixed, Cell::Total(start.elapsed()));
    }

    if let (true, Some(seq)) = (sel.has(Group::Clear), &seq) {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        txn.clear_table(seq).expect("clear");
        txn.commit().expect("commit");
        col.set(Row::Clear, Cell::Total(start.elapsed()));
    }

    env.sync().expect("sync");
    drop(env);

    if sel.has(Group::Append) {
        let rows = [
            (Row::AppendPuts, "append", ApiPut::Append),
            (Row::CursorPuts, "cursor", ApiPut::Cursor),
            (Row::CursorAppendPuts, "cursor-append", ApiPut::CursorAppend),
            (Row::TxnCursorPuts, "txn-cursor", ApiPut::TxnCursor),
            (Row::TxnCursorAppendPuts, "txn-cursor-append", ApiPut::TxnCursorAppend),
        ];
        for (row, name, how) in rows {
            let path = dir.join(format!("{name}.tndb-b"));
            col.set(row, Cell::Total(tndb_fresh_puts(&path, options, data, how)));
        }
    }

    if sel.has(Group::Delete) {
        tndb_delete(dir, options, data, sel, &mut col);
    }

    if sel.has(Group::Big) {
        tndb_big(dir, options, data, sel, &mut col);
    }

    if sel.has(Group::ChangeSets) {
        tndb_changesets(dir, options, cs, sel, &mut col);
    }
    col
}

/// The `changesets` rows: each shape's rows loaded into each form in [`CsForm::ABBA`]'s order,
/// each pass in a fresh environment in `dir`, removed after it; a cell is the mean of its form's
/// two passes (the file and table pages are the same in both).
fn tndb_changesets(
    dir: &Path,
    options: EnvOptions,
    cs: &[CsData],
    sel: &Selection,
    col: &mut Column,
) {
    for (shape, data) in CS_SHAPES.iter().zip(cs) {
        // Per form: load, walk and get times, and the file's and the table's pages.
        let mut times = [[Duration::ZERO; 3]; 2];
        let mut pages = [[0u64; 2]; 2];
        for (pass, form) in CsForm::ABBA.into_iter().enumerate() {
            let at = form as usize;
            let path = dir.join(format!("{}-{pass}.tndb-b", shape.name));
            let env = Env::open(&path, options).expect("open tndb-b");
            let (table, load) = cs_load(&env, shape, data, form);
            let txn = Arc::new(env.begin_ro().expect("begin"));
            let walk = sel.median(|| cs_walks(&txn, &table, form, data));
            let get = sel.median(|| cs_gets(&txn, &table, data));
            let stat = txn.table_stat(&table).expect("table stat");
            drop(txn);
            let t = &mut times[at];
            t[0] += load;
            t[1] += walk;
            t[2] += get;
            let tree = stat.branch_pages + stat.leaf_pages + stat.overflow_pages;
            pages[at] = [u64::from(env.stat().page_count), u64::from(tree)];
            drop(env);
            std::fs::remove_file(&path).expect("remove the pass's file");
        }
        let walked: usize = data.walk_starts.iter().map(|&w| data.walk_rows(w)).sum();
        let page = u64::from(options.page_size);
        for (at, rows) in shape.rows.iter().enumerate() {
            let [load, walk, get] = times[at];
            col.set(rows[0], Cell::PerOp(load, 2 * data.rows()));
            col.set(rows[1], Cell::PerOp(walk, 2 * walked));
            col.set(rows[2], Cell::PerOp(get, 2 * data.gets.len()));
            col.set(rows[3], Cell::Bytes(pages[at][0] * page));
            col.set(rows[4], Cell::Bytes(pages[at][1] * page));
        }
    }
}

/// The `rewrite` rows (see the module docs): per table a rewrite pass and a seek-only pass, each
/// in a fresh environment in `dir`.
fn tndb_rewrite(dir: &Path, options: EnvOptions, data: &RwData, col: &mut Column) {
    let layout = dup_layout(HashedStorages::NAME).expect("HashedStorages' layout");
    assert_eq!((layout.key, layout.subkey), (RW_DUP_WIDTH, RW_DUP_WIDTH), "HashedStorages' widths");
    let dup = layout.options().expect("HashedStorages is an engine dup table");
    let rows = data.rows() as u32;
    for (write, looped) in [(true, Row::RwHist), (false, Row::RwHistSeek)] {
        let (time, commit_time) = tndb_rw_pass(
            &dir.join(format!("history-{write}.tndb-b")),
            options,
            write,
            |txn| {
                let table = txn.create_table("history").expect("create");
                for i in 0..rows {
                    txn.put(&table, data.hist_key(i), data.hist_value(i, 0)).expect("put");
                }
                table
            },
            |txn, table, t| tndb_hist_txn(txn, table, data, &data.hist_txns[t], write),
        );
        col.set(looped, Cell::Nanos(time, data.hist_txns[0].len()));
        if let Some(commit_time) = commit_time {
            col.set(Row::RwHistCommit, Cell::Total(commit_time));
        }
    }
    for (write, looped) in [(true, Row::RwDup), (false, Row::RwDupSeek)] {
        let (time, commit_time) = tndb_rw_pass(
            &dir.join(format!("dup-{write}.tndb-b")),
            options,
            write,
            |txn| {
                let table = txn.create_dup_table("dup", dup).expect("create");
                for i in 0..rows {
                    txn.put(&table, &data.dup_stored(i), data.dup_value(i, 0)).expect("put");
                }
                table
            },
            |txn, table, t| tndb_dup_txn(txn, table, data, &data.dup_txns[t], write),
        );
        col.set(looped, Cell::Nanos(time, data.dup_txns[0].len()));
        if let Some(commit_time) = commit_time {
            col.set(Row::RwDupCommit, Cell::Total(commit_time));
        }
    }
}

/// One `rewrite` pass in a fresh environment at `path`, removed after it: `load` fills a table in
/// one transaction (untimed), then each of `RW_TXNS` transactions runs `txn_loop` (given the
/// transaction's index) and commits if `write`, else aborts. The median loop, from `begin` to
/// just before the commit, and the median commit (`None` if aborted).
fn tndb_rw_pass(
    path: &Path,
    options: EnvOptions,
    write: bool,
    load: impl FnOnce(&mut RwTxn) -> Table,
    mut txn_loop: impl FnMut(&mut RwTxn, &Table, usize),
) -> (Duration, Option<Duration>) {
    let env = Env::open(path, options).expect("open tndb-b");
    let mut txn = env.begin_rw().expect("begin");
    let table = load(&mut txn);
    txn.commit().expect("commit");
    let (mut loops, mut commits) = (Vec::with_capacity(RW_TXNS), Vec::with_capacity(RW_TXNS));
    for t in 0..RW_TXNS {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        txn_loop(&mut txn, &table, t);
        let looped = start.elapsed();
        loops.push(looped);
        if write {
            txn.commit().expect("commit");
            commits.push(start.elapsed() - looped);
        } else {
            txn.abort();
        }
    }
    drop(env);
    std::fs::remove_file(path).expect("remove the pass's file");
    (median_time(loops), write.then(|| median_time(commits)))
}

/// A history `rewrite` transaction's loop through one write cursor, as the adapter runs reth's
/// `seek_exact` and `upsert` on a plain table: `TxnCursor::seek_exact`, then (if `write`)
/// `TxnCursor::put` of the value grown by `RW_GROW` bytes. Not inlined, so a profile names it.
#[inline(never)]
fn tndb_hist_txn(txn: &mut RwTxn, table: &Table, data: &RwData, picks: &[u32], write: bool) {
    let mut cursor = txn.cursor_detached(table).expect("cursor");
    for &i in picks {
        let key = data.hist_key(i);
        let found = cursor.seek_exact(txn, key).expect("seek_exact").map(<[u8]>::len);
        black_box(found.expect("a present key"));
        if write {
            cursor.put(txn, key, data.hist_value(i, RW_GROW)).expect("upsert");
        }
    }
}

/// A dup `rewrite` transaction's loop through one dup write cursor: the adapter's engine calls
/// for reth's `seek_by_key_subkey`, `delete_current` and `upsert` on a nested dup table.
/// `DupTxnCursor::seek_dup_with_key_tail`, then (if `write`) on the exact row
/// `DupTxnCursor::delete_current`, `follower_shares_prefix` and, only if that is false,
/// `prev_in_prefix`, and `DupTxnCursor::insert` of key ‖ subkey with the new value. Not inlined,
/// so a profile names it.
#[inline(never)]
fn tndb_dup_txn(txn: &mut RwTxn, table: &Table, data: &RwData, picks: &[u32], write: bool) {
    let mut cursor = txn.dup_cursor_detached(table).expect("cursor");
    for &i in picks {
        let (key, subkey) = data.dup_row(i);
        let stored = data.dup_stored(i);
        let hit = cursor
            .seek_dup_with_key_tail(txn, key, subkey, RW_DUP_WIDTH)
            .expect("seek_by_key_subkey")
            .is_some_and(|(found, tail)| {
                black_box(tail);
                found == &stored[..]
            });
        assert!(hit, "a present pair");
        if write {
            assert!(cursor.delete_current(txn).expect("delete_current"), "at a row");
            if !cursor.follower_shares_prefix(txn, RW_DUP_WIDTH).expect("follower") {
                cursor.prev_in_prefix(txn, RW_DUP_WIDTH).expect("prev_in_prefix");
            }
            cursor.insert(txn, &stored, data.dup_value(i, 1)).expect("upsert");
        }
    }
}

/// The large-value rows, each size into a fresh table of its own fresh environment in `dir`.
fn tndb_big(dir: &Path, options: EnvOptions, data: &Data, sel: &Selection, col: &mut Column) {
    for (len, count, rows) in BIG {
        let env = Env::open(dir.join(format!("big{len}.tndb-b")), options).expect("open tndb-b");
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        let table = txn.create_table(&format!("big{len}")).expect("create");
        for (i, k) in data.big_keys[..count].iter().enumerate() {
            txn.put(&table, k, data.big_value(i, len)).expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        col.set(rows[0], Cell::PerOp(puts, count));
        col.set(rows[1], Cell::Total(start.elapsed() - puts));
        let ro = env.begin_ro().expect("begin ro");
        let time = sel.median(|| {
            let start = Instant::now();
            let mut bytes = 0;
            for j in 0..count {
                bytes +=
                    ro.get(&table, data.big_get_key(j, count)).expect("get").expect("a hit").len();
            }
            assert_eq!(bytes, len * count);
            start.elapsed()
        });
        col.set(rows[2], Cell::PerOp(time, count));
        drop(ro);
        env.sync().expect("sync");
    }
}

/// A fresh environment at `path` holding the random table: all N entries, put in one
/// transaction (untimed).
fn tndb_rand_env(path: &Path, options: EnvOptions, data: &Data) -> (Env, Table) {
    let env = Env::open(path, options).expect("open tndb-b");
    let mut txn = env.begin_rw().expect("begin");
    let rand = txn.create_table("rand").expect("create rand");
    for (i, k) in data.rand_keys.iter().enumerate() {
        txn.put(&rand, k, data.value(i)).expect("put");
    }
    txn.commit().expect("commit");
    (env, rand)
}

/// The delete rows (see the module docs), each in a fresh environment in `dir`.
fn tndb_delete(dir: &Path, options: EnvOptions, data: &Data, sel: &Selection, col: &mut Column) {
    let n = data.n;
    let (env, rand) = tndb_rand_env(&dir.join("delete.tndb-b"), options, data);
    let start = Instant::now();
    let mut txn = env.begin_rw().expect("begin");
    for &i in &data.del_order[..deleted(n)] {
        assert!(txn.delete(&rand, &data.rand_keys[i as usize]).expect("delete"), "a hit");
    }
    let deletes = start.elapsed();
    txn.commit().expect("commit");
    col.set_bulk([Row::DelTotal, Row::DelLoop, Row::DelCommit], deletes, start.elapsed());
    let ro = env.begin_ro().expect("begin ro");
    let mut cursor = ro.cursor(&rand).expect("cursor");
    let time = sel.median(|| {
        let start = Instant::now();
        let (mut count, mut bytes) = (0, 0);
        let mut entry = cursor.first().expect("first");
        while let Some((k, v)) = entry {
            count += 1;
            bytes += k.len() + v.len();
            entry = cursor.next().expect("next");
        }
        let time = start.elapsed();
        assert_eq!(count, n - deleted(n));
        black_box(bytes);
        time
    });
    col.set(Row::DelScan, Cell::Total(time));
    drop(ro);
    env.sync().expect("sync");
    drop(env);

    let (env, rand) = tndb_rand_env(&dir.join("mixed-half.tndb-b"), options, data);
    let start = Instant::now();
    for (b, batch) in data.mixed_half.chunks(MIXED_BATCH).enumerate() {
        let mut txn = env.begin_rw().expect("begin");
        for (j, &(idx, delete)) in batch.iter().enumerate() {
            let k = &data.rand_keys[idx];
            if delete {
                assert!(txn.delete(&rand, k).expect("delete"), "a hit");
            } else {
                txn.put(&rand, k, data.value(b + j)).expect("put");
            }
        }
        txn.commit().expect("commit");
    }
    col.set(Row::MixedHalf, Cell::Total(start.elapsed()));
    env.sync().expect("sync");
    drop(env);

    let path = dir.join("churn.tndb-b");
    let env = Env::open(&path, options).expect("open tndb-b");
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("churn").expect("create churn");
    txn.commit().expect("commit");
    let gone = deleted(data.churn);
    let start = Instant::now();
    for r in 0..CHURN_ROUNDS {
        // The fill puts every key the table lacks.
        let mut txn = env.begin_rw().expect("begin");
        let mut put = |i: usize| txn.put(&table, &data.rand_keys[i], data.value(i)).expect("put");
        match r {
            0 => (0..data.churn).for_each(&mut put),
            _ => data.churn_order(r - 1)[..gone].iter().for_each(|&i| put(i as usize)),
        }
        txn.commit().expect("commit");
        let mut txn = env.begin_rw().expect("begin");
        for &i in &data.churn_order(r)[..gone] {
            assert!(txn.delete(&table, &data.rand_keys[i as usize]).expect("delete"), "a hit");
        }
        txn.commit().expect("commit");
    }
    col.set(Row::ChurnTime, Cell::Total(start.elapsed()));
    let stat = env.stat();
    let page = u64::from(stat.page_size);
    let table = env.check().expect("check").tables["churn"].pages;
    col.set(Row::ChurnTable, Cell::Bytes(table * page));
    col.set(Row::ChurnUsed, Cell::Bytes(u64::from(stat.page_count) * page));
    col.set(Row::ChurnSize, Cell::Bytes(file_size(&path)));
    env.sync().expect("sync");
}

/// How an API row puts its keys.
#[derive(Clone, Copy)]
enum ApiPut {
    /// `RwTxn::append`.
    Append,
    /// `RwCursor::put`.
    Cursor,
    /// `RwCursor::append`.
    CursorAppend,
    /// `TxnCursor::put`.
    TxnCursor,
    /// `TxnCursor::append`.
    TxnCursorAppend,
}

/// One API row: N sequential keys put `how` into a fresh table of a fresh environment at `path`;
/// returns the put loop's time (from `begin`), then commits.
fn tndb_fresh_puts(path: &Path, options: EnvOptions, data: &Data, how: ApiPut) -> Duration {
    let env = Env::open(path, options).expect("open tndb-b");
    let start = Instant::now();
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("fresh").expect("create");
    match how {
        ApiPut::Append => {
            for i in 0..data.n {
                txn.append(&table, &(i as u64).to_be_bytes(), data.value(i)).expect("append");
            }
        }
        ApiPut::Cursor | ApiPut::CursorAppend => {
            let mut cursor = txn.cursor_mut(&table).expect("cursor");
            for i in 0..data.n {
                let key = (i as u64).to_be_bytes();
                match how {
                    ApiPut::CursorAppend => cursor.append(&key, data.value(i)).expect("append"),
                    _ => cursor.put(&key, data.value(i)).expect("put"),
                }
            }
        }
        ApiPut::TxnCursor | ApiPut::TxnCursorAppend => {
            let mut cursor = txn.cursor_detached(&table).expect("cursor");
            for i in 0..data.n {
                let key = (i as u64).to_be_bytes();
                match how {
                    ApiPut::TxnCursorAppend => {
                        cursor.append(&mut txn, &key, data.value(i)).expect("append")
                    }
                    _ => cursor.put(&mut txn, &key, data.value(i)).expect("put"),
                }
            }
        }
    }
    let puts = start.elapsed();
    txn.commit().expect("commit");
    env.sync().expect("sync");
    puts
}

/// The read rows over the random table, each run `sel.repeat` times.
fn tndb_reads(env: &Env, rand: &Table, data: &Data, sel: &Selection, col: &mut Column) {
    let n = data.n;
    let ro = env.begin_ro().expect("begin ro");
    if sel.has(Group::GetHit) {
        let time = sel.median(|| {
            let start = Instant::now();
            let mut bytes = 0;
            for k in &data.rand_keys {
                bytes += ro.get(rand, k).expect("get").expect("a hit").len();
            }
            black_box(bytes);
            start.elapsed()
        });
        col.set(Row::GetHit, Cell::PerOp(time, n));
    }

    if sel.has(Group::GetMiss) {
        let time = sel.median(|| {
            let start = Instant::now();
            for k in &data.miss_keys {
                assert!(ro.get(rand, k).expect("get").is_none(), "a miss");
            }
            start.elapsed()
        });
        col.set(Row::GetMiss, Cell::PerOp(time, n));
    }

    let mut cursor = ro.cursor(rand).expect("cursor");
    if sel.has(Group::ScanFwd) {
        let time = sel.median(|| {
            let start = Instant::now();
            let (mut count, mut bytes) = (0, 0);
            let mut entry = cursor.first().expect("first");
            while let Some((k, v)) = entry {
                count += 1;
                bytes += k.len() + v.len();
                entry = cursor.next().expect("next");
            }
            let time = start.elapsed();
            assert_eq!(count, n);
            black_box(bytes);
            time
        });
        col.set(Row::ScanForward, Cell::Total(time));
    }

    if sel.has(Group::ScanRev) {
        let time = sel.median(|| {
            let start = Instant::now();
            let (mut count, mut bytes) = (0, 0);
            let mut entry = cursor.last().expect("last");
            while let Some((k, v)) = entry {
                count += 1;
                bytes += k.len() + v.len();
                entry = cursor.prev().expect("prev");
            }
            let time = start.elapsed();
            assert_eq!(count, n);
            black_box(bytes);
            time
        });
        col.set(Row::ScanReverse, Cell::Total(time));
    }

    let seeks = &data.miss_keys[..SEEKS.min(n)];
    for (group, row, targets) in [
        (Group::Seek, Row::SeekRange, seeks),
        (Group::SeekFwd, Row::SeekFwdRange, &data.fwd_keys[..]),
    ] {
        if !sel.has(group) {
            continue;
        }
        let time = sel.median(|| {
            let start = Instant::now();
            let mut bytes = 0;
            for k in targets {
                let mut entry = cursor.seek(k).expect("seek");
                for _ in 1..SEEK_RANGE {
                    let Some((_, v)) = entry else { break };
                    bytes += v.len();
                    entry = cursor.next().expect("next");
                }
            }
            black_box(bytes);
            start.elapsed()
        });
        col.set(row, Cell::PerOp(time, targets.len()));
    }
}

/// The small-transaction rows: `SMALL_TXNS` commits of `SMALL_PUTS` puts each.
fn tndb_small(env: &Env, data: &Data, col: &mut Column) {
    let mut txn = env.begin_rw().expect("begin");
    let small = txn.create_table("small").expect("create small");
    txn.commit().expect("commit");
    let times = (0..SMALL_TXNS as u64 * SMALL_PUTS as u64)
        .step_by(SMALL_PUTS)
        .map(|first| {
            let start = Instant::now();
            let mut txn = env.begin_rw().expect("begin");
            for key in first..first + SMALL_PUTS as u64 {
                txn.put(&small, &key.to_be_bytes(), data.value(key as usize)).expect("put");
            }
            txn.commit().expect("commit");
            start.elapsed()
        })
        .collect();
    col.set_commits([Row::SmallMedian, Row::SmallP99, Row::SmallTotal], times);
}

/// The page size MDBX opens with when the column names none: the OS page size, as the node uses.
fn mdbx_os_page_size() -> usize {
    page_size().clamp(4096, 65536)
}

/// Open MDBX as `MdbxDatabase::open_with_sync_mode` does (with this page size), with a generous
/// geometry.
fn open_mdbx(dir: &Path, sync_mode: MdbxSync, page_size: usize) -> Environment {
    let mut builder = Environment::builder();
    builder
        .set_max_dbs(8)
        .write_map()
        .set_geometry(Geometry {
            size: Some(0..(64usize << 30)),
            growth_step: Some(256 << 20),
            shrink_threshold: Some(0),
            page_size: Some(PageSize::Set(page_size)),
        })
        .set_flags(EnvironmentFlags { mode: Mode::ReadWrite { sync_mode }, ..Default::default() });
    builder.open(dir).expect("open mdbx")
}

/// Run the selected rows on a fresh MDBX environment.
fn run_mdbx(
    dir: &Path,
    name: &str,
    sync_mode: MdbxSync,
    page_size: usize,
    data: &Data,
    sel: &Selection,
) -> Column {
    let mut col = Column::new(name);
    let env = open_mdbx(dir, sync_mode, page_size);
    let n = data.n;

    let seq = sel.needs_seq().then(|| {
        let start = Instant::now();
        let txn = env.begin_rw_txn().expect("begin");
        let seq = txn.create_db(Some("seq"), DatabaseFlags::default()).expect("create seq").dbi();
        for i in 0..n {
            txn.put(seq, (i as u64).to_be_bytes(), data.value(i), WriteFlags::empty())
                .expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        if sel.has(Group::Seq) {
            col.set_bulk([Row::SeqInsert, Row::SeqPuts, Row::SeqCommit], puts, start.elapsed());
        }
        seq
    });

    let rand = sel.needs_rand().then(|| {
        let start = Instant::now();
        let txn = env.begin_rw_txn().expect("begin");
        let rand =
            txn.create_db(Some("rand"), DatabaseFlags::default()).expect("create rand").dbi();
        for (i, k) in data.rand_keys.iter().enumerate() {
            txn.put(rand, k, data.value(i), WriteFlags::empty()).expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        if sel.has(Group::Rand) {
            col.set_bulk([Row::RandInsert, Row::RandPuts, Row::RandCommit], puts, start.elapsed());
        }
        rand
    });

    if sel.has(Group::Seq) || sel.has(Group::Rand) {
        let info = env.info().expect("info");
        let page_size = env.stat().expect("stat").page_size();
        col.set(Row::FileSize, Cell::Bytes(file_size(&dir.join("mdbx.dat"))));
        col.set(Row::FileUsed, Cell::Bytes((info.last_pgno() as u64 + 1) * u64::from(page_size)));
    }

    if let Some(rand) = rand {
        mdbx_reads(&env, rand, data, sel, &mut col);
    }

    if sel.has(Group::Small) {
        let txn = env.begin_rw_txn().expect("begin");
        let small = txn.create_db(Some("small"), DatabaseFlags::default()).expect("create").dbi();
        txn.commit().expect("commit");
        let mut times = Vec::with_capacity(SMALL_TXNS);
        let mut next_key = 0u64;
        for _ in 0..SMALL_TXNS {
            let start = Instant::now();
            let txn = env.begin_rw_txn().expect("begin");
            for _ in 0..SMALL_PUTS {
                txn.put(
                    small,
                    next_key.to_be_bytes(),
                    data.value(next_key as usize),
                    WriteFlags::empty(),
                )
                .expect("put");
                next_key += 1;
            }
            txn.commit().expect("commit");
            times.push(start.elapsed());
        }
        col.set_commits([Row::SmallMedian, Row::SmallP99, Row::SmallTotal], times);
    }

    if let (true, Some(rand)) = (sel.has(Group::Mixed), rand) {
        let start = Instant::now();
        for (b, batch) in data.mixed.chunks(MIXED_BATCH).enumerate() {
            let txn = env.begin_rw_txn().expect("begin");
            for (j, &(idx, delete)) in batch.iter().enumerate() {
                let k = &data.rand_keys[idx];
                if delete {
                    txn.del(rand, k, None).expect("delete");
                } else {
                    txn.put(rand, k, data.value(b + j), WriteFlags::empty()).expect("put");
                }
            }
            txn.commit().expect("commit");
        }
        col.set(Row::Mixed, Cell::Total(start.elapsed()));
    }

    if let (true, Some(seq)) = (sel.has(Group::Clear), seq) {
        let start = Instant::now();
        let txn = env.begin_rw_txn().expect("begin");
        txn.clear_db(seq).expect("clear");
        txn.commit().expect("commit");
        col.set(Row::Clear, Cell::Total(start.elapsed()));
    }
    drop(env);

    if sel.has(Group::Append) {
        let rows = [
            (Row::AppendPuts, "append", false, WriteFlags::APPEND),
            (Row::CursorPuts, "cursor", true, WriteFlags::empty()),
            (Row::CursorAppendPuts, "cursor-append", true, WriteFlags::APPEND),
        ];
        for (row, sub, cursor, flags) in rows {
            let time =
                mdbx_fresh_puts(&dir.join(sub), sync_mode, page_size, data, cursor, flags.bits());
            col.set(row, Cell::Total(time));
        }
    }

    if sel.has(Group::Delete) {
        mdbx_delete(dir, sync_mode, page_size, data, sel, &mut col);
    }

    if sel.has(Group::Big) {
        mdbx_big(dir, sync_mode, page_size, data, sel, &mut col);
    }
    col
}

/// The large-value rows, each size into a fresh table of its own fresh environment under `dir`.
fn mdbx_big(
    dir: &Path,
    sync_mode: MdbxSync,
    page_size: usize,
    data: &Data,
    sel: &Selection,
    col: &mut Column,
) {
    for (len, count, rows) in BIG {
        let sub = dir.join(format!("big{len}"));
        std::fs::create_dir(&sub).expect("big row dir");
        let env = open_mdbx(&sub, sync_mode, page_size);
        let start = Instant::now();
        let txn = env.begin_rw_txn().expect("begin");
        let db = txn
            .create_db(Some(&format!("big{len}")), DatabaseFlags::default())
            .expect("create")
            .dbi();
        for (i, k) in data.big_keys[..count].iter().enumerate() {
            txn.put(db, k, data.big_value(i, len), WriteFlags::empty()).expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        col.set(rows[0], Cell::PerOp(puts, count));
        col.set(rows[1], Cell::Total(start.elapsed() - puts));
        let ro = env.begin_ro_txn().expect("begin ro");
        let time = sel.median(|| {
            let start = Instant::now();
            let mut bytes = 0;
            for j in 0..count {
                let k = data.big_get_key(j, count);
                bytes += ro.get::<Cow<'_, [u8]>>(db, k).expect("get").expect("a hit").len();
            }
            assert_eq!(bytes, len * count);
            start.elapsed()
        });
        col.set(rows[2], Cell::PerOp(time, count));
    }
}

/// The `rewrite` rows (see the module docs) through MDBX's cursor as reth-db's calls it: per table
/// a rewrite pass and a seek-only pass, each in a fresh environment under `dir`.
fn mdbx_rewrite(
    dir: &Path,
    sync_mode: MdbxSync,
    page_size: usize,
    data: &RwData,
    col: &mut Column,
) {
    let rows = data.rows() as u32;
    for (write, looped) in [(true, Row::RwHist), (false, Row::RwHistSeek)] {
        let (time, commit_time) = mdbx_rw_pass(
            &dir.join(format!("history-{write}")),
            (sync_mode, page_size),
            write,
            DatabaseFlags::default(),
            |txn, db| {
                for i in 0..rows {
                    let value = data.hist_value(i, 0);
                    txn.put(db, data.hist_key(i), value, WriteFlags::empty()).expect("put");
                }
            },
            |txn, db, t| mdbx_hist_txn(txn, db, data, &data.hist_txns[t], write),
        );
        col.set(looped, Cell::Nanos(time, data.hist_txns[0].len()));
        if let Some(commit_time) = commit_time {
            col.set(Row::RwHistCommit, Cell::Total(commit_time));
        }
    }
    for (write, looped) in [(true, Row::RwDup), (false, Row::RwDupSeek)] {
        let (time, commit_time) = mdbx_rw_pass(
            &dir.join(format!("dup-{write}")),
            (sync_mode, page_size),
            write,
            DatabaseFlags::DUP_SORT,
            |txn, db| {
                for i in 0..rows {
                    let value = data.dup_mdbx_value(i, 0);
                    txn.put(db, data.dup_row(i).0, value, WriteFlags::empty()).expect("put");
                }
            },
            |txn, db, t| mdbx_dup_txn(txn, db, data, &data.dup_txns[t], write),
        );
        col.set(looped, Cell::Nanos(time, data.dup_txns[0].len()));
        if let Some(commit_time) = commit_time {
            col.set(Row::RwDupCommit, Cell::Total(commit_time));
        }
    }
}

/// [`tndb_rw_pass`] on MDBX (opened with `open`'s sync mode and page size, the table created
/// with `flags`) in the new directory `dir`, removed after it; a read pass's transactions are
/// dropped, which aborts them.
fn mdbx_rw_pass(
    dir: &Path,
    open: (MdbxSync, usize),
    write: bool,
    flags: DatabaseFlags,
    load: impl FnOnce(&Transaction<RW>, u32),
    mut txn_loop: impl FnMut(&Transaction<RW>, u32, usize),
) -> (Duration, Option<Duration>) {
    std::fs::create_dir(dir).expect("rewrite pass dir");
    let env = open_mdbx(dir, open.0, open.1);
    let txn = env.begin_rw_txn().expect("begin");
    let db = txn.create_db(Some("rewrite"), flags).expect("create").dbi();
    load(&txn, db);
    txn.commit().expect("commit");
    let (mut loops, mut commits) = (Vec::with_capacity(RW_TXNS), Vec::with_capacity(RW_TXNS));
    for t in 0..RW_TXNS {
        let start = Instant::now();
        let txn = env.begin_rw_txn().expect("begin");
        txn_loop(&txn, db, t);
        let looped = start.elapsed();
        loops.push(looped);
        if write {
            txn.commit().expect("commit");
            commits.push(start.elapsed() - looped);
        }
    }
    drop(env);
    std::fs::remove_dir_all(dir).expect("remove the pass's dir");
    (median_time(loops), write.then(|| median_time(commits)))
}

/// [`tndb_hist_txn`] on MDBX, as reth-db's cursor runs `seek_exact` and `upsert`: `Cursor::set_key`
/// (`MDBX_SET_KEY`), then (if `write`) `Cursor::put` with `WriteFlags::UPSERT`.
#[inline(never)]
fn mdbx_hist_txn(txn: &Transaction<RW>, db: u32, data: &RwData, picks: &[u32], write: bool) {
    type Kv<'a> = Option<(Cow<'a, [u8]>, Cow<'a, [u8]>)>;
    let mut cursor = txn.cursor(db).expect("cursor");
    for &i in picks {
        let key = data.hist_key(i);
        let found: Kv<'_> = cursor.set_key(key).expect("seek_exact");
        black_box(found.expect("a present key").1.len());
        if write {
            cursor.put(key, data.hist_value(i, RW_GROW), WriteFlags::UPSERT).expect("upsert");
        }
    }
}

/// [`tndb_dup_txn`] on MDBX, as reth-db's cursor runs `seek_by_key_subkey`, `delete_current` and
/// `upsert`: `Cursor::get_both_range`, then (if `write`) on the exact row `Cursor::del` with
/// `WriteFlags::CURRENT`, and `Cursor::put` of subkey ‖ the new value with `WriteFlags::UPSERT`.
#[inline(never)]
fn mdbx_dup_txn(txn: &Transaction<RW>, db: u32, data: &RwData, picks: &[u32], write: bool) {
    let mut cursor = txn.cursor(db).expect("cursor");
    for &i in picks {
        let (key, subkey) = data.dup_row(i);
        let found: Option<Cow<'_, [u8]>> =
            cursor.get_both_range(key, subkey).expect("seek_by_key_subkey");
        assert!(found.is_some_and(|value| value.starts_with(subkey)), "a present pair");
        if write {
            cursor.del(WriteFlags::CURRENT).expect("delete_current");
            cursor.put(key, &data.dup_mdbx_value(i, 1), WriteFlags::UPSERT).expect("upsert");
        }
    }
}

/// A fresh environment in the new directory `dir` holding the random table: all N entries, put
/// in one transaction (untimed).
fn mdbx_rand_env(
    dir: &Path,
    sync_mode: MdbxSync,
    page_size: usize,
    data: &Data,
) -> (Environment, u32) {
    std::fs::create_dir(dir).expect("delete row dir");
    let env = open_mdbx(dir, sync_mode, page_size);
    let txn = env.begin_rw_txn().expect("begin");
    let rand = txn.create_db(Some("rand"), DatabaseFlags::default()).expect("create rand").dbi();
    for (i, k) in data.rand_keys.iter().enumerate() {
        txn.put(rand, k, data.value(i), WriteFlags::empty()).expect("put");
    }
    txn.commit().expect("commit");
    (env, rand)
}

/// The delete rows (see the module docs), each in a fresh environment under `dir`.
fn mdbx_delete(
    dir: &Path,
    sync_mode: MdbxSync,
    page_size: usize,
    data: &Data,
    sel: &Selection,
    col: &mut Column,
) {
    type Kv<'a> = Option<(Cow<'a, [u8]>, Cow<'a, [u8]>)>;
    let n = data.n;
    let (env, rand) = mdbx_rand_env(&dir.join("delete"), sync_mode, page_size, data);
    let start = Instant::now();
    let txn = env.begin_rw_txn().expect("begin");
    for &i in &data.del_order[..deleted(n)] {
        assert!(txn.del(rand, data.rand_keys[i as usize], None).expect("delete"), "a hit");
    }
    let deletes = start.elapsed();
    txn.commit().expect("commit");
    col.set_bulk([Row::DelTotal, Row::DelLoop, Row::DelCommit], deletes, start.elapsed());
    let ro = env.begin_ro_txn().expect("begin ro");
    let mut cursor = ro.cursor(rand).expect("cursor");
    let time = sel.median(|| {
        let start = Instant::now();
        let (mut count, mut bytes) = (0, 0);
        let mut entry: Kv<'_> = cursor.first().expect("first");
        while let Some((k, v)) = entry {
            count += 1;
            bytes += k.len() + v.len();
            entry = cursor.next().expect("next");
        }
        let time = start.elapsed();
        assert_eq!(count, n - deleted(n));
        black_box(bytes);
        time
    });
    col.set(Row::DelScan, Cell::Total(time));
    drop(cursor);
    drop(ro);
    drop(env);

    let (env, rand) = mdbx_rand_env(&dir.join("mixed-half"), sync_mode, page_size, data);
    let start = Instant::now();
    for (b, batch) in data.mixed_half.chunks(MIXED_BATCH).enumerate() {
        let txn = env.begin_rw_txn().expect("begin");
        for (j, &(idx, delete)) in batch.iter().enumerate() {
            let k = &data.rand_keys[idx];
            if delete {
                assert!(txn.del(rand, k, None).expect("delete"), "a hit");
            } else {
                txn.put(rand, k, data.value(b + j), WriteFlags::empty()).expect("put");
            }
        }
        txn.commit().expect("commit");
    }
    col.set(Row::MixedHalf, Cell::Total(start.elapsed()));
    drop(env);

    let sub = dir.join("churn");
    std::fs::create_dir(&sub).expect("churn row dir");
    let env = open_mdbx(&sub, sync_mode, page_size);
    let txn = env.begin_rw_txn().expect("begin");
    let db = txn.create_db(Some("churn"), DatabaseFlags::default()).expect("create").dbi();
    txn.commit().expect("commit");
    let gone = deleted(data.churn);
    let start = Instant::now();
    for r in 0..CHURN_ROUNDS {
        // The fill puts every key the table lacks.
        let txn = env.begin_rw_txn().expect("begin");
        let put = |i: usize| {
            txn.put(db, data.rand_keys[i], data.value(i), WriteFlags::empty()).expect("put")
        };
        match r {
            0 => (0..data.churn).for_each(put),
            _ => data.churn_order(r - 1)[..gone].iter().for_each(|&i| put(i as usize)),
        }
        txn.commit().expect("commit");
        let txn = env.begin_rw_txn().expect("begin");
        for &i in &data.churn_order(r)[..gone] {
            assert!(txn.del(db, data.rand_keys[i as usize], None).expect("delete"), "a hit");
        }
        txn.commit().expect("commit");
    }
    col.set(Row::ChurnTime, Cell::Total(start.elapsed()));
    let used = env.info().expect("info").last_pgno() as u64 + 1;
    let page = u64::from(env.stat().expect("stat").page_size());
    let stat = env.begin_ro_txn().expect("begin ro").db_stat(db).expect("db stat");
    let table = stat.branch_pages() + stat.leaf_pages() + stat.overflow_pages();
    col.set(Row::ChurnTable, Cell::Bytes(table as u64 * page));
    col.set(Row::ChurnUsed, Cell::Bytes(used * page));
    col.set(Row::ChurnSize, Cell::Bytes(file_size(&sub.join("mdbx.dat"))));
}

/// The read rows over the random table, each run `sel.repeat` times.
fn mdbx_reads(env: &Environment, rand: u32, data: &Data, sel: &Selection, col: &mut Column) {
    type Kv<'a> = Option<(Cow<'a, [u8]>, Cow<'a, [u8]>)>;
    let n = data.n;
    let ro = env.begin_ro_txn().expect("begin ro");
    if sel.has(Group::GetHit) {
        let time = sel.median(|| {
            let start = Instant::now();
            let mut bytes = 0;
            for k in &data.rand_keys {
                bytes += ro.get::<Cow<'_, [u8]>>(rand, k).expect("get").expect("a hit").len();
            }
            black_box(bytes);
            start.elapsed()
        });
        col.set(Row::GetHit, Cell::PerOp(time, n));
    }

    if sel.has(Group::GetMiss) {
        let time = sel.median(|| {
            let start = Instant::now();
            for k in &data.miss_keys {
                assert!(ro.get::<Cow<'_, [u8]>>(rand, k).expect("get").is_none(), "a miss");
            }
            start.elapsed()
        });
        col.set(Row::GetMiss, Cell::PerOp(time, n));
    }

    let mut cursor = ro.cursor(rand).expect("cursor");
    if sel.has(Group::ScanFwd) {
        let time = sel.median(|| {
            let start = Instant::now();
            let (mut count, mut bytes) = (0, 0);
            let mut entry: Kv<'_> = cursor.first().expect("first");
            while let Some((k, v)) = entry {
                count += 1;
                bytes += k.len() + v.len();
                entry = cursor.next().expect("next");
            }
            let time = start.elapsed();
            assert_eq!(count, n);
            black_box(bytes);
            time
        });
        col.set(Row::ScanForward, Cell::Total(time));
    }

    if sel.has(Group::ScanRev) {
        let time = sel.median(|| {
            let start = Instant::now();
            let (mut count, mut bytes) = (0, 0);
            let mut entry: Kv<'_> = cursor.last().expect("last");
            while let Some((k, v)) = entry {
                count += 1;
                bytes += k.len() + v.len();
                entry = cursor.prev().expect("prev");
            }
            let time = start.elapsed();
            assert_eq!(count, n);
            black_box(bytes);
            time
        });
        col.set(Row::ScanReverse, Cell::Total(time));
    }

    let seeks = &data.miss_keys[..SEEKS.min(n)];
    for (group, row, targets) in [
        (Group::Seek, Row::SeekRange, seeks),
        (Group::SeekFwd, Row::SeekFwdRange, &data.fwd_keys[..]),
    ] {
        if !sel.has(group) {
            continue;
        }
        let time = sel.median(|| {
            let start = Instant::now();
            let mut bytes = 0;
            for k in targets {
                let mut entry: Kv<'_> = cursor.set_range(k).expect("seek");
                for _ in 1..SEEK_RANGE {
                    let Some((_, v)) = entry else { break };
                    bytes += v.len();
                    entry = cursor.next().expect("next");
                }
            }
            black_box(bytes);
            start.elapsed()
        });
        col.set(row, Cell::PerOp(time, targets.len()));
    }
}

/// The put loop of `n` sequential keys into a fresh table of a fresh MDBX environment in `dir`,
/// through `Transaction::put` or `Cursor::put` with the `WriteFlags` bits `flags`; the commit
/// follows untimed.
fn mdbx_fresh_puts(
    dir: &Path,
    sync_mode: MdbxSync,
    page_size: usize,
    data: &Data,
    cursor: bool,
    flags: u32,
) -> Duration {
    std::fs::create_dir(dir).expect("api row dir");
    let env = open_mdbx(dir, sync_mode, page_size);
    let start = Instant::now();
    let txn = env.begin_rw_txn().expect("begin");
    let db = txn.create_db(Some("fresh"), DatabaseFlags::default()).expect("create").dbi();
    if cursor {
        let mut cursor = txn.cursor(db).expect("cursor");
        for i in 0..data.n {
            cursor
                .put(
                    &(i as u64).to_be_bytes(),
                    data.value(i),
                    WriteFlags::from_bits_truncate(flags),
                )
                .expect("put");
        }
    } else {
        for i in 0..data.n {
            txn.put(
                db,
                (i as u64).to_be_bytes(),
                data.value(i),
                WriteFlags::from_bits_truncate(flags),
            )
            .expect("put");
        }
    }
    let puts = start.elapsed();
    txn.commit().expect("commit");
    puts
}

fn print_table(columns: &[Column], n: usize) {
    let shown: Vec<Row> = Row::ALL
        .into_iter()
        .filter(|&r| columns.iter().any(|c| !matches!(c.cells[r as usize], Cell::None)))
        .collect();
    let labels: Vec<String> = shown.iter().map(|r| r.label(n)).collect();
    let label_w = labels.iter().map(String::len).max().unwrap_or(0);
    let widths: Vec<usize> = columns
        .iter()
        .map(|c| {
            let cells = c.cells.iter().map(|cell| cell.render().len());
            cells.chain([c.name.len()]).max().unwrap_or(0)
        })
        .collect();
    let mut header = format!("| {:label_w$} |", "");
    let mut rule = format!("|{}|", "-".repeat(label_w + 2));
    for (c, w) in columns.iter().zip(&widths) {
        header.push_str(&format!(" {:>w$} |", c.name));
        rule.push_str(&format!("{}:|", "-".repeat(w + 1)));
    }
    println!("{header}\n{rule}");
    for (row, label) in shown.iter().zip(&labels) {
        let mut line = format!("| {label:label_w$} |");
        for (c, w) in columns.iter().zip(&widths) {
            line.push_str(&format!(" {:>w$} |", c.cells[*row as usize].render()));
        }
        println!("{line}");
    }
}

/// Compare tndb-b with MDBX across the benchmark rows and print one side-by-side table.
///
/// On-demand perf test (kept out of the default suite). Run with:
/// `cargo test --release -p tndb-b-reth tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1`.
#[test]
#[ignore = "on-demand performance comparison; run with --release --ignored --nocapture --test-threads 1"]
fn tndb_b_vs_mdbx() {
    let sel = Selection::from_env();
    let n = sel.n;
    let tmp = tempfile::tempdir().expect("temp dir");
    announce_temp_dir(tmp.path());
    let data = Data::new(n);
    let cs: Vec<CsData> = if sel.has(Group::ChangeSets) {
        CS_SHAPES.iter().map(|shape| CsData::new(shape, n, CS_WALK_BLOCKS)).collect()
    } else {
        Vec::new()
    };
    let rw = sel.has(Group::Rewrite).then(|| RwData::new(n));
    let columns: Vec<Column> = sel
        .engines
        .iter()
        .map(|&e| {
            let (dir_name, title, engine) = ENGINES[e];
            let dir = tmp.path().join(dir_name);
            std::fs::create_dir(&dir).expect("column dir");
            let rw = rw.as_ref().filter(|_| RW_ENGINES.contains(&dir_name));
            match engine {
                Engine::Tndb(page_size, sync) => {
                    let mut col = run_tndb(&dir, title, page_size, sync, &data, &cs, &sel);
                    if let Some(rw) = rw {
                        tndb_rewrite(&dir, tndb_options(page_size, sync), rw, &mut col);
                    }
                    col
                }
                Engine::Mdbx(sync, page) => {
                    let page = page.unwrap_or_else(mdbx_os_page_size);
                    let mut col = run_mdbx(&dir, title, sync, page, &data, &sel);
                    if let Some(rw) = rw {
                        mdbx_rewrite(&dir, sync, page, rw, &mut col);
                    }
                    col
                }
            }
        })
        .collect();
    println!(
        "\ntndb-b vs MDBX: {n} entries per bulk table, {VALUE_LEN}-byte values, MDBX page {} B \
         (md4: 4096 B){}\n",
        mdbx_os_page_size(),
        if sel.repeat > 1 {
            format!(", read rows the median of {} runs", sel.repeat)
        } else {
            String::new()
        }
    );
    print_table(&columns, n);
    println!(
        "\nNotes: NoSync/SafeNoSync commits are not durable. On macOS msync/fsync do not flush the \
         drive cache, so durable rows cost far less than on Linux. tndb-b Durable syncs the data \
         by one msync when the dirty pages form one range, else by one fdatasync of the file, then \
         the meta page by msync; MDBX Durable msyncs the whole used span, then the meta page. The \
         API rows (append put, cursor put) time the put loop into a fresh environment. MDBX opens \
         with the OS page size; md4 opens it with 4 KiB pages, the like-for-like column for t4d \
         (on x86_64 the OS page is 4 KiB, so md and md4 are the same configuration there)."
    );
}

/// The `changesets` rows' builders hold the same rows in both forms: a full scan of each form's
/// table is the shape's seeded rows, the packed form is a dup table of key width K whose keys
/// pack (StorageChangeSets) or all nest (AccountChangeSets), and every walk and get finds its rows.
#[test]
fn changesets_forms_hold_the_same_rows() {
    let _open = crate::tests::SPAWN_LOCK.read();
    let tmp = tempfile::tempdir().expect("temp dir");
    let options = EnvOptions { sync: SyncMode::NoSync, ..EnvOptions::default() };
    for (s, shape) in CS_SHAPES.iter().enumerate() {
        let data = CsData::new(shape, 3000, 2);
        let expected: Vec<(Vec<u8>, Vec<u8>)> =
            (0..data.rows()).map(|i| (data.key(i).to_vec(), data.value(i).to_vec())).collect();
        assert!(data.blocks() > 1 && data.walk_starts.iter().any(|&w| w > 0), "walks cross blocks");
        for form in CsForm::ALL {
            let env = Env::open(tmp.path().join(format!("{s}-{}", form.name())), options)
                .expect("open tndb-b");
            let (table, _) = cs_load(&env, shape, &data, form);
            let txn = Arc::new(env.begin_ro().expect("begin"));
            assert!(cs_scan(&txn, &table, form) == expected, "{} {}", shape.name, form.name());
            let stat = txn.table_stat(&table).expect("table stat");
            assert_eq!(stat.entries, data.rows() as u64);
            match form {
                CsForm::Composite => assert_eq!(stat.dup, None),
                CsForm::Packed => {
                    assert_eq!(stat.dup.map(|d| usize::from(d.key_len)), Some(shape.key_len));
                    let nested = if shape.key_len == 8 { data.blocks() } else { 0 };
                    assert_eq!(stat.nested_trees as usize, nested, "{}", shape.name);
                }
            }
            cs_walks(&txn, &table, form, &data);
            cs_gets(&txn, &table, &data);
        }
    }
}
