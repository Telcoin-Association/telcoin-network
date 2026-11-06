//! The reth-shaped rows: the shapes and write patterns of reth's tables on this engine, as the
//! reth adapter (`crates/tn-lmdb-reth`) lays them out: the state tables are dup tables of
//! `DupOptions::packed(key_len)` (PlainStorageState key 20 / subkey 32, HashedStorages 32 / 32),
//! the changesets plain tables keyed `key ‖ subkey` (StorageChangeSets 28 + 32, AccountChangeSets
//! 8 + 20), the history indexes plain tables keyed `address (‖ slot) ‖ shard`. Each function is
//! one job, its own environment in the rep's fresh directory, closed before it returns.

use std::{
    collections::BTreeSet,
    hint::black_box,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use tn_lmdb::{DupOptions, EnvOptions, RwTxn, SyncMode, Table};

use crate::{
    data::{random_bytes, shuffled, Rng},
    open, per_op, settle, used_bytes, Out,
};

/// The outer key width of the state rows' table (PlainStorageState: an address).
const STATE_KEY: usize = 20;
/// The subkey width of the state rows' table (a storage slot).
const STATE_SUB: usize = 32;
/// The rows of each transaction of `reth.state.load`.
const STATE_BATCH: usize = 10_000;
/// The rows a nested key of the state table holds on average.
const STATE_NESTED_MEAN: usize = 5000;
/// The values of the reth rows are windows of one random buffer at this many offsets.
const SHIFTS: usize = 4096;
/// The longest window any reth row takes.
const WINDOW: usize = 640;

/// The seeded rows of the `reth.state.*` rows: a PlainStorageState-shaped dup table of about `n`
/// rows whose keys are skewed: 40% of the rows under keys of 1..=4 rows (each a packed set), 60%
/// under a few keys of thousands of rows (each a nested tree).
pub(crate) struct StateData {
    /// Every row's logical key `k ‖ s` (`STATE_KEY + STATE_SUB` bytes), in load order (random).
    logical: Vec<u8>,
    /// Every row's value length (1..=32), in load order.
    value_lens: Vec<u8>,
    /// The outer keys, `STATE_KEY` bytes each.
    keys: Vec<u8>,
    /// The values: windows of this random buffer.
    buf: Vec<u8>,
    /// The rows `reth.state.get` reads (indexes in load order), random.
    gets: Vec<u32>,
    /// The keys `reth.state.walk` walks (indexes into `keys`): every key, in a scrambled order.
    walks: Vec<u32>,
}

impl StateData {
    /// The rows of a run of `n`.
    pub(crate) fn new(n: usize) -> Self {
        let mut rng = Rng(0x5eed_0000_0000_0020);
        let nested_rows = n * 3 / 5;
        let nested_keys = (nested_rows / STATE_NESTED_MEAN).max(1);
        let mean = nested_rows / nested_keys;
        let mut per_key: Vec<usize> =
            (0..nested_keys).map(|_| rng.within((mean / 2).max(1), mean * 3 / 2)).collect();
        let mut left = n.saturating_sub(per_key.iter().sum());
        while left > 0 {
            let rows = rng.within(1, 4).min(left);
            per_key.push(rows);
            left -= rows;
        }
        let mut keys = Vec::with_capacity(per_key.len() * STATE_KEY);
        random_bytes(&mut rng, &mut keys, per_key.len() * STATE_KEY);
        let stride = STATE_KEY + STATE_SUB;
        let mut rows = Vec::with_capacity(n * stride);
        for (k, &count) in per_key.iter().enumerate() {
            for _ in 0..count {
                rows.extend_from_slice(&keys[k * STATE_KEY..][..STATE_KEY]);
                random_bytes(&mut rng, &mut rows, STATE_SUB);
            }
        }
        let total = rows.len() / stride;
        let order = shuffled(&mut rng, total);
        let mut logical = Vec::with_capacity(rows.len());
        for &i in &order {
            logical.extend_from_slice(&rows[i as usize * stride..][..stride]);
        }
        let value_lens = (0..total).map(|_| rng.within(1, 32) as u8).collect();
        let mut buf = Vec::new();
        random_bytes(&mut rng, &mut buf, SHIFTS + WINDOW);
        let gets = (0..total.min(100_000)).map(|_| rng.below(total as u64) as u32).collect();
        let walks = shuffled(&mut rng, per_key.len());
        Self { logical, value_lens, keys, buf, gets, walks }
    }

    fn rows(&self) -> usize {
        self.value_lens.len()
    }

    fn row(&self, i: usize) -> &[u8] {
        &self.logical[i * (STATE_KEY + STATE_SUB)..][..STATE_KEY + STATE_SUB]
    }

    fn value(&self, i: usize) -> &[u8] {
        &self.buf[i % SHIFTS..][..usize::from(self.value_lens[i])]
    }

    fn key(&self, k: u32) -> &[u8] {
        &self.keys[k as usize * STATE_KEY..][..STATE_KEY]
    }
}

/// `reth.state.load` (every row put by its logical key in load order, in transactions of
/// `STATE_BATCH`, commits included, each settled untimed; per row), `reth.state.file_bytes`,
/// `reth.state.get` (a `DupRoCursor::seek_dup` of a random row through a reset cursor, as reth's
/// `seek_by_key_subkey` through a pooled cursor; per get) and `reth.state.walk` (every key's rows
/// by `seek_key` then `next_in_prefix`, as reth's `walk_dup`; per row).
pub(crate) fn state(dir: &Path, options: EnvOptions, data: &StateData, out: &mut Out<'_>) {
    let env = open(dir, "state", options);
    let mut txn = env.begin_rw().expect("begin");
    let key_len = u16::try_from(STATE_KEY).expect("a narrow key");
    let table = txn
        .create_dup_table("PlainStorageState", DupOptions::packed(key_len))
        .expect("create the dup table");
    txn.commit().expect("commit");
    settle(&env);
    let rows = data.rows();
    let mut time = Duration::ZERO;
    for first in (0..rows).step_by(STATE_BATCH) {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        for i in first..(first + STATE_BATCH).min(rows) {
            txn.put(&table, data.row(i), data.value(i)).expect("put");
        }
        txn.commit().expect("commit");
        time += start.elapsed();
        settle(&env);
    }
    out.push("reth.state.load", per_op(time, rows));
    out.push("reth.state.file_bytes", used_bytes(&env));
    let ro = Arc::new(env.begin_ro().expect("begin ro"));
    let mut cursor = ro.dup_cursor_owned(&table).expect("dup cursor");
    if out.wants("reth.state.get") {
        let start = Instant::now();
        let (mut hits, mut bytes) = (0, 0);
        for &i in &data.gets {
            let logical = data.row(i as usize);
            cursor.reset();
            let (k, v) = cursor
                .seek_dup(&logical[..STATE_KEY], &logical[STATE_KEY..])
                .expect("seek_dup")
                .expect("a row");
            hits += usize::from(k == logical);
            bytes += v.len();
        }
        let time = start.elapsed();
        assert_eq!(hits, data.gets.len(), "every get finds its row");
        black_box(bytes);
        out.push("reth.state.get", per_op(time, data.gets.len()));
    }
    if out.wants("reth.state.walk") {
        let start = Instant::now();
        let (mut walked, mut bytes) = (0, 0);
        for &k in &data.walks {
            let mut row = cursor.seek_key(data.key(k)).expect("seek_key");
            while let Some((_, v)) = row {
                walked += 1;
                bytes += v.len();
                row = cursor.next_in_prefix(STATE_KEY).expect("next_in_prefix");
            }
        }
        let time = start.elapsed();
        assert_eq!(walked, rows, "the walks read every row once");
        black_box(bytes);
        out.push("reth.state.walk", per_op(time, walked));
    }
}

/// The addresses a block of the `reth.changesets.*` rows changes.
const CS_ADDRESSES: usize = 200;
/// The `reth.changesets.*` rows commit their load once per this many blocks.
const CS_COMMIT_BLOCKS: usize = 10;
/// The blocks one walk of `reth.changesets.walk` reads.
const CS_WALK_BLOCKS: usize = 100;
/// The walks of `reth.changesets.walk`, at starts spread evenly over the blocks.
const CS_WALKS: usize = 10;
/// The StorageChangeSets key `block ‖ address` and its subkey `slot`, as one logical key.
const CS_STRIDE: usize = 8 + 20 + 32;

/// The seeded rows of the `reth.changesets.*` rows: about `n` rows of StorageChangeSets' shape,
/// in ascending order of their logical keys. Block `b`'s keys start with `b` big-endian; it
/// changes `CS_ADDRESSES` random addresses, each 1..=4 random slots, each with a value of 1..=32
/// bytes.
pub(crate) struct ChangeSetData {
    /// Every row's logical key `block ‖ address ‖ slot`, `CS_STRIDE` bytes each.
    keys: Vec<u8>,
    /// Every row's value: row `i`'s ends at `value_ends[i]` and starts where row `i - 1`'s ends.
    values: Vec<u8>,
    value_ends: Vec<u32>,
    /// The first row of each block, then the row count.
    block_starts: Vec<usize>,
    /// The first block of each walk, and the blocks each reads.
    walk_starts: Vec<usize>,
    walk_blocks: usize,
}

impl ChangeSetData {
    /// The rows of a run of `n`.
    pub(crate) fn new(n: usize) -> Self {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ 28);
        let blocks = (n / (CS_ADDRESSES * 5 / 2)).max(1);
        let sorted_set = |rng: &mut Rng, count: usize, len: usize| {
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
        let (mut keys, mut values, mut value_ends) = (Vec::new(), Vec::new(), Vec::new());
        let mut block_starts = Vec::with_capacity(blocks + 1);
        for b in 0..blocks as u64 {
            block_starts.push(value_ends.len());
            for address in sorted_set(&mut rng, CS_ADDRESSES, 20) {
                let slots = rng.within(1, 4);
                for slot in sorted_set(&mut rng, slots, 32) {
                    keys.extend_from_slice(&b.to_be_bytes());
                    keys.extend_from_slice(&address);
                    keys.extend_from_slice(&slot);
                    let len = rng.within(1, 32);
                    random_bytes(&mut rng, &mut values, len);
                    value_ends.push(u32::try_from(values.len()).expect("values within u32"));
                }
            }
        }
        block_starts.push(value_ends.len());
        let walk_blocks = blocks.min(CS_WALK_BLOCKS);
        let last = blocks - walk_blocks;
        let walk_starts = (0..CS_WALKS).map(|i| last * i / (CS_WALKS - 1)).collect();
        Self { keys, values, value_ends, block_starts, walk_starts, walk_blocks }
    }

    fn rows(&self) -> usize {
        self.value_ends.len()
    }

    fn blocks(&self) -> usize {
        self.block_starts.len() - 1
    }

    fn key(&self, i: usize) -> &[u8] {
        &self.keys[i * CS_STRIDE..][..CS_STRIDE]
    }

    fn value(&self, i: usize) -> &[u8] {
        let start = if i == 0 { 0 } else { self.value_ends[i - 1] as usize };
        &self.values[start..self.value_ends[i] as usize]
    }

    /// The rows of the walk from block `first`.
    fn walk_rows(&self, first: usize) -> usize {
        self.block_starts[first + self.walk_blocks] - self.block_starts[first]
    }
}

/// `reth.changesets.append` (every row appended to a composite plain table in order, a commit
/// every `CS_COMMIT_BLOCKS` blocks, commits included, each settled untimed; per row),
/// `reth.changesets.file_bytes`, and `reth.changesets.walk` (`CS_WALKS` walks as reth's
/// `walk_range` takes them: a seek to a block's first row, then steps across keys to the last row
/// of the walk's last block; per row read).
pub(crate) fn changesets(dir: &Path, options: EnvOptions, data: &ChangeSetData, out: &mut Out<'_>) {
    let env = open(dir, "changesets", options);
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("StorageChangeSets").expect("create");
    txn.commit().expect("commit");
    settle(&env);
    let mut time = Duration::ZERO;
    for first in (0..data.blocks()).step_by(CS_COMMIT_BLOCKS) {
        let last = (first + CS_COMMIT_BLOCKS).min(data.blocks());
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        for i in data.block_starts[first]..data.block_starts[last] {
            txn.append(&table, data.key(i), data.value(i)).expect("append");
        }
        txn.commit().expect("commit");
        time += start.elapsed();
        settle(&env);
    }
    out.push("reth.changesets.append", per_op(time, data.rows()));
    out.push("reth.changesets.file_bytes", used_bytes(&env));
    if out.wants("reth.changesets.walk") {
        let ro = Arc::new(env.begin_ro().expect("begin ro"));
        let start = Instant::now();
        let (mut walked, mut bytes) = (0, 0);
        for &first in &data.walk_starts {
            let mut cursor = ro.cursor_owned(&table).expect("cursor");
            let end = ((first + data.walk_blocks) as u64).to_be_bytes();
            let mut row = cursor.seek(data.key(data.block_starts[first])).expect("seek");
            while let Some((k, v)) = row {
                if k[..8] >= end[..] {
                    break;
                }
                walked += 1;
                bytes += v.len();
                row = cursor.next().expect("next");
            }
        }
        let time = start.elapsed();
        let expected: usize = data.walk_starts.iter().map(|&w| data.walk_rows(w)).sum();
        assert_eq!(walked, expected, "a walk reads its blocks' rows");
        black_box(bytes);
        out.push("reth.changesets.walk", per_op(time, walked));
    }
}

/// The transactions of each rewrite row.
const RW_TXNS: usize = 20;
/// The history rewrite row's key width: a 20-byte address ‖ the open shard's 8-byte `u64::MAX`.
const RW_HIST_KEY: usize = 28;
/// The history rewrite row's value lengths: `RW_HIST_MIN..=RW_HIST_MAX` bytes.
const RW_HIST_MIN: usize = 16;
const RW_HIST_MAX: usize = 160;
/// The bytes a history rewrite grows a value by (one block number more).
const RW_GROW: usize = 8;
/// The dup rewrite row's keys, which hold all its subkeys (TN's state: two stablecoins and a
/// router hold all the slots).
const RW_DUP_KEYS: usize = 3;
/// The dup rewrite row's key, subkey and value width (HashedStorages').
const RW_DUP_WIDTH: usize = 32;

/// The seeded inputs of `reth.history.rewrite` and `reth.dup.rewrite`: `n` rows a table, and
/// `RW_TXNS` transactions of `keys` present rows each, drawn in turn from a permutation (no row
/// twice while `n` is at least `RW_TXNS * keys`), sorted, as reth writes.
pub(crate) struct RewriteData {
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

/// A rewrite row's transactions: `RW_TXNS` runs of `keys` (at most `n`) rows of `0..n`, taken in
/// turn from a seeded permutation, unsorted.
fn rw_txns(rng: &mut Rng, n: usize, keys: usize) -> Vec<Vec<u32>> {
    let order = shuffled(rng, n);
    let keys = keys.min(n);
    (0..RW_TXNS).map(|r| (0..keys).map(|j| order[(r * keys + j) % n]).collect()).collect()
}

impl RewriteData {
    /// The inputs of a run of `n`: 10% of `n` keys a transaction, between 10 and 10k.
    pub(crate) fn new(n: usize) -> Self {
        let keys = (n / 10).clamp(10, 10_000);
        let mut rng = Rng(0x5eed_0000_0000_0010);
        let mut hist_keys = Vec::with_capacity(n * RW_HIST_KEY);
        let mut hist_lens = Vec::with_capacity(n);
        for _ in 0..n {
            random_bytes(&mut rng, &mut hist_keys, RW_HIST_KEY - 8);
            hist_keys.extend_from_slice(&u64::MAX.to_be_bytes());
            hist_lens.push(rng.within(RW_HIST_MIN, RW_HIST_MAX) as u8);
        }
        let mut dup_keys = Vec::new();
        random_bytes(&mut rng, &mut dup_keys, RW_DUP_KEYS * RW_DUP_WIDTH);
        let mut dup_subkeys = Vec::with_capacity(n * RW_DUP_WIDTH);
        random_bytes(&mut rng, &mut dup_subkeys, n * RW_DUP_WIDTH);
        let mut buf = Vec::new();
        random_bytes(&mut rng, &mut buf, SHIFTS + RW_HIST_MAX + RW_GROW);
        let mut this = Self {
            hist_keys,
            hist_lens,
            dup_keys,
            dup_subkeys,
            buf,
            hist_txns: Vec::new(),
            dup_txns: Vec::new(),
        };
        let mut hist_txns = rw_txns(&mut rng, n, keys);
        for picks in &mut hist_txns {
            picks.sort_unstable_by(|&a, &b| this.hist_key(a).cmp(this.hist_key(b)));
        }
        let mut dup_txns = rw_txns(&mut rng, n, keys);
        for picks in &mut dup_txns {
            picks.sort_unstable_by(|&a, &b| this.dup_row(a).cmp(&this.dup_row(b)));
        }
        this.hist_txns = hist_txns;
        this.dup_txns = dup_txns;
        this
    }

    fn rows(&self) -> u32 {
        self.hist_lens.len() as u32
    }

    fn hist_key(&self, i: u32) -> &[u8] {
        &self.hist_keys[i as usize * RW_HIST_KEY..][..RW_HIST_KEY]
    }

    /// History row `i`'s value as loaded, grown by `grow` bytes.
    fn hist_value(&self, i: u32, grow: usize) -> &[u8] {
        &self.buf[i as usize % SHIFTS..][..usize::from(self.hist_lens[i as usize]) + grow]
    }

    /// Dup row `i`'s key and subkey.
    fn dup_row(&self, i: u32) -> (&[u8], &[u8]) {
        let key = i as usize % RW_DUP_KEYS;
        (
            &self.dup_keys[key * RW_DUP_WIDTH..][..RW_DUP_WIDTH],
            &self.dup_subkeys[i as usize * RW_DUP_WIDTH..][..RW_DUP_WIDTH],
        )
    }

    /// Dup row `i`'s key ‖ subkey, the dup table's logical key.
    fn dup_stored(&self, i: u32) -> [u8; 2 * RW_DUP_WIDTH] {
        let (key, subkey) = self.dup_row(i);
        let mut stored = [0; 2 * RW_DUP_WIDTH];
        stored[..RW_DUP_WIDTH].copy_from_slice(key);
        stored[RW_DUP_WIDTH..].copy_from_slice(subkey);
        stored
    }

    /// Dup row `i`'s value as loaded (`round` 0) or as rewritten (1).
    fn dup_value(&self, i: u32, round: usize) -> &[u8] {
        &self.buf[(i as usize + round) % SHIFTS..][..RW_DUP_WIDTH]
    }
}

/// One rewrite row's passes: `load` fills a table in one transaction (untimed), then each of
/// `RW_TXNS` transactions runs `txn_loop` (given the transaction's index; timed) and commits and
/// settles (untimed). The loops' total time per key.
fn rewrite_pass(
    dir: &Path,
    name: &str,
    options: EnvOptions,
    load: impl FnOnce(&mut RwTxn) -> Table,
    mut txn_loop: impl FnMut(&mut RwTxn, &Table, usize) -> usize,
) -> f64 {
    let env = open(dir, name, options);
    let mut txn = env.begin_rw().expect("begin");
    let table = load(&mut txn);
    txn.commit().expect("commit");
    settle(&env);
    let (mut time, mut keys) = (Duration::ZERO, 0);
    for t in 0..RW_TXNS {
        let mut txn = env.begin_rw().expect("begin");
        let start = Instant::now();
        keys += txn_loop(&mut txn, &table, t);
        time += start.elapsed();
        txn.commit().expect("commit");
        settle(&env);
    }
    per_op(time, keys)
}

/// `reth.history.rewrite`: per key, reth's `append_history_index` on the open shard through one
/// detached write cursor a transaction: `TxnCursor::seek_exact`, then `TxnCursor::put` of the
/// value grown by `RW_GROW` bytes, over a plain table of `n` rows.
pub(crate) fn history_rewrite(
    dir: &Path,
    options: EnvOptions,
    data: &RewriteData,
    out: &mut Out<'_>,
) {
    let time = rewrite_pass(
        dir,
        "history",
        options,
        |txn| {
            let table = txn.create_table("history").expect("create");
            for i in 0..data.rows() {
                txn.put(&table, data.hist_key(i), data.hist_value(i, 0)).expect("put");
            }
            table
        },
        |txn, table, t| {
            let picks = &data.hist_txns[t];
            let mut cursor = txn.cursor_detached(table).expect("cursor");
            for &i in picks {
                let key = data.hist_key(i);
                let found = cursor.seek_exact(txn, key).expect("seek_exact").map(<[u8]>::len);
                black_box(found.expect("a present key"));
                cursor.put(txn, key, data.hist_value(i, RW_GROW)).expect("upsert");
            }
            picks.len()
        },
    );
    out.push("reth.history.rewrite", time);
}

/// `reth.dup.rewrite`: per pair, reth's `write_hashed_state` storage loop as engine calls, over a
/// HashedStorages-shaped dup table whose `n` rows sit under `RW_DUP_KEYS` nested keys:
/// `DupTxnCursor::seek_dup_with_key_tail`, then at the row `delete_current`,
/// `follower_shares_prefix` (and `prev_in_prefix` when that is false) and `insert` of the pair
/// with a new value.
pub(crate) fn dup_rewrite(dir: &Path, options: EnvOptions, data: &RewriteData, out: &mut Out<'_>) {
    let key_len = u16::try_from(RW_DUP_WIDTH).expect("a narrow key");
    let time = rewrite_pass(
        dir,
        "dup",
        options,
        |txn| {
            let table = txn.create_dup_table("dup", DupOptions::packed(key_len)).expect("create");
            for i in 0..data.rows() {
                txn.put(&table, &data.dup_stored(i), data.dup_value(i, 0)).expect("put");
            }
            table
        },
        |txn, table, t| {
            let picks = &data.dup_txns[t];
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
                assert!(cursor.delete_current(txn).expect("delete_current"), "at a row");
                if !cursor.follower_shares_prefix(txn, RW_DUP_WIDTH).expect("follower") {
                    cursor.prev_in_prefix(txn, RW_DUP_WIDTH).expect("prev_in_prefix");
                }
                cursor.insert(txn, &stored, data.dup_value(i, 1)).expect("upsert");
            }
            picks.len()
        },
    );
    out.push("reth.dup.rewrite", time);
}

/// A block's header length.
const HEADER_LEN: usize = 600;
/// A block's transactions (and receipts).
const BLOCK_TXS: usize = 50;
/// A transaction's and a receipt's lengths.
const TX_LEN: usize = 200;
const RECEIPT_LEN: usize = 150;
/// An account's encoding length.
const ACCOUNT_LEN: usize = 80;
/// The accounts a block changes.
const BLOCK_ACCOUNTS: usize = 100;
/// The storage slots a block draws (a draw of a slot already drawn in the block is dropped).
const BLOCK_SLOTS: usize = 300;
/// The storage slots of a block whose history index it extends (its first, in key order).
const BLOCK_HISTORY_SLOTS: usize = 100;
/// A history index value's lengths: `HIST_MIN..=HIST_MAX`; a value that would grow past the
/// longest starts a new shard of `HIST_MIN` bytes.
const HIST_MIN: usize = 16;
const HIST_MAX: usize = 160;
/// The hot contracts of the base state, each holding a fifth as many slots as there are
/// accounts (in nested trees); the other contracts hold 1..=8 slots each (packed).
const HOT_CONTRACTS: usize = 2;

/// One block's writes of `reth.block`, its keys precomputed and each list in key order.
struct BlockOps {
    /// The block's number.
    number: u64,
    /// The hashed accounts it changes.
    accounts: Vec<[u8; 32]>,
    /// The storage slots it changes: hashed account ‖ hashed slot, a dup table's logical key.
    slots: Vec<[u8; 64]>,
    /// Its AccountChangeSets keys: block ‖ address.
    account_changes: Vec<[u8; 28]>,
    /// Its StorageChangeSets keys: block ‖ address ‖ slot.
    storage_changes: Vec<[u8; 60]>,
    /// Its AccountsHistory keys: address ‖ `u64::MAX` (the open shard).
    account_history: Vec<[u8; 28]>,
    /// Its StoragesHistory keys: address ‖ slot ‖ `u64::MAX`.
    storage_history: Vec<[u8; 60]>,
}

/// The seeded inputs of `reth.block`: a base state (untimed) and the blocks written over it.
pub(crate) struct BlockData {
    /// The base's hashed accounts, in load order (random); contract `c` is account `c`.
    accounts: Vec<[u8; 32]>,
    /// The base's storage rows: contract ‖ slot.
    slots: Vec<[u8; 64]>,
    /// The blocks, in order.
    blocks: Vec<BlockOps>,
    /// The values: windows of this random buffer.
    buf: Vec<u8>,
}

/// `account[..20] ‖ tail ‖ suffix`, a key of a table keyed by address.
fn address_key<const N: usize>(account: &[u8], tail: &[u8], suffix: &[u8]) -> [u8; N] {
    let mut key = [0; N];
    key[..20].copy_from_slice(&account[..20]);
    key[20..20 + tail.len()].copy_from_slice(tail);
    key[20 + tail.len()..].copy_from_slice(suffix);
    key
}

/// `block ‖ rest`, a changeset key.
fn block_key<const N: usize>(block: u64, rest: &[u8]) -> [u8; N] {
    let mut key = [0; N];
    key[..8].copy_from_slice(&block.to_be_bytes());
    key[8..].copy_from_slice(rest);
    key
}

impl BlockData {
    /// The base and blocks of a run of `n`: `n / 10` accounts (1k to 100k), `n / 1000` blocks (4
    /// to 1000).
    pub(crate) fn new(n: usize) -> Self {
        let mut rng = Rng(0x5eed_0000_0000_0030);
        let base = (n / 10).clamp(1000, 100_000);
        let accounts: Vec<[u8; 32]> = (0..base).map(|_| rng.array()).collect();
        let contracts = HOT_CONTRACTS + base / 10;
        let mut contract_slots: Vec<Vec<[u8; 32]>> = (0..contracts)
            .map(|c| {
                let count = if c < HOT_CONTRACTS { base / 5 } else { rng.within(1, 8) };
                (0..count).map(|_| rng.array()).collect()
            })
            .collect();
        let slot_key = |c: usize, slot: &[u8; 32]| {
            let mut key = [0; 64];
            key[..32].copy_from_slice(&accounts[c]);
            key[32..].copy_from_slice(slot);
            key
        };
        let slots = contract_slots
            .iter()
            .enumerate()
            .flat_map(|(c, s)| s.iter().map(move |slot| (c, *slot)))
            .map(|(c, slot)| slot_key(c, &slot))
            .collect();
        let max = u64::MAX.to_be_bytes();
        let count = (n / 1000).clamp(4, 1000);
        let mut blocks = Vec::with_capacity(count);
        for number in 1..=count as u64 {
            let mut picked = BTreeSet::new();
            while picked.len() < BLOCK_ACCOUNTS {
                picked.insert(accounts[rng.below(base as u64) as usize]);
            }
            let accounts_changed: Vec<[u8; 32]> = picked.into_iter().collect();
            let mut slots_changed = BTreeSet::new();
            for _ in 0..BLOCK_SLOTS {
                let c = if rng.below(5) < 3 {
                    rng.below(HOT_CONTRACTS as u64) as usize
                } else {
                    HOT_CONTRACTS + rng.below((contracts - HOT_CONTRACTS) as u64) as usize
                };
                let held = &mut contract_slots[c];
                let slot = if !held.is_empty() && rng.below(5) != 0 {
                    held[rng.below(held.len() as u64) as usize]
                } else {
                    let slot = rng.array();
                    held.push(slot);
                    slot
                };
                slots_changed.insert(slot_key(c, &slot));
            }
            let slots_changed: Vec<[u8; 64]> = slots_changed.into_iter().collect();
            blocks.push(BlockOps {
                number,
                account_changes: accounts_changed
                    .iter()
                    .map(|a| block_key(number, &a[..20]))
                    .collect(),
                storage_changes: slots_changed
                    .iter()
                    .map(|s| block_key(number, &address_key::<52>(s, &s[32..], &[])))
                    .collect(),
                account_history: accounts_changed
                    .iter()
                    .map(|a| address_key(a, &[], &max))
                    .collect(),
                storage_history: slots_changed[..BLOCK_HISTORY_SLOTS.min(slots_changed.len())]
                    .iter()
                    .map(|s| address_key(s, &s[32..], &max))
                    .collect(),
                accounts: accounts_changed,
                slots: slots_changed,
            });
        }
        let mut buf = Vec::new();
        random_bytes(&mut rng, &mut buf, SHIFTS + WINDOW);
        Self { accounts, slots, blocks, buf }
    }

    /// `len` bytes of value for write `i`.
    fn value(&self, i: usize, len: usize) -> &[u8] {
        &self.buf[i % SHIFTS..][..len]
    }

    /// A history value's length for write `i`.
    fn hist_len(i: usize) -> usize {
        HIST_MIN + (i * 7919) % (HIST_MAX - HIST_MIN + 1)
    }
}

/// The tables of `reth.block`, in creation order.
struct BlockTables {
    headers: Table,
    transactions: Table,
    receipts: Table,
    accounts: Table,
    storages: Table,
    account_changes: Table,
    storage_changes: Table,
    account_history: Table,
    storage_history: Table,
}

/// Extend the history index at `key` through `cursor` as reth's `append_history_index` does: read
/// the open shard (`seek_exact`), then put it grown by the block number, or a new shard if it is
/// missing or would grow past `HIST_MAX`.
fn extend_history(
    txn: &mut RwTxn,
    cursor: &mut tn_lmdb::TxnCursor,
    scratch: &mut Vec<u8>,
    key: &[u8],
    number: u64,
) {
    scratch.clear();
    match cursor.seek_exact(txn, key).expect("seek_exact") {
        Some(shard) if shard.len() + 8 <= HIST_MAX => scratch.extend_from_slice(shard),
        _ => scratch.extend_from_slice(&[0; HIST_MIN - 8]),
    }
    scratch.extend_from_slice(&number.to_be_bytes());
    cursor.put(txn, key, scratch).expect("upsert");
}

/// `reth.block` (per block, its commit included): the base state put untimed (accounts, their
/// storage, both history indexes) and settled, then each block in one transaction appends
/// its header, `BLOCK_TXS` transactions and receipts, upserts `BLOCK_ACCOUNTS` accounts, upserts
/// its slots through a dup cursor (`seek_dup`, then `put`), appends its account and storage
/// changesets, extends the history index of each account and of its first
/// `BLOCK_HISTORY_SLOTS` slots, and commits (then settles, untimed). Then `reth.block.file_bytes`
/// and, under `SyncMode::Durable`, `reth.block.pages_synced` (the pages the block commits synced,
/// per block, from `Env::io_stats`).
pub(crate) fn block(dir: &Path, options: EnvOptions, data: &BlockData, out: &mut Out<'_>) {
    let env = open(dir, "block", options);
    let mut txn = env.begin_rw().expect("begin");
    let packed = DupOptions::packed(32);
    let t = BlockTables {
        headers: txn.create_table("Headers").expect("create"),
        transactions: txn.create_table("Transactions").expect("create"),
        receipts: txn.create_table("Receipts").expect("create"),
        accounts: txn.create_table("HashedAccounts").expect("create"),
        storages: txn.create_dup_table("HashedStorages", packed).expect("create"),
        account_changes: txn.create_table("AccountChangeSets").expect("create"),
        storage_changes: txn.create_table("StorageChangeSets").expect("create"),
        account_history: txn.create_table("AccountsHistory").expect("create"),
        storage_history: txn.create_table("StoragesHistory").expect("create"),
    };
    let max = u64::MAX.to_be_bytes();
    for (i, account) in data.accounts.iter().enumerate() {
        txn.put(&t.accounts, account, data.value(i, ACCOUNT_LEN)).expect("put");
        let key: [u8; 28] = address_key(account, &[], &max);
        txn.put(&t.account_history, &key, data.value(i, BlockData::hist_len(i))).expect("put");
    }
    for (i, slot) in data.slots.iter().enumerate() {
        txn.put(&t.storages, slot, data.value(i, 1 + i % 32)).expect("put");
        let key: [u8; 60] = address_key(slot, &slot[32..], &max);
        txn.put(&t.storage_history, &key, data.value(i, BlockData::hist_len(i))).expect("put");
    }
    txn.commit().expect("commit");
    settle(&env);
    let before = env.io_stats();
    let mut scratch = Vec::with_capacity(HIST_MAX);
    let mut time = Duration::ZERO;
    for (b, ops) in data.blocks.iter().enumerate() {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        let number = ops.number;
        txn.append(&t.headers, &number.to_be_bytes(), data.value(b, HEADER_LEN)).expect("header");
        let first_tx = (number - 1) * BLOCK_TXS as u64;
        for tx in first_tx..first_tx + BLOCK_TXS as u64 {
            let value = data.value(tx as usize, TX_LEN);
            txn.append(&t.transactions, &tx.to_be_bytes(), value).expect("transaction");
        }
        for tx in first_tx..first_tx + BLOCK_TXS as u64 {
            let value = data.value(tx as usize + 1, RECEIPT_LEN);
            txn.append(&t.receipts, &tx.to_be_bytes(), value).expect("receipt");
        }
        for (i, account) in ops.accounts.iter().enumerate() {
            txn.put(&t.accounts, account, data.value(b + i, ACCOUNT_LEN)).expect("account");
        }
        let mut cursor = txn.dup_cursor_detached(&t.storages).expect("dup cursor");
        for (i, slot) in ops.slots.iter().enumerate() {
            let found = cursor.seek_dup(&txn, &slot[..32], &slot[32..]).expect("seek_dup");
            black_box(found.map(|(_, v)| v.len()));
            cursor.put(&mut txn, slot, data.value(b + i, 1 + i % 32)).expect("slot");
        }
        for (i, key) in ops.account_changes.iter().enumerate() {
            let value = data.value(b + i, ACCOUNT_LEN);
            txn.append(&t.account_changes, key, value).expect("account change");
        }
        for (i, key) in ops.storage_changes.iter().enumerate() {
            let value = data.value(b + i, 1 + i % 32);
            txn.append(&t.storage_changes, key, value).expect("storage change");
        }
        let mut cursor = txn.cursor_detached(&t.account_history).expect("cursor");
        for key in &ops.account_history {
            extend_history(&mut txn, &mut cursor, &mut scratch, key, number);
        }
        let mut cursor = txn.cursor_detached(&t.storage_history).expect("cursor");
        for key in &ops.storage_history {
            extend_history(&mut txn, &mut cursor, &mut scratch, key, number);
        }
        txn.commit().expect("commit");
        time += start.elapsed();
        settle(&env);
    }
    let blocks = data.blocks.len();
    out.push("reth.block", per_op(time, blocks));
    out.push("reth.block.file_bytes", used_bytes(&env));
    if options.sync == SyncMode::Durable {
        let synced = env.io_stats().since(&before).pages_synced;
        out.push("reth.block.pages_synced", synced as f64 / blocks as f64);
    }
}
