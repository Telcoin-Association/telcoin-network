//! Transactions: read-only ones ([`RoTxn`]) on a published snapshot, and the single write
//! transaction ([`RwTxn`]), which builds the next state in pages no reader can reach and
//! publishes it at [`RwTxn::commit`]. Tables are named by [`Table`] handles, checked against
//! the catalog of the state they are used in; a read-only transaction's handle is checked once
//! per snapshot, which every transaction on it shares ([`TableRoots`]).

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::Instant,
};

#[cfg(test)]
use crate::env::CrashAt;
use crate::{
    catalog::{self, DupOptions, DupRecord, TableRecord},
    env::{EnvInner, Snapshot, SyncMode, Writer},
    error::{Error, Result},
    freelist,
    latch::UnwindLatch,
    meta::Meta,
    page,
    stats::elapsed_ns,
    tree::{
        cursor::{self, Cursor, Entry, KeyTail, State, TreeCursor},
        dup_cursor::{nested_cell, DupWalk, NestedRoots},
        read,
        write::{self, PutKind},
        PageRead, PageWrite, TreeRoot,
    },
};

/// A handle on one table: its name and its id. A committed table's id is never given to another
/// table over the file's life, so a handle of a table that was dropped (and perhaps re-created
/// under the same name) is refused with [`Error::StaleTable`] rather than reaching the new table;
/// a handle used in a transaction whose state predates the table is refused with
/// [`Error::TableNotFound`]. The id alone does not name a table: the id of a table created in a
/// transaction that aborted goes to the next table created, under its own name, and a handle of
/// another file may carry it too. A handle is the table with its id and its name.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Table {
    id: u32,
    name: Arc<str>,
}

impl Table {
    fn new(id: u32, name: &str) -> Self {
        Self { id, name: Arc::from(name) }
    }

    /// The table's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `self == other`, with the shortcut of a clone: handles that share the name's allocation
    /// skip the name compare.
    #[inline]
    fn is(&self, other: &Self) -> bool {
        self.id == other.id && (Arc::ptr_eq(&self.name, &other.name) || self.name == other.name)
    }

    /// Test-only: the table's id.
    #[cfg(test)]
    pub(crate) fn id(&self) -> u32 {
        self.id
    }
}

// A read-only transaction may be shared between threads.
const _: fn() = || {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<RoTxn>();
    send_sync::<RoCursor>();
    send_sync::<TxnCursor>();
    send_sync::<Table>();
};

// A read-only transaction is two `Arc`s, cheap to begin and to put in the `Arc` that
// `RoTxn::cursor_owned` takes: the table roots it resolves live in its snapshot (`TableRoots`).
const _: () = assert!(std::mem::size_of::<RoTxn>() == 2 * std::mem::size_of::<usize>());

/// Figures about one table in a transaction's state, read from its catalog record: O(1), with no
/// page read. An empty plain table has every figure zero; an empty dup table has them zero but
/// [`Self::dup`]. A dup table's page figures sum its outer tree's and its nested trees'; its
/// `entries` are its rows, and `height` is the outer tree's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableStat {
    /// The number of entries (a dup table's rows: every `k ‖ s` it holds).
    pub entries: u64,
    /// The height of the table's B+tree (0: empty; a dup table's outer tree).
    pub height: u32,
    /// The branch pages of its trees: every level above the leaves.
    pub branch_pages: u32,
    /// The leaf pages of its trees.
    pub leaf_pages: u32,
    /// The pages of the overflow runs holding its values too large for a leaf.
    pub overflow_pages: u32,
    /// A dup table's nested trees, one per outer key (0 for a plain table).
    pub nested_trees: u32,
    /// A dup table's shape; `None` for a plain table.
    pub dup: Option<DupOptions>,
}

impl TableStat {
    /// The figures of a table whose tree is `root`, with `dup` for a dup table: its outer and
    /// nested trees summed. The record rules bound the sums ([`TableRecord::decode`]).
    fn of(root: &TreeRoot, dup: Option<&DupRecord>) -> Self {
        let plain = Self {
            entries: root.entries,
            height: root.height,
            branch_pages: root.branch_pages,
            leaf_pages: root.leaf_pages,
            overflow_pages: root.overflow_pages,
            nested_trees: 0,
            dup: None,
        };
        let Some(d) = dup else { return plain };
        Self {
            entries: d.nested_entries + d.packed_rows,
            branch_pages: plain.branch_pages + d.nested_branch_pages,
            leaf_pages: plain.leaf_pages + d.nested_leaf_pages,
            overflow_pages: plain.overflow_pages + d.nested_overflow_pages,
            nested_trees: d.nested_trees,
            dup: Some(d.options()),
            ..plain
        }
    }
}

/// The reads both kinds of transaction offer, each on the transaction's own state. Damage a
/// read finds is [`Error::Corrupt`] and latches the environment (later writes fail; reads
/// continue).
pub trait ReadTxn {
    /// The table named `name`, if it exists in this state. [`Error::InvalidTableName`] unless
    /// the name is 1..=255 bytes.
    fn open_table(&self, name: &str) -> Result<Option<Table>>;

    /// The value stored under `key` in `table`, borrowed from the mapping for as long as the
    /// transaction lives. A value held in an overflow run is borrowed from the run's pages; a run
    /// of more than one page passes its full CRC first, so such a get reads every byte of it.
    ///
    /// In a dup table `key` is the logical key `k ‖ s`: its first `key_len` bytes are the outer
    /// key `k`, the rest the subkey `s`, and the value is `s`'s in `k`'s nested tree. A key of at
    /// most `key_len` bytes (no subkey) is [`Error::KeyTooShort`], with no latch; a key over the
    /// page size's `max_key` misses, as in a plain table. A write transaction reads its own
    /// unflushed nested trees.
    fn get<'t>(&'t self, table: &Table, key: &[u8]) -> Result<Option<&'t [u8]>>;

    /// A new, unset cursor over `table`: ordered access to its entries in both directions,
    /// borrowed from the transaction for as long as it lives (see [`Cursor`]).
    /// [`Error::TableKind`] for a dup table.
    fn cursor<'t>(&'t self, table: &Table) -> Result<Cursor<'t>>;

    /// The id of the state this transaction reads: a read-only transaction's snapshot, or the
    /// id the write transaction commits as.
    fn txn_id(&self) -> u64;

    /// Figures about `table` in this state, O(1) for either kind (see [`TableStat`]); a write
    /// transaction's include its changes not yet committed.
    fn table_stat(&self, table: &Table) -> Result<TableStat>;
}

/// Check `table` against the record `found` under its name in a state whose next table id is
/// `next_table_id`: the record of the handle's id, else [`Error::TableNotFound`] if the handle's
/// table was created after this state, else [`Error::StaleTable`] (it was dropped, perhaps
/// re-created).
fn resolve(found: Option<TableRecord>, table: &Table, next_table_id: u32) -> Result<TableRecord> {
    match found {
        Some(record) if record.id == table.id => Ok(record),
        _ if table.id >= next_table_id => Err(Error::TableNotFound(table.name().to_string())),
        _ => Err(Error::StaleTable(table.name().to_string())),
    }
}

/// One outer key's nested tree in the write transaction: an entry of [`Working::nested`], the
/// working map of a dup table. The map, not the outer cell, is the truth for its key until
/// commit: the cell may name a superseded root.
struct Nested {
    /// The working nested root: never empty while in the map (an emptied tree leaves with its
    /// cell).
    root: TreeRoot,
    /// Its provenance, as [`write::put_sized`]'s: true only after this transaction's writer wrote
    /// it. A root read from a cell is never trusted ([`load_cell`]).
    trusted: bool,
    /// True when the outer cell does not hold `root`: the flush at commit writes it
    /// ([`Work::flush_nested`]).
    changed: bool,
    /// The nested tree's write-path cache (boxed: the map's nodes then move small values).
    cache: Box<write::PathCache>,
}

/// The working map of a dup table: its nested trees written (or loaded for a write) in this
/// transaction, by outer key. Every outer key whose nested tree this transaction created or
/// changed has an entry from that write until commit, or until the key is removed, and the
/// entry's root is the key's current nested root; entries are never evicted. So a cell naming a
/// page private to this transaction always has an entry, and a cell with no entry holds the
/// committed root.
type NestedMap = BTreeMap<Box<[u8]>, Nested>;

/// Split a dup table's logical `key` into its outer key (the table's `key_len` bytes) and its
/// subkey (the rest, at least one byte), else [`Error::KeyTooShort`].
fn split_dup_key(key: &[u8], key_len: u16) -> Result<(&[u8], &[u8])> {
    let k = usize::from(key_len);
    if key.len() <= k {
        return Err(Error::KeyTooShort { len: key.len(), min: k + 1 });
    }
    Ok(key.split_at(k))
}

/// The outer cell of key `k` in the dup table `table`'s outer tree `outer`: the nested cell's
/// root bytes, `None` for no cell. Any other cell is [`Error::Corrupt`] (this build writes only
/// nested cells into an outer tree), as is a miss beside an outer key of another width than `k`'s
/// ([`read::find_outer`]).
fn outer_cell<'s, S: PageRead + ?Sized>(
    store: &'s S,
    table: &Table,
    outer: &TreeRoot,
    k: &[u8],
) -> Result<Option<&'s [u8; page::NESTED_CELL]>> {
    read::find_outer(store, outer, k)?.map(|val| nested_cell(table.name(), k, val)).transpose()
}

/// The nested root an outer cell of key `k` holds (`b`), decoded and checked: a cell names a
/// non-empty tree, else [`Error::Corrupt`].
fn cell_root(table: &Table, k: &[u8], b: &[u8; page::NESTED_CELL]) -> Result<TreeRoot> {
    let root = TreeRoot::decode(b)?;
    if root.is_empty() {
        return Err(Error::Corrupt(format!(
            "dup table {}: the outer cell of key {k:02x?} names an empty nested tree",
            table.name()
        )));
    }
    Ok(root)
}

/// The nested root of a cell met in the write transaction with no entry in the working map: the
/// committed root ([`NestedMap`]), untrusted. A cell naming a page private to this transaction
/// with no entry is [`Error::Corrupt`]: only a superseded root of this transaction can do that.
fn load_cell(
    writer: &Writer,
    table: &Table,
    k: &[u8],
    b: &[u8; page::NESTED_CELL],
) -> Result<TreeRoot> {
    let root = cell_root(table, k, b)?;
    if writer.is_private(root.root) {
        return Err(Error::Corrupt(format!(
            "dup table {}: the outer cell of key {k:02x?} names page {}, private to this \
             transaction, with no working root",
            table.name(),
            root.root
        )));
    }
    Ok(root)
}

/// The working map's entry of outer key `k` (the dup table `table`, outer tree `outer`, map
/// `map`), loaded from its cell on a miss ([`load_cell`]); `None` when the key has no cell.
/// Every dup write takes its nested root here.
fn nested_entry<'m>(
    writer: &Writer,
    table: &Table,
    outer: &TreeRoot,
    map: &'m mut NestedMap,
    k: &[u8],
) -> Result<Option<&'m mut Nested>> {
    if !map.contains_key(k) {
        let Some(b) = outer_cell(writer, table, outer, k)? else {
            return Ok(None);
        };
        let root = load_cell(writer, table, k, b)?;
        let entry = Nested { root, trusted: false, changed: false, cache: Box::default() };
        map.insert(k.into(), entry);
    }
    Ok(map.get_mut(k))
}

/// A dup table's totals in its working record (`record.dup` of a table whose kind was checked).
fn dup_mut(dup: &mut Option<DupRecord>) -> &mut DupRecord {
    dup.as_mut().expect("a dup table's working record holds its dup record")
}

/// The [`Error::Corrupt`] of dup totals that cannot count the nested tree of outer key `k` in
/// `table` (only a damaged record gets there).
#[cold]
fn totals_error(table: &Table, k: &[u8]) -> Error {
    Error::Corrupt(format!(
        "dup table {}: the dup table totals do not count the nested tree of key {k:02x?}",
        table.name()
    ))
}

/// Move the dup totals `dup` from nested tree `before` to `after` (of outer key `k` in `table`):
/// rows and pages, with checked arithmetic ([`totals_error`]).
fn apply_delta(
    dup: &mut DupRecord,
    table: &Table,
    k: &[u8],
    before: &TreeRoot,
    after: &TreeRoot,
) -> Result<()> {
    let pages = |n: u32, before: u32, after: u32| n.checked_sub(before)?.checked_add(after);
    let moved = (
        dup.nested_entries.checked_sub(before.entries).and_then(|n| n.checked_add(after.entries)),
        pages(dup.nested_branch_pages, before.branch_pages, after.branch_pages),
        pages(dup.nested_leaf_pages, before.leaf_pages, after.leaf_pages),
        pages(dup.nested_overflow_pages, before.overflow_pages, after.overflow_pages),
    );
    let (Some(entries), Some(branch), Some(leaf), Some(overflow)) = moved else {
        return Err(totals_error(table, k));
    };
    dup.nested_entries = entries;
    dup.nested_branch_pages = branch;
    dup.nested_leaf_pages = leaf;
    dup.nested_overflow_pages = overflow;
    Ok(())
}

/// Where a dup put left its row, for a [`DupTxnCursor`] to land on it ([`dup_put`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct DupPlaced {
    /// True if the put created its outer key (a new nested tree and its cell): the outer tree
    /// changed.
    created: bool,
    /// The row's slot in the leaf its nested tree's path cache ([`Nested::cache`]) then names;
    /// `None` when the put left that cache empty.
    slot: Option<usize>,
}

/// What a dup delete did, for a [`DupTxnCursor`] to land on the deleted row's follower
/// ([`dup_delete`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct DupRemoved {
    /// True if the row was there (and is now gone).
    found: bool,
    /// True if the delete emptied the key's nested tree, so the key's outer cell went too.
    emptied: bool,
    /// The deleted row's old slot ([`write::Removed::slot`]): in the leaf the nested tree's path
    /// cache names when the tree was kept, else in the leaf the table's outer path cache
    /// ([`Working::cache`]) names, where the removed cell was; `None` when that cache is empty.
    slot: Option<usize>,
}

/// The put, insert or append (`kind`) of the logical `key` and `value` into the dup table at `i`
/// in [`Work::tables`]: into the key's nested tree, or a new tree and its outer cell. An append's
/// order rule is the plain table's over the logical key: the outer key must not sort before the
/// table's last outer key, and with that key the subkey must sort after its last subkey.
#[inline(never)]
fn dup_put(
    work: &mut Work,
    i: usize,
    key: &[u8],
    value: &[u8],
    kind: PutKind,
) -> Result<DupPlaced> {
    let key_len = dup_mut(&mut work.tables[i].record.dup).key_len;
    let (k, s) = split_dup_key(key, key_len)?;
    let (writer, w) = work.writer_and_table(i);
    // Test-only: emptied caches per write, so every write descends.
    #[cfg(test)]
    if writer.no_path_cache {
        w.cache.forget();
        w.nested.values_mut().for_each(|n| n.cache.forget());
    }
    let new_key = match kind {
        PutKind::Append => match read::last_key(&*writer, &w.record.root)? {
            // Every outer key has the table's width (as `outer_cell`'s lookups hold it).
            Some(last) if last.len() != k.len() => {
                return Err(Error::Corrupt(format!(
                    "dup table {}: its last outer key has {} bytes, not the table's width {}",
                    w.table.name(),
                    last.len(),
                    k.len()
                )))
            }
            Some(last) if k < last => return Err(Error::AppendOutOfOrder),
            Some(last) => k != last,
            None => true,
        },
        PutKind::Upsert | PutKind::Insert => false,
    };
    let (mut outer, mut trusted) = (w.record.root, w.trusted);
    let entry =
        if new_key { None } else { nested_entry(writer, &w.table, &outer, &mut w.nested, k)? };
    if let Some(n) = entry {
        let before = n.root;
        let placed =
            write::put_cached(writer, &mut n.root, &mut n.trusted, &mut n.cache, s, value, kind)?;
        n.changed = true;
        apply_delta(dup_mut(&mut w.record.dup), &w.table, k, &before, &n.root)?;
        work.set_dup_record(i, outer, trusted);
        return Ok(DupPlaced { created: false, slot: placed.slot });
    }
    // A key with no cell: its nested tree first, then the cell naming it, current from the start.
    let (mut root, mut root_trusted) = (TreeRoot::EMPTY, false);
    let mut cache = Box::<write::PathCache>::default();
    let placed = write::put_cached(
        writer,
        &mut root,
        &mut root_trusted,
        &mut cache,
        s,
        value,
        PutKind::Upsert,
    )?;
    let outer_kind = if kind == PutKind::Append { PutKind::Append } else { PutKind::Insert };
    match write::put_outer_cell(
        writer,
        &mut outer,
        &mut trusted,
        &mut w.cache,
        k,
        &root,
        outer_kind,
    ) {
        Ok(_) => {}
        Err(Error::KeyExists | Error::AppendOutOfOrder) => {
            return Err(Error::Failed(format!(
                "dup table {}: the outer put refused the new cell of key {k:02x?}",
                w.table.name()
            )))
        }
        Err(e) => return Err(e),
    }
    let dup = dup_mut(&mut w.record.dup);
    dup.nested_trees = dup.nested_trees.checked_add(1).ok_or_else(|| totals_error(&w.table, k))?;
    apply_delta(dup, &w.table, k, &TreeRoot::EMPTY, &root)?;
    w.nested.insert(k.into(), Nested { root, trusted: root_trusted, changed: false, cache });
    work.set_dup_record(i, outer, trusted);
    Ok(DupPlaced { created: true, slot: placed.slot })
}

/// The delete of the logical `key` from the dup table at `i` in [`Work::tables`]: from the key's
/// nested tree, and the tree's outer cell with it when the delete empties the tree.
#[inline(never)]
fn dup_delete(work: &mut Work, i: usize, key: &[u8]) -> Result<DupRemoved> {
    let missed = DupRemoved { found: false, emptied: false, slot: None };
    let key_len = dup_mut(&mut work.tables[i].record.dup).key_len;
    let (k, s) = split_dup_key(key, key_len)?;
    let (writer, w) = work.writer_and_table(i);
    // Test-only: emptied caches per delete, so every delete descends, and no slot, so a cursor
    // finds its place again by key (as a plain table's delete under the same flag).
    #[cfg(test)]
    let uncached = writer.no_path_cache;
    #[cfg(test)]
    if uncached {
        w.cache.forget();
        w.nested.values_mut().for_each(|n| n.cache.forget());
    }
    let (mut outer, mut trusted) = (w.record.root, w.trusted);
    let Some(n) = nested_entry(writer, &w.table, &outer, &mut w.nested, k)? else {
        return Ok(missed);
    };
    let before = n.root;
    let removed = write::delete_cached(writer, &mut n.root, &mut n.trusted, &mut n.cache, s)?;
    if !removed.found {
        return Ok(missed);
    }
    let mut slot = removed.slot;
    let after = n.root;
    n.changed = true;
    let dup = dup_mut(&mut w.record.dup);
    apply_delta(dup, &w.table, k, &before, &after)?;
    if after.is_empty() {
        // The delete freed the tree's last leaf: its cell and its entry go too.
        let removed = write::delete_outer_cell(writer, &mut outer, &mut trusted, &mut w.cache, k)?;
        if !removed.found {
            return Err(no_cell(&w.table, k));
        }
        slot = removed.slot;
        w.nested.remove(k);
        dup.nested_trees =
            dup.nested_trees.checked_sub(1).ok_or_else(|| totals_error(&w.table, k))?;
    }
    work.set_dup_record(i, outer, trusted);
    #[cfg(test)]
    let slot = slot.filter(|_| !uncached);
    Ok(DupRemoved { found: true, emptied: after.is_empty(), slot })
}

/// The get of the logical `key` from the dup table `table` whose outer tree is `outer` in a
/// published state (`store`): the key's nested tree, through its outer cell, whose subkeys have
/// 1 to `max_subkey` bytes ([`nested_get`]).
#[inline(never)]
fn dup_get<'s, S: PageRead + ?Sized>(
    store: &'s S,
    table: &Table,
    outer: &TreeRoot,
    dup: &DupRecord,
    key: &[u8],
    max_subkey: usize,
) -> Result<Option<&'s [u8]>> {
    let (k, s) = split_dup_key(key, dup.key_len)?;
    match outer_cell(store, table, outer, k)? {
        None => Ok(None),
        Some(b) => nested_get(store, &cell_root(table, k, b)?, s, max_subkey),
    }
}

/// The get of subkey `s` from the nested tree `root`, a miss beside a subkey of another length
/// than 1 to `max_subkey` bytes [`Error::Corrupt`] ([`read::find_nested`]).
fn nested_get<'s, S: PageRead + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    s: &[u8],
    max_subkey: usize,
) -> Result<Option<&'s [u8]>> {
    read::find_nested(store, root, s, max_subkey)?.map(|v| store.value(v)).transpose()
}

/// [`dup_get`] in the write transaction: the key's nested root from the working map, else from
/// its cell ([`Work::nested_root`]).
#[inline(never)]
fn dup_get_in_txn<'t>(
    work: &'t Work,
    table: &Table,
    outer: &TreeRoot,
    dup: &DupRecord,
    key: &[u8],
) -> Result<Option<&'t [u8]>> {
    let (k, s) = split_dup_key(key, dup.key_len)?;
    match work.nested_root(table, outer, k)? {
        None => Ok(None),
        Some(root) => {
            let writer = work.writer();
            nested_get(writer, &root, s, writer.geometry.max_subkey(usize::from(dup.key_len)))
        }
    }
}

/// Test-only: every row of a dup table whose outer tree is `outer` in `store`, as `(logical key,
/// value)` in key order, each nested root from `working` (the write transaction's map) else from
/// its cell; every tree is walked with the test walkers' checks, and the totals in `dup` must
/// count exactly the trees walked.
#[cfg(test)]
fn dup_rows<S: PageRead + ?Sized>(
    store: &S,
    outer: &TreeRoot,
    dup: &DupRecord,
    working: impl Fn(&[u8]) -> Option<TreeRoot>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    use crate::tree::test_harness::{walk_outer_tree, walk_tree};
    let mut rows = Vec::new();
    let mut sums =
        DupRecord { key_len: dup.key_len, packed_max: dup.packed_max, ..DupRecord::default() };
    for (k, cell) in walk_outer_tree(store, outer).entries {
        assert_eq!(k.len(), usize::from(dup.key_len), "an outer key of the table's width");
        let root = working(&k).unwrap_or_else(|| {
            TreeRoot::decode(cell.as_slice().try_into().expect("a 32-byte cell")).expect("a root")
        });
        assert!(!root.is_empty(), "the outer cell of key {k:02x?} names an empty tree");
        sums.nested_trees += 1;
        sums.nested_entries += root.entries;
        sums.nested_branch_pages += root.branch_pages;
        sums.nested_leaf_pages += root.leaf_pages;
        sums.nested_overflow_pages += root.overflow_pages;
        for (s, v) in walk_tree(store, &root).entries {
            rows.push(([k.as_slice(), &s].concat(), v));
        }
    }
    assert_eq!(sums, *dup, "the dup totals against the trees walked");
    rows
}

/// The [`Error::Corrupt`] of a key with a nested tree in the working map but no outer cell in
/// the dup table `table`.
#[cold]
fn no_cell(table: &Table, k: &[u8]) -> Error {
    Error::Corrupt(format!("dup table {}: no outer cell for key {k:02x?}", table.name()))
}

/// The delete of every row under the outer key `key` from the dup table at `i` in
/// [`Work::tables`]: the key's nested tree is freed in one walk ([`write::free_subtree`]: its
/// pages are collected first, so damage frees nothing), then its cell and its entry go; returns
/// the rows removed. The totals are moved first, so a record that cannot count the tree frees
/// nothing either. A key of another width than the table's is refused before anything changes.
fn dup_delete_prefix(work: &mut Work, i: usize, key: &[u8]) -> Result<u64> {
    let key_len = usize::from(dup_mut(&mut work.tables[i].record.dup).key_len);
    if key.len() < key_len {
        return Err(Error::KeyTooShort { len: key.len(), min: key_len });
    }
    if key.len() > key_len {
        return Err(Error::KeyTooLarge { len: key.len(), max: key_len });
    }
    let (writer, w) = work.writer_and_table(i);
    #[cfg(test)]
    if writer.no_path_cache {
        w.cache.forget();
        w.nested.values_mut().for_each(|n| n.cache.forget());
    }
    let (mut outer, mut trusted) = (w.record.root, w.trusted);
    let Some(n) = nested_entry(writer, &w.table, &outer, &mut w.nested, key)? else {
        return Ok(0);
    };
    let (root, root_trusted) = (n.root, n.trusted);
    let mut totals = *dup_mut(&mut w.record.dup);
    totals.nested_trees =
        totals.nested_trees.checked_sub(1).ok_or_else(|| totals_error(&w.table, key))?;
    apply_delta(&mut totals, &w.table, key, &root, &TreeRoot::EMPTY)?;
    write::free_subtree(writer, &root, root_trusted)?;
    let removed = write::delete_outer_cell(writer, &mut outer, &mut trusted, &mut w.cache, key)?;
    if !removed.found {
        return Err(no_cell(&w.table, key));
    }
    w.nested.remove(key);
    w.record.dup = Some(totals);
    work.set_dup_record(i, outer, trusted);
    Ok(root.entries)
}

/// The nested roots of a free walk over a dup table's outer tree ([`write::free_dup_tree`]): for
/// a key the working map holds, its entry's root and provenance (each such hit counted), else the
/// cell's committed root, untrusted (so a cell naming a page private to this transaction with no
/// entry is refused by the walk). Once walked, every entry must have been met (each has its cell)
/// and the nested counters must be the table's totals, else nothing is freed.
struct WalkRoots<'a> {
    table: &'a Table,
    map: &'a NestedMap,
    dup: &'a DupRecord,
    hits: usize,
}

impl write::NestedRoots for WalkRoots<'_> {
    fn root(&mut self, key: &[u8], cell: &[u8; page::NESTED_CELL]) -> Result<(TreeRoot, bool)> {
        if let Some(n) = self.map.get(key) {
            self.hits += 1;
            return Ok((n.root, n.trusted));
        }
        Ok((cell_root(self.table, key, cell)?, false))
    }

    fn walked(&mut self, sums: &write::NestedSums) -> Result<()> {
        if self.hits != self.map.len() {
            return Err(Error::Failed(format!(
                "dup table {}: the free walk met {} of its {} working nested roots",
                self.table.name(),
                self.hits,
                self.map.len()
            )));
        }
        let d = self.dup;
        let fields = [
            ("nested trees", sums.trees, u64::from(d.nested_trees)),
            ("nested entries", sums.entries, d.nested_entries),
            ("nested branch pages", sums.branch, u64::from(d.nested_branch_pages)),
            ("nested leaf pages", sums.leaf, u64::from(d.nested_leaf_pages)),
            ("nested overflow pages", sums.overflow, u64::from(d.nested_overflow_pages)),
        ];
        for (field, walked, record) in fields {
            if walked != record {
                return Err(Error::Corrupt(format!(
                    "dup table {}: its nested trees count {walked} {field}, its record {record}",
                    self.table.name()
                )));
            }
        }
        Ok(())
    }
}

/// Free the dup table `table`'s outer tree `outer` (provenance `trusted`) and every nested tree
/// its cells name, each root through the working map `map` ([`WalkRoots`]), checked against the
/// totals `dup` before anything is freed ([`write::free_dup_tree`]).
fn free_dup_table(
    writer: &mut Writer,
    table: &Table,
    outer: &TreeRoot,
    trusted: bool,
    dup: &DupRecord,
    map: &NestedMap,
) -> Result<()> {
    let mut roots = WalkRoots { table, map, dup, hits: 0 };
    write::free_dup_tree(writer, outer, trusted, usize::from(dup.key_len), &mut roots).map(drop)
}

// Test-only counter, debug builds only: the benchmark is compiled with the test configuration,
// and a thread-local update on a timed path would show in its release timings.
#[cfg(all(test, debug_assertions))]
thread_local! {
    /// Test-only: how many times a [`RoTxn`] resolved a handle through the catalog.
    pub(crate) static CATALOG_RESOLVES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// How many tables a snapshot's [`TableRoots`] remembers.
const SNAPSHOT_TABLES: usize = 32;

/// A table resolved in a snapshot: its tree's root and, for a dup table, the dup fields of its
/// record.
type Resolved = (TreeRoot, Option<DupRecord>);

/// The tree roots (with a dup table's [`DupRecord`]) of the tables resolved in one snapshot,
/// shared by every [`RoTxn`] on it ([`RoTxn::root`], [`RoTxn::resolved`]). The snapshot's catalog
/// never changes, so a handle resolved once resolves
/// the same for the snapshot's life, and each commit publishes a new snapshot with an empty set.
/// Only a resolved handle is remembered: a refused one is checked again each time. A handle's
/// slot is the first one from `id % SNAPSHOT_TABLES` on (wrapping) that no other handle holds,
/// and a slot is never emptied, so a lookup stops at the first empty slot. A hit is
/// [`Table::is`]: the id, then the name (a handle of another file, or one of a table an aborted
/// transaction created, may carry the same id). Once every slot holds another handle, a handle
/// resolves through the catalog on each use.
#[derive(Default)]
pub(crate) struct TableRoots([OnceLock<(Table, TreeRoot, Option<DupRecord>)>; SNAPSHOT_TABLES]);

impl std::fmt::Debug for TableRoots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let known = self.0.iter().filter(|slot| slot.get().is_some()).count();
        f.debug_struct("TableRoots").field("known", &known).finish()
    }
}

impl TableRoots {
    /// The slot a lookup of `table` starts from.
    #[inline]
    fn home(table: &Table) -> usize {
        table.id as usize % SNAPSHOT_TABLES
    }

    /// `table`'s slot if its first slot holds it: the hit path of [`RoTxn::root`] and
    /// [`RoTxn::resolved`].
    #[inline]
    fn at_home(&self, table: &Table) -> Option<&(Table, TreeRoot, Option<DupRecord>)> {
        match self.0[Self::home(table)].get() {
            Some(slot) if slot.0.is(table) => Some(slot),
            _ => None,
        }
    }

    /// `table`'s resolution, or else the first empty slot of its probe (`None`: every slot holds
    /// another handle).
    fn find(&self, table: &Table) -> std::result::Result<Resolved, Option<usize>> {
        let home = Self::home(table);
        for i in 0..SNAPSHOT_TABLES {
            let at = (home + i) % SNAPSHOT_TABLES;
            match self.0[at].get() {
                Some((known, root, dup)) if known.is(table) => return Ok((*root, *dup)),
                Some(_) => {}
                None => return Err(Some(at)),
            }
        }
        Err(None)
    }

    /// Remember `table`'s resolution in the first empty slot from `from` on, the one
    /// [`Self::find`] gave. A racing reader may take that slot first: for this handle, which ends
    /// the search, or for another, which moves it to the next.
    fn remember(&self, from: usize, table: &Table, (root, dup): Resolved) {
        for i in 0..SNAPSHOT_TABLES {
            let slot = &self.0[(from + i) % SNAPSHOT_TABLES];
            if slot.get_or_init(|| (table.clone(), root, dup)).0.is(table) {
                return;
            }
        }
    }
}

/// A read-only transaction: one committed state of every table, unaffected by later commits.
/// The state's pages stay intact while it lives.
pub struct RoTxn {
    inner: Arc<EnvInner>,
    snapshot: Arc<Snapshot>,
}

impl std::fmt::Debug for RoTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoTxn").field("txn", &self.snapshot.meta.txn).finish_non_exhaustive()
    }
}

impl RoTxn {
    pub(crate) fn new(inner: Arc<EnvInner>, snapshot: Arc<Snapshot>) -> Self {
        Self { inner, snapshot }
    }

    /// Run a read, latching damage it finds.
    fn read<'t, T>(&'t self, op: impl FnOnce(&'t Snapshot) -> Result<T>) -> Result<T> {
        op(&self.snapshot).inspect_err(|e| self.inner.latch.latch_error(e))
    }

    /// The tree root of the plain table `table` in the snapshot (see [`resolve`]): from the
    /// snapshot's [`TableRoots`] once a reader on it resolved the handle, else from the catalog
    /// ([`Self::resolve_root`]). A dup table is [`Error::TableKind`]: the root of its outer tree
    /// is not a tree of the table's rows.
    #[inline]
    fn root(&self, table: &Table) -> Result<TreeRoot> {
        match self.snapshot.tables.at_home(table) {
            Some((_, root, None)) => Ok(*root),
            _ => self.resolve_root(table),
        }
    }

    /// [`Self::root`] past its hit path: [`Self::resolve_table`], then the kind.
    #[inline(never)]
    fn resolve_root(&self, table: &Table) -> Result<TreeRoot> {
        match self.resolve_table(table)? {
            (root, None) => Ok(root),
            (_, Some(_)) => Err(Error::TableKind(table.name().to_string())),
        }
    }

    /// `table`'s tree root and, for a dup table, its [`DupRecord`] in the snapshot: [`Self::root`]
    /// for a table of either kind.
    #[inline]
    fn resolved(&self, table: &Table) -> Result<Resolved> {
        match self.snapshot.tables.at_home(table) {
            Some((_, root, dup)) => Ok((*root, *dup)),
            None => self.resolve_table(table),
        }
    }

    /// `table`'s resolution past the handle's first slot: the rest of its probe, else the
    /// catalog, whose answer is remembered if it resolved and a slot is free.
    #[inline(never)]
    fn resolve_table(&self, table: &Table) -> Result<Resolved> {
        let tables = &self.snapshot.tables;
        let free = match tables.find(table) {
            Ok(resolved) => return Ok(resolved),
            Err(free) => free,
        };
        #[cfg(all(test, debug_assertions))]
        CATALOG_RESOLVES.with(|n| n.set(n.get() + 1));
        let meta = &self.snapshot.meta;
        let found = catalog::get(&*self.snapshot, &meta.catalog, table.name())?;
        let record = resolve(found, table, meta.next_table_id)?;
        let resolved = (record.root, record.dup);
        if let Some(from) = free {
            tables.remember(from, table, resolved);
        }
        Ok(resolved)
    }

    /// [`ReadTxn::get`] past its hit path (a plain table in its first slot, read from the slot
    /// with no copy of the resolution): the handle resolved, then the get of its kind.
    #[inline(never)]
    fn get_past_home<'t>(&'t self, table: &Table, key: &[u8]) -> Result<Option<&'t [u8]>> {
        match self.resolved(table)? {
            (root, None) => read::get(&*self.snapshot, &root, key),
            (outer, Some(dup)) => {
                let max_subkey = self.snapshot.geometry.max_subkey(usize::from(dup.key_len));
                dup_get(&*self.snapshot, table, &outer, &dup, key, max_subkey)
            }
        }
    }

    /// Test-only: every row of the dup table `table` in this snapshot, checked ([`dup_rows`]).
    #[cfg(test)]
    pub(crate) fn dup_rows_for_test(&self, table: &Table) -> Vec<(Vec<u8>, Vec<u8>)> {
        let (outer, dup) = self.resolved(table).expect("the table resolves");
        dup_rows(&*self.snapshot, &outer, &dup.expect("a dup table"), |_| None)
    }

    /// A new, unset cursor over `table` that owns a share of this transaction (see
    /// [`RoCursor`]): it has no lifetime, and keeps the snapshot readable after every other
    /// handle of the transaction drops. [`Error::TableKind`] for a dup table.
    #[inline]
    pub fn cursor_owned(self: &Arc<Self>, table: &Table) -> Result<RoCursor> {
        let root = self.read(|_| self.root(table))?;
        Ok(RoCursor { txn: Arc::clone(self), root, tree: TreeCursor::default() })
    }

    /// A new, unset cursor over the dup table `table` that owns a share of this transaction, as
    /// [`Self::cursor_owned`]'s does (see [`DupRoCursor`]). [`Error::TableKind`] for a plain
    /// table.
    pub fn dup_cursor_owned(self: &Arc<Self>, table: &Table) -> Result<DupRoCursor> {
        let (outer_root, dup) = self.read(|_| self.resolved(table))?;
        let Some(dup) = dup else {
            return Err(Error::TableKind(table.name().to_string()));
        };
        Ok(DupRoCursor {
            txn: Arc::clone(self),
            outer_root,
            table: table.clone(),
            walk: DupWalk::new(
                dup.key_len,
                self.snapshot.geometry.max_subkey(usize::from(dup.key_len)),
            ),
        })
    }
}

/// A cursor over one table of a read-only transaction that owns a share of the transaction
/// (created by [`RoTxn::cursor_owned`] on an `Arc<RoTxn>`). It has no lifetime: it can be stored,
/// sent to another thread, and outlive every other handle of the transaction, whose snapshot
/// stays readable (and its pages unreused) until the last share drops. Its moves, states and
/// prefix steps are a [`Cursor`]'s; the entries it returns, overflow values included, borrow the
/// cursor until its next call. Where a [`Cursor`] keeps the page of its leaf, each `next` and
/// `prev` fetches it again by page number, without the per-fetch checks the page passed when the
/// cursor entered it (a snapshot's pages do not change while it lives); every leaf it enters gets
/// those checks.
///
/// Damage a move finds is [`Error::Corrupt`], leaves the cursor unset and latches the environment
/// (later writes fail; reads continue).
pub struct RoCursor {
    txn: Arc<RoTxn>,
    /// The table's tree in the transaction's snapshot.
    root: TreeRoot,
    tree: TreeCursor,
}

impl std::fmt::Debug for RoCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoCursor")
            .field("txn", &self.txn.snapshot.meta.txn)
            .field("state", &self.tree.state())
            .finish_non_exhaustive()
    }
}

impl RoCursor {
    /// Test-only: where the cursor is.
    #[cfg(test)]
    pub(crate) fn state(&self) -> cursor::State {
        self.tree.state()
    }

    /// Run a move over the snapshot, latching damage it finds.
    fn moved<'c, T>(
        &'c mut self,
        op: impl FnOnce(&mut TreeCursor, &'c Snapshot, &TreeRoot) -> Result<T>,
    ) -> Result<T> {
        let Self { txn, root, tree } = self;
        let txn: &'c RoTxn = txn;
        op(tree, &txn.snapshot, root).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Move to the first entry and return it (`None` for an empty table).
    pub fn first(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.first(store, root))
    }

    /// Move to the last entry and return it (`None` for an empty table).
    pub fn last(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.last(store, root))
    }

    /// Move to the next entry and return it; see [`Cursor::next`].
    // A cursor's moves are fallible and go both ways: it is not an `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.step(true)
    }

    /// Move to the previous entry and return it; see [`Cursor::prev`].
    pub fn prev(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.step(false)
    }

    /// [`Self::next`] (`forward`) or [`Self::prev`]: from an entry, from the page of its leaf
    /// fetched again without the per-fetch checks it passed when the cursor entered it
    /// ([`Snapshot::page_again`]).
    fn step(&mut self, forward: bool) -> Result<Option<Entry<'_>>> {
        self.moved(|c, store, root| match c.leaf_pgno().and_then(|p| store.page_again(p)) {
            Some(leaf) => c.step_from(store, root, leaf, forward),
            None if forward => c.next(store, root),
            None => c.prev(store, root),
        })
    }

    /// The entry the cursor is at, if it is at one. The cursor does not move.
    pub fn current(&self) -> Result<Option<(&[u8], &[u8])>> {
        self.tree.current(&*self.txn.snapshot).inspect_err(|e| self.txn.inner.latch.latch_error(e))
    }

    /// The entry the cursor is at, if it is at one, as its key and its value with the key's last
    /// `t` bytes before it; see [`Cursor::current_with_key_tail`]. The leaf is fetched again as
    /// [`Self::next`] fetches it. The cursor does not move.
    #[inline]
    pub fn current_with_key_tail(&self, t: usize) -> Result<Option<(&[u8], KeyTail<'_>)>> {
        let snapshot = &*self.txn.snapshot;
        let leaf = self.tree.leaf_pgno().and_then(|p| snapshot.page_again(p));
        self.tree
            .current_with_key_tail(snapshot, leaf, t)
            .inspect_err(|e| self.txn.inner.latch.latch_error(e))
    }

    /// Move to the first entry whose key is `>= key` and return it; `None` (the cursor after the
    /// end) if there is none. From an entry, the seek starts from the page of its leaf, fetched
    /// again as [`Self::next`] fetches it (see [`Cursor::seek`]).
    //
    // A cursor not at an entry seeks with no leaf, from the root: that constant `None` folds the
    // seek from a leaf away, so the seek of a new cursor (the adapter makes one per read)
    // compiles as the root descent alone, plus one compare. A cursor at an entry makes one call,
    // `seek_at_entry`. `seek_exact` and `seek_with_key_tail` are built the same way.
    #[inline]
    pub fn seek(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        if self.tree.state() == State::At {
            self.seek_at_entry(key)
        } else {
            self.moved(|c, store, root| c.seek(store, root, None, key))
        }
    }

    /// [`Self::seek`] from an entry: from the page of its leaf ([`Self::leaf`]). Never inlined:
    /// that fetch and the seek from the leaf, inline in the seek's body, grew the seek of a new
    /// cursor too, which never takes them.
    #[inline(never)]
    fn seek_at_entry(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.seek(store, root, Self::leaf(c, store), key))
    }

    /// The page of the leaf `tree` is at, fetched again without the per-fetch checks it passed
    /// when the cursor entered it ([`Snapshot::page_again`]), for a seek from it.
    #[inline]
    fn leaf<'s>(tree: &TreeCursor, snapshot: &'s Snapshot) -> Option<&'s [u8]> {
        tree.leaf_pgno().and_then(|p| snapshot.page_again(p))
    }

    /// [`Self::seek`] followed by [`Self::current_with_key_tail`], from one leaf access: move to
    /// the first entry whose key is `>= key` and return it as its key and its value with the
    /// key's last `t` bytes before it ([`KeyTail`]); `None` (the cursor after the end) if there
    /// is none. The entry is read from the leaf the descent fetched and parsed, an overflow value
    /// once. The cursor is left as [`Self::seek`] leaves it.
    #[inline]
    pub fn seek_with_key_tail(
        &mut self,
        key: &[u8],
        t: usize,
    ) -> Result<Option<(&[u8], KeyTail<'_>)>> {
        if self.tree.state() == State::At {
            self.seek_with_key_tail_at_entry(key, t)
        } else {
            self.moved(|c, store, root| c.seek_with_key_tail(store, root, None, key, t))
        }
    }

    /// [`Self::seek_with_key_tail`] from an entry, never inlined as [`Self::seek_at_entry`] is
    /// not.
    #[inline(never)]
    fn seek_with_key_tail_at_entry(
        &mut self,
        key: &[u8],
        t: usize,
    ) -> Result<Option<(&[u8], KeyTail<'_>)>> {
        self.moved(|c, store, root| c.seek_with_key_tail(store, root, Self::leaf(c, store), key, t))
    }

    /// Move to the entry `key` and return its value; `None` (the cursor unset) if there is none.
    /// The seek starts as [`Self::seek`]'s does.
    #[inline]
    pub fn seek_exact(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        if self.tree.state() == State::At {
            self.seek_exact_at_entry(key)
        } else {
            self.moved(|c, store, root| c.seek_exact(store, root, None, key))
        }
    }

    /// [`Self::seek_exact`] from an entry, never inlined as [`Self::seek_at_entry`] is not.
    #[inline(never)]
    fn seek_exact_at_entry(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        self.moved(|c, store, root| c.seek_exact(store, root, Self::leaf(c, store), key))
    }

    /// Move to the next entry only if its key shares the first `n` bytes of the current entry's
    /// key, and return it; else return `None` and stay at the current entry. The edge rules are
    /// [`Cursor::next_in_prefix`]'s.
    pub fn next_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.step_in_prefix(store, root, None, n, true).map(cursor::entry))
    }

    /// Move to the previous entry only if its key shares the first `n` bytes of the current
    /// entry's key, and return it; else return `None` and stay at the current entry. The edge
    /// rules are [`Cursor::prev_in_prefix`]'s.
    pub fn prev_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| {
            c.step_in_prefix(store, root, None, n, false).map(cursor::entry)
        })
    }
}

/// A reader's nested roots: each outer cell's own ([`cell_root`]), a snapshot's cells being its
/// truth.
struct SnapshotRoots<'a> {
    table: &'a Table,
}

impl NestedRoots for SnapshotRoots<'_> {
    fn name(&self) -> &str {
        self.table.name()
    }

    #[inline]
    fn root(&mut self, k: &[u8], cell: &[u8; page::NESTED_CELL]) -> Result<TreeRoot> {
        cell_root(self.table, k, cell)
    }
}

/// A cursor over one dup table of a read-only transaction that owns a share of the transaction
/// (created by [`RoTxn::dup_cursor_owned`] on an `Arc<RoTxn>`), as a [`RoCursor`] does.
///
/// It walks the table's rows as the logical keys `k ‖ s` in order, the rows of the composite
/// twin (a plain table keyed `k ‖ s` holding the same rows), and every call gives what the same
/// call of a [`RoCursor`] gives on the twin: the same states, edge rules and prefix steps (a
/// prefix of at least the key width `K` never leaves the outer key, since no other outer key
/// shares its `K` bytes), and [`Self::seek_exact`] of a key of at most `K` bytes misses. Two
/// calls differ by design: [`Self::current_with_key_tail`] with a tail longer than the subkey
/// gives the value apart, and the bounded seeks ([`Self::seek_key`], [`Self::seek_dup`],
/// [`Self::seek_dup_with_key_tail`]) stay inside one outer key and leave the cursor unset when
/// it has no such row, reading no other key's nested tree.
///
/// The key a call returns is the cursor's copy of the logical key (no page holds `k ‖ s` in one
/// piece); the value borrows the snapshot. Both borrow the cursor until its next call. A move
/// inside an outer key starts from the nested leaf the cursor is at, a move to another key from
/// the outer leaf, each fetched again without the per-fetch checks it passed when the cursor
/// entered it, as [`RoCursor`]'s moves fetch their leaf.
///
/// Damage a move finds is [`Error::Corrupt`], leaves the cursor unset and latches the environment
/// (later writes fail; reads continue): a cell that is not a nested cell, an empty or damaged
/// nested root, an outer key of another width than the table's, and every check of a descent.
pub struct DupRoCursor {
    txn: Arc<RoTxn>,
    /// The table's outer tree in the transaction's snapshot.
    outer_root: TreeRoot,
    table: Table,
    walk: DupWalk,
}

impl std::fmt::Debug for DupRoCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DupRoCursor")
            .field("txn", &self.txn.snapshot.meta.txn)
            .field("table", &self.table.name())
            .field("state", &self.walk.state())
            .finish_non_exhaustive()
    }
}

/// What a walker's move gives a dup cursor shell: the value of the row it landed on.
type Walked<'s> = Result<Option<&'s [u8]>>;

impl DupRoCursor {
    /// Test-only: where the cursor is.
    #[cfg(test)]
    pub(crate) fn state(&self) -> cursor::State {
        self.walk.state()
    }

    /// Test-only: the first and last keys of the outer and the nested leaf the cursor is at.
    #[cfg(test)]
    pub(crate) fn leaf_bounds_for_test(&self) -> [crate::tree::dup_cursor::LeafBounds; 2] {
        self.walk.leaf_bounds_for_test(&*self.txn.snapshot)
    }

    /// Run a walker move over the snapshot, latching damage it finds, and pair the value it
    /// returns with the logical key. `op` takes the walker, the snapshot, the outer root and the
    /// root source.
    #[inline]
    fn walked<'c>(
        &'c mut self,
        op: impl FnOnce(&mut DupWalk, &'c Snapshot, &TreeRoot, &mut SnapshotRoots<'_>) -> Walked<'c>,
    ) -> Result<Option<(&'c [u8], &'c [u8])>> {
        let Self { txn, outer_root, table, walk } = self;
        let txn: &'c RoTxn = txn;
        let mut roots = SnapshotRoots { table };
        let value = op(walk, &txn.snapshot, outer_root, &mut roots)
            .inspect_err(|e| txn.inner.latch.latch_error(e))?;
        let walk: &'c DupWalk = walk;
        Ok(value.map(|v| (walk.key(), v)))
    }

    /// Move to the first row and return it (`None` for an empty table).
    pub fn first(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.walked(|w, store, root, roots| w.start(store, root, roots, true))
    }

    /// Move to the last row and return it (`None` for an empty table).
    pub fn last(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.walked(|w, store, root, roots| w.start(store, root, roots, false))
    }

    /// Move to the next row and return it; see [`Cursor::next`].
    // A cursor's moves are fallible and go both ways: it is not an `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.walked(|w, store, root, roots| w.step(store, root, roots, &again(store), true))
    }

    /// Move to the previous row and return it; see [`Cursor::prev`].
    pub fn prev(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.walked(|w, store, root, roots| w.step(store, root, roots, &again(store), false))
    }

    /// The row the cursor is at, if it is at one. The cursor does not move.
    pub fn current(&self) -> Result<Option<(&[u8], &[u8])>> {
        let snapshot = &*self.txn.snapshot;
        let value = self
            .walk
            .current(snapshot, &again(snapshot))
            .inspect_err(|e| self.txn.inner.latch.latch_error(e))?;
        Ok(value.map(|v| (self.walk.key(), v)))
    }

    /// The row the cursor is at, if it is at one, as its logical key and its value with the key's
    /// last `t` bytes before it ([`KeyTail`]). A tail inside the subkey (`t <= |s|`) and an inline
    /// value come [`KeyTail::Joined`], one slice of the nested leaf; a value in a run, and any
    /// `t > |s|`, come [`KeyTail::Apart`], for the caller to join `key[key.len() - min(t,
    /// key.len())..]`. Either way the bytes are those [`RoCursor::current_with_key_tail`] gives on
    /// the twin. The cursor does not move.
    pub fn current_with_key_tail(&self, t: usize) -> Result<Option<(&[u8], KeyTail<'_>)>> {
        let snapshot = &*self.txn.snapshot;
        let tail = self
            .walk
            .current_with_key_tail(snapshot, &again(snapshot), t)
            .inspect_err(|e| self.txn.inner.latch.latch_error(e))?;
        Ok(tail.map(|tail| (self.walk.key(), tail)))
    }

    /// Move to the first row whose logical key is `>= key` (of any length) and return it; `None`
    /// (the cursor after the end) if there is none. From a row of the outer key `key` starts
    /// with, the outer tree is not read again and the nested seek starts from the nested leaf
    /// the cursor is at; from a row of another key the outer seek starts from the outer leaf it
    /// is at (see [`Cursor::seek`]).
    //
    // A cursor not at a row seeks from the roots with no leaf, as `RoCursor::seek` does; a
    // cursor at a row makes one call, `seek_at_row`. The other seeks are built the same way.
    #[inline]
    pub fn seek(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        if self.walk.state() == State::At {
            self.seek_at_row(key)
        } else {
            self.walked(|w, store, root, roots| w.seek(store, root, roots, &no_leaf, key))
        }
    }

    /// [`Self::seek`] from a row: from the leaves the cursor is at. Never inlined, as
    /// [`RoCursor::seek`]'s seek from an entry is not.
    #[inline(never)]
    fn seek_at_row(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.walked(|w, store, root, roots| w.seek(store, root, roots, &again(store), key))
    }

    /// [`Self::seek`] followed by [`Self::current_with_key_tail`]: move to the first row whose
    /// logical key is `>= key` and return it as its key and its value with the key's last `t`
    /// bytes ([`KeyTail`]); `None` (the cursor after the end) if there is none.
    #[inline]
    pub fn seek_with_key_tail(
        &mut self,
        key: &[u8],
        t: usize,
    ) -> Result<Option<(&[u8], KeyTail<'_>)>> {
        self.seek(key)?;
        self.current_with_key_tail(t)
    }

    /// Move to the row `key` and return its value; `None` (the cursor unset) if there is none. A
    /// key of at most the key width is no row's and misses with no read. The seek starts as
    /// [`Self::seek`]'s does.
    #[inline]
    pub fn seek_exact(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        let found = if self.walk.state() == State::At {
            self.seek_exact_at_row(key)
        } else {
            self.walked(|w, store, root, roots| w.seek_exact(store, root, roots, &no_leaf, key))
        };
        Ok(found?.map(|(_, v)| v))
    }

    /// [`Self::seek_exact`] from a row, never inlined as [`Self::seek_at_row`] is not.
    #[inline(never)]
    fn seek_exact_at_row(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.walked(|w, store, root, roots| w.seek_exact(store, root, roots, &again(store), key))
    }

    /// Move to the first row of the outer key `k` and return it; `None` (the cursor unset) if `k`
    /// has no row. No other key's nested tree is read (a miss reads only the neighbouring outer
    /// key). A `k` of another length than the table's key width is [`Error::KeyTooShort`] or
    /// [`Error::KeyTooLarge`], the cursor unmoved.
    pub fn seek_key(&mut self, k: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.seek_dup(k, &[])
    }

    /// Move to the first row `k ‖ s'` of the outer key `k` with `s' >= s` and return it; `None`
    /// (the cursor unset) if `k` has none, never a row of the next key. [`Self::seek_key`]'s rules;
    /// an empty `s` is [`Self::seek_key`]. From a row of `k` the outer tree is not read again and
    /// the nested seek starts from the nested leaf the cursor is at.
    #[inline]
    pub fn seek_dup(&mut self, k: &[u8], s: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        if self.walk.state() == State::At {
            self.seek_dup_at_row(k, s)
        } else {
            self.walked(|w, store, root, roots| w.seek_dup(store, root, roots, &no_leaf, k, s))
        }
    }

    /// [`Self::seek_dup`] from a row, never inlined as [`Self::seek_at_row`] is not.
    #[inline(never)]
    fn seek_dup_at_row(&mut self, k: &[u8], s: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.walked(|w, store, root, roots| w.seek_dup(store, root, roots, &again(store), k, s))
    }

    /// [`Self::seek_dup`] followed by [`Self::current_with_key_tail`]: move to the first row `k ‖
    /// s'` of `k` with `s' >= s` and return it as its key and its value with the key's last `t`
    /// bytes ([`KeyTail`]); `None` (the cursor unset) if `k` has none.
    #[inline]
    pub fn seek_dup_with_key_tail(
        &mut self,
        k: &[u8],
        s: &[u8],
        t: usize,
    ) -> Result<Option<(&[u8], KeyTail<'_>)>> {
        self.seek_dup(k, s)?;
        self.current_with_key_tail(t)
    }

    /// Move to the next row only if its logical key shares the first `n` bytes of the current
    /// row's, and return it; else return `None` and stay at the current row. The edge rules are
    /// [`Cursor::next_in_prefix`]'s.
    pub fn next_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.walked(|w, store, root, roots| {
            w.step_in_prefix(store, root, roots, &again(store), n, true)
        })
    }

    /// Move to the previous row only if its logical key shares the first `n` bytes of the current
    /// row's, and return it; else return `None` and stay at the current row. The edge rules are
    /// [`Cursor::prev_in_prefix`]'s.
    pub fn prev_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.walked(|w, store, root, roots| {
            w.step_in_prefix(store, root, roots, &again(store), n, false)
        })
    }
}

/// A snapshot's fetch of a leaf a dup cursor is at, for a move from it: the page again, without
/// the per-fetch checks it passed when the cursor entered it ([`Snapshot::page_again`]).
#[inline(always)]
fn again<'s>(snapshot: &'s Snapshot) -> impl Fn(u32) -> Result<Option<&'s [u8]>> + 's {
    move |pgno| Ok(snapshot.page_again(pgno))
}

/// The leaf fetch of a cursor not at a row: no leaf, so a move descends from the roots.
#[inline(always)]
fn no_leaf<'s>(_: u32) -> Result<Option<&'s [u8]>> {
    Ok(None)
}

impl ReadTxn for RoTxn {
    fn open_table(&self, name: &str) -> Result<Option<Table>> {
        catalog::check_name(name)?;
        self.read(|s| {
            Ok(catalog::get(s, &s.meta.catalog, name)?.map(|record| Table::new(record.id, name)))
        })
    }

    fn get<'t>(&'t self, table: &Table, key: &[u8]) -> Result<Option<&'t [u8]>> {
        self.read(|s| match self.snapshot.tables.at_home(table) {
            Some((_, root, None)) => read::get(s, root, key),
            _ => self.get_past_home(table, key),
        })
    }

    fn cursor<'t>(&'t self, table: &Table) -> Result<Cursor<'t>> {
        let root = self.read(|_| self.root(table))?;
        Ok(Cursor::new(&*self.snapshot, root, &self.inner.latch))
    }

    fn txn_id(&self) -> u64 {
        self.snapshot.meta.txn
    }

    fn table_stat(&self, table: &Table) -> Result<TableStat> {
        self.read(|_| self.resolved(table).map(|(root, dup)| TableStat::of(&root, dup.as_ref())))
    }
}

/// The write transaction: the only one at a time (see `Env::begin_rw`). Its changes are seen by
/// its own reads and by no one else until [`Self::commit`]; dropping it (or [`Self::abort`])
/// discards them without I/O.
///
/// Any I/O error, growth failure or damage met by an operation latches the environment: that
/// operation returns the error, and every later write and commit returns [`Error::Failed`].
/// Validation errors ([`Error::KeyTooLarge`], [`Error::KeyTooShort`], [`Error::ValueTooLarge`],
/// [`Error::AppendOutOfOrder`], [`Error::KeyExists`], table errors, [`Error::TableKind`],
/// [`Error::InvalidDupOptions`]) change nothing and do not latch.
pub struct RwTxn {
    inner: Arc<EnvInner>,
    work: Work,
}

impl std::fmt::Debug for RwTxn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwTxn")
            .field("txn", &self.work.txn)
            .field("changed", &self.work.changed)
            .finish_non_exhaustive()
    }
}

/// The write transaction's working state.
struct Work {
    /// The writer's state, held until the transaction drops.
    writer: Option<Box<Writer>>,
    /// The id this transaction commits as.
    txn: u64,
    /// The committed state it began from.
    base: Meta,
    /// The working catalog.
    catalog: TreeRoot,
    /// The working catalog root's provenance ([`write::put_sized`]): true only while it names a
    /// page this transaction's writer wrote for it.
    catalog_trusted: bool,
    /// The id the next table created gets.
    next_table_id: u32,
    /// The tables resolved by a write, with their working records (those whose tree changed
    /// are written into the catalog at commit). A dropped table leaves.
    tables: Vec<Working>,
    /// True once anything changed (a commit of no change writes nothing).
    changed: bool,
    /// This transaction's serial: unique in the process ([`NEXT_SERIAL`]), so a [`TxnCursor`]
    /// tells its own transaction from every other (a txn id is reused after an abort).
    serial: u64,
    /// Moves on every table created or dropped: a [`TxnCursor`] that saw another value checks its
    /// table again, and forgets the index of its working record ([`Self::tables`] loses entries
    /// only on a drop).
    schema: u64,
    /// The last write stamp handed out by [`Self::set_record`] ([`Working::writes`]).
    stamp: u64,
}

/// The source of [`Work::serial`].
static NEXT_SERIAL: AtomicU64 = AtomicU64::new(0);

/// A table's working record in the write transaction.
struct Working {
    /// The table (its handle's id and name).
    table: Table,
    /// Its working record.
    record: TableRecord,
    /// Its root's provenance ([`write::put_sized`]): true only while the record's root names a
    /// page this transaction's writer wrote for this table.
    trusted: bool,
    /// Its write-path cache ([`write::put_cached`]).
    cache: write::PathCache,
    /// True once its tree changed: the catalog gets the record at commit.
    changed: bool,
    /// The write stamp of its last change ([`Work::set_record`]; 0: unchanged in this
    /// transaction). A [`TxnCursor`] that saw another stamp finds its place again.
    writes: u64,
    /// A dup table's working map ([`NestedMap`]; empty for a plain table, and `BTreeMap::new`
    /// allocates nothing).
    nested: NestedMap,
}

impl Working {
    fn new(table: Table, record: TableRecord) -> Self {
        Self {
            table,
            record,
            trusted: false,
            cache: write::PathCache::default(),
            changed: false,
            writes: 0,
            nested: NestedMap::new(),
        }
    }
}

impl Work {
    fn writer(&self) -> &Writer {
        self.writer.as_deref().expect("a live write transaction holds the writer")
    }

    fn writer_mut(&mut self) -> &mut Writer {
        self.writer.as_deref_mut().expect("a live write transaction holds the writer")
    }

    /// The writer and the working catalog with its provenance, borrowed together.
    fn catalog_mut(&mut self) -> (&mut Writer, &mut TreeRoot, &mut bool) {
        let writer = self.writer.as_deref_mut().expect("a live write transaction holds the writer");
        (writer, &mut self.catalog, &mut self.catalog_trusted)
    }

    /// Table `name`'s working record, if it exists.
    fn lookup(&self, name: &str) -> Result<Option<TableRecord>> {
        match self.tables.iter().find(|w| *w.table.name == *name) {
            Some(w) => Ok(Some(w.record)),
            None => catalog::get(self.writer(), &self.catalog, name),
        }
    }

    /// `table`'s working record (see [`resolve`]).
    fn record(&self, table: &Table) -> Result<TableRecord> {
        match self.tables.iter().find(|w| w.table == *table) {
            Some(w) => Ok(w.record),
            None => resolve(self.lookup(table.name())?, table, self.next_table_id),
        }
    }

    /// The nested root of outer key `k` in the dup table `table` whose working outer root is
    /// `outer`, for a read: the working map's entry, else the key's cell ([`load_cell`]); `None`
    /// when the key has no cell. A table with no working record has no map: its cells are
    /// committed.
    fn nested_root(&self, table: &Table, outer: &TreeRoot, k: &[u8]) -> Result<Option<TreeRoot>> {
        let working = self.tables.iter().find(|w| w.table == *table);
        if let Some(n) = working.and_then(|w| w.nested.get(k)) {
            return Ok(Some(n.root));
        }
        let writer = self.writer();
        match outer_cell(writer, table, outer, k)? {
            None => Ok(None),
            Some(b) => load_cell(writer, table, k, b).map(Some),
        }
    }

    /// `table`'s working root for a plain cursor: [`Error::TableKind`] for a dup table.
    fn plain_root(&self, table: &Table) -> Result<TreeRoot> {
        let record = self.record(table)?;
        match record.dup {
            None => Ok(record.root),
            Some(_) => Err(Error::TableKind(table.name().to_string())),
        }
    }

    /// Open the table `name`, creating it (empty, with a new id) if it does not exist: a plain
    /// table, or with `dup` a dup table of that shape. An existing table of the other kind, or a
    /// dup table of other options, is [`Error::TableKind`], changing nothing.
    fn create(&mut self, name: &str, dup: Option<DupOptions>) -> Result<Table> {
        if let Some(record) = self.lookup(name)? {
            return if record.dup.map(|d| d.options()) == dup {
                Ok(Table::new(record.id, name))
            } else {
                Err(Error::TableKind(name.to_string()))
            };
        }
        let id = self.next_table_id;
        self.next_table_id = id
            .checked_add(1)
            .ok_or_else(|| Error::Failed("every table id has been used".to_string()))?;
        let record = match dup {
            None => TableRecord::new(id, self.txn),
            Some(options) => TableRecord::new_dup(id, self.txn, options),
        };
        let (writer, catalog, trusted) = self.catalog_mut();
        catalog::put(writer, catalog, trusted, name, &record)?;
        self.changed = true;
        self.schema += 1;
        let table = Table::new(id, name);
        self.tables.push(Working::new(table.clone(), record));
        Ok(table)
    }

    /// The index in [`Self::tables`] of `table`'s working record (see [`resolve`]), added if
    /// it is not there yet. Every table write starts here, so the scan is inline and the add is
    /// out of line.
    #[inline]
    fn working(&mut self, table: &Table) -> Result<usize> {
        match self.tables.iter().position(|w| w.table.is(table)) {
            Some(i) => Ok(i),
            None => self.add_working(table),
        }
    }

    /// [`Self::working`] for a table without a working record yet.
    #[inline(never)]
    fn add_working(&mut self, table: &Table) -> Result<usize> {
        let record = self.record(table)?;
        self.tables.push(Working::new(table.clone(), record));
        Ok(self.tables.len() - 1)
    }

    /// The writer and the table at `i` in [`Self::tables`], borrowed together.
    fn writer_and_table(&mut self, i: usize) -> (&mut Writer, &mut Working) {
        let writer = self.writer.as_deref_mut().expect("a live write transaction holds the writer");
        (writer, &mut self.tables[i])
    }

    /// Record the changed tree of the table at `i` in [`Self::tables`], its new `root` with the
    /// root's provenance, under a new write stamp. Every change to a table's tree comes here (put,
    /// append, insert, delete, clear), so the stamp tells a [`TxnCursor`] whether its table
    /// changed since its last call. It takes the root alone, not the whole [`TableRecord`]: a put
    /// copies only what it changes.
    fn set_record(&mut self, i: usize, root: TreeRoot, trusted: bool) {
        self.stamp += 1;
        let w = &mut self.tables[i];
        w.record.root = root;
        w.trusted = trusted;
        w.changed = true;
        w.writes = self.stamp;
        self.changed = true;
    }

    /// [`Self::set_record`] after a dup write, which moved the table's totals in place: debug
    /// builds check that the record the commit writes still decodes.
    fn set_dup_record(&mut self, i: usize, root: TreeRoot, trusted: bool) {
        self.set_record(i, root, trusted);
        debug_assert_eq!(
            self.tables[i].record.dup.and_then(|d| d.broken_rule(&root)),
            None,
            "dup table {}: the working record breaks a record rule",
            self.tables[i].table.name()
        );
    }

    /// Write the changed nested roots of the dup table at `i` in [`Self::tables`] into their
    /// outer cells, in key order: commit's first step. A cell replaced with a cell of the same
    /// size changes its leaf in place, so a published outer path is copied once and the keys of
    /// one leaf then hit the outer path cache.
    fn flush_nested(&mut self, i: usize) -> Result<()> {
        let (writer, w) = self.writer_and_table(i);
        let (mut outer, mut trusted) = (w.record.root, w.trusted);
        let mut flushed = false;
        for (k, n) in w.nested.iter_mut().filter(|(_, n)| n.changed) {
            let placed = write::put_outer_cell(
                writer,
                &mut outer,
                &mut trusted,
                &mut w.cache,
                k,
                &n.root,
                PutKind::Upsert,
            )?;
            if !placed.replaced {
                return Err(Error::Failed(format!(
                    "dup table {}: the working root of key {k:02x?} had no outer cell",
                    w.table.name()
                )));
            }
            n.changed = false;
            flushed = true;
        }
        #[cfg(debug_assertions)]
        for (k, n) in &w.nested {
            let cell = outer_cell(&*writer, &w.table, &outer, k).ok().flatten();
            assert_eq!(cell, Some(&n.root.encode()), "the flushed outer cell of key {k:02x?}");
        }
        if flushed {
            self.set_record(i, outer, trusted);
        }
        Ok(())
    }

    /// Commit steps 1–7 (see the env module docs): the changed tables' roots into the catalog,
    /// the free list's save, the new meta, the pages' CRCs, the durable steps (or their deferral),
    /// and publication.
    fn commit(&mut self, inner: &EnvInner) -> Result<u64> {
        for i in 0..self.tables.len() {
            if !self.tables[i].nested.is_empty() {
                self.flush_nested(i)?;
            }
        }
        let mut dirty: Vec<_> = std::mem::take(&mut self.tables)
            .into_iter()
            .filter_map(|w| w.changed.then_some((w.table.name, w.record)))
            .collect();
        dirty.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let Self { writer, txn, base, catalog, catalog_trusted, next_table_id, .. } = self;
        let (txn, base, next_table_id) = (*txn, *base, *next_table_id);
        let writer = writer.as_deref_mut().expect("a live write transaction holds the writer");
        #[cfg(test)]
        writer.crash_point(CrashAt::BeforeSave)?;
        let started = Instant::now();
        for (name, record) in &dirty {
            catalog::put(writer, catalog, catalog_trusted, name, record)?;
        }
        let mut free = base.free;
        let rounds = freelist::save(writer, &mut free)?;
        writer.stats.save_ns += elapsed_ns(started);
        writer.stats.free_list_save_rounds += u64::from(rounds);
        #[cfg(test)]
        {
            writer.max_save_rounds = writer.max_save_rounds.max(rounds);
            writer.crash_point(CrashAt::AfterSave)?;
        }
        let meta = Meta {
            page_size: base.page_size,
            txn,
            page_count: writer.alloc.page_count(),
            next_table_id,
            catalog: *catalog,
            free,
        };
        let pages = writer.stamp_private()?;
        #[cfg(test)]
        writer.crash_point(CrashAt::AfterStamp)?;
        match inner.sync_mode {
            SyncMode::Durable => writer.make_durable(&pages, &meta)?,
            SyncMode::NoSync => writer.defer_sync(&pages),
        }
        writer.publish(inner, meta);
        writer.alloc.begin_txn(txn + 1);
        Ok(txn)
    }
}

impl RwTxn {
    pub(crate) fn new(inner: Arc<EnvInner>, writer: Box<Writer>) -> Self {
        let base = writer.last;
        debug_assert_eq!(writer.alloc.txn(), base.txn + 1, "the writer is set for the next txn");
        Self {
            inner,
            work: Work {
                writer: Some(writer),
                txn: base.txn + 1,
                base,
                catalog: base.catalog,
                catalog_trusted: false,
                next_table_id: base.next_table_id,
                tables: Vec::new(),
                changed: false,
                serial: NEXT_SERIAL.fetch_add(1, Ordering::Relaxed),
                schema: 0,
                stamp: 0,
            },
        }
    }

    /// Test-only: the writer's state.
    #[cfg(test)]
    pub(crate) fn writer_mut(&mut self) -> &mut Writer {
        self.work.writer_mut()
    }

    /// Run a write: [`Error::Failed`] once latched; a failure (or a panic) part way latches.
    fn write<T>(&mut self, op: impl FnOnce(&mut Work) -> Result<T>) -> Result<T> {
        let latch = &self.inner.latch;
        latch.check()?;
        let _guard = UnwindLatch(latch, "a write panicked part way");
        op(&mut self.work).inspect_err(|e| latch.latch_error(e))
    }

    /// Run a read of the working state, latching damage it finds.
    fn read<'t, T>(&'t self, op: impl FnOnce(&'t Work) -> Result<T>) -> Result<T> {
        op(&self.work).inspect_err(|e| self.inner.latch.latch_error(e))
    }

    /// Open the table named `name`, creating it (empty, with a new id) if it does not exist.
    /// [`Error::InvalidTableName`] unless the name is 1..=255 bytes; [`Error::TableKind`] if `name`
    /// is a dup table.
    pub fn create_table(&mut self, name: &str) -> Result<Table> {
        catalog::check_name(name)?;
        self.write(|work| work.create(name, None))
    }

    /// Open the dup table `name`, creating it (empty, new id) if it does not exist: every key of
    /// the table is `k ‖ s`, an outer key `k` of exactly `options.key_len` bytes and a subkey `s`
    /// of at least one byte. [`Error::InvalidTableName`] unless the name is 1..=255 bytes;
    /// [`Error::InvalidDupOptions`] unless `options.packed_max` is 0 and `options.key_len` is at
    /// least 1 and leaves the outer cell (the key and a 32-byte nested root) an inline entry with
    /// room for a 1-byte subkey (975 bytes at 4 KiB pages); [`Error::TableKind`] if `name` is a
    /// plain table or a dup table of other options. Each refusal changes nothing and does not
    /// latch.
    pub fn create_dup_table(&mut self, name: &str, options: DupOptions) -> Result<Table> {
        catalog::check_name(name)?;
        options.check(&self.inner.geometry)?;
        self.write(|work| work.create(name, Some(options)))
    }

    /// Drop `table`: its pages are freed and its name removed from the catalog. Every handle of
    /// it is stale from now on. A dup table's nested trees are freed with it, in one walk that
    /// collects every page first: damage (a page reached twice, an outer cell that is not a
    /// nested tree, totals that do not count the trees) is [`Error::Corrupt`] and frees nothing.
    pub fn drop_table(&mut self, table: Table) -> Result<()> {
        self.write(|work| {
            let record = work.record(&table)?;
            let trusted = work.tables.iter().any(|w| w.table == table && w.trusted);
            match &record.dup {
                None => write::free_subtree(work.writer_mut(), &record.root, trusted)?,
                Some(dup) => {
                    // A table with no working record has no map: its cells are committed.
                    let empty = NestedMap::new();
                    let working = work.tables.iter().find(|w| w.table == table);
                    let map = working.map_or(&empty, |w| &w.nested);
                    let writer = work.writer.as_deref_mut().expect("a live write transaction");
                    free_dup_table(writer, &table, &record.root, trusted, dup, map)?;
                }
            }
            let (writer, catalog, trusted) = work.catalog_mut();
            if !catalog::delete(writer, catalog, trusted, table.name())? {
                return Err(Error::Failed(format!(
                    "table {:?} vanished from the catalog",
                    table.name()
                )));
            }
            work.tables.retain(|w| w.table.id != record.id);
            work.schema += 1;
            work.changed = true;
            Ok(())
        })
    }

    /// Remove every entry of `table` (its pages are freed; the table stays). A dup table's nested
    /// trees are freed as by [`Self::drop_table`], and it stays a dup table of the same options.
    pub fn clear_table(&mut self, table: &Table) -> Result<()> {
        self.write(|work| {
            let i = work.working(table)?;
            let (mut root, trusted) = (work.tables[i].record.root, work.tables[i].trusted);
            if root.is_empty() {
                return Ok(());
            }
            if let Some(dup) = work.tables[i].record.dup {
                let (writer, w) = work.writer_and_table(i);
                free_dup_table(writer, &w.table, &root, trusted, &dup, &w.nested)?;
                w.nested.clear();
                let (key_len, packed_max) = (dup.key_len, dup.packed_max);
                w.record.dup = Some(DupRecord { key_len, packed_max, ..DupRecord::default() });
                work.set_dup_record(i, TreeRoot::EMPTY, false);
                return Ok(());
            }
            write::clear(work.writer_mut(), &mut root, trusted)?;
            work.set_record(i, root, false);
            Ok(())
        })
    }

    /// Store `value` under `key` in `table`, replacing any value there. An entry over the page
    /// size's limits is refused first ([`Error::KeyTooLarge`], [`Error::ValueTooLarge`]; see
    /// `Env::max_key_len` and `Env::max_value_len`), changing nothing.
    ///
    /// In a dup table `key` is the logical key `k ‖ s` (see [`Self::create_dup_table`]): the
    /// limits above apply to the whole key, so a dup table refuses exactly the entries a plain
    /// table keyed `k ‖ s` refuses, and a key with no subkey (at most `key_len` bytes) is then
    /// [`Error::KeyTooShort`]. The row goes into `k`'s nested tree, which a new `k` creates.
    pub fn put(&mut self, table: &Table, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(table, key, value, PutKind::Upsert, None).map(drop)
    }

    /// Store `value` under `key` in `table` as its new last entry: `key` must sort strictly after
    /// the table's last key (an empty table takes any key), else [`Error::AppendOutOfOrder`]. It is
    /// the fast way to load keys in ascending order: an append into the table's cached rightmost
    /// leaf neither descends nor searches. The table's path cache holds the leaf of its last put or
    /// append in this transaction (after an append split, the new right leaf) until a page is
    /// allocated or freed; any other append descends. A refused append
    /// ([`Error::AppendOutOfOrder`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes
    /// nothing and does not fail the environment.
    ///
    /// In a dup table the order is the logical key's, `k ‖ s` (every outer key has the same
    /// width): a `k` before the table's last outer key is refused, the last one appends to its
    /// nested tree (a subkey not past that tree's last is refused), and a later one starts a new
    /// nested tree. A key with no subkey is [`Error::KeyTooShort`].
    pub fn append(&mut self, table: &Table, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(table, key, value, PutKind::Append, None).map(drop)
    }

    /// Store `value` under `key` in `table` only if the table does not hold `key`: a present key is
    /// [`Error::KeyExists`], found by the put's own search (in the table's cached leaf, else after
    /// its descent) and refused before anything changes, with no latch. A new key is stored exactly
    /// as [`Self::put`] stores it. In a dup table `key` is the logical key `k ‖ s`: a present
    /// subkey under `k` is [`Error::KeyExists`], and a key with no subkey [`Error::KeyTooShort`].
    pub fn insert(&mut self, table: &Table, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(table, key, value, PutKind::Insert, None).map(drop)
    }

    /// [`Self::put`], [`Self::append`] or [`Self::insert`] (`kind`). With a `cursor` (an
    /// [`RwCursor`]'s path and root), the root becomes the table's new one, and when the put
    /// leaves the table's path cache naming the entry's leaf ([`write::Placed::slot`]), the
    /// path is placed at the entry. Returns whether it was placed.
    fn put_entry(
        &mut self,
        table: &Table,
        key: &[u8],
        value: &[u8],
        kind: PutKind,
        cursor: Option<(&mut TreeCursor, &mut TreeRoot)>,
    ) -> Result<bool> {
        self.inner.geometry.place(key.len(), value.len())?;
        self.write(|work| {
            let i = work.working(table)?;
            if work.tables[i].record.dup.is_some() {
                // No plain cursor exists on a dup table (its constructors refuse one).
                debug_assert!(cursor.is_none(), "a plain cursor's put into a dup table");
                if cursor.is_some() {
                    return Err(Error::TableKind(table.name().to_string()));
                }
                return dup_put(work, i, key, value, kind).map(|_| false);
            }
            let (writer, w) = work.writer_and_table(i);
            let (mut root, mut trusted) = (w.record.root, w.trusted);
            // Test-only: an emptied cache per put, so every put descends.
            #[cfg(test)]
            if writer.no_path_cache {
                w.cache.forget();
            }
            let cache = &mut w.cache;
            let placed =
                write::put_cached(writer, &mut root, &mut trusted, cache, key, value, kind)?;
            let mut at = false;
            if let Some((tree, cursor_root)) = cursor {
                *cursor_root = root;
                if let Some(slot) = placed.slot {
                    tree.place(cache.branches(), cache.leaf(), slot);
                    at = true;
                }
            }
            work.set_record(i, root, trusted);
            Ok(at)
        })
    }

    /// [`Self::put_entry`] into the dup table `table` for a [`DupTxnCursor`]: where the row went
    /// ([`DupPlaced`]). [`Error::TableKind`] for a plain table (no dup cursor exists on one).
    fn dup_put_entry(
        &mut self,
        table: &Table,
        key: &[u8],
        value: &[u8],
        kind: PutKind,
    ) -> Result<DupPlaced> {
        self.inner.geometry.place(key.len(), value.len())?;
        self.write(|work| {
            let i = work.working(table)?;
            if work.tables[i].record.dup.is_none() {
                return Err(Error::TableKind(table.name().to_string()));
            }
            dup_put(work, i, key, value, kind)
        })
    }

    /// [`Self::delete_entry`] from the dup table `table` for a [`DupTxnCursor`]: what the delete
    /// did ([`DupRemoved`]). [`Error::TableKind`] for a plain table.
    fn dup_delete_entry(&mut self, table: &Table, key: &[u8]) -> Result<DupRemoved> {
        self.write(|work| {
            let i = work.working(table)?;
            if work.tables[i].record.dup.is_none() {
                return Err(Error::TableKind(table.name().to_string()));
            }
            dup_delete(work, i, key)
        })
    }

    /// Remove `key` from `table`, returning whether it was there. A page the delete leaves under
    /// a quarter full absorbs same-parent siblings that fit, and the parents that lose a cell get
    /// the same test, up the tree.
    ///
    /// In a dup table `key` is the logical key `k ‖ s`: a key with no subkey is
    /// [`Error::KeyTooShort`] (use [`Self::delete_prefix`] for every row of `k`), and a delete
    /// that empties `k`'s nested tree removes `k` from the outer tree.
    pub fn delete(&mut self, table: &Table, key: &[u8]) -> Result<bool> {
        Ok(self.delete_entry(table, key)?.found)
    }

    /// [`Self::delete`] through the table's path cache ([`write::delete_cached`]), returning where
    /// the delete left the cache ([`write::Removed::slot`]: the leaf it names holds the deleted
    /// entry's follower at that slot).
    fn delete_entry(&mut self, table: &Table, key: &[u8]) -> Result<write::Removed> {
        self.write(|work| {
            let i = work.working(table)?;
            if work.tables[i].record.dup.is_some() {
                return dup_delete(work, i, key)
                    .map(|r| write::Removed { found: r.found, slot: None });
            }
            let (writer, w) = work.writer_and_table(i);
            let (mut root, mut trusted) = (w.record.root, w.trusted);
            // Test-only: an emptied cache per delete, so every delete descends, and no slot, so a
            // cursor finds its place again by key: the delete as it was before the cache.
            #[cfg(test)]
            let uncached = writer.no_path_cache;
            #[cfg(test)]
            if uncached {
                w.cache.forget();
            }
            let removed = write::delete_cached(writer, &mut root, &mut trusted, &mut w.cache, key)?;
            #[cfg(test)]
            let removed = write::Removed { slot: removed.slot.filter(|_| !uncached), ..removed };
            if removed.found {
                work.set_record(i, root, trusted);
            }
            Ok(removed)
        })
    }

    /// Remove every row of the dup table `table` whose key starts with the outer key `key`
    /// (exactly the table's `key_len` bytes), returning the rows removed (0: no such key, and
    /// nothing changes). The key's nested tree is freed in one walk that collects every page and
    /// run before any is freed, and its outer cell removed. [`Error::TableKind`] for a plain
    /// table; [`Error::KeyTooShort`] for a key shorter than `key_len`, [`Error::KeyTooLarge`] for
    /// a longer one; each refusal changes nothing and does not latch.
    pub fn delete_prefix(&mut self, table: &Table, key: &[u8]) -> Result<u64> {
        self.write(|work| {
            let i = work.working(table)?;
            if work.tables[i].record.dup.is_none() {
                return Err(Error::TableKind(table.name().to_string()));
            }
            dup_delete_prefix(work, i, key)
        })
    }

    /// A new, unset cursor over `table` that can also write: the moves of a [`Cursor`] over this
    /// transaction's working state, plus [`RwCursor::put`], [`RwCursor::append`] and
    /// [`RwCursor::delete_current`]. It holds the transaction until it is dropped.
    /// [`Error::TableKind`] for a dup table.
    pub fn cursor_mut(&mut self, table: &Table) -> Result<RwCursor<'_>> {
        let root = self.read(|work| work.plain_root(table))?;
        Ok(RwCursor {
            txn: self,
            table: table.clone(),
            root,
            tree: TreeCursor::default(),
            deleted: false,
            deleted_key: Vec::new(),
        })
    }

    /// A new, unset cursor over the dup table `table` that holds no borrow of this transaction
    /// (see [`DupTxnCursor`]), as [`Self::cursor_detached`]'s does over a plain table.
    /// [`Error::TableKind`] for a plain table.
    pub fn dup_cursor_detached(&self, table: &Table) -> Result<DupTxnCursor> {
        self.read(|work| {
            let record = work.record(table)?;
            let Some(dup) = record.dup else {
                return Err(Error::TableKind(table.name().to_string()));
            };
            let key_len = usize::from(dup.key_len);
            Ok(DupTxnCursor {
                track: Tracker::new(table.clone(), work),
                outer_root: record.root,
                walk: DupWalk::new(dup.key_len, self.inner.geometry.max_subkey(key_len)),
                deleted: false,
                deleted_key: Vec::new(),
                target: Vec::new(),
                #[cfg(test)]
                reseeks: 0,
                #[cfg(test)]
                placements: 0,
                #[cfg(test)]
                landings: [0; 3],
            })
        })
    }

    /// A new, unset cursor over `table` that holds no borrow of this transaction (see
    /// [`TxnCursor`]): each call takes the transaction, `&self` for a move and `&mut self` for a
    /// write, so several cursors and the transaction's own `put` and `delete` can interleave, each
    /// seeing the others' writes. [`Error::TableKind`] for a dup table.
    pub fn cursor_detached(&self, table: &Table) -> Result<TxnCursor> {
        self.read(|work| {
            let root = work.plain_root(table)?;
            Ok(TxnCursor {
                track: Tracker::new(table.clone(), work),
                root,
                tree: TreeCursor::default(),
                key: Vec::new(),
                deleted: false,
                deleted_key: Vec::new(),
                #[cfg(test)]
                reseeks: 0,
            })
        })
    }

    /// Commit: make this transaction's state the newest (durable when this returns in
    /// `SyncMode::Durable`; at the next sync in `SyncMode::NoSync`) and return its txn id. A
    /// transaction that changed nothing returns the id of the state it began from, with no
    /// I/O. An error latches the environment, and whether the commit survives is unknown until
    /// the file is reopened.
    pub fn commit(mut self) -> Result<u64> {
        let inner = &*self.inner;
        inner.latch.check()?;
        if !self.work.changed {
            return Ok(self.work.base.txn);
        }
        let _guard = UnwindLatch(&inner.latch, "a commit panicked part way");
        let txn = self.work.commit(inner).inspect_err(|e| match e {
            Error::Corrupt(cause) => inner.latch.fail_damaged(format!("commit: {cause}")),
            e => inner.latch.fail(format!("commit: {e}")),
        })?;
        // The allocator has begun the next transaction, which changed nothing: hand the writer
        // back without the abort a drop does.
        if let Some(writer) = self.work.writer.take() {
            inner.return_writer(writer);
        }
        Ok(txn)
    }

    /// Discard every change (as dropping the transaction does): no I/O; the pages it wrote are
    /// unreferenced and reused by the next transaction.
    pub fn abort(self) {
        drop(self);
    }

    /// Test-only: every row of the dup table `table` in the working state, each nested root from
    /// the working map else from its cell, checked ([`dup_rows`]).
    #[cfg(test)]
    pub(crate) fn dup_rows_for_test(&self, table: &Table) -> Vec<(Vec<u8>, Vec<u8>)> {
        let record = self.work.record(table).expect("the table resolves");
        let working = self.work.tables.iter().find(|w| w.table == *table);
        let entry = |k: &[u8]| working.and_then(|w| w.nested.get(k)).map(|n| n.root);
        dup_rows(self.work.writer(), &record.root, &record.dup.expect("a dup table"), entry)
    }

    /// Test-only: the write stamp of `table`'s last change in this transaction
    /// ([`Working::writes`]; 0: none).
    #[cfg(test)]
    pub(crate) fn table_writes(&self, table: &Table) -> u64 {
        self.work.tables.iter().find(|w| w.table == *table).map_or(0, |w| w.writes)
    }

    /// Test-only: drop outer key `k`'s entry from the dup table `table`'s working map, changing
    /// nothing else (so its cell may name a superseded root, or a page private to this
    /// transaction); true if there was one.
    #[cfg(test)]
    pub(crate) fn forget_nested(&mut self, table: &Table, k: &[u8]) -> bool {
        let working = self.work.tables.iter_mut().find(|w| w.table == *table);
        working.is_some_and(|w| w.nested.remove(k).is_some())
    }
}

impl Drop for RwTxn {
    fn drop(&mut self) {
        if let Some(mut writer) = self.work.writer.take() {
            writer.alloc.abort();
            self.inner.return_writer(writer);
        }
    }
}

impl ReadTxn for RwTxn {
    fn open_table(&self, name: &str) -> Result<Option<Table>> {
        catalog::check_name(name)?;
        self.read(|work| Ok(work.lookup(name)?.map(|record| Table::new(record.id, name))))
    }

    fn get<'t>(&'t self, table: &Table, key: &[u8]) -> Result<Option<&'t [u8]>> {
        self.read(|work| {
            let record = work.record(table)?;
            match record.dup {
                None => read::get(work.writer(), &record.root, key),
                Some(dup) => dup_get_in_txn(work, table, &record.root, &dup, key),
            }
        })
    }

    fn cursor<'t>(&'t self, table: &Table) -> Result<Cursor<'t>> {
        let root = self.read(|work| work.plain_root(table))?;
        Ok(Cursor::new(self.work.writer(), root, &self.inner.latch))
    }

    fn txn_id(&self) -> u64 {
        self.work.txn
    }

    fn table_stat(&self, table: &Table) -> Result<TableStat> {
        self.read(|work| work.record(table).map(|r| TableStat::of(&r.root, r.dup.as_ref())))
    }
}

/// A cursor over one table of a write transaction that can also write (created by
/// [`RwTxn::cursor_mut`]). Its moves are a [`Cursor`]'s, over the transaction's working state;
/// the entries it returns are borrowed until its next call.
///
/// [`Self::put`], [`Self::append`] and [`Self::insert`] write through the transaction and leave the
/// cursor at the entry written. [`Self::delete_current`] removes the entry the cursor is at and
/// leaves it on the entry that followed it (the follower) in a deleted state, so a walk that
/// deletes as it goes skips nothing: [`Self::next`] returns the follower without stepping past it,
/// [`Self::current`] returns it too, [`Self::prev`] returns the entry before it, another
/// [`Self::delete_current`] deletes it, and the prefix steps compare with the deleted entry's key
/// ([`Self::next_in_prefix`]). Each of these ends the deleted state, except a prefix step that
/// finds nothing (a delete starts a new one). With no follower (the table's last entry deleted) the
/// cursor is after the end: [`Self::next`], [`Self::current`] and a delete find nothing, and
/// [`Self::prev`] returns the new last entry. A put that did not split its leaf, or did an append
/// split, places the cursor from the path the table's path cache then holds for the entry; any
/// other split, and a delete, find the place again by key, so a delete that merged pages still
/// leaves the cursor on the follower.
/// Errors are the transaction's (a failure latches the environment).
pub struct RwCursor<'t> {
    txn: &'t mut RwTxn,
    table: Table,
    /// The table's working tree as of the cursor's last change.
    root: TreeRoot,
    tree: TreeCursor,
    /// Set by [`Self::delete_current`]: the position is the deleted entry's follower (or after
    /// the end), which the next [`Self::next`] returns without moving (see the type docs).
    deleted: bool,
    /// The deleted entry's key while `deleted` is set (the buffer is kept for the next delete).
    deleted_key: Vec<u8>,
}

impl std::fmt::Debug for RwCursor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwCursor")
            .field("table", &self.table)
            .field("state", &self.tree.state())
            .field("deleted", &self.deleted)
            .finish_non_exhaustive()
    }
}

impl RwCursor<'_> {
    /// Test-only: where the cursor is, and whether it sits after a deleted entry.
    #[cfg(test)]
    pub(crate) fn state(&self) -> (crate::tree::cursor::State, bool) {
        (self.tree.state(), self.deleted)
    }

    /// Run a move over the working tree, latching damage it finds.
    fn moved<'c, T>(
        &'c mut self,
        op: impl FnOnce(&mut TreeCursor, &'c Writer, &TreeRoot) -> Result<T>,
    ) -> Result<T> {
        self.deleted = false;
        let Self { txn, root, tree, .. } = self;
        let txn: &'c RwTxn = txn;
        op(tree, txn.work.writer(), root).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Move to the first entry and return it (`None` for an empty table).
    pub fn first(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.first(store, root))
    }

    /// Move to the last entry and return it (`None` for an empty table).
    pub fn last(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.last(store, root))
    }

    /// Move to the next entry and return it (after [`Self::delete_current`]: the entry that
    /// followed the deleted one); see [`Cursor::next`].
    // A cursor's moves are fallible and go both ways: it is not an `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        if std::mem::take(&mut self.deleted) {
            let txn: &RwTxn = self.txn;
            return self
                .tree
                .current(txn.work.writer())
                .inspect_err(|e| txn.inner.latch.latch_error(e));
        }
        self.moved(|c, store, root| c.next(store, root))
    }

    /// Move to the previous entry and return it (after [`Self::delete_current`]: the entry
    /// before the deleted one); see [`Cursor::prev`].
    pub fn prev(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.prev(store, root))
    }

    /// The entry the cursor is at, if it is at one. Right after [`Self::delete_current`] that is
    /// the deleted entry's follower, and the cursor is then at it as after any move ([`Self::next`]
    /// steps past it). The cursor does not move.
    pub fn current(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.deleted = false;
        let txn: &RwTxn = self.txn;
        self.tree.current(txn.work.writer()).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Move to the first entry whose key is `>= key` and return it; `None` (the cursor after the
    /// end) if there is none.
    pub fn seek(&mut self, key: &[u8]) -> Result<Option<(&[u8], &[u8])>> {
        self.moved(|c, store, root| c.seek(store, root, None, key))
    }

    /// Move to the entry `key` and return its value; `None` (the cursor unset) if there is none.
    pub fn seek_exact(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        self.moved(|c, store, root| c.seek_exact(store, root, None, key))
    }

    /// Move to the next entry only if its key shares the first `n` bytes of the current entry's
    /// key, and return it; else return `None` and stay where the cursor is. The edge rules are
    /// [`Cursor::next_in_prefix`]'s. Right after [`Self::delete_current`] the prefix is the deleted
    /// key's, so a walk of a prefix that deletes as it goes stays in it: the deleted entry's
    /// follower is returned, and the cursor moves to it, if it shares the deleted key's first `n`
    /// bytes; else `None`, and the cursor stays on the follower in the deleted state (`n` longer
    /// than the deleted key included), so [`Self::next`] still returns the follower.
    pub fn next_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.step_in_prefix(n, true)
    }

    /// Move to the previous entry only if its key shares the first `n` bytes of the current
    /// entry's key, and return it; else return `None` and stay where the cursor is. The edge
    /// rules are [`Cursor::prev_in_prefix`]'s. Right after [`Self::delete_current`] the prefix is
    /// the deleted key's: the entry before the deleted one is returned, and the cursor moves to
    /// it, if it shares the deleted key's first `n` bytes; else `None`, and the cursor stays on
    /// the follower in the deleted state.
    pub fn prev_in_prefix(&mut self, n: usize) -> Result<Option<(&[u8], &[u8])>> {
        self.step_in_prefix(n, false)
    }

    /// [`Self::next_in_prefix`] (`forward`) or [`Self::prev_in_prefix`].
    fn step_in_prefix(&mut self, n: usize, forward: bool) -> Result<Option<Entry<'_>>> {
        let Self { txn, root, tree, deleted, deleted_key, .. } = self;
        let txn: &RwTxn = txn;
        let store = txn.work.writer();
        prefix_step(tree, store, root, deleted, deleted_key, n, forward)
            .inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Store `value` under `key` (as [`RwTxn::put`] does) and move to that entry. A refused
    /// entry ([`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes nothing, the cursor
    /// included.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(key, value, PutKind::Upsert)
    }

    /// Store `value` under `key` as the table's new last entry (as [`RwTxn::append`] does) and
    /// move to that entry. A refused append ([`Error::AppendOutOfOrder`], [`Error::KeyTooLarge`],
    /// [`Error::ValueTooLarge`]) changes nothing, the cursor included.
    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(key, value, PutKind::Append)
    }

    /// Store `value` under `key` only if the table does not hold `key` (as [`RwTxn::insert`]
    /// does) and move to that entry. A refused insert ([`Error::KeyExists`],
    /// [`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes nothing, the cursor included.
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(key, value, PutKind::Insert)
    }

    /// [`Self::put`], [`Self::append`] or [`Self::insert`] (`kind`): placed from the path the put
    /// left in the table's path cache, else found again by key.
    fn put_entry(&mut self, key: &[u8], value: &[u8], kind: PutKind) -> Result<()> {
        let Self { txn, table, root, tree, deleted, .. } = self;
        if txn.put_entry(table, key, value, kind, Some((tree, root)))? {
            *deleted = false;
            return Ok(());
        }
        self.reseek(key, true)
    }

    /// Remove the entry the cursor is at and return `true`: right after a delete, the deleted
    /// entry's follower, so repeated deletes remove the entries in order. `false` (nothing changes)
    /// if the cursor is not at an entry: unset, before the start, or after the end (a delete of the
    /// table's last entry leaves it there). The cursor is left on the deleted entry's follower in
    /// the deleted state (see the type docs).
    pub fn delete_current(&mut self) -> Result<bool> {
        // The key goes into the deleted-key buffer, reused from the last delete.
        let Self { txn, tree, deleted_key, .. } = self;
        let txn: &RwTxn = txn;
        let Some((k, _)) =
            tree.current(txn.work.writer()).inspect_err(|e| txn.inner.latch.latch_error(e))?
        else {
            return Ok(false);
        };
        deleted_key.clear();
        deleted_key.extend_from_slice(k);
        let key = std::mem::take(deleted_key);
        if !self.txn.delete(&self.table, &key)? {
            let cause =
                format!("the entry under a cursor of table {:?} is missing", self.table.name());
            self.txn.inner.latch.fail(cause.clone());
            return Err(Error::Failed(cause));
        }
        self.reseek(&key, false)?;
        self.deleted_key = key;
        Ok(true)
    }

    /// After a change to the tree, find the place again by `key` ([`find_again`]).
    fn reseek(&mut self, key: &[u8], put: bool) -> Result<()> {
        let Self { txn, table, root, tree, deleted, .. } = self;
        let txn: &RwTxn = txn;
        let found = find_again(&txn.work, table, root, tree, key, put);
        *deleted = !put && found.is_ok();
        found.inspect_err(|e| txn.inner.latch.latch_error(e))
    }
}

/// A write cursor's prefix step ([`RwCursor::next_in_prefix`], [`TxnCursor::next_in_prefix`]):
/// from the entry it is at, or, while `deleted`, with `deleted_key`'s prefix from the deleted
/// entry's follower (forward: the follower itself; backward: the entry before it). A step that
/// lands, or fails, clears `deleted`.
#[inline]
fn prefix_step<'s>(
    tree: &mut TreeCursor,
    store: &'s Writer,
    root: &TreeRoot,
    deleted: &mut bool,
    deleted_key: &[u8],
    n: usize,
    forward: bool,
) -> Result<Option<Entry<'s>>> {
    if !*deleted {
        return tree.step_in_prefix(store, root, None, n, forward).map(cursor::entry);
    }
    let Some(prefix) = deleted_key.get(..n) else { return Ok(None) };
    // The position is the deleted entry's follower (or after the end).
    let landed = if forward {
        tree.current(store).map(|e| e.filter(|(k, _)| k.starts_with(prefix)))
    } else {
        tree.step_if(store, root, prefix, false).map(cursor::entry)
    };
    if matches!(landed, Ok(Some(_)) | Err(_)) {
        *deleted = false;
    }
    landed
}

/// After a write cursor's own change to `table`, find its place again by `key` in the table's
/// working tree, refreshing `root`: the entry just written after a put (`put`; it must be
/// there), else the deleted entry's follower (or after the end).
fn find_again(
    work: &Work,
    table: &Table,
    root: &mut TreeRoot,
    tree: &mut TreeCursor,
    key: &[u8],
    put: bool,
) -> Result<()> {
    *root = work.record(table)?.root;
    let store = work.writer();
    if put {
        if tree.seek_exact(store, root, None, key)?.is_none() {
            return Err(Error::Failed(format!(
                "the entry just put in table {:?} is missing",
                table.name()
            )));
        }
    } else {
        tree.seek(store, root, None, key)?;
    }
    Ok(())
}

/// Keep a copy of `key` in `saved`, a [`TxnCursor`]'s key buffer. A table's keys often share one
/// length, so a key as long as the one saved overwrites it in place with [`page::copy_short`],
/// which makes no `memcpy` call; any other length goes through `clear` and `extend_from_slice`.
#[inline(always)]
fn save_key(saved: &mut Vec<u8>, key: &[u8]) {
    if saved.len() == key.len() {
        page::copy_short(saved, key);
    } else {
        saved.clear();
        saved.extend_from_slice(key);
    }
}

/// What a detached cursor of a write transaction ([`TxnCursor`], [`DupTxnCursor`]) checks first on
/// every call: its transaction, its table, and the table's write stamp as of its last call, with
/// the remembered index of the table's working record.
struct Tracker {
    table: Table,
    /// The serial of the transaction the cursor belongs to ([`Work::serial`]).
    serial: u64,
    /// The transaction's schema counter as of the cursor's last call ([`Work::schema`]).
    schema: u64,
    /// The index of the table's working record in [`Work::tables`], once it has one.
    slot: Option<usize>,
    /// While `slot` is `None`: the number of working records when the cursor last looked (a new
    /// one is pushed, so an unchanged count means the table still has none).
    scanned: usize,
    /// The table's write stamp as of the cursor's last call ([`Working::writes`]).
    writes: u64,
}

impl Tracker {
    /// The tracker of a new cursor over `table` in `work`, up to date with it.
    fn new(table: Table, work: &Work) -> Self {
        let mut track = Self {
            table,
            serial: work.serial,
            schema: work.schema,
            slot: None,
            scanned: usize::MAX,
            writes: 0,
        };
        track.writes = track.table_writes(work);
        track
    }

    /// The table's write stamp in `work` (0 while it has no working record), from the remembered
    /// index of its working record.
    #[inline]
    fn table_writes(&mut self, work: &Work) -> u64 {
        match self.slot {
            Some(i) => match work.tables.get(i) {
                Some(w) if w.table.id == self.table.id => w.writes,
                _ => self.find_slot(work),
            },
            None if self.scanned == work.tables.len() => 0,
            None => self.find_slot(work),
        }
    }

    /// [`Self::table_writes`] by a search of the working records.
    #[inline(never)]
    fn find_slot(&mut self, work: &Work) -> u64 {
        self.slot = work.tables.iter().position(|w| w.table.id == self.table.id);
        self.scanned = work.tables.len();
        self.slot.map_or(0, |i| work.tables[i].writes)
    }

    /// The checks every call makes first: the transaction ([`Error::WrongTxn`]), then, after a
    /// table was created or dropped, the table ([`Error::StaleTable`]).
    #[inline]
    fn check(&mut self, work: &Work) -> Result<()> {
        if work.serial != self.serial {
            return Err(Error::WrongTxn);
        }
        if work.schema != self.schema {
            self.schema_changed(work)?;
        }
        Ok(())
    }

    /// [`Self::check`] after a table was created or dropped: the table must still exist, and the
    /// index of its working record is looked up again.
    #[inline(never)]
    fn schema_changed(&mut self, work: &Work) -> Result<()> {
        work.record(&self.table)?;
        self.schema = work.schema;
        self.slot = None;
        self.scanned = usize::MAX;
        Ok(())
    }

    /// True if the cursor is up to date with `work`: its transaction, no table created or
    /// dropped, and its table unchanged since its last call. The check of every call, inline.
    #[inline(always)]
    fn up_to_date(&self, work: &Work) -> bool {
        work.serial == self.serial
            && work.schema == self.schema
            && match self.slot {
                Some(i) => work
                    .tables
                    .get(i)
                    .is_some_and(|w| w.table.id == self.table.id && w.writes == self.writes),
                None => self.scanned == work.tables.len() && self.writes == 0,
            }
    }
}

/// A cursor over one table of a write transaction that holds no borrow of it (created by
/// [`RwTxn::cursor_detached`]): each call takes the transaction, `&RwTxn` for a move (the entry
/// returned borrows the transaction) and `&mut RwTxn` for a write. So several cursors, over one
/// table or several, and the transaction's own writes can interleave, each seeing the others'
/// writes: what a caller that shares the transaction behind a lock needs (reth's `DbTxMut`, whose
/// methods all take `&self`). Its moves, states and prefix steps are a [`Cursor`]'s, and its writes
/// and the deleted state an [`RwCursor`]'s.
///
/// **Staleness.** Every call first brings the cursor up to date with the transaction:
///
/// - a transaction other than the one that made the cursor (a later one, even one that reuses an
///   aborted transaction's id) is [`Error::WrongTxn`];
/// - a dropped table (perhaps re-created since) is [`Error::StaleTable`];
/// - if the table changed since the cursor's last call (another cursor's write, or the
///   transaction's `put`, `append`, `insert`, `delete` or `clear_table`), the cursor takes the
///   table's new tree and finds its entry again by key. If the key is still there the cursor stays
///   on it (with its new value, if it changed). If not, the cursor is left as if it had deleted the
///   entry itself ([`Self::delete_current`]), so another writer's delete reads as its own: on the
///   first entry after the key (the follower) in the deleted state, so [`Self::next`] and
///   [`Self::current`] return the follower, [`Self::prev`] the entry before it, a delete deletes
///   it, and the prefix steps compare with the gone key. A cursor already in that state stays in
///   it, on its follower, or on the first entry after the follower if that is gone too. An unset
///   cursor, or one after the end or before the start, keeps its state over the new tree.
///
/// A seek ([`Self::seek`], [`Self::seek_exact`], [`Self::seek_with_key_tail`]) places the cursor
/// anew, so on a changed table it takes the new tree and descends once from its root, without first
/// finding its saved key again. While the table is unchanged (a table created or dropped changes
/// none), it starts from the leaf the cursor is at.
///
/// The reseek is lazy: it sees the table as it is at the cursor's next call. So if, before that
/// call, another writer deleted the cursor's entry and put it back, or put an entry between it
/// and its follower, the cursor is on that entry, not on the follower the delete left: it finds
/// its place at that call, not at the delete.
///
/// A write to another table, and the cursor's own writes, cause no reseek. Each landing on an
/// entry keeps a copy of its key, so the cursor can find it again.
///
/// Errors are the transaction's (a failure latches the environment); [`Error::WrongTxn`] and
/// [`Error::StaleTable`] change nothing and do not latch.
pub struct TxnCursor {
    /// The table, and what tells whether it changed since the cursor's last call.
    track: Tracker,
    /// The table's working tree as of the cursor's last call.
    root: TreeRoot,
    tree: TreeCursor,
    /// The key of the entry the cursor is at (while `deleted`, the deleted entry's follower): the
    /// key a reseek finds it again by.
    key: Vec<u8>,
    /// Set when the cursor's entry was deleted (by it or another writer): the cursor is at the
    /// deleted entry's follower, or after the end, and the next [`Self::next`] does not move (see
    /// the type docs).
    deleted: bool,
    /// The deleted entry's key while `deleted` is set, for the prefix steps (the buffer is
    /// reused).
    deleted_key: Vec<u8>,
    /// Test-only: how many times the cursor found its place again after another writer's change.
    #[cfg(test)]
    pub(crate) reseeks: u32,
}

impl std::fmt::Debug for TxnCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnCursor")
            .field("table", &self.track.table)
            .field("state", &self.tree.state())
            .field("deleted", &self.deleted)
            .finish_non_exhaustive()
    }
}

/// Test-only: what [`TxnCursor::position`] reports.
#[cfg(test)]
pub(crate) type CursorPosition = (State, Vec<(u32, u16)>, TreeRoot, Option<Vec<u8>>, bool);

impl TxnCursor {
    /// Test-only: where the cursor is, and whether it sits after a deleted entry.
    #[cfg(test)]
    pub(crate) fn state(&self) -> (State, bool) {
        (self.tree.state(), self.deleted)
    }

    /// Test-only: what places the cursor, as a seek leaves it: its state and path, the tree it
    /// reads, the key it would find its entry again by (while at one), and whether it is up to
    /// date with `txn` (its next call makes no reseek).
    #[cfg(test)]
    pub(crate) fn position(&self, txn: &RwTxn) -> CursorPosition {
        let at = self.tree.state() == State::At;
        let key = at.then(|| self.key.clone());
        (self.tree.state(), self.tree.path(), self.root, key, self.track.up_to_date(&txn.work))
    }

    /// Bring the cursor up to date with `work` (see the type docs); [`Self::up_to_date`] is its
    /// fast path.
    #[inline(always)]
    fn sync(&mut self, work: &Work) -> Result<()> {
        if self.track.up_to_date(work) {
            return Ok(());
        }
        self.sync_slow(work)
    }

    /// [`Self::sync`] for a cursor that is not up to date.
    #[inline(never)]
    fn sync_slow(&mut self, work: &Work) -> Result<()> {
        self.track.check(work)?;
        let writes = self.track.table_writes(work);
        if writes != self.track.writes {
            self.refresh(work, writes)?;
        }
        Ok(())
    }

    /// After a change to the table by another writer: take its new tree and find the place again
    /// by the saved key (see the type docs). An error leaves the cursor unset.
    #[inline(never)]
    fn refresh(&mut self, work: &Work, writes: u64) -> Result<()> {
        #[cfg(test)]
        {
            self.reseeks += 1;
        }
        self.root = match self.track.slot {
            Some(i) => work.tables[i].record.root,
            None => work.record(&self.track.table)?.root,
        };
        self.track.writes = writes;
        let store = work.writer();
        if self.tree.state() != State::At {
            return Ok(());
        }
        let Self { tree, root, key, deleted, deleted_key, .. } = self;
        // At the entry `key` (while `deleted`, the deleted entry's follower): back on it if it is
        // still there, else on the first entry after it, as if its own delete had moved it there.
        let found = match tree.seek(store, root, None, key) {
            Ok(Some((k, _))) if k == key.as_slice() => Ok(()),
            Ok(landed) => {
                if !*deleted {
                    *deleted = true;
                    std::mem::swap(key, deleted_key);
                }
                if let Some((k, _)) = landed {
                    key.clear();
                    key.extend_from_slice(k);
                }
                Ok(())
            }
            Err(e) => Err(e),
        };
        if found.is_err() {
            *deleted = false;
        }
        found
    }

    /// For a seek, which places the cursor anew: [`Self::sync_slow`]'s checks, then the table's
    /// new tree if it changed, without finding the place again ([`Self::refresh`]). Returns
    /// whether the cursor's path is still a path of its tree (the table unchanged), so the seek
    /// may start from its leaf; else the seek descends from the new root. [`Error::WrongTxn`] and
    /// [`Error::StaleTable`] change nothing.
    #[inline(never)]
    fn retarget(&mut self, work: &Work) -> Result<bool> {
        self.track.check(work)?;
        let writes = self.track.table_writes(work);
        if writes == self.track.writes {
            return Ok(true);
        }
        self.root = match self.track.slot {
            Some(i) => work.tables[i].record.root,
            None => work.record(&self.track.table)?.root,
        };
        self.track.writes = writes;
        Ok(false)
    }

    /// Run a seek: bring the cursor up to date with `txn` as [`Self::retarget`] does, leave the
    /// deleted state, then `op` with the page of the leaf the cursor is at, fetched with the
    /// checks ([`TreeCursor::leaf_page`]) while its path is still its tree's, else `None` (see
    /// [`TreeCursor::seek`]). A failure latches the environment if it is one that latches.
    #[inline(always)]
    fn seek_call<'t, T>(
        &mut self,
        txn: &'t RwTxn,
        op: impl FnOnce(&mut Self, &'t Writer, Option<&'t [u8]>) -> Result<T>,
    ) -> Result<T> {
        let work = &txn.work;
        let store = work.writer();
        let holds = if self.track.up_to_date(work) { Ok(true) } else { self.retarget(work) };
        holds
            .and_then(|holds| {
                self.deleted = false;
                let leaf = if holds { self.tree.leaf_page(store)? } else { None };
                op(self, store, leaf)
            })
            .inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Run a call: bring the cursor up to date with `txn`, then `op`. A failure latches the
    /// environment if it is one that latches.
    #[inline(always)]
    fn call<'t, T>(
        &mut self,
        txn: &'t RwTxn,
        op: impl FnOnce(&mut Self, &'t Writer) -> Result<T>,
    ) -> Result<T> {
        if !self.track.up_to_date(&txn.work) {
            if let Err(e) = self.sync_slow(&txn.work) {
                txn.inner.latch.latch_error(&e);
                return Err(e);
            }
        }
        op(self, txn.work.writer()).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Finish a move: keep the key of the entry it landed on, which the next call may have to
    /// find again.
    #[inline]
    fn landed<'s>(&mut self, moved: Result<Option<Entry<'s>>>) -> Result<Option<Entry<'s>>> {
        if let Ok(Some((k, _))) = moved {
            save_key(&mut self.key, k);
        }
        moved
    }

    /// Run a plain move (it leaves the deleted state) and keep the key it lands on.
    fn moved<'t>(
        &mut self,
        txn: &'t RwTxn,
        op: impl FnOnce(&mut TreeCursor, &'t Writer, &TreeRoot) -> Result<Option<Entry<'t>>>,
    ) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.call(txn, |c, store| {
            c.deleted = false;
            let moved = op(&mut c.tree, store, &c.root);
            c.landed(moved)
        })
    }

    /// Move to the first entry and return it (`None` for an empty table).
    pub fn first<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.moved(txn, |c, store, root| c.first(store, root))
    }

    /// Move to the last entry and return it (`None` for an empty table). The cursor is then on
    /// that entry as after any move: [`Self::next`] returns an entry another writer has put past
    /// it since, else nothing (the cursor after the end).
    pub fn last<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.moved(txn, |c, store, root| c.last(store, root))
    }

    /// Move to the next entry and return it (after the cursor's entry was deleted, by this cursor
    /// or another writer: the first entry after the deleted key); see [`Cursor::next`].
    // A cursor's moves are fallible and go both ways: it is not an `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.call(txn, |c, store| {
            let moved = if std::mem::take(&mut c.deleted) {
                c.tree.current(store)
            } else {
                c.tree.next(store, &c.root)
            };
            c.landed(moved)
        })
    }

    /// Move to the previous entry and return it (after the cursor's entry was deleted: the entry
    /// before the deleted entry's follower; from an unset cursor: the last entry); see
    /// [`Cursor::prev`].
    pub fn prev<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.moved(txn, |c, store, root| c.prev(store, root))
    }

    /// The entry the cursor is at, if it is at one: after its entry was deleted, the deleted
    /// entry's follower, which the cursor is then at as after any move ([`Self::next`] steps past
    /// it). The cursor does not move, unless the table changed and it finds its place again.
    pub fn current<'t>(&mut self, txn: &'t RwTxn) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.call(txn, |c, store| {
            c.deleted = false;
            c.tree.current(store)
        })
    }

    /// [`Self::current`] with the entry's value joined to the key's last `t` bytes; see
    /// [`Cursor::current_with_key_tail`].
    pub fn current_with_key_tail<'t>(
        &mut self,
        txn: &'t RwTxn,
        t: usize,
    ) -> Result<Option<(&'t [u8], KeyTail<'t>)>> {
        self.call(txn, |c, store| {
            c.deleted = false;
            c.tree.current_with_key_tail(store, None, t)
        })
    }

    /// Move to the first entry whose key is `>= key` and return it; `None` (the cursor after the
    /// end) if there is none. While the table is unchanged since the cursor's last call, the seek
    /// starts from the leaf the cursor is at (see [`Cursor::seek`]); after a change it descends
    /// once from the table's new tree, without first finding its old place again.
    pub fn seek<'t>(&mut self, txn: &'t RwTxn, key: &[u8]) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.seek_call(txn, |c, store, leaf| {
            let moved = c.tree.seek(store, &c.root, leaf, key);
            c.landed(moved)
        })
    }

    /// [`Self::seek`] followed by [`Self::current_with_key_tail`], from one leaf access: move to
    /// the first entry whose key is `>= key` and return it as its key and its value with the
    /// key's last `t` bytes before it ([`KeyTail`]); `None` (the cursor after the end) if there
    /// is none. The cursor is left as [`Self::seek`] leaves it, and the seek starts as its does.
    pub fn seek_with_key_tail<'t>(
        &mut self,
        txn: &'t RwTxn,
        key: &[u8],
        t: usize,
    ) -> Result<Option<(&'t [u8], KeyTail<'t>)>> {
        self.seek_call(txn, |c, store, leaf| {
            let found = c.tree.seek_with_key_tail(store, &c.root, leaf, key, t);
            if let Ok(Some((k, _))) = found {
                save_key(&mut c.key, k);
            }
            found
        })
    }

    /// Move to the entry `key` and return its value; `None` (the cursor unset) if there is none.
    /// The seek starts as [`Self::seek`]'s does.
    pub fn seek_exact<'t>(&mut self, txn: &'t RwTxn, key: &[u8]) -> Result<Option<&'t [u8]>> {
        self.seek_call(txn, |c, store, leaf| {
            let found = c.tree.seek_exact(store, &c.root, leaf, key);
            if let Ok(Some(_)) = found {
                save_key(&mut c.key, key);
            }
            found
        })
    }

    /// Move to the next entry only if its key shares the first `n` bytes of the current entry's
    /// key, and return it; else return `None` and stay where the cursor is. The edge rules are
    /// [`Cursor::next_in_prefix`]'s, and after the cursor's entry was deleted (by it or another
    /// writer) [`RwCursor::next_in_prefix`]'s: the prefix is the deleted key's.
    pub fn next_in_prefix<'t>(
        &mut self,
        txn: &'t RwTxn,
        n: usize,
    ) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.step_in_prefix(txn, n, true)
    }

    /// Move to the previous entry only if its key shares the first `n` bytes of the current
    /// entry's key, and return it; else return `None` and stay where the cursor is. The edge
    /// rules are [`Cursor::prev_in_prefix`]'s, and after the cursor's entry was deleted
    /// [`RwCursor::prev_in_prefix`]'s.
    pub fn prev_in_prefix<'t>(
        &mut self,
        txn: &'t RwTxn,
        n: usize,
    ) -> Result<Option<(&'t [u8], &'t [u8])>> {
        self.step_in_prefix(txn, n, false)
    }

    /// [`Self::next_in_prefix`] (`forward`) or [`Self::prev_in_prefix`].
    fn step_in_prefix<'t>(
        &mut self,
        txn: &'t RwTxn,
        n: usize,
        forward: bool,
    ) -> Result<Option<Entry<'t>>> {
        self.call(txn, |c, store| {
            let moved = prefix_step(
                &mut c.tree,
                store,
                &c.root,
                &mut c.deleted,
                &c.deleted_key,
                n,
                forward,
            );
            c.landed(moved)
        })
    }

    /// Store `value` under `key` (as [`RwTxn::put`] does) and move to that entry. A refused
    /// entry ([`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes nothing, the cursor
    /// included.
    pub fn put(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(txn, key, value, PutKind::Upsert)
    }

    /// Store `value` under `key` as the table's new last entry (as [`RwTxn::append`] does) and
    /// move to that entry. A refused append ([`Error::AppendOutOfOrder`], [`Error::KeyTooLarge`],
    /// [`Error::ValueTooLarge`]) changes nothing, the cursor included.
    pub fn append(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(txn, key, value, PutKind::Append)
    }

    /// Store `value` under `key` only if the table does not hold `key` (as [`RwTxn::insert`]
    /// does) and move to that entry. A refused insert ([`Error::KeyExists`],
    /// [`Error::KeyTooLarge`], [`Error::ValueTooLarge`]) changes nothing, the cursor included.
    pub fn insert(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(txn, key, value, PutKind::Insert)
    }

    /// [`Self::put`], [`Self::append`] or [`Self::insert`] (`kind`): the transaction and table
    /// checked, but no reseek, since the write places the cursor anew; placed from the path the
    /// put left in the table's path cache, else found again by key. The table's new write stamp
    /// is the cursor's own, so its write causes it no reseek.
    fn put_entry(
        &mut self,
        txn: &mut RwTxn,
        key: &[u8],
        value: &[u8],
        kind: PutKind,
    ) -> Result<()> {
        self.track.check(&txn.work).inspect_err(|e| txn.inner.latch.latch_error(e))?;
        let Self { track, root, tree, .. } = self;
        let placed = txn.put_entry(&track.table, key, value, kind, Some((tree, root)))?;
        self.deleted = false;
        save_key(&mut self.key, key);
        self.track.writes = self.track.table_writes(&txn.work);
        if placed {
            return Ok(());
        }
        let Self { track, root, tree, .. } = self;
        find_again(&txn.work, &track.table, root, tree, key, true)
            .inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Remove the entry the cursor is at and return `true`: after its entry was deleted (by it or
    /// another writer), the follower, as [`RwCursor::delete_current`] does. `false` (nothing
    /// changes) if the cursor is not at an entry: unset, before the start, or after the end. The
    /// cursor is left on the deleted entry's follower in the deleted state, as an [`RwCursor`] is.
    pub fn delete_current(&mut self, txn: &mut RwTxn) -> Result<bool> {
        self.sync(&txn.work).inspect_err(|e| txn.inner.latch.latch_error(e))?;
        if self.tree.state() != State::At {
            return Ok(false);
        }
        // The saved key is the current entry's (while `deleted`, the follower's); it becomes the
        // deleted key.
        std::mem::swap(&mut self.key, &mut self.deleted_key);
        let key = std::mem::take(&mut self.deleted_key);
        let deleted = self.delete_key(txn, &key);
        self.deleted_key = key;
        deleted.map(|()| true)
    }

    /// Whether the deleted entry's follower shares the deleted key's first `n` bytes: after the
    /// cursor's entry was deleted (by it or another writer), `true` if the cursor is on a follower
    /// whose key starts with them, the entry [`Self::next_in_prefix`] would return; `false` if it
    /// is after the end or `n` is longer than the deleted key. `false` too if the cursor is not in
    /// the deleted state (no delete since its last move). The cursor does not move and stays in
    /// the deleted state, unless the table changed and it finds its place again, as every call
    /// does (a follower another writer deleted is then the first entry after it). Up to date, it
    /// reads no page: the answer comes from the saved keys.
    #[inline]
    pub fn follower_shares_prefix(&mut self, txn: &RwTxn, n: usize) -> Result<bool> {
        self.call(txn, |c, _| {
            let on_follower = c.deleted && c.tree.state() == State::At;
            Ok(on_follower && c.deleted_key.get(..n).is_some_and(|p| c.key.starts_with(p)))
        })
    }

    /// [`Self::delete_current`] of the entry `key`, which must be there. The cursor lands on the
    /// deleted entry's follower (or after the end): placed from the table's path cache when the
    /// delete left it naming the entry's leaf ([`write::Removed::slot`]), else found again by key.
    fn delete_key(&mut self, txn: &mut RwTxn, key: &[u8]) -> Result<()> {
        let removed = txn.delete_entry(&self.track.table, key)?;
        if !removed.found {
            let cause = format!(
                "the entry under a cursor of table {:?} is missing",
                self.track.table.name()
            );
            txn.inner.latch.fail(cause.clone());
            return Err(Error::Failed(cause));
        }
        // Also leaves `slot` on the table's working record, which the delete created if needed.
        self.track.writes = self.track.table_writes(&txn.work);
        let Self { track, root, tree, .. } = self;
        let found = match (removed.slot, track.slot) {
            (Some(at), Some(i)) => {
                let w = &txn.work.tables[i];
                *root = w.record.root;
                let store = txn.work.writer();
                tree.place_after_delete(store, root, w.cache.branches(), w.cache.leaf(), at)
            }
            _ => find_again(&txn.work, &track.table, root, tree, key, false)
                .and_then(|()| tree.current(txn.work.writer())),
        }
        .map(|follower| {
            // The follower's key, which a reseek finds the cursor again by.
            if let Some((k, _)) = follower {
                save_key(&mut self.key, k);
            }
        });
        self.deleted = found.is_ok();
        found.inspect_err(|e| txn.inner.latch.latch_error(e))
    }
}

/// The root source of a [`DupTxnCursor`] ([`NestedRoots`]): the table's working map, whose root
/// of a key is the truth while it holds the key, else the key's cell, which then holds the
/// committed root ([`load_cell`]); never a cell the map supersedes.
struct WorkRoots<'a> {
    writer: &'a Writer,
    table: &'a Table,
    /// The table's working map; `None` while the table has none or it is empty (no lookup).
    map: Option<&'a NestedMap>,
}

impl<'a> WorkRoots<'a> {
    /// The root source of a cursor whose tracker `track` is up to date with `work`, so its slot
    /// names the table's working record whenever the table has one.
    #[inline]
    fn new(work: &'a Work, track: &'a Tracker) -> Self {
        let map = track
            .slot
            .and_then(|i| work.tables.get(i))
            .filter(|w| w.table.id == track.table.id)
            .map(|w| &w.nested)
            .filter(|m| !m.is_empty());
        // A key with an entry has no other truth: the map is never skipped while it has one.
        debug_assert!(
            map.is_some()
                || work.tables.iter().all(|w| w.table.id != track.table.id || w.nested.is_empty()),
            "dup table {}: a cursor's root source skipped the working map",
            track.table.name()
        );
        Self { writer: work.writer(), table: &track.table, map }
    }
}

impl NestedRoots for WorkRoots<'_> {
    fn name(&self) -> &str {
        self.table.name()
    }

    #[inline]
    fn root(&mut self, k: &[u8], cell: &[u8; page::NESTED_CELL]) -> Result<TreeRoot> {
        if let Some(n) = self.map.and_then(|m| m.get(k)) {
            return Ok(n.root);
        }
        load_cell(self.writer, self.table, k, cell)
    }
}

/// A write transaction's fetch of a leaf a dup cursor is at, for a move from it: the page with
/// the per-fetch checks ([`PageRead::page`]), as a [`TxnCursor`]'s moves fetch their leaf.
#[inline(always)]
fn checked<'s>(writer: &'s Writer) -> impl Fn(u32) -> Result<Option<&'s [u8]>> + 's {
    move |pgno| PageRead::page(writer, pgno).map(Some)
}

/// A cursor over one dup table of a write transaction that holds no borrow of it (created by
/// [`RwTxn::dup_cursor_detached`]), as a [`TxnCursor`] is over a plain table: each call takes the
/// transaction, `&RwTxn` for a move and `&mut RwTxn` for a write, so several cursors and the
/// transaction's own writes can interleave, each seeing the others' writes.
///
/// It walks the table's rows as the logical keys `k ‖ s` in order, the rows of the composite twin
/// (a plain table keyed `k ‖ s` holding the same rows), and every call gives what the same call
/// of a [`TxnCursor`] gives on the twin, as a [`DupRoCursor`] does for a [`RoCursor`]: the same
/// moves, states, prefix steps, writes, deleted state and staleness, with "entry" read as "row".
/// The bounded seeks ([`Self::seek_key`], [`Self::seek_dup`], [`Self::seek_dup_with_key_tail`])
/// and [`Self::current_with_key_tail`] are [`DupRoCursor`]'s. The key and value a call returns
/// borrow the cursor and the transaction until the cursor's next call.
///
/// **Staleness.** A table change since the cursor's last call (another cursor's write, or the
/// transaction's `put`, `append`, `insert`, `delete`, `delete_prefix` or `clear_table`, a write
/// to one key's nested tree included) makes the cursor find its row again by its logical key, as
/// [`TxnCursor`]'s type docs describe: back on it if it is still there, else on the first row
/// after it in the deleted state. So a cursor whose key another writer emptied (its last row
/// deleted, `delete_prefix` of the key, or `clear_table`) is left on the next key's first row
/// (or after the end) in the deleted state, as the twin's cursor is after the same rows went one
/// by one. A seek on a changed table descends from both roots without first finding its row
/// again. A key's nested root comes from the transaction's working map while it holds the key,
/// never from a cell the map supersedes.
///
/// Errors are the transaction's (a failure latches the environment; damage is
/// [`Error::Corrupt`] and leaves the cursor unset); [`Error::WrongTxn`] and
/// [`Error::StaleTable`] change nothing and do not latch.
pub struct DupTxnCursor {
    /// The table, and what tells whether it changed since the cursor's last call.
    track: Tracker,
    /// The table's working outer tree as of the cursor's last call.
    outer_root: TreeRoot,
    /// The position, with the logical key of the row it is at (while `deleted`, the deleted
    /// row's follower): the key a reseek finds it again by.
    walk: DupWalk,
    /// Set when the cursor's row was deleted (by it or another writer): the cursor is at the
    /// deleted row's follower, or after the end (see [`TxnCursor`]'s field of the same name).
    deleted: bool,
    /// The deleted row's logical key while `deleted` is set, for the prefix steps.
    deleted_key: Vec<u8>,
    /// Scratch: a reseek's target, a guarded step's saved key (the buffer is reused).
    target: Vec<u8>,
    /// Test-only: how many times the cursor found its place again after another writer's change.
    #[cfg(test)]
    pub(crate) reseeks: u32,
    /// Test-only: how many of the cursor's own writes placed it from a nested tree's path cache.
    #[cfg(test)]
    pub(crate) placements: u32,
    /// Test-only: how the cursor's own deletes landed on the follower: from the nested tree's path
    /// cache, from the outer path cache (the delete emptied the key), and found again by key.
    #[cfg(test)]
    pub(crate) landings: [u32; 3],
}

impl std::fmt::Debug for DupTxnCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DupTxnCursor")
            .field("table", &self.track.table)
            .field("state", &self.walk.state())
            .field("deleted", &self.deleted)
            .finish_non_exhaustive()
    }
}

impl DupTxnCursor {
    /// Test-only: where the cursor is, and whether it sits after a deleted row.
    #[cfg(test)]
    pub(crate) fn state(&self) -> (State, bool) {
        (self.walk.state(), self.deleted)
    }

    /// Test-only: the deleted row's logical key while the cursor is in the deleted state.
    #[cfg(test)]
    pub(crate) fn deleted_key_for_test(&self) -> Option<&[u8]> {
        self.deleted.then_some(self.deleted_key.as_slice())
    }

    /// Test-only: the logical key of the row the cursor is at (while deleted, the follower's), as
    /// of its last call: the key a reseek finds it again by.
    #[cfg(test)]
    pub(crate) fn key_for_test(&self) -> Option<&[u8]> {
        (self.walk.state() == State::At).then(|| self.walk.key())
    }

    /// Test-only: the first and last keys of the outer and the nested leaf the cursor is at, read
    /// from `txn`'s pages (meaningful only while the cursor is up to date with `txn`).
    #[cfg(test)]
    pub(crate) fn leaf_bounds_for_test(
        &self,
        txn: &RwTxn,
    ) -> [crate::tree::dup_cursor::LeafBounds; 2] {
        self.walk.leaf_bounds_for_test(txn.work.writer())
    }

    /// Bring the cursor up to date with `work` (see the type docs).
    #[inline(always)]
    fn sync(&mut self, work: &Work) -> Result<()> {
        if self.track.up_to_date(work) {
            return Ok(());
        }
        self.sync_slow(work)
    }

    /// [`Self::sync`] for a cursor that is not up to date.
    #[inline(never)]
    fn sync_slow(&mut self, work: &Work) -> Result<()> {
        self.track.check(work)?;
        let writes = self.track.table_writes(work);
        if writes != self.track.writes {
            self.refresh(work, writes)?;
        }
        Ok(())
    }

    /// The table's working outer root in `work`, by the tracker's slot.
    fn working_root(&self, work: &Work) -> Result<TreeRoot> {
        match self.track.slot {
            Some(i) => Ok(work.tables[i].record.root),
            None => Ok(work.record(&self.track.table)?.root),
        }
    }

    /// After a change to the table by another writer: take its new outer tree and find the row
    /// again by its logical key from both roots (the paths may be stale at both levels), as
    /// [`TxnCursor`]'s refresh does. An error leaves the cursor unset.
    #[inline(never)]
    fn refresh(&mut self, work: &Work, writes: u64) -> Result<()> {
        #[cfg(test)]
        {
            self.reseeks += 1;
        }
        self.outer_root = self.working_root(work)?;
        self.track.writes = writes;
        if self.walk.state() != State::At {
            return Ok(());
        }
        let Self { track, outer_root, walk, deleted, deleted_key, target, .. } = self;
        target.clear();
        target.extend_from_slice(walk.key());
        walk.unset();
        let mut roots = WorkRoots::new(work, track);
        let found = match walk.seek(work.writer(), outer_root, &mut roots, &no_leaf, target) {
            Ok(Some(_)) if walk.key() == target.as_slice() => Ok(()),
            Ok(_) => {
                // Gone: on the first row after it, as if its own delete had moved it there.
                if !*deleted {
                    *deleted = true;
                    std::mem::swap(target, deleted_key);
                }
                Ok(())
            }
            Err(e) => Err(e),
        };
        if found.is_err() {
            *deleted = false;
        }
        found
    }

    /// For a seek: [`Self::sync_slow`]'s checks, then the table's new outer tree if it changed,
    /// without finding the row again. Returns whether the cursor's paths are still its trees'.
    #[inline(never)]
    fn retarget(&mut self, work: &Work) -> Result<bool> {
        self.track.check(work)?;
        let writes = self.track.table_writes(work);
        if writes == self.track.writes {
            return Ok(true);
        }
        self.outer_root = self.working_root(work)?;
        self.track.writes = writes;
        Ok(false)
    }

    /// Run a seek: bring the cursor up to date as [`Self::retarget`] does, leave the deleted
    /// state, forget the paths if they are no longer the trees' (the seek then descends from both
    /// roots), then `op`. A failure latches the environment if it is one that latches.
    #[inline(always)]
    fn seek_call<'t, T>(
        &mut self,
        txn: &'t RwTxn,
        op: impl FnOnce(&mut Self, &'t Work) -> Result<T>,
    ) -> Result<T> {
        let work = &txn.work;
        let holds = if self.track.up_to_date(work) { Ok(true) } else { self.retarget(work) };
        holds
            .and_then(|holds| {
                self.deleted = false;
                if !holds {
                    self.walk.unset();
                }
                op(self, work)
            })
            .inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Run a call: bring the cursor up to date with `txn`, then `op`. A failure latches the
    /// environment if it is one that latches.
    #[inline(always)]
    fn call<'t, T>(
        &mut self,
        txn: &'t RwTxn,
        op: impl FnOnce(&mut Self, &'t Work) -> Result<T>,
    ) -> Result<T> {
        let work = &txn.work;
        if !self.track.up_to_date(work) {
            if let Err(e) = self.sync_slow(work) {
                txn.inner.latch.latch_error(&e);
                return Err(e);
            }
        }
        op(self, work).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Run walker move `op` over `work` with the cursor's outer root and root source.
    #[inline(always)]
    fn walk_op<'w>(
        &mut self,
        work: &'w Work,
        op: impl FnOnce(&mut DupWalk, &'w Writer, &TreeRoot, &mut WorkRoots<'_>) -> Walked<'w>,
    ) -> Walked<'w> {
        let Self { track, outer_root, walk, .. } = self;
        let mut roots = WorkRoots::new(work, track);
        op(walk, work.writer(), outer_root, &mut roots)
    }

    /// Pair the value a move returned with the logical key of the row it landed on.
    #[inline(always)]
    fn row<'c>(&'c self, value: Option<&'c [u8]>) -> Option<(&'c [u8], &'c [u8])> {
        value.map(|v| (self.walk.key(), v))
    }

    /// Run a plain move (it leaves the deleted state).
    #[inline]
    fn moved<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        op: impl FnOnce(&mut DupWalk, &'c Writer, &TreeRoot, &mut WorkRoots<'_>) -> Walked<'c>,
    ) -> Result<Option<(&'c [u8], &'c [u8])>> {
        let value = self.call(txn, |c, work| {
            c.deleted = false;
            c.walk_op(work, op)
        })?;
        Ok(self.row(value))
    }

    /// Move to the first row and return it (`None` for an empty table).
    pub fn first<'c>(&'c mut self, txn: &'c RwTxn) -> Result<Option<(&'c [u8], &'c [u8])>> {
        self.moved(txn, |w, store, root, roots| w.start(store, root, roots, true))
    }

    /// Move to the last row and return it (`None` for an empty table); see [`TxnCursor::last`].
    pub fn last<'c>(&'c mut self, txn: &'c RwTxn) -> Result<Option<(&'c [u8], &'c [u8])>> {
        self.moved(txn, |w, store, root, roots| w.start(store, root, roots, false))
    }

    /// Move to the next row and return it (after the cursor's row was deleted, by this cursor or
    /// another writer: the first row after the deleted key); see [`TxnCursor::next`].
    // A cursor's moves are fallible and go both ways: it is not an `Iterator`.
    #[allow(clippy::should_implement_trait)]
    pub fn next<'c>(&'c mut self, txn: &'c RwTxn) -> Result<Option<(&'c [u8], &'c [u8])>> {
        let value = self.call(txn, |c, work| {
            if std::mem::take(&mut c.deleted) {
                let store = work.writer();
                return c.walk.current(store, &checked(store));
            }
            c.walk_op(work, |w, store, root, roots| {
                w.step(store, root, roots, &checked(store), true)
            })
        })?;
        Ok(self.row(value))
    }

    /// Move to the previous row and return it (after the cursor's row was deleted: the row before
    /// the deleted row's follower); see [`TxnCursor::prev`].
    pub fn prev<'c>(&'c mut self, txn: &'c RwTxn) -> Result<Option<(&'c [u8], &'c [u8])>> {
        self.moved(txn, |w, store, root, roots| w.step(store, root, roots, &checked(store), false))
    }

    /// The row the cursor is at, if it is at one (after its row was deleted: the follower); see
    /// [`TxnCursor::current`].
    pub fn current<'c>(&'c mut self, txn: &'c RwTxn) -> Result<Option<(&'c [u8], &'c [u8])>> {
        let value = self.call(txn, |c, work| {
            c.deleted = false;
            let store = work.writer();
            c.walk.current(store, &checked(store))
        })?;
        Ok(self.row(value))
    }

    /// [`Self::current`] with the value and the logical key's last `t` bytes ([`KeyTail`]), as
    /// [`DupRoCursor::current_with_key_tail`] gives them.
    pub fn current_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        t: usize,
    ) -> Result<Option<(&'c [u8], KeyTail<'c>)>> {
        let tail = self.call(txn, |c, work| {
            c.deleted = false;
            let store = work.writer();
            c.walk.current_with_key_tail(store, &checked(store), t)
        })?;
        Ok(tail.map(|tail| (self.walk.key(), tail)))
    }

    /// Move to the first row whose logical key is `>= key` (of any length) and return it; `None`
    /// (the cursor after the end) if there is none. While the table is unchanged since the
    /// cursor's last call, the seek starts from the leaves the cursor is at, as
    /// [`DupRoCursor::seek`]'s does; after a change it descends from both roots.
    pub fn seek<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        key: &[u8],
    ) -> Result<Option<(&'c [u8], &'c [u8])>> {
        let value = self.seek_call(txn, |c, work| {
            c.walk_op(work, |w, store, root, roots| {
                w.seek(store, root, roots, &checked(store), key)
            })
        })?;
        Ok(self.row(value))
    }

    /// [`Self::seek`] followed by [`Self::current_with_key_tail`]; `None` (the cursor after the
    /// end) if there is no row `>= key`.
    pub fn seek_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        key: &[u8],
        t: usize,
    ) -> Result<Option<(&'c [u8], KeyTail<'c>)>> {
        let tail = self.seek_call(txn, |c, work| {
            let store = work.writer();
            let found = c.walk_op(work, |w, store, root, roots| {
                w.seek(store, root, roots, &checked(store), key)
            })?;
            if found.is_none() {
                return Ok(None);
            }
            c.walk.current_with_key_tail(store, &checked(store), t)
        })?;
        Ok(tail.map(|tail| (self.walk.key(), tail)))
    }

    /// Move to the row `key` and return its value; `None` (the cursor unset) if there is none. A
    /// key of at most the key width is no row's. The seek starts as [`Self::seek`]'s does.
    pub fn seek_exact<'c>(&'c mut self, txn: &'c RwTxn, key: &[u8]) -> Result<Option<&'c [u8]>> {
        self.seek_call(txn, |c, work| {
            c.walk_op(work, |w, store, root, roots| {
                w.seek_exact(store, root, roots, &checked(store), key)
            })
        })
    }

    /// Move to the first row of the outer key `k` and return it; `None` (the cursor unset) if `k`
    /// has none; see [`DupRoCursor::seek_key`]. A `k` of another length than the key width is
    /// [`Error::KeyTooShort`] or [`Error::KeyTooLarge`], changing nothing.
    pub fn seek_key<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        k: &[u8],
    ) -> Result<Option<(&'c [u8], &'c [u8])>> {
        self.seek_dup(txn, k, &[])
    }

    /// Move to the first row `k ‖ s'` of the outer key `k` with `s' >= s` and return it; `None`
    /// (the cursor unset) if `k` has none, never a row of the next key; see
    /// [`DupRoCursor::seek_dup`] and [`Self::seek_key`]'s refusals.
    pub fn seek_dup<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        k: &[u8],
        s: &[u8],
    ) -> Result<Option<(&'c [u8], &'c [u8])>> {
        self.walk.check_key(k)?;
        let value = self.seek_call(txn, |c, work| {
            c.walk_op(work, |w, store, root, roots| {
                w.seek_dup(store, root, roots, &checked(store), k, s)
            })
        })?;
        Ok(self.row(value))
    }

    /// [`Self::seek_dup`] followed by [`Self::current_with_key_tail`]; `None` (the cursor unset)
    /// if `k` has no such row.
    pub fn seek_dup_with_key_tail<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        k: &[u8],
        s: &[u8],
        t: usize,
    ) -> Result<Option<(&'c [u8], KeyTail<'c>)>> {
        self.walk.check_key(k)?;
        let tail = self.seek_call(txn, |c, work| {
            let store = work.writer();
            let found = c.walk_op(work, |w, store, root, roots| {
                w.seek_dup(store, root, roots, &checked(store), k, s)
            })?;
            if found.is_none() {
                return Ok(None);
            }
            c.walk.current_with_key_tail(store, &checked(store), t)
        })?;
        Ok(tail.map(|tail| (self.walk.key(), tail)))
    }

    /// Move to the next row only if its logical key shares the first `n` bytes of the current
    /// row's, and return it; else return `None` and stay. The rules are
    /// [`TxnCursor::next_in_prefix`]'s on logical keys (after a delete: the deleted key's prefix).
    pub fn next_in_prefix<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        n: usize,
    ) -> Result<Option<(&'c [u8], &'c [u8])>> {
        self.step_in_prefix(txn, n, true)
    }

    /// Move to the previous row only if its logical key shares the first `n` bytes of the current
    /// row's, and return it; else return `None` and stay. The rules are
    /// [`TxnCursor::prev_in_prefix`]'s on logical keys (after a delete: the deleted key's prefix,
    /// so after a key's last row went the step reaches the key's new last row).
    pub fn prev_in_prefix<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        n: usize,
    ) -> Result<Option<(&'c [u8], &'c [u8])>> {
        self.step_in_prefix(txn, n, false)
    }

    /// [`Self::next_in_prefix`] (`forward`) or [`Self::prev_in_prefix`].
    fn step_in_prefix<'c>(
        &'c mut self,
        txn: &'c RwTxn,
        n: usize,
        forward: bool,
    ) -> Result<Option<(&'c [u8], &'c [u8])>> {
        let value = self.call(txn, |c, work| {
            let Self { track, outer_root, walk, deleted, deleted_key, target, .. } = c;
            let store = work.writer();
            let again = checked(store);
            let mut roots = WorkRoots::new(work, track);
            if !*deleted {
                return walk.step_in_prefix(store, outer_root, &mut roots, &again, n, forward);
            }
            let Some(prefix) = deleted_key.get(..n) else { return Ok(None) };
            // The position is the deleted row's follower (or after the end).
            let landed = if forward {
                walk.current(store, &again).map(|v| v.filter(|_| walk.key().starts_with(prefix)))
            } else {
                walk.step_if(store, outer_root, &mut roots, &again, prefix, false, target)
            };
            if matches!(landed, Ok(Some(_)) | Err(_)) {
                *deleted = false;
            }
            landed
        })?;
        Ok(self.row(value))
    }

    /// Store `value` under the logical `key` (as [`RwTxn::put`] does) and move to that row. A
    /// refused row ([`Error::KeyTooShort`], [`Error::KeyTooLarge`], [`Error::ValueTooLarge`])
    /// changes nothing, the cursor included.
    pub fn put(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(txn, key, value, PutKind::Upsert)
    }

    /// Store `value` under the logical `key` as the table's new last row (as [`RwTxn::append`]
    /// does) and move to that row. A refused append ([`Error::AppendOutOfOrder`] and
    /// [`Self::put`]'s refusals) changes nothing, the cursor included.
    pub fn append(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(txn, key, value, PutKind::Append)
    }

    /// Store `value` under the logical `key` only if the table does not hold it (as
    /// [`RwTxn::insert`] does) and move to that row. A refused insert ([`Error::KeyExists`] and
    /// [`Self::put`]'s refusals) changes nothing, the cursor included.
    pub fn insert(&mut self, txn: &mut RwTxn, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_entry(txn, key, value, PutKind::Insert)
    }

    /// [`Self::put`], [`Self::append`] or [`Self::insert`] (`kind`): the transaction and table
    /// checked, but no reseek, since the write places the cursor anew. A write into the key of
    /// the row the cursor was at, up to date, that created no key changed only that key's nested
    /// tree: the outer path stays and the nested path is placed from the tree's path cache. Any
    /// other write finds the row again from both roots. The table's new write stamp is the
    /// cursor's own, so its write causes it no reseek.
    fn put_entry(
        &mut self,
        txn: &mut RwTxn,
        key: &[u8],
        value: &[u8],
        kind: PutKind,
    ) -> Result<()> {
        self.track.check(&txn.work).inspect_err(|e| txn.inner.latch.latch_error(e))?;
        let was_current = self.track.up_to_date(&txn.work);
        let placed = txn.dup_put_entry(&self.track.table, key, value, kind)?;
        self.deleted = false;
        self.track.writes = self.track.table_writes(&txn.work);
        let work = &txn.work;
        // The put took the key: it is longer than the key width.
        let (k, s) = key.split_at(self.walk.kl());
        let in_key = was_current
            && !placed.created
            && self.walk.state() == State::At
            && self.walk.key().starts_with(k);
        let cached = self.track.slot.and_then(|i| work.tables[i].nested.get(k)).zip(placed.slot);
        let found = match cached {
            Some((n, slot)) if in_key => {
                #[cfg(test)]
                {
                    self.placements += 1;
                }
                self.walk.place_in_key(n.root, n.cache.branches(), n.cache.leaf(), slot, s)
            }
            _ => self.find_put(work, key),
        };
        found.inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// After the cursor's own put of the row `key`: take the table's working outer tree and find
    /// the row from both roots; it must be there.
    fn find_put(&mut self, work: &Work, key: &[u8]) -> Result<()> {
        self.outer_root = self.working_root(work)?;
        let Self { track, outer_root, walk, .. } = self;
        walk.unset();
        let mut roots = WorkRoots::new(work, track);
        if walk.seek_exact(work.writer(), outer_root, &mut roots, &no_leaf, key)?.is_none() {
            return Err(Error::Failed(format!(
                "the row just put in dup table {:?} is missing",
                track.table.name()
            )));
        }
        Ok(())
    }

    /// Remove the row the cursor is at and return `true`: after its row was deleted (by it or
    /// another writer), the follower. `false` (nothing changes) if the cursor is not at a row. The
    /// cursor is left on the deleted row's follower (the next row of its key, else the next key's
    /// first row, else after the end) in the deleted state, as [`TxnCursor::delete_current`]
    /// leaves it on the twin.
    pub fn delete_current(&mut self, txn: &mut RwTxn) -> Result<bool> {
        self.sync(&txn.work).inspect_err(|e| txn.inner.latch.latch_error(e))?;
        if self.walk.state() != State::At {
            return Ok(false);
        }
        // The row the cursor is at (while `deleted`, the follower) becomes the deleted row.
        self.deleted_key.clear();
        self.deleted_key.extend_from_slice(self.walk.key());
        let removed = match txn.dup_delete_entry(&self.track.table, &self.deleted_key) {
            Ok(removed) if removed.found => removed,
            Ok(_) => {
                let cause = format!(
                    "the row under a cursor of dup table {:?} is missing",
                    self.track.table.name()
                );
                txn.inner.latch.fail(cause.clone());
                self.walk.unset();
                self.deleted = false;
                return Err(Error::Failed(cause));
            }
            Err(e) => {
                self.walk.unset();
                self.deleted = false;
                return Err(e);
            }
        };
        // Also leaves `slot` on the table's working record, which the delete created if needed.
        self.track.writes = self.track.table_writes(&txn.work);
        let landed = self.land_after_delete(&txn.work, removed);
        self.deleted = landed.is_ok();
        landed.map(|()| true).inspect_err(|e| txn.inner.latch.latch_error(e))
    }

    /// Land on the follower of the row `deleted_key` the cursor just deleted: from the nested
    /// tree's path cache when the delete kept the key's tree and left the cache naming the row's
    /// leaf, from the outer path cache when the delete emptied the key and left that cache naming
    /// the cell's leaf, else found again from both roots ([`DupRemoved`]).
    fn land_after_delete(&mut self, work: &Work, removed: DupRemoved) -> Result<()> {
        self.outer_root = self.working_root(work)?;
        let Self { track, outer_root, walk, deleted_key, .. } = self;
        let w = track.slot.map(|i| &work.tables[i]);
        let store = work.writer();
        let mut roots = WorkRoots::new(work, track);
        let k = &deleted_key[..walk.kl()];
        let landed = match (w, removed.slot) {
            (Some(w), Some(at)) if removed.emptied => {
                #[cfg(test)]
                {
                    self.landings[1] += 1;
                }
                let path = (w.cache.branches(), w.cache.leaf(), at);
                walk.place_after_outer_delete(store, outer_root, &mut roots, path)
            }
            (Some(w), Some(at)) if w.nested.contains_key(k) => {
                let n = &w.nested[k];
                #[cfg(test)]
                {
                    self.placements += 1;
                    self.landings[0] += 1;
                }
                let path = (n.cache.branches(), n.cache.leaf(), at);
                walk.place_after_nested_delete(
                    store,
                    outer_root,
                    &mut roots,
                    &checked(store),
                    n.root,
                    path,
                )
            }
            _ => {
                #[cfg(test)]
                {
                    self.landings[2] += 1;
                }
                walk.unset();
                walk.seek(store, outer_root, &mut roots, &no_leaf, deleted_key)
            }
        };
        landed.map(drop)
    }

    /// Whether the deleted row's follower shares the deleted key's first `n` bytes; see
    /// [`TxnCursor::follower_shares_prefix`], on logical keys. Up to date, it reads no page.
    #[inline]
    pub fn follower_shares_prefix(&mut self, txn: &RwTxn, n: usize) -> Result<bool> {
        self.call(txn, |c, _| {
            let on_follower = c.deleted && c.walk.state() == State::At;
            Ok(on_follower && c.deleted_key.get(..n).is_some_and(|p| c.walk.key().starts_with(p)))
        })
    }
}
