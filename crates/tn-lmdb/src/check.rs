//! `Env::check`: a full structural check of the newest committed state — the catalog, every
//! table and the free tree, page by page — and its exact page accounting: every page of
//! `2..page_count` is either reachable from exactly one tree, once, or listed exactly once by the
//! free list. Each tree's root must count the entries and the pages of each kind the walk finds.
//! Every overflow run a leaf names is checked as a read checks it, plus the full CRC of every run
//! (a 1-page one too) and its zero pad, and its pages count as its table's. A dup table's outer
//! tree holds one cell per outer key of the table's width: a nested cell, whose tree is walked as
//! a plain one, under the table's name, or, in a table that packs, a packed set, validated whole.
//! The record's nested totals must be the sums of the nested roots' counters, and its
//! `packed_rows` the rows of the packed sets (`Checker::walk_dup`).
//!
//! [`check_since`] is open's check of a trusted weak head: the same walk and the same checks of
//! every page it reaches, but it descends only into a page or a run its parent names with a txn
//! after the last sync's, and it skips the counters and the page accounting, which need the whole
//! state (README, Open and recovery). So it keeps only the pages it claims ([`Claims::Visited`]),
//! where `Env::check` keeps a word for every page of the state ([`Claims::Every`]).

use std::collections::{hash_map::Entry, BTreeMap, HashMap};

use memmap2::MmapMut;

use crate::{
    catalog::{self, DupRecord, TableRecord},
    crc::CrcState,
    env::Snapshot,
    error::{Error, Result},
    file,
    freelist::{decode_pages, RecordKey},
    overflow::{self, Ovf, RunCrc},
    packed,
    page::{self, Kind, Val},
    tree::{PageRead, TreeRoot},
};

/// What `Env::check` found in a sound state: how its pages are used, and each table's size.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckReport {
    /// The id of the state checked.
    pub txn_id: u64,
    /// Its page count: the two meta pages plus every page counted below.
    pub page_count: u32,
    /// The pages of the catalog's tree.
    pub catalog_pages: u64,
    /// The pages of every table's tree, its overflow runs' included.
    pub table_pages: u64,
    /// The pages of the free tree.
    pub free_tree_pages: u64,
    /// The pages the free list lists as reusable.
    pub free_pages: u64,
    /// The number of free records.
    pub free_records: u64,
    /// Each table, by name.
    pub tables: BTreeMap<String, TableCheck>,
}

/// One table in a [`CheckReport`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableCheck {
    /// The number of entries.
    pub entries: u64,
    /// The pages of its tree, its overflow runs' included.
    pub pages: u64,
    /// The height of its tree (0: empty).
    pub height: u32,
}

/// Who uses a page: index into [`Checker::owners`] (0: nothing).
type Owner = u32;

/// The bytes of an [`Owner`] in [`Claims::Every`]'s table.
const OWNER_BYTES: usize = size_of::<Owner>();

/// Who claimed each page.
enum Claims {
    /// An owner word for every page below the state's page count, as `Env::check`'s page
    /// accounting needs ([`leaked_runs`]): 4 bytes a page, in an anonymous mapping, whose pages
    /// are zero-filled as the walk first touches them, so only the pages it claims cost memory.
    /// A meta counting far more pages than the file holds data for (a sparse file meets open's
    /// length check for free) is a crafted file: its table costs address space and one scan, not
    /// memory (16 GiB of address space at most; a host that refuses it is
    /// [`Error::OutOfMemory`], [`Claims::every`]).
    Every(MmapMut),
    /// The pages claimed, by number, and nothing for the others: [`check_since`] visits the pages
    /// written since the last sync and the free pages its free leaves list, so its memory follows
    /// what it reads, not the size of the file.
    Visited(HashMap<u32, Owner>),
}

impl Claims {
    /// A table of every page of a state of `page_count` pages, none claimed: [`Self::Every`]. A
    /// table the host refuses is [`Error::OutOfMemory`] (on a 32-bit target, one whose size
    /// passes `usize` is the mapping's [`Error::Io`], never a wrapped size).
    fn every(page_count: u32) -> Result<Self> {
        Self::zeroed((page_count as usize).saturating_mul(OWNER_BYTES)).map(Self::Every)
    }

    /// `bytes` of zeros, mapped anonymously: a mapping refused for want of memory is
    /// [`Error::OutOfMemory`] ([`file::mapped`]). `vec![0; n]` would abort the process when the
    /// allocator refuses, and a fallible reservation would need its zeros written, every page of
    /// it then costing memory.
    fn zeroed(bytes: usize) -> Result<MmapMut> {
        file::mapped(MmapMut::map_anon(bytes), bytes)
    }

    /// Claim page `p` for `owner`, or, if something claimed it already, leave it to that first
    /// owner and return it.
    fn claim(&mut self, p: u32, owner: Owner) -> Option<Owner> {
        match self {
            Self::Every(table) => {
                let word = &mut table.as_chunks_mut::<OWNER_BYTES>().0[p as usize];
                let first = Owner::from_ne_bytes(*word);
                if first != 0 {
                    return Some(first);
                }
                *word = owner.to_ne_bytes();
                None
            }
            Self::Visited(claimed) => match claimed.entry(p) {
                Entry::Occupied(first) => Some(*first.get()),
                Entry::Vacant(slot) => {
                    slot.insert(owner);
                    None
                }
            },
        }
    }

    /// The runs of pages nothing claimed ([`leaked_runs`]), or `None`: always for
    /// [`Self::Visited`], which knows only the pages claimed.
    fn leaked(&self) -> Option<String> {
        match self {
            Self::Every(table) => leaked_runs(table.as_chunks::<OWNER_BYTES>().0),
            Self::Visited(_) => None,
        }
    }

    /// Test-only: the pages the claims hold room for: every page of the table, or the map's
    /// capacity.
    #[cfg(test)]
    fn slots(&self) -> usize {
        match self {
            Self::Every(table) => table.len() / OWNER_BYTES,
            Self::Visited(claimed) => claimed.capacity(),
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only: the slots ([`Claims::slots`]) of this thread's last walk that reached the page
    /// accounting, read by its tests.
    static CLAIM_SLOTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// What the leaves of a walked tree may hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Leaves {
    /// Inline values and overflow runs: a nested cell is [`Error::Corrupt`]. The catalog, the
    /// free tree, a plain table's tree and a dup table's nested trees.
    Plain,
    /// A dup table's outer tree: nested cells and, in a table that packs, packed sets (inline
    /// cells), which the entry callback checks (an inline cell of a table that never packs is
    /// its refusal), and no overflow cell.
    DupOuter,
}

/// The page walk's state: who uses each page, and every page claimed twice.
struct Checker<'s> {
    snapshot: &'s Snapshot,
    /// The owner of each page claimed: of every page of the state in a full walk, of the pages
    /// claimed only in [`check_since`]'s.
    claims: Claims,
    /// The owners' names: `[none, catalog, free tree, free list, tables...]`.
    owners: Vec<String>,
    /// The pages claimed twice.
    twice: Twice,
    /// [`check_since`]'s last sync: the walk descends only into what its parent names with a txn
    /// after it ([`named_after`]). `None`: into everything.
    since: Option<u64>,
    /// Hold each root's and record's counters to what the walk found (a full walk only).
    accounting: bool,
}

/// The pages claimed twice: every one counted, the first [`LISTED`] kept as `(page, first owner,
/// second owner)`, so a damaged free tree listing every page the trees hold (one double claim
/// per page) costs no memory per page and a short message.
#[derive(Default)]
struct Twice {
    listed: Vec<(u32, Owner, Owner)>,
    count: u64,
}

impl Twice {
    fn note(&mut self, p: u32, first: Owner, second: Owner) {
        self.count += 1;
        if self.listed.len() < LISTED {
            self.listed.push((p, first, second));
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The pages for a message, by `owners`' names: `[9 (catalog and free list)]`, and past
    /// [`LISTED`] of them `[9 (..), ...; 7 more]`.
    fn describe(&self, owners: &[String]) -> String {
        let mut named: Vec<String> = self
            .listed
            .iter()
            .map(|&(p, a, b)| format!("{p} ({} and {})", owners[a as usize], owners[b as usize]))
            .collect();
        let more = self.count - self.listed.len() as u64;
        if more > 0 {
            named.push(format!("...; {more} more"));
        }
        format!("[{}]", named.join(", "))
    }
}

const CATALOG: Owner = 1;
const FREE_TREE: Owner = 2;
const FREE_LIST: Owner = 3;

/// The most runs of leaked pages, and the most pages used twice, a failed check's message names;
/// it counts the rest.
const LISTED: usize = 16;

impl Checker<'_> {
    /// Whether the walk descends into a page or run its parent names with the low 32 bits `txn`
    /// of the txn that wrote it: always, but in [`check_since`] only when that txn is after the
    /// last sync's.
    fn descends(&self, txn: u32) -> bool {
        self.since.is_none_or(|since| named_after(txn, since))
    }

    /// Claim page `p` for `owner`: false (and noted) if something claimed it already.
    fn claim(&mut self, p: u32, owner: Owner) -> bool {
        let Some(first) = self.claims.claim(p, owner) else {
            return true;
        };
        self.twice.note(p, first, owner);
        false
    }

    /// Walk the tree `root` names for `owner`, checking every page, holding its leaves to
    /// `policy` and passing each leaf entry to `entry`. Returns the tree's page count. A page
    /// claimed already is noted and not descended into, and so is one the walk skips
    /// ([`Self::descends`]). In a full walk, a root whose entry count or page counters differ from
    /// what the walk found is [`Error::Corrupt`] (unless a page was claimed twice: the accounting
    /// reports that).
    fn walk(
        &mut self,
        root: &TreeRoot,
        owner: Owner,
        policy: Leaves,
        mut entry: impl FnMut(&mut Self, &[u8], Val<'_>) -> Result<()>,
    ) -> Result<u64> {
        root.check()?;
        if root.is_empty() || !self.descends(root.root_txn) {
            return Ok(0);
        }
        let snapshot = self.snapshot;
        let geometry = snapshot.geometry;
        let name = self.owners[owner as usize].clone();
        let corrupt =
            |p: u32, what: String| Error::Corrupt(format!("check: {name}: page {p} {what}"));
        let (mut branches, mut leaves, mut overflows, mut entries) = (0_u64, 0_u64, 0_u64, 0);
        let mut stack = vec![(root.root, root.root_txn, 1, None::<Vec<u8>>, None::<Vec<u8>>)];
        while let Some((p, want_txn, depth, lo, hi)) = stack.pop() {
            if p < 2 || p >= snapshot.meta.page_count {
                return Err(corrupt(p, "is not a tree page".to_string()));
            }
            if !self.claim(p, owner) {
                continue;
            }
            let page = snapshot.page(p).map_err(|e| corrupt(p, e.to_string()))?;
            if page::crc_state(page) != CrcState::Valid {
                return Err(corrupt(p, "fails its CRC".to_string()));
            }
            page::validate(page, &geometry).map_err(|e| corrupt(p, e.to_string()))?;
            let txn = page::txn(page);
            if txn as u32 != want_txn {
                return Err(corrupt(p, format!("has txn {txn}, not the {want_txn} named")));
            }
            let leaf = page::kind(page)? == Kind::Leaf;
            if leaf != (depth == root.height) {
                return Err(corrupt(p, format!("is the wrong kind at depth {depth}")));
            }
            if !page::keys_in_range(page, lo.as_deref(), hi.as_deref())? {
                return Err(corrupt(p, "holds keys outside its parents' separators".to_string()));
            }
            let n = page::nkeys(page);
            if leaf {
                leaves += 1;
                if depth > 1 && n == 0 {
                    return Err(corrupt(p, "is an empty non-root leaf".to_string()));
                }
                entries += n as u64;
                for i in 0..n {
                    let (k, v) = page::leaf_entry(page, i)?;
                    if let (Leaves::DupOuter, Val::Overflow(ovf)) = (policy, v) {
                        return Err(corrupt(
                            p,
                            format!(
                                "holds an overflow cell in a dup table's outer tree (a \
                                 reference to run {})",
                                ovf.pgno
                            ),
                        ));
                    }
                    entry(self, k, v).map_err(|e| corrupt(p, e.to_string()))?;
                    match v {
                        Val::Inline(_) => {}
                        Val::Overflow(ovf) if !self.descends(ovf.txn) => {}
                        Val::Overflow(ovf) => {
                            overflows += self
                                .claim_run(ovf, owner)
                                .map_err(|e| corrupt(p, e.to_string()))?;
                        }
                        Val::Nested(root) if policy == Leaves::Plain => {
                            return Err(corrupt(p, page::nested_error(root).to_string()));
                        }
                        Val::Nested(_) => {}
                    }
                }
                continue;
            }
            branches += 1;
            // A root branch the writer leaves with one child gives way to it (a collapse, or a
            // merge's `merged_root`), so a root branch keeps two children or more.
            if depth == 1 && n < 2 {
                return Err(corrupt(p, "is a root branch with one child".to_string()));
            }
            for i in 0..n {
                let (child, child_txn) = page::branch_child(page, i)?;
                if !self.descends(child_txn) {
                    continue;
                }
                let child_lo =
                    if i == 0 { lo.clone() } else { Some(page::branch_key(page, i)?.to_vec()) };
                let child_hi = if i + 1 < n {
                    Some(page::branch_key(page, i + 1)?.to_vec())
                } else {
                    hi.clone()
                };
                stack.push((child, child_txn, depth + 1, child_lo, child_hi));
            }
        }
        if !self.twice.is_empty() || !self.accounting {
            return Ok(branches + leaves + overflows);
        }
        if entries != root.entries {
            return Err(Error::Corrupt(format!(
                "check: {name}: {entries} entries, not the {} its root counts",
                root.entries
            )));
        }
        let counted = [
            ("branch", branches, root.branch_pages),
            ("leaf", leaves, root.leaf_pages),
            ("overflow", overflows, root.overflow_pages),
        ];
        for (kind, found, counts) in counted {
            if found != u64::from(counts) {
                return Err(Error::Corrupt(format!(
                    "check: {name}: {found} {kind} pages, not the {counts} its root counts"
                )));
            }
        }
        Ok(branches + leaves + overflows)
    }

    /// Walk a dup table's outer tree `root` and every nested tree its cells name, all for `owner`
    /// (so a page any two of them name is claimed twice), then hold the record `dup` to the
    /// trees and sets found. Each outer key is `dup.key_len` bytes. Each cell is nested, or, when
    /// `dup.packed_max` is not 0, a packed set that passes the whole [`packed::validate`] (at most
    /// `packed_max` bytes, subkeys of 1 byte up to the table's widest, strictly ascending); each
    /// nested tree passes the walk of a plain tree (its pages, its counters against them, its
    /// runs), with subkeys of 1 byte up to the table's widest. Unless a page was claimed twice
    /// (the page accounting reports that), the record's nested totals are the sums of the nested
    /// roots' counters, its `packed_rows` the rows of the packed sets, and the outer entries the
    /// nested trees plus the packed sets; any other figure is [`Error::Corrupt`], naming the
    /// field.
    fn walk_dup(&mut self, root: &TreeRoot, dup: &DupRecord, owner: Owner) -> Result<TableCheck> {
        // The refusal of an inline cell in a table that never packs (an overflow cell is the
        // walk's).
        const NOT_NESTED: &str =
            "a cell that is not nested in a dup table's outer tree, which nests every key";
        let width = usize::from(dup.key_len);
        let max_subkey = self.snapshot.geometry.max_subkey(width);
        let packed_max = usize::from(dup.packed_max);
        // The nested roots' counters, summed: [trees, entries, branch, leaf, overflow pages].
        let mut sums = [0_u64; 5];
        // The packed sets and their rows.
        let (mut sets, mut packed_rows) = (0_u64, 0_u64);
        let mut nested_pages = 0;
        let outer_pages = self.walk(root, owner, Leaves::DupOuter, |checker, k, v| {
            if k.len() != width {
                return Err(Error::Corrupt(format!(
                    "outer key of {} bytes in a table of width {width}",
                    k.len()
                )));
            }
            let in_key = |e: Error| match e {
                Error::Corrupt(m) => Error::Corrupt(format!("key {}: {m}", hex(k))),
                other => other,
            };
            let cell = match v {
                Val::Nested(cell) => cell,
                Val::Inline(blob) if packed_max != 0 => {
                    let set = packed::validate(blob, packed_max, max_subkey).map_err(in_key)?;
                    sets = sets.saturating_add(1);
                    packed_rows = packed_rows.saturating_add(set.rows() as u64);
                    return Ok(());
                }
                _ => return Err(Error::Corrupt(NOT_NESTED.to_string())),
            };
            let nested = TreeRoot::decode(cell).map_err(in_key)?;
            nested_pages += checker
                .walk(&nested, owner, Leaves::Plain, |_, subkey, _| {
                    if (1..=max_subkey).contains(&subkey.len()) {
                        Ok(())
                    } else {
                        Err(Error::Corrupt(format!(
                            "subkey of {} bytes, not 1 to {max_subkey}",
                            subkey.len()
                        )))
                    }
                })
                .map_err(in_key)?;
            let counters = [
                1,
                nested.entries,
                u64::from(nested.branch_pages),
                u64::from(nested.leaf_pages),
                u64::from(nested.overflow_pages),
            ];
            for (sum, n) in sums.iter_mut().zip(counters) {
                *sum = sum.saturating_add(n);
            }
            Ok(())
        })?;
        if self.twice.is_empty() && self.accounting {
            let [trees, entries, branches, leaves, overflows] = sums;
            let fields = [
                ("nested_trees", trees, u64::from(dup.nested_trees)),
                ("nested_entries", entries, dup.nested_entries),
                ("nested_branch_pages", branches, u64::from(dup.nested_branch_pages)),
                ("nested_leaf_pages", leaves, u64::from(dup.nested_leaf_pages)),
                ("nested_overflow_pages", overflows, u64::from(dup.nested_overflow_pages)),
                ("packed_rows", packed_rows, dup.packed_rows),
                ("outer entries", trees.saturating_add(sets), root.entries),
            ];
            for (field, found, recorded) in fields {
                if found != recorded {
                    return Err(Error::Corrupt(format!(
                        "check: {}: {field} {found} in its trees, not the {recorded} its record \
                         counts",
                        self.owners[owner as usize]
                    )));
                }
            }
        }
        Ok(TableCheck {
            entries: dup.nested_entries.saturating_add(dup.packed_rows),
            pages: outer_pages + nested_pages,
            height: root.height,
        })
    }

    /// Check the overflow run `ovf` names as a read does, plus its full CRC even for a 1-page run
    /// ([`RunCrc::Always`]) and its zero pad, then claim each of its pages for `owner`. Returns
    /// its page count.
    fn claim_run(&mut self, ovf: Ovf, owner: Owner) -> Result<u64> {
        let run = self.snapshot.run(ovf, RunCrc::Always)?;
        if !overflow::pad_is_zero(run, ovf) {
            return Err(Error::Corrupt(format!("overflow run {} has a non-zero pad", ovf.pgno)));
        }
        let n = (run.len() / self.snapshot.geometry.page_size()) as u32;
        for p in ovf.pgno..ovf.pgno + n {
            self.claim(p, owner);
        }
        Ok(u64::from(n))
    }
}

/// Whether a page or run its parent names with `txn`, the low 32 bits of the txn that wrote it,
/// was written after txn `since`: its wrapping distance from `since`'s low 32 bits is 1 to
/// 2^31 - 1. That is exact for every txn after `since` and fewer than 2^31 past it
/// ([`check_since`] walks everything past that); a txn at or before `since` and less than 2^31
/// before it is never after, and one further back may be taken as after, only checked needlessly.
pub(crate) fn named_after(txn: u32, since: u64) -> bool {
    let distance = txn.wrapping_sub(since as u32);
    distance != 0 && distance < 1 << 31
}

/// `Env::check` of one snapshot.
pub(crate) fn check_snapshot(snapshot: &Snapshot) -> Result<CheckReport> {
    walk_state(snapshot, None, true)
}

/// Open's check of a trusted weak head (README, Open and recovery): `snapshot` is the head's
/// state and `since` the txn of the steady meta beside it, the last sync. Every page and run the
/// walk reaches passes `Env::check`'s checks (the full CRC and structure of each page, its txn the
/// one its parent names, its keys inside its parents' separators, each run's full CRC and zero
/// pad, every packed set validated, every catalog and free record decoded), but the walk descends
/// into a page or run only when its parent names it with a txn after `since` ([`named_after`]):
/// one named at or before it was written by a txn at or before the last sync, so it was on disk
/// before that sync's meta was. It covers the catalog, every table (a dup table's outer tree and
/// its nested trees) and the free tree. A page reached twice is [`Error::Corrupt`]; the counters
/// and the accounting of the file's other pages are not checked, so it keeps only the pages it
/// claims ([`Claims::Visited`]): its memory follows the pages it reads, not the file's page count.
/// When `since` is 2^31 or more txns before the head, the walk descends into everything. Returns
/// the pages checked.
pub(crate) fn check_since(snapshot: &Snapshot, since: u64) -> Result<u64> {
    let prune = snapshot.meta.txn.checked_sub(since).is_some_and(|gap| gap < 1 << 31);
    let report = walk_state(snapshot, prune.then_some(since), false)?;
    Ok(report.catalog_pages + report.table_pages + report.free_tree_pages)
}

/// The walk of [`check_snapshot`] (`accounting`: its counters and its page accounting too, over
/// a table of every page, [`Claims::every`], which a host that refuses it fails as
/// [`Error::OutOfMemory`]) or of [`check_since`] (`since`: only what was written after it, its
/// claims in a map of the pages claimed).
fn walk_state(snapshot: &Snapshot, since: Option<u64>, accounting: bool) -> Result<CheckReport> {
    let meta = snapshot.meta;
    let mut checker = Checker {
        snapshot,
        claims: if accounting {
            Claims::every(meta.page_count)?
        } else {
            Claims::Visited(HashMap::new())
        },
        owners: ["", "catalog", "free tree", "free list"].map(String::from).to_vec(),
        twice: Twice::default(),
        since,
        accounting,
    };
    let mut report =
        CheckReport { txn_id: meta.txn, page_count: meta.page_count, ..Default::default() };

    let mut tables: Vec<(String, TableRecord)> = Vec::new();
    report.catalog_pages = checker.walk(&meta.catalog, CATALOG, Leaves::Plain, |_, k, v| {
        let name = std::str::from_utf8(k)
            .map_err(|_| Error::Corrupt("a table name is not UTF-8".to_string()))?;
        catalog::check_name(name)
            .map_err(|_| Error::Corrupt(format!("bad table name {name:?}")))?;
        let record = *TableRecord::decode(v.inline()?)?.usable(&snapshot.geometry)?;
        if record.id >= meta.next_table_id || record.created_txn > meta.txn {
            return Err(Error::Corrupt(format!(
                "table {name:?} has an impossible record {record:?}"
            )));
        }
        tables.push((name.to_string(), record));
        Ok(())
    })?;
    let mut ids: Vec<u32> = tables.iter().map(|(_, r)| r.id).collect();
    ids.sort_unstable();
    if ids.windows(2).any(|w| w[0] == w[1]) {
        return Err(Error::Corrupt("check: catalog: two tables share an id".to_string()));
    }

    for (name, record) in tables {
        let owner = checker.owners.len() as Owner;
        checker.owners.push(format!("table {name:?}"));
        let table = match &record.dup {
            None => {
                let pages = checker.walk(&record.root, owner, Leaves::Plain, |_, _, _| Ok(()))?;
                TableCheck { entries: record.root.entries, pages, height: record.root.height }
            }
            Some(dup) => checker.walk_dup(&record.root, dup, owner)?,
        };
        report.table_pages += table.pages;
        report.tables.insert(name, table);
    }

    let (mut free_pages, mut free_records) = (0, 0);
    report.free_tree_pages =
        checker.walk(&meta.free, FREE_TREE, Leaves::Plain, |checker, k, v| {
            let key = RecordKey::decode(k)?;
            if key.txn > meta.txn {
                return Err(Error::Corrupt(format!("record {key:?} is later than the state")));
            }
            free_records += 1;
            for p in decode_pages(v.inline()?, meta.page_count)? {
                free_pages += 1;
                checker.claim(p, FREE_LIST);
            }
            Ok(())
        })?;
    report.free_pages = free_pages;
    report.free_records = free_records;

    #[cfg(test)]
    CLAIM_SLOTS.with(|slots| slots.set(checker.claims.slots()));
    let leaked = checker.claims.leaked();
    if leaked.is_none() && checker.twice.is_empty() {
        return Ok(report);
    }
    Err(Error::Corrupt(format!(
        "check: page accounting of txn {}: leaked {}; used twice {}",
        meta.txn,
        leaked.as_deref().unwrap_or("[]"),
        checker.twice.describe(&checker.owners)
    )))
}

/// `bytes` in hex, for a message.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The runs of pages of `2..owner.len()` nothing claimed (each page's owner word, its type's
/// default, zero, for none), found in one pass, as compact runs: `[2, 5..8]`; `None` if there are
/// none. Only the first [`LISTED`] runs are kept, the rest counted: `[2, 5..8, ...; 7 more runs,
/// 4294967268 pages in all]`. So a meta whose page count runs far past the pages any tree or
/// record names (a crafted file) costs no memory past the owner table.
fn leaked_runs<W: Default + PartialEq>(owner: &[W]) -> Option<String> {
    let none = W::default();
    let (mut listed, mut runs, mut pages, mut start) = (Vec::new(), 0_u64, 0_u64, 2);
    for chunk in owner.get(2..).unwrap_or_default().chunk_by(|a, b| (*a == none) == (*b == none)) {
        let end = start + chunk.len();
        if chunk[0] == none {
            runs += 1;
            pages += chunk.len() as u64;
            if listed.len() < LISTED {
                listed.push(if chunk.len() == 1 {
                    start.to_string()
                } else {
                    format!("{start}..{end}")
                });
            }
        }
        start = end;
    }
    if runs == 0 {
        return None;
    }
    let more = runs - listed.len() as u64;
    if more > 0 {
        listed.push(format!("...; {more} more runs, {pages} pages in all"));
    }
    Some(format!("[{}]", listed.join(", ")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        env::{Env, EnvOptions},
        ReadTxn as _,
    };

    /// The skip rule of open's check of a weak head: a txn named in a parent's 32 bits is after the
    /// last sync exactly when it is 1 to 2^31 - 1 past it, whatever the high bits and across the
    /// 32-bit wrap: every txn after the sync and fewer than 2^31 past it is checked, and none at or
    /// before it and fewer than 2^31 before it.
    #[test]
    fn a_txn_is_named_after_the_last_sync_by_its_wrapping_distance() {
        for since in
            [0, 5, u64::from(u32::MAX) - 2, (7 << 32) + (1 << 31) + 9, u64::MAX - 1 - (1 << 31)]
        {
            let named = |txn: u64| named_after(txn as u32, since);
            for d in [1, 2, 1000, (1 << 31) - 1] {
                assert!(named(since + d), "{since} + {d}");
            }
            for d in [0, 1, 2, 1000, (1 << 31) - 1] {
                if let Some(txn) = since.checked_sub(d) {
                    assert!(!named(txn), "{since} - {d}");
                }
            }
            assert!(!named(since + (1 << 31)), "{since} + 2^31: taken as before, so walked whole");
        }
    }

    /// Leaked pages are named as runs, up to [`LISTED`] of them, and the rest counted: the
    /// message stays short however many runs the meta's page count leaves unclaimed.
    #[test]
    fn leaked_runs_name_the_first_runs_and_count_the_rest() {
        assert_eq!(leaked_runs(&[0, 0]), None);
        assert_eq!(leaked_runs(&[0, 0, 1, 2]), None);
        assert_eq!(leaked_runs(&[0, 0, 0, 1, 0, 0, 0, 1, 0]).as_deref(), Some("[2, 4..7, 8]"));
        // From page 2 on, every other page leaked: a run per page, far more than are named.
        let runs = 100_000;
        let owner: Vec<Owner> = (0..2 + 2 * runs).map(|p| (p % 2) as Owner).collect();
        let named: Vec<String> = (0..LISTED).map(|i| (2 + 2 * i).to_string()).collect();
        assert_eq!(
            leaked_runs(&owner).expect("leaked"),
            format!(
                "[{}, ...; {} more runs, {runs} pages in all]",
                named.join(", "),
                runs - LISTED
            )
        );
    }

    /// Pages used twice are named up to [`LISTED`] of them and the rest counted: a free tree
    /// listing every page a tree holds keeps the message short.
    #[test]
    fn pages_used_twice_name_the_first_and_count_the_rest() {
        let owners = ["", "catalog", "free tree", "free list", "table \"a\""].map(String::from);
        let mut twice = Twice::default();
        assert!(twice.is_empty());
        assert_eq!(twice.describe(&owners), "[]");
        twice.note(9, 4, FREE_LIST);
        twice.note(11, CATALOG, 4);
        assert!(!twice.is_empty());
        let both = "[9 (table \"a\" and free list), 11 (catalog and table \"a\")]";
        assert_eq!(twice.describe(&owners), both);
        // A free tree listing each of 100k pages a table holds: one double claim per page.
        let pages = 100_000;
        let mut many = Twice::default();
        for p in 2..2 + pages {
            many.note(p, 4, FREE_LIST);
        }
        assert_eq!((many.listed.len(), many.count), (LISTED, u64::from(pages)));
        let named: Vec<String> =
            (2..2 + LISTED as u32).map(|p| format!("{p} (table \"a\" and free list)")).collect();
        assert_eq!(
            many.describe(&owners),
            format!("[{}, ...; {} more]", named.join(", "), pages - LISTED as u32)
        );
    }

    /// Both claim structures give a page to its first claimer: a second claim is refused,
    /// returning that first owner, and a third too (the first is kept). The table of every page
    /// starts with every page unclaimed (its zeros, which [`leaked_runs`] reads); the map of the
    /// pages claimed holds room for those it claims, however high their numbers.
    #[test]
    fn a_page_claimed_twice_stays_its_first_claimers() {
        let table = Claims::every(64).expect("a table of 64 pages");
        assert_eq!(table.slots(), 64);
        assert_eq!(table.leaked().as_deref(), Some("[2..64]"), "nothing claimed");
        for (mut claims, pages) in [
            (table, [2, 9, 63]),
            (Claims::Visited(HashMap::new()), [2, u32::MAX - 2, u32::MAX - 1]),
        ] {
            for (owner, &p) in (4..).zip(&pages) {
                assert_eq!(claims.claim(p, owner), None, "page {p}'s first claim");
            }
            for (owner, &p) in (4..).zip(&pages) {
                assert_eq!(claims.claim(p, CATALOG), Some(owner), "page {p} claimed twice");
                assert_eq!(claims.claim(p, FREE_LIST), Some(owner), "page {p}: the first is kept");
            }
            if let Claims::Every(_) = claims {
                assert_eq!(claims.leaked().as_deref(), Some("[3..9, 10..63]"));
            } else {
                assert!(claims.slots() < 8, "{} slots for 3 pages", claims.slots());
                assert_eq!(claims.leaked(), None, "the map knows only the pages claimed");
            }
        }
    }

    /// A table of every page that the host refuses to map (here more than the address space) is
    /// [`Error::OutOfMemory`], with the bytes asked for and `mmap`'s `ENOMEM`: `Env::check`
    /// returns it where an infallible allocation would abort the process.
    #[test]
    fn a_page_table_the_host_refuses_is_out_of_memory() {
        let refused = Claims::zeroed(isize::MAX as usize);
        let Err(Error::OutOfMemory { bytes, source }) = &refused else { panic!("{refused:?}") };
        assert_eq!((*bytes, source.raw_os_error()), (isize::MAX as u64, Some(libc::ENOMEM)));
        assert_eq!(Claims::zeroed(1 << 20).expect("a MiB").iter().filter(|&&b| b != 0).count(), 0);
    }

    /// Open's check of a weak head since the last sync holds room for the pages it claims (the
    /// pages written since the sync and the free pages their free leaves list), not for every page
    /// the meta counts: over the same state with a page count of 2^32 - 1, the most a file holds
    /// (16 GiB of owner words in a table of every page), it passes with a map a few times the
    /// size of what it read.
    #[test]
    fn the_check_since_the_last_sync_holds_the_pages_it_claims_not_the_files() {
        let dir = tempfile::TempDir::with_prefix("tn_lmdb_check").expect("temp dir");
        let env = Env::open(dir.path().join("db"), EnvOptions::default()).expect("open");
        let mut txn = env.begin_rw().expect("begin");
        let table = txn.create_table("a").expect("table");
        for i in 0_u32..2_000 {
            txn.put(&table, &i.to_be_bytes(), &[7; 100]).expect("put");
        }
        let since = txn.commit().expect("the last sync");
        let mut txn = env.begin_rw().expect("begin");
        let table = txn.open_table("a").expect("open").expect("table a");
        for i in (0_u32..2_000).step_by(500) {
            txn.put(&table, &i.to_be_bytes(), &[8; 100]).expect("put");
        }
        txn.commit().expect("the head");
        let head = env.inner().current();
        let full = check_snapshot(&head).expect("the head's full check");
        let state_pages = full.catalog_pages + full.table_pages + full.free_tree_pages;

        let mut meta = head.meta;
        meta.page_count = u32::MAX;
        let huge = Snapshot {
            meta,
            geometry: head.geometry,
            view: Arc::clone(&head.view),
            tables: Default::default(),
        };
        let pages = check_since(&huge, since).expect("the pages since the last sync pass");
        let slots = CLAIM_SLOTS.with(std::cell::Cell::get);
        assert!(0 < pages && pages < state_pages, "{pages} of {state_pages} pages checked");
        let claimed = pages + full.free_pages;
        assert!(
            slots as u64 <= 2 * claimed + 4,
            "{slots} slots for {pages} pages checked and {} free pages listed",
            full.free_pages
        );
    }
}
