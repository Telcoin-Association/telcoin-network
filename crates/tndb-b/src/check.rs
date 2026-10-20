//! `Env::check`: a full structural check of the newest committed state — the catalog, every
//! table and the free tree, page by page — and its exact page accounting: every page of
//! `2..page_count` is either reachable from exactly one tree, once, or listed exactly once by the
//! free list.

use std::collections::BTreeMap;

use crate::{
    catalog::{self, TableRecord},
    crc::{self, CrcState},
    env::Snapshot,
    error::{Error, Result},
    freelist::{decode_pages, RecordKey},
    page::{self, Kind},
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
    /// The pages of every table's tree.
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
    /// The pages of its tree.
    pub pages: u64,
    /// The height of its tree (0: empty).
    pub height: u32,
}

/// Who uses a page: index into [`Checker::owners`] (0: nothing).
type Owner = u32;

/// The page walk's state: who uses each page, and every page claimed twice.
struct Checker<'s> {
    snapshot: &'s Snapshot,
    /// The owner of each page number below the page count.
    owner: Vec<Owner>,
    /// The owners' names: `[none, catalog, free tree, free list, tables...]`.
    owners: Vec<String>,
    /// `(page, first owner, second owner)` for each page claimed twice.
    twice: Vec<(u32, Owner, Owner)>,
}

const CATALOG: Owner = 1;
const FREE_TREE: Owner = 2;
const FREE_LIST: Owner = 3;

impl Checker<'_> {
    /// Claim page `p` for `owner`: false (and noted) if something claimed it already.
    fn claim(&mut self, p: u32, owner: Owner) -> bool {
        let first = self.owner[p as usize];
        if first != 0 {
            self.twice.push((p, first, owner));
            return false;
        }
        self.owner[p as usize] = owner;
        true
    }

    /// Walk the tree `root` names for `owner`, checking every page and passing each leaf entry
    /// to `entry`. Returns the tree's page count. A page claimed already is noted and not
    /// descended into.
    fn walk(
        &mut self,
        root: &TreeRoot,
        owner: Owner,
        mut entry: impl FnMut(&mut Self, &[u8], &[u8]) -> Result<()>,
    ) -> Result<u64> {
        root.check()?;
        if root.is_empty() {
            return Ok(0);
        }
        let snapshot = self.snapshot;
        let geometry = snapshot.geometry;
        let name = self.owners[owner as usize].clone();
        let corrupt =
            |p: u32, what: String| Error::Corrupt(format!("check: {name}: page {p} {what}"));
        let (mut pages, mut entries) = (0, 0);
        let mut stack = vec![(root.root, root.root_txn, 1, None::<Vec<u8>>, None::<Vec<u8>>)];
        while let Some((p, want_txn, depth, lo, hi)) = stack.pop() {
            if p < 2 || p >= snapshot.meta.page_count {
                return Err(corrupt(p, "is not a tree page".to_string()));
            }
            if !self.claim(p, owner) {
                continue;
            }
            pages += 1;
            let page = snapshot.page(p).map_err(|e| corrupt(p, e.to_string()))?;
            if crc::crc_state(page) != CrcState::Valid {
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
                if depth > 1 && n == 0 {
                    return Err(corrupt(p, "is an empty non-root leaf".to_string()));
                }
                entries += n as u64;
                for i in 0..n {
                    let (k, v) = page::leaf_entry(page, i)?;
                    entry(self, k, v).map_err(|e| corrupt(p, e.to_string()))?;
                }
                continue;
            }
            for i in 0..n {
                let (child, child_txn) = page::branch_child(page, i)?;
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
        if entries != root.entries && self.twice.is_empty() {
            return Err(Error::Corrupt(format!(
                "check: {name}: {entries} entries, not the {} its root counts",
                root.entries
            )));
        }
        Ok(pages)
    }
}

/// `Env::check` of one snapshot.
pub(crate) fn check_snapshot(snapshot: &Snapshot) -> Result<CheckReport> {
    let meta = snapshot.meta;
    let mut checker = Checker {
        snapshot,
        owner: vec![0; meta.page_count as usize],
        owners: ["", "catalog", "free tree", "free list"].map(String::from).to_vec(),
        twice: Vec::new(),
    };
    let mut report =
        CheckReport { txn_id: meta.txn, page_count: meta.page_count, ..Default::default() };

    let mut tables: Vec<(String, TableRecord)> = Vec::new();
    report.catalog_pages = checker.walk(&meta.catalog, CATALOG, |_, k, v| {
        let name = std::str::from_utf8(k)
            .map_err(|_| Error::Corrupt("a table name is not UTF-8".to_string()))?;
        catalog::check_name(name)
            .map_err(|_| Error::Corrupt(format!("bad table name {name:?}")))?;
        let record = TableRecord::decode(v)?;
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
        let pages = checker.walk(&record.root, owner, |_, _, _| Ok(()))?;
        report.table_pages += pages;
        report.tables.insert(
            name,
            TableCheck { entries: record.root.entries, pages, height: record.root.height },
        );
    }

    let (mut free_pages, mut free_records) = (0, 0);
    report.free_tree_pages = checker.walk(&meta.free, FREE_TREE, |checker, k, v| {
        let key = RecordKey::decode(k)?;
        if key.txn > meta.txn {
            return Err(Error::Corrupt(format!("record {key:?} is later than the state")));
        }
        free_records += 1;
        for p in decode_pages(v, meta.page_count)? {
            free_pages += 1;
            checker.claim(p, FREE_LIST);
        }
        Ok(())
    })?;
    report.free_pages = free_pages;
    report.free_records = free_records;

    let leaked: Vec<u32> =
        (2..meta.page_count).filter(|&p| checker.owner[p as usize] == 0).collect();
    if leaked.is_empty() && checker.twice.is_empty() {
        return Ok(report);
    }
    let twice: Vec<String> = checker
        .twice
        .iter()
        .map(|&(p, a, b)| {
            format!("{p} ({} and {})", checker.owners[a as usize], checker.owners[b as usize])
        })
        .collect();
    Err(Error::Corrupt(format!(
        "check: page accounting of txn {}: leaked {}; used twice [{}]",
        meta.txn,
        page_runs(&leaked),
        twice.join(", ")
    )))
}

/// `pages` (ascending) as compact runs: `[2, 5..8]`.
fn page_runs(pages: &[u32]) -> String {
    let mut runs: Vec<String> = Vec::new();
    let mut i = 0;
    while i < pages.len() {
        let mut j = i;
        while j + 1 < pages.len() && pages[j + 1] == pages[j] + 1 {
            j += 1;
        }
        runs.push(if i == j {
            pages[i].to_string()
        } else {
            format!("{}..{}", pages[i], pages[j] + 1)
        });
        i = j + 1;
    }
    format!("[{}]", runs.join(", "))
}
