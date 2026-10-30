//! The catalog: a B+tree keyed by table name (UTF-8, 1..=255 bytes) whose values are
//! [`TableRecord`]s, each naming one table's tree. The meta page names the catalog's root.
//!
//! Record layout (96 bytes, little-endian):
//!
//! | off | size | field |
//! |---|---|---|
//! | 0 | 4 | id |
//! | 4 | 4 | flags: bit 0 [`FLAG_DUP`], every other bit zero |
//! | 8 | 32 | root [`TreeRoot`] (a dup table's outer tree: its own pages only) |
//! | 40 | 2 | key_len (dup: the outer key width; plain: 0) |
//! | 42 | 2 | packed_max (dup: the packed-set limit, 0 = never packed; plain: 0) |
//! | 44 | 4 | nested_trees (dup; plain: 0) |
//! | 48 | 8 | nested_entries (dup: the rows in nested trees; plain: 0) |
//! | 56 | 8 | packed_rows (dup: the rows in packed sets; plain: 0) |
//! | 64 | 4 | nested_branch_pages (dup; plain: 0) |
//! | 68 | 4 | nested_leaf_pages (dup; plain: 0) |
//! | 72 | 4 | nested_overflow_pages (dup; plain: 0) |
//! | 76 | 4 | reserved (zero) |
//! | 80 | 8 | created_txn |
//! | 88 | 8 | reserved (zero) |
//!
//! The record ends as version 2's 56-byte record did, `created_txn` and then 8 reserved bytes, so
//! the bytes at a given distance from a record's end mean what they meant there.
//!
//! The codec reads and writes both kinds, but no table is a dup table yet: every use of a record
//! takes a plain one only ([`TableRecord::plain`]), so a dup record is damage.

use crate::{
    error::{Error, Result},
    tree::{read, write, PageRead, PageWrite, TreeRoot},
};

/// The longest table name, in bytes.
pub(crate) const MAX_NAME_LEN: usize = 255;

/// The size of an encoded [`TableRecord`].
pub(crate) const RECORD_LEN: usize = 96;

/// The record flag of a dup table ([`DupRecord`]).
pub(crate) const FLAG_DUP: u32 = 1;

/// Where the record's [`TreeRoot`] lies.
const ROOT: std::ops::Range<usize> = 8..8 + TreeRoot::ENCODED_LEN;

/// Where the creating transaction's id lies.
const CREATED: std::ops::Range<usize> = 80..88;

/// The reserved bytes, zero in every record.
const RESERVED: [std::ops::Range<usize>; 2] = [76..80, CREATED.end..RECORD_LEN];

/// One table: its id, its tree, the transaction that created it, and for a dup table its
/// [`DupRecord`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TableRecord {
    /// The table's id: unique over the file's life (a re-created table gets a new one), so a
    /// handle of a dropped table never names its successor.
    pub(crate) id: u32,
    /// The table's tree (a dup table's outer tree).
    pub(crate) root: TreeRoot,
    /// The id of the transaction that created the table.
    pub(crate) created_txn: u64,
    /// A dup table's shape and nested totals; `None` for a plain table (flags 0).
    pub(crate) dup: Option<DupRecord>,
}

/// The fields of a dup table's record ([`FLAG_DUP`]): its outer key width, its packed-set limit
/// and the totals of the trees nested in its outer cells, which its outer [`TreeRoot`] does not
/// count.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DupRecord {
    /// The width of every outer key, at least 1.
    pub(crate) key_len: u16,
    /// The most rows a packed set holds; 0: the table never packs, every outer cell is nested.
    pub(crate) packed_max: u16,
    /// The rows in nested trees.
    pub(crate) nested_entries: u64,
    /// The rows in packed sets.
    pub(crate) packed_rows: u64,
    /// The nested trees: one per nested outer cell.
    pub(crate) nested_trees: u32,
    /// The nested trees' branch pages.
    pub(crate) nested_branch_pages: u32,
    /// The nested trees' leaf pages.
    pub(crate) nested_leaf_pages: u32,
    /// The nested trees' overflow pages.
    pub(crate) nested_overflow_pages: u32,
}

impl DupRecord {
    /// The record rules a dup table's fields obey against its outer tree `root`, else the broken
    /// rule: an outer key of at least 1 byte; at most one nested tree per outer entry; every
    /// nested tree non-empty, so with a leaf and an entry of its own, and none of them without a
    /// nested tree; with no packing every outer cell nested and no packed row, and with packing a
    /// row in each outer cell that is not nested.
    fn broken_rule(&self, root: &TreeRoot) -> Option<&'static str> {
        let trees = u64::from(self.nested_trees);
        let pages = u64::from(self.nested_branch_pages)
            + u64::from(self.nested_leaf_pages)
            + u64::from(self.nested_overflow_pages);
        if self.key_len == 0 {
            Some("a dup table with an empty outer key")
        } else if trees > root.entries {
            Some("more nested trees than outer entries")
        } else if trees == 0 && (self.nested_entries != 0 || pages != 0) {
            Some("nested rows or pages without a nested tree")
        } else if u64::from(self.nested_leaf_pages) < trees || self.nested_entries < trees {
            Some("a nested tree without a leaf or a row")
        } else if self.packed_max == 0 && (self.packed_rows != 0 || trees != root.entries) {
            Some("packed rows or an unnested outer entry in a table that never packs")
        } else if self.packed_max != 0 && self.packed_rows < root.entries - trees {
            Some("fewer packed rows than packed sets")
        } else {
            None
        }
    }
}

impl TableRecord {
    /// A new, empty plain table `id` created by transaction `txn`.
    pub(crate) fn new(id: u32, txn: u64) -> Self {
        Self { id, root: TreeRoot::EMPTY, created_txn: txn, dup: None }
    }

    /// The record of a plain table; a dup table's is [`Error::Corrupt`]: no table is a dup table
    /// yet, so every use of a record (a table's resolution, `Env::check`) takes this.
    pub(crate) fn plain(&self) -> Result<&Self> {
        match self.dup {
            None => Ok(self),
            Some(_) => Err(Error::Corrupt(format!(
                "catalog record: a dup table (id {}), unsupported",
                self.id
            ))),
        }
    }

    /// The 96-byte encoding.
    pub(crate) fn encode(&self) -> [u8; RECORD_LEN] {
        let mut out = [0; RECORD_LEN];
        let flags = if self.dup.is_some() { FLAG_DUP } else { 0 };
        out[0..4].copy_from_slice(&self.id.to_le_bytes());
        out[4..8].copy_from_slice(&flags.to_le_bytes());
        out[ROOT].copy_from_slice(&self.root.encode());
        out[CREATED].copy_from_slice(&self.created_txn.to_le_bytes());
        let d = self.dup.unwrap_or_default();
        out[40..42].copy_from_slice(&d.key_len.to_le_bytes());
        out[42..44].copy_from_slice(&d.packed_max.to_le_bytes());
        out[44..48].copy_from_slice(&d.nested_trees.to_le_bytes());
        out[48..56].copy_from_slice(&d.nested_entries.to_le_bytes());
        out[56..64].copy_from_slice(&d.packed_rows.to_le_bytes());
        out[64..68].copy_from_slice(&d.nested_branch_pages.to_le_bytes());
        out[68..72].copy_from_slice(&d.nested_leaf_pages.to_le_bytes());
        out[72..76].copy_from_slice(&d.nested_overflow_pages.to_le_bytes());
        out
    }

    /// Decode [`Self::encode`]'s bytes: exactly [`RECORD_LEN`] of them, a sound tree root, no
    /// flag but [`FLAG_DUP`], zero reserved bytes, and either a plain record (every dup field
    /// zero) or a dup one obeying [`DupRecord`]'s rules; else [`Error::Corrupt`].
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let corrupt = |what: &str| Err(Error::Corrupt(format!("catalog record: {what}")));
        let Ok(bytes) = <&[u8; RECORD_LEN]>::try_from(bytes) else {
            return corrupt(&format!("{} bytes, not {RECORD_LEN}", bytes.len()));
        };
        let u16_at = |off: usize| u16::from_le_bytes(bytes[off..off + 2].try_into().expect("2"));
        let u32_at = |off: usize| u32::from_le_bytes(bytes[off..off + 4].try_into().expect("4"));
        let u64_at = |off: usize| u64::from_le_bytes(bytes[off..off + 8].try_into().expect("8"));
        let (id, flags) = (u32_at(0), u32_at(4));
        let root = TreeRoot::decode(bytes[ROOT].try_into().expect("a tree root"))?;
        if flags & !FLAG_DUP != 0 {
            return corrupt(&format!("unknown flags {flags:#x}"));
        }
        if RESERVED.iter().any(|r| bytes[r.clone()].iter().any(|&b| b != 0)) {
            return corrupt("reserved bytes are not zero");
        }
        let d = DupRecord {
            key_len: u16_at(40),
            packed_max: u16_at(42),
            nested_trees: u32_at(44),
            nested_entries: u64_at(48),
            packed_rows: u64_at(56),
            nested_branch_pages: u32_at(64),
            nested_leaf_pages: u32_at(68),
            nested_overflow_pages: u32_at(72),
        };
        let dup = if flags & FLAG_DUP == 0 {
            if d != DupRecord::default() {
                return corrupt("a plain table with dup fields");
            }
            None
        } else {
            if let Some(rule) = d.broken_rule(&root) {
                return corrupt(rule);
            }
            Some(d)
        };
        Ok(Self { id, root, created_txn: u64_at(CREATED.start), dup })
    }
}

/// Check a table name: 1..=255 bytes, else [`Error::InvalidTableName`].
pub(crate) fn check_name(name: &str) -> Result<()> {
    if (1..=MAX_NAME_LEN).contains(&name.len()) {
        Ok(())
    } else {
        Err(Error::InvalidTableName)
    }
}

/// The record of table `name` in the catalog `catalog` names, if there is one: the engine's one
/// read of a record, so a dup record is refused here for every use ([`TableRecord::plain`]).
pub(crate) fn get<S: PageRead + ?Sized>(
    store: &S,
    catalog: &TreeRoot,
    name: &str,
) -> Result<Option<TableRecord>> {
    check_name(name)?;
    read::find(store, catalog, name.as_bytes())?
        .map(|v| v.inline().and_then(TableRecord::decode).and_then(|r| r.plain().copied()))
        .transpose()
}

/// Insert or replace table `name`'s record. `trusted` is the catalog root's provenance
/// ([`write::put_sized`]).
pub(crate) fn put<S: PageWrite + ?Sized>(
    store: &mut S,
    catalog: &mut TreeRoot,
    trusted: &mut bool,
    name: &str,
    record: &TableRecord,
) -> Result<()> {
    check_name(name)?;
    write::put(store, catalog, trusted, name.as_bytes(), &record.encode())?;
    Ok(())
}

/// Remove table `name`'s record, returning whether there was one. `trusted` is the catalog
/// root's provenance ([`write::put_sized`]).
pub(crate) fn delete<S: PageWrite + ?Sized>(
    store: &mut S,
    catalog: &mut TreeRoot,
    trusted: &mut bool,
    name: &str,
) -> Result<bool> {
    check_name(name)?;
    write::delete(store, catalog, trusted, name.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::test_harness::MemStore;

    /// The record round-trips, and damage is `Corrupt`.
    #[test]
    fn table_record_encoding() {
        let record = TableRecord {
            id: 9,
            root: TreeRoot {
                root: 12,
                root_txn: 7,
                height: 2,
                entries: 300,
                branch_pages: 1,
                leaf_pages: 20,
                overflow_pages: 5,
            },
            created_txn: 1 << 33,
            dup: None,
        };
        let bytes = record.encode();
        assert_eq!(TableRecord::decode(&bytes).expect("decode"), record);
        assert_eq!(TableRecord::new(3, 4).root, TreeRoot::EMPTY);
        assert_eq!(TableRecord::new(3, 4).dup, None);
        assert_eq!((ROOT, CREATED, RECORD_LEN), (8..40, 80..88, 96), "the layout");
        assert_eq!(RESERVED, [76..80, 88..96], "the layout");
        assert_eq!(bytes[ROOT], record.root.encode());
        assert_eq!(bytes[CREATED], (1_u64 << 33).to_le_bytes());
        assert_eq!(bytes[4..8], [0; 4], "a plain record's flags");
        assert_eq!(bytes[40..80], [0; 40], "a plain record's dup fields and reserved bytes");
        assert_eq!(bytes[88..], [0; 8], "a plain record's reserved tail");
        assert_eq!(record.plain().expect("plain"), &record);

        let mut flips = vec![("flags", 4), ("flag bit 1", 4), ("root branch count", 32)];
        flips.extend((40..80).chain(88..RECORD_LEN).map(|at| ("a dup field or reserved", at)));
        for (what, at) in flips {
            let mut bad = bytes;
            // The dup flag alone leaves an empty outer key; a dup field set makes a plain
            // record with dup fields.
            bad[at] ^= if what == "flag bit 1" { 2 } else { 1 };
            assert!(matches!(TableRecord::decode(&bad), Err(Error::Corrupt(_))), "{what} {at}");
        }
        assert!(matches!(TableRecord::decode(&bytes[..95]), Err(Error::Corrupt(_))));
        assert!(matches!(TableRecord::decode(&[0; 97]), Err(Error::Corrupt(_))));
        assert!(matches!(TableRecord::decode(&[0; 56]), Err(Error::Corrupt(_))), "version 2's");
    }

    /// A dup record round-trips with each field at its offset, is refused by every use
    /// ([`TableRecord::plain`]), and breaking each of its rules, or a reserved byte, is `Corrupt`.
    #[test]
    fn dup_records_round_trip_and_refuse_damage() {
        let outer = TreeRoot {
            root: 12,
            root_txn: 7,
            height: 1,
            entries: 5,
            branch_pages: 0,
            leaf_pages: 1,
            overflow_pages: 0,
        };
        let nested = DupRecord {
            key_len: 32,
            packed_max: 0,
            nested_entries: 900,
            packed_rows: 0,
            nested_trees: 5,
            nested_branch_pages: 3,
            nested_leaf_pages: 40,
            nested_overflow_pages: 6,
        };
        let record = TableRecord { id: 4, root: outer, created_txn: 3, dup: Some(nested) };
        let bytes = record.encode();
        assert_eq!(TableRecord::decode(&bytes).expect("decode"), record);
        assert_eq!(bytes[4..8], FLAG_DUP.to_le_bytes());
        let fields: [(std::ops::Range<usize>, u64); 9] = [
            (40..42, 32),
            (42..44, 0),
            (44..48, 5),
            (48..56, 900),
            (56..64, 0),
            (64..68, 3),
            (68..72, 40),
            (72..76, 6),
            (80..88, 3),
        ];
        for (at, want) in fields {
            let mut field = [0; 8];
            field[..at.len()].copy_from_slice(&bytes[at.clone()]);
            assert_eq!(u64::from_le_bytes(field), want, "the field at {at:?}");
        }
        assert!(RESERVED.iter().all(|r| bytes[r.clone()].iter().all(|&b| b == 0)));
        assert!(
            matches!(record.plain(), Err(Error::Corrupt(m)) if m.contains("a dup table (id 4)")),
            "every use refuses a dup record"
        );

        // A packing table: three outer cells nested, two packed sets.
        let packed = DupRecord { packed_max: 8, packed_rows: 2, nested_trees: 3, ..nested };
        let ok = TableRecord { dup: Some(packed), ..record };
        assert_eq!(TableRecord::decode(&ok.encode()).expect("packing"), ok);
        // An empty dup table.
        let empty = TableRecord {
            root: TreeRoot::EMPTY,
            dup: Some(DupRecord { key_len: 1, ..Default::default() }),
            ..record
        };
        assert_eq!(TableRecord::decode(&empty.encode()).expect("empty"), empty);

        let broken = |what: &str, outer: TreeRoot, d: DupRecord| {
            let bad = TableRecord { root: outer, dup: Some(d), ..record }.encode();
            assert!(
                matches!(TableRecord::decode(&bad), Err(Error::Corrupt(m)) if m.contains(what)),
                "{what}: {d:?}"
            );
        };
        broken("empty outer key", outer, DupRecord { key_len: 0, ..nested });
        broken("more nested trees", outer, DupRecord { nested_trees: 6, ..packed });
        let none = DupRecord { nested_trees: 0, packed_max: 8, packed_rows: 5, ..nested };
        broken("without a nested tree", outer, DupRecord { nested_leaf_pages: 0, ..none });
        broken(
            "without a nested tree",
            outer,
            DupRecord { nested_entries: 0, nested_leaf_pages: 0, nested_branch_pages: 0, ..none },
        );
        let no_pages = DupRecord {
            nested_branch_pages: 0,
            nested_leaf_pages: 0,
            nested_overflow_pages: 0,
            ..none
        };
        broken("without a nested tree", outer, DupRecord { nested_entries: 1, ..no_pages });
        broken("without a leaf or a row", outer, DupRecord { nested_leaf_pages: 4, ..nested });
        broken("without a leaf or a row", outer, DupRecord { nested_entries: 4, ..nested });
        broken("never packs", outer, DupRecord { packed_rows: 1, ..nested });
        broken("never packs", outer, DupRecord { nested_trees: 4, ..nested });
        broken("fewer packed rows", outer, DupRecord { packed_rows: 1, ..packed });
        let sound = DupRecord {
            nested_entries: 0,
            nested_leaf_pages: 0,
            nested_branch_pages: 0,
            nested_overflow_pages: 0,
            ..none
        };
        assert!(TableRecord::decode(&TableRecord { dup: Some(sound), ..record }.encode()).is_ok());
        for at in [76, 79, 88, 95] {
            let mut bad = bytes;
            bad[at] = 1;
            assert!(
                matches!(TableRecord::decode(&bad), Err(Error::Corrupt(m)) if m.contains("reserved")),
                "reserved byte {at}"
            );
        }
        let mut bad = bytes;
        bad[4] |= 2;
        assert!(
            matches!(TableRecord::decode(&bad), Err(Error::Corrupt(m)) if m.contains("unknown flags"))
        );
    }

    /// Names of 1..=255 bytes are tables' keys; others are refused before the tree is touched.
    #[test]
    fn catalog_ops_and_names() {
        let mut store = MemStore::new(4096);
        let mut catalog = TreeRoot::EMPTY;
        let mut trusted = false;
        let longest = "n".repeat(MAX_NAME_LEN);
        let a = TableRecord::new(1, 1);
        let b = TableRecord::new(2, 1);
        put(&mut store, &mut catalog, &mut trusted, "a", &a).expect("put a");
        put(&mut store, &mut catalog, &mut trusted, &longest, &b).expect("put longest");
        assert_eq!(get(&store, &catalog, "a").expect("get"), Some(a));
        assert_eq!(get(&store, &catalog, &longest).expect("get"), Some(b));
        assert_eq!(get(&store, &catalog, "b").expect("get"), None);

        let root = TreeRoot {
            root: 2,
            root_txn: 1,
            height: 1,
            entries: 1,
            leaf_pages: 1,
            ..TreeRoot::EMPTY
        };
        let changed = TableRecord { root, ..a };
        put(&mut store, &mut catalog, &mut trusted, "a", &changed).expect("replace");
        assert_eq!(get(&store, &catalog, "a").expect("get"), Some(changed));
        assert_eq!(catalog.entries, 2);
        assert!(delete(&mut store, &mut catalog, &mut trusted, "a").expect("delete"));
        assert!(!delete(&mut store, &mut catalog, &mut trusted, "a").expect("delete again"));
        assert_eq!(get(&store, &catalog, "a").expect("get"), None);

        let too_long = "n".repeat(MAX_NAME_LEN + 1);
        let before = catalog;
        for bad in ["", too_long.as_str()] {
            assert!(matches!(get(&store, &catalog, bad), Err(Error::InvalidTableName)));
            assert!(matches!(
                put(&mut store, &mut catalog, &mut trusted, bad, &a),
                Err(Error::InvalidTableName)
            ));
            assert!(matches!(
                delete(&mut store, &mut catalog, &mut trusted, bad),
                Err(Error::InvalidTableName)
            ));
        }
        assert_eq!(catalog, before);
    }
}
