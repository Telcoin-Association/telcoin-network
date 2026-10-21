//! The catalog: a B+tree keyed by table name (UTF-8, 1..=255 bytes) whose values are
//! [`TableRecord`]s, each naming one table's tree. The meta page names the catalog's root.
//!
//! Record layout (56 bytes, little-endian): `id u32 | flags u32 | root TreeRoot (32) |
//! created_txn u64 | reserved 8`.

use crate::{
    error::{Error, Result},
    tree::{read, write, PageRead, PageWrite, TreeRoot},
};

/// The longest table name, in bytes.
pub(crate) const MAX_NAME_LEN: usize = 255;

/// The size of an encoded [`TableRecord`].
pub(crate) const RECORD_LEN: usize = 56;

/// Where the record's [`TreeRoot`] lies.
const ROOT: std::ops::Range<usize> = 8..8 + TreeRoot::ENCODED_LEN;

/// Where the creating transaction's id lies.
const CREATED: std::ops::Range<usize> = ROOT.end..ROOT.end + 8;

/// One table: its id, its tree, and the transaction that created it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TableRecord {
    /// The table's id: unique over the file's life (a re-created table gets a new one), so a
    /// handle of a dropped table never names its successor.
    pub(crate) id: u32,
    /// Table flags (none are defined: zero).
    pub(crate) flags: u32,
    /// The table's tree.
    pub(crate) root: TreeRoot,
    /// The id of the transaction that created the table.
    pub(crate) created_txn: u64,
}

impl TableRecord {
    /// A new, empty table `id` created by transaction `txn`.
    pub(crate) fn new(id: u32, txn: u64) -> Self {
        Self { id, flags: 0, root: TreeRoot::EMPTY, created_txn: txn }
    }

    /// The 56-byte encoding.
    pub(crate) fn encode(&self) -> [u8; RECORD_LEN] {
        let mut out = [0; RECORD_LEN];
        out[0..4].copy_from_slice(&self.id.to_le_bytes());
        out[4..8].copy_from_slice(&self.flags.to_le_bytes());
        out[ROOT].copy_from_slice(&self.root.encode());
        out[CREATED].copy_from_slice(&self.created_txn.to_le_bytes());
        out
    }

    /// Decode [`Self::encode`]'s bytes: exactly [`RECORD_LEN`] of them, no flags, a zero reserved
    /// tail and a sound tree root; else [`Error::Corrupt`].
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let corrupt = |what: &str| Err(Error::Corrupt(format!("catalog record: {what}")));
        let Ok(bytes) = <&[u8; RECORD_LEN]>::try_from(bytes) else {
            return corrupt(&format!("{} bytes, not {RECORD_LEN}", bytes.len()));
        };
        let u32_at = |off: usize| u32::from_le_bytes(bytes[off..off + 4].try_into().expect("4"));
        let record = Self {
            id: u32_at(0),
            flags: u32_at(4),
            root: TreeRoot::decode(bytes[ROOT].try_into().expect("a tree root"))?,
            created_txn: u64::from_le_bytes(bytes[CREATED].try_into().expect("8 bytes")),
        };
        if record.flags != 0 {
            return corrupt(&format!("unknown flags {:#x}", record.flags));
        }
        if bytes[CREATED.end..].iter().any(|&b| b != 0) {
            return corrupt("reserved bytes are not zero");
        }
        Ok(record)
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

/// The record of table `name` in the catalog `catalog` names, if there is one.
pub(crate) fn get<S: PageRead + ?Sized>(
    store: &S,
    catalog: &TreeRoot,
    name: &str,
) -> Result<Option<TableRecord>> {
    check_name(name)?;
    read::find(store, catalog, name.as_bytes())?
        .map(|v| v.inline().and_then(TableRecord::decode))
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
            flags: 0,
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
        };
        let bytes = record.encode();
        assert_eq!(TableRecord::decode(&bytes).expect("decode"), record);
        assert_eq!(TableRecord::new(3, 4).root, TreeRoot::EMPTY);
        assert_eq!((ROOT, CREATED, RECORD_LEN), (8..40, 40..48, 56), "the layout");
        assert_eq!(bytes[ROOT], record.root.encode());
        assert_eq!(bytes[CREATED], (1_u64 << 33).to_le_bytes());

        for (what, at) in
            [("flags", 4), ("root branch count", 32), ("reserved", 48), ("reserved", 55)]
        {
            let mut bad = bytes;
            bad[at] ^= 1;
            assert!(matches!(TableRecord::decode(&bad), Err(Error::Corrupt(_))), "{what}");
        }
        assert!(matches!(TableRecord::decode(&bytes[..55]), Err(Error::Corrupt(_))));
        assert!(matches!(TableRecord::decode(&[0; 57]), Err(Error::Corrupt(_))));
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
