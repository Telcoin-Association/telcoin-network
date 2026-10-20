//! The catalog: a B+tree keyed by table name (UTF-8, 1..=255 bytes) whose values are
//! [`TableRecord`]s, each naming one table's tree. The meta page names the catalog's root.
//!
//! Record layout (48 bytes, little-endian): `id u32 | flags u32 | root TreeRoot (24) |
//! created_txn u64 | reserved 8`.

use crate::{
    error::{Error, Result},
    tree::{read, write, PageRead, PageWrite, TreeRoot},
};

/// The longest table name, in bytes.
pub(crate) const MAX_NAME_LEN: usize = 255;

/// The size of an encoded [`TableRecord`].
pub(crate) const RECORD_LEN: usize = 48;

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

    /// The 48-byte encoding.
    pub(crate) fn encode(&self) -> [u8; RECORD_LEN] {
        let mut out = [0; RECORD_LEN];
        out[0..4].copy_from_slice(&self.id.to_le_bytes());
        out[4..8].copy_from_slice(&self.flags.to_le_bytes());
        out[8..32].copy_from_slice(&self.root.encode());
        out[32..40].copy_from_slice(&self.created_txn.to_le_bytes());
        out
    }

    /// Decode [`Self::encode`]'s bytes: exactly 48 of them, no flags, a zero reserved tail and a
    /// sound tree root; else [`Error::Corrupt`].
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let corrupt = |what: &str| Err(Error::Corrupt(format!("catalog record: {what}")));
        let Ok(bytes) = <&[u8; RECORD_LEN]>::try_from(bytes) else {
            return corrupt(&format!("{} bytes, not {RECORD_LEN}", bytes.len()));
        };
        let u32_at = |off: usize| u32::from_le_bytes(bytes[off..off + 4].try_into().expect("4"));
        let record = Self {
            id: u32_at(0),
            flags: u32_at(4),
            root: TreeRoot::decode(bytes[8..32].try_into().expect("24 bytes"))?,
            created_txn: u64::from_le_bytes(bytes[32..40].try_into().expect("8 bytes")),
        };
        if record.flags != 0 {
            return corrupt(&format!("unknown flags {:#x}", record.flags));
        }
        if bytes[40..].iter().any(|&b| b != 0) {
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
    read::get(store, catalog, name.as_bytes())?.map(TableRecord::decode).transpose()
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
            root: TreeRoot { root: 12, root_txn: 7, height: 2, entries: 300 },
            created_txn: 1 << 33,
        };
        let bytes = record.encode();
        assert_eq!(TableRecord::decode(&bytes).expect("decode"), record);
        assert_eq!(TableRecord::new(3, 4).root, TreeRoot::EMPTY);

        for (what, at) in [("flags", 4), ("root reserved word", 20), ("reserved", 47)] {
            let mut bad = bytes;
            bad[at] ^= 1;
            assert!(matches!(TableRecord::decode(&bad), Err(Error::Corrupt(_))), "{what}");
        }
        assert!(matches!(TableRecord::decode(&bytes[..47]), Err(Error::Corrupt(_))));
        assert!(matches!(TableRecord::decode(&[0; 49]), Err(Error::Corrupt(_))));
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

        let changed =
            TableRecord { root: TreeRoot { root: 2, root_txn: 1, height: 1, entries: 1 }, ..a };
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
