//! Dup-sort tables as composite keys: the row layout of each dup-sort table this crate stores.
//!
//! MDBX holds several values under one key of a dup-sort table, sorted by value. Each of reth's
//! dup-sort values starts with its subkey (`DupSort::SubKey`, encoded), so here each value is a row
//! of its own: the stored key is the encoded key followed by the value's first `subkey` bytes,
//! and the stored value is the rest. Every one of these tables' keys encodes to a fixed width, so
//! the stored keys sort by key, then by subkey, as MDBX sorts a key's values by their leading
//! bytes.
//!
//! MDBX's cursor on such a table is two cursors, one over the keys and one over a key's values,
//! and the inner one can end a key's values (after `last`, or after a delete of a key's last value
//! that leaves it earlier ones) where a cursor over these rows would be on the next key's first.
//! The cursors keep that end as a state of their own (`Pos::DupEnd` in `crate::cursor`), stepping
//! past it with [`DupLayout::past`].

use reth_db_api::{
    table::Table,
    tables::{
        AccountChangeSets, HashedStorages, PlainStorageState, StorageChangeSets, StoragesTrie,
    },
};
use tndb_b::KeyTail;

/// How a dup-sort table's rows are stored: the stored key is `key ‖ value[..subkey]` (the
/// compressed value), the stored value `value[subkey..]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DupLayout {
    /// The width of the table's encoded key.
    pub(crate) key: usize,
    /// The width of the subkey: the encoded `DupSort::SubKey`, which every compressed value of the
    /// table starts with.
    pub(crate) subkey: usize,
}

/// The dup-sort tables this crate stores, with their layouts. `dup_layouts_match_reths_encodings`
/// checks each width against reth's encodings.
pub(crate) const DUP_LAYOUTS: [(&str, DupLayout); 5] = [
    // `Address` → `StorageEntry` (a `B256` slot first).
    (PlainStorageState::NAME, DupLayout { key: 20, subkey: 32 }),
    // `BlockNumberAddress` (8 + 20 bytes) → `StorageEntry`.
    (StorageChangeSets::NAME, DupLayout { key: 28, subkey: 32 }),
    // `B256` → `StorageEntry`.
    (HashedStorages::NAME, DupLayout { key: 32, subkey: 32 }),
    // `BlockNumber` → `AccountBeforeTx` (an `Address` first).
    (AccountChangeSets::NAME, DupLayout { key: 8, subkey: 20 }),
    // `B256` → `StorageTrieEntry` (a 65-byte `StoredNibblesSubKey` first).
    (StoragesTrie::NAME, DupLayout { key: 32, subkey: 65 }),
];

/// The layout of dup-sort table `name`, if this crate stores it.
pub(crate) fn dup_layout(name: &str) -> Option<DupLayout> {
    DUP_LAYOUTS.iter().find(|(table, _)| *table == name).map(|&(_, layout)| layout)
}

impl DupLayout {
    /// The row of `key` holding the compressed `value`: its stored key, written into `stored`, and
    /// its stored value, returned. `None` if `key` is not the table's key width or `value` is
    /// shorter than the subkey.
    pub(crate) fn join<'v>(
        &self,
        key: &[u8],
        value: &'v [u8],
        stored: &mut Vec<u8>,
    ) -> Option<&'v [u8]> {
        if key.len() != self.key || value.len() < self.subkey {
            return None;
        }
        let (subkey, rest) = value.split_at(self.subkey);
        stored.clear();
        stored.extend_from_slice(key);
        stored.extend_from_slice(subkey);
        Some(rest)
    }

    /// A stored key's key and subkey; `None` if it is not `key + subkey` bytes long.
    pub(crate) fn split<'k>(&self, stored: &'k [u8]) -> Option<(&'k [u8], &'k [u8])> {
        (stored.len() == self.key + self.subkey).then(|| stored.split_at(self.key))
    }

    /// Whether the stored key `stored` is a row of `key`.
    pub(crate) fn is_row_of(&self, stored: &[u8], key: &[u8]) -> bool {
        self.split(stored).is_some_and(|(k, _)| k == key)
    }

    /// A key between the rows of the stored key's key and those of the next key:
    /// `key ‖ 0xFF × (subkey + 1)`, which sorts after `key ‖ s` for every subkey `s`, and before
    /// the rows of any greater key since all keys have one width.
    pub(crate) fn past(&self, stored: &[u8]) -> Vec<u8> {
        let key = &stored[..self.key.min(stored.len())];
        let mut past = Vec::with_capacity(key.len() + self.subkey + 1);
        past.extend_from_slice(key);
        past.resize(key.len() + self.subkey + 1, 0xFF);
        past
    }

    /// The full value of a row read with its stored key's last `subkey` bytes joined to its
    /// stored value (the engine's `current_with_key_tail`), `subkey ‖ rest`: for an inline value
    /// the slice of the page itself, no copy; for a value in an overflow run rebuilt into `value`
    /// ([`Self::rebuild`]). `None` if `stored` is not a row of this layout.
    pub(crate) fn joined<'v>(
        &self,
        stored: &[u8],
        tail: KeyTail<'v>,
        value: &'v mut Vec<u8>,
    ) -> Option<&'v [u8]> {
        match tail {
            KeyTail::Joined(joined) => self.split(stored).map(|_| joined),
            KeyTail::Apart(rest) => self.rebuild(stored, rest, value),
        }
    }

    /// The full value of a row, `subkey ‖ rest`, rebuilt into `value`; `None` if `stored` is not a
    /// row of this layout.
    pub(crate) fn rebuild<'v>(
        &self,
        stored: &[u8],
        rest: &[u8],
        value: &'v mut Vec<u8>,
    ) -> Option<&'v [u8]> {
        let (_, subkey) = self.split(stored)?;
        value.clear();
        value.extend_from_slice(subkey);
        value.extend_from_slice(rest);
        Some(value)
    }
}
