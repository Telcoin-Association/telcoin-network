//! The free list: a B+tree (the free tree, named by the meta) of the pages committed transactions
//! freed, LMDB style.
//!
//! A record's key is `txn u64 BE | seq u32 BE` (the committing transaction and a sequence number,
//! so records sort by txn) and its value the page numbers, strictly ascending, as `u32` LE (at
//! most [`record_capacity`] of them; an empty value is allowed).
//!
//! **Loading.** A write transaction reuses a page only once no durable meta and no live reader
//! can reach it. Every page a record of txn `M` lists is unreachable from the states `M`
//! onwards, so the writer takes records in key order ([`next_record`]), smallest first, while
//! their txn is no later than the reuse gate: the lower txn of the two meta slots and the oldest
//! snapshot a reader may hold (`Writer::reuse_gate`). In `SyncMode::Durable` a page freed by txn
//! `N` is thus reusable from txn `N + 2`; in `SyncMode::NoSync` the slots move only at a sync,
//! so it comes back once two syncs have made states from `N` on durable in both slots. A record's
//! pages are checked before any is handed out ([`decode_pages`]): the leaf holding it passes its
//! CRC and structure checks, and each page is a tree page of the state (`2 <= p < page_count`),
//! strictly ascending, and not already taken from the list by this transaction. The free tree
//! itself is not changed while the transaction runs.
//!
//! **Saving** at commit ([`save`]), with loading stopped (pages come from the pool or the end of
//! the file): the records taken are deleted, then the pages to list — those the transaction
//! freed and those left in its pool — are written as records `(M, 0..k)` of the committing txn
//! `M`. Writing them can take pages from the pool and free pages of the free tree, which changes
//! the list, so it is written again until it lists exactly what it should: a fixed point,
//! reached in at most three rounds. Keys are never deleted inside the loop (a record no longer
//! needed is left empty), so a round never frees a page a later round must list.

use crate::{
    crc::{self, CrcState},
    env::Writer,
    error::{Error, Result},
    page::{self, Geometry, Search},
    tree::{read::descend, write, PageRead, TreeRoot},
};

/// The size of a record key.
pub(crate) const KEY_LEN: usize = 12;

/// The most rounds [`save`] may take before it gives up (a broken invariant: three suffice).
const MAX_ROUNDS: u32 = 8;

/// A free record's key: the transaction that wrote it and a sequence number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RecordKey {
    /// The committing transaction: every page listed is unreachable from its state on.
    pub(crate) txn: u64,
    /// The record's place among its transaction's records.
    pub(crate) seq: u32,
}

impl RecordKey {
    /// The 12-byte big-endian encoding (byte order is key order).
    pub(crate) fn encode(&self) -> [u8; KEY_LEN] {
        let mut out = [0; KEY_LEN];
        out[..8].copy_from_slice(&self.txn.to_be_bytes());
        out[8..].copy_from_slice(&self.seq.to_be_bytes());
        out
    }

    /// Decode [`Self::encode`]'s bytes; any other length is [`Error::Corrupt`].
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let Ok(bytes) = <&[u8; KEY_LEN]>::try_from(bytes) else {
            return Err(Error::Corrupt(format!(
                "free list: a {}-byte record key, not {KEY_LEN}",
                bytes.len()
            )));
        };
        Ok(Self {
            txn: u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes")),
            seq: u32::from_be_bytes(bytes[8..].try_into().expect("4 bytes")),
        })
    }

    /// The least key after this one, if there is one.
    fn successor(&self) -> Option<Self> {
        match self.seq.checked_add(1) {
            Some(seq) => Some(Self { txn: self.txn, seq }),
            None => self.txn.checked_add(1).map(|txn| Self { txn, seq: 0 }),
        }
    }
}

/// The most pages one record lists at this page size (its value fills an entry with a
/// [`KEY_LEN`]-byte key, rounded down to whole page numbers).
pub(crate) fn record_capacity(geometry: &Geometry) -> usize {
    geometry.max_value(KEY_LEN) / 4
}

/// The first record of the free tree `root` names after the key `after` (from the smallest
/// with `None`), with its value borrowed from the store. Every page on the way to it passes its
/// CRC and structure checks and holds keys inside its ancestors' separators: the record decides
/// which pages are handed out, so damage at rest must not reach the allocator. A leaf that ends
/// before the next key is left through its upper separator: the next descent starts there.
pub(crate) fn next_record<'s, S: PageRead + ?Sized>(
    store: &'s S,
    root: &TreeRoot,
    after: Option<RecordKey>,
) -> Result<Option<(RecordKey, &'s [u8])>> {
    let start = match after {
        None => RecordKey { txn: 0, seq: 0 },
        Some(key) => match key.successor() {
            Some(next) => next,
            None => return Ok(None),
        },
    };
    let geometry = store.geometry();
    let mut probe = start.encode().to_vec();
    loop {
        let verify = |pgno: u32, page: &[u8], lo: Option<&[u8]>, hi: Option<&[u8]>| {
            if crc::crc_state(page) != CrcState::Valid {
                return Err(Error::Corrupt(format!("free list: page {pgno} fails its CRC")));
            }
            page::validate(page, &geometry)?;
            if !page::keys_in_range(page, lo, hi)? {
                return Err(Error::Corrupt(format!(
                    "free list: page {pgno} holds keys outside its parents' separators"
                )));
            }
            Ok(())
        };
        let Some(leaf) = descend(store, root, &probe, verify, None)? else {
            return Ok(None);
        };
        let (Search::Found(i) | Search::NotFound(i)) = page::leaf_search(leaf.page, &probe)?;
        if i < page::nkeys(leaf.page) {
            let (key, value) = page::leaf_entry(leaf.page, i)?;
            return Ok(Some((RecordKey::decode(key)?, value)));
        }
        match leaf.hi {
            None => return Ok(None),
            Some(hi) if hi > &probe[..] => probe = hi.to_vec(),
            Some(_) => {
                return Err(Error::Corrupt(format!(
                    "free list: leaf {} ends at a separator not past its keys",
                    leaf.pgno
                )))
            }
        }
    }
}

/// The pages a record's `value` lists: whole `u32`s, each a tree page of a state of
/// `page_count` pages (`2 <= p < page_count`), strictly ascending; else [`Error::Corrupt`].
pub(crate) fn decode_pages(value: &[u8], page_count: u32) -> Result<Vec<u32>> {
    if !value.len().is_multiple_of(4) {
        return Err(Error::Corrupt(format!(
            "free list: a {}-byte record is not whole page numbers",
            value.len()
        )));
    }
    let mut pages = Vec::with_capacity(value.len() / 4);
    for chunk in value.chunks_exact(4) {
        let p = u32::from_le_bytes(chunk.try_into().expect("4 bytes"));
        if p < 2 || p >= page_count {
            return Err(Error::Corrupt(format!(
                "free list: page {p} is not a tree page of a file of {page_count} pages"
            )));
        }
        if pages.last().is_some_and(|&last| p <= last) {
            return Err(Error::Corrupt(format!("free list: page {p} is out of order")));
        }
        pages.push(p);
    }
    Ok(pages)
}

/// The value of a record listing `pages` (ascending).
pub(crate) fn encode_pages(pages: &[u32]) -> Vec<u8> {
    pages.iter().flat_map(|p| p.to_le_bytes()).collect()
}

/// Save the free list at commit (see the module docs) into the working free tree `free`, as
/// records of the committing transaction: delete the records the transaction took, then write
/// the pages it freed and those left in its pool until the records list exactly them. Returns the
/// number of write rounds. Afterwards the pool and the freed pages are empty: the tree lists
/// them. Loading stays stopped until the next transaction begins.
pub(crate) fn save(writer: &mut Writer, free: &mut TreeRoot) -> Result<u32> {
    writer.alloc.stop_loading();
    for key in writer.alloc.take_consumed() {
        if !write::delete(writer, free, &key.encode())? {
            return Err(Error::Failed(format!("free list: the record {key:?} taken has vanished")));
        }
    }
    let txn = writer.alloc.txn();
    let capacity = record_capacity(&writer.geometry);
    let mut written: Vec<Vec<u8>> = Vec::new();
    for round in 0..=MAX_ROUNDS {
        let mut want: Vec<Vec<u8>> =
            writer.alloc.reusable()?.chunks(capacity).map(encode_pages).collect();
        if want.len() < written.len() {
            want.resize(written.len(), Vec::new());
        }
        if want == written {
            writer.alloc.saved();
            return Ok(round);
        }
        if round == MAX_ROUNDS {
            break;
        }
        for (seq, value) in want.iter().enumerate() {
            if written.get(seq) == Some(value) {
                continue;
            }
            let key = RecordKey { txn, seq: seq as u32 };
            let replaced = write::put(writer, free, &key.encode(), value)?;
            if replaced != (seq < written.len()) {
                return Err(Error::Corrupt(format!(
                    "free list: a record {key:?} of the committing transaction already exists"
                )));
            }
        }
        written = want;
    }
    Err(Error::Failed(format!("free list: the save did not settle in {MAX_ROUNDS} rounds")))
}

#[cfg(test)]
mod tests;
