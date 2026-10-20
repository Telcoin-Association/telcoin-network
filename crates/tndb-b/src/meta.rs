//! The meta pages: pages 0 and 1 each hold a 128-byte [`Meta`] record (the rest of the page is
//! zero) naming one committed state of the file: its transaction id, page count, next table id and
//! the roots of the catalog and the free tree. A commit writes the new meta over the slot holding
//! the lower transaction id ([`slot_to_write`]), so the other slot always names the previous
//! state: a crash while the meta is written leaves either the new record intact or the old one.
//! Open ([`choose_meta`]) takes the valid meta with the highest transaction id.
//!
//! Record layout (little-endian):
//!
//! | off | size | field |
//! |---|---|---|
//! | 0 | 8 | magic `b"tndb-b\0\0"` |
//! | 8 | 4 | format version (1) |
//! | 12 | 4 | page size |
//! | 16 | 8 | txn id |
//! | 24 | 4 | page count (pages `0..page_count` are allocated) |
//! | 28 | 4 | next table id |
//! | 32 | 24 | catalog [`TreeRoot`] |
//! | 56 | 24 | free-tree [`TreeRoot`] |
//! | 80 | 44 | reserved (zero) |
//! | 124 | 4 | CRC32 over bytes 0..124 |

use std::io;

use crate::{
    crc,
    error::{Error, Result},
    page::Geometry,
    tree::TreeRoot,
};

/// The first 8 bytes of every meta record.
pub(crate) const MAGIC: [u8; 8] = *b"tndb-b\0\0";

/// The on-disk format version this build reads and writes.
pub(crate) const FORMAT_VERSION: u32 = 1;

/// The size of a meta record (at offset 0 of its page).
pub(crate) const META_LEN: usize = 128;

/// Where the record's CRC32 (over the bytes before it) lies.
const CRC_AT: usize = 124;

/// The bytes the format reserves (zero in version 1).
const RESERVED: std::ops::Range<usize> = 80..CRC_AT;

/// Every page size a file can have, smallest first: where meta 1 may lie when meta 0 cannot be
/// read.
pub(crate) const PAGE_SIZES: [u32; 5] = [4096, 8192, 16384, 32768, 65536];

/// The id of the first table a file creates (`next_table_id` of a new file).
pub(crate) const FIRST_TABLE_ID: u32 = 1;

/// One committed state of the file, as a meta page records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Meta {
    /// The page size, fixed when the file is created.
    pub(crate) page_size: u32,
    /// The id of the transaction that committed this state.
    pub(crate) txn: u64,
    /// Pages `0..page_count` are allocated (the file may be longer: growth slack).
    pub(crate) page_count: u32,
    /// The id the next table created gets.
    pub(crate) next_table_id: u32,
    /// The catalog: table name to table record.
    pub(crate) catalog: TreeRoot,
    /// The free tree: the pages freed by committed transactions.
    pub(crate) free: TreeRoot,
}

impl Meta {
    /// The state of a new file of `page_size`-byte pages: transaction 0, the two meta pages
    /// only, no tables.
    pub(crate) fn initial(page_size: u32) -> Self {
        Self {
            page_size,
            txn: 0,
            page_count: 2,
            next_table_id: FIRST_TABLE_ID,
            catalog: TreeRoot::EMPTY,
            free: TreeRoot::EMPTY,
        }
    }

    /// The 128-byte record (see the module docs), CRC included.
    pub(crate) fn encode(&self) -> [u8; META_LEN] {
        let mut out = [0; META_LEN];
        out[0..8].copy_from_slice(&MAGIC);
        out[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        out[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        out[16..24].copy_from_slice(&self.txn.to_le_bytes());
        out[24..28].copy_from_slice(&self.page_count.to_le_bytes());
        out[28..32].copy_from_slice(&self.next_table_id.to_le_bytes());
        out[32..56].copy_from_slice(&self.catalog.encode());
        out[56..80].copy_from_slice(&self.free.encode());
        let crc = crc::crc32(&out[..CRC_AT]);
        out[CRC_AT..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decode a record: [`Error::NotTndbB`] without the magic, [`Error::UnsupportedVersion`] for
    /// another format version (checked before the CRC: another version may lay the record out
    /// differently), and [`Error::Corrupt`] for a failed CRC or a field no committed state can
    /// hold (an invalid page size, fewer than the two meta pages, a non-zero reserved byte, an
    /// impossible tree root, a zero table id).
    pub(crate) fn decode(rec: &[u8; META_LEN]) -> Result<Self> {
        let u32_at =
            |off: usize| u32::from_le_bytes(rec[off..off + 4].try_into().expect("4 bytes"));
        if rec[0..8] != MAGIC {
            return Err(Error::NotTndbB);
        }
        let version = u32_at(8);
        if version != FORMAT_VERSION {
            return Err(Error::UnsupportedVersion(version));
        }
        if crc::crc32(&rec[..CRC_AT]) != u32_at(CRC_AT) {
            return Err(Error::Corrupt("meta record fails its CRC".to_string()));
        }
        let corrupt = |what: String| Err(Error::Corrupt(format!("meta record: {what}")));
        let page_size = u32_at(12);
        if Geometry::new(page_size).is_err() {
            return corrupt(format!("invalid page size {page_size}"));
        }
        if rec[RESERVED].iter().any(|&b| b != 0) {
            return corrupt("reserved bytes are not zero".to_string());
        }
        let meta = Self {
            page_size,
            txn: u64::from_le_bytes(rec[16..24].try_into().expect("8 bytes")),
            page_count: u32_at(24),
            next_table_id: u32_at(28),
            catalog: TreeRoot::decode(rec[32..56].try_into().expect("24 bytes"))?,
            free: TreeRoot::decode(rec[56..80].try_into().expect("24 bytes"))?,
        };
        if meta.page_count < 2 {
            return corrupt(format!("page count {} leaves no room for the metas", meta.page_count));
        }
        if meta.next_table_id < FIRST_TABLE_ID {
            return corrupt(format!("next table id {}", meta.next_table_id));
        }
        Ok(meta)
    }
}

/// What open found in the two meta slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Chosen {
    /// The newest valid meta: the state the file opens at.
    pub(crate) meta: Meta,
    /// The txn of each slot's meta (`None`: invalid, a torn or damaged record). An invalid slot
    /// counts as the lowest and is the next one written ([`slot_to_write`]).
    pub(crate) slots: [Option<u64>; 2],
}

/// Choose the meta to open at (open step 4). `read(offset)` returns the 128-byte record at
/// `offset` of the file, or `None` if the file ends before it.
///
/// Meta 0 lies at offset 0. If it is valid its page size P places meta 1; if not, meta 1 is
/// probed at every page size P and accepted only if it names that P. The valid meta with the
/// highest txn wins. When both are valid they must agree on the page size, and two metas of the
/// same txn must be byte-identical (a commit writes a new txn); else [`Error::Corrupt`]. With no
/// valid meta the file fails closed: [`Error::NotTndbB`] if no record has the magic, else
/// [`Error::Corrupt`]. A record of another format version is [`Error::UnsupportedVersion`] even
/// if the other slot is valid: opening at the older slot would overwrite a newer state.
pub(crate) fn choose_meta(
    read: impl Fn(u64) -> io::Result<Option<[u8; META_LEN]>>,
) -> Result<Chosen> {
    let slot = |offset: u64| -> Result<Option<([u8; META_LEN], Result<Meta>)>> {
        Ok(read(offset)?.map(|rec| (rec, Meta::decode(&rec))))
    };
    let unsupported = |decoded: &Result<Meta>| match decoded {
        Err(Error::UnsupportedVersion(v)) => Err(Error::UnsupportedVersion(*v)),
        _ => Ok(()),
    };

    let zero = slot(0)?;
    if let Some((_, decoded)) = &zero {
        unsupported(decoded)?;
    }
    if let Some((rec0, Ok(m0))) = zero {
        return match slot(u64::from(m0.page_size))? {
            Some((rec1, Ok(m1))) => {
                if m1.page_size != m0.page_size {
                    return Err(Error::Corrupt(format!(
                        "the meta pages disagree on the page size ({} and {})",
                        m0.page_size, m1.page_size
                    )));
                }
                if m1.txn == m0.txn && rec1 != rec0 {
                    return Err(Error::Corrupt(format!(
                        "the meta pages hold two different states of txn {}",
                        m0.txn
                    )));
                }
                let meta = if m1.txn > m0.txn { m1 } else { m0 };
                Ok(Chosen { meta, slots: [Some(m0.txn), Some(m1.txn)] })
            }
            Some((_, decoded)) => {
                unsupported(&decoded)?;
                Ok(Chosen { meta: m0, slots: [Some(m0.txn), None] })
            }
            None => Ok(Chosen { meta: m0, slots: [Some(m0.txn), None] }),
        };
    }

    // Meta 0 is unreadable: find meta 1 by the page size it names.
    let mut magic_seen =
        matches!(&zero, Some((_, decoded)) if !matches!(decoded, Err(Error::NotTndbB)));
    let mut found = None;
    for p in PAGE_SIZES {
        let Some((_, decoded)) = slot(u64::from(p))? else { break };
        unsupported(&decoded)?;
        match decoded {
            Ok(m) if m.page_size == p => {
                if found.replace(m).is_some() {
                    return Err(Error::Corrupt(
                        "meta 0 is invalid and meta 1 appears at two page sizes".to_string(),
                    ));
                }
            }
            Err(Error::NotTndbB) => {}
            _ => magic_seen = true,
        }
    }
    match found {
        Some(meta) => Ok(Chosen { meta, slots: [None, Some(meta.txn)] }),
        None if magic_seen => Err(Error::Corrupt("neither meta page is valid".to_string())),
        None => Err(Error::NotTndbB),
    }
}

/// The slot a commit writes: the one holding the lower txn (an invalid slot first; on a tie,
/// slot 1), so the other keeps the newest durable state until the write completes.
pub(crate) fn slot_to_write(slots: &[Option<u64>; 2]) -> usize {
    match slots {
        [None, _] => 0,
        [_, None] => 1,
        [Some(a), Some(b)] => usize::from(a >= b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(page_size: u32, txn: u64) -> Meta {
        Meta {
            txn,
            page_count: 9,
            next_table_id: 3,
            catalog: TreeRoot { root: 5, root_txn: txn as u32, height: 2, entries: 4 },
            ..Meta::initial(page_size)
        }
    }

    /// A file image holding `m0` at offset 0 and `m1` at its page size (`None`: left zero).
    fn image(page_size: u32, m0: Option<[u8; META_LEN]>, m1: Option<[u8; META_LEN]>) -> Vec<u8> {
        let p = page_size as usize;
        let mut file = vec![0; 3 * p];
        if let Some(rec) = m0 {
            file[..META_LEN].copy_from_slice(&rec);
        }
        if let Some(rec) = m1 {
            file[p..p + META_LEN].copy_from_slice(&rec);
        }
        file
    }

    fn choose(file: &[u8]) -> Result<Chosen> {
        choose_meta(|off| {
            let off = off as usize;
            Ok(file.get(off..off + META_LEN).map(|rec| rec.try_into().expect("128 bytes")))
        })
    }

    /// Restamp a record's CRC after changing a field.
    fn restamp(rec: &mut [u8; META_LEN]) {
        let crc = crc::crc32(&rec[..CRC_AT]);
        rec[CRC_AT..].copy_from_slice(&crc.to_le_bytes());
    }

    /// The record round-trips, and each kind of damage is refused with its own error.
    #[test]
    fn meta_encoding_round_trips_and_refuses_damage() {
        let m = Meta {
            free: TreeRoot { root: 7, root_txn: 1, height: 1, entries: 2 },
            ..meta(8192, 42)
        };
        let rec = m.encode();
        assert_eq!(Meta::decode(&rec).expect("decode"), m);
        assert!(rec[RESERVED].iter().all(|&b| b == 0));

        let damaged = |f: &dyn Fn(&mut [u8; META_LEN]), stamp: bool| {
            let mut bad = rec;
            f(&mut bad);
            if stamp {
                restamp(&mut bad);
            }
            Meta::decode(&bad)
        };
        assert!(matches!(damaged(&|r| r[3] ^= 1, true), Err(Error::NotTndbB)));
        assert!(matches!(damaged(&|r| r[8] = 2, false), Err(Error::UnsupportedVersion(2))));
        assert!(matches!(damaged(&|r| r[20] ^= 1, false), Err(Error::Corrupt(_))), "crc");
        assert!(matches!(damaged(&|r| r[CRC_AT] ^= 1, false), Err(Error::Corrupt(_))), "crc");
        assert!(matches!(damaged(&|r| r[100] = 1, true), Err(Error::Corrupt(_))), "reserved");
        let set_u32 = |off: usize, v: u32| {
            move |r: &mut [u8; META_LEN]| r[off..off + 4].copy_from_slice(&v.to_le_bytes())
        };
        for (what, f) in [
            ("page size", set_u32(12, 6000)),
            ("page size", set_u32(12, 2048)),
            ("page count", set_u32(24, 1)),
            ("table id", set_u32(28, 0)),
            ("catalog shape", set_u32(40, 0)),
            ("free reserved word", set_u32(68, 1)),
        ] {
            assert!(matches!(damaged(&f, true), Err(Error::Corrupt(_))), "{what}");
        }
    }

    /// The valid meta with the highest txn wins, whichever slot holds it, at every page size.
    #[test]
    fn the_newest_valid_meta_wins() {
        for p in PAGE_SIZES {
            let file = image(p, Some(meta(p, 5).encode()), Some(meta(p, 6).encode()));
            assert_eq!(
                choose(&file).expect("choose"),
                Chosen { meta: meta(p, 6), slots: [Some(5), Some(6)] }
            );
            let file = image(p, Some(meta(p, 8).encode()), Some(meta(p, 7).encode()));
            assert_eq!(
                choose(&file).expect("choose"),
                Chosen { meta: meta(p, 8), slots: [Some(8), Some(7)] }
            );
        }
    }

    /// A torn newest meta falls back to the other one, which stays the slot to keep; the torn
    /// slot is the next written.
    #[test]
    fn a_torn_newest_meta_falls_back() {
        let mut torn = meta(4096, 6).encode();
        torn[17] ^= 0x40;
        let chosen =
            choose(&image(4096, Some(meta(4096, 5).encode()), Some(torn))).expect("choose");
        assert_eq!(chosen, Chosen { meta: meta(4096, 5), slots: [Some(5), None] });
        assert_eq!(slot_to_write(&chosen.slots), 1);

        // Meta 0 torn: meta 1 is found by probing its page size, at every page size.
        for p in PAGE_SIZES {
            let mut torn = meta(p, 9).encode();
            torn[CRC_AT] ^= 1;
            let chosen = choose(&image(p, Some(torn), Some(meta(p, 8).encode()))).expect("choose");
            assert_eq!(chosen, Chosen { meta: meta(p, 8), slots: [None, Some(8)] });
            assert_eq!(slot_to_write(&chosen.slots), 0);
            // Zeroed meta 0 (a never-written page) probes the same way.
            let chosen = choose(&image(p, None, Some(meta(p, 8).encode()))).expect("choose");
            assert_eq!(chosen.meta, meta(p, 8));
        }
    }

    /// A probed meta 1 counts only at the offset its own page size gives it.
    #[test]
    fn a_probed_meta_must_name_its_offset() {
        // An 8 KiB meta at offset 4096 is not meta 1 of any file.
        let mut file = vec![0; 3 * 8192];
        file[4096..4096 + META_LEN].copy_from_slice(&meta(8192, 3).encode());
        assert!(matches!(choose(&file), Err(Error::Corrupt(_))));
        // Nor is a 4 KiB one at offset 8192.
        let mut file = vec![0; 3 * 8192];
        file[8192..8192 + META_LEN].copy_from_slice(&meta(4096, 3).encode());
        assert!(matches!(choose(&file), Err(Error::Corrupt(_))));
        // Two candidates for meta 1, each at its own page size: ambiguous, so refused.
        file[4096..4096 + META_LEN].copy_from_slice(&meta(4096, 3).encode());
        file[8192..8192 + META_LEN].copy_from_slice(&meta(8192, 3).encode());
        assert!(matches!(choose(&file), Err(Error::Corrupt(_))));
    }

    /// No valid meta fails closed: `Corrupt` when the magic is there, `NotTndbB` when it is not.
    #[test]
    fn no_valid_meta_fails_closed() {
        let mut bad0 = meta(4096, 5).encode();
        bad0[30] ^= 1;
        let mut bad1 = meta(4096, 6).encode();
        bad1[CRC_AT + 1] ^= 1;
        assert!(matches!(choose(&image(4096, Some(bad0), Some(bad1))), Err(Error::Corrupt(_))));
        assert!(matches!(choose(&image(4096, Some(bad0), None)), Err(Error::Corrupt(_))));
        assert!(matches!(choose(&image(4096, None, Some(bad1))), Err(Error::Corrupt(_))));
        assert!(matches!(choose(&image(4096, None, None)), Err(Error::NotTndbB)));
        assert!(matches!(choose(&vec![0xA5; 70_000]), Err(Error::NotTndbB)));
        assert!(matches!(choose(b"short"), Err(Error::NotTndbB)));
        let mut wrong_magic = meta(4096, 5).encode();
        wrong_magic[0] = b'T';
        restamp(&mut wrong_magic);
        assert!(matches!(choose(&image(4096, Some(wrong_magic), None)), Err(Error::NotTndbB)));
    }

    /// Two valid metas must agree on the page size, and metas of one txn must be identical.
    #[test]
    fn valid_metas_must_agree() {
        let mut other_size = meta(4096, 6);
        other_size.page_size = 8192;
        let file = image(4096, Some(meta(4096, 5).encode()), Some(other_size.encode()));
        assert!(matches!(choose(&file), Err(Error::Corrupt(_))));

        let same = image(4096, Some(meta(4096, 5).encode()), Some(meta(4096, 5).encode()));
        assert_eq!(choose(&same).expect("identical").meta, meta(4096, 5));
        let differ = Meta { page_count: 10, ..meta(4096, 5) };
        let file = image(4096, Some(meta(4096, 5).encode()), Some(differ.encode()));
        assert!(matches!(choose(&file), Err(Error::Corrupt(_))));
    }

    /// A record of another format version refuses the file, in either slot, even next to a
    /// valid one.
    #[test]
    fn another_format_version_is_refused() {
        let mut v2 = meta(4096, 6).encode();
        v2[8] = 2;
        restamp(&mut v2);
        let ok = meta(4096, 5).encode();
        assert!(matches!(
            choose(&image(4096, Some(v2), Some(ok))),
            Err(Error::UnsupportedVersion(2))
        ));
        assert!(matches!(
            choose(&image(4096, Some(ok), Some(v2))),
            Err(Error::UnsupportedVersion(2))
        ));
        assert!(matches!(choose(&image(4096, None, Some(v2))), Err(Error::UnsupportedVersion(2))));
    }

    /// The commit writes over the lower txn, an invalid slot first, slot 1 on a tie.
    #[test]
    fn the_lower_slot_is_written() {
        assert_eq!(slot_to_write(&[Some(4), Some(5)]), 0);
        assert_eq!(slot_to_write(&[Some(5), Some(4)]), 1);
        assert_eq!(slot_to_write(&[Some(0), Some(0)]), 1);
        assert_eq!(slot_to_write(&[None, Some(4)]), 0);
        assert_eq!(slot_to_write(&[Some(4), None]), 1);
    }
}
