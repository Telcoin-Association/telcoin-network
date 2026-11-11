//! The meta pages: pages 0 and 1 each hold a 128-byte [`Meta`] record (the rest of the page is
//! zero) naming one committed state of the file: its transaction id, page count, next table id and
//! the roots of the catalog and the free tree. A record is *steady*, written by the durable steps
//! after its state's pages were synced, or *weak*, written by a `SyncMode::NoSync` commit with no
//! sync at all, and carries the boot it was written in ([`Tag`]). Every record goes into the slot
//! that does not hold the newest steady record ([`slot_to_write`]), so one slot always holds a
//! steady state that no write touches: a crash while a record is written leaves either the new
//! record intact or that steady one. Open ([`choose_meta`]) takes the valid record with the
//! highest transaction id, unless it is weak and not from the running boot: its pages may never
//! have reached the disk, and open takes the steady one beside it.
//!
//! Record layout (little-endian):
//!
//! | off | size | field |
//! |---|---|---|
//! | 0 | 8 | magic `b"tn-lmdb\0"` |
//! | 8 | 4 | format version (6) |
//! | 12 | 4 | page size |
//! | 16 | 8 | txn id |
//! | 24 | 4 | page count (pages `0..page_count` are allocated) |
//! | 28 | 4 | next table id |
//! | 32 | 32 | catalog [`TreeRoot`] |
//! | 64 | 32 | free-tree [`TreeRoot`] |
//! | 96 | 4 | flags: bit 0 weak; every other bit zero |
//! | 100 | 16 | boot id of the writer (zero: unknown) |
//! | 116 | 8 | reserved (zero) |
//! | 124 | 4 | CRC32 over bytes 0..124 |

use std::io;

use crate::{
    crc,
    error::{Error, Result},
    page::Geometry,
    sys::BootId,
    tree::TreeRoot,
};

/// The first 8 bytes of every meta record.
pub(crate) const MAGIC: [u8; 8] = *b"tn-lmdb\0";

/// The on-disk format version this build reads and writes: 6, whose meta records carry a flags
/// word and a boot id ([`Tag`]), whose leaves hold inline values up to their entry limit of `U / 2`
/// (`page::Geometry::max_value`) and whose tree pages' CRCs skip their free gaps (`page::crc`).
/// Version 5's records kept those bytes reserved and had no weak record; version 4's leaves held
/// inline values only to `U / 4`, every larger value in an overflow run, and version 3's pages
/// carry whole-page CRCs; each is refused as [`Error::UnsupportedVersion`], as are 2 and 1: the
/// format is pre-release, with no migration.
pub(crate) const FORMAT_VERSION: u32 = 6;

/// The size of a meta record (at offset 0 of its page).
pub(crate) const META_LEN: usize = 128;

/// Where the record's CRC32 (over the bytes before it) lies.
const CRC_AT: usize = 124;

/// Where the catalog's [`TreeRoot`] lies.
const CATALOG: std::ops::Range<usize> = 32..32 + TreeRoot::ENCODED_LEN;

/// Where the free tree's [`TreeRoot`] lies.
const FREE: std::ops::Range<usize> = CATALOG.end..CATALOG.end + TreeRoot::ENCODED_LEN;

/// Where the flags word lies ([`WEAK`]; every other bit must be zero).
const FLAGS: std::ops::Range<usize> = FREE.end..FREE.end + 4;

/// The flag of a weak record ([`Tag::weak`]).
const WEAK: u32 = 1;

/// Where the boot id of the record's writer lies ([`Tag::boot_id`]).
const BOOT_ID: std::ops::Range<usize> = FLAGS.end..FLAGS.end + 16;

/// The bytes the format reserves (zero in version 6).
const RESERVED: std::ops::Range<usize> = BOOT_ID.end..CRC_AT;

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

    /// The 128-byte record of this state written as `tag` says (see the module docs), CRC
    /// included.
    pub(crate) fn encode(&self, tag: Tag) -> [u8; META_LEN] {
        let mut out = [0; META_LEN];
        out[0..8].copy_from_slice(&MAGIC);
        out[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        out[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        out[16..24].copy_from_slice(&self.txn.to_le_bytes());
        out[24..28].copy_from_slice(&self.page_count.to_le_bytes());
        out[28..32].copy_from_slice(&self.next_table_id.to_le_bytes());
        out[CATALOG].copy_from_slice(&self.catalog.encode());
        out[FREE].copy_from_slice(&self.free.encode());
        out[FLAGS].copy_from_slice(&(if tag.weak { WEAK } else { 0 }).to_le_bytes());
        out[BOOT_ID].copy_from_slice(&tag.boot_id);
        let crc = crc::crc32(&out[..CRC_AT]);
        out[CRC_AT..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    /// Decode a record: the state and its [`Tag`]. [`Error::NotTnLmdb`] without the magic,
    /// [`Error::UnsupportedVersion`] for another format version (checked before the CRC: another
    /// version may lay the record out differently), and [`Error::Corrupt`] for a failed CRC or a
    /// field no committed state can hold (an invalid page size, fewer than the two meta pages, a
    /// flag this format does not define, a non-zero reserved byte, an impossible tree root, a zero
    /// table id).
    pub(crate) fn decode(rec: &[u8; META_LEN]) -> Result<(Self, Tag)> {
        let u32_at =
            |off: usize| u32::from_le_bytes(rec[off..off + 4].try_into().expect("4 bytes"));
        if rec[0..8] != MAGIC {
            return Err(Error::NotTnLmdb);
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
        let flags = u32_at(FLAGS.start);
        if flags & !WEAK != 0 {
            return corrupt(format!("unknown flags {flags:#x}"));
        }
        if rec[RESERVED].iter().any(|&b| b != 0) {
            return corrupt("reserved bytes are not zero".to_string());
        }
        let meta = Self {
            page_size,
            txn: u64::from_le_bytes(rec[16..24].try_into().expect("8 bytes")),
            page_count: u32_at(24),
            next_table_id: u32_at(28),
            catalog: TreeRoot::decode(rec[CATALOG].try_into().expect("a tree root"))?,
            free: TreeRoot::decode(rec[FREE].try_into().expect("a tree root"))?,
        };
        if meta.page_count < 2 {
            return corrupt(format!("page count {} leaves no room for the metas", meta.page_count));
        }
        if meta.next_table_id < FIRST_TABLE_ID {
            return corrupt(format!("next table id {}", meta.next_table_id));
        }
        let tag =
            Tag { weak: flags & WEAK != 0, boot_id: rec[BOOT_ID].try_into().expect("16 bytes") };
        Ok((meta, tag))
    }
}

/// How a record was written, beside the state it names: its flags and boot id.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Tag {
    /// A weak record: written by a `SyncMode::NoSync` commit with no sync, so its state's pages
    /// may still be only in the page cache. Open trusts it only in the boot that wrote it
    /// ([`choose_meta`]). A steady record (`false`) was written by the durable steps once its
    /// state's pages were synced, or by a create.
    pub(crate) weak: bool,
    /// The boot the writer ran in (`sys::boot_id`); zero when unknown, which no open trusts.
    pub(crate) boot_id: BootId,
}

impl Tag {
    /// A steady record's tag, written in boot `boot_id`.
    pub(crate) fn steady(boot_id: BootId) -> Self {
        Self { weak: false, boot_id }
    }

    /// A weak record's tag, written in boot `boot_id`.
    pub(crate) fn weak(boot_id: BootId) -> Self {
        Self { weak: true, boot_id }
    }
}

/// What a meta slot holds: the txn of its valid record, and whether that record is weak. Open
/// reports both slots ([`Chosen::slots`]) and the writer keeps them up to date: the reuse gate
/// counts every valid slot, the slot rule ([`slot_to_write`]) and the sync's check for a pending
/// state only the steady ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Slot {
    /// The txn of the slot's record.
    pub(crate) txn: u64,
    /// The record is weak ([`Tag::weak`]).
    pub(crate) weak: bool,
}

/// What open found in the two meta slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Chosen {
    /// The state the file opens at: the newest valid meta, or the steady one beside it when the
    /// newest is weak and not trusted ([`choose_meta`]).
    pub(crate) meta: Meta,
    /// What each slot holds (`None`: invalid, a torn or damaged record), the chosen one's
    /// included. An untrusted weak slot is listed as it is: its txn is above the steady slot's,
    /// and it is the next one written ([`slot_to_write`]).
    pub(crate) slots: [Option<Slot>; 2],
    /// When `meta` is a trusted weak head, the steady meta beside it, the last sync: open checks
    /// the pages the head names that were written since it, and opens at it if any fails
    /// (`check::check_since`). `None` when `meta` is steady.
    pub(crate) last_sync: Option<Meta>,
}

#[cfg(test)]
impl Slot {
    /// Test-only: a slot holding a valid steady record of `txn`.
    pub(crate) fn steady(txn: u64) -> Option<Self> {
        Some(Self { txn, weak: false })
    }

    /// Test-only: a slot holding a valid weak record of `txn`.
    pub(crate) fn weak(txn: u64) -> Option<Self> {
        Some(Self { txn, weak: true })
    }
}

/// The most txns a writer stamps past its newest steady meta: a `SyncMode::NoSync` commit whose
/// txn is further past it commits durably (`Writer::sync_due`), and a writer open starts one past
/// that run ([`Chosen::next_txn`]). Open's check of a weak head and every fetch's identity check
/// tell a page from another only by its number and its writer's txn, so no txn may be given to a
/// commit whose pages a weak meta names while pages another writer stamped with it can be on the
/// disk; the run is what makes every txn a dead or rolled-back writer stamped lower than the next
/// writer's first. 2^20: a writer that never syncs on its own (`SyncMode::NoSync` with neither
/// bound) syncs once per about a million changing commits, which costs nothing measurable; an open
/// consumes at most 2^20 + 1 txn ids, so the ids last 2^43 opens; and a weak head is at most 2^20
/// past the steady meta beside it, far inside the 2^31 window open's check of it prunes in
/// (`check::named_after`).
pub(crate) const MAX_WEAK_RUN: u64 = 1 << 20;

impl Chosen {
    /// The txn of the writer's first transaction after this open: [`MAX_WEAK_RUN`] + 1 past the
    /// newest steady slot's, and at least one past every txn a valid slot holds, an untrusted weak
    /// head's too. So no txn a commit stamped pages with is ever given to a weak commit again.
    /// Every writer runs at most [`MAX_WEAK_RUN`] txns past its newest steady meta before a commit
    /// is durable, and its first changing commit, at this txn, is one; that steady meta stays on
    /// the disk (no write touches the newest steady slot, and open syncs what it found before
    /// building on it), so every txn a writer that ended in any way stamped (a commit dead in its
    /// steps 3 to 6, a torn weak or steady copy, weak commits a reboot or a volume lost in the boot
    /// rolled back, whose pages the kernel had written) is below the next open's first, but for
    /// the one a durable commit stamped at the end of a full run, which only the next writer's
    /// first, durable, commit is given again: its pages are synced before its meta is copied, so
    /// no meta naming that txn ever finds the dead commit's pages. Anchoring the start at the
    /// steady slot rather than at the newest slot keeps that so when the newest slot is lost: a
    /// writer that opened over a weak head and died inside its first commit's steady copy over
    /// the weak slot leaves the steady slot alone, and the next open starts at the dead
    /// commit's txn again, not below it. Every page written from then on is also newer than
    /// every state a slot names: if a later open trusted a weak head this one passed over
    /// (`EnvOptions::trust_unsynced` off, then on again, in the same boot), a page this writer
    /// reused under it fails the read's checks (`Corrupt`), never passing for the head's own.
    /// `None` when the txn cannot be formed (a slot at `u64::MAX`, a steady slot less than
    /// [`MAX_WEAK_RUN`] + 1 below it).
    pub(crate) fn next_txn(&self) -> Option<u64> {
        let newest = self.slots.iter().flatten().map(|slot| slot.txn).max()?.checked_add(1)?;
        let past_run = match newest_steady(&self.slots) {
            Some(txn) => txn.checked_add(MAX_WEAK_RUN + 1)?,
            None => 0,
        };
        Some(newest.max(past_run))
    }

    /// The txn ids this open's writer never gives: from one past the newest valid slot's up to
    /// [`Self::next_txn`]. The reuse gate's lag leaves them out (`IoStats::gate_lag_sum`). `None`
    /// as for [`Self::next_txn`].
    pub(crate) fn skipped_txns(&self) -> Option<std::ops::Range<u64>> {
        let next = self.next_txn()?;
        let newest = self.slots.iter().flatten().map(|slot| slot.txn).max()?;
        Some(newest + 1..next)
    }
}

/// The txn of the newest steady record `slots` hold, the last state made durable.
pub(crate) fn newest_steady(slots: &[Option<Slot>; 2]) -> Option<u64> {
    slots.iter().flatten().filter(|slot| !slot.weak).map(|slot| slot.txn).max()
}

/// The newest txn a `SyncMode::NoSync` commit may leave weak beside `slots`: [`MAX_WEAK_RUN`]
/// past the newest steady slot's (`Writer::sync_due`); 0 with none, which no open leaves.
pub(crate) fn weak_limit(slots: &[Option<Slot>; 2]) -> u64 {
    newest_steady(slots).map_or(0, |txn| txn.saturating_add(MAX_WEAK_RUN))
}

/// Choose the meta to open at (open step 5). `read(offset)` returns the 128-byte record at
/// `offset` of the file, or `None` if the file ends before it. `trusted` is the boot whose weak
/// records may be trusted: the running boot's id, or `None` to trust none (its id is unknown, or
/// the open was asked not to: `EnvOptions::trust_unsynced`).
///
/// Meta 0 lies at offset 0. If it is valid its page size P places meta 1; if not, meta 1 is the
/// first record carrying the magic at an offset P of [`PAGE_SIZES`], smallest first (the offsets
/// below it lie in meta 0's zero tail), accepted only if it names that P; the offsets past it
/// are never read, as they hold the file's other pages. When both are valid they must agree on
/// the page size, and two metas of the same txn must be byte-identical (a commit writes a new
/// txn); else [`Error::Corrupt`]. With no valid meta the file fails closed: [`Error::NotTnLmdb`]
/// if no record read has the magic, else [`Error::Corrupt`]. A record of another format version
/// is [`Error::UnsupportedVersion`] even if the other slot is valid: opening at the older slot
/// would overwrite a newer state.
///
/// The valid meta with the highest txn wins if it is steady, or weak and written in the `trusted`
/// boot (both ids known and equal): a weak meta's pages were never synced, so only the page cache
/// of the boot that wrote them holds them for sure. A weak meta from any other boot, or with an
/// unknown id, is passed over for the other slot, which must then hold a valid steady meta, else
/// [`Error::Corrupt`]: never the weak head. A trusted weak head needs that steady slot beside it
/// too, as every write keeps one ([`slot_to_write`]): a file without one is damaged. It is
/// reported as [`Chosen::last_sync`], which open falls back to if the head's pages written since
/// it fail their check.
pub(crate) fn choose_meta(
    read: impl Fn(u64) -> io::Result<Option<[u8; META_LEN]>>,
    trusted: Option<BootId>,
) -> Result<Chosen> {
    let found = read_slots(read)?;
    let slots = found.map(|slot| slot.map(|(meta, tag)| Slot { txn: meta.txn, weak: tag.weak }));
    let newest = match found {
        [Some((m0, _)), Some((m1, _))] => usize::from(m1.txn > m0.txn),
        [_, Some(_)] => 1,
        _ => 0,
    };
    let (meta, tag) = found[newest].expect("read_slots returns a valid meta");
    if !tag.weak {
        return Ok(Chosen { meta, slots, last_sync: None });
    }
    let steady = found[1 - newest].filter(|(_, other)| !other.weak);
    let Some((steady, _)) = steady else {
        return Err(Error::Corrupt(format!(
            "the newest meta (txn {}) is weak, and the other slot holds no valid steady meta",
            meta.txn
        )));
    };
    let trust = trusted.is_some_and(|boot| boot != [0; 16] && boot == tag.boot_id);
    Ok(if trust {
        Chosen { meta, slots, last_sync: Some(steady) }
    } else {
        Chosen { meta: steady, slots, last_sync: None }
    })
}

/// A record as read from a slot: its bytes and what they decode to.
type ReadRecord = ([u8; META_LEN], Result<(Meta, Tag)>);

/// The valid records of both meta slots, found and checked as [`choose_meta`] says: at least one,
/// or the error that refuses the file.
fn read_slots(
    read: impl Fn(u64) -> io::Result<Option<[u8; META_LEN]>>,
) -> Result<[Option<(Meta, Tag)>; 2]> {
    let slot = |offset: u64| -> Result<Option<ReadRecord>> {
        Ok(read(offset)?.map(|rec| (rec, Meta::decode(&rec))))
    };
    let unsupported = |decoded: &Result<(Meta, Tag)>| match decoded {
        Err(Error::UnsupportedVersion(v)) => Err(Error::UnsupportedVersion(*v)),
        _ => Ok(()),
    };

    let zero = slot(0)?;
    if let Some((_, decoded)) = &zero {
        unsupported(decoded)?;
    }
    if let Some((rec0, Ok(m0))) = zero {
        return match slot(u64::from(m0.0.page_size))? {
            Some((rec1, Ok(m1))) => {
                if m1.0.page_size != m0.0.page_size {
                    return Err(Error::Corrupt(format!(
                        "the meta pages disagree on the page size ({} and {})",
                        m0.0.page_size, m1.0.page_size
                    )));
                }
                if m1.0.txn == m0.0.txn && rec1 != rec0 {
                    return Err(Error::Corrupt(format!(
                        "the meta pages hold two different states of txn {}",
                        m0.0.txn
                    )));
                }
                Ok([Some(m0), Some(m1)])
            }
            Some((_, decoded)) => {
                unsupported(&decoded)?;
                Ok([Some(m0), None])
            }
            None => Ok([Some(m0), None]),
        };
    }

    // Meta 0 is unreadable: meta 1 is the first record carrying the magic. A meta page is zero
    // past its record (create writes the zeros, a commit the record alone), so the probe stops
    // at meta 1 and never decodes a page after it, whose bytes can be anything (a value's).
    let neither = || Error::Corrupt("neither meta page is valid".to_string());
    for p in PAGE_SIZES {
        let Some((_, decoded)) = slot(u64::from(p))? else { break };
        match decoded {
            Err(Error::NotTnLmdb) => {}
            Ok(m) if m.0.page_size == p => return Ok([None, Some(m)]),
            decoded => {
                unsupported(&decoded)?;
                return Err(neither());
            }
        }
    }
    match zero {
        Some((_, Err(Error::NotTnLmdb))) | None => Err(Error::NotTnLmdb),
        Some(_) => Err(neither()),
    }
}

/// The slot the next record goes to, weak or steady: the one that does not hold the newest steady
/// record, which no write then touches (a slot holding a weak or an invalid record counts as
/// holding none; with both steady, the lower txn, slot 1 on a tie). With both slots steady that is
/// the slot holding the lower txn, so every commit of `SyncMode::Durable` keeps the previous
/// durable state; with a steady and a weak slot it is the weak one, which a weak commit rewrites
/// again and again and a sync's steady record replaces, so a torn write loses at most the weak
/// slot. Open guarantees one steady slot ([`choose_meta`]), and every write keeps it.
pub(crate) fn slot_to_write(slots: &[Option<Slot>; 2]) -> usize {
    let steady = |slot: &Option<Slot>| slot.filter(|s| !s.weak).map(|s| s.txn);
    match (steady(&slots[0]), steady(&slots[1])) {
        (None, _) => 0,
        (_, None) => 1,
        (Some(a), Some(b)) => usize::from(a >= b),
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
            catalog: TreeRoot {
                root: 5,
                root_txn: txn as u32,
                height: 2,
                entries: 4,
                branch_pages: 1,
                leaf_pages: 2,
                ..TreeRoot::EMPTY
            },
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

    /// A steady record's tag with no boot id, as a create writes it.
    const STEADY: Tag = Tag { weak: false, boot_id: [0; 16] };

    /// The running boot of these tests, and another one.
    const BOOT: BootId = [7; 16];
    const OTHER_BOOT: BootId = [8; 16];

    /// [`choose_meta`] over the bytes of `file`, trusting the weak records of `trusted`.
    fn choose_in(file: &[u8], trusted: Option<BootId>) -> Result<Chosen> {
        choose_meta(
            |off| {
                let off = off as usize;
                Ok(file.get(off..off + META_LEN).map(|rec| rec.try_into().expect("128 bytes")))
            },
            trusted,
        )
    }

    /// [`choose_in`] in the running boot [`BOOT`].
    fn choose(file: &[u8]) -> Result<Chosen> {
        choose_in(file, Some(BOOT))
    }

    /// Restamp a record's CRC after changing a field.
    fn restamp(rec: &mut [u8; META_LEN]) {
        let crc = crc::crc32(&rec[..CRC_AT]);
        rec[CRC_AT..].copy_from_slice(&crc.to_le_bytes());
    }

    /// The record round-trips, and each kind of damage is refused with its own error.
    #[test]
    fn meta_encoding_round_trips_and_refuses_damage() {
        let free = TreeRoot {
            root: 7,
            root_txn: 1,
            height: 1,
            entries: 2,
            leaf_pages: 1,
            overflow_pages: 3,
            ..TreeRoot::EMPTY
        };
        let m = Meta { free, ..meta(8192, 42) };
        let rec = m.encode(STEADY);
        assert_eq!(Meta::decode(&rec).expect("decode"), (m, STEADY));
        assert_eq!(
            (CATALOG, FREE, FLAGS, BOOT_ID),
            (32..64, 64..96, 96..100, 100..116),
            "the layout"
        );
        assert_eq!(RESERVED, 116..124, "the layout");
        assert_eq!(rec[CATALOG], m.catalog.encode());
        assert_eq!(rec[FREE], free.encode());
        assert!(rec[FLAGS.start..RESERVED.end].iter().all(|&b| b == 0), "steady, no boot id");

        let damaged = |f: &dyn Fn(&mut [u8; META_LEN]), stamp: bool| {
            let mut bad = rec;
            f(&mut bad);
            if stamp {
                restamp(&mut bad);
            }
            Meta::decode(&bad)
        };
        assert!(matches!(damaged(&|r| r[3] ^= 1, true), Err(Error::NotTnLmdb)));
        let other = FORMAT_VERSION + 1;
        assert!(matches!(
            damaged(&|r| r[8..12].copy_from_slice(&other.to_le_bytes()), false),
            Err(Error::UnsupportedVersion(v)) if v == other
        ));
        assert!(matches!(
            damaged(&|r| r[8..12].copy_from_slice(&1_u32.to_le_bytes()), true),
            Err(Error::UnsupportedVersion(1))
        ));
        assert!(matches!(damaged(&|r| r[20] ^= 1, false), Err(Error::Corrupt(_))), "crc");
        assert!(matches!(damaged(&|r| r[CRC_AT] ^= 1, false), Err(Error::Corrupt(_))), "crc");
        for at in [RESERVED.start, 120, RESERVED.end - 1] {
            assert!(matches!(damaged(&|r| r[at] = 1, true), Err(Error::Corrupt(_))), "reserved");
        }
        for bit in 1..32 {
            let flags = (1_u32 << bit).to_le_bytes();
            let set = move |r: &mut [u8; META_LEN]| r[FLAGS].copy_from_slice(&flags);
            assert!(matches!(damaged(&set, true), Err(Error::Corrupt(_))), "flag bit {bit}");
        }
        let set_u32 = |off: usize, v: u32| {
            move |r: &mut [u8; META_LEN]| r[off..off + 4].copy_from_slice(&v.to_le_bytes())
        };
        for (what, f) in [
            ("page size", set_u32(12, 6000)),
            ("page size", set_u32(12, 2048)),
            ("page count", set_u32(24, 1)),
            ("table id", set_u32(28, 0)),
            ("catalog shape", set_u32(40, 0)),
            ("catalog branch count", set_u32(56, 0)),
            ("free leaf count", set_u32(92, 2)),
        ] {
            assert!(matches!(damaged(&f, true), Err(Error::Corrupt(_))), "{what}");
        }
    }

    /// The valid meta with the highest txn wins, whichever slot holds it, at every page size.
    #[test]
    fn the_newest_valid_meta_wins() {
        for p in PAGE_SIZES {
            let file = image(p, Some(meta(p, 5).encode(STEADY)), Some(meta(p, 6).encode(STEADY)));
            assert_eq!(
                choose(&file).expect("choose"),
                Chosen {
                    meta: meta(p, 6),
                    slots: [Slot::steady(5), Slot::steady(6)],
                    last_sync: None
                }
            );
            let file = image(p, Some(meta(p, 8).encode(STEADY)), Some(meta(p, 7).encode(STEADY)));
            assert_eq!(
                choose(&file).expect("choose"),
                Chosen {
                    meta: meta(p, 8),
                    slots: [Slot::steady(8), Slot::steady(7)],
                    last_sync: None
                }
            );
        }
    }

    /// A torn newest meta falls back to the other one, which stays the slot to keep; the torn
    /// slot is the next written.
    #[test]
    fn a_torn_newest_meta_falls_back() {
        let mut torn = meta(4096, 6).encode(STEADY);
        torn[17] ^= 0x40;
        let chosen =
            choose(&image(4096, Some(meta(4096, 5).encode(STEADY)), Some(torn))).expect("choose");
        assert_eq!(
            chosen,
            Chosen { meta: meta(4096, 5), slots: [Slot::steady(5), None], last_sync: None }
        );
        assert_eq!(slot_to_write(&chosen.slots), 1);

        // Meta 0 torn: meta 1 is found by probing its page size, at every page size.
        for p in PAGE_SIZES {
            let mut torn = meta(p, 9).encode(STEADY);
            torn[CRC_AT] ^= 1;
            let chosen =
                choose(&image(p, Some(torn), Some(meta(p, 8).encode(STEADY)))).expect("choose");
            assert_eq!(
                chosen,
                Chosen { meta: meta(p, 8), slots: [None, Slot::steady(8)], last_sync: None }
            );
            assert_eq!(slot_to_write(&chosen.slots), 0);
            // Zeroed meta 0 (a never-written page) probes the same way.
            let chosen = choose(&image(p, None, Some(meta(p, 8).encode(STEADY)))).expect("choose");
            assert_eq!(chosen.meta, meta(p, 8));
        }
    }

    /// A probed meta 1 counts only at the offset its own page size gives it.
    #[test]
    fn a_probed_meta_must_name_its_offset() {
        // An 8 KiB meta at offset 4096 is not meta 1 of any file.
        let mut file = vec![0; 3 * 8192];
        file[4096..4096 + META_LEN].copy_from_slice(&meta(8192, 3).encode(STEADY));
        assert!(matches!(choose(&file), Err(Error::Corrupt(_))));
        // Nor is a 4 KiB one at offset 8192.
        let mut file = vec![0; 3 * 8192];
        file[8192..8192 + META_LEN].copy_from_slice(&meta(4096, 3).encode(STEADY));
        assert!(matches!(choose(&file), Err(Error::Corrupt(_))));
        // The first record carrying the magic is meta 1: one valid at a larger page size is page
        // 2 of the 4 KiB file the first names, never read.
        file[4096..4096 + META_LEN].copy_from_slice(&meta(4096, 3).encode(STEADY));
        file[8192..8192 + META_LEN].copy_from_slice(&meta(8192, 4).encode(STEADY));
        assert_eq!(choose(&file).expect("choose").meta, meta(4096, 3));
    }

    /// With meta 0 invalid, the probe stops at meta 1, which carries the magic: the pages past
    /// it are never read as metas, whatever their bytes (a value's can hold the magic and
    /// another version, or a whole record naming its offset), and a damaged meta 1 is refused
    /// whatever follows it.
    #[test]
    fn a_probe_never_reads_the_pages_past_meta_1() {
        let other = FORMAT_VERSION + 1;
        for p in PAGE_SIZES {
            let mut torn = meta(p, 9).encode(STEADY);
            torn[CRC_AT] ^= 1;
            for later in PAGE_SIZES.into_iter().filter(|&q| q > p) {
                let mut newer = meta(later, 11).encode(STEADY);
                newer[8..12].copy_from_slice(&other.to_le_bytes());
                for planted in [newer, meta(later, 10).encode(STEADY)] {
                    let at = later as usize;
                    let mut file = image(p, Some(torn), Some(meta(p, 8).encode(STEADY)));
                    file.resize(file.len().max(at + META_LEN), 0);
                    file[at..at + META_LEN].copy_from_slice(&planted);
                    assert_eq!(
                        choose(&file).expect("opens at meta 1"),
                        Chosen {
                            meta: meta(p, 8),
                            slots: [None, Slot::steady(8)],
                            last_sync: None
                        },
                        "page size {p}, record at {later}"
                    );
                    // Meta 1 torn too: no valid meta, whatever the later page holds.
                    let p = p as usize;
                    file[p..p + META_LEN].copy_from_slice(&torn);
                    assert!(matches!(choose(&file), Err(Error::Corrupt(_))), "both torn");
                }
            }
        }
    }

    /// No valid meta fails closed: `Corrupt` when the magic is there, `NotTnLmdb` when it is not.
    #[test]
    fn no_valid_meta_fails_closed() {
        let mut bad0 = meta(4096, 5).encode(STEADY);
        bad0[30] ^= 1;
        let mut bad1 = meta(4096, 6).encode(STEADY);
        bad1[CRC_AT + 1] ^= 1;
        assert!(matches!(choose(&image(4096, Some(bad0), Some(bad1))), Err(Error::Corrupt(_))));
        assert!(matches!(choose(&image(4096, Some(bad0), None)), Err(Error::Corrupt(_))));
        assert!(matches!(choose(&image(4096, None, Some(bad1))), Err(Error::Corrupt(_))));
        assert!(matches!(choose(&image(4096, None, None)), Err(Error::NotTnLmdb)));
        assert!(matches!(choose(&vec![0xA5; 70_000]), Err(Error::NotTnLmdb)));
        assert!(matches!(choose(b"short"), Err(Error::NotTnLmdb)));
        let mut wrong_magic = meta(4096, 5).encode(STEADY);
        wrong_magic[0] = b'T';
        restamp(&mut wrong_magic);
        assert!(matches!(choose(&image(4096, Some(wrong_magic), None)), Err(Error::NotTnLmdb)));
    }

    /// Two valid metas must agree on the page size, and metas of one txn must be identical.
    #[test]
    fn valid_metas_must_agree() {
        let mut other_size = meta(4096, 6);
        other_size.page_size = 8192;
        let file = image(4096, Some(meta(4096, 5).encode(STEADY)), Some(other_size.encode(STEADY)));
        assert!(matches!(choose(&file), Err(Error::Corrupt(_))));

        let same =
            image(4096, Some(meta(4096, 5).encode(STEADY)), Some(meta(4096, 5).encode(STEADY)));
        assert_eq!(choose(&same).expect("identical").meta, meta(4096, 5));
        let differ = Meta { page_count: 10, ..meta(4096, 5) };
        let file = image(4096, Some(meta(4096, 5).encode(STEADY)), Some(differ.encode(STEADY)));
        assert!(matches!(choose(&file), Err(Error::Corrupt(_))));
    }

    /// A record of another format version refuses the file, in either slot, even next to a
    /// valid one.
    #[test]
    fn another_format_version_is_refused() {
        let other = FORMAT_VERSION + 1;
        let mut newer = meta(4096, 6).encode(STEADY);
        newer[8..12].copy_from_slice(&other.to_le_bytes());
        restamp(&mut newer);
        let ok = meta(4096, 5).encode(STEADY);
        let refused =
            |r: Result<Chosen>| matches!(r, Err(Error::UnsupportedVersion(v)) if v == other);
        assert!(refused(choose(&image(4096, Some(newer), Some(ok)))));
        assert!(refused(choose(&image(4096, Some(ok), Some(newer)))));
        assert!(refused(choose(&image(4096, None, Some(newer)))));
    }

    /// A record of format `version` is refused before its CRC, stamped or not, by `decode` and
    /// by `choose_meta`.
    fn assert_version_refused(version: u32) {
        for stamp in [false, true] {
            let mut rec = meta(4096, 5).encode(STEADY);
            rec[8..12].copy_from_slice(&version.to_le_bytes());
            if stamp {
                restamp(&mut rec);
            }
            let decoded = Meta::decode(&rec);
            assert!(
                matches!(decoded, Err(Error::UnsupportedVersion(v)) if v == version),
                "version {version}, stamped {stamp}"
            );
            let chosen = choose(&image(4096, Some(rec), Some(rec)));
            assert!(
                matches!(chosen, Err(Error::UnsupportedVersion(v)) if v == version),
                "version {version}, stamped {stamp}"
            );
        }
    }

    /// A version-2 record (a 56-byte table record, no nested cells) is refused before its CRC,
    /// stamped or not: the format is pre-release, with no migration.
    #[test]
    fn a_version_2_record_is_refused() {
        assert_eq!(FORMAT_VERSION, 6);
        assert_version_refused(2);
    }

    /// A version-3 record (tree pages CRC'd whole, their free gaps included) is refused before
    /// its CRC, stamped or not, never read as damage: the format is pre-release, with no
    /// migration.
    #[test]
    fn a_version_3_record_is_refused() {
        assert_eq!(FORMAT_VERSION, 6);
        assert_version_refused(3);
    }

    /// A version-4 record (leaves holding inline values only to `U / 4`, so its overflow cells
    /// for larger ones are not this build's canonical placement) is refused before its CRC,
    /// stamped or not, never read as damage: the format is pre-release, with no migration.
    #[test]
    fn a_version_4_record_is_refused() {
        assert_eq!(FORMAT_VERSION, 6);
        assert_version_refused(4);
    }

    /// A version-5 record (bytes 96..124 reserved, no weak record or boot id) is refused before
    /// its CRC, stamped or not, never read as damage: the format is pre-release, with no
    /// migration.
    #[test]
    fn a_version_5_record_is_refused() {
        assert_eq!(FORMAT_VERSION, 6);
        assert_version_refused(5);
    }

    /// A weak record and a steady one carrying a boot id round-trip with their tags, the flag at
    /// bit 0 of byte 96 and the id at 100..116, and the record's CRC covers both.
    #[test]
    fn a_tag_round_trips() {
        let m = meta(4096, 9);
        for tag in [Tag::weak(BOOT), Tag::steady(BOOT), Tag::weak([0; 16])] {
            let rec = m.encode(tag);
            assert_eq!(Meta::decode(&rec).expect("decode"), (m, tag));
            assert_eq!(rec[FLAGS], u32::from(tag.weak).to_le_bytes());
            assert_eq!(rec[BOOT_ID], tag.boot_id);
            for at in [FLAGS.start, BOOT_ID.start, BOOT_ID.end - 1] {
                let mut bad = rec;
                bad[at] ^= 2;
                assert!(matches!(Meta::decode(&bad), Err(Error::Corrupt(_))), "{tag:?} at {at}");
            }
        }
        assert_ne!(m.encode(Tag::weak(BOOT)), m.encode(Tag::steady(BOOT)));
    }

    /// A file image holding `r0` in slot 0 and `r1` in slot 1, at 4 KiB pages.
    fn slots_image(r0: [u8; META_LEN], r1: [u8; META_LEN]) -> Vec<u8> {
        image(4096, Some(r0), Some(r1))
    }

    /// A weak head is chosen exactly when it was written in the trusted boot (both ids known):
    /// with another boot, an unknown id on either side or nothing trusted, the steady slot beside
    /// it is, and both slots are reported as they are, in either order. A trusted head carries
    /// that steady meta as the last sync, which open falls back to; an untrusted one carries none.
    #[test]
    fn a_weak_head_is_trusted_only_in_its_own_boot() {
        let (s, w) = (meta(4096, 5), meta(4096, 8));
        let unknown = [0; 16];
        for (head_boot, trusted, trusts) in [
            (BOOT, Some(BOOT), true),
            (BOOT, Some(OTHER_BOOT), false),
            (BOOT, None, false),
            (unknown, None, false),
            (unknown, Some(unknown), false),
            (unknown, Some(BOOT), false),
        ] {
            let case = format!("{head_boot:?} trusted {trusted:?}");
            let (want, last_sync) = if trusts { (w, Some(s)) } else { (s, None) };
            let (steady_rec, weak_rec) =
                (s.encode(Tag::steady(BOOT)), w.encode(Tag::weak(head_boot)));
            let chosen = choose_in(&slots_image(steady_rec, weak_rec), trusted).expect(&case);
            assert_eq!(
                chosen,
                Chosen { meta: want, slots: [Slot::steady(5), Slot::weak(8)], last_sync },
                "{case}"
            );
            let chosen = choose_in(&slots_image(weak_rec, steady_rec), trusted).expect(&case);
            assert_eq!(
                chosen,
                Chosen { meta: want, slots: [Slot::weak(8), Slot::steady(5)], last_sync },
                "{case}"
            );
        }
    }

    /// A weak head with no valid steady record beside it (torn, damaged, missing or weak too) is
    /// `Corrupt`, whether trusted or not: never the weak head alone. A weak record older than the
    /// steady head is passed over as any older slot is.
    #[test]
    fn a_weak_head_needs_a_steady_slot_beside_it() {
        let w = meta(4096, 8).encode(Tag::weak(BOOT));
        let mut torn = meta(4096, 5).encode(Tag::steady(BOOT));
        torn[30] ^= 1;
        let older_weak = meta(4096, 5).encode(Tag::weak(BOOT));
        for trusted in [Some(BOOT), Some(OTHER_BOOT), None] {
            for file in [
                slots_image(w, torn),
                slots_image(torn, w),
                slots_image(w, older_weak),
                slots_image(w, w),
                image(4096, Some(w), None),
                image(4096, None, Some(w)),
            ] {
                assert!(
                    matches!(choose_in(&file, trusted), Err(Error::Corrupt(_))),
                    "trusted {trusted:?}"
                );
            }
            let file = slots_image(meta(4096, 9).encode(Tag::steady(BOOT)), older_weak);
            let chosen = choose_in(&file, trusted).expect("the steady head");
            assert_eq!(
                chosen,
                Chosen {
                    meta: meta(4096, 9),
                    slots: [Slot::steady(9), Slot::weak(5)],
                    last_sync: None
                }
            );
        }
    }

    /// With both slots steady the newest wins, whatever the boot ids and the trusted boot.
    #[test]
    fn with_both_slots_steady_the_newest_wins_in_any_boot() {
        let file = slots_image(
            meta(4096, 5).encode(Tag::steady(OTHER_BOOT)),
            meta(4096, 6).encode(STEADY),
        );
        for trusted in [Some(BOOT), Some(OTHER_BOOT), None] {
            let chosen = choose_in(&file, trusted).expect("choose");
            assert_eq!(
                chosen,
                Chosen {
                    meta: meta(4096, 6),
                    slots: [Slot::steady(5), Slot::steady(6)],
                    last_sync: None
                }
            );
        }
    }

    /// Every record, weak or steady, goes into the slot that does not hold the newest steady one:
    /// beside a weak slot, never the steady one; an invalid slot counts as holding none.
    #[test]
    fn the_steady_slot_is_never_written_while_the_other_is_weak() {
        assert_eq!(slot_to_write(&[Slot::steady(5), Slot::weak(8)]), 1);
        assert_eq!(slot_to_write(&[Slot::weak(8), Slot::steady(5)]), 0);
        assert_eq!(slot_to_write(&[Slot::weak(3), Slot::steady(5)]), 0, "an older weak slot too");
        assert_eq!(slot_to_write(&[Slot::steady(5), Slot::weak(3)]), 1, "an older weak slot too");
        assert_eq!(slot_to_write(&[None, Slot::steady(5)]), 0);
        assert_eq!(slot_to_write(&[Slot::steady(5), None]), 1);
    }

    /// The commit writes over the lower txn, an invalid slot first, slot 1 on a tie.
    #[test]
    fn the_lower_slot_is_written() {
        assert_eq!(slot_to_write(&[Slot::steady(4), Slot::steady(5)]), 0);
        assert_eq!(slot_to_write(&[Slot::steady(5), Slot::steady(4)]), 1);
        assert_eq!(slot_to_write(&[Slot::steady(0), Slot::steady(0)]), 1);
        assert_eq!(slot_to_write(&[None, Slot::steady(4)]), 0);
        assert_eq!(slot_to_write(&[Slot::steady(4), None]), 1);
    }
}
