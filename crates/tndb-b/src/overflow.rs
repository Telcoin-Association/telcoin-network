//! Overflow runs: a value too large for its leaf ([`crate::page::Geometry::place`]) lives in a
//! run of `n` contiguous pages, and its leaf cell holds a 12-byte reference to it ([`Ovf`]):
//!
//! ```text
//! overflow cell value (12): ovf_pgno u32 | ovf_txn u32 (low 32 bits of the run's txn) | len u32
//!
//! run (n·P bytes, n = ceil((28 + len) / P)):
//!   @0  pgno u32 | @4 kind u8 = 3 | @5 flags u8 = 0 | @6 reserved u16 = 0 | @8 txn u64
//!   @16 len u32  | @20 npages u32 | @24 value (len bytes) | zero pad | CRC u32 at n·P − 4
//! ```
//!
//! **One CRC per run.** The trailer covers `[0, n·P − 4)`: pages 2..n have no header of their
//! own, so only the run's CRC ties them to it. All zeros means dirty (a run being written), as for
//! a tree page. [`crate::page::kind`] refuses kind 3, so a tree pointer that lands on a run is
//! [`Error::Corrupt`].
//!
//! **Runs are immutable.** A run is written once, before the leaf names it, and never changed: a
//! copy of the leaf copies the 12-byte reference and shares the run. A replace or a delete frees
//! the old run (as `n` single pages).
//!
//! **Reading** ([`checked_run`]) checks the reference against the run's header (page number,
//! kind, flags, reserved bytes, txn identity, length, page count), the range against the
//! snapshot, and, unless the run is private to the writer, the non-zero trailer and, for a run of
//! more than one page, the full CRC: without it a reused or redirected middle page would return
//! wrong data. A 1-page run has no page without a header, so its header checks catch a reused
//! page, as a tree page's do; damage at rest inside it is [`crate::Env::check`]'s to find, as it is
//! inside a leaf ([`RunCrc`]).

use crate::{
    crc::{self, CrcState},
    error::{Error, Result},
    page::TRAILER,
};

/// The largest value a table accepts (16 MiB): the run's `len` field never exceeds it.
pub(crate) const MAX_VALUE: usize = 16 << 20;

/// The bytes of an overflow cell's value: `ovf_pgno u32 | ovf_txn u32 | len u32`.
pub(crate) const OVF_CELL: usize = 12;

/// How much of a run's CRC [`checked_run`] verifies, past the non-zero trailer of a published run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RunCrc {
    /// None, for a caller that never reads the value: a walk that frees the run (`free_subtree`,
    /// a replace's old run) and the copy-time rule's check of the run's txn
    /// (`check_cells_naming_this_txn`).
    Skip,
    /// The full CRC of a run of more than one page: a value read. Pages 2..n carry no header, so
    /// only the CRC ties them to the run; a 1-page run is all header-checked page, which catches
    /// a reused page as a tree page's header does (tree pages skip the CRC on a read too).
    MultiPage,
    /// The full CRC of every run: [`crate::Env::check`], which also finds damage at rest inside a
    /// 1-page run.
    Always,
}

/// A run's header bytes, before its value.
pub(crate) const RUN_HEADER: usize = 24;

/// A run's kind tag (1 and 2 are a branch and a leaf).
pub(crate) const KIND_RUN: u8 = 3;

const PGNO_OFF: usize = 0;
const KIND_OFF: usize = 4;
const FLAGS_OFF: usize = 5;
const RESERVED_OFF: usize = 6;
const TXN_OFF: usize = 8;
const LEN_OFF: usize = 16;
const NPAGES_OFF: usize = 20;

/// An overflow cell's reference to its run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Ovf {
    /// The run's first page.
    pub(crate) pgno: u32,
    /// The low 32 bits of the txn that wrote the run (the identity check, as a branch cell's
    /// `child_txn`).
    pub(crate) txn: u32,
    /// The value's length.
    pub(crate) len: u32,
}

impl Ovf {
    /// Decode an overflow cell's 12 value bytes.
    pub(crate) fn decode(bytes: &[u8; OVF_CELL]) -> Self {
        let u32_at = |off: usize| {
            u32::from_le_bytes(bytes[off..off + 4].try_into().expect("a 4-byte field"))
        };
        Self { pgno: u32_at(0), txn: u32_at(4), len: u32_at(8) }
    }

    /// The 12 value bytes of an overflow cell.
    pub(crate) fn encode(&self) -> [u8; OVF_CELL] {
        let mut out = [0; OVF_CELL];
        out[0..4].copy_from_slice(&self.pgno.to_le_bytes());
        out[4..8].copy_from_slice(&self.txn.to_le_bytes());
        out[8..12].copy_from_slice(&self.len.to_le_bytes());
        out
    }
}

/// The pages of a run holding a `len`-byte value: `ceil((28 + len) / page_size)`.
pub(crate) fn run_pages(len: usize, page_size: usize) -> usize {
    (RUN_HEADER + TRAILER).saturating_add(len).div_ceil(page_size)
}

/// Write the run of `value` into `run` (exactly [`run_pages`] pages of `page_size` bytes): the
/// header for page `pgno` and txn `txn`, the value, a zero pad and a zero (dirty) trailer. Every
/// byte is written, so the pages need not be zero.
pub(crate) fn encode_run(run: &mut [u8], page_size: usize, pgno: u32, txn: u64, value: &[u8]) {
    let n = run_pages(value.len(), page_size);
    debug_assert_eq!(run.len(), n * page_size, "a run of {n} pages");
    run[PGNO_OFF..PGNO_OFF + 4].copy_from_slice(&pgno.to_le_bytes());
    run[KIND_OFF] = KIND_RUN;
    run[FLAGS_OFF] = 0;
    run[RESERVED_OFF..RESERVED_OFF + 2].fill(0);
    run[TXN_OFF..TXN_OFF + 8].copy_from_slice(&txn.to_le_bytes());
    run[LEN_OFF..LEN_OFF + 4].copy_from_slice(&(value.len() as u32).to_le_bytes());
    run[NPAGES_OFF..NPAGES_OFF + 4].copy_from_slice(&(n as u32).to_le_bytes());
    let end = RUN_HEADER + value.len();
    run[RUN_HEADER..end].copy_from_slice(value);
    run[end..].fill(0);
}

/// The value of a run [`checked_run`] returned for `ovf`.
pub(crate) fn value(run: &[u8], ovf: Ovf) -> &[u8] {
    &run[RUN_HEADER..RUN_HEADER + ovf.len as usize]
}

/// True if the bytes between a checked run's value and its trailer are all zero (the encoding
/// [`encode_run`] writes; [`crate::Env::check`] requires it).
pub(crate) fn pad_is_zero(run: &[u8], ovf: Ovf) -> bool {
    run[RUN_HEADER + ovf.len as usize..run.len() - TRAILER].iter().all(|&b| b == 0)
}

/// The txn in a run's header (the first page's bytes suffice).
pub(crate) fn run_txn(run: &[u8]) -> u64 {
    u64::from_le_bytes(run[TXN_OFF..TXN_OFF + 8].try_into().expect("an 8-byte field"))
}

/// The run `ovf` names, after every check a read makes, shared by every
/// [`crate::tree::PageRead`] store: `slice(pgno, len)` maps the run's bytes (`None` if not
/// mapped). The run must lie in `2..page_count` (a checked add), hold `ovf.pgno`, kind 3, flags 0
/// and zero reserved bytes in its header, a txn no later than `snapshot_txn` whose low 32 bits are
/// `ovf.txn`, and the length and page count `ovf` implies (`len <=` [`MAX_VALUE`]). A run that is
/// not `private` to the writer must also carry a non-zero trailer and a valid CRC over the whole
/// run when `verify` asks for it. Any failure is [`Error::Corrupt`].
///
/// Every read of a value verifies the CRC of a run of more than one page ([`RunCrc::MultiPage`]):
/// pages 2..n of a run carry no header, so a reused or redirected middle page is caught by the CRC
/// alone. A walk that only frees the run checks the header and identity and skips the CRC.
pub(crate) fn checked_run<'a>(
    ovf: Ovf,
    page_size: usize,
    page_count: u32,
    snapshot_txn: u64,
    private: bool,
    verify: RunCrc,
    slice: impl FnOnce(u32, usize) -> Option<&'a [u8]>,
) -> Result<&'a [u8]> {
    let len = ovf.len as usize;
    if len > MAX_VALUE {
        return Err(run_error(ovf, format_args!("a value of {len} bytes is over the limit")));
    }
    let n = run_pages(len, page_size);
    let in_file = u32::try_from(n)
        .ok()
        .and_then(|n| ovf.pgno.checked_add(n))
        .is_some_and(|end| ovf.pgno >= 2 && end <= page_count);
    if !in_file {
        return Err(run_error(
            ovf,
            format_args!("{n} pages are not tree pages of a file of {page_count} pages"),
        ));
    }
    let run = slice(ovf.pgno, n * page_size)
        .ok_or_else(|| run_error(ovf, format_args!("{n} pages are not mapped")))?;
    let u32_at = |off: usize| u32::from_le_bytes(run[off..off + 4].try_into().expect("4 bytes"));
    let txn = run_txn(run);
    if u32_at(PGNO_OFF) != ovf.pgno
        || run[KIND_OFF] != KIND_RUN
        || run[FLAGS_OFF] != 0
        || run[RESERVED_OFF..RESERVED_OFF + 2] != [0, 0]
    {
        return Err(run_error(ovf, "the header is not this run's"));
    }
    if txn > snapshot_txn {
        return Err(run_error(
            ovf,
            format_args!("written by txn {txn}, after the reading snapshot's txn {snapshot_txn}"),
        ));
    }
    if txn as u32 != ovf.txn {
        return Err(run_error(
            ovf,
            format_args!("txn {txn} is not the run its cell names: a redirected or stale pointer"),
        ));
    }
    if u32_at(LEN_OFF) as usize != len || u32_at(NPAGES_OFF) as usize != n {
        return Err(run_error(ovf, "its length or page count is not its cell's"));
    }
    if !private {
        if crc::crc_is_zero(run) {
            return Err(run_error(ovf, "published but never stamped (zero CRC)"));
        }
        let full = match verify {
            RunCrc::Skip => false,
            RunCrc::MultiPage => n > 1,
            RunCrc::Always => true,
        };
        if full && crc::crc_state(run) != CrcState::Valid {
            return Err(run_error(ovf, "fails its CRC (at-rest corruption)"));
        }
    }
    Ok(run)
}

/// [`checked_run`]'s [`Error::Corrupt`].
#[cold]
#[inline(never)]
fn run_error(ovf: Ovf, what: impl std::fmt::Display) -> Error {
    Error::Corrupt(format!("overflow run {} (len {}, txn {}): {what}", ovf.pgno, ovf.len, ovf.txn))
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: usize = 4096;

    /// A txn whose high bits are set, so a 32-bit identity differs from the full id.
    const TXN: u64 = 0x1_0000_0007;

    /// A value spanning three pages, every byte non-zero.
    fn three_page_value() -> Vec<u8> {
        (0..3 * P - RUN_HEADER - TRAILER - 100).map(|i| (i % 251) as u8 + 1).collect()
    }

    /// A file image of pages `0..2 + n` with `value`'s run, written by [`TXN`], at page 2 and
    /// stamped; the cell naming it; the image's page count.
    fn image(value: &[u8]) -> (Vec<u8>, Ovf, u32) {
        let n = run_pages(value.len(), P);
        let mut file = vec![0; (2 + n) * P];
        let run = &mut file[2 * P..];
        encode_run(run, P, 2, TXN, value);
        crc::add_crc32_nonzero(run);
        let ovf = Ovf { pgno: 2, txn: TXN as u32, len: value.len() as u32 };
        (file, ovf, (2 + n) as u32)
    }

    /// [`checked_run`] over a file image: `(ovf, page_count, snapshot_txn, private, verify)`.
    fn read(
        file: &[u8],
        (ovf, page_count, snapshot, private, verify): (Ovf, u32, u64, bool, RunCrc),
    ) -> Result<&[u8]> {
        checked_run(ovf, P, page_count, snapshot, private, verify, |p, len| {
            let start = p as usize * P;
            file.get(start..start.checked_add(len)?)
        })
    }

    /// A run reads back its value, with a zero pad; the cell's 12 bytes round-trip; a private run
    /// is read before its CRC is stamped; the page count covers the header and the trailer.
    #[test]
    fn a_run_reads_back_its_value() {
        let bytes = three_page_value();
        let (mut file, ovf, count) = image(&bytes);
        assert_eq!(count, 5, "three pages");
        assert_eq!(Ovf::decode(&ovf.encode()), ovf);
        for verify in [RunCrc::Always, RunCrc::MultiPage, RunCrc::Skip] {
            let run = read(&file, (ovf, count, TXN, false, verify)).expect("a stamped run");
            assert_eq!((value(run, ovf), run.len()), (bytes.as_slice(), 3 * P));
            assert!(pad_is_zero(run, ovf));
            assert_eq!(run_txn(run), TXN);
        }
        crc::zero_crc(&mut file[2 * P..]);
        let run =
            read(&file, (ovf, count, TXN, true, RunCrc::MultiPage)).expect("a private, dirty run");
        assert_eq!(value(run, ovf), bytes.as_slice());

        assert_eq!(run_pages(0, P), 1);
        assert_eq!(run_pages(P - RUN_HEADER - TRAILER, P), 1);
        assert_eq!(run_pages(P - RUN_HEADER - TRAILER + 1, P), 2);
        assert_eq!(run_pages(MAX_VALUE, P), MAX_VALUE / P + 1);
    }

    /// The test page store reads runs through [`checked_run`] against its snapshot: the writer
    /// reads its own run before the commit stamps it, a reader of the last commit refuses it
    /// (written later), a published run is immutable, and after the commit the reader and the
    /// writer both verify its one CRC, so a flipped byte in a middle page is `Corrupt`.
    #[test]
    fn the_test_store_reads_runs_against_its_snapshot() {
        use crate::tree::{test_harness::MemStore, PageRead, PageWrite};
        let mut store = MemStore::new(P as u32);
        let bytes = three_page_value();
        let n = run_pages(bytes.len(), P) as u32;
        let r = store.alloc_run(n).expect("run");
        let txn = store.txn();
        encode_run(store.run_mut(r, n).expect("a private run"), P, r, txn, &bytes);
        let ovf = Ovf { pgno: r, txn: txn as u32, len: bytes.len() as u32 };
        assert_eq!(store.overflow(ovf).expect("the writer's own run"), bytes.as_slice());
        let got = store.reader().overflow(ovf).map(<[u8]>::len);
        assert!(matches!(got, Err(Error::Corrupt(_))), "written after the snapshot: {got:?}");

        store.commit_like();
        assert!(store.run_mut(r, n).is_err(), "a published run is immutable");
        assert_eq!(store.reader().overflow(ovf).expect("published"), bytes.as_slice());
        assert_eq!(store.overflow(ovf).expect("the writer, published"), bytes.as_slice());
        store.raw_mut(r + 1)[100] ^= 1;
        for got in [store.reader().overflow(ovf).map(drop), store.overflow(ovf).map(drop)] {
            assert!(matches!(got, Err(Error::Corrupt(_))), "a middle page flipped: {got:?}");
        }
    }

    /// Every kind of damage to a run, or a cell that does not name it, is `Corrupt`: each header
    /// field (re-stamped, so only the header check can see it), a zero trailer, a CRC flip in a
    /// middle page, a range past the file's end or past the last page number, a run written after
    /// the snapshot, and a cell whose txn, length or limit is not the run's.
    #[test]
    fn each_damaged_run_is_corrupt() {
        let (file, ovf, count) = image(&three_page_value());
        let ok = (ovf, count, TXN, false, RunCrc::MultiPage);
        read(&file, ok).expect("the undamaged run");

        // Header fields, each changed and the CRC re-stamped.
        let fields: [(&str, usize, u8); 9] = [
            ("pgno", PGNO_OFF, 1),
            ("kind (a leaf)", KIND_OFF, KIND_RUN ^ 2),
            ("flags", FLAGS_OFF, 1),
            ("reserved, low byte", RESERVED_OFF, 1),
            ("reserved, high byte", RESERVED_OFF + 1, 0x80),
            ("txn, low 32 bits", TXN_OFF, 1),
            ("txn, past the snapshot", TXN_OFF + 4, 2),
            ("len", LEN_OFF, 1),
            ("npages", NPAGES_OFF, 1),
        ];
        for (name, off, xor) in fields {
            let mut bad = file.clone();
            bad[2 * P + off] ^= xor;
            crc::add_crc32_nonzero(&mut bad[2 * P..]);
            for verify in [RunCrc::Always, RunCrc::MultiPage, RunCrc::Skip] {
                let got = read(&bad, (ovf, count, TXN, false, verify)).map(<[u8]>::len);
                assert!(matches!(got, Err(Error::Corrupt(_))), "{name} ({verify:?}): {got:?}");
            }
        }

        // The trailer and the CRC.
        let mut zero = file.clone();
        crc::zero_crc(&mut zero[2 * P..]);
        for verify in [RunCrc::Always, RunCrc::MultiPage, RunCrc::Skip] {
            let got = read(&zero, (ovf, count, TXN, false, verify)).map(<[u8]>::len);
            assert!(matches!(got, Err(Error::Corrupt(_))), "zero trailer: {got:?}");
        }
        let mut flipped = file.clone();
        flipped[3 * P + 17] ^= 0x10;
        let got = read(&flipped, ok).map(<[u8]>::len);
        assert!(matches!(got, Err(Error::Corrupt(_))), "middle page flip: {got:?}");
        assert!(
            read(&flipped, (ovf, count, TXN, false, RunCrc::Skip)).is_ok(),
            "a free skips the CRC"
        );

        // The cell and the snapshot.
        let cases = [
            ("range past the end", (ovf, count - 1, TXN, false, RunCrc::MultiPage)),
            ("a meta page", (Ovf { pgno: 1, ..ovf }, count, TXN, false, RunCrc::MultiPage)),
            (
                "page number overflow",
                (Ovf { pgno: u32::MAX - 1, ..ovf }, u32::MAX, TXN, false, RunCrc::MultiPage),
            ),
            ("after the snapshot", (ovf, count, TXN - 1, false, RunCrc::MultiPage)),
            (
                "txn32 mismatch",
                (Ovf { txn: ovf.txn ^ 1, ..ovf }, count, TXN, false, RunCrc::MultiPage),
            ),
            (
                "len, same pages",
                (Ovf { len: ovf.len - 1, ..ovf }, count, TXN, false, RunCrc::MultiPage),
            ),
            ("len, fewer pages", (Ovf { len: 100, ..ovf }, count, TXN, false, RunCrc::MultiPage)),
            (
                "len over the limit",
                (Ovf { len: MAX_VALUE as u32 + 1, ..ovf }, count, TXN, false, RunCrc::MultiPage),
            ),
        ];
        let short = &file[..file.len() - 1];
        let got = read(short, ok).map(<[u8]>::len);
        assert!(matches!(got, Err(Error::Corrupt(_))), "not mapped: {got:?}");
        for (name, args) in cases {
            let got = read(&file, args).map(<[u8]>::len);
            assert!(matches!(got, Err(Error::Corrupt(_))), "{name}: {got:?}");
            let private = (args.0, args.1, args.2, true, args.4);
            let got = read(&file, private).map(<[u8]>::len);
            assert!(matches!(got, Err(Error::Corrupt(_))), "{name}, private: {got:?}");
        }
    }
}
