//! Commit step 3's CRCs on several threads, for [`crate::env::Writer::stamp_private`].
//!
//! A commit stamps one CRC per unit ([`Unit`]): a tree page, whose CRC skips its free gap
//! ([`page::crc`]), or an overflow run as a whole ([`crate::alloc::PrivatePages::units`]). Its
//! units may total hundreds of MiB, and one thread computing every CRC was most of a large
//! commit's CPU time. From [`threads`]' threshold up, the writer computes the CRCs here
//! ([`crcs`]): it lends the threads read-only slices of the units
//! ([`crate::file::PageFile::pages`]), each thread computes the CRCs of a contiguous share of
//! about equal unit bytes, and once every thread is done the writer stores each CRC into its
//! unit's trailer itself, in unit order, through `PageFile::pages_mut` ([`put_crc`]). The threads
//! only read: no page changes while they run, and they need no `unsafe`.
//!
//! The shares are balanced by the units' bytes, not by the bytes hashed, which a page's gap
//! makes fewer: weighing by those would read every page's header on the writer's thread before
//! the threads start, and a commit's gaps spread evenly over its pages.
//!
//! The result is the serial loop's ([`stamp`] on each unit): the same bytes, stored in the same
//! order, with the same errors, which only the stores can raise. A failed
//! thread spawn computes its share on the writer's thread; a panic on a thread resumes on the
//! writer's thread, as the serial loop's would have happened there.

use std::{
    num::NonZeroUsize,
    panic,
    sync::OnceLock,
    thread::{self, ScopedJoinHandle},
};

use crate::{crc, page};

/// The unit bytes each thread gets at least: [`threads`] gives a second thread from twice this,
/// and one more per this many bytes after that.
const BYTES_PER_THREAD: usize = 1 << 20;

/// The most threads a commit's CRCs use, the writer's own included. A commit of ~100k 4 KiB tree
/// pages stamps little faster past 8 threads, the CRC being bound by memory there (the
/// `stamp_probe` bench), and the commit shares the machine with the node's other work, so it
/// takes no more.
const MAX_THREADS: usize = 8;

/// How many threads should compute the CRCs of `bytes` bytes of units: 1 (the serial loop) below
/// twice [`BYTES_PER_THREAD`], else one per [`BYTES_PER_THREAD`], at most [`MAX_THREADS`] and the
/// CPUs this process may use (`std::thread::available_parallelism`).
pub(crate) fn threads(bytes: usize) -> usize {
    (bytes / BYTES_PER_THREAD).clamp(1, max_threads())
}

/// [`MAX_THREADS`] capped by `std::thread::available_parallelism`, which reads the CPU affinity
/// and, on Linux, the cgroup quota files: read once, at the first commit ([`threads`] asks for
/// it at every commit that stamps pages, small ones included).
fn max_threads() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    *MAX.get_or_init(|| {
        thread::available_parallelism().map_or(1, NonZeroUsize::get).min(MAX_THREADS)
    })
}

/// One unit a commit stamps, whole (its payload and its 4-byte trailer).
#[derive(Clone, Copy, Debug)]
pub(crate) enum Unit<'u> {
    /// A tree page: its CRC is [`page::crc`], which skips the page's free gap.
    Page(&'u [u8]),
    /// An overflow run: its CRC covers all of it but its trailer, as
    /// [`crc::add_crc32_nonzero`] stamps it.
    Run(&'u [u8]),
}

impl Unit<'_> {
    /// The unit's bytes, its trailer included.
    fn bytes(&self) -> &[u8] {
        match *self {
            Unit::Page(bytes) | Unit::Run(bytes) => bytes,
        }
    }

    /// The CRC [`stamp`] stores in the unit's trailer.
    fn crc(&self) -> u32 {
        match *self {
            Unit::Page(page) => page::crc(page),
            Unit::Run(run) => run_crc(run),
        }
    }
}

/// Stamp one unit's trailer, the serial loop's step: tree page `unit` with [`page::stamp`], run
/// `unit` (`run`) with [`crc::add_crc32_nonzero`]. Each is a no-op on fewer than 5 bytes.
pub(crate) fn stamp(unit: &mut [u8], run: bool) {
    if run {
        crc::add_crc32_nonzero(unit);
    } else {
        page::stamp(unit);
    }
}

/// The CRC [`stamp`] stores on each of `units`, in their order, computed on up to `threads`
/// threads: the current one and up to `threads - 1` scoped ones, each over a contiguous share of
/// [`shares`].
pub(crate) fn crcs(units: &[Unit<'_>], threads: usize) -> Vec<u32> {
    /// A share left to another thread, or to this one if that thread could not be spawned.
    enum Share<'s, 'u> {
        Spawned(ScopedJoinHandle<'s, Vec<u32>>),
        Here(&'u [Unit<'u>]),
    }
    let mut shares = shares(units, threads).into_iter();
    let mut out = Vec::with_capacity(units.len());
    thread::scope(|s| {
        let first = shares.next().unwrap_or_default();
        let rest: Vec<Share<'_, '_>> = shares
            .map(|share| {
                thread::Builder::new()
                    .name("tn-lmdb-stamp".into())
                    .spawn_scoped(s, move || share.iter().map(Unit::crc).collect())
                    .map_or(Share::Here(share), Share::Spawned)
            })
            .collect();
        out.extend(first.iter().map(Unit::crc));
        for share in rest {
            match share {
                Share::Spawned(handle) => {
                    out.extend(handle.join().unwrap_or_else(|p| panic::resume_unwind(p)));
                }
                Share::Here(share) => out.extend(share.iter().map(Unit::crc)),
            }
        }
    });
    out
}

/// `units` split into at most `threads` contiguous, non-empty shares of about equal unit bytes:
/// share `k` ends at the first unit at which the running total reaches `(k + 1) / threads` of
/// the whole, and the last share takes the rest.
fn shares<'u>(units: &'u [Unit<'u>], threads: usize) -> Vec<&'u [Unit<'u>]> {
    let threads = threads.max(1) as u128;
    let total: u128 = units.iter().map(|u| u.bytes().len() as u128).sum();
    let mut shares = Vec::new();
    let (mut start, mut bytes) = (0, 0u128);
    for (i, unit) in units.iter().enumerate() {
        bytes += unit.bytes().len() as u128;
        let closed = shares.len() as u128 + 1;
        if closed < threads && bytes * threads >= total * closed {
            shares.push(&units[start..=i]);
            start = i + 1;
        }
    }
    if start < units.len() {
        shares.push(&units[start..]);
    }
    shares
}

/// The CRC [`crc::add_crc32_nonzero`] stamps on `run`: the CRC-32 of all but its last 4 bytes,
/// where a CRC of 0 becomes the all-ones sentinel, as `crc`'s non-zero stamp maps it (a stamped
/// trailer never reads as the all-zero dirty marker).
fn run_crc(run: &[u8]) -> u32 {
    match crc::crc32(&run[..run.len().saturating_sub(4)]) {
        0 => u32::MAX,
        crc => crc,
    }
}

/// Store `crc` (from [`crcs`]) in `unit`'s 4-byte trailer, as [`crc::add_crc32_nonzero`] does;
/// like it, a no-op on fewer than 5 bytes.
pub(crate) fn put_crc(unit: &mut [u8], crc: u32) {
    let len = unit.len();
    debug_assert!(len >= 5, "put_crc needs at least 5 bytes (>=1 payload byte + 4 crc bytes)");
    if len >= 5 {
        unit[len - 4..].copy_from_slice(&crc.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tree page, in a shape of units.
    const P: (usize, bool) = (1, false);

    /// A run of `n` pages, in a shape of units.
    const fn r(n: usize) -> (usize, bool) {
        (n, true)
    }

    /// Units of the given `(pages, run)` shapes, laid out one after another in one buffer of
    /// 4 KiB pages, as a commit's units lie in the file; their bytes differ by `seed`. The tree
    /// pages' headers take turns: a free gap, a full page (an empty gap), slots past `upper`, and
    /// `upper` past the trailer (the last two hashed whole, [`page::free_gap`]).
    fn buffer(units: &[(usize, bool)], seed: u32) -> (Vec<u8>, Vec<(usize, bool)>) {
        let lens: Vec<(usize, bool)> = units.iter().map(|&(n, run)| (n * 4096, run)).collect();
        let mut x = seed | 1;
        let mut bytes: Vec<u8> = (0..lens.iter().map(|u| u.0).sum())
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                x as u8
            })
            .collect();
        let mut at = 0;
        for (i, &(len, run)) in lens.iter().enumerate() {
            if !run {
                let nkeys = 20 + 7 * i % 300;
                let lower = page::HEADER + page::SLOT * nkeys;
                let upper = match i % 4 {
                    0 => lower + 1000 + 37 * i % 1500,
                    1 => lower,
                    2 => lower - 1,
                    _ => 4096 - page::TRAILER + 1,
                };
                bytes[at + 6..at + 8].copy_from_slice(&(nkeys as u16).to_le_bytes());
                bytes[at + 16..at + 18].copy_from_slice(&(upper as u16).to_le_bytes());
            }
            at += len;
        }
        (bytes, lens)
    }

    /// `bytes` cut into units of `lens` bytes, each tagged a page or a run.
    fn units<'b>(bytes: &'b [u8], lens: &[(usize, bool)]) -> Vec<Unit<'b>> {
        let mut rest = bytes;
        lens.iter()
            .map(|&(len, run)| {
                let (unit, tail) = rest.split_at(len);
                rest = tail;
                if run {
                    Unit::Run(unit)
                } else {
                    Unit::Page(unit)
                }
            })
            .collect()
    }

    /// `bytes` stamped by the serial loop, [`stamp`] on each unit, and by [`crcs`] on `threads`
    /// threads then [`put_crc`] on each unit.
    fn both_stamps(
        mut bytes: Vec<u8>,
        lens: &[(usize, bool)],
        threads: usize,
    ) -> (Vec<u8>, Vec<u8>) {
        let crcs = crcs(&units(&bytes, lens), threads);
        assert_eq!(crcs.len(), lens.len(), "one CRC per unit");
        let mut serial = bytes.clone();
        let mut rest = &mut serial[..];
        for &(len, run) in lens {
            let (unit, tail) = rest.split_at_mut(len);
            stamp(unit, run);
            rest = tail;
        }
        let mut rest = &mut bytes[..];
        for (&(len, _), crc) in lens.iter().zip(crcs) {
            let (unit, tail) = rest.split_at_mut(len);
            put_crc(unit, crc);
            rest = tail;
        }
        (serial, bytes)
    }

    #[test]
    fn crcs_stamp_the_bytes_of_the_serial_loop() {
        // Tree pages (with a free gap, full, and hashed whole) and runs of 1 to 9 pages in every
        // order, from no unit to more units than threads, on 1 to 9 threads: the stamped bytes
        // are the serial loop's, a page's CRC skipping its gap and a run's covering it all.
        let shapes: [&[(usize, bool)]; 8] = [
            &[],
            &[P],
            &[r(9)],
            &[P, P],
            &[r(1), P, r(1)],
            &[r(3), P, P, r(9), P, r(2)],
            &[P, P, P, P, P, P, P, P, P, P, P, P, P],
            &[r(9), P, P, P, P, P, P, P, P, P, P, P, P, P, P, P, P, P, P, r(4), P, P, r(2), r(7)],
        ];
        for (s, shape) in shapes.iter().enumerate() {
            for threads in 1..=9 {
                let (bytes, lens) = buffer(shape, s as u32 * 31 + threads as u32);
                let (serial, parallel) = both_stamps(bytes, &lens, threads);
                assert!(serial == parallel, "units {shape:?} on {threads} threads");
            }
        }
    }

    #[test]
    fn a_zero_crc_payload_gets_the_serial_loops_sentinel() {
        // A unit whose payload's CRC-32 is 0 is stamped all ones, never the all-zero dirty
        // trailer, as `crc::add_crc32_nonzero` stamps it. The payload ends in 4 bytes solved for
        // that: the CRC register run back, through 32 zero bits of the reflected IEEE polynomial,
        // from the all-ones value that finishes as a CRC of 0.
        const POLY: u32 = 0xEDB8_8320;
        let (mut bytes, lens) = buffer(&[P, r(1), r(2)], 7);
        let payload = &mut bytes[4096..4096 + 4096 - 4];
        let reg = !crc::crc32(&payload[..payload.len() - 4]);
        let mut want = u32::MAX;
        for _ in 0..32 {
            want = if want & 0x8000_0000 != 0 { ((want ^ POLY) << 1) | 1 } else { want << 1 };
        }
        let at = payload.len() - 4;
        payload[at..].copy_from_slice(&(reg ^ want).to_le_bytes());
        assert_eq!(crc::crc32(payload), 0, "the payload's CRC is 0");
        for threads in [1, 2, 3] {
            let (serial, parallel) = both_stamps(bytes.clone(), &lens, threads);
            assert_eq!(serial[2 * 4096 - 4..2 * 4096], [0xFF; 4], "the sentinel");
            assert!(serial == parallel, "{threads} threads");
        }
    }

    #[test]
    fn shares_cover_the_units_in_order_in_balanced_parts() {
        // Every unit lands in exactly one share, in order; no share is empty; there are at most
        // `threads` of them, exactly `min(threads, units)` for units of one size, whose shares
        // then differ by at most one unit. A unit larger than the rest together ends the first
        // share, and each unit after it closes one more.
        for n in 0..=20 {
            let bytes = vec![0; n * 4096];
            let lens = vec![(4096, false); n];
            let units = units(&bytes, &lens);
            for threads in 1..=9 {
                let shares = shares(&units, threads);
                let flat: Vec<*const u8> =
                    shares.iter().flat_map(|s| s.iter()).map(|u| u.bytes().as_ptr()).collect();
                let all: Vec<*const u8> = units.iter().map(|u| u.bytes().as_ptr()).collect();
                assert_eq!(flat, all, "{n} units, {threads} threads: every unit once, in order");
                assert!(shares.iter().all(|s| !s.is_empty()), "{n} units, {threads} threads");
                assert_eq!(shares.len(), threads.min(n), "{n} units, {threads} threads");
                let (min, max) = shares
                    .iter()
                    .fold((usize::MAX, 0), |(lo, hi), s| (lo.min(s.len()), hi.max(s.len())));
                assert!(n == 0 || max - min <= 1, "{n} units, {threads} threads: {min}..{max}");
            }
        }
        let (bytes, lens) = buffer(&[P, P, r(30), P, P], 3);
        let units = units(&bytes, &lens);
        let lens: Vec<usize> = shares(&units, 4).iter().map(|s| s.len()).collect();
        assert_eq!(
            lens,
            [3, 1, 1],
            "the large unit closes the first share; one unit per share after"
        );
    }

    #[test]
    fn threads_grow_with_the_bytes_to_the_cap() {
        // One thread below twice `BYTES_PER_THREAD`, one per `BYTES_PER_THREAD` from there, never
        // more than `MAX_THREADS` nor the CPUs available.
        let cpus = thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let cap = cpus.min(MAX_THREADS);
        assert_eq!(max_threads(), cap);
        assert_eq!(threads(0), 1);
        assert_eq!(threads(2 * BYTES_PER_THREAD - 1), 1);
        assert_eq!(threads(2 * BYTES_PER_THREAD), 2.min(cap));
        assert_eq!(threads(3 * BYTES_PER_THREAD), 3.min(cap));
        assert_eq!(threads(1000 * BYTES_PER_THREAD), cap);
        assert_eq!(threads(usize::MAX), cap);
    }
}
