//! Commit step 3's CRCs on several threads, for [`crate::env::Writer::stamp_private`].
//!
//! A commit stamps one CRC per unit: a tree page, or an overflow run as a whole
//! ([`crate::alloc::PrivatePages::units`]). Its units may total hundreds of MiB, and one thread
//! computing every CRC was most of a large commit's CPU time. From [`threads`]' threshold up, the
//! writer computes the CRCs here ([`crcs`]): it lends the threads read-only slices of the units
//! ([`crate::file::PageFile::pages`]), each thread computes the CRCs of a contiguous share of
//! about equal bytes, and once every thread is done the writer stores each CRC into its unit's
//! trailer itself, in unit order, through `PageFile::pages_mut` ([`put_crc`]). The threads only
//! read: no page changes while they run, and they need no `unsafe`. The same split and threads
//! ([`spread`]) start a large durable commit's writeback once it is stamped
//! (`Writer::start_writebacks`).
//!
//! The result is the serial loop's (`crc::add_crc32_nonzero` on each unit): the same bytes,
//! stored in the same order, with the same errors, which only the stores can raise. A failed
//! thread spawn computes its share on the writer's thread; a panic on a thread resumes on the
//! writer's thread, as the serial loop's would have happened there.

use std::{
    num::NonZeroUsize,
    panic,
    sync::OnceLock,
    thread::{self, ScopedJoinHandle},
};

use crate::crc;

/// The unit bytes each thread gets at least: [`threads`] gives a second thread from twice this,
/// and one more per this many bytes after that.
const BYTES_PER_THREAD: usize = 1 << 20;

/// The most threads a commit's CRCs use, the writer's own included.
const MAX_THREADS: usize = 4;

/// How many threads should compute the CRCs of `bytes` bytes of units: 1 (the serial loop) below
/// twice [`BYTES_PER_THREAD`], else one per [`BYTES_PER_THREAD`], at most [`MAX_THREADS`] and the
/// CPUs this process may use (`std::thread::available_parallelism`).
pub(crate) fn threads(bytes: usize) -> usize {
    (bytes / BYTES_PER_THREAD).clamp(1, max_threads())
}

/// [`MAX_THREADS`] capped by [`cpus`].
fn max_threads() -> usize {
    cpus().min(MAX_THREADS)
}

/// The CPUs this process may use, `std::thread::available_parallelism`, which reads the CPU
/// affinity and, on Linux, the cgroup quota files: read once, at the first large commit.
pub(crate) fn cpus() -> usize {
    static CPUS: OnceLock<usize> = OnceLock::new();
    *CPUS.get_or_init(|| thread::available_parallelism().map_or(1, NonZeroUsize::get))
}

/// The CRC [`crc::add_crc32_nonzero`] stamps on each of `units` (a whole unit: its payload and
/// its 4-byte trailer), in their order, computed on up to `threads` threads: the current one and
/// up to `threads - 1` scoped ones, each over a contiguous share of [`shares`] ([`spread`]).
pub(crate) fn crcs(units: &[&[u8]], threads: usize) -> Vec<u32> {
    let crcs = |share: &[&[u8]]| share.iter().map(|u| unit_crc(u)).collect::<Vec<_>>();
    spread(units, threads, |u| u.len(), crcs).concat()
}

/// `work` over each of the contiguous shares of about equal `weight` that [`shares`] splits
/// `items` into for `threads` threads: the first share on the current thread, each other on a
/// scoped thread of its own, or on the current thread after its own share when that thread could
/// not be spawned. Returns `work`'s results in share order, once every thread is joined; a
/// panic on a thread resumes on the current one, as `work` there would have raised it.
pub(crate) fn spread<T: Sync, R: Send>(
    items: &[T],
    threads: usize,
    weight: impl Fn(&T) -> usize,
    work: impl Fn(&[T]) -> R + Sync,
) -> Vec<R> {
    /// A share left to another thread, or to this one if that thread could not be spawned.
    enum Share<'s, 't, T, R> {
        Spawned(ScopedJoinHandle<'s, R>),
        Here(&'t [T]),
    }
    let mut shares = shares(items, threads, weight).into_iter();
    let work = &work;
    thread::scope(|s| {
        let first = shares.next().unwrap_or_default();
        let rest: Vec<Share<'_, '_, T, R>> = shares
            .map(|share| {
                thread::Builder::new()
                    .name("tndb-b-stamp".into())
                    .spawn_scoped(s, move || work(share))
                    .map_or(Share::Here(share), Share::Spawned)
            })
            .collect();
        let mut out = Vec::with_capacity(rest.len() + 1);
        out.push(work(first));
        for share in rest {
            out.push(match share {
                Share::Spawned(handle) => handle.join().unwrap_or_else(|p| panic::resume_unwind(p)),
                Share::Here(share) => work(share),
            });
        }
        out
    })
}

/// `items` split into at most `threads` contiguous, non-empty shares of about equal weight (the
/// sum of each item's `weight`: a unit's bytes, a slice's pages): share `k` ends at the first
/// item at which the running total reaches `(k + 1) / threads` of the whole, and the last share
/// takes the rest.
fn shares<T>(items: &[T], threads: usize, weight: impl Fn(&T) -> usize) -> Vec<&[T]> {
    let threads = threads.max(1) as u128;
    let total: u128 = items.iter().map(|t| weight(t) as u128).sum();
    let mut shares = Vec::new();
    let (mut start, mut sum) = (0, 0u128);
    for (i, item) in items.iter().enumerate() {
        sum += weight(item) as u128;
        let closed = shares.len() as u128 + 1;
        if closed < threads && sum * threads >= total * closed {
            shares.push(&items[start..=i]);
            start = i + 1;
        }
    }
    if start < items.len() {
        shares.push(&items[start..]);
    }
    shares
}

/// The CRC [`crc::add_crc32_nonzero`] stamps on `unit`: the CRC-32 of all but its last 4 bytes,
/// where a CRC of 0 becomes the all-ones sentinel, as `crc`'s non-zero stamp maps it (a stamped
/// trailer never reads as the all-zero dirty marker).
fn unit_crc(unit: &[u8]) -> u32 {
    match crc::crc32(&unit[..unit.len().saturating_sub(4)]) {
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

    /// Units of the given page counts, laid out one after another in one buffer of 4 KiB pages,
    /// as a commit's units lie in the file; their bytes differ by `seed`.
    fn buffer(pages: &[usize], seed: u32) -> (Vec<u8>, Vec<usize>) {
        let lens: Vec<usize> = pages.iter().map(|n| n * 4096).collect();
        let mut x = seed | 1;
        let bytes = (0..lens.iter().sum())
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                x as u8
            })
            .collect();
        (bytes, lens)
    }

    /// `bytes` cut into units of `lens` bytes.
    fn units<'b>(bytes: &'b [u8], lens: &[usize]) -> Vec<&'b [u8]> {
        let mut rest = bytes;
        lens.iter()
            .map(|&len| {
                let (unit, tail) = rest.split_at(len);
                rest = tail;
                unit
            })
            .collect()
    }

    /// `bytes` stamped by the serial loop, `crc::add_crc32_nonzero` on each unit, and by [`crcs`]
    /// on `threads` threads then [`put_crc`] on each unit.
    fn both_stamps(mut bytes: Vec<u8>, lens: &[usize], threads: usize) -> (Vec<u8>, Vec<u8>) {
        let crcs = crcs(&units(&bytes, lens), threads);
        assert_eq!(crcs.len(), lens.len(), "one CRC per unit");
        let mut serial = bytes.clone();
        let mut rest = &mut serial[..];
        for &len in lens {
            let (unit, tail) = rest.split_at_mut(len);
            crc::add_crc32_nonzero(unit);
            rest = tail;
        }
        let mut rest = &mut bytes[..];
        for (&len, crc) in lens.iter().zip(crcs) {
            let (unit, tail) = rest.split_at_mut(len);
            put_crc(unit, crc);
            rest = tail;
        }
        (serial, bytes)
    }

    #[test]
    fn crcs_stamp_the_bytes_of_the_serial_loop() {
        // Tree pages and runs of up to 9 pages in every order, from no unit to more units than
        // threads, on 1 to 9 threads: the stamped bytes are the serial loop's.
        let shapes: [&[usize]; 7] = [
            &[],
            &[1],
            &[9],
            &[1, 1],
            &[3, 1, 1, 9, 1, 2],
            &[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            &[9, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 4, 1, 1, 2, 7],
        ];
        for (s, pages) in shapes.iter().enumerate() {
            for threads in 1..=9 {
                let (bytes, lens) = buffer(pages, s as u32 * 31 + threads as u32);
                let (serial, parallel) = both_stamps(bytes, &lens, threads);
                assert!(serial == parallel, "units {pages:?} on {threads} threads");
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
        let (mut bytes, lens) = buffer(&[1, 1, 2], 7);
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
            let lens = vec![4096; n];
            let units = units(&bytes, &lens);
            for threads in 1..=9 {
                let shares = shares(&units, threads, |u| u.len());
                let flat: Vec<*const u8> =
                    shares.iter().flat_map(|s| s.iter()).map(|u| u.as_ptr()).collect();
                let all: Vec<*const u8> = units.iter().map(|u| u.as_ptr()).collect();
                assert_eq!(flat, all, "{n} units, {threads} threads: every unit once, in order");
                assert!(shares.iter().all(|s| !s.is_empty()), "{n} units, {threads} threads");
                assert_eq!(shares.len(), threads.min(n), "{n} units, {threads} threads");
                let (min, max) = shares
                    .iter()
                    .fold((usize::MAX, 0), |(lo, hi), s| (lo.min(s.len()), hi.max(s.len())));
                assert!(n == 0 || max - min <= 1, "{n} units, {threads} threads: {min}..{max}");
            }
        }
        let (bytes, lens) = buffer(&[1, 1, 30, 1, 1], 3);
        let units = units(&bytes, &lens);
        let lens: Vec<usize> = shares(&units, 4, |u| u.len()).iter().map(|s| s.len()).collect();
        assert_eq!(
            lens,
            [3, 1, 1],
            "the large unit closes the first share; one unit per share after"
        );
    }

    #[test]
    fn spread_runs_each_share_once_the_first_here_and_returns_them_in_order() {
        // Nine items of weight 1 on 3 threads: three shares of three, the first on this thread and
        // each other on a thread of its own, their results in share order; a panic on another
        // thread resumes here.
        let items: Vec<u32> = (0..9).collect();
        let here = thread::current().id();
        let runs = spread(&items, 3, |_| 1, |share| (share.to_vec(), thread::current().id()));
        let shares: Vec<Vec<u32>> = runs.iter().map(|(share, _)| share.clone()).collect();
        assert_eq!(shares, [vec![0, 1, 2], vec![3, 4, 5], vec![6, 7, 8]]);
        let ids: Vec<_> = runs.iter().map(|&(_, id)| id).collect();
        assert_eq!(ids[0], here, "the first share on the current thread");
        assert!(ids[1] != here && ids[2] != here && ids[1] != ids[2], "{ids:?}");
        let boom = panic::catch_unwind(|| {
            spread(
                &items,
                3,
                |_| 1,
                |share| {
                    if share[0] == 6 {
                        panic::panic_any("boom");
                    }
                },
            );
        });
        let payload = boom.expect_err("the panic resumed");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"boom"));
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
