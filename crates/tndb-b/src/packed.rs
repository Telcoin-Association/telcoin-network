//! Packed sets: the codec of a dup table's small key, whose rows stay in its outer cell as one
//! sorted, immutable value (a flags-0 cell) instead of a nested tree.
//!
//! **Layout** (little-endian; the whole cell value):
//!
//! | offset | size | field |
//! |---|---|---|
//! | 0 | 2 | `n`: the rows, at least 1 |
//! | `2 + 4i` | 2 | `end[i]`: the offset (from the blob's start) one past row i's bytes |
//! | `4 + 4i` | 2 | `slen[i]`: row i's subkey length |
//! | `2 + 4n` | ... | the rows, one after another: `s_i ‖ v_i` |
//!
//! Row i spans `start_i..end[i]`, with `start_0 = 2 + 4n` and `start_i = end[i − 1]`; its first
//! `slen[i]` bytes are the subkey and the rest (possibly none) the value. A row is two slot reads
//! away, so a seek is a binary search and a cursor steps by index; a row's subkey and value are
//! contiguous. The offsets are u16: a blob is at most the inline value limit of its outer key
//! (16366 bytes at 64 KiB pages), so every offset fits. The smallest blob is [`PACKED_MIN`] bytes.
//!
//! **Checks.** The dup layer knows a cell is a packed set only by its table's bound, so every
//! rule is checked here, in three tiers, and broken as [`Error::Corrupt`] (the caller adds the
//! table and key):
//! - [`PackedView::new`], at every decode, O(1): the length within `PACKED_MIN..=packed_max`, at
//!   least one row, the slot table within the blob, the last row ending at the blob's end.
//! - [`PackedView::row`], per row touched, O(1): the row within the rows area (`2 + 4n <= start_i
//!   <= end[i] <= len`), and its subkey of 1 to `max_subkey` bytes within the row.
//!   [`PackedView::find`] gives each probe the subkey part of these checks (what its compare reads)
//!   and a miss's two neighbours all of them.
//! - [`validate`], the whole set, O(n): `new`, every `row` (so the rows tile the blob exactly) and
//!   the subkeys strictly ascending. A write validates a blob before it rebuilds it.
//!
//! A read checks only what it touches: damage to a row it does not probe (a misorder between
//! two rows off its search path, say) is left to the writer's [`validate`] and the full check,
//! as damage to a page's cells is. No input makes a decode panic or read out of bounds.

use std::{cmp::Ordering, fmt};

use crate::{
    error::{Error, Result},
    page::{cmp_keys, Search},
};

/// The smallest blob: one row of a 1-byte subkey and an empty value.
pub(crate) const PACKED_MIN: usize = HEADER + SLOT + 1;

/// The row count's bytes.
const HEADER: usize = 2;

/// A row's slot: its end offset and its subkey length.
const SLOT: usize = 4;

/// Where the rows start in a blob of `n` rows.
#[inline]
fn rows_start(n: usize) -> usize {
    HEADER + SLOT * n
}

/// The u16 at `off`; the caller has checked `off + 2 <= b.len()`.
#[inline]
fn u16_at(b: &[u8], off: usize) -> usize {
    usize::from(u16::from_le_bytes([b[off], b[off + 1]]))
}

/// A packed set's rows, after the O(1) checks of [`PackedView::new`] (or the whole check, from
/// [`validate`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct PackedView<'a> {
    blob: &'a [u8],
    n: usize,
}

impl<'a> PackedView<'a> {
    /// The view of `blob` with the O(1) checks: `PACKED_MIN <= len <= packed_max`, `n >= 1`,
    /// the slot table within the blob (`2 + 4n <= len`) and `end[n − 1] == len`.
    #[inline]
    pub(crate) fn new(blob: &'a [u8], packed_max: usize) -> Result<Self> {
        let len = blob.len();
        if len < PACKED_MIN || len > packed_max {
            return Err(bad_len(len, packed_max));
        }
        let n = u16_at(blob, 0);
        if n == 0 || rows_start(n) > len {
            return Err(bad_count(n, len));
        }
        let last = u16_at(blob, rows_start(n - 1));
        if last != len {
            return Err(bad_last(n, last, len));
        }
        Ok(Self { blob, n })
    }

    /// The set's rows, at least 1.
    #[inline]
    pub(crate) fn rows(&self) -> usize {
        self.n
    }

    /// Row `i`'s subkey and value, with its O(1) checks: the row within the rows area
    /// (`2 + 4n <= start_i <= end[i] <= len`) and `1 <= slen[i] <= max_subkey`, within the row.
    /// An `i` past the rows is [`Error::Corrupt`] too.
    #[inline]
    pub(crate) fn row(&self, i: usize, max_subkey: usize) -> Result<(&'a [u8], &'a [u8])> {
        let (start, end, slen) = self.slots(i)?;
        if start < rows_start(self.n) || start > end || end > self.blob.len() {
            return Err(self.bad_span(i, start, end));
        }
        if slen == 0 || slen > max_subkey || slen > end - start {
            return Err(bad_subkey(i, slen, end - start, max_subkey));
        }
        Ok(self.blob[start..end].split_at(slen))
    }

    /// The binary search for the subkey `s`. Each probe reads its row's subkey with the subkey
    /// part of [`Self::row`]'s checks; a miss's neighbours (rows `i − 1` and `i` of
    /// `NotFound(i)`) get all of them, so damage beside a miss is refused rather than read as
    /// an absent subkey. A hit's subkey is `s` itself; its reader takes the value by `row`.
    #[inline]
    pub(crate) fn find(&self, s: &[u8], max_subkey: usize) -> Result<Search> {
        let (mut lo, mut hi) = (0, self.n);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match cmp_keys(self.subkey(mid, max_subkey)?, s) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Ok(Search::Found(mid)),
            }
        }
        if lo > 0 {
            self.row(lo - 1, max_subkey)?;
        }
        if lo < self.n {
            self.row(lo, max_subkey)?;
        }
        Ok(Search::NotFound(lo))
    }

    /// Row `i`'s `start_i`, `end[i]` and `slen[i]`, unchecked against each other; an `i` past
    /// the rows is refused, so every slot read is within the slot table `new` checked.
    #[inline]
    fn slots(&self, i: usize) -> Result<(usize, usize, usize)> {
        if i >= self.n {
            return Err(past_rows(i, self.n));
        }
        let slot = rows_start(i);
        let start = if i == 0 { rows_start(self.n) } else { u16_at(self.blob, slot - SLOT) };
        Ok((start, u16_at(self.blob, slot), u16_at(self.blob, slot + 2)))
    }

    /// Row `i`'s subkey, with the checks its compare needs: `1 <= slen[i] <= max_subkey`, the
    /// row starting in the rows area and the subkey within the blob.
    #[inline]
    fn subkey(&self, i: usize, max_subkey: usize) -> Result<&'a [u8]> {
        let (start, _, slen) = self.slots(i)?;
        if (1..=max_subkey).contains(&slen) && start >= rows_start(self.n) {
            if let Some(s) = self.blob.get(start..start + slen) {
                return Ok(s);
            }
        }
        Err(self.refused(i, max_subkey))
    }

    /// Why [`Self::subkey`] refused row `i`: every rule it applies is one of [`Self::row`]'s.
    #[cold]
    #[inline(never)]
    fn refused(&self, i: usize, max_subkey: usize) -> Error {
        match self.row(i, max_subkey) {
            Err(e) => e,
            Ok(_) => corrupt(format_args!("packed row {i}: its subkey is out of bounds")),
        }
    }

    #[cold]
    #[inline(never)]
    fn bad_span(&self, i: usize, start: usize, end: usize) -> Error {
        corrupt(format_args!(
            "packed row {i} of {} spans {start}..{end}, outside {}..{}",
            self.n,
            rows_start(self.n),
            self.blob.len()
        ))
    }

    /// Row `i`'s byte range `start_i..end[i]`, in a validated view.
    fn range(&self, i: usize) -> (usize, usize) {
        let slot = rows_start(i);
        let start = if i == 0 { rows_start(self.n) } else { u16_at(self.blob, slot - SLOT) };
        (start, u16_at(self.blob, slot))
    }

    /// Row `i`'s `end[i]` and `slen[i]`, in a validated view.
    fn slot(&self, i: usize) -> (usize, usize) {
        let slot = rows_start(i);
        (u16_at(self.blob, slot), u16_at(self.blob, slot + 2))
    }
}

/// The whole check: [`PackedView::new`], [`PackedView::row`] for every row (so the rows tile the
/// blob exactly) and the subkeys strictly ascending. O(n).
pub(crate) fn validate(
    blob: &[u8],
    packed_max: usize,
    max_subkey: usize,
) -> Result<PackedView<'_>> {
    let view = PackedView::new(blob, packed_max)?;
    let mut prev: &[u8] = &[];
    for i in 0..view.n {
        let (s, _) = view.row(i, max_subkey)?;
        if i > 0 && cmp_keys(prev, s) != Ordering::Less {
            return Err(misordered(i));
        }
        prev = s;
    }
    Ok(view)
}

/// The length [`build_with`] gives for the same arguments.
pub(crate) fn encoded_len_with(view: &PackedView<'_>, at: Search, s: &[u8], v: &[u8]) -> usize {
    let row = s.len() + v.len();
    match at {
        Search::Found(i) => {
            let (start, end) = view.range(i);
            view.blob.len() - (end - start) + row
        }
        Search::NotFound(_) => view.blob.len() + SLOT + row,
    }
}

/// The length [`build_without`] gives for the same arguments.
pub(crate) fn encoded_len_without(view: &PackedView<'_>, i: usize) -> usize {
    let (start, end) = view.range(i);
    view.blob.len() - SLOT - (end - start)
}

/// The length [`build_one`] gives for the same arguments.
pub(crate) fn encoded_len_one(s: &[u8], v: &[u8]) -> usize {
    rows_start(1) + s.len() + v.len()
}

/// Into `out` (cleared first, its capacity reused): the set of the validated `view` with the row
/// `(s, v)` at `at`, as [`PackedView::find`] gave it for `s`: `Found(i)` replaces row i,
/// `NotFound(i)` inserts before it. The caller checks [`encoded_len_with`] against its bound
/// first, and `s` has 1 to `max_subkey` bytes. O(blob).
pub(crate) fn build_with(view: &PackedView<'_>, at: Search, s: &[u8], v: &[u8], out: &mut Vec<u8>) {
    debug_assert!(!s.is_empty());
    let (n, len) = (view.n, view.blob.len());
    // The new row goes at `i`, the old rows from `rest` on follow it, and its old bytes are
    // `start..old_end`: the replaced row, or none for an insert.
    let (i, rest, new_n, start, old_end) = match at {
        Search::Found(i) => {
            let (start, end) = view.range(i);
            (i, i + 1, n, start, end)
        }
        Search::NotFound(i) => {
            let start = if i < n { view.range(i).0 } else { len };
            (i, i, n + 1, start, start)
        }
    };
    let grow = rows_start(new_n) - rows_start(n);
    let new_end = start + grow + s.len() + v.len();
    out.clear();
    out.reserve(encoded_len_with(view, at, s, v));
    put_u16(out, new_n);
    for j in 0..i {
        let (end, slen) = view.slot(j);
        put_slot(out, end + grow, slen);
    }
    put_slot(out, new_end, s.len());
    for j in rest..n {
        let (end, slen) = view.slot(j);
        put_slot(out, end - old_end + new_end, slen);
    }
    out.extend_from_slice(&view.blob[rows_start(n)..start]);
    out.extend_from_slice(s);
    out.extend_from_slice(v);
    out.extend_from_slice(&view.blob[old_end..]);
    debug_assert_eq!(out.len(), encoded_len_with(view, at, s, v));
}

/// Into `out` (cleared first, its capacity reused): the set of the validated `view` without row
/// `i`. The view has at least 2 rows (a set never empties: its last row's delete removes the
/// cell). O(blob).
pub(crate) fn build_without(view: &PackedView<'_>, i: usize, out: &mut Vec<u8>) {
    debug_assert!(view.n >= 2 && i < view.n);
    let n = view.n;
    let (start, end) = view.range(i);
    let removed = end - start;
    out.clear();
    out.reserve(encoded_len_without(view, i));
    put_u16(out, n - 1);
    for j in (0..n).filter(|&j| j != i) {
        let (e, slen) = view.slot(j);
        put_slot(out, e - SLOT - if j > i { removed } else { 0 }, slen);
    }
    out.extend_from_slice(&view.blob[rows_start(n)..start]);
    out.extend_from_slice(&view.blob[end..]);
    debug_assert_eq!(out.len(), encoded_len_without(view, i));
}

/// Into `out` (cleared first, its capacity reused): the one-row set `(s, v)`, `s` of 1 to
/// `max_subkey` bytes. The caller checks [`encoded_len_one`] against its bound first.
pub(crate) fn build_one(s: &[u8], v: &[u8], out: &mut Vec<u8>) {
    debug_assert!(!s.is_empty());
    out.clear();
    out.reserve(encoded_len_one(s, v));
    put_u16(out, 1);
    put_slot(out, encoded_len_one(s, v), s.len());
    out.extend_from_slice(s);
    out.extend_from_slice(v);
}

/// Appends `x` as a u16; a blob within its bound (at most 16366 bytes) keeps every field in one.
#[inline]
fn put_u16(out: &mut Vec<u8>, x: usize) {
    debug_assert!(x <= usize::from(u16::MAX));
    out.extend_from_slice(&(x as u16).to_le_bytes());
}

#[inline]
fn put_slot(out: &mut Vec<u8>, end: usize, slen: usize) {
    put_u16(out, end);
    put_u16(out, slen);
}

#[cold]
#[inline(never)]
fn corrupt(what: fmt::Arguments<'_>) -> Error {
    Error::Corrupt(fmt::format(what))
}

#[cold]
#[inline(never)]
fn bad_len(len: usize, packed_max: usize) -> Error {
    corrupt(format_args!("a packed set of {len} bytes, not {PACKED_MIN} to {packed_max}"))
}

#[cold]
#[inline(never)]
fn bad_count(n: usize, len: usize) -> Error {
    corrupt(format_args!("a packed set of {n} rows in {len} bytes"))
}

#[cold]
#[inline(never)]
fn bad_last(n: usize, last: usize, len: usize) -> Error {
    corrupt(format_args!("a packed set of {len} bytes whose last row (of {n}) ends at {last}"))
}

#[cold]
#[inline(never)]
fn bad_subkey(i: usize, slen: usize, row: usize, max_subkey: usize) -> Error {
    corrupt(format_args!(
        "packed row {i}: a subkey of {slen} bytes, not 1 to {max_subkey} within its {row}-byte row"
    ))
}

#[cold]
#[inline(never)]
fn past_rows(i: usize, n: usize) -> Error {
    corrupt(format_args!("packed row {i} of a {n}-row set"))
}

#[cold]
#[inline(never)]
fn misordered(i: usize) -> Error {
    corrupt(format_args!("packed rows {} and {i}: subkeys not strictly ascending", i - 1))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{
        page::{Geometry, MAX_PAGE_SIZE},
        tree::test_harness::{ReportSeed, Rng},
    };

    type Rows = Vec<(Vec<u8>, Vec<u8>)>;

    /// The bound and the subkey limit of a table of 20-byte outer keys at 4 KiB pages.
    fn limits() -> (usize, usize) {
        let g = Geometry::new(4096).unwrap();
        (g.max_value(20), g.max_subkey(20))
    }

    fn is_corrupt<T: fmt::Debug>(r: Result<T>) -> bool {
        matches!(r, Err(Error::Corrupt(_)))
    }

    /// The layout written by hand, for any rows in any order (the builders keep them sorted).
    fn encode(rows: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut blob = (rows.len() as u16).to_le_bytes().to_vec();
        let mut end = rows_start(rows.len());
        for (s, v) in rows {
            end += s.len() + v.len();
            blob.extend_from_slice(&(end as u16).to_le_bytes());
            blob.extend_from_slice(&(s.len() as u16).to_le_bytes());
        }
        for (s, v) in rows {
            blob.extend_from_slice(s);
            blob.extend_from_slice(v);
        }
        blob
    }

    fn set_u16(blob: &mut [u8], off: usize, x: u16) {
        blob[off..off + 2].copy_from_slice(&x.to_le_bytes());
    }

    /// Every row of a view whose rows all pass `row`.
    fn decode(view: &PackedView<'_>, max_subkey: usize) -> Rows {
        (0..view.rows())
            .map(|i| {
                let (s, v) = view.row(i, max_subkey).unwrap();
                (s.to_vec(), v.to_vec())
            })
            .collect()
    }

    /// The sorted `rows` re-encoded by the builders: one row, then each next one appended.
    fn rebuild(rows: &[(Vec<u8>, Vec<u8>)], max_subkey: usize) -> Vec<u8> {
        let (mut blob, mut out) = (Vec::new(), Vec::new());
        build_one(&rows[0].0, &rows[0].1, &mut blob);
        for (k, (s, v)) in rows.iter().enumerate().skip(1) {
            let view = validate(&blob, usize::MAX, max_subkey).unwrap();
            build_with(&view, Search::NotFound(k), s, v, &mut out);
            std::mem::swap(&mut blob, &mut out);
        }
        blob
    }

    /// 1 to 6 bytes over `b`, `c` and `d`: shared prefixes and repeats are common, `a` sorts
    /// before every subkey and `e` after.
    fn random_subkey(rng: &mut Rng) -> Vec<u8> {
        (0..1 + rng.below(6)).map(|_| b'b' + rng.below(3) as u8).collect()
    }

    fn random_value(rng: &mut Rng) -> Vec<u8> {
        (0..rng.len_upto(12)).map(|_| rng.next() as u8).collect()
    }

    /// `find` agrees with the model on every present subkey, before the first, past the last
    /// and on random probes.
    fn check_find(
        view: &PackedView<'_>,
        model: &BTreeMap<Vec<u8>, Vec<u8>>,
        rng: &mut Rng,
        max_subkey: usize,
    ) {
        for (i, s) in model.keys().enumerate() {
            assert_eq!(view.find(s, max_subkey).unwrap(), Search::Found(i));
        }
        assert_eq!(view.find(b"a", max_subkey).unwrap(), Search::NotFound(0));
        assert_eq!(view.find(b"e", max_subkey).unwrap(), Search::NotFound(model.len()));
        for _ in 0..4 {
            let s = random_subkey(rng);
            let rank = model.range(..s.clone()).count();
            let want =
                if model.contains_key(&s) { Search::Found(rank) } else { Search::NotFound(rank) };
            assert_eq!(view.find(&s, max_subkey).unwrap(), want);
        }
    }

    /// `steps` random edits (insert, replace, delete) of one set against a `BTreeMap`; an edit
    /// past the bound is skipped, as the dup layer promotes it instead.
    fn edit_against_a_model(seed: u64, steps: usize) {
        let _report = ReportSeed(seed);
        let (packed_max, max_subkey) = limits();
        let mut rng = Rng(seed);
        let mut model = BTreeMap::new();
        let (mut blob, mut out) = (Vec::new(), Vec::new());
        let (s, v) = (random_subkey(&mut rng), random_value(&mut rng));
        build_one(&s, &v, &mut blob);
        assert_eq!(blob.len(), encoded_len_one(&s, &v));
        model.insert(s, v);
        let mut skipped = 0;
        for _ in 0..steps {
            let view = validate(&blob, packed_max, max_subkey).unwrap();
            let want: Rows = model.iter().map(|(s, v)| (s.clone(), v.clone())).collect();
            assert_eq!(decode(&view, max_subkey), want);
            check_find(&view, &model, &mut rng, max_subkey);
            // One delete to three puts, so the set grows to its bound and stays near it.
            let op = rng.below(4);
            if op == 0 && model.len() >= 2 {
                let i = rng.below(model.len() as u64) as usize;
                let s = model.keys().nth(i).unwrap().clone();
                let len = encoded_len_without(&view, i);
                build_without(&view, i, &mut out);
                assert_eq!(out.len(), len);
                model.remove(&s);
            } else {
                let s = if op == 1 {
                    model.keys().nth(rng.below(model.len() as u64) as usize).unwrap().clone()
                } else {
                    random_subkey(&mut rng)
                };
                let v = random_value(&mut rng);
                let at = view.find(&s, max_subkey).unwrap();
                let len = encoded_len_with(&view, at, &s, &v);
                if len > packed_max {
                    skipped += 1;
                    continue;
                }
                build_with(&view, at, &s, &v, &mut out);
                assert_eq!(out.len(), len);
                model.insert(s, v);
            }
            std::mem::swap(&mut blob, &mut out);
        }
        let view = validate(&blob, packed_max, max_subkey).unwrap();
        assert_eq!(view.rows(), model.len());
        assert!(skipped > 0, "the sequence never reached the bound");
    }

    #[test]
    fn packed_sets_built_by_random_edits_match_a_model() {
        for seed in [1, 0x5eed, 0x9e37_79b9_7f4a_7c15] {
            edit_against_a_model(seed, 600);
        }
    }

    #[test]
    #[ignore = "heavy: run with --release"]
    fn packed_sets_built_by_random_edits_match_a_model_heavy() {
        for seed in 1..=64 {
            edit_against_a_model(seed * 0x9e37_79b9_7f4a_7c15, 5000);
        }
    }

    /// `new`, every `row` and `find` on `blob`, which only must not panic.
    fn touch(blob: &[u8], packed_max: usize, max_subkey: usize, probes: &[Vec<u8>]) {
        let Ok(view) = PackedView::new(blob, packed_max) else { return };
        for i in 0..=view.rows() {
            if let Ok((s, v)) = view.row(i, max_subkey) {
                assert!(!s.is_empty() && s.len() <= max_subkey && s.len() + v.len() < blob.len());
            }
        }
        for s in probes {
            if let Ok(Search::Found(i) | Search::NotFound(i)) = view.find(s, max_subkey) {
                assert!(i <= view.rows());
            }
        }
    }

    #[test]
    fn every_single_byte_change_is_refused_or_another_valid_set() {
        let (packed_max, max_subkey) = limits();
        let mut rng = Rng(0x0b17_f11b);
        let (mut valid, mut refused) = (0, 0);
        for rows in [1, 2, 3, 5, 12] {
            let mut model = BTreeMap::new();
            while model.len() < rows {
                model.insert(random_subkey(&mut rng), random_value(&mut rng));
            }
            let rows: Rows = model.into_iter().collect();
            let blob = rebuild(&rows, max_subkey);
            validate(&blob, packed_max, max_subkey).unwrap();
            let mut probes: Vec<Vec<u8>> = rows.iter().map(|(s, _)| s.clone()).collect();
            probes.extend([b"a".to_vec(), b"e".to_vec(), b"bcd".to_vec()]);
            for at in 0..blob.len() {
                for byte in [0x00, 0xff, blob[at].wrapping_add(1)] {
                    if byte == blob[at] {
                        continue;
                    }
                    let mut changed = blob.clone();
                    changed[at] = byte;
                    touch(&changed, packed_max, max_subkey, &probes);
                    match validate(&changed, packed_max, max_subkey) {
                        Err(Error::Corrupt(_)) => refused += 1,
                        Err(e) => panic!("byte {at} set to {byte:#x}: {e}"),
                        Ok(view) => {
                            valid += 1;
                            assert_eq!(rebuild(&decode(&view, max_subkey), max_subkey), changed);
                        }
                    }
                }
            }
            for cut in 0..blob.len() {
                assert!(is_corrupt(PackedView::new(&blob[..cut], packed_max)), "cut at {cut}");
            }
        }
        // Both outcomes occur: a changed value byte is another valid set.
        assert!(valid > 0 && refused > 0);
    }

    #[test]
    fn the_lazy_checks_refuse_what_they_touch() {
        let max_subkey = 8;
        // Rows 14..17, 17..18 and 18..22; `end[i]` at `2 + 4i`, `slen[i]` at `4 + 4i`.
        let good = encode(&[(b"b", b"xx"), (b"d", b""), (b"f", b"yyy")]);
        assert_eq!(good.len(), 22);
        let view = validate(&good, 22, max_subkey).unwrap();
        assert_eq!(rebuild(&decode(&view, max_subkey), max_subkey), good);
        let forged = |off: usize, x: u16| {
            let mut b = good.clone();
            set_u16(&mut b, off, x);
            b
        };

        // `new`: a blob under PACKED_MIN, over the bound, of no rows, with its slot table past
        // its end, or whose last row ends short of it.
        assert!(is_corrupt(PackedView::new(&encode(&[(b"", b"")]), 22)));
        assert!(is_corrupt(PackedView::new(&good, 21)));
        assert!(is_corrupt(PackedView::new(&forged(0, 0), 22)));
        assert!(is_corrupt(PackedView::new(&forged(0, 6), 22)));
        assert!(is_corrupt(PackedView::new(&forged(10, 21), 22)));

        // `row`, each rule on its own row; the other rows still read.
        let rows_of = |blob: &[u8], max_subkey: usize| -> Vec<bool> {
            let view = PackedView::new(blob, 22).unwrap();
            (0..3).map(|i| view.row(i, max_subkey).is_ok()).collect()
        };
        // A decreasing `end`: row 1 starts past its end.
        assert_eq!(rows_of(&forged(2, 19), max_subkey), [true, false, true]);
        // An end past the blob.
        assert_eq!(rows_of(&forged(6, 23), max_subkey), [true, false, false]);
        // Row 1 starting in the slot table (row 0 ends before it starts).
        assert_eq!(rows_of(&forged(2, 13), max_subkey), [false, false, true]);
        // A subkey of 0 bytes, or longer than its row.
        assert_eq!(rows_of(&forged(8, 0), max_subkey), [true, false, true]);
        assert_eq!(rows_of(&forged(8, 2), max_subkey), [true, false, true]);
        // A subkey past `max_subkey`: row 2's 2-byte subkey at a limit of 1 and of 2.
        assert_eq!(rows_of(&forged(12, 2), 1), [true, true, false]);
        assert_eq!(rows_of(&forged(12, 2), 2), [true, true, true]);
        // A row past the set.
        assert!(is_corrupt(view.row(3, max_subkey)));
        for blob in [forged(2, 19), forged(6, 23), forged(2, 13), forged(8, 0), forged(8, 2)] {
            assert!(is_corrupt(validate(&blob, 22, max_subkey)));
        }

        // `find`: a probed row's damage is refused.
        let blob = forged(8, 0);
        assert!(is_corrupt(PackedView::new(&blob, 22).unwrap().find(b"d", max_subkey)));
        // Row 0's subkey of 4 bytes reads (`bxxd`, into row 1) but overruns its 3-byte row: a
        // miss beside it is refused, a hit elsewhere is not.
        let blob = forged(4, 4);
        let view = PackedView::new(&blob, 22).unwrap();
        assert!(is_corrupt(view.find(b"c", max_subkey)));
        assert_eq!(view.find(b"d", max_subkey).unwrap(), Search::Found(1));
        assert_eq!(view.find(b"f", max_subkey).unwrap(), Search::Found(2));

        // A misorder off the search path passes `find` (the hit and the bracketing miss are
        // read as written: the documented exposure of the lazy reads) and fails `validate`.
        let rows: [(&[u8], &[u8]); 5] =
            [(b"b", b""), (b"d", b""), (b"c", b""), (b"e", b""), (b"f", b"")];
        let blob = encode(&rows);
        let view = PackedView::new(&blob, blob.len()).unwrap();
        assert_eq!(view.find(b"e", max_subkey).unwrap(), Search::Found(3));
        assert_eq!(view.find(b"d", max_subkey).unwrap(), Search::NotFound(3));
        assert!(is_corrupt(validate(&blob, blob.len(), max_subkey)));
        // Equal subkeys are a misorder too.
        let blob = encode(&[(b"b", b"1"), (b"b", b"2")]);
        assert!(is_corrupt(validate(&blob, blob.len(), max_subkey)));
    }

    #[test]
    fn packed_codec_limits() {
        assert_eq!(PACKED_MIN, 7);
        let mut blob = Vec::new();
        build_one(b"s", b"", &mut blob);
        assert_eq!((blob.len(), encoded_len_one(b"s", b"")), (PACKED_MIN, PACKED_MIN));
        let view = validate(&blob, PACKED_MIN, 1).unwrap();
        assert_eq!(view.row(0, 1).unwrap(), (&b"s"[..], &b""[..]));
        // Six bytes are refused at any bound, and a bound of 0 refuses every blob.
        assert!(is_corrupt(PackedView::new(&blob[..6], usize::MAX)));
        assert!(is_corrupt(validate(&encode(&[(b"", b"")]), usize::MAX, 1)));
        assert!(is_corrupt(PackedView::new(&blob, 0)));

        // A blob of exactly the bound passes; one byte more is refused.
        let (packed_max, max_subkey) = limits();
        let v = vec![7; packed_max - encoded_len_one(b"s", b"")];
        build_one(b"s", &v, &mut blob);
        assert_eq!(blob.len(), packed_max);
        validate(&blob, packed_max, max_subkey).unwrap();
        assert!(is_corrupt(validate(&blob, packed_max - 1, max_subkey)));
        build_one(b"s", &[&v[..], &[7]].concat(), &mut blob);
        assert!(is_corrupt(PackedView::new(&blob, packed_max)));

        // The largest blob, at 64 KiB pages with a 1-byte outer key: 1000 short rows and a
        // last one filling the bound, every offset in u16.
        let g = Geometry::new(MAX_PAGE_SIZE as u32).unwrap();
        let (max, max_subkey) = (g.max_value(1), g.max_subkey(1));
        assert_eq!(max, 16366);
        assert!(max <= usize::from(u16::MAX));
        let (mut out, mut ends) = (Vec::new(), Vec::new());
        build_one(&[0, 0], b"abc", &mut blob);
        for k in 1..1001u16 {
            let view = validate(&blob, max, max_subkey).unwrap();
            let (s, v) = if k < 1000 {
                (k.to_be_bytes().to_vec(), b"abc".to_vec())
            } else {
                let last = encoded_len_with(&view, Search::NotFound(1000), &[0xff, 0xff], b"");
                (vec![0xff, 0xff], vec![9; max - last])
            };
            let at = view.find(&s, max_subkey).unwrap();
            assert_eq!(at, Search::NotFound(usize::from(k)));
            build_with(&view, at, &s, &v, &mut out);
            std::mem::swap(&mut blob, &mut out);
        }
        assert_eq!(blob.len(), max);
        let view = validate(&blob, max, max_subkey).unwrap();
        assert_eq!(view.rows(), 1001);
        let mut end = rows_start(1001);
        for i in 0..1001 {
            let (s, v) = view.row(i, max_subkey).unwrap();
            end += s.len() + v.len();
            ends.push(u16_at(&blob, rows_start(i)));
            assert_eq!(ends[i], end);
        }
        assert_eq!(ends[1000], max);
        assert_eq!(view.find(&[0xff, 0xff], max_subkey).unwrap(), Search::Found(1000));
        assert_eq!(view.find(&500u16.to_be_bytes(), max_subkey).unwrap(), Search::Found(500));
        // One row of the longest subkey that fits.
        build_one(&vec![1; max - encoded_len_one(b"", b"")], b"", &mut blob);
        assert_eq!(blob.len(), max);
        validate(&blob, max, max_subkey).unwrap();
    }
}
