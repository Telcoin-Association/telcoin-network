//! A set of page or bucket numbers as a dense bitset, shared by the indexes that track which of
//! their pages this handle wrote since the last sync (the B+tree's pages, the digest index's
//! buckets).

/// A set of page (or bucket) numbers as a dense bitset: the hot-path test and set are a single bit
/// operation with no hashing. It holds a bit for every number up to the largest one inserted, so it
/// is for dense numbering (pages `1..page_count`, buckets `0..buckets`), not sparse ids.
#[derive(Debug, Default)]
pub(crate) struct PageSet {
    words: Vec<u64>,
}

impl PageSet {
    /// Add `p` (a no-op if present).
    pub(crate) fn insert(&mut self, p: impl Into<u64>) {
        let (word, bit) = Self::locate(p.into());
        if word >= self.words.len() {
            self.words.resize(word + 1, 0);
        }
        self.words[word] |= bit;
    }

    /// Remove `p` (a no-op if absent).
    pub(crate) fn remove(&mut self, p: impl Into<u64>) {
        let (word, bit) = Self::locate(p.into());
        if let Some(word) = self.words.get_mut(word) {
            *word &= !bit;
        }
    }

    /// True if `p` is in the set.
    pub(crate) fn contains(&self, p: impl Into<u64>) -> bool {
        let (word, bit) = Self::locate(p.into());
        self.words.get(word).is_some_and(|word| word & bit != 0)
    }

    /// Empty the set, keeping its allocation.
    pub(crate) fn clear(&mut self) {
        self.words.fill(0);
    }

    /// Yield every number in ascending order, clearing each word as it is reached (consume it fully
    /// to empty the set; the allocation is kept).
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = u64> + '_ {
        self.words.iter_mut().enumerate().flat_map(|(i, word)| {
            let mut bits = std::mem::take(word);
            std::iter::from_fn(move || {
                (bits != 0).then(|| {
                    let bit = bits.trailing_zeros();
                    bits &= bits - 1;
                    i as u64 * 64 + u64::from(bit)
                })
            })
        })
    }

    /// The word index and bit mask of `p`.
    fn locate(p: u64) -> (usize, u64) {
        ((p / 64) as usize, 1 << (p % 64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_set_insert_contains_remove_drain() {
        let mut set = PageSet::default();
        for p in [1_u32, 63, 64, 65, 200, 4_000] {
            set.insert(p);
        }
        set.insert(64_u64); // idempotent, and u64 numbers address the same bits
        assert!(set.contains(63_u32) && set.contains(4_000_u64));
        assert!(!set.contains(2_u32) && !set.contains(9_999_u64), "absent, and past the end");

        set.remove(65_u64);
        set.remove(9_999_u32); // past the end: a no-op
        assert!(!set.contains(65_u32) && set.contains(64_u32) && set.contains(200_u32));

        assert_eq!(set.drain().collect::<Vec<_>>(), vec![1, 63, 64, 200, 4_000], "ascending");
        assert!(!set.contains(1_u32) && set.drain().next().is_none(), "drain empties the set");
        let words = set.words.len();
        assert!(words > 0, "drain keeps the allocation");

        set.insert(7_u32);
        set.clear();
        assert!(!set.contains(7_u32) && set.words.len() == words, "clear keeps the allocation");
    }
}
