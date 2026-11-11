//! A set of page numbers as a dense bitset, for the writer's page bookkeeping (pages written
//! since the last commit, pages stamped, pages awaiting a sync).

/// A set of page numbers as a dense bitset: the hot-path test and set are a single bit
/// operation with no hashing. It holds a bit for every number up to the largest one inserted, so it
/// is for dense numbering (pages `0..page_count`), not sparse ids.
///
/// It also lists the words an insert made non-zero, so [`Self::clear`] and [`Self::drain`] cost
/// what was inserted since the last of them, not the size of the file. Past a cap (a quarter of
/// the words) the list is dropped and those two walk every word, which is then no dearer.
#[derive(Debug, Default)]
pub(crate) struct PageSet {
    words: Vec<u64>,
    /// The indexes of the words an insert made non-zero since the last `clear` or `drain`
    /// (unsorted, possibly repeated); meaningless while `overflow` is set.
    touched: Vec<usize>,
    /// True if `touched` passed its cap: every word may be non-zero.
    overflow: bool,
    /// Test-only: the words `clear` and `drain` have visited.
    #[cfg(test)]
    visited: usize,
}

impl PageSet {
    /// Add `p` (a no-op if present).
    pub(crate) fn insert(&mut self, p: impl Into<u64>) {
        let (word, bit) = Self::locate(p.into());
        if word >= self.words.len() {
            self.words.resize(word + 1, 0);
        }
        if self.words[word] == 0 {
            self.touch(word);
        }
        self.words[word] |= bit;
    }

    /// List word `word`, about to become non-zero, or give up listing past the cap.
    fn touch(&mut self, word: usize) {
        if self.overflow {
            return;
        }
        if self.touched.len() >= self.words.len() / 4 + 16 {
            self.overflow = true;
            self.touched.clear();
        } else {
            self.touched.push(word);
        }
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

    /// Test-only: how many numbers the set holds.
    #[cfg(test)]
    pub(crate) fn count(&self) -> usize {
        self.words.iter().map(|word| word.count_ones() as usize).sum()
    }

    /// Test-only: the words [`Self::clear`] and [`Self::drain`] have visited.
    #[cfg(test)]
    pub(crate) fn visited(&self) -> usize {
        self.visited
    }

    /// Empty the set, keeping its allocation.
    pub(crate) fn clear(&mut self) {
        if self.overflow {
            #[cfg(test)]
            {
                self.visited += self.words.len();
            }
            self.words.fill(0);
        } else {
            #[cfg(test)]
            {
                self.visited += self.touched.len();
            }
            for &word in &self.touched {
                self.words[word] = 0;
            }
        }
        self.touched.clear();
        self.overflow = false;
    }

    /// Yield every number in ascending order, clearing each word as it is reached (consume it fully
    /// to empty the set; the allocation is kept).
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = u64> + '_ {
        if self.overflow {
            #[cfg(test)]
            {
                self.visited += self.words.len();
            }
            self.touched = (0..self.words.len()).filter(|&i| self.words[i] != 0).collect();
            self.overflow = false;
        } else {
            self.touched.sort_unstable();
            self.touched.dedup();
        }
        // The list is emptied only once every word in it is reached: a drain left part way
        // keeps a list covering the words still set.
        let (mut next, mut i, mut bits) = (0, 0, 0_u64);
        std::iter::from_fn(move || loop {
            if bits != 0 {
                let bit = bits.trailing_zeros();
                bits &= bits - 1;
                return Some(i as u64 * 64 + u64::from(bit));
            }
            let Some(&word) = self.touched.get(next) else {
                self.touched.clear();
                return None;
            };
            #[cfg(test)]
            {
                self.visited += 1;
            }
            next += 1;
            i = word;
            bits = std::mem::take(&mut self.words[word]);
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

    /// `clear` and `drain` visit the words inserted into, not every word up to the largest
    /// number; past the cap they walk every word and still empty the set.
    #[test]
    fn clear_and_drain_cost_what_was_inserted() {
        let mut set = PageSet::default();
        set.insert(64_000_000_u64); // a million words
        set.clear();
        set.insert(5_u32);
        set.insert(6_400_000_u32);
        set.insert(5_u32);
        set.remove(5_u32);
        set.insert(5_u32); // made non-zero twice: listed twice
        assert_eq!(set.drain().collect::<Vec<_>>(), vec![5, 6_400_000]);
        assert!(set.visited() <= 4, "visited {} words", set.visited());

        // A drain left part way still leaves every remaining number to the next clear.
        set.insert(1_u32);
        set.insert(640_u32);
        assert_eq!(set.drain().next(), Some(1));
        set.clear();
        assert!(set.drain().next().is_none() && !set.contains(640_u32));

        // Past the cap: every word is walked, and the set still empties.
        let mut set = PageSet::default();
        let pages: Vec<u64> = (0..1_000).map(|i| i * 64).collect();
        for &p in &pages {
            set.insert(p);
        }
        assert!(set.overflow);
        assert_eq!(set.drain().collect::<Vec<_>>(), pages);
        assert_eq!(set.count(), 0);
        for &p in &pages {
            set.insert(p);
        }
        set.clear();
        assert_eq!(set.count(), 0);
        assert!(!set.overflow && set.touched.is_empty());
    }
}
