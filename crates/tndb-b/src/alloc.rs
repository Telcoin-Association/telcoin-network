//! The writer's page allocator: which pages the writing transaction created (`private`), which
//! pages this writer stamped at its own commits (`stamped`), the reusable pages (`pool`), the
//! published pages the transaction took out of its trees (`freed`), and the end of the file
//! (`next_pgno`).
//!
//! A page leaving a tree goes to `freed` if a published tree can still reach it (it is reusable
//! only once no durable meta and no reader can), or straight back to `pool` if the transaction
//! created it ("loose": nothing outside the transaction ever saw it). An allocation takes a page
//! from `pool`, else the next page number at the end of the file, growing it. The writer refills
//! an empty pool from the persisted free list ([`crate::freelist`]) first, through
//! [`Allocator::load_record`]: the allocator remembers the records taken (deleted when the commit
//! saves the free list) and their pages.

use crate::{
    error::{Error, Result},
    freelist::RecordKey,
    page_set::PageSet,
    tree::NULL_PGNO,
};

/// The writer's page bookkeeping for one transaction at a time (see the module docs).
#[derive(Debug)]
pub(crate) struct Allocator {
    /// The writing transaction's id.
    txn: u64,
    /// Pages this transaction created and still holds: changed in place, stamped at commit.
    private: PageSet,
    /// Pages this writer stamped at its own commits since it opened: their bytes were built
    /// here, so copy-on-write does not re-verify their CRC.
    stamped: PageSet,
    /// Pages free for this transaction to reuse.
    pool: Vec<u32>,
    /// Published pages this transaction took out of its trees.
    freed: Vec<u32>,
    /// The first page number past the end of the file (the transaction's page count).
    next_pgno: u32,
    /// The pool and page count the writing transaction began with, which [`Self::abort`]
    /// returns to.
    base: (Vec<u32>, u32),
    /// The free records the transaction took, ascending.
    consumed: Vec<RecordKey>,
    /// The pages those records listed. Every private page below the base page count came from
    /// one of them, so a record listing a page in here lists a page taken already (private, in
    /// the pool, or listed twice).
    loaded: PageSet,
    /// True while an empty pool may be refilled from the free list: until no record passes the
    /// reuse gate, or the commit saves the list.
    loading: bool,
    /// The writer-wide epoch a table's write-path cache is valid in (see [`Self::epoch`]).
    epoch: u64,
    /// Test-only: fail the allocation after this many more succeed.
    #[cfg(test)]
    pub(crate) fail_allocs_after: Option<u64>,
    /// Test-only: `(txn, page)` for every page a transaction loaded from the free list.
    #[cfg(test)]
    pub(crate) load_log: Vec<(u64, u32)>,
    /// Test-only: `(txn, page)` for every published page a committed transaction freed.
    #[cfg(test)]
    pub(crate) free_log: Vec<(u64, u32)>,
}

impl Allocator {
    /// The allocator of a writer opening on a file of `page_count` pages, writing transaction
    /// `txn`.
    pub(crate) fn new(txn: u64, page_count: u32) -> Self {
        Self {
            txn,
            private: PageSet::default(),
            stamped: PageSet::default(),
            pool: Vec::new(),
            freed: Vec::new(),
            next_pgno: page_count,
            base: (Vec::new(), page_count),
            consumed: Vec::new(),
            loaded: PageSet::default(),
            loading: true,
            epoch: 1,
            #[cfg(test)]
            fail_allocs_after: None,
            #[cfg(test)]
            load_log: Vec::new(),
            #[cfg(test)]
            free_log: Vec::new(),
        }
    }

    /// The writing transaction's id.
    pub(crate) fn txn(&self) -> u64 {
        self.txn
    }

    /// The transaction's page count: pages `0..page_count` are allocated.
    pub(crate) fn page_count(&self) -> u32 {
        self.next_pgno
    }

    /// The writer-wide epoch: it changes at every allocation and free, whenever the private set
    /// empties (commit, abort) and when a transaction begins, and never otherwise; it is never 0.
    /// Every change to a tree's shape or separators allocates or frees a page, so a write-path
    /// cache filled in this epoch still names the tree's pages and bounds while the epoch is
    /// unchanged (`write::PathCache`).
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    /// A new epoch (see [`Self::epoch`]).
    fn bump_epoch(&mut self) {
        self.epoch += 1;
    }

    /// True if the transaction created page `p` and still holds it.
    pub(crate) fn is_private(&self, p: u32) -> bool {
        self.private.contains(p)
    }

    /// True if this writer stamped page `p` at one of its commits and has not reused it since.
    pub(crate) fn is_stamped(&self, p: u32) -> bool {
        self.stamped.contains(p)
    }

    /// Test-only: the published pages this transaction freed.
    #[cfg(test)]
    pub(crate) fn freed(&self) -> &[u32] {
        &self.freed
    }

    /// Test-only: the pages free for reuse.
    #[cfg(test)]
    pub(crate) fn pool(&self) -> &[u32] {
        &self.pool
    }

    /// True if the pool is empty and may be refilled from the free list.
    pub(crate) fn wants_refill(&self) -> bool {
        self.loading && self.pool.is_empty()
    }

    /// The last free record the transaction took: the next one is past it.
    pub(crate) fn last_consumed(&self) -> Option<RecordKey> {
        self.consumed.last().copied()
    }

    /// Take free record `key`'s `pages` (checked by `freelist::decode_pages`) into the pool. A
    /// page taken from the list already in this transaction is [`Error::Corrupt`], and then
    /// nothing is taken.
    pub(crate) fn load_record(&mut self, key: RecordKey, pages: Vec<u32>) -> Result<()> {
        if let Some(&p) = pages.iter().find(|&&p| self.loaded.contains(p)) {
            return Err(Error::Corrupt(format!(
                "free list: record {key:?} lists page {p}, already taken from the list"
            )));
        }
        for &p in &pages {
            self.loaded.insert(p);
            #[cfg(test)]
            self.load_log.push((self.txn, p));
        }
        self.pool.extend(pages);
        self.consumed.push(key);
        Ok(())
    }

    /// Stop refilling from the free list for the rest of the transaction.
    pub(crate) fn stop_loading(&mut self) {
        self.loading = false;
    }

    /// At commit: take the keys of the free records the transaction took, to delete.
    pub(crate) fn take_consumed(&mut self) -> Vec<RecordKey> {
        std::mem::take(&mut self.consumed)
    }

    /// At commit: the pages the free list must list, ascending: those the transaction freed and
    /// those left in its pool. A page in both, or twice in one, is a broken invariant.
    pub(crate) fn reusable(&self) -> Result<Vec<u32>> {
        let mut pages: Vec<u32> = self.freed.iter().chain(&self.pool).copied().collect();
        pages.sort_unstable();
        if let Some(w) = pages.windows(2).find(|w| w[0] == w[1]) {
            return Err(Error::Failed(format!("page {} is free twice", w[0])));
        }
        Ok(pages)
    }

    /// At commit, once the free list lists [`Self::reusable`]: the pool and the freed pages are
    /// emptied.
    pub(crate) fn saved(&mut self) {
        #[cfg(test)]
        self.free_log.extend(self.freed.iter().map(|&p| (self.txn, p)));
        self.freed.clear();
        self.pool.clear();
    }

    /// Take a page for the transaction: one from `pool`, else the next page number, for which
    /// `grow(new_page_count)` must first make room. The caller zero-fills the page. A failure
    /// changes nothing.
    pub(crate) fn allocate(&mut self, grow: impl FnOnce(u32) -> Result<()>) -> Result<u32> {
        self.bump_epoch();
        #[cfg(test)]
        if let Some(left) = self.fail_allocs_after.as_mut() {
            if *left == 0 {
                return Err(Error::Io(std::io::Error::other("injected page allocation failure")));
            }
            *left -= 1;
        }
        if let Some(p) = self.pool.pop() {
            // Its old bytes are about to be rewritten: it is no longer the page that was stamped.
            self.stamped.remove(p);
            self.private.insert(p);
            return Ok(p);
        }
        let p = self.next_pgno;
        if p == NULL_PGNO {
            return Err(Error::Failed("the file has used every page number".to_string()));
        }
        grow(p + 1)?;
        self.next_pgno = p + 1;
        self.private.insert(p);
        Ok(p)
    }

    /// Page `p` left the transaction's trees: a private page goes back to `pool` at once (no one
    /// else ever saw it), a published one to `freed`.
    pub(crate) fn free(&mut self, p: u32) {
        self.bump_epoch();
        if self.private.contains(p) {
            self.private.remove(p);
            self.pool.push(p);
        } else {
            self.freed.push(p);
        }
    }

    /// At commit: every private page, ascending, now recorded as stamped (the caller stamps their
    /// CRCs); the private set is left empty.
    pub(crate) fn stamp_private(&mut self) -> Vec<u32> {
        self.bump_epoch();
        let pages: Vec<u32> = self.private.drain().map(|p| p as u32).collect();
        for &p in &pages {
            self.stamped.insert(p);
        }
        pages
    }

    /// Test-only: take the published pages the transaction freed (the test page store's commit).
    #[cfg(test)]
    pub(crate) fn take_freed(&mut self) -> Vec<u32> {
        std::mem::take(&mut self.freed)
    }

    /// Start writing transaction `txn` after a commit ([`Self::stamp_private`] emptied the private
    /// set; the free list's save emptied the pool and the freed pages).
    pub(crate) fn begin_txn(&mut self, txn: u64) {
        debug_assert!(self.freed.is_empty(), "freed pages left unsaved");
        self.bump_epoch();
        self.txn = txn;
        self.base = (self.pool.clone(), self.next_pgno);
        self.reset_loading();
    }

    /// Forget the free records taken: the next transaction starts from the smallest again.
    fn reset_loading(&mut self) {
        self.consumed.clear();
        self.loaded.clear();
        self.loading = true;
    }

    /// Abort the writing transaction: its private pages become unreferenced (those past the
    /// page count it began with are file slack), the published pages it freed stay in their
    /// trees, and the pool and page count return to where it began. What this writer stamped at
    /// its commits stays stamped (the transaction changed none of those pages; one it took from
    /// the pool was unstamped then and is free again). The free records it took stay in the free
    /// tree, which it did not change.
    pub(crate) fn abort(&mut self) {
        self.bump_epoch();
        self.private.clear();
        self.freed.clear();
        self.pool.clone_from(&self.base.0);
        self.next_pgno = self.base.1;
        self.reset_loading();
    }

    /// Test-only: make `pages` reusable (as if their reuse gate had passed).
    #[cfg(test)]
    pub(crate) fn add_to_pool(&mut self, pages: impl IntoIterator<Item = u32>) {
        self.pool.extend(pages);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The epoch moves at every allocation and free, at the commit's stamp, and when a transaction
    /// begins or aborts, and at nothing else: a write-path cache (`write::PathCache`) filled in
    /// one transaction never matches in the next.
    #[test]
    fn the_epoch_moves_at_every_page_and_transaction_change() {
        let mut a = Allocator::new(5, 2);
        let mut seen = vec![a.epoch()];
        let mut step = |a: &Allocator, what: &str| {
            assert!(!seen.contains(&a.epoch()), "{what}: the epoch did not move");
            seen.push(a.epoch());
        };
        let p = a.allocate(|_| Ok(())).expect("alloc");
        step(&a, "allocate");
        a.free(p);
        step(&a, "free");
        a.allocate(|_| Ok(())).expect("alloc");
        step(&a, "allocate from the pool");
        a.abort();
        step(&a, "abort");
        a.allocate(|_| Ok(())).expect("alloc");
        step(&a, "allocate");
        let _ = a.stamp_private();
        step(&a, "stamp_private");
        a.saved();
        let e = a.epoch();
        let _ = (a.reusable(), a.is_private(p), a.wants_refill(), a.page_count());
        assert_eq!(a.epoch(), e, "reading the allocator keeps the epoch");
        a.begin_txn(6);
        step(&a, "begin_txn");
        assert_ne!(a.epoch(), 0, "0 is never an epoch: an empty cache never matches");
    }

    /// The end of the file grows one page per allocation, private pages come back through the
    /// pool, published ones go to `freed`, and a reused page stops counting as stamped.
    #[test]
    fn allocate_free_and_stamp() {
        let mut a = Allocator::new(5, 2);
        let mut grown = Vec::new();
        let p = a.allocate(|n| {
            grown.push(n);
            Ok(())
        });
        assert_eq!(p.expect("alloc"), 2);
        assert_eq!(grown, [3], "grow is asked for the new page count");
        assert_eq!(a.allocate(|_| Ok(())).expect("alloc"), 3);
        assert!(a.is_private(2) && a.is_private(3) && a.page_count() == 4);

        // A failed growth changes nothing.
        let failed = a.allocate(|_| Err(Error::Io(std::io::Error::other("disk full"))));
        assert!(matches!(failed, Err(Error::Io(_))));
        assert_eq!(a.page_count(), 4);

        // A private page leaving the tree is loose: reusable at once, no longer private.
        a.free(3);
        assert!(!a.is_private(3) && a.pool() == [3] && a.freed().is_empty());
        assert_eq!(a.allocate(|_| panic!("the pool serves first")).expect("alloc"), 3);
        assert!(a.is_private(3) && a.pool().is_empty());

        assert_eq!(a.stamp_private(), [2, 3]);
        assert!(!a.is_private(2) && a.is_stamped(2) && a.is_stamped(3));
        a.begin_txn(6);
        assert_eq!(a.txn(), 6);

        // A published page leaving the tree is freed, not pooled.
        a.free(2);
        assert_eq!(a.freed(), [2]);
        assert!(a.pool().is_empty());
        assert_eq!(a.take_freed(), [2]);

        // A stamped page handed out again is rewritten: it no longer counts as stamped.
        a.add_to_pool([3]);
        assert_eq!(a.allocate(|_| Ok(())).expect("alloc"), 3);
        assert!(a.is_private(3) && !a.is_stamped(3));
    }

    /// An abort returns the pool and page count to where the transaction began and forgets its
    /// private and freed pages, keeping what earlier commits stamped.
    #[test]
    fn abort_returns_to_the_start_of_the_transaction() {
        let mut a = Allocator::new(1, 2);
        let (p, q) =
            (a.allocate(|_| Ok(())).expect("alloc"), a.allocate(|_| Ok(())).expect("alloc"));
        a.free(q);
        assert_eq!(a.stamp_private(), [p]);
        a.begin_txn(2);
        assert_eq!((a.pool(), a.page_count()), (&[q][..], 4));

        assert_eq!(a.allocate(|_| Ok(())).expect("from the pool"), q);
        let r = a.allocate(|_| Ok(())).expect("grow");
        a.free(p);
        a.abort();
        assert_eq!((a.pool(), a.page_count()), (&[q][..], 4));
        assert!(!a.is_private(q) && !a.is_private(r) && a.freed().is_empty());
        assert!(a.is_stamped(p), "an aborted transaction changed no stamped page");
        assert_eq!(a.txn(), 2);
    }

    /// The injected failure fires after the given number of allocations and changes nothing.
    #[test]
    fn injected_failure_changes_nothing() {
        let mut a = Allocator::new(1, 2);
        a.fail_allocs_after = Some(1);
        assert_eq!(a.allocate(|_| Ok(())).expect("first"), 2);
        assert!(matches!(a.allocate(|_| Ok(())), Err(Error::Io(_))));
        assert_eq!(a.page_count(), 3);
        a.fail_allocs_after = None;
        assert_eq!(a.allocate(|_| Ok(())).expect("then"), 3);
    }

    /// The per-commit passes over the page sets (stamping, beginning the next transaction, an
    /// abort) cost the pages the transaction touched, not the size of the file.
    #[test]
    fn commit_bookkeeping_costs_the_pages_touched_not_the_file() {
        let big = 1 << 26; // 64 Mi pages: a million words in each set
        let mut a = Allocator::new(1, big);
        let p = a.allocate(|_| Ok(())).expect("alloc");
        a.load_record(RecordKey { txn: 0, seq: 0 }, vec![big - 7]).expect("load");
        assert_eq!(a.stamp_private(), [p]);
        a.begin_txn(2);
        let q = a.allocate(|_| Ok(())).expect("alloc");
        a.abort();
        assert!(!a.is_private(q) && a.is_stamped(p));
        let visited = a.private.visited() + a.loaded.visited();
        assert!(visited <= 8, "visited {visited} words for two pages");
    }

    /// The last page number is never handed out: it is the null page.
    #[test]
    fn page_numbers_run_out_before_null() {
        let mut a = Allocator::new(1, NULL_PGNO - 1);
        assert_eq!(a.allocate(|_| Ok(())).expect("last"), NULL_PGNO - 1);
        assert!(matches!(a.allocate(|_| Ok(())), Err(Error::Failed(_))));
        assert_eq!(a.page_count(), NULL_PGNO);
    }
}
