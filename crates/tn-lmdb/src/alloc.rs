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
//!
//! An overflow run ([`crate::overflow`]) takes `n` contiguous pages at once. Its pages are private
//! like a tree page's, and the allocator also records the run (`private_runs`), so the commit
//! stamps one CRC over the whole run ([`PrivatePages::units`]) and never counts a run page as
//! stamped: copy-on-write trust covers tree pages only. A run leaves its tree as `n` single pages
//! ([`Allocator::free_run`]).
//!
//! **Run allocation** tries, in order:
//! 1. the first fit in the pool: its lowest `n` consecutive pages ([`Allocator::pool_fits`]);
//! 2. more free records that pass the reuse gate, loaded until the pool fits or
//!    [`RUN_LOAD_RECORDS`] records are loaded, then step 1 again (`Writer::alloc_run` does this
//!    step: the records live in the free tree);
//! 3. tail reuse: pool pages `[next_pgno − j, next_pgno)` for some `j >= 1` start the run, and the
//!    file grows by the other `n − j`;
//! 4. `n` pages at the end of the file, in one growth.
//!
//! [`Allocator::allocate_run`] does steps 1, 3 and 4. Under churn, freed runs come back as single
//! pages: step 2 keeps a run from growing the file while the records the gate already passed hold
//! a fit, and its budget bounds what one allocation reads (a record lists at most
//! `freelist::record_capacity` pages). Pages loaded and left unused go back to the list at commit
//! under the keys of the records taken, so the next transaction may take them at once
//! (`freelist::save`). The budget counts records, not pages: a record often lists more
//! pages than a run needs, and a budget that stopped at the first such record left runs growing
//! the file while later records held fits (`run_churn_keeps_the_file_bounded_heavy`).

use std::collections::BTreeMap;

use crate::{
    error::{Error, Result},
    freelist::RecordKey,
    page_set::PageSet,
    tree::NULL_PGNO,
};

/// Run allocation step 2's budget: the most free records one allocation loads.
pub(crate) const RUN_LOAD_RECORDS: u32 = 8;

/// The writer's page bookkeeping for one transaction at a time (see the module docs).
#[derive(Debug)]
pub(crate) struct Allocator {
    /// The writing transaction's id.
    txn: u64,
    /// Pages this transaction created and still holds: changed in place, stamped at commit.
    /// Every page of a private run is here too.
    private: PageSet,
    /// The overflow runs this transaction created and still holds: first page to page count.
    private_runs: BTreeMap<u32, u32>,
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
    /// Test-only: the commit's free-list save lists the pages the way it did before each list
    /// was sorted once ([`Self::reusable_reference`], and a sort for the pool pages that spill
    /// to `freed`), so a test can hold the records a commit writes to the old code's.
    #[cfg(test)]
    pub(crate) reference_save: bool,
    /// Test-only: `(txn, page)` for every page a transaction loaded from the free list.
    #[cfg(test)]
    pub(crate) load_log: Vec<(u64, u32)>,
    /// Test-only: `(txn, page)` for every published page a committed transaction freed.
    #[cfg(test)]
    pub(crate) free_log: Vec<(u64, u32)>,
    /// Test-only: what run allocation did (see [`RunStats`]).
    #[cfg(test)]
    pub(crate) run_stats: RunStats,
}

/// Test-only: counts of run allocation's work since the writer opened.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RunStats {
    /// Runs served by each step of the policy (see the module docs): the pool as it was, the pool
    /// after loading more records, tail reuse, the end of the file.
    pub(crate) steps: [u64; 4],
    /// Free records loaded, by any allocation.
    pub(crate) records: u64,
}

impl Allocator {
    /// The allocator of a writer opening on a file of `page_count` pages, writing transaction
    /// `txn`.
    pub(crate) fn new(txn: u64, page_count: u32) -> Self {
        Self {
            txn,
            private: PageSet::default(),
            private_runs: BTreeMap::new(),
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
            reference_save: false,
            #[cfg(test)]
            load_log: Vec::new(),
            #[cfg(test)]
            free_log: Vec::new(),
            #[cfg(test)]
            run_stats: RunStats::default(),
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

    /// True if the transaction created the overflow run starting at page `p` and still holds it.
    pub(crate) fn is_private_run(&self, p: u32) -> bool {
        self.private_runs.contains_key(&p)
    }

    /// How the run of `n` pages from `p` stands to this transaction: `Some(true)` if it is a run
    /// the transaction created (exactly those pages), `Some(false)` if none of its pages is
    /// private (a published run), `None` if it overlaps private pages otherwise (damage: a run
    /// named so is no run this writer holds or can free).
    pub(crate) fn run_privacy(&self, p: u32, n: u32) -> Option<bool> {
        if self.private_runs.get(&p) == Some(&n) {
            return Some(true);
        }
        let end = p.checked_add(n)?;
        (p..end).all(|q| !self.private.contains(q)).then_some(false)
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

    /// True while more free records may be loaded: until one past the reuse gate or the end of
    /// the list is reached, or the commit saves the list.
    pub(crate) fn loading(&self) -> bool {
        self.loading
    }

    /// True if the pool holds `n` consecutive pages (run allocation's step 1 would take them).
    pub(crate) fn pool_fits(&mut self, n: u32) -> bool {
        find_fit(&mut self.pool, n).is_some()
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
        #[cfg(test)]
        {
            self.run_stats.records += 1;
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

    /// At commit: the pages the free list must list, as `(pool, freed)`, each ascending. The
    /// pool's pages are free in every state a durable meta or a reader holds (loaded from records
    /// the gate passed, or never published), so they may be reused at once; the freed pages are
    /// reachable from the transaction's base state, so they wait for the gate to pass the
    /// committing txn. Two kinds of duplicate are damage ([`Error::Corrupt`]):
    /// - a page twice in `freed`: a published page leaves the trees once, unless two pointers name
    ///   it (two cells naming one run, say, both deleted in the transaction);
    /// - a page in both: a free record the transaction loaded listed a page that a published tree
    ///   still holds, and the transaction then took that page out of the tree.
    ///
    /// Any other duplicate (a page twice in the pool) is a broken invariant ([`Error::Failed`]).
    /// The smallest duplicate page is the one reported.
    ///
    /// Each list is sorted once ([`sort_pages`]); [`first_duplicate`] then finds the smallest
    /// duplicate in one linear pass over the two sorted lists.
    pub(crate) fn reusable(&self) -> Result<(Vec<u32>, Vec<u32>)> {
        #[cfg(test)]
        if self.reference_save {
            return self.reusable_reference();
        }
        let mut pool = self.pool.clone();
        sort_pages(&mut pool);
        let mut freed = self.freed.clone();
        sort_pages(&mut freed);
        if let Some(p) = first_duplicate(&pool, &freed) {
            let start = freed.partition_point(|&q| q < p);
            let freed_count = freed[start..].partition_point(|&q| q == p);
            if freed_count > 1 {
                return Err(Error::Corrupt(format!(
                    "page {p} left the published trees {freed_count} times: more than one \
                     pointer names it"
                )));
            }
            if self.loaded.contains(p) && pool.binary_search(&p).is_ok() && freed_count == 1 {
                return Err(Error::Corrupt(format!(
                    "free list: page {p} is listed free, yet a published tree held it"
                )));
            }
            return Err(Error::Failed(format!("page {p} is free twice")));
        }
        Ok((pool, freed))
    }

    /// Test-only: [`Self::reusable`] as it was, one sort of both lists together to find the
    /// duplicates and one more of each: the reference the oracle tests hold it to.
    #[cfg(test)]
    pub(crate) fn reusable_reference(&self) -> Result<(Vec<u32>, Vec<u32>)> {
        let mut pages: Vec<u32> = self.freed.iter().chain(&self.pool).copied().collect();
        pages.sort_unstable();
        if let Some(w) = pages.windows(2).find(|w| w[0] == w[1]) {
            let p = w[0];
            let freed = self.freed.iter().filter(|&&q| q == p).count();
            if freed > 1 {
                return Err(Error::Corrupt(format!(
                    "page {p} left the published trees {freed} times: more than one pointer \
                     names it"
                )));
            }
            if self.loaded.contains(p) && self.pool.contains(&p) && freed == 1 {
                return Err(Error::Corrupt(format!(
                    "free list: page {p} is listed free, yet a published tree held it"
                )));
            }
            return Err(Error::Failed(format!("page {p} is free twice")));
        }
        let mut pool = self.pool.clone();
        pool.sort_unstable();
        let mut freed = self.freed.clone();
        freed.sort_unstable();
        Ok((pool, freed))
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
    /// `grow(new_page_count)` must first make room. The page keeps its old bytes: the caller
    /// lays it out (`PageWrite::alloc`). A failure changes nothing.
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

    /// Take an overflow run of `n` contiguous pages (`n >= 1`) for the transaction, by steps 1, 3
    /// and 4 of the policy (see the module docs): the first fit in `pool` ([`first_fit`]), else
    /// the pool's pages that end the file plus new ones ([`tail_pages`]), else the next `n` page
    /// numbers. `grow(new_page_count)` must first make room for new pages (one growth). The pages
    /// are private, and the run is recorded until it is freed or the commit stamps it. The caller
    /// writes every byte of the run. A failure changes nothing.
    pub(crate) fn allocate_run(
        &mut self,
        n: u32,
        grow: impl FnOnce(u32) -> Result<()>,
    ) -> Result<u32> {
        debug_assert!(n >= 1, "a run of no pages");
        self.bump_epoch();
        #[cfg(test)]
        if let Some(left) = self.fail_allocs_after.as_mut() {
            if *left == 0 {
                return Err(Error::Io(std::io::Error::other("injected run allocation failure")));
            }
            *left -= 1;
        }
        let p = match first_fit(&mut self.pool, n) {
            Some(p) => p,
            None => {
                // `first_fit` left the pool sorted, holding fewer than `n` pages that end the file.
                let tail = tail_pages(&self.pool, self.next_pgno);
                debug_assert!(tail < n, "{tail} pages ending the file fit a run of {n}");
                let p = self.next_pgno - tail;
                // An end within `u32` keeps every page of the run below the null page.
                let Some(end) = p.checked_add(n) else {
                    return Err(Error::Failed(format!(
                        "the file has no {n} page numbers left for an overflow run"
                    )));
                };
                grow(end)?;
                self.pool.truncate(self.pool.len() - tail as usize);
                self.next_pgno = end;
                p
            }
        };
        for q in p..p + n {
            // Its old bytes are about to be rewritten: it is no longer the page that was stamped.
            self.stamped.remove(q);
            self.private.insert(q);
        }
        self.private_runs.insert(p, n);
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

    /// The run of `n` pages from `p` left the transaction's trees, as `n` single pages
    /// ([`Self::free`]): a run this transaction created goes back to `pool`, a published one to
    /// `freed`. The caller checked [`Self::run_privacy`].
    pub(crate) fn free_run(&mut self, p: u32, n: u32) {
        debug_assert!(self.run_privacy(p, n).is_some(), "freeing run {p}+{n} across private pages");
        self.private_runs.remove(&p);
        for q in p..p + n {
            self.free(q);
        }
    }

    /// At commit: every private page and run, now recorded as stamped except the runs' pages (the
    /// caller stamps the CRCs of [`PrivatePages::units`]); the private set is left empty.
    pub(crate) fn stamp_private(&mut self) -> PrivatePages {
        self.bump_epoch();
        let pages: Vec<u32> = self.private.drain().map(|p| p as u32).collect();
        let runs: Vec<(u32, u32)> = std::mem::take(&mut self.private_runs).into_iter().collect();
        let private = PrivatePages { pages, runs };
        for (p, _, run) in private.units() {
            if !run {
                self.stamped.insert(p);
            }
        }
        private
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
        debug_assert!(self.private_runs.is_empty(), "private runs left unstamped");
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
        self.private_runs.clear();
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

/// The lowest `n` consecutive page numbers in `pool`, taken out of it; `None` (the pool
/// unchanged as a set) if it holds no such run. One page is simply the last in the pool, as
/// [`Allocator::allocate`] takes it; see [`find_fit`] for more.
fn first_fit(pool: &mut Vec<u32>, n: u32) -> Option<u32> {
    if n == 1 {
        return pool.pop();
    }
    let start = find_fit(pool, n)?;
    let p = pool[start];
    pool.drain(start..start + n as usize);
    Some(p)
}

/// Where the lowest `n` consecutive page numbers in `pool` start, if it holds them. One page is
/// the last in the pool. For more, the pool is a set: this sorts it (ascending, so the next
/// single-page allocation takes its highest page), which an already sorted pool makes a linear
/// pass.
fn find_fit(pool: &mut [u32], n: u32) -> Option<usize> {
    let n = n as usize;
    if n == 1 {
        return pool.len().checked_sub(1);
    }
    pool.sort_unstable();
    let mut start = 0;
    for i in 1..pool.len() {
        if pool[i] != pool[i - 1] + 1 {
            start = i;
        } else if i + 1 - start == n {
            return Some(start);
        }
    }
    None
}

/// How many of the sorted `pool`'s last pages are the pages just below `next_pgno` (the end of
/// the file): run allocation's step 3 starts a run there.
fn tail_pages(pool: &[u32], next_pgno: u32) -> u32 {
    let mut tail = 0;
    for &p in pool.iter().rev() {
        if p != next_pgno - 1 - tail {
            break;
        }
        tail += 1;
    }
    tail
}

/// What a commit stamps ([`Allocator::stamp_private`]): every private page, and the private
/// overflow runs among them.
#[derive(Debug, Default)]
pub(crate) struct PrivatePages {
    /// Every private page, ascending: the pages the commit's data sync writes.
    pub(crate) pages: Vec<u32>,
    /// The private runs as `(first page, pages)`, ascending; their pages are in `pages`.
    runs: Vec<(u32, u32)>,
}

impl PrivatePages {
    /// What gets one CRC each, ascending, as `(first page, pages, run)`: a tree page alone
    /// (`run` false: the page CRC, `page::crc`), and a run as one unit (`run` true: its CRC
    /// covers all its pages; its interior is never stamped page by page).
    pub(crate) fn units(&self) -> impl Iterator<Item = (u32, u32, bool)> + '_ {
        let mut runs = self.runs.iter().peekable();
        let mut i = 0;
        std::iter::from_fn(move || {
            let &p = self.pages.get(i)?;
            let (n, run) = match runs.peek() {
                Some(&&(q, n)) if q == p => {
                    runs.next();
                    (n, true)
                }
                _ => (1, false),
            };
            debug_assert!(
                self.pages.get(i + n as usize - 1) == Some(&(p + n - 1)),
                "run {p}+{n} is not among the private pages"
            );
            i += n as usize;
            Some((p, n, run))
        })
    }
}

/// The bits of a page number one [`sort_pages`] pass orders by.
const RADIX_BITS: u32 = 11;

/// Lists shorter than this are sorted by comparison: a radix pass costs its counters too.
const RADIX_MIN: usize = 1024;

/// Sort page numbers ascending: an LSD radix sort, [`RADIX_BITS`] of the number a pass, which
/// skips a pass whose digit every page shares (the high bits of a file's page numbers mostly
/// are). A commit's lists hold about as many pages as it wrote, so a comparison sort's
/// `log n` factor is what this saves.
pub(crate) fn sort_pages(pages: &mut Vec<u32>) {
    const DIGITS: usize = 1 << RADIX_BITS;
    const PASSES: usize = 32_usize.div_ceil(RADIX_BITS as usize);
    let n = pages.len();
    if n < RADIX_MIN {
        pages.sort_unstable();
        return;
    }
    let digit = |p: u32, pass: usize| (p >> (pass as u32 * RADIX_BITS)) as usize & (DIGITS - 1);
    let mut counts = vec![[0usize; DIGITS]; PASSES];
    for &p in pages.iter() {
        for (pass, count) in counts.iter_mut().enumerate() {
            count[digit(p, pass)] += 1;
        }
    }
    let mut scratch = vec![0u32; n];
    for (pass, count) in counts.iter_mut().enumerate() {
        if count.contains(&n) {
            continue;
        }
        let mut next = 0;
        for c in count.iter_mut() {
            let here = *c;
            *c = next;
            next += here;
        }
        for &p in pages.iter() {
            let d = digit(p, pass);
            scratch[count[d]] = p;
            count[d] += 1;
        }
        std::mem::swap(pages, &mut scratch);
    }
}

/// The smallest page listed more than once in `a` and `b` together (each ascending): the least
/// of the first repeat inside `a`, the first inside `b`, and the first page in both. One linear
/// pass each; the cross check steps both lists without a data-dependent branch.
fn first_duplicate(a: &[u32], b: &[u32]) -> Option<u32> {
    let repeat = |l: &[u32]| l.windows(2).find(|w| w[0] == w[1]).map(|w| w[0]);
    let (mut i, mut j) = (0, 0);
    let mut both = None;
    while i < a.len() && j < b.len() {
        let (x, y) = (a[i], b[j]);
        if x == y {
            both = Some(x);
            break;
        }
        i += usize::from(x < y);
        j += usize::from(y < x);
    }
    [repeat(a), repeat(b), both].into_iter().flatten().min()
}

/// `a` and `b` (each ascending) as one ascending list: what sorting their concatenation gives.
pub(crate) fn merge_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        if a[i] <= b[j] {
            out.push(a[i]);
            i += 1;
        } else {
            out.push(b[j]);
            j += 1;
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
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
        let r = a.allocate_run(3, |_| Ok(())).expect("run");
        step(&a, "allocate_run");
        a.free_run(r, 3);
        step(&a, "free_run");
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

        assert_eq!(a.stamp_private().pages, [2, 3]);
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
        assert_eq!(a.stamp_private().pages, [p]);
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
        assert_eq!(a.stamp_private().pages, [p]);
        a.begin_txn(2);
        let q = a.allocate(|_| Ok(())).expect("alloc");
        a.abort();
        assert!(!a.is_private(q) && a.is_stamped(p));
        let visited = a.private.visited() + a.loaded.visited();
        assert!(visited <= 8, "visited {visited} words for two pages");
    }

    /// A run is the first fit in the pool (the lowest `n` consecutive pages), else `n` pages
    /// from the end of the file in one growth; its pages are private and the run is recorded. A
    /// failed growth or injected failure changes nothing, and a run never reaches the null page.
    #[test]
    fn a_run_is_the_first_fit_in_the_pool_else_the_file_end() {
        let mut a = Allocator::new(1, 20);
        a.add_to_pool([12, 3, 4, 8, 5, 10, 11, 7]);
        assert_eq!(a.allocate_run(3, |_| panic!("the pool fits")).expect("run"), 3);
        assert_eq!(a.allocate_run(2, |_| panic!("the pool fits")).expect("run"), 7);
        let mut pool = a.pool().to_vec();
        pool.sort_unstable();
        assert_eq!(pool, [10, 11, 12]);
        assert!((3..6).chain(7..9).all(|p| a.is_private(p)));
        assert!(a.is_private_run(3) && a.is_private_run(7) && !a.is_private_run(4));
        assert_eq!(a.run_privacy(3, 3), Some(true));
        assert_eq!(a.run_privacy(3, 2), None, "a run across part of a private run");
        assert_eq!(a.run_privacy(4, 2), None, "a run from inside a private run");
        assert_eq!(a.run_privacy(9, 3), Some(false), "published pages");
        assert_eq!(a.run_privacy(u32::MAX - 1, 3), None, "past the last page number");

        // No fit: the end of the file, in one growth.
        let mut grown = Vec::new();
        let r = a.allocate_run(4, |n| {
            grown.push(n);
            Ok(())
        });
        assert_eq!((r.expect("run"), grown, a.page_count()), (20, vec![24], 24));
        let failed = a.allocate_run(4, |_| Err(Error::Io(std::io::Error::other("disk full"))));
        assert!(matches!(failed, Err(Error::Io(_))));
        a.fail_allocs_after = Some(0);
        assert!(matches!(a.allocate_run(1, |_| Ok(())), Err(Error::Io(_))));
        a.fail_allocs_after = None;
        assert_eq!(a.page_count(), 24, "a failed run takes nothing");
        assert!(!a.is_private(24) && !a.is_private_run(24));

        // One page is the pool's last, as `allocate` takes it.
        assert_eq!(a.allocate_run(1, |_| panic!("the pool fits")).expect("run"), 12);

        let mut end = Allocator::new(1, NULL_PGNO - 2);
        assert!(matches!(end.allocate_run(3, |_| Ok(())), Err(Error::Failed(_))));
        assert_eq!(end.allocate_run(2, |_| Ok(())).expect("the last two"), NULL_PGNO - 2);
        assert_eq!(end.page_count(), NULL_PGNO);
    }

    /// The commit stamps a run once (one unit over its pages) and every tree page alone, lists
    /// every page for the data sync, and never counts a run page as stamped.
    #[test]
    fn a_run_is_stamped_once_and_never_trusted_as_stamped() {
        let mut a = Allocator::new(1, 2);
        let p = a.allocate(|_| Ok(())).expect("page");
        let r = a.allocate_run(3, |_| Ok(())).expect("run");
        let q = a.allocate(|_| Ok(())).expect("page");
        let one = a.allocate_run(1, |_| Ok(())).expect("run");
        let r2 = a.allocate_run(2, |_| Ok(())).expect("run");
        let private = a.stamp_private();
        assert_eq!(private.pages, (2..10).collect::<Vec<_>>(), "every page, for the data sync");
        let units: Vec<_> = private.units().collect();
        assert_eq!(
            units,
            [(p, 1, false), (r, 3, true), (q, 1, false), (one, 1, true), (r2, 2, true)]
        );
        assert!(a.is_stamped(p) && a.is_stamped(q));
        assert!((r..r + 3).chain([one, r2, r2 + 1]).all(|x| !a.is_stamped(x)), "run pages");
        assert!(!a.is_private_run(r) && !a.is_private(r));
    }

    /// A freed run becomes single pages: a private one goes to the pool, a published one to the
    /// freed pages; an abort forgets the private runs.
    #[test]
    fn a_freed_run_becomes_single_pages() {
        let mut a = Allocator::new(1, 2);
        let r = a.allocate_run(3, |_| Ok(())).expect("run");
        a.free_run(r, 3);
        let mut pool = a.pool().to_vec();
        pool.sort_unstable();
        assert_eq!(pool, [2, 3, 4]);
        assert!(!a.is_private_run(r) && (2..5).all(|p| !a.is_private(p)));
        assert_eq!(a.run_privacy(r, 3), Some(false));

        let r = a.allocate_run(3, |_| Ok(())).expect("run from the pool");
        assert_eq!(r, 2);
        let _ = a.stamp_private();
        a.saved();
        a.begin_txn(2);
        assert_eq!(a.run_privacy(r, 3), Some(false), "published");
        a.free_run(r, 3);
        assert_eq!((a.freed(), a.pool()), (&[2, 3, 4][..], &[][..]));
        assert_eq!(a.take_freed(), [2, 3, 4]);

        let s = a.allocate_run(2, |_| Ok(())).expect("run");
        a.abort();
        assert!(!a.is_private_run(s) && !a.is_private(s) && a.page_count() == 5);
        assert_eq!(a.run_privacy(s, 2), Some(false), "an aborted run is gone");
        assert!(a.stamp_private().pages.is_empty(), "nothing left to stamp");
    }

    /// Step 3: with no fit in the pool, the pool pages that end the file start the run and the
    /// file grows by the rest, in one growth; the pool's other pages stay.
    #[test]
    fn a_run_reuses_the_pool_pages_that_end_the_file() {
        let mut a = Allocator::new(1, 20);
        a.add_to_pool([18, 10, 19, 15, 16]);
        let mut grown = Vec::new();
        let r = a.allocate_run(5, |n| {
            grown.push(n);
            Ok(())
        });
        assert_eq!((r.expect("run"), grown, a.page_count()), (18, vec![23], 23));
        assert_eq!(a.pool(), [10, 15, 16]);
        assert!((18..23).all(|p| a.is_private(p)) && a.run_privacy(18, 5) == Some(true));

        // Only the pages right below the end count: a gap ends the tail.
        let mut a = Allocator::new(1, 30);
        a.add_to_pool([27, 29]);
        assert_eq!(
            a.allocate_run(3, |n| if n == 32 { Ok(()) } else { panic!("{n}") }).ok(),
            Some(29)
        );
        assert_eq!((a.pool(), a.page_count()), (&[27][..], 32));
    }

    /// A failed growth leaves the allocator as it was (but for its epoch), whichever step grew:
    /// tail reuse (its pool pages stay in the pool), or the end of the file.
    #[test]
    fn a_failed_growth_leaves_the_allocator_identical() {
        let state = |a: &Allocator| {
            let mut pool = a.pool.clone();
            pool.sort_unstable();
            let pages = 0..a.next_pgno + 8;
            let private: Vec<u32> = pages.clone().filter(|&p| a.is_private(p)).collect();
            let stamped: Vec<u32> = pages.filter(|&p| a.is_stamped(p)).collect();
            (pool, a.next_pgno, private, a.private_runs.clone(), stamped, a.freed.clone())
        };
        let mut a = Allocator::new(1, 2);
        let published = a.allocate(|_| Ok(())).expect("page");
        let _ = a.allocate_run(2, |_| Ok(())).expect("run");
        let _ = a.stamp_private();
        a.saved();
        a.begin_txn(2);
        let _ = a.allocate(|_| Ok(())).expect("page");
        let _ = a.allocate_run(3, |_| Ok(())).expect("run");
        a.free(published);
        let end = a.page_count();
        a.add_to_pool([end - 2, end - 1]);
        let full = || Err(Error::Io(std::io::Error::other("disk full")));
        let before = state(&a);
        assert!(matches!(a.allocate_run(4, |_| full()), Err(Error::Io(_))));
        assert_eq!(state(&a), before, "a failed run by tail reuse");
        a.pool.clear();
        for n in [1, 2] {
            let before = state(&a);
            assert!(matches!(a.allocate_run(n, |_| full()), Err(Error::Io(_))), "{n}");
            assert_eq!(state(&a), before, "a failed run of {n} from the end of the file");
        }
    }

    /// The last page number is never handed out: it is the null page.
    #[test]
    fn page_numbers_run_out_before_null() {
        let mut a = Allocator::new(1, NULL_PGNO - 1);
        assert_eq!(a.allocate(|_| Ok(())).expect("last"), NULL_PGNO - 1);
        assert!(matches!(a.allocate(|_| Ok(())), Err(Error::Failed(_))));
        assert_eq!(a.page_count(), NULL_PGNO);
    }

    /// A random allocator state for the [`Allocator::reusable`] oracle: `pool` and `freed` of
    /// up to `max` distinct pages each, drawn from a span of 64 pages up to `top` pages,
    /// some pool pages loaded, then, by the case's bits, a duplicate of each kind: a page twice
    /// in `freed`, a loaded pool page also freed, an unloaded page in both, a page twice in the
    /// pool, and a page twice freed and in the pool.
    fn random_state(rng: &mut crate::tree::test_harness::Rng, max: usize, top: u64) -> Allocator {
        let span = match rng.below(4) {
            0 => 64,
            1 => 1 << 16,
            2 => 1 << 24,
            _ => top,
        };
        let cap = usize::try_from(span / 4).unwrap_or(usize::MAX).min(max);
        let mut seen = std::collections::HashSet::new();
        let mut draw = |rng: &mut crate::tree::test_harness::Rng, n: usize| -> Vec<u32> {
            let mut out = Vec::with_capacity(n);
            while out.len() < n {
                let p = 2 + rng.below(span) as u32;
                if seen.insert(p) {
                    out.push(p);
                }
            }
            out
        };
        let mut a = Allocator::new(1, u32::MAX);
        let n = rng.len_upto(cap);
        a.pool = draw(rng, n);
        let n = rng.len_upto(cap);
        a.freed = draw(rng, n);
        for &p in &a.pool {
            if rng.below(3) == 0 {
                a.loaded.insert(p);
            }
        }
        let bits = rng.below(64);
        let pick = |rng: &mut crate::tree::test_harness::Rng, v: &[u32]| {
            (!v.is_empty()).then(|| v[rng.below(v.len() as u64) as usize])
        };
        let at = |rng: &mut crate::tree::test_harness::Rng, v: &mut Vec<u32>, p: u32| {
            let i = rng.below(v.len() as u64 + 1) as usize;
            v.insert(i, p);
        };
        if bits & 1 != 0 {
            if let Some(p) = pick(rng, &a.freed) {
                at(rng, &mut a.freed, p);
            }
        }
        if bits & 2 != 0 {
            if let Some(p) = pick(rng, &a.pool) {
                a.loaded.insert(p);
                at(rng, &mut a.freed, p);
            }
        }
        if bits & 4 != 0 {
            if let Some(p) = pick(rng, &a.freed) {
                at(rng, &mut a.pool, p);
            }
        }
        if bits & 8 != 0 {
            if let Some(p) = pick(rng, &a.pool) {
                at(rng, &mut a.pool, p);
            }
        }
        if bits & 16 != 0 {
            if let Some(p) = pick(rng, &a.freed) {
                at(rng, &mut a.freed, p);
                at(rng, &mut a.pool, p);
                if bits & 32 != 0 {
                    a.loaded.insert(p);
                }
            }
        }
        a
    }

    /// The oracle: [`Allocator::reusable`] gives what [`Allocator::reusable_reference`] (the
    /// combined sort it replaced) gives, the same `(pool, freed)` or the same error, by variant
    /// and message, over `cases` random states with and without each kind of duplicate, of up to
    /// `sizes[0]` pages a list in one case of 50, `sizes[1]` in five, `sizes[2]` in the rest,
    /// drawn from up to `top` page numbers; every outcome occurs.
    fn reusable_oracle(seed: u64, cases: u32, sizes: [usize; 3], top: u64) {
        let mut rng = crate::tree::test_harness::Rng(seed);
        let mut seen = [0_u32; 4];
        for case in 0..cases {
            let max = match case % 50 {
                0 => sizes[0],
                1..=5 => sizes[1],
                _ => sizes[2],
            };
            let a = random_state(&mut rng, max, top);
            let (got, want) = (a.reusable(), a.reusable_reference());
            let outcome = match (&got, &want) {
                (Ok(got), Ok(want)) => {
                    assert_eq!(got, want, "case {case}");
                    0
                }
                (Err(g), Err(w)) => {
                    assert_eq!(format!("{g:?}"), format!("{w:?}"), "case {case}");
                    let m = format!("{w:?}");
                    if m.contains("left the published trees") {
                        1
                    } else if m.contains("listed free, yet") {
                        2
                    } else {
                        3
                    }
                }
                _ => panic!("case {case}: {got:?} against the reference's {want:?}"),
            };
            seen[outcome] += 1;
        }
        assert!(seen.iter().all(|&n| n > 0), "every outcome: {seen:?}");
    }

    /// [`reusable_oracle`] on lists of up to 20K pages of up to 2^26 page numbers (`loaded`, a
    /// bit set, stays small).
    #[test]
    fn reusable_matches_the_reference() {
        reusable_oracle(0x0a11_0c5a_7e00_0001, 300, [20_000, 5_000, 1_000], 1 << 26);
    }

    /// [`reusable_oracle`] on lists of up to 200K pages, a peak commit's, of every page number.
    #[test]
    #[ignore = "heavy: run with --release"]
    fn reusable_matches_the_reference_heavy() {
        let top = u64::from(u32::MAX) - 2;
        reusable_oracle(0x0a11_0c5a_7e00_0002, 400, [200_000, 20_000, 2_000], top);
    }

    /// [`sort_pages`] sorts as a comparison sort does over `cases` lists: either side of
    /// [`RADIX_MIN`] or up to `max` pages, with duplicates, and page numbers whose high digits
    /// vary or are shared.
    fn sort_oracle(seed: u64, cases: usize, max: usize) {
        let mut rng = crate::tree::test_harness::Rng(seed);
        for case in 0..cases {
            let n = match case % 4 {
                0 => rng.len_upto(RADIX_MIN * 2),
                1 => RADIX_MIN - 1 + rng.below(3) as usize,
                _ => rng.len_upto(max),
            };
            let span = [16_u64, 1 << 11, 1 << 22, 1 << 32][case % 4];
            let span = if case % 8 >= 4 { 1 << 32 } else { span };
            let base = if case % 16 >= 8 { u32::MAX - span.min(1 << 31) as u32 } else { 0 };
            let mut v: Vec<u32> =
                (0..n).map(|_| base.wrapping_add(rng.below(span) as u32)).collect();
            let mut want = v.clone();
            want.sort_unstable();
            sort_pages(&mut v);
            assert_eq!(v, want, "case {case}: {n} pages");
        }
    }

    /// [`sort_oracle`] on lists of up to 20K pages.
    #[test]
    fn sort_pages_matches_a_comparison_sort() {
        sort_oracle(0x50a7_0000_0000_0001, 200, 20_000);
    }

    /// [`sort_oracle`] on lists of up to 400K pages, a peak commit's two lists together.
    #[test]
    #[ignore = "heavy: run with --release"]
    fn sort_pages_matches_a_comparison_sort_heavy() {
        sort_oracle(0x50a7_0000_0000_0002, 400, 400_000);
    }

    /// [`merge_sorted`] gives what sorting the two lists' concatenation gives.
    #[test]
    fn merge_sorted_matches_a_sort() {
        let mut rng = crate::tree::test_harness::Rng(0x3e26_0000_0000_0001);
        for case in 0..200 {
            let list = |rng: &mut crate::tree::test_harness::Rng| {
                let n = rng.len_upto(3_000);
                let mut v: Vec<u32> = (0..n).map(|_| rng.below(5_000) as u32).collect();
                v.sort_unstable();
                v
            };
            let (a, b) = (list(&mut rng), list(&mut rng));
            let mut want = [a.clone(), b.clone()].concat();
            want.sort_unstable();
            assert_eq!(merge_sorted(&a, &b), want, "case {case}");
        }
    }

    /// On-demand probe: [`Allocator::reusable`] against [`Allocator::reusable_reference`], and
    /// [`sort_pages`] against `sort_unstable`, on a peak commit's lists (100K freed pages, 100K
    /// pool pages, distinct, of a 4M-page file). Run with
    /// `cargo test --release -p tn-lmdb reusable_probe -- --ignored --nocapture`.
    #[test]
    #[ignore = "on-demand probe; run with --release --ignored --nocapture"]
    fn reusable_probe() {
        let mut rng = crate::tree::test_harness::Rng(0x9e0b_0000_0000_0001);
        let mut a = Allocator::new(1, u32::MAX);
        let mut pages: Vec<u32> = (2..4_000_002).collect();
        for i in (1..pages.len()).rev() {
            pages.swap(i, rng.below(i as u64 + 1) as usize);
        }
        a.pool = pages[..100_000].to_vec();
        a.freed = pages[100_000..200_000].to_vec();
        let time = |f: &mut dyn FnMut()| {
            let mut ns: Vec<u128> = (0..21)
                .map(|_| {
                    let t = std::time::Instant::now();
                    f();
                    t.elapsed().as_nanos()
                })
                .collect();
            ns.sort_unstable();
            ns[ns.len() / 2] as f64 / 1e6
        };
        let reference = time(&mut || drop(std::hint::black_box(a.reusable_reference())));
        let new = time(&mut || drop(std::hint::black_box(a.reusable())));
        let comparison = time(&mut || {
            let mut v = a.pool.clone();
            v.sort_unstable();
            std::hint::black_box(v);
        });
        let radix = time(&mut || {
            let mut v = a.pool.clone();
            sort_pages(&mut v);
            std::hint::black_box(v);
        });
        println!(
            "\nreusable probe, 100K + 100K pages (medians of 21): reference {reference:.3} ms, \
             reusable {new:.3} ms; one 100K list: sort_unstable {comparison:.3} ms, sort_pages \
             {radix:.3} ms"
        );
    }
}
