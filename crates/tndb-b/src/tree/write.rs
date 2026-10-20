//! The write path: copy-on-write of the root-to-leaf path, insert and replace with leaf and
//! branch splits, and a new root when the old one splits; delete, with emptied pages unlinked and
//! a single-child root collapsed; and freeing a whole tree.
//!
//! A put first plans from a read-only descent (checking every page on the path, and verifying
//! each published page it will copy), then allocates every page it can need — the copies of the
//! published pages on the path and, if the leaf overflows, the worst case of the split cascade —
//! and only then changes anything. A failed allocation (a full disk, say) therefore leaves the
//! tree exactly as it was: the same root handle and every reachable page byte for byte. A delete
//! plans and reserves the same way ([`delete`]).

use super::{
    read::{descend, get},
    PageRead, PageWrite, TreeRoot, MAX_HEIGHT,
};
use crate::{
    crc::{self, CrcState},
    error::{Error, Result},
    page::{self, Find, Fit, Geometry, Kind, Search},
    page_set::PageSet,
};

#[cfg(test)]
thread_local! {
    /// Test-only: how many published pages copy-on-write verified (CRC and structure).
    pub(crate) static COW_VERIFIES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// What a put does to the leaf it reaches.
#[derive(Clone, Copy, Debug)]
enum LeafChange {
    /// Replace entry `i`'s value in place.
    Replace(usize),
    /// Insert the entry at `i`.
    Insert(usize),
    /// The leaf overflows: split it, inserting the entry at `at` (after deleting the old entry
    /// there first when `replace`), in append mode when the entry goes past the end of the tree's
    /// rightmost leaf.
    Split { at: usize, replace: bool, append: bool },
}

/// A broken invariant of this module (not damage on disk): the writer must stop.
#[cold]
fn bug(what: impl std::fmt::Display) -> Error {
    Error::Failed(format!("b+tree write invariant broken: {what}"))
}

/// Insert `(key, value)` into the tree `root` names, or replace the value if `key` is present
/// (returning true). Entries over the size limits are refused ([`Error::KeyTooLarge`],
/// [`Error::ValueTooLarge`]) before anything changes; so is damage found on the path
/// ([`Error::Corrupt`]) and a failed allocation. `root` is updated: a new root page when the old
/// one is copied or splits, the height, the entry count.
pub(crate) fn put<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    key: &[u8],
    value: &[u8],
) -> Result<bool> {
    store.geometry().check_entry(key.len(), value.len())?;
    root.check()?;
    let txn = store.txn();
    if root.is_empty() {
        let p = store.alloc()?;
        let leaf = store.page_mut(p)?;
        page::init_leaf(leaf, p, txn);
        if page::leaf_insert(leaf, 0, key, value)? == Fit::Full {
            return Err(bug("an entry overflows an empty leaf"));
        }
        *root = TreeRoot { root: p, root_txn: txn as u32, height: 1, entries: 1 };
        return Ok(false);
    }
    let mut path = Vec::with_capacity(root.height as usize);
    let (leaf, change, needed) = plan(&*store, root, key, value, &mut path)?;
    let mut reserved = reserve(store, needed)?;
    let result = make_path_writable(store, root, &mut path, leaf, &mut reserved)
        .and_then(|leaf| apply(store, root, path, leaf, key, value, change, &mut reserved));
    // The split's worst case may not have happened: the pages it did not use are reusable.
    for p in reserved {
        store.free(p);
    }
    result
}

/// The read-only first half of a put: descend to the leaf for `key`, checking each page (a
/// published page this writer did not stamp itself is verified, CRC and structure, since its copy
/// will be stamped afresh; every page's keys must lie inside its ancestors' separators), and
/// decide the change. Returns the leaf, the change, and how many pages it can need.
fn plan<S: PageWrite + ?Sized>(
    store: &S,
    root: &TreeRoot,
    key: &[u8],
    value: &[u8],
    path: &mut Vec<(u32, usize)>,
) -> Result<(u32, LeafChange, usize)> {
    let geometry = store.geometry();
    let mut copies = 0;
    let leaf = descend(
        store,
        root,
        key,
        |pgno, page, lo, hi| {
            if !store.is_private(pgno) {
                copies += 1;
            }
            check_path_page(store, &geometry, pgno, page, lo, hi)
        },
        Some(path),
    )?
    .ok_or_else(|| bug("a non-empty tree without a leaf"))?;

    let change = match page::leaf_find(leaf.page, key)? {
        Find::Found { index: i, value: old } => {
            let old = old.len();
            if value.len() <= old || value.len() - old <= page::free_space(leaf.page)? {
                LeafChange::Replace(i)
            } else {
                LeafChange::Split { at: i, replace: true, append: false }
            }
        }
        Find::NotFound { index: at, .. } => {
            if page::fits(leaf.page, page::cell_size_leaf(key.len(), value.len()))? {
                LeafChange::Insert(at)
            } else {
                let append = at == page::nkeys(leaf.page) && is_rightmost(store, path)?;
                LeafChange::Split { at, replace: false, append }
            }
        }
    };
    let split = match change {
        LeafChange::Split { .. } => split_pages(store, root, path, leaf.page, key.len())?,
        _ => 0,
    };
    Ok((leaf.pgno, change, copies + split))
}

/// The checks a writer's descent makes of each page on its path: a published page this writer
/// did not stamp itself is verified, CRC and structure ([`verify_published`]: it may be copied,
/// and its contents decide what the write does), and every page's keys must lie inside its
/// ancestors' separators.
fn check_path_page<S: PageWrite + ?Sized>(
    store: &S,
    geometry: &Geometry,
    pgno: u32,
    page: &[u8],
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Result<()> {
    if !store.is_private(pgno) && !store.is_stamped(pgno) {
        verify_published(page, geometry)?;
    }
    if !page::keys_in_range(page, lo, hi)? {
        return Err(Error::Corrupt(format!(
            "page {pgno} holds keys outside its parents' separators"
        )));
    }
    Ok(())
}

/// A published page about to be copied must pass its CRC and [`page::validate`]: the copy is
/// stamped afresh at commit, so copying a page damaged at rest would make the damage CRC-valid
/// and invisible, and the mutators keep a page valid only if it was valid.
fn verify_published(page: &[u8], geometry: &Geometry) -> Result<()> {
    #[cfg(test)]
    COW_VERIFIES.with(|n| n.set(n.get() + 1));
    if crc::crc_state(page) != CrcState::Valid {
        return Err(Error::Corrupt(format!(
            "page {} fails its CRC (at-rest corruption); refusing to copy it",
            page::pgno(page)
        )));
    }
    page::validate(page, geometry)
}

/// True if `path` is the tree's rightmost path (every step takes the last child).
fn is_rightmost<S: PageRead + ?Sized>(store: &S, path: &[(u32, usize)]) -> Result<bool> {
    for &(p, ci) in path {
        if ci + 1 != page::nkeys(store.page(p)?) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The length of the longest key of a page.
fn longest_key(page: &[u8]) -> Result<usize> {
    let kind = page::kind(page)?;
    let mut longest = 0;
    for i in 0..page::nkeys(page) {
        let key = match kind {
            Kind::Leaf => page::leaf_key(page, i)?,
            Kind::Branch => page::branch_key(page, i)?,
        };
        longest = longest.max(key.len());
    }
    Ok(longest)
}

/// The worst-case page count of splitting the full `leaf` on `path`: the new right leaf, a new
/// right branch for each ancestor the pending separator may not fit (the split climbs while it
/// does not), and a new root if the root splits. Fullness is by bytes, so it depends on the
/// separator's length, unknown until the split picks it; but a separator is a key of the page
/// that splits or the entry it receives, so the longest of those bounds it at every level.
fn split_pages<S: PageRead + ?Sized>(
    store: &S,
    root: &TreeRoot,
    path: &[(u32, usize)],
    leaf: &[u8],
    klen: usize,
) -> Result<usize> {
    let mut sep_bound = klen.max(longest_key(leaf)?);
    let mut needed = 1;
    for &(p, _) in path.iter().rev() {
        let branch = store.page(p)?;
        if page::fits(branch, page::cell_size_branch(sep_bound))? {
            return Ok(needed);
        }
        needed += 1;
        sep_bound = sep_bound.max(longest_key(branch)?);
    }
    if root.height >= MAX_HEIGHT {
        return Err(Error::Failed(format!("the tree is at the height limit of {MAX_HEIGHT}")));
    }
    Ok(needed + 1)
}

/// Allocate `n` pages for a put, or none: on a failure the pages taken so far go back (they are
/// private, so straight to the pool) and nothing else has changed.
fn reserve<S: PageWrite + ?Sized>(store: &mut S, n: usize) -> Result<Vec<u32>> {
    let mut pages = Vec::with_capacity(n);
    for _ in 0..n {
        match store.alloc() {
            Ok(p) => pages.push(p),
            Err(e) => {
                for p in pages.into_iter().rev() {
                    store.free(p);
                }
                return Err(e);
            }
        }
    }
    Ok(pages)
}

/// The writable version of page `p`: `p` itself if private, else a copy in a reserved page,
/// re-stamped with its new page number and this transaction's id and its trailer zeroed (dirty);
/// the original is freed. (The planning descent verified it.)
fn writable<S: PageWrite + ?Sized>(
    store: &mut S,
    p: u32,
    reserved: &mut Vec<u32>,
    scratch: &mut Vec<u8>,
) -> Result<u32> {
    if store.is_private(p) {
        return Ok(p);
    }
    let q = reserved.pop().ok_or_else(|| bug("copy-on-write ran out of reserved pages"))?;
    let txn = store.txn();
    scratch.clear();
    scratch.extend_from_slice(store.page(p)?);
    let copy = store.page_mut(q)?;
    if copy.len() != scratch.len() {
        return Err(bug(format!("page {q} of {} bytes, not {}", copy.len(), scratch.len())));
    }
    copy.copy_from_slice(scratch);
    page::set_pgno_txn(copy, q, txn);
    crc::zero_crc(copy);
    store.free(p);
    Ok(q)
}

/// Make the planned root-to-leaf path writable, top-down: a copied child is re-pointed in its
/// (already writable) parent with its new `child_txn`, and a copied root becomes the tree's root.
/// `path` is updated to the writable branches; returns the writable leaf.
fn make_path_writable<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    path: &mut [(u32, usize)],
    leaf: u32,
    reserved: &mut Vec<u32>,
) -> Result<u32> {
    let txn = store.txn() as u32;
    let mut scratch = Vec::new();
    let mut parent: Option<(u32, usize)> = None;
    let mut current = leaf;
    for depth in 0..=path.len() {
        let old = path.get(depth).map_or(leaf, |&(p, _)| p);
        current = writable(store, old, reserved, &mut scratch)?;
        if current != old {
            match parent {
                None => (root.root, root.root_txn) = (current, txn),
                Some((p, ci)) => page::branch_set_child(store.page_mut(p)?, ci, current, txn)?,
            }
        }
        if let Some(step) = path.get_mut(depth) {
            step.0 = current;
            parent = Some(*step);
        }
    }
    Ok(current)
}

/// Apply the planned change to the writable `leaf`, splitting up the writable `path` with pages
/// from `reserved`. Returns true for a replaced value.
#[allow(clippy::too_many_arguments)]
fn apply<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    mut path: Vec<(u32, usize)>,
    leaf: u32,
    key: &[u8],
    value: &[u8],
    change: LeafChange,
    reserved: &mut Vec<u32>,
) -> Result<bool> {
    let fit = |fit: Fit, what: &str| match fit {
        Fit::Done => Ok(()),
        Fit::Full => Err(bug(format!("{what} did not fit as planned"))),
    };
    let (at, replace, append) = match change {
        LeafChange::Replace(i) => {
            fit(page::leaf_replace_value(store.page_mut(leaf)?, i, value)?, "a value")?;
            return Ok(true);
        }
        LeafChange::Insert(i) => {
            fit(page::leaf_insert(store.page_mut(leaf)?, i, key, value)?, "an entry")?;
            root.entries += 1;
            return Ok(false);
        }
        LeafChange::Split { at, replace, append } => (at, replace, append),
    };
    if replace {
        page::leaf_delete(store.page_mut(leaf)?, at)?;
    }
    let txn = store.txn();
    let txn32 = txn as u32;
    let mut take = || reserved.pop().ok_or_else(|| bug("a split ran out of reserved pages"));

    // Split the leaf in place; build the right leaf in a scratch buffer.
    let mut scratch = vec![0; store.geometry().page_size()];
    let mut right = take()?;
    page::init_leaf(&mut scratch, right, txn);
    let mut sep = page::split_leaf(store.page_mut(leaf)?, &mut scratch, at, key, value, append)?;
    store.page_mut(right)?.copy_from_slice(&scratch);

    // Insert the separator up the path, splitting each branch it does not fit.
    let mut placed = false;
    while let Some((p, ci)) = path.pop() {
        if page::branch_insert(store.page_mut(p)?, ci + 1, &sep, right, txn32)? == Fit::Done {
            placed = true;
            break;
        }
        let q = take()?;
        page::init_branch(&mut scratch, q, txn);
        sep = page::split_branch(
            store.page_mut(p)?,
            &mut scratch,
            ci + 1,
            &sep,
            right,
            txn32,
            append,
        )?;
        store.page_mut(q)?.copy_from_slice(&scratch);
        right = q;
    }
    // The root split: grow a new root one level up.
    if !placed {
        let q = take()?;
        page::init_root(store.page_mut(q)?, q, txn, (root.root, txn32), &sep, (right, txn32))?;
        (root.root, root.root_txn) = (q, txn32);
        root.height += 1;
    }
    if !replace {
        root.entries += 1;
    }
    Ok(replace)
}

/// What a delete does to the tree once the entry is gone from its leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeleteShape {
    /// The leaf keeps other entries: only the leaf changes.
    Leaf,
    /// The leaf empties: path step `d` is the deepest branch with another child, and loses the
    /// child the path took; the emptied pages below it leave the tree.
    Unlink(usize),
    /// The leaf empties and the root is left with one child: the tree below that child (past
    /// any single-child branches) becomes the tree; the root and every page passed leave it.
    Collapse { root: u32, root_txn: u32, height: u32 },
    /// The leaf held the tree's last entry: the tree becomes [`TreeRoot::EMPTY`], every page on
    /// the path (the whole tree) leaving it.
    Empty,
}

/// What [`plan_delete`] decided.
#[derive(Debug)]
struct DeletePlan {
    /// The leaf holding the key.
    leaf: u32,
    /// The entry's index in the leaf.
    at: usize,
    /// The change to the tree.
    shape: DeleteShape,
    /// The pages that leave the tree (as they are now: published or private).
    dropped: Vec<u32>,
    /// How many published pages the delete copies.
    copies: usize,
}

/// Delete `key` from the tree `root` names, returning whether it was present. The key is looked
/// up first, with a lookup's checks ([`get`]): a miss copies, verifies and frees nothing.
///
/// A hit plans from a read-only descent (each page on the path checked as a put checks it,
/// published pages verified before the delete trusts or copies them), reserves the copies of the
/// published pages that stay in the tree, and only then changes anything, so a failed allocation
/// leaves the tree exactly as it was. The entry leaves its leaf; a leaf it empties is unlinked from
/// its parent, and a parent left with no child from its own, up the path; a root branch left with
/// a single child gives way to it ([`DeleteShape::Collapse`]). A delete of the last entry leaves
/// [`TreeRoot::EMPTY`] (no pages), as an empty tree always is. Emptied pages are never copied:
/// they leave the tree as they are, a published one freed and a private one reusable at once.
///
/// Pages that are merely sparse are not merged or rebalanced: only empty pages leave the tree, so
/// a tree never keeps (or scans) pages that hold nothing, but a tree shrunk by scattered deletes
/// keeps part-full pages. `root` is updated (root page, height, entry count).
pub(crate) fn delete<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    key: &[u8],
) -> Result<bool> {
    if get(&*store, root, key)?.is_none() {
        return Ok(false);
    }
    let mut path = Vec::with_capacity(root.height as usize);
    let plan = plan_delete(&*store, root, key, &mut path)?;
    let mut reserved = reserve(store, plan.copies)?;
    let result = apply_delete(store, root, path, &plan, &mut reserved);
    // Every reserved page is used, so this frees nothing unless the apply failed part way.
    for p in reserved {
        store.free(p);
    }
    result.map(|()| true)
}

/// The read-only first half of a delete of `key` (present: [`delete`] looked it up): descend to
/// its leaf, checking each page ([`check_path_page`]), and decide the change.
fn plan_delete<S: PageWrite + ?Sized>(
    store: &S,
    root: &TreeRoot,
    key: &[u8],
    path: &mut Vec<(u32, usize)>,
) -> Result<DeletePlan> {
    let geometry = store.geometry();
    let leaf = descend(
        store,
        root,
        key,
        |pgno, page, lo, hi| check_path_page(store, &geometry, pgno, page, lo, hi),
        Some(path),
    )?
    .ok_or_else(|| bug("a deleted key without a leaf"))?;
    let Search::Found(at) = page::leaf_search(leaf.page, key)? else {
        return Err(bug("a key found by the lookup is missing from its leaf"));
    };
    let mut dropped = Vec::new();
    let shape = if page::nkeys(leaf.page) > 1 {
        DeleteShape::Leaf
    } else {
        let mut keeper = None;
        for (d, &(p, _)) in path.iter().enumerate().rev() {
            if page::nkeys(store.page(p)?) > 1 {
                keeper = Some(d);
                break;
            }
        }
        match keeper {
            None => DeleteShape::Empty,
            Some(0) if page::nkeys(store.page(path[0].0)?) == 2 => {
                let (r, ci) = path[0];
                let (child, child_txn) = page::branch_child(store.page(r)?, 1 - ci)?;
                collapse_to(store, &geometry, child, child_txn, root.height - 1, &mut dropped)?
            }
            Some(d) => DeleteShape::Unlink(d),
        }
    };
    // The handle's count must agree with the tree: the last entry goes only with an `Empty`, and
    // every other shape leaves an entry (a non-root leaf is never empty).
    let count_agrees = match shape {
        DeleteShape::Empty => root.entries == 1,
        _ => root.entries >= 2,
    };
    if !count_agrees {
        return Err(Error::Corrupt(format!(
            "tree root {root:?} does not count the entries its pages hold ({shape:?})"
        )));
    }
    let kept = match shape {
        DeleteShape::Leaf => path.len() + 1,
        DeleteShape::Unlink(d) => d + 1,
        DeleteShape::Collapse { .. } | DeleteShape::Empty => 0,
    };
    let pages = path.iter().map(|&(p, _)| p).chain([leaf.pgno]);
    let copies = pages.clone().take(kept).filter(|&p| !store.is_private(p)).count();
    dropped.extend(pages.skip(kept));
    Ok(DeletePlan { leaf: leaf.pgno, at, shape, dropped, copies })
}

/// Where a collapse ends: from the root's remaining child `(pgno, txn)` at `height`, follow each
/// branch with a single child (pushed on `passed`: they leave the tree too) down to the first page
/// that is a leaf or has two children or more, the new root. Each page whose pointer the collapse
/// follows, and the one it ends at, gets a descent's checks (the page its parent names, a branch
/// above the leaf level and a leaf at it, a non-empty leaf) and, if published and not stamped here,
/// is verified ([`verify_published`]): a pointer damaged at rest must not become the tree's root.
fn collapse_to<S: PageWrite + ?Sized>(
    store: &S,
    geometry: &Geometry,
    mut pgno: u32,
    mut txn: u32,
    mut height: u32,
    passed: &mut Vec<u32>,
) -> Result<DeleteShape> {
    loop {
        let page = store.page(pgno)?;
        let header_txn = page::txn(page);
        if page::pgno(page) != pgno || header_txn as u32 != txn {
            return Err(Error::Corrupt(format!(
                "page {pgno} (txn {header_txn}) is not the copy its parent names (txn {txn}): a \
                 redirected or stale pointer"
            )));
        }
        let kind = page::kind(page)?;
        if (kind == Kind::Leaf) != (height == 1) {
            return Err(Error::Corrupt(format!(
                "page {pgno} is a {kind:?} {height} levels above the leaves' level"
            )));
        }
        if !store.is_private(pgno) && !store.is_stamped(pgno) {
            verify_published(page, geometry)?;
        }
        if kind == Kind::Leaf && page::nkeys(page) == 0 {
            return Err(Error::Corrupt(format!("non-root leaf {pgno} is empty")));
        }
        if kind == Kind::Leaf || page::nkeys(page) > 1 {
            return Ok(DeleteShape::Collapse { root: pgno, root_txn: txn, height });
        }
        passed.push(pgno);
        (pgno, txn) = page::branch_child(page, 0)?;
        height -= 1;
    }
}

/// Apply a planned delete: make the pages that stay on the path writable (with pages from
/// `reserved`), take the entry or the emptied child out, free the dropped pages and update the
/// handle.
fn apply_delete<S: PageWrite + ?Sized>(
    store: &mut S,
    root: &mut TreeRoot,
    mut path: Vec<(u32, usize)>,
    plan: &DeletePlan,
    reserved: &mut Vec<u32>,
) -> Result<()> {
    match plan.shape {
        DeleteShape::Leaf => {
            let leaf = make_path_writable(store, root, &mut path, plan.leaf, reserved)?;
            page::leaf_delete(store.page_mut(leaf)?, plan.at)?;
        }
        DeleteShape::Unlink(d) => {
            let (keeper, ci) = path[d];
            path.truncate(d);
            let keeper = make_path_writable(store, root, &mut path, keeper, reserved)?;
            page::branch_delete(store.page_mut(keeper)?, ci)?;
        }
        DeleteShape::Collapse { root: new_root, root_txn, height } => {
            *root = TreeRoot { root: new_root, root_txn, height, entries: root.entries };
        }
        DeleteShape::Empty => {}
    }
    for &p in &plan.dropped {
        store.free(p);
    }
    if plan.shape == DeleteShape::Empty {
        *root = TreeRoot::EMPTY;
    } else {
        root.entries -= 1;
    }
    Ok(())
}

/// Every page of the tree `root` names, root first, level by level, collected without changing
/// anything (so a caller freeing them refuses damage before it frees a page). Each branch is read
/// with a descent's checks (the page its parent names, a branch above the leaf level) and, if
/// published and not stamped here, verified ([`verify_published`]) before its child pointers are
/// followed: they decide which pages leave the tree, so a pointer damaged at rest must not free a
/// live page. Every child must be a tree page of the file reached once. Leaves are taken from
/// their parents' pointers and only the first is read (it must be a leaf: the tree is balanced,
/// so that checks the height). Damage is [`Error::Corrupt`].
pub(crate) fn tree_pages<S: PageWrite + ?Sized>(store: &S, root: &TreeRoot) -> Result<Vec<u32>> {
    root.check()?;
    if root.is_empty() {
        return Ok(Vec::new());
    }
    let geometry = store.geometry();
    let page_count = store.page_count();
    let corrupt = |what: String| Error::Corrupt(format!("tree walk: {what}"));
    let check_header = |pgno: u32, txn: u32| -> Result<&[u8]> {
        let page = store.page(pgno)?;
        if page::pgno(page) != pgno || page::txn(page) as u32 != txn {
            return Err(corrupt(format!(
                "page {pgno} is not the copy its parent names (txn {txn})"
            )));
        }
        Ok(page)
    };
    // Range-check the root as the children are, before it sizes the dense set: a damaged root
    // number must not allocate a bitset of up to 2^32 bits before it is refused.
    if root.root < 2 || root.root >= page_count {
        return Err(corrupt(format!(
            "root {} is outside the file's {page_count} pages",
            root.root
        )));
    }
    let mut seen = PageSet::default();
    seen.insert(root.root);
    let mut pages = vec![root.root];
    let mut level = vec![(root.root, root.root_txn)];
    for _ in 1..root.height {
        let mut next = Vec::new();
        for &(p, txn) in &level {
            let page = check_header(p, txn)?;
            if page::kind(page)? != Kind::Branch {
                return Err(corrupt(format!("page {p} is a leaf above the leaves' level")));
            }
            if !store.is_private(p) && !store.is_stamped(p) {
                verify_published(page, &geometry)?;
            }
            for i in 0..page::nkeys(page) {
                let (child, child_txn) = page::branch_child(page, i)?;
                if child < 2 || child >= page_count || seen.contains(child) {
                    return Err(corrupt(format!("page {p} has a bad child {child}")));
                }
                seen.insert(child);
                next.push((child, child_txn));
            }
        }
        pages.extend(next.iter().map(|&(p, _)| p));
        level = next;
    }
    let Some(&(first, txn)) = level.first() else {
        return Err(corrupt(format!("a level of the tree of height {} is empty", root.height)));
    };
    if page::kind(check_header(first, txn)?)? != Kind::Leaf {
        return Err(corrupt(format!("height {} does not reach the leaves", root.height)));
    }
    Ok(pages)
}

/// Free every page of the tree `root` names: all collected first ([`tree_pages`]), so damage is
/// refused before anything is freed. A published page is freed once no reader or durable meta can
/// reach it, a private one is reusable at once. The handle is the caller's to drop or reset.
pub(crate) fn free_subtree<S: PageWrite + ?Sized>(store: &mut S, root: &TreeRoot) -> Result<()> {
    for p in tree_pages(&*store, root)? {
        store.free(p);
    }
    Ok(())
}

/// Empty the tree `root` names: free every page ([`free_subtree`]) and reset `root` to
/// [`TreeRoot::EMPTY`]. Published snapshots keep reading the old tree. On damage nothing is
/// freed and `root` is unchanged.
pub(crate) fn clear<S: PageWrite + ?Sized>(store: &mut S, root: &mut TreeRoot) -> Result<()> {
    free_subtree(store, root)?;
    *root = TreeRoot::EMPTY;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};

    use super::*;
    use crate::tree::{
        read::get,
        test_harness::{leaf_fill, walk_tree, MemStore, ReportSeed, Rng},
    };

    /// A random entry: a fresh key (mostly short, sometimes up to the key limit) or, a third of the
    /// time, one of `keys` again; a value mostly short, sometimes up to the limit for its key.
    fn random_entry(rng: &mut Rng, g: &Geometry, keys: &[Vec<u8>]) -> (Vec<u8>, Vec<u8>) {
        let key = if !keys.is_empty() && rng.below(3) == 0 {
            keys[rng.below(keys.len() as u64) as usize].clone()
        } else {
            let klen = match rng.below(10) {
                0 => 1 + rng.len_upto(g.max_key() - 1),
                1 | 2 => 1 + rng.len_upto(g.max_key() / 8),
                _ => 1 + rng.len_upto(24),
            };
            (0..klen).map(|_| rng.next() as u8).collect()
        };
        let max = g.max_value(key.len());
        let vlen = match rng.below(10) {
            0 => max - rng.len_upto(max.min(16)),
            1 | 2 => rng.len_upto(max / 4),
            _ => rng.len_upto(max.min(64)),
        };
        (key, (0..vlen).map(|_| rng.next() as u8).collect())
    }

    /// Random puts (inserts and replaces, sizes up to the limits) checked against a `BTreeMap`
    /// after every op: the walker passes and the entries and lookups match. Commits, reuse of
    /// freed pages and reopens (published pages verified before their first copy) come at random.
    /// Returns the final height.
    fn random_puts_match_a_model(page_size: u32, seed: u64, ops: usize) -> u32 {
        let _report = ReportSeed(seed);
        let mut rng = Rng(seed);
        let mut store = MemStore::new(page_size);
        let g = store.geometry();
        let mut root = TreeRoot::EMPTY;
        let mut model = BTreeMap::new();
        let mut keys = Vec::new();
        for op in 0..ops {
            let (k, v) = random_entry(&mut rng, &g, &keys);
            let replaced = put(&mut store, &mut root, &k, &v).expect("put");
            assert_eq!(replaced, model.insert(k.clone(), v.clone()).is_some(), "op {op}");
            if !replaced {
                keys.push(k.clone());
            }
            let walk = walk_tree(&store, &root);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "op {op}: entries");
            assert_eq!(get(&store, &root, &k).expect("get"), Some(v.as_slice()), "op {op}");
            if rng.below(25) == 0 {
                if rng.below(4) == 0 {
                    store.reopen();
                } else {
                    store.commit_like();
                }
                if rng.below(3) == 0 {
                    store.recycle();
                }
                store.assert_accounting(&[root]);
                // A reader of the commit sees every entry, through published pages only.
                let reader = store.reader();
                walk_tree(&reader, &root);
                for (k, v) in &model {
                    assert_eq!(get(&reader, &root, k).expect("get"), Some(v.as_slice()));
                }
                for _ in 0..20 {
                    let (k, _) = random_entry(&mut rng, &g, &[]);
                    let got = get(&reader, &root, &k).expect("get");
                    assert_eq!(got, model.get(&k).map(Vec::as_slice), "op {op}: absent key");
                }
            }
        }
        root.height
    }

    #[test]
    fn random_puts_match_a_btreemap_model() {
        assert!(
            random_puts_match_a_model(4096, 0x2545_F491_4F6C_DD1D, 1_000) >= 3,
            "deep at 4 KiB"
        );
        random_puts_match_a_model(4096, 0x9E37_79B9_7F4A_7C15, 300);
        assert!(random_puts_match_a_model(8192, 0x1234_5678_9ABC_DEF1, 300) >= 2);
        assert!(random_puts_match_a_model(65536, 0x0F0F_0F0F_1234_5679, 150) >= 2, "64 KiB splits");
    }

    #[test]
    #[ignore = "heavy: run with --release"]
    fn random_puts_match_a_btreemap_model_heavy() {
        for seed in 1..=8_u64 {
            let seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for page_size in [4096, 8192, 16384, 32768, 65536] {
                random_puts_match_a_model(page_size, seed, 3_000);
            }
        }
    }

    /// Insert and grow-replace wide entries (a few per page, so splits cascade several levels),
    /// failing each allocation of each put in turn: a failed put leaves the root handle, every
    /// reachable page (byte for byte) and the freed list exactly as they were.
    #[test]
    fn a_failed_allocation_leaves_the_tree_identical() {
        let mut store = MemStore::new(4096);
        let g = store.geometry();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut root = TreeRoot::EMPTY;
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut failures = 0;
        for n in 0..150_u64 {
            let key = match model.keys().nth(rng.below(model.len() as u64 + 1) as usize) {
                Some(k) if n % 4 == 3 => k.clone(),
                _ => {
                    let id = if n % 3 == 0 { n } else { rng.below(100_000) };
                    let mut k = vec![0xA5; g.max_key() - rng.len_upto(g.max_key() / 2)];
                    k[..8].copy_from_slice(&id.to_be_bytes());
                    k
                }
            };
            let value = vec![n as u8; rng.len_upto(g.max_value(key.len()))];
            for fail_at in 0.. {
                let before = (root, store.reachable(&root), store.alloc.freed().to_vec());
                let (pool, count) = (store.alloc.pool().len(), store.alloc.page_count());
                store.alloc.fail_allocs_after = Some(fail_at);
                let result = put(&mut store, &mut root, &key, &value);
                store.alloc.fail_allocs_after = None;
                match result {
                    Ok(_) => break,
                    Err(Error::Io(_)) => failures += 1,
                    Err(e) => panic!("put {n}, failing allocation {fail_at}: {e}"),
                }
                let case = format!("put {n}, failing allocation {fail_at}");
                assert_eq!(root, before.0, "{case}: the root handle changed");
                assert!(store.reachable(&root) == before.1, "{case}: a reachable page changed");
                assert_eq!(store.alloc.freed(), before.2, "{case}: pages were freed");
                let grown = (store.alloc.page_count() - count) as usize;
                assert_eq!(store.alloc.pool().len(), pool + grown, "{case}: pages taken were kept");
            }
            model.insert(key, value);
            let walk = walk_tree(&store, &root);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "put {n}");
            if n % 10 == 9 {
                store.commit_like();
                store.assert_accounting(&[root]);
            }
        }
        assert!(root.height >= 4, "the test reaches deep cascades (height {})", root.height);
        assert!(failures > 150, "{failures} failed allocations");
    }

    /// Ascending keys split in append mode, filling each leaf before starting the next; random
    /// keys leave leaves part full.
    #[test]
    fn ascending_inserts_fill_leaves() {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..20_000_u64 {
            put(&mut store, &mut root, &i.to_be_bytes(), &i.to_le_bytes()).expect("put");
        }
        let walk = walk_tree(&store, &root);
        let fill = leaf_fill(&store, &walk.leaves);
        assert!(fill > 0.95, "ascending fill {fill}");

        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for _ in 0..20_000 {
            let x = rng.next();
            put(&mut store, &mut root, &x.to_be_bytes(), &x.to_le_bytes()).expect("put");
        }
        let walk = walk_tree(&store, &root);
        let fill = leaf_fill(&store, &walk.leaves);
        assert!((0.55..0.9).contains(&fill), "random fill {fill}");
    }

    /// An ascending run inside the key space (not at its end) splits at the byte midpoint: append
    /// mode is only for the rightmost path, where nothing is inserted behind the run.
    #[test]
    fn only_the_rightmost_run_appends() {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..10_000_u64 {
            for run in [b'a', b'b'] {
                let mut k = vec![run];
                k.extend_from_slice(&i.to_be_bytes());
                put(&mut store, &mut root, &k, &i.to_le_bytes()).expect("put");
            }
        }
        let walk = walk_tree(&store, &root);
        let run_fill = |run: u8| {
            let leaves: Vec<u32> = walk
                .leaves
                .iter()
                .copied()
                .filter(|&p| page::leaf_key(store.raw(p), 0).expect("key")[0] == run)
                .collect();
            leaf_fill(&store, &leaves)
        };
        let (inner, rightmost) = (run_fill(b'a'), run_fill(b'b'));
        assert!((0.4..0.75).contains(&inner), "inner run fill {inner}");
        assert!(rightmost > 0.95, "rightmost run fill {rightmost}");
    }

    /// A leaf of `entries` (in order), private to the store's transaction.
    fn leaf_page(store: &mut MemStore, entries: &[&[u8]]) -> u32 {
        let (p, txn) = (store.alloc().expect("alloc"), store.txn());
        let page = store.page_mut(p).expect("page");
        page::init_leaf(page, p, txn);
        for (i, k) in entries.iter().enumerate() {
            assert_eq!(page::leaf_insert(page, i, k, b"").expect("insert"), Fit::Done);
        }
        p
    }

    /// A branch over `children` (`(lower-bound key, child)`, the first key empty).
    fn branch_page(store: &mut MemStore, children: &[(Vec<u8>, u32)]) -> u32 {
        let (p, txn) = (store.alloc().expect("alloc"), store.txn());
        let page = store.page_mut(p).expect("page");
        page::init_branch(page, p, txn);
        for (i, (k, child)) in children.iter().enumerate() {
            let fit = page::branch_insert(page, i, k, *child, txn as u32).expect("insert");
            assert_eq!(fit, Fit::Done, "branch cell {i}");
        }
        p
    }

    /// Fullness is by bytes, so whether a split climbs depends on the separator's length. Here a
    /// full leaf of short keys splits; its parent, packed with short separators (and one long key
    /// at its byte midpoint), cannot take even a short one and splits, sending the long key up;
    /// the root has room for a short separator but not that long one, so it splits too. The
    /// up-front reservation must bound each level's separator by the keys of the page below,
    /// not by the leaf's keys alone.
    #[test]
    fn a_long_separator_climbing_a_split_is_reserved_for() {
        let mut store = MemStore::new(4096);
        let long_key = |first: u8, second: u8, len: usize| {
            let mut k = vec![first, second];
            k.resize(len, 0xFF);
            k
        };
        // The parent: 217 two-byte separators and one 1000-byte key after the 145th, over
        // one-entry leaves; its last child is the full leaf (keys [1, 217, j]).
        let full = {
            let (p, txn) = (store.alloc().expect("alloc"), store.txn());
            let page = store.page_mut(p).expect("page");
            page::init_leaf(page, p, txn);
            let mut j = 0_u8;
            for vlen in [100, 0] {
                while page::leaf_insert(page, page::nkeys(page), &[1, 217, j], &vec![0; vlen])
                    .expect("insert")
                    == Fit::Done
                {
                    j += 1;
                }
            }
            p
        };
        let mut children = vec![(vec![], leaf_page(&mut store, &[&[1, 0]]))];
        for i in 1..=216_u8 {
            children.push((vec![1, i], leaf_page(&mut store, &[&[1, i]])));
            if i == 145 {
                let k = long_key(1, 145, 1000);
                children.push((k.clone(), leaf_page(&mut store, &[&k])));
            }
        }
        children.push((vec![1, 217], full));
        let parent = branch_page(&mut store, &children);
        assert!(page::free_space(store.raw(parent)).expect("free") < 14, "the parent is full");
        // The root: the parent, then three 1000-byte and one 101-byte separators.
        let mut root_children = vec![(vec![], parent)];
        for (first, len) in [(2, 1000), (3, 1000), (4, 1000), (5, 101)] {
            let k = long_key(first, 0, len);
            let leaf = leaf_page(&mut store, &[&k]);
            root_children.push((k, branch_page(&mut store, &[(vec![], leaf)])));
        }
        let top = branch_page(&mut store, &root_children);
        let free = page::free_space(store.raw(top)).expect("free");
        assert!((14..1012).contains(&free), "room for a short separator only ({free})");
        let entries = 218 + 4 + page::nkeys(store.raw(full)) as u64;
        let txn = store.txn() as u32;
        let mut root = TreeRoot { root: top, root_txn: txn, height: 3, entries };
        walk_tree(&store, &root);
        store.commit_like();

        let key = [1, 217, 0xFF, 0xFF];
        put(&mut store, &mut root, &key, b"v").expect("the reservation covers the cascade");
        assert_eq!(root.height, 4, "the root split");
        walk_tree(&store, &root);
        assert_eq!(get(&store, &root, &key).expect("get"), Some(&b"v"[..]));
    }

    /// A published tree of `n` entries (8-byte keys, 100-byte values) as a reopened writer sees it:
    /// nothing stamped by this writer.
    fn reopened_tree(n: u64) -> (MemStore, TreeRoot) {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..n {
            put(&mut store, &mut root, &(i * 7919 % n).to_be_bytes(), &[i as u8; 100])
                .expect("put");
        }
        store.reopen();
        (store, root)
    }

    /// Copy-on-write refuses a published page that fails its CRC (a flipped value bit) or, with a
    /// valid CRC, fails `validate` (a reserved header byte set): the copy would be stamped valid
    /// at the next commit. Nothing changes.
    #[test]
    fn copy_on_write_refuses_a_damaged_page() {
        for structural in [false, true] {
            let (mut store, mut root) = reopened_tree(2_000);
            let leaf = walk_tree(&store, &root).leaves[0];
            let p = store.geometry().page_size();
            if structural {
                store.raw_mut(leaf)[20] = 1;
                store.restamp(leaf);
            } else {
                store.raw_mut(leaf)[p - 5] ^= 0x01;
            }
            let before = (root, store.reachable(&root));
            let put = put(&mut store, &mut root, &0_u64.to_be_bytes(), b"new");
            assert!(matches!(put, Err(Error::Corrupt(_))), "structural {structural}: {put:?}");
            assert_eq!((root, store.reachable(&root)), before, "structural {structural}");
            assert!(store.alloc.freed().is_empty() && store.alloc.pool().is_empty());
        }
    }

    /// Copy-on-write verifies each published page from disk once, before its first copy, and
    /// neither the private copies nor pages this writer stamped itself.
    #[test]
    fn copy_on_write_verifies_only_pages_it_did_not_stamp() {
        let (mut store, mut root) = reopened_tree(2_000);
        assert!(root.height >= 2, "a path to copy");
        let verifies = || COW_VERIFIES.with(std::cell::Cell::get);
        COW_VERIFIES.with(|n| n.set(0));
        put(&mut store, &mut root, &0_u64.to_be_bytes(), b"one").expect("put");
        assert_eq!(verifies(), u64::from(root.height), "each page on the path, verified once");
        put(&mut store, &mut root, &1_u64.to_be_bytes(), b"two").expect("put");
        assert_eq!(verifies(), u64::from(root.height), "private copies are not verified");
        store.commit_like();
        put(&mut store, &mut root, &0_u64.to_be_bytes(), b"three").expect("put");
        assert_eq!(verifies(), u64::from(root.height), "pages it stamped are not re-verified");
    }

    /// Over-limit entries are refused before anything changes.
    #[test]
    fn over_limit_entries_are_refused() {
        let mut store = MemStore::new(4096);
        let g = store.geometry();
        let mut root = TreeRoot::EMPTY;
        let key = vec![1; g.max_key() + 1];
        assert!(matches!(put(&mut store, &mut root, &key, b""), Err(Error::KeyTooLarge { .. })));
        let value = vec![1; g.max_value(4) + 1];
        let put_value = put(&mut store, &mut root, b"four", &value);
        assert!(matches!(put_value, Err(Error::ValueTooLarge { .. })));
        assert_eq!(root, TreeRoot::EMPTY);
        assert_eq!(store.alloc.page_count(), 2, "nothing allocated");
    }

    /// A `klen`-byte key that sorts by `id`.
    fn id_key(id: u64, klen: usize) -> Vec<u8> {
        let mut k = vec![0x5A; klen.max(8)];
        k[..8].copy_from_slice(&id.to_be_bytes());
        k
    }

    /// A tree of keys `id_key(0..n, klen)` (inserted out of order, 100-byte values), with the
    /// model, committed and reopened (nothing stamped by the writer).
    fn wide_tree(n: u64, klen: usize) -> (MemStore, TreeRoot, BTreeMap<Vec<u8>, Vec<u8>>) {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        let mut model = BTreeMap::new();
        for i in 0..n {
            let (k, v) = (id_key(i * 7919 % n, klen), vec![i as u8; 100]);
            put(&mut store, &mut root, &k, &v).expect("put");
            model.insert(k, v);
        }
        store.reopen();
        (store, root, model)
    }

    /// The tree holds exactly the model.
    fn assert_holds(store: &MemStore, root: &TreeRoot, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
        let walk = walk_tree(store, root);
        assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "entries");
    }

    /// A delete that misses (below, between and above the keys, and a key over the limit) runs
    /// only the lookup: it verifies, copies, allocates and frees nothing, and leaves the root and
    /// every reachable page byte for byte as they were, published or private.
    #[test]
    fn a_delete_miss_copies_nothing() {
        let (mut store, mut root, _) = wide_tree(600, 200);
        assert!(root.height >= 3);
        let g = store.geometry();
        let misses = [
            vec![0],
            id_key(17, 201),
            id_key(300, 199),
            id_key(u64::MAX, 200),
            vec![0xFF; g.max_key() + 1],
        ];
        for private in [false, true] {
            if private {
                // Copy a path: the misses must leave private pages as they are too.
                assert!(delete(&mut store, &mut root, &id_key(17, 200)).expect("hit"));
            }
            let before = (root, store.reachable(&root), store.alloc.page_count());
            let freed = (store.alloc.freed().to_vec(), store.alloc.pool().to_vec());
            COW_VERIFIES.with(|n| n.set(0));
            for key in &misses {
                assert!(!delete(&mut store, &mut root, key).expect("miss"), "{private}");
            }
            assert_eq!(COW_VERIFIES.with(std::cell::Cell::get), 0, "a miss verifies nothing");
            assert_eq!(root, before.0, "{private}: the root handle changed");
            assert!(store.reachable(&root) == before.1, "{private}: a reachable page changed");
            assert_eq!(store.alloc.page_count(), before.2, "{private}: a page was allocated");
            assert_eq!(
                (store.alloc.freed().to_vec(), store.alloc.pool().to_vec()),
                freed,
                "{private}: pages were freed"
            );
        }
    }

    /// Deleting every key (in scrambled order) empties leaves, unlinks them, collapses the root
    /// level by level and ends at the empty tree, which holds no page: after the commit every page
    /// is retired or pooled, and the tree refills. A snapshot of the full tree keeps reading every
    /// entry throughout. Three rounds, so later ones delete from trees built over reused pages.
    #[test]
    fn deleting_every_key_collapses_to_the_empty_tree() {
        let n = 600_u64;
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for round in 0..3_u64 {
            let mut model = BTreeMap::new();
            for i in 0..n {
                let (k, v) = (id_key(i * 7 % n, 200), vec![(i + round) as u8; 100]);
                put(&mut store, &mut root, &k, &v).expect("put");
                model.insert(k, v);
            }
            store.commit_like();
            let (full, full_txn) = (root, store.alloc.txn() - 1);
            assert!(full.height >= 3, "round {round}: height {}", full.height);
            let tree: Vec<u32> = store.reachable(&full).into_iter().map(|(p, _)| p).collect();
            let mut heights = vec![root.height];
            for (j, i) in (0..n).map(|i| i * 11 % n).enumerate() {
                let k = id_key(i, 200);
                assert!(delete(&mut store, &mut root, &k).expect("delete"), "round {round}");
                model.remove(&k);
                if heights.last() != Some(&root.height) {
                    heights.push(root.height);
                }
                if j % 50 == 0 || model.len() < 30 {
                    assert_holds(&store, &root, &model);
                }
            }
            assert_eq!(root, TreeRoot::EMPTY, "round {round}");
            let expect: Vec<u32> = (0..=full.height).rev().collect();
            assert_eq!(heights, expect, "round {round}: the root collapses one level at a time");
            // Every page of the published tree was freed once, as it was (no copy).
            let mut freed = store.alloc.freed().to_vec();
            freed.sort_unstable();
            let mut want = tree.clone();
            want.sort_unstable();
            assert_eq!(freed, want, "round {round}: the published tree's pages are freed");
            store.commit_like();
            store.assert_accounting(&[root]);
            let reader = store.reader_at(full_txn);
            assert_eq!(walk_tree(&reader, &full).entries.len() as u64, n, "round {round}");
            store.recycle();
        }
    }

    /// Ascending keys with the oldest deleted (a sliding window), committing and reusing freed
    /// pages as it goes: emptied leaves are unlinked and their pages reused, so the file stays the
    /// size of the window, not of every key ever written.
    #[test]
    fn a_sliding_window_of_deletes_stays_bounded() {
        const WINDOW: u64 = 1_000;
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for i in 0..40_000_u64 {
            put(&mut store, &mut root, &i.to_be_bytes(), &i.to_le_bytes()).expect("put");
            if i >= WINDOW {
                assert!(delete(&mut store, &mut root, &(i - WINDOW).to_be_bytes()).expect("del"));
            }
            if i % 100 == 99 {
                store.commit_like();
                store.recycle();
            }
        }
        let walk = walk_tree(&store, &root);
        let expect: Vec<(Vec<u8>, Vec<u8>)> = (40_000 - WINDOW..40_000)
            .map(|i: u64| (i.to_be_bytes().to_vec(), i.to_le_bytes().to_vec()))
            .collect();
        assert_eq!(walk.entries, expect);
        let pages = store.alloc.page_count();
        assert!(
            pages < 25,
            "{pages} pages for a {WINDOW}-key window of {} pages",
            walk.pages.len()
        );
        store.commit_like();
        store.assert_accounting(&[root]);
    }

    /// The first child of a branch emptying takes its cell 0 with it: the next child becomes
    /// cell 0, its key dropped, so it now takes every key below its old lower bound (as far down
    /// as the parent's own range). Keys below every remaining one then go to it, and the tree
    /// stays sound.
    #[test]
    fn unlinking_a_first_child_widens_the_next_to_the_parents_range() {
        let (mut store, mut root, mut model) = wide_tree(600, 200);
        let leftmost = walk_tree(&store, &root).leaves[0];
        let first_keys: Vec<Vec<u8>> = (0..page::nkeys(store.raw(leftmost)))
            .map(|i| page::leaf_key(store.raw(leftmost), i).expect("key").to_vec())
            .collect();
        for k in &first_keys {
            assert!(delete(&mut store, &mut root, k).expect("delete"));
            model.remove(k);
        }
        let walk = walk_tree(&store, &root);
        assert!(!walk.pages.contains(&leftmost), "the emptied leaf left the tree");
        assert_holds(&store, &root, &model);
        // Below every key left: into the leaf that became the leftmost.
        let leaf_of = |store: &MemStore, root: &TreeRoot, k: &[u8]| {
            descend(store, root, k, |_, _, _, _| Ok(()), None).expect("descend").expect("leaf").pgno
        };
        let smallest = model.keys().next().expect("a key").clone();
        assert_eq!(leaf_of(&store, &root, &[0]), walk.leaves[0]);
        assert_eq!(leaf_of(&store, &root, &smallest), walk.leaves[0]);
        for k in [vec![0], id_key(0, 8), first_keys[0].clone()] {
            put(&mut store, &mut root, &k, b"low").expect("put");
            model.insert(k.clone(), b"low".to_vec());
            assert_eq!(get(&store, &root, &k).expect("get"), Some(&b"low"[..]));
        }
        assert_holds(&store, &root, &model);
    }

    /// Delete wide entries (four per leaf, so emptied leaves unlink and roots collapse often)
    /// from a deep published tree, failing each allocation of each delete in turn: a failed delete
    /// leaves the root handle, every reachable page (byte for byte) and the freed list exactly as
    /// they were, and gives back every page it took.
    #[test]
    fn a_failed_allocation_during_a_delete_leaves_the_tree_identical() {
        let mut store = MemStore::new(4096);
        let g = store.geometry();
        let mut root = TreeRoot::EMPTY;
        let mut model = BTreeMap::new();
        let mut rng = Rng(0x2545_F491_4F6C_DD1D);
        for i in 0..400_u64 {
            let k = id_key(i * 7919 % 400, 600 + rng.len_upto(g.max_key() - 600));
            let v = vec![i as u8; g.max_value(k.len())];
            put(&mut store, &mut root, &k, &v).expect("put");
            model.insert(k, v);
        }
        store.reopen();
        assert!(root.height >= 4, "height {}", root.height);
        let keys: Vec<Vec<u8>> = model.keys().cloned().collect();
        let (mut failures, mut unlinks, mut collapses) = (0, 0, 0);
        for (n, k) in (0..keys.len()).map(|i| &keys[i * 13 % keys.len()]).enumerate() {
            let pages = store.reachable(&root).len();
            let height = root.height;
            for fail_at in 0.. {
                let before = (root, store.reachable(&root), store.alloc.freed().to_vec());
                let (pool, count) = (store.alloc.pool().len(), store.alloc.page_count());
                store.alloc.fail_allocs_after = Some(fail_at);
                let result = delete(&mut store, &mut root, k);
                store.alloc.fail_allocs_after = None;
                match result {
                    Ok(true) => break,
                    Err(Error::Io(_)) => failures += 1,
                    other => panic!("delete {n}, failing allocation {fail_at}: {other:?}"),
                }
                let case = format!("delete {n}, failing allocation {fail_at}");
                assert_eq!(root, before.0, "{case}: the root handle changed");
                assert!(store.reachable(&root) == before.1, "{case}: a reachable page changed");
                assert_eq!(store.alloc.freed(), before.2, "{case}: pages were freed");
                let grown = (store.alloc.page_count() - count) as usize;
                assert_eq!(store.alloc.pool().len(), pool + grown, "{case}: pages taken were kept");
            }
            model.remove(k);
            unlinks += usize::from(store.reachable(&root).len() < pages);
            collapses += usize::from(root.height < height && !root.is_empty());
            if n % 8 == 7 {
                assert_holds(&store, &root, &model);
                if n % 32 == 31 {
                    store.reopen();
                } else {
                    store.commit_like();
                }
                store.assert_accounting(&[root]);
            }
        }
        assert_eq!(root, TreeRoot::EMPTY);
        assert!(failures > 300, "{failures} failed allocations");
        assert!(unlinks > 50 && collapses >= 3, "{unlinks} unlinks, {collapses} collapses");
    }

    /// A tree of `n` entries whose first version's pages are retired, intact (a pool of stray
    /// pages a damaged pointer can name), reopened.
    fn tree_with_strays(n: u64) -> (MemStore, TreeRoot) {
        let mut store = MemStore::new(4096);
        let mut root = TreeRoot::EMPTY;
        for round in 0..2_u64 {
            for i in 0..n {
                put(&mut store, &mut root, &id_key(i, 200), &[(i + round) as u8; 100])
                    .expect("put");
            }
            store.commit_like();
        }
        store.reopen();
        (store, root)
    }

    /// The free walk takes the old tree's pages from its child pointers, so each branch read from
    /// disk must pass its CRC first and every pointer must name a tree page not reached before:
    /// a pointer redirected to a stray page with a stale CRC, or (CRC-valid) to a meta page, past
    /// the page count or to a page the tree already reaches, refuses the clear before anything is
    /// freed, leaving the root handle as it was.
    #[test]
    fn the_free_walk_refuses_a_damaged_pointer_and_frees_nothing() {
        for damage in 0..4 {
            let (mut store, mut root) = tree_with_strays(600);
            assert!(root.height >= 3, "height {}", root.height);
            let walk = walk_tree(&store, &root);
            let tree: HashSet<u32> = walk.pages.iter().copied().collect();
            let stray = (2..store.alloc.page_count()).find(|p| !tree.contains(p)).expect("stray");
            // The last branch above the leaves, so the walk has read others before reaching it.
            let branch = *walk
                .pages
                .iter()
                .rev()
                .find(|&&p| page::kind(store.raw(p)).expect("kind") == Kind::Branch)
                .expect("a branch");
            let (target, restamp) = match damage {
                0 => (stray, false),
                1 => (1, true),
                2 => (store.alloc.page_count(), true),
                _ => (walk.leaves[0], true),
            };
            let target_txn = page::txn(store.raw(target.min(store.alloc.page_count() - 1))) as u32;
            let last = page::nkeys(store.raw(branch)) - 1;
            page::branch_set_child(store.raw_mut(branch), last, target, target_txn).expect("set");
            if restamp {
                store.restamp(branch);
            }
            let before = root;
            let cleared = clear(&mut store, &mut root);
            assert!(matches!(cleared, Err(Error::Corrupt(_))), "damage {damage}: {cleared:?}");
            assert_eq!(root, before, "damage {damage}: the root handle changed");
            assert!(store.alloc.freed().is_empty(), "damage {damage}: pages were freed");
            assert!(store.alloc.pool().is_empty(), "damage {damage}: pages were pooled");
        }
    }

    /// The free walk also refuses a CRC-valid pointer that names the right page but a stale txn
    /// (the page is not the copy its parent names), and a handle whose height is off by one
    /// either way (a leaf read as a branch, or the walk ending above the leaves).
    #[test]
    fn the_free_walk_refuses_a_stale_pointer_or_a_wrong_height() {
        for damage in 0..3 {
            let (mut store, root) = tree_with_strays(600);
            let mut handle = root;
            match damage {
                0 => {
                    let (child, txn) = page::branch_child(store.raw(root.root), 0).expect("child");
                    page::branch_set_child(store.raw_mut(root.root), 0, child, txn ^ 1)
                        .expect("set");
                    store.restamp(root.root);
                }
                1 => handle.height += 1,
                _ => handle.height -= 1,
            }
            let before = handle;
            let cleared = clear(&mut store, &mut handle);
            assert!(matches!(cleared, Err(Error::Corrupt(_))), "damage {damage}: {cleared:?}");
            assert_eq!(handle, before, "damage {damage}: the root handle changed");
            assert!(store.alloc.freed().is_empty(), "damage {damage}: pages were freed");
        }
    }

    /// A two-leaf tree of 200-byte keys `0..n` (appended, so the left leaf is full), its first
    /// version's pages retired intact, reopened; with the right leaf.
    fn two_leaf_tree(n: u64) -> (MemStore, TreeRoot, u32) {
        let (mut store, mut root) = (MemStore::new(4096), TreeRoot::EMPTY);
        for round in 0..2_u8 {
            for i in 0..n {
                put(&mut store, &mut root, &id_key(i, 200), &[round; 100]).expect("put");
            }
            store.commit_like();
        }
        store.reopen();
        let walk = walk_tree(&store, &root);
        assert_eq!((root.height, walk.leaves.len()), (2, 2), "two leaves under a root");
        (store, root, walk.leaves[1])
    }

    /// A collapse makes the root's remaining child the root without a descent reaching it, so it
    /// checks that page itself: a sibling damaged at rest (a flipped bit: its CRC fails), or a
    /// CRC-valid root pointer redirected to a stale copy of it (an older txn), is `Corrupt`, and
    /// the failed delete changes nothing.
    #[test]
    fn a_collapse_checks_the_page_it_promotes() {
        for damage in 0..2 {
            let (mut store, mut root, right) = two_leaf_tree(20);
            let left_keys: Vec<Vec<u8>> =
                (0..20).map(|i| id_key(i, 200)).filter(|k| k < &right_key(&store, right)).collect();
            let (last, rest) = left_keys.split_last().expect("left keys");
            for k in rest {
                assert!(delete(&mut store, &mut root, k).expect("delete"));
            }
            if damage == 0 {
                let p = store.geometry().page_size();
                store.raw_mut(right)[p - 5] ^= 1;
            } else {
                // The old version of the right leaf: retired, intact, CRC-valid.
                let stale = (2..store.alloc.page_count())
                    .find(|&p| {
                        p != right
                            && page::kind(store.raw(p)).ok() == Some(Kind::Leaf)
                            && page::leaf_key(store.raw(p), 0).ok()
                                == Some(right_key(&store, right).as_slice())
                    })
                    .expect("a stale copy");
                let txn = page::branch_child(store.raw(root.root), 1).expect("child").1;
                page::branch_set_child(store.page_mut(root.root).expect("private"), 1, stale, txn)
                    .expect("set");
            }
            let before = (root, store.reachable(&root), store.alloc.freed().to_vec());
            let pool = store.alloc.pool().to_vec();
            let deleted = delete(&mut store, &mut root, last);
            assert!(matches!(deleted, Err(Error::Corrupt(_))), "damage {damage}: {deleted:?}");
            assert_eq!(root, before.0, "damage {damage}: the root handle changed");
            assert!(store.reachable(&root) == before.1, "damage {damage}: a page changed");
            assert_eq!(store.alloc.freed(), before.2, "damage {damage}: pages were freed");
            assert_eq!(store.alloc.pool(), pool, "damage {damage}: pages were pooled");
        }
    }

    /// The first key of leaf `p`.
    fn right_key(store: &MemStore, p: u32) -> Vec<u8> {
        page::leaf_key(store.raw(p), 0).expect("key").to_vec()
    }

    /// A delete checks the handle's entry count against the tree before changing anything: a
    /// count of one for a tree with more entries, or of more than one for the tree's last entry,
    /// is `Corrupt` (the count is kept exact, never wrapped).
    #[test]
    fn a_delete_refuses_a_wrong_entry_count() {
        let (mut store, root, _) = two_leaf_tree(20);
        let mut handle = TreeRoot { entries: 1, ..root };
        let deleted = delete(&mut store, &mut handle, &id_key(3, 200));
        assert!(matches!(deleted, Err(Error::Corrupt(_))), "{deleted:?}");
        assert_eq!(handle, TreeRoot { entries: 1, ..root });

        let mut handle = TreeRoot::EMPTY;
        put(&mut store, &mut handle, b"only", b"one").expect("put");
        handle.entries = 2;
        let deleted = delete(&mut store, &mut handle, b"only");
        assert!(matches!(deleted, Err(Error::Corrupt(_))), "{deleted:?}");
        assert_eq!(handle.entries, 2);
        handle.entries = 1;
        assert!(delete(&mut store, &mut handle, b"only").expect("delete"));
        assert_eq!(handle, TreeRoot::EMPTY);
    }

    /// A clear frees exactly the tree's pages: published ones to the freed list, private ones
    /// straight back to the pool; the root becomes empty and the tree refills.
    #[test]
    fn clear_frees_every_page_of_the_tree() {
        let (mut store, mut root) = tree_with_strays(600);
        let mut tree: Vec<u32> = store.reachable(&root).into_iter().map(|(p, _)| p).collect();
        tree.sort_unstable();
        clear(&mut store, &mut root).expect("clear");
        assert_eq!(root, TreeRoot::EMPTY);
        let mut freed = store.alloc.freed().to_vec();
        freed.sort_unstable();
        assert_eq!(freed, tree, "every published page freed, once");
        assert!(store.alloc.pool().is_empty());

        // A tree built in this transaction: its pages are loose, reusable at once.
        let freed = freed.len();
        for i in 0..600 {
            put(&mut store, &mut root, &id_key(i, 200), b"again").expect("put");
        }
        let private = store.reachable(&root).len();
        assert!(private > 20, "{private} pages");
        let pool = store.alloc.pool().len();
        clear(&mut store, &mut root).expect("clear");
        assert_eq!(root, TreeRoot::EMPTY);
        assert_eq!(store.alloc.freed().len(), freed, "no published page among them");
        assert_eq!(store.alloc.pool().len(), pool + private, "the private pages are pooled");
        store.commit_like();
        store.assert_accounting(&[root]);
        assert!(clear(&mut store, &mut root).is_ok(), "clearing the empty tree");
    }

    /// One randomized run of [`random_batches`].
    #[derive(Clone, Copy, Debug)]
    struct BatchRun {
        page_size: u32,
        /// Random keys are drawn from ids `0..key_space`; ascending puts go above it, descending
        /// ones down from four times it.
        key_space: u64,
        rounds: usize,
        /// Rounds per phase: phases alternate between growing (mostly puts) and shrinking
        /// (mostly deletes), so the tree deepens, empties out and refills.
        phase: usize,
        seed: u64,
        /// The height the tree must reach.
        min_height: u32,
    }

    /// How often each operation and tree change ran: every one must have run.
    #[derive(Debug, Default)]
    struct Coverage {
        inserts: usize,
        replaces: usize,
        deletes: usize,
        missed_deletes: usize,
        clears: usize,
        splits: usize,
        unlinks: usize,
        collapses: usize,
        empties: usize,
        refills: usize,
        aborts: usize,
        held: usize,
        dropped_out_of_order: usize,
        reopens: usize,
    }

    impl Coverage {
        /// The operations and changes that never ran.
        fn missing(&self) -> Vec<&'static str> {
            let Self {
                inserts,
                replaces,
                deletes,
                missed_deletes,
                clears,
                splits,
                unlinks,
                collapses,
                empties,
                refills,
                aborts,
                held,
                dropped_out_of_order,
                reopens,
            } = *self;
            [
                ("inserts", inserts),
                ("replaces", replaces),
                ("deletes", deletes),
                ("missed_deletes", missed_deletes),
                ("clears", clears),
                ("splits", splits),
                ("unlinks", unlinks),
                ("collapses", collapses),
                ("empties", empties),
                ("refills", refills),
                ("aborts", aborts),
                ("held", held),
                ("dropped_out_of_order", dropped_out_of_order),
                ("reopens", reopens),
            ]
            .into_iter()
            .filter_map(|(what, n)| (n == 0).then_some(what))
            .collect()
        }
    }

    /// The key of `id`: its 8 big-endian bytes (so keys sort by id), padded to a length fixed by
    /// the id, mostly short, sometimes up to the key limit.
    fn batch_key(id: u64, g: &Geometry) -> Vec<u8> {
        let h = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 8;
        let len = match h % 10 {
            0 => g.max_key() - (h >> 8) as usize % (g.max_key() / 2),
            1 | 2 => 8 + (h >> 8) as usize % (g.max_key() / 8),
            _ => 8 + (h >> 8) as usize % 24,
        };
        let mut k = vec![(h >> 40) as u8; len];
        k[..8].copy_from_slice(&id.to_be_bytes());
        k
    }

    /// A value for `key`: mostly short, sometimes up to the limit for its key.
    fn batch_value(rng: &mut Rng, g: &Geometry, key: &[u8]) -> Vec<u8> {
        let max = g.max_value(key.len());
        let vlen = match rng.below(10) {
            0 => max - rng.len_upto(max.min(16)),
            1 | 2 => rng.len_upto(max / 4),
            _ => rng.len_upto(max.min(64)),
        };
        (0..vlen).map(|_| rng.next() as u8).collect()
    }

    /// Writer-shaped batches against a `BTreeMap` model. Each round is a batch of random,
    /// ascending and descending puts (inserts and replaces, sizes up to the limits), deletes (the
    /// smallest, largest or a random present key, or a likely miss) and clears; one in four
    /// batches has an allocation failure injected, and the batch then aborts (the private pages
    /// dropped, the published root restored), the others commit. After every operation the tree
    /// passes [`walk_tree`] and holds exactly the model, and a failed operation changed nothing.
    /// After every commit the page accounting is exact and a reader of the commit sees the model.
    /// Reader snapshots are held and dropped out of order; freed pages are reused once no held
    /// snapshot can reach them, and every held snapshot keeps reading the state it saw (a page
    /// reused too early is `Corrupt` to it). Reopens (stamped pages forgotten, readers gone) come
    /// at random and at the end, followed by writes. A coverage gate proves each tree change ran:
    /// splits, unlinks, collapses, empties and refills.
    fn random_batches(run: BatchRun) {
        let _seed = ReportSeed(run.seed);
        let BatchRun { page_size, key_space, rounds, phase, seed, min_height } = run;
        let mut rng = Rng(seed);
        let mut store = MemStore::new(page_size);
        let g = store.geometry();
        let mut root = TreeRoot::EMPTY;
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut published = (model.clone(), root);
        // Held reader snapshots: the committed txn, the root and the model at that commit.
        let mut held = Vec::<(u64, TreeRoot, BTreeMap<Vec<u8>, Vec<u8>>)>::new();
        let (mut ascending, mut descending) = (key_space, key_space * 4);
        let (mut max_height, mut emptied) = (0, false);
        let mut seen = Coverage::default();
        let mut pages = 0;
        for round in 0..rounds {
            let ops = 1 + rng.below(12);
            let inject = rng.below(4) == 0;
            if inject {
                store.alloc.fail_allocs_after = Some(rng.below(6));
            }
            let grow = (round / phase) % 2 == 0;
            let mut failed = false;
            for op in 0..ops {
                let r = rng.below(100);
                let (put_below, delete_below) = if grow { (85, 99) } else { (5, 100) };
                let height = root.height;
                let was_empty = root.is_empty();
                let what = format!("round {round} op {op}");
                let result = if r < put_below {
                    let id = match rng.below(10) {
                        0 | 1 => {
                            ascending += 1;
                            ascending
                        }
                        2 => {
                            descending = descending.saturating_sub(1);
                            descending
                        }
                        _ => rng.below(key_space),
                    };
                    let k = batch_key(id, &g);
                    let v = batch_value(&mut rng, &g, &k);
                    put(&mut store, &mut root, &k, &v).map(|replaced| {
                        assert_eq!(replaced, model.insert(k, v).is_some(), "{what}: put");
                        if replaced {
                            seen.replaces += 1;
                        } else {
                            seen.inserts += 1;
                        }
                        if was_empty && emptied {
                            seen.refills += 1;
                            emptied = false;
                        }
                    })
                } else if r < delete_below || rng.below(4) != 0 {
                    let k = match rng.below(8) {
                        0 => model.keys().next().cloned(),
                        1 => model.keys().next_back().cloned(),
                        2 => None,
                        _ if !model.is_empty() => {
                            model.keys().nth(rng.below(model.len() as u64) as usize).cloned()
                        }
                        _ => None,
                    }
                    .unwrap_or_else(|| batch_key(rng.below(key_space), &g));
                    delete(&mut store, &mut root, &k).map(|removed| {
                        assert_eq!(removed, model.remove(&k).is_some(), "{what}: delete");
                        if !removed {
                            seen.missed_deletes += 1;
                            return;
                        }
                        seen.deletes += 1;
                        if root.is_empty() {
                            seen.empties += 1;
                            emptied = true;
                        } else if root.height < height {
                            seen.collapses += 1;
                        }
                    })
                } else {
                    if !was_empty {
                        seen.clears += 1;
                        emptied = true;
                    }
                    clear(&mut store, &mut root).map(|()| model.clear())
                };
                let walk = walk_tree(&store, &root);
                assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "{what}");
                match result {
                    Ok(()) => {}
                    Err(Error::Io(_)) if inject => {
                        assert_eq!(walk.pages.len(), pages, "{what}: a failed op changed the tree");
                        failed = true;
                        break;
                    }
                    Err(e) => panic!("{what}: {e}"),
                }
                if walk.pages.len() > pages && r < put_below {
                    seen.splits += usize::from(!was_empty && root.height >= height);
                } else if walk.pages.len() < pages && !root.is_empty() && r >= put_below {
                    seen.unlinks += 1;
                }
                pages = walk.pages.len();
                max_height = max_height.max(root.height);
            }
            store.alloc.fail_allocs_after = None;
            if failed {
                seen.aborts += 1;
                store.abort_like();
                (model, root) = (published.0.clone(), published.1);
                pages = walk_tree(&store, &root).pages.len();
                store.assert_accounting(&[root]);
                continue;
            }
            store.commit_like();
            store.assert_accounting(&[root]);
            let reader = store.reader();
            let walk = walk_tree(&reader, &root);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "round {round}");
            published = (model.clone(), root);
            if rng.below(3) == 0 {
                seen.held += 1;
                held.push((store.alloc.txn() - 1, root, model.clone()));
            }
            if rng.below(4) == 0 && !held.is_empty() {
                let i = rng.below(held.len() as u64) as usize;
                if i + 1 != held.len() {
                    seen.dropped_out_of_order += 1;
                }
                held.remove(i);
            }
            if rng.below(40) == 0 {
                seen.reopens += 1;
                held.clear();
                store.reopen();
            }
            // Reuse what no held snapshot can reach: pages freed by txns up to the oldest one.
            store.recycle_upto(held.iter().map(|&(t, _, _)| t).min().unwrap_or(u64::MAX));
            for (t, r, m) in &held {
                let reader = store.reader_at(*t);
                for (k, v) in m.iter().take(4).chain(m.iter().rev().take(4)) {
                    let got = get(&reader, r, k).expect("held snapshot get");
                    assert_eq!(got, Some(v.as_slice()), "round {round}: held snapshot {t}");
                }
            }
            if !held.is_empty() {
                let (t, r, m) = &held[round % held.len()];
                let walk = walk_tree(&store.reader_at(*t), r);
                assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(m.iter()), "held {t}");
            }
        }
        eprintln!(
            "page size {page_size}: max height {max_height}, {} keys, {} pages, {seen:?}",
            model.len(),
            store.alloc.page_count()
        );
        assert!(max_height >= min_height, "the tree reached height {max_height}, not {min_height}");
        assert!(seen.missing().is_empty(), "never ran: {:?} ({seen:?})", seen.missing());
        assert!(seen.aborts > 10, "too few aborted batches: {seen:?}");
        for (t, r, m) in &held {
            let walk = walk_tree(&store.reader_at(*t), r);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(m.iter()), "held {t} at the end");
        }

        // A reopen serves the same tree; writes after it (verifying every published page before
        // its first copy, reusing the pages the reopen pooled) keep it sound.
        held.clear();
        store.reopen();
        store.recycle();
        for op in 0..64 {
            let k = batch_key(rng.below(key_space), &g);
            if rng.below(2) == 0 {
                let v = batch_value(&mut rng, &g, &k);
                put(&mut store, &mut root, &k, &v).expect("put after the reopen");
                model.insert(k, v);
            } else {
                let had = model.remove(&k).is_some();
                assert_eq!(
                    delete(&mut store, &mut root, &k).expect("delete after the reopen"),
                    had
                );
            }
            let walk = walk_tree(&store, &root);
            assert!(walk.entries.iter().map(|(k, v)| (k, v)).eq(model.iter()), "reopen op {op}");
        }
        store.commit_like();
        store.assert_accounting(&[root]);
    }

    #[test]
    fn random_batches_match_a_btreemap_model() {
        // Walks of 64 KiB pages (a CRC of each page) dominate: fewer, shorter rounds there.
        for (page_size, key_space, rounds, seed, min_height) in [
            (4096, 2_000, 400, 0x2545_F491_4F6C_DD1D, 3),
            (8192, 2_000, 400, 0x9E37_79B9_7F4A_7C15, 3),
            (65536, 2_000, 160, 0x1234_5678_9ABC_DEF1, 2),
        ] {
            random_batches(BatchRun {
                page_size,
                key_space,
                rounds,
                phase: rounds / 4,
                seed,
                min_height,
            });
        }
    }

    #[test]
    #[ignore = "heavy: run with --release"]
    fn random_batches_match_a_btreemap_model_heavy() {
        for seed in 1..=6_u64 {
            let seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            for (page_size, key_space) in
                [(4096, 2_000), (8192, 3_000), (16384, 4_000), (32768, 6_000), (65536, 8_000)]
            {
                random_batches(BatchRun {
                    page_size,
                    key_space,
                    rounds: 3_000,
                    phase: 60,
                    seed,
                    min_height: 2,
                });
            }
        }
    }
}
