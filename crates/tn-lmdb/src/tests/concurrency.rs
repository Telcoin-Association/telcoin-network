//! Concurrent readers under a churning writer: reader threads rescan a pinned snapshot (its
//! digest never changes until they drop it) and take fresh snapshots (each internally consistent:
//! it holds the digest the writer stored in the same commit), while the writer replaces, deletes
//! and clears so pages are freed and reused. No read sees damage or a torn state. Readers also
//! hold slices and cursors while growth past a small mapping reservation moves the mapping, and
//! owned cursors on reader threads scan values in overflow runs of every size while the mapping
//! moves under them and `Env::check` runs beside the writer.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use super::*;
use crate::tree::test_harness::Rng;

/// FNV-1a over every key and value of `rows`, in key order, with the row count.
fn digest<'r>(rows: impl IntoIterator<Item = (&'r [u8], &'r [u8])>) -> (u64, u64) {
    let (mut count, mut hash) = (0, 0xCBF2_9CE4_8422_2325_u64);
    for (k, v) in rows {
        count += 1;
        for &b in (k.len() as u32).to_le_bytes().iter().chain(k).chain(v) {
            hash = (hash ^ u64::from(b)).wrapping_mul(0x0100_0000_01B3);
        }
    }
    (count, hash)
}

/// The digest of table `t` as `txn` sees it, scanned by a cursor; checked against the digest the
/// writer stored with it in table `sum`.
fn scan(txn: &impl ReadTxn) -> (u64, u64) {
    let t = txn.open_table("t").expect("open").expect("t");
    let mut cursor = txn.cursor(&t).expect("cursor");
    let mut rows = Vec::new();
    while let Some((k, v)) = cursor.next().expect("next") {
        rows.push((k, v));
    }
    let got = digest(rows);
    assert_eq!(got, stored_digest(txn), "snapshot {}: a torn state", txn.txn_id());
    got
}

/// The digest the writer stored in table `sum` with the state `txn` sees.
fn stored_digest(txn: &impl ReadTxn) -> (u64, u64) {
    let sum = txn.open_table("sum").expect("open").expect("sum");
    let stored = txn.get(&sum, b"digest").expect("get").expect("the stored digest");
    let count = u64::from_le_bytes(stored[..8].try_into().expect("8 bytes"));
    let hash = u64::from_le_bytes(stored[8..].try_into().expect("8 bytes"));
    (count, hash)
}

/// Store the digest of `model` in table `sum`, beside the state it describes.
fn store_digest(txn: &mut RwTxn, model: &Rows) {
    let (count, hash) = digest(model.iter().map(|(k, v)| (&k[..], &v[..])));
    let sum = txn.create_table("sum").expect("sum");
    let stored: Vec<u8> = count.to_le_bytes().into_iter().chain(hash.to_le_bytes()).collect();
    txn.put(&sum, b"digest", &stored).expect("put");
}

/// Sets the flag when dropped: the readers stop even if the writer panics.
struct StopOnDrop<'a>(&'a AtomicBool);

impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// `readers` threads read while the writer makes `commits` commits in `sync` mode. Reader 0 holds
/// each pinned snapshot across 40 commits (so the writer's queue of snapshots grows past its
/// pruning threshold behind it); the others swap theirs for a fresh one at random. The writer
/// goes on past `commits` (up to four times as many) until reader 0 has held two snapshots so,
/// and the writer has reused pages while one of them was pinned: the reuse gate is then tested
/// against a reader it must hold back, however late the readers started.
fn readers_under_churn(readers: usize, commits: u32, sync: SyncMode, seed: u64) {
    let (_dir, path) = temp("tn_lmdb_concurrency");
    let env = open(&path, sync);
    env.with_writer(|w| w.alloc.log_pages = true);
    let mut model = Rows::new();
    let stop = AtomicBool::new(false);
    let (rescans, fresh, committed) = (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
    // The pages reused so far (the allocator's load log, stored before each commit's count is
    // released), reader 0's swaps after holding a snapshot across 40 commits, and those of its
    // pins under which pages were reused.
    let (loaded, held, reused_under_pin) =
        (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
    let write = |rng: &mut Rng, model: &mut Rows, round: u32| {
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table("t").expect("t");
        for _ in 0..1 + rng.below(60) {
            let k = (rng.below(500) as u32).to_be_bytes().to_vec();
            if rng.below(4) == 0 {
                assert_eq!(txn.delete(&t, &k).expect("delete"), model.remove(&k).is_some());
            } else {
                let v = value(round, rng.below(1 << 20) as u32, rng.len_upto(300));
                txn.put(&t, &k, &v).expect("put");
                model.insert(k, v);
            }
        }
        // A second table filled and cleared in turn frees many pages at once.
        let u = txn.create_table("u").expect("u");
        if round % 8 == 7 {
            txn.clear_table(&u).expect("clear");
        } else {
            for i in 0..40_u32 {
                txn.put(&u, &(round * 64 + i).to_be_bytes(), &value(round, i, 200)).expect("put");
            }
        }
        store_digest(&mut txn, model);
        txn.commit().expect("commit");
        loaded.store(env.with_writer(|w| w.alloc.load_log.len()) as u64, Ordering::Relaxed);
        committed.fetch_add(1, Ordering::Release);
        if round % 10 == 9 {
            // In `SyncMode::NoSync` freed pages are reusable only once syncs move the metas.
            env.sync().expect("sync");
        }
    };
    let mut rng = Rng(seed);
    write(&mut rng, &mut model, 0);
    thread::scope(|s| {
        for r in 0..readers {
            let (env, stop, rescans, fresh, committed) =
                (&env, &stop, &rescans, &fresh, &committed);
            let (loaded, held, reused_under_pin) = (&loaded, &held, &reused_under_pin);
            s.spawn(move || {
                let mut rng = Rng(seed ^ ((r as u64 + 1) * 0x9E37_79B9));
                // Reader 0's count of reused pages once two commits followed its pin: a later
                // reuse took place under the pin.
                let mut pinned_loaded = None;
                let mut pinned = env.begin_ro().expect("ro");
                let mut pinned_at = committed.load(Ordering::Relaxed);
                let mut want = scan(&pinned);
                while !stop.load(Ordering::Relaxed) {
                    assert_eq!(scan(&pinned), want, "the pinned snapshot changed");
                    rescans.fetch_add(1, Ordering::Relaxed);
                    let ro = env.begin_ro().expect("ro");
                    assert!(ro.txn_id() >= pinned.txn_id());
                    let got = scan(&ro);
                    fresh.fetch_add(1, Ordering::Relaxed);
                    let now = committed.load(Ordering::Acquire);
                    if r == 0 && pinned_loaded.is_none() && now >= pinned_at + 2 {
                        pinned_loaded = Some(loaded.load(Ordering::Relaxed));
                    }
                    let spanned = now >= pinned_at + 40;
                    if r == 0 && spanned {
                        // Held across 40 commits, and any reuse counted here was under it: one
                        // more rescan before it goes.
                        let n = pinned_loaded.take();
                        let reused = n.is_some_and(|n| loaded.load(Ordering::Relaxed) > n);
                        assert_eq!(scan(&pinned), want, "the pinned snapshot changed");
                        held.fetch_add(1, Ordering::Relaxed);
                        reused_under_pin.fetch_add(u64::from(reused), Ordering::Relaxed);
                    }
                    let swap = if r == 0 { spanned } else { rng.below(4) == 0 };
                    if swap {
                        (pinned, want, pinned_at) = (ro, got, now);
                    }
                }
                assert_eq!(scan(&pinned), want, "the pinned snapshot changed");
            });
        }
        let _stop = StopOnDrop(&stop);
        let shown =
            || held.load(Ordering::Relaxed) >= 2 && reused_under_pin.load(Ordering::Relaxed) >= 1;
        let mut round = 1;
        while round < commits || (!shown() && round < 4 * commits) {
            write(&mut rng, &mut model, round);
            round += 1;
        }
    });
    assert_eq!(env.failure(), None);
    check(&env);
    let reused = env.with_writer(|w| w.alloc.load_log.len());
    assert!(reused > 0, "pages were reused");
    assert!(held.load(Ordering::Relaxed) >= 2, "reader 0 held two snapshots across 40 commits");
    assert!(reused_under_pin.load(Ordering::Relaxed) >= 1, "pages were reused under a pin");
    assert!(rescans.load(Ordering::Relaxed) > readers as u64, "the readers rescanned");
    assert!(fresh.load(Ordering::Relaxed) > readers as u64, "the readers took fresh snapshots");
    let ro = env.begin_ro().expect("ro");
    assert_eq!(scan(&ro), digest(model.iter().map(|(k, v)| (&k[..], &v[..]))));
}

#[test]
fn readers_keep_their_snapshots_under_a_churning_writer() {
    readers_under_churn(4, 150, SyncMode::Durable, 0xC0C0_0001);
}

#[test]
#[ignore = "heavy: run with --release"]
fn readers_keep_their_snapshots_under_a_churning_writer_heavy() {
    for (i, sync) in [SyncMode::Durable, SyncMode::NoSync].into_iter().enumerate() {
        readers_under_churn(8, 4000, sync, 0xC0C0_1000 + i as u64);
    }
}

/// The base address of `env`'s current mapping, read through its first page past the metas (no
/// reader slices a meta page): it changes exactly when growth past the reservation remaps the file.
fn mapping_base(env: &Env) -> usize {
    let snapshot = env.inner().current();
    let page = u64::from(snapshot.meta.page_size);
    snapshot.view.slice(2 * page, 1).expect("page 2").as_ptr() as usize - 2 * page as usize
}

/// One level of `held_views_survive_remaps_past_the_reservation`: begin a reader, hold `get`
/// slices of its rows and a cursor stopped half way with the rows it returned, commit until the
/// mapping moves, recurse for the next `levels - 1` levels (more remaps), then check that every
/// held slice still reads its row, that the cursor finishes the scan of the reader's snapshot, and
/// that the snapshot is unchanged. Returns the number of remaps from this level down.
fn hold_across_remaps(env: &Env, model: &mut Rows, round: &mut u32, levels: u32) -> u32 {
    if levels == 0 {
        return 0;
    }
    let ro = env.begin_ro().expect("ro");
    let want = model.clone();
    let t = ro.open_table("t").expect("open").expect("t");
    let held: Vec<(&[u8], &[u8])> = want
        .iter()
        .step_by(5)
        .map(|(k, v)| (ro.get(&t, k).expect("get").expect("row"), &v[..]))
        .collect();
    let mut cursor = ro.cursor(&t).expect("cursor");
    let mut scanned = Vec::new();
    for _ in 0..want.len() / 2 {
        scanned.push(cursor.next().expect("next").expect("a row"));
    }
    let before = mapping_base(env);
    while mapping_base(env) == before {
        assert!(*round < 200, "the mapping never moved");
        // New rows grow the file; rewriting old ones frees pages for reuse.
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table("t").expect("t");
        for i in 0..120_u32 {
            let k =
                if i % 4 == 0 { (*round * 7 + i) % (*round * 120 + 1) } else { *round * 120 + i };
            let v = value(*round, i, 900);
            txn.put(&t, &k.to_be_bytes(), &v).expect("put");
            model.insert(k.to_be_bytes().to_vec(), v);
        }
        txn.commit().expect("commit");
        *round += 1;
    }
    let remaps = 1 + hold_across_remaps(env, model, round, levels - 1);
    for (got, v) in &held {
        assert_eq!(got, v, "a held slice changed");
    }
    while let Some(row) = cursor.next().expect("next") {
        scanned.push(row);
    }
    let want_rows: Vec<(&[u8], &[u8])> = want.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    assert!(scanned == want_rows, "the held cursor finished a different scan");
    assert!(rows(&ro, &t) == want, "the snapshot changed");
    remaps
}

/// Growth past a small mapping reservation (1 MiB) moves the mapping while readers hold `get`
/// slices and mid-scan cursors from before each move: replaced mappings are retired, not
/// unmapped, while a reader can reach them, so every held slice keeps its bytes and every
/// snapshot reads as it did.
#[test]
fn held_views_survive_remaps_past_the_reservation() {
    let (_dir, path) = temp("tn_lmdb_remap");
    let options = EnvOptions { map_reserve: 1 << 20, ..options(SyncMode::Durable) };
    let env = Env::open(&path, options).expect("open");
    let mut model = Rows::new();
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in 0..40_u32 {
        let v = value(0, i, 900);
        txn.put(&t, &i.to_be_bytes(), &v).expect("put");
        model.insert(i.to_be_bytes().to_vec(), v);
    }
    txn.commit().expect("commit");
    let mut round = 1;
    assert_eq!(hold_across_remaps(&env, &mut model, &mut round, 3), 3);
    assert_eq!(env.failure(), None);
    check(&env);
    let ro = env.begin_ro().expect("ro");
    assert!(rows(&ro, &ro.open_table("t").expect("open").expect("t")) == model);
}

/// The digest of table `t` in `ro`'s snapshot, scanned by an owned cursor (`RoCursor`, whose steps
/// re-fetch their leaf through `Snapshot::page_again`), with `pause` run half way.
fn owned_scan(ro: &Arc<crate::RoTxn>, pause: impl FnOnce()) -> (u64, u64) {
    let t = ro.open_table("t").expect("open").expect("t");
    let mut cursor = ro.cursor_owned(&t).expect("cursor");
    let mut rows = Vec::new();
    for _ in 0..ro.table_stat(&t).expect("stat").entries / 2 {
        let (k, v) = cursor.next().expect("next").expect("a row");
        rows.push((k.to_vec(), v.to_vec()));
    }
    pause();
    while let Some((k, v)) = cursor.next().expect("next") {
        rows.push((k.to_vec(), v.to_vec()));
    }
    digest(rows.iter().map(|(k, v)| (&k[..], &v[..])))
}

/// Four reader threads scan with owned cursors while the writer makes `commits` commits in `sync`
/// mode of values inline, in one-page runs and in runs of several pages, replaced and deleted so
/// runs are freed and reused, in a file that grows past a small mapping reservation (1 MiB): the
/// mapping moves while the readers read. Each scan takes a fresh snapshot and must match the
/// digest stored with it; reader 0 holds its half-done scan across commits, and reader 3 runs
/// `Env::check` after each scan, beside the writer.
fn owned_readers_under_runs_and_remaps(commits: u32, sync: SyncMode, seed: u64) {
    let (_dir, path) = temp("tn_lmdb_owned_readers");
    let options = EnvOptions { map_reserve: 1 << 20, ..options(sync) };
    let env = Env::open(&path, options).expect("open");
    let mut model = Rows::new();
    let stop = AtomicBool::new(false);
    let (committed, scans, held, checks) =
        (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
    let write = |rng: &mut Rng, model: &mut Rows, round: u32| {
        let mut txn = env.begin_rw().expect("begin");
        let t = txn.create_table("t").expect("t");
        for _ in 0..1 + rng.below(30) {
            let k = (rng.below(400) as u32).to_be_bytes().to_vec();
            if rng.below(3) == 0 {
                assert_eq!(txn.delete(&t, &k).expect("delete"), model.remove(&k).is_some());
            } else {
                let len = match rng.below(4) {
                    0 => rng.len_upto(200),
                    1 => 2100 + rng.len_upto(1500), // a one-page run at 4 KiB pages
                    2 => 5000 + rng.len_upto(20_000), // a run of several pages
                    _ => rng.len_upto(1500),
                };
                let v = value(round, rng.below(1 << 20) as u32, len);
                txn.put(&t, &k, &v).expect("put");
                model.insert(k, v);
            }
        }
        store_digest(&mut txn, model);
        txn.commit().expect("commit");
        committed.fetch_add(1, Ordering::Release);
        if round % 5 == 4 {
            // In `SyncMode::NoSync` freed pages are reusable only once syncs move the metas.
            env.sync().expect("sync");
        }
    };
    let mut rng = Rng(seed);
    write(&mut rng, &mut model, 0);
    let before = mapping_base(&env);
    thread::scope(|s| {
        for r in 0..4 {
            let (env, stop, committed) = (&env, &stop, &committed);
            let (scans, held, checks) = (&scans, &held, &checks);
            s.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let ro = Arc::new(env.begin_ro().expect("ro"));
                    let want = stored_digest(&*ro);
                    let at = committed.load(Ordering::Acquire);
                    let got = owned_scan(&ro, || {
                        if r != 0 {
                            return;
                        }
                        let until = Instant::now() + Duration::from_millis(300);
                        while committed.load(Ordering::Acquire) < at + 3
                            && Instant::now() < until
                            && !stop.load(Ordering::Relaxed)
                        {
                            thread::yield_now();
                        }
                        if committed.load(Ordering::Acquire) >= at + 3 {
                            held.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                    assert_eq!(got, want, "reader {r}: snapshot {} torn", ro.txn_id());
                    scans.fetch_add(1, Ordering::Relaxed);
                    if r == 3 {
                        env.check().expect("check beside the writer");
                        checks.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        let _stop = StopOnDrop(&stop);
        for round in 1..commits {
            write(&mut rng, &mut model, round);
        }
    });
    assert_eq!(env.failure(), None);
    assert_ne!(mapping_base(&env), before, "the mapping moved under the readers");
    assert!(env.io_stats().pages_from_pool > 0, "pages were reused");
    assert!(scans.load(Ordering::Relaxed) > 4, "the readers scanned");
    assert!(held.load(Ordering::Relaxed) >= 1, "a scan was held across commits");
    assert!(checks.load(Ordering::Relaxed) >= 1, "a check ran beside the writer");
    check(&env);
    let ro = env.begin_ro().expect("ro");
    assert_eq!(scan(&ro), digest(model.iter().map(|(k, v)| (&k[..], &v[..]))));
}

#[test]
fn owned_readers_keep_their_snapshots_under_runs_and_remaps() {
    owned_readers_under_runs_and_remaps(160, SyncMode::Durable, 0x52_0001);
}

#[test]
#[ignore = "heavy: run with --release"]
fn owned_readers_keep_their_snapshots_under_runs_and_remaps_heavy() {
    for (i, sync) in [SyncMode::Durable, SyncMode::NoSync].into_iter().enumerate() {
        owned_readers_under_runs_and_remaps(2000, sync, 0x52_1000 + i as u64);
    }
}
