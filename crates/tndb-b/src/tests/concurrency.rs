//! Concurrent readers under a churning writer: reader threads rescan a pinned snapshot (its
//! digest never changes until they drop it) and take fresh snapshots (each internally consistent:
//! it holds the digest the writer stored in the same commit), while the writer replaces, deletes
//! and clears so pages are freed and reused. No read sees damage or a torn state. Readers also
//! hold slices and cursors while growth past a small mapping reservation moves the mapping.

use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    thread,
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
    let sum = txn.open_table("sum").expect("open").expect("sum");
    let stored = txn.get(&sum, b"digest").expect("get").expect("the stored digest");
    let count = u64::from_le_bytes(stored[..8].try_into().expect("8 bytes"));
    let hash = u64::from_le_bytes(stored[8..].try_into().expect("8 bytes"));
    assert_eq!(got, (count, hash), "snapshot {}: a torn state", txn.txn_id());
    got
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
/// pruning threshold behind it); the others swap theirs for a fresh one at random.
fn readers_under_churn(readers: usize, commits: u32, sync: SyncMode, seed: u64) {
    let (_dir, path) = temp("tndb_b_concurrency");
    let env = open(&path, sync);
    let mut model = Rows::new();
    let stop = AtomicBool::new(false);
    let (rescans, fresh, committed) = (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
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
        let (count, hash) = digest(model.iter().map(|(k, v)| (&k[..], &v[..])));
        let sum = txn.create_table("sum").expect("sum");
        let stored: Vec<u8> = count.to_le_bytes().into_iter().chain(hash.to_le_bytes()).collect();
        txn.put(&sum, b"digest", &stored).expect("put");
        txn.commit().expect("commit");
        committed.fetch_add(1, Ordering::Relaxed);
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
            s.spawn(move || {
                let mut rng = Rng(seed ^ ((r as u64 + 1) * 0x9E37_79B9));
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
                    let now = committed.load(Ordering::Relaxed);
                    let swap = if r == 0 { now >= pinned_at + 40 } else { rng.below(4) == 0 };
                    if swap {
                        (pinned, want, pinned_at) = (ro, got, now);
                    }
                }
                assert_eq!(scan(&pinned), want, "the pinned snapshot changed");
            });
        }
        let _stop = StopOnDrop(&stop);
        for round in 1..commits {
            write(&mut rng, &mut model, round);
        }
    });
    assert_eq!(env.failure(), None);
    check(&env);
    let reused = env.with_writer(|w| w.alloc.load_log.len());
    assert!(reused > 0, "pages were reused");
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

/// The base address of `env`'s current mapping: it changes exactly when growth past the
/// reservation remaps the file.
fn mapping_base(env: &Env) -> usize {
    env.inner().current().view.slice(0, 1).expect("page 0").as_ptr() as usize
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
    let (_dir, path) = temp("tndb_b_remap");
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
