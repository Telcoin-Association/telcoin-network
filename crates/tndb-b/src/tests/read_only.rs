//! The read-only open (`Env::open_read_only`): it reads every table of a file no writer holds,
//! values in overflow runs included, and never creates, writes or syncs anything; its shared lock
//! coexists with other read-only opens and excludes a writer; it refuses write transactions
//! without latching; it chooses the meta as `Env::open` does; and damage at rest fails its open
//! closed or is met by its reads and `Env::check` as `Corrupt`, which latches.

use std::{
    fs,
    os::unix::fs::{FileExt as _, PermissionsExt as _},
    time::SystemTime,
};

use super::{
    damage::{fixture, places, restamped, set_child, tree, write_page, Stage, P},
    *,
};
use crate::{disk_model, env::tests::flip, Error};

fn open_ro(path: &Path) -> Env {
    Env::open_read_only(path, EnvOptions::default()).expect("open read-only")
}

/// The file's bytes and modification time.
fn bytes_and_mtime(path: &Path) -> (Vec<u8>, SystemTime) {
    let mtime = fs::metadata(path).expect("metadata").modified().expect("mtime");
    (fs::read(path).expect("read"), mtime)
}

/// A cleanly closed file of several commits: table `a` (inline values, a few levels), `big`
/// (values inline and in runs of 1 to 18 pages, some replaced), `c` (created later), and a dropped
/// table's pages on the free list. Returns its newest state.
fn written(path: &Path) -> State {
    let env = open(path, SyncMode::Durable);
    let mut state = State::new();
    let mut txn = env.begin_rw().expect("begin");
    for i in 0..1500_u32 {
        put(&mut txn, &mut state, "a", &i.to_be_bytes(), &value(1, i, 60));
    }
    for (i, len) in [100, 3000, 5000, 9000, 20_000, 70_000].into_iter().enumerate() {
        put(&mut txn, &mut state, "big", &(i as u32).to_be_bytes(), &value(2, i as u32, len));
    }
    for i in 0..300_u32 {
        put(&mut txn, &mut state, "tmp", &i.to_be_bytes(), &value(3, i, 200));
    }
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    for i in (0..1500_u32).step_by(3) {
        delete(&mut txn, &mut state, "a", &i.to_be_bytes());
    }
    put(&mut txn, &mut state, "big", &1_u32.to_be_bytes(), &value(4, 1, 12_000));
    put(&mut txn, &mut state, "big", &4_u32.to_be_bytes(), b"inline now");
    drop_table(&mut txn, &mut state, "tmp");
    txn.commit().expect("commit");
    let mut txn = env.begin_rw().expect("begin");
    for i in 0..50_u32 {
        put(&mut txn, &mut state, "c", &i.to_be_bytes(), &value(5, i, 30));
    }
    txn.commit().expect("commit");
    assert_eq!(newest_state(&env), state);
    assert!(check(&env).free_pages > 0, "the dropped table's pages are free");
    let ro = env.begin_ro().expect("ro");
    let stat =
        |name| ro.table_stat(&ro.open_table(name).expect("open").expect(name)).expect("stat");
    assert!(stat("a").height >= 2 && stat("big").overflow_pages > 18, "{:?}", stat("big"));
    drop(ro);
    drop(env);
    state
}

/// A read-only open reads back every key of every table through gets and both cursor scans,
/// passes `Env::check` and reports the file's figures, and leaves the file byte-identical with
/// its modification time unchanged after the open, the reads, the check, a sync and the drop
/// (the file writable, so nothing but the open itself keeps it so). A mode-0444 file opens too.
#[test]
fn a_read_only_open_reads_every_table_and_changes_nothing() {
    let (_dir, path) = temp("tndb_b_ro");
    let state = written(&path);
    let (stat, max_key) = {
        let env = open(&path, SyncMode::Durable);
        (env.stat(), env.max_key_len())
    };
    let before = bytes_and_mtime(&path);

    let env = open_ro(&path);
    assert_eq!(env.stat(), stat);
    assert_eq!((env.max_key_len(), env.max_value_len(8)), (max_key, crate::overflow::MAX_VALUE));
    let ro = env.begin_ro().expect("ro");
    for (name, rows) in &state {
        let table = ro.open_table(name).expect("open table").expect("the table exists");
        for (k, v) in rows {
            assert_eq!(ro.get(&table, k).expect("get"), Some(&v[..]), "{name}");
        }
    }
    assert_eq!(ro.open_table("tmp").expect("open table"), None, "the dropped table");
    drop(ro);
    assert_eq!(newest_state(&env), state, "both scans and the check");
    env.sync().expect("a read-only sync does nothing");
    let clone = env.clone();
    drop(env);
    assert_eq!(state_of(&clone.begin_ro().expect("ro"), state.keys().map(String::as_str)), state);
    assert!(clone.failure().is_none());
    drop(clone);
    assert!(bytes_and_mtime(&path) == before, "the read-only open changed the file");

    fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).expect("chmod");
    let env = open_ro(&path);
    assert_eq!(newest_state(&env), state, "a mode-0444 file");
    drop(env);
    assert!(bytes_and_mtime(&path) == before);
}

/// A read-only open syncs nothing: a meta that reached the page cache but not the disk (its
/// commit's msync never finished) is the state it reads, and a power loss after it still loses
/// that meta, where an ordinary open would have made it durable.
#[test]
fn a_read_only_open_syncs_nothing() {
    let (_dir, path) = temp("tndb_b_ro_sync");
    let env = open(&path, SyncMode::Durable);
    let mut state = State::new();
    for v in [&b"1"[..], b"2"] {
        let mut txn = env.begin_rw().expect("begin");
        put(&mut txn, &mut state, "a", b"k", v);
        txn.commit().expect("commit"); // txn 1: slot 1; txn 2: slot 0
    }
    drop(env);
    disk_model::track(&path);
    // Rewrite slot 1 as txn 3 naming txn 2's state, in the page cache only.
    let file = fs::OpenOptions::new().read(true).write(true).open(&path).expect("open");
    let mut record = [0; META_LEN];
    file.read_exact_at(&mut record, 0).expect("read");
    let meta = Meta { txn: 3, ..Meta::decode(&record).expect("txn 2") };
    file.write_all_at(&meta.encode(), P).expect("write");
    drop(file);

    let env = open_ro(&path);
    assert_eq!(env.stat().txn_id, 3, "the page cache's newest meta");
    check(&env);
    drop(env);
    disk_model::power_loss(&path);
    disk_model::untrack(&path);
    assert_eq!(open_ro(&path).stat().txn_id, 2, "the read-only open synced nothing");
}

/// Read-only opens share the lock: any number coexist, and while one (or a transaction of it)
/// lives a writer's open is `Locked`; while a writer's open lives a read-only one is `Locked`.
#[test]
fn read_only_opens_share_the_lock_and_exclude_a_writer() {
    let (_dir, path) = temp("tndb_b_ro_lock");
    written(&path);
    let locked = |r: Result<Env, Error>| matches!(r, Err(Error::Locked));

    let writer = open(&path, SyncMode::Durable);
    assert!(locked(Env::open_read_only(&path, EnvOptions::default())), "a writer excludes it");
    drop(writer);

    let first = open_ro(&path);
    let second = open_ro(&path);
    assert!(locked(Env::open(&path, EnvOptions::default())), "two readers exclude a writer");
    drop(first);
    assert!(locked(Env::open(&path, EnvOptions::default())), "one reader excludes a writer");
    let ro = second.begin_ro().expect("ro");
    drop(second);
    assert!(locked(Env::open(&path, EnvOptions::default())), "a transaction holds the lock");
    drop(ro);
    let writer = open(&path, SyncMode::Durable);
    assert!(locked(Env::open_read_only(&path, EnvOptions::default())));
    drop(writer);
}

/// A read-only environment refuses write transactions with `ReadOnly`, which does not latch:
/// reads continue, and so do the refusals.
#[test]
fn a_read_only_env_refuses_write_transactions_without_latching() {
    let (_dir, path) = temp("tndb_b_ro_rw");
    let state = written(&path);
    let env = open_ro(&path);
    for _ in 0..2 {
        assert!(matches!(env.begin_rw(), Err(Error::ReadOnly)));
        assert!(matches!(env.try_begin_rw(), Err(Error::ReadOnly)));
    }
    assert!(env.failure().is_none(), "ReadOnly does not latch");
    assert_eq!(newest_state(&env), state, "reads continue");
}

/// A read-only open never creates: a missing file is `NotFound` and an empty one `NotTndbB`,
/// with nothing left behind (no file, no temporary file), as is a foreign file; an invalid page
/// size in the options is refused as `Env::open` refuses it.
#[test]
fn a_read_only_open_creates_nothing() {
    let (dir, path) = temp("tndb_b_ro_create");
    let listing = || {
        let mut names: Vec<_> = fs::read_dir(dir.path())
            .expect("list")
            .map(|e| e.expect("entry").file_name().into_string().expect("utf-8"))
            .collect();
        names.sort();
        names
    };
    let ro = |path: &Path| Env::open_read_only(path, EnvOptions::default());
    assert!(matches!(ro(&path), Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound));
    assert!(listing().is_empty(), "nothing created: {:?}", listing());

    fs::write(&path, b"").expect("create empty");
    assert!(matches!(ro(&path), Err(Error::NotTndbB)));
    assert_eq!(listing(), ["db"]);
    assert_eq!(fs::metadata(&path).expect("metadata").len(), 0, "still empty");

    let foreign = vec![0x5A; 3 * P as usize];
    fs::write(&path, &foreign).expect("write");
    assert!(matches!(ro(&path), Err(Error::NotTndbB)));
    assert!(fs::read(&path).expect("read") == foreign);

    let bad = EnvOptions { page_size: 1000, ..EnvOptions::default() };
    assert!(matches!(Env::open_read_only(&path, bad), Err(Error::InvalidPageSize(1000))));
    assert_eq!(listing(), ["db"]);
}

/// A damaged newest meta falls back exactly one txn, as in `Env::open`, and the read-only open
/// leaves the damaged slot as it is.
#[test]
fn a_read_only_open_falls_back_past_a_damaged_newest_meta() {
    let (_dir, path) = temp("tndb_b_ro_meta");
    let env = open(&path, SyncMode::Durable);
    let mut state = State::new();
    for v in [&b"1"[..], b"2"] {
        let mut txn = env.begin_rw().expect("begin");
        put(&mut txn, &mut state, "a", b"k", v);
        txn.commit().expect("commit"); // txn 1: slot 1; txn 2: slot 0
    }
    drop(env);
    flip(&path, 20);
    let before = fs::read(&path).expect("read");

    let env = open_ro(&path);
    assert_eq!(env.stat().txn_id, 1);
    let ro = env.begin_ro().expect("ro");
    let a = ro.open_table("a").expect("open table").expect("a");
    assert_eq!(ro.get(&a, b"k").expect("get"), Some(&b"1"[..]));
    drop(ro);
    check(&env);
    drop(env);
    assert!(fs::read(&path).expect("read") == before, "no meta is repaired");
}

/// Use a damaged read-only environment as a reader would (get every key, scan every table) and
/// then check it: the first error must be `Corrupt` and come from `stage` (never
/// `Stage::Write`: there is no writer), the operation that returned it must have latched the
/// environment, and write transactions are still refused as `ReadOnly`.
fn sweep_read_only(env: &Env, state: &State, what: &str, stage: Stage) {
    let mut first = None;
    let ro = env.begin_ro().expect("ro");
    for (name, rows) in state {
        let get = (|| {
            let Some(table) = ro.open_table(name)? else { return Ok(()) };
            for k in rows.keys() {
                ro.get(&table, k)?;
            }
            Ok(())
        })();
        if let Err(e) = get {
            first.get_or_insert((Stage::Get, e, env.failure().is_some()));
        }
    }
    for name in state.keys() {
        let scan = (|| {
            let Some(table) = ro.open_table(name)? else { return Ok(()) };
            let mut cursor = ro.cursor(&table)?;
            while cursor.next()?.is_some() {}
            Ok(())
        })();
        if let Err(e) = scan {
            first.get_or_insert((Stage::Scan, e, env.failure().is_some()));
        }
    }
    drop(ro);
    if env.failure().is_none() {
        if let Err(e) = env.check() {
            first.get_or_insert((Stage::Check, e, env.failure().is_some()));
        }
    }
    assert!(
        matches!(&first, Some((s, Error::Corrupt(_), true)) if *s == stage),
        "{what}: {first:?}, expected Corrupt from {stage:?}, latched by that operation"
    );
    assert!(matches!(env.begin_rw(), Err(Error::ReadOnly)), "{what}: still read-only");
}

/// Damage at rest to a leaf, a branch, the catalog or the free list, opened read-only, is met as
/// `Corrupt` by the first read that reaches it, else by `Env::check` (damage an ordinary
/// environment meets at its first write), and latches; damage to a tree root fails the open
/// closed. The file is byte-identical after each.
#[test]
fn damage_at_rest_is_met_by_read_only_reads_and_check() {
    let fx = fixture();
    let at = places(&fx.path);
    let catalog_root = {
        let env = open(&fx.path, SyncMode::Durable);
        let root = env.inner().current().meta.catalog.root;
        assert_ne!(root, tree(&env, "t").root);
        root
    };
    type Damage<'a> = (&'a str, Stage, Box<dyn Fn(&Path) + 'a>);
    let (redirect, target) = at.same_txn;
    let cases: Vec<Damage<'_>> = vec![
        // A cell byte under a stale CRC: reads do not recompute CRCs, the check does.
        ("leaf", Stage::Check, Box::new(|p| flip(p, u64::from(at.t_leaf) * P + P - 16))),
        ("root", Stage::Scan, Box::new(|p| flip(p, u64::from(at.t_root) * P + P - 16))),
        ("branch", Stage::Get, Box::new(|p| flip(p, u64::from(at.t_branch) * P + 26))),
        ("zeroed leaf", Stage::Get, Box::new(|p| write_page(p, at.t_leaf, &vec![0; P as usize]))),
        (
            "redirected",
            Stage::Get,
            Box::new(|p| restamped(p, at.t_branch, |b| set_child(b, redirect, target))),
        ),
        (
            "stale copy",
            Stage::Get,
            Box::new(|p| restamped(p, at.t_branch, |b| set_child(b, 0, fx.stale))),
        ),
        ("free leaf", Stage::Check, Box::new(|p| flip(p, u64::from(at.free_leaf) * P + P - 16))),
        (
            "catalog leaf",
            Stage::Check,
            Box::new(|p| flip(p, u64::from(at.catalog_leaf) * P + P - 16)),
        ),
        (
            "free record",
            Stage::Check,
            Box::new(|p| {
                restamped(p, at.free_leaf, |b| {
                    let offset = (at.free_value - u64::from(at.free_leaf) * P) as usize + 4;
                    b[offset..offset + 4].copy_from_slice(&(at.page_count + 5).to_le_bytes());
                })
            }),
        ),
    ];
    for (what, stage, damage) in &cases {
        let path = fx.dir.path().join("case");
        fs::copy(&fx.path, &path).expect("copy");
        damage(&path);
        let before = fs::read(&path).expect("read");
        let env = open_ro(&path);
        sweep_read_only(&env, &fx.state, what, *stage);
        drop(env);
        assert!(fs::read(&path).expect("read") == before, "{what}: the file changed");
        fs::remove_file(&path).expect("remove");
    }

    // A damaged tree root fails the read-only open closed, as it fails `Env::open`.
    for (what, root) in [("catalog root", catalog_root), ("free tree root", at.free_root)] {
        let path = fx.dir.path().join("case");
        fs::copy(&fx.path, &path).expect("copy");
        flip(&path, u64::from(root) * P + P - 16);
        let before = fs::read(&path).expect("read");
        match Env::open_read_only(&path, EnvOptions::default()) {
            Err(Error::Corrupt(m)) => assert!(m.contains(what), "{what}: {m}"),
            r => panic!("{what}: {r:?}"),
        }
        assert!(fs::read(&path).expect("read") == before, "{what}: the file changed");
        fs::remove_file(&path).expect("remove");
    }
}
