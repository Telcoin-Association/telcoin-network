//! Step 4's gap fill (`EnvOptions::gap_fill`): the option's bound and its parse, which gaps a
//! fill rewrites, when it stops trying, that it changes no byte of the file, at a fixed width and
//! at auto, and what auto publishes. The crash matrix runs with it on too (`tests/crash.rs`,
//! `GAP_FILL`). The automatic width's chooser on modelled disks: `gap_auto_tests.rs`.

use std::fs;

use tempfile::TempDir;

use super::*;
use crate::{file::refuses_nowait_reads, ReadTxn as _};

fn temp() -> (TempDir, PathBuf) {
    let dir = TempDir::with_prefix("tn_lmdb_gap").expect("temp dir");
    let path = dir.path().join("db");
    (dir, path)
}

/// Rows of the workload's table.
const ROWS: u32 = 20_000;

/// The workload's keys: spread over the table, so each round's updates copy leaves all over it.
fn key(i: u32) -> [u8; 4] {
    i.wrapping_mul(2_654_435_761).to_be_bytes()
}

/// Round `round`'s value of row `i`.
fn value(round: u32, i: u32) -> [u8; 200] {
    [(round ^ i) as u8; 200]
}

/// Fill table `t` with [`ROWS`] rows in one commit.
fn load(env: &Env) {
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.create_table("t").expect("t");
    for i in 0..ROWS {
        txn.put(&t, &key(i), &value(0, i)).expect("put");
    }
    txn.commit().expect("commit");
}

/// One commit of 1500 scattered updates: its copies take pages freed all over the file, so its
/// pages form many short runs with short gaps between them.
fn scatter(env: &Env, round: u32) {
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.open_table("t").expect("open").expect("t exists");
    for j in 0..1500u32 {
        let i = j.wrapping_mul(7919).wrapping_add(round * 104_729) % ROWS;
        txn.put(&t, &key(i), &value(round, i)).expect("put");
    }
    txn.commit().expect("commit");
}

fn open_with(path: &Path, sync: SyncMode, gap_fill: GapFill) -> Env {
    Env::open(path, EnvOptions { sync, gap_fill, ..EnvOptions::default() }).expect("open")
}

/// The same commits with gap fill off, at 4 pages, at `MAX_GAP_FILL` and at auto leave the four
/// files identical, byte for byte, after every durable state (each `SyncMode::Durable` commit,
/// each `Env::sync` of `SyncMode::NoSync` commits), while the fills rewrote gaps: the larger the
/// width, the more, and auto at the widths its warm-up tried. Step 4 synced the same runs in all
/// four. On a file system that refuses reads that do not wait for the disk (tmpfs) the three
/// fills turn themselves off at their first gap instead, filling none (auto before it files a
/// step): the files are identical all the same.
#[test]
fn a_gap_fill_changes_no_byte_of_the_file() {
    for sync in [SyncMode::Durable, SyncMode::NoSync] {
        let fills = [GapFill::OFF, GapFill::Pages(4), GapFill::Pages(MAX_GAP_FILL), GapFill::Auto];
        let dirs: Vec<(TempDir, PathBuf)> = fills.iter().map(|_| temp()).collect();
        let refused = refuses_nowait_reads(dirs[1].0.path());
        let envs: Vec<Env> =
            fills.iter().zip(&dirs).map(|(&g, (_, path))| open_with(path, sync, g)).collect();
        let compare = |step: &str| {
            let bytes: Vec<Vec<u8>> =
                dirs.iter().map(|(_, path)| fs::read(path).expect("read")).collect();
            for (g, b) in fills.iter().zip(&bytes).skip(1) {
                assert!(*b == bytes[0], "{sync:?} {step}: gap fill {g} changed the file");
            }
        };
        for env in &envs {
            load(env);
        }
        compare("load");
        for round in 1..=6u32 {
            for env in &envs {
                scatter(env, round);
                if sync == SyncMode::NoSync && round % 2 == 0 {
                    env.sync().expect("sync");
                }
            }
            if sync == SyncMode::Durable || round % 2 == 0 {
                compare(&format!("round {round}"));
            }
        }
        let stats: Vec<IoStats> = envs.iter().map(Env::io_stats).collect();
        for (g, s) in fills.iter().zip(&stats) {
            assert_eq!(s.sync_runs, stats[0].sync_runs, "{sync:?} gap fill {g}: the same runs");
            assert_eq!(s.pages_synced, stats[0].pages_synced, "{sync:?} gap fill {g}");
            assert!(s.gap_pages >= s.gaps_filled, "{sync:?} gap fill {g}: {s:?}");
            assert!(s.gaps_filled < s.sync_runs, "{sync:?} gap fill {g}: {s:?}");
        }
        let off = (stats[0].gaps_filled, stats[0].gap_pages, stats[0].gap_fill_refusals);
        assert_eq!(off, (0, 0, 0), "{sync:?}: off fills none");
        let auto = stats[3];
        if refused {
            eprintln!("{sync:?}: the file system refuses RWF_NOWAIT: gap fill turned itself off");
            for (g, s) in fills.iter().zip(&stats).skip(1) {
                let counts = (s.gaps_filled, s.gap_pages, s.gap_fill_refusals);
                assert_eq!(counts, (0, 0, 1), "{sync:?} gap fill {g}: refused once: {s:?}");
            }
            assert_eq!(auto.gap_auto_steps, 0, "{sync:?}: auto off at its first step: {auto:?}");
        } else {
            assert!(stats.iter().all(|s| s.gap_fill_refusals == 0), "{sync:?}: {stats:?}");
            assert!(stats[1].gaps_filled > 0, "{sync:?}: 4 pages filled gaps: {:?}", stats[1]);
            assert!(
                stats[2].gaps_filled > stats[1].gaps_filled
                    && stats[2].gap_pages > stats[1].gap_pages,
                "{sync:?}: a larger gap fill fills more: {:?} {:?}",
                stats[1],
                stats[2]
            );
            let filled = auto.gap_auto_steps > 0 && auto.gaps_filled > 0;
            assert!(filled, "{sync:?}: auto filled: {auto:?}");
            assert_eq!(auto.gap_auto_steps, auto.fdatasync_syncs, "{sync:?}: each scattered step");
        }
        let empty = envs[0].with_writer(|w| w.gap_buf.capacity());
        assert_eq!(empty, 0, "{sync:?}: off, no buffer");
        for env in envs {
            env.check().expect("check");
        }
    }
}

/// A fill rewrites exactly the gaps of 1 to `gap_fill` clean pages between two of the pages
/// step 4 syncs, and none wider: here the gaps of 1, 3, 7 and 8 pages between pages 10..=40,
/// so at 1 page one gap (1 page), at 4 two (4), at 7 three (11), at 8 four (19). No byte of the
/// file changes, and on macOS the gap pages it wrote are dirty until a sync writes them. On a
/// file system that refuses reads that do not wait for the disk (tmpfs) each fill fills nothing
/// and turns gap fill off at its first gap.
#[test]
fn a_fill_rewrites_the_gaps_up_to_its_size_and_no_wider() {
    let (dir, path) = temp();
    let refused = refuses_nowait_reads(dir.path());
    let env = open_with(&path, SyncMode::Durable, GapFill::OFF);
    load(&env);
    let pages = [10u32, 12, 16, 24, 25, 34];
    let before = fs::read(&path).expect("read");
    for (g, filled, pages_filled) in [(1, 1, 1), (4, 2, 4), (7, 3, 11), (8, 4, 19)] {
        let was = env.io_stats();
        env.with_writer(|w| {
            #[cfg(target_os = "macos")]
            assert_eq!(w.file.dirty_os_pages(), 0, "gap fill {g}: clean before the fill");
            w.gap_fill = GapWidth::Fixed(g);
            let fill = w.fill_gaps(&pages, g).expect("fill");
            let want = if refused {
                Fill::default()
            } else {
                Fill { filled, missed: 0, pages: pages_filled }
            };
            assert_eq!(fill, want, "gap fill {g}: refused {refused}");
            assert_eq!(w.gap_fill.is_on(), !refused, "gap fill {g}: off once refused");
            #[cfg(target_os = "macos")]
            assert!(w.file.dirty_os_pages() > 0, "gap fill {g}: the fill wrote its gaps");
            // Write them now, so the next round starts clean.
            w.file.fdatasync_all().expect("sync");
        });
        let s = env.io_stats().since(&was);
        let want = if refused { (0, 0, 0, 1) } else { (filled, pages_filled, 0, 0) };
        let counts = (s.gaps_filled, s.gap_pages, s.gap_misses, s.gap_fill_refusals);
        assert_eq!(counts, want, "gap fill {g}: refused {refused}");
        assert!(fs::read(&path).expect("read") == before, "gap fill {g} changed the file");
    }
    env.check().expect("check");
}

/// A commit stops trying once its gaps not in the page cache outnumber those it filled by
/// `GAP_MISS_SLACK` (`PageFile::miss_reads`: every read misses): it tries 17 gaps of a commit of
/// many, fills none and writes nothing.
#[test]
fn a_fill_stops_once_its_misses_outnumber_its_fills_by_the_slack() {
    let (_dir, path) = temp();
    let env = open_with(&path, SyncMode::Durable, GapFill::Pages(4));
    load(&env);
    let pages: Vec<u32> = (0..200).map(|i| 10 + 2 * i).collect();
    let was = env.io_stats();
    env.with_writer(|w| {
        w.file.miss_reads = true;
        w.fill_gaps(&pages, 4).expect("fill");
        w.file.miss_reads = false;
    });
    let s = env.io_stats().since(&was);
    assert_eq!((s.gaps_filled, s.gap_pages), (0, 0));
    assert_eq!(s.gap_misses, GAP_MISS_SLACK + 1, "it stopped after the slack");
}

/// A kernel or file system that refuses reads that do not wait for the disk
/// (`PageFile::refuse_reads`, as tmpfs refuses `RWF_NOWAIT`) turns gap fill off for the
/// environment at the first gap a durable commit reads, at a fixed width and at auto alike: one
/// refusal, so one warning, however many later commits have gaps to fill (each of the twin whose
/// every read misses, `miss_reads`, tries some), no gap filled or missed, no step filed with auto,
/// the bounce buffer given back, and the file byte for byte the one gap fill off leaves.
#[test]
fn a_file_that_refuses_nowait_reads_turns_gap_fill_off_once() {
    let wide = GapFill::Pages(MAX_GAP_FILL);
    let fills = [GapFill::OFF, wide, wide, GapFill::Auto, GapFill::Auto];
    let dirs: Vec<(TempDir, PathBuf)> = fills.iter().map(|_| temp()).collect();
    let envs: Vec<Env> = fills
        .iter()
        .zip(&dirs)
        .map(|(&g, (_, path))| open_with(path, SyncMode::Durable, g))
        .collect();
    // Each width's environment that refuses, then its twin whose every read misses.
    let pairs = [(1, 2), (3, 4)];
    for &(refusing, missing) in &pairs {
        envs[refusing].with_writer(|w| w.file.refuse_reads = true);
        envs[missing].with_writer(|w| w.file.miss_reads = true);
    }
    for env in &envs {
        load(env);
    }
    let mut tried = [0u32; 2];
    for round in 1..=6u32 {
        let was: Vec<IoStats> = envs.iter().map(Env::io_stats).collect();
        for env in &envs {
            scatter(env, round);
        }
        for (&(refusing, missing), tried) in pairs.iter().zip(&mut tried) {
            let g = fills[refusing];
            let r = envs[refusing].io_stats().since(&was[refusing]);
            let m = envs[missing].io_stats().since(&was[missing]);
            *tried += u32::from(m.gap_misses > 0);
            let counts = (r.gaps_filled, r.gap_pages, r.gap_misses, r.gap_auto_steps);
            assert_eq!(counts, (0, 0, 0, 0), "{g} round {round}: {r:?}");
            let on = envs[refusing].with_writer(|w| (w.gap_fill.is_on(), w.gap_buf.capacity()));
            let refused = envs[refusing].io_stats().gap_fill_refusals;
            assert_eq!(on, (refused == 0, 0), "{g} round {round}");
        }
    }
    assert!(tried.iter().all(|&t| t >= 2), "several commits read gaps: {tried:?}");
    let bytes = |env: usize| fs::read(&dirs[env].1).expect("read");
    for &(refusing, missing) in &pairs {
        let g = fills[refusing];
        assert_eq!(envs[refusing].io_stats().gap_fill_refusals, 1, "{g}: refused once");
        assert_eq!(envs[missing].io_stats().gap_fill_refusals, 0, "{g}: a miss is no refusal");
        assert!(bytes(refusing) == bytes(0), "{g}: the refused fill changed the file");
        envs[refusing].check().expect("check");
    }
    assert_eq!(envs[0].io_stats().gap_fill_refusals, 0, "off reads no gap");
}

/// `EnvOptions::gap_fill` defaults to off; past `MAX_GAP_FILL` it is refused at open as
/// `InvalidGapFill`, before the file is touched: a missing file is not created, an existing one
/// keeps every byte. `MAX_GAP_FILL` itself opens, and a read-only open ignores the field.
#[test]
fn a_gap_fill_past_its_maximum_is_refused_at_open() {
    assert_eq!(EnvOptions::default().gap_fill, GapFill::OFF);
    assert_eq!(GapFill::default(), GapFill::Pages(0));
    let (dir, path) = temp();
    let wide = EnvOptions { gap_fill: GapFill::Pages(MAX_GAP_FILL + 1), ..EnvOptions::default() };
    assert!(matches!(
        Env::open(&path, wide),
        Err(Error::InvalidGapFill(g)) if g == MAX_GAP_FILL + 1
    ));
    assert_eq!(fs::read_dir(dir.path()).expect("dir").count(), 0, "the refusal created a file");
    load(&open_with(&path, SyncMode::Durable, GapFill::OFF));
    let before = fs::read(&path).expect("read");
    assert!(matches!(Env::open(&path, wide), Err(Error::InvalidGapFill(_))));
    assert!(fs::read(&path).expect("read") == before, "the refused open changed the file");
    drop(Env::open_read_only(&path, wide).expect("a read-only open ignores gap_fill"));
    let env = open_with(&path, SyncMode::Durable, GapFill::Pages(MAX_GAP_FILL));
    let ro = env.begin_ro().expect("ro");
    let t = ro.open_table("t").expect("open").expect("t exists");
    assert_eq!(ro.get(&t, &key(7)).expect("get"), Some(&value(0, 7)[..]));
}

/// macOS: with gap fill on, once a durable commit returns no page of the file is dirty in memory
/// only: step 4's `msync`s of `DataSync::Ranges` write the gaps the fill rewrote through the file
/// with the pages written through the mapping (each gap lies inside a merged range).
#[test]
#[cfg(target_os = "macos")]
fn a_gap_filled_scattered_commit_leaves_no_page_dirty() {
    let (_dir, path) = temp();
    let env = open_with(&path, SyncMode::Durable, GapFill::Pages(8));
    load(&env);
    let mut filled = 0;
    for round in 1..8u32 {
        let before = env.io_stats();
        scatter(&env, round);
        let s = env.io_stats().since(&before);
        // The first rounds take new pages at the end; the later ones reuse pages freed all over.
        if round >= 3 {
            assert_eq!(s.fdatasync_syncs, 1, "round {round}: scattered: {s:?}");
        }
        filled += s.gaps_filled;
        let dirty = env.with_writer(|w| w.file.dirty_os_pages());
        assert_eq!(dirty, 0, "round {round}: OS pages left dirty after the commit returned");
    }
    assert!(filled > 0, "the commits filled gaps");
}

/// `GapFill` parses from `auto`, `off` and a page count of at most `MAX_GAP_FILL` (0 is off), and
/// shows as it parses; anything else is refused, naming what it got.
#[test]
fn a_gap_fill_parses_from_auto_off_or_a_page_count() {
    let parse = |s: &str| s.parse::<GapFill>();
    assert_eq!(parse("auto"), Ok(GapFill::Auto));
    assert_eq!(parse("off"), Ok(GapFill::OFF));
    assert_eq!(parse("0"), Ok(GapFill::OFF));
    assert_eq!(parse("8"), Ok(GapFill::Pages(8)));
    assert_eq!(parse("64"), Ok(GapFill::Pages(MAX_GAP_FILL)));
    for bad in ["65", "-1", "", "Auto", "8 pages"] {
        let err = parse(bad).expect_err(bad);
        assert!(err.to_string().contains(&format!("{bad:?}")), "{bad}: {err}");
    }
    for g in [GapFill::Auto, GapFill::OFF, GapFill::Pages(8)] {
        assert_eq!(parse(&g.to_string()), Ok(g), "{g} round trips");
    }
    assert!(!GapFill::OFF.is_on() && GapFill::Pages(1).is_on() && GapFill::Auto.is_on());
}

/// Under `GapFill::Auto` every scattered durable step (synced by the `fdatasync`; on macOS by the
/// `msync`s of `DataSync::Ranges`, which count there too) is one of `IoStats::gap_auto_steps`,
/// whose widths follow the warm-up (8, 0, 4, ...), each step at another width than the model's
/// (0 before the warm-up ends) counted a probe; a step synced by one `msync` fills nothing and
/// is not counted. The fill's cost per gap is published, and the file checks. On a file system
/// that refuses reads that do not wait for the disk (tmpfs) the first scattered step's first gap
/// read turns gap fill off instead, once: no step is filed and none fills.
#[test]
fn an_auto_gap_fill_publishes_its_width_and_its_fit() {
    let (dir, path) = temp();
    let refused = refuses_nowait_reads(dir.path());
    let env = open_with(&path, SyncMode::Durable, GapFill::Auto);
    load(&env);
    let was = env.io_stats();
    let mut txn = env.begin_rw().expect("begin");
    let t = txn.open_table("t").expect("open").expect("t exists");
    txn.put(&t, &key(1), &value(9, 1)).expect("put");
    txn.commit().expect("a commit of a few pages");
    let few = env.io_stats().since(&was);
    assert_eq!((few.gap_auto_steps, few.gaps_filled), (0, 0), "a step of few runs: {few:?}");
    let (mut widths, mut filled) = (Vec::new(), 0);
    for round in 1..=12u32 {
        let before = env.io_stats();
        scatter(&env, round);
        let s = env.io_stats().since(&before);
        if refused {
            assert_eq!((s.gap_auto_steps, s.gaps_filled), (0, 0), "round {round}: refused: {s:?}");
            continue;
        }
        assert_eq!(s.gap_auto_steps, s.fdatasync_syncs, "round {round}: {s:?}");
        if s.gap_auto_steps == 0 {
            assert_eq!(s.gaps_filled, 0, "round {round}: one msync fills nothing");
            continue;
        }
        assert_eq!(s.gap_auto_width_sum, s.gap_auto_width, "round {round}");
        assert_eq!(s.gap_auto_probes, u64::from(s.gap_auto_width != 0), "round {round}: {s:?}");
        if s.gap_auto_width == 0 {
            assert_eq!(s.gaps_filled, 0, "round {round}: width 0 fills nothing");
        }
        filled += s.gaps_filled;
        widths.push(s.gap_auto_width);
    }
    let s = env.io_stats();
    if refused {
        eprintln!("the file system refuses RWF_NOWAIT: auto turned itself off");
        assert_eq!(s.gap_fill_refusals, 1, "refused once: {s:?}");
        assert!(!env.with_writer(|w| w.gap_fill.is_on()), "auto is off");
    } else {
        assert_eq!(s.gap_fill_refusals, 0, "{s:?}");
        assert!(widths.len() >= 6, "the later rounds are scattered: {widths:?}");
        assert_eq!(widths, [8, 0, 4].repeat(4)[..widths.len()], "the warm-up's widths");
        assert!(filled > 0 && s.gap_auto_gap_ns > 0, "the warm-up filled gaps: {s:?}");
    }
    assert_eq!(env.io_stats().since(&was).gap_auto_readapts, 0);
    env.check().expect("check");
}
