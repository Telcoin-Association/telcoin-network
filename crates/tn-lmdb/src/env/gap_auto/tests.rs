//! The automatic width's chooser on modelled disks: steps of scattered pages whose gap sizes
//! follow the measured commits', synced by a disk whose time is a known function of the runs and
//! pages written (with noise, spikes, or a change mid-run). Each test checks the widths chosen
//! past the warm-up against the width that model makes cheapest.

use super::*;
use crate::tree::test_harness::{ReportSeed, Rng};

/// A uniform value in `[0, 1)`.
fn unit(rng: &mut Rng) -> f64 {
    (rng.next() >> 11) as f64 / (1u64 << 53) as f64
}

/// A scattered step's pages: `runs` runs, most of one page and some of two or three, separated
/// by gaps of 1 to 64 clean pages (log-uniform) and, one in seven, wider ones. Like the measured
/// commits' (README, "Gap fill"): about a third of the gaps at most 4 pages, half at most 8.
fn step_pages(rng: &mut Rng, runs: usize) -> Vec<u32> {
    let mut pages = Vec::new();
    let mut page = 100u32;
    for _ in 0..runs {
        let len = match rng.below(10) {
            0 => 3,
            1 | 2 => 2,
            _ => 1,
        };
        pages.extend(page..page + len);
        page += len;
        page += if rng.below(7) == 0 {
            65 + rng.below(300) as u32
        } else {
            (65f64.powf(unit(rng)) as u32).clamp(1, 64)
        };
    }
    pages
}

/// A disk's sync time, ns, for a step at index `i` writing `runs` runs of `pages` pages.
type Disk<'a> = dyn Fn(usize, f64, f64, &mut Rng) -> f64 + 'a;

/// A disk's noise-free sync time, ns, for a step at index `i` writing `runs` runs of `pages`.
type Truth<'a> = dyn Fn(usize, f64, f64) -> f64 + 'a;

/// A linear disk: `fixed + per_run · runs + per_page · pages`.
fn linear(fixed: f64, per_run: f64, per_page: f64) -> impl Fn(f64, f64) -> f64 {
    move |runs, pages| fixed + per_run * runs + per_page * pages
}

/// What one modelled step chose, and what was best.
#[derive(Debug)]
struct Step {
    /// The width chosen.
    width: u32,
    /// Whether it was a probe (or a warm-up step at another width than the model's).
    probe: bool,
    /// The width the noise-free model makes cheapest, fill included.
    best: u32,
    /// The noise-free time of step 4 at the width chosen and at the best, ns.
    cost: f64,
    best_cost: f64,
}

/// Run `steps` modelled steps through a fresh [`AutoGap`]: each of 300 to 1,800 runs, filled at
/// the chosen width with every gap in memory at `fill_ns` per gap, synced by `disk` (its time as
/// the outcome) and judged by `truth` (its noise-free time). Returns each step's record, and the
/// re-adaptations and the last model.
fn simulate(
    seed: u64,
    steps: usize,
    fill_ns: f64,
    disk: &Disk<'_>,
    truth: &Truth<'_>,
) -> (Vec<Step>, u64, Model) {
    let _seed = ReportSeed(seed);
    let mut rng = Rng(seed);
    let mut auto = AutoGap::default();
    let (mut out, mut readapts) = (Vec::with_capacity(steps), 0);
    for i in 0..steps {
        let runs = 300 + rng.below(1500) as usize;
        let gaps = Gaps::of(&step_pages(&mut rng, runs));
        let upto = gaps.upto();
        let at = |width: u32| {
            let (n, extra) = upto[CANDIDATES.iter().position(|&c| c == width).expect("candidate")];
            (gaps.runs - n, gaps.pages + extra, n)
        };
        let cost = |width: u32| {
            let (runs, pages, n) = at(width);
            truth(i, runs as f64, pages as f64) + fill_ns * n as f64
        };
        let best = CANDIDATES
            .iter()
            .copied()
            .min_by(|&a, &b| cost(a).total_cmp(&cost(b)))
            .expect("candidates");
        let choice = auto.choose(&gaps);
        let (runs, pages, n) = at(choice.width);
        let sync = disk(i, runs as f64, pages as f64, &mut rng).max(0.0);
        readapts += u64::from(auto.observe(Outcome {
            runs,
            pages,
            sync_ns: sync as u64,
            fill_ns: (fill_ns * n as f64) as u64,
            tried: n,
            filled: n,
        }));
        out.push(Step {
            width: choice.width,
            probe: choice.probe,
            best,
            cost: cost(choice.width),
            best_cost: cost(best),
        });
    }
    (out, readapts, auto.model())
}

/// The steps `from..to` at the model's choice (no probe).
fn chosen(steps: &[Step], from: usize, to: usize) -> impl Iterator<Item = &Step> {
    steps[from..to].iter().filter(|s| !s.probe)
}

/// The share of `steps` that chose the best width.
fn share_best<'a>(steps: impl Iterator<Item = &'a Step>) -> f64 {
    let (mut n, mut hit) = (0, 0);
    for s in steps {
        n += 1;
        hit += usize::from(s.width == s.best);
    }
    hit as f64 / n.max(1) as f64
}

/// The time of `steps` at their widths over their time at the best: 1 is optimal.
fn excess<'a>(steps: impl Iterator<Item = &'a Step>) -> f64 {
    let (mut cost, mut best) = (0.0, 0.0);
    for s in steps {
        cost += s.cost;
        best += s.best_cost;
    }
    cost / best
}

/// The measured fill cost per gap (4.7 µs on the Mac).
const FILL_NS: f64 = 4_700.0;

/// A disk whose syncs cost per write only (an IOPS cap of 5,000: 200 µs a write, bandwidth
/// free): every gap filled saves a write for a few µs of copying, so the widest width wins and
/// auto chooses it at every step past the warm-up.
#[test]
fn a_disk_that_costs_per_write_chooses_the_widest_width() {
    let disk = linear(1e6, 200_000.0, 0.0);
    let (steps, readapts, _) =
        simulate(0x9e37_79b9_7f4a_7c15, 300, FILL_NS, &|_, r, p, _| disk(r, p), &|_, r, p| {
            disk(r, p)
        });
    assert!(steps.iter().all(|s| s.best == MAX_GAP_FILL), "the model's best is the widest");
    assert_eq!(share_best(chosen(&steps, 20, 300)), 1.0, "{:?}", &steps[20..40]);
    assert_eq!(readapts, 0);
}

/// A disk whose syncs cost per byte only (local NVMe: 1.5 µs a page, writes free): every page a
/// fill adds costs, so auto chooses 0 at every step past the warm-up.
#[test]
fn a_disk_that_costs_per_byte_chooses_no_fill() {
    let disk = linear(200_000.0, 0.0, 1_500.0);
    let (steps, readapts, model) =
        simulate(0x2545_f491_4f6c_dd1d, 300, FILL_NS, &|_, r, p, _| disk(r, p), &|_, r, p| {
            disk(r, p)
        });
    assert!(steps.iter().all(|s| s.best == 0));
    assert!(chosen(&steps, 20, 300).all(|s| s.width == 0), "{:?}", &steps[20..40]);
    assert_eq!(readapts, 0);
    assert!(!model.capped, "{model:?}");
}

/// Mixed costs choose the crossover the model predicts:
/// - a fast drive (3 µs a write, 1.5 µs a page): a gap of one page saves 3 µs and costs 1.5 µs plus
///   the fill's 4.7 µs, so no fill;
/// - a drive at 20 µs a write and 1.5 µs a page: a gap of `s` pages pays while `1.5 s + 4.7 < 20`,
///   up to 10 pages, so 8;
/// - a cloud disk at 4,500 IOPS (222 µs a write) and 125 MB/s (33 µs a page): about 4.
///
/// Past the warm-up, nearly every step chooses the best width, and the steps' time is within a
/// percent of the best.
#[test]
fn mixed_costs_choose_the_crossover() {
    for (seed, (per_run, per_page), want) in [
        (0x1234_5678_9abc_def1, (3_000.0, 1_500.0), Some(0)),
        (0x0fed_cba9_8765_4321, (20_000.0, 1_500.0), Some(8)),
        (0x5555_aaaa_3333_cccc, (222_000.0, 33_000.0), None),
    ] {
        let disk = linear(500_000.0, per_run, per_page);
        let (steps, readapts, model) =
            simulate(seed, 400, FILL_NS, &|_, r, p, _| disk(r, p), &|_, r, p| disk(r, p));
        let case = format!("{per_run} ns a write, {per_page} ns a page");
        if let Some(want) = want {
            let bests = steps.iter().filter(|s| s.best == want).count();
            assert!(bests * 10 >= steps.len() * 9, "{case}: the crossover is {want}");
        }
        let share = share_best(chosen(&steps, 50, 400));
        let excess = excess(chosen(&steps, 50, 400));
        assert!(share >= 0.9, "{case}: {share} chose the best");
        assert!(excess < 1.01, "{case}: {excess}x the best time");
        assert_eq!(readapts, 0, "{case}");
        assert!(!model.capped, "{case}: a linear disk keeps the sum: {model:?}");
    }
}

/// The cloud disk of `mixed_costs_choose_the_crossover` with noise: each sync's time scaled by up
/// to ±30% (a sum of uniforms), and one in thirty a 10x stall. The fit's robust weights keep the
/// stalls from moving it: past the warm-up the steps' time is within 2% of the best, nearly every
/// step chooses the best width or a neighbour (4 and 8 cost within a percent here), and no run of
/// stalls reads as a changed disk.
#[test]
fn noisy_samples_still_choose_near_the_best() {
    let disk = linear(2e6, 222_000.0, 33_000.0);
    let noisy = |_: usize, r: f64, p: f64, rng: &mut Rng| {
        let noise = 1.0 + 0.15 * (unit(rng) + unit(rng) + unit(rng) + unit(rng) - 2.0);
        let stall = if rng.below(30) == 0 { 10.0 } else { 1.0 };
        disk(r, p) * noise * stall
    };
    for seed in [0xdead_beef_cafe_f00d, 0x0123_4567_89ab_cdef, 0x7777_1111_9999_3333] {
        let (steps, readapts, model) = simulate(seed, 600, FILL_NS, &noisy, &|_, r, p| disk(r, p));
        let near = chosen(&steps, 50, 600)
            .filter(|s| [s.best / 2, s.best, s.best * 2].contains(&s.width))
            .count() as f64
            / chosen(&steps, 50, 600).count() as f64;
        let excess = excess(chosen(&steps, 50, 600));
        assert!(near >= 0.95, "seed {seed:#x}: {near} chose the best or a neighbour");
        assert!(!model.capped, "seed {seed:#x}: a noisy linear disk keeps the sum: {model:?}");
        assert!(excess < 1.02, "seed {seed:#x}: {excess}x the best time");
        assert_eq!(readapts, 0, "seed {seed:#x}");
    }
}

/// A disk whose syncs take the larger of their writes over an IOPS cap and their bytes over a
/// bandwidth cap (4,500 IOPS, 125 MB/s), which only the capped form fits: past the warm-up
/// nearly every step chooses the balance (8 here), the steps' time is within 5% of the best (the
/// rest: a few steps at the widest after the warm-up, whose widths all sat below the cap), and
/// the sum form, which cannot fit the cap, would pick far more widely.
#[test]
fn a_disk_capped_on_writes_and_bytes_chooses_the_balance() {
    let capped = |_: usize, r: f64, p: f64| 1e6 + (r * 222_000.0).max(p * 33_000.0);
    let (steps, _, model) =
        simulate(0x4242_4242_1717_1717, 500, FILL_NS, &|i, r, p, _| capped(i, r, p), &capped);
    let share = share_best(chosen(&steps, 50, 500));
    let excess = excess(chosen(&steps, 50, 500));
    assert!(share >= 0.95, "{share} chose the best");
    assert!(excess < 1.05, "{excess}x the best time");
    assert!(model.capped, "the capped form fits: {model:?}");
    assert!((model.per_run / 222_000.0 - 1.0).abs() < 0.01, "{model:?}");
    assert!((model.per_page / 33_000.0 - 1.0).abs() < 0.01, "{model:?}");
}

/// A disk whose costs change mid-run re-adapts. Raised cloud limits first (from 3,000 IOPS and
/// 125 MB/s to 16,000 IOPS and 250 MB/s: the best width narrows), then a move to a fast local
/// drive (no fill best): each change is seen as a run of syncs off the prediction, which restarts
/// the warm-up, and 100 steps after each change nearly every step chooses the new best.
#[test]
fn a_disk_whose_costs_change_re_adapts() {
    let phase = |i: usize| match i {
        0..400 => linear(1e6, 333_000.0, 33_000.0),
        400..800 => linear(1e6, 62_500.0, 16_400.0),
        _ => linear(100_000.0, 3_000.0, 1_500.0),
    };
    let disk = |i: usize, r: f64, p: f64, _: &mut Rng| phase(i)(r, p);
    let truth = |i: usize, r: f64, p: f64| phase(i)(r, p);
    let (steps, readapts, model) = simulate(0x3141_5926_5358_9793, 1200, FILL_NS, &disk, &truth);
    assert!(readapts >= 2, "each change re-adapted: {readapts}");
    assert!(!model.capped && model.per_run < 10_000.0, "the local drive's fit: {model:?}");
    let bests = |from, to| {
        let mut b: Vec<u32> = steps[from..to].iter().map(|s| s.best).collect();
        b.sort_unstable();
        b[b.len() / 2]
    };
    let (first, raised, local) = (bests(0, 400), bests(400, 800), bests(800, 1200));
    assert!(first > raised && raised > local && local == 0, "{first} {raised} {local}");
    for (from, to) in [(100, 400), (500, 800), (900, 1200)] {
        let share = share_best(chosen(&steps, from, to));
        let excess = excess(chosen(&steps, from, to));
        assert!(share >= 0.85, "steps {from}..{to}: {share} chose the best");
        assert!(excess < 1.02, "steps {from}..{to}: {excess}x the best time");
    }
}

/// The schedule: a warm-up of `WARMUP_STEPS` cycling `WARMUP`, each at another width than the
/// model's 0 a probe; then probes `PROBE_EVERY` steps apart, the interval doubling after each up
/// to `PROBE_EVERY_MAX` while the choice holds, and back to `PROBE_EVERY` at a new choice. A
/// probe takes half or twice the choice (at least 2), alternately, the only one that exists at 0
/// and at the widest.
#[test]
fn the_schedule_warms_up_then_probes_beside_the_choice() {
    let gaps = Gaps::of(&(0..400).map(|i| i * 3).collect::<Vec<u32>>());
    let mut auto = AutoGap::default();
    let mut widths = Vec::new();
    for _ in 0..WARMUP_STEPS {
        let c = auto.choose(&gaps);
        assert_eq!(c.probe, c.width != 0);
        widths.push(c.width);
        auto.pending = None;
    }
    assert_eq!(widths, [8, 0, 4].repeat(4));
    // Past the warm-up, a zero model chooses 0: probes at 16, 48, 112, 240, then 128 apart.
    let probe_steps = |auto: &mut AutoGap, steps: u64| -> Vec<(u64, u32)> {
        (1..=steps)
            .filter_map(|i| {
                let c = auto.choose(&gaps);
                auto.pending = None;
                c.probe.then_some((i, c.width))
            })
            .collect()
    };
    let probes = probe_steps(&mut auto, 400);
    assert_eq!(probes, [(16, 2), (48, 2), (112, 2), (240, 2), (368, 2)]);
    // A new choice brings the interval back: a model that pays per write fills every gap, here
    // all of 2 pages, so 2 (the smallest of the widths that tie). Seen at the first step, it puts
    // the next probe 16 steps on, then 32; the probes alternate twice and half of it.
    auto.model = Model { per_run: 1e6, ..Model::default() };
    let probes = probe_steps(&mut auto, 64);
    assert_eq!(probes, [(17, 4), (49, 1)], "16 after the new choice, then 32");
    assert_eq!(auto.probe(0), 2, "at 0 only the higher");
    assert_eq!(auto.probe(MAX_GAP_FILL), 32, "at the widest only the lower");
    let both: Vec<u32> = (0..4).map(|_| auto.probe(8)).collect();
    assert!(both == [4, 16, 4, 16] || both == [16, 4, 16, 4], "alternately: {both:?}");
    assert_eq!([auto.probe(1), auto.probe(1)].iter().sum::<u32>(), 2, "half of 1 is 0, twice 2");
}

/// `Gaps` counts the runs, pages and gap sizes of a step in one pass, and `upto` the gaps and
/// their pages at each candidate width: here gaps of 1, 3, 7, 8 and 100 pages.
#[test]
fn gaps_count_each_candidates_gaps_and_pages() {
    let pages = [10u32, 12, 16, 17, 25, 34, 135];
    let gaps = Gaps::of(&pages);
    assert_eq!((gaps.runs, gaps.pages), (6, 7));
    let upto = gaps.upto();
    let want = [(0, 0), (1, 1), (1, 1), (2, 4), (4, 19), (4, 19), (4, 19), (4, 19)];
    assert_eq!(upto, want);
    let empty = Gaps::of(&[]);
    assert_eq!((empty.runs, empty.pages, empty.upto()[7]), (0, 0, (0, 0)));
}

/// The fit recovers a linear disk's costs exactly from steps at three widths, never returns a
/// negative cost (a disk faster for more runs fits 0 per run), and a stall among the samples
/// moves it little.
#[test]
fn the_fit_recovers_the_costs_and_resists_a_stall() {
    let sample = |runs: f64, pages: f64, ns: f64| Sample { runs, pages, ns };
    let disk = linear(800_000.0, 40_000.0, 2_000.0);
    let mut samples: Vec<Sample> = (0..12)
        .map(|i| {
            let runs = 500.0 + 37.0 * f64::from(i) - 120.0 * f64::from(i % 3);
            let pages = 700.0 + 61.0 * f64::from(i) + 400.0 * f64::from(i % 3);
            sample(runs, pages, disk(runs, pages))
        })
        .collect();
    let m = fit(&samples);
    assert!((m.fixed - 800_000.0).abs() < 1.0, "{m:?}");
    assert!((m.per_run - 40_000.0).abs() < 0.01 && (m.per_page - 2_000.0).abs() < 0.01, "{m:?}");
    samples[5].ns *= 10.0;
    let m = fit(&samples);
    assert!((m.per_run - 40_000.0).abs() < 400.0, "a stall moves it little: {m:?}");
    assert!((m.per_page - 2_000.0).abs() < 100.0, "{m:?}");
    let faster: Vec<Sample> = (0..8)
        .map(|i| {
            let runs = 100.0 + 50.0 * f64::from(i);
            let pages = 1_000.0 + 10.0 * f64::from(i % 3);
            sample(runs, pages, 5e6 - 1_000.0 * runs + 1_000.0 * pages)
        })
        .collect();
    let m = fit(&faster);
    assert!(m.fixed >= 0.0 && m.per_run >= 0.0 && m.per_page >= 0.0, "{m:?}");
    assert_eq!(m.per_run, 0.0, "{m:?}");
}

/// A capped disk's exact samples at three widths, two on the write cap and three probes on the
/// byte cap (two widths alone a sum fits as well): the fit takes the capped form and recovers
/// both caps. The sum fits the twelve samples of the first two widths well and misses the three
/// probes, which the trimmed error counts (a median of the residuals would not: it chose the sum
/// here).
#[test]
fn the_fit_keeps_a_few_exact_samples_past_a_cap() {
    let disk = |runs: f64, pages: f64| 1e6 + (runs * 222_000.0).max(pages * 33_000.0);
    let samples: Vec<Sample> = (0..15)
        .map(|i| {
            let size = 1.0 + f64::from(i % 5) / 4.0;
            let (runs, pages) = match i {
                0..8 => (600.0, 900.0),
                8..12 => (450.0, 1_500.0),
                _ => (150.0, 4_000.0),
            };
            let (runs, pages) = (runs * size, pages * size);
            Sample { runs, pages, ns: disk(runs, pages) }
        })
        .collect();
    let m = fit(&samples);
    assert!(m.capped, "{m:?}");
    assert!((m.per_run / 222_000.0 - 1.0).abs() < 1e-6, "{m:?}");
    assert!((m.per_page / 33_000.0 - 1.0).abs() < 1e-6, "the byte cap: {m:?}");
}

/// The chooser's cost per scattered step, on steps about the execution benchmark's busy size
/// (about 21,000 runs and 25,000 pages), synced by a modelled cloud disk: `Gaps::of`'s pass over
/// the pages, the choice, and the outcome's fit. Guarded at 1 ms a step; prints the figures.
#[test]
#[ignore = "measurement: the chooser's cost per step; run with --release"]
fn the_chooser_costs_microseconds_per_step() {
    let mut rng = Rng(0x0bad_c0de_dead_10cc);
    let steps: Vec<Vec<u32>> = (0..16)
        .map(|_| {
            let runs = 18_000 + rng.below(6_000) as usize;
            step_pages(&mut rng, runs)
        })
        .collect();
    let disk = |r: f64, p: f64| 1e6 + (r * 222_000.0).max(p * 33_000.0);
    let mut auto = AutoGap::default();
    let rounds = 2_000usize;
    let (mut of_ns, mut choose_ns, mut observe_ns) = (0u128, 0u128, 0u128);
    for i in 0..rounds {
        let pages = &steps[i % steps.len()];
        let t = std::time::Instant::now();
        let gaps = Gaps::of(std::hint::black_box(pages));
        of_ns += t.elapsed().as_nanos();
        let t = std::time::Instant::now();
        let c = auto.choose(&gaps);
        choose_ns += t.elapsed().as_nanos();
        let (n, extra) = gaps.upto()[CANDIDATES.iter().position(|&w| w == c.width).expect("w")];
        let (runs, pages) = (gaps.runs - n, gaps.pages + extra);
        let outcome = Outcome {
            runs,
            pages,
            sync_ns: disk(runs as f64, pages as f64) as u64,
            fill_ns: (FILL_NS * n as f64) as u64,
            tried: n,
            filled: n,
        };
        let t = std::time::Instant::now();
        auto.observe(outcome);
        observe_ns += t.elapsed().as_nanos();
    }
    let pages = steps.iter().map(Vec::len).sum::<usize>() / steps.len();
    let per = |ns: u128| ns as f64 / rounds as f64 / 1e3;
    eprintln!(
        "gap_auto_cost pages={pages} gaps_of_us={:.1} choose_us={:.2} observe_us={:.1} model={:?}",
        per(of_ns),
        per(choose_ns),
        per(observe_ns),
        auto.model()
    );
    assert!(per(of_ns + choose_ns + observe_ns) < 1_000.0, "under 1 ms a step");
}
