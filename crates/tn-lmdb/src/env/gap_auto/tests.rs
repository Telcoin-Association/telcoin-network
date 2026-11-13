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
    /// Step 4's time as the disk took it, fill included, ns.
    time: f64,
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
    simulate_with(seed, steps, (300, 1500), None, fill_ns, disk, truth)
}

/// [`simulate`] with each step's runs `runs.0` plus up to `runs.1` more, at the width `fixed`
/// when given (no chooser: the steps of a fixed width, for a disk whose time depends on the
/// steps before), else at the chooser's.
fn simulate_with(
    seed: u64,
    steps: usize,
    runs: (usize, u64),
    fixed: Option<u32>,
    fill_ns: f64,
    disk: &Disk<'_>,
    truth: &Truth<'_>,
) -> (Vec<Step>, u64, Model) {
    let _seed = ReportSeed(seed);
    let mut rng = Rng(seed);
    let mut auto = AutoGap::default();
    let (mut out, mut readapts) = (Vec::with_capacity(steps), 0);
    let (min_runs, more_runs) = runs;
    for i in 0..steps {
        let runs = min_runs + rng.below(more_runs) as usize;
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
        let choice = match fixed {
            Some(width) => Choice { width, probe: false },
            None => auto.choose(&gaps),
        };
        let (runs, pages, n) = at(choice.width);
        let sync = disk(i, runs as f64, pages as f64, &mut rng).max(0.0);
        if fixed.is_none() {
            readapts += u64::from(auto.observe(Outcome {
                runs,
                pages,
                sync_ns: sync as u64,
                fill_ns: (fill_ns * n as f64) as u64,
                tried: n,
                filled: n,
            }));
        }
        out.push(Step {
            width: choice.width,
            probe: choice.probe,
            best,
            cost: cost(choice.width),
            best_cost: cost(best),
            time: sync + fill_ns * n as f64,
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
/// free): every gap filled saves a write for a few µs of copying, so the widest width wins, and
/// auto, from 8 past the warm-up, climbs a width at a time to it (each probed, then held) and
/// chooses it at every step from the 40th.
#[test]
fn a_disk_that_costs_per_write_chooses_the_widest_width() {
    let disk = linear(1e6, 200_000.0, 0.0);
    let (steps, readapts, _) =
        simulate(0x9e37_79b9_7f4a_7c15, 300, FILL_NS, &|_, r, p, _| disk(r, p), &|_, r, p| {
            disk(r, p)
        });
    assert!(steps.iter().all(|s| s.best == MAX_GAP_FILL), "the model's best is the widest");
    assert_eq!(share_best(chosen(&steps, 40, 300)), 1.0, "{:?}", &steps[20..40]);
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
/// step chooses the best width or a neighbour (4 and 8 cost within a percent here), the choice
/// does not trade those two on noise (a move must save 5%), and no run of stalls reads as a
/// changed disk.
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
        let moves = moves(&steps).len();
        assert!(near >= 0.95, "seed {seed:#x}: {near} chose the best or a neighbour");
        assert!(moves <= 5, "seed {seed:#x}: {moves} moves between widths that cost alike");
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

/// The longest run of steps in a row past the warm-up that chose 0 (probes included).
fn longest_run_at_zero(steps: &[Step]) -> usize {
    let (mut longest, mut run) = (0, 0);
    for s in &steps[WARMUP_STEPS as usize..] {
        run = if s.width == 0 { run + 1 } else { 0 };
        longest = longest.max(run);
    }
    longest
}

/// The first step past the warm-up held at a width no earlier step measured, if any: a move goes
/// only to a measured width.
fn held_unmeasured(steps: &[Step]) -> Option<usize> {
    let mut measured = [false; CANDIDATES.len()];
    for (i, s) in steps.iter().enumerate() {
        if i >= WARMUP_STEPS as usize && !s.probe && !measured[slot(s.width)] {
            return Some(i);
        }
        measured[slot(s.width)] = true;
    }
    None
}

/// The moves of the choice in `steps` past the warm-up: a step at the choice (no probe) at
/// another width than the last such step.
fn moves(steps: &[Step]) -> Vec<(usize, u32)> {
    let mut last = None;
    let mut out = Vec::new();
    for (i, s) in steps.iter().enumerate().skip(WARMUP_STEPS as usize).filter(|(_, s)| !s.probe) {
        if last.is_some_and(|w| w != s.width) {
            out.push((i, s.width));
        }
        last = Some(s.width);
    }
    out
}

/// The time of `steps` past `from` over their count: the mean step 4, ns.
fn mean_time(steps: &[Step], from: usize) -> f64 {
    steps[from..].iter().map(|s| s.time).sum::<f64>() / (steps.len() - from) as f64
}

/// A disk whose writes past a burst allowance cost far more each than the ones within it (a cloud
/// disk's IOPS cap past its burst): steps of about 1,000 runs, 10 µs a page, 5 µs a write for a
/// sync's first 880 writes and 300 µs for each after, whichever of bytes and writes takes longer.
/// Every width from 1 up is bound by its bytes, so 1 is best (about 17 ms), 2 and 4 cost 12% and
/// 37% more, and 0, 120 writes past the allowance, about 2.4 times as much. Neither form fits the
/// disk whole: fitted to 0's steps, the writes all cost about 40 µs, and fitted to the others',
/// nothing (no sampled width is bound by its writes, and the sum's fit then puts 0 there, which
/// makes 0 the cheapest by its pages). So past the warm-up the choice never moves to 0 (0 only as
/// a probe, never twice in a row), and settles at 1: the steps' time within 3% of the best's.
#[test]
fn a_disk_whose_writes_past_a_burst_cost_more_never_settles_at_no_fill() {
    let burst = |_: usize, runs: f64, pages: f64| {
        1e6 + (pages * 10_000.0)
            .max(5_000.0 * runs.min(880.0) + 300_000.0 * (runs - 880.0).max(0.0))
    };
    for seed in [0x6a09_e667_f3bc_c908, 0xbb67_ae85_84ca_a73b, 0x3c6e_f372_fe94_f82b] {
        let (steps, readapts, model) = simulate_with(
            seed,
            600,
            (970, 60),
            None,
            FILL_NS,
            &|i, r, p, _| burst(i, r, p),
            &burst,
        );
        let at_zero = chosen(&steps, WARMUP_STEPS as usize, 600).filter(|s| s.width == 0).count();
        let longest = longest_run_at_zero(&steps);
        let share = share_best(chosen(&steps, 150, 600));
        let excess = excess(chosen(&steps, 150, 600));
        let widths: Vec<u32> = steps.iter().map(|s| s.width).collect();
        let case = format!("seed {seed:#x}: {model:?} {widths:?}");
        assert_eq!(at_zero, 0, "{case}: chose 0 {at_zero} times");
        assert!(longest <= 1, "{case}: {longest} steps at 0 in a row");
        assert_eq!(held_unmeasured(&steps), None, "{case}: held before measured");
        assert!(share >= 0.9, "{case}: {share} chose the best");
        assert!(excess < 1.03, "{case}: {excess}x the best time");
        assert_eq!(readapts, 0, "{case}");
    }
}

/// A disk with a burst allowance of writes refilled between syncs (a cloud disk's IOPS cap over
/// the time between commits): a bucket of 1,000 writes, 600 more each step; writes past what the
/// bucket holds cost 300 µs each and the rest 5 µs, and pages 10 µs, whichever of bytes and writes
/// takes longer. Steps of about 1,000 runs. Held at a width, 4 writes about 670 runs a step, 70
/// past the refill, and costs about 27 ms once the bucket drains, 8 about 31 ms and 2 about 57;
/// 0 about 123 ms. But a step at a narrower width right after wider ones spends the bucket they
/// left and looks cheap once: 0 after 8 costs 15 ms. So past the warm-up the choice never stays at
/// 0 two steps in a row, and the steps' time is within 10% of the best fixed width's.
#[test]
fn a_disk_with_a_refilled_burst_of_writes_never_stays_at_no_fill() {
    let bucket = || {
        let tokens = std::cell::Cell::new(1_000.0f64);
        move |_: usize, runs: f64, pages: f64, _: &mut Rng| {
            let free = runs.min(tokens.get());
            tokens.set((tokens.get() - free + 600.0).min(1_000.0));
            1e6 + (pages * 10_000.0).max(5_000.0 * free + 300_000.0 * (runs - free))
        }
    };
    let none = |_: usize, _: f64, _: f64| 0.0;
    for seed in [0x510e_527f_ade6_82d1, 0x9b05_688c_2b3e_6c1f, 0x1f83_d9ab_fb41_bd6b] {
        let run = |fixed| simulate_with(seed, 600, (970, 60), fixed, FILL_NS, &bucket(), &none);
        let (steps, readapts, model) = run(None);
        let fixed: Vec<(u32, f64)> =
            CANDIDATES.iter().map(|&w| (w, mean_time(&run(Some(w)).0, 100))).collect();
        let (best, best_time) =
            fixed.iter().copied().min_by(|a, b| a.1.total_cmp(&b.1)).expect("widths");
        let longest = longest_run_at_zero(&steps);
        let excess = mean_time(&steps, 100) / best_time;
        let widths: Vec<u32> = steps.iter().map(|s| s.width).collect();
        let case = format!("seed {seed:#x}: fixed {fixed:?}, {model:?} {widths:?}");
        assert_eq!(best, 4, "{case}");
        assert!(longest <= 1, "{case}: {longest} steps at 0 in a row");
        assert!(excess < 1.1, "{case}: {excess}x the best fixed width's time");
        assert_eq!(readapts, 0, "{case}");
    }
}

/// A disk whose burst allowance of writes drains within a step or two: a bucket of 700 writes, 560
/// more each step, the rest as in `a_disk_with_a_refilled_burst_of_writes_never_stays_at_no_fill`.
/// Held, 8 writes about 549 runs a step, within the refill, and costs about 31 ms; 4 writes about
/// 670 and costs about 38 ms. But the first step at 4 after steps at 8 is within the bucket and
/// costs about 23 ms, so every probe at 4 looks cheaper than 8. The choice tries 4 once its probes
/// say so, holds it, finds it dearer and returns; then it judges 4 by those held steps, not by the
/// cheap probes that follow, and never tries it again: over 2,000 steps two moves, and the steps'
/// time within 1% of 8's.
#[test]
fn a_width_whose_probes_look_cheaper_than_it_holds_is_tried_once() {
    let bucket = || {
        let tokens = std::cell::Cell::new(700.0f64);
        move |_: usize, runs: f64, pages: f64, _: &mut Rng| {
            let free = runs.min(tokens.get());
            tokens.set((tokens.get() - free + 560.0).min(700.0));
            1e6 + (pages * 10_000.0).max(5_000.0 * free + 300_000.0 * (runs - free))
        }
    };
    let none = |_: usize, _: f64, _: f64| 0.0;
    for seed in [0x510e_527f_ade6_82d1, 0x9b05_688c_2b3e_6c1f, 0x1f83_d9ab_fb41_bd6b] {
        let run = |fixed| simulate_with(seed, 2000, (970, 60), fixed, FILL_NS, &bucket(), &none);
        let (steps, readapts, model) = run(None);
        let at_8 = mean_time(&run(Some(8)).0, 100);
        let at_4 = mean_time(&run(Some(4)).0, 100);
        let moves = moves(&steps);
        let excess = mean_time(&steps, 100) / at_8;
        let case = format!("seed {seed:#x}: 8 {at_8}, 4 {at_4}, {model:?}, moves {moves:?}");
        assert!(at_4 > 1.15 * at_8, "{case}: 4 held is dearer");
        assert!(moves.len() <= 2, "{case}: {} moves", moves.len());
        assert!(excess < 1.01, "{case}: {excess}x 8's time");
        assert_eq!(readapts, 0, "{case}");
    }
}

/// The shift detector judges a step by its width's corrected prediction: steps at a width that
/// steadily costs twice the model (its correction 2) are no change, however long they run, while
/// [`SHIFT_STREAK`] steps in a row at 1.75 times that (3.5 times the model) are.
#[test]
fn the_shift_detector_judges_a_step_by_its_widths_correction() {
    let mut auto = AutoGap { warm: 0, ..AutoGap::default() };
    auto.model = Model { per_page: 1_000.0, ..Model::default() };
    let at = |ns_per_page: f64| Sample { runs: 100.0, pages: 1_000.0, ns: ns_per_page * 1_000.0 };
    for _ in 0..PER_WIDTH {
        auto.kept[slot(4)].push_back(Kept { sample: at(2_000.0), held: true });
    }
    assert_eq!(auto.correction(slot(4)), 2.0);
    assert!((0..3 * SHIFT_STREAK).all(|_| !auto.shifted(at(2_000.0), slot(4))), "no change");
    let streak: Vec<bool> = (0..SHIFT_STREAK).map(|_| auto.shifted(at(3_500.0), slot(4))).collect();
    assert_eq!(streak.iter().filter(|&&s| s).count(), 1, "{streak:?}");
    assert!(streak[SHIFT_STREAK as usize - 1], "the streak's last step: {streak:?}");
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
    // all of 2 pages, so 2 beside 0 (the smallest of the widths that tie), measured by a step
    // there. Taken at the first step, it puts the next probe 16 steps on, then 32; the probes
    // alternate twice and half of it.
    auto.model = Model { per_run: 1e6, ..Model::default() };
    let sample = Sample { runs: 1.0, pages: 800.0, ns: 1e6 };
    auto.kept[slot(2)].push_back(Kept { sample, held: false });
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
    let groups: Vec<u8> = (0..12).map(|i| i % 3).collect();
    let m = fit(&samples, &groups);
    assert!((m.fixed - 800_000.0).abs() < 1.0, "{m:?}");
    assert!((m.per_run - 40_000.0).abs() < 0.01 && (m.per_page - 2_000.0).abs() < 0.01, "{m:?}");
    samples[5].ns *= 10.0;
    let m = fit(&samples, &groups);
    assert!((m.per_run - 40_000.0).abs() < 400.0, "a stall moves it little: {m:?}");
    assert!((m.per_page - 2_000.0).abs() < 100.0, "{m:?}");
    let faster: Vec<Sample> = (0..8)
        .map(|i| {
            let runs = 100.0 + 50.0 * f64::from(i);
            let pages = 1_000.0 + 10.0 * f64::from(i % 3);
            sample(runs, pages, 5e6 - 1_000.0 * runs + 1_000.0 * pages)
        })
        .collect();
    let m = fit(&faster, &groups[..8]);
    assert!(m.fixed >= 0.0 && m.per_run >= 0.0 && m.per_page >= 0.0, "{m:?}");
    assert_eq!(m.per_run, 0.0, "{m:?}");
}

/// A capped disk's exact samples at three widths, two on the write cap and three probes on the
/// byte cap (two widths alone a sum fits as well): the fit takes the capped form and recovers
/// both caps. The sum fits the twelve samples of the first two widths well and misses the three
/// probes, which the error over the widths counts as one width of three (a median of all the
/// residuals would not: it chose the sum here).
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
    let groups: Vec<u8> =
        (0..15).map(|i| [3, 4, 5][usize::from(i >= 8) + usize::from(i >= 12)]).collect();
    let m = fit(&samples, &groups);
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

/// The cheapest width is one of those given, whatever their costs: a NaN cost counts as infinite
/// (else a choice of nothing but NaN costs would be `u32::MAX`, a width past `MAX_GAP_FILL`), and
/// a tie goes to the smaller width.
#[test]
fn the_cheapest_width_is_always_one_given() {
    assert_eq!(cheapest(CANDIDATES.into_iter(), |_| f64::NAN), (0, f64::INFINITY));
    assert_eq!(cheapest(CANDIDATES.into_iter(), |_| f64::INFINITY), (0, f64::INFINITY));
    let cost = |width: u32| if width == 0 { f64::NAN } else { 1.0 + f64::from(width) };
    assert_eq!(cheapest(CANDIDATES.into_iter(), cost), (1, 2.0));
    assert_eq!(cheapest(beside(MAX_GAP_FILL).into_iter().flatten(), |_| f64::NAN).0, 32);
}
