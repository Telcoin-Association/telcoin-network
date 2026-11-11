//! Gap fill's automatic width ([`GapFill::Auto`](crate::GapFill::Auto)): for each scattered
//! durable step, the widest gap of clean pages step 4 fills, chosen from what the disk's syncs
//! were measured to cost (README, "Gap fill", "Automatic width").
//!
//! **The cost model.** A data sync costs a write per run of adjacent dirty pages (each gap filled
//! saves one) and its pages' bytes (a gap filled adds its pages), over a fixed cost per sync
//! (`c`). Two forms combine them, and the fit keeps the one the samples fit ([`fit`]):
//! - the sum, `c + a·runs + b·pages`: a drive that pays for each write and each byte, such as a
//!   local NVMe drive, whose `a` is small beside its `b` and the fill's own cost;
//! - the capped form, `c + max(a·runs, b·pages)`: a disk capped on writes per second (`a` the time
//!   per write at the cap) and on bytes per second (`b` per page), whichever binds, such as network
//!   block storage. Its best width balances the two caps, which the sum, a plane, can only
//!   straddle: fitted to such a disk it swings between widths on either side.
//!
//! The capped form is kept only when its residuals are markedly smaller (their median under
//! [`CAPPED_GAIN`] times the sum's), so the sum serves wherever the two fit alike. The fill costs
//! the writer `f` per gap it tries (its read and its write, timed), and fills a share `h` of them
//! (the others are not in memory). [`AutoGap::observe`] fits `c`, `a` and `b` (each at least 0) to
//! the recent steps' runs written, pages written and sync time, robustly (least squares over the
//! seven supports of the three, reweighted by Huber's and then Tukey's weights of the residuals,
//! so a sync the drive stalled on, or a latency spike of a cloud disk, moves the fit little), and
//! keeps `f` and `h` as running means.
//!
//! **The choice.** A step's counts at each candidate width `g` ([`CANDIDATES`]) follow from its
//! gap sizes ([`Gaps`], one pass over its pages; the candidates' counts in one pass over the
//! sizes): `gaps(g)`, the gaps of at most `g` clean pages, and `gap_pages(g)`, their pages. Step 4
//! is predicted to take the model's time for `runs − h·gaps(g)` runs of `pages + h·gap_pages(g)`
//! pages, plus `f·gaps(g)`, and the width is the candidate that predicts the least, the smaller one
//! on a tie.
//!
//! **Exploration.** At one width a step's runs and pages move together (their ratio is the
//! workload's), so steps at one width cannot tell `a` from `b`. A warm-up of [`WARMUP_STEPS`]
//! steps cycles the widths of [`WARMUP`] (0, 4 and 8); after it, a step is a probe at another
//! width than the choice, half of it or twice it (at least 2), alternately (only the one that
//! exists at 0 and at [`MAX_GAP_FILL`]). Probes start [`PROBE_EVERY`] steps apart, and the
//! interval doubles after each, up to [`PROBE_EVERY_MAX`], while the choice holds: a new choice
//! brings it back to [`PROBE_EVERY`]. So a disk whose choice moves is explored often, and one
//! that settles (a fast drive at 0, where a probe's fill costs about half that step's sync on the
//! Mac) seldom. The fit keeps the latest [`BASE_WINDOW`] steps at the chosen width and, apart,
//! the latest [`PROBE_WINDOW`] probes, so the probes keep their weight in the fit without a high
//! probe rate.
//!
//! **Re-adaptation.** The windows forget a disk's old costs as new steps arrive. A disk whose
//! costs change at once (a cloud disk's limits raised, a drive that changed speed) is seen
//! sooner: [`SHIFT_STREAK`] steps in a row whose sync took more than [`SHIFT_FACTOR`] times the
//! prediction, or less than its inverse, the same way, empty both windows (but that step) and
//! start the warm-up again (`IoStats::gap_auto_readapts`). Each open starts cold: the model lives
//! in memory only.
//!
//! **Safety.** The width only says which clean gaps step 4 rewrites with the bytes they hold, so
//! no choice, good or bad, changes a byte of the file or anything a reader sees: only how long
//! step 4 takes. The fill's own guards (a gap read only from memory, the miss slack) hold at every
//! width.

use std::collections::VecDeque;

use super::MAX_GAP_FILL;

/// The widths [`AutoGap`] chooses among, ascending: 0 (no fill) and each power of two up to
/// [`MAX_GAP_FILL`]. Halving or doubling one gives another (or 0), which the probes rely on.
pub(crate) const CANDIDATES: [u32; 8] = [0, 1, 2, 4, 8, 16, 32, 64];

const _: () = assert!(CANDIDATES[CANDIDATES.len() - 1] == MAX_GAP_FILL, "the widest candidate");

/// The widths a warm-up cycles through, from its first step: 8 first, so the first scattered
/// step after an open fills (the crash matrices at auto rely on it), then 0 (no fill) and 4.
const WARMUP: [u32; 3] = [8, 0, 4];

/// The steps of a warm-up, at an open and after a re-adaptation: each width of [`WARMUP`] four
/// times, the least that fits `c`, `a` and `b` with a spare step at each width.
const WARMUP_STEPS: u32 = 12;

/// The steps from the warm-up's end, or from a new choice, to the next probe: the shortest
/// interval between probes.
const PROBE_EVERY: u64 = 16;

/// The longest interval between probes, which the interval doubles to while the choice holds. On
/// the Mac's SSD, where the choice is 0, a probe's fill costs about half its step's sync: a 128th
/// of that over all steps once settled.
const PROBE_EVERY_MAX: u64 = 128;

/// The steps at the chosen width the fit keeps: about a minute of a node's commits.
const BASE_WINDOW: usize = 48;

/// The probes the fit keeps: from [`PROBE_WINDOW`] · [`PROBE_EVERY`] steps' worth to
/// [`PROBE_WINDOW`] · [`PROBE_EVERY_MAX`].
const PROBE_WINDOW: usize = 16;

/// Steps in a row off the prediction by more than [`SHIFT_FACTOR`], the same way, that restart
/// the warm-up.
const SHIFT_STREAK: u32 = 8;

/// How far a step's sync time must be off the prediction, as a factor either way, to count
/// towards [`SHIFT_STREAK`].
const SHIFT_FACTOR: f64 = 1.5;

/// The fill's cost per gap tried, in nanoseconds, until the first fill is timed: about what it was
/// measured at (4.7 µs on the Mac).
const FILL_PRIOR_NS: f64 = 5_000.0;

/// The weight of one step's measurement in the running fill cost and hit share.
const EWMA: f64 = 1.0 / 8.0;

/// Huber's constant: a residual within this many robust standard deviations keeps its full weight
/// in the fit, a larger one is weighted down in proportion.
const HUBER: f64 = 1.345;

/// The fit's Huber rounds after its first, unweighted one.
const IRLS_ROUNDS: usize = 3;

/// The fewest samples the fit runs on.
const MIN_SAMPLES: usize = 4;

/// The gap sizes of one durable step's pages (ascending, distinct): its runs of adjacent pages,
/// its pages, and the gaps of each size from 1 to [`MAX_GAP_FILL`] clean pages between two runs.
#[derive(Debug)]
pub(crate) struct Gaps {
    /// Runs of adjacent pages.
    pub(crate) runs: u64,
    /// Pages.
    pub(crate) pages: u64,
    /// `sizes[s]`: the gaps of `s` clean pages, `s` in `1..=MAX_GAP_FILL`; `sizes[0]` counts
    /// adjacent pages and the last entry wider gaps, neither of which a width fills.
    sizes: [u32; MAX_GAP_FILL as usize + 2],
}

impl Gaps {
    /// The gap sizes of `pages` (ascending, distinct), in one pass.
    pub(crate) fn of(pages: &[u32]) -> Gaps {
        let mut sizes = [0u32; MAX_GAP_FILL as usize + 2];
        let mut runs = u64::from(!pages.is_empty());
        for w in pages.windows(2) {
            let clean = w[1] - w[0] - 1;
            runs += u64::from(clean > 0);
            sizes[clean.min(MAX_GAP_FILL + 1) as usize] += 1;
        }
        Gaps { runs, pages: pages.len() as u64, sizes }
    }

    /// For each of [`CANDIDATES`]: the gaps of at most its width, and their pages. One pass over
    /// the sizes.
    pub(crate) fn upto(&self) -> [(u64, u64); CANDIDATES.len()] {
        let mut out = [(0, 0); CANDIDATES.len()];
        let (mut gaps, mut pages, mut size) = (0u64, 0u64, 1usize);
        for (slot, &width) in out.iter_mut().zip(&CANDIDATES) {
            while size <= width as usize {
                let n = u64::from(self.sizes[size]);
                gaps += n;
                pages += n * size as u64;
                size += 1;
            }
            *slot = (gaps, pages);
        }
        out
    }
}

/// One step's sync, for the fit: what it wrote, and how long the sync took.
#[derive(Clone, Copy, Debug)]
struct Sample {
    /// Runs written: the step's runs less the gaps filled.
    runs: f64,
    /// Pages written: the step's pages and the filled gaps' pages.
    pages: f64,
    /// The data sync's time, ns.
    ns: f64,
}

/// A data sync's fitted cost, in nanoseconds: `fixed + per_run · runs + per_page · pages`, or,
/// [`Self::capped`], `fixed + max(per_run · runs, per_page · pages)`; each term at least 0.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Model {
    /// `c`: per sync.
    pub(crate) fixed: f64,
    /// `a`: per run written.
    pub(crate) per_run: f64,
    /// `b`: per page written.
    pub(crate) per_page: f64,
    /// Whether the run and page terms combine by their maximum, not their sum: the samples fit a
    /// disk capped on writes and on bytes per second, whichever binds, markedly better.
    pub(crate) capped: bool,
}

impl Model {
    /// The predicted time of a sync that writes `runs` runs of `pages` pages.
    fn predict(&self, runs: f64, pages: f64) -> f64 {
        let (runs, pages) = (self.per_run * runs, self.per_page * pages);
        self.fixed + if self.capped { runs.max(pages) } else { runs + pages }
    }
}

/// [`AutoGap::choose`]'s answer for one step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Choice {
    /// The widest gap to fill, one of [`CANDIDATES`].
    pub(crate) width: u32,
    /// Whether the width differs from the model's choice: a warm-up step at another width, or a
    /// probe.
    pub(crate) probe: bool,
}

/// What one step's fill and sync did, for [`AutoGap::observe`].
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Outcome {
    /// Runs written: the step's runs less the gaps filled.
    pub(crate) runs: u64,
    /// Pages written: the step's pages and the filled gaps' pages.
    pub(crate) pages: u64,
    /// The data sync's time, ns.
    pub(crate) sync_ns: u64,
    /// The fill's time, ns: its reads and writes.
    pub(crate) fill_ns: u64,
    /// Gaps the fill tried: filled or found not in memory.
    pub(crate) tried: u64,
    /// Gaps it filled.
    pub(crate) filled: u64,
}

/// The automatic width's state (module docs): the samples, the fitted model, the fill's running
/// cost and hit share, and the schedule.
#[derive(Debug)]
pub(crate) struct AutoGap {
    /// The latest steps at the model's choice (warm-up steps at 0 too), oldest first.
    base: VecDeque<Sample>,
    /// The latest steps at another width, oldest first.
    probes: VecDeque<Sample>,
    /// The fit of [`Self::base`] and [`Self::probes`]; zero until they hold [`MIN_SAMPLES`].
    model: Model,
    /// The fill's time per gap tried, ns: [`FILL_PRIOR_NS`] until a fill is timed.
    fill_ns: f64,
    /// Whether [`Self::fill_ns`] holds a measurement.
    fill_timed: bool,
    /// The share of the gaps tried that were filled.
    hit: f64,
    /// Warm-up steps left.
    warm: u32,
    /// Steps since the warm-up ended: the probe schedule's clock.
    steps: u64,
    /// The interval to the probe after the next ([`PROBE_EVERY`] to [`PROBE_EVERY_MAX`]).
    interval: u64,
    /// The step ([`Self::steps`]) of the next probe.
    next_probe: u64,
    /// The model's choice at the last step past the warm-up.
    last_best: u32,
    /// Whether the next probe that has two widths to pick from takes the larger.
    higher: bool,
    /// Steps in a row off the prediction past [`SHIFT_FACTOR`]: positive slower, negative faster.
    streak: i32,
    /// The last [`Self::choose`]'s answer, which [`Self::observe`] files its sample under.
    pending: Option<Choice>,
}

impl Default for AutoGap {
    fn default() -> Self {
        AutoGap {
            base: VecDeque::with_capacity(BASE_WINDOW + 1),
            probes: VecDeque::with_capacity(PROBE_WINDOW + 1),
            model: Model::default(),
            fill_ns: FILL_PRIOR_NS,
            fill_timed: false,
            hit: 1.0,
            warm: WARMUP_STEPS,
            steps: 0,
            interval: PROBE_EVERY,
            next_probe: PROBE_EVERY,
            last_best: 0,
            higher: true,
            streak: 0,
            pending: None,
        }
    }
}

impl AutoGap {
    /// The width for a step whose gap sizes are `gaps`: the warm-up's next width during a
    /// warm-up, else the model's choice, but a probe's width at the probe schedule's steps
    /// (module docs, "Exploration").
    pub(crate) fn choose(&mut self, gaps: &Gaps) -> Choice {
        let best = if self.warm == 0 { self.best(gaps) } else { 0 };
        let width = if self.warm > 0 {
            let done = (WARMUP_STEPS - self.warm) as usize;
            self.warm -= 1;
            WARMUP[done % WARMUP.len()]
        } else {
            self.steps += 1;
            if best != self.last_best {
                // A new choice: explore around it soon.
                self.last_best = best;
                self.interval = PROBE_EVERY;
                self.next_probe = self.next_probe.min(self.steps + PROBE_EVERY);
            }
            if self.steps >= self.next_probe {
                self.interval = (2 * self.interval).min(PROBE_EVERY_MAX);
                self.next_probe = self.steps + self.interval;
                self.probe(best)
            } else {
                best
            }
        };
        let choice = Choice { width, probe: width != best };
        self.pending = Some(choice);
        choice
    }

    /// File the outcome of the step [`Self::choose`] last answered for: update the fill's cost
    /// and hit share, watch for a change of the disk's costs, and refit. Returns whether the step
    /// restarted the warm-up (module docs, "Re-adaptation").
    pub(crate) fn observe(&mut self, outcome: Outcome) -> bool {
        let Some(choice) = self.pending.take() else { return false };
        if outcome.tried > 0 {
            let per_gap = outcome.fill_ns as f64 / outcome.tried as f64;
            let hit = outcome.filled as f64 / outcome.tried as f64;
            if self.fill_timed {
                self.fill_ns += EWMA * (per_gap - self.fill_ns);
                self.hit += EWMA * (hit - self.hit);
            } else {
                (self.fill_ns, self.hit, self.fill_timed) = (per_gap, hit, true);
            }
        }
        let sample = Sample {
            runs: outcome.runs as f64,
            pages: outcome.pages as f64,
            ns: outcome.sync_ns as f64,
        };
        let readapt = self.warm == 0 && self.shifted(sample);
        if readapt {
            self.base.clear();
            self.probes.clear();
            (self.warm, self.steps, self.streak) = (WARMUP_STEPS, 0, 0);
            (self.interval, self.next_probe) = (PROBE_EVERY, PROBE_EVERY);
        }
        let (window, cap) = if choice.probe {
            (&mut self.probes, PROBE_WINDOW)
        } else {
            (&mut self.base, BASE_WINDOW)
        };
        window.push_back(sample);
        if window.len() > cap {
            window.pop_front();
        }
        self.refit();
        readapt
    }

    /// The fitted model.
    pub(crate) fn model(&self) -> Model {
        self.model
    }

    /// The fill's time per gap tried, ns.
    pub(crate) fn fill_cost_ns(&self) -> f64 {
        self.fill_ns
    }

    /// The candidate that predicts the least time for step 4, fill and sync, of a step with
    /// `gaps`; the smaller on a tie.
    fn best(&self, gaps: &Gaps) -> u32 {
        let (runs, pages) = (gaps.runs as f64, gaps.pages as f64);
        let mut best = (0, f64::INFINITY);
        for (&width, &(n, gap_pages)) in CANDIDATES.iter().zip(&gaps.upto()) {
            let (n, gap_pages) = (n as f64, gap_pages as f64);
            let cost = self.model.predict(runs - self.hit * n, pages + self.hit * gap_pages)
                + self.fill_ns * n;
            if cost < best.1 {
                best = (width, cost);
            }
        }
        best.0
    }

    /// A probe's width beside the choice `best`: half of it or twice it (at least 2),
    /// alternately, or the one of the two that exists.
    fn probe(&mut self, best: u32) -> u32 {
        let lower = (best >= 1).then_some(best / 2);
        let higher = (best < MAX_GAP_FILL).then_some((2 * best).max(2));
        self.higher = !self.higher;
        match (lower, higher) {
            (Some(lower), Some(higher)) => {
                if self.higher {
                    higher
                } else {
                    lower
                }
            }
            (Some(width), None) | (None, Some(width)) => width,
            (None, None) => unreachable!("every candidate has a neighbour"),
        }
    }

    /// Whether `sample` ends a streak of [`SHIFT_STREAK`] steps off the prediction the same way.
    fn shifted(&mut self, sample: Sample) -> bool {
        let predicted = self.model.predict(sample.runs, sample.pages).max(1.0);
        let ratio = sample.ns / predicted;
        let way = if ratio > SHIFT_FACTOR {
            1
        } else if ratio < 1.0 / SHIFT_FACTOR {
            -1
        } else {
            0
        };
        self.streak = if way != 0 && self.streak.signum() == way { self.streak + way } else { way };
        self.streak.unsigned_abs() >= SHIFT_STREAK
    }

    /// Fit the model to the windows' samples, once they hold [`MIN_SAMPLES`].
    fn refit(&mut self) {
        let mut samples = [Sample { runs: 0.0, pages: 0.0, ns: 0.0 }; MAX_SAMPLES];
        let mut n = 0;
        for (slot, s) in samples.iter_mut().zip(self.base.iter().chain(&self.probes)) {
            *slot = *s;
            n += 1;
        }
        self.model = if n >= MIN_SAMPLES { fit(&samples[..n]) } else { Model::default() };
    }
}

/// The data sync's cost model fitted to `samples` (module docs): each form's robust fit
/// ([`fit_form`]), and the capped one only when its trimmed error is under [`CAPPED_GAIN`] times
/// the sum's (the sum fits a capped disk's samples on one side of its cap as well, and is the one
/// to keep when the two fit alike).
fn fit(samples: &[Sample]) -> Model {
    let n = samples.len().min(MAX_SAMPLES);
    let mut data = Data::default();
    for (i, s) in samples.iter().take(n).enumerate() {
        data.runs[i] = s.runs / 1e3;
        data.pages[i] = s.pages / 1e3;
        data.ys[i] = s.ns / 1e6;
    }
    data.n = n;
    let mut ys = data.ys;
    let typical = median(&mut ys[..n]);
    let (sum, sum_err) = fit_form(&data, Form::Sum, typical);
    let (capped_beta, capped_err) = fit_form(&data, Form::Capped, typical);
    // Two fits within a thousandth of a typical sync of exact are alike.
    let floor = typical / 1e3;
    let capped = capped_err + floor < CAPPED_GAIN * (sum_err + floor);
    let [fixed, per_run, per_page] = if capped { capped_beta } else { sum };
    Model { fixed: fixed * 1e6, per_run: per_run * 1e3, per_page: per_page * 1e3, capped }
}

/// The median of `xs` (reordered), 0 for none.
fn median(xs: &mut [f64]) -> f64 {
    let n = xs.len();
    if n == 0 {
        return 0.0;
    }
    xs.sort_unstable_by(f64::total_cmp);
    if n % 2 == 1 {
        xs[n / 2]
    } else {
        (xs[n / 2 - 1] + xs[n / 2]) / 2.0
    }
}

/// The most samples a fit takes: both windows.
const MAX_SAMPLES: usize = BASE_WINDOW + PROBE_WINDOW;

/// The capped form is kept when its trimmed error ([`TRIM`]) is under this share of the sum's.
const CAPPED_GAIN: f64 = 0.7;

/// The share of a fit's samples its trimmed error counts, the closest first: the root mean
/// square of their residuals. It leaves out the stalls, but not the probes (a quarter of the
/// samples), which a form that misfits them could otherwise wave off as outliers: a median would
/// not see a sum that fits the chosen width's samples, most of the window, and misses the rest.
const TRIM: f64 = 0.9;

/// Tukey's bisquare constant: a residual past this many robust standard deviations gets no
/// weight in the fit's last rounds.
const BISQUARE: f64 = 4.685;

/// The bisquare rounds after the Huber rounds.
const BISQUARE_ROUNDS: usize = 2;

/// A fit's samples, scaled: runs and pages in thousands, times in milliseconds, so the normal
/// equations stay well conditioned whatever the steps' sizes.
#[derive(Debug)]
struct Data {
    runs: [f64; MAX_SAMPLES],
    pages: [f64; MAX_SAMPLES],
    ys: [f64; MAX_SAMPLES],
    n: usize,
}

impl Default for Data {
    fn default() -> Self {
        Data { runs: [0.0; MAX_SAMPLES], pages: [0.0; MAX_SAMPLES], ys: [0.0; MAX_SAMPLES], n: 0 }
    }
}

/// The two forms of the cost model ([`Model::capped`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Form {
    /// `c + a·runs + b·pages`: a drive that pays for each write and each byte (local NVMe, and the
    /// default).
    Sum,
    /// `c + max(a·runs, b·pages)`: a disk capped on writes per second and on bytes per second,
    /// whichever binds (network block storage).
    Capped,
}

/// The prediction of `beta` (`[c, a, b]`, scaled) in `form` for sample `i` of `data`.
fn predict_form(form: Form, beta: &[f64; 3], data: &Data, i: usize) -> f64 {
    let (runs, pages) = (beta[1] * data.runs[i], beta[2] * data.pages[i]);
    match form {
        Form::Sum => beta[0] + runs + pages,
        Form::Capped => beta[0] + runs.max(pages),
    }
}

/// `form`'s coefficients fitted to `data` (scaled, each at least 0), and the fit's trimmed error
/// ([`TRIM`]): one unweighted fit ([`solve_form`]), [`IRLS_ROUNDS`] reweighted by Huber's
/// weights of the residuals, then [`BISQUARE_ROUNDS`] by Tukey's bisquare, which gives a stall far
/// off the rest no weight at all (skipped when it would leave under [`MIN_SAMPLES`] weighted).
/// Each round's scale is the residuals' median absolute value times 1.4826 (a normal
/// deviation's), but at least a hundredth of `typical`, the samples' median time: a fit closer
/// than that is exact, and no sample of it loses weight (a probe the fit matches to rounding
/// must keep its say).
fn fit_form(data: &Data, form: Form, typical: f64) -> ([f64; 3], f64) {
    let n = data.n;
    let mut weights = [1.0f64; MAX_SAMPLES];
    let mut beta = solve_form(data, form, &weights, None);
    let mut abs = [0.0f64; MAX_SAMPLES];
    let median_abs = |beta: &[f64; 3], abs: &mut [f64; MAX_SAMPLES]| {
        for (i, r) in abs.iter_mut().enumerate().take(n) {
            *r = (data.ys[i] - predict_form(form, beta, data, i)).abs();
        }
        let mut sorted = *abs;
        median(&mut sorted[..n])
    };
    for round in 0..IRLS_ROUNDS + BISQUARE_ROUNDS {
        let scale = (1.4826 * median_abs(&beta, &mut abs)).max(typical / 100.0);
        if scale <= f64::EPSILON {
            break;
        }
        let mut next = [0.0f64; MAX_SAMPLES];
        for (w, r) in next.iter_mut().zip(&abs[..n]) {
            *w = if round < IRLS_ROUNDS {
                if *r <= HUBER * scale {
                    1.0
                } else {
                    HUBER * scale / r
                }
            } else {
                let u = r / (BISQUARE * scale);
                if u < 1.0 {
                    (1.0 - u * u).powi(2)
                } else {
                    0.0
                }
            };
        }
        if next[..n] == weights[..n] {
            continue; // the same weights give the same fit
        }
        if next[..n].iter().filter(|&&w| w > 0.0).count() < MIN_SAMPLES {
            break;
        }
        weights = next;
        beta = solve_form(data, form, &weights, Some(&beta));
    }
    median_abs(&beta, &mut abs);
    let mut squares = [0.0f64; MAX_SAMPLES];
    for (sq, r) in squares.iter_mut().zip(&abs[..n]) {
        *sq = r * r;
    }
    let squares = &mut squares[..n];
    squares.sort_unstable_by(f64::total_cmp);
    let kept = ((n as f64 * TRIM).ceil() as usize).clamp(1, n.max(1));
    let trimmed = (squares[..kept.min(n)].iter().sum::<f64>() / kept as f64).sqrt();
    (beta, trimmed)
}

/// `form`'s weighted non-negative least squares on `data`. The sum: [`nnls`] on `[1, runs,
/// pages]`. The capped form: each sample pays the run term `a·runs` or the page term `b·pages`.
/// Given `last`, an earlier fit, each pays the larger of its two under it (a reweighting round's
/// refit). Without one, the samples are ordered by runs per page and each split of that order
/// tried (those before it on the run term, those after on the page term, the normal equations
/// updated by one sample a split), and the split whose [`nnls`] fit has the least weighted
/// squared error kept.
fn solve_form(
    data: &Data,
    form: Form,
    weights: &[f64; MAX_SAMPLES],
    last: Option<&[f64; 3]>,
) -> [f64; 3] {
    let n = data.n;
    let mut gram = [[0.0f64; 3]; 3];
    let mut moment = [0.0f64; 3];
    let mut yy = 0.0;
    // Each sample's row: the sum's, or the capped form's term under `last` (the page term when
    // there is none: the split scan's start).
    let row = |i: usize| {
        let (runs, pages) = (data.runs[i], data.pages[i]);
        match (form, last) {
            (Form::Sum, _) => [1.0, runs, pages],
            (Form::Capped, Some(last)) if last[1] * runs >= last[2] * pages => [1.0, runs, 0.0],
            (Form::Capped, _) => [1.0, 0.0, pages],
        }
    };
    for (i, &w) in weights.iter().enumerate().take(n) {
        add(&mut gram, &mut moment, row(i), data.ys[i], w);
        yy += w * data.ys[i] * data.ys[i];
    }
    match (form, last) {
        (Form::Sum, _) | (Form::Capped, Some(_)) => nnls(&gram, &moment, yy).0,
        (Form::Capped, None) => {
            let mut order = [0usize; MAX_SAMPLES];
            for (i, o) in order.iter_mut().enumerate().take(n) {
                *o = i;
            }
            let ratio = |i: usize| data.runs[i] / data.pages[i].max(f64::MIN_POSITIVE);
            order[..n].sort_unstable_by(|&p, &q| ratio(q).total_cmp(&ratio(p)));
            // Every sample on the page term first, then one moved to the run term a split.
            let mut best = nnls(&gram, &moment, yy);
            for &i in &order[..n] {
                let (y, w) = (data.ys[i], weights[i]);
                add(&mut gram, &mut moment, [1.0, 0.0, data.pages[i]], y, -w);
                add(&mut gram, &mut moment, [1.0, data.runs[i], 0.0], y, w);
                let fit = nnls(&gram, &moment, yy);
                if fit.1 < best.1 {
                    best = fit;
                }
            }
            best.0
        }
    }
}

/// Add row `x` with time `y` at weight `w` (negative: take it out) to the normal equations.
fn add(gram: &mut [[f64; 3]; 3], moment: &mut [f64; 3], x: [f64; 3], y: f64, w: f64) {
    for j in 0..3 {
        for k in 0..3 {
            gram[j][k] += w * x[j] * x[k];
        }
        moment[j] += w * x[j] * y;
    }
}

/// The non-negative least-squares coefficients from the normal equations `gram · β = moment`
/// (with `yy`, the weighted sum of the squared times), and their weighted squared error: of the
/// seven supports of the three coefficients, the solution on the support whose coefficients are
/// all non-negative and whose error is least (exact non-negative least squares for three
/// unknowns; the full support alone when its solution is non-negative). All zero, with the error
/// `yy`, if no support solves.
fn nnls(gram: &[[f64; 3]; 3], moment: &[f64; 3], yy: f64) -> ([f64; 3], f64) {
    // The weighted squared error, from the normal equations: Σw·y² − 2·β·m + βᵀ·G·β.
    let err = |beta: &[f64; 3]| {
        let mut err = yy - 2.0 * dot(beta, moment);
        for j in 0..3 {
            err += beta[j] * dot(&gram[j], beta);
        }
        err
    };
    // The unconstrained minimum, when it is non-negative, is the constrained one too.
    const ALL: u8 = 0b111;
    if let Some(beta) = solve_on(gram, moment, ALL).filter(|b| b.iter().all(|&b| b >= 0.0)) {
        return (beta, err(&beta));
    }
    let mut best = ([0.0; 3], yy);
    for support in 1..ALL {
        let Some(beta) = solve_on(gram, moment, support) else { continue };
        if beta.iter().any(|&b| b < 0.0) {
            continue;
        }
        let err = err(&beta);
        if err < best.1 {
            best = (beta, err);
        }
    }
    best
}

/// The solution of the normal equations `gram · β = moment` restricted to the coefficients of
/// `support` (a bit mask; the others 0), by Gaussian elimination with partial pivoting; `None`
/// when the restricted system is singular.
fn solve_on(gram: &[[f64; 3]; 3], moment: &[f64; 3], support: u8) -> Option<[f64; 3]> {
    let mut idx = [0usize; 3];
    let mut k = 0;
    for j in 0..3 {
        if support & (1 << j) != 0 {
            idx[k] = j;
            k += 1;
        }
    }
    let idx = &idx[..k];
    let mut a = [[0.0f64; 4]; 3];
    let mut scale = 0.0f64;
    for (r, &j) in idx.iter().enumerate() {
        for (c, &l) in idx.iter().enumerate() {
            a[r][c] = gram[j][l];
        }
        a[r][k] = moment[j];
        scale = scale.max(gram[j][j].abs());
    }
    if scale == 0.0 {
        return None;
    }
    for col in 0..k {
        let pivot = (col..k).max_by(|&p, &q| a[p][col].abs().total_cmp(&a[q][col].abs()))?;
        if a[pivot][col].abs() <= 1e-12 * scale {
            return None;
        }
        a.swap(col, pivot);
        for r in 0..k {
            if r != col {
                let factor = a[r][col] / a[col][col];
                let pivot_row = a[col];
                for (x, p) in a[r][col..=k].iter_mut().zip(&pivot_row[col..=k]) {
                    *x -= factor * p;
                }
            }
        }
    }
    let mut beta = [0.0f64; 3];
    for (r, &j) in idx.iter().enumerate() {
        beta[j] = a[r][k] / a[r][r];
    }
    beta.iter().all(|b| b.is_finite()).then_some(beta)
}

/// `a · b`.
fn dot(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

#[cfg(test)]
mod tests;
