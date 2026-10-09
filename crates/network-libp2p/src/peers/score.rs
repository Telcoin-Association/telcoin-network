//! Scores for peer ranking.
//!
//! Peer scores are rational numbers in the range [-100, 100].
//! This is an experimental approach and is subject to change.
//!
//! Heavily inspired by Sigma Prime Lighthouse's scoring system.

use super::penalty::{Penalty, Severity};
use serde::Serialize;
use std::{fmt::Display, sync::Arc, time::Instant};
use tn_config::ScoreConfig;

/// A peer's score (perceived potential usefulness).
///
/// This simplistic version consists of a global score per peer which decays to 0 over time. The
/// decay rate applies equally to positive and negative scores.
#[derive(Clone, Debug, Serialize)]
pub(super) struct Score {
    /// Immutable scoring policy shared by peers in the owning network instance.
    #[serde(skip)]
    config: Arc<ScoreConfig>,
    /// The global score used to accumulate penalties.
    ///
    /// Once penalties are applied, they affect the `aggregate_score`.
    telcoin_score: f64,
    /// The aggregate score.
    ///
    /// This is the score used to rank peers.
    aggregate_score: f64,
    /// The time the score was last updated to perform time-based adjustments such as score-decay.
    #[serde(skip)]
    last_updated: Instant,
}

impl Score {
    /// Create a score using the owning network instance's default value and policy.
    pub(super) fn new(config: Arc<ScoreConfig>) -> Self {
        Self {
            telcoin_score: config.default_score,
            aggregate_score: config.default_score,
            last_updated: Instant::now(),
            config,
        }
    }

    /// Create `Self` with the owning network instance's maximum score.
    pub(super) fn new_max(config: Arc<ScoreConfig>) -> Self {
        Self {
            telcoin_score: config.max_score,
            aggregate_score: config.max_score,
            last_updated: Instant::now(),
            config,
        }
    }

    /// Reset to this instance's maximum score, clearing any previous decay lockout.
    pub(super) fn reset_to_max(&mut self) {
        self.telcoin_score = self.config.max_score;
        self.aggregate_score = self.config.max_score;
        self.last_updated = Instant::now();
    }

    /// The aggregate score.
    pub(super) fn aggregate_score(&self) -> f64 {
        self.aggregate_score
    }

    /// Modifies the score based on the penalty type and returns the new score.
    pub(super) fn apply_penalty(&mut self, penalty: Penalty) {
        // NOTE: these use `Self::add`
        // which cannot overflow using default config min and max scores
        let new_score = match penalty.severity() {
            Severity::Mild => self.add(-1.0),
            Severity::Medium => self.add(-5.0),
            Severity::Severe => self.add(-10.0),
            Severity::Fatal => self.config.min_score, // The worst possible score
        };

        // set application score
        self.telcoin_score = new_score;

        self.update_score();
    }

    /// Add an f64 to the currrent application score within the min/max limits.
    fn add(&mut self, score: f64) -> f64 {
        (self.telcoin_score + score).clamp(self.config.min_score, self.config.max_score)
    }

    /// Update all relevant scores based on the current instant.
    ///
    /// Nodes periodically call this method to assess decaying time intervals.
    pub(super) fn update(&mut self) {
        self.update_at(Instant::now());
    }

    /// Assess time intervals to update scores accordingly.
    ///
    /// This method decays the current score using an exponential decay based on a constant half
    /// life. The `checked_duration_since` method is used instead of `elapsed` because
    /// `last_updated` is set in the future when peers are banned. Banned peers return `None`, so
    /// their score will not decay.
    ///
    /// NOTE: this is a separate method for testing purposes.
    fn update_at(&mut self, now: Instant) {
        now.checked_duration_since(self.last_updated).into_iter().for_each(|duration| {
            // e^(-ln(2)/HL*t)
            let halflife_decay = self.config.halflife_decay();
            let decay_factor = (halflife_decay * duration.as_secs_f64()).exp();
            self.telcoin_score *= decay_factor;
            self.last_updated = now;
            self.update_score();
        });
    }

    /// Update the aggregate score by effectively assessing penalties.
    ///
    /// If the updated score is below the threshold, the peer will be banned.
    fn update_score(&mut self) {
        // capture current status
        let already_banned = self.is_banned();

        // update aggregate score
        self.aggregate_score = self.telcoin_score;

        // ban the peer if threshold reached
        if !already_banned && self.is_banned() {
            // ban the peer for at least BANNED_BEFORE_DECAY seconds
            self.last_updated += self.config.banned_before_decay();
        }
    }

    /// Helper method if a peer has reached the threshold for being banned.
    pub(super) fn is_banned(&self) -> bool {
        self.aggregate_score <= self.config.min_score_before_ban
    }

    /// Derive the peer's [Reputation] from its aggregate score.
    ///
    /// The instance's shared [ScoreConfig] supplies both reputation thresholds and the ban/decay
    /// lockout, so operator-set thresholds remain consistent for every peer in that instance.
    pub(super) fn reputation(&self) -> Reputation {
        reputation_for(self.aggregate_score, &self.config)
    }
}

/// Map an aggregate score onto a [Reputation] using the ban/disconnect thresholds in `config`.
///
/// Split out from [`Score::reputation`] so the threshold logic can be exercised against an
/// arbitrary [ScoreConfig].
fn reputation_for(aggregate_score: f64, config: &ScoreConfig) -> Reputation {
    match aggregate_score {
        score if score <= config.min_score_before_ban => Reputation::Banned,
        score if score <= config.min_score_before_disconnect => Reputation::Disconnected,
        _ => Reputation::Trusted,
    }
}

impl Eq for Score {}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.telcoin_score == other.telcoin_score
            && self.aggregate_score == other.aggregate_score
            && self.last_updated == other.last_updated
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.aggregate_score
            .partial_cmp(&other.aggregate_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl Display for Score {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.3}", self.aggregate_score())
    }
}

/// The expected status of the peer based on the peer's score.
#[derive(Debug, PartialEq, Clone, Copy)]
pub(super) enum Reputation {
    /// The peer is performing within the tolerable threshold.
    Trusted,
    /// The peer is below the tolerable threshold and should be disconnected. Peers may be able to
    /// reconnect if they persist.
    Disconnected,
    /// The peer is well below the tolerable threshold and is banned. The peer may only establish a
    /// new connection once the score has decayed back into the tolerable threshold.
    Banned,
}

impl Reputation {
    /// Matches on self.
    pub(super) fn banned(&self) -> bool {
        matches!(self, Reputation::Banned)
    }
}

/// The peer's reputation change after a heartbeat score update.
///
/// The reputation update is used to generate a `PeerAction` for the manager.
#[derive(Debug, PartialEq, Clone, Copy)]
pub(super) enum ReputationUpdate {
    /// The updated score resulted in a peer becoming banned.
    Banned,
    /// The updated score resulted in a peer becoming unbanned.
    Unbanned,
    /// The updated score resulted in peer disconnected.
    Disconnect,
    /// The updated score resulted no effective change for the peer's reputation.
    None,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Scores retain their instance's defaults, limits and reputation thresholds in either order.
    #[test]
    fn instance_score_configuration_is_order_independent() {
        let strict = Arc::new(ScoreConfig {
            default_score: 0.0,
            max_score: 20.0,
            min_score: -20.0,
            min_score_before_disconnect: -5.0,
            min_score_before_ban: -10.0,
            ..ScoreConfig::default()
        });
        let relaxed = Arc::new(ScoreConfig {
            default_score: 5.0,
            max_score: 50.0,
            min_score: -50.0,
            min_score_before_disconnect: -30.0,
            min_score_before_ban: -40.0,
            ..ScoreConfig::default()
        });

        [false, true].into_iter().for_each(|reverse| {
            let configs = if reverse {
                [relaxed.clone(), strict.clone()]
            } else {
                [strict.clone(), relaxed.clone()]
            };
            let scores = configs.map(|config| (Score::new(config.clone()), config));
            scores.into_iter().for_each(|(mut score, config)| {
                assert!(Arc::ptr_eq(&score.config, &config));
                assert_eq!(score.aggregate_score(), config.default_score);
                assert_eq!(Score::new_max(config.clone()).aggregate_score(), config.max_score);
                score.apply_penalty(Penalty::Severe);
                assert_eq!(score.aggregate_score(), config.default_score - 10.0);
                let expected = if config.default_score == 0.0 {
                    Reputation::Banned
                } else {
                    Reputation::Trusted
                };
                assert_eq!(score.reputation(), expected);
                assert_eq!(score.is_banned(), expected.banned());
                assert_eq!(score.add(-1000.0), config.min_score);
                assert_eq!(score.add(1000.0), config.max_score);
                score.apply_penalty(Penalty::Fatal);
                assert_eq!(score.aggregate_score(), config.min_score);
            });
        });
    }

    /// Decay and ban lockout use each instance's policy with explicit instants.
    #[test]
    fn instance_decay_and_ban_lockout() {
        [10_u64, 20_u64].into_iter().for_each(|seconds| {
            let config = Arc::new(ScoreConfig {
                default_score: 16.0,
                score_halflife: Duration::from_secs(seconds).as_secs_f64(),
                banned_before_decay_secs: seconds,
                ..ScoreConfig::default()
            });
            let mut score = Score::new(config.clone());
            let now = score.last_updated;
            score.update_at(now + Duration::from_secs(10));
            let expected = if seconds == 10 { 8.0 } else { 16.0 / 2.0_f64.sqrt() };
            assert!((score.aggregate_score() - expected).abs() < 1e-10);

            let before_ban = score.last_updated;
            score.apply_penalty(Penalty::Fatal);
            assert_eq!(score.last_updated, before_ban + Duration::from_secs(seconds));
            score.update_at(before_ban + Duration::from_secs(seconds.saturating_sub(1)));
            assert_eq!(score.aggregate_score(), config.min_score);
            score.update_at(before_ban + Duration::from_secs(seconds * 2));
            assert!((score.aggregate_score() - config.min_score / 2.0).abs() < 1e-10);
        });
    }

    /// Cloning and committee score resets preserve the owning instance's policy.
    #[test]
    fn instance_score_clone_and_reset() {
        [25.0, 75.0].into_iter().for_each(|max_score| {
            let config = Arc::new(ScoreConfig { max_score, ..ScoreConfig::default() });
            let mut original = Score::new(config.clone());
            original.apply_penalty(Penalty::Fatal);
            let mut score = original.clone();
            assert!(Arc::ptr_eq(&score.config, &config));
            score.reset_to_max();
            assert_ne!(score.last_updated, original.last_updated);
            assert_eq!(score.aggregate_score(), max_score);
            assert_eq!(score.telcoin_score, max_score);
            assert_eq!(original.aggregate_score(), config.min_score);
            score.update_at(score.last_updated + Duration::from_secs(1));
            assert!(score.aggregate_score() < max_score);
            score.apply_penalty(Penalty::Severe);
            assert!(score.aggregate_score() < max_score - 10.0);
        });
    }

    /// Build a [ScoreConfig] with the given ban/disconnect thresholds, defaulting the rest.
    fn config_with(min_score_before_disconnect: f64, min_score_before_ban: f64) -> ScoreConfig {
        ScoreConfig { min_score_before_disconnect, min_score_before_ban, ..ScoreConfig::default() }
    }

    #[test]
    fn reputation_honors_operator_thresholds() {
        // Default-equivalent thresholds: ban at -50, disconnect at -20.
        let strict = config_with(-20.0, -50.0);
        // Operator relaxes both thresholds to tolerate honest peers that fall behind during WAN
        // sync lag (the scenario in #689). Before #746 these overrides were silently ignored.
        let relaxed = config_with(-90.0, -95.0);

        // A score of -60 is well past the strict ban threshold...
        assert_eq!(reputation_for(-60.0, &strict), Reputation::Banned);
        // ...but the operator's relaxed config keeps the same peer trusted.
        assert_eq!(reputation_for(-60.0, &relaxed), Reputation::Trusted);
    }

    #[test]
    fn reputation_threshold_boundaries() {
        let config = config_with(-20.0, -50.0);

        // At or below a threshold counts as crossing it; ban takes precedence over disconnect.
        assert_eq!(reputation_for(-50.0, &config), Reputation::Banned);
        assert_eq!(reputation_for(-49.9, &config), Reputation::Disconnected);
        assert_eq!(reputation_for(-20.0, &config), Reputation::Disconnected);
        assert_eq!(reputation_for(-19.9, &config), Reputation::Trusted);
        assert_eq!(reputation_for(0.0, &config), Reputation::Trusted);
    }
}
