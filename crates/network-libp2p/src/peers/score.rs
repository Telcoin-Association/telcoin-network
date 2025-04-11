//! Scores for peer ranking.
//!
//! Peer scores are rational numbers in the range [-100, 100].
//! This is an experimental approach and is subject to change.
//!
//! Heavily inspired by Sigma Prime Lighthouse's scoring system.

use super::types::Penalty;
use serde::Serialize;
use std::{
    fmt::Display,
    sync::{Arc, OnceLock},
    time::Instant,
};
use tn_config::ScoreConfig;

/// Global static configuration that is initialized only once for all peers.
pub(crate) static GLOBAL_SCORE_CONFIG: OnceLock<Arc<ScoreConfig>> = OnceLock::new();

/// Initialize the global peer score configuration.
pub(super) fn init_peer_score_config(config: ScoreConfig) {
    let config = Arc::new(config);

    // allow multiple calls to this fn
    let _ = GLOBAL_SCORE_CONFIG.set(config);
}

/// Get a reference to the global peer score configuration
///
/// Panics if score config isn't set.
fn global_score_config() -> Arc<ScoreConfig> {
    GLOBAL_SCORE_CONFIG.get().expect("Peer score configuration not initialized").clone()
}

/// A peer's score (perceived potential usefulness).
///
/// This simplistic version consists of a global score per peer which decays to 0 over time. The
/// decay rate applies equally to positive and negative scores.
#[derive(PartialEq, Clone, Debug, Serialize)]
pub(super) struct Score {
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

impl Default for Score {
    fn default() -> Self {
        let config = global_score_config();

        Score {
            telcoin_score: config.default_score,
            aggregate_score: config.default_score,
            last_updated: Instant::now(),
        }
    }
}

impl Score {
    /// Create `Self` with max values.
    pub(super) fn new_max() -> Self {
        let config = global_score_config();

        Self {
            telcoin_score: config.max_score,
            aggregate_score: config.max_score,
            last_updated: Instant::now(),
        }
    }

    /// The aggregate score.
    pub(super) fn aggregate_score(&self) -> f64 {
        self.aggregate_score
    }

    /// Modifies the score based on the penalty type and returns the new score.
    pub(super) fn apply_penalty(&mut self, penalty: Penalty) {
        let config = global_score_config();

        // NOTE: these use `Self::add`
        // which cannot overflow using default config min and max scores
        let new_score = match penalty {
            Penalty::Mild => self.add(-1.0),
            Penalty::Medium => self.add(-5.0),
            Penalty::Severe => self.add(-10.0),
            Penalty::Fatal => config.min_score, // The worst possible score
        };

        // set application score
        self.telcoin_score = new_score;

        self.update_score();
    }

    /// Add an f64 to the currrent application score within the min/max limits.
    fn add(&mut self, score: f64) -> f64 {
        let config = global_score_config();
        (self.telcoin_score + score).clamp(config.min_score, config.max_score)
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
        if let Some(prev_update) =
            now.checked_duration_since(self.last_updated).map(|d| d.as_secs())
        {
            let config = global_score_config();

            // e^(-ln(2)/HL*t)
            let halflife_decay = config.halflife_decay();
            let decay_factor = (halflife_decay * prev_update as f64).exp();
            self.telcoin_score *= decay_factor;
            self.last_updated = now;
            self.update_score();
        }
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
            let config = global_score_config();

            // ban the peer for at least BANNED_BEFORE_DECAY seconds
            self.last_updated += config.banned_before_decay();
        }
    }

    /// Helper method if a peer has reached the threshold for being banned.
    pub(super) fn is_banned(&self) -> bool {
        let config = global_score_config();
        self.aggregate_score <= config.min_score_before_ban
    }
}

impl Eq for Score {}

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
