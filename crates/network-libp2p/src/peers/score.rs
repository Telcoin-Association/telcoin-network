//! Scores for peer ranking.
//!
//! Peer scores are rational numbers in the range [-100, 100].
//! This is an experimental approach and is subject to change.
//!
//! Heavily inspired by Sigma Prime Lighthouse's scoring system.

use serde::Serialize;
use std::fmt::Display;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

// Replace the lazy_static macro with a static OnceLock
static HALFLIFE_DECAY: OnceLock<f64> = OnceLock::new();

/// The default score for new peers.
pub(crate) const DEFAULT_SCORE: f64 = 0.0;
/// The threshold for a peer's score before they are banned, regardless of any other scoring
/// parameters.
const MIN_APPLICATION_SCORE_BEFORE_BAN: f64 = -60.0;
/// The maximum score a peer can obtain.
const MAX_SCORE: f64 = 100.0;
/// The minimum score a peer can obtain.
const MIN_SCORE: f64 = -100.0;
/// The halflife of a peer's score.
///
/// This represents the time (seconds) before the score decays to half its value.
const SCORE_HALFLIFE: f64 = 600.0;
/// The minimum amount of time (seconds) a peer is banned before their score begins to decay.
const BANNED_BEFORE_DECAY: Duration = Duration::from_secs(12 * 3600); // 12 hours
/// Threshold to prevent libp2p gossipsub from disconnecting peers.
pub const GOSSIPSUB_GREYLIST_THRESHOLD: f64 = -16000.0;

// TODO: all values go into config - use once lock and initialize when peer manager starts
const MIN_SCORE_BEFORE_DISCONNECT: f64 = -20.0;
const MIN_SCORE_BEFORE_BAN: f64 = -50.0;

/// Weight for gossipsub scores to prevent non-decaying scores for disconnected peers.
const GOSSIPSUB_SCORE_WEIGHT: f64 =
    (MIN_SCORE_BEFORE_DISCONNECT + 1.0) / GOSSIPSUB_GREYLIST_THRESHOLD;

/// Penalties applied to peers based on the significance of their actions.
///
/// Each variant has an associated score change.
///
/// NOTE: the number of variations is intentionally low.
/// Too many variations or specific penalties would result in more complexity.
#[derive(Debug, Clone, Copy)]
pub enum Penalty {
    /// The penalty assessed for actions that result in an error and are likely not malicious.
    ///
    /// Peers have a high tolerance for this type of error and will be banned ~50 occurances.
    Mild,
    /// The penalty assessed for actions that result in an error and are likely not malicious.
    ///
    /// Peers have a medium tolerance for this type of error and will be banned ~10 occurances.
    Medium,
    /// The penalty assessed for actions that are likely not malicious, but will not be tolerated.
    ///
    /// The peer will be banned after ~5 occurances (based on -100).
    Severe,
    /// The penalty assessed for unforgiveable actions.
    ///
    /// This type of action results in disconnecting from a peer and banning them.
    Fatal,
}

/// A peer's score (perceived potential usefulness).
///
/// This simplistic version consists of a global score per peer which decays to 0 over time. The
/// decay rate applies equally to positive and negative scores.
#[derive(PartialEq, Clone, Debug, Serialize)]
pub struct Score {
    /// The global score used to accumulate penalties.
    ///
    /// Once penalties are applied, they affect the `aggregate_score`.
    telcoin_score: f64,
    /// The score from gossip network peers.
    gossipsub_score: f64,
    /// Indicates if a negative gossipsub score should be ignored.
    ///
    /// Optional: allow a peer to stay connected while their score decays.
    ignore_negative_gossipsub_score: bool,
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
        Score {
            telcoin_score: DEFAULT_SCORE,
            gossipsub_score: DEFAULT_SCORE,
            aggregate_score: DEFAULT_SCORE,
            last_updated: Instant::now(),
            ignore_negative_gossipsub_score: false,
        }
    }
}

impl Score {
    /// The aggregate score.
    pub(super) fn aggregate_score(&self) -> f64 {
        self.aggregate_score
    }

    /// Modifies the score based on the penalty type and returns the new score.
    pub fn apply_penalty(&mut self, penalty: Penalty) {
        // TODO: can these overflow?
        let new_score = match penalty {
            Penalty::Mild => self.add(-1.0),
            Penalty::Medium => self.add(-5.0),
            Penalty::Severe => self.add(-10.0),
            Penalty::Fatal => MIN_SCORE, // The worst possible score
        };

        // set application score
        self.telcoin_score = new_score;

        self.update_score();
    }

    /// Add an f64 to the currrent application score within the min/max limits.
    fn add(&mut self, score: f64) -> f64 {
        (self.telcoin_score + score).clamp(MIN_SCORE, MAX_SCORE)
    }

    /// Update all relevant scores based on the current instant.
    ///
    /// Nodes periodically call this method to assess decaying time intervals.
    pub fn update(&mut self) {
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
            // e^(-ln(2)/HL*t)
            let halflife_decay = self.get_halflife_decay();
            let decay_factor = (halflife_decay * prev_update as f64).exp();
            self.telcoin_score *= decay_factor;
            self.last_updated = now;
            self.update_score();
        }

        // // return no update if this is the first time a score is set
        // ReputationUpdate::None
    }

    /// Function to get the [HALFLIFE_DECAY] and initializing it if needed.
    fn get_halflife_decay(&self) -> f64 {
        *HALFLIFE_DECAY.get_or_init(|| -(2.0f64.ln()) / SCORE_HALFLIFE)
    }

    /// Update the aggregate score by effectively assessing penalties.
    ///
    /// If the updated score is below the threshold, the peer will be banned.
    fn update_score(&mut self) {
        // capture current status
        let already_banned = self.is_banned();

        // apply gossip score weights
        self.apply_gossip_weights();

        // ban the peer if threshold reached
        if !already_banned && self.is_banned() {
            // ban the peer for at least BANNED_BEFORE_DECAY seconds
            self.last_updated += BANNED_BEFORE_DECAY;
        }
    }

    /// Calculate the aggregate score based on application and gossipsub scores.
    ///
    /// If the application score is too low, the method does nothing because the peer will be
    /// banned.
    fn apply_gossip_weights(&mut self) {
        // start with new application score
        self.aggregate_score = self.telcoin_score;

        // apply additional weight factors
        if self.telcoin_score <= MIN_APPLICATION_SCORE_BEFORE_BAN {
            //ignore all other scores - peer is banned
        } else if self.gossipsub_score >= 0.0 {
            self.aggregate_score += self.gossipsub_score * GOSSIPSUB_SCORE_WEIGHT;
        } else if !self.ignore_negative_gossipsub_score {
            self.aggregate_score += self.gossipsub_score * GOSSIPSUB_SCORE_WEIGHT;
        }
    }

    /// Update the gossipsub score for this peer with a new value.
    pub fn update_gossipsub_score(&mut self, new_score: f64, ignore: bool) {
        // we only update gossipsub if last_updated is in the past which means either the peer is
        // not banned or the BANNED_BEFORE_DECAY time is over.
        if self.last_updated <= Instant::now() {
            self.gossipsub_score = new_score;
            self.ignore_negative_gossipsub_score = ignore;
            self.update_score();
        }
    }

    /// Helper method if a peer is scored above the default `0.0`.
    pub fn is_good_gossipsub_peer(&self) -> bool {
        self.gossipsub_score >= 0.0
    }

    /// Helper method if a peer has reached the threshold for being banned.
    pub fn is_banned(&self) -> bool {
        self.aggregate_score <= MIN_SCORE_BEFORE_BAN
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
// TODO: this was ScoreState
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
    pub(super) fn trusted(&self) -> bool {
        matches!(self, Reputation::Trusted)
    }

    /// Matches on self.
    pub(super) fn disconnected(&self) -> bool {
        matches!(self, Reputation::Disconnected)
    }

    /// Matches on self.
    pub(super) fn banned(&self) -> bool {
        matches!(self, Reputation::Banned)
    }
}

/// The peer's reputation change after a heartbeat score update.
///
/// TODO: remove `PeerAction` and only use `ReputationUpdate`?
/// These are essentially the same thing and the reputation should be the source of truth.
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
