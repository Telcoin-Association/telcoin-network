//! Scores for peer ranking.
//!
//! Peer scores are rational numbers in the range [-100, 100].
//! This is an experimental approach and is subject to change.
//!
//! Heavily inspired by Sigma Prime Lighthouse's scoring system.

// !!~~~~~~~~~~
// TODO: do not merge until this is resolved
//
//
//
// lazy_static::lazy_static! {
//     static ref HALFLIFE_DECAY: f64 = -(2.0f64.ln()) / SCORE_HALFLIFE;
// }

use serde::Serialize;
use std::time::{Duration, Instant};

/// The default score for new peers.
pub(crate) const DEFAULT_SCORE: f64 = 0.0;
/// The threshold for a peer's score before they are disconnected.
const MIN_SCORE_BEFORE_DISCONNECT: f64 = -20.0;
/// The threshold for a peer's score before they are banned.
const MIN_SCORE_BEFORE_BAN: f64 = -50.0;
/// The threshold for a peer's score before they are banned, regardless of any other scoring parameters.
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
/// Weight for gossipsub scores to prevent non-decaying scores for disconnected peers.
const GOSSIPSUB_SCORE_WEIGHT: f64 =
    (MIN_SCORE_BEFORE_DISCONNECT + 1.0) / GOSSIPSUB_GREYLIST_THRESHOLD;

#[derive(Serialize, Clone, Debug)]
pub enum Score {
    /// The highest score attainable (infinity).
    ///
    /// This value is set when node operators explicitly specify trusted peers.
    Max,
    /// Default value for discovered peers.
    Real(RealScore),
}

impl Score {
    ///
    pub fn score(&self) -> f64 {
        match self {
            Self::Max => f64::INFINITY,
            Self::Real(score) => score.aggregate_score(),
        }
    }

    pub fn max_score() -> Self {
        Self::Max
    }

    /// Returns the expected state of the peer given it's score.
    pub(crate) fn state(&self) -> PeerStatus {
        match self.score() {
            score if score <= MIN_SCORE_BEFORE_BAN => PeerStatus::Banned,
            score if score <= MIN_SCORE_BEFORE_DISCONNECT => PeerStatus::Disconnected,
            _ => PeerStatus::Healthy,
        }
    }

    pub fn is_good_gossipsub_peer(&self) -> bool {
        match self {
            Self::Max => true,
            Self::Real(score) => score.is_good_gossipsub_peer(),
        }
    }
}

impl Default for Score {
    fn default() -> Self {
        Self::Real(RealScore::default())
    }
}

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
pub struct RealScore {
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
    aggregate_score: f64,
    /// The time the score was last updated to perform time-based adjustments such as score-decay.
    #[serde(skip)]
    last_updated: Instant,
}

impl Default for RealScore {
    fn default() -> Self {
        RealScore {
            telcoin_score: DEFAULT_SCORE,
            gossipsub_score: DEFAULT_SCORE,
            aggregate_score: DEFAULT_SCORE,
            last_updated: Instant::now(),
            ignore_negative_gossipsub_score: false,
        }
    }
}

impl RealScore {
    /// Calculate the aggregate score based on application and gossipsub scores.
    ///
    /// If the application score is too low, the method does nothing because the peer will be banned.
    fn calculate_score(&mut self) {
        self.aggregate_score = self.telcoin_score;
        if self.telcoin_score <= MIN_APPLICATION_SCORE_BEFORE_BAN {
            //ignore all other scores - peer is banned
        } else if self.gossipsub_score >= 0.0 {
            self.aggregate_score += self.gossipsub_score * GOSSIPSUB_SCORE_WEIGHT;
        } else if !self.ignore_negative_gossipsub_score {
            self.aggregate_score += self.gossipsub_score * GOSSIPSUB_SCORE_WEIGHT;
        }
    }

    /// The aggregate score.
    fn aggregate_score(&self) -> f64 {
        self.aggregate_score
    }

    /// Modifies the score based on the penalty type.
    pub fn apply_penalty(&mut self, penalty: Penalty) {
        match penalty {
            Penalty::Mild => self.add(-1.0),
            Penalty::Medium => self.add(-5.0),
            Penalty::Severe => self.add(-10.0),
            Penalty::Fatal => self.set_telcoin_score(MIN_SCORE), // The worst possible score
        }
    }

    /// Replace the current application score with a new one.
    fn set_telcoin_score(&mut self, new_score: f64) {
        self.telcoin_score = new_score;
        self.update_score();
    }

    /// Add an f64 to the score abiding by the limits.
    fn add(&mut self, score: f64) {
        let new_score = (self.telcoin_score + score).clamp(MIN_SCORE, MAX_SCORE);
        self.set_telcoin_score(new_score);
    }

    /// Update all relevant scores based on the current instant.
    ///
    /// Nodes periodically call this method to assess decaying time intervals.
    pub fn update(&mut self) {
        self.update_at(Instant::now())
    }

    /// Assess time intervals to update scores accordingly.
    ///
    /// This method decays the current score using an exponential decay based on a constant half life. The `checked_duration_since` method is used instead of `elapsed` because `last_updated` is set in the future when peers are banned. Banned peers return `None`, so their score will not be decayed.
    ///
    /// NOTE: this is kept separate mainly for testing purposes.
    fn update_at(&mut self, now: Instant) {
        if let Some(secs_since_update) =
            now.checked_duration_since(self.last_updated).map(|d| d.as_secs())
        {
            // e^(-ln(2)/HL*t)
            let todo_delete_me = -(2.0f64.ln()) / SCORE_HALFLIFE;
            // let decay_factor = (*HALFLIFE_DECAY * secs_since_update as f64).exp();
            let decay_factor = (todo_delete_me * secs_since_update as f64).exp();
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

        // update score
        self.calculate_score();

        // ban the peer if threshold reached
        if !already_banned && self.is_banned() {
            // ban the peer for at least BANNED_BEFORE_DECAY seconds
            self.last_updated += BANNED_BEFORE_DECAY;
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

/// The expected status of the peer based on the peer's score.
#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum PeerStatus {
    /// The peer is performing within the tolerable threshold.
    Healthy,
    /// The peer is below the tolerable threshold and should be disconnected. Peers may be able to reconnect if they are persistent.
    Disconnected,
    /// The peer is well below the tolerable threshold and is banned. The peer may only establish a new connection once the score has decayed back into the tolerable threshold.
    Banned,
}
