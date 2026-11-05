//! Validator keys tracked for export only, outside the committee cache.
//!
//! A swarm tracks the committee scheduled two epochs ahead and the eligible validator pool so the
//! node can report their signed addresses before any of them needs a connection. A tracked key is
//! never admitted, scored, pinned, or bound to a peer id. Its record lives here, not in the peer
//! manager's `known_peers` cache or the peer store, so admission, gossip attribution, and request
//! delivery behave exactly as they would without tracking.

use crate::{freshness::RecordTimestamp, types::NetworkInfo};
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tn_types::{BlsPublicKey, TimestampSec};
use tokio::time::Instant;

/// The most distinct keys one swarm tracks across its scheduled and eligible sets.
///
/// Each key holds at most one record of about half a kilobyte (the RPC endpoint is not kept), so
/// the retained records stay near half a megabyte per swarm.
pub(super) const MAX_TRACKED_KEYS: usize = 1024;

/// Scheduled and eligible keys with their retained records and lookup schedule.
#[derive(Debug, Default)]
pub(super) struct TrackedKeys {
    /// Epoch of the accepted sets, `None` until the first update.
    epoch: Option<u64>,
    /// Keys of the committee scheduled two epochs ahead.
    scheduled: HashSet<BlsPublicKey>,
    /// Keys of the eligible validator pool.
    eligible: HashSet<BlsPublicKey>,
    /// Verified records retained for tracked keys, with their local freshness metadata.
    ///
    /// Only tracked keys are inserted and an update drops every key it no longer lists, so this
    /// map never holds more than [`MAX_TRACKED_KEYS`] entries.
    records: HashMap<BlsPublicKey, (NetworkInfo, RecordTimestamp)>,
    /// Lookup schedule of tracked keys whose record is missing or outdated.
    lookups: HashMap<BlsPublicKey, Backoff>,
}

impl TrackedKeys {
    /// Replace the tracked sets with one epoch's publication.
    ///
    /// Returns `None` and changes nothing when `epoch` is older than the held epoch; a repeat of
    /// the held epoch replaces it. Past [`MAX_TRACKED_KEYS`] distinct keys, every scheduled key is
    /// kept first and eligible keys fill the remaining room in key order, so nodes that read the
    /// same pool keep the same keys. Records and lookup schedules of keys no longer tracked are
    /// dropped. Otherwise returns how many distinct published keys were left out.
    pub(super) fn update(
        &mut self,
        epoch: u64,
        scheduled: HashSet<BlsPublicKey>,
        eligible: HashSet<BlsPublicKey>,
    ) -> Option<usize> {
        if self.epoch.is_some_and(|held| epoch < held) {
            return None;
        }
        let published = distinct(&scheduled, &eligible);
        let (scheduled, eligible) = cap(scheduled, eligible);
        let left_out = published.saturating_sub(distinct(&scheduled, &eligible));
        self.epoch = Some(epoch);
        self.scheduled = scheduled;
        self.eligible = eligible;
        let (scheduled, eligible) = (&self.scheduled, &self.eligible);
        let tracked = |key: &BlsPublicKey| scheduled.contains(key) || eligible.contains(key);
        self.records.retain(|key, _| tracked(key));
        self.lookups.retain(|key, _| tracked(key));
        Some(left_out)
    }

    /// Whether `key` is in the scheduled or the eligible set.
    pub(super) fn contains(&self, key: &BlsPublicKey) -> bool {
        self.scheduled.contains(key) || self.eligible.contains(key)
    }

    /// Every tracked key once, scheduled keys first.
    pub(super) fn keys(&self) -> impl Iterator<Item = &BlsPublicKey> {
        self.scheduled.iter().chain(self.eligible.difference(&self.scheduled))
    }

    /// The record retained for `key`, if any.
    pub(super) fn record(&self, key: &BlsPublicKey) -> Option<&NetworkInfo> {
        self.records.get(key).map(|(info, _)| info)
    }

    /// Retain a verified record for a tracked key when it supersedes the held one.
    ///
    /// The caller has already checked the record against the configured bindings. Freshness
    /// follows the shared local policy, so a replayed or relayed older copy never regresses the
    /// held record. The RPC endpoint is dropped because the export never carries it. Returns
    /// whether the record was kept; nothing is kept for an untracked key.
    pub(super) fn retain(
        &mut self,
        key: BlsPublicKey,
        mut info: NetworkInfo,
        timestamp: RecordTimestamp,
        observed: TimestampSec,
    ) -> bool {
        let keep = self.contains(&key)
            && self.records.get(&key).is_none_or(|(_, held)| timestamp.supersedes(*held, observed));
        if keep {
            info.rpc = None;
            self.records.insert(key, (info, timestamp));
        }
        keep
    }

    /// Pick at most `limit` tracked keys from `wanted` whose lookup is due at `now`, and advance
    /// their schedule.
    ///
    /// Keys never looked up go first, then the keys due the longest, so a set larger than `limit`
    /// is covered across ticks instead of the same keys being picked again. A picked key waits
    /// `initial` before its next lookup, doubling on each pick up to `max`. A key outside `wanted`
    /// loses its schedule, so a key that resolved and later needs a lookup again starts over from
    /// `initial`.
    pub(super) fn due_lookups(
        &mut self,
        wanted: &HashSet<BlsPublicKey>,
        now: Instant,
        limit: usize,
        initial: Duration,
        max: Duration,
    ) -> Vec<BlsPublicKey> {
        self.lookups.retain(|key, _| wanted.contains(key));
        let mut due: Vec<(Option<Instant>, BlsPublicKey)> = wanted
            .iter()
            .filter(|key| self.contains(key))
            .map(|key| (self.lookups.get(key).map(|backoff| backoff.next), *key))
            .filter(|(next, _)| next.is_none_or(|next| next <= now))
            .collect();
        // `None` orders first, so keys never looked up lead
        due.sort_unstable_by_key(|(next, _)| *next);
        due.into_iter()
            .take(limit)
            .map(|(_, key)| {
                let backoff =
                    self.lookups.entry(key).or_insert(Backoff { next: now, delay: initial });
                backoff.next = now + backoff.delay;
                backoff.delay = backoff.delay.saturating_mul(2).min(max);
                key
            })
            .collect()
    }
}

/// Lookup schedule for one tracked key.
#[derive(Debug)]
struct Backoff {
    /// Earliest time the next lookup may start.
    next: Instant,
    /// Delay applied after the next lookup starts.
    delay: Duration,
}

/// Count the distinct keys across both sets.
fn distinct(scheduled: &HashSet<BlsPublicKey>, eligible: &HashSet<BlsPublicKey>) -> usize {
    scheduled.len().saturating_add(eligible.difference(scheduled).count())
}

/// Bound the published sets to [`MAX_TRACKED_KEYS`] distinct keys, scheduled keys first.
///
/// The sets are returned unchanged when they fit, so only an oversized publication pays for the
/// sort that makes the kept subset independent of hash order.
fn cap(
    scheduled: HashSet<BlsPublicKey>,
    eligible: HashSet<BlsPublicKey>,
) -> (HashSet<BlsPublicKey>, HashSet<BlsPublicKey>) {
    if distinct(&scheduled, &eligible) <= MAX_TRACKED_KEYS {
        return (scheduled, eligible);
    }
    let scheduled: HashSet<BlsPublicKey> = sorted(scheduled).take(MAX_TRACKED_KEYS).collect();
    let room = MAX_TRACKED_KEYS.saturating_sub(scheduled.len());
    let (shared, extra): (Vec<BlsPublicKey>, Vec<BlsPublicKey>) =
        eligible.into_iter().partition(|key| scheduled.contains(key));
    let eligible = shared.into_iter().chain(sorted(extra).take(room)).collect();
    (scheduled, eligible)
}

/// Collect `keys` in ascending key order.
fn sorted(keys: impl IntoIterator<Item = BlsPublicKey>) -> std::vec::IntoIter<BlsPublicKey> {
    let mut keys: Vec<BlsPublicKey> = keys.into_iter().collect();
    keys.sort_unstable();
    keys.into_iter()
}
