//! Primary Receiver Handler is the entrypoint for peer network requests.
//!
//! This module includes implementations for when the primary receives network
//! requests from it's own workers and other primaries.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crate::{
    error::{PrimaryNetworkError, PrimaryNetworkResult},
    proposer::OurDigestMessage,
    state_sync::StateSynchronizer,
    ConsensusBus, ConsensusBusApp,
};
use futures::{
    AsyncRead, AsyncWrite, AsyncWriteExt as _, FutureExt as _, StreamExt as _, TryStreamExt as _,
};
use handler::RequestHandler;
pub use message::{MissingCertificatesRequest, PrimaryRequest, PrimaryResponse};
use message::{PrimaryGossip, PrimaryRPCError};
use parking_lot::Mutex;
use rand::{seq::SliceRandom as _, Rng};
use sync_codec::{SyncRateBound, SyncServeMeter, SyncServeSpeed};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    error::NetworkError,
    read_frame,
    types::{
        GossipPayload, GossipValidation, IntoResponse as _, NetworkCommand, NetworkEvent,
        NetworkHandle, NetworkResponseMessage, NetworkResult,
    },
    write_frame, AdmissionShed, DenyReason, PeerSlot, PeerSlots, PeerTier, Penalty,
    PrimarySyncRequest, ReservedPeers, ReservedPool, ResponseChannel, ServiceClass, Stream,
    StreamError, SyncFrame, SyncFrameError,
};
use tn_network_types::{WorkerOthersBatchMessage, WorkerOwnBatchMessage, WorkerToPrimaryClient};
use tn_storage::{
    consensus::{ConsensusChain, ConsensusChainError},
    consensus_pack::PackError,
    PayloadStore,
};
use tn_types::{
    encode, BlsPublicKey, BlsSignature, Certificate, ConsensusHeaderDigest, ConsensusOutput,
    ConsensusResult, Database, Epoch, EpochCertificate, EpochDigest, EpochRecord, EpochVote,
    Header, HeaderDigest, Round, TaskError, TaskSpawner, TnReceiver, TnSender, Vote, WorkerId,
};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, warn};
pub(crate) mod gap_policy;
pub mod handler;
mod message;
mod sync_codec;

#[cfg(test)]
#[path = "../tests/network_tests.rs"]
mod network_tests;

/// Convenience type for Primary network.
pub(crate) type Req = PrimaryRequest;
/// Convenience type for Primary network.
pub(crate) type Res = PrimaryResponse;

/// Public share of each inbound sync serve class.
///
/// The primary serves inbound sync requests from one serve pool per class (epoch pack, missing
/// certificates, consensus output; see `SyncServePools`), so a flood of one class cannot
/// starve another. Any peer may take one of these permits. Each pool also holds
/// `COMMITTEE_RESERVED_SYNC_STREAMS` permits per committee key, and only that key of the
/// current or the next committee may take them. A serve permit is held only while its request
/// is served, never during the opening read (see `MAX_CONCURRENT_OPENING_READS`).
pub const MAX_CONCURRENT_EPOCH_STREAMS: usize = 5;

/// Reserve of each committee key in each inbound sync serve class.
///
/// Each serve pool holds this many permits for each key of the current or the next committee,
/// on top of its public share ([`MAX_CONCURRENT_EPOCH_STREAMS`]). Only that key may take them,
/// and no other key can take from its reserve. So peers outside the committee that fill the
/// public share, and committee keys that hold streams without progress, cannot stop a different
/// committee key from catching up (issue #1750). The per-peer cap
/// ([`MAX_PENDING_REQUESTS_PER_PEER`], 2) lets a committee key open one more stream from the
/// public share. See GHSA-9h5w-9282-pc3g.
pub(crate) const COMMITTEE_RESERVED_SYNC_STREAMS: usize = 1;

/// Public share of the opening-read budget for inbound sync streams.
///
/// A permit is held only while the responder waits up to [`opening_read_bound`] (1 s public,
/// 5 s committee) for the opening request frame of a stream. It is released as soon as that
/// read ends, whatever its result, so no serve, control write or close holds it. In the worst
/// case this many opening reads (plus the reserve of each committee key) are in flight, each
/// bounded by [`MAX_SYNC_REQUEST_FRAME_SIZE`].
pub(crate) const MAX_CONCURRENT_OPENING_READS: usize = 8;

/// Reserve of each committee key in the opening-read budget for inbound sync streams.
///
/// The budget holds this many permits for each key of the current or the next committee, on top
/// of its public share ([`MAX_CONCURRENT_OPENING_READS`]). Only that key may take them, and no
/// other key can take from its reserve. So silent streams from peers outside the committee, and
/// committee keys that hold streams without progress, cannot stop a different committee key from
/// sending its request and catching up (issue #1750). The per-peer cap
/// ([`MAX_PENDING_REQUESTS_PER_PEER`], 2) lets a committee key open one more stream from the
/// public share.
pub(crate) const COMMITTEE_RESERVED_OPENING_READS: usize = 1;

/// Maximum number of concurrent `EpochRecord` request-response serves across all peers.
///
/// The `EpochRecord` request-response arm is reachable by any peer whose identity resolves
/// (not committee membership), and each serve does real work: a database lookup. A record
/// without its certificate is answered as unavailable at once (no wait), so a permit is held only
/// for the lookup and the response. Without a bound a peer could spawn unbounded concurrent,
/// penalty-free serve tasks. A dedicated semaphore (separate from `MAX_CONCURRENT_EPOCH_STREAMS`
/// so an epoch-record flood and the stream-serving paths never starve one another) caps the
/// concurrent serve count. See GHSA-vc2r-9cp2-w74j.
pub const MAX_CONCURRENT_EPOCH_RECORD_REQUESTS: usize = 5;

/// Hard cap on the number of distinct pending consensus results tracked in the
/// handler's `consensus_certs` signature-tally map at any time.
///
/// `consensus_certs` is a transient tally that is wiped wholesale the moment any
/// consensus result reaches quorum, so under honest operation only a handful of
/// entries (typically one, for the next consensus number) are ever live at once.
/// Capping the map bounds its memory against a gossip flood of validly-signed but
/// non-quorum `ConsensusResult`s from Byzantine committee members: no matter how
/// many members collude, they cannot grow the map past this cap. See the
/// `PrimaryGossip::Consensus` handler and GHSA-2r5c-c4h7-gp5h.
pub(crate) const MAX_CONSENSUS_CERTS: usize = 20;

/// Hard cap on the number of distinct epoch votes deduplicated at ingress.
///
/// The handler's `epoch_votes_seen` map records one entry per `(author, epoch, epoch-record
/// hash)` once a vote's signature has verified, so a replayed or flooded vote is dropped before the
/// next (expensive) BLS verify. Sized well above a single committee's worth of votes across a
/// few candidate epoch records. Eviction is least-recently-seen, so an over-flood only costs
/// redundant verifies (never a dropped honest vote — the collector dedups per signer). See the
/// `PrimaryGossip::EpochVote` handler and issue #898.
pub(crate) const MAX_EPOCH_VOTES: usize = 1024;

/// Maximum number of distinct in-flight `consensus_certs` tallies any single committee member
/// may be a signer of for one consensus number at a time. An honest validator signs exactly one
/// hash per consensus number, so a signer present in this many distinct live tallies for the same
/// number is equivocating; its further fresh digests for that number are dropped. Per
/// `(signer, number)`, not per signer. See the handler and GHSA-pvhw-9pmg-q2hg.
pub(crate) const MAX_TALLIES_PER_SIGNER_PER_NUMBER: usize = 2;
/// Maximum number of concurrent pending batch requests from a single peer.
///
/// Prevents a single malicious peer from filling all global slots.
pub const MAX_PENDING_REQUESTS_PER_PEER: usize = 2;

/// Maximum number of gossip messages from a single peer in signature verification at once
/// (#1772).
///
/// The honest peak per author is one certificate per round plus a consensus result and an
/// epoch vote. Four leaves headroom for a burst after a reconnect.
pub const MAX_PENDING_GOSSIP_PER_PEER: usize = 4;

/// How long this node ignores gossip from an author after its first strike in an epoch (#1772).
///
/// A strike is a fault of the strike class ([`PrimaryNetworkError::strikes_gossip_author`]), for
/// example a certificate whose aggregate signature fails. It counts only for a frame that the
/// author delivered itself while the ticket was pending (see [`gossip_author_to_strike`]). A
/// forged certificate is dropped at the first hop and honest nodes never relay it, so the
/// forger's own delivery is still struck. The gossipsub envelope is signed, so the author is
/// accountable. Each further strike in the epoch doubles the hold, up to
/// [`GOSSIP_AUTHOR_COOLDOWN_MAX`].
///
/// The peer manager exempts committee keys from every penalty (`docs/src/network/peers.md`, test
/// `committee_members_are_exempt_from_all_penalties`). The `Fatal` is then recorded with
/// `outcome=exempt`, and this local cooldown is the cost that the author pays. The slots are
/// rebuilt with the primary network each epoch, which clears the strikes.
pub(crate) const GOSSIP_AUTHOR_COOLDOWN_BASE: Duration = Duration::from_secs(10);

/// Upper bound on the gossip cooldown of an author after repeated strikes in one epoch (#1772).
///
/// See [`GOSSIP_AUTHOR_COOLDOWN_BASE`].
pub(crate) const GOSSIP_AUTHOR_COOLDOWN_MAX: Duration = Duration::from_secs(160);

/// Guard for one gossip message in signature verification (#1772).
///
/// It holds a per-peer slot, or a permit from the shared bucket for gossip whose author and
/// relayer are both unresolved. Dropping the guard frees the slot.
pub(crate) enum GossipSlot {
    /// A slot keyed by the author, or by the relayer when the author is unresolved.
    Peer {
        /// The per-peer slot. It frees on drop.
        ///
        /// Boxed: the slot is far larger than the shared permit (clippy `large_enum_variant`).
        _slot: Box<PeerSlot>,
    },
    /// A permit from the shared bucket for gossip with no resolved author or relayer.
    Shared {
        /// The shared permit. It returns to the bucket on drop.
        _permit: OwnedSemaphorePermit,
    },
}

/// Per-peer bound on gossip in signature verification (#1772).
///
/// A message takes a slot keyed by its author when the author is resolved, else by its relayer.
/// Gossip with neither identity resolved shares one bucket with the same cap.
///
/// An author struck for a fault of the strike class
/// ([`PrimaryNetworkError::strikes_gossip_author`]) gets no slot for a cooldown that starts at
/// [`GOSSIP_AUTHOR_COOLDOWN_BASE`] and doubles per strike up to [`GOSSIP_AUTHOR_COOLDOWN_MAX`].
/// The strike applies only to a frame that the author delivered itself while the ticket was
/// pending (see [`gossip_author_to_strike`]).
#[derive(Clone, Debug)]
pub(crate) struct GossipSlots {
    /// Slots keyed by the author or relayer BLS key.
    peers: PeerSlots,
    /// Shared bucket for gossip whose author and relayer are both unresolved.
    unresolved: Arc<Semaphore>,
    /// Cooldowns of struck authors. The slots are rebuilt each epoch, which clears them.
    cooldowns: Arc<Mutex<HashMap<BlsPublicKey, AuthorCooldown>>>,
}

/// The gossip cooldown of one struck author in this epoch (#1772).
#[derive(Clone, Copy, Debug)]
struct AuthorCooldown {
    /// Strikes against the author in this epoch.
    strikes: u32,
    /// The author's gossip gets no slot until this instant.
    until: Instant,
}

impl GossipSlots {
    /// Create the slots with `cap` messages per peer and `cap` messages in the shared bucket.
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            peers: PeerSlots::new(cap),
            unresolved: Arc::new(Semaphore::new(cap)),
            cooldowns: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Take a slot for gossip from `author`, else `relayer`, else the shared bucket.
    ///
    /// Returns `None` when that peer or the shared bucket is at the cap, or when `author` is in a
    /// cooldown.
    pub(crate) fn take(
        &self,
        author: Option<BlsPublicKey>,
        relayer: Option<BlsPublicKey>,
    ) -> Option<GossipSlot> {
        self.take_at(author, relayer, Instant::now())
    }

    /// [`Self::take`] at the instant `now`.
    fn take_at(
        &self,
        author: Option<BlsPublicKey>,
        relayer: Option<BlsPublicKey>,
        now: Instant,
    ) -> Option<GossipSlot> {
        let cooling = author
            .and_then(|a| self.cooldowns.lock().get(&a).copied())
            .is_some_and(|c| c.until > now);
        if cooling {
            return None;
        }
        author.or(relayer).map_or_else(
            || {
                Arc::clone(&self.unresolved)
                    .try_acquire_owned()
                    .ok()
                    .map(|permit| GossipSlot::Shared { _permit: permit })
            },
            |peer| self.peers.try_take(peer).map(|slot| GossipSlot::Peer { _slot: Box::new(slot) }),
        )
    }

    /// Strike `author` for a fault of the strike class on its own delivery (#1772).
    ///
    /// The caller picks the author with [`gossip_author_to_strike`]: the fault is one where
    /// [`PrimaryNetworkError::strikes_gossip_author`] holds, and the author delivered the frame
    /// itself while the ticket was pending.
    ///
    /// Its gossip gets no slot for [`GOSSIP_AUTHOR_COOLDOWN_BASE`], doubled for each earlier
    /// strike in this epoch, up to [`GOSSIP_AUTHOR_COOLDOWN_MAX`].
    pub(crate) fn strike(&self, author: BlsPublicKey) {
        self.strike_at(author, Instant::now())
    }

    /// [`Self::strike`] at the instant `now`.
    fn strike_at(&self, author: BlsPublicKey, now: Instant) {
        let mut guard = self.cooldowns.lock();
        let strikes = guard.get(&author).map_or(1, |c| c.strikes.saturating_add(1));
        let hold = GOSSIP_AUTHOR_COOLDOWN_BASE
            .saturating_mul(1u32 << strikes.saturating_sub(1).min(4))
            .min(GOSSIP_AUTHOR_COOLDOWN_MAX);
        guard.insert(author, AuthorCooldown { strikes, until: now + hold });
    }

    /// The end of the cooldown of `author`, if it was struck in this epoch.
    #[cfg(test)]
    pub(crate) fn cooldown_until(&self, author: &BlsPublicKey) -> Option<Instant> {
        self.cooldowns.lock().get(author).map(|c| c.until)
    }

    /// Admit one gossip message for signature verification.
    ///
    /// Returns `None` when no slot is free. The ticket then drops and reports `Ignore`.
    pub(crate) fn admit(
        &self,
        validation: GossipValidation<Req, Res>,
        author: Option<BlsPublicKey>,
        relayer: Option<BlsPublicKey>,
    ) -> Option<PendingGossip> {
        self.take(author, relayer).map(|slot| PendingGossip::new(validation, slot))
    }
}

/// The author that a failed gossip message strikes, if any (#1772).
///
/// This is the same by-author rule that the network layer applies on its reject path
/// (GHSA-vw36): any peer that once held the frame can deliver it again. The author is struck
/// only for a fault of the strike class ([`PrimaryNetworkError::strikes_gossip_author`]) on a
/// frame that the author delivered itself while the ticket was pending. A relayed forged frame
/// is still rejected and never forwarded, but it strikes no one. A frame whose ticket was
/// already settled (a fault after Accept) strikes no one.
pub(crate) fn gossip_author_to_strike(
    e: &PrimaryNetworkError,
    was_pending: bool,
    author: Option<BlsPublicKey>,
    relayer: Option<BlsPublicKey>,
) -> Option<BlsPublicKey> {
    author.filter(|a| was_pending && relayer == Some(*a) && e.strikes_gossip_author())
}

/// One gossip message in signature verification: its verdict ticket and its slot (#1772).
pub(crate) struct PendingGossip {
    /// The deferred verdict ticket for gossipsub.
    validation: GossipValidation<Req, Res>,
    /// The verification slot. The first verdict frees it, so downstream work does not hold it.
    slot: Option<GossipSlot>,
}

impl PendingGossip {
    /// Pair a ticket with the slot it was admitted under.
    pub(crate) fn new(validation: GossipValidation<Req, Res>, slot: GossipSlot) -> Self {
        Self { validation, slot: Some(slot) }
    }

    /// A message with no ticket to report and no slot, for direct handler calls in tests.
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self { validation: GossipValidation::detached(), slot: None }
    }

    /// Free the slot and report `Accept`, so gossipsub forwards the message.
    ///
    /// Call it right after the signature check and before any downstream work.
    pub(crate) async fn accept(&mut self) {
        drop(self.slot.take());
        self.validation.accept().await;
    }

    /// Free the slot and report `Reject`, so gossipsub drops the message as invalid.
    pub(crate) async fn reject(&mut self) {
        drop(self.slot.take());
        self.validation.reject().await;
    }

    /// Whether the ticket still waits for a verdict.
    pub(crate) fn is_pending(&self) -> bool {
        self.validation.is_pending()
    }

    /// Settle the ticket after the handler failed with `e`.
    ///
    /// An author content fault reports `Reject` while the ticket is pending. Any other error
    /// drops the ticket, which reports `Ignore`.
    pub(crate) async fn settle_error(mut self, e: &crate::error::PrimaryNetworkError) {
        if e.is_author_content_fault() && self.is_pending() {
            self.reject().await;
        }
    }
}

/// Maximum number of concurrent shed tasks for denied inbound sync streams.
///
/// A denied stream still gets a short-lived task that writes
/// [`DenyReason::AtCapacity`] so the requester fails fast instead of waiting out
/// a timeout. Without a budget on that spawn, a burst of opened substreams
/// beyond the admission caps fans out to one task each, bounded only by the
/// stream behavior's per-peer inbound rate limit (#1308). Past this budget the
/// stream is dropped without spawning (the requester sees a reset and retries
/// elsewhere). Admitted tasks are bounded by the opening-read budget
/// ([`MAX_CONCURRENT_OPENING_READS`]), the serve pools ([`SyncServePools`]) and the per-peer
/// cap ([`MAX_PENDING_REQUESTS_PER_PEER`]), so the primary's total sync-task fan-out stays
/// bounded by those budgets plus this many shed tasks.
pub(crate) const MAX_CONCURRENT_SHED_TASKS: usize = 8;

/// Longest peer-supplied error text, in characters, that a vote request keeps.
///
/// A voter chooses the text of its error responses, bounded only by the RPC message size, and
/// the requester logs it and hands it to the certifier, which retries until the proposal is
/// superseded. Clipping keeps a Byzantine voter from turning each response into a megabyte-long
/// log line.
const MAX_PEER_ERROR_LEN: usize = 256;

/// Timeout for the responder's first sync frame (`Ack`/`Deny`) after the epoch-pack
/// request frame is written. A peer that negotiated the sync protocol but does not
/// answer (a pre-cutover node that registered the protocol but reads the stream on
/// the legacy digest path) trips this and is cached unsyncable this epoch, so the
/// probe moves on to the next peer.
const SYNC_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Overall bound on reading a streamed missing-certificates response, on top of the
/// per-frame timeout. Without it a Byzantine peer that `Ack`s and then drips a tiny
/// valid `Data` frame just inside the per-frame timeout could keep the requester's
/// read loop (and its open substream) alive far longer than any honest transfer;
/// this caps the whole post-`Ack` read so such a peer is dropped and the fan-out
/// moves on. Comfortably covers a realistic catch-up response (a few MiB) while
/// keeping an abandoned per-peer fetch task's lifetime in the same ballpark as the
/// legacy request-response path's network timeout; a peer too slow to finish in this
/// window is treated as failed and another peer (or the legacy path) is tried.
const MISSING_CERTS_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound on the opening read of a committee-tier inbound sync stream, and on each control
/// write (`Ack`, `Deny`, `End`, malformed `Err`) and the close. A peer that opens a sync stream
/// but never sends its request, or applies receive backpressure and stops reading, cannot hold
/// an admission slot indefinitely. Public-tier openings use the shorter
/// [`PUBLIC_SYNC_REQUEST_READ_TIMEOUT`].
const SYNC_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound on the opening read of a public-tier sync stream.
///
/// A public opening request is small (an epoch number, or one bitmap per authority for a
/// missing-certificates request), so one second is ample on a working link. A public opening
/// that uses more than half of this bound is charged `Penalty::Mild` whatever frame it carried,
/// so a peer cannot sit on an opening permit for most of the window at no cost
/// (GHSA-vr4r-x9mm-6x9g). Committee peers keep [`SYNC_REQUEST_READ_TIMEOUT`].
const PUBLIC_SYNC_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(1);

/// Fixed, size-independent wall-clock cap on one public-tier epoch-pack serve.
///
/// This bounds how long one public serve permit can be held, not how fast the pack streams.
/// A serve that runs past it is cut, classed as a stall and charged. It is its own constant,
/// not the consensus-output pack timeout in `handler.rs`, which bounds a much smaller message.
/// Interim limit: a pack must stream at pack length / 200 s to complete, so a long
/// high-throughput epoch can be cut before its pack ends. A follow-up scales the public bound
/// by pack length and adds a per-identity hold budget.
pub(crate) const PUBLIC_EPOCH_PACK_SERVE_TIMEOUT: Duration = Duration::from_secs(200);

/// Deadline for writing and flushing the opening request frame on the requester side.
///
/// It tracks the public opening-read bound: a responder drops a public opening that does not
/// arrive within [`PUBLIC_SYNC_REQUEST_READ_TIMEOUT`] and charges one that arrives in the
/// second half of it, so a requester that cannot flush the frame inside the same window gives
/// up instead of holding a responder opening permit.
const SYNC_REQUEST_WRITE_TIMEOUT: Duration = PUBLIC_SYNC_REQUEST_READ_TIMEOUT;

/// Bound on the opening `Req` frame of an inbound sync stream (and on the tiny
/// best-effort control writes that follow). The `EpochPack` request is a few bytes,
/// but a `MissingCertificates` request carries one skip-round bitmap per committee
/// authority, so this is sized well above the per-pack-chunk bound to admit a large
/// (but still bounded) request. An oversized request is rejected at read, after
/// which the requester simply falls back to the legacy path, so this is a safety
/// bound rather than a hard compatibility limit.
const MAX_SYNC_REQUEST_FRAME_SIZE: usize = 4 * 1024 * 1024;

/// Every `SYNC_PREFERENCE_EXPLORE_EVERY`-th probe walk is an exploration walk
/// (`PrimaryNetworkHandle::exploration_walk`, #1787): it probes the holder of the probe-first
/// preference (`last_sync_server`) last, behind every other peer, marked peers included
/// (`order_probe_peers`), and the first peer that serves it at the target takes the preference
/// and bumps the slot's generation (`record_sync_served`, `SyncPreference`).
///
/// The serve rule alone never displaces a holder that serves at exactly the target rate. On an
/// exploration walk any other peer that serves at the target takes the slot, and the generation
/// bump fences every walk that started under the old holder, so a serve by the old holder that
/// completes afterwards does not put it back. The holder keeps the slot through an exploration
/// walk only when no other peer serves that walk at the target. Once per
/// `SYNC_PREFERENCE_EXPLORE_EVERY` walks an honest fast holder is probed last and may yield the
/// slot to another peer that serves at the target. A peer cut at the target on
/// an exploration walk is marked for the epoch and sorts behind the fresh peers afterwards, so
/// each slow peer costs that at most once per epoch.
pub(crate) const SYNC_PREFERENCE_EXPLORE_EVERY: u64 = 16;

/// How many distinct peers one probe walk must see cut at the target rate before the requester
/// concludes that its own link, not the peers, is below the target (Gap 4 of #1787). The walk
/// stops its target pass at that cut, reads the rest of the walk at the floor, and sets
/// `PrimaryNetworkHandle::floor_only_this_epoch`, so every later walk of the node epoch reads
/// every peer at the floor. One cut is one slow peer; two in one walk are the requester's own
/// link. The cost is about two target-cut probes, around one `SYNC_RATE_WINDOW` each on a slow
/// link, once per node epoch, after which the requester syncs at the floor as the parent always
/// did.
const LINK_BELOW_TARGET_CUTS: usize = 2;

/// The outcome of an epoch-pack fetch attempt over the typed sync protocol.
enum EpochPackAttempt {
    /// The peer served and the pack was imported into the consensus chain.
    Imported,
    /// The peer did not answer the sync exchange; cache it unsyncable this epoch
    /// and skip it on later probes (it must upgrade to be syncable again).
    Unsupported,
    /// The peer answered but the exchange failed; try the next peer.
    Failed(NetworkError),
}

/// The outcome of a single-consensus-output fetch attempt over the typed sync
/// protocol.
///
/// Mirrors [`EpochPackAttempt`] but carries the fully decoded, header-hash-verified
/// output on success: the exchange stream-decodes the reassembled pack bytes and
/// verifies the header digest against the requested hash BEFORE buffering batches, so
/// only a verified output ever reaches the caller (a wrong-hash peer becomes `Failed`
/// and the probe falls through to the next peer instead of wedging the walk).
enum ConsensusOutputAttempt {
    /// The peer served a header-hash-verified output.
    Fetched(ConsensusOutput),
    /// The peer did not answer the sync exchange. Unlike a full-pack `Unsupported`
    /// this is NOT cached unsyncable, because a peer that speaks `/tn-primary-sync`
    /// but cannot decode this newer `ConsensusOutput` variant still serves full epoch
    /// packs and must not be hidden from the full-pack path.
    Unsupported,
    /// The peer answered but the exchange failed; try the next peer.
    Failed(NetworkError),
}

/// One epoch-pack fetch over the typed sync protocol, as threaded unchanged
/// through every per-peer probe (`request_epoch_pack_sync` down to the stream
/// exchange). Bundles what the exchange needs so each probe hop takes the peer
/// plus this instead of six loose parameters.
#[derive(Clone, Copy)]
struct EpochPackSyncRequest<'a> {
    /// The epoch whose pack is requested.
    epoch: Epoch,
    /// `None` requests the full pack; `Some(n)` requests a verifiable prefix
    /// stopping after consensus number `n`.
    last_consensus_number: Option<u64>,
    /// Record of the epoch being fetched; the streamed pack verifies against it.
    epoch_record: &'a EpochRecord,
    /// The preceding epoch's record, anchoring verification.
    previous_epoch: &'a EpochRecord,
    /// Destination chain the served pack imports into.
    consensus_chain: &'a ConsensusChain,
    /// Timeout applied when importing the streamed records.
    record_timeout: Duration,
}

/// One single-consensus-output fetch over the typed sync protocol, threaded through
/// each per-peer probe (`request_consensus_output` down to the stream exchange).
/// Bundles what the exchange needs — the destination chain (for the epoch committee)
/// and the already-verified header hash — so the reassembled bytes are stream-decoded
/// and header-hash-verified in place instead of round-tripping an unverified `Vec<u8>`
/// back to the caller.
#[derive(Clone, Copy)]
struct ConsensusOutputSyncRequest<'a> {
    /// Consensus number requested.
    number: u64,
    /// Epoch that `number` falls in; selects the committee for decoding.
    epoch: Epoch,
    /// The already-verified header digest for `number` (from validated gossip or a
    /// verified descendant's `parent_hash`). The decoded header MUST hash to this.
    expected_hash: ConsensusHeaderDigest,
    /// Chain holding the epoch's committee, used to stream-decode and verify.
    consensus_chain: &'a ConsensusChain,
}

/// The verdict of one sync probe, as `walk_probes` folds it (#1787).
enum ProbeVerdict<T> {
    /// The peer served; the walk stops here.
    Served(T),
    /// The reader cut this peer for falling below the target bound
    /// (`SyncServeMeter::cut_at_target`), so the peer is worth a floor retry in the same walk
    /// (`walk_with_floor_fallback`).
    CutAtTarget,
    /// Any other failure, or the peer did not answer the sync protocol.
    Missed,
}

/// The outcome of one pass of `walk_probes` over a probe plan (#1787).
enum WalkOutcome<T> {
    /// A peer served; no peer past it was probed.
    Served(T),
    /// No peer served.
    Unserved {
        /// The peers the reader cut for falling below the target bound in this pass, in probe
        /// order; `walk_with_floor_fallback` probes them again at the floor.
        cut_at_target: Vec<BlsPublicKey>,
    },
    /// The pass stopped at the second distinct peer the reader cut at the target
    /// (`LINK_BELOW_TARGET_CUTS`): two peers below the target in one walk mean the requester's
    /// own link is below it, so no further peer was read at the target (Gap 4 of #1787).
    LinkBelowTarget {
        /// The peers cut at the target, in probe order.
        cut_at_target: Vec<BlsPublicKey>,
        /// The peers of the plan past the second cut, not probed in this pass, in plan order.
        unprobed: Vec<BlsPublicKey>,
    },
}

/// Why `walk_probes` stopped before the end of its plan: the `Err` of its fold (#1787).
enum WalkStop<T> {
    /// A peer served.
    Served(T),
    /// A second distinct peer was cut at the target; `next` indexes the first plan entry not
    /// probed.
    LinkBelowTarget {
        /// The peers cut at the target, in probe order.
        cut_at_target: Vec<BlsPublicKey>,
        /// Index into the plan of the first entry not probed.
        next: usize,
    },
}

/// RAII guard for one admitted in-flight request holding a global concurrency permit
/// and a per-peer in-flight slot.
///
/// Holds a semaphore permit and counts toward the peer's entry in a per-peer in-flight
/// counter. Dropping it releases the global slot and decrements the per-peer count, so a
/// finished or aborted request frees capacity again. Used by the `EpochRecord`
/// request-response path ([`try_admit_epoch_record`]); the inbound sync path admits with
/// [`PeerSlots`] and [`ReservedPool`] instead.
#[derive(Debug)]
pub(crate) struct PeerSlotPermit {
    /// Global concurrency permit, released on drop.
    _permit: OwnedSemaphorePermit,
    /// Shared per-peer in-flight counter, decremented on drop.
    peers: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    /// The peer whose count this permit holds.
    peer: BlsPublicKey,
}

impl Drop for PeerSlotPermit {
    fn drop(&mut self) {
        let mut peers = self.peers.lock();
        if let Some(count) = peers.get_mut(&self.peer) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                peers.remove(&self.peer);
            }
        }
    }
}

/// Try to reserve a slot in the bounded shed-task budget.
///
/// Returns `None` when [`MAX_CONCURRENT_SHED_TASKS`] shed tasks are already in
/// flight; the caller then drops the stream without spawning. Dropping the
/// permit frees the slot for the next denied stream.
fn try_admit_shed(semaphore: &Arc<Semaphore>) -> Option<OwnedSemaphorePermit> {
    semaphore.clone().try_acquire_owned().ok()
}

/// Try to admit one `EpochRecord` request-response serve for `peer`.
///
/// Acquires a global permit from the dedicated epoch-record semaphore and admits only while the
/// peer has fewer than [`MAX_PENDING_REQUESTS_PER_PEER`] in-flight epoch-record serves. Returns
/// `None` (shedding the global permit) when either cap is hit, so the caller sheds the request
/// without spawning work. The returned [`PeerSlotPermit`] is held for the serve's lifetime and
/// frees both caps on drop. This path has its own semaphore and counter, separate from the
/// inbound sync budgets, so it neither consults nor contends them.
pub(crate) fn try_admit_epoch_record(
    semaphore: &Arc<Semaphore>,
    peers: &Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    peer: BlsPublicKey,
) -> Option<PeerSlotPermit> {
    let permit = semaphore.clone().try_acquire_owned().ok()?;
    let mut guard = peers.lock();
    let count = guard.get(&peer).copied().unwrap_or(0);
    (count < MAX_PENDING_REQUESTS_PER_PEER).then(|| {
        *guard.entry(peer).or_insert(0) += 1;
        PeerSlotPermit { _permit: permit, peers: peers.clone(), peer }
    })
}

/// The serve class of an inbound sync request. Each class has its own serve pool in
/// [`SyncServePools`], so a flood of one class cannot starve another class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncClass {
    /// A full or a partial epoch pack (`EpochPack` or `EpochPackPartial`).
    EpochPack,
    /// The certificates that a lagging peer is missing (`MissingCertificates`).
    MissingCertificates,
    /// One consensus output (`ConsensusOutput`).
    ConsensusOutput,
}

impl SyncClass {
    /// The serve class of `request`. A partial epoch pack shares the class of the full pack.
    pub(crate) fn of(request: &PrimarySyncRequest) -> Self {
        match request {
            PrimarySyncRequest::EpochPack { .. } | PrimarySyncRequest::EpochPackPartial { .. } => {
                Self::EpochPack
            }
            PrimarySyncRequest::MissingCertificates { .. } => Self::MissingCertificates,
            PrimarySyncRequest::ConsensusOutput { .. } => Self::ConsensusOutput,
        }
    }
}

/// The serve pools of the inbound sync path: one [`ReservedPool`] for each [`SyncClass`].
///
/// Each pool has [`MAX_CONCURRENT_EPOCH_STREAMS`] public permits plus
/// [`COMMITTEE_RESERVED_SYNC_STREAMS`] permits per committee key, which only that key may take.
/// A clone shares the same permits.
#[derive(Clone, Debug)]
pub(crate) struct SyncServePools {
    /// The pool for full and partial epoch-pack serves.
    epoch_pack: ReservedPool,
    /// The pool for missing-certificates serves.
    missing_certificates: ReservedPool,
    /// The pool for consensus-output serves.
    consensus_output: ReservedPool,
}

impl SyncServePools {
    /// Create the three serve pools, each with its public share and a reserve per committee key.
    pub(crate) fn new() -> Self {
        let pool =
            || ReservedPool::new(MAX_CONCURRENT_EPOCH_STREAMS, COMMITTEE_RESERVED_SYNC_STREAMS);
        Self { epoch_pack: pool(), missing_certificates: pool(), consensus_output: pool() }
    }

    /// The pool that serves requests of `class`.
    pub(crate) fn pool(&self, class: SyncClass) -> &ReservedPool {
        match class {
            SyncClass::EpochPack => &self.epoch_pack,
            SyncClass::MissingCertificates => &self.missing_certificates,
            SyncClass::ConsensusOutput => &self.consensus_output,
        }
    }
}

/// How an inbound sync serve ended, as the admission layer sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncServeOutcome {
    /// The serve finished without an error. A `Deny(Unavailable)` answer is a completed serve.
    Completed,
    /// The serve ran out of time: a write to the requester timed out, the requester read below
    /// the throughput floor or past the epoch-pack deadline, the whole serve timed out, or a
    /// public-tier epoch-pack serve outlived [`PUBLIC_EPOCH_PACK_SERVE_TIMEOUT`]. It also covers
    /// a requester that stopped or closed the stream during the serve (a reset or not-connected
    /// write error), the same set the worker uses. Only this outcome earns a penalty, and only a
    /// public-tier peer pays it.
    Stalled,
    /// The serve failed for a different reason (a local error or a malformed request). No
    /// penalty.
    Failed,
}

impl SyncServeOutcome {
    /// Classify the result of a serve. A [`PrimaryNetworkError::Timeout`] error (the whole-serve
    /// timeout) and a [`PrimaryNetworkError::StdIo`] error of kind
    /// [`std::io::ErrorKind::TimedOut`] (a write timeout, a missed floor or a missed deadline),
    /// [`std::io::ErrorKind::ConnectionReset`] or [`std::io::ErrorKind::NotConnected`] (the
    /// requester stopped or closed the stream) are [`Self::Stalled`]; a different error is
    /// [`Self::Failed`].
    pub(crate) fn of(served: &PrimaryNetworkResult<()>) -> Self {
        served.as_ref().map_or_else(
            |error| {
                let stalled = matches!(error, PrimaryNetworkError::Timeout(_))
                    || matches!(
                        error,
                        PrimaryNetworkError::StdIo(io) if matches!(
                            io.kind(),
                            std::io::ErrorKind::TimedOut
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::NotConnected
                        )
                    );
                if stalled {
                    Self::Stalled
                } else {
                    Self::Failed
                }
            },
            |&()| Self::Completed,
        )
    }
}

/// The wall-clock bound the admission layer puts on one serve, by tier and class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ServeHoldLimit {
    /// The serve runs under its own bounds only (a fixed whole-serve timeout, or the throughput
    /// floor and the deadline derived from it).
    Unbounded,
    /// The serve is dropped after this long and reported as [`SyncServeOutcome::Stalled`].
    Bounded(Duration),
}

impl ServeHoldLimit {
    /// The limit for a serve of `class` to a peer of `tier`: a public-tier epoch-pack serve is
    /// bounded by [`PUBLIC_EPOCH_PACK_SERVE_TIMEOUT`]; every other serve keeps its own bounds.
    fn of(tier: PeerTier, class: SyncClass) -> Self {
        match tier {
            PeerTier::Committee => Self::Unbounded,
            PeerTier::Public => match class {
                SyncClass::EpochPack => Self::Bounded(PUBLIC_EPOCH_PACK_SERVE_TIMEOUT),
                SyncClass::MissingCertificates | SyncClass::ConsensusOutput => Self::Unbounded,
            },
        }
    }

    /// Run `serve` under this limit. A serve cut at the limit is dropped, which resets the stream
    /// for the requester, and is reported as [`SyncServeOutcome::Stalled`].
    async fn run<Fut: Future<Output = SyncServeOutcome>>(self, serve: Fut) -> SyncServeOutcome {
        match self {
            Self::Unbounded => serve.await,
            Self::Bounded(limit) => {
                tokio::time::timeout(limit, serve).await.unwrap_or(SyncServeOutcome::Stalled)
            }
        }
    }
}

/// The grants that admit one inbound sync stream, taken before its task is spawned.
#[derive(Debug)]
pub(crate) struct SyncAdmission {
    /// The peer that opened the stream.
    peer: BlsPublicKey,
    /// The admission tier of the peer.
    tier: PeerTier,
    /// The per-peer slot, held for the whole life of the task.
    slot: PeerSlot,
    /// The opening-read permit, released as soon as the opening read ends, whatever its result.
    opening: OwnedSemaphorePermit,
}

/// The next step for an admitted sync stream once its opening read ends.
#[derive(Debug)]
enum SyncOpening {
    /// No frame arrived within [`opening_read_bound`] for the peer's tier.
    Silent,
    /// The opening frame could not be read (oversized, undecodable or a reset stream).
    Unreadable,
    /// The opening frame was not `Req`.
    Unexpected,
    /// The serve class of the request has no free permit for the peer's tier.
    AtCapacity(SyncClass),
    /// The request holds a serve permit for its class.
    Admitted {
        /// The request to serve.
        request: PrimarySyncRequest,
        /// The serve class of the request.
        class: SyncClass,
        /// The serve permit, held only while the serve runs.
        permit: OwnedSemaphorePermit,
    },
}

impl SyncOpening {
    /// Classify a readable opening frame and, for a `Req`, try to take a serve permit of its
    /// class for `peer`, a peer of `tier`. A committee-tier `peer` takes a permit from its own
    /// reserve first and then from the public share.
    fn of_frame(
        frame: SyncFrame<PrimarySyncRequest>,
        pools: &SyncServePools,
        peer: &BlsPublicKey,
        tier: PeerTier,
    ) -> Self {
        match frame {
            SyncFrame::Req(request) => {
                let class = SyncClass::of(&request);
                let admitted = |permit| Self::Admitted { request, class, permit };
                pools.pool(class).try_acquire(peer, tier).map_or(Self::AtCapacity(class), admitted)
            }
            SyncFrame::Ack
            | SyncFrame::Deny(_)
            | SyncFrame::Data(_)
            | SyncFrame::End
            | SyncFrame::Err(_) => Self::Unexpected,
        }
    }
}

/// Opening-read bound for a peer tier: public peers get [`PUBLIC_SYNC_REQUEST_READ_TIMEOUT`],
/// committee peers keep [`SYNC_REQUEST_READ_TIMEOUT`].
fn opening_read_bound(tier: PeerTier) -> Duration {
    match tier {
        PeerTier::Committee => SYNC_REQUEST_READ_TIMEOUT,
        PeerTier::Public => PUBLIC_SYNC_REQUEST_READ_TIMEOUT,
    }
}

/// Drive one admitted inbound sync stream: read the opening request, take a serve permit for its
/// class, and serve it.
///
/// The per-peer slot is held for the whole task. The opening read is bounded by
/// [`opening_read_bound`] for the peer's tier. The opening-read permit is released as soon as
/// the opening read ends, whatever its result, so no serve, control write or close holds it. The
/// serve permit is held only while `serve` runs, and a public-tier epoch-pack serve runs for at
/// most [`PUBLIC_EPOCH_PACK_SERVE_TIMEOUT`]. A public-tier peer is charged [`Penalty::Mild`] for
/// a `Silent`, `Unreadable` or `Unexpected` opening, for a public opening that arrives in the
/// second half of its bound (charged Mild whether it is then admitted or refused), for a serve
/// that reports [`SyncServeOutcome::Stalled`], and for an epoch-pack serve cut at that limit. A
/// prompt `AtCapacity` answer is not charged: an honest peer gets it when the public share of a
/// class is full, and it no longer holds a permit. The late charge is sent once the stream holds
/// no serve permit: after the serve for an admitted request, before the deny for a refused one.
pub(crate) async fn admit_and_serve_sync_stream<S, F, Fut>(
    mut stream: S,
    admission: SyncAdmission,
    serve_pools: SyncServePools,
    network_handle: PrimaryNetworkHandle,
    serve: F,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: FnOnce(S, PrimarySyncRequest) -> Fut + Send,
    Fut: Future<Output = SyncServeOutcome> + Send,
{
    let SyncAdmission { peer, tier, slot: _slot, opening } = admission;
    let (mut decode_buffer, mut decompress_buffer) = (Vec::new(), Vec::new());
    let read_bound = opening_read_bound(tier);
    let opened = tokio::time::Instant::now();
    let step = tokio::time::timeout(
        read_bound,
        read_frame::<_, PrimarySyncRequest>(
            &mut stream,
            &mut decode_buffer,
            &mut decompress_buffer,
            MAX_SYNC_REQUEST_FRAME_SIZE,
        ),
    )
    .await
    .map_or(SyncOpening::Silent, |read| {
        read.map_or_else(
            |e| {
                warn!(target: "primary::network", %peer, ?tier, ?e, "unreadable sync request frame");
                SyncOpening::Unreadable
            },
            |frame| SyncOpening::of_frame(frame, &serve_pools, &peer, tier),
        )
    });
    let elapsed = opened.elapsed();
    let late = elapsed > read_bound / 2;
    // the opening read has ended: release its permit before any serve, penalty, write or close
    drop(opening);
    match step {
        SyncOpening::Silent => {
            warn!(target: "primary::network", %peer, ?tier, "no sync request before timeout");
            charge_public_peer(&network_handle, peer, tier).await;
            let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;
        }
        SyncOpening::Unreadable => {
            charge_public_peer(&network_handle, peer, tier).await;
            let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;
        }
        SyncOpening::Unexpected => {
            warn!(target: "primary::network", %peer, ?tier, "unexpected opening sync frame");
            charge_public_peer(&network_handle, peer, tier).await;
            write_control_frame_and_close(&mut stream, SyncFrame::Err(SyncFrameError::Malformed))
                .await;
        }
        SyncOpening::AtCapacity(class) => {
            debug!(target: "primary::network", %peer, ?class, ?tier, "sync serve class full");
            // a refusal is free only when the request was prompt; the refused stream holds no
            // serve permit, so the charge goes out before the deny
            if late {
                warn!(
                    target: "primary::network", %peer, ?tier, ?elapsed, "late sync request opening"
                );
                charge_public_peer(&network_handle, peer, tier).await;
            }
            write_control_frame_and_close(&mut stream, SyncFrame::Deny(DenyReason::AtCapacity))
                .await;
        }
        SyncOpening::Admitted { request, class, permit } => {
            let outcome = ServeHoldLimit::of(tier, class).run(serve(stream, request)).await;
            // free the serve permit before any penalty send so a stall cannot hold it
            drop(permit);
            match outcome {
                SyncServeOutcome::Stalled => {
                    warn!(target: "primary::network", %peer, ?class, ?tier, "sync serve stalled");
                    charge_public_peer(&network_handle, peer, tier).await;
                }
                SyncServeOutcome::Completed | SyncServeOutcome::Failed => {}
            }
            // a request that used more than half of the opening bound is charged on top of the
            // serve outcome, so a late opening never pays less than a silent one
            if late {
                warn!(
                    target: "primary::network", %peer, ?tier, ?elapsed, "late sync request opening"
                );
                charge_public_peer(&network_handle, peer, tier).await;
            }
        }
    }
}

/// Charge a public-tier peer [`Penalty::Mild`] for holding a sync budget without progress: a
/// `Silent`, `Unreadable` or `Unexpected` opening, a public opening that arrives in the second
/// half of its bound (whether it is then admitted or refused), or a
/// [`SyncServeOutcome::Stalled`] serve (a write timeout, a missed floor or deadline, the
/// whole-serve timeout, a reset or closed stream, or a public-tier epoch-pack serve cut at
/// [`PUBLIC_EPOCH_PACK_SERVE_TIMEOUT`]). A prompt `AtCapacity` answer is not charged. A
/// committee-tier peer is never charged. Fifty mild penalties reach the default ban threshold.
async fn charge_public_peer(handle: &PrimaryNetworkHandle, peer: BlsPublicKey, tier: PeerTier) {
    match tier {
        PeerTier::Public => handle.report_penalty(peer, Penalty::Mild).await,
        PeerTier::Committee => {}
    }
}

/// Best-effort write of one control frame (`Deny` or `Err`), then a close. The write and the
/// close share one [`SYNC_REQUEST_READ_TIMEOUT`] bound, so a peer that never reads cannot pin the
/// task and the budgets that it holds.
async fn write_control_frame_and_close<S: AsyncWrite + Unpin + Send>(
    stream: &mut S,
    frame: SyncFrame<PrimarySyncRequest>,
) {
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
    let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
        let _ = write_frame(
            &mut *stream,
            &frame,
            &mut encode_buffer,
            &mut compressed_buffer,
            MAX_SYNC_REQUEST_FRAME_SIZE,
        )
        .await;
        let _ = stream.close().await;
    })
    .await;
}

/// Serve one inbound sync request through the matching request-handler path.
///
/// `EpochPack` and `EpochPackPartial` stream the full pack or its verifiable prefix,
/// `MissingCertificates` streams the matching certificates, and `ConsensusOutput` streams one
/// output's raw bytes, each as `Ack` + `Data`* + `End`.
async fn serve_sync_request<DB: Database>(
    handler: &RequestHandler<DB>,
    peer: BlsPublicKey,
    stream: Stream,
    request: PrimarySyncRequest,
    consensus_chain: &ConsensusChain,
) -> SyncServeOutcome {
    match request {
        PrimarySyncRequest::EpochPack { epoch } => {
            handler.process_sync_epoch_pack_stream(peer, stream, epoch, None, consensus_chain).await
        }
        PrimarySyncRequest::EpochPackPartial { epoch, last_consensus_number } => {
            handler
                .process_sync_epoch_pack_stream(
                    peer,
                    stream,
                    epoch,
                    Some(last_consensus_number),
                    consensus_chain,
                )
                .await
        }
        PrimarySyncRequest::MissingCertificates { exclusive_lower_bound, skip_rounds } => {
            handler
                .process_sync_missing_certs_stream(peer, stream, exclusive_lower_bound, skip_rounds)
                .await
        }
        PrimarySyncRequest::ConsensusOutput { number } => {
            handler.process_sync_consensus_output_stream(peer, stream, number).await
        }
    }
}

/// The probe-first sync preference behind a generation fence (#1787).
///
/// `peer` is the last peer that served at the target rate. `generation` counts the
/// displacements of the slot: a serve on an exploration walk and a rotation clear each bump it
/// (`displace`). A probe walk reads both when it starts, and its serve records its peer only
/// while the generation it read is still current ([`PrimaryNetworkHandle::record_sync_served`]),
/// so a walk that started under the old holder, or before the rotation, cannot put the old
/// holder back after it was displaced.
#[derive(Clone, Copy, Debug, Default)]
struct SyncPreference {
    /// The last peer that served at the target rate, if any.
    peer: Option<BlsPublicKey>,
    /// Bumped by every exploration serve and every rotation clear.
    generation: u64,
}

impl SyncPreference {
    /// Move the slot to `peer` and fence every walk that started under the old holder.
    fn displace(&mut self, peer: Option<BlsPublicKey>) {
        self.peer = peer;
        self.generation = self.generation.wrapping_add(1);
    }
}

/// What a probe walk read from the preference slot when it started (#1787), threaded through
/// the `probe_*` functions to [`PrimaryNetworkHandle::record_sync_served`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct ProbeWalk {
    /// The slot generation at walk start; a serve records its peer only while this is current.
    generation: u64,
    /// Whether this is an exploration walk (`exploration_walk`): its target serve takes the slot
    /// unconditionally and bumps the generation.
    explore: bool,
}

/// Primary network specific handle.
#[derive(Clone, Debug)]
pub struct PrimaryNetworkHandle {
    handle: NetworkHandle<Req, Res>,
    /// The genesis chain id, used to namespace gossip topics this handle publishes.
    chain_id: u64,
    /// Per-peer sync-protocol capability for epoch-pack fetch, learned by probing.
    ///
    /// Absent means not yet probed (try the sync protocol); `false` means the peer
    /// did not answer a sync open (a pre-cutover peer that fails negotiation, or one
    /// that negotiated but never `Ack`ed), so it is skipped on later probes this epoch
    /// (full-pack fetch has no legacy fallback); `true` means the peer served (or is
    /// serving) the sync protocol. Cleared each epoch via
    /// [`Self::clear_sync_capability`] so a peer upgraded over the rotation boundary
    /// is re-probed.
    sync_capability: Arc<Mutex<HashMap<BlsPublicKey, bool>>>,
    /// Per-(peer, epoch) marks: a sync probe to the peer for that fetched epoch failed, timed
    /// out, was cut by the reader, or served below the target rate ([`SyncServeSpeed::Slow`]).
    ///
    /// The probe walks shuffle their candidate peers, move every peer marked for the epoch
    /// being fetched to the BACK (`order_probe_peers`), and read a marked peer at the floor
    /// (`probe_bound`): a peer that failed once for an epoch is probed after every fresh peer
    /// (de-prioritised, not skipped) on the retries for that epoch, so a drip or slow peer, or
    /// a Byzantine peer in stable `HashMap` order, cannot stall the front of every attempt.
    ///
    /// A mark is keyed by the peer AND the fetched epoch (#1787), so a walk on another epoch
    /// never overwrites or clears it: a peer slow on epoch 3 stays at the floor for epoch 3
    /// while it is read at the target for epoch 4. The set is cleared at rotation by
    /// [`Self::clear_sync_capability`], so it holds at most one mark per peer for each epoch
    /// probed within one node epoch.
    epoch_sync_failed: Arc<Mutex<HashSet<(BlsPublicKey, Epoch)>>>,
    /// The probe-first preference ([`SyncPreference`]): the last peer that served a full pack, a
    /// partial pack or a verified consensus output at the target rate, behind a generation
    /// fence.
    ///
    /// The probe walks try this peer first unless it is marked for the epoch being fetched, so
    /// the header-by-header output walk returns to the peer that just served instead of
    /// reshuffling across the whole peer set for every number. Set only by a pack import that
    /// returned `Ok` or by a hash-verified output, never by a failed or unanswered probe, and
    /// only by a serve at the target rate ([`SyncServeSpeed::Target`]): a floor-rate serve
    /// never sets it, so the preference cannot be captured by a slow peer.
    ///
    /// A hold is bounded (#1787): every `SYNC_PREFERENCE_EXPLORE_EVERY`-th walk probes the
    /// holder last (`exploration_walk`), so a holder that serves at exactly the target is
    /// displaced by the first other peer that serves that walk at the target, and
    /// [`Self::clear_sync_capability`] drops it at rotation. Each displacement and each
    /// rotation bumps the slot's generation, and a serve records its peer only while the
    /// generation its walk read at its start is still current
    /// ([`Self::record_sync_served`]), so a walk that started under the old holder, or before
    /// the rotation, cannot put the old holder back.
    last_sync_server: Arc<Mutex<SyncPreference>>,
    /// Count of the probe walks started on this handle and its clones: the counter behind
    /// `exploration_walk` (#1787).
    probe_walks: Arc<AtomicU64>,
    /// Whether this node's own link was found below the target rate in this node epoch (Gap 4
    /// of #1787): one probe walk saw `LINK_BELOW_TARGET_CUTS` distinct peers cut at the target.
    /// While set, `walk_with_floor_fallback` plans every peer at the floor and reads nothing at
    /// the target. Reset at rotation by [`Self::clear_sync_capability`].
    floor_only_this_epoch: Arc<AtomicBool>,
}

// Test-only conversion that defaults the chain id to 0. Gated to tests so the only
// way to build a production handle is `new`, with the genesis chain id supplied
// explicitly: a 0 here would publish on the `-0` topic suffix while validators read
// the real id, a silent gossip mismatch.
#[cfg(test)]
impl From<NetworkHandle<Req, Res>> for PrimaryNetworkHandle {
    fn from(handle: NetworkHandle<Req, Res>) -> Self {
        Self {
            handle,
            chain_id: 0,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
            epoch_sync_failed: Arc::new(Mutex::new(HashSet::new())),
            last_sync_server: Arc::new(Mutex::new(SyncPreference::default())),
            probe_walks: Arc::new(AtomicU64::new(0)),
            floor_only_this_epoch: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl PrimaryNetworkHandle {
    /// Create a new instance of Self.
    pub fn new(handle: NetworkHandle<Req, Res>, chain_id: u64) -> Self {
        Self {
            handle,
            chain_id,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
            epoch_sync_failed: Arc::new(Mutex::new(HashSet::new())),
            last_sync_server: Arc::new(Mutex::new(SyncPreference::default())),
            probe_walks: Arc::new(AtomicU64::new(0)),
            floor_only_this_epoch: Arc::new(AtomicBool::new(false)),
        }
    }

    //// Convenience method for creating a new Self for tests.
    pub fn new_for_test(sender: mpsc::Sender<NetworkCommand<Req, Res>>) -> Self {
        Self {
            handle: NetworkHandle::new(sender),
            chain_id: 0,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
            epoch_sync_failed: Arc::new(Mutex::new(HashSet::new())),
            last_sync_server: Arc::new(Mutex::new(SyncPreference::default())),
            probe_walks: Arc::new(AtomicU64::new(0)),
            floor_only_this_epoch: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Clear the per-peer epoch-pack sync-capability cache, the per-(peer, epoch) failure
    /// marks, the probe-first preference and the floor-only flag.
    ///
    /// Called at each epoch boundary: committees rotate and binaries are upgraded there, so a
    /// peer that did not answer the sync protocol last epoch is re-probed for it this epoch.
    /// The preference is dropped at rotation too, and the slot's generation is bumped
    /// (`SyncPreference::displace`), so a serve in flight at the clear, whose walk read the old
    /// generation when it started, does not re-install its peer in the new epoch (#1787).
    /// Within the epoch the holder-last exploration order (`exploration_walk`) displaces a
    /// holder that serves at exactly the target rate. The floor-only flag
    /// (`floor_only_this_epoch`) is reset, so the new epoch's walks read fresh peers at the
    /// target again until two distinct peers are cut at the target in one walk.
    pub fn clear_sync_capability(&self) {
        self.sync_capability.lock().clear();
        self.epoch_sync_failed.lock().clear();
        self.last_sync_server.lock().displace(None);
        self.floor_only_this_epoch.store(false, Ordering::Relaxed);
    }

    /// Record that `peer` served a full pack, a partial pack or a verified consensus output for
    /// `epoch` at the target rate ([`SyncServeSpeed::Target`]) on `walk`.
    ///
    /// The peer is cached sync-capable and its mark for `epoch` is cleared so a peer that
    /// recovered is no longer de-prioritised for that epoch (a mark for another epoch stays,
    /// #1787). The probe-first preference is fenced by the slot's generation
    /// ([`SyncPreference`]): a serve on an exploration walk takes the slot and bumps the
    /// generation, which fences every walk that started under the old holder; a serve on any
    /// other walk takes the slot only while the generation the walk read at its start is still
    /// current, so a walk older than the last displacement or rotation cannot put its peer
    /// back. The preference lasts until another peer takes it, until `peer` fails a probe
    /// (`forget_sync_server`), or until rotation (`clear_sync_capability`), so a peer that
    /// served once and then stalls does not stay at the front of every later walk.
    pub(crate) fn record_sync_served(&self, peer: BlsPublicKey, epoch: Epoch, walk: ProbeWalk) {
        self.sync_capability.lock().insert(peer, true);
        self.epoch_sync_failed.lock().remove(&(peer, epoch));
        let mut preference = self.last_sync_server.lock();
        if walk.explore {
            preference.displace(Some(peer));
        } else if preference.generation == walk.generation {
            preference.peer = Some(peer);
        }
    }

    /// Record that `peer` served a valid pack or a verified output for `epoch`, but below the
    /// target rate ([`SyncServeSpeed::Slow`]).
    ///
    /// The peer stays sync-capable, is marked for `epoch` so it sorts behind every fresh peer
    /// and is read at the floor for the rest of the epoch (`order_probe_peers`,
    /// `probe_bound`), and never holds or keeps the probe-first preference, so a peer that
    /// serves at the floor cannot capture the walk (GHSA-8fj9-xc3h-gmxm). The bookkeeping is
    /// that of `mark_sync_failed`.
    pub(crate) fn record_sync_served_slowly(&self, peer: BlsPublicKey, epoch: Epoch) {
        self.mark_sync_failed(peer, epoch);
    }

    /// Record that a sync exchange with `peer` for `epoch` failed after the peer answered the
    /// protocol (#1787).
    ///
    /// The peer stays sync-capable, is marked for `epoch` so later walks for that epoch probe
    /// it last and at the floor, and loses the probe-first preference (`forget_sync_server`).
    pub(crate) fn mark_sync_failed(&self, peer: BlsPublicKey, epoch: Epoch) {
        self.sync_capability.lock().insert(peer, true);
        self.epoch_sync_failed.lock().insert((peer, epoch));
        self.forget_sync_server(peer);
    }

    /// The rate bound a probe of `peer` for `epoch` reads at (#1787): the floor when `peer` is
    /// marked for `epoch`, the target otherwise. A mark for another epoch does not lower it.
    pub(crate) fn probe_bound(&self, peer: BlsPublicKey, epoch: Epoch) -> SyncRateBound {
        SyncRateBound::for_probe(self.epoch_sync_failed.lock().contains(&(peer, epoch)))
    }

    /// Count one probe walk and report whether it is an exploration walk (#1787): the last
    /// walk of every run of `SYNC_PREFERENCE_EXPLORE_EVERY` probes the holder of the probe-first
    /// preference last in `order_probe_peers`, and its target serve takes the slot and bumps
    /// the generation (`record_sync_served`).
    pub(crate) fn exploration_walk(&self) -> bool {
        self.probe_walks.fetch_add(1, Ordering::Relaxed) % SYNC_PREFERENCE_EXPLORE_EVERY
            == SYNC_PREFERENCE_EXPLORE_EVERY - 1
    }

    /// Forget `peer` as the preferred sync server when it is the one on record. Every probe of
    /// `peer` that does not serve calls this, so a peer that served once and then stalls or fails
    /// does not stay at the front of every later walk. The slot's generation is NOT bumped: a
    /// walk whose preferred peer failed its probe stays current, so it can still record the
    /// next peer that serves it (#1787).
    fn forget_sync_server(&self, peer: BlsPublicKey) {
        let mut preference = self.last_sync_server.lock();
        preference.peer = preference.peer.filter(|recorded| *recorded != peer);
    }

    /// Whether `peer` is cached as not speaking the sync protocol this epoch
    /// (`sync_capability` holds `false`), so the probe walks skip it.
    fn is_known_unsyncable(&self, peer: &BlsPublicKey) -> bool {
        self.sync_capability.lock().get(peer) == Some(&false)
    }

    /// Return a reference to the inner handle.
    pub fn inner_handle(&self) -> &NetworkHandle<PrimaryRequest, PrimaryResponse> {
        &self.handle
    }

    /// Publish a certificate to the consensus network.
    pub async fn publish_certificate(&self, certificate: Certificate) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Certificate(Box::new(certificate)));
        self.handle.publish(tn_config::LibP2pConfig::primary_topic(self.chain_id), data).await?;
        Ok(())
    }

    /// Publish a consensus block number and hash of the header.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish_consensus(
        &self,
        epoch: Epoch,
        round: Round,
        consensus_block_num: u64,
        consensus_header_hash: ConsensusHeaderDigest,
        key: BlsPublicKey,
        signature: BlsSignature,
    ) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::Consensus(Box::new(ConsensusResult {
            epoch,
            round,
            number: consensus_block_num,
            hash: consensus_header_hash,
            validator: key,
            signature,
        })));
        self.handle
            .publish(tn_config::LibP2pConfig::consensus_output_topic(self.chain_id), data)
            .await?;
        Ok(())
    }

    /// Publish a certificate to the consensus network.
    pub async fn publish_epoch_vote(&self, vote: EpochVote) -> NetworkResult<()> {
        let data = encode(&PrimaryGossip::EpochVote(Box::new(vote)));
        self.handle.publish(tn_config::LibP2pConfig::epoch_vote_topic(self.chain_id), data).await?;
        Ok(())
    }

    /// Request a vote for header from the peer.
    /// Can return a response of Vote or MissingParents, other responses will be an error.
    pub async fn request_vote(
        &self,
        peer: BlsPublicKey,
        header: Header,
        parents: Vec<Certificate>,
    ) -> NetworkResult<RequestVoteResult> {
        let header = Arc::new(header);
        let request = PrimaryRequest::Vote { header: header.clone(), parents: parents.clone() };
        let res = self.handle.send_request(request, peer).await?;
        let mut res = res.await??.result;
        let mut tries = 0;
        while let PrimaryResponse::RecoverableError(PrimaryRPCError(s)) = res {
            debug!(
                target: "primary::network",
                %peer,
                error = %clip_peer_error(s),
                "recoverable vote error, retrying"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
            let request = PrimaryRequest::Vote { header: header.clone(), parents: parents.clone() };
            let res_raw = self.handle.send_request(request, peer).await?;
            res = res_raw.await??.result;
            tries += 1;
            if tries > 5 {
                break;
            }
        }
        match res {
            PrimaryResponse::Vote(vote) => Ok(RequestVoteResult::Vote(vote)),
            // still recoverable after the retries above: report it as retryable so the caller
            // backs off and asks again rather than giving up on this peer for the header
            PrimaryResponse::RecoverableError(PrimaryRPCError(s)) => {
                Err(NetworkError::RPCRetryable(clip_peer_error(s)))
            }
            PrimaryResponse::Error(PrimaryRPCError(s)) => {
                Err(NetworkError::RPCError(clip_peer_error(s)))
            }
            PrimaryResponse::MissingParents(parents) => {
                Ok(RequestVoteResult::MissingParents(parents))
            }
            PrimaryResponse::EpochRecord { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is epoch record!".to_string(),
            )),
            PrimaryResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a vote is peer exchange!".to_string(),
            )),
        }
    }

    /// Fetch missing certificates from `peer` over the typed sync protocol.
    ///
    /// Item 7 (#739) cut the certificate fetch fully over to `/tn-primary-sync`: there is
    /// no legacy request-response fallback. The exchange opens a stream, writes the
    /// request in the opening frame, reads the `Ack`, and reassembles the streamed
    /// certificate `Data` frames; any failure (negotiation, shed, transport, or protocol)
    /// returns an error so the staggered fan-out tries the next peer. The exchange is
    /// penalty-exempt, and cancelling this future (the fan-out's cancel-on-first-success)
    /// drops the in-flight substream, resetting it. The returned certificates are
    /// unverified; the caller validates them.
    pub async fn fetch_certificates(
        &self,
        peer: BlsPublicKey,
        request: MissingCertificatesRequest,
    ) -> NetworkResult<Vec<Certificate>> {
        // open the sync stream, flattening the command-channel and stream-open results. A
        // peer that does not advertise the protocol surfaces an error here; with no
        // fallback it is simply a failed peer and the fan-out moves on.
        let mut stream = self.handle.open_stream(peer).await.and_then(|s| s)?;

        // write the request in the opening frame, bounded by `SYNC_REQUEST_WRITE_TIMEOUT` so a
        // peer that grants no receive credit cannot hold this fetch pending for the life of
        // the connection (a cut write is a failed peer and the fan-out moves on)
        let request_frame = SyncFrame::Req(PrimarySyncRequest::MissingCertificates {
            exclusive_lower_bound: request.exclusive_lower_bound,
            skip_rounds: request.skip_rounds.clone(),
        });
        write_sync_request(&mut stream, &request_frame, MAX_SYNC_REQUEST_FRAME_SIZE)
            .await
            .map_err(|e| {
                NetworkError::RPCRetryable(format!(
                    "failed to write missing certificates sync request frame: {e}"
                ))
            })?;

        // read the responder's first frame, bounded by the ack timeout
        let (mut decode_buffer, mut decompress_buffer) = (Vec::new(), Vec::new());
        let first = tokio::time::timeout(
            SYNC_ACK_TIMEOUT,
            read_frame::<_, PrimarySyncRequest>(
                &mut stream,
                &mut decode_buffer,
                &mut decompress_buffer,
                sync_codec::MAX_SYNC_CERT_FRAME_SIZE,
            ),
        )
        .await
        .map_err(|_elapsed| {
            NetworkError::RPCRetryable(
                "timed out awaiting missing certificates sync ack".to_string(),
            )
        })?
        .map_err(|e| {
            NetworkError::RPCRetryable(format!(
                "failed to read missing certificates sync ack frame: {e}"
            ))
        })?;

        match first {
            // accepted: reassemble the streamed certificate `Data` frames under an overall
            // read timeout (on top of the per-frame timeout) so a peer that `Ack`s then
            // drips frames cannot pin the read loop and its substream
            SyncFrame::Ack => tokio::time::timeout(
                MISSING_CERTS_RESPONSE_TIMEOUT,
                sync_codec::read_sync_certificates(&mut stream),
            )
            .await
            .map_err(|_elapsed| {
                NetworkError::RPCRetryable(
                    "timed out reading missing certificates over sync stream".to_string(),
                )
            })?
            .map_err(|e| {
                NetworkError::RPCError(format!(
                    "failed to read missing certificates over sync stream: {e}"
                ))
            }),
            // the responder is shedding load or declines the request: try another peer
            SyncFrame::Deny(reason) => Err(NetworkError::RPCRetryable(format!(
                "peer denied missing certificates sync request: {reason:?}"
            ))),
            SyncFrame::Err(err) => Err(NetworkError::RPCError(format!(
                "peer aborted missing certificates sync exchange: {err:?}"
            ))),
            // a well-behaved responder never opens with these
            SyncFrame::Req(_) | SyncFrame::Data(_) | SyncFrame::End => Err(
                NetworkError::ProtocolError("unexpected opening sync frame from peer".to_string()),
            ),
        }
    }

    /// Fetch, decode, and header-hash-verify the consensus output for `number` from any
    /// connected peer over the typed `/tn-primary-sync` protocol.
    ///
    /// `expected_hash` is the already-verified header digest for `number` (from validated
    /// gossip or a verified descendant's `parent_hash`); `consensus_chain` supplies the epoch
    /// committee. The v1 pack is header-first, so each per-peer exchange stream-decodes the
    /// reassembled bytes and checks the header digest against `expected_hash` BEFORE buffering
    /// any batch — bounding the unverified buffer to a single header record and returning only
    /// a verified [`ConsensusOutput`]. Probes every connected peer not cached unsyncable, one at a
    /// time, and returns the first VERIFIED output, otherwise `Err` (the caller retries).
    /// A peer that serves proves it speaks the sync protocol (cached `true`); a peer that does
    /// not answer is NOT cached (an ambiguous `Unsupported`; see `ConsensusOutputAttempt`). A
    /// peer that streams a well-formed but wrong-hash output is penalized (Severe) and skipped,
    /// so the probe falls through to the next peer instead of wedging the caller's retry loop.
    pub async fn request_consensus_output(
        &self,
        number: u64,
        consensus_chain: &ConsensusChain,
        expected_hash: ConsensusHeaderDigest,
    ) -> NetworkResult<ConsensusOutput> {
        let peers = self.handle.connected_peers().await?;
        let epoch = consensus_chain.epochs().number_to_epoch(number);
        if !consensus_chain.contains_decode_epoch(epoch).await {
            // We do not have a pack file for epoch and will not be able to decode so bail now.
            // This should never happen, normally we would downloading consensus for the current
            // pack file, anything else will not be savable and would not make sense.
            // This check will just short circuit early if in an invalid state vs loop
            // over peers continually.
            return Err(NetworkError::RPCError(
                "we do not contain a pack file to decode this consensus output".to_string(),
            ));
        }
        // Resolve the committee-selecting epoch once and bundle the request so every probe hop
        // carries the chain and the verified hash needed to stream-decode + verify in place.
        let request = ConsensusOutputSyncRequest { number, epoch, expected_hash, consensus_chain };
        // Shuffle, put the last peer that served first, or last on an exploration walk
        // (`exploration_walk`), and put the peers marked for this epoch last (same ordering as
        // the full-pack path, `order_probe_peers`). `walk_with_floor_fallback` then probes one
        // peer at a time, each at its own bound (a marked peer at the floor, a fresh one at the
        // target, every peer at the floor once a walk of this node epoch found this node's own
        // link below the target), stops at the first VERIFIED output, and probes the peers the
        // first pass cut at the target again at the floor (#1787). A peer that does not serve
        // loses the preference (`forget_sync_server`), and every probe's opening write is
        // bounded by `SYNC_REQUEST_WRITE_TIMEOUT`. The preference and its generation are read
        // in one statement, whose guard drops before any await, and the failure set's guard
        // drops at the end of the ordering statement, before any probe.
        let SyncPreference { peer: preferred, generation } = *self.last_sync_server.lock();
        let walk = ProbeWalk { generation, explore: self.exploration_walk() };
        let peers = order_probe_peers(
            peers,
            &self.epoch_sync_failed.lock(),
            epoch,
            preferred,
            walk.explore,
            &mut rand::rng(),
        );
        self.walk_with_floor_fallback(peers, epoch, move |peer, bound| {
            self.probe_consensus_output(peer, request, bound, walk)
        })
        .await
        .ok_or_else(|| {
            NetworkError::RPCError(
                "no peer served the consensus output over the sync protocol".to_string(),
            )
        })
    }

    /// Probe `peer` once for the consensus output `request` names, reading it at `bound`, and
    /// classify the exchange for `walk_probes` (#1787).
    ///
    /// A serve records the peer by its measured speed for `request.epoch` on `walk`
    /// (`record_sync_served` or `record_sync_served_slowly`) and is `Served`. A peer that does
    /// not answer is NOT cached (an ambiguous `Unsupported`; see `ConsensusOutputAttempt`) and
    /// is `Missed`. A failed exchange marks the peer for the epoch (`mark_sync_failed`) and is
    /// `CutAtTarget` when the reader cut the peer for falling below the target bound, else
    /// `Missed`.
    async fn probe_consensus_output(
        &self,
        peer: BlsPublicKey,
        request: ConsensusOutputSyncRequest<'_>,
        bound: SyncRateBound,
        walk: ProbeWalk,
    ) -> ProbeVerdict<ConsensusOutput> {
        let ConsensusOutputSyncRequest { number, epoch, .. } = request;
        let meter = SyncServeMeter::default();
        let started = tokio::time::Instant::now();
        match self.sync_consensus_output_from_peer(peer, request, bound, meter.clone()).await {
            ConsensusOutputAttempt::Fetched(output) => {
                let elapsed = started.elapsed();
                let speed = meter.speed(elapsed);
                match speed {
                    SyncServeSpeed::Target => self.record_sync_served(peer, epoch, walk),
                    SyncServeSpeed::Slow => self.record_sync_served_slowly(peer, epoch),
                }
                debug!(
                    target: "primary::network",
                    %peer,
                    number,
                    bytes = meter.bytes(),
                    elapsed_ms = elapsed.as_millis(),
                    ?speed,
                    "fetched verified consensus output over sync protocol"
                );
                ProbeVerdict::Served(output)
            }
            ConsensusOutputAttempt::Unsupported => {
                // Ambiguous: the peer may serve full epoch packs but not decode
                // this newer `ConsensusOutput` variant, so do NOT cache `false`
                // and hide it from the full-pack path. It did not serve, so it
                // loses the probe-first preference if it held it.
                self.forget_sync_server(peer);
                debug!(
                    target: "primary::network",
                    %peer,
                    number,
                    "peer did not answer consensus output sync; skipping this probe"
                );
                ProbeVerdict::Missed
            }
            ConsensusOutputAttempt::Failed(e) => {
                // the peer speaks sync but this exchange failed; keep it
                // sync-capable, mark it for this epoch, drop its probe-first
                // preference, and try the next peer
                self.mark_sync_failed(peer, epoch);
                warn!(
                    target: "primary::network",
                    %peer,
                    number,
                    ?e,
                    "consensus output sync exchange failed; trying next peer"
                );
                if meter.cut_at_target() {
                    ProbeVerdict::CutAtTarget
                } else {
                    ProbeVerdict::Missed
                }
            }
        }
    }

    /// Walk `plan` one peer at a time, reading each peer at its planned bound, and stop at the
    /// first peer that serves, so no peer past it is probed, or at the second distinct peer the
    /// reader cuts at the target (`LINK_BELOW_TARGET_CUTS`), so no further peer is read at a
    /// bound this node's own link cannot meet (#1787).
    ///
    /// A peer cached unsyncable (`is_known_unsyncable`) is skipped. The check runs as the walk
    /// reaches the peer, so a peer that an earlier probe in the same walk cached unsyncable is
    /// skipped too. Returns the served value, the peers the reader cut at the target in probe
    /// order, or, after the second cut, those peers plus the peers of the plan not yet probed.
    async fn walk_probes<T, F, Fut>(
        &self,
        plan: Vec<(BlsPublicKey, SyncRateBound)>,
        probe: F,
    ) -> WalkOutcome<T>
    where
        F: Fn(BlsPublicKey, SyncRateBound) -> Fut,
        Fut: Future<Output = ProbeVerdict<T>>,
    {
        let walked = futures::stream::iter(plan.iter().copied().enumerate())
            .filter(|(_, (peer, _))| futures::future::ready(!self.is_known_unsyncable(peer)))
            .then(|(index, (peer, bound))| {
                probe(peer, bound).map(move |verdict| match verdict {
                    ProbeVerdict::Served(served) => Err(WalkStop::Served(served)),
                    ProbeVerdict::CutAtTarget => Ok((index, Some(peer))),
                    ProbeVerdict::Missed => Ok((index, None)),
                })
            })
            // `Err` short-circuits the fold: a serve, so no peer past it is probed, or the
            // second distinct peer cut at the target, so the rest of the plan is left for a
            // floor pass. `Ok` carries the peers cut at the target so far, in probe order.
            .try_fold(Vec::new(), |cut: Vec<BlsPublicKey>, (index, cut_peer)| {
                let cut_at_target: Vec<BlsPublicKey> = cut
                    .into_iter()
                    .filter(|earlier| Some(*earlier) != cut_peer)
                    .chain(cut_peer)
                    .collect();
                futures::future::ready(if cut_at_target.len() < LINK_BELOW_TARGET_CUTS {
                    Ok(cut_at_target)
                } else {
                    Err(WalkStop::LinkBelowTarget { cut_at_target, next: index + 1 })
                })
            })
            .await;
        walked.map_or_else(
            |stop| match stop {
                WalkStop::Served(served) => WalkOutcome::Served(served),
                WalkStop::LinkBelowTarget { cut_at_target, next } => {
                    let unprobed = plan.iter().skip(next).map(|(peer, _)| *peer).collect();
                    WalkOutcome::LinkBelowTarget { cut_at_target, unprobed }
                }
            },
            |cut_at_target| WalkOutcome::Unserved { cut_at_target },
        )
    }

    /// Walk `peers` in the given order with a floor retry (#1787).
    ///
    /// Pass 1 reads every peer at its own bound (`probe_bound`: the floor for a peer marked for
    /// `epoch`, the target otherwise). When no peer serves, pass 2 probes again, at the floor
    /// and with no sleep, the peers that pass 1 cut at the target, so one peer cut at the
    /// target costs the walk one floor retry rather than a failure.
    ///
    /// Two distinct peers cut at the target in one pass mean this node's own link, not the
    /// peers, is below the target (Gap 4 of #1787): pass 1 stops at the second cut
    /// (`LINK_BELOW_TARGET_CUTS`), the rest of this walk (the cut peers, then the peers not yet
    /// probed) is read at the floor, and `floor_only_this_epoch` is set, so every later walk of
    /// this node epoch plans every peer at the floor and reads nothing at the target. The flag
    /// is reset at rotation (`clear_sync_capability`). A slow requester therefore pays about two
    /// target-cut probes, around one `SYNC_RATE_WINDOW` each, once per node epoch, and then
    /// syncs at the floor as the parent always did. A floor read that completes at the target
    /// speed takes the preference and one that does not is marked slow, as for any serve
    /// (`probe_consensus_output`, `probe_epoch_pack`). Returns the first served value, or
    /// `None` when no pass serves.
    async fn walk_with_floor_fallback<T, F, Fut>(
        &self,
        peers: Vec<BlsPublicKey>,
        epoch: Epoch,
        probe: F,
    ) -> Option<T>
    where
        F: Fn(BlsPublicKey, SyncRateBound) -> Fut,
        Fut: Future<Output = ProbeVerdict<T>>,
    {
        let floor_only = self.floor_only_this_epoch.load(Ordering::Relaxed);
        let plan = peers
            .into_iter()
            .map(|peer| {
                let bound =
                    if floor_only { SyncRateBound::Floor } else { self.probe_bound(peer, epoch) };
                (peer, bound)
            })
            .collect();
        match self.walk_probes(plan, &probe).await {
            WalkOutcome::Served(served) => Some(served),
            WalkOutcome::Unserved { cut_at_target } if cut_at_target.is_empty() => None,
            WalkOutcome::Unserved { cut_at_target } => {
                info!(
                    target: "primary::network",
                    epoch,
                    peers = cut_at_target.len(),
                    "no peer served at the target rate; probing the peers cut at the target \
                     again at the floor"
                );
                self.walk_floor_pass(cut_at_target, &probe).await
            }
            WalkOutcome::LinkBelowTarget { cut_at_target, unprobed } => {
                self.floor_only_this_epoch.store(true, Ordering::Relaxed);
                info!(
                    target: "primary::network",
                    epoch,
                    cut = cut_at_target.len(),
                    unprobed = unprobed.len(),
                    "two peers cut at the target rate in one walk, so this node's link is below \
                     the target; reading every peer at the floor for the rest of the epoch"
                );
                let rest: Vec<BlsPublicKey> = cut_at_target.into_iter().chain(unprobed).collect();
                self.walk_floor_pass(rest, &probe).await
            }
        }
    }

    /// Read `peers` at the floor, in order, and return the first served value (#1787). A floor
    /// pass has nothing left to retry, so any unserved outcome is `None`.
    async fn walk_floor_pass<T, F, Fut>(&self, peers: Vec<BlsPublicKey>, probe: F) -> Option<T>
    where
        F: Fn(BlsPublicKey, SyncRateBound) -> Fut,
        Fut: Future<Output = ProbeVerdict<T>>,
    {
        let plan = peers.into_iter().map(|peer| (peer, SyncRateBound::Floor)).collect();
        match self.walk_probes(plan, probe).await {
            WalkOutcome::Served(served) => Some(served),
            WalkOutcome::Unserved { .. } | WalkOutcome::LinkBelowTarget { .. } => None,
        }
    }

    /// Run one consensus-output sync exchange against `peer`, flattening the classified
    /// outcome into a single [`ConsensusOutputAttempt`].
    ///
    /// `bound` is the rate the reader (`sync_codec::sync_pack_reader`) holds the peer to and
    /// `meter` counts the bytes it delivers, so the walk can judge the serve's rate.
    async fn sync_consensus_output_from_peer(
        &self,
        peer: BlsPublicKey,
        request: ConsensusOutputSyncRequest<'_>,
        bound: SyncRateBound,
        meter: SyncServeMeter,
    ) -> ConsensusOutputAttempt {
        self.try_sync_consensus_output_exchange(peer, request, bound, meter)
            .await
            .map_or_else(|attempt| attempt, ConsensusOutputAttempt::Fetched)
    }

    /// Open a `/tn-primary-sync` stream, write the consensus-output request in the
    /// opening frame, read the `Ack`, then stream-decode and header-hash-verify the output.
    ///
    /// `Err(ConsensusOutputAttempt::Unsupported)` means the peer did not answer the
    /// protocol (negotiation failed, or it negotiated but never `Ack`ed - e.g. a peer
    /// that speaks `/tn-primary-sync` but cannot decode the newer `ConsensusOutput`
    /// variant and shut the stream). `Err(ConsensusOutputAttempt::Failed(_))` means a
    /// transient or exchange-level error (including a decode/verification failure, which
    /// is also penalized) once the peer has proved sync-capable. A transport I/O error
    /// during the open (`UpgradeIo`) is transient rather than a protocol mismatch, so it
    /// maps to `Failed`. `Ok` is always a header-hash-verified output.
    ///
    /// `bound` is the rate the reader (`sync_codec::sync_pack_reader`) holds the peer to and
    /// `meter` counts the bytes it delivers, so the walk can judge the serve's rate.
    async fn try_sync_consensus_output_exchange(
        &self,
        peer: BlsPublicKey,
        request: ConsensusOutputSyncRequest<'_>,
        bound: SyncRateBound,
        meter: SyncServeMeter,
    ) -> Result<ConsensusOutput, ConsensusOutputAttempt> {
        // open the sync stream, flattening the command-channel and stream-open results.
        // Only a genuine negotiation failure (`UpgradeFailed`) is a peer that does not
        // advertise the protocol -> Unsupported; a transient upgrade I/O error or any
        // other open error is not proof the peer lacks sync -> try next.
        let mut stream =
            self.handle.open_stream(peer).await.and_then(|s| s).map_err(|e| match () {
                () if matches!(e, NetworkError::Stream(StreamError::UpgradeFailed)) => {
                    ConsensusOutputAttempt::Unsupported
                }
                () => ConsensusOutputAttempt::Failed(e),
            })?;

        // write the request in the opening frame, bounded by `SYNC_REQUEST_WRITE_TIMEOUT`.
        // Negotiation already succeeded, so a write failure or a write still pending at the
        // deadline (a peer that grants no receive credit) is transient -> try next.
        let max_frame = sync_codec::MAX_SYNC_PACK_FRAME_SIZE;
        let req_frame =
            SyncFrame::Req(PrimarySyncRequest::ConsensusOutput { number: request.number });
        write_sync_request(&mut stream, &req_frame, max_frame).await.map_err(|e| {
            ConsensusOutputAttempt::Failed(NetworkError::RPCRetryable(format!(
                "failed to write consensus output sync request frame: {e}"
            )))
        })?;

        // read the responder's first frame. A timeout (outer `Err`) means no `Ack` -> a
        // peer that negotiated sync but does not serve this variant, so try the next
        // peer. A clean close on the read (inner `Err`) is the same Unsupported signal;
        // any other read error after negotiation is transient -> Failed.
        let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        let first = tokio::time::timeout(
            SYNC_ACK_TIMEOUT,
            read_frame::<_, PrimarySyncRequest>(
                &mut stream,
                &mut decode_buffer,
                &mut compressed_buffer,
                max_frame,
            ),
        )
        .await
        .map_err(|_elapsed| ConsensusOutputAttempt::Unsupported)?
        .map_err(|e| {
            if matches!(
                e.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::BrokenPipe
            ) {
                ConsensusOutputAttempt::Unsupported
            } else {
                ConsensusOutputAttempt::Failed(NetworkError::RPCRetryable(format!(
                    "failed to read consensus output sync ack frame: {e}"
                )))
            }
        })?;

        match first {
            // accepted: stream-decode the reassembled `Data`/`End` frames straight into the
            // pack decoder, verifying the header digest against the requested (already-
            // verified) hash the instant the header record is read — before any batch is
            // buffered. A wrong-hash or malformed output fails here (and is penalized below),
            // so the probe falls through to the next peer instead of returning unverified
            // bytes. An empty stream yields `NotConsensus`, i.e. a miss -> next peer.
            SyncFrame::Ack => {
                let reader = sync_codec::sync_pack_reader(stream, bound, meter);
                match request
                    .consensus_chain
                    .stream_decode_consensus_output(
                        request.epoch,
                        reader,
                        request.expected_hash,
                        sync_codec::SYNC_RECORD_TIMEOUT,
                    )
                    .await
                {
                    Ok(output) => Ok(output),
                    Err(e) => {
                        // A classified protocol/verification violation (a header-digest
                        // mismatch -> Severe) scores the peer down; transient decode/IO errors
                        // carry no penalty. Either way, drop this peer and try the next.
                        if let Some(penalty) = Self::consensus_chain_error_to_penalty(&e) {
                            self.report_penalty(peer, penalty).await;
                        }
                        Err(ConsensusOutputAttempt::Failed(NetworkError::RPCError(format!(
                            "failed to decode/verify consensus output over sync stream: {e}"
                        ))))
                    }
                }
            }
            // sync-capable, but shedding load or lacking the output: try next peer
            SyncFrame::Deny(reason) => {
                Err(ConsensusOutputAttempt::Failed(NetworkError::RPCRetryable(format!(
                    "peer denied consensus output sync request: {reason:?}"
                ))))
            }
            SyncFrame::Err(err) => Err(ConsensusOutputAttempt::Failed(NetworkError::RPCError(
                format!("peer aborted consensus output sync exchange: {err:?}"),
            ))),
            // a well-behaved responder never opens with these
            SyncFrame::Req(_) | SyncFrame::Data(_) | SyncFrame::End => {
                Err(ConsensusOutputAttempt::Failed(NetworkError::ProtocolError(
                    "unexpected opening sync frame from peer".to_string(),
                )))
            }
        }
    }

    /// Request an epoch record and its certificate from a random peer, up to three times from
    /// three different peers.
    ///
    /// The error tells a definitive answer apart from a failed exchange:
    /// - [`NetworkError::RPCError`]: at least one peer answered that it holds no certified record
    ///   for the request ([`PrimaryResponse::unavailable_epoch_record`]) and no peer returned one;
    /// - [`NetworkError::RPCRetryable`]: no peer answered either way. There was no connected peer,
    ///   the request was dropped (a peer between epochs drops inbound primary requests), the
    ///   responder's identity was not resolved yet, or the reply was another error or an unexpected
    ///   response.
    ///
    /// A closed network command channel returns its own error at once.
    pub async fn request_epoch_cert(
        &self,
        epoch: Option<Epoch>,
        hash: Option<EpochDigest>,
    ) -> NetworkResult<(EpochRecord, EpochCertificate)> {
        let request = PrimaryRequest::EpochRecord { epoch, hash };
        let unavailable = PrimaryResponse::unavailable_epoch_record(epoch, hash);
        let mut answered = false;
        // Try up to three times (from three peers) to get consensus.
        // This could be a lot more complicated but this KISS method should work fine.
        for _ in 0..3 {
            let res = self.handle.send_request_any(request.clone()).await?;
            match res.await {
                Ok(Ok(NetworkResponseMessage {
                    peer: _,
                    result: PrimaryResponse::EpochRecord { record, certificate },
                })) => return Ok((record, certificate)),
                Ok(Ok(NetworkResponseMessage { peer: _, result })) => {
                    answered |= result == unavailable;
                }
                // no peer, a dropped request or an unresolved responder: no answer either way
                Ok(Err(_)) | Err(_) => {}
            }
        }
        if answered {
            Err(NetworkError::RPCError("Peers do not have the epoch record".to_string()))
        } else {
            Err(NetworkError::RPCRetryable("Could not get the epoch record!".to_string()))
        }
    }

    /// Report a penalty to the network's peer manager.
    async fn report_penalty(&self, peer: BlsPublicKey, penalty: Penalty) {
        self.handle.report_penalty(peer, penalty).await;
    }

    /// Retrieve the count of connected peers.
    pub async fn connected_peers_count(&self) -> NetworkResult<usize> {
        self.handle.connected_peer_count().await
    }

    /// Attempt to get a complete epoch pack file from any peer over the typed
    /// `/tn-primary-sync` protocol.
    ///
    /// Full-pack fetch is typed-only (no legacy fallback): this returns `Err`, and
    /// the caller retries, when no connected peer successfully serves a pack over
    /// the sync protocol (no peers, all unsupported, or all failed to import). See
    /// [`Self::request_epoch_pack_sync`] for the per-peer probe behavior. The partial
    /// prefix ([`Self::request_partial_epoch_pack`]) reuses the same probe over the
    /// sync protocol but keeps a legacy fallback.
    pub async fn request_epoch_pack(
        &self,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        record_timeout: Duration,
    ) -> NetworkResult<()> {
        self.request_epoch_pack_inner(
            epoch_record,
            previous_epoch,
            consensus_chain,
            None,
            record_timeout,
        )
        .await
    }

    /// Attempt to get a verifiable PREFIX of an epoch's pack file from any peer via stream,
    /// stopping after consensus number `last_consensus_number`.
    ///
    /// Behaves exactly like [`Self::request_epoch_pack`] but negotiates a partial transfer. The
    /// caller must supply an `epoch_record` whose `final_consensus` matches the partial stop point
    /// so the streamed prefix verifies. Used to fetch the in-progress current epoch up to a
    /// known point.
    pub async fn request_partial_epoch_pack(
        &self,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        last_consensus_number: u64,
        record_timeout: Duration,
    ) -> NetworkResult<()> {
        self.request_epoch_pack_inner(
            epoch_record,
            previous_epoch,
            consensus_chain,
            Some(last_consensus_number),
            record_timeout,
        )
        .await
    }

    /// Shared implementation for full ([`Self::request_epoch_pack`]) and partial
    /// ([`Self::request_partial_epoch_pack`]) epoch pack streaming. A full pack
    /// (`last_consensus_number` is `None`) is fetched over the typed sync protocol
    /// only; a partial prefix (`Some`) is fetched over the sync protocol first and
    /// falls back to the legacy stream path for peers that do not yet serve the
    /// `EpochPackPartial` sync variant.
    async fn request_epoch_pack_inner(
        &self,
        epoch_record: &EpochRecord,
        previous_epoch: &EpochRecord,
        consensus_chain: &ConsensusChain,
        last_consensus_number: Option<u64>,
        record_timeout: Duration,
    ) -> NetworkResult<()> {
        let epoch = epoch_record.epoch;

        // Both full and partial epoch packs are typed-sync-only (#739, step 9c): the
        // legacy `StreamEpoch`/`StreamEpochPartial` raw-stream fallback was removed, so
        // a fetch hard-fails (and the caller retries) when no connected peer serves the
        // `/tn-primary-sync` protocol, rather than syncing over the legacy stream path.
        // This forces peers to upgrade. `request_epoch_pack_sync` selects the full pack
        // for `None` and the verifiable prefix for `Some(n)`.
        self.request_epoch_pack_sync(EpochPackSyncRequest {
            epoch,
            last_consensus_number,
            epoch_record,
            previous_epoch,
            consensus_chain,
            record_timeout,
        })
        .await
    }

    /// Attempt to fetch and import a full or partial epoch pack over the typed sync protocol.
    ///
    /// Probes every connected peer not cached unsyncable this epoch, one at a time,
    /// opening a `/tn-primary-sync` stream whose opening frame carries the request.
    /// Returns `Ok(())` as soon as one peer serves a pack that imports; otherwise `Err`
    /// (there is no legacy fallback, so the caller retries). A peer that does not answer
    /// a full-pack request is cached unsyncable (skipping its probe next time); a
    /// partial-pack `Unsupported` is ambiguous and is not cached. A probe that never
    /// reaches `Ack` is penalty-exempt; an import fault after `Ack` that is attributable
    /// to the peer's bytes is penalised. Every phase of a probe is bounded (stream open,
    /// the opening request write under `SYNC_REQUEST_WRITE_TIMEOUT`, the `Ack` read, and
    /// the reader's per-frame timeout plus throughput bound after it), and the last peer
    /// that served is probed first only until it fails a probe, so one stalled peer costs
    /// one bounded probe per walk rather than the walk. Only a serve at the target rate makes
    /// a peer the preferred server; a slower completed serve marks it for the epoch
    /// (`record_sync_served_slowly`), so a peer that serves at the floor cannot capture the
    /// walk. Every `SYNC_PREFERENCE_EXPLORE_EVERY`-th walk probes the preferred peer last, and
    /// when no peer serves, the peers the reader cut at the target are probed again at the
    /// floor (`walk_with_floor_fallback`, #1787).
    async fn request_epoch_pack_sync(&self, sync: EpochPackSyncRequest<'_>) -> NetworkResult<()> {
        let EpochPackSyncRequest { epoch, .. } = sync;
        let peers = self.handle.connected_peers().await?;
        // Shuffle, put the last peer that served first, or last on an exploration walk
        // (`exploration_walk`), and put the peers marked for this epoch last: a peer that
        // failed once for this epoch is probed after every fresh peer, so a Byzantine peer
        // fixed in stable `HashMap` order cannot stall the front of every one of state-sync's
        // retries for the same epoch (see `order_probe_peers`). `walk_with_floor_fallback` then
        // probes one peer at a time, each at its own bound, stops at the first pack that
        // imports, and probes the peers the first pass cut at the target again at the floor
        // (#1787). A peer that does not serve loses the preference (`forget_sync_server`), and
        // every probe's opening write is bounded by `SYNC_REQUEST_WRITE_TIMEOUT`. The
        // preference and its generation are read in one statement, whose guard drops before
        // any await, and the failure set's guard drops at the end of the ordering statement,
        // before any probe.
        let SyncPreference { peer: preferred, generation } = *self.last_sync_server.lock();
        let walk = ProbeWalk { generation, explore: self.exploration_walk() };
        let peers = order_probe_peers(
            peers,
            &self.epoch_sync_failed.lock(),
            epoch,
            preferred,
            walk.explore,
            &mut rand::rng(),
        );
        self.walk_with_floor_fallback(peers, epoch, move |peer, bound| {
            self.probe_epoch_pack(peer, sync, bound, walk)
        })
        .await
        .ok_or_else(|| {
            NetworkError::RPCError(
                "no peer served the epoch pack over the sync protocol".to_string(),
            )
        })
    }

    /// Probe `peer` once for the epoch pack `sync` names, reading it at `bound`, and classify
    /// the exchange for `walk_probes` (#1787).
    ///
    /// An import records the peer by its measured speed for `sync.epoch` on `walk`
    /// (`record_sync_served` or `record_sync_served_slowly`) and is `Served`. A peer that does
    /// not answer is cached unsyncable only for a full pack and is `Missed`. A failed exchange
    /// marks the peer for the epoch (`mark_sync_failed`) and is `CutAtTarget` when the reader
    /// cut the peer for falling below the target bound, else `Missed`.
    async fn probe_epoch_pack(
        &self,
        peer: BlsPublicKey,
        sync: EpochPackSyncRequest<'_>,
        bound: SyncRateBound,
        walk: ProbeWalk,
    ) -> ProbeVerdict<()> {
        let EpochPackSyncRequest { epoch, last_consensus_number, .. } = sync;
        let meter = SyncServeMeter::default();
        let started = tokio::time::Instant::now();
        match self.sync_epoch_pack_from_peer(peer, sync, bound, meter.clone()).await {
            EpochPackAttempt::Imported => {
                let elapsed = started.elapsed();
                let speed = meter.speed(elapsed);
                match speed {
                    SyncServeSpeed::Target => self.record_sync_served(peer, epoch, walk),
                    SyncServeSpeed::Slow => self.record_sync_served_slowly(peer, epoch),
                }
                info!(
                    target: "primary::network",
                    %peer,
                    epoch,
                    bytes = meter.bytes(),
                    elapsed_ms = elapsed.as_millis(),
                    ?speed,
                    "streamed epoch pack file over sync protocol"
                );
                ProbeVerdict::Served(())
            }
            EpochPackAttempt::Unsupported => {
                // Only a FULL-pack `Unsupported` authoritatively proves the peer
                // does not speak `/tn-primary-sync`. A PARTIAL `Unsupported` is
                // ambiguous: the peer may be on an earlier item that serves full
                // packs but cannot decode the newer `EpochPackPartial` variant, so
                // it must not cache `false` and hide that peer from full-pack sync.
                // (Partial still honors a `false` a full probe wrote and caches
                // `true` on success, so it dedups without poisoning.)
                if last_consensus_number.is_none() {
                    self.sync_capability.lock().insert(peer, false);
                }
                // it did not serve, so it loses the probe-first preference if it held it
                self.forget_sync_server(peer);
                debug!(
                    target: "primary::network",
                    %peer,
                    partial = last_consensus_number.is_some(),
                    "peer did not answer epoch pack sync; skipping this probe"
                );
                ProbeVerdict::Missed
            }
            EpochPackAttempt::Failed(e) => {
                // the peer speaks sync but this exchange failed; keep it
                // sync-capable, drop its probe-first preference, and try the
                // next peer. Remember it failed THIS epoch so the retries for the
                // same epoch probe it last and at the floor (a drip/slow peer no
                // longer starves every attempt).
                self.mark_sync_failed(peer, epoch);
                warn!(
                    target: "primary::network",
                    %peer,
                    epoch,
                    ?e,
                    "epoch pack sync exchange failed; trying next peer"
                );
                if meter.cut_at_target() {
                    ProbeVerdict::CutAtTarget
                } else {
                    ProbeVerdict::Missed
                }
            }
        }
    }

    /// Run one epoch-pack sync exchange against `peer`, flattening the classified
    /// outcome into a single [`EpochPackAttempt`].
    ///
    /// `bound` is the rate the reader (`sync_codec::sync_pack_reader`) holds the peer to and
    /// `meter` counts the bytes it delivers, so the walk can judge the serve's rate.
    async fn sync_epoch_pack_from_peer(
        &self,
        peer: BlsPublicKey,
        sync: EpochPackSyncRequest<'_>,
        bound: SyncRateBound,
        meter: SyncServeMeter,
    ) -> EpochPackAttempt {
        self.try_sync_epoch_pack_exchange(peer, sync, bound, meter)
            .await
            .map_or_else(|attempt| attempt, |()| EpochPackAttempt::Imported)
    }

    /// Open a `/tn-primary-sync` stream, write the epoch-pack request in the opening
    /// frame, and stream the pack into the consensus chain. `last_consensus_number`
    /// selects the transfer: `None` requests the full pack ([`PrimarySyncRequest::EpochPack`])
    /// and imports it in place via [`ConsensusChain::stream_import`]; `Some(number)`
    /// requests a verifiable prefix ([`PrimarySyncRequest::EpochPackPartial`]) and imports
    /// it into a side staging dir via [`ConsensusChain::import_partial_to_staging`].
    ///
    /// `Err(EpochPackAttempt::Unsupported)` means the peer did not answer the
    /// protocol (negotiation failed, or it negotiated but never `Ack`ed), so it is
    /// cached unsyncable and skipped on later probes this epoch.
    /// `Err(EpochPackAttempt::Failed(_))` means a transient or exchange-level error
    /// once the peer has proved sync-capable, so the caller keeps it sync-capable and
    /// tries the next peer. A transport I/O
    /// error during the open (`UpgradeIo`) is transient rather than a protocol
    /// mismatch, so it maps to `Failed` instead of poisoning the capability cache.
    ///
    /// `bound` is the rate the reader (`sync_codec::sync_pack_reader`) holds the peer to and
    /// `meter` counts the bytes it delivers, so the walk can judge the serve's rate.
    async fn try_sync_epoch_pack_exchange(
        &self,
        peer: BlsPublicKey,
        sync: EpochPackSyncRequest<'_>,
        bound: SyncRateBound,
        meter: SyncServeMeter,
    ) -> Result<(), EpochPackAttempt> {
        let EpochPackSyncRequest {
            epoch,
            last_consensus_number,
            epoch_record,
            previous_epoch,
            consensus_chain,
            record_timeout,
        } = sync;
        // open the sync stream, flattening the command-channel and stream-open
        // results. Only a genuine negotiation failure (`UpgradeFailed`) is a
        // pre-cutover peer that does not advertise the protocol -> Unsupported
        // (penalty-exempt, cached unsyncable this epoch). A transient upgrade I/O
        // error or any other open error is not proof the peer lacks sync -> try next
        // peer.
        let mut stream =
            self.handle.open_stream(peer).await.and_then(|s| s).map_err(|e| match () {
                () if matches!(e, NetworkError::Stream(StreamError::UpgradeFailed)) => {
                    EpochPackAttempt::Unsupported
                }
                () => EpochPackAttempt::Failed(e),
            })?;

        // write the request in the opening frame, bounded by `SYNC_REQUEST_WRITE_TIMEOUT`.
        // Negotiation already succeeded, so the peer is sync-capable; a write failure or a
        // write still pending at the deadline (a peer that grants no receive credit) is
        // transient -> try next.
        let max_frame = sync_codec::MAX_SYNC_PACK_FRAME_SIZE;
        // full pack vs a verifiable partial prefix: select the request variant through
        // the `Option`'s combinator (no Some/None branch).
        let request = SyncFrame::Req(
            last_consensus_number.map_or(PrimarySyncRequest::EpochPack { epoch }, |number| {
                PrimarySyncRequest::EpochPackPartial { epoch, last_consensus_number: number }
            }),
        );
        write_sync_request(&mut stream, &request, max_frame).await.map_err(|e| {
            EpochPackAttempt::Failed(NetworkError::RPCRetryable(format!(
                "failed to write epoch pack sync request frame: {e}"
            )))
        })?;

        // read the responder's first frame. Negotiation already succeeded, so the
        // peer IS sync-capable: a missing `Ack` (timeout) or any read error is a
        // transient exchange failure, not a capability signal. Both map to `Failed`
        // (try the next peer, keep the peer sync-capable) rather than `Unsupported`,
        // which for a full pack would cache the peer unsyncable for the whole epoch
        // with no fallback (#739 completed the sync-only cutover). Only a negotiation
        // failure at open time (`UpgradeFailed`, above) proves the peer lacks sync.
        let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        let first = tokio::time::timeout(
            SYNC_ACK_TIMEOUT,
            read_frame::<_, PrimarySyncRequest>(
                &mut stream,
                &mut decode_buffer,
                &mut compressed_buffer,
                max_frame,
            ),
        )
        .await
        .map_err(|_elapsed| {
            EpochPackAttempt::Failed(NetworkError::RPCRetryable(
                "timed out reading epoch pack sync ack frame".to_string(),
            ))
        })?
        .map_err(|e| {
            EpochPackAttempt::Failed(NetworkError::RPCRetryable(format!(
                "failed to read epoch pack sync ack frame: {e}"
            )))
        })?;

        match first {
            // accepted: reassemble the `Data`/`End` frames into a contiguous reader
            // and import it. A full pack imports in place; a partial prefix imports to
            // a side staging dir so it cannot race the in-order build of the current
            // epoch's main pack (matching the legacy responder split). A bad pack is
            // the peer's fault on the legacy path, but the sync path is metrics-only
            // during rollout: classify it `Failed` (try next peer) without a penalty.
            SyncFrame::Ack => {
                let reader = sync_codec::sync_pack_reader(stream, bound, meter);
                // The reader's own per-frame timeout and throughput floor cut a stalled or
                // dribbling peer; the per-record timeout must not cut a transfer the floor admits.
                let record_timeout = record_timeout.max(sync_codec::SYNC_RECORD_TIMEOUT);
                // Pick the import target on `last_consensus_number.is_some()`; both resolve to
                // `Result<(), ConsensusChainError>`, so box to unify the future types while keeping
                // the TYPED error (a peer-caused stream fault must be penalised before it is
                // flattened into `Failed`). A partial prefix imports to a side
                // staging dir so it cannot race the in-order build of the current
                // epoch's main pack; a full pack imports in place.
                let import: futures::future::BoxFuture<'_, Result<(), ConsensusChainError>> =
                    if last_consensus_number.is_some() {
                        consensus_chain
                            .import_partial_to_staging(
                                reader,
                                epoch_record,
                                previous_epoch,
                                record_timeout,
                            )
                            .boxed()
                    } else {
                        consensus_chain
                            .stream_import(reader, epoch_record, previous_epoch, record_timeout)
                            .boxed()
                    };
                // No fixed overall wall-clock cap: the reader (`sync_pack_reader`) enforces a
                // rate bound over a rolling window plus a per-frame timeout. A fresh peer must
                // sustain `SYNC_TARGET_MIN_BYTES` per window and a de-prioritised peer the floor,
                // both prorated over the real window length, and the `End` frame is checked too,
                // so one slow probe costs about one window; the probe-first preference needs a
                // target-rate serve. On such an error the
                // future resolves `Err`, which unwinds `stream_import` so its
                // `ImportPath` guard removes the import dir, releases the epoch's claim, and the
                // epoch is retryable against another peer. The responder applies
                // `PUBLIC_EPOCH_PACK_SERVE_TIMEOUT` to a public requester's serve, so a pack cut
                // there ends the stream before its `End` frame (the reader reports an early EOF as
                // `ConnectionAborted`), and the requester retries from the next peer.
                match import.await {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        // Charge the peer only for a fault attributable SOLELY to its streamed
                        // bytes (never our local storage/IO).
                        // Committee peers stay score-exempt for liveness. Operator trust alone
                        // does not exempt these protocol penalties. Still classified `Failed`
                        // so the probe moves to the next peer, regardless of scoring policy.
                        if Self::import_fault_is_peer_caused(&e) {
                            if let Some(penalty) = Self::consensus_chain_error_to_penalty(&e) {
                                self.report_penalty(peer, penalty).await;
                            }
                        }
                        Err(EpochPackAttempt::Failed(NetworkError::RPCError(format!(
                            "failed to import epoch pack over sync stream: {e}"
                        ))))
                    }
                }
            }
            // sync-capable, but shedding load or lacking the pack: try next peer
            SyncFrame::Deny(reason) => Err(EpochPackAttempt::Failed(NetworkError::RPCRetryable(
                format!("peer denied epoch pack sync request: {reason:?}"),
            ))),
            SyncFrame::Err(err) => Err(EpochPackAttempt::Failed(NetworkError::RPCError(format!(
                "peer aborted epoch pack sync exchange: {err:?}"
            )))),
            // a well-behaved responder never opens with these
            SyncFrame::Req(_) | SyncFrame::Data(_) | SyncFrame::End => {
                Err(EpochPackAttempt::Failed(NetworkError::ProtocolError(
                    "unexpected opening sync frame from peer".to_string(),
                )))
            }
        }
    }

    /// Helper to convert a consensus chain error to penalty.
    /// This error does not have network knowledge so do it here for the
    /// streaming case vs with the error.
    fn consensus_chain_error_to_penalty(error: &ConsensusChainError) -> Option<Penalty> {
        match error {
            ConsensusChainError::PackError(pack_error) => match pack_error {
                PackError::MissingBatch
                | PackError::NotConsensus
                | PackError::NotBatch
                | PackError::NotEpoch
                // A record out of place or not what the header declares, or one that frames but
                // fails its CRC/decode: the sender's bytes are bad. Medium, not Severe: an honest
                // peer serving a pack damaged at rest on its own disk produces the same bytes.
                | PackError::UnexpectedRecord(_)
                | PackError::UndecodableRecord(_) => Some(Penalty::Medium),
                PackError::InvalidConsensusChain
                | PackError::ExtraBatches
                | PackError::MissingBatches
                | PackError::TooManyBatches(_)
                | PackError::CorruptPack(_)
                | PackError::UnexpectedConsensusDigest { .. }
                | PackError::EmptySubDag
                // An oversized batch is peer misbehavior (the batch validator caps legitimate
                // batches at `max_batch_size`); charge Severe.
                | PackError::BatchTooLarge { .. }
                // A crafted output whose buffered batches exceed the per-output decoded-memory budget
                // is peer misbehavior (a memory-exhaustion attempt); charge Severe.
                | PackError::OutputTooLarge { .. }
                // A non-advancing / gapped / over-`final` consensus number is peer misbehavior (a peer
                // trying to wedge a fetch/import with a non-advancing chain); charge Severe.
                | PackError::InvalidConsensusNumber(_, _)
                | PackError::InvalidEpoch(_, _) => Some(Penalty::Severe),
                PackError::IO(_)
                | PackError::BatchLoad(_)
                | PackError::EpochLoad(_)
                | PackError::Append(_)
                | PackError::IndexAppend(_)
                | PackError::Fetch(_)
                | PackError::Open(_)
                | PackError::ReadOnly
                | PackError::ReadError(_)
                | PackError::MissingAuthority
                // The caller's committee or previous record is for another epoch: local state.
                | PackError::LocalEpochMismatch { .. }
                | PackError::SendFailed
                | PackError::ReceiveFailed
                | PackError::PersistError(_)
                | PackError::ConsensusNumberAlreadyAdded
                | PackError::ConflictingOutput { .. }
                | PackError::ConsensusNumberTooLow
                | PackError::InvalidVersion(_, _)
                | PackError::ConsensusNumberTooHigh => None,
            },
            ConsensusChainError::EpochMismatch
            | ConsensusChainError::PrevCommitteeEpochMismatch
            | ConsensusChainError::CrcError => Some(Penalty::Mild),
            ConsensusChainError::StreamUnavailable
            | ConsensusChainError::NoCurrentEpoch
            | ConsensusChainError::EpochDbError(_)
            | ConsensusChainError::InvalidPackEpoch(_, _)
            | ConsensusChainError::CantSaveAndNotAvailable(_)
            | ConsensusChainError::LiveEpochImport(_)
            // The documented cause is LOCAL state (a resume that collapsed this node's
            // "latest"), not peer misbehavior, so no penalty is charged to the peer.
            | ConsensusChainError::NonMonotonicConsensusNumber { .. }
            | ConsensusChainError::IO(_) => None,
        }
    }

    /// Whether an epoch-pack IMPORT failure can be attributed SOLELY to the peer's streamed bytes,
    /// so it is safe to charge the peer a penalty (via
    /// [`Self::consensus_chain_error_to_penalty`]).
    ///
    /// The import builds a fresh pack from a single peer's stream, so a structural/framing fault in
    /// that stream is unambiguously the peer's fault. But the same error types are ALSO produced by
    /// local storage/IO during the import (a full disk, a failed mmap, an index write), and those
    /// must never ban an honest peer. This whitelist admits only the peer-stream-caused
    /// variants and excludes every local/ambiguous one — notably `CorruptPack` (also produced
    /// by local recovery), all IO/Append/Persist/Open/EpochDb variants, and the
    /// local-chain-state mismatches. Exhaustive so a new error variant forces a deliberate
    /// classification here.
    fn import_fault_is_peer_caused(error: &ConsensusChainError) -> bool {
        match error {
            ConsensusChainError::PackError(pack_error) => match pack_error {
                // The peer's streamed pack is structurally malformed / mis-framed / over a resource
                // bound: solely attributable to the bytes it sent.
                PackError::MissingBatch
                | PackError::NotConsensus
                | PackError::NotBatch
                | PackError::NotEpoch
                | PackError::UnexpectedRecord(_)
                | PackError::UndecodableRecord(_)
                | PackError::InvalidConsensusChain
                | PackError::ExtraBatches
                | PackError::MissingBatches
                | PackError::TooManyBatches(_)
                | PackError::UnexpectedConsensusDigest { .. }
                | PackError::EmptySubDag
                | PackError::BatchTooLarge { .. }
                | PackError::OutputTooLarge { .. }
                | PackError::InvalidConsensusNumber(_, _)
                | PackError::InvalidEpoch(_, _) => true,
                // Local storage / IO / decode-of-our-own-state, or ambiguous: never charge the
                // peer. `CorruptPack` is excluded deliberately — it is also
                // produced by local recovery, so on the import path it is treated
                // as possibly-local.
                PackError::CorruptPack(_)
                | PackError::IO(_)
                | PackError::BatchLoad(_)
                | PackError::EpochLoad(_)
                | PackError::Append(_)
                | PackError::IndexAppend(_)
                | PackError::Fetch(_)
                | PackError::Open(_)
                | PackError::ReadOnly
                | PackError::ReadError(_)
                | PackError::MissingAuthority
                // The caller's committee or previous record is for another epoch: local state.
                | PackError::LocalEpochMismatch { .. }
                | PackError::SendFailed
                | PackError::ReceiveFailed
                | PackError::PersistError(_)
                | PackError::ConsensusNumberAlreadyAdded
                | PackError::ConflictingOutput { .. }
                | PackError::ConsensusNumberTooLow
                | PackError::InvalidVersion(_, _)
                | PackError::ConsensusNumberTooHigh => false,
            },
            // Local chain state, our own storage, or a benign mismatch: never charge the peer.
            ConsensusChainError::EpochMismatch
            | ConsensusChainError::PrevCommitteeEpochMismatch
            | ConsensusChainError::CrcError
            | ConsensusChainError::StreamUnavailable
            | ConsensusChainError::NoCurrentEpoch
            | ConsensusChainError::EpochDbError(_)
            | ConsensusChainError::InvalidPackEpoch(_, _)
            | ConsensusChainError::CantSaveAndNotAvailable(_)
            | ConsensusChainError::LiveEpochImport(_)
            | ConsensusChainError::NonMonotonicConsensusNumber { .. }
            | ConsensusChainError::IO(_) => false,
        }
    }
}

/// Why the opening request frame of an outbound sync stream was not delivered.
#[derive(Debug)]
enum SyncRequestWriteError {
    /// The write or the flush did not finish within [`SYNC_REQUEST_WRITE_TIMEOUT`].
    TimedOut,
    /// The write or the flush failed.
    Io(std::io::Error),
}

impl std::fmt::Display for SyncRequestWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => write!(f, "timed out after {SYNC_REQUEST_WRITE_TIMEOUT:?}"),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

/// Write and flush `frame` as the opening request of an outbound sync stream, bounded by
/// [`SYNC_REQUEST_WRITE_TIMEOUT`]. A timeout drops the in-flight write; the caller drops the
/// stream, which resets it, and treats the peer as a failed probe.
async fn write_sync_request<S, R>(
    stream: &mut S,
    frame: &SyncFrame<R>,
    max_frame: usize,
) -> Result<(), SyncRequestWriteError>
where
    S: AsyncWrite + Unpin + Send,
    R: serde::Serialize + Sync,
{
    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
    tokio::time::timeout(SYNC_REQUEST_WRITE_TIMEOUT, async {
        write_frame(stream, frame, &mut encode_buffer, &mut compressed_buffer, max_frame).await?;
        stream.flush().await
    })
    .await
    .map_err(|_elapsed| SyncRequestWriteError::TimedOut)
    .and_then(|written| written.map_err(SyncRequestWriteError::Io))
}

/// Order the candidate peers for a sync probe: shuffle for an unbiased base order, then stably
/// sort them into groups, each keeping its shuffled order. On an ordinary walk the groups are:
/// the `preferred` peer (the last one that served) if it is fresh, the other fresh peers, the
/// `preferred` peer if it is marked for THIS `epoch` in `failed` (it failed, timed out, was cut,
/// or served below the target rate on a probe for this epoch), and the other peers marked for
/// this epoch. On an exploration walk (`explore`, see `SYNC_PREFERENCE_EXPLORE_EVERY`) the
/// holder goes last: the other fresh peers, the other marked peers, then the `preferred` peer,
/// so the holder is probed only after every other peer, marked peers included, failed to serve
/// (#1787). A mark for another epoch does not count. A `preferred` peer that is not in `peers`
/// changes nothing. Pure so it is unit-testable with a seeded rng.
///
/// The shuffle defeats the stable-`HashMap`-order starvation (a Byzantine peer fixed at the
/// front of every walk), and the de-prioritisation keeps a slow/drip peer that failed one call
/// from being probed first again on the retries for the same epoch, without dropping it (a
/// small network whose only peer had a transient failure is still retried, just last). A stale
/// `preferred` peer costs at most one probe per walk until another peer serves and takes the
/// preference; once a probe to it fails it also sorts behind every fresh peer for the rest of
/// the epoch. The holder-last exploration order means a holder cannot keep the front of every
/// walk by being the only unmarked peer: an exploration walk reaches the marked peers before it.
fn order_probe_peers(
    mut peers: Vec<BlsPublicKey>,
    failed: &HashSet<(BlsPublicKey, Epoch)>,
    epoch: Epoch,
    preferred: Option<BlsPublicKey>,
    explore: bool,
    rng: &mut impl Rng,
) -> Vec<BlsPublicKey> {
    peers.shuffle(rng);
    // `sort_by_key` is stable and `false < true`. On an ordinary walk the groups come out as
    // fresh preferred, fresh, marked preferred, marked; on an exploration walk as fresh, marked,
    // preferred, each keeping its shuffled order.
    peers.sort_by_key(|p| {
        let (marked, held) = (failed.contains(&(*p, epoch)), Some(*p) == preferred);
        if explore {
            (held, marked)
        } else {
            (marked, !held)
        }
    });
    peers
}

/// Handle inter-node communication between primaries.
#[derive(Debug)]
pub struct PrimaryNetwork<DB, Events> {
    /// Receiver for network events.
    network_events: Events,
    /// Network handle to send commands.
    network_handle: PrimaryNetworkHandle,
    /// Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// Hold a reference to the consensus chain.
    consensus_chain: ConsensusChain,
    /// Opening-read budget for inbound sync streams: [`MAX_CONCURRENT_OPENING_READS`] public
    /// permits plus [`COMMITTEE_RESERVED_OPENING_READS`] permits per committee key, which only
    /// that key may take. A permit is held only while the opening request frame is read (bounded
    /// by [`opening_read_bound`], 1 s public, 5 s committee).
    sync_opening: ReservedPool,
    /// Serve pools for inbound sync requests, one per [`SyncClass`]. A permit is held only while
    /// its request is served.
    sync_serve: SyncServePools,
    /// Per-peer cap on in-flight inbound sync streams across every phase, at
    /// [`MAX_PENDING_REQUESTS_PER_PEER`]. A slot is held for the whole life of the stream task.
    sync_peer_slots: PeerSlots,
    /// Per-peer cap on gossip in signature verification, at [`MAX_PENDING_GOSSIP_PER_PEER`]
    /// (#1772). A slot is held from admission until the handler's verdict.
    gossip_slots: GossipSlots,
    /// BLS keys of the current and the next committee, captured when the network is built for
    /// the epoch. A peer in this set may take the committee reserve of every sync budget.
    reserved_peers: ReservedPeers,
    /// Semaphore bounding concurrent shed tasks for denied sync streams.
    ///
    /// A permit is held for the lifetime of a shed task (the bounded
    /// `Deny(AtCapacity)` write), so [`MAX_CONCURRENT_SHED_TASKS`] caps the
    /// spawn fan-out from over-cap substream bursts.
    shed_task_semaphore: Arc<Semaphore>,
    /// Semaphore bounding concurrent `EpochRecord` request-response serves.
    ///
    /// Separate from the inbound sync budgets (`sync_opening`, `sync_serve`) so an epoch-record
    /// flood and the stream-serving paths never starve one another. See
    /// [`MAX_CONCURRENT_EPOCH_RECORD_REQUESTS`].
    epoch_record_semaphore: Arc<Semaphore>,
    /// Per-peer count of in-flight `EpochRecord` request-response serves, capped at
    /// [`MAX_PENDING_REQUESTS_PER_PEER`].
    epoch_record_peers: Arc<Mutex<HashMap<BlsPublicKey, usize>>>,
    /// Counts the `EpochRecord` requests dropped at the admission cap, in the primary network's
    /// `inbound_requests_shed_total{class="epoch_record", reason="admission"}` series.
    epoch_record_admission_shed: AdmissionShed,
}

impl<DB, Events> PrimaryNetwork<DB, Events>
where
    DB: Database,
    Events: TnReceiver<NetworkEvent<Req, Res>> + 'static,
{
    /// Create a new instance of Self.
    pub fn new(
        network_events: Events,
        network_handle: PrimaryNetworkHandle,
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBusApp,
        state_sync: StateSynchronizer<DB>,
        task_spawner: TaskSpawner,
        consensus_chain: ConsensusChain,
    ) -> Self {
        // capture the committee tier before `consensus_config` moves into the request handler:
        // the current and the next committee may take the reserve of every sync budget
        let committee_keys = consensus_config.committee().bls_keys();
        let next_keys = consensus_config.next_committee_keys().iter().copied();
        let reserved_peers = ReservedPeers::new(committee_keys.into_iter().chain(next_keys));
        let request_handler = RequestHandler::new(
            consensus_config,
            consensus_bus,
            state_sync.clone(),
            consensus_chain.clone(),
        );
        Self {
            network_events,
            network_handle,
            request_handler,
            task_spawner,
            consensus_chain,
            sync_opening: ReservedPool::new(
                MAX_CONCURRENT_OPENING_READS,
                COMMITTEE_RESERVED_OPENING_READS,
            ),
            sync_serve: SyncServePools::new(),
            sync_peer_slots: PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER),
            gossip_slots: GossipSlots::new(MAX_PENDING_GOSSIP_PER_PEER),
            reserved_peers,
            shed_task_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SHED_TASKS)),
            epoch_record_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_EPOCH_RECORD_REQUESTS)),
            epoch_record_peers: Arc::new(Mutex::new(HashMap::default())),
            epoch_record_admission_shed: AdmissionShed::epoch_record(),
        }
    }

    pub fn handle(&self) -> &PrimaryNetworkHandle {
        &self.network_handle
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_critical_task("primary network events", async move {
            loop {
                match self.network_events.recv().await {
                    Some(event) => {
                        self.process_network_event(event);
                    }
                    None => {
                        warn!(target: "primary::network", "critical worker network events channel dropped");
                        break Err(TaskError::from_message("critical worker network events channel dropped"));
                    }
                }
            }
        });
    }

    /// Handle events concurrently.
    fn process_network_event(&mut self, event: NetworkEvent<Req, Res>) {
        // match event
        match event {
            NetworkEvent::Request { peer, request, channel, cancel } => match request {
                PrimaryRequest::Vote { header, parents } => {
                    self.process_vote_request(
                        peer,
                        Arc::unwrap_or_clone(header),
                        parents,
                        channel,
                        cancel,
                    );
                }
                PrimaryRequest::PeerExchange { .. } => {
                    warn!(target: "primary::network", "primary application received unexpected peer exchange message");
                }
                PrimaryRequest::EpochRecord { epoch, hash } => {
                    self.process_epoch_record_request(peer, epoch, hash, channel, cancel)
                }
            },
            NetworkEvent::Gossip(gossip) => {
                self.process_gossip(*gossip);
            }
            NetworkEvent::Error(msg, channel) => {
                let err = PrimaryResponse::Error(PrimaryRPCError(msg));
                let network_handle = self.network_handle.clone();
                self.task_spawner.spawn_task("report request error", async move {
                    let _ = network_handle.handle.send_response(err, channel).await;
                    Ok(())
                });
            }
            NetworkEvent::InboundStream { peer, stream } => {
                self.process_inbound_sync_stream(peer, stream)
            }
        }
    }

    /// Process vote request.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    fn process_vote_request(
        &self,
        peer: BlsPublicKey,
        header: Header,
        parents: Vec<Certificate>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // Serve budget: offer the request to the vote class before any work for it. `None` means
        // the class has no free permit; the shed is already counted under `serve_budget`.
        // Dropping the response channel and the cancel receiver sends no reply, so the swarm
        // also counts the request as an `omitted` failure, exactly as an admission shed does.
        // The default budget is unbounded, so this only observes until an operator sets
        // `serve_budget.vote`. An enforcing vote class also caps each peer at
        // `MAX_PERMITS_PER_PEER`, so one committee peer cannot hold the whole pool while its
        // requests wait on its author lock in `RequestHandler::vote`.
        if let Some(serve_permit) =
            self.network_handle.handle.serve_permits().try_acquire(ServiceClass::Vote, peer)
        {
            // clone for spawned tasks
            let request_handler = self.request_handler.clone();
            let network_handle = self.network_handle.clone();
            let task_name = format!("VoteRequest-{}", header.digest());

            self.task_spawner.spawn_task(task_name, async move {
                // hold the serve permit for the task's lifetime, so every exit releases it
                let _serve_permit = serve_permit;
                tokio::select! {
                    vote = request_handler.vote(peer, header, parents) => {
                        // report penalty if any
                        //
                        // votes are consensus-critical, so a peer that returns a penalizable
                        // error must pay the same reputational cost as on every other request
                        // handler. Route the error through the central PrimaryNetworkError →
                        // Penalty table before collapsing it into a response, instead of
                        // silently dropping it.
                        if let Err(ref e) = vote {
                            if let Some(penalty) = e.into() {
                                network_handle.report_penalty(peer, penalty).await;
                            }
                        }

                        let response = vote.into_response();
                        let _ = network_handle.handle.send_response(response, channel).await;
                    }
                    // cancel notification from network layer
                    _ = cancel => (),
                }
                Ok(())
            });
        }
    }

    /// Attempt to retrieve consensus chain header from the database.
    fn process_epoch_record_request(
        &self,
        peer: BlsPublicKey,
        epoch: Option<Epoch>,
        hash: Option<EpochDigest>,
        channel: ResponseChannel<PrimaryResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // Admission control: bound concurrent epoch-record serving globally and per peer.
        //
        // This request-response arm is reachable by any peer whose identity resolves (not
        // committee membership) and each serve does real work (a database read) while returning
        // no penalty for an unavailable epoch. Without a bound a peer could spawn unbounded,
        // penalty-free serve tasks. The permit is held for the serve's lifetime. A record without
        // its certificate is answered as unavailable at once (no wait), so that lifetime is only
        // the lookup and the response. See GHSA-vc2r-9cp2-w74j.
        let Some(permit) =
            try_admit_epoch_record(&self.epoch_record_semaphore, &self.epoch_record_peers, peer)
        else {
            // At capacity: shed in O(1) by dropping the response channel rather than spawning
            // work, so a flood beyond the cap costs only a permit try. Dropping the channel sends
            // no rejection over the wire, so the caller (`request_epoch_cert`) rotates to another
            // peer only after its request-response timeout elapses; that is added catch-up latency
            // on a background path, not a hang, and only while this primary is at capacity.
            // Replying "at capacity" instead would reintroduce a per-request task spawn, which is
            // exactly what this bound removes.
            self.epoch_record_admission_shed.record();
            return;
        };

        // Serve budget: after the admission cap, offer the request to the epoch-record class.
        // `None` sheds it the same way as the admission cap does; the shed is already counted
        // under `serve_budget`, and the admission permit releases on return. The default budget
        // is unbounded, so this only observes until an operator sets `serve_budget.epoch_record`.
        if let Some(serve_permit) =
            self.network_handle.handle.serve_permits().try_acquire(ServiceClass::EpochRecord, peer)
        {
            // clone for spawned tasks
            let request_handler = self.request_handler.clone();
            let network_handle = self.network_handle.clone();
            let task_name = format!("ConsensusOutputReq-{peer}");
            self.task_spawner.spawn_task(task_name, async move {
                // hold the admission permit for the serve's lifetime; dropping it on completion
                // (or cancel) frees the global slot and decrements the per-peer count
                let _permit = permit;
                // hold the serve permit for the task's lifetime, so every exit releases it
                let _serve_permit = serve_permit;
                tokio::select! {
                    header =
                        request_handler.retrieve_epoch_record(epoch, hash) => {
                            // penalize peer's reputation for bad request
                            if let Err(err) = &header {
                                if let Some(penalty) = err.into() {
                                    network_handle.report_penalty(peer, penalty).await;
                                }
                            }
                            let response = header.into_response();
                            let _ = network_handle.handle.send_response(response, channel).await;
                        }
                    // cancel notification from network layer
                    _ = cancel => (),
                }
                Ok(())
            });
        }
    }

    /// Process gossip from committee.
    ///
    /// Each message first takes a verification slot keyed by its author, else its relayer, else
    /// one shared bucket for gossip with neither identity resolved, at
    /// [`MAX_PENDING_GOSSIP_PER_PEER`] per key (#1772). With no free slot the message is not
    /// processed: its ticket drops and reports `Ignore`, and no peer is charged. The slot is held
    /// only through decode, the committee lookup and the signature check, and the handler frees it
    /// at the verdict. Downstream work on verified gossip is not bounded here. That is safe because
    /// only signed messages from committee members, and certificates signed by a quorum of the
    /// committee, reach it.
    ///
    /// A fault of the strike class on a frame that the author delivered itself also strikes the
    /// author in [`GossipSlots`] (see [`gossip_author_to_strike`]): that author's
    /// gossip is ignored for [`GOSSIP_AUTHOR_COOLDOWN_BASE`], doubling per strike up to
    /// [`GOSSIP_AUTHOR_COOLDOWN_MAX`], because the peer manager exempts committee keys from
    /// penalties and the cooldown is the cost the author pays.
    fn process_gossip(&self, gossip: GossipPayload<Req, Res>) {
        let GossipPayload { message: msg, relayer, author, validation } = gossip;
        self.gossip_slots.admit(validation, author, relayer).map_or_else(
            || {
                debug!(
                    target: "primary::network",
                    ?author,
                    ?relayer,
                    "gossip verification slots exhausted, ignoring"
                );
            },
            |mut pending| {
                // clone for spawned tasks
                let request_handler = self.request_handler.clone();
                let network_handle = self.network_handle.clone();
                let gossip_slots = self.gossip_slots.clone();
                let relayer_label = relayer
                    .as_ref()
                    .map_or_else(|| "unresolved".to_string(), |bls| bls.to_string());
                let task_name = format!("ProcessGossip-{}-{relayer_label}", msg.topic);
                // spawn task to process gossip
                self.task_spawner.spawn_task(task_name, async move {
                    if let Err(e) = request_handler.process_gossip_with(&msg, &mut pending).await {
                        warn!(target: "primary::network", ?e, "process_gossip");
                        // Read before the settle, which consumes the ticket.
                        let was_pending = pending.is_pending();
                        // Report Reject for an author content fault while the ticket is pending.
                        // Any other error drops the ticket, which reports Ignore (#1772).
                        pending.settle_error(&e).await;
                        // The peer manager exempts committee keys from every penalty, so a fault
                        // of the strike class on the author's own delivery also cools down the
                        // author's gossip here. A relayed frame strikes no one (GHSA-vw36).
                        gossip_author_to_strike(&e, was_pending, author, relayer)
                            .into_iter()
                            .for_each(|struck| {
                                gossip_slots.strike(struck);
                                debug!(
                                    target: "primary::network",
                                    author = %struck,
                                    "author content fault on the author's own delivery, cooling down the author's gossip"
                                );
                            });
                        // Charge the accountable peer, and only once its BLS identity has resolved.
                        // A content-determined fault (malformed payload /
                        // mis-topic) is the message author's: the network
                        // layer forwarded it after a shallow check, so an honest
                        // relayer could not have screened it, and banning the relayer lets a
                        // Byzantine author partition the mesh (issues
                        // #801/#819). Every other fault is the relaying
                        // peer's, as before. `zip` skips the penalty when that peer is unresolved
                        // or the error carries no penalty.
                        let charged = if e.is_author_content_fault() { author } else { relayer };
                        if let Some((peer, penalty)) = charged.zip(Option::<Penalty>::from(&e)) {
                            network_handle.report_penalty(peer, penalty).await;
                        }
                        Err(e.into())
                    } else {
                        Ok(())
                    }
                });
            },
        );
    }

    /// Process an inbound sync stream.
    ///
    /// Admission runs on stream open, ahead of any task spawn (#1308). The stream takes a slot
    /// under the per-peer cap ([`MAX_PENDING_REQUESTS_PER_PEER`]) and a permit from the
    /// opening-read budget ([`MAX_CONCURRENT_OPENING_READS`], plus
    /// [`COMMITTEE_RESERVED_OPENING_READS`] per committee key). A stream that misses
    /// either takes the bounded shed path ([`Self::shed_inbound_sync_stream`]), which writes
    /// [`DenyReason::AtCapacity`] without reading so the requester retries elsewhere at once.
    ///
    /// An admitted stream runs [`admit_and_serve_sync_stream`] on its own task. The task reads
    /// the opening request frame (bounded by [`opening_read_bound`]), releases the
    /// opening-read permit, and takes a permit from the serve pool of the request's class
    /// ([`SyncServePools`]) before [`RequestHandler`] serves it. The per-peer slot is held for the
    /// life of the task and the serve permit only while the serve runs, so a silent peer holds no
    /// serve capacity. Each key of the current or the next committee may take its own reserve
    /// in each budget, and no other key can take from it. So neither peers outside the committee
    /// nor other committee keys can stop a lagging validator from syncing. See
    /// GHSA-9h5w-9282-pc3g and issue #1750.
    fn process_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        let tier = self.reserved_peers.tier(&peer);
        // take both admission grants before spawning; a stream that misses either takes the
        // bounded shed path instead, so over-cap substream bursts cannot fan out past the shed
        // budget (#1308)
        let Some((slot, opening)) =
            self.sync_peer_slots.try_take(peer).zip(self.sync_opening.try_acquire(&peer, tier))
        else {
            self.shed_inbound_sync_stream(peer, stream);
            return;
        };
        let admission = SyncAdmission { peer, tier, slot, opening };
        let serve_pools = self.sync_serve.clone();
        let handle = self.network_handle.clone();
        let request_handler = self.request_handler.clone();
        let consensus_chain = self.consensus_chain.clone();
        let task_name = format!("sync-epoch-pack-{peer}");
        self.task_spawner.spawn_task(task_name, async move {
            let serve = move |stream, request| async move {
                serve_sync_request(&request_handler, peer, stream, request, &consensus_chain).await
            };
            admit_and_serve_sync_stream(stream, admission, serve_pools, handle, serve).await;
            Ok(())
        });
    }

    /// Shed a denied inbound sync stream on a budgeted task.
    ///
    /// Spawns a short-lived task that writes [`DenyReason::AtCapacity`] and
    /// closes, so the requester fails fast and tries elsewhere. The spawn is
    /// gated by [`MAX_CONCURRENT_SHED_TASKS`]: past that budget the stream is
    /// dropped without spawning (the requester sees a reset), so a burst of
    /// over-cap substreams cannot fan out to unbounded tasks (#1308).
    fn shed_inbound_sync_stream(&self, peer: BlsPublicKey, stream: Stream) {
        try_admit_shed(&self.shed_task_semaphore).map_or_else(
            || {
                debug!(target: "primary::network", %peer, "dropping inbound sync stream: shed budget exhausted");
            },
            |shed_permit| {
                let task_name = format!("shed-sync-epoch-pack-{peer}");
                self.task_spawner.spawn_task(task_name, async move {
                    // hold the shed budget slot for the lifetime of the task
                    let _shed_permit = shed_permit;
                    let mut stream = stream;
                    let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
                    debug!(target: "primary::network", %peer, "denying inbound sync stream: at capacity");
                    // bound the best-effort shed write: a peer that applies receive
                    // backpressure and never reads must not pin the shed budget slot.
                    let _ = tokio::time::timeout(SYNC_REQUEST_READ_TIMEOUT, async {
                        let _ = write_frame(
                            &mut stream,
                            &SyncFrame::<PrimarySyncRequest>::Deny(DenyReason::AtCapacity),
                            &mut encode_buffer,
                            &mut compressed_buffer,
                            MAX_SYNC_REQUEST_FRAME_SIZE,
                        )
                        .await;
                        let _ = stream.close().await;
                    })
                    .await;
                    Ok(())
                });
            },
        )
    }
}

/// Defines how the network receiver handles incoming workers messages.
///
/// One instance is bound to each worker's `LocalNetwork` with that worker's id: the binding is
/// what establishes the reporting worker's identity. A message whose stamped id disagrees with
/// the instance it arrived on is rejected before it reaches the proposer's digest channel or
/// the payload store, so a mis-wired (or, once workers are remote, misbehaving) worker cannot
/// impersonate another worker's lane.
#[derive(Clone)]
pub(super) struct WorkerReceiverHandler<DB> {
    /// The worker id this instance is bound to.
    worker_id: WorkerId,
    consensus_bus: ConsensusBus,
    payload_store: DB,
}

impl<DB: PayloadStore> WorkerReceiverHandler<DB> {
    /// Create a new instance of Self bound to `worker_id`.
    pub(crate) fn new(worker_id: WorkerId, consensus_bus: ConsensusBus, payload_store: DB) -> Self {
        Self { worker_id, consensus_bus, payload_store }
    }

    /// Reject a message stamped with a different worker id than this instance is bound to.
    fn check_worker_id(&self, message_worker_id: WorkerId) -> eyre::Result<()> {
        if message_worker_id == self.worker_id {
            Ok(())
        } else {
            Err(eyre::eyre!(
                "message stamped with worker id {message_worker_id} arrived on the local network \
                 instance bound to worker id {}",
                self.worker_id
            ))
        }
    }
}

#[async_trait::async_trait]
impl<DB: Database> WorkerToPrimaryClient for WorkerReceiverHandler<DB> {
    async fn report_own_batch(&self, message: WorkerOwnBatchMessage) -> eyre::Result<()> {
        self.check_worker_id(message.worker_id())?;
        let (tx_ack, rx_ack) = oneshot::channel();
        let response = self
            .consensus_bus
            .our_digests()
            .send(OurDigestMessage {
                digest: message.digest(),
                worker_id: message.worker_id(),
                ack_channel: tx_ack,
            })
            .await?;

        // If we are ok, then wait for the ack
        rx_ack.await?;

        Ok(response)
    }

    async fn report_others_batch(&self, message: WorkerOthersBatchMessage) -> eyre::Result<()> {
        self.check_worker_id(message.worker_id())?;
        self.payload_store.write_payload(&message.digest(), &message.worker_id())?;
        Ok(())
    }
}

/// Responses to a vote request.
#[derive(Clone, Debug, PartialEq)]
pub enum RequestVoteResult {
    /// The peer's vote if the peer considered the proposed header valid.
    Vote(Vote),
    /// Missing certificates in order to vote.
    ///
    /// If the peer was unable to verify parents for a proposed header, they respond requesting
    /// the missing certificate by digest.
    MissingParents(Vec<HeaderDigest>),
}

/// Clip peer-supplied error text to its first [`MAX_PEER_ERROR_LEN`] characters.
///
/// Cuts on a character boundary, so multi-byte text never splits mid-character.
fn clip_peer_error(mut error: String) -> String {
    if let Some((end, _)) = error.char_indices().nth(MAX_PEER_ERROR_LEN) {
        error.truncate(end);
    }
    error
}

#[cfg(test)]
mod clip_peer_error_tests {
    use super::{clip_peer_error, MAX_PEER_ERROR_LEN};

    /// Text at or under the limit is kept whole; longer text keeps exactly the first
    /// [`MAX_PEER_ERROR_LEN`] characters, cut on a character boundary when they are multi-byte.
    #[test]
    fn clips_to_the_limit_on_a_char_boundary() {
        let short = "x".repeat(MAX_PEER_ERROR_LEN);
        assert_eq!(clip_peer_error(short.clone()), short);

        let long = "x".repeat(1 << 20);
        assert_eq!(clip_peer_error(long), "x".repeat(MAX_PEER_ERROR_LEN));

        // three bytes per char, so a byte-count cut would land mid-character
        let wide = "€".repeat(MAX_PEER_ERROR_LEN + 1);
        let clipped = clip_peer_error(wide);
        assert_eq!(clipped.chars().count(), MAX_PEER_ERROR_LEN);
        assert_eq!(clipped, "€".repeat(MAX_PEER_ERROR_LEN));
    }
}

/// Tests of the inbound sync admission budgets: the per-peer cap, the opening-read budget, and
/// the per-class serve pools with their committee reserves (GHSA-9h5w-9282-pc3g).
#[cfg(test)]
mod sync_admission_tests {
    use super::{
        admit_and_serve_sync_stream, gossip_author_to_strike, opening_read_bound, write_frame,
        GossipSlots, PrimaryNetworkError, PrimaryNetworkHandle, Req, Res, ServeHoldLimit,
        SyncAdmission, SyncClass, SyncFrame, SyncServeOutcome, SyncServePools,
        COMMITTEE_RESERVED_OPENING_READS, COMMITTEE_RESERVED_SYNC_STREAMS,
        MAX_CONCURRENT_EPOCH_STREAMS, MAX_CONCURRENT_OPENING_READS, MAX_PENDING_GOSSIP_PER_PEER,
        MAX_PENDING_REQUESTS_PER_PEER, MAX_SYNC_REQUEST_FRAME_SIZE,
        PUBLIC_EPOCH_PACK_SERVE_TIMEOUT, PUBLIC_SYNC_REQUEST_READ_TIMEOUT,
        SYNC_REQUEST_READ_TIMEOUT,
    };
    use crate::error::CertManagerError;
    use futures::{io::Cursor, AsyncRead, AsyncWrite, StreamExt as _};
    use rand::{rngs::StdRng, SeedableRng as _};
    use std::{
        future::Future,
        io,
        pin::Pin,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        task::{Context, Poll},
        time::{Duration, Instant},
    };
    use tn_network_libp2p::{
        types::NetworkCommand, PeerSlots, PeerTier, Penalty, PrimarySyncRequest, ReservedPeers,
        ReservedPool,
    };
    use tn_types::{
        error::CertificateError, BlsKeypair, BlsPublicKey, ConsensusHeaderDigest, HeaderDigest,
    };
    use tokio::{sync::mpsc, task::JoinHandle};

    /// Every serve class.
    const CLASSES: [SyncClass; 3] =
        [SyncClass::EpochPack, SyncClass::MissingCertificates, SyncClass::ConsensusOutput];

    /// A deterministic BLS public key for `seed`.
    fn key(seed: u64) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
    }

    /// True when the serve pool of `class` still has its whole public share.
    fn public_share_whole(pools: &SyncServePools, class: SyncClass) -> bool {
        pools.pool(class).available_public() == MAX_CONCURRENT_EPOCH_STREAMS
    }

    /// True when every serve pool still has all of its public permits and all of the reserved
    /// permits of `committee`.
    fn serve_pools_untouched(pools: &SyncServePools, committee: &BlsPublicKey) -> bool {
        let whole = |class: SyncClass| {
            let pool = pools.pool(class);
            pool.available_public() == MAX_CONCURRENT_EPOCH_STREAMS
                && pool.available_reserved(committee) == COMMITTEE_RESERVED_SYNC_STREAMS
        };
        CLASSES.into_iter().all(whole)
    }

    /// The opening-read budget at its production sizes.
    fn opening_pool() -> ReservedPool {
        ReservedPool::new(MAX_CONCURRENT_OPENING_READS, COMMITTEE_RESERVED_OPENING_READS)
    }

    /// Let the spawned tasks run until each one waits on the paused clock or finishes.
    async fn settle() {
        futures::stream::iter(0..16).for_each(|_| tokio::task::yield_now()).await;
    }

    /// The peer of a mild penalty report, or `None` for a different command.
    fn mild_penalty_peer(command: NetworkCommand<Req, Res>) -> Option<BlsPublicKey> {
        if let NetworkCommand::ReportPenalty { peer, penalty: Penalty::Mild } = command {
            Some(peer)
        } else {
            None
        }
    }

    /// A requester that opens a sync stream and never sends its request frame. Reads stay
    /// pending; writes and the close succeed at once.
    #[derive(Debug)]
    struct SilentStream;

    impl AsyncRead for SilentStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for SilentStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// A requester that sends `wire` as its opening bytes only after `delay`. Reads stay pending
    /// until the delay ends and then drain `wire`; writes, flushes and the close succeed at once.
    #[derive(Debug)]
    struct DelayedStream {
        /// The wait before the opening bytes arrive.
        delay: Pin<Box<tokio::time::Sleep>>,
        /// The opening bytes.
        wire: Cursor<Vec<u8>>,
    }

    impl DelayedStream {
        /// A stream whose opening bytes `wire` arrive after `delay`.
        fn new(delay: Duration, wire: Vec<u8>) -> Self {
            Self { delay: Box::pin(tokio::time::sleep(delay)), wire: Cursor::new(wire) }
        }
    }

    impl AsyncRead for DelayedStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            std::task::ready!(this.delay.as_mut().poll(cx));
            Pin::new(&mut this.wire).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DelayedStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// The inbound sync admission state of one responder, wired the way the primary network
    /// wires it.
    struct Responder {
        /// The committee tier.
        reserved: ReservedPeers,
        /// The per-peer cap.
        slots: PeerSlots,
        /// The opening-read budget.
        opening: ReservedPool,
        /// The per-class serve pools.
        pools: SyncServePools,
        /// The handle that receives penalty reports.
        handle: PrimaryNetworkHandle,
        /// Set when any stream reaches its serve.
        served: Arc<AtomicBool>,
    }

    impl Responder {
        /// A responder whose committee tier is `committee`, and the receiver of its commands.
        fn new(committee: BlsPublicKey) -> (Self, mpsc::Receiver<NetworkCommand<Req, Res>>) {
            let (sender, commands) = mpsc::channel(64);
            let responder = Self {
                reserved: ReservedPeers::new([committee]),
                slots: PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER),
                opening: opening_pool(),
                pools: SyncServePools::new(),
                handle: PrimaryNetworkHandle::new_for_test(sender),
                served: Arc::new(AtomicBool::new(false)),
            };
            (responder, commands)
        }

        /// Admit a silent stream from `peer` the way the primary network does and drive it on a
        /// task. `None` when the stream would be shed.
        fn open_silent_stream(&self, peer: BlsPublicKey) -> Option<JoinHandle<()>> {
            let tier = self.reserved.tier(&peer);
            let grants = self.slots.try_take(peer).zip(self.opening.try_acquire(&peer, tier));
            grants.map(|(slot, opening)| {
                let grant = SyncAdmission { peer, tier, slot, opening };
                let (pools, net) = (self.pools.clone(), self.handle.clone());
                let served = self.served.clone();
                let serve = move |_stream: SilentStream, _request: PrimarySyncRequest| {
                    served.store(true, Ordering::SeqCst);
                    std::future::ready(SyncServeOutcome::Completed)
                };
                tokio::spawn(admit_and_serve_sync_stream(SilentStream, grant, pools, net, serve))
            })
        }

        /// Admit `stream` from `peer` the way the primary network does and drive it on a task
        /// with `serve`. `None` when the stream would be shed.
        fn open_stream<S, F, Fut>(
            &self,
            peer: BlsPublicKey,
            stream: S,
            serve: F,
        ) -> Option<JoinHandle<()>>
        where
            S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
            F: FnOnce(S, PrimarySyncRequest) -> Fut + Send + 'static,
            Fut: Future<Output = SyncServeOutcome> + Send + 'static,
        {
            let tier = self.reserved.tier(&peer);
            let grants = self.slots.try_take(peer).zip(self.opening.try_acquire(&peer, tier));
            grants.map(|(slot, opening)| {
                let grant = SyncAdmission { peer, tier, slot, opening };
                let (pools, net) = (self.pools.clone(), self.handle.clone());
                tokio::spawn(admit_and_serve_sync_stream(stream, grant, pools, net, serve))
            })
        }

        /// The in-flight stream count of each of `peers`.
        fn in_flight(&self, peers: &[BlsPublicKey]) -> Vec<usize> {
            peers.iter().map(|peer| self.slots.in_flight(peer)).collect()
        }
    }

    /// A quarter of the public opening-read bound: the spacing of the silent-stream opens.
    fn opening_step() -> Duration {
        opening_read_bound(PeerTier::Public) / 4
    }

    /// Open a silent stream from `peer`, let the responder task start, then let one opening step
    /// pass.
    async fn open_then_wait(responder: &Responder, peer: BlsPublicKey) -> Option<JoinHandle<()>> {
        let task = responder.open_silent_stream(peer);
        settle().await;
        tokio::time::advance(opening_step()).await;
        task
    }

    /// A requester that sends `wire` as its opening bytes and then grants no receive credit:
    /// reads drain `wire` and then report the end of the stream, while writes, flushes and the
    /// close stay pending.
    #[derive(Debug)]
    struct StuckWriterStream {
        /// The opening bytes.
        wire: Cursor<Vec<u8>>,
    }

    impl StuckWriterStream {
        /// A stuck writer whose opening bytes are `wire`.
        fn new(wire: Vec<u8>) -> Self {
            Self { wire: Cursor::new(wire) }
        }
    }

    impl AsyncRead for StuckWriterStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.wire).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for StuckWriterStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    /// A serve that records in `served` that it started and then never finishes.
    fn pending_serve<S: 'static>(
        served: Arc<AtomicBool>,
    ) -> impl FnOnce(S, PrimarySyncRequest) -> std::future::Pending<SyncServeOutcome> + Send + 'static
    {
        move |_stream: S, _request: PrimarySyncRequest| {
            served.store(true, Ordering::SeqCst);
            std::future::pending()
        }
    }

    /// A serve that records in `served` that it ran and then completes at once.
    fn completed_serve<S: 'static>(
        served: Arc<AtomicBool>,
    ) -> impl FnOnce(S, PrimarySyncRequest) -> std::future::Ready<SyncServeOutcome> + Send + 'static
    {
        move |_stream: S, _request: PrimarySyncRequest| {
            served.store(true, Ordering::SeqCst);
            std::future::ready(SyncServeOutcome::Completed)
        }
    }

    /// The wire bytes of `frame`.
    async fn encoded(frame: SyncFrame<PrimarySyncRequest>) -> Vec<u8> {
        let (mut wire, mut encode_buffer, mut compressed_buffer) =
            (Vec::new(), Vec::new(), Vec::new());
        let written = write_frame(
            &mut wire,
            &frame,
            &mut encode_buffer,
            &mut compressed_buffer,
            MAX_SYNC_REQUEST_FRAME_SIZE,
        )
        .await;
        assert!(written.is_ok());
        wire
    }

    /// Open one stuck-writer stream per peer of `peers`, each with the opening bytes `wire`, and
    /// check that every opening permit is released as soon as the opening read ends, while the
    /// bounded close or control write is still pending. Returns the mild penalties, in order,
    /// once every task has ended.
    async fn open_stuck_streams(
        responder: &Responder,
        commands: &mut mpsc::Receiver<NetworkCommand<Req, Res>>,
        peers: &[BlsPublicKey],
        wire: &[u8],
    ) -> Vec<Option<BlsPublicKey>> {
        let opened: Option<Vec<JoinHandle<()>>> = peers
            .iter()
            .map(|&peer| {
                let stream = StuckWriterStream::new(wire.to_vec());
                responder.open_stream(peer, stream, pending_serve(responder.served.clone()))
            })
            .collect();
        settle().await;
        // the opening read ended at once, and its permit is free while the close or the control
        // write is still pending
        assert_eq!(responder.opening.available_public(), MAX_CONCURRENT_OPENING_READS);
        let reserves: Vec<_> =
            peers.iter().map(|peer| responder.opening.available_reserved(peer)).collect();
        assert_eq!(reserves, vec![COMMITTEE_RESERVED_OPENING_READS; peers.len()]);
        assert!(opened.as_ref().is_some_and(|tasks| tasks.iter().all(|task| !task.is_finished())));
        assert_eq!(responder.in_flight(peers), vec![1; peers.len()]);
        // the bounded close or control write gives up, and the task ends
        let bound =
            SYNC_REQUEST_READ_TIMEOUT.saturating_mul(2).saturating_add(Duration::from_millis(1));
        tokio::time::advance(bound).await;
        settle().await;
        assert!(opened.is_some_and(|tasks| tasks.iter().all(JoinHandle::is_finished)));
        assert_eq!(responder.in_flight(peers), vec![0; peers.len()]);
        assert!(!responder.served.load(Ordering::SeqCst));
        std::iter::from_fn(|| commands.try_recv().ok()).map(mild_penalty_peer).collect()
    }

    /// Public keys that fill the public share of a serve class cannot lock out a committee key,
    /// and one committee key cannot take the reserve of another (issue #1750). Three public keys
    /// at the per-peer cap (two, two and one streams) fill the share of five, and a fourth public
    /// key is refused. A committee key is served from its own reserve until that reserve is empty,
    /// and then it is refused. A second committee key still has its whole reserve and is served.
    /// The other classes keep their whole share, and released permits refill the public share.
    #[test]
    fn committee_key_is_served_when_public_keys_fill_a_serve_class() {
        let (committee, other) = (key(1), key(4));
        let reserved = ReservedPeers::new([committee, other]);
        let slots = PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER);
        let pools = SyncServePools::new();
        let pool = pools.pool(SyncClass::EpochPack);
        let held: Option<Vec<_>> = [(key(10), 2), (key(11), 2), (key(12), 1)]
            .into_iter()
            .flat_map(|(peer, count)| std::iter::repeat_n(peer, count))
            .map(|peer| slots.try_take(peer).zip(pool.try_acquire(&peer, reserved.tier(&peer))))
            .collect();
        assert_eq!(held.as_ref().map(Vec::len), Some(MAX_CONCURRENT_EPOCH_STREAMS));
        assert_eq!(pool.available_public(), 0);
        // a third stream from a public key at the per-peer cap is refused by its slot
        assert!(slots.try_take(key(10)).is_none());
        // a fourth public key has a free slot but no serve permit
        assert!(slots.try_take(key(13)).is_some());
        assert!(pool.try_acquire(&key(13), reserved.tier(&key(13))).is_none());
        // the committee key is served from its own reserve until that reserve is empty
        assert_eq!(reserved.tier(&committee), PeerTier::Committee);
        let committee_permits: Option<Vec<_>> =
            std::iter::repeat_with(|| pool.try_acquire(&committee, reserved.tier(&committee)))
                .take(COMMITTEE_RESERVED_SYNC_STREAMS)
                .collect();
        assert!(committee_permits.is_some());
        assert_eq!(pool.available_reserved(&committee), 0);
        assert!(pool.try_acquire(&committee, reserved.tier(&committee)).is_none());
        // a second committee key keeps its whole reserve and is still served
        assert_eq!(pool.available_reserved(&other), COMMITTEE_RESERVED_SYNC_STREAMS);
        let other_permit = pool.try_acquire(&other, reserved.tier(&other));
        assert!(other_permit.is_some());
        assert_eq!(
            pool.available_reserved(&other),
            COMMITTEE_RESERVED_SYNC_STREAMS.saturating_sub(1)
        );
        // a flood of one class does not touch the other classes
        assert!(public_share_whole(&pools, SyncClass::MissingCertificates));
        assert!(public_share_whole(&pools, SyncClass::ConsensusOutput));
        // released permits refill the public share
        drop(held);
        assert_eq!(pool.available_public(), MAX_CONCURRENT_EPOCH_STREAMS);
        assert!(pool.try_acquire(&key(13), reserved.tier(&key(13))).is_some());
    }

    /// Opening reads draw only on the opening-read budget. Four public keys at the per-peer cap
    /// fill its public share of eight, and a fifth public key is refused. A committee key opens
    /// from its own reserve until that reserve is empty, and then it is refused. A second
    /// committee key still has its whole reserve and opens from it (issue #1750). Every serve
    /// pool stays whole.
    #[test]
    fn opening_reads_leave_the_serve_pools_untouched() {
        let (committee, other) = (key(2), key(5));
        let reserved = ReservedPeers::new([committee, other]);
        let slots = PeerSlots::new(MAX_PENDING_REQUESTS_PER_PEER);
        let opening = opening_pool();
        let pools = SyncServePools::new();
        let held: Option<Vec<_>> = (20..24)
            .flat_map(|seed| std::iter::repeat_n(key(seed), MAX_PENDING_REQUESTS_PER_PEER))
            .map(|peer| slots.try_take(peer).zip(opening.try_acquire(&peer, reserved.tier(&peer))))
            .collect();
        assert_eq!(held.as_ref().map(Vec::len), Some(MAX_CONCURRENT_OPENING_READS));
        assert_eq!(opening.available_public(), 0);
        assert!(opening.try_acquire(&key(24), reserved.tier(&key(24))).is_none());
        // the committee key opens from its own reserve until that reserve is empty
        let committee_opening: Option<Vec<_>> =
            std::iter::repeat_with(|| opening.try_acquire(&committee, reserved.tier(&committee)))
                .take(COMMITTEE_RESERVED_OPENING_READS)
                .collect();
        assert!(committee_opening.is_some());
        assert_eq!(opening.available_reserved(&committee), 0);
        assert!(opening.try_acquire(&committee, reserved.tier(&committee)).is_none());
        // a second committee key keeps its whole reserve and still opens
        assert_eq!(opening.available_reserved(&other), COMMITTEE_RESERVED_OPENING_READS);
        let other_opening = opening.try_acquire(&other, reserved.tier(&other));
        assert!(other_opening.is_some());
        assert!(serve_pools_untouched(&pools, &committee));
        assert!(serve_pools_untouched(&pools, &other));
        drop((held, committee_opening, other_opening));
        assert_eq!(opening.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(opening.available_reserved(&committee), COMMITTEE_RESERVED_OPENING_READS);
        assert_eq!(opening.available_reserved(&other), COMMITTEE_RESERVED_OPENING_READS);
    }

    /// A struck author gets no gossip slot during its cooldown, the hold doubles per strike, and
    /// it stops at the cap. Other authors are not affected (#1772).
    #[test]
    fn gossip_author_cooldown_ignores_struck_author() {
        let slots = GossipSlots::new(MAX_PENDING_GOSSIP_PER_PEER);
        let t0 = Instant::now();
        let s = Duration::from_secs;
        slots.strike_at(key(1), t0);
        assert!(slots.take_at(Some(key(1)), None, t0).is_none());
        // a resolved relayer does not give a struck author a slot
        assert!(slots.take_at(Some(key(1)), Some(key(2)), t0).is_none());
        assert!(slots.take_at(Some(key(2)), None, t0).is_some());
        assert!(slots.take_at(Some(key(1)), None, t0 + s(9)).is_none());
        // the first hold is over
        assert!(slots.take_at(Some(key(1)), None, t0 + s(10)).is_some());
        // the second strike holds for 20 s
        slots.strike_at(key(1), t0 + s(10));
        assert!(slots.take_at(Some(key(1)), None, t0 + s(29)).is_none());
        assert!(slots.take_at(Some(key(1)), None, t0 + s(30)).is_some());
        // more strikes stop at the cap
        (0..5).for_each(|_| slots.strike_at(key(1), t0 + s(30)));
        assert_eq!(slots.cooldown_until(&key(1)), Some(t0 + s(30) + s(160)));
    }

    /// Only the author's own delivery of a pending frame strikes the author, and only for a fault
    /// of the strike class. A relayed frame, an unresolved relayer and a settled ticket strike no
    /// one (#1772, GHSA-vw36).
    #[test]
    fn gossip_author_strike_needs_own_pending_delivery() {
        let (author, relayer) = (key(40), key(41));
        let strike = |e: &PrimaryNetworkError, was_pending: bool, from: Option<BlsPublicKey>| {
            gossip_author_to_strike(e, was_pending, Some(author), from)
        };
        let forged = PrimaryNetworkError::Certificate(CertManagerError::Certificate(
            CertificateError::InvalidSignature,
        ));
        // own delivery
        assert_eq!(strike(&forged, true, Some(author)), Some(author));
        // relayed
        assert_eq!(strike(&forged, true, Some(relayer)), None);
        // unresolved relayer
        assert_eq!(strike(&forged, true, None), None);
        // already settled
        assert_eq!(strike(&forged, false, Some(author)), None);
        // a consensus result with a bad signature strikes, though its penalty is mild
        let forged_result =
            PrimaryNetworkError::UnknownConsensusHeaderCert(ConsensusHeaderDigest::default());
        assert_eq!(strike(&forged_result, true, Some(author)), Some(author));
        // a mild certificate fault does not strike
        let too_old = PrimaryNetworkError::Certificate(CertManagerError::Certificate(
            CertificateError::TooOld(HeaderDigest::default(), 1, 2),
        ));
        assert_eq!(strike(&too_old, true, Some(author)), None);
    }

    /// Silent streams hold only the opening-read budget, and only until the opening read for
    /// their tier times out (one second for a public peer, five for the committee peer). The
    /// serve pools stay whole, each stream frees its opening permit and its peer slot at its own
    /// timeout, nothing is served, and each public peer (but not the committee peer) is charged
    /// one mild penalty.
    #[tokio::test(start_paused = true)]
    async fn silent_streams_hold_only_the_opening_budget_and_are_charged() {
        let (committee, first, second, third) = (key(3), key(30), key(31), key(32));
        let peers = [committee, first, second, third];
        let (open_cap, open_reserve) =
            (MAX_CONCURRENT_OPENING_READS, COMMITTEE_RESERVED_OPENING_READS);
        let (responder, mut commands) = Responder::new(committee);
        let mut charged = || {
            let reports = std::iter::from_fn(|| commands.try_recv().ok());
            reports.map(mild_penalty_peer).collect::<Vec<_>>()
        };
        // the committee stream and the first public stream open at 0 ms, the others one and two
        // opening steps (250 ms each) later
        let opened = [
            responder.open_silent_stream(committee),
            open_then_wait(&responder, first).await,
            open_then_wait(&responder, second).await,
            open_then_wait(&responder, third).await,
        ];
        let tasks: Option<Vec<JoinHandle<()>>> = opened.into_iter().collect();
        assert!(tasks.is_some());

        // 750 ms: four opening reads are in flight, and no serve permit is taken
        assert_eq!(responder.opening.available_public(), open_cap.saturating_sub(3));
        assert_eq!(
            responder.opening.available_reserved(&committee),
            open_reserve.saturating_sub(1)
        );
        assert_eq!(responder.in_flight(&peers), vec![1, 1, 1, 1]);
        assert!(serve_pools_untouched(&responder.pools, &committee));

        // 1125 ms: only the first public read timed out, and only its peer is charged
        tokio::time::advance(opening_step().saturating_add(opening_step() / 2)).await;
        settle().await;
        assert_eq!(responder.opening.available_public(), open_cap.saturating_sub(2));
        assert_eq!(
            responder.opening.available_reserved(&committee),
            open_reserve.saturating_sub(1)
        );
        assert_eq!(responder.in_flight(&peers), vec![1, 0, 1, 1]);
        assert!(serve_pools_untouched(&responder.pools, &committee));
        assert_eq!(charged(), vec![Some(first)]);

        // 1625 ms: the other public reads timed out, and the public share is whole again
        tokio::time::advance(opening_step().saturating_mul(2)).await;
        settle().await;
        assert_eq!(responder.opening.available_public(), open_cap);
        assert_eq!(
            responder.opening.available_reserved(&committee),
            open_reserve.saturating_sub(1)
        );
        assert_eq!(responder.in_flight(&peers), vec![1, 0, 0, 0]);
        assert!(serve_pools_untouched(&responder.pools, &committee));
        let later = charged();
        assert_eq!(later.len(), 2);
        assert!(later.contains(&Some(second)) && later.contains(&Some(third)));

        // the committee read times out at its own, longer bound: every budget is whole, nothing
        // is served, and the committee peer is not charged
        tokio::time::advance(opening_read_bound(PeerTier::Committee)).await;
        settle().await;
        assert_eq!(responder.opening.available_public(), open_cap);
        assert_eq!(responder.opening.available_reserved(&committee), open_reserve);
        assert_eq!(responder.in_flight(&peers), vec![0, 0, 0, 0]);
        assert!(tasks.is_some_and(|tasks| tasks.iter().all(JoinHandle::is_finished)));
        assert!(serve_pools_untouched(&responder.pools, &committee));
        assert!(!responder.served.load(Ordering::SeqCst));
        assert!(charged().is_empty());
    }

    /// A stream whose opening frame is unreadable (the requester ends its write side at once, so
    /// the read ends early) frees its opening permit as soon as the read ends, holds no serve
    /// permit while its close is pending, and costs a public peer one mild penalty. The committee
    /// peer is not charged.
    #[tokio::test(start_paused = true)]
    async fn unreadable_openings_free_the_budget_and_are_charged() {
        let (committee, public) = (key(4), key(40));
        let (responder, mut commands) = Responder::new(committee);
        let peers = [committee, public];
        let charged = open_stuck_streams(&responder, &mut commands, &peers, &[]).await;
        assert_eq!(charged, vec![Some(public)]);
        assert!(serve_pools_untouched(&responder.pools, &committee));
    }

    /// A stream whose opening frame is not a request frees its opening permit as soon as the
    /// read ends, holds no serve permit while its malformed-frame answer is pending, and costs a
    /// public peer one mild penalty. The committee peer is not charged.
    #[tokio::test(start_paused = true)]
    async fn unexpected_openings_free_the_budget_and_are_charged() {
        let (committee, public) = (key(5), key(50));
        let (responder, mut commands) = Responder::new(committee);
        let peers = [committee, public];
        let wire = encoded(SyncFrame::Ack).await;
        let charged = open_stuck_streams(&responder, &mut commands, &peers, &wire).await;
        assert_eq!(charged, vec![Some(public)]);
        assert!(serve_pools_untouched(&responder.pools, &committee));
    }

    /// A request for a class whose public share is full is refused: it frees its opening permit
    /// as soon as the read ends, holds no serve permit while its refusal is pending, and is not
    /// charged.
    #[tokio::test(start_paused = true)]
    async fn at_capacity_openings_free_the_budget_without_a_charge() {
        let (committee, first, second) = (key(6), key(60), key(61));
        let (responder, mut commands) = Responder::new(committee);
        let pool = responder.pools.pool(SyncClass::EpochPack);
        let held: Option<Vec<_>> =
            std::iter::repeat_with(|| pool.try_acquire(&key(62), PeerTier::Public))
                .take(MAX_CONCURRENT_EPOCH_STREAMS)
                .collect();
        assert_eq!(held.as_ref().map(Vec::len), Some(MAX_CONCURRENT_EPOCH_STREAMS));
        let wire = encoded(SyncFrame::Req(PrimarySyncRequest::EpochPack { epoch: 0 })).await;
        let peers = [first, second];
        let charged = open_stuck_streams(&responder, &mut commands, &peers, &wire).await;
        assert_eq!(charged, vec![]);
        assert_eq!(pool.available_reserved(&committee), COMMITTEE_RESERVED_SYNC_STREAMS);
        drop(held);
        assert!(serve_pools_untouched(&responder.pools, &committee));
    }

    /// A valid request that arrives in the second half of the public opening window is admitted
    /// and served, but charged, so four identities cannot park eight opening permits for free.
    #[tokio::test(start_paused = true)]
    async fn late_valid_public_openings_are_charged() {
        let committee = key(9);
        let (responder, mut commands) = Responder::new(committee);
        let peers = [key(90), key(91), key(92), key(93)];
        let late = PUBLIC_SYNC_REQUEST_READ_TIMEOUT.saturating_sub(Duration::from_millis(1));
        let wire = encoded(SyncFrame::Req(PrimarySyncRequest::EpochPack { epoch: 0 })).await;
        // each public key opens two streams whose request arrives just inside the window
        let opened: Option<Vec<JoinHandle<()>>> = peers
            .iter()
            .flat_map(|&peer| std::iter::repeat_n(peer, MAX_PENDING_REQUESTS_PER_PEER))
            .map(|peer| {
                let stream = DelayedStream::new(late, wire.clone());
                responder.open_stream(peer, stream, completed_serve(responder.served.clone()))
            })
            .collect();
        assert_eq!(opened.as_ref().map(Vec::len), Some(MAX_CONCURRENT_OPENING_READS));
        settle().await;
        // the eight openings hold the whole public opening share, and nothing is charged yet
        assert_eq!(responder.opening.available_public(), 0);
        assert!(opened.as_ref().is_some_and(|tasks| tasks.iter().all(|task| !task.is_finished())));
        assert!(commands.try_recv().is_err());
        // the requests arrive late: each one is admitted and served, and every budget is whole
        tokio::time::advance(late).await;
        settle().await;
        assert!(opened.is_some_and(|tasks| tasks.iter().all(JoinHandle::is_finished)));
        assert!(responder.served.load(Ordering::SeqCst));
        assert_eq!(responder.opening.available_public(), MAX_CONCURRENT_OPENING_READS);
        assert_eq!(
            responder.opening.available_reserved(&committee),
            COMMITTEE_RESERVED_OPENING_READS
        );
        assert_eq!(responder.in_flight(&peers), vec![0; peers.len()]);
        assert!(serve_pools_untouched(&responder.pools, &committee));
        // every late opening costs one mild penalty, two per key
        let reports = std::iter::from_fn(|| commands.try_recv().ok());
        let charged: Vec<_> = reports.map(mild_penalty_peer).collect();
        assert_eq!(charged.len(), MAX_CONCURRENT_OPENING_READS);
        let per_key: Vec<_> = peers
            .iter()
            .map(|&peer| charged.iter().filter(|charge| **charge == Some(peer)).count())
            .collect();
        assert_eq!(per_key, vec![MAX_PENDING_REQUESTS_PER_PEER; peers.len()]);
    }

    /// The whole-serve timeout is a stall, as is a timed-out write, and a finished serve is
    /// complete.
    #[tokio::test]
    async fn a_whole_serve_timeout_is_a_stall() {
        let elapsed = tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .expect_err("a zero timeout elapses");
        let io_error = |kind: io::ErrorKind| PrimaryNetworkError::StdIo(io::Error::from(kind));
        assert_eq!(
            SyncServeOutcome::of(&Err(PrimaryNetworkError::Timeout(elapsed))),
            SyncServeOutcome::Stalled
        );
        assert_eq!(
            SyncServeOutcome::of(&Err(io_error(io::ErrorKind::TimedOut))),
            SyncServeOutcome::Stalled
        );
        assert_eq!(SyncServeOutcome::of(&Ok(())), SyncServeOutcome::Completed);
    }

    /// A requester that stops or closes the stream during the serve is a stall, as on the worker.
    /// Any other I/O failure is a plain failure.
    #[test]
    fn a_reset_or_closed_stream_during_the_serve_is_a_stall() {
        let classify = |kind: io::ErrorKind| {
            SyncServeOutcome::of(&Err(PrimaryNetworkError::StdIo(io::Error::from(kind))))
        };
        assert_eq!(classify(io::ErrorKind::ConnectionReset), SyncServeOutcome::Stalled);
        assert_eq!(classify(io::ErrorKind::NotConnected), SyncServeOutcome::Stalled);
        assert_eq!(classify(io::ErrorKind::TimedOut), SyncServeOutcome::Stalled);
        assert_eq!(classify(io::ErrorKind::InvalidData), SyncServeOutcome::Failed);
    }

    /// A public-tier epoch-pack serve is cut at the hold limit: it holds its serve permit up to
    /// the limit, then the serve is dropped, the permit and the slot are freed, and the peer is
    /// charged one mild penalty.
    #[tokio::test(start_paused = true)]
    async fn public_epoch_pack_serves_are_cut_at_the_hold_limit() {
        let (committee, public) = (key(7), key(70));
        let (responder, mut commands) = Responder::new(committee);
        let wire = encoded(SyncFrame::Req(PrimarySyncRequest::EpochPack { epoch: 0 })).await;
        let stream = StuckWriterStream::new(wire);
        let serve = pending_serve(responder.served.clone());
        let task = responder.open_stream(public, stream, serve);
        assert!(task.is_some());
        settle().await;
        // the serve started: it holds one public epoch-pack permit and no opening permit
        assert!(responder.served.load(Ordering::SeqCst));
        let pool = responder.pools.pool(SyncClass::EpochPack);
        assert_eq!(pool.available_public(), MAX_CONCURRENT_EPOCH_STREAMS.saturating_sub(1));
        assert_eq!(responder.opening.available_public(), MAX_CONCURRENT_OPENING_READS);
        // just short of the limit the serve still holds its permit, and nothing is charged
        let almost = PUBLIC_EPOCH_PACK_SERVE_TIMEOUT.saturating_sub(Duration::from_millis(1));
        tokio::time::advance(almost).await;
        settle().await;
        assert_eq!(pool.available_public(), MAX_CONCURRENT_EPOCH_STREAMS.saturating_sub(1));
        assert!(task.as_ref().is_some_and(|task| !task.is_finished()));
        assert!(commands.try_recv().is_err());
        // at the limit the serve is cut, every budget is whole, and the peer is charged
        tokio::time::advance(Duration::from_millis(2)).await;
        settle().await;
        assert!(task.is_some_and(|task| task.is_finished()));
        assert!(serve_pools_untouched(&responder.pools, &committee));
        assert_eq!(responder.in_flight(&[public]), vec![0]);
        let reports = std::iter::from_fn(|| commands.try_recv().ok());
        let charged: Vec<_> = reports.map(mild_penalty_peer).collect();
        assert_eq!(charged, vec![Some(public)]);
    }

    /// Only a public-tier epoch-pack serve has a hold limit; committee-tier serves and the other
    /// classes keep their own bounds.
    #[test]
    fn committee_epoch_pack_serves_keep_the_floor_deadline() {
        let limits: Vec<_> = [PeerTier::Committee, PeerTier::Public]
            .into_iter()
            .flat_map(|tier| CLASSES.into_iter().map(move |class| ServeHoldLimit::of(tier, class)))
            .collect();
        let (bounded, unbounded) =
            (ServeHoldLimit::Bounded(PUBLIC_EPOCH_PACK_SERVE_TIMEOUT), ServeHoldLimit::Unbounded);
        assert_eq!(limits, vec![unbounded, unbounded, unbounded, bounded, unbounded, unbounded]);
    }
}

/// Tests of the outbound sync request write deadline (GHSA-qf73-5cfc-3jgc): the opening request
/// frame of every client path is written under [`SYNC_REQUEST_WRITE_TIMEOUT`].
#[cfg(test)]
mod sync_request_write_tests {
    use super::{
        read_frame, write_sync_request, PrimarySyncRequest, SyncFrame, SyncRequestWriteError,
        SYNC_REQUEST_WRITE_TIMEOUT,
    };
    use futures::AsyncWrite;
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };

    /// A responder that negotiated the stream and then grants no receive credit: every write
    /// and every flush stays pending forever.
    #[derive(Debug)]
    struct StalledStream;

    impl AsyncWrite for StalledStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// The request frame every test writes.
    fn request() -> SyncFrame<PrimarySyncRequest> {
        SyncFrame::Req(PrimarySyncRequest::EpochPack { epoch: 1 })
    }

    /// A write that never completes is cut after exactly [`SYNC_REQUEST_WRITE_TIMEOUT`] and
    /// reported as a timeout, so the probe walk moves on instead of waiting on the connection.
    #[tokio::test(start_paused = true)]
    async fn stalled_stream_times_out_the_request_write() {
        let start = tokio::time::Instant::now();
        let mut stream = StalledStream;
        let written = write_sync_request(
            &mut stream,
            &request(),
            super::sync_codec::MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await;
        assert!(matches!(written, Err(SyncRequestWriteError::TimedOut)));
        assert_eq!(start.elapsed(), SYNC_REQUEST_WRITE_TIMEOUT);
    }

    /// A ready writer receives the frame inside the deadline, and the bytes decode back to the
    /// same request.
    #[tokio::test]
    async fn ready_stream_delivers_the_request_frame() {
        let mut stream = futures::io::Cursor::new(Vec::new());
        let written = write_sync_request(
            &mut stream,
            &request(),
            super::sync_codec::MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await;
        assert!(written.is_ok());
        let bytes = stream.into_inner();
        assert!(!bytes.is_empty());

        let mut reader = futures::io::Cursor::new(bytes);
        let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        let decoded = read_frame::<_, PrimarySyncRequest>(
            &mut reader,
            &mut decode_buffer,
            &mut compressed_buffer,
            super::sync_codec::MAX_SYNC_PACK_FRAME_SIZE,
        )
        .await
        .expect("frame decodes");
        assert_eq!(decoded, request());
    }
}

/// Tests of the preferred sync server bookkeeping (GHSA-qf73-5cfc-3jgc): the last peer that
/// served is probed first only until it fails a probe.
#[cfg(test)]
mod sync_server_preference_tests {
    use super::{order_probe_peers, PrimaryNetworkHandle, ProbeVerdict, ProbeWalk, SyncRateBound};
    use parking_lot::Mutex;
    use rand::{rngs::StdRng, SeedableRng as _};
    use std::{collections::HashSet, sync::atomic::Ordering};
    use tn_types::{BlsKeypair, BlsPublicKey, Epoch};
    use tokio::sync::mpsc;

    /// A deterministic BLS public key for `seed`.
    fn key(seed: u64) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
    }

    /// A handle whose command channel is never read; these tests only touch its caches.
    fn handle() -> PrimaryNetworkHandle {
        let (sender, _receiver) = mpsc::channel(1);
        PrimaryNetworkHandle::new_for_test(sender)
    }

    /// The context of an ordinary (non-exploration) walk that starts now on `handle`: it reads
    /// the preference slot's current generation, so its serve is current.
    fn walk(handle: &PrimaryNetworkHandle) -> ProbeWalk {
        ProbeWalk { generation: handle.last_sync_server.lock().generation, explore: false }
    }

    /// The context of an exploration walk that starts now on `handle`.
    fn exploration(handle: &PrimaryNetworkHandle) -> ProbeWalk {
        ProbeWalk { explore: true, ..walk(handle) }
    }

    /// The preferred sync server on record.
    fn preferred(handle: &PrimaryNetworkHandle) -> Option<BlsPublicKey> {
        handle.last_sync_server.lock().peer
    }

    /// Whether `handle` reads every peer at the floor for the rest of the epoch.
    fn floor_only(handle: &PrimaryNetworkHandle) -> bool {
        handle.floor_only_this_epoch.load(Ordering::Relaxed)
    }

    /// The scripted verdict of one probe for the `walk_with_floor_fallback` tests: a probe
    /// listed in `serves` serves, a peer listed in `cut` and read at the target is cut at the
    /// target, and every other probe misses.
    fn scripted(
        serves: &[(BlsPublicKey, SyncRateBound)],
        cut: &[BlsPublicKey],
        peer: BlsPublicKey,
        bound: SyncRateBound,
    ) -> ProbeVerdict<()> {
        if serves.contains(&(peer, bound)) {
            ProbeVerdict::Served(())
        } else if cut.contains(&peer) && bound == SyncRateBound::Target {
            ProbeVerdict::CutAtTarget
        } else {
            ProbeVerdict::Missed
        }
    }

    /// Run `walk_with_floor_fallback` over `peers` for `epoch` with the `scripted` probe and no
    /// network; returns the walk result and every probe in the order it ran.
    async fn scripted_walk(
        handle: &PrimaryNetworkHandle,
        peers: Vec<BlsPublicKey>,
        epoch: Epoch,
        serves: &[(BlsPublicKey, SyncRateBound)],
        cut: &[BlsPublicKey],
    ) -> (Option<()>, Vec<(BlsPublicKey, SyncRateBound)>) {
        let calls = Mutex::new(Vec::new());
        let served = handle
            .walk_with_floor_fallback(peers, epoch, |peer, bound| {
                let calls = &calls;
                async move {
                    calls.lock().push((peer, bound));
                    scripted(serves, cut, peer, bound)
                }
            })
            .await;
        (served, calls.into_inner())
    }

    /// Forgetting a peer other than the recorded one keeps the preference; forgetting the
    /// recorded one clears it, and forgetting with nothing on record stays empty. Forgetting does
    /// not bump the generation, so the walk whose preferred peer failed can still record the next
    /// peer that serves it (#1787).
    #[test]
    fn forget_sync_server_drops_only_the_recorded_peer() {
        let handle = handle();
        let (served, other) = (key(1), key(2));
        let current = walk(&handle);
        handle.record_sync_served(served, 0, current);
        assert_eq!(preferred(&handle), Some(served));

        handle.forget_sync_server(other);
        assert_eq!(preferred(&handle), Some(served));

        handle.forget_sync_server(served);
        assert_eq!(preferred(&handle), None);

        handle.forget_sync_server(served);
        assert_eq!(preferred(&handle), None);

        assert_eq!(handle.last_sync_server.lock().generation, current.generation);
        handle.record_sync_served(other, 0, current);
        assert_eq!(preferred(&handle), Some(other));
    }

    /// With a preference the preferred peer leads the walk; without one the walk is the seeded
    /// shuffle alone, so no peer is pinned to the front.
    #[test]
    fn order_probe_peers_puts_no_peer_first_without_a_preference() {
        let peers: Vec<BlsPublicKey> = (0..6).map(key).collect();
        let preferred = key(3);
        let failed = HashSet::new();

        let with_preference = order_probe_peers(
            peers.clone(),
            &failed,
            0,
            Some(preferred),
            false,
            &mut StdRng::seed_from_u64(7),
        );
        assert_eq!(with_preference.first(), Some(&preferred));
        assert_eq!(with_preference.len(), peers.len());
        assert!(peers.iter().all(|p| with_preference.contains(p)));

        let without = order_probe_peers(
            peers.clone(),
            &failed,
            0,
            None,
            false,
            &mut StdRng::seed_from_u64(7),
        );
        let shuffle_only = order_probe_peers(
            peers.clone(),
            &failed,
            0,
            None,
            false,
            &mut StdRng::seed_from_u64(7),
        );
        assert_eq!(without, shuffle_only);
        assert_eq!(without.len(), peers.len());
        assert!(peers.iter().all(|p| without.contains(p)));
    }

    /// A completed serve below the target rate keeps the peer sync-capable but drops its
    /// preference and de-prioritises it for the epoch (GHSA-8fj9-xc3h-gmxm).
    #[test]
    fn record_sync_served_slowly_demotes_and_forgets_the_peer() {
        let handle = handle();
        let peer = key(1);
        handle.record_sync_served(peer, 3, walk(&handle));
        handle.record_sync_served_slowly(peer, 3);
        assert_eq!(preferred(&handle), None);
        assert!(handle.epoch_sync_failed.lock().contains(&(peer, 3)));
        assert_eq!(handle.sync_capability.lock().get(&peer), Some(&true));
    }

    /// A later target-rate serve for the same epoch clears a slow mark and restores the
    /// preference.
    #[test]
    fn record_sync_served_clears_a_slow_mark() {
        let handle = handle();
        let peer = key(1);
        handle.record_sync_served_slowly(peer, 3);
        handle.record_sync_served(peer, 3, walk(&handle));
        assert_eq!(preferred(&handle), Some(peer));
        assert!(!handle.epoch_sync_failed.lock().contains(&(peer, 3)));
    }

    /// A slow server marked for the epoch sorts behind every fresh peer even while it holds the
    /// recorded preference.
    #[test]
    fn order_probe_peers_puts_a_slow_server_behind_fresh_peers() {
        let peers: Vec<BlsPublicKey> = (0..6).map(key).collect();
        let slow = key(3);
        let failed = HashSet::from([(slow, 3)]);
        let ordered = order_probe_peers(
            peers.clone(),
            &failed,
            3,
            Some(slow),
            false,
            &mut StdRng::seed_from_u64(7),
        );
        assert_eq!(ordered.last(), Some(&slow));
        assert_eq!(ordered.len(), peers.len());
    }

    /// A target-rate serve for epoch 4 sets the preference but keeps the slow mark for epoch 3:
    /// the peer is read at the floor and probed last for epoch 3, and read at the target and
    /// probed first for epoch 4 (#1787).
    #[test]
    fn a_serve_on_another_epoch_keeps_this_epochs_floor() {
        let handle = handle();
        let peers: Vec<BlsPublicKey> = (0..6).map(key).collect();
        let peer = key(3);
        handle.record_sync_served_slowly(peer, 3);
        handle.record_sync_served(peer, 4, walk(&handle));
        assert_eq!(handle.probe_bound(peer, 3), SyncRateBound::Floor);
        assert_eq!(handle.probe_bound(peer, 4), SyncRateBound::Target);
        assert_eq!(preferred(&handle), Some(peer));

        let held = preferred(&handle);
        let failed = handle.epoch_sync_failed.lock();
        let on_three = order_probe_peers(
            peers.clone(),
            &failed,
            3,
            held,
            false,
            &mut StdRng::seed_from_u64(7),
        );
        let on_four =
            order_probe_peers(peers, &failed, 4, held, false, &mut StdRng::seed_from_u64(7));
        assert_eq!(on_three.last(), Some(&peer));
        assert_eq!(on_four.first(), Some(&peer));
    }

    /// Rotation drops the probe-first preference with the capability cache and the marks, bumps
    /// the slot's generation and resets the floor-only flag (#1787).
    #[test]
    fn clear_sync_capability_drops_the_preference() {
        let handle = handle();
        let (served, slow) = (key(1), key(2));
        handle.record_sync_served(served, 3, walk(&handle));
        handle.record_sync_served_slowly(slow, 3);
        handle.floor_only_this_epoch.store(true, Ordering::Relaxed);
        let before = handle.last_sync_server.lock().generation;
        handle.clear_sync_capability();
        assert_eq!(preferred(&handle), None);
        assert_eq!(handle.last_sync_server.lock().generation, before.wrapping_add(1));
        assert!(handle.epoch_sync_failed.lock().is_empty());
        assert!(handle.sync_capability.lock().is_empty());
        assert!(!floor_only(&handle));
    }

    /// A serve that lands after an exploration serve, from a walk that started under the old
    /// holder, keeps the explorer: the exploration serve bumped the generation, so the stale
    /// walk's serve is fenced. A walk started after the displacement records as usual (#1787).
    #[test]
    fn a_stale_walks_serve_after_an_exploration_serve_keeps_the_explorer() {
        let handle = handle();
        let (holder, explorer, next) = (key(1), key(2), key(3));
        handle.record_sync_served(holder, 3, walk(&handle));
        // a walk starts under the holder; then another peer serves an exploration walk
        let stale = walk(&handle);
        handle.record_sync_served(explorer, 3, exploration(&handle));
        assert_eq!(preferred(&handle), Some(explorer));
        // the holder serves the stale walk afterwards: it does not take the slot back
        handle.record_sync_served(holder, 3, stale);
        assert_eq!(preferred(&handle), Some(explorer));
        assert_eq!(handle.sync_capability.lock().get(&holder), Some(&true));
        // a walk that started after the displacement records as usual
        handle.record_sync_served(next, 3, walk(&handle));
        assert_eq!(preferred(&handle), Some(next));
    }

    /// A serve that completes after rotation, from a walk that started before it, does not set
    /// the slot: the clear bumped the generation. A walk started in the new epoch records as
    /// usual (#1787).
    #[test]
    fn a_serve_that_completes_after_rotation_does_not_set_the_slot() {
        let handle = handle();
        let peer = key(1);
        let before_rotation = walk(&handle);
        handle.clear_sync_capability();
        handle.record_sync_served(peer, 3, before_rotation);
        assert_eq!(preferred(&handle), None);
        assert_eq!(handle.sync_capability.lock().get(&peer), Some(&true));
        handle.record_sync_served(peer, 3, walk(&handle));
        assert_eq!(preferred(&handle), Some(peer));
    }

    /// Rotation resets the floor-only flag, so the new epoch reads fresh peers at the target
    /// again (#1787, Gap 4).
    #[test]
    fn clear_sync_capability_resets_the_floor_only_flag() {
        let handle = handle();
        handle.floor_only_this_epoch.store(true, Ordering::Relaxed);
        assert!(floor_only(&handle));
        handle.clear_sync_capability();
        assert!(!floor_only(&handle));
    }

    /// Exactly the last walk of every run of `SYNC_PREFERENCE_EXPLORE_EVERY` is an exploration
    /// walk (#1787).
    #[test]
    fn exploration_walk_fires_every_nth_walk() {
        let handle = handle();
        let fired: Vec<bool> = (0..32).map(|_| handle.exploration_walk()).collect();
        let fired_at: Vec<usize> =
            fired.iter().enumerate().filter_map(|(walk, hit)| hit.then_some(walk)).collect();
        assert_eq!(fired_at, vec![15, 31]);
    }

    /// An exploration walk puts the holder last and keeps the seeded shuffle for every other
    /// peer: with the same seed the other peers come out exactly as on a walk with no preference
    /// (#1787).
    #[test]
    fn order_probe_peers_exploration_walk_puts_the_holder_last() {
        let peers: Vec<BlsPublicKey> = (0..6).map(key).collect();
        let holder = key(3);
        let failed = HashSet::new();
        let exploring = order_probe_peers(
            peers.clone(),
            &failed,
            0,
            Some(holder),
            true,
            &mut StdRng::seed_from_u64(7),
        );
        let unpreferred =
            order_probe_peers(peers, &failed, 0, None, false, &mut StdRng::seed_from_u64(7));
        let others_then_holder: Vec<BlsPublicKey> =
            unpreferred.into_iter().filter(|p| *p != holder).chain(Some(holder)).collect();
        assert_eq!(exploring, others_then_holder);
    }

    /// On an exploration walk the holder is probed last even when every other peer is marked for
    /// the epoch, so a holder cannot keep the front of every walk by being the only fresh peer;
    /// on an ordinary walk the same holder leads (#1787).
    #[test]
    fn order_probe_peers_exploration_walk_puts_the_holder_behind_marked_peers() {
        let peers: Vec<BlsPublicKey> = (0..6).map(key).collect();
        let holder = key(3);
        let failed: HashSet<(BlsPublicKey, Epoch)> =
            peers.iter().filter(|p| **p != holder).map(|p| (*p, 3)).collect();
        let exploring = order_probe_peers(
            peers.clone(),
            &failed,
            3,
            Some(holder),
            true,
            &mut StdRng::seed_from_u64(7),
        );
        assert_eq!(exploring.last(), Some(&holder));
        assert_eq!(exploring.len(), peers.len());
        let ordinary = order_probe_peers(
            peers,
            &failed,
            3,
            Some(holder),
            false,
            &mut StdRng::seed_from_u64(7),
        );
        assert_eq!(ordinary.first(), Some(&holder));
    }

    /// A peer the first pass cut at the target is probed again at the floor, after every peer
    /// of the first pass, and its floor serve ends the walk (#1787).
    #[tokio::test]
    async fn walk_with_floor_fallback_reprobes_target_cut_peers_at_the_floor() {
        let handle = handle();
        let (a, b) = (key(1), key(2));
        let (served, calls) =
            scripted_walk(&handle, vec![a, b], 0, &[(a, SyncRateBound::Floor)], &[a]).await;
        assert_eq!(served, Some(()));
        assert_eq!(
            calls,
            vec![(a, SyncRateBound::Target), (b, SyncRateBound::Target), (a, SyncRateBound::Floor),]
        );
        assert!(!floor_only(&handle), "one cut must not set the floor-only flag");
    }

    /// The first serve ends the walk: no peer past it is probed and no floor pass runs, even
    /// when an earlier peer was cut at the target (#1787).
    #[tokio::test]
    async fn walk_with_floor_fallback_stops_at_the_first_serve() {
        let handle = handle();
        let (a, b, c) = (key(1), key(2), key(3));
        let (served, calls) =
            scripted_walk(&handle, vec![a, b, c], 0, &[(b, SyncRateBound::Target)], &[a]).await;
        assert_eq!(served, Some(()));
        assert_eq!(calls, vec![(a, SyncRateBound::Target), (b, SyncRateBound::Target)]);
    }

    /// A peer marked for the epoch is read at the floor from its first probe (#1787).
    #[tokio::test]
    async fn walk_with_floor_fallback_reads_a_marked_peer_at_the_floor() {
        let handle = handle();
        let (a, b) = (key(1), key(2));
        handle.record_sync_served_slowly(a, 5);
        let (served, calls) =
            scripted_walk(&handle, vec![a, b], 5, &[(a, SyncRateBound::Floor)], &[]).await;
        assert_eq!(served, Some(()));
        assert_eq!(calls.first(), Some(&(a, SyncRateBound::Floor)));
        assert_eq!(calls, vec![(a, SyncRateBound::Floor)]);
    }

    /// With every probe missed and no peer cut at the target, the walk fails after one pass
    /// (#1787).
    #[tokio::test]
    async fn walk_with_floor_fallback_returns_none_when_nothing_is_cut() {
        let handle = handle();
        let (a, b) = (key(1), key(2));
        let (served, calls) = scripted_walk(&handle, vec![a, b], 0, &[], &[]).await;
        assert_eq!(served, None);
        assert_eq!(calls, vec![(a, SyncRateBound::Target), (b, SyncRateBound::Target)]);
    }

    /// Two distinct peers cut at the target in one walk stop the target pass there: the rest of
    /// the walk (the cut peers, then the peers not yet probed) is read at the floor, and the
    /// handle reads every peer at the floor for the rest of the epoch (#1787, Gap 4).
    #[tokio::test]
    async fn walk_with_floor_fallback_stops_the_target_pass_at_the_second_cut() {
        let handle = handle();
        let (a, b, c, d) = (key(1), key(2), key(3), key(4));
        let (served, calls) =
            scripted_walk(&handle, vec![a, b, c, d], 0, &[(c, SyncRateBound::Floor)], &[a, b])
                .await;
        assert_eq!(served, Some(()));
        assert_eq!(
            calls,
            vec![
                (a, SyncRateBound::Target),
                (b, SyncRateBound::Target),
                (a, SyncRateBound::Floor),
                (b, SyncRateBound::Floor),
                (c, SyncRateBound::Floor),
            ]
        );
        assert!(floor_only(&handle), "two cuts in one walk must set the floor-only flag");
    }

    /// Once the floor-only flag is set, the next walk plans every peer at the floor and reads
    /// nothing at the target, a fresh peer included (#1787, Gap 4).
    #[tokio::test]
    async fn walk_with_floor_fallback_plans_every_peer_at_the_floor_once_the_link_is_slow() {
        let handle = handle();
        handle.floor_only_this_epoch.store(true, Ordering::Relaxed);
        let (a, b) = (key(1), key(2));
        let (served, calls) =
            scripted_walk(&handle, vec![a, b], 0, &[(b, SyncRateBound::Floor)], &[a]).await;
        assert_eq!(served, Some(()));
        assert_eq!(calls, vec![(a, SyncRateBound::Floor), (b, SyncRateBound::Floor)]);
    }
}
