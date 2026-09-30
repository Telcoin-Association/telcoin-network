//! The epoch seed chain: the rolling value that seeds the epoch-close committee shuffle.
//!
//! Each epoch starts from a deterministic root derived from the epoch number alone. Every committed
//! sub-dag folds the committing leader's round and its deterministic BLS seed signature into that
//! value, so the seed carried by the epoch's closing commit is a hash chain over the epoch's entire
//! committed prefix rather than a value any single authority can compute in advance.
//!
//! Two properties are load bearing:
//!
//! - **Canonicality.** Every fold input is digest-pinned or derived state: the seed signature sits
//!   under the leader's header digest, the leader round comes from the committed leader, the
//!   previous value is read back from the previous commit, and the root is a per-epoch constant.
//!   Honest nodes therefore fold byte identical inputs in the same order and reach the same value.
//! - **Unpredictability for observers.** A candidate seed cannot be evaluated until the immediately
//!   preceding commit is published, because that commit's value is an input, and round binding
//!   means an authority's contribution for round `r` first becomes public when it proposes at round
//!   `r`. No observer can therefore enumerate any authority's future contributions, and pre-signing
//!   future rounds offline is useless without the previous value.
//!
//! What this deliberately does NOT provide is last-actor resistance. The closing leader can
//! evaluate the seed its own commit would produce before deciding whether to broadcast: the
//! previous value is public once the preceding commit lands, and its own signature over its own
//! `(epoch, round)` message is deterministic. It can therefore withhold or delay a proposal whose
//! seed it dislikes and take its chances at a later round, at the cost of forfeiting that commit.
//! That residual last-actor bias is accepted here. Round binding removes cheap offline grinding
//! over other authorities' future contributions; it does not remove the single propose-or-withhold
//! choice the last actor always has.

use crate::{BlsSignature, Epoch, Round, B256};
use alloy::primitives::keccak256;
use std::fmt::{self, Display, Formatter};

/// Domain separator for the per-epoch root of the seed chain.
const ROOT_DOMAIN: &[u8] = b"TN_EPOCH_SEED_ROOT_V1";

/// Domain separator for a single fold step of the seed chain.
const FOLD_DOMAIN: &[u8] = b"TN_EPOCH_SEED_FOLD_V1";

/// One link of the epoch seed chain: the committee-shuffle randomness as of some committed sub-dag.
///
/// # Why this type has no `Default`
///
/// The absence of [`Default`] (and of any `From<Option<_>>`) is the type-level defence against a
/// silent re-root of the chain. If this type were defaultable, a fallible recovery path could
/// paper over a failed read with `unwrap_or_default()` and quietly restart the chain from zero.
/// That is not a recoverable condition: the folded value reaches
/// [`CommittedSubDag::randomness`](crate::CommittedSubDag::randomness), which is covered by the
/// sub-dag digest, which is covered by the consensus header digest, which becomes the executed
/// block's `parent_beacon_block_root`. A node that re-roots therefore forks execution permanently
/// and never re-converges. Every value must come from [`Self::epoch_root`],
/// [`Self::from_committed`], [`Self::genesis_placeholder`] or [`Self::fold`], each of which names
/// the provenance it asserts.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EpochSeedChainValue(B256);

impl EpochSeedChainValue {
    /// The chain value an epoch starts from, before any sub-dag of that epoch is committed.
    ///
    /// A domain-separated per-epoch constant, `keccak256(ROOT_DOMAIN || epoch)`. It depends on
    /// nothing but the epoch number, so every node derives the same root for an epoch without
    /// consulting any local state, and the roots of two epochs are always distinct.
    ///
    /// # Why the prior epoch record is not an input
    ///
    /// The cross-epoch entropy lives in the *signature*, deliberately not in the root. Every
    /// authority's [`EpochSeedMessage`](crate::EpochSeedMessage) still binds the previous epoch's
    /// [`EpochRecord`](crate::EpochRecord) digest, and voters verify that binding against their own
    /// view before signing a header. An authority whose prior-record view diverges from the
    /// committee therefore produces a signature no peer accepts: its headers are never certified,
    /// never committed, and never reach anyone's chain, while its own fold over everyone else's
    /// commits still matches the network exactly.
    ///
    /// Feeding a node-local prior-record digest into the root here would invert that property. That
    /// digest is read from a first-write-wins local `EpochRecord` (certificate-verified at capture
    /// for seed-signature-active epochs, but still node-local state), and the root flows
    /// into [`CommittedSubDag::randomness`](crate::CommittedSubDag::randomness), then the sub-dag
    /// digest, then the consensus header digest, and finally the executed block's
    /// `parent_beacon_block_root`. A node whose record is divergent or partially written would then
    /// fork execution permanently on the epoch's *first* commit, rather than merely being unable to
    /// vote for that epoch. The root must never depend on node-local state.
    pub fn epoch_root(epoch: Epoch) -> Self {
        let preimage: Vec<u8> = ROOT_DOMAIN.iter().copied().chain(epoch.to_le_bytes()).collect();
        Self(keccak256(preimage))
    }

    /// Recover the chain value carried by an already-committed sub-dag.
    ///
    /// The argument must be the `randomness` of a sub-dag this node has committed (either just now
    /// or read back from the consensus pack during recovery); it is the chain value at that point
    /// by construction, so no further derivation is needed.
    pub fn from_committed(randomness: B256) -> Self {
        Self(randomness)
    }

    /// The pinned placeholder value used by the synthetic genesis
    /// [`ConsensusHeader::default`](crate::ConsensusHeader::default) and by test fixtures.
    ///
    /// This value is a constant and MUST stay one forever. It is deliberately not derived from any
    /// node-local state: `ConsensusHeader::default` is reached on non-test paths (state sync uses
    /// it as the pre-genesis anchor), so a node-local derivation would give every node a
    /// different genesis anchor and fork the chain at its first link. Nothing outside those two
    /// callers may use it - real epochs start from [`Self::epoch_root`].
    pub fn genesis_placeholder() -> Self {
        Self(B256::ZERO)
    }

    /// Fold one committed sub-dag into the chain.
    ///
    /// `leader_round` is the round of the sub-dag's committing leader and `signature` is that
    /// leader's deterministic BLS signature over its per-`(author, round)`
    /// [`EpochSeedMessage`](crate::EpochSeedMessage). Binding the round means the fold input for a
    /// round only becomes public when its author proposes at that round, so no observer can
    /// enumerate any authority's future contributions. It does not stop the committing leader from
    /// evaluating its own fold before broadcasting (see the module docs on last-actor bias).
    pub fn fold(&self, leader_round: Round, signature: &BlsSignature) -> Self {
        let preimage: Vec<u8> = FOLD_DOMAIN
            .iter()
            .copied()
            .chain(self.0.iter().copied())
            .chain(leader_round.to_le_bytes())
            .chain(signature.to_bytes())
            .collect();
        Self(keccak256(preimage))
    }

    /// Unwrap the raw chain value so it can be stored as a sub-dag's `randomness`.
    pub fn into_inner(self) -> B256 {
        self.0
    }
}

/// Failures that make the epoch seed chain unrecoverable at startup.
///
/// These are all "the recovered commit cursor and the recovered sub-dag disagree" conditions. They
/// are fatal on purpose: continuing past any of them would either re-root the chain or anchor it to
/// the wrong commit, and both fork execution permanently (see [`EpochSeedChainValue`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EpochSeedChainError {
    /// The store reports a committed round for this epoch but no sub-dag was recovered for it.
    ///
    /// Treating this as a fresh epoch would re-root the chain mid-epoch.
    MissingSubDagForCommittedRound {
        /// The last committed round recovered from the store.
        last_committed_round: Round,
    },
    /// A sub-dag was recovered but the store reports no committed round for this epoch.
    ///
    /// The two cursors come from the same per-epoch pack, so this means the pack is inconsistent.
    SubDagWithoutCommittedRound {
        /// The leader round of the recovered sub-dag.
        sub_dag_leader_round: Round,
    },
    /// The recovered sub-dag's leader round does not match the recovered last committed round.
    ///
    /// The chain value must be anchored to the same commit the cursor points at, so a mismatch
    /// means one of the two is stale and the resulting fold would diverge from the network.
    LeaderRoundMismatch {
        /// The leader round of the recovered sub-dag.
        sub_dag_leader_round: Round,
        /// The last committed round recovered from the store.
        last_committed_round: Round,
    },
}

impl Display for EpochSeedChainError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSubDagForCommittedRound { last_committed_round } => write!(
                f,
                "no committed sub dag recovered for this epoch but last committed round is {last_committed_round}"
            ),
            Self::SubDagWithoutCommittedRound { sub_dag_leader_round } => write!(
                f,
                "recovered a committed sub dag at leader round {sub_dag_leader_round} but no committed round for this epoch"
            ),
            Self::LeaderRoundMismatch { sub_dag_leader_round, last_committed_round } => write!(
                f,
                "recovered sub dag leader round {sub_dag_leader_round} is not equal to the last committed round {last_committed_round}"
            ),
        }
    }
}

impl std::error::Error for EpochSeedChainError {}
