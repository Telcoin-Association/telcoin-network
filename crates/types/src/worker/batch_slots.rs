//! Consensus-ordered producer slots for sender buckets.
//!
//! A quorum of ordered timeout votes rotates the producer of an unfilled slot. An earlier
//! proposal remains eligible until one proposal fills the slot. Opening the next slot requires
//! durable execution. Availability voters must also validate proposal bodies and durably
//! reserve one proposal digest per slot and view before acknowledging them.

use crate::{
    bls_verify_secure, Address, Batch, BlsPublicKey, BlsSignature, Committee, Epoch, Signer, B256,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt, num::NonZeroU32};

/// Domain separating slot records from header and consensus-result signatures.
const SIGNING_DOMAIN: &[u8] = b"TN_BATCH_SLOT_V1\0";
/// Native record marker inside the existing batch transport.
const WIRE_PREFIX: &[u8] = b"\x7fTN\x01";

/// Chain identifier authenticated by every slot record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BatchSlotChainId(u64);

impl From<u64> for BatchSlotChainId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// A sender bucket in one epoch's committee.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BatchBucket(u32);

impl BatchBucket {
    /// Return the bucket's ordinal.
    pub const fn value(self) -> u32 {
        self.0
    }
}

/// Number of selected proposals in a bucket.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BatchSlotSequence(u64);

impl BatchSlotSequence {
    /// Return the sequence number.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Advance without reopening old slots through wraparound.
    fn next(self) -> Result<Self, BatchSlotError> {
        self.0.checked_add(1).map(Self).ok_or(BatchSlotError::SequenceExhausted)
    }
}

/// Consensus-approved retry number within an unfilled slot.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BatchSlotView(u64);

impl BatchSlotView {
    /// Return the retry number.
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Advance without reusing old reservations through wraparound.
    fn next(self) -> Result<Self, BatchSlotError> {
        self.0.checked_add(1).map(Self).ok_or(BatchSlotError::ViewExhausted)
    }
}

/// Canonical consensus and execution state against which a slot opened.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BatchSlotParent {
    /// Consensus output that opened the slot.
    consensus: B256,
    /// Execution head after that entire output became durable.
    execution: B256,
}

impl BatchSlotParent {
    /// Associate a consensus output with its durable execution head.
    pub const fn new(consensus: B256, execution: B256) -> Self {
        Self { consensus, execution }
    }

    /// Return the opening consensus digest.
    pub const fn consensus(self) -> B256 {
        self.consensus
    }

    /// Return the execution head used for admission checks.
    pub const fn execution(self) -> B256 {
        self.execution
    }
}

/// A proposal opportunity and its canonical admission anchor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BatchSlotPosition {
    /// Sender bucket assigned to this slot.
    bucket: BatchBucket,
    /// Proposal sequence within the bucket.
    sequence: BatchSlotSequence,
    /// Consensus-approved retry view.
    view: BatchSlotView,
    /// Canonical state at which the slot opened.
    parent: BatchSlotParent,
}

impl BatchSlotPosition {
    /// Identify this slot independently of its retry view.
    pub const fn id(self, epoch: Epoch) -> BatchSlotId {
        BatchSlotId { epoch, bucket: self.bucket, sequence: self.sequence }
    }

    /// Return the sender bucket.
    pub const fn bucket(self) -> BatchBucket {
        self.bucket
    }

    /// Return the bucket sequence.
    pub const fn sequence(self) -> BatchSlotSequence {
        self.sequence
    }

    /// Return the retry view.
    pub const fn view(self) -> BatchSlotView {
        self.view
    }

    /// Return the canonical admission anchor.
    pub const fn parent(self) -> BatchSlotParent {
        self.parent
    }
}

/// Epoch-scoped identity of one sender bucket's proposal sequence.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BatchSlotId {
    /// Committee epoch governing the slot.
    epoch: Epoch,
    /// Sender bucket shared across all workers.
    bucket: BatchBucket,
    /// Proposal sequence within the bucket.
    sequence: BatchSlotSequence,
}

/// Canonical admission anchor and highest approved retry for one slot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BatchSlotAuthorization {
    /// Chain domain of the canonical state that authorized this slot.
    chain_id: BatchSlotChainId,
    /// Committee epoch of the canonical authorization.
    epoch: Epoch,
    /// Slot identity, opening state, and highest retry approved before the slot closed.
    position: BatchSlotPosition,
}

impl BatchSlotAuthorization {
    /// Return the committee epoch that approved this slot.
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Return the key used to retain this authorization through the epoch.
    pub const fn id(&self) -> BatchSlotId {
        self.position.id(self.epoch)
    }
}

/// A producer proposal or a committee member's request to retry an unfilled slot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BatchSlotMessage {
    /// Transactions selected by the assigned producer.
    Proposal {
        /// Slot and canonical admission anchor.
        position: BatchSlotPosition,
        /// Ordinary execution batch, authenticated in its entirety.
        batch: Batch,
    },
    /// Vote to rotate the producer without filling the slot.
    Timeout {
        /// Exact slot and view for which the vote counts.
        position: BatchSlotPosition,
    },
}

impl BatchSlotMessage {
    /// Return the position authenticated by this message.
    pub const fn position(&self) -> BatchSlotPosition {
        match self {
            Self::Proposal { position, .. } | Self::Timeout { position } => *position,
        }
    }
}

/// A record binding its author, chain, epoch, position, and complete body to one signature.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedBatchSlotRecord {
    /// Replay protection across chains.
    chain_id: BatchSlotChainId,
    /// Replay protection across committee changes.
    epoch: Epoch,
    /// Committee member signing this record.
    authority: BlsPublicKey,
    /// Proposal or timeout vote.
    message: BatchSlotMessage,
    /// Signature over the domain-separated canonical record.
    signature: BlsSignature,
}

impl SignedBatchSlotRecord {
    /// Sign a complete record with the committee's BLS signer.
    fn sign(
        chain_id: BatchSlotChainId,
        epoch: Epoch,
        authority: BlsPublicKey,
        message: BatchSlotMessage,
        signer: &impl Signer,
    ) -> Result<Self, BatchSlotError> {
        Self::signing_bytes(chain_id, epoch, &authority, &message).map(|bytes| Self {
            chain_id,
            epoch,
            authority,
            message,
            signature: signer.sign(&bytes),
        })
    }

    /// Return the authenticated committee identity.
    pub const fn authority(&self) -> &BlsPublicKey {
        &self.authority
    }

    /// Return the authenticated proposal or timeout vote.
    pub const fn message(&self) -> &BatchSlotMessage {
        &self.message
    }

    /// Return the epoch-scoped slot whose authorization this record requires.
    pub const fn slot(&self) -> BatchSlotId {
        self.message.position().id(self.epoch)
    }

    /// Hash the entire authenticated record for durable vote reservations.
    pub fn digest(&self) -> Result<B256, BatchSlotError> {
        self.encode().map(|bytes| B256::from(*blake3::hash(&bytes).as_bytes()))
    }

    /// Encode a record for the existing batch transport.
    pub fn encode(&self) -> Result<Vec<u8>, BatchSlotError> {
        bcs::to_bytes(self).map_err(BatchSlotError::Encoding).map(|body| {
            let mut encoded = WIRE_PREFIX.to_vec();
            encoded.extend(body);
            encoded
        })
    }

    /// Decode a record after the transport's byte-limit check.
    pub fn decode(bytes: &[u8]) -> Result<Self, BatchSlotError> {
        bytes
            .strip_prefix(WIRE_PREFIX)
            .ok_or(BatchSlotError::InvalidPrefix)
            .and_then(|body| bcs::from_bytes(body).map_err(BatchSlotError::Encoding))
    }

    /// Check domain and membership before performing BLS verification.
    fn verify(
        &self,
        chain_id: BatchSlotChainId,
        committee: &Committee,
    ) -> Result<(), BatchSlotError> {
        match () {
            () if self.chain_id != chain_id => Err(BatchSlotError::WrongChain),
            () if self.epoch != committee.epoch() => Err(BatchSlotError::WrongEpoch),
            () if committee.voting_power(&self.authority) == 0 => {
                Err(BatchSlotError::UnknownAuthority)
            }
            () => Self::signing_bytes(self.chain_id, self.epoch, &self.authority, &self.message)
                .and_then(|bytes| {
                    if bls_verify_secure(&self.signature, &self.authority, &bytes) {
                        Ok(())
                    } else {
                        Err(BatchSlotError::InvalidSignature)
                    }
                }),
        }
    }

    /// Canonical preimage shared by signing and verification.
    fn signing_bytes(
        chain_id: BatchSlotChainId,
        epoch: Epoch,
        authority: &BlsPublicKey,
        message: &BatchSlotMessage,
    ) -> Result<Vec<u8>, BatchSlotError> {
        bcs::to_bytes(&(chain_id, epoch, authority, message)).map_err(BatchSlotError::Encoding).map(
            |body| {
                let mut bytes = SIGNING_DOMAIN.to_vec();
                bytes.extend(body);
                bytes
            },
        )
    }
}

/// Whether durable execution has supplied a new slot's admission anchor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum SlotOpening {
    /// The slot may accept proposals against this state.
    Ready(BatchSlotParent),
    /// The selecting output must finish execution before this slot opens.
    Pending(B256),
}

impl SlotOpening {
    /// Expose an anchor only after the selecting output has become durable.
    fn parent(self) -> Result<BatchSlotParent, BatchSlotError> {
        match self {
            Self::Ready(parent) => Ok(parent),
            Self::Pending(_) => Err(BatchSlotError::OpeningNotFinalized),
        }
    }
}

/// Ordered state of one sender bucket, shared across all workers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct BucketSlot {
    /// Number of proposals already selected for this bucket.
    sequence: BatchSlotSequence,
    /// Current retry of the unfilled slot.
    view: BatchSlotView,
    /// Canonical admission anchor, or execution awaiting finalization.
    opening: SlotOpening,
    /// Distinct authors whose ordered votes count toward this retry.
    timeout_voters: BTreeSet<BlsPublicKey>,
}

/// Effect of applying an authenticated record in consensus order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchSlotTransition {
    /// This proposal filled the slot and must be executed.
    Selected,
    /// A new timeout vote was recorded, without reaching a quorum.
    TimeoutRecorded,
    /// A timeout quorum rotated the producer of the unfilled slot.
    ViewAdvanced,
    /// An earlier record already resolved the sequence, view, or vote.
    Unchanged,
}

/// Namespace for a durable availability vote, shared by every worker on one validator.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum BatchSlotVoteKey {
    /// One execution proposal per sender bucket, sequence, and view.
    Proposal {
        /// Proposal sequence in its committee epoch.
        slot: BatchSlotId,
        /// Retry whose reservation must not be released during the epoch.
        view: BatchSlotView,
    },
    /// One timeout record per bucket and authenticated timeout author.
    Timeout {
        /// Proposal sequence in its committee epoch.
        slot: BatchSlotId,
        /// Retry for which the author requests rotation.
        view: BatchSlotView,
        /// Committee identity signing the timeout.
        authority: BlsPublicKey,
    },
}

/// An authenticated vote decision to persist before acknowledging a batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BatchSlotVote {
    /// Chain domain in which this reservation is valid.
    chain_id: BatchSlotChainId,
    /// Committee epoch in which the vote was made.
    epoch: Epoch,
    /// Proposal or timeout namespace.
    key: BatchSlotVoteKey,
    /// Exact slot and view reserved by the vote.
    position: BatchSlotPosition,
    /// Digest of the complete signed record.
    digest: B256,
}

impl BatchSlotVote {
    /// Return the committee epoch authenticated by this vote.
    pub const fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Return the database key shared across workers.
    pub const fn key(&self) -> &BatchSlotVoteKey {
        &self.key
    }

    /// Check that replacing a durable reservation cannot acknowledge an equivocation.
    ///
    /// Every authorized retry has its own key. Old reservations remain intact through the
    /// epoch so delayed honest proposals can finish certification without reopening equivocation.
    pub fn permits(&self, next: &Self) -> Result<(), BatchSlotError> {
        match () {
            () if self.chain_id != next.chain_id => Err(BatchSlotError::WrongChain),
            () if self.key != next.key => Err(BatchSlotError::WrongReservationKey),
            () if self != next => Err(BatchSlotError::ConflictingVote),
            () => Ok(()),
        }
    }
}

/// Bounded, deterministic producer scheduling for an epoch.
///
/// Apply records to a private execution snapshot. Publish that snapshot only after the
/// entire consensus output is durable and its pending openings have been finalized.
#[derive(Clone, Debug)]
pub struct BatchSlots {
    /// Signature replay-protection domain.
    chain_id: BatchSlotChainId,
    /// Epoch identities and voting power.
    committee: Committee,
    /// Producers in canonical public-key order.
    producers: Vec<BlsPublicKey>,
    /// Nonzero committee size used for producer rotation.
    producer_count: NonZeroU32,
    /// Sender bucket count, preserving one parallel slot per validator and worker.
    bucket_count: NonZeroU32,
    /// One outstanding sequence per sender bucket across all workers.
    buckets: Vec<BucketSlot>,
}

impl BatchSlots {
    /// Initialize an epoch after the preceding epoch's execution is durable.
    pub fn new(
        chain_id: BatchSlotChainId,
        committee: Committee,
        parent: BatchSlotParent,
    ) -> Result<Self, BatchSlotError> {
        let producers: Vec<_> = committee
            .authorities()
            .iter()
            .map(|authority| *authority.protocol_key())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let count =
            u32::try_from(producers.len()).map_err(|_| BatchSlotError::CommitteeTooLarge)?;
        let producer_count = NonZeroU32::new(count).ok_or(BatchSlotError::EmptyCommittee)?;
        let total = producers
            .len()
            .checked_mul(committee.number_of_workers())
            .and_then(|total| u32::try_from(total).ok())
            .ok_or(BatchSlotError::CommitteeTooLarge)?;
        let bucket_count = NonZeroU32::new(total).ok_or(BatchSlotError::EmptyCommittee)?;
        let buckets = (0..bucket_count.get())
            .map(|_| BucketSlot {
                sequence: BatchSlotSequence::default(),
                view: BatchSlotView::default(),
                opening: SlotOpening::Ready(parent),
                timeout_voters: BTreeSet::new(),
            })
            .collect();
        Ok(Self { chain_id, committee, producers, producer_count, bucket_count, buckets })
    }

    /// Return the epoch whose committee controls these slots.
    pub fn epoch(&self) -> Epoch {
        self.committee.epoch()
    }

    /// Map every transaction from one sender to the same bucket across all workers.
    pub fn bucket(&self, sender: Address) -> BatchBucket {
        let prefix = sender
            .iter()
            .take(8)
            .enumerate()
            .fold(0_u64, |value, (index, byte)| value | (u64::from(*byte) << (index * 8)));
        // The remainder is strictly less than the nonzero u32 divisor.
        BatchBucket(u32::try_from(prefix % u64::from(self.bucket_count.get())).unwrap_or_default())
    }

    /// Iterate over the epoch's sender buckets.
    pub fn buckets(&self) -> impl Iterator<Item = BatchBucket> + '_ {
        (0..self.bucket_count.get()).map(BatchBucket)
    }

    /// Return the current slot only after its opening execution is durable.
    pub fn position(&self, bucket: BatchBucket) -> Result<BatchSlotPosition, BatchSlotError> {
        self.bucket_state(bucket).and_then(|slot| {
            slot.opening.parent().map(|parent| BatchSlotPosition {
                bucket,
                sequence: slot.sequence,
                view: slot.view,
                parent,
            })
        })
    }

    /// Capture a slot's canonical authorization, retaining it when the slot closes.
    pub fn authorization(
        &self,
        bucket: BatchBucket,
    ) -> Result<BatchSlotAuthorization, BatchSlotError> {
        self.position(bucket).map(|position| BatchSlotAuthorization {
            chain_id: self.chain_id,
            epoch: self.epoch(),
            position,
        })
    }

    /// Return the producer for a position in this epoch.
    pub fn producer(&self, position: BatchSlotPosition) -> Result<&BlsPublicKey, BatchSlotError> {
        self.bucket_state(position.bucket)?;
        let count = u64::from(self.producer_count.get());
        // Three terms, each below u32::MAX, cannot overflow u64.
        let ordinal =
            (u64::from(position.bucket.0) + position.sequence.0 % count + position.view.0 % count)
                % count;
        usize::try_from(ordinal)
            .ok()
            .and_then(|index| self.producers.get(index))
            .ok_or(BatchSlotError::UnknownBucket)
    }

    /// Sign a proposal only for the current producer and execution anchor.
    pub fn sign_proposal(
        &self,
        bucket: BatchBucket,
        authority: BlsPublicKey,
        batch: Batch,
        signer: &impl Signer,
    ) -> Result<SignedBatchSlotRecord, BatchSlotError> {
        let position = self.position(bucket)?;
        match () {
            () if self.producer(position)? != &authority => Err(BatchSlotError::WrongProducer),
            () if batch.epoch != self.epoch() => Err(BatchSlotError::WrongEpoch),
            () => SignedBatchSlotRecord::sign(
                self.chain_id,
                self.epoch(),
                authority,
                BatchSlotMessage::Proposal { position, batch },
                signer,
            ),
        }
    }

    /// Sign one member's request to retry the current unfilled slot.
    pub fn sign_timeout(
        &self,
        bucket: BatchBucket,
        authority: BlsPublicKey,
        signer: &impl Signer,
    ) -> Result<SignedBatchSlotRecord, BatchSlotError> {
        if self.committee.voting_power(&authority) == 0 {
            Err(BatchSlotError::UnknownAuthority)
        } else {
            self.position(bucket).and_then(|position| {
                SignedBatchSlotRecord::sign(
                    self.chain_id,
                    self.epoch(),
                    authority,
                    BatchSlotMessage::Timeout { position },
                    signer,
                )
            })
        }
    }

    /// Authenticate an availability vote for any authorized view of the current slot.
    ///
    /// The caller must also validate the execution body, and persist this decision before
    /// acknowledging it. A delayed proposal can finish certification after a retry because
    /// each view keeps its own durable reservation through the epoch.
    pub fn vote(&self, record: &SignedBatchSlotRecord) -> Result<BatchSlotVote, BatchSlotError> {
        self.authorization(record.message().position().bucket)
            .and_then(|authorization| self.vote_for_authorization(record, &authorization))
    }

    /// Authenticate a vote against an authorization retained from canonical execution.
    ///
    /// Historical authorizations let delayed honest headers finish certification. They must
    /// come from the current epoch's canonical slot history, never from the submitted record.
    /// The caller must fully validate the execution body and durably reserve the resulting vote.
    pub fn vote_for_authorization(
        &self,
        record: &SignedBatchSlotRecord,
        authorization: &BatchSlotAuthorization,
    ) -> Result<BatchSlotVote, BatchSlotError> {
        record.verify(self.chain_id, &self.committee)?;
        let position = record.message().position();
        match () {
            () if authorization.chain_id != self.chain_id => Err(BatchSlotError::WrongChain),
            () if authorization.epoch != self.epoch() => Err(BatchSlotError::WrongEpoch),
            () if record.slot() != authorization.id() => Err(BatchSlotError::StalePosition),
            () if position.parent != authorization.position.parent => {
                Err(BatchSlotError::WrongParent)
            }
            () if position.view > authorization.position.view => Err(BatchSlotError::FutureView),
            () => {
                let key = match record.message() {
                    BatchSlotMessage::Proposal { batch, .. } => match () {
                        () if self.producer(position)? != record.authority() => {
                            Err(BatchSlotError::WrongProducer)
                        }
                        () if batch.epoch != self.epoch() => Err(BatchSlotError::WrongEpoch),
                        () => Ok(BatchSlotVoteKey::Proposal {
                            slot: record.slot(),
                            view: position.view,
                        }),
                    },
                    BatchSlotMessage::Timeout { .. } => Ok(BatchSlotVoteKey::Timeout {
                        slot: record.slot(),
                        view: position.view,
                        authority: *record.authority(),
                    }),
                }?;
                record.digest().map(|digest| BatchSlotVote {
                    chain_id: self.chain_id,
                    epoch: self.epoch(),
                    key,
                    position,
                    digest,
                })
            }
        }
    }

    /// Authenticate and apply one record in consensus order.
    ///
    /// The caller must validate a proposal's execution transactions, sender buckets, and
    /// opening-state nonces before calling this method. A selected proposal must execute
    /// before the snapshot is published or its next slot can be used.
    pub fn apply(
        &mut self,
        record: &SignedBatchSlotRecord,
        output: B256,
    ) -> Result<BatchSlotTransition, BatchSlotError> {
        record.verify(self.chain_id, &self.committee)?;
        match record.message() {
            BatchSlotMessage::Proposal { position, batch } => match () {
                () if self.producer(*position)? != record.authority() => {
                    Err(BatchSlotError::WrongProducer)
                }
                () if batch.epoch != self.epoch() => Err(BatchSlotError::WrongEpoch),
                () => self.apply_proposal(*position, output),
            },
            BatchSlotMessage::Timeout { position } => {
                self.apply_timeout(*position, *record.authority())
            }
        }
    }

    /// Open selected slots against the durable head of their entire consensus output.
    pub fn finalize_openings(
        &mut self,
        consensus: B256,
        execution: B256,
    ) -> Result<(), BatchSlotError> {
        if self
            .buckets
            .iter()
            .any(|slot| matches!(slot.opening, SlotOpening::Pending(hash) if hash != consensus))
        {
            Err(BatchSlotError::DifferentOutputPending)
        } else {
            self.buckets.iter_mut().for_each(|slot| {
                if matches!(slot.opening, SlotOpening::Pending(_)) {
                    slot.opening = SlotOpening::Ready(BatchSlotParent::new(consensus, execution));
                }
            });
            Ok(())
        }
    }

    /// Look up a bucket without trusting a remote ordinal.
    fn bucket_state(&self, bucket: BatchBucket) -> Result<&BucketSlot, BatchSlotError> {
        usize::try_from(bucket.0)
            .ok()
            .and_then(|index| self.buckets.get(index))
            .ok_or(BatchSlotError::UnknownBucket)
    }

    /// Check sequence and anchor; old retry views remain eligible until the slot is filled.
    fn is_current_sequence(&self, position: BatchSlotPosition) -> Result<bool, BatchSlotError> {
        let slot = self.bucket_state(position.bucket)?;
        match () {
            () if position.sequence < slot.sequence => Ok(false),
            () if position.sequence > slot.sequence => Err(BatchSlotError::FutureSequence),
            () if position.parent != slot.opening.parent()? => Err(BatchSlotError::WrongParent),
            () if position.view > slot.view => Err(BatchSlotError::FutureView),
            () => Ok(true),
        }
    }

    /// Select the first proposal for a sequence and fence its successor on durable execution.
    fn apply_proposal(
        &mut self,
        position: BatchSlotPosition,
        output: B256,
    ) -> Result<BatchSlotTransition, BatchSlotError> {
        if !self.is_current_sequence(position)? {
            Ok(BatchSlotTransition::Unchanged)
        } else {
            let sequence = position.sequence.next()?;
            let index =
                usize::try_from(position.bucket.0).map_err(|_| BatchSlotError::UnknownBucket)?;
            let slot = self.buckets.get_mut(index).ok_or(BatchSlotError::UnknownBucket)?;
            slot.sequence = sequence;
            slot.view = BatchSlotView::default();
            slot.opening = SlotOpening::Pending(output);
            slot.timeout_voters.clear();
            Ok(BatchSlotTransition::Selected)
        }
    }

    /// Count each member once and rotate only after a quorum votes for the exact current view.
    fn apply_timeout(
        &mut self,
        position: BatchSlotPosition,
        author: BlsPublicKey,
    ) -> Result<BatchSlotTransition, BatchSlotError> {
        let slot = self.bucket_state(position.bucket)?;
        match () {
            () if !self.is_current_sequence(position)? => Ok(BatchSlotTransition::Unchanged),
            () if position.view != slot.view || slot.timeout_voters.contains(&author) => {
                Ok(BatchSlotTransition::Unchanged)
            }
            () => {
                let power = slot.timeout_voters.iter().try_fold(
                    self.committee.voting_power(&author),
                    |sum, voter| {
                        sum.checked_add(self.committee.voting_power(voter))
                            .ok_or(BatchSlotError::VotingPowerOverflow)
                    },
                )?;
                let next_view = if power >= self.committee.quorum_threshold() {
                    slot.view.next()?
                } else {
                    slot.view
                };
                let index = usize::try_from(position.bucket.0)
                    .map_err(|_| BatchSlotError::UnknownBucket)?;
                let slot = self.buckets.get_mut(index).ok_or(BatchSlotError::UnknownBucket)?;
                if power >= self.committee.quorum_threshold() {
                    slot.view = next_view;
                    slot.timeout_voters.clear();
                    Ok(BatchSlotTransition::ViewAdvanced)
                } else {
                    slot.timeout_voters.insert(author);
                    Ok(BatchSlotTransition::TimeoutRecorded)
                }
            }
        }
    }
}

/// Invalid authentication, ordering, committee configuration, or canonical encoding.
#[derive(Debug)]
pub enum BatchSlotError {
    /// A committee must contain at least one producer.
    EmptyCommittee,
    /// The committee cannot be represented by bucket ordinals.
    CommitteeTooLarge,
    /// A bucket lies outside the epoch's committee.
    UnknownBucket,
    /// The author has no voting power in this epoch.
    UnknownAuthority,
    /// The record belongs to a different chain.
    WrongChain,
    /// The record belongs to a different epoch.
    WrongEpoch,
    /// The signature does not authenticate the content.
    InvalidSignature,
    /// The proposal was not signed by its assigned producer.
    WrongProducer,
    /// The admission anchor differs from the slot's opening.
    WrongParent,
    /// The sequence has not opened.
    FutureSequence,
    /// Consensus has not approved this retry view.
    FutureView,
    /// Execution has not finalized the new slot's admission anchor.
    OpeningNotFinalized,
    /// Finalization tried to combine different consensus outputs.
    DifferentOutputPending,
    /// A sequence number cannot advance further.
    SequenceExhausted,
    /// A retry view cannot advance further.
    ViewExhausted,
    /// Committee voting power cannot be summed without overflow.
    VotingPowerOverflow,
    /// The native record marker was absent.
    InvalidPrefix,
    /// A fresh vote refers to a slot or view that is no longer current.
    StalePosition,
    /// A reservation was looked up under the wrong proposal or timeout namespace.
    WrongReservationKey,
    /// Another proposal was already acknowledged for the same slot and view.
    ConflictingVote,
    /// Canonical serialization or decoding failed.
    Encoding(bcs::Error),
}

impl fmt::Display for BatchSlotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCommittee => formatter.write_str("empty batch-slot committee"),
            Self::CommitteeTooLarge => formatter.write_str("batch-slot committee is too large"),
            Self::UnknownBucket => formatter.write_str("unknown batch bucket"),
            Self::UnknownAuthority => formatter.write_str("unknown batch-slot authority"),
            Self::WrongChain => formatter.write_str("batch-slot chain mismatch"),
            Self::WrongEpoch => formatter.write_str("batch-slot epoch mismatch"),
            Self::InvalidSignature => formatter.write_str("invalid batch-slot signature"),
            Self::WrongProducer => formatter.write_str("wrong producer for batch slot"),
            Self::WrongParent => formatter.write_str("batch-slot opening mismatch"),
            Self::FutureSequence => {
                formatter.write_str("batch-slot sequence is ahead of consensus")
            }
            Self::FutureView => formatter.write_str("batch-slot retry is ahead of consensus"),
            Self::OpeningNotFinalized => formatter.write_str("batch-slot opening is not finalized"),
            Self::DifferentOutputPending => formatter.write_str("another slot output is pending"),
            Self::SequenceExhausted => formatter.write_str("batch-slot sequence exhausted"),
            Self::ViewExhausted => formatter.write_str("batch-slot retry exhausted"),
            Self::VotingPowerOverflow => formatter.write_str("batch-slot voting power overflow"),
            Self::InvalidPrefix => formatter.write_str("invalid batch-slot record prefix"),
            Self::StalePosition => formatter.write_str("batch-slot vote is not current"),
            Self::WrongReservationKey => formatter.write_str("batch-slot reservation key mismatch"),
            Self::ConflictingVote => {
                formatter.write_str("batch-slot vote conflicts with a durable reservation")
            }
            Self::Encoding(error) => write!(formatter, "batch-slot encoding failed: {error}"),
        }
    }
}

impl std::error::Error for BatchSlotError {}

#[cfg(test)]
mod tests {
    //! Ordering and authentication regressions independent of transport timing.

    use super::*;
    use crate::{BlsKeypair, CommitteeBuilder};
    use rand::{rngs::StdRng, SeedableRng};

    /// Four deterministic, equally weighted committee members and their initial slots.
    struct Fixture {
        /// Keys used to authenticate proposals and timeout votes.
        keys: Vec<BlsKeypair>,
        /// Initial canonical slot state.
        slots: BatchSlots,
    }

    impl Fixture {
        /// Build a four-member committee with a quorum of three.
        fn new() -> Result<Self, BatchSlotError> {
            let mut rng = StdRng::seed_from_u64(1377);
            let keys: Vec<_> = (0..4).map(|_| BlsKeypair::generate(&mut rng)).collect();
            let committee = keys
                .iter()
                .fold(CommitteeBuilder::new(7), |mut builder, key| {
                    builder.add_authority(*key.public(), Address::ZERO);
                    builder
                })
                .build();
            BatchSlots::new(
                2017.into(),
                committee,
                BatchSlotParent::new(B256::repeat_byte(1), B256::repeat_byte(2)),
            )
            .map(|slots| Self { keys, slots })
        }

        /// Sign a proposal using the producer selected by the supplied snapshot.
        fn proposal(
            &self,
            slots: &BatchSlots,
            bucket: BatchBucket,
        ) -> Result<SignedBatchSlotRecord, BatchSlotError> {
            let producer = slots.producer(slots.position(bucket)?)?;
            self.keys
                .iter()
                .find(|key| key.public() == producer)
                .ok_or(BatchSlotError::UnknownAuthority)
                .and_then(|key| {
                    slots.sign_proposal(
                        bucket,
                        *producer,
                        Batch {
                            transactions: vec![vec![1]],
                            epoch: slots.epoch(),
                            beneficiary: Address::ZERO,
                            base_fee_per_gas: 1,
                            worker_id: 0,
                            received_at: None,
                        },
                        key,
                    )
                })
        }

        /// Sign one vote for the current view from each member.
        fn timeouts(
            &self,
            slots: &BatchSlots,
            bucket: BatchBucket,
        ) -> Result<Vec<SignedBatchSlotRecord>, BatchSlotError> {
            self.keys.iter().map(|key| slots.sign_timeout(bucket, *key.public(), key)).collect()
        }

        /// Order the three distinct votes needed for one retry.
        fn rotate(
            &self,
            slots: &mut BatchSlots,
            bucket: BatchBucket,
        ) -> Result<(), BatchSlotError> {
            self.timeouts(slots, bucket)?
                .iter()
                .take(3)
                .try_for_each(|vote| slots.apply(vote, B256::repeat_byte(3)).map(|_| ()))
        }
    }

    #[test]
    fn repeated_votes_do_not_replace_a_timeout_quorum() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let original = slots.position(bucket)?;
        let votes = fixture.timeouts(&slots, bucket)?;
        votes.iter().take(2).try_for_each(|vote| {
            assert_eq!(slots.apply(vote, B256::ZERO)?, BatchSlotTransition::TimeoutRecorded);
            assert_eq!(slots.apply(vote, B256::ZERO)?, BatchSlotTransition::Unchanged);
            Ok::<_, BatchSlotError>(())
        })?;
        assert_eq!(slots.position(bucket)?, original);
        let third = votes.get(2).ok_or(BatchSlotError::UnknownAuthority)?;
        assert_eq!(slots.apply(third, B256::ZERO)?, BatchSlotTransition::ViewAdvanced);
        assert_eq!(slots.position(bucket)?.view().value(), 1);
        assert_eq!(slots.position(bucket)?.parent(), original.parent());
        assert_eq!(slots.apply(third, B256::ZERO)?, BatchSlotTransition::Unchanged);
        Ok(())
    }

    #[test]
    fn timeout_quorum_allows_another_producer_to_fill_the_slot() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let original = *slots.producer(slots.position(bucket)?)?;
        fixture.rotate(&mut slots, bucket)?;
        let replacement = fixture.proposal(&slots, bucket)?;
        assert_ne!(replacement.authority(), &original);
        assert_eq!(slots.apply(&replacement, B256::repeat_byte(4))?, BatchSlotTransition::Selected);
        Ok(())
    }

    #[test]
    fn late_original_proposal_wins_only_while_its_slot_is_unfilled() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let original = fixture.proposal(&slots, bucket)?;
        fixture.rotate(&mut slots, bucket)?;
        let replacement = fixture.proposal(&slots, bucket)?;
        assert_eq!(slots.apply(&original, B256::repeat_byte(4))?, BatchSlotTransition::Selected);
        assert_eq!(
            slots.apply(&replacement, B256::repeat_byte(4))?,
            BatchSlotTransition::Unchanged
        );
        assert_eq!(slots.apply(&original, B256::repeat_byte(4))?, BatchSlotTransition::Unchanged);
        Ok(())
    }

    #[test]
    fn replacement_proposal_fences_a_late_original() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let original = fixture.proposal(&slots, bucket)?;
        fixture.rotate(&mut slots, bucket)?;
        let replacement = fixture.proposal(&slots, bucket)?;
        assert_eq!(slots.apply(&replacement, B256::repeat_byte(4))?, BatchSlotTransition::Selected);
        assert_eq!(slots.apply(&original, B256::repeat_byte(4))?, BatchSlotTransition::Unchanged);
        Ok(())
    }

    #[test]
    fn successor_waits_for_durable_execution_of_the_entire_output() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let proposal = fixture.proposal(&slots, bucket)?;
        let output = B256::repeat_byte(4);
        let execution = B256::repeat_byte(5);
        assert_eq!(slots.apply(&proposal, output)?, BatchSlotTransition::Selected);
        assert!(matches!(slots.position(bucket), Err(BatchSlotError::OpeningNotFinalized)));
        assert!(matches!(
            slots.finalize_openings(B256::ZERO, execution),
            Err(BatchSlotError::DifferentOutputPending)
        ));
        assert!(matches!(slots.position(bucket), Err(BatchSlotError::OpeningNotFinalized)));
        slots.finalize_openings(output, execution)?;
        let next = slots.position(bucket)?;
        assert_eq!(next.sequence().value(), 1);
        assert_eq!(next.view().value(), 0);
        assert_eq!(next.parent(), BatchSlotParent::new(output, execution));
        assert_eq!(slots.apply(&proposal, output)?, BatchSlotTransition::Unchanged);
        Ok(())
    }

    #[test]
    fn valid_committee_signature_does_not_authorize_another_producers_slot(
    ) -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let original = fixture.proposal(&slots, bucket)?;
        let other = fixture
            .keys
            .iter()
            .find(|key| key.public() != original.authority())
            .ok_or(BatchSlotError::UnknownAuthority)?;
        let forged = SignedBatchSlotRecord::sign(
            slots.chain_id,
            slots.epoch(),
            *other.public(),
            original.message.clone(),
            other,
        )?;
        assert!(matches!(slots.apply(&forged, B256::ZERO), Err(BatchSlotError::WrongProducer)));
        assert_eq!(slots.position(bucket)?.sequence().value(), 0);
        Ok(())
    }

    #[test]
    fn authentication_binds_chain_epoch_and_proposal_body() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let original = fixture.proposal(&slots, bucket)?;
        let mut wrong_chain = original.clone();
        wrong_chain.chain_id = 2018.into();
        assert!(matches!(slots.apply(&wrong_chain, B256::ZERO), Err(BatchSlotError::WrongChain)));
        let mut wrong_epoch = original.clone();
        wrong_epoch.epoch = 8;
        assert!(matches!(slots.apply(&wrong_epoch, B256::ZERO), Err(BatchSlotError::WrongEpoch)));
        let mut wrong_body = original.clone();
        if let BatchSlotMessage::Proposal { batch, .. } = &mut wrong_body.message {
            batch.transactions.push(vec![2]);
        }
        assert!(matches!(
            slots.apply(&wrong_body, B256::ZERO),
            Err(BatchSlotError::InvalidSignature)
        ));
        assert_eq!(slots.position(bucket)?.sequence().value(), 0);
        let decoded = SignedBatchSlotRecord::decode(&original.encode()?)?;
        assert_eq!(slots.apply(&decoded, B256::ZERO)?, BatchSlotTransition::Selected);
        Ok(())
    }

    #[test]
    fn bucket_sequences_progress_independently() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let first = slots.bucket(Address::ZERO);
        let second = slots.bucket(Address::repeat_byte(1));
        assert_ne!(first, second);
        let second_before = slots.position(second)?;
        let proposal = fixture.proposal(&slots, first)?;
        assert_eq!(slots.apply(&proposal, B256::ZERO)?, BatchSlotTransition::Selected);
        assert_eq!(slots.position(second)?, second_before);
        Ok(())
    }

    #[test]
    fn restored_reservation_rejects_producer_equivocation() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let bucket = fixture.slots.bucket(Address::ZERO);
        let first = fixture.proposal(&fixture.slots, bucket)?;
        let reserved = fixture.slots.vote(&first)?;
        let encoded = bcs::to_bytes(&reserved).map_err(BatchSlotError::Encoding)?;
        let restored: BatchSlotVote =
            bcs::from_bytes(&encoded).map_err(BatchSlotError::Encoding)?;
        assert_eq!(reserved, restored);
        restored.permits(&fixture.slots.vote(&first)?)?;
        let signer = fixture
            .keys
            .iter()
            .find(|key| key.public() == first.authority())
            .ok_or(BatchSlotError::UnknownAuthority)?;
        let mut different = first.message.clone();
        if let BatchSlotMessage::Proposal { batch, .. } = &mut different {
            batch.transactions.push(vec![2]);
        }
        let conflicting = SignedBatchSlotRecord::sign(
            fixture.slots.chain_id,
            fixture.slots.epoch(),
            *first.authority(),
            different,
            signer,
        )?;
        assert!(matches!(
            restored.permits(&fixture.slots.vote(&conflicting)?),
            Err(BatchSlotError::ConflictingVote)
        ));
        Ok(())
    }

    #[test]
    fn retry_views_keep_independent_reservations_for_late_votes() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let original = fixture.proposal(&slots, bucket)?;
        let reserved = slots.vote(&original)?;
        fixture.rotate(&mut slots, bucket)?;
        let late = slots.vote(&original);
        assert!(late.is_ok(), "a delayed honest proposal must still be able to obtain votes");
        reserved.permits(&late?)?;
        let replacement = fixture.proposal(&slots, bucket)?;
        let next = slots.vote(&replacement)?;
        assert_ne!(reserved.key(), next.key());
        assert!(matches!(reserved.permits(&next), Err(BatchSlotError::WrongReservationKey)));
        assert!(matches!(next.permits(&reserved), Err(BatchSlotError::WrongReservationKey)));
        assert_eq!(slots.apply(&original, B256::ZERO)?, BatchSlotTransition::Selected);
        Ok(())
    }

    #[test]
    fn closed_slot_authorization_keeps_delayed_headers_certifiable() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let original = fixture.proposal(&slots, bucket)?;
        let authorization = slots.authorization(bucket)?;
        let reserved = slots.vote(&original)?;
        assert_eq!(slots.apply(&original, B256::repeat_byte(4))?, BatchSlotTransition::Selected);
        slots.finalize_openings(B256::repeat_byte(4), B256::repeat_byte(5))?;
        assert!(matches!(slots.vote(&original), Err(BatchSlotError::StalePosition)));
        let late = slots.vote_for_authorization(&original, &authorization);
        assert!(
            late.is_ok(),
            "the retained authorization must let honest peers finish a delayed header"
        );
        reserved.permits(&late?)?;
        assert_eq!(slots.apply(&original, B256::repeat_byte(6))?, BatchSlotTransition::Unchanged);
        Ok(())
    }

    #[test]
    fn closed_slot_history_does_not_authorize_an_unopened_retry() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let mut slots = fixture.slots.clone();
        let bucket = slots.bucket(Address::ZERO);
        let original = fixture.proposal(&slots, bucket)?;
        let authorization = slots.authorization(bucket)?;
        assert_eq!(slots.apply(&original, B256::repeat_byte(4))?, BatchSlotTransition::Selected);
        slots.finalize_openings(B256::repeat_byte(4), B256::repeat_byte(5))?;
        let mut forged = original.message.clone();
        if let BatchSlotMessage::Proposal { position, .. } = &mut forged {
            position.view = BatchSlotView(1);
        }
        let author = *slots.producer(forged.position())?;
        let key = fixture
            .keys
            .iter()
            .find(|key| key.public() == &author)
            .ok_or(BatchSlotError::UnknownAuthority)?;
        let forged =
            SignedBatchSlotRecord::sign(slots.chain_id, slots.epoch(), author, forged, key)?;
        assert!(matches!(
            slots.vote_for_authorization(&forged, &authorization),
            Err(BatchSlotError::FutureView)
        ));
        Ok(())
    }

    #[test]
    fn multiple_workers_preserve_parallel_proposal_capacity() -> Result<(), BatchSlotError> {
        let fixture = Fixture::new()?;
        let workers = std::num::NonZeroUsize::new(2).ok_or(BatchSlotError::CommitteeTooLarge)?;
        let committee = fixture.slots.committee.with_num_workers(workers);
        let slots = BatchSlots::new(
            fixture.slots.chain_id,
            committee,
            BatchSlotParent::new(B256::ZERO, B256::ZERO),
        )?;
        let positions =
            slots.buckets().map(|bucket| slots.position(bucket)).collect::<Result<Vec<_>, _>>()?;
        assert_eq!(positions.len(), 8, "four validators with two workers need eight slots");
        let owners: Vec<_> =
            positions.into_iter().map(|position| slots.producer(position)).collect();
        assert!(
            owners.iter().all(Result::is_ok),
            "every worker slot must map to a committee producer"
        );
        let counts = owners.into_iter().collect::<Result<Vec<_>, _>>()?.into_iter().fold(
            std::collections::BTreeMap::<BlsPublicKey, usize>::new(),
            |mut counts, owner| {
                *counts.entry(*owner).or_default() += 1;
                counts
            },
        );
        assert_eq!(counts.len(), 4);
        assert!(counts.values().all(|count| *count == 2));
        Ok(())
    }
}
