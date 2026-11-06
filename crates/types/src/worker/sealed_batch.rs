//! Batch implementation for consensus.
//!
//! Batches hold transactions and other data. This type is used to represent worker proposals that
//! have reached quorum.

use crate::{
    crypto, encode, Address, BlockHash, BlsPublicKey, ByteSlice, ByteVec, Epoch, ExecHeader,
    RpcInfo, TimestampSec, MIN_PROTOCOL_BASE_FEE,
};
use serde::{
    de::{self, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::fmt::{self, Debug};
use thiserror::Error;

use super::WorkerId;

/// The batch for workers to communicate for consensus.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SealedBatch {
    /// The immutable batch fields.
    pub batch: Batch,
    /// The immutable digest of the batch.
    pub digest: BlockHash,
}

impl SealedBatch {
    /// Create a new instance of Self.
    ///
    /// WARNING: this does not verify the provided digest matches the provided batch.
    pub fn new(batch: Batch, digest: BlockHash) -> Self {
        Self { batch, digest }
    }

    /// Consume self to extract the batch so it can be modified.
    pub fn unseal(self) -> Batch {
        self.batch
    }

    /// Return the sealed batch fields.
    pub fn batch(&self) -> &Batch {
        &self.batch
    }

    /// Return the digest of the sealed batch.
    pub fn digest(&self) -> BlockHash {
        self.digest
    }

    /// Split Self into separate parts.
    ///
    /// This is the inverse of [`Batch::seal_slow`].
    pub fn split(self) -> (Batch, BlockHash) {
        (self.batch, self.digest)
    }

    /// Size of the sealed batch.
    pub fn size(&self) -> usize {
        self.batch.size() + size_of::<BlockHash>()
    }
}

/// The batch for workers to communicate for consensus.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Batch {
    /// The collection of transactions in this batch as bytes.
    ///
    /// Decoded through [`deserialize_transactions`], which rejects a zero-byte (empty) transaction
    /// and bounds the transaction count at [`MAX_TXS_PER_BATCH`] before allocating — an untrusted
    /// peer must not be able to make a tiny compressed record decode into a huge `Vec<Vec<u8>>`
    /// (see the epoch-pack import path). Each transaction is encoded and decoded as one byte
    /// string ([`serialize_transactions`]), which is byte-identical in `bcs` to a sequence of
    /// `u8`, so the digest/wire format are stable.
    #[serde(
        serialize_with = "serialize_transactions",
        deserialize_with = "deserialize_transactions"
    )]
    pub transactions: Vec<Vec<u8>>,
    /// The epoch that this batch belongs to.
    pub epoch: Epoch,
    /// The 160-bit address to which all fees collected from the successful mining of this batch
    /// be transferred; formally Hc.
    pub beneficiary: Address,
    /// The EIP-1559 base fee in effect for this batch.
    ///
    /// Unlike Ethereum, this does not move from batch to batch. The value is keyed by `worker_id`
    /// and is constant for the whole epoch, so every validator's worker N carries the same base
    /// fee until the epoch closes; a batch carrying any other value is rejected as
    /// `InvalidBaseFee`.
    ///
    /// It is recomputed only at the epoch boundary, from that worker's gas accumulated over the
    /// epoch against its target: either by the EIP-1559 formula (floored at
    /// `MIN_PROTOCOL_BASE_FEE`) or pinned to a fixed value, according to the worker's on-chain fee
    /// strategy. Collected base fees go to the configured base fee recipient.
    pub base_fee_per_gas: u64,
    /// The worker id for the worker that orginated this batch.
    /// Worker ids will be consistent accross validators (i.e. worker 0 talks to othere worker 0s,
    /// etc). We can use this for tracking to support base fee calculations.
    /// Note: worker id 0 is the default.
    pub worker_id: WorkerId,
    /// Timestamp of when the entity was received by another node. This will help
    /// calculate latencies that are not affected by clock drift or network
    /// delays. This field is not set for own batchs.
    #[serde(skip)]
    // This field changes often so don't serialize (i.e. don't use it in the digest)
    pub received_at: Option<TimestampSec>,
}

impl Batch {
    /// Create a new batch for testing only!
    ///
    /// This is NOT a valid batch for consensus.
    pub fn new_for_test(
        transactions: Vec<Vec<u8>>,
        header: ExecHeader,
        worker_id: WorkerId,
        epoch: Epoch,
    ) -> Self {
        Self {
            transactions,
            epoch,
            beneficiary: header.beneficiary,
            base_fee_per_gas: header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE),
            worker_id,
            received_at: None,
        }
    }

    /// Size of the batch in bytes (including transactions).
    pub fn size(&self) -> usize {
        size_of::<Self>() + self.transactions.iter().map(|tx| tx.len()).sum::<usize>()
    }

    /// Digest for this batch (the hash of the sealed header).
    ///
    /// NOTE: `Self::received_at` is skipped during serialization and is excluded from the digest.
    pub fn digest(&self) -> BlockHash {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(encode(self).as_ref());
        // finalize
        BlockHash::from_slice(hasher.finalize().as_bytes())
    }

    /// Pass a reference to a collection of transaction bytes;
    pub fn transactions(&self) -> &Vec<Vec<u8>> {
        &self.transactions
    }

    /// Returns a mutable reference to a collection of transaction bytes.
    pub fn transactions_mut(&mut self) -> &mut Vec<Vec<u8>> {
        &mut self.transactions
    }

    /// Returns the received at time if available.
    pub fn received_at(&self) -> Option<TimestampSec> {
        self.received_at
    }

    /// Sets the recieved at field.
    pub fn set_received_at(&mut self, time: TimestampSec) {
        self.received_at = Some(time)
    }

    /// Seal the header with a known hash.
    ///
    /// WARNING: This method does not verify whether the hash is correct.
    pub fn seal(self, digest: BlockHash) -> SealedBatch {
        SealedBatch::new(self, digest)
    }

    /// Seal the batch.
    ///
    /// Calculate the hash and seal the batch so it can't be changed.
    ///
    /// NOTE: `Batch::received_at` is skipped during serialization and is excluded from the
    /// digest.
    pub fn seal_slow(self) -> SealedBatch {
        let digest = self.digest();
        self.seal(digest)
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            transactions: vec![],
            received_at: None,
            epoch: Epoch::default(),
            beneficiary: Address::ZERO,
            worker_id: 0,
            base_fee_per_gas: MIN_PROTOCOL_BASE_FEE,
        }
    }
}

impl From<&SealedBatch> for Vec<u8> {
    fn from(value: &SealedBatch) -> Self {
        crate::encode(value)
    }
}

impl From<&[u8]> for SealedBatch {
    fn from(value: &[u8]) -> Self {
        crate::decode(value)
    }
}

/// Return the max gas per batch in effect at timestamp.
/// Currently allways 30,000,000 but can change in the future at a fork.
pub fn max_batch_gas(_epoch: Epoch) -> u64 {
    30_000_000
}

/// Max batch size in effect at an epoch, measured in bytes.
/// Currently always 1,000,000 but can change in the future at a fork.
///
/// Fork changes that lower this limit must also lower [`min_batch_size`] and extend
/// `min_batch_size_bounds_every_epoch` with the fork boundary and its adjacent epochs. Fork changes
/// that raise it must also raise [`MAX_TXS_PER_BATCH`] (or a valid batch would fail to decode) and
/// extend `max_txs_per_batch_bounds_every_epoch` the same way.
/// The transaction pool checks its admission byte limit against that floor only once
/// at node startup: the pool and its validator persist across epoch changes.
pub const fn max_batch_size(_epoch: Epoch) -> usize {
    1_000_000
}

/// Upper bound on the number of transactions a single [`Batch`] may carry, enforced on decode by
/// [`deserialize_transactions`].
///
/// A valid transaction is non-empty (≥ 1 byte) and a batch's transaction bytes never exceed
/// [`max_batch_size`], so a legitimate batch can never hold more than `max_batch_size`
/// transactions. Bounding the count on decode stops an untrusted peer from making a small
/// (compressible) record deserialize into a `Vec<Vec<u8>>` far larger than its byte content — each
/// entry costs `size_of::<Vec<u8>>()` (24 bytes on 64-bit) beyond its data.
pub const MAX_TXS_PER_BATCH: usize = max_batch_size(0);

/// Serialize a batch's transaction list as a sequence of byte strings: each transaction is one
/// length prefix and one copy (`serialize_bytes`) instead of a call per byte. In `bcs` this is
/// byte-identical to the derived encoding of `Vec<Vec<u8>>`, so batch digests do not change.
fn serialize_transactions<S: Serializer>(
    txs: &[Vec<u8>],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.collect_seq(txs.iter().map(|tx| ByteSlice(tx)))
}

/// Deserialize a batch's transaction list, rejecting inputs an honest producer never creates and an
/// attacker uses to blow up memory during decode:
/// - a **zero-byte (empty) transaction** is invalid (a real transaction is a non-empty RLP/EIP-2718
///   envelope; see the batch validator's `EmptyBatch` and transaction recovery), and
/// - the transaction count is bounded at [`MAX_TXS_PER_BATCH`], checked against the declared length
///   **before** allocating so a crafted huge count cannot force a large up-front allocation.
///
/// Each transaction is read as one byte string (see [`crate::byte_vec`]) rather than byte by byte.
/// Honest batches decode unchanged; only serialization (unchanged bytes) feeds the digest, so this
/// does not affect any batch hash.
fn deserialize_transactions<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct TransactionsVisitor;

    impl<'de> Visitor<'de> for TransactionsVisitor {
        type Value = Vec<Vec<u8>>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "a sequence of at most {MAX_TXS_PER_BATCH} non-empty transactions")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            // Reject an absurd declared length before allocating anything for it.
            if let Some(declared) = seq.size_hint() {
                if declared > MAX_TXS_PER_BATCH {
                    return Err(de::Error::custom(format!(
                        "batch declares {declared} transactions, exceeds MAX_TXS_PER_BATCH \
                         ({MAX_TXS_PER_BATCH})"
                    )));
                }
            }
            // Cautious initial capacity: never trust the declared length for the allocation size.
            let mut txs: Vec<Vec<u8>> = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
            while let Some(ByteVec(tx)) = seq.next_element::<ByteVec>()? {
                if tx.is_empty() {
                    return Err(de::Error::custom("zero-byte transaction is invalid"));
                }
                if txs.len() >= MAX_TXS_PER_BATCH {
                    return Err(de::Error::custom(format!(
                        "batch exceeds MAX_TXS_PER_BATCH ({MAX_TXS_PER_BATCH}) transactions"
                    )));
                }
                txs.push(tx);
            }
            Ok(txs)
        }
    }

    deserializer.deserialize_seq(TransactionsVisitor)
}

/// Smallest batch byte limit across every epoch this build can serve.
///
/// Used by startup guards whose consumers cannot update their limits at epoch boundaries.
/// Keep this floor in sync with every fork that lowers [`max_batch_size`].
pub fn min_batch_size() -> usize {
    max_batch_size(0)
}

/// Startup admission must fit every supported batch-size schedule segment.
#[cfg(test)]
#[test]
fn min_batch_size_bounds_every_epoch() {
    // Add each batch-size fork boundary and its adjacent epochs when the schedule changes.
    [0, 1, Epoch::MAX].into_iter().for_each(|epoch| {
        assert!(
            min_batch_size() <= max_batch_size(epoch),
            "batch byte floor exceeds epoch {epoch}"
        );
    });
}

#[cfg(test)]
mod transaction_bounds_tests {
    use super::{max_batch_size, Batch, MAX_TXS_PER_BATCH};
    use crate::{decode, encode, try_decode, Address, Epoch, ExecHeader, WorkerId};
    use serde::{Deserialize, Serialize};

    /// `Batch` as it was derived before its transactions were encoded as byte strings (each
    /// transaction element by element). The oracle the byte-string encoding must match exactly:
    /// batch digests hash these bytes.
    #[derive(Serialize, Deserialize)]
    struct ElementwiseBatch {
        transactions: Vec<Vec<u8>>,
        epoch: Epoch,
        beneficiary: Address,
        base_fee_per_gas: u64,
        worker_id: WorkerId,
    }

    /// Encoding each transaction as one byte string leaves a batch's bytes (and so its digest)
    /// exactly as the element-by-element derive wrote them, including at the ULEB128 length
    /// boundaries, and each encoding decodes the other's bytes.
    #[test]
    fn transactions_encode_identically_as_byte_strings() {
        let shapes: [&[usize]; 5] = [&[], &[1], &[127, 128], &[16_383, 16_384], &[200; 500]];
        for shape in shapes {
            let transactions: Vec<Vec<u8>> = shape
                .iter()
                .enumerate()
                .map(|(i, &len)| (0..len).map(|b| (b + i) as u8).collect())
                .collect();
            let batch = Batch {
                transactions: transactions.clone(),
                epoch: 7,
                beneficiary: Address::repeat_byte(3),
                base_fee_per_gas: 42,
                worker_id: 1,
                received_at: None,
            };
            let oracle = ElementwiseBatch {
                transactions,
                epoch: 7,
                beneficiary: Address::repeat_byte(3),
                base_fee_per_gas: 42,
                worker_id: 1,
            };
            let bytes = encode(&batch);
            assert_eq!(bytes, encode(&oracle), "{shape:?}: bytes must not change");
            let decoded: Batch = decode(&encode(&oracle));
            assert_eq!(decoded, batch, "{shape:?}: the old bytes decode to the same batch");
            assert_eq!(decoded.digest(), batch.digest(), "{shape:?}: digest unchanged");
            let back: ElementwiseBatch = decode(&bytes);
            assert_eq!(encode(&back), bytes, "{shape:?}: the old decoder reads the new bytes");
        }
    }

    /// The count cap can never reject a legitimate batch in any epoch: a valid transaction is
    /// non-empty (>= 1 byte) and a batch's transaction bytes are capped at that epoch's
    /// `max_batch_size`, so a valid batch holds at most that many transactions.
    #[test]
    fn max_txs_per_batch_bounds_every_epoch() {
        // Add each batch-size fork boundary and its adjacent epochs when the schedule changes.
        for epoch in [0, 1, crate::Epoch::MAX] {
            assert!(
                MAX_TXS_PER_BATCH >= max_batch_size(epoch),
                "a valid epoch-{epoch} batch could exceed the decode count cap"
            );
        }
    }

    /// A zero-byte (empty) transaction is invalid and must fail to decode (encode does not
    /// validate, so the malicious payload can still be produced).
    #[test]
    fn decode_rejects_zero_byte_transaction() {
        let batch = Batch::new_for_test(vec![vec![]], ExecHeader::default(), 0, 0);
        let bytes = encode(&batch);
        let decoded: bcs::Result<Batch> = try_decode(&bytes);
        assert!(decoded.is_err(), "a batch with a zero-byte transaction must fail to decode");
    }

    /// A `Batch` whose `transactions` field declares more than `MAX_TXS_PER_BATCH` entries must be
    /// rejected before allocating. The bytes are the leading `uleb128(MAX_TXS_PER_BATCH + 1)`
    /// length prefix of a BCS `Batch` (transactions is the first field); decode fails on that
    /// field.
    #[test]
    fn decode_rejects_absurd_transaction_count() {
        // MAX_TXS_PER_BATCH == 1_000_000, so declare 1_000_001 == uleb128 [0xC1, 0x84, 0x3D].
        assert_eq!(
            MAX_TXS_PER_BATCH, 1_000_000,
            "update the crafted length prefix if this changes"
        );
        let bytes = [0xC1_u8, 0x84, 0x3D];
        let decoded: bcs::Result<Batch> = try_decode(&bytes);
        assert!(
            decoded.is_err(),
            "a batch declaring > MAX_TXS_PER_BATCH transactions must be rejected"
        );
    }

    /// A legitimate batch round-trips unchanged and its digest is stable (the guard is
    /// decode-only).
    #[test]
    fn valid_batch_round_trips_with_stable_digest() {
        let batch =
            Batch::new_for_test(vec![vec![1, 2, 3], vec![4, 5]], ExecHeader::default(), 0, 0);
        let bytes = encode(&batch);
        let decoded: Batch = try_decode(&bytes).expect("a valid batch decodes");
        assert_eq!(decoded, batch);
        assert_eq!(decoded.digest(), batch.digest());
    }
}

/// Defines the validation procedure for receiving either a new single transaction (from a client)
/// of a batch of transactions (from another validator).
///
/// Invalid transactions will not receive further processing.
pub trait BatchValidation: Send + Sync + Debug {
    /// Determines if this batch can be voted on
    fn validate_batch(&self, b: SealedBatch) -> Result<(), BatchValidationError>;

    /// Submit a transaction (as bytes) for inclusion in a batch.
    /// Will only submit if the txn hash fits the provided committee slot.
    fn submit_txn_if_mine(&self, tx_bytes: &[u8], committee_size: u64, committee_slot: u64);
}

/// Forwards accepted transactions to committee validators over their advertised JSON-RPC
/// endpoints.
///
/// A non-committee ("observer") worker cannot include the transactions it accepts in a batch
/// itself, so it forwards them to the committee. Instead of pushing over the libp2p worker
/// protocol, the observer forwards each transaction to the JSON-RPC endpoint the owning
/// validator advertised on its worker record (issue #804), so the submitter gets the same RPC
/// experience they would get talking to a validator directly.
///
/// Implementations are best-effort and must not block: forwarding runs on a background task so
/// batch production is never stalled by a slow or unreachable validator.
pub trait TxnForwarder: Send + Sync + Debug {
    /// Forward `transactions` to the validators that own them.
    ///
    /// `committee_slots` holds the committee's BLS public keys in slot order (index = committee
    /// slot); a transaction is routed to the validator whose slot owns the sender, matching
    /// [`BatchValidation::submit_txn_if_mine`] so all transactions from one account converge on a
    /// single validator and nonce ordering is preserved. `validator_rpcs` is the set of
    /// currently-known advertised endpoints; a validator that has not advertised an endpoint is
    /// skipped in favor of one that has.
    ///
    /// Returns `true` if the batch was admitted to a forward task. `false` means it was dropped
    /// at the door and the caller still owns these transactions: they must stay in the caller's
    /// pool for a future batch. Admission is not delivery — delivery stays best-effort on the
    /// background task.
    fn forward_txns(
        &self,
        transactions: Vec<Vec<u8>>,
        committee_slots: Vec<BlsPublicKey>,
        validator_rpcs: Vec<(BlsPublicKey, RpcInfo)>,
    ) -> bool;
}

/// A [`TxnForwarder`] that admits nothing.
///
/// Committee voting validators never forward (they include transactions directly), so they can
/// be constructed with this; it is also convenient in tests that do not exercise forwarding.
/// Refusing admission keeps the honest contract: a caller that relies on forwarding sees the
/// batch refused and keeps its transactions, instead of believing they were handed off.
#[derive(Clone, Debug, Default)]
pub struct NoopTxnForwarder;

impl TxnForwarder for NoopTxnForwarder {
    fn forward_txns(
        &self,
        _transactions: Vec<Vec<u8>>,
        _committee_slots: Vec<BlsPublicKey>,
        _validator_rpcs: Vec<(BlsPublicKey, RpcInfo)>,
    ) -> bool {
        false
    }
}

/// Block validation error types
#[derive(Error, Debug)]
pub enum BatchValidationError {
    /// The sealed batch hash does not match this worker's calculated digest.
    #[error("Invalid digest for sealed batch.")]
    InvalidDigest,
    /// Canonical chain header cannot be found.
    #[error("Canonical chain header {block_hash} can't be found for peer batch's parent")]
    CanonicalChain {
        /// The executed block hash of the missing canonical chain header.
        block_hash: BlockHash,
    },
    /// Empty batch.
    #[error("Batch contains no transactions")]
    EmptyBatch,
    /// Error when the max gas included in the header exceeds the batch's gas limit.
    #[error("Peer's batch total possible gas ({total_possible_gas}) is greater than batch's gas limit ({gas_limit})")]
    HeaderMaxGasExceedsGasLimit {
        /// The total possible gas used in the batch header measured by included transactions max
        /// gas.
        total_possible_gas: u64,
        /// The gas limit in the batch header.
        gas_limit: u64,
    },
    /// Error while calculating max possible gas from icluded transactions.
    #[error("Unable to reduce max possible gas limit for peer's batch")]
    CalculateMaxPossibleGas,
    /// Error when peer's transaction list exceeds the maximum bytes allowed.
    #[error("Peer's transactions exceed max byte size: {0}")]
    HeaderTransactionBytesExceedsMax(usize),
    /// Error trying to decode a transaction in a peer's batch.
    /// If any transaction fails to decode, the entire batch validation fails.
    #[error("Failed to decode transaction for batch {0}: {1}")]
    RecoverTransaction(BlockHash, String),
    /// Error, invalid base fee set.
    #[error("Invalid base fee, expected {expected_base_fee} got {base_fee}")]
    InvalidBaseFee { expected_base_fee: u64, base_fee: u64 },
    /// Error, wrong worker id.
    #[error("Invalid worker id, expected {expected_worker_id} got {worker_id}")]
    InvalidWorkerId { expected_worker_id: WorkerId, worker_id: WorkerId },
    /// The batch contains blob transactions EIP-4844.
    #[error("Proposed batch contains blob transaction. Tx hash: {0}")]
    InvalidTx4844(BlockHash),
    /// The batch contains a transaction whose EIP-2718 type byte is outside the
    /// executable allowlist (legacy, EIP-2930, EIP-1559).
    #[error("Proposed batch contains unsupported transaction type {tx_type}. Tx hash: {hash}")]
    UnsupportedTxType {
        /// The EIP-2718 type byte of the offending transaction.
        tx_type: u8,
        /// Hash of the offending transaction.
        hash: BlockHash,
    },
    /// The total allowable gas in the batch exceeds `u64::MAX`.
    #[error("Overflow calculating max possible gas.")]
    GasOverflow,
    /// Error, wrong epoch.
    #[error("Invalid epoch, expected epoch {expected} got epoch {found}")]
    InvalidEpoch { expected: Epoch, found: Epoch },
}

/// On-demand timing of a realistic batch's codec work: encode, digest (a hash of the encoding) and
/// decode of 1,000 transactions of 200 bytes.
/// `cargo test --release -p tn-types batch_codec_bench -- --ignored --nocapture`
#[cfg(test)]
mod codec_bench {
    use super::Batch;
    use crate::{decode, encode, Address};
    use std::{hint::black_box, time::Instant};

    #[test]
    #[ignore = "on-demand batch codec benchmark; run with --release --ignored --nocapture"]
    fn batch_codec_bench() {
        let transactions: Vec<Vec<u8>> =
            (0..1_000).map(|i| (0..200).map(|b| (b + i) as u8).collect()).collect();
        let batch = Batch {
            transactions,
            epoch: 7,
            beneficiary: Address::repeat_byte(3),
            base_fee_per_gas: 42,
            worker_id: 1,
            received_at: None,
        };
        let bytes = encode(&batch);
        let rounds = 2_000_u32;
        let time = |label: &str, op: &mut dyn FnMut()| {
            let start = Instant::now();
            for _ in 0..rounds {
                op();
            }
            println!(
                "{label:<8} {:>9.1} us/batch",
                start.elapsed().as_secs_f64() * 1e6 / rounds as f64
            );
        };
        println!("batch: 1000 txs x 200 B = {} encoded bytes", bytes.len());
        time("encode", &mut || {
            black_box(encode(black_box(&batch)));
        });
        time("digest", &mut || {
            black_box(black_box(&batch).digest());
        });
        time("decode", &mut || {
            black_box(decode::<Batch>(black_box(&bytes)));
        });
    }
}
