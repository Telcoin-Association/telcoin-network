//! The payload that contains all data from consensus to be executed.

use crate::RethEnv;
use reth_rpc_eth_api::helpers::pending_block::BuildPendingEnv;
use serde::{Deserialize, Serialize};
use tn_types::{
    forks::subsecond_timestamp_active, Address, ConsensusOutput, ExecHeader, SealedHeader,
    TimestampSec, WorkerId, B256, MIN_PROTOCOL_BASE_FEE,
};

/// The type for building blocks that extend the canonical tip.
#[derive(Debug)]
pub struct BuildArguments {
    /// State provider.
    pub reth_env: RethEnv,
    /// Output from consensus that contains all the transactions to execute.
    pub output: ConsensusOutput,
    /// Last executed block from the previous consensus output.
    pub parent_header: SealedHeader,
}

impl BuildArguments {
    /// Initialize new instance of [Self].
    pub fn new(reth_env: RethEnv, output: ConsensusOutput, parent_header: SealedHeader) -> Self {
        Self { reth_env, output, parent_header }
    }
}

/// The type used to build the next canonical block.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TNPayload {
    /// The previous canonical block's number and hash.
    pub parent_header: SealedHeader,
    /// The authority responsible for producing the batch.
    /// This is used for block's coinbase where priority fees are sent.
    pub beneficiary: Address,
    /// Used as the executed block header's `nonce`.
    ///
    /// The result of the leader's epoch and round:
    /// `((self.epoch as u64) << 32) | self.round as u64`
    ///
    /// See ConsensusOutput::nonce()
    pub nonce: u64,
    /// The index of the block within the entire output from consensus.
    ///
    /// Used as executed block header's `difficulty`.
    pub batch_index: usize,
    /// Value for the `timestamp` field of the new payload, in whole seconds.
    ///
    /// Consensus times commits in milliseconds from the sub-second timestamp fork on, but the EVM
    /// `timestamp` stays in seconds: it is [`ConsensusOutput::committed_at`], the floor of the
    /// commit time, because contracts and Ethereum tooling read `block.timestamp` as Unix
    /// seconds. Under sub-second commit cadence several consecutive blocks therefore share one
    /// `timestamp`.
    ///
    /// For leaders of epochs where [`subsecond_timestamp_active`] holds, the value is raised to
    /// at least the parent block's `timestamp`, so EVM time never runs backwards even if
    /// consensus hands execution a regressed commit time. The clamp is gated on the leader's
    /// epoch carried in the output, never on a node-local epoch, so every node executing the
    /// same output computes the same value and replaying pre-fork history reproduces the
    /// original timestamps. Consensus already keeps commit times non-decreasing from the fork
    /// on, so a clamp that raises the value signals a consensus bug; the engine counts those
    /// blocks in `evm_timestamp_clamped_total`.
    pub timestamp: u64,
    /// This is used as the ommers hash.
    /// The default is `B256::ZERO` (no batches to execute).
    pub batch_digest: B256,
    /// Hash value for the `ConsensusHeader`. Used as the executed block's
    /// "parent_beacon_block_root".
    pub consensus_header_digest: B256,
    /// The base fee per gas used to construct this block.
    /// The value comes from the proposed batch.
    pub base_fee_per_gas: u64,
    /// The gas limit for the constructed block.
    ///
    /// The value comes from the worker's block.
    pub gas_limit: u64,
    /// The mix hash used for prev_randao.
    ///
    /// Fork-gated by `prevrandao_seed_active` for the committing leader's epoch: the legacy
    /// `output_digest ^ batch_digest` before the PREVRANDAO fork, the domain-separated
    /// keccak over the epoch seed chain value, consensus block number, and batch index from
    /// it (`ConsensusOutput::prev_randao`, #1247). Post-fork the value is immune to payload
    /// grinding, but the committing leader still sees every value its commit will produce
    /// before broadcasting and keeps one propose-or-withhold choice per commit; contracts
    /// that need unbiasable randomness must not use `PREVRANDAO` alone.
    pub mix_hash: B256,
    /// Randomness digest carried only by the payload that closes the epoch.
    ///
    /// `Some` for the last batch of an epoch-closing `ConsensusOutput`; `None` otherwise. The
    /// value is whatever `ConsensusOutput::committee_shuffle_seed` yields for the closing epoch,
    /// which is epoch-gated: the epoch seed chain value as of the closing commit once
    /// `seed_signature_active` holds, and the legacy keccak256 of the leader certificate's
    /// aggregate BLS signature for every epoch before the fork. During execution it seeds the
    /// deterministic shuffle that selects the next committee, and it is recorded in the executed
    /// block header's `extra_data` so replay recovers the same seed.
    pub close_epoch: Option<B256>,
    /// Worker that created this payload.
    pub worker_id: WorkerId,
    /// Test-only slash injection for the epoch boundary.
    ///
    /// Flows through `TNBlockExecutionCtx` into the executor's `epoch_boundary_slashes` seam so a
    /// test can drive a non-empty slash list through the production close path. Production builds
    /// have no such field: slashing is not live and the executor's production body always returns
    /// an empty list. `serde(skip)` keeps the serialized payload byte-identical to production.
    #[cfg(test)]
    #[serde(skip)]
    pub epoch_boundary_slashes: Vec<crate::system_calls::ConsensusRegistry::Slash>,
}

impl TNPayload {
    /// Create a new instance of [Self].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_header: SealedHeader,
        beneficiary: Address,
        batch_index: usize,
        batch_digest: B256,
        output: &ConsensusOutput,
        consensus_header_digest: B256,
        base_fee_per_gas: u64,
        gas_limit: u64,
        mix_hash: B256,
        worker_id: WorkerId,
    ) -> Self {
        // include the committee-shuffle seed (the epoch seed chain value as of the closing
        // commit) if this is the last payload for the epoch
        let close_epoch = output
            .close_epoch_for_last_batch(batch_index)
            .is_some_and(|last_batch| last_batch)
            .then(|| output.committee_shuffle_seed());
        let timestamp = evm_block_timestamp(output, &parent_header);

        Self {
            parent_header,
            beneficiary,
            nonce: output.nonce(),
            batch_index,
            timestamp,
            batch_digest,
            consensus_header_digest,
            base_fee_per_gas,
            gas_limit,
            mix_hash,
            close_epoch,
            worker_id,
            #[cfg(test)]
            epoch_boundary_slashes: Vec::new(),
        }
    }

    /// The block's `PREVRANDAO` ([EIP-4399]), stored as the executed block's `mix_hash`.
    ///
    /// Derived by [`ConsensusOutput::prev_randao`]. Contract-visible, and grinding-resistant
    /// from the PREVRANDAO fork epoch onward, but NOT unbiasable: the committing leader can
    /// compute the value before broadcasting and withhold the commit. Contracts needing
    /// unbiasable randomness must not use this value alone.
    ///
    /// [EIP-4399]: https://eips.ethereum.org/EIPS/eip-4399
    pub(crate) fn prev_randao(&self) -> B256 {
        self.mix_hash
    }

    /// The TN parent "beacon" block root.
    pub(crate) fn parent_beacon_block_root(&self) -> Option<B256> {
        Some(self.consensus_header_digest)
    }

    /// Method to create an instance of Self useful for tests.
    ///
    /// WARNING: only use this for tests. Data is invalid.
    #[cfg(any(feature = "test-utils", test))]
    pub fn new_for_test(parent_header: SealedHeader, output: &ConsensusOutput) -> Self {
        use tn_types::{Hash as _, MIN_PROTOCOL_BASE_FEE};

        let beneficiary = Address::random();
        let batch_index = 0;
        let batch_digest = B256::random();
        let consensus_header_digest = output.digest().into();
        let base_fee_per_gas = parent_header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE);
        let gas_limit = parent_header.gas_limit;
        let mix_hash = B256::random();

        Self::new(
            parent_header,
            beneficiary,
            batch_index,
            batch_digest,
            output,
            consensus_header_digest,
            base_fee_per_gas,
            gas_limit,
            mix_hash,
            0,
        )
    }

    /// Inject slashes to submit at the epoch boundary this payload closes.
    ///
    /// Only meaningful on an epoch-closing payload: the executor reads the list in its
    /// `epoch_boundary_slashes` seam and sequences it through `applySlashes` between
    /// `applyIncentives` and `concludeEpoch`.
    #[cfg(test)]
    pub(crate) fn with_epoch_boundary_slashes(
        mut self,
        slashes: Vec<crate::system_calls::ConsensusRegistry::Slash>,
    ) -> Self {
        self.epoch_boundary_slashes = slashes;
        self
    }
}

/// Build the env a `pending`-tag query would simulate on top of the canonical tip.
///
/// No worker ever produces the block this env describes, so its values are placeholder
/// approximations: `beneficiary` is the zero address (COINBASE-reading contracts see
/// address zero), `worker_id` is 0, `timestamp` is parent + 1, and the base fee is the
/// parent's, which goes stale across an epoch boundary (TN fees are epoch-flat). TN
/// therefore defaults `--rpc.pending-block` to `none`, so this env is only simulated
/// for an operator who opts in with `--rpc.pending-block full`.
///
/// The parent + 1 `timestamp` can overstate the real next block's. The EVM `timestamp` keeps
/// whole seconds, so under sub-second commit cadence several consecutive blocks share one
/// `timestamp` and the next block often lands in the parent's second. From the sub-second
/// timestamp fork on, execution never lets a block's `timestamp` fall below its parent's, so
/// the overstatement is at most 1 s. That is acceptable for gas estimation and `eth_call`
/// against `pending`, which only need a plausible next-block env.
impl BuildPendingEnv<ExecHeader> for TNPayload {
    fn build_pending_env(parent: &SealedHeader<ExecHeader>) -> Self {
        Self {
            parent_header: parent.clone(),
            beneficiary: Address::ZERO,
            nonce: 0,
            batch_index: 0,
            timestamp: parent.timestamp + 1,
            batch_digest: B256::ZERO,
            consensus_header_digest: B256::ZERO,
            base_fee_per_gas: parent.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE),
            gas_limit: parent.gas_limit,
            mix_hash: B256::ZERO,
            close_epoch: None,
            worker_id: 0,
            #[cfg(test)]
            epoch_boundary_slashes: Vec::new(),
        }
    }
}

/// The EVM block `timestamp` for a payload that executes `output` on top of `parent`.
///
/// Exactly [`ConsensusOutput::committed_at`] for leaders of epochs before the sub-second
/// timestamp fork; from the fork on, raised to at least `parent.timestamp`. See
/// [`TNPayload::timestamp`] for why the clamp exists and why it is gated on the leader's epoch.
fn evm_block_timestamp(output: &ConsensusOutput, parent: &SealedHeader) -> TimestampSec {
    let committed_at = output.committed_at();
    if subsecond_timestamp_active(output.leader().epoch()) {
        committed_at.max(parent.timestamp)
    } else {
        committed_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::consensus_output_for_tests;
    use tn_types::{
        forks::{seed_signature_fork_epoch_override, subsecond_timestamp_fork_epoch_override},
        Epoch,
    };

    /// Leader epoch of every output built here; any epoch below `Epoch::MAX` works.
    const LEADER_EPOCH: Epoch = 3;

    /// Pins this test process's sub-second timestamp fork active (or dormant) from genesis, with
    /// the seed-signature fork it requires active from genesis.
    ///
    /// The gates read their `test-utils` environment overrides once per process, so this must run
    /// before anything consults a gate, including building the consensus output. nextest runs each
    /// test in its own process, which keeps one test's pin from reaching another; a single-process
    /// `cargo test` run shares one latch across the whole test binary instead. Reading the
    /// overrides back turns a value that latched before the pin into a named failure.
    fn pin_subsecond_fork(active: bool) {
        let subsecond_fork: Epoch = if active { 0 } else { Epoch::MAX };
        std::env::set_var("TN_SEED_SIGNATURE_FORK_EPOCH", "0");
        std::env::set_var("TN_SUBSECOND_TIMESTAMP_FORK_EPOCH", subsecond_fork.to_string());
        assert_eq!(
            seed_signature_fork_epoch_override(),
            Some(0),
            "TN_SEED_SIGNATURE_FORK_EPOCH latched to another value before this test pinned it"
        );
        assert_eq!(
            subsecond_timestamp_fork_epoch_override(),
            Some(subsecond_fork),
            "TN_SUBSECOND_TIMESTAMP_FORK_EPOCH latched to another value before this test pinned it"
        );
    }

    /// The `timestamp` of a payload that executes `output` on a parent block stamped
    /// `parent_timestamp`.
    fn payload_timestamp(output: &ConsensusOutput, parent_timestamp: TimestampSec) -> TimestampSec {
        let parent = SealedHeader::seal_slow(ExecHeader {
            timestamp: parent_timestamp,
            ..Default::default()
        });
        TNPayload::new_for_test(parent, output).timestamp
    }

    #[test]
    fn post_fork_payload_timestamp_never_precedes_parent() {
        pin_subsecond_fork(true);
        let output = consensus_output_for_tests(1, LEADER_EPOCH, 1, false);
        assert!(
            subsecond_timestamp_active(output.leader().epoch()),
            "the pinned fork must be active for the leader's epoch"
        );
        let committed_at = output.committed_at();

        // a commit 3 s behind its parent is raised to the parent's timestamp
        let parent_timestamp = committed_at + 3;
        assert_eq!(payload_timestamp(&output, parent_timestamp), parent_timestamp);

        // a commit at or ahead of its parent passes through unchanged
        assert_eq!(payload_timestamp(&output, committed_at), committed_at);
        assert_eq!(payload_timestamp(&output, committed_at - 2), committed_at);
    }

    #[test]
    fn pre_fork_payload_timestamp_is_the_commit_time() {
        pin_subsecond_fork(false);
        let output = consensus_output_for_tests(1, LEADER_EPOCH, 1, false);
        assert!(
            !subsecond_timestamp_active(output.leader().epoch()),
            "the pinned fork must be dormant for the leader's epoch"
        );
        let committed_at = output.committed_at();

        // pre-fork blocks keep the raw commit time even when it precedes the parent's, so
        // replaying pre-fork history reproduces the original timestamps
        assert_eq!(payload_timestamp(&output, committed_at + 3), committed_at);
        assert_eq!(payload_timestamp(&output, committed_at - 2), committed_at);
    }
}
