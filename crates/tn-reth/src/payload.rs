//! The payload that contains all data from consensus to be executed.

use crate::RethEnv;
use serde::{Deserialize, Serialize};
use tn_types::{Address, ConsensusOutput, SealedHeader, WorkerId, B256};

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
    /// The beneficiary from the round of consensus.
    pub beneficiary: Address,
    /// The index of the subdag, which equates to the round of consensus.
    ///
    /// Used as the executed block header's `nonce`.
    pub nonce: u64,
    /// The index of the block within the entire output from consensus.
    ///
    /// Used as executed block header's `difficulty`.
    pub batch_index: usize,
    /// Value for the `timestamp` field of the new payload
    pub timestamp: u64,
    /// Value for the `extra_data` field in the new block.
    pub batch_digest: Option<B256>,
    /// Hash value for [ConsensusHeader]. Used as the executed block's "parent_beacon_block_root".
    pub consensus_header_digest: B256,
    /// The base fee per gas used to construct this block.
    /// The value comes from the proposed batch.
    pub base_fee_per_gas: u64,
    /// The gas limit for the constructed block.
    ///
    /// The value comes from the worker's block.
    pub gas_limit: u64,
    /// The mix hash used for prev_randao.
    pub mix_hash: B256,
    /// Boolean indicating if the payload should use system calls to close the epoch during
    /// execution.
    ///
    /// This is the last batch for the `ConsensusOutput` if the epoch is closing.
    pub close_epoch: Option<B256>,
    /// Worker that created this payload.
    pub worker_id: WorkerId,
}

impl TNPayload {
    /// Create a new instance of [Self].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_header: SealedHeader,
        batch_index: usize,
        batch_digest: Option<B256>,
        output: &ConsensusOutput,
        consensus_header_digest: B256,
        base_fee_per_gas: u64,
        gas_limit: u64,
        mix_hash: B256,
        worker_id: WorkerId,
    ) -> Self {
        // include leader's aggregate bls signature if this is the last payload for the epoch
        let close_epoch = output
            .close_epoch_for_last_batch()
            .is_some_and(|last_batch| last_batch)
            .then(|| output.keccak_leader_sigs());

        Self {
            parent_header,
            beneficiary: output.beneficiary(),
            nonce: output.nonce(),
            batch_index,
            timestamp: output.committed_at(),
            batch_digest,
            consensus_header_digest,
            base_fee_per_gas,
            gas_limit,
            mix_hash,
            close_epoch,
            worker_id,
        }
    }

    /// PrevRandao is used by TN to provide a source for randomness on-chain.
    ///
    /// This is used as the executed block's "mix_hash".
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
    #[cfg(test)]
    pub fn new_for_test(parent_header: SealedHeader, output: &ConsensusOutput) -> Self {
        use tn_types::{Hash as _, MIN_PROTOCOL_BASE_FEE};

        let batch_index = 0;
        let batch_digest = Some(B256::random());
        let consensus_header_digest = output.digest().into();
        let base_fee_per_gas = parent_header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE);
        let gas_limit = parent_header.gas_limit;
        let mix_hash = B256::random();

        Self::new(
            parent_header,
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
}
