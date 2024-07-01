//! Recreated `AutoSealConsensus` to reduce the amount of imports from reth.

use crate::ConsensusOutput;

use super::{Consensus, ConsensusError};
use reth_chainspec::ChainSpec;
use reth_consensus::PostExecutionInput;
use reth_payload_primitives::PayloadBuilderAttributes;
use reth_primitives::{
    constants::EIP1559_INITIAL_BASE_FEE, revm::config::revm_spec_by_timestamp_after_merge, Address,
    BlockWithSenders, Header, SealedBlock, SealedBlockWithSenders, SealedHeader, Withdrawals, B256,
    U256,
};
use reth_revm::primitives::{BlobExcessGasAndPrice, BlockEnv, CfgEnv, CfgEnvWithHandlerCfg};
use reth_rpc_types::{engine::PayloadId, BlockNumHash};
use std::{convert::Infallible, sync::Arc};
use tracing::warn;

/// A consensus implementation that validates everything.
///
/// Taken from reth's `AutoSealConsensus`.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AutoSealConsensus {
    /// Configuration
    chain_spec: Arc<ChainSpec>,
}

impl AutoSealConsensus {
    /// Create a new instance of [AutoSealConsensus]
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }

    /// Clone copy of `ChainSpec`.
    pub fn chain_spec(&self) -> &Arc<ChainSpec> {
        &self.chain_spec
    }
}

impl Consensus for AutoSealConsensus {
    fn validate_header(&self, _header: &SealedHeader) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_against_parent(
        &self,
        _header: &SealedHeader,
        _parent: &SealedHeader,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_with_total_difficulty(
        &self,
        _header: &Header,
        _total_difficulty: U256,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_block_pre_execution(&self, _block: &SealedBlock) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_block_post_execution(
        &self,
        _block: &BlockWithSenders,
        _input: PostExecutionInput<'_>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }
}

/// The type for building blocks that extend the canonical tip.
#[derive(Debug)]
pub struct BuildArguments<Provider> {
    /// State provider.
    pub provider: Provider,
    /// Output from consensus that contains all the transactions to execute.
    pub output: ConsensusOutput,
    /// Last executed block from the previous consensus output.
    pub parent_block: BlockNumHash,
    /// The chain spec.
    pub chain_spec: Arc<ChainSpec>,
}

impl<P> BuildArguments<P> {
    /// Initialize new instance of [Self].
    pub fn new(
        provider: P,
        output: ConsensusOutput,
        parent_block: BlockNumHash,
        chain_spec: Arc<ChainSpec>,
    ) -> Self {
        Self { provider, output, parent_block, chain_spec }
    }
}

/// The type used to build the next canonical block.
#[derive(Debug)]
pub struct TNPayload {
    /// Attributes to use when building the payload.
    ///
    /// Stored here for simplicity to maintain compatibility with reth api and implementing
    /// `PayloadBuilderAttributes` on Self.
    pub attributes: TNPayloadAttributes,
}

impl TNPayload {
    /// Create a new instance of [Self].
    pub fn new(attributes: TNPayloadAttributes) -> Self {
        Self { attributes }
    }
}

/// The type used to construct a [TNPayload].
///
/// It contains all the attributes required to initiate a payload build process. The actual struct
/// itself is a mediary for maintaining compatibility with reth's api. Otherwise, doesn't provide
/// much utility.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TNPayloadAttributes {
    /// The previous canonical block's number and hash.
    pub parent_block: BlockNumHash,
    /// Ommers on TN are all the hashes of batches.
    pub ommers: Vec<Header>,
    /// Hash of all ommers in this output from consensus.
    pub ommers_root: B256,
    /// The beneficiary from the round of consensus.
    pub beneficiary: Address,
    /// The index of the subdag, which equates to the round of consensus.
    ///
    /// Used as the executed block header's `nonce`.
    pub nonce: u64,
    /// The index of the block within the entire output from consensus.
    ///
    /// Used as executed block header's `difficulty`.
    pub batch_index: u64,
    /// Value for the `timestamp` field of the new payload
    pub timestamp: u64,
    /// TODO: support withdrawals
    ///
    /// This is currently always empty vec.
    pub withdrawals: Withdrawals,
    /// Value for the `extra_data` field in the new block.
    pub batch_digest: B256,
    /// Hash value for [ConsensusOutput]. Used as the executed block's "parent_beacon_block_root".
    ///
    /// TODO: ensure optimized hashing of output:
    /// - don't rehash batches, just hash their hashes?
    pub consensus_output_digest: B256,
    /// The block from this batch that's used to build this payload from.
    ///
    /// Ensures transaction senders are recovered for execution.
    ///
    /// TODO: this is where Withdrawals should come from, but currently always empty.
    pub batch_block: SealedBlockWithSenders,
    // TODO:
    // - indicate first batch in new output to process rewards?
    // - or is it better to have a special "rewards" block at each epoch?
}

impl TNPayloadAttributes {
    /// Create a new instance of [Self].
    pub fn new(
        parent_block: BlockNumHash,
        ommers: Vec<Header>,
        ommers_root: B256,
        batch_index: u64,
        batch_digest: B256,
        output: &ConsensusOutput,
        consensus_output_digest: B256,
        batch_block: SealedBlockWithSenders,
    ) -> Self {
        // TODO: support withdrawals
        let withdrawals = batch_block.withdrawals.clone().unwrap_or_default();

        Self {
            parent_block,
            ommers,
            ommers_root,
            beneficiary: output.beneficiary(),
            nonce: output.nonce(),
            batch_index,
            timestamp: output.committed_at(),
            withdrawals,
            batch_digest,
            consensus_output_digest,
            batch_block,
        }
    }
}

/// Implement [PayloadBuilderAttributes] for extending the canonical tip after consensus is reached.
impl PayloadBuilderAttributes for TNPayload {
    type RpcPayloadAttributes = TNPayloadAttributes;

    // try_new cannot fail because Self::new() cannot fail
    type Error = Infallible;

    /// This is bypassed in the current implementation.
    fn try_new(_parent: B256, attributes: Self::RpcPayloadAttributes) -> Result<Self, Self::Error> {
        Ok(Self::new(attributes))
    }

    fn payload_id(&self) -> PayloadId {
        // construct the payload id from the block's index
        // guaranteed to always be unique within each output
        PayloadId::new(self.attributes.batch_index.to_le_bytes())
    }

    fn parent(&self) -> B256 {
        self.attributes.parent_block.hash
    }

    fn timestamp(&self) -> u64 {
        self.attributes.timestamp
    }

    fn parent_beacon_block_root(&self) -> Option<B256> {
        Some(self.attributes.consensus_output_digest)
    }

    fn suggested_fee_recipient(&self) -> Address {
        self.attributes.beneficiary
    }

    /// PrevRandao is used by TN to provide a source for randomness on-chain.
    ///
    /// This is used as the executed block's "mix_hash".
    /// [EIP-4399]: https://eips.ethereum.org/EIPS/eip-4399
    fn prev_randao(&self) -> B256 {
        self.attributes.batch_block.mix_hash
    }

    /// Taken from batch, but currently always empty.
    fn withdrawals(&self) -> &Withdrawals {
        &self.attributes.withdrawals
    }

    fn cfg_and_block_env(
        &self,
        chain_spec: &ChainSpec,
        batch: &Header,
    ) -> (CfgEnvWithHandlerCfg, BlockEnv) {
        // configure evm env based on parent block
        let cfg = CfgEnv::default().with_chain_id(chain_spec.chain().id());

        // ensure we're not missing any timestamp based hardforks
        let spec_id = revm_spec_by_timestamp_after_merge(chain_spec, self.timestamp());

        // use the blob excess gas and price set by the worker during batch creation
        let blob_excess_gas_and_price = Some(BlobExcessGasAndPrice::new(0));

        // use the basefee set by the worker during batch creation
        let worker_basefee = match batch.base_fee_per_gas {
            Some(fee) => fee,
            None => {
                warn!(target: "executor::payload", "missing worker's basefee - using default");
                EIP1559_INITIAL_BASE_FEE
            }
        };

        // cast to U256
        let basefee = U256::from(worker_basefee);

        // ensure gas_limit enforced during block validation
        let gas_limit = U256::from(batch.gas_limit);

        // create block environment to re-execute batch
        let block_env = BlockEnv {
            // the block's number should come from the canonical tip, NOT the batch block's number
            number: U256::from(self.attributes.parent_block.number + 1),
            coinbase: self.suggested_fee_recipient(),
            timestamp: U256::from(self.timestamp()),
            // leave difficulty zero
            // this value is useful for post-execution, but batch is created with this value
            difficulty: U256::ZERO,
            prevrandao: Some(self.prev_randao()),
            gas_limit,
            basefee,
            // calculate excess gas based on parent block's blob gas usage
            blob_excess_gas_and_price,
        };

        (CfgEnvWithHandlerCfg::new_with_spec_id(cfg, spec_id), block_env)
    }
}
