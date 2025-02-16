//! Compatibility with reth's API for engine types.

use reth_chainspec::ChainSpec;
pub use reth_consensus::{Consensus, ConsensusError};
use reth_consensus::{FullConsensus, HeaderValidator, PostExecutionInput};
use reth_engine_primitives::PayloadValidator;
use reth_revm::primitives::{
    BlobExcessGasAndPrice, BlockEnv, CfgEnv, CfgEnvWithHandlerCfg, SpecId,
};
use serde::{Deserialize, Serialize};
use tn_types::{
    Address, BlockExt as _, BlockWithSenders, ConsensusOutput, NodePrimitives, SealedBlock,
    SealedHeader, Withdrawals, B256, U256,
};

/// Compatibility type to easily integrate with reth.
///
/// This type is used to noop verify all data. It is not used by Telcoin Network, but is required to
/// integrate with reth for convenience. TN is mostly EVM/Ethereum types, but with a different
/// consensus. The traits impl on this type are only used beacon engine, which is not used by TN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TNExecution;

impl<H> HeaderValidator<H> for TNExecution {
    fn validate_header(&self, _header: &SealedHeader<H>) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_against_parent(
        &self,
        _header: &SealedHeader<H>,
        _parent: &SealedHeader<H>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_with_total_difficulty(
        &self,
        _header: &H,
        _total_difficulty: U256,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }
}

impl<H, B> Consensus<H, B> for TNExecution {
    fn validate_body_against_header(
        &self,
        _body: &B,
        _header: &SealedHeader<H>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_block_pre_execution(
        &self,
        _block: &SealedBlock<H, B>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }
}

impl<N: NodePrimitives> FullConsensus<N> for TNExecution {
    fn validate_block_post_execution(
        &self,
        _block: &BlockWithSenders<N::Block>,
        _input: PostExecutionInput<'_, N::Receipt>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }
}

// Compatibility noop trait impl.
// This is for the reth rpc build method.
// NOTE: this should never be called because there is no beacon API
impl PayloadValidator for TNExecution {
    type Block = tn_types::Block;

    fn ensure_well_formed_payload(
        &self,
        payload: alloy::rpc::types::engine::ExecutionPayload,
        sidecar: alloy::rpc::types::engine::ExecutionPayloadSidecar,
    ) -> Result<reth_primitives::SealedBlockFor<Self::Block>, alloy::rpc::types::engine::PayloadError>
    {
        Ok(payload.try_into_block_with_sidecar(&sidecar)?.seal_slow())
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
    pub parent_header: SealedHeader,
}

impl<P> BuildArguments<P> {
    /// Initialize new instance of [Self].
    pub fn new(provider: P, output: ConsensusOutput, parent_header: SealedHeader) -> Self {
        Self { provider, output, parent_header }
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

    pub fn cfg_and_block_env(&self, chain_spec: &ChainSpec) -> (CfgEnvWithHandlerCfg, BlockEnv) {
        // configure evm env based on parent block
        let cfg = CfgEnv::default().with_chain_id(chain_spec.chain().id());

        // ensure we're not missing any timestamp based hardforks
        let spec_id = SpecId::SHANGHAI;

        // use the blob excess gas and price set by the worker during batch creation
        let blob_excess_gas_and_price = Some(BlobExcessGasAndPrice::new(0, false));

        // use the basefee set by the worker during batch creation
        let basefee = U256::from(self.attributes.base_fee_per_gas);

        // ensure gas_limit enforced during block validation
        let gas_limit = U256::from(self.attributes.gas_limit);

        // create block environment to re-execute worker's block
        let block_env = BlockEnv {
            // the block's number should come from the canonical tip, NOT the batch's number
            number: U256::from(self.attributes.parent_header.number + 1),
            coinbase: self.suggested_fee_recipient(),
            timestamp: U256::from(self.timestamp()),
            // leave difficulty zero
            // this value is useful for post-execution, but worker's block is created with this
            // value
            difficulty: U256::ZERO,
            prevrandao: Some(self.prev_randao()),
            gas_limit,
            basefee,
            // calculate excess gas based on parent block's blob gas usage
            blob_excess_gas_and_price,
        };

        (CfgEnvWithHandlerCfg::new_with_spec_id(cfg, spec_id), block_env)
    }

    pub fn timestamp(&self) -> u64 {
        self.attributes.timestamp
    }

    pub fn suggested_fee_recipient(&self) -> Address {
        self.attributes.beneficiary
    }

    /// PrevRandao is used by TN to provide a source for randomness on-chain.
    ///
    /// This is used as the executed block's "mix_hash".
    /// [EIP-4399]: https://eips.ethereum.org/EIPS/eip-4399
    pub fn prev_randao(&self) -> B256 {
        self.attributes.mix_hash
    }

    pub fn parent(&self) -> B256 {
        self.attributes.parent_header.hash()
    }

    pub fn parent_beacon_block_root(&self) -> Option<B256> {
        Some(self.attributes.consensus_output_digest)
    }

    /// Taken from worker's block, but currently always empty.
    pub fn withdrawals(&self) -> &Withdrawals {
        &self.attributes.withdrawals
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
    pub batch_index: u64,
    /// Value for the `timestamp` field of the new payload
    pub timestamp: u64,
    /// Value for the `extra_data` field in the new block.
    pub batch_digest: B256,
    /// Hash value for [ConsensusOutput]. Used as the executed block's "parent_beacon_block_root".
    pub consensus_output_digest: B256,
    /// The base fee per gas used to construct this block.
    /// The value comes from the proposed batch.
    pub base_fee_per_gas: u64,
    /// The gas limit for the constructed block.
    ///
    /// The value comes from the worker's block.
    pub gas_limit: u64,
    /// The mix hash used for prev_randao.
    pub mix_hash: B256,
    /// TODO: support withdrawals
    ///
    /// This is currently always empty vec. This comes from the batch block's withdrawals.
    pub withdrawals: Withdrawals,
}

impl TNPayloadAttributes {
    /// Create a new instance of [Self].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_header: SealedHeader,
        batch_index: u64,
        batch_digest: B256,
        output: &ConsensusOutput,
        consensus_output_digest: B256,
        base_fee_per_gas: u64,
        gas_limit: u64,
        mix_hash: B256,
        withdrawals: Withdrawals,
    ) -> Self {
        Self {
            parent_header,
            beneficiary: output.beneficiary(),
            nonce: output.nonce(),
            batch_index,
            timestamp: output.committed_at(),
            batch_digest,
            consensus_output_digest,
            base_fee_per_gas,
            gas_limit,
            mix_hash,
            withdrawals,
        }
    }
}
