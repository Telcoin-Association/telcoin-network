//! Compatibility types for Telcoin Node and reth.
//!
//! These are used to spawn execution components for the node and maintain compatibility with reth's
//! API.

use crate::RethEnv;
use reth_chainspec::ChainSpec;
pub use reth_consensus::{Consensus, ConsensusError};
use reth_consensus::{FullConsensus, HeaderValidator, PostExecutionInput};
use reth_db::DatabaseEnv;
use reth_engine_primitives::PayloadValidator;
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use reth_evm_ethereum::EthEvmConfig;
use reth_node_builder::{NodeTypes, NodeTypesWithDB, NodeTypesWithEngine};
use reth_node_ethereum::{
    BasicBlockExecutorProvider, EthEngineTypes, EthExecutionStrategyFactory, EthExecutorProvider,
};
use reth_provider::EthStorage;
use reth_trie_db::MerklePatriciaTrie;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tn_types::{
    Address, BlockExt as _, BlockWithSenders, BlsSignature, ConsensusOutput, EthPrimitives,
    ExecHeader, Hash as _, NodePrimitives, SealedBlock, SealedHeader, TransactionSigned,
    Withdrawals, B256, MIN_PROTOCOL_BASE_FEE, U256,
};
use tracing::error;

/// Telcoin Network specific node types for reth compatibility.
pub trait TelcoinNodeTypes: NodeTypesWithEngine + NodeTypesWithDB {
    /// The EVM executor type
    type Executor: BlockExecutorProvider<Primitives = EthPrimitives>;

    /// The EVM configuration type
    type EvmConfig: ConfigureEvm<Transaction = TransactionSigned, Header = ExecHeader>;

    /// Create the Reth evm config.
    fn create_evm_config(chain: Arc<ChainSpec>) -> Self::EvmConfig;
    /// Create the Reth executor.
    fn create_executor(chain: Arc<ChainSpec>) -> Self::Executor;
}

/// Empty struct that implements Reth traits to supply GATs and functionality for Reth integration.
#[derive(Clone, Debug)]
pub struct TelcoinNode {}

impl NodeTypes for TelcoinNode {
    type Primitives = EthPrimitives;
    type ChainSpec = ChainSpec;
    type StateCommitment = MerklePatriciaTrie;
    type Storage = EthStorage;
}

impl NodeTypesWithEngine for TelcoinNode {
    type Engine = EthEngineTypes;
}

impl NodeTypesWithDB for TelcoinNode {
    type DB = Arc<DatabaseEnv>;
}

impl TelcoinNodeTypes for TelcoinNode {
    type Executor = BasicBlockExecutorProvider<EthExecutionStrategyFactory>;
    type EvmConfig = EthEvmConfig;

    fn create_evm_config(chain: Arc<ChainSpec>) -> Self::EvmConfig {
        EthEvmConfig::new(chain)
    }

    fn create_executor(chain: Arc<ChainSpec>) -> Self::Executor {
        EthExecutorProvider::ethereum(chain)
    }
}

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

    /// Passthrough attribute timestamp.
    pub(crate) fn timestamp(&self) -> u64 {
        self.attributes.timestamp
    }

    /// Who should get fees?
    pub(crate) fn suggested_fee_recipient(&self) -> Address {
        self.attributes.beneficiary
    }

    /// PrevRandao is used by TN to provide a source for randomness on-chain.
    ///
    /// This is used as the executed block's "mix_hash".
    /// [EIP-4399]: https://eips.ethereum.org/EIPS/eip-4399
    pub(crate) fn prev_randao(&self) -> B256 {
        self.attributes.mix_hash
    }

    /// Parent hash.
    pub(crate) fn parent(&self) -> B256 {
        self.attributes.parent_header.hash()
    }

    /// Taken from worker's block, but currently always empty.
    pub(crate) fn withdrawals(&self) -> &Withdrawals {
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
    pub batch_index: usize,
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
    /// Boolean indicating if the payload should use system calls to close the epoch during
    /// execution.
    ///
    /// This is the last batch for the `ConsensusOutput` if the epoch is closing.
    pub close_epoch: Option<BlsSignature>,
}

impl TNPayloadAttributes {
    /// Create a new instance of [Self].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_header: SealedHeader,
        batch_index: usize,
        batch_digest: B256,
        output: &ConsensusOutput,
        consensus_output_digest: B256,
        base_fee_per_gas: u64,
        gas_limit: u64,
        mix_hash: B256,
        withdrawals: Withdrawals,
    ) -> Self {
        // include leader's aggregate bls signature if this is the last payload for the epoch
        let close_epoch = output
            .epoch_closing_index()
            .is_some_and(|idx| idx == batch_index)
            .then(|| output.leader().aggregated_signature().unwrap_or_else(|| {
                error!(target: "engine", ?output, "BLS signature missing for leader - using default for closing epoch");
                BlsSignature::default()
            }));

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
            close_epoch,
        }
    }

    /// Method to create an instance of Self useful for tests.
    ///
    /// WARNING: only use this for tests. Data is invalid.
    pub fn new_for_test(parent_header: SealedHeader, output: &ConsensusOutput) -> Self {
        let batch_index = 0;
        let batch_digest = B256::random();
        let consensus_output_digest = output.digest().into();
        let base_fee_per_gas = parent_header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE);
        let gas_limit = parent_header.gas_limit;
        let mix_hash = B256::random();
        let withdrawals = Withdrawals::default();

        Self::new(
            parent_header,
            batch_index,
            batch_digest,
            output,
            consensus_output_digest,
            base_fee_per_gas,
            gas_limit,
            mix_hash,
            withdrawals,
        )
    }
}
