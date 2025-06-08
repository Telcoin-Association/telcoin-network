//! Compatibility types for Telcoin Node and reth.
//!
//! These are used to spawn execution components for the node and maintain compatibility with reth's
//! API.

use alloy::rpc::types::engine::ExecutionPayload;
use reth::{
    payload::{EthBuiltPayload, EthPayloadBuilderAttributes},
    rpc::types::engine::ExecutionData,
};
use reth_chainspec::ChainSpec;
pub use reth_consensus::{Consensus, ConsensusError};
use reth_consensus::{FullConsensus, HeaderValidator};
use reth_db::DatabaseEnv;
use reth_engine_primitives::PayloadValidator;
use reth_node_builder::{
    BuiltPayload, EngineValidator, NewPayloadError, NodeTypes, NodeTypesWithDB, PayloadTypes,
};
use reth_node_ethereum::{engine::EthPayloadAttributes, EthEngineTypes};
use reth_primitives_traits::Block;
use reth_provider::{BlockExecutionResult, EthStorage};
use reth_trie_db::MerklePatriciaTrie;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tn_types::{EthPrimitives, NodePrimitives, RecoveredBlock, SealedBlock, SealedHeader};

/// Type for primitives.
pub type TNPrimitives = EthPrimitives;

/// Empty struct that implements Reth traits to supply GATs and functionality for Reth integration.
#[derive(Clone, Debug)]
pub struct TelcoinNode {}

impl NodeTypes for TelcoinNode {
    type Primitives = TNPrimitives;
    type ChainSpec = ChainSpec;
    type StateCommitment = MerklePatriciaTrie;
    type Storage = EthStorage;
    type Payload = EthEngineTypes;
}

impl NodeTypesWithDB for TelcoinNode {
    type DB = Arc<DatabaseEnv>;
}

/// Compatibility type to easily integrate with reth.
///
/// This type is used to noop verify all data. It is not used by Telcoin Network, but is required to
/// integrate with reth for convenience. TN is mostly EVM/Ethereum types, but with a different
/// consensus. The traits impl on this type are only used beacon engine, which is not used by TN.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
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
}

impl<B: Block> Consensus<B> for TNExecution {
    type Error = ConsensusError;

    fn validate_body_against_header(
        &self,
        _body: &B::Body,
        _header: &SealedHeader<B::Header>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn validate_block_pre_execution(&self, _block: &SealedBlock<B>) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<N: NodePrimitives> FullConsensus<N> for TNExecution {
    fn validate_block_post_execution(
        &self,
        _block: &RecoveredBlock<N::Block>,
        _result: &BlockExecutionResult<N::Receipt>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }
}

// Compatibility noop trait impl.
// This is for the reth rpc build method.
// NOTE: this should never be called because there is no beacon API
impl PayloadValidator for TNExecution {
    type Block = tn_types::Block;
    type ExecutionData = ExecutionData;

    fn ensure_well_formed_payload(
        &self,
        _payload: ExecutionData,
    ) -> Result<RecoveredBlock<Self::Block>, NewPayloadError> {
        Ok(Default::default())
    }
}

impl<T> EngineValidator<T> for TNExecution
where
    T: PayloadTypes<PayloadAttributes = EthPayloadAttributes, ExecutionData = ExecutionData>,
{
    fn validate_version_specific_fields(
        &self,
        _version: reth_node_builder::EngineApiMessageVersion,
        _payload_or_attrs: reth_node_builder::PayloadOrAttributes<
            '_,
            T::ExecutionData,
            <T as PayloadTypes>::PayloadAttributes,
        >,
    ) -> Result<(), reth_node_builder::EngineObjectValidationError> {
        Ok(())
    }

    fn ensure_well_formed_attributes(
        &self,
        _version: reth_node_builder::EngineApiMessageVersion,
        _attributes: &<T as PayloadTypes>::PayloadAttributes,
    ) -> Result<(), reth_node_builder::EngineObjectValidationError> {
        Ok(())
    }
}

/// A default payload type for [`EthEngineTypes`]
///
/// This is required by the `EngineApiTreeHandler` but is never used bc
/// TN doesn't send beacon messages.
#[derive(Debug, Default, Clone, serde::Deserialize, serde::Serialize)]
#[non_exhaustive]
pub struct DefaultEthPayloadTypes;

impl PayloadTypes for DefaultEthPayloadTypes {
    type BuiltPayload = EthBuiltPayload;
    type PayloadAttributes = EthPayloadAttributes;
    type PayloadBuilderAttributes = EthPayloadBuilderAttributes;
    type ExecutionData = ExecutionData;

    fn block_to_payload(
        block: SealedBlock<
            <<Self::BuiltPayload as BuiltPayload>::Primitives as NodePrimitives>::Block,
        >,
    ) -> Self::ExecutionData {
        let (payload, sidecar) =
            ExecutionPayload::from_block_unchecked(block.hash(), &block.into_block());
        ExecutionData { payload, sidecar }
    }
}
