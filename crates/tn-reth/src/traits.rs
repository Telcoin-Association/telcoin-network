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
use reth_consensus::{FullConsensus, HeaderValidator, ReceiptRootBloom};
use reth_db::DatabaseEnv;
use reth_engine_primitives::PayloadValidator;
use reth_node_builder::{BuiltPayload, NewPayloadError, NodeTypes, NodeTypesWithDB, PayloadTypes};
use reth_node_ethereum::{engine::EthPayloadAttributes, EthEngineTypes};
use reth_primitives_traits::Block;
use reth_provider::{BlockExecutionResult, EthStorage};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tn_types::{NodePrimitives, RecoveredBlock, SealedBlock, SealedHeader};

use crate::TNPrimitives;

/// Empty struct that implements Reth traits to supply GATs and functionality for Reth integration.
#[derive(Clone, Debug)]
pub struct TelcoinNode {}

impl NodeTypes for TelcoinNode {
    type Primitives = TNPrimitives;
    type ChainSpec = ChainSpec;
    type Storage = EthStorage;
    type Payload = EthEngineTypes;
}

impl NodeTypesWithDB for TelcoinNode {
    type DB = Arc<DatabaseEnv>;
}

/// Compatibility type that stands in for reth's beacon-engine consensus and payload machinery.
///
/// Telcoin Network never drives this machinery: TN runs its own consensus and canonicalizes blocks
/// directly (see `finish_executing_output` in `lib.rs`), so no header, body, or payload ever flows
/// through the trait methods below. The impls exist only to satisfy reth's trait bounds when wiring
/// the RPC stack; they are not part of any driven code path on current `main`.
///
/// For that reason every method fails loudly instead of silently accepting (or, for the payload
/// methods, fabricating) data. None is reachable today, so a call means reth's engine or
/// validation machinery has been wired to TN by mistake (for example by registering the flashbots
/// `ValidationApi`, building an auth/engine server, or constructing the engine tree handler).
/// Returning a descriptive error surfaces that regression at the first call, rather than letting a
/// skipped check or a fabricated empty block propagate undetected. See issue #1048.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TNExecution;

/// Build the message for the error every [`TNExecution`] validation method returns.
///
/// `method` names the trait method that was reached. The message is wrapped in
/// [`ConsensusError::Other`] or [`NewPayloadError::Other`] at the call site, so both the consensus
/// and payload paths share one wording for the "this should never run on TN" invariant.
fn unreachable_validation(method: &str) -> String {
    format!(
        "TNExecution::{method} was called: Telcoin Network never drives reth's beacon-engine \
         validation or payload conversion, so reaching this path means that machinery was wired to \
         TN by mistake (see issue #1048)"
    )
}

impl<H> HeaderValidator<H> for TNExecution {
    fn validate_header(&self, _header: &SealedHeader<H>) -> Result<(), ConsensusError> {
        Err(ConsensusError::Other(unreachable_validation("validate_header")))
    }

    fn validate_header_against_parent(
        &self,
        _header: &SealedHeader<H>,
        _parent: &SealedHeader<H>,
    ) -> Result<(), ConsensusError> {
        Err(ConsensusError::Other(unreachable_validation("validate_header_against_parent")))
    }
}

impl<B: Block> Consensus<B> for TNExecution {
    fn validate_body_against_header(
        &self,
        _body: &B::Body,
        _header: &SealedHeader<B::Header>,
    ) -> Result<(), ConsensusError> {
        Err(ConsensusError::Other(unreachable_validation("validate_body_against_header")))
    }

    fn validate_block_pre_execution(&self, _block: &SealedBlock<B>) -> Result<(), ConsensusError> {
        Err(ConsensusError::Other(unreachable_validation("validate_block_pre_execution")))
    }
}

impl<N: NodePrimitives> FullConsensus<N> for TNExecution {
    fn validate_block_post_execution(
        &self,
        _block: &RecoveredBlock<N::Block>,
        _result: &BlockExecutionResult<N::Receipt>,
        _receipt_root_bloom: Option<ReceiptRootBloom>,
    ) -> Result<(), ConsensusError> {
        Err(ConsensusError::Other(unreachable_validation("validate_block_post_execution")))
    }
}

// Compatibility trait impl for the reth rpc build method.
//
// This is never called because TN wires no beacon/engine API. Rather than fabricate a default
// (empty) block for every payload, `convert_payload_to_block` fails loudly; the trait's default
// `ensure_well_formed_payload` delegates to it via `?`, so it inherits the same error and no
// override is needed.
impl<Types> PayloadValidator<Types> for TNExecution
where
    Types: PayloadTypes<ExecutionData = ExecutionData>,
{
    type Block = tn_types::Block;

    fn convert_payload_to_block(
        &self,
        _payload: ExecutionData,
    ) -> Result<SealedBlock<Self::Block>, NewPayloadError> {
        Err(NewPayloadError::Other(unreachable_validation("convert_payload_to_block").into()))
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

#[cfg(test)]
mod tests {
    //! Each test pins the fail-loud contract of one `TNExecution` validation method: the method
    //! must return `Err`, and the error must name the method, so a test cannot pass on some
    //! unrelated error. Confirm-by-mutation: reverting a body to its old `Ok(..)` form reds its
    //! test; because the deleted `ensure_well_formed_payload` override now delegates to
    //! `convert_payload_to_block`, reverting that one body reds both payload tests. The tests
    //! were observed to fail under that reversion, so they pin the property rather than passing
    //! vacuously.
    use super::*;

    /// Concrete block type used to source header/body/block fixtures for the generic trait methods.
    type TestBlock = tn_types::Block;

    /// A default (empty) sealed block; the methods ignore their arguments, so an empty fixture is
    /// enough to exercise the error path.
    fn default_block() -> SealedBlock<TestBlock> {
        SealedBlock::default()
    }

    /// A minimal [`ExecutionData`], built from an empty block via the crate's own conversion.
    fn execution_data() -> ExecutionData {
        DefaultEthPayloadTypes::block_to_payload(default_block())
    }

    /// Assert that `result` is the fail-loud `Err` whose message names `method`.
    ///
    /// Checking the message (not merely `is_err()`) pins each test to the specific error origin:
    /// the delegation test can only pass on the error raised by `convert_payload_to_block`, not on
    /// a decoy error from some other fallible leg of the trait default.
    fn assert_fails_loudly<T, E: std::fmt::Display>(result: Result<T, E>, method: &str) {
        let marker = format!("TNExecution::{method} was called");
        assert!(
            result.err().is_some_and(|error| error.to_string().contains(&marker)),
            "expected a fail-loud error whose message contains `{marker}`",
        );
    }

    #[test]
    fn validate_header_fails_loudly() {
        let block = default_block();
        assert_fails_loudly(TNExecution.validate_header(block.sealed_header()), "validate_header");
    }

    #[test]
    fn validate_header_against_parent_fails_loudly() {
        let block = default_block();
        let header = block.sealed_header();
        assert_fails_loudly(
            TNExecution.validate_header_against_parent(header, header),
            "validate_header_against_parent",
        );
    }

    #[test]
    fn validate_body_against_header_fails_loudly() {
        let block = default_block();
        let result = <TNExecution as Consensus<TestBlock>>::validate_body_against_header(
            &TNExecution,
            block.body(),
            block.sealed_header(),
        );
        assert_fails_loudly(result, "validate_body_against_header");
    }

    #[test]
    fn validate_block_pre_execution_fails_loudly() {
        let result = <TNExecution as Consensus<TestBlock>>::validate_block_pre_execution(
            &TNExecution,
            &default_block(),
        );
        assert_fails_loudly(result, "validate_block_pre_execution");
    }

    #[test]
    fn validate_block_post_execution_fails_loudly() {
        let recovered = RecoveredBlock::<TestBlock>::default();
        let result = <TNExecution as FullConsensus<TNPrimitives>>::validate_block_post_execution(
            &TNExecution,
            &recovered,
            &BlockExecutionResult::default(),
            None,
        );
        assert_fails_loudly(result, "validate_block_post_execution");
    }

    #[test]
    fn convert_payload_to_block_fails_loudly() {
        let result =
            <TNExecution as PayloadValidator<DefaultEthPayloadTypes>>::convert_payload_to_block(
                &TNExecution,
                execution_data(),
            );
        assert_fails_loudly(result, "convert_payload_to_block");
    }

    #[test]
    fn ensure_well_formed_payload_inherits_the_error() {
        // The override was deleted; the trait default delegates to `convert_payload_to_block` via
        // `?`, so this must surface that method's error rather than a fabricated block. Asserting
        // on the `convert_payload_to_block` marker (not `ensure_well_formed_payload`) is
        // deliberate: it is the delegated-to method whose error must propagate.
        let result =
            <TNExecution as PayloadValidator<DefaultEthPayloadTypes>>::ensure_well_formed_payload(
                &TNExecution,
                execution_data(),
            );
        assert_fails_loudly(result, "convert_payload_to_block");
    }
}
