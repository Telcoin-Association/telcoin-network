//! Provide config and implement traits to bridge protocol extensions to Reth.
//!
//! Inspired by: crates/ethereum/evm/src/lib.rs

use super::{TNBlockAssembler, TNBlockExecutionCtx, TNBlockExecutorFactory, TNEvmFactory};
use crate::{error::TnRethError, payload::TNPayload, traits::TNPrimitives};
use reth_chainspec::{ChainSpec, EthChainSpec as _};
use reth_evm::{ConfigureEvm, EvmEnv, EvmEnvFor, ExecutionCtxFor};
use reth_evm_ethereum::RethReceiptBuilder;
use reth_primitives::{BlockTy, HeaderTy};
use reth_revm::{
    context::{BlockEnv, CfgEnv},
    context_interface::block::BlobExcessGasAndPrice,
    primitives::hardfork::SpecId,
};
use std::sync::Arc;
use tn_types::{
    gas_accumulator::RewardsCounter, BlockHeader as _, Bytes, SealedBlock, SealedHeader, B256, U256,
};

/// TN-related EVM configuration.
#[derive(Debug, Clone)]
pub struct TnEvmConfig {
    /// Inner [`TNBlockExecutorFactory`].
    pub executor_factory: TNBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, TNEvmFactory>,
    /// Ethereum block assembler.
    pub block_assembler: TNBlockAssembler<ChainSpec>,
    /// Counter used to allocate rewards for block leaders.
    pub rewards_counter: RewardsCounter,
}

impl TnEvmConfig {
    /// Creates a new TN EVM configuration with the given chain spec.
    pub fn new(chain_spec: Arc<ChainSpec>, rewards_counter: RewardsCounter) -> Self {
        Self {
            block_assembler: TNBlockAssembler::new(chain_spec.clone()),
            executor_factory: TNBlockExecutorFactory::new(
                RethReceiptBuilder::default(),
                chain_spec,
                TNEvmFactory::default(),
            ),
            rewards_counter,
        }
    }

    /// Returns the chain spec associated with this configuration.
    pub const fn chain_spec(&self) -> &Arc<ChainSpec> {
        self.executor_factory.spec()
    }

    // TODO: remove this after compile
    //
    /// Returns blob params by hard fork as specified in chain spec.
    /// Blob params are in format `(spec id, target blob count, max blob count)`.
    pub fn blob_max_and_target_count_by_hardfork(&self) -> Vec<(SpecId, u64, u64)> {
        let cancun = self.chain_spec().blob_params.cancun();
        let prague = self.chain_spec().blob_params.prague();
        let osaka = self.chain_spec().blob_params.osaka();
        Vec::from([
            (SpecId::CANCUN, cancun.target_blob_count, cancun.max_blob_count),
            (SpecId::PRAGUE, prague.target_blob_count, prague.max_blob_count),
            (SpecId::OSAKA, osaka.target_blob_count, osaka.max_blob_count),
        ])
    }
}

// reth-evm
impl ConfigureEvm for TnEvmConfig {
    type Primitives = TNPrimitives;

    type Error = TnRethError;

    type NextBlockEnvCtx = TNPayload;

    type BlockExecutorFactory =
        TNBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, TNEvmFactory>;

    type BlockAssembler = TNBlockAssembler<ChainSpec>;

    fn block_executor_factory(&self) -> &Self::BlockExecutorFactory {
        &self.executor_factory
    }

    fn block_assembler(&self) -> &Self::BlockAssembler {
        &self.block_assembler
    }

    fn evm_env(&self, header: &HeaderTy<Self::Primitives>) -> EvmEnvFor<Self> {
        let spec = reth_evm_ethereum::revm_spec(self.chain_spec(), header);

        // configure evm env based on parent block
        let mut cfg_env =
            CfgEnv::new().with_chain_id(self.chain_spec().chain().id()).with_spec(spec);

        let blob_params = self.chain_spec().blob_params_at_timestamp(header.timestamp);
        if let Some(blob_params) = &blob_params {
            cfg_env.set_blob_max_count(blob_params.max_blob_count);
        }

        // derive the EIP-4844 blob fees from the header's `excess_blob_gas` and the current
        // blobparams
        let blob_excess_gas_and_price = header
            .excess_blob_gas
            .zip(self.chain_spec().blob_params_at_timestamp(header.timestamp))
            .map(|(excess_blob_gas, params)| {
                let blob_gasprice = params.calc_blob_fee(excess_blob_gas);
                BlobExcessGasAndPrice { excess_blob_gas, blob_gasprice }
            });

        let block_env = BlockEnv {
            number: header.number(),
            beneficiary: header.beneficiary(),
            timestamp: header.timestamp(),
            difficulty: if spec >= SpecId::MERGE { U256::ZERO } else { header.difficulty() },
            prevrandao: if spec >= SpecId::MERGE { header.mix_hash() } else { None },
            gas_limit: header.gas_limit(),
            basefee: header.base_fee_per_gas().unwrap_or_default(),
            blob_excess_gas_and_price,
        };

        EvmEnv { cfg_env, block_env }
    }

    fn next_evm_env(
        &self,
        parent: &HeaderTy<Self::Primitives>,
        payload: &Self::NextBlockEnvCtx,
    ) -> Result<EvmEnvFor<Self>, Self::Error> {
        // ensure we're not missing any timestamp based hardforks
        let spec_id = reth_evm_ethereum::revm_spec_by_timestamp_and_block_number(
            self.chain_spec(),
            payload.timestamp,
            parent.number() + 1,
        );

        // configure evm env based on parent block
        let mut cfg =
            CfgEnv::new().with_chain_id(self.chain_spec().chain().id()).with_spec(spec_id);

        let blob_params = self.chain_spec().blob_params_at_timestamp(payload.timestamp);
        if let Some(blob_params) = &blob_params {
            cfg.set_blob_max_count(blob_params.max_blob_count);
        }

        let block_env = BlockEnv {
            number: parent.number + 1,
            beneficiary: payload.beneficiary,
            timestamp: payload.timestamp,
            difficulty: U256::from(payload.batch_index),
            prevrandao: Some(payload.prev_randao()),
            gas_limit: payload.gas_limit,
            basefee: payload.base_fee_per_gas,
            blob_excess_gas_and_price: None,
        };

        let evm_env = EvmEnv::new(cfg, block_env);

        Ok(evm_env)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<BlockTy<Self::Primitives>>,
    ) -> ExecutionCtxFor<'a, Self> {
        // extra data is default otherwise it contains the hashed bls signature
        let close_epoch = if block.extra_data == Bytes::default() {
            None
        } else {
            Some(B256::from_slice(block.extra_data.as_ref()))
        };

        TNBlockExecutionCtx {
            parent_hash: block.header().parent_hash,
            parent_beacon_block_root: block.header().parent_beacon_block_root,
            nonce: block.nonce.into(),
            requests_hash: block.requests_hash,
            close_epoch,
            difficulty: block.difficulty,
            rewards_counter: self.rewards_counter.clone(),
        }
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader<HeaderTy<Self::Primitives>>,
        payload: Self::NextBlockEnvCtx,
    ) -> ExecutionCtxFor<'_, Self> {
        TNBlockExecutionCtx {
            parent_hash: parent.hash(),
            parent_beacon_block_root: payload.parent_beacon_block_root(),
            nonce: payload.nonce,
            requests_hash: payload.batch_digest,
            close_epoch: payload.close_epoch,
            difficulty: U256::from(payload.batch_index << 16 | payload.worker_id as usize),
            rewards_counter: self.rewards_counter.clone(),
        }
    }
}
