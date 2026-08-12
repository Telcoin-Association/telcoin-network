//! Provide config and implement traits to bridge protocol extensions to Reth.
//!
//! Inspired by: crates/ethereum/evm/src/lib.rs
//!
//! [`TnEvmConfig`] is TN's `ConfigureEvm` implementation. It wires three pieces together: the
//! `TNEvmFactory` (every EVM instance, with TN precompiles installed), the
//! `TNBlockExecutorFactory` (block-level execution, including the epoch-closing system calls),
//! and the `TNBlockAssembler` (header assembly), plus the shared `GasAccumulator` threaded into
//! each block's execution context — per-worker gas totals, current base fees, worker count, and
//! the rewards counter that drives leader-reward attribution.
//!
//! The epoch-close digest flows through here in both directions. Building a new block,
//! `context_for_next_block` copies `TNPayload::close_epoch` into the execution context, and the
//! assembler later records it as the header's `extra_data`. Re-executing a stored block,
//! `context_for_block` recovers the same value by decoding `extra_data` (empty = normal block,
//! 32 bytes = the closing-epoch digest, any other length = malformed-header error).
//!
//! Replay caveat: `gas_accumulator` is NOT header-derived — both context builders clone the
//! config's live shared accumulator, so the ctx carries whatever gas totals, base fees, and
//! leader counts it holds at execution time. Every canonical path is seeded before it executes:
//! `catchup_accumulator` (in `tn-node`) rebuilds the accumulator from chain state at startup, and
//! the epoch manager seeds the rewards counter's committee before replaying an epoch's outputs —
//! so a block built or re-executed through the payload builder always sees correct live values.
//! Ad-hoc re-execution that rebuilds the ctx from a sealed header alone (e.g.
//! `debug_executionWitness`) gets no such seeding and stays degraded: it runs `applyIncentives`
//! with whatever the counter holds at that moment, not the amounts the historical block
//! distributed. Reconstructing the historical counts from `block.body.withdrawals` (which records
//! them) is a known follow-up. Widening this from the rewards counter to the whole accumulator
//! extends the same accepted limitation to the gas and fee data.
//!
//! Callers that construct a `RethEnv` with a defaulted accumulator (e.g. the snapshot machinery,
//! `new_for_temp_chain`) must never drive block EXECUTION through it. The accumulator now backs
//! an EVM state write — `record_next_epoch_base_fees`' `setWorkerConfigsData` — so re-executing
//! an epoch-closing block there would price every EIP-1559 worker from one empty slot (zero gas,
//! `MIN_PROTOCOL_BASE_FEE`) and produce a divergent state root, not merely degraded reward
//! accounting.

use super::{TNBlockAssembler, TNBlockExecutionCtx, TNBlockExecutorFactory, TNEvmFactory};
use crate::{error::TnRethError, payload::TNPayload, TNPrimitives};
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
    gas_accumulator::GasAccumulator, BlockHeader as _, SealedBlock, SealedHeader, B256, U256,
};

/// TN-related EVM configuration.
#[derive(Debug, Clone)]
pub struct TnEvmConfig {
    /// Inner [`TNBlockExecutorFactory`].
    pub executor_factory: TNBlockExecutorFactory<RethReceiptBuilder, Arc<ChainSpec>, TNEvmFactory>,
    /// Ethereum block assembler.
    pub block_assembler: TNBlockAssembler<ChainSpec>,
    /// Live shared accumulator for the current epoch: per-worker gas totals, current base fees,
    /// worker count, and the leader counts used to allocate block rewards.
    ///
    /// Cloned into every block's execution context. Execution reads only the rewards counter
    /// today — the gas and fee data is carried so the epoch-closing block executor can reach it.
    pub gas_accumulator: GasAccumulator,
}

impl TnEvmConfig {
    /// Creates a new TN EVM configuration with the given chain spec.
    pub fn new(chain_spec: Arc<ChainSpec>, gas_accumulator: GasAccumulator) -> Self {
        Self {
            block_assembler: TNBlockAssembler::new(chain_spec.clone()),
            executor_factory: TNBlockExecutorFactory::new(
                RethReceiptBuilder::default(),
                chain_spec,
                TNEvmFactory::default(),
            ),
            gas_accumulator,
        }
    }

    /// Returns the chain spec associated with this configuration.
    pub const fn chain_spec(&self) -> &Arc<ChainSpec> {
        self.executor_factory.spec()
    }

    /// Build the spec-derived `CfgEnv` both env constructors share.
    ///
    /// Gas params must stay spec-derived: revm's EIP-7702 intrinsic charge re-derives its gas
    /// table from spec (`calculate_initial_tx_gas`, revm cfg/gas.rs:147) and ignores
    /// `cfg.gas_params`, while the penalty basis in `evm/handler.rs` reads `cfg.gas_params()`.
    /// Routing both constructors through this one builder keeps the two derivations coupled by
    /// construction, pinned by `evm_env_gas_params_match_spec_derived_table`.
    fn spec_derived_cfg_env(&self, spec: SpecId) -> CfgEnv {
        CfgEnv::new()
            .with_chain_id(self.chain_spec().chain().id())
            .with_spec_and_mainnet_gas_params(spec)
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

    fn evm_env(&self, header: &HeaderTy<Self::Primitives>) -> Result<EvmEnv, Self::Error> {
        let spec = reth_evm_ethereum::revm_spec(self.chain_spec(), header);

        // configure evm env based on parent block
        let mut cfg_env = self.spec_derived_cfg_env(spec);

        let blob_params = self.chain_spec().blob_params_at_timestamp(header.timestamp);
        if let Some(blob_params) = &blob_params {
            cfg_env.set_max_blobs_per_tx(blob_params.max_blob_count);
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
            number: U256::from(header.number()),
            beneficiary: header.beneficiary(),
            timestamp: U256::from(header.timestamp()),
            difficulty: U256::ZERO,
            prevrandao: header.mix_hash(),
            gas_limit: header.gas_limit(),
            basefee: header.base_fee_per_gas().unwrap_or_default(),
            blob_excess_gas_and_price,
        };

        Ok(EvmEnv { cfg_env, block_env })
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
        let mut cfg = self.spec_derived_cfg_env(spec_id);

        let blob_params = self.chain_spec().blob_params_at_timestamp(payload.timestamp);
        if let Some(blob_params) = &blob_params {
            cfg.set_max_blobs_per_tx(blob_params.max_blob_count);
        }

        let block_env = BlockEnv {
            number: U256::from(parent.number + 1),
            beneficiary: payload.beneficiary,
            timestamp: U256::from(payload.timestamp),
            // unread post-merge; the header's difficulty comes from ctx.difficulty in the
            // assembler (batch_index << 16 | worker_id), not from this block-env field
            difficulty: U256::from(payload.batch_index),
            prevrandao: Some(payload.prev_randao()),
            gas_limit: payload.gas_limit,
            basefee: payload.base_fee_per_gas,
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice {
                excess_blob_gas: 0,       // no excess gas for blobs
                blob_gasprice: u128::MAX, // eip4844 transactions are ignored
            }),
        };

        let evm_env = EvmEnv::new(cfg, block_env);

        Ok(evm_env)
    }

    fn context_for_block<'a>(
        &self,
        block: &'a SealedBlock<BlockTy<Self::Primitives>>,
    ) -> Result<ExecutionCtxFor<'a, Self>, Self::Error> {
        // `extra_data` is empty for a normal block; an epoch-closing block carries the 32-byte
        // epoch seed chain value as of the closing commit (see
        // `TNBlockAssembler::assemble_block`).
        // Any other length is a malformed header — error instead of panicking in `from_slice`.
        let close_epoch = match block.extra_data.len() {
            0 => None,
            32 => Some(B256::from_slice(block.extra_data.as_ref())),
            len => {
                return Err(TnRethError::EVMCustom(format!(
                    "invalid extra_data length {len} in block {}: expected empty or a 32-byte \
                     closing-epoch digest",
                    block.hash(),
                )))
            }
        };

        Ok(TNBlockExecutionCtx {
            parent_hash: block.header().parent_hash,
            parent_beacon_block_root: block.header().parent_beacon_block_root,
            nonce: block.nonce.into(),
            ommers_hash: block.ommers_hash,
            close_epoch,
            difficulty: block.difficulty,
            gas_accumulator: self.gas_accumulator.clone(),
            // the header carries no slash list: a replayed block cannot reproduce a
            // test-injected slash (and does not need to — production lists are always empty)
            #[cfg(test)]
            epoch_boundary_slashes: Vec::new(),
        })
    }

    fn context_for_next_block(
        &self,
        parent: &SealedHeader<HeaderTy<Self::Primitives>>,
        payload: Self::NextBlockEnvCtx,
    ) -> Result<ExecutionCtxFor<'_, Self>, Self::Error> {
        Ok(TNBlockExecutionCtx {
            parent_hash: parent.hash(),
            parent_beacon_block_root: payload.parent_beacon_block_root(),
            nonce: payload.nonce,
            ommers_hash: payload.batch_digest,
            close_epoch: payload.close_epoch,
            difficulty: U256::from(payload.batch_index << 16 | payload.worker_id as usize),
            gas_accumulator: self.gas_accumulator.clone(),
            #[cfg(test)]
            epoch_boundary_slashes: payload.epoch_boundary_slashes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_revm::context_interface::cfg::GasParams;
    use tn_types::{Block, Bytes, ExecHeader};

    /// Replay-path `extra_data` decode: empty → no epoch close, 32 bytes → the closing-epoch
    /// digest, anything else → an error rather than a `B256::from_slice` panic.
    #[test]
    fn test_context_for_block_extra_data_lengths() {
        let chain: ChainSpec = tn_types::test_genesis().into();
        let config = TnEvmConfig::new(Arc::new(chain), GasAccumulator::default());
        let block_with_extra = |extra_data: Bytes| {
            SealedBlock::seal_slow(Block {
                header: ExecHeader { extra_data, ..Default::default() },
                body: Default::default(),
            })
        };

        // normal block: empty extra_data
        let block = block_with_extra(Bytes::default());
        let ctx = config.context_for_block(&block).expect("empty extra_data is valid");
        assert!(ctx.close_epoch.is_none());

        // epoch-closing block: 32-byte digest
        let digest = B256::repeat_byte(7);
        let block = block_with_extra(digest.to_vec().into());
        let ctx = config.context_for_block(&block).expect("32-byte extra_data is valid");
        assert_eq!(ctx.close_epoch, Some(digest));

        // malformed header: any other length errors instead of panicking
        let block = block_with_extra(Bytes::from_static(b"bad"));
        assert!(config.context_for_block(&block).is_err());
    }

    /// The 7702 penalty basis in `evm/handler.rs` reads `cfg.gas_params()`, while revm charges
    /// the intrinsic from `GasParams::new_spec(cfg.spec)` (`calculate_initial_tx_gas`, revm
    /// `cfg/gas.rs:147`). Those are independent derivations, so pin that every `CfgEnv` TN
    /// constructs keeps them in agreement — a desync would have the handler subtract an intrinsic
    /// revm never charged. The pin exercises `evm_env`; `next_evm_env` (the block-production
    /// path) is covered by construction, since both constructors build their `CfgEnv` through
    /// `spec_derived_cfg_env`.
    #[test]
    fn evm_env_gas_params_match_spec_derived_table() {
        let chain: ChainSpec = tn_types::test_genesis().into();
        let config = TnEvmConfig::new(Arc::new(chain), GasAccumulator::default());

        // timestamp 0 on the default header: every TN chain spec activates Prague at 0
        let header = ExecHeader::default();
        let cfg_env = config.evm_env(&header).expect("evm env").cfg_env;

        // guard against a vacuous pass: both tables agree trivially on a pre-Prague spec, where
        // the per-empty-account cost is 0 and the handler's subtraction is a no-op
        assert_eq!(
            cfg_env.gas_params.tx_eip7702_per_empty_account_cost(),
            25_000,
            "fixture must resolve a spec with a live eip-7702 gas table"
        );

        assert_eq!(
            cfg_env.gas_params.table(),
            GasParams::new_spec(cfg_env.spec).table(),
            "TN cfg must keep gas params coupled to spec: revm's EIP-7702 intrinsic charge \
             re-derives its table from spec and ignores cfg.gas_params"
        );
    }
}
