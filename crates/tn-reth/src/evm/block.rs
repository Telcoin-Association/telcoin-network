//! The types that build blocks for EVM execution.

use crate::{
    error::{TnRethError, TnRethResult},
    system_calls::{
        ConsensusRegistry::{self, RewardInfo, ValidatorStatus},
        CONSENSUS_REGISTRY_ADDRESS,
    },
    SYSTEM_ADDRESS,
};
use alloy::{
    consensus::{proofs, Block, BlockBody, Transaction, TxReceipt},
    eips::eip7685::Requests,
    sol_types::SolCall as _,
};
use alloy_evm::{Database, Evm};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_evm::{
    block::{
        BlockExecutor, BlockExecutorFactory, CommitChanges, ExecutableTx,
        InternalBlockExecutionError,
    },
    eth::receipt_builder::{ReceiptBuilder, ReceiptBuilderCtx},
    execute::{BlockAssembler, BlockAssemblerInput},
    FromRecoveredTx, FromTxWithEncoded, OnStateHook,
};
use reth_primitives::logs_bloom;
use reth_provider::BlockExecutionResult;
use reth_revm::{
    context::result::{ExecutionResult, ResultAndState},
    db::states::bundle_state::BundleRetention,
    DatabaseCommit as _, State,
};
use std::{collections::BTreeMap, sync::Arc};
use tn_types::{
    gas_accumulator::RewardsCounter, Address, Bytes, Encodable2718, ExecHeader, Receipt,
    TransactionSigned, Withdrawals, B256, EMPTY_OMMER_ROOT_HASH, EMPTY_WITHDRAWALS, U256,
};
use tracing::{debug, error, trace};

/// Context for TN block execution.
#[derive(Debug, Clone)]
pub struct TNBlockExecutionCtx {
    /// Parent block hash.
    pub parent_hash: B256,
    /// Parent beacon block root.
    pub parent_beacon_block_root: Option<B256>,
    /// The index for the batch.
    pub nonce: u64,
    /// The batch digest.
    ///
    /// This is the batch that was validated by consensus and is responsible for the
    /// request for execution.
    pub requests_hash: Option<B256>,
    /// Keccak hash of the bls signature for the leader certificate.
    ///
    /// Executor makes closing epoch system call when this if included.
    /// The hash is stored in the `extra_data` field so clients know when the
    /// closing epoch call was made.
    pub close_epoch: Option<B256>,
    /// Difficulty- this will contain the worker id ancd batch index.
    pub difficulty: U256,
    /// Counter used to allocate rewards for block leaders.
    pub rewards_counter: RewardsCounter,
}

/// Block executor for Ethereum.
#[derive(Debug)]
pub(crate) struct TNBlockExecutor<Evm, Spec, R: ReceiptBuilder> {
    /// Reference to the specification object.
    spec: Spec,
    /// Context for block execution.
    pub ctx: TNBlockExecutionCtx,
    /// Inner EVM.
    evm: Evm,
    /// Receipt builder.
    receipt_builder: R,

    /// Receipts of executed transactions.
    receipts: Vec<R::Receipt>,
    /// Total gas used by transactions in this block.
    gas_used: u64,
}

impl<'db, Evm, Spec, R, DB> TNBlockExecutor<Evm, Spec, R>
where
    DB: Database + 'db,
    DB::Error: core::fmt::Display,
    Evm: alloy_evm::Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<R::Transaction> + FromTxWithEncoded<R::Transaction>,
    >,
    Spec: EthereumHardforks,
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt = Receipt>,
{
    /// Creates a new [`TNBlockExecutor`]
    pub(crate) fn new(evm: Evm, ctx: TNBlockExecutionCtx, spec: Spec, receipt_builder: R) -> Self {
        Self { evm, ctx, receipts: Vec::new(), gas_used: 0, spec, receipt_builder }
    }

    /// Increase the beneficiary account balance and withdraw from governance safe.
    ///
    /// This must be called once per epoch, before the conclude epoch call.
    fn apply_consensus_block_rewards(
        &mut self,
        rewards: BTreeMap<Address, u32>,
    ) -> TnRethResult<()> {
        let calldata = self.generate_apply_incentives_calldata(
            rewards.iter().map(|(address, count)| (*address, *count)).collect(),
        )?;

        trace!(target: "engine", ?calldata, "apply incentives calldata");

        // execute system call to consensus registry
        let res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        ) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", "error applying consensus block rewards contract call: {:?}", e);
                return Err(TnRethError::EVMCustom(format!(
                    "applying consensus block rewards failed: {e}"
                )));
            }
        };

        // return error if closing epoch call failed
        if !res.result.is_success() {
            // execution failed
            error!(target: "engine", "failed applying consensus block rewards call: {:?}", res.result);
            return Err(TnRethError::EVMCustom(
                "failed applying consensus block rewards".to_string(),
            ));
        }
        trace!(target: "engine", ?res, "applying consensus block rewards");

        // commit the changes
        self.evm.db_mut().commit(res.state);

        Ok(())
    }

    /// Apply the closing epoch call to ConsensusRegistry.
    fn apply_closing_epoch_contract_call(&mut self, randomness: B256) -> TnRethResult<()> {
        debug!(target: "engine", ?randomness, "applying closing contract call");
        let calldata = self.generate_conclude_epoch_calldata(randomness)?;
        trace!(target: "engine", ?calldata, "close epoch calldata");

        // execute system call to consensus registry
        let res = match self.evm.transact_system_call(
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        ) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", "error executing closing epoch contract call: {:?}", e);
                return Err(TnRethError::EVMCustom(format!("epoch closing execution failed: {e}")));
            }
        };

        trace!(target: "engine", ?res, "transact system call for conclude epoch");

        // return error if closing epoch call failed
        if !res.result.is_success() {
            // execution failed
            error!(target: "engine", "failed to apply closing epoch call: {:?}", res.result);
            return Err(TnRethError::EVMCustom("failed to close epoch".to_string()));
        }

        trace!(target: "engine", "closing epoch logs:\n{:?}", res.result.logs());

        // commit the changes
        self.evm.db_mut().commit(res.state);
        Ok(())
    }

    /// Generate calldata for updating the ConsensusRegistry to conclude the epoch.
    fn generate_conclude_epoch_calldata(&mut self, randomness: B256) -> TnRethResult<Bytes> {
        // shuffle all validators for new committee
        let mut new_committee = self.shuffle_new_committee(randomness)?;

        // sort addresses in ascending order (0x0...0xf)
        new_committee.sort();
        debug!(target: "engine", ?new_committee, "new committee sorted by address");

        // encode the call to bytes with method selector and args
        let bytes = ConsensusRegistry::concludeEpochCall { newCommittee: new_committee }
            .abi_encode()
            .into();

        Ok(bytes)
    }

    /// Generate calldata for applying incentives when concluding the epoch.
    fn generate_apply_incentives_calldata(
        &mut self,
        reward_infos: Vec<(Address, u32)>,
    ) -> TnRethResult<Bytes> {
        debug!(target: "engine", ?reward_infos, "applying incentives");

        // encode the call to bytes with method selector and args
        let bytes = ConsensusRegistry::applyIncentivesCall {
            rewardInfos: reward_infos
                .iter()
                .map(|(address, count)| RewardInfo {
                    validatorAddress: *address,
                    consensusHeaderCount: U256::from(*count),
                })
                .collect(),
        }
        .abi_encode()
        .into();

        Ok(bytes)
    }

    /// Read eligible validators from latest state and shuffle the committee deterministically.
    fn shuffle_new_committee(&mut self, randomness: B256) -> TnRethResult<Vec<Address>> {
        let new_committee_size = self.next_committee_size()?;

        // read all active validators from consensus registry
        let calldata =
            ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                .abi_encode()
                .into();
        let state =
            self.read_state_on_chain(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;

        trace!(target: "engine", "get validators call:\n{:?}", state);

        let mut eligible_validators: Vec<ConsensusRegistry::ValidatorInfo> =
            alloy::sol_types::SolValue::abi_decode(&state)?;

        debug!(target: "engine",  "validators pre-shuffle {:?}", eligible_validators);

        // simple Fisher-Yates shuffle
        //
        // create seed from hashed bls agg signature
        let mut seed = [0; 32];
        seed.copy_from_slice(randomness.as_slice());
        trace!(target: "engine", ?seed, "seed after");

        let mut rng = StdRng::from_seed(seed);
        for i in (1..eligible_validators.len()).rev() {
            let j = rng.random_range(0..=i);
            eligible_validators.swap(i, j);
        }

        debug!(target: "engine",  "validators post-shuffle {:?}", eligible_validators);

        let mut new_committee =
            eligible_validators.into_iter().map(|v| v.validatorAddress).collect::<Vec<_>>();

        // trim the shuffled committee to maintain correct size
        new_committee.truncate(new_committee_size);

        trace!(target: "engine",  ?new_committee_size, ?new_committee, "truncated shuffle for new committee");

        Ok(new_committee)
    }

    /// Read state on-chain.
    fn read_state_on_chain(
        &mut self,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> TnRethResult<Bytes> {
        // read from state
        let res = match self.evm.transact_system_call(caller, contract, calldata) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", ?caller, ?contract, "failed to read state on chain: {}", e);
                return Err(TnRethError::EVMCustom(format!("failed to read state on chain: {e}")));
            }
        };

        // retrieve data from execution result
        let data = match res.result {
            ExecutionResult::Success { output, .. } => output.into_data(),
            e => {
                // fatal error
                error!(target: "engine", "error reading state on chain: {:?}", e);
                return Err(TnRethError::EVMCustom(format!("error reading state on chain: {e:?}")));
            }
        };

        Ok(data)
    }

    /// Return the next committee size.
    ///
    /// This is isolated into a function and requires a fork to change.
    fn next_committee_size(&mut self) -> TnRethResult<usize> {
        // retrieve the current committee size
        let epoch = self.extract_epoch_from_nonce(self.ctx.nonce);
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        let state =
            self.read_state_on_chain(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        let current_committee: Vec<ConsensusRegistry::ValidatorInfo> =
            alloy::sol_types::SolValue::abi_decode(&state)?;

        trace!(target: "engine",  ?current_committee, "read current committee to get the next committee size");

        // this will fail on-chain if incorrect
        Ok(current_committee.len())
    }

    /// Extract the epoch number from a header's nonce.
    fn extract_epoch_from_nonce(&self, nonce: u64) -> u32 {
        // epochs are packed into nonce as 32 bits
        let epoch = nonce >> 32;
        epoch as u32
    }
}

// alloy-evm
impl<'db, DB, E, Spec, R> BlockExecutor for TNBlockExecutor<E, Spec, R>
where
    DB: Database + 'db,
    E: Evm<
        DB = &'db mut State<DB>,
        Tx: FromRecoveredTx<TransactionSigned> + FromTxWithEncoded<TransactionSigned>,
    >,
    Spec: EthereumHardforks,
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt = Receipt>,
{
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;
    type Evm = E;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag =
            self.spec.is_spurious_dragon_active_at_block(self.evm.block().number);
        self.evm.db_mut().set_state_clear_flag(state_clear_flag);

        Ok(())
    }

    fn execute_transaction_with_commit_condition(
        &mut self,
        tx: impl ExecutableTx<Self>,
        f: impl FnOnce(&ExecutionResult<<Self::Evm as Evm>::HaltReason>) -> CommitChanges,
    ) -> Result<Option<u64>, BlockExecutionError> {
        // The sum of the transaction's gas limit, Tg, and the gas utilized in this block prior,
        // must be no greater than the block's gasLimit.
        let block_available_gas = self.evm.block().gas_limit - self.gas_used;

        if tx.tx().gas_limit() > block_available_gas {
            return Err(BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                transaction_gas_limit: tx.tx().gas_limit(),
                block_available_gas,
            }
            .into());
        }

        // Execute transaction.
        let ResultAndState { result, state } = self
            .evm
            .transact(tx)
            .map_err(|err| BlockExecutionError::evm(err, tx.tx().trie_hash()))?;

        if !f(&result).should_commit() {
            return Ok(None);
        }

        let gas_used = result.gas_used();

        // append gas used
        self.gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        self.receipts.push(self.receipt_builder.build_receipt(ReceiptBuilderCtx {
            tx: tx.tx(),
            evm: &self.evm,
            result,
            state: &state,
            cumulative_gas_used: self.gas_used,
        }));

        // Commit the state changes.
        self.evm.db_mut().commit(state);

        Ok(Some(gas_used))
    }

    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<R::Receipt>), BlockExecutionError> {
        // don't support prague deposit requests
        let requests = Requests::default();

        // potentially close epoch boundary
        if let Some(randomness) = self.ctx.close_epoch {
            debug!(target: "engine", ?randomness, "ctx indicates close epoch");
            self.apply_consensus_block_rewards(self.ctx.rewards_counter.get_address_counts())
                .map_err(|e| {
                    BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
                })?;

            self.apply_closing_epoch_contract_call(randomness).map_err(|e| {
                BlockExecutionError::Internal(InternalBlockExecutionError::Other(e.into()))
            })?;

            // merge transitions into bundle state
            self.evm.db_mut().merge_transitions(BundleRetention::Reverts);
        }

        Ok((
            self.evm,
            BlockExecutionResult { receipts: self.receipts, requests, gas_used: self.gas_used },
        ))
    }

    fn set_state_hook(&mut self, _hook: Option<Box<dyn OnStateHook>>) {
        unimplemented!("not using SystemCaller - nothing to set hook on")
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }
}

/// Block builder for TN.
#[derive(Debug, Clone)]
pub struct TNBlockAssembler<ChainSpec = reth_chainspec::ChainSpec> {
    /// The chainspec.
    pub chain_spec: Arc<ChainSpec>,
}

impl<ChainSpec> TNBlockAssembler<ChainSpec> {
    /// Creates a new [`TNBlockAssembler`].
    pub fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }
}

impl<F, ChainSpec> BlockAssembler<F> for TNBlockAssembler<ChainSpec>
where
    F: for<'a> BlockExecutorFactory<
        ExecutionCtx<'a> = TNBlockExecutionCtx,
        Transaction = TransactionSigned,
        Receipt = Receipt,
    >,
    ChainSpec: EthChainSpec + EthereumHardforks,
{
    type Block = Block<TransactionSigned>;

    fn assemble_block(
        &self,
        input: BlockAssemblerInput<'_, '_, F>,
    ) -> Result<Block<TransactionSigned>, BlockExecutionError> {
        let BlockAssemblerInput {
            evm_env,
            execution_ctx: ctx,
            transactions,
            output: BlockExecutionResult { receipts, gas_used, .. },
            state_root,
            ..
        } = input;

        let timestamp = evm_env.block_env.timestamp;
        let transactions_root = proofs::calculate_transaction_root(&transactions);
        let receipts_root = Receipt::calculate_receipt_root_no_memo(receipts);
        let logs_bloom = logs_bloom(receipts.iter().flat_map(|r| r.logs()));

        let withdrawals = Some(Withdrawals::default());
        let withdrawals_root = Some(EMPTY_WITHDRAWALS);

        // cancun isn't active
        let excess_blob_gas = None;
        let blob_gas_used = None;

        // TN-specific values
        let requests_hash = ctx.requests_hash; // prague inactive
        let nonce = ctx.nonce.into(); // subdag leader's nonce: ((epoch as u64) << 32) | self.round as u64
        let difficulty = ctx.difficulty; // worker id and batch index

        // use keccak256(bls_sig) if closing epoch or Bytes::default
        let extra_data = ctx.close_epoch.map(|hash| hash.to_vec().into()).unwrap_or_default();

        let header = ExecHeader {
            parent_hash: ctx.parent_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: evm_env.block_env.beneficiary,
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root,
            logs_bloom,
            timestamp,
            mix_hash: evm_env.block_env.prevrandao.unwrap_or_default(),
            nonce,
            base_fee_per_gas: Some(evm_env.block_env.basefee),
            number: evm_env.block_env.number,
            gas_limit: evm_env.block_env.gas_limit,
            difficulty,
            gas_used: *gas_used,
            extra_data,
            parent_beacon_block_root: ctx.parent_beacon_block_root,
            blob_gas_used,
            excess_blob_gas,
            requests_hash,
        };

        Ok(Block {
            header,
            body: BlockBody { transactions, ommers: Default::default(), withdrawals },
        })
    }
}
