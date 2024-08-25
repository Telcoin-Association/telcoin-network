//! The logic for building worker blocks.

use reth_evm::ConfigureEvm;
use reth_payload_builder::database::CachedReads;
use reth_primitives::{
    constants::EMPTY_WITHDRAWALS, keccak256, proofs, Block, Header, IntoRecoveredTransaction,
    Receipt, SealedBlockWithSenders, Withdrawals, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{ChainSpecProvider, ExecutionOutcome, StateProviderFactory};
use reth_revm::{
    database::StateProviderDatabase,
    db::states::bundle_state::BundleRetention,
    primitives::{EVMError, EnvWithHandlerCfg, InvalidTransaction, ResultAndState},
    DatabaseCommit, State,
};
use reth_transaction_pool::{BestTransactionsAttributes, TransactionPool};
use tn_types::{now, PendingBlockConfig, WorkerBlockBuilderArgs, WorkerBlockUpdate};
use tracing::{debug, warn};

use crate::error::{BlockBuilderError, BlockBuilderResult};

/// Construct an TN worker block using the best transactions from the pool.
///
/// Returns a result indicating success with the payload or an error in case of failure.
#[inline]
pub fn build_worker_block<EvmConfig, Pool, Provider>(
    evm_config: EvmConfig,
    args: WorkerBlockBuilderArgs<Pool, Provider>,
) -> BlockBuilderResult<WorkerBlockUpdate>
where
    EvmConfig: ConfigureEvm,
    Provider: StateProviderFactory + ChainSpecProvider,
    Pool: TransactionPool,
{
    let WorkerBlockBuilderArgs { provider, pool, block_config, beneficiary } = args;
    let PendingBlockConfig { parent, initialized_block_env, initialized_cfg } = block_config;
    let state_provider = provider.state_by_block_hash(parent.hash())?;
    let state = StateProviderDatabase::new(state_provider);

    // TODO: using same apprach as reth here bc I can't find the State::builder()'s methods
    // I'm not sure what `with_bundle_update` does, and using `CachedReads` is the only way
    // I can get the state root section below to compile using `db.commit(state)`.
    //
    // TODO: create `CachedReads` during batch validation?
    // same problem with engine's payload_builder
    //
    // it would be great for txpool to manage this on tx validation
    let mut cached_reads = CachedReads::default();
    let mut db =
        State::builder().with_database_ref(cached_reads.as_db(state)).with_bundle_update().build();

    debug!(target: "block_builder", parent_hash = ?parent.hash(), parent_number = parent.number, "building new payload");
    let chain_spec = provider.chain_spec();
    let block_gas_limit: u64 =
        initialized_block_env.gas_limit.try_into().unwrap_or(chain_spec.max_gas_limit);
    let base_fee = initialized_block_env.basefee.to::<u64>();

    let mut best_txs = pool.best_transactions_with_attributes(BestTransactionsAttributes::new(
        base_fee,
        initialized_block_env.get_blob_gasprice().map(|gasprice| gasprice as u64),
    ));

    let block_number = initialized_block_env.number.to::<u64>();

    // // apply eip-4788 pre block contract call
    // pre_block_beacon_root_contract_call(
    //     &mut db,
    //     &evm_config,
    //     &chain_spec,
    //     &initialized_cfg,
    //     &initialized_block_env,
    //     block_number,
    //     attributes.timestamp,
    //     attributes.parent_beacon_block_root,
    // )
    // .map_err(|err| {
    //     warn!(target: "block_builder",
    //         parent_hash=%parent.hash(),
    //         %err,
    //         "failed to apply beacon root contract call for empty payload"
    //     );
    //     PayloadBuilderError::Internal(err.into())
    // })?;

    // TODO: TN needs to support this
    // // apply eip-2935 blockhashes update
    // apply_blockhashes_update(
    //     &mut db,
    //     &chain_spec,
    //     initialized_block_env.timestamp.to::<u64>(),
    //     block_number,
    //     parent.hash(),
    // )
    // .map_err(|e| BlockBuilderError::Reth(e.into()))?;

    // collect data for successful transactions
    // let mut sum_blob_gas_used = 0;
    let mut cumulative_gas_used = 0;
    let mut receipts = Vec::new();
    let mut senders = Vec::new();
    let mut total_fees = U256::ZERO;
    let mut executed_txs = Vec::new();

    // begin loop through sorted "best" transactions in pending pool
    // and execute them to build the block
    while let Some(pool_tx) = best_txs.next() {
        // ensure we still have capacity for this transaction
        if cumulative_gas_used + pool_tx.gas_limit() > block_gas_limit {
            // we can't fit this transaction into the block, so we need to mark it as invalid
            // which also removes all dependent transaction from the iterator before we can
            // continue
            best_txs.mark_invalid(&pool_tx);
            continue;
        }

        // convert tx to a signed transaction
        let tx = pool_tx.to_recovered_transaction();

        // TODO: support blob gas with cancun genesis hardfork
        //
        // if let Some(blob_tx) = tx.transaction.as_eip4844() {
        //     let tx_blob_gas = blob_tx.blob_gas();
        //     if sum_blob_gas_used + tx_blob_gas > MAX_DATA_GAS_PER_BLOCK {
        //         // we can't fit this _blob_ transaction into the block, so we mark it as
        //         // invalid, which removes its dependent transactions from
        //         // the iterator. This is similar to the gas limit condition
        //         // for regular transactions above.
        //         tracing::trace!(target: "block_builder", tx=?tx.hash, ?sum_blob_gas_used, ?tx_blob_gas, "skipping blob transaction because it would exceed the max data gas per block");
        //         best_txs.mark_invalid(&pool_tx);
        //         continue;
        //     }
        // }

        // create env for EVM
        let env = EnvWithHandlerCfg::new_with_cfg_env(
            initialized_cfg.clone(),
            initialized_block_env.clone(),
            evm_config.tx_env(&tx),
        );

        // Configure the environment for the block.
        let mut evm = evm_config.evm_with_env(&mut db, env);

        // EVM execution
        let ResultAndState { result, state } = match evm.transact() {
            Ok(res) => res,
            Err(err) => {
                match err {
                    EVMError::Transaction(err) => {
                        if matches!(err, InvalidTransaction::NonceTooLow { .. }) {
                            // if the nonce is too low, we can skip this transaction
                            tracing::trace!(target: "block_builder", %err, ?tx, "skipping nonce too low transaction");
                        } else {
                            // if the transaction is invalid, we can skip it and all of its
                            // descendants
                            tracing::trace!(target: "block_builder", %err, ?tx, "skipping invalid transaction and its descendants");
                            best_txs.mark_invalid(&pool_tx);
                        }

                        continue;
                    }
                    err => {
                        // this is an error that we should treat as fatal for this attempt
                        return Err(BlockBuilderError::EvmExecution(err));
                    }
                }
            }
        };

        // drop evm so db is released.
        drop(evm);

        // commit changes
        db.commit(state);

        // // add to the total blob gas used if the transaction successfully executed
        // if let Some(blob_tx) = tx.transaction.as_eip4844() {
        //     let tx_blob_gas = blob_tx.blob_gas();
        //     sum_blob_gas_used += tx_blob_gas;

        //     // if we've reached the max data gas per block, we can skip blob txs entirely
        //     if sum_blob_gas_used == MAX_DATA_GAS_PER_BLOCK {
        //         best_txs.skip_blobs();
        //     }
        // }

        let gas_used = result.gas_used();

        // add gas used by the transaction to cumulative gas used, before creating the receipt
        cumulative_gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        #[allow(clippy::needless_update)] // side-effect of optimism fields
        receipts.push(Some(Receipt {
            tx_type: tx.tx_type(),
            success: result.is_success(),
            cumulative_gas_used,
            logs: result.into_logs().into_iter().map(Into::into).collect(),
            ..Default::default()
        }));

        // update add to total fees
        let miner_fee = tx
            .effective_tip_per_gas(Some(base_fee))
            .expect("fee is always valid; execution succeeded");
        total_fees += U256::from(miner_fee) * U256::from(gas_used);

        // append transaction to the list of executed transactions
        senders.push(tx.signer());
        executed_txs.push(tx.into_signed());
    }

    // // calculate the requests and the requests root
    // let (requests, requests_root) = if chain_spec
    //     .is_prague_active_at_timestamp(attributes.timestamp)
    // {
    //     let deposit_requests = parse_deposits_from_receipts(&chain_spec, receipts.iter().flatten())
    //         .map_err(|err| BlockBuilderError::Reth(RethError::Execution(err.into())))?;
    //     let withdrawal_requests = post_block_withdrawal_requests_contract_call(
    //         &evm_config,
    //         &mut db,
    //         &initialized_cfg,
    //         &initialized_block_env,
    //     )
    //     .map_err(|err| BlockBuilderError::Reth(err.into()))?;

    //     let requests = [deposit_requests, withdrawal_requests].concat();
    //     let requests_root = calculate_requests_root(&requests);
    //     (Some(requests.into()), Some(requests_root))
    // } else {
    //     (None, None)
    // };

    // TODO: logic for withdrawals
    //
    // let WithdrawalsOutcome { withdrawals_root, withdrawals } =
    //     commit_withdrawals(&mut db, &chain_spec, attributes.timestamp, attributes.withdrawals)?;

    // merge all transitions into bundle state, this would apply the withdrawal balance changes
    // and 4788 contract call
    db.merge_transitions(BundleRetention::PlainState);

    // TODO: use this and return WorkerBlockUpdate
    //
    let execution_outcome =
        ExecutionOutcome::new(db.take_bundle(), vec![receipts].into(), block_number, vec![]);
    let receipts_root =
        execution_outcome.receipts_root_slow(block_number).expect("Number is in range");
    let logs_bloom = execution_outcome.block_logs_bloom(block_number).expect("Number is in range");

    // calculate the state root
    let state_root = {
        let state_provider = db.database.0.inner.borrow_mut();
        state_provider.db.state_root(execution_outcome.state())?
    };

    // create the block header
    let transactions_root = proofs::calculate_transaction_root(&executed_txs);

    // // initialize empty blob sidecars at first. If cancun is active then this will
    // let mut blob_sidecars = Vec::new();
    let excess_blob_gas = None;
    let blob_gas_used = None;

    // // only determine cancun fields when active
    // if chain_spec.is_cancun_active_at_timestamp(attributes.timestamp) {
    //     // grab the blob sidecars from the executed txs
    //     blob_sidecars = pool.get_all_blobs_exact(
    //         executed_txs.iter().filter(|tx| tx.is_eip4844()).map(|tx| tx.hash).collect(),
    //     )?;

    //     excess_blob_gas = if chain_spec.is_cancun_active_at_timestamp(parent.timestamp) {
    //         let parent_excess_blob_gas = parent.excess_blob_gas.unwrap_or_default();
    //         let parent_blob_gas_used = parent.blob_gas_used.unwrap_or_default();
    //         Some(calculate_excess_blob_gas(parent_excess_blob_gas, parent_blob_gas_used))
    //     } else {
    //         // for the first post-fork block, both parent.blob_gas_used and
    //         // parent.excess_blob_gas are evaluated as 0
    //         Some(calculate_excess_blob_gas(0, 0))
    //     };

    //     blob_gas_used = Some(sum_blob_gas_used);
    // }

    let mut header = Header {
        parent_hash: parent.hash(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary,
        state_root,
        transactions_root,
        receipts_root,
        withdrawals_root: Some(EMPTY_WITHDRAWALS),
        logs_bloom,
        timestamp: now(),
        mix_hash: Default::default(),
        nonce: 0,
        base_fee_per_gas: Some(base_fee),
        number: parent.number + 1,
        gas_limit: block_gas_limit,
        difficulty: U256::ZERO,
        gas_used: cumulative_gas_used,
        extra_data: Default::default(),
        parent_beacon_block_root: None,
        blob_gas_used,
        excess_blob_gas,
        requests_root: None,
    };

    // TODO: this is easy to manipulate
    //
    // calculate mix hash as a source of randomness
    // - consensus output digest from parent (beacon block root)
    // - timestamp
    //
    // see https://eips.ethereum.org/EIPS/eip-4399
    //
    // idea;
    // - use peer proposed block digest as prevrandao
    // - logic to check this prevrandao vs timestamp of vote for first peer
    //   in the new round that this worker signed?
    //      - what if this is the only peer with transactions/block to propose?
    //
    // For now: this provides sufficent randomness for on-chain security,
    // but requires trust in the validator node operator
    if let Some(root) = parent.parent_beacon_block_root {
        header.mix_hash =
            keccak256([root.as_slice(), header.timestamp.to_le_bytes().as_slice()].concat());
    }

    // TODO: is there a better way?
    //
    // sometimes worker block are produced too quickly (<1s)
    // resulting in batch timestamp == parent timestamp
    if header.timestamp == parent.timestamp {
        warn!(target: "execution::batch_maker", "header template timestamp same as parent");
        header.timestamp = parent.timestamp + 1;
    }

    let withdrawals = Some(Withdrawals::new(vec![]));
    let requests = None;

    // seal the block
    let block = Block { header, body: executed_txs, ommers: vec![], withdrawals, requests };

    let sealed_block = block.seal_slow();
    debug!(target: "block_builder", ?sealed_block, "sealed built block");

    // create SealedBlockWithSenders for worker update
    let pending = SealedBlockWithSenders::new(sealed_block, senders)
        .ok_or(BlockBuilderError::SealBlockWithSenders)?;

    // extend the payload with the blob sidecars from the executed txs
    // payload.extend_sidecars(blob_sidecars);

    // TODO: add total_fees to worker metrics
    let build = WorkerBlockUpdate::new(parent, pending, execution_outcome);

    Ok(build)
}
