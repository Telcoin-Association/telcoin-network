//! Payload builder function for extending the canonical tip using the output from consensus.
//!
//! This approach heavily inspired by reth's `default_ethereum_payload_builder`.

use crate::error::{EngineResult, TnEngineError};
use alloy::{
    constants::{EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
    Block, Header, Receipt, SealedBlockWithSenders, SealedHeader, Withdrawals, B256,
    EMPTY_OMMER_ROOT_HASH, U256,
};
use fastcrypto::hash::Hash as _;
use reth_blockchain_tree::{BlockValidationKind, BlockchainTreeEngine};
use reth_chainspec::ChainSpec;
use reth_evm::ConfigureEvm;
use reth_execution_types::ExecutionOutcome;
use reth_payload_builder::database::CachedReads;
use reth_provider::{CanonChainTracker, ChainSpecProvider, StateProviderFactory};
use reth_revm::{
    database::StateProviderDatabase,
    db::states::bundle_state::BundleRetention,
    primitives::{EVMError, EnvWithHandlerCfg, ResultAndState},
    DatabaseCommit, State,
};
use reth_trie::HashedPostState;
use std::sync::Arc;
use tn_types::{max_batch_gas, Batch, BuildArguments, TNPayload, TNPayloadAttributes};
use tracing::{debug, error, info, warn};

/// Execute output from consensus to extend the canonical chain.
///
/// The function handles all types of output, included multiple blocks and empty blocks.
#[inline]
pub fn execute_consensus_output<EvmConfig, Provider>(
    evm_config: EvmConfig,
    args: BuildArguments<Provider>,
) -> EngineResult<SealedHeader>
where
    EvmConfig: ConfigureEvm,
    Provider: StateProviderFactory
        + ChainSpecProvider<ChainSpec = ChainSpec>
        + BlockchainTreeEngine
        + CanonChainTracker,
{
    let BuildArguments { provider, mut output, parent_header } = args;
    debug!(target: "engine", ?output, "executing output");
    // TODO: create "sealed consensus" type that contains hash and output
    //
    // create ommers while converting Batch to SealedBlockWithSenders

    // capture values from consensus output for full execution
    let output_digest: B256 = output.digest().into();
    let batches = output.flatten_batches();

    // assert vecs match
    debug_assert_eq!(
        batches.len(),
        output.batch_digests.len(),
        "uneven number of sealed blocks from batches and batch digests"
    );

    // rename canonical header for clarity
    let mut canonical_header = parent_header;

    // extend canonical tip if output contains batches with transactions
    // otherwise execute an empty block to extend canonical tip
    if batches.is_empty() {
        // execute single block with no transactions
        //
        // use parent values for next block (these values would come from the worker's block)
        let base_fee_per_gas = canonical_header.base_fee_per_gas.unwrap_or_default();
        let gas_limit = canonical_header.gas_limit;

        // empty withdrawals
        let withdrawals = Withdrawals::new(vec![]);
        let payload_attributes = TNPayloadAttributes::new(
            canonical_header,
            0,
            B256::ZERO, // no batch to digest
            &output,
            output_digest,
            base_fee_per_gas,
            gas_limit,
            output_digest, // use output digest for mix hash
            withdrawals,
        );
        let payload = TNPayload::new(payload_attributes);

        // execute
        let next_canonical_block = build_block_from_empty_payload(
            payload,
            &provider,
            provider.chain_spec(),
            output.consensus_header_hash(),
        )?;

        debug!(target: "engine", ?next_canonical_block, "empty block");

        // update header for next block execution in loop
        canonical_header = next_canonical_block.header.clone();

        // add block to the tree and skip state root validation
        provider
            .insert_block(next_canonical_block, BlockValidationKind::SkipStateRootValidation).inspect_err(|e| {
                error!(target: "engine", header=?canonical_header, ?e, "failed to insert next canonical block");
            })?;
    } else {
        // loop and construct blocks with transactions
        for (block_index, block) in batches.into_iter().enumerate() {
            let batch_digest =
                output.next_batch_digest().ok_or(TnEngineError::NextBlockDigestMissing)?;
            // use batch's base fee, gas limit, and withdrawals
            let base_fee_per_gas = block.base_fee_per_gas.unwrap_or_default();
            let gas_limit = max_batch_gas(block.timestamp);

            // apply XOR bitwise operator with worker's digest to ensure unique mixed hash per block
            // for round
            let mix_hash = output_digest ^ block.digest();
            let withdrawals = Withdrawals::new(vec![]);
            let payload_attributes = TNPayloadAttributes::new(
                canonical_header,
                block_index as u64,
                batch_digest,
                &output,
                output_digest,
                base_fee_per_gas,
                gas_limit,
                mix_hash,
                withdrawals,
            );
            let payload = TNPayload::new(payload_attributes);

            // execute
            let next_canonical_block = build_block_from_batch_payload(
                &evm_config,
                payload,
                &provider,
                provider.chain_spec(),
                block,
                output.consensus_header_hash(),
            )?;

            debug!(target: "engine", ?next_canonical_block, "worker's block executed");

            // update header for next block execution in loop
            canonical_header = next_canonical_block.header.clone();

            // add block to the tree and skip state root validation
            provider
                .insert_block(next_canonical_block, BlockValidationKind::SkipStateRootValidation).inspect_err(|e| {
                    error!(target: "engine", header=?canonical_header, ?e, "failed to insert next canonical block");
                })?;
        }
    } // end block execution for round

    // broadcast new base_fee after executing round
    //
    // ensure this value is updated before making the round canonical
    // because pool maintenance task needs the protocol's new base fee
    // before it can accurately process the canon_state_notification update
    //
    // TODO: actually impl this
    // basefee_watch.send(new_base_fee);

    // NOTE: this makes all blocks canonical, commits them to the database,
    // and broadcasts new chain on `canon_state_notification_sender`
    //
    // the canon_state_notifications include every block executed in this round
    //
    // the worker's pool maintenance task subcribes to these events
    provider.make_canonical(canonical_header.hash())?;

    // set last executed header as the tracked header
    //
    // see: reth/crates/consensus/beacon/src/engine/mod.rs:update_canon_chain
    provider.set_canonical_head(canonical_header.clone());
    info!(target: "engine", "canonical head for round {:?}: {:?} - {:?}", canonical_header.nonce, canonical_header.number, canonical_header.hash());

    // finalize the last block executed from consensus output and update chain info
    //
    // this removes canonical blocks from the tree, stores the finalized block number in the
    // database, but still need to set_finalized afterwards for utilization in-memory for
    // components, like RPC
    provider.finalize_block(canonical_header.number)?;
    provider.set_finalized(canonical_header.clone());

    // update safe block last because this is less time sensitive but still needs to happen
    provider.set_safe(canonical_header.clone());

    // return new canonical header for next engine task
    Ok(canonical_header)
}

/// Construct a canonical block from a worker's block that reached consensus.
#[inline]
fn build_block_from_batch_payload<EvmConfig, Provider>(
    evm_config: &EvmConfig,
    payload: TNPayload,
    provider: &Provider,
    chain_spec: Arc<ChainSpec>,
    batch: Batch,
    consensus_header_hash: B256,
) -> EngineResult<SealedBlockWithSenders>
where
    EvmConfig: ConfigureEvm,
    Provider: StateProviderFactory,
{
    let state_provider = provider.state_by_block_hash(payload.attributes.parent_header.hash())?;
    let state = StateProviderDatabase::new(state_provider);

    // TODO: using same apprach as reth here bc I can't find the State::builder()'s methods
    // I'm not sure what `with_bundle_update` does, and using `CachedReads` is the only way
    // I can get the state root section below to compile using `db.commit(state)`.
    //
    // TODO: create `CachedReads` during batch validation?
    let mut cached_reads = CachedReads::default();
    let mut db =
        State::builder().with_database_ref(cached_reads.as_db(state)).with_bundle_update().build();

    debug!(target: "payload_builder", parent_hash = ?payload.attributes.parent_header.hash(), parent_number = payload.attributes.parent_header.number, "building new payload");
    // collect these totals to report at the end
    let _total_gas_used = 0; // TODO: include blobs
    let mut cumulative_gas_used = 0;
    let mut total_fees = U256::ZERO;
    let mut executed_txs = Vec::new();
    let mut senders = Vec::new();
    let mut receipts = Vec::new();

    // initialize values for execution from block env
    //
    // note: uses the worker's sealed header for "parent" values
    // note the sealed header below is more or less junk but payload trait requires it.
    let (cfg, block_env) = payload.cfg_and_block_env(chain_spec.as_ref());

    // TODO: better to get these from payload attributes?
    // - more efficient, but harder to maintain?
    let block_gas_limit: u64 = block_env.gas_limit.try_into().unwrap_or(u64::MAX);
    let base_fee = block_env.basefee.to::<u64>();
    let block_number = block_env.number.to::<u64>();

    // // apply eip-4788 pre block contract call
    // pre_block_beacon_root_contract_call(
    //     &mut db,
    //     &chain_spec,
    //     block_number,
    //     &initialized_cfg,
    //     &initialized_block_env,
    //     &attributes,
    // )?;

    // // TODO: TN needs to support this
    // // apply eip-2935 blockhashes update
    // apply_blockhashes_update(
    //     &mut db,
    //     &chain_spec,
    //     initialized_block_env.timestamp.to::<u64>(),
    //     block_number,
    //     parent_header.hash(),
    // )
    // .map_err(|err| PayloadBuilderError::Internal(err.into()))?;

    // TODO: parallelize tx recovery when it's worth it (see TransactionSigned::recover_signers())

    for tx in batch.transactions {
        //txs {
        // // TODO: support blob gas with cancun genesis hardfork
        // //
        // // There's only limited amount of blob space available per block, so we need to check if
        // // the EIP-4844 can still fit in the block
        // //
        // // note: this should never be a problem
        // if let Some(blob_tx) = tx.as_eip4844() {
        //     let tx_blob_gas = blob_tx.blob_gas();
        //     if sum_blob_gas_used + tx_blob_gas > MAX_DATA_GAS_PER_BLOCK {
        //         // this should never happen and is considered an error
        //         error!(target: "payload_builder", tx=?tx.hash, ?sum_blob_gas_used, ?tx_blob_gas,
        // "skipping blob transaction because it would exceed the max data gas per block");
        //         // TODO: should this break the process?
        //         continue;
        //     }
        // }

        let tx = if let Some(signer) = tx.recover_signer() {
            tx.with_signer(signer)
        } else {
            error!(target: "engine", "Could not recover signer for {tx:?}");
            return Err(TnEngineError::MissingSigner);
        };
        let env = EnvWithHandlerCfg::new_with_cfg_env(
            cfg.clone(),
            block_env.clone(),
            evm_config.tx_env(&tx),
        );

        // Configure the environment for the block.
        let mut evm = evm_config.evm_with_env(&mut db, env);

        let ResultAndState { result, state } = match evm.transact() {
            Ok(res) => res,
            Err(err) => {
                match err {
                    // allow transaction errors (ie - duplicates)
                    //
                    // it's possible that another worker's batch included this transaction
                    EVMError::Transaction(err) => {
                        warn!(target: "engine", tx_hash=?tx.hash(), ?err);

                        // TODO: collect metrics here

                        continue;
                    }
                    err => {
                        // this is an error that we should treat as fatal
                        // - invalid header resulting from misconfigured BlockEnv
                        // - Database error
                        // - custom error (unsure)
                        return Err(err.into());
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

        //     // TODO: this is important for worker's batch payload builder
        //     // // if we've reached the max data gas per block, we can skip blob txs entirely
        //     // if sum_blob_gas_used == MAX_DATA_GAS_PER_BLOCK {
        //     //     best_txs.skip_blobs();
        //     // }
        // }

        let gas_used = result.gas_used();

        // add gas used by the transaction to cumulative gas used, before creating the receipt
        cumulative_gas_used += gas_used;

        // Push transaction changeset and calculate header bloom filter for receipt.
        receipts.push(Some(Receipt {
            tx_type: tx.tx_type(),
            success: result.is_success(),
            cumulative_gas_used,
            logs: result.into_logs().into_iter().collect(),
        }));

        // update add to total fees
        let miner_fee = tx
            .effective_tip_per_gas(Some(base_fee))
            .expect("fee is always valid; execution succeeded");
        total_fees += U256::from(miner_fee) * U256::from(gas_used);

        // append transaction to the list of executed transactions and keep signers
        senders.push(tx.signer());
        executed_txs.push(tx.into_signed());
    }

    // TODO: logic for withdrawals
    //
    // let WithdrawalsOutcome { withdrawals_root, withdrawals } =
    //     commit_withdrawals(&mut db, &chain_spec, attributes.timestamp, attributes.withdrawals)?;

    // merge all transitions into bundle state, this would apply the withdrawal balance changes
    // and 4788 contract call
    db.merge_transitions(BundleRetention::PlainState);

    let execution_outcome = ExecutionOutcome::new(
        db.take_bundle(),
        vec![receipts].into(),
        block_number,
        vec![], // TODO: support requests
    );
    let receipts_root =
        execution_outcome.receipts_root_slow(block_number).expect("Number is in range");
    let logs_bloom = execution_outcome.block_logs_bloom(block_number).expect("Number is in range");
    let hashed_state = HashedPostState::from_bundle_state(&execution_outcome.state().state);

    // calculate the state root
    let state_root = {
        let state_provider = db.database.0.inner.borrow_mut();
        state_provider.db.state_root(hashed_state)?
    };

    // create the block header
    let transactions_root = reth_primitives::proofs::calculate_transaction_root(&executed_txs);

    // // initialize empty blob sidecars at first. If cancun is active then this will
    // let mut blob_sidecars = Vec::new();
    // let mut excess_blob_gas = None;
    // let mut blob_gas_used = None;

    // // only determine cancun fields when active
    // if chain_spec.is_cancun_active_at_timestamp(payload.attributes.timestamp) {
    //     // grab the blob sidecars from the executed txs
    //     blob_sidecars = pool.get_all_blobs_exact(
    //         executed_txs.iter().filter(|tx| tx.is_eip4844()).map(|tx| tx.hash).collect(),
    //     )?;

    //     excess_blob_gas = if chain_spec.is_cancun_active_at_timestamp(parent_header.timestamp) {
    //         let parent_excess_blob_gas = parent_header.excess_blob_gas.unwrap_or_default();
    //         let parent_blob_gas_used = parent_header.blob_gas_used.unwrap_or_default();
    //         Some(calculate_excess_blob_gas(parent_excess_blob_gas, parent_blob_gas_used))
    //     } else {
    //         // for the first post-fork block, both parent.blob_gas_used and
    //         // parent.excess_blob_gas are evaluated as 0
    //         Some(calculate_excess_blob_gas(0, 0))
    //     };

    //     blob_gas_used = Some(sum_blob_gas_used);
    // }

    let header = Header {
        parent_hash: payload.parent(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: block_env.coinbase,
        state_root,
        transactions_root,
        receipts_root,
        withdrawals_root: Some(EMPTY_WITHDRAWALS),
        logs_bloom,
        timestamp: payload.timestamp(),
        mix_hash: payload.prev_randao(),
        nonce: payload.attributes.nonce,
        base_fee_per_gas: Some(base_fee),
        number: payload.attributes.parent_header.number + 1, // ensure this matches the block env
        gas_limit: block_gas_limit,
        difficulty: U256::from(payload.attributes.batch_index),
        gas_used: cumulative_gas_used,
        extra_data: payload.attributes.batch_digest.into(),
        parent_beacon_block_root: Some(consensus_header_hash),
        blob_gas_used: None,   // TODO: support blobs
        excess_blob_gas: None, // TODO: support blobs
        requests_root: None,
    };

    // seal the block
    let withdrawals = Some(payload.withdrawals().clone());
    let block = Block { header, body: executed_txs, ommers: vec![], withdrawals, requests: None };

    let sealed_block = block.seal_slow();
    let sealed_block_with_senders = SealedBlockWithSenders::new(sealed_block, senders)
        .ok_or(TnEngineError::SealBlockWithSenders)?;

    Ok(sealed_block_with_senders)
}

/// Extend the canonical tip with one block, despite no blocks from workers are included in the
/// output from consensus.
#[inline]
fn build_block_from_empty_payload<Provider>(
    payload: TNPayload,
    provider: &Provider,
    chain_spec: Arc<ChainSpec>,
    consensus_header_digest: B256,
) -> EngineResult<SealedBlockWithSenders>
where
    Provider: StateProviderFactory,
{
    let state =
        provider.state_by_block_hash(payload.attributes.parent_header.hash()).map_err(|err| {
            warn!(target: "engine",
                parent_hash=%payload.attributes.parent_header.hash(),
                %err,
                "failed to get state for empty output",
            );
            err
        })?;

    let mut db = State::builder()
        .with_database(StateProviderDatabase::new(state))
        .with_bundle_update()
        .build();

    // initialize values for execution from block env
    //
    // use the parent's header bc there are no batches and the header arg is not used
    let (_cfg, block_env) = payload.cfg_and_block_env(chain_spec.as_ref());

    // merge all transitions into bundle state, this would apply the withdrawal balance
    // changes and 4788 contract call
    db.merge_transitions(BundleRetention::PlainState);

    // calculate the state root
    let bundle_state = db.take_bundle();
    let hashed_state = HashedPostState::from_bundle_state(&bundle_state.state);
    let state_root = db.database.state_root(hashed_state).map_err(|err| {
        warn!(target: "engine",
            parent_hash=%payload.attributes.parent_header.hash(),
            %err,
            "failed to calculate state root for empty output"
        );
        err
    })?;

    let header = Header {
        parent_hash: payload.parent(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: block_env.coinbase,
        state_root,
        transactions_root: EMPTY_TRANSACTIONS,
        receipts_root: EMPTY_RECEIPTS,
        withdrawals_root: Some(EMPTY_WITHDRAWALS),
        logs_bloom: Default::default(),
        timestamp: payload.timestamp(),
        mix_hash: payload.prev_randao(),
        nonce: payload.attributes.nonce,
        base_fee_per_gas: Some(payload.attributes.base_fee_per_gas),
        number: payload.attributes.parent_header.number + 1, // ensure this matches the block env
        gas_limit: payload.attributes.gas_limit,
        difficulty: U256::ZERO, // batch index
        gas_used: 0,
        extra_data: payload.attributes.batch_digest.into(),
        parent_beacon_block_root: Some(consensus_header_digest),
        blob_gas_used: None,   // TODO: support blobs
        excess_blob_gas: None, // TODO: support blobs
        requests_root: None,
    };

    // seal the block
    let withdrawals = Some(payload.withdrawals().clone());
    let block = Block { header, body: vec![], ommers: vec![], withdrawals, requests: None };

    let sealed_block = block.seal_slow();

    let sealed_block_with_senders = SealedBlockWithSenders::new(sealed_block, vec![])
        .ok_or(TnEngineError::SealBlockWithSenders)?;

    Ok(sealed_block_with_senders)
}
