//! Payload builder function for extending the canonical tip using the output from consensus.
//!
//! This approach heavily inspired by reth's `default_ethereum_payload_builder`.

use crate::error::{EngineResult, TnEngineError};
use tn_reth::{
    payload::{BuildArguments, TNPayload},
    RethEnv,
};
use tn_types::{max_batch_gas, ConsensusOutput, Hash as _, SealedHeader, B256};
use tracing::{debug, error};

fn finalize_signed_blocks(
    reth_env: &RethEnv,
    output: &ConsensusOutput,
    canonical_header: &SealedHeader,
) -> EngineResult<()> {
    let mut last_executed = output.sub_dag.leader.header.latest_execution_block;

    // Find the latest block that was signed off by the committee.
    for cert in &output.sub_dag.certificates {
        if cert.header.latest_execution_block.number > last_executed.number {
            last_executed = cert.header.latest_execution_block;
        }
    }

    if last_executed.number <= canonical_header.number {
        if let Some(block) = reth_env.sealed_header_by_hash(last_executed.hash)? {
            // finalize the last block from a cert in consensus output and update chain info
            //
            // this removes canonical blocks from the tree, stores the finalized block number in the
            // database, but still need to set_finalized afterwards for utilization in-memory for
            // components, like RPC
            reth_env.finalize_block(block)?;
        } else {
            error!(target: "engine", ?output, "missing the block to finalize!");
            return Err(TnEngineError::MissingFinalBlock);
        }
    }

    Ok(())
}

/// Execute output from consensus to extend the canonical chain.
///
/// The function handles all types of output, included multiple blocks and empty blocks.
pub fn execute_consensus_output(args: BuildArguments) -> EngineResult<SealedHeader> {
    let BuildArguments { reth_env, mut output, parent_header } = args;
    debug!(target: "engine", ?output, "executing output");

    // output digest returns the `ConsensusHeader` digest
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
    let mut executed_blocks = Vec::with_capacity(batches.len());

    //
    // /
    // TODO
    //
    //
    // ReFactor this before merge - too many arguments for TNPayload
    // - logic for mix hash, output digest, etc. should be in TNPayload::new

    // extend canonical tip if output contains batches with transactions
    // otherwise execute an empty block to extend canonical tip
    if batches.is_empty() {
        // execute single block with no transactions
        //
        // use parent values for next block (these values would come from the worker's block)
        let base_fee_per_gas = canonical_header.base_fee_per_gas.unwrap_or_default();
        let gas_limit = canonical_header.gas_limit;

        let payload = TNPayload::new(
            canonical_header,
            0,
            B256::ZERO, // no batch to digest
            &output,
            output_digest,
            base_fee_per_gas,
            gas_limit,
            output_digest, // use output digest for mix hash
        );

        // execute
        let next_canonical_block = if payload.close_epoch.is_none() {
            reth_env.build_block_from_empty_payload(payload, output.consensus_header_hash())?
        } else {
            // pass empty transactions and use logic to add receipts for closing epoch
            reth_env.build_block_from_batch_payload(payload, vec![])?
        };

        debug!(target: "engine", ?next_canonical_block, "empty block");

        // update header for next block execution in loop
        canonical_header = next_canonical_block.recovered_block.sealed_header().clone();

        // // add block to the tree and skip state root validation
        // reth_env.insert_block(next_canonical_block).inspect_err(|e| {
        //     error!(target: "engine", header=?canonical_header, ?e, "failed to insert next canonical block");
        // })?;

        // collect all executed blocks for this output
        executed_blocks.push(next_canonical_block);
    } else {
        // loop and construct blocks with transactions
        for (batch_index, batch) in batches.into_iter().enumerate() {
            let batch_digest =
                output.next_batch_digest().ok_or(TnEngineError::NextBlockDigestMissing)?;
            // use batch's base fee, gas limit, and withdrawals
            let base_fee_per_gas = batch.base_fee_per_gas.unwrap_or_default();
            let gas_limit = max_batch_gas(batch.timestamp);

            // apply XOR bitwise operator with worker's digest to ensure unique mixed hash per batch
            // for round
            let mix_hash = output_digest ^ batch.digest();
            let payload = TNPayload::new(
                canonical_header,
                batch_index,
                batch_digest,
                &output,
                output_digest,
                base_fee_per_gas,
                gas_limit,
                mix_hash,
            );

            // execute
            let next_canonical_block =
                reth_env.build_block_from_batch_payload(payload, batch.transactions)?;

            // tree flow:
            // - try receive eng message
            //      - set_pending_block
            //      - insert_executed
            // - store to disk
            //      - collect and store in group
            //          - see Persistence::on_save_block
            //
            // TODO:
            //  - check BeaconConsensusEngineEvents to ensure nothing missed
            //  - still need to broadcast canonical update
            //  - ensure read/write access isn't a problem

            debug!(target: "engine", ?next_canonical_block, "worker's block executed");

            // update header for next block execution in loop
            canonical_header = next_canonical_block.recovered_block.sealed_header().clone();

            // // add block to the tree and skip state root validation
            // reth_env.insert_block(next_canonical_block).inspect_err(|e| {
            //     error!(target: "engine", header=?canonical_header, ?e, "failed to insert next canonical block");
            // })?;

            // collect all executed blocks for this output
            executed_blocks.push(next_canonical_block);
        }
    } // end block execution for round

    // broadcast new base_fee after executing round
    //
    // ensure this value is updated before making the round canonical
    // because pool maintenance task needs the protocol's new base fee
    // before it can accurately process the canon_state_notification update

    // NOTE: this makes all blocks canonical, commits them to the database,
    // and broadcasts new chain on `canon_state_notification_sender`
    //
    // the canon_state_notifications include every block executed in this round
    //
    // the worker's pool maintenance task subcribes to these events
    // reth_env.make_canonical(canonical_header.clone())?;
    reth_env.finish_executing_output(executed_blocks)?;

    if output.early_finalize {
        // finalize the last block executed from consensus output and update chain info
        //
        // this removes canonical blocks from the tree, stores the finalized block number in the
        // database, but still need to set_finalized afterwards for utilization in-memory for
        // components, like RPC
        reth_env.finalize_block(canonical_header.clone())?;
    } else {
        finalize_signed_blocks(&reth_env, &output, &canonical_header)?;
    }

    // return new canonical header for next engine task
    Ok(canonical_header)
}
