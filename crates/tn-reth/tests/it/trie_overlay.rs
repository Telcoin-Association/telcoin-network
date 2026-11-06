//! Differential tests for the #1301 output-scoped trie overlay.
//!
//! Consensus-critical invariant: TN's layered state-root path
//! (`OutputTrieOverlay::layered_root_with_updates` - current block's sorted deltas
//! over the accumulated output overlay over the database) must produce EXACTLY the
//! root and trie updates reth itself computes. Two oracles pin it:
//!
//! - trie level: reth's `StateRoot::overlay_root_from_nodes_with_updates` over the
//!   `prepend_self`-merged `TrieInput` (the merge reth's memory-overlay provider performs per
//!   block), driven over seeded synthetic delta sequences;
//! - pipeline level: reth's own memory-overlay provider root
//!   (`state_by_block_hash(parent).state_root_with_updates(hashed)`) for blocks built and chained
//!   in memory WITHOUT persisting between them - the true production shape of a multi-block
//!   `ConsensusOutput`, which no other test in the tree exercises.

use crate::pipeline_helpers::{
    consensus_output_for_test, consensus_output_with_batches, PipelineTestEnv,
};
use reth_chain_state::ComputedTrieData;
use reth_provider::{DBProvider as _, HashedPostStateProvider as _, StateRootProvider as _};
use reth_trie::{HashedPostState, HashedStorage, StateRoot, TrieInput, TrieInputSorted};
use reth_trie_db::DatabaseStateRoot as _;
use secp256k1::rand::{rngs::StdRng, RngCore as _, SeedableRng as _};
use std::{sync::Arc, time::Duration};
use tn_reth::{payload::TNPayload, ExecutedBlock, NewCanonicalChain, OutputTrieOverlay, RethEnv};
use tn_types::{Account, Address, Bytes, TransactionTrait as _, B256, MIN_PROTOCOL_BASE_FEE, U256};

/// Draw a deterministic 32-byte key from the seeded rng.
fn rand_b256(rng: &mut StdRng) -> B256 {
    let mut buf = [0u8; 32];
    rng.fill_bytes(&mut buf);
    B256::from(buf)
}

/// Draw a deterministic account leaf (EOA shape: no bytecode hash) from the seeded rng.
fn rand_account(rng: &mut StdRng) -> Account {
    Account { nonce: rng.next_u64(), balance: U256::from(rng.next_u64()), bytecode_hash: None }
}

/// Draw a deterministic contract account leaf (with a bytecode hash) from the seeded rng.
fn rand_contract(rng: &mut StdRng) -> Account {
    Account {
        nonce: rng.next_u64(),
        balance: U256::from(rng.next_u64()),
        bytecode_hash: Some(rand_b256(rng)),
    }
}

/// Build the scripted, seeded sequence of per-block `HashedPostState` deltas.
///
/// Covers every hazard class the layered-cursor composition must shadow correctly:
/// overlapping keys across blocks, destroyed accounts (`None` tombstones), storage
/// wiped in one block and re-created in a later one, wipe-and-set within a single
/// delta, an empty delta, and zeroing every slot without an explicit wipe.
fn seeded_deltas(rng: &mut StdRng) -> Vec<HashedPostState> {
    let a0 = rand_b256(rng);
    let a1 = rand_b256(rng);
    let a2 = rand_b256(rng);
    let c0 = rand_b256(rng);
    let c1 = rand_b256(rng);
    let s0 = rand_b256(rng);
    let s1 = rand_b256(rng);
    let s2 = rand_b256(rng);

    let v = |rng: &mut StdRng| U256::from(rng.next_u64());

    vec![
        // block 0: create EOAs and both contracts with initial storage
        HashedPostState::default()
            .with_accounts([
                (a0, Some(rand_account(rng))),
                (a1, Some(rand_account(rng))),
                (c0, Some(rand_contract(rng))),
                (c1, Some(rand_contract(rng))),
            ])
            .with_storages([
                (c0, HashedStorage::from_iter(false, [(s0, v(rng)), (s1, v(rng))])),
                (c1, HashedStorage::from_iter(false, [(s0, v(rng))])),
            ]),
        // block 1: overlapping updates (a0, c0/s0) plus a new EOA
        HashedPostState::default()
            .with_accounts([(a0, Some(rand_account(rng))), (a2, Some(rand_account(rng)))])
            .with_storages([(c0, HashedStorage::from_iter(false, [(s0, v(rng))]))]),
        // block 2: destroy a1 and c0 (account tombstones + storage wipe)
        HashedPostState::default()
            .with_accounts([(a1, None), (c0, None)])
            .with_storages([(c0, HashedStorage::from_iter(true, std::iter::empty()))]),
        // block 3: empty delta (a block whose transactions net to no state change)
        HashedPostState::default(),
        // block 4: re-create c0 with DIFFERENT storage than before the wipe
        HashedPostState::default()
            .with_accounts([(c0, Some(rand_contract(rng))), (a0, Some(rand_account(rng)))])
            .with_storages([(c0, HashedStorage::from_iter(false, [(s1, v(rng)), (s2, v(rng))]))]),
        // block 5: wipe-and-set in ONE delta on c1
        HashedPostState::default()
            .with_accounts([(c1, Some(rand_contract(rng)))])
            .with_storages([(c1, HashedStorage::from_iter(true, [(s2, v(rng))]))]),
        // block 6: churn every account key at once
        HashedPostState::default()
            .with_accounts([
                (a0, Some(rand_account(rng))),
                (a1, Some(rand_account(rng))),
                (a2, Some(rand_account(rng))),
            ])
            .with_storages([
                (c0, HashedStorage::from_iter(false, [(s0, v(rng))])),
                (c1, HashedStorage::from_iter(false, [(s0, v(rng)), (s1, v(rng))])),
            ]),
        // block 7: destroy a2, touch a1 again
        HashedPostState::default().with_accounts([(a2, None), (a1, Some(rand_account(rng)))]),
        // block 8: zero all c1 slots while older non-zero writes remain in the run stack.
        HashedPostState::default().with_storages([(
            c1,
            HashedStorage::from_iter(false, [(s0, U256::ZERO), (s1, U256::ZERO), (s2, U256::ZERO)]),
        )]),
    ]
}

/// Trie-level differential: per block, the layered-cursor root and updates equal
/// reth's `overlay_root_from_nodes_with_updates` over the `prepend_self`-merged
/// `TrieInput` built from the same deltas (#1301).
/// Includes interleaving distinct keys across carries at 8, 16, 32, and 64 blocks.
#[test]
fn test_layered_overlay_root_matches_merged_trie_input_oracle() -> eyre::Result<()> {
    let env = PipelineTestEnv::new();
    let provider = env.reth_env.database_provider_ro_for_test()?;
    let tx = provider.tx_ref();

    let mut rng = StdRng::seed_from_u64(0x1301);
    let deltas = (0..8)
        .flat_map(|_| seeded_deltas(&mut rng))
        .chain(std::iter::once(HashedPostState::default()));

    deltas.enumerate().try_fold(
        (OutputTrieOverlay::new(), TrieInput::default()),
        |(mut overlay, mut oracle_input),
         (i, delta)|
         -> eyre::Result<(OutputTrieOverlay, TrieInput)> {
            // new path: current delta layered over the accumulated overlay over the db
            let (new_root, new_updates) = overlay.layered_root_with_updates(tx, delta.clone())?;

            // oracle: reth's own merge - prepend the accumulated input, sort, walk
            let mut input = TrieInput::from_state(delta.clone());
            input.prepend_self(oracle_input.clone());
            let (oracle_root, oracle_updates) = StateRoot::overlay_root_from_nodes_with_updates(
                tx,
                TrieInputSorted::from_unsorted(input),
            )?;

            assert_eq!(new_root, oracle_root, "state root diverged at block {i}");
            assert_eq!(new_updates, oracle_updates, "trie updates diverged at block {i}");

            // advance both accumulations exactly as production does: the block's own
            // sorted deltas plus the updates its root computation produced
            let sorted_state = Arc::new(delta.into_sorted());
            let sorted_nodes = Arc::new(new_updates.into_sorted());
            overlay.extend_from_block(&ComputedTrieData::without_trie_input(
                sorted_state.clone(),
                sorted_nodes.clone(),
            ));
            oracle_input.nodes.extend_from_sorted(&sorted_nodes);
            oracle_input.state.extend_from_sorted(&sorted_state);
            Ok((overlay, oracle_input))
        },
    )?;
    Ok(())
}

/// Pipeline differential in the PRODUCTION shape: three blocks built and chained in
/// memory WITHOUT persisting between them (`update_chain` + `set_canonical_head`
/// only), each block's new-path state root asserted equal to the root reth's
/// memory-overlay provider computes, then ONE persist at the end (#1301).
#[test]
fn test_unpersisted_chain_roots_match_memory_overlay_oracle() -> eyre::Result<()> {
    let mut env = PipelineTestEnv::new();
    let cims = env.reth_env.canonical_in_memory_state();
    let recipient = Address::random();
    let mut overlay = OutputTrieOverlay::new();

    let blocks =
        (1..=3u64).try_fold(Vec::new(), |mut acc, i| -> eyre::Result<Vec<ExecutedBlock>> {
            let parent = env.canonical_header.clone();
            let output = consensus_output_for_test(i as u32, 0, i, env.block_timestamp);
            let payload = TNPayload::new_for_test(parent.clone(), &output);
            // same recipient every block: overlapping keys across the output
            let transfer = env.user_factory.create_eip1559_encoded(
                env.chain.clone(),
                Some(1_000_000),
                MIN_PROTOCOL_BASE_FEE.into(),
                Some(recipient),
                U256::from(i),
                Bytes::new(),
            );
            let block = env.reth_env.build_block_from_batch_payload(
                payload,
                &vec![transfer],
                &mut overlay,
            )?;

            // oracle: the exact reth path the pre-#1301 code used - the parent's
            // memory-overlay provider over the unpersisted ancestors
            let parent_provider = env.reth_env.state_by_block_hash_for_test(parent.hash())?;
            let hashed = parent_provider.hashed_post_state(&block.execution_output.state);
            let (oracle_root, oracle_updates) = parent_provider.state_root_with_updates(hashed)?;

            let header = block.recovered_block.clone_sealed_header();
            assert_eq!(header.state_root, oracle_root, "state root diverged at block {i}");
            assert_eq!(
                block.trie_data_handle().wait_cloned().trie_updates.as_ref(),
                &oracle_updates.into_sorted(),
                "trie updates diverged at block {i}",
            );

            // chain in memory only - the durable commit happens once, after the loop
            cims.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
            cims.set_canonical_head(header.clone());
            env.canonical_header = header;
            env.block_timestamp += 1;
            acc.push(block);
            Ok(acc)
        })?;

    // persist the whole output once and confirm the run lands cleanly
    env.reth_env.finish_executing_output(blocks, None)?;
    env.reth_env.finalize_block(env.canonical_header.clone())?;
    assert_eq!(env.reth_env.last_block_number()?, 3, "all three blocks persisted");
    assert_eq!(
        env.reth_env.canonical_tip().hash(),
        env.canonical_header.hash(),
        "canonical tip is the last built block",
    );
    Ok(())
}

/// The untimed wrapper is transparent and the build is deterministic. Two blocks of one output
/// are built from the same payloads through `build_block_from_batch_payload` (now a wrapper over
/// the timed build) and `build_block_from_batch_payload_timed`, each path with its own output
/// overlay, and must agree on the block hash, state root, gas, receipts and bundle state. Each
/// block carries a replayed transaction (skipped as invalid) and bytes that do not decode
/// (dropped at recovery), so both drop paths run, and block 2's root runs over a non-empty
/// overlay.
///
/// Both paths share one implementation, so this cannot catch a difference from the pre-split
/// build. That equivalence rests on the transaction moving into the builder as the same
/// `Recovered<T>` value it used to clone, and on
/// `test_unpersisted_chain_roots_match_memory_overlay_oracle`, whose independent reth oracle
/// checks the roots built through the timed root provider.
#[test]
fn test_timed_build_matches_untimed_build() -> eyre::Result<()> {
    let mut env = PipelineTestEnv::new();
    let cims = env.reth_env.canonical_in_memory_state();
    let recipient = Address::random();
    let beneficiary = Address::random();
    let mut untimed_overlay = OutputTrieOverlay::new();
    let mut timed_overlay = OutputTrieOverlay::new();

    for i in 1..=2u64 {
        let parent = env.canonical_header.clone();
        let output = consensus_output_for_test(i as u32, 0, i, env.block_timestamp);
        let mut txs: Vec<Vec<u8>> = (0..3u64)
            .map(|amount| {
                env.user_factory.create_eip1559_encoded(
                    env.chain.clone(),
                    Some(1_000_000),
                    MIN_PROTOCOL_BASE_FEE.into(),
                    Some(recipient),
                    U256::from(amount + 1),
                    Bytes::new(),
                )
            })
            .collect();
        txs.push(txs[0].clone());
        txs.push(vec![0xde, 0xad, 0xbe, 0xef]);

        // `new_for_test` randomizes the beneficiary, batch digest and mix hash: pin them so both
        // paths build from the same payload
        let payload = || {
            TNPayload::new_for_test_with(
                parent.clone(),
                &output,
                beneficiary,
                B256::with_last_byte(i as u8),
                B256::with_last_byte(0x80 | i as u8),
            )
        };
        let untimed =
            env.reth_env.build_block_from_batch_payload(payload(), &txs, &mut untimed_overlay)?;
        let (timed, timings) = env.reth_env.build_block_from_batch_payload_timed(
            payload(),
            &txs,
            &mut timed_overlay,
        )?;

        let header = untimed.recovered_block.clone_sealed_header();
        let timed_header = timed.recovered_block.clone_sealed_header();
        assert_eq!(header.hash(), timed_header.hash(), "block hash diverged at block {i}");
        assert_eq!(header.state_root, timed_header.state_root, "state root diverged at block {i}");
        assert_eq!(header.gas_used, timed_header.gas_used, "gas used diverged at block {i}");
        assert_eq!(
            untimed.execution_output.result.receipts, timed.execution_output.result.receipts,
            "receipts diverged at block {i}",
        );
        assert_eq!(
            untimed.execution_output.state, timed.execution_output.state,
            "bundle state diverged at block {i}",
        );
        assert_eq!(timed.recovered_block.transaction_count(), 3, "both drop paths ran");
        // the root lap is read off the root provider wrapper: zero would mean it was never timed
        assert!(timings.root > Duration::ZERO, "root lap not measured at block {i}");
        assert!(timings.evm > Duration::ZERO, "evm lap not measured at block {i}");

        // chain the (identical) block in memory only, as the engine does within an output
        cims.update_chain(NewCanonicalChain::Commit { new: vec![untimed] });
        cims.set_canonical_head(header.clone());
        env.canonical_header = header;
        env.block_timestamp += 1;
    }
    Ok(())
}

/// Signers recovered ahead of the build (`recover_output_signers`, as the engine's background job
/// runs it) build the blocks the bytes path builds. One three-batch output, split across two
/// certificates, is built block by block through `build_block_from_batch_payload` from each
/// batch's bytes and through `build_block_from_recovered_payload` from the batch's entry of the
/// recovered signers, each path with its own output overlay, and the two must agree on the block
/// hash, state root, gas, receipts and bundle state.
///
/// The recovered signers come one `Vec` per batch in flattened order (certificate, then batch); a
/// flat list would shift transactions across blocks, so the test pins the per-batch shape and
/// order across both certificates. Batch 1 replays one of its own transactions and batch 3 (the
/// second certificate's) repeats one of batch 1's, and both carry bytes that do not decode: every
/// copy after the first executes nowhere (it is skipped as invalid in either block), the
/// undecodable bytes are dropped at recovery, and the later blocks' roots run over a non-empty
/// overlay.
#[test]
fn test_recovered_build_matches_bytes_build() -> eyre::Result<()> {
    let mut env = PipelineTestEnv::new();
    let cims = env.reth_env.canonical_in_memory_state();
    let recipient = Address::random();
    let beneficiary = Address::random();
    let undecodable = vec![0xde, 0xad, 0xbe, 0xef];
    // one sender, consecutive nonces 0..6
    let txs: Vec<Vec<u8>> = (0..6u64)
        .map(|amount| {
            env.user_factory.create_eip1559_encoded(
                env.chain.clone(),
                Some(1_000_000),
                MIN_PROTOCOL_BASE_FEE.into(),
                Some(recipient),
                U256::from(amount + 1),
                Bytes::new(),
            )
        })
        .collect();
    // the first certificate carries batches 1 and 2, the second batch 3
    let certificates = vec![
        vec![
            // nonce 0 replayed within the batch
            vec![
                txs[0].clone(),
                txs[1].clone(),
                txs[2].clone(),
                txs[0].clone(),
                undecodable.clone(),
            ],
            vec![txs[3].clone()],
        ],
        // nonce 1 again, already executed by block 1 of the same output
        vec![vec![txs[1].clone(), txs[4].clone(), undecodable, txs[5].clone()]],
    ];
    let batch_bytes: Vec<Vec<Vec<u8>>> = certificates.iter().flatten().cloned().collect();
    let output = consensus_output_with_batches(1, env.block_timestamp, certificates);

    let signers = RethEnv::recover_output_signers(&output);
    assert_eq!(signers.len(), output.flatten_batches().len(), "one entry per flattened batch");
    // per batch and in batch order: every decodable copy survives recovery, duplicates included
    // (only execution skips them)
    let recovered_hashes: Vec<Vec<B256>> =
        signers.iter().map(|batch| batch.iter().map(|tx| *tx.tx_hash()).collect()).collect();
    let decoded_hashes: Vec<Vec<B256>> = batch_bytes
        .iter()
        .map(|batch| {
            batch
                .iter()
                .filter_map(|bytes| tn_reth::recover_raw_transaction(bytes).ok())
                .map(|tx| *tx.tx_hash())
                .collect()
        })
        .collect();
    assert_eq!(recovered_hashes, decoded_hashes);
    assert_eq!(recovered_hashes.iter().map(Vec::len).collect::<Vec<_>>(), [4, 1, 3]);

    let mut bytes_overlay = OutputTrieOverlay::new();
    let mut recovered_overlay = OutputTrieOverlay::new();
    for (i, (bytes, recovered)) in batch_bytes.iter().zip(signers).enumerate() {
        let parent = env.canonical_header.clone();
        // pin the beneficiary, batch digest and mix hash so both paths build the same payload
        let payload = || {
            TNPayload::new_for_test_with(
                parent.clone(),
                &output,
                beneficiary,
                B256::with_last_byte(i as u8 + 1),
                B256::with_last_byte(0x80 | (i as u8 + 1)),
            )
        };
        let from_bytes =
            env.reth_env.build_block_from_batch_payload(payload(), bytes, &mut bytes_overlay)?;
        let (from_recovered, timings) = env.reth_env.build_block_from_recovered_payload(
            payload(),
            recovered,
            &mut recovered_overlay,
        )?;

        let header = from_bytes.recovered_block.clone_sealed_header();
        let recovered_header = from_recovered.recovered_block.clone_sealed_header();
        assert_eq!(header.hash(), recovered_header.hash(), "block hash diverged at block {i}");
        assert_eq!(header.state_root, recovered_header.state_root, "state root diverged at {i}");
        assert_eq!(header.receipts_root, recovered_header.receipts_root, "receipts root at {i}");
        assert_eq!(header.gas_used, recovered_header.gas_used, "gas used diverged at block {i}");
        assert_eq!(
            from_bytes.execution_output.result.receipts,
            from_recovered.execution_output.result.receipts,
            "receipts diverged at block {i}",
        );
        assert_eq!(
            from_bytes.execution_output.state, from_recovered.execution_output.state,
            "bundle state diverged at block {i}",
        );
        assert_eq!(timings.recover, Duration::ZERO, "the recovered build recovers nothing");

        // each nonce executes exactly once across the output: block 1 runs nonces 0-2 (its
        // replayed nonce 0 is skipped), block 2 nonce 3, block 3 nonces 4-5 (the second
        // certificate's repeat of nonce 1 is skipped)
        let executed_nonces: Vec<u64> =
            from_recovered.recovered_block.transactions_recovered().map(|tx| tx.nonce()).collect();
        let expected_nonces: &[u64] = match i {
            0 => &[0, 1, 2],
            1 => &[3],
            _ => &[4, 5],
        };
        assert_eq!(executed_nonces, expected_nonces, "executed nonces of block {i}");

        // chain the (identical) block in memory only, as the engine does within an output
        cims.update_chain(NewCanonicalChain::Commit { new: vec![from_bytes] });
        cims.set_canonical_head(header.clone());
        env.canonical_header = header;
    }
    Ok(())
}
