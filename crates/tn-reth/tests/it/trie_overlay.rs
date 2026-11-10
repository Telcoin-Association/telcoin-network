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
    block_differences, child_init_code, consensus_output_for_test, consensus_output_with_batches,
    deploy_code, PipelineTestEnv, CHILD_RUNTIME, FACTORY_RUNTIME,
};
use reth_chain_state::ComputedTrieData;
use reth_provider::{DBProvider as _, HashedPostStateProvider as _, StateRootProvider as _};
use reth_revm::{db::AccountStatus, primitives::KECCAK_EMPTY, state::AccountInfo};
use reth_trie::{HashedPostState, HashedStorage, StateRoot, TrieInput, TrieInputSorted};
use reth_trie_db::DatabaseStateRoot as _;
use secp256k1::rand::{rngs::StdRng, RngCore as _, SeedableRng as _};
use std::{sync::Arc, time::Duration};
use tn_reth::{
    error::TnRethError, payload::TNPayload, ExecutedBlock, NewCanonicalChain, OutputBuildScope,
    OutputTrieOverlay, RethEnv,
};
use tn_types::{
    keccak256, Account, Address, Bytes, TransactionTrait as _, B256, MIN_PROTOCOL_BASE_FEE, U256,
};

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
/// batch's bytes (its own output overlay, a fresh read cache per block) and through
/// `build_block_from_recovered_payload` from the batch's entry of the recovered signers (one
/// build scope for the output, as the engine passes it), and the two must agree on the block
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
    let mut recovered_scope = OutputBuildScope::new();
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
            &mut recovered_scope,
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

/// Runtime of the probe: `sstore(addr, extcodehash(addr))` for the address in the calldata, a
/// read of an account that neither touches it nor changes it.
const PROBE_RUNTIME: [u8; 7] = [0x5f, 0x35, 0x3f, 0x5f, 0x35, 0x55, 0x00];

/// The cache entry of `address`: `None` without one, `Some(None)` for an account cached as absent.
fn cached_info(scope: &OutputBuildScope, address: Address) -> Option<Option<AccountInfo>> {
    scope.reads().accounts.get(&address).map(|account| account.info.clone())
}

/// The cached value of `address`'s storage `slot`, if the cache holds one.
fn cached_slot(scope: &OutputBuildScope, address: Address, slot: u64) -> Option<U256> {
    scope.reads().accounts.get(&address)?.storage.get(&U256::from(slot)).copied()
}

/// One read cache kept across the blocks of a consensus output builds the blocks a fresh cache
/// per block builds (E5). A three-block output is built in memory, without persisting, twice from
/// the same payloads: through the bytes path, which reads every value from the parent state
/// provider, and through the recovered path with ONE build scope for the output. The two must
/// agree on block hash, state root, receipts root, gas, receipts and bundle state at every block,
/// and the shared cache's contents are asserted after each block.
///
/// The output walks every branch of the cache's update rule with a repeated sender:
/// After every block the whole cache is also checked against the state the next block reads
/// (`PipelineTestEnv::cache_mismatches`).
///
/// - block 1 deploys a CREATE2 factory and a probe by transaction, probes an existing empty
///   account, creates and self-destructs child A in one transaction and funds child B's future
///   address (a balance-only account);
/// - block 2 recreates A at the same address with slot 1 written, creates B over the funded account
///   and self-destructs it (an existing account destroyed) and recreates it within the block
///   (`DestroyedChanged`), and clears the empty account with a zero-value transfer (EIP-161);
/// - block 3 calls A and B (each reads slot 1 and writes slot 2), probes A and touches the cleared
///   account again: a stale cache would revive it and destroy it a second time in the bundle.
///
/// One revm `State` kept across the blocks fails this shape: A's sticky `DestroyedChanged` makes
/// reth rebuild its storage as wiped and drop block 2's slot 1 from the root.
#[test]
fn test_shared_read_cache_matches_fresh_reads() -> eyre::Result<()> {
    let mut env = PipelineTestEnv::new_with_empty_governance_eoa();
    let user = env.user_factory.address();
    let empty = env.governance_factory.address();
    let factory = user.create(0);
    let probe = user.create(1);
    let (child_a_init, child_b_init) = (child_init_code(0), child_init_code(1));
    let child_a = factory.create2_from_code(B256::ZERO, &child_a_init);
    let child_b = factory.create2_from_code(B256::ZERO, &child_b_init);
    let child_code_hash = keccak256(CHILD_RUNTIME);
    let word = |value: u64| Bytes::from(B256::from(U256::from(value)).to_vec());
    let probe_of = |address: Address| Bytes::from(address.into_word().to_vec());
    let mut tx = |to: Option<Address>, value: u64, input: Bytes| {
        env.user_factory.create_eip1559_encoded(
            env.chain.clone(),
            Some(1_000_000),
            MIN_PROTOCOL_BASE_FEE.into(),
            to,
            U256::from(value),
            input,
        )
    };
    let blocks = [
        vec![
            tx(None, 0, deploy_code(&FACTORY_RUNTIME)),
            tx(None, 0, deploy_code(&PROBE_RUNTIME)),
            tx(Some(probe), 0, probe_of(empty)),
            tx(Some(factory), 0, child_a_init.clone().into()),
            tx(Some(child_b), 1, Bytes::new()),
        ],
        vec![
            tx(Some(factory), 7, child_a_init.into()),
            tx(Some(factory), 0, child_b_init.clone().into()),
            tx(Some(factory), 9, child_b_init.into()),
            tx(Some(empty), 0, Bytes::new()),
        ],
        vec![
            tx(Some(child_a), 0, word(5)),
            tx(Some(child_b), 0, word(5)),
            tx(Some(probe), 0, probe_of(child_a)),
            tx(Some(empty), 0, Bytes::new()),
        ],
    ];

    let mut overlay = OutputTrieOverlay::new();
    let mut scope = OutputBuildScope::new();
    let mut sent = 0;
    for (i, txs) in blocks.iter().enumerate() {
        sent += txs.len() as u64;
        let (fresh, shared) =
            env.build_unpersisted_fresh_and_shared(txs, &mut overlay, &mut scope)?;
        assert_eq!(block_differences(&fresh, &shared), Vec::<&str>::new(), "block {}", i + 1);
        // every cached account, slot, bytecode and block hash equals the next block's parent state
        assert_eq!(env.cache_mismatches(&scope, &fresh)?, Vec::<String>::new(), "block {}", i + 1);
        assert_eq!(fresh.recovered_block.transaction_count(), txs.len(), "block {}", i + 1);
        for index in 0..txs.len() {
            assert!(env.tx_succeeded(&fresh, index), "block {} tx {index} failed", i + 1);
        }
        let bundle = &fresh.execution_output.state;
        let reads = scope.reads();
        // the sender and every contract read by the block are cached at their post-block state,
        // without code; the read cache never carries an account's code or `account_id` storage hint
        assert!(reads
            .accounts
            .values()
            .filter_map(|account| account.info.as_ref())
            .all(|info| info.code.is_none() && info.account_id.is_none()));
        let user_info = cached_info(&scope, user).flatten().expect("sender cached");
        assert_eq!(user_info.nonce, sent, "sender nonce after block {}", i + 1);
        let factory_info = cached_info(&scope, factory).flatten().expect("factory cached");
        assert_eq!(factory_info.code_hash, keccak256(FACTORY_RUNTIME));

        match i {
            0 => {
                // created and destroyed in one transaction: not an account after the block
                assert_eq!(cached_info(&scope, child_a).flatten(), None, "A after block 1");
                assert!(bundle.state.get(&child_a).is_none_or(|account| account.info.is_none()));
                // read, not touched: the empty account is cached as it is, an existing account
                let empty_info = cached_info(&scope, empty).flatten().expect("empty cached");
                assert!(empty_info.is_empty(), "the empty account is cached empty");
                // funded by a transfer: a balance-only account
                let b = cached_info(&scope, child_b).flatten().expect("B funded in block 1");
                assert_eq!((b.balance, b.code_hash), (U256::from(1), KECCAK_EMPTY));
            }
            1 => {
                // recreated with slot 1: cached without code, storage from the bundle only
                let a = cached_info(&scope, child_a).flatten().expect("A cached after block 2");
                assert_eq!((a.balance, a.nonce, a.code_hash), (U256::from(7), 1, child_code_hash));
                assert_eq!(cached_slot(&scope, child_a, 1), Some(U256::from(7)));
                assert_eq!(scope.reads().accounts[&child_a].storage.len(), 1);
                // destroyed and recreated within the block: evicted, read again from the provider
                assert_eq!(bundle.state[&child_b].status, AccountStatus::DestroyedChanged);
                assert_eq!(cached_info(&scope, child_b), None, "B evicted after block 2");
                // cleared under EIP-161: destroyed without info, evicted
                assert!(bundle.state[&empty].info.is_none());
                assert!(bundle.state[&empty].status.was_destroyed());
                assert_eq!(cached_info(&scope, empty), None, "cleared account evicted");
            }
            _ => {
                assert_eq!(cached_slot(&scope, child_a, 1), Some(U256::from(7)));
                assert_eq!(cached_slot(&scope, child_a, 2), Some(U256::from(12)));
                // re-read from the provider in block 3: the value block 2 wrote
                assert_eq!(cached_slot(&scope, child_b, 1), Some(U256::from(9)));
                assert_eq!(cached_slot(&scope, child_b, 2), Some(U256::from(14)));
                // touched again: no longer an account, and no second destruction in the bundle
                assert_eq!(cached_info(&scope, empty), Some(None), "cleared account stays absent");
                assert!(
                    !bundle.state.contains_key(&empty),
                    "a dead account is not destroyed twice"
                );
                // the probe stored A's code hash, read through the cached info
                assert_eq!(
                    bundle.state[&probe].storage[&U256::from_be_bytes(child_a.into_word().0)]
                        .present_value,
                    U256::from_be_bytes(child_code_hash.0),
                );
            }
        }
    }
    Ok(())
}

/// A build scope builds only on the block it was last advanced to. After one block of an output,
/// a payload on any other parent (here the output's own parent, as a scope kept past its output
/// or reused on another branch would see it) fails with `ScopeParentMismatch` before anything is
/// built, and the scope still builds the output's next block on its tip.
#[test]
fn test_scope_rejects_payload_on_another_parent() -> eyre::Result<()> {
    let mut env = PipelineTestEnv::new();
    let genesis = env.canonical_header.clone();
    let mut overlay = OutputTrieOverlay::new();
    let mut scope = OutputBuildScope::new();
    let (block, _) =
        env.build_unpersisted_fresh_and_shared(&Vec::new(), &mut overlay, &mut scope)?;
    let tip = block.recovered_block.hash();

    let output = consensus_output_for_test(9, 0, 9, env.block_timestamp);
    let stale = TNPayload::new_for_test(genesis.clone(), &output);
    let err = env
        .reth_env
        .build_block_from_recovered_payload(stale, Vec::new(), &mut scope)
        .expect_err("a payload off the scope's tip must fail closed");
    assert!(
        matches!(err, TnRethError::ScopeParentMismatch { expected, got }
            if expected == tip && got == genesis.hash()),
        "unexpected error: {err}",
    );

    // the rejected payload changed nothing: the next block of the output still builds on the tip
    let (fresh, shared) =
        env.build_unpersisted_fresh_and_shared(&Vec::new(), &mut overlay, &mut scope)?;
    assert_eq!(block_differences(&fresh, &shared), Vec::<&str>::new());
    assert_eq!(fresh.recovered_block.parent_hash, tip);
    Ok(())
}
