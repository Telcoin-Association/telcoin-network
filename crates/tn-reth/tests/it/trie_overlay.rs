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

use crate::pipeline_helpers::{consensus_output_for_test, PipelineTestEnv};
use reth_chain_state::ComputedTrieData;
use reth_provider::{DBProvider as _, HashedPostStateProvider as _, StateRootProvider as _};
use reth_trie::{HashedPostState, HashedStorage, StateRoot, TrieInput, TrieInputSorted};
use reth_trie_db::DatabaseStateRoot as _;
use secp256k1::rand::{rngs::StdRng, RngCore as _, SeedableRng as _};
use std::sync::Arc;
use tn_reth::{payload::TNPayload, ExecutedBlock, NewCanonicalChain, OutputTrieOverlay};
use tn_types::{Account, Address, Bytes, B256, MIN_PROTOCOL_BASE_FEE, U256};

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
/// delta, and an empty delta.
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
    ]
}

/// Trie-level differential: per block, the layered-cursor root and updates equal
/// reth's `overlay_root_from_nodes_with_updates` over the `prepend_self`-merged
/// `TrieInput` built from the same deltas (#1301).
#[test]
fn test_layered_overlay_root_matches_merged_trie_input_oracle() -> eyre::Result<()> {
    let env = PipelineTestEnv::new();
    let provider = env.reth_env.database_provider_ro_for_test()?;
    let tx = provider.tx_ref();

    let mut rng = StdRng::seed_from_u64(0x1301);
    let deltas = seeded_deltas(&mut rng);

    deltas.into_iter().enumerate().try_fold(
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
