//! Regression tests for geometric compaction and layered cursor equivalence.

use super::{cursors::RunCursorFactory, sorted_runs::SortedTrieRuns};
use reth_db::DatabaseError;
use reth_primitives_traits::Account;
use reth_trie::{
    hashed_cursor::{
        noop::NoopHashedCursorFactory, HashedCursor, HashedCursorFactory,
        HashedPostStateCursorFactory, HashedStorageCursor,
    },
    trie_cursor::{noop::NoopTrieCursorFactory, InMemoryTrieCursorFactory},
    updates::TrieUpdatesSorted,
    HashedPostState, HashedStorage, Nibbles, StateRoot,
};
use std::sync::Arc;
use tn_types::{B256, U256};

/// Encode deterministic ordered keys without a random seed or wall clock.
fn key(value: u64) -> B256 {
    B256::from(U256::from(value).to_be_bytes::<32>())
}

/// Verify that interleaving account and trie-node deltas do not rewrite older runs each block.
#[test]
fn compaction_preserves_older_runs_between_binary_carries() {
    let mut runs = SortedTrieRuns::default();
    (1..=256u64).for_each(|block| {
        let oldest = runs
            .iter()
            .next()
            .map(|run| (std::ptr::from_ref(run.state()), std::ptr::from_ref(run.nodes())));
        let keys = [key(block), key(256 + block)];
        let state = Arc::new(
            HashedPostState::default()
                .with_accounts(keys.map(|key| (key, Some(Account::default()))))
                .into_sorted(),
        );
        let nodes = Arc::new(TrieUpdatesSorted::new(
            keys.map(|key| (Nibbles::unpack(key), None)).into(),
            Default::default(),
        ));
        let original_state = state.clone();
        let original_nodes = nodes.clone();
        runs.extend(state, nodes);

        let expected = (0..u64::BITS)
            .rev()
            .filter(|level| block & (1u64 << level) != 0)
            .map(|level| 2usize << level)
            .collect::<Vec<_>>();
        assert_eq!(runs.iter().map(|run| run.state().accounts.len()).collect::<Vec<_>>(), expected);
        assert_eq!(runs.iter().map(|run| run.nodes().total_len()).collect::<Vec<_>>(), expected);
        assert_eq!(original_state.accounts.len(), 2, "a block's shared state was mutated");
        assert_eq!(original_nodes.total_len(), 2, "a block's shared nodes were mutated");
        if !block.is_power_of_two() {
            assert_eq!(
                runs.iter().next().map(|run| {
                    (std::ptr::from_ref(run.state()), std::ptr::from_ref(run.nodes()))
                }),
                oldest,
                "the oldest run was replaced without a carry reaching its level"
            );
        }
    });
}

/// Changes spanning tombstones, zero slots, wipes, recreation, and empty blocks.
fn deltas() -> Vec<HashedPostState> {
    let account = Some(Account { nonce: 1, ..Default::default() });
    let contract = key(1);
    vec![
        HashedPostState::default()
            .with_accounts([(contract, account), (key(2), account)])
            .with_storages([(
                contract,
                HashedStorage::from_iter(false, [(key(1), U256::from(11))]),
            )]),
        HashedPostState::default()
            .with_accounts([(key(2), None)])
            .with_storages([(contract, HashedStorage::from_iter(false, [(key(1), U256::ZERO)]))]),
        HashedPostState::default()
            .with_accounts([(contract, None)])
            .with_storages([(contract, HashedStorage::from_iter(true, []))]),
        HashedPostState::default(),
        HashedPostState::default().with_accounts([(contract, account)]).with_storages([(
            contract,
            HashedStorage::from_iter(false, [(key(2), U256::from(22))]),
        )]),
        HashedPostState::default().with_storages([(
            contract,
            HashedStorage::from_iter(true, [(key(3), U256::from(33))]),
        )]),
        HashedPostState::default().with_accounts([(key(2), account)]).with_storages([(
            contract,
            HashedStorage::from_iter(false, [(key(1), U256::from(44))]),
        )]),
        HashedPostState::default().with_accounts([(key(2), None)]),
    ]
}

/// Compare roots and trie updates at every block, including carries at 8, 16, 32, and 64.
#[test]
fn run_roots_match_flattened_oracle_across_compaction_boundaries(
) -> Result<(), reth_provider::ProviderError> {
    let base = HashedPostState::default()
        .with_accounts([(key(1), Some(Account::default())), (key(3), Some(Account::default()))])
        .with_storages([(key(1), HashedStorage::from_iter(false, [(key(9), U256::from(99))]))]);
    let base_prefixes = base.construct_prefix_sets().freeze();
    let base = base.into_sorted();
    let (_, base_nodes) = StateRoot::new(
        NoopTrieCursorFactory::default(),
        HashedPostStateCursorFactory::new(NoopHashedCursorFactory::default(), &base),
    )
    .with_prefix_sets(base_prefixes)
    .root_with_updates()?;
    let base_nodes = base_nodes.into_sorted();
    let mut oracle_state = base.clone();
    let mut oracle_nodes = base_nodes.clone();
    let mut runs = SortedTrieRuns::default();

    deltas().into_iter().cycle().take(65).enumerate().try_for_each(|(block, delta)| {
        let prefixes = delta.construct_prefix_sets().freeze();
        let current = delta.into_sorted();
        let layered = StateRoot::new(
            RunCursorFactory::new(
                InMemoryTrieCursorFactory::new(NoopTrieCursorFactory::default(), &base_nodes),
                &runs,
            ),
            HashedPostStateCursorFactory::new(
                RunCursorFactory::new(
                    HashedPostStateCursorFactory::new(NoopHashedCursorFactory::default(), &base),
                    &runs,
                ),
                &current,
            ),
        )
        .with_prefix_sets(prefixes.clone())
        .root_with_updates()?;

        oracle_state.extend_ref_and_sort(&current);
        let oracle = StateRoot::new(
            InMemoryTrieCursorFactory::new(NoopTrieCursorFactory::default(), &oracle_nodes),
            HashedPostStateCursorFactory::new(NoopHashedCursorFactory::default(), &oracle_state),
        )
        .with_prefix_sets(prefixes)
        .root_with_updates()?;
        assert_eq!(layered, oracle, "root or trie updates differ at block {block}");

        let nodes = Arc::new(layered.1.into_sorted());
        oracle_nodes.extend_ref_and_sort(&nodes);
        runs.extend(Arc::new(current), nodes);
        Ok(())
    })
}

/// Cursor resets and address changes must propagate through every run, including a wipe.
#[test]
fn storage_cursor_retargets_all_layers() -> Result<(), DatabaseError> {
    let mut runs = SortedTrieRuns::default();
    [
        HashedPostState::default().with_storages([
            (key(1), HashedStorage::from_iter(false, [(key(1), U256::from(11))])),
            (key(2), HashedStorage::from_iter(false, [(key(2), U256::from(22))])),
        ]),
        HashedPostState::default(),
        HashedPostState::default().with_storages([(key(1), HashedStorage::from_iter(true, []))]),
    ]
    .into_iter()
    .for_each(|delta| runs.extend(Arc::new(delta.into_sorted()), Arc::default()));
    let factory = RunCursorFactory::new(NoopHashedCursorFactory::default(), &runs);
    let mut cursor = factory.hashed_storage_cursor(key(1))?;
    assert!(cursor.is_storage_empty()?);
    assert_eq!(cursor.seek(B256::ZERO)?, None);
    cursor.set_hashed_address(key(2));
    assert!(!cursor.is_storage_empty()?);
    assert_eq!(cursor.seek(B256::ZERO)?, Some((key(2), U256::from(22))));
    assert_eq!(cursor.next()?, None);
    cursor.reset();
    assert_eq!(cursor.seek(B256::ZERO)?, Some((key(2), U256::from(22))));
    cursor.set_hashed_address(key(1));
    assert!(cursor.is_storage_empty()?);
    assert_eq!(cursor.seek(B256::ZERO)?, None);
    Ok(())
}
