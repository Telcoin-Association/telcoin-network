//! Compatibility coverage for Reth consumers of header-only canonical updates.

use super::*;
use crate::{test_utils::TransactionFactory, RethChainSpec, RethEnv};
use rand::{rngs::StdRng, SeedableRng as _};
use reth_transaction_pool::error::PoolErrorKind;
use std::collections::BTreeSet;
use tempfile::TempDir;
use tn_types::{
    calculate_transaction_root, test_genesis, Bytes, GenesisAccount, TaskManager,
    MIN_PROTOCOL_BASE_FEE,
};

/// Compare transaction membership independently of Reth's internal iteration order.
fn transaction_hashes(transactions: Vec<Arc<PoolTxn>>) -> BTreeSet<TxHash> {
    transactions.into_iter().map(|transaction| *transaction.hash()).collect()
}

/// Admit the same signed transactions through each pool's real Reth validator.
async fn seed_pool(
    pool: &WorkerTxPool,
    transactions: &[TransactionSigned],
) -> Result<(), PoolError> {
    futures::future::try_join_all(
        transactions
            .iter()
            .cloned()
            .map(|transaction| pool.add_raw_transaction_external(transaction)),
    )
    .await
    .map(|_| ())
}

/// The pinned Reth pool derives its bookkeeping from the header and explicit account/hash
/// inputs, and its validator's `on_new_head_block` reads only the header. Recheck this contract
/// when upgrading Reth: replacing a nonempty body with an empty one must preserve both consumers.
/// See issue #1420 and the review that requested this regression:
/// <https://github.com/Telcoin-Association/telcoin-network/pull/1386#discussion_r4066267497>.
#[tokio::test]
async fn header_only_canonical_update_matches_full_block() -> eyre::Result<()> {
    const EPOCH_FEE: u64 = MIN_PROTOCOL_BASE_FEE + 1234;
    const TIP_GAS_LIMIT: u64 = 60_000;
    const PENDING_BLOB_FEE: Option<u128> = Some(17);

    let mut rng = StdRng::from_seed([14; 32]);
    let mut mined_sender = TransactionFactory::new_random_from_seed(&mut rng);
    let mut nonce_sender = TransactionFactory::new_random_from_seed(&mut rng);
    let mut balance_sender = TransactionFactory::new_random_from_seed(&mut rng);
    let mut fee_sender = TransactionFactory::new_random_from_seed(&mut rng);
    let mut validation_sender = TransactionFactory::new_random_from_seed(&mut rng);
    let genesis = test_genesis().extend_accounts(
        [
            mined_sender.address(),
            nonce_sender.address(),
            balance_sender.address(),
            fee_sender.address(),
            validation_sender.address(),
        ]
        .into_iter()
        .map(|address| (address, GenesisAccount::default().with_balance(U256::MAX))),
    );
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let reference_dir = TempDir::new()?;
    let header_only_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let reference_env =
        RethEnv::new_for_temp_chain(chain.clone(), reference_dir.path(), &task_manager, None)?;
    let header_only_env =
        RethEnv::new_for_temp_chain(chain.clone(), header_only_dir.path(), &task_manager, None)?;
    // No maintenance subscriber can race these explicit updates or mask a missing input.
    let reference_fee = BaseFeeContainer::new(MIN_PROTOCOL_BASE_FEE);
    let header_only_fee = BaseFeeContainer::new(MIN_PROTOCOL_BASE_FEE);
    let reference = reference_env.init_txn_pool_without_maintenance(reference_fee.clone())?;
    let header_only = header_only_env.init_txn_pool_without_maintenance(header_only_fee.clone())?;

    let transaction = |sender: &mut TransactionFactory, gas_limit, fee| {
        sender.create_eip1559(
            chain.clone(),
            Some(gas_limit),
            u128::from(fee),
            Some(Address::ZERO),
            U256::ZERO,
            Bytes::new(),
        )
    };
    let mined = transaction(&mut mined_sender, 21_000, EPOCH_FEE);
    let stale = transaction(&mut nonce_sender, 21_000, EPOCH_FEE);
    let ready = transaction(&mut nonce_sender, 21_000, EPOCH_FEE);
    let unfunded = transaction(&mut balance_sender, 21_000, EPOCH_FEE);
    let underpriced = transaction(&mut fee_sender, 21_000, MIN_PROTOCOL_BASE_FEE);
    let over_limit = transaction(&mut validation_sender, TIP_GAS_LIMIT + 1, EPOCH_FEE);
    let initial = [
        mined.clone(),
        stale.clone(),
        ready.clone(),
        unfunded.clone(),
        underpriced.clone(),
        over_limit.clone(),
    ];
    seed_pool(&reference, &initial).await?;
    seed_pool(&header_only, &initial).await?;
    let initial_hashes: BTreeSet<_> =
        initial.iter().map(|transaction| *transaction.hash()).collect();
    assert_eq!(transaction_hashes(reference.pending_transactions()), initial_hashes);
    assert_eq!(transaction_hashes(header_only.pending_transactions()), initial_hashes);

    // This transaction is valid before the new head, so its later rejection must reflect
    // the validator's updated header rather than a static admission limit.
    assert_eq!(reference.0.remove_transactions(vec![*over_limit.hash()]).len(), 1);
    assert_eq!(header_only.0.remove_transactions(vec![*over_limit.hash()]).len(), 1);

    let genesis_header = chain.sealed_genesis_header();
    let mut tip_header = (*genesis_header).clone();
    tip_header.parent_hash = genesis_header.hash();
    tip_header.number = 1;
    tip_header.timestamp += 1;
    tip_header.gas_limit = TIP_GAS_LIMIT;
    tip_header.gas_used = 21_000;
    let body = BlockBody { transactions: vec![mined.clone()], ..Default::default() };
    tip_header.transactions_root = calculate_transaction_root(&body.transactions);
    let tip_header = SealedHeader::seal_slow(tip_header);
    let full_tip: SealedBlock = SealedBlock::from_sealed_parts(tip_header.clone(), body);
    assert!(!full_tip.body().transactions.is_empty());
    assert_ne!(tip_header.base_fee_per_gas, Some(EPOCH_FEE));

    // Keep mined-hash removal independent of changed-account nonce invalidation.
    let mined_hashes = vec![*mined.hash()];
    let changed_accounts = vec![
        ChangedAccount { address: nonce_sender.address(), nonce: 1, balance: U256::MAX },
        ChangedAccount { address: balance_sender.address(), nonce: 0, balance: U256::ZERO },
    ];
    reference_fee.set_base_fee(EPOCH_FEE);
    header_only_fee.set_base_fee(EPOCH_FEE);

    // The reference calls Reth directly with the complete block and explicit effective fees;
    // sharing the production helper would let a bug in that helper affect both sides equally.
    reference.0.on_canonical_state_change(CanonicalStateUpdate {
        new_tip: &full_tip,
        pending_block_base_fee: EPOCH_FEE,
        pending_block_blob_fee: PENDING_BLOB_FEE,
        mined_transactions: mined_hashes.clone(),
        changed_accounts: changed_accounts.clone(),
        update_kind: PoolUpdateKind::Commit,
    });
    header_only
        .update_canonical_state(&tip_header, PENDING_BLOB_FEE, mined_hashes, changed_accounts)
        .await?;

    let expected_info = RethBlockInfo {
        last_seen_block_hash: tip_header.hash(),
        last_seen_block_number: tip_header.number,
        block_gas_limit: TIP_GAS_LIMIT,
        pending_basefee: EPOCH_FEE,
        pending_blob_fee: PENDING_BLOB_FEE,
    };
    assert_eq!(reference.block_info(), expected_info);
    assert_eq!(header_only.block_info(), reference.block_info());
    let expected_pending = BTreeSet::from([*ready.hash()]);
    let expected_queued = BTreeSet::from([*unfunded.hash(), *underpriced.hash()]);
    assert_eq!(transaction_hashes(reference.pending_transactions()), expected_pending);
    assert_eq!(transaction_hashes(header_only.pending_transactions()), expected_pending);
    assert_eq!(transaction_hashes(reference.queued_transactions()), expected_queued);
    assert_eq!(transaction_hashes(header_only.queued_transactions()), expected_queued);
    assert!(reference.get(mined.hash()).is_none());
    assert!(header_only.get(mined.hash()).is_none());
    assert!(reference.get(stale.hash()).is_none());
    assert!(header_only.get(stale.hash()).is_none());

    let reference_rejected = reference.add_raw_transaction_external(over_limit.clone()).await;
    let header_only_rejected = header_only.add_raw_transaction_external(over_limit).await;
    let rejects_new_gas_limit = |error: PoolError| {
        matches!(
            error.kind,
            PoolErrorKind::InvalidTransaction(InvalidPoolTransactionError::ExceedsGasLimit(
                transaction_gas_limit,
                block_gas_limit,
            )) if transaction_gas_limit == TIP_GAS_LIMIT + 1 && block_gas_limit == TIP_GAS_LIMIT
        )
    };
    assert!(reference_rejected.is_err_and(rejects_new_gas_limit));
    assert!(header_only_rejected.is_err_and(rejects_new_gas_limit));

    validation_sender.set_nonce(0);
    let valid = transaction(&mut validation_sender, TIP_GAS_LIMIT, EPOCH_FEE);
    reference.add_raw_transaction_external(valid.clone()).await?;
    header_only.add_raw_transaction_external(valid.clone()).await?;
    let expected_pending = BTreeSet::from([*ready.hash(), *valid.hash()]);
    assert_eq!(transaction_hashes(reference.pending_transactions()), expected_pending);
    assert_eq!(transaction_hashes(header_only.pending_transactions()), expected_pending);
    Ok(())
}
