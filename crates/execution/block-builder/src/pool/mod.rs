//! The worker's approach to maintaining and updating transaction pool state.

mod backup;
pub mod maintain;
mod metrics;

// tests for all mods
#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::MAINNET;
    use reth_fs_util as fs;
    use reth_primitives::{hex, PooledTransactionsElement, U256};
    use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
    use reth_tasks::TaskManager;

    #[test]
    fn changed_acc_entry() {
        // empty changed account
        let changed_acc = ChangedAccountEntry(ChangedAccount {
            address: Address::random(),
            nonce: 0,
            balance: U256::ZERO,
        });
        let mut copy = changed_acc.0;
        copy.nonce = 10;
        assert!(changed_acc.eq(&ChangedAccountEntry(copy)));
    }

    const EXTENSION: &str = "rlp";
    const FILENAME: &str = "test_transactions_backup";

    #[tokio::test(flavor = "multi_thread")]
    async fn test_save_local_txs_backup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let transactions_path = temp_dir.path().join(FILENAME).with_extension(EXTENSION);
        let tx_bytes = hex!("02f87201830655c2808505ef61f08482565f94388c818ca8b9251b393131c08a736a67ccb192978801049e39c4b5b1f580c001a01764ace353514e8abdfb92446de356b260e3c1225b73fc4c8876a6258d12a129a04f02294aa61ca7676061cd99f29275491218b4754b46a0248e5e42bc5091f507");
        let tx = PooledTransactionsElement::decode_enveloped(&mut &tx_bytes[..]).unwrap();
        let provider = MockEthProvider::default();
        let transaction = EthPooledTransaction::from_recovered_pooled_transaction(
            tx.try_into_ecrecovered().unwrap(),
        );
        let tx_to_cmp = transaction.clone();
        let sender = hex!("1f9090aaE28b8a3dCeaDf281B0F12828e676c326").into();
        provider.add_account(sender, ExtendedAccount::new(42, U256::MAX));
        let blob_store = InMemoryBlobStore::default();
        let validator = EthTransactionValidatorBuilder::new(MAINNET.clone())
            .build(provider, blob_store.clone());

        let txpool = Pool::new(
            validator.clone(),
            CoinbaseTipOrdering::default(),
            blob_store.clone(),
            Default::default(),
        );

        txpool.add_transaction(TransactionOrigin::Local, transaction.clone()).await.unwrap();

        let handle = tokio::runtime::Handle::current();
        let manager = TaskManager::new(handle);
        let config = LocalTransactionBackupConfig::with_local_txs_backup(transactions_path.clone());
        manager.executor().spawn_critical_with_graceful_shutdown_signal("test task", |shutdown| {
            backup_local_transactions_task(shutdown, txpool.clone(), config)
        });

        let mut txns = txpool.get_local_transactions();
        let tx_on_finish = txns.pop().expect("there should be 1 transaction");

        assert_eq!(*tx_to_cmp.hash(), *tx_on_finish.hash());

        // shutdown the executor
        manager.graceful_shutdown();

        let data = fs::read(transactions_path).unwrap();

        let txs: Vec<TransactionSigned> =
            alloy_rlp::Decodable::decode(&mut data.as_slice()).unwrap();
        assert_eq!(txs.len(), 1);

        temp_dir.close().unwrap();
    }
}
