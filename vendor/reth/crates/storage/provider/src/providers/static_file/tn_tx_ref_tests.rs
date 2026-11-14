//! TN(TN-0203): reading the `Transactions` rows that store a transaction's position in its batch
//! (TN-0202). A chain whose blocks dropped some of their batches' transactions is written with
//! its transactions inline and by position, and every transaction read answers the same, each
//! transaction compared by its EIP-2718 bytes and hash. A position row that cannot be resolved is
//! an error on every read, never a missing or another transaction; the static-file provider's own
//! reads refuse position rows; `remove_blocks_above` resolves them to drop their hashes.

use super::{JarFormat, TxBodyReadError, TxBodyResolver, TxBodyResolverInstalled, TxRow};
use crate::{
    providers::BlockchainProvider,
    test_utils::{create_test_provider_factory_with_blocks_per_file, MockNodeTypesWithDB},
    BlockNumReader, BlockReader, BlockWriter, ProviderFactory, SaveBlocksMode,
    StaticFileProviderFactory, TransactionVariant, TransactionsProvider, TransactionsProviderExt,
};
use alloy_consensus::{Header, TxEip1559, TxEip2930, TxEip4844, TxEip7702, TxLegacy};
use alloy_eips::{
    eip2718::{Decodable2718, Encodable2718},
    eip2930::{AccessList, AccessListItem},
    eip7702::{Authorization, SignedAuthorization},
};
use alloy_primitives::{Address, BlockNumber, Bytes, TxHash, TxKind, TxNumber, B256, U256};
use parking_lot::Mutex;
use reth_chain_state::{ComputedTrieData, ExecutedBlock};
use reth_db_api::{models::StorageSettings, tables, transaction::DbTxMut};
use reth_ethereum_primitives::{
    Block, BlockBody, EthPrimitives, Receipt, Transaction, TransactionSigned,
};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{BlockBody as _, RecoveredBlock, SealedBlock};
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::StorageSettingsCache;
use reth_storage_errors::provider::{ProviderError, ProviderResult};
use reth_testing_utils::generators::{self, sign_tx_with_random_key_pair};
use std::{collections::HashMap, ops::Range, sync::Arc};

type Factory = ProviderFactory<MockNodeTypesWithDB>;
type Provider = BlockchainProvider<MockNodeTypesWithDB>;

/// Blocks per static file: the chain spans four `Transactions` jars.
const BLOCKS_PER_FILE: u64 = 3;
/// The last block.
const TIP: BlockNumber = 9;
/// A block without transactions.
const EMPTY: BlockNumber = 5;
/// A block whose positions take two LEB128 bytes.
const FAR: BlockNumber = 7;
/// The block `remove_blocks_above` keeps the chain to.
const KEEP: BlockNumber = 4;

/// A chain and the batches its blocks executed.
struct Chain {
    /// Genesis and blocks 1 to [`TIP`].
    blocks: Vec<ExecutedBlock>,
    /// Each block's transactions' positions in its batch.
    positions: Vec<Arc<[u32]>>,
    /// Each block's batch by the block's hash: every transaction's EIP-2718 bytes, those the block
    /// dropped included.
    batches: HashMap<B256, Vec<Bytes>>,
}

impl Chain {
    /// Every transaction of the chain, by transaction number, with its block.
    fn txs(&self) -> Vec<(BlockNumber, TransactionSigned)> {
        self.blocks
            .iter()
            .flat_map(|block| {
                let block = block.recovered_block();
                let number = block.header().number;
                block.body().transactions.iter().map(move |tx| (number, tx.clone()))
            })
            .collect()
    }

    /// The transaction numbers of block `number`.
    fn tx_range(&self, number: BlockNumber) -> Range<TxNumber> {
        let count =
            |block: &ExecutedBlock| block.recovered_block().body().transaction_count() as TxNumber;
        let start = self.blocks[..number as usize].iter().map(count).sum();
        start..start + count(&self.blocks[number as usize])
    }

    fn block(&self, number: BlockNumber) -> &RecoveredBlock<Block> {
        self.blocks[number as usize].recovered_block()
    }
}

/// Transaction `n` of the chain, of each type in turn: legacy with and without a chain id,
/// EIP-2930, EIP-1559, EIP-4844 and EIP-7702.
fn tx(rng: &mut impl generators::Rng, n: u64) -> TransactionSigned {
    let to = Address::with_last_byte(n as u8);
    let value = U256::from(n);
    let input = Bytes::from(vec![n as u8; (n % 4) as usize]);
    let access_list = AccessList(vec![AccessListItem {
        address: Address::with_last_byte(0xaa),
        storage_keys: vec![B256::with_last_byte(n as u8)],
    }]);
    let tx = match n % 6 {
        0 => Transaction::Legacy(TxLegacy {
            chain_id: Some(1),
            nonce: n,
            gas_price: 7,
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value,
            input,
        }),
        1 => Transaction::Legacy(TxLegacy {
            chain_id: None,
            nonce: n,
            gas_price: 7,
            gas_limit: 21_000,
            to: TxKind::Create,
            value,
            input,
        }),
        2 => Transaction::Eip2930(TxEip2930 {
            chain_id: 1,
            nonce: n,
            gas_price: 7,
            gas_limit: 30_000,
            to: TxKind::Call(to),
            value,
            access_list,
            input,
        }),
        3 => Transaction::Eip1559(TxEip1559 {
            chain_id: 1,
            nonce: n,
            gas_limit: 30_000,
            max_fee_per_gas: 9,
            max_priority_fee_per_gas: 2,
            to: TxKind::Call(to),
            value,
            access_list,
            input,
        }),
        4 => Transaction::Eip4844(TxEip4844 {
            chain_id: 1,
            nonce: n,
            gas_limit: 30_000,
            max_fee_per_gas: 9,
            max_priority_fee_per_gas: 2,
            to,
            value,
            access_list,
            blob_versioned_hashes: vec![B256::with_last_byte(1)],
            max_fee_per_blob_gas: 3,
            input,
        }),
        _ => Transaction::Eip7702(TxEip7702 {
            chain_id: 1,
            nonce: n,
            gas_limit: 30_000,
            max_fee_per_gas: 9,
            max_priority_fee_per_gas: 2,
            to,
            value,
            access_list,
            authorization_list: vec![SignedAuthorization::new_unchecked(
                Authorization { chain_id: U256::from(1), address: to, nonce: n },
                1,
                U256::from(n + 1),
                U256::from(n + 2),
            )],
            input,
        }),
    };
    sign_tx_with_random_key_pair(rng, tx)
}

/// Genesis and blocks 1 to [`TIP`], each with 1 to 3 transactions but [`EMPTY`]. Each block's
/// batch holds its transactions at increasing positions with gaps, a dropped transaction in each,
/// and one more after the last; [`FAR`]'s positions start at 200.
fn chain() -> Chain {
    let mut rng = generators::rng_with_seed(b"tn-0203 transaction refs");
    let mut next_tx = 0;
    let mut parent = B256::ZERO;
    let mut chain = Chain { blocks: Vec::new(), positions: Vec::new(), batches: HashMap::new() };
    for number in 0..=TIP {
        let count = if number == 0 || number == EMPTY { 0 } else { 1 + number % 3 };
        let transactions: Vec<_> = (0..count)
            .map(|_| {
                next_tx += 1;
                tx(&mut rng, next_tx)
            })
            .collect();
        let base = if number == FAR { 200 } else { (number % 2) as u32 };
        let positions: Arc<[u32]> = (0..count as u32).map(|i| base + 3 * i).collect();
        let len = positions.last().map_or(0, |last| last + 2);
        let batch = (0..len)
            .map(|position| {
                match positions.iter().position(|p| *p == position) {
                    Some(i) => transactions[i].encoded_2718(),
                    None => tx(&mut rng, 1_000 + position as u64).encoded_2718(),
                }
                .into()
            })
            .collect();
        let header =
            Header { parent_hash: parent, number, timestamp: 1_000 + number, ..Default::default() };
        let block = Block { header, body: BlockBody { transactions, ..Default::default() } };
        let block = SealedBlock::seal_slow(block);
        parent = block.hash();
        chain.batches.insert(block.hash(), batch);
        chain.positions.push(positions);
        chain.blocks.push(executed(block));
    }
    chain
}

/// An executed block with one receipt per transaction and no state change.
fn executed(block: SealedBlock<Block>) -> ExecutedBlock {
    let receipts = vec![Receipt::default(); block.body().transaction_count()];
    ExecutedBlock::new(
        Arc::new(block.try_recover().unwrap()),
        Arc::new(BlockExecutionOutput {
            result: BlockExecutionResult {
                receipts,
                requests: Default::default(),
                gas_used: 0,
                blob_gas_used: 0,
            },
            state: Default::default(),
        }),
        ComputedTrieData::default(),
    )
}

/// How [`MockResolver`] answers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum Answer {
    /// The transactions at the positions asked.
    #[default]
    Right,
    /// An error.
    Fail,
    /// One transaction fewer than asked.
    Short,
    /// The transaction after each one asked.
    Next,
}

/// A resolver over a [`Chain`]'s batches, found by the header it is given, decoding their
/// EIP-2718 bytes; it records each call.
#[derive(Debug, Default)]
struct MockResolver {
    batches: HashMap<B256, Vec<Bytes>>,
    answer: Mutex<Answer>,
    calls: Mutex<Vec<(BlockNumber, Vec<u32>)>>,
}

/// The error a failing [`MockResolver`] returns.
#[derive(Debug)]
struct MockFailure(BlockNumber);

impl core::fmt::Display for MockFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "no batch for block {}", self.0)
    }
}

impl core::error::Error for MockFailure {}

impl TxBodyResolver<EthPrimitives> for MockResolver {
    fn resolve(
        &self,
        number: BlockNumber,
        header: &Header,
        positions: &[u32],
    ) -> ProviderResult<Vec<TransactionSigned>> {
        self.calls.lock().push((number, positions.to_vec()));
        assert_eq!(header.number, number, "the header is the block's");
        let answer = *self.answer.lock();
        let batch = self.batches.get(&header.hash_slow());
        let (Some(batch), false) = (batch, answer == Answer::Fail) else {
            return Err(ProviderError::other(MockFailure(number)))
        };
        let shift = u32::from(answer == Answer::Next);
        let mut txs: Vec<_> = positions
            .iter()
            .map(|position| {
                let bytes = &batch[(position + shift) as usize];
                TransactionSigned::decode_2718(&mut bytes.as_ref()).unwrap()
            })
            .collect();
        if answer == Answer::Short {
            txs.pop();
        }
        Ok(txs)
    }
}

/// How a chain's `Transactions` rows are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rows {
    /// Storage v1: upstream's jars.
    Plain,
    /// Storage v2: checksummed jars without tags, given positions (which they ignore).
    Checksummed,
    /// Storage v2 with tagged jars: a position row per transaction.
    Positions,
    /// Storage v2 with tagged jars: position rows for the odd blocks, inline rows (tag 0) for the
    /// even ones.
    Mixed,
}

/// A chain written as `rows`.
struct Built {
    factory: Factory,
    provider: Provider,
    resolver: Arc<MockResolver>,
}

/// Writes `chain` as `rows`, with a [`MockResolver`] installed if `install`.
fn build(chain: &Chain, rows: Rows, install: bool) -> Built {
    let factory = create_test_provider_factory_with_blocks_per_file(BLOCKS_PER_FILE);
    factory.set_storage_settings_cache(if rows == Rows::Plain {
        StorageSettings::v1()
    } else {
        StorageSettings::v2()
    });
    let static_files = factory.static_file_provider();
    static_files.set_tx_pointer_jars(matches!(rows, Rows::Positions | Rows::Mixed));
    let resolver = Arc::new(MockResolver { batches: chain.batches.clone(), ..Default::default() });
    if install {
        static_files.set_tx_body_resolver(resolver.clone()).unwrap();
    }
    let positions: Vec<_> = chain
        .positions
        .iter()
        .enumerate()
        .map(|(number, positions)| match rows {
            Rows::Plain => None,
            Rows::Mixed if number % 2 == 0 => None,
            _ => Some(positions.clone()),
        })
        .collect();
    let provider_rw = factory.provider_rw().unwrap();
    provider_rw
        .save_blocks_with_tx_positions(chain.blocks.clone(), SaveBlocksMode::Full, &positions)
        .unwrap();
    provider_rw.commit().unwrap();

    // The rows are as asked.
    let formats: Vec<_> = static_files
        .jar_formats(StaticFileSegment::Transactions)
        .unwrap()
        .into_iter()
        .map(|(_, format)| format)
        .collect();
    let format = match rows {
        Rows::Plain => JarFormat::Plain,
        Rows::Checksummed => JarFormat::Checksummed,
        Rows::Positions | Rows::Mixed => JarFormat::TaggedTx,
    };
    assert_eq!(formats, vec![format; 4], "{rows:?}");
    for (tx, (number, _)) in chain.txs().into_iter().enumerate() {
        let row = static_files.tx_row_by_id(tx as TxNumber).unwrap().unwrap();
        let by_position = rows == Rows::Positions || (rows == Rows::Mixed && number % 2 == 1);
        assert_eq!(matches!(row, TxRow::Ref(_)), by_position, "{rows:?} tx {tx}");
    }

    let provider = BlockchainProvider::new(factory.clone()).unwrap();
    Built { factory, provider, resolver }
}

/// `read` is `expected`: the same EIP-2718 bytes and hash.
#[track_caller]
fn assert_tx(read: &TransactionSigned, expected: &TransactionSigned, what: &str) {
    assert_eq!(read.encoded_2718(), expected.encoded_2718(), "{what}");
    assert_eq!(read.tx_hash(), expected.tx_hash(), "{what}");
}

/// `read` is `expected`, transaction by transaction.
#[track_caller]
fn assert_txs(read: &[TransactionSigned], expected: &[TransactionSigned], what: &str) {
    assert_eq!(read.len(), expected.len(), "{what}");
    for (i, (read, expected)) in read.iter().zip(expected).enumerate() {
        assert_tx(read, expected, &format!("{what}, transaction {i}"));
    }
}

/// `read` is block `number` of `chain`: its header, its transactions and, if `senders`, their
/// senders.
#[track_caller]
fn assert_block(read: &RecoveredBlock<Block>, chain: &Chain, number: BlockNumber, what: &str) {
    let expected = chain.block(number);
    assert_eq!(read.header(), expected.header(), "{what}");
    assert_txs(&read.body().transactions, &expected.body().transactions, what);
    assert_eq!(read.senders(), expected.senders(), "{what}");
}

/// Every transaction read of `built` answers `chain`'s transactions.
fn assert_reads(built: &Built, chain: &Chain, rows: Rows) {
    let Built { factory, provider, .. } = built;
    let txs = chain.txs();
    let total = txs.len() as TxNumber;
    let all: Vec<_> = txs.iter().map(|(_, tx)| tx.clone()).collect();

    for (id, (number, expected)) in txs.iter().enumerate() {
        let id = id as TxNumber;
        let hash = *expected.tx_hash();
        let what = |read: &str| format!("{rows:?} {read} of tx {id}");
        assert_tx(&provider.transaction_by_id(id).unwrap().unwrap(), expected, &what("by id"));
        assert_tx(
            &provider.transaction_by_id_unhashed(id).unwrap().unwrap(),
            expected,
            &what("by id unhashed"),
        );
        assert_tx(
            &provider.transaction_by_hash(hash).unwrap().unwrap(),
            expected,
            &what("by hash"),
        );
        let (read, meta) = provider.transaction_by_hash_with_meta(hash).unwrap().unwrap();
        assert_tx(&read, expected, &what("by hash with meta"));
        let block = chain.block(*number);
        assert_eq!(
            (meta.tx_hash, meta.block_number, meta.block_hash, meta.index),
            (hash, *number, block.hash(), id - chain.tx_range(*number).start),
            "{}",
            what("meta")
        );
        // TN(TN-0202): the factory's own reads go through a database provider.
        assert_tx(&factory.transaction_by_id(id).unwrap().unwrap(), expected, &what("factory id"));
        assert_tx(
            &factory.transaction_by_id_unhashed(id).unwrap().unwrap(),
            expected,
            &what("factory id unhashed"),
        );
        assert_tx(
            &factory.transaction_by_hash(hash).unwrap().unwrap(),
            expected,
            &what("factory hash"),
        );
    }

    for number in 0..=TIP {
        let what = |read: &str| format!("{rows:?} {read} of block {number}");
        let expected = chain.block(number);
        let block_txs = &expected.body().transactions;
        assert_txs(
            &provider.transactions_by_block(number.into()).unwrap().unwrap(),
            block_txs,
            &what("transactions_by_block"),
        );
        assert_txs(
            &provider.transactions_by_block(expected.hash().into()).unwrap().unwrap(),
            block_txs,
            &what("transactions_by_block by hash"),
        );
        for id in [number.into(), expected.hash().into()] {
            let block = provider.block(id).unwrap().unwrap();
            assert_eq!(block.header, *expected.header(), "{}", what("block"));
            assert_txs(&block.body.transactions, block_txs, &what("block"));
        }
        let read = provider.recovered_block(number.into(), TransactionVariant::NoHash).unwrap();
        assert_block(&read.unwrap(), chain, number, &what("recovered_block"));
        let read = provider
            .sealed_block_with_senders(number.into(), TransactionVariant::WithHash)
            .unwrap();
        assert_block(&read.unwrap(), chain, number, &what("sealed_block_with_senders"));
    }

    for range in [0..=TIP, 2..=8, FAR..=FAR] {
        let what = |read: &str| format!("{rows:?} {read} of blocks {range:?}");
        let expected: Vec<_> = range.clone().map(|number| chain.block(number)).collect();
        let read = provider.transactions_by_block_range(range.clone()).unwrap();
        assert_eq!(read.len(), expected.len(), "{}", what("transactions_by_block_range"));
        for (read, expected) in read.iter().zip(&expected) {
            assert_txs(read, &expected.body().transactions, &what("transactions_by_block_range"));
        }
        let read = provider.block_range(range.clone()).unwrap();
        assert_eq!(read.len(), expected.len(), "{}", what("block_range"));
        for (read, expected) in read.iter().zip(&expected) {
            assert_eq!(read.header, *expected.header(), "{}", what("block_range"));
            assert_txs(
                &read.body.transactions,
                &expected.body().transactions,
                &what("block_range"),
            );
        }
        let read = provider.block_with_senders_range(range.clone()).unwrap();
        assert_eq!(read.len(), expected.len(), "{}", what("block_with_senders_range"));
        for (read, number) in read.iter().zip(range.clone()) {
            assert_block(read, chain, number, &what("block_with_senders_range"));
        }
        let read = provider.recovered_block_range(range.clone()).unwrap();
        assert_eq!(read.len(), expected.len(), "{}", what("recovered_block_range"));
        for (read, number) in read.iter().zip(range.clone()) {
            assert_block(read, chain, number, &what("recovered_block_range"));
        }
    }

    // Transaction ranges across blocks and jars.
    for range in
        [0..total, 1..total - 1, chain.tx_range(FAR).start - 1..chain.tx_range(FAR).end + 1]
    {
        let expected = &all[range.start as usize..range.end as usize];
        let what = format!("{rows:?} transactions_by_tx_range {range:?}");
        assert_txs(&provider.transactions_by_tx_range(range.clone()).unwrap(), expected, &what);
        assert_txs(&factory.transactions_by_tx_range(range.clone()).unwrap(), expected, &what);
        let mut hashes =
            factory.provider().unwrap().transaction_hashes_by_range(range.clone()).unwrap();
        hashes.sort_by_key(|(_, tx)| *tx);
        let expected: Vec<_> =
            range.clone().zip(expected).map(|(id, tx)| (*tx.tx_hash(), id)).collect();
        assert_eq!(hashes, expected, "{rows:?} transaction_hashes_by_range {range:?}");
    }
}

/// The [`TxBodyReadError`] in `err`, if any.
fn read_error(err: &ProviderError) -> Option<&TxBodyReadError> {
    match err {
        ProviderError::Other(err) => err.as_error().downcast_ref(),
        _ => None,
    }
}

/// Every read through `provider` that meets transaction `id` (of block `number`, hash `hash`),
/// as `Ok(())` or its error.
fn reads_of(
    provider: &Provider,
    id: TxNumber,
    hash: TxHash,
    number: BlockNumber,
) -> Vec<(&'static str, ProviderResult<()>)> {
    vec![
        ("transaction_by_id", provider.transaction_by_id(id).map(drop)),
        ("transaction_by_id_unhashed", provider.transaction_by_id_unhashed(id).map(drop)),
        ("transaction_by_hash", provider.transaction_by_hash(hash).map(drop)),
        ("transaction_by_hash_with_meta", provider.transaction_by_hash_with_meta(hash).map(drop)),
        ("transactions_by_block", provider.transactions_by_block(number.into()).map(drop)),
        (
            "transactions_by_block_range",
            provider.transactions_by_block_range(number..=number).map(drop),
        ),
        ("transactions_by_tx_range", provider.transactions_by_tx_range(id..id + 1).map(drop)),
        ("block", provider.block(number.into()).map(drop)),
        (
            "recovered_block",
            provider.recovered_block(number.into(), TransactionVariant::NoHash).map(drop),
        ),
        (
            "sealed_block_with_senders",
            provider
                .sealed_block_with_senders(number.into(), TransactionVariant::WithHash)
                .map(drop),
        ),
        ("block_range", provider.block_range(number..=number).map(drop)),
        ("block_with_senders_range", provider.block_with_senders_range(number..=number).map(drop)),
        ("recovered_block_range", provider.recovered_block_range(number..=number).map(drop)),
    ]
}

/// Every read of [`FAR`]'s first transaction through `built` fails, each error passing `check`.
#[track_caller]
fn assert_reads_fail(built: &Built, chain: &Chain, check: impl Fn(&ProviderError) -> bool) {
    let id = chain.tx_range(FAR).start;
    let hash = *chain.block(FAR).body().transactions[0].tx_hash();
    for (read, result) in reads_of(&built.provider, id, hash, FAR) {
        let err = result.expect_err(read);
        assert!(check(&err), "{read}: {err}");
    }
}

#[test]
fn position_rows_read_as_inline_rows() {
    let chain = chain();
    for rows in [Rows::Plain, Rows::Checksummed, Rows::Positions, Rows::Mixed] {
        let built = build(&chain, rows, true);
        assert_reads(&built, &chain, rows);
    }
}

#[test]
fn a_block_read_resolves_its_rows_in_one_call() {
    let chain = chain();
    let built = build(&chain, Rows::Positions, true);
    let calls = || std::mem::take(&mut *built.resolver.calls.lock());
    let far = (FAR, chain.positions[FAR as usize].to_vec());

    built.provider.block(FAR.into()).unwrap().unwrap();
    assert_eq!(calls(), vec![far.clone()]);
    built.provider.recovered_block(FAR.into(), TransactionVariant::NoHash).unwrap().unwrap();
    assert_eq!(calls(), vec![far.clone()]);
    built.provider.transactions_by_block(FAR.into()).unwrap().unwrap();
    assert_eq!(calls(), vec![far]);
    built.provider.block(EMPTY.into()).unwrap().unwrap();
    assert_eq!(calls(), vec![]);

    // A transaction range: one call per block it meets, with that block's positions in it.
    let range = chain.tx_range(FAR);
    built.provider.transactions_by_tx_range(range.start - 1..range.end - 1).unwrap();
    let before = FAR - 1;
    let last_of_before = *chain.positions[before as usize].last().unwrap();
    let mut first_of_far = chain.positions[FAR as usize].to_vec();
    first_of_far.pop();
    assert_eq!(calls(), vec![(before, vec![last_of_before]), (FAR, first_of_far)]);
}

#[test]
fn a_failing_resolver_fails_every_read() {
    let chain = chain();
    let built = build(&chain, Rows::Positions, true);
    *built.resolver.answer.lock() = Answer::Fail;
    assert_reads_fail(&built, &chain, |err| match err {
        ProviderError::Other(err) => err.as_error().downcast_ref::<MockFailure>().is_some(),
        _ => false,
    });
}

#[test]
fn a_short_answer_fails_every_read() {
    let chain = chain();
    let built = build(&chain, Rows::Positions, true);
    *built.resolver.answer.lock() = Answer::Short;
    assert_reads_fail(&built, &chain, |err| {
        matches!(read_error(err), Some(TxBodyReadError::WrongCount { block: FAR, .. }))
    });
}

#[test]
fn without_a_resolver_every_read_fails() {
    let chain = chain();
    let built = build(&chain, Rows::Positions, false);
    assert_reads_fail(&built, &chain, |err| {
        matches!(read_error(err), Some(TxBodyReadError::NoResolver { block: FAR, .. }))
    });
    // A chain stored inline needs none.
    let built = build(&chain, Rows::Checksummed, false);
    assert_reads(&built, &chain, Rows::Checksummed);
}

#[test]
fn a_read_by_hash_checks_the_resolved_hash() {
    let chain = chain();
    let built = build(&chain, Rows::Positions, true);
    *built.resolver.answer.lock() = Answer::Next;
    let id = chain.tx_range(FAR).start;
    let expected = &chain.block(FAR).body().transactions[0];
    let hash = *expected.tx_hash();
    let mismatch = |err: ProviderError| {
        assert!(
            matches!(
                read_error(&err),
                Some(TxBodyReadError::TxBodyMismatch { hash: asked, tx, .. })
                    if *asked == hash && *tx == id
            ),
            "{err}"
        )
    };
    mismatch(built.provider.transaction_by_hash(hash).unwrap_err());
    mismatch(built.provider.transaction_by_hash_with_meta(hash).unwrap_err());
    mismatch(built.factory.transaction_by_hash(hash).unwrap_err());
    // A read by number has no hash to check.
    let read = built.provider.transaction_by_id(id).unwrap().unwrap();
    assert_ne!(read.tx_hash(), expected.tx_hash());
}

#[test]
fn static_file_reads_refuse_position_rows() {
    let chain = chain();
    let built = build(&chain, Rows::Positions, true);
    let static_files = built.factory.static_file_provider();
    let total = chain.txs().len() as TxNumber;
    let id = chain.tx_range(FAR).start;
    let position = chain.positions[FAR as usize][0];
    let in_pack = |result: ProviderResult<()>| {
        let err = result.unwrap_err();
        assert!(
            matches!(
                read_error(&err),
                Some(TxBodyReadError::InConsensusPack { tx, position: at, .. })
                    if *tx == id && *at == position
            ),
            "{err}"
        );
    };
    in_pack(static_files.transaction_by_id(id).map(drop));
    in_pack(static_files.transaction_by_id_unhashed(id).map(drop));
    in_pack(static_files.transactions_by_tx_range(id..id + 1).map(drop));
    in_pack(static_files.transaction_hashes_by_range(id..id + 1).map(drop));
    let jar = static_files
        .get_segment_provider_for_transaction(StaticFileSegment::Transactions, id, None)
        .unwrap();
    in_pack(jar.transaction_by_id(id).map(drop));
    in_pack(jar.transaction_by_id_unhashed(id).map(drop));
    in_pack(jar.transactions_by_tx_range(id..id + 1).map(drop));
    assert_eq!(jar.tx_row_by_id(id).unwrap(), Some(TxRow::Ref(position)));
    assert_eq!(jar.tx_rows_by_tx_range(id..id + 1).unwrap(), vec![TxRow::Ref(position)]);
    // Never `None` or a shorter list, whichever row a range starts at.
    assert!(static_files.transactions_by_tx_range(0..total).is_err());
    assert!(static_files.transaction_hashes_by_range(0..total).is_err());
    assert_eq!(static_files.tx_rows_by_tx_range(0..total).unwrap().len(), total as usize);

    // The same reads of inline rows answer as upstream's.
    let inline = build(&chain, Rows::Checksummed, true);
    let static_files = inline.factory.static_file_provider();
    let expected = &chain.block(FAR).body().transactions[0];
    assert_tx(&static_files.transaction_by_id(id).unwrap().unwrap(), expected, "inline by id");
    assert_eq!(static_files.transactions_by_tx_range(0..total).unwrap().len(), total as usize);
    assert_eq!(static_files.transaction_hashes_by_range(0..total).unwrap().len(), total as usize);
}

#[test]
fn remove_blocks_above_resolves_position_rows() {
    let chain = chain();
    let kept = chain.tx_range(KEEP).end;
    for rows in [Rows::Checksummed, Rows::Positions, Rows::Mixed] {
        let built = build(&chain, rows, true);
        let provider_rw = built.factory.provider_rw().unwrap();
        provider_rw.remove_blocks_above(KEEP).unwrap();
        provider_rw.commit().unwrap();

        let provider = built.factory.provider().unwrap();
        assert_eq!(provider.last_block_number().unwrap(), KEEP, "{rows:?}");
        for (id, (number, tx)) in chain.txs().iter().enumerate() {
            let indexed = provider.transaction_id(*tx.tx_hash()).unwrap();
            let expected = (*number <= KEEP).then_some(id as TxNumber);
            assert_eq!(indexed, expected, "{rows:?} hash of tx {id}");
        }
        let all: Vec<_> = chain.txs().into_iter().map(|(_, tx)| tx).collect();
        assert_txs(
            &provider.transactions_by_tx_range(0..kept).unwrap(),
            &all[..kept as usize],
            &format!("{rows:?} after the removal"),
        );
    }

    // Without a resolver the removal fails, rather than leave the hashes of the removed
    // transactions indexed.
    let built = build(&chain, Rows::Positions, false);
    let provider_rw = built.factory.provider_rw().unwrap();
    let err = provider_rw.remove_blocks_above(KEEP).unwrap_err();
    assert!(matches!(read_error(&err), Some(TxBodyReadError::NoResolver { .. })), "{err}");
}

#[test]
fn a_position_row_outside_its_indexed_block_is_an_error() {
    let chain = chain();
    let built = build(&chain, Rows::Positions, true);
    // `TransactionBlocks` loses [`FAR`]'s entry, so it names the next block for its transactions.
    let provider_rw = built.factory.provider_rw().unwrap();
    provider_rw
        .tx_ref()
        .delete::<tables::TransactionBlocks>(chain.tx_range(FAR).end - 1, None)
        .unwrap();
    provider_rw.commit().unwrap();
    let id = chain.tx_range(FAR).start;
    let err = built.provider.transaction_by_id(id).unwrap_err();
    assert!(
        matches!(
            read_error(&err),
            Some(TxBodyReadError::NoBlock { tx, block: Some(block) }) if *tx == id && *block == FAR + 1
        ),
        "{err}"
    );
    // A read that knows the block still resolves it.
    let block = built.provider.block(FAR.into()).unwrap().unwrap();
    assert_txs(&block.body.transactions, &chain.block(FAR).body().transactions, "block");
}

#[test]
fn a_second_resolver_is_refused() {
    let chain = chain();
    let built = build(&chain, Rows::Positions, true);
    let other = Arc::new(MockResolver::default());
    let err = built.factory.static_file_provider().set_tx_body_resolver(other).unwrap_err();
    assert!(
        matches!(&err, ProviderError::Other(err) if err.as_error().downcast_ref::<TxBodyResolverInstalled>().is_some()),
        "{err}"
    );
    // The first one stays.
    assert_reads(&built, &chain, Rows::Positions);
}
