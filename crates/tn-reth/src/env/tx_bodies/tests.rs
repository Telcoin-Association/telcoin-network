//! Tests of the decoded-batch cache and the transaction resolver.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

use parking_lot::Mutex;
use tempfile::TempDir;
use tn_storage::consensus_pack::BatchReadError;
use tn_types::{Batch, Epoch, ExecHeader, B256};

use super::*;
use crate::test_utils::batch_with_transactions;

/// Bytes that do not decode as an EIP-2718 transaction.
const UNDECODABLE_TX: &[u8] = &[0xde, 0xad, 0xbe, 0xef];

/// A batch of `txs` signed transactions on the test chain, plus `extra` raw transactions after
/// them.
fn batch(txs: usize, extra: &[&[u8]]) -> Batch {
    let mut batch = batch_with_transactions(tn_types::test_chain_spec_arc(), txs, 0);
    batch.transactions.extend(extra.iter().map(|tx| tx.to_vec()));
    batch
}

/// A header naming batch `digest` of `epoch`.
fn header(epoch: Epoch, digest: B256) -> ExecHeader {
    ExecHeader { ommers_hash: digest, nonce: ((epoch as u64) << 32).into(), ..Default::default() }
}

/// The transactions of `batch` at `positions`, decoded as the resolver must return them.
fn expected(batch: &Batch, positions: &[u32]) -> Vec<TransactionSigned> {
    positions
        .iter()
        .map(|&p| {
            TransactionSigned::decode_2718_exact(&batch.transactions[p as usize]).expect("decodes")
        })
        .collect()
}

/// Resolve `positions` of `batch` (by its digest) in `epoch` through `source`.
fn resolve(
    source: &TxBodySource,
    epoch: Epoch,
    batch: &Batch,
    positions: &[u32],
) -> Result<Vec<TransactionSigned>, TxBodyError> {
    source.transactions(7, &header(epoch, batch.digest()), positions)
}

/// The kind of a resolve's error.
fn kind(result: Result<Vec<TransactionSigned>, TxBodyError>) -> TxBodyErrorKind {
    let err = result.expect_err("the resolve fails");
    assert_eq!(err.block, 7, "the error names the block");
    err.kind
}

// --- the cache --------------------------------------------------------------------------------

#[test]
fn cached_batch_keeps_each_transaction_and_its_weight() {
    let b = batch(0, &[b"a", b"", b"bcd"]);
    let cached = CachedBatch::of(&b).expect("small");
    assert_eq!(cached.len(), 3);
    assert_eq!(cached.tx(0), Some(&b"a"[..]));
    assert_eq!(cached.tx(1), Some(&b""[..]));
    assert_eq!(cached.tx(2), Some(&b"bcd"[..]));
    assert_eq!(cached.tx(3), None);
    assert_eq!(cached.weight(), 4 + 4 * 3 + 64);
}

/// 64 MiB splits into 56 MiB recent and 8 MiB cold; 0 into nothing.
#[test]
fn the_cache_budget_splits_seven_eighths_recent() {
    let mib = 1024 * 1024;
    assert_eq!(BatchCache::new(TX_BODY_CACHE_BYTES).budgets(), (56 * mib, 8 * mib));
    assert_eq!(BatchCache::new(0).budgets(), (0, 0));
    let (recent, cold) = BatchCache::new(1001).budgets();
    assert_eq!(recent + cold, 1001);
}

/// A cached batch of weight `weight`, under a fresh digest.
fn entry(seed: u8, weight: usize) -> (B256, Arc<CachedBatch>) {
    let tx = vec![seed; weight - 4 - 64];
    let cached = CachedBatch::of(&batch(0, &[&tx])).expect("small");
    assert_eq!(cached.weight(), weight);
    (B256::repeat_byte(seed), Arc::new(cached))
}

/// Each part charges its entries' weight, evicts the least recently used while over its budget,
/// never keeps an entry heavier than its budget, and charges a replaced entry once.
#[test]
fn the_cache_charges_each_entry_and_evicts_the_least_recently_used() {
    // recent part: 7/8 of 1600 = 1400 bytes, room for two 600-byte entries
    let cache = BatchCache::new(1600);
    let ((a, ba), (b, bb), (c, bc)) = (entry(1, 600), entry(2, 600), entry(3, 600));
    cache.insert_recent(a, ba.clone());
    cache.insert_recent(b, bb);
    assert_eq!(cache.used(), (1200, 0));
    assert!(cache.get(&a).is_some(), "touch a: b is now least recently used");
    cache.insert_recent(c, bc);
    assert_eq!(cache.used(), (1200, 0));
    assert_eq!(
        (cache.holds(&a), cache.holds(&b), cache.holds(&c)),
        ((true, false), (false, false), (true, false))
    );
    cache.insert_recent(a, ba);
    assert_eq!(cache.used(), (1200, 0), "a replaced entry is charged once");
    assert_eq!(cache.holds(&c), (true, false), "and evicts nothing");
    let (heavy, bh) = entry(4, 1500);
    cache.insert_recent(heavy, bh);
    assert_eq!(cache.holds(&heavy), (false, false), "heavier than the part");
    assert_eq!(cache.used(), (1200, 0), "and evicted nothing");
}

/// Pack reads fill only the cold part: a scan of many cold batches never evicts a recent one.
#[test]
fn a_cold_scan_does_not_evict_recent_batches() {
    let cache = BatchCache::new(8000); // 7000 recent, 1000 cold
    let (recent, br) = entry(1, 600);
    cache.insert_recent(recent, br);
    for seed in 10..60 {
        let (digest, cached) = entry(seed, 300);
        cache.insert_cold(digest, cached);
        assert!(cache.used().1 <= 1000);
    }
    assert_eq!(cache.holds(&recent), (true, false));
    assert_eq!(cache.used().0, 600);
    assert_eq!(cache.get(&recent).map(|(_, part)| part), Some(CachePart::Recent));
    assert_eq!(cache.get(&B256::repeat_byte(59)).map(|(_, part)| part), Some(CachePart::Cold));
}

/// A budget of 0 keeps nothing, in either part.
#[test]
fn a_zero_budget_keeps_nothing() {
    let cache = BatchCache::new(0);
    let (a, ba) = entry(1, 100);
    cache.insert_recent(a, ba.clone());
    cache.insert_cold(a, ba);
    assert!(cache.get(&a).is_none());
    assert_eq!(cache.used(), (0, 0));
}

// --- the resolver with MemoryBatches --------------------------------------------------------

/// A source reading its own (not the process-wide) memory batches, with no packs.
fn memory_source() -> (TxBodySource, Arc<MemoryBatches>) {
    let memory = Arc::new(MemoryBatches::default());
    (TxBodySource::new(TX_BODY_CACHE_BYTES).with_memory_batches(memory.clone()), memory)
}

/// Every arm a source with `MemoryBatches` and no packs reaches, and its answer.
#[test]
fn memory_batches_resolve_and_fail_by_arm() -> eyre::Result<()> {
    let (source, memory) = memory_source();
    let good = batch(3, &[UNDECODABLE_TX]);
    memory.insert(good.digest(), &good);

    // positions in any order, each its own transaction
    assert_eq!(resolve(&source, 1, &good, &[2, 0])?, expected(&good, &[2, 0]));
    assert!(resolve(&source, 1, &good, &[])?.is_empty(), "nothing asked");
    assert!(matches!(
        kind(resolve(&source, 1, &good, &[0, 4])),
        TxBodyErrorKind::PositionOutOfRange { pos: 4, len: 4 }
    ));
    assert!(matches!(
        kind(resolve(&source, 1, &good, &[3])),
        TxBodyErrorKind::Decode { pos: 3, .. }
    ));
    assert!(matches!(
        kind(source.transactions(7, &header(1, B256::ZERO), &[0])),
        TxBodyErrorKind::NoBatchDigest
    ));
    let other = batch(1, &[]);
    assert!(matches!(kind(resolve(&source, 1, &other, &[0])), TxBodyErrorKind::BatchMissing));

    // without memory batches and packs
    let bare = TxBodySource::new(TX_BODY_CACHE_BYTES);
    assert!(matches!(kind(resolve(&bare, 1, &good, &[0])), TxBodyErrorKind::PacksNotAttached));
    // the provider error names the block, the epoch and the digest
    let err = TxBodyResolver::<TNPrimitives>::resolve(&bare, 7, &header(1, good.digest()), &[0])
        .expect_err("no packs");
    let text = err.to_string();
    assert!(text.contains("block 7") && text.contains("epoch 1"), "{text}");
    assert!(text.contains(&good.digest().to_string()), "{text}");
    Ok(())
}

/// A recent fill answers before memory, and memory answers before packs.
#[test]
fn the_cache_answers_before_memory_batches() -> eyre::Result<()> {
    let (source, memory) = memory_source();
    let b = batch(2, &[]);
    memory.insert(b.digest(), &b);
    assert_eq!(source.batch(1, b.digest()).expect("memory").1, BatchSource::Memory);
    source.fill_recent([(b.digest(), &b)]);
    assert_eq!(
        source.batch(1, b.digest()).expect("cached").1,
        BatchSource::Cache(CachePart::Recent)
    );
    assert_eq!(resolve(&source, 1, &b, &[1, 0])?, expected(&b, &[1, 0]));
    Ok(())
}

// --- the resolver with a fake pack source --------------------------------------------------

/// A scripted open failure.
#[derive(Debug, Clone, Copy)]
enum Scripted {
    Missing,
    Installing,
    Unsealed,
    Legacy,
}

/// A batch read failure.
type MakeError = fn() -> BatchReadError;

/// One closed epoch's pack as a reader opened it: its batches then.
struct FakeEpoch {
    batches: HashMap<B256, Batch>,
    error: Option<MakeError>,
}

impl EpochBatches for FakeEpoch {
    fn batch(&self, digest: B256) -> Result<Option<Batch>, BatchReadError> {
        if let Some(error) = self.error {
            return Err(error());
        }
        Ok(self.batches.get(&digest).cloned())
    }
}

/// Packs in memory: one live epoch and closed ones, with scripted failures.
#[derive(Debug, Default)]
struct FakePacks {
    generation: AtomicU64,
    live_epoch: Mutex<Option<Epoch>>,
    live: Mutex<HashMap<B256, Batch>>,
    live_error: Mutex<Option<MakeError>>,
    closed: Mutex<HashMap<Epoch, HashMap<B256, Batch>>>,
    read_error: Mutex<Option<MakeError>>,
    script: Mutex<VecDeque<Scripted>>,
    opens: AtomicUsize,
    live_reads: AtomicUsize,
}

impl PackBatches for Arc<FakePacks> {
    fn install_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    fn live_batch(
        &self,
        epoch: Epoch,
        digest: B256,
    ) -> Option<Result<Option<Batch>, BatchReadError>> {
        (*self.live_epoch.lock() == Some(epoch)).then(|| {
            self.live_reads.fetch_add(1, Ordering::SeqCst);
            match *self.live_error.lock() {
                Some(error) => Err(error()),
                None => Ok(self.live.lock().get(&digest).cloned()),
            }
        })
    }

    fn open(&self, epoch: Epoch) -> Result<Arc<dyn EpochBatches>, PackOpenError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        match self.script.lock().pop_front() {
            Some(Scripted::Missing) => return Err(PackOpenError::Missing),
            Some(Scripted::Installing) => return Err(PackOpenError::Installing),
            Some(Scripted::Unsealed) => return Err(PackOpenError::Unsealed),
            Some(Scripted::Legacy) => return Err(PackOpenError::Failed(BatchReadError::Legacy)),
            None => {}
        }
        let batches = self.closed.lock().get(&epoch).cloned().ok_or(PackOpenError::Missing)?;
        Ok(Arc::new(FakeEpoch { batches, error: *self.read_error.lock() }))
    }
}

impl FakePacks {
    /// Put `batch` in closed epoch `epoch`.
    fn close(&self, epoch: Epoch, batch: &Batch) {
        self.closed.lock().entry(epoch).or_default().insert(batch.digest(), batch.clone());
    }
}

/// A source with a cache of `cache_bytes` reading fake packs.
fn pack_source(cache_bytes: usize) -> (TxBodySource, Arc<FakePacks>) {
    let packs = Arc::new(FakePacks::default());
    let source = TxBodySource::new(cache_bytes);
    source.attach_packs(PackSource::new(packs.clone())).expect("first attach");
    assert!(source.attach_packs(PackSource::new(packs.clone())).is_err(), "attached once");
    (source, packs)
}

/// The live epoch's pack answers, and each of its failures is its own arm.
#[test]
fn the_live_pack_resolves_and_fails_by_arm() -> eyre::Result<()> {
    let (source, packs) = pack_source(0);
    let b = batch(2, &[UNDECODABLE_TX]);
    *packs.live_epoch.lock() = Some(4);
    packs.live.lock().insert(b.digest(), b.clone());
    assert_eq!(source.batch(4, b.digest()).expect("live").1, BatchSource::LivePack);
    assert_eq!(resolve(&source, 4, &b, &[1])?, expected(&b, &[1]));
    assert!(matches!(
        kind(resolve(&source, 4, &b, &[3])),
        TxBodyErrorKind::PositionOutOfRange { pos: 3, len: 3 }
    ));
    assert!(matches!(kind(resolve(&source, 4, &b, &[2])), TxBodyErrorKind::Decode { pos: 2, .. }));
    assert!(matches!(
        kind(resolve(&source, 4, &batch(1, &[]), &[0])),
        TxBodyErrorKind::BatchMissing
    ));
    *packs.live_error.lock() =
        Some(|| BatchReadError::DigestMismatch { pos: 9, found: B256::repeat_byte(9) });
    assert!(matches!(
        kind(resolve(&source, 4, &b, &[0])),
        TxBodyErrorKind::Read(e) if matches!(*e, BatchReadError::DigestMismatch { pos: 9, .. })
    ));
    // sealed under the read: read as a closed epoch
    *packs.live_error.lock() = Some(|| BatchReadError::Closed);
    packs.close(4, &b);
    assert_eq!(source.batch(4, b.digest()).expect("closed").1, BatchSource::ClosedPack);
    assert_eq!(packs.opens.load(Ordering::SeqCst), 1);
    Ok(())
}

/// A closed epoch's pack answers, and each of its failures is its own arm.
#[test]
fn a_closed_pack_resolves_and_fails_by_arm() -> eyre::Result<()> {
    let (source, packs) = pack_source(0);
    let b = batch(2, &[]);
    packs.close(2, &b);
    assert_eq!(source.batch(2, b.digest()).expect("closed").1, BatchSource::ClosedPack);
    assert_eq!(resolve(&source, 2, &b, &[0, 1])?, expected(&b, &[0, 1]));
    assert!(matches!(kind(resolve(&source, 3, &b, &[0])), TxBodyErrorKind::PackMissing));
    assert!(matches!(
        kind(resolve(&source, 2, &batch(1, &[]), &[0])),
        TxBodyErrorKind::BatchMissing
    ));

    packs.script.lock().push_back(Scripted::Missing);
    packs.generation.fetch_add(1, Ordering::SeqCst); // drop the kept reader
    assert!(matches!(kind(resolve(&source, 2, &b, &[0])), TxBodyErrorKind::PackMissing));
    packs.script.lock().push_back(Scripted::Legacy);
    assert!(matches!(
        kind(resolve(&source, 2, &b, &[0])),
        TxBodyErrorKind::Read(e) if matches!(*e, BatchReadError::Legacy)
    ));
    *packs.read_error.lock() = Some(|| BatchReadError::NotBatch { pos: 3 });
    assert!(matches!(
        kind(resolve(&source, 2, &b, &[0])),
        TxBodyErrorKind::Read(e) if matches!(*e, BatchReadError::NotBatch { pos: 3 })
    ));
    Ok(())
}

/// An unsealed or mid-install pack is tried again until it opens, for up to a second; then the
/// read fails.
#[test]
fn an_unsealed_or_installing_pack_is_waited_for() -> eyre::Result<()> {
    let (source, packs) = pack_source(0);
    let b = batch(1, &[]);
    packs.close(5, &b);
    packs.script.lock().extend([Scripted::Unsealed, Scripted::Installing, Scripted::Unsealed]);
    assert_eq!(resolve(&source, 5, &b, &[0])?, expected(&b, &[0]));
    assert_eq!(packs.opens.load(Ordering::SeqCst), 4, "three retries, then the open");

    packs.generation.fetch_add(1, Ordering::SeqCst);
    packs.script.lock().extend([Scripted::Unsealed; 200]);
    let started = Instant::now();
    assert!(matches!(kind(resolve(&source, 5, &b, &[0])), TxBodyErrorKind::HandoffTimeout(_)));
    assert!(started.elapsed() >= PACK_WAIT, "{:?}", started.elapsed());
    Ok(())
}

/// A kept reader that misses a batch is opened again once (an install may have replaced a
/// partial copy since it was opened); a reader opened by the read is trusted.
#[test]
fn a_kept_reader_that_misses_is_opened_again_once() -> eyre::Result<()> {
    let (source, packs) = pack_source(0);
    let (a, b) = (batch(1, &[]), batch(2, &[]));
    packs.close(3, &a);
    assert_eq!(resolve(&source, 3, &a, &[0])?, expected(&a, &[0]));
    assert_eq!(packs.opens.load(Ordering::SeqCst), 1);
    // a fuller copy of epoch 3 is installed; the kept reader holds the old one
    packs.close(3, &b);
    assert_eq!(resolve(&source, 3, &b, &[1])?, expected(&b, &[1]));
    assert_eq!(packs.opens.load(Ordering::SeqCst), 2, "opened again once");
    let absent = batch(3, &[]);
    assert!(matches!(kind(resolve(&source, 3, &absent, &[0])), TxBodyErrorKind::BatchMissing));
    assert_eq!(packs.opens.load(Ordering::SeqCst), 3, "a kept reader's miss opens once more");
    assert!(matches!(kind(resolve(&source, 3, &absent, &[0])), TxBodyErrorKind::BatchMissing));
    assert_eq!(packs.opens.load(Ordering::SeqCst), 4);
    Ok(())
}

/// At most four readers stay open, most recently used first, and all are dropped when the
/// install generation moves.
#[test]
fn readers_follow_the_install_generation_and_stay_at_four() -> eyre::Result<()> {
    let (source, packs) = pack_source(0);
    let batches: Vec<Batch> = (1..=5).map(|n| batch(n, &[])).collect();
    for (epoch, b) in (1..).zip(&batches) {
        packs.close(epoch, b);
        resolve(&source, epoch, b, &[0])?;
    }
    let readers = source.packs().expect("attached");
    assert_eq!(readers.open_epochs(), [5, 4, 3, 2]);
    resolve(&source, 3, &batches[2], &[0])?;
    assert_eq!(readers.open_epochs(), [3, 5, 4, 2], "a hit moves to the front");
    assert_eq!(packs.opens.load(Ordering::SeqCst), 5);

    packs.generation.fetch_add(1, Ordering::SeqCst);
    resolve(&source, 4, &batches[3], &[0])?;
    assert_eq!(readers.open_epochs(), [4], "every reader dropped, one opened");
    assert_eq!(packs.opens.load(Ordering::SeqCst), 6);
    Ok(())
}

/// A pack read goes into the cold part, and the next read of that batch reads no pack; with a
/// budget of 0 every read reads the pack.
#[test]
fn a_pack_read_fills_the_cold_part() -> eyre::Result<()> {
    let b = batch(2, &[]);
    let (source, packs) = pack_source(TX_BODY_CACHE_BYTES);
    *packs.live_epoch.lock() = Some(6);
    packs.live.lock().insert(b.digest(), b.clone());
    resolve(&source, 6, &b, &[0])?;
    assert_eq!(source.cache().holds(&b.digest()), (false, true));
    assert_eq!(source.batch(6, b.digest()).expect("cached").1, BatchSource::Cache(CachePart::Cold));
    assert_eq!(packs.live_reads.load(Ordering::SeqCst), 1, "the hit read no pack");

    let (source, packs) = pack_source(0);
    *packs.live_epoch.lock() = Some(6);
    packs.live.lock().insert(b.digest(), b.clone());
    for _ in 0..3 {
        assert_eq!(resolve(&source, 6, &b, &[1])?, expected(&b, &[1]));
    }
    assert_eq!(packs.live_reads.load(Ordering::SeqCst), 3, "no cache");
    Ok(())
}

/// A missing epoch counts as mid-install while its old directory is set aside, or once its data
/// log is back; a directory without a data log is missing.
#[test]
fn a_missing_pack_is_mid_install_only_beside_an_install() -> eyre::Result<()> {
    let dir = TempDir::new()?;
    assert!(!ChainPacks::installing(dir.path(), 3), "nothing there");
    std::fs::create_dir(dir.path().join("epoch-3"))?;
    assert!(!ChainPacks::installing(dir.path(), 3), "a directory without its data log");
    std::fs::create_dir(dir.path().join("epoch-3.replaced"))?;
    assert!(ChainPacks::installing(dir.path(), 3), "the old copy set aside");
    std::fs::remove_dir(dir.path().join("epoch-3.replaced"))?;
    std::fs::write(dir.path().join("epoch-3").join(tn_storage::consensus_pack::DATA_NAME), b"")?;
    assert!(ChainPacks::installing(dir.path(), 3), "the new copy renamed in");
    assert!(!ChainPacks::installing(dir.path(), 4), "another epoch");
    Ok(())
}

// --- end to end: the engine's persist on a pointer datadir ------------------------------------

/// Pointer datadirs are storage v2, which runs on the tn-lmdb build only.
#[cfg(not(feature = "mdbx"))]
mod end_to_end {
    use super::*;
    use reth_provider::{
        providers::TxRow, DatabaseProviderFactory as _, StaticFileProviderFactory as _,
        TransactionsProvider as _,
    };
    use tn_types::{
        test_genesis, Address, CertifiedBatch, ConsensusHeader, ConsensusOutput,
        Encodable2718 as _, SealedHeader, TaskManager,
    };

    use crate::{
        env::OutputBuildScope,
        payload::TNPayload,
        test_utils::{consensus_output_for_tests, seeded_genesis_from_random_batches},
        DatadirFormat, NewCanonicalChain, RethChainSpec, StorageLayout, TxBodies,
    };

    /// Consensus output `number` with one certificate carrying `batches`, each under its digest.
    fn output_with(number: u64, batches: Vec<Batch>) -> ConsensusOutput {
        let base = consensus_output_for_tests(number as u32, 0, number, false);
        let digests: Vec<B256> = batches.iter().map(Batch::digest).collect();
        ConsensusOutput::new(
            base.sub_dag().clone(),
            ConsensusHeader::default().digest(),
            number,
            false,
            digests.into(),
            vec![CertifiedBatch { address: Address::ZERO, batches }],
        )
    }

    /// Execute `output` on `env` as the engine does: recover the signers, build one block per batch
    /// under its digest, persist the blocks with their positions, then hand the persisted batches
    /// to the env's cache fill. Returns the last block's header.
    fn execute_as_the_engine(
        env: &RethEnv,
        mut parent: SealedHeader,
        output: &ConsensusOutput,
    ) -> eyre::Result<SealedHeader> {
        let mut scope = OutputBuildScope::new();
        let cims = env.canonical_in_memory_state();
        let (mut blocks, mut positions) = (Vec::new(), Vec::new());
        for (index, recovered) in RethEnv::recover_output_signers(output).into_iter().enumerate() {
            let digest = output.get_batch_digest(index).unwrap_or_default();
            let payload = TNPayload::new_for_test_with(
                parent.clone(),
                output,
                Address::repeat_byte(0xbe),
                digest,
                digest,
            );
            let built = env.build_block_from_recovered_payload(payload, recovered, &mut scope)?;
            parent = built.block.recovered_block.clone_sealed_header();
            cims.update_chain(NewCanonicalChain::Commit { new: vec![built.block.clone()] });
            cims.set_canonical_head(parent.clone());
            blocks.push(built.block);
            positions.push(built.tx_positions);
        }
        env.persist_executed_output(&blocks, &positions)?;
        env.cache_persisted_batches(output, &positions);
        Ok(parent)
    }

    /// On a pointer datadir (the jar policy on before genesis), the engine's persist writes
    /// position rows (tag 1) into a tagged jar, and every transaction reads back through the
    /// resolver exactly as the same chain stored inline reads it: by number, by range and by
    /// hash, compared by encoding and hash. The inline datadir's rows are inline.
    #[tokio::test]
    async fn persisted_position_rows_read_back_as_the_inline_chain() -> eyre::Result<()> {
        // an undecodable transaction between the decodable ones: its position is skipped
        let mut a = batch_with_transactions(tn_types::test_chain_spec_arc(), 3, 0);
        let b = batch_with_transactions(tn_types::test_chain_spec_arc(), 2, 0);
        let (genesis, ..) = seeded_genesis_from_random_batches(test_genesis(), [&a, &b]);
        a.transactions.insert(1, UNDECODABLE_TX.to_vec());
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let output = output_with(1, vec![a, b]);

        let tmp = TempDir::new()?;
        let tm = TaskManager::new("tx bodies end to end");
        let mut read = Vec::new();
        for tx_bodies in [TxBodies::Inline, TxBodies::Pointers] {
            let format = DatadirFormat { layout: StorageLayout::V2, tx_bodies };
            let path = tmp.path().join(tx_bodies.to_string());
            let env = RethEnv::new_for_temp_chain_with_format(chain.clone(), &path, &tm, format)?;
            assert_eq!(env.datadir_format(), format);
            execute_as_the_engine(&env, chain.sealed_genesis_header(), &output)?;

            let static_files = env.blockchain_provider().static_file_provider();
            let rows = static_files.tx_rows_by_tx_range(0..4)?;
            let positions: Vec<Option<u32>> = rows
                .iter()
                .map(|row| match row {
                    TxRow::Ref(position) => Some(*position),
                    TxRow::Inline(_) => None,
                })
                .collect();
            match tx_bodies {
                TxBodies::Inline => assert_eq!(positions, [None; 4]),
                TxBodies::Pointers => {
                    assert_eq!(
                        positions,
                        [Some(0), Some(2), Some(3), Some(0)],
                        "a skips position 1"
                    )
                }
            }
            // the committed rows, through the database provider (the in-memory blocks are not read)
            let provider = env.blockchain_provider().database_provider_ro()?;
            let by_range = provider.transactions_by_tx_range(0..4)?;
            let mut by_number = Vec::new();
            for n in 0..4 {
                let tx = provider.transaction_by_id(n)?.expect("stored");
                let hash = *tx.tx_hash();
                assert_eq!(provider.transaction_by_hash(hash)?.map(|t| *t.tx_hash()), Some(hash));
                by_number.push(tx);
            }
            assert_eq!(by_range.len(), 4);
            for (x, y) in by_range.iter().zip(&by_number) {
                assert_eq!(x.encoded_2718(), y.encoded_2718());
            }
            read.push(by_number);
        }
        let (inline, pointers) = (&read[0], &read[1]);
        for (x, y) in inline.iter().zip(pointers) {
            assert_eq!(x.encoded_2718(), y.encoded_2718());
            assert_eq!(x.tx_hash(), y.tx_hash());
        }
        Ok(())
    }
}
