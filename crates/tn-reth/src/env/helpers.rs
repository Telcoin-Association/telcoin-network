//! Helper methods for retrieving state.

use std::{ops::RangeInclusive, sync::Arc};

use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
use reth_chain_state::CanonicalInMemoryState;
use reth_chainspec::BaseFeeParams;
use reth_errors::ProviderError;
use reth_primitives_traits::SignerRecoverable as _;
use reth_provider::{
    AccountReader as _, BlockBodyIndicesProvider as _, BlockIdReader as _, BlockNumReader as _,
    BlockReader as _, CanonStateNotificationStream, CanonStateSubscriptions as _, Chain,
    ChainStateBlockReader as _, DatabaseProviderFactory as _, ExecutionOutcome,
    HeaderProvider as _, ReceiptProvider as _, StateProviderBox, StateProviderFactory as _,
    TransactionVariant, TransactionsProvider as _,
};
use tn_types::{
    Account, Address, BlockHashOrNumber, BlockNumHash, BlockNumber, Bytes, ExecHeader, Receipt,
    Recovered, SealedBlock, SealedHeader, TransactionMeta, TransactionSigned, TxHash, TxNumber,
    B256,
};

use crate::{
    error::{TnRethError, TnRethResult},
    BlockWithSenders, ChainSpec, RethEnv,
};

/// One transaction in the chain-wide sequential transaction feed.
///
/// Storage assigns every mined transaction a sequential [`TxNumber`]; this entry pairs the
/// recovered transaction with that number and the containing block's context so consumers can
/// serve "latest N transactions" style queries without touching block bodies of empty blocks.
#[derive(Clone, Debug)]
pub struct TxFeedEntry {
    /// Sequential, chain-wide transaction number (position in the global feed).
    pub tx_number: TxNumber,
    /// The signed transaction with its sender attached.
    pub transaction: Recovered<TransactionSigned>,
    /// Number of the block containing this transaction.
    pub block_number: BlockNumber,
    /// Hash of the block containing this transaction.
    pub block_hash: B256,
    /// Timestamp of the containing block.
    pub timestamp: u64,
    /// Zero-based index of the transaction within its block.
    pub index: u64,
}

impl RethEnv {
    /// Return a channel reciever that will return each canonical block in turn.
    pub fn canonical_block_stream(&self) -> CanonStateNotificationStream {
        self.inner.blockchain_provider.canonical_state_stream()
    }

    /// Return the chainspec for this instance.
    pub fn chainspec(&self) -> ChainSpec {
        ChainSpec(self.node_config().chain.clone())
    }

    /// Return the canonical in-memory state.
    pub fn canonical_in_memory_state(&self) -> CanonicalInMemoryState {
        self.inner.blockchain_provider.canonical_in_memory_state()
    }

    /// Look up and return the sealed header for hash.
    pub fn sealed_header_by_hash(&self, hash: B256) -> TnRethResult<Option<SealedHeader>> {
        Ok(self.inner.blockchain_provider.sealed_header_by_hash(hash)?)
    }

    /// Look up and return the sealed header for block number.
    pub fn sealed_header_by_number(&self, number: u64) -> TnRethResult<Option<SealedHeader>> {
        Ok(self.inner.blockchain_provider.database_provider_ro()?.sealed_header(number)?)
    }

    /// Look up and return the sealed block for number.
    pub fn sealed_block_by_number(&self, number: u64) -> TnRethResult<Option<SealedBlock>> {
        Ok(self
            .inner
            .blockchain_provider
            .sealed_block_with_senders(
                BlockHashOrNumber::Number(number),
                TransactionVariant::NoHash,
            )?
            .map(|b| b.clone_sealed_block()))
    }

    /// Look up and return the sealed header (with senders) for hash.
    pub fn sealed_block_with_senders(
        &self,
        id: BlockHashOrNumber,
    ) -> TnRethResult<Option<BlockWithSenders>> {
        Ok(self
            .inner
            .blockchain_provider
            .sealed_block_with_senders(id, TransactionVariant::NoHash)?)
    }

    /// Return the blocks with senders for a range of block numbers.
    pub fn block_with_senders_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> TnRethResult<Vec<BlockWithSenders>> {
        Ok(self.inner.blockchain_provider.block_with_senders_range(range)?)
    }

    /// Return the blocks for a range of block numbers.
    pub fn blocks_for_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> TnRethResult<Vec<SealedHeader>> {
        Ok(self.inner.blockchain_provider.sealed_headers_range(range)?)
    }

    /// Build a [`Chain`] from a historical block in the database.
    ///
    /// Reads the block with recovered senders and its receipts, then constructs
    /// a `Chain` suitable for ExEx replay notifications.
    ///
    /// Returns `None` if the block does not exist in the database.
    ///
    /// # Replay fidelity
    ///
    /// The returned `Chain` carries an **empty `BundleState`**: account/storage
    /// diffs are already committed to the DB at execution time and are not
    /// re-derived here. ExExes that need historical state diffs must read them
    /// from the provider by block number. Live `ChainExecuted` notifications
    /// (from `finish_executing_output`) *do* carry the full `BundleState`.
    pub fn replay_block_as_chain(
        &self,
        block_number: BlockNumber,
    ) -> TnRethResult<Option<Arc<reth_provider::Chain>>> {
        // Read block with senders
        let Some(block) = self.inner.blockchain_provider.sealed_block_with_senders(
            BlockHashOrNumber::Number(block_number),
            TransactionVariant::NoHash,
        )?
        else {
            return Ok(None);
        };

        // Read receipts for this block. The block exists (read above), so missing
        // receipts indicate a DB inconsistency; an empty block legitimately
        // returns `Some(vec![])`. Treat `None` as an error rather than silently
        // yielding an empty receipt set (which would make a non-empty block look
        // empty to a stateful indexer).
        let receipts = self
            .inner
            .blockchain_provider
            .receipts_by_block(BlockHashOrNumber::Number(block_number))?
            .ok_or(TnRethError::ReplayReceiptsMissing(block_number))?;

        // Construct a minimal ExecutionOutcome with receipts only (no bundle state)
        let execution_outcome = ExecutionOutcome::new(
            Default::default(), // empty BundleState — state already committed to DB
            vec![receipts],
            block_number,
            Vec::new(), // no requests
        );

        Ok(Some(Arc::new(Chain::new(vec![block], execution_outcome, Default::default()))))
    }

    /// Return the head header from the reth db.
    pub fn lookup_head(&self) -> TnRethResult<SealedHeader> {
        let head = self.node_config().lookup_head(&self.inner.blockchain_provider)?;
        let header = self
            .inner
            .blockchain_provider
            .sealed_header(head.number)?
            .expect("Failed to retrieve sealed header from head's block number");
        Ok(header)
    }

    /// If a dubug max round is set then return it.
    pub fn get_debug_max_round(&self) -> Option<u64> {
        self.node_config().debug.max_block
    }

    /// Helper to get the gas price based on the provider's latest header.
    pub fn get_gas_price(&self) -> TnRethResult<u128> {
        let header = self.lookup_head()?;
        Ok(header.next_block_base_fee(BaseFeeParams::ethereum()).unwrap_or_default().into())
    }

    /// Return the execution header for hash if available.
    pub fn header(&self, hash: B256) -> TnRethResult<Option<ExecHeader>> {
        Ok(self.inner.blockchain_provider.header(hash)?)
    }

    /// Return the execution header for block number if available.
    pub fn header_by_number(&self, block_num: u64) -> TnRethResult<Option<ExecHeader>> {
        Ok(self.inner.blockchain_provider.database_provider_ro()?.header_by_number(block_num)?)
    }

    /// Return the finalized header, sealed with its hash, if available.
    ///
    /// Number and hash come from one logical read (the finalized num/hash pair, then the header
    /// looked up by that hash), so callers can pin block ranges, epoch classification, and state
    /// reads to this single header without consulting a second source (see `catchup_accumulator`
    /// and the epoch-entry base-fee seeding in the epoch manager, which pair this with
    /// [`Self::epoch_state_at_header`]).
    pub fn finalized_header(&self) -> TnRethResult<Option<SealedHeader>> {
        let Some(finalized_num_hash) = self.finalized_block_num_hash()? else {
            return Ok(None);
        };
        self.sealed_header_by_hash(finalized_num_hash.hash)
    }

    /// Return the latest canonical block number.
    pub fn last_block_number(&self) -> TnRethResult<u64> {
        Ok(self.inner.blockchain_provider.database_provider_ro()?.last_block_number().unwrap_or(0))
    }

    /// Return the block number and hash for the current canonical tip.
    ///
    /// This checks the canonical-in-memory-state.
    pub fn canonical_tip(&self) -> SealedHeader {
        self.inner.blockchain_provider.canonical_in_memory_state().get_canonical_head()
    }

    /// If available return the finalized block number and hash.
    ///
    /// This checks the canonical-in-memory-state.
    pub fn finalized_block_num_hash(&self) -> TnRethResult<Option<BlockNumHash>> {
        Ok(self.inner.blockchain_provider.finalized_block_num_hash()?)
    }

    /// Returns the block number of the last finialized block.
    pub fn last_finalized_block_number(&self) -> TnRethResult<u64> {
        Ok(self
            .inner
            .blockchain_provider
            .database_provider_ro()?
            .last_finalized_block_number()?
            .unwrap_or(0))
    }

    /// Provide the state for the latest block in this instance.
    pub fn latest(&self) -> TnRethResult<StateProviderBox> {
        Ok(self.inner.blockchain_provider.latest()?)
    }

    /// Look up a transaction by hash, returning it with its sender and block metadata
    /// (block number/hash, index in block, base fee, block timestamp).
    pub fn transaction_by_hash_with_meta(
        &self,
        hash: TxHash,
    ) -> TnRethResult<Option<(Recovered<TransactionSigned>, TransactionMeta)>> {
        let Some((tx, meta)) =
            self.inner.blockchain_provider.transaction_by_hash_with_meta(hash)?
        else {
            return Ok(None);
        };

        // prefer the sender persisted at execution time (two point reads, no ecrecover)
        let sender = match self.inner.blockchain_provider.transaction_id(hash)? {
            Some(id) => self.inner.blockchain_provider.transaction_sender(id)?,
            None => None,
        };
        let sender = match sender {
            Some(sender) => sender,
            None => tx.recover_signer()?,
        };

        Ok(Some((Recovered::new_unchecked(tx, sender), meta)))
    }

    /// Return the receipt for a transaction hash, if the transaction has been mined.
    ///
    /// `Receipt::cumulative_gas_used` is cumulative within the block; use
    /// [`Self::receipt_by_hash_with_gas_used`] for this transaction's own gas.
    pub fn receipt_by_hash(&self, hash: TxHash) -> TnRethResult<Option<Receipt>> {
        Ok(self.inner.blockchain_provider.receipt_by_hash(hash)?)
    }

    /// Return the receipt for a transaction hash together with the gas used by that
    /// transaction alone (the delta of cumulative gas vs. the previous receipt in the block).
    pub fn receipt_by_hash_with_gas_used(
        &self,
        hash: TxHash,
    ) -> TnRethResult<Option<(Receipt, u64)>> {
        let Some(id) = self.inner.blockchain_provider.transaction_id(hash)? else {
            return Ok(None);
        };
        let Some(receipt) = self.inner.blockchain_provider.receipt(id)? else {
            return Ok(None);
        };
        let block = self
            .inner
            .blockchain_provider
            .block_by_transaction_id(id)?
            .ok_or(ProviderError::TransactionNotFound(id.into()))?;
        let indices = self
            .inner
            .blockchain_provider
            .block_body_indices(block)?
            .ok_or(ProviderError::BlockBodyIndicesNotFound(block))?;

        let gas_used = if id == indices.first_tx_num() {
            receipt.cumulative_gas_used
        } else {
            let prev = self
                .inner
                .blockchain_provider
                .receipt(id - 1)?
                .ok_or(ProviderError::ReceiptNotFound((id - 1).into()))?;
            receipt.cumulative_gas_used.saturating_sub(prev.cumulative_gas_used)
        };

        Ok(Some((receipt, gas_used)))
    }

    /// Return all receipts for a block by hash or number.
    ///
    /// `Ok(None)` means the block is unknown; an empty vec means the block has no transactions.
    pub fn receipts_by_block(
        &self,
        block: BlockHashOrNumber,
    ) -> TnRethResult<Option<Vec<Receipt>>> {
        Ok(self.inner.blockchain_provider.receipts_by_block(block)?)
    }

    /// Total number of transactions ever mined on the canonical chain.
    ///
    /// The latest transaction's [`TxNumber`] is `total - 1`; serve "latest N, newest-first"
    /// pages by reading descending [`TxNumber`] ranges via
    /// [`Self::transactions_by_tx_range_with_meta`].
    pub fn total_transactions(&self) -> TnRethResult<u64> {
        let tip = self.last_block_number()?;
        Ok(self
            .inner
            .blockchain_provider
            .block_body_indices(tip)?
            .map(|indices| indices.next_tx_num())
            .unwrap_or(0))
    }

    /// Return the transactions in a chain-wide [`TxNumber`] range (inclusive), each with its
    /// sender and containing-block metadata.
    ///
    /// Cost scales with the number of transactions returned, not the number of blocks the
    /// range spans: empty blocks are never visited. Ranges extending past the newest
    /// transaction are clamped to what exists.
    pub fn transactions_by_tx_range_with_meta(
        &self,
        range: RangeInclusive<TxNumber>,
    ) -> TnRethResult<Vec<TxFeedEntry>> {
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let start = *range.start();
        let txs = self.inner.blockchain_provider.transactions_by_tx_range(range.clone())?;
        if txs.is_empty() {
            return Ok(Vec::new());
        }
        let mut senders = self.inner.blockchain_provider.senders_by_tx_range(range)?;
        if senders.len() != txs.len() {
            // defensive: persistence always writes senders alongside transactions, so a
            // misaligned read should be impossible — recover in parallel if it happens
            senders = txs
                .par_iter()
                .map(|tx| tx.recover_signer().map_err(TnRethError::from))
                .collect::<TnRethResult<Vec<_>>>()?;
        }

        let mut entries = Vec::with_capacity(txs.len());
        // cached (block_number, first_tx_num, next_tx_num, block_hash, timestamp) for the block
        // containing the previous transaction; refreshed only when the cursor crosses into the
        // next non-empty block, so each non-empty block costs three point reads and empty
        // blocks are never visited (the `TransactionBlocks` seek skips them by construction).
        let mut block_ctx: Option<(BlockNumber, TxNumber, TxNumber, B256, u64)> = None;
        for (offset, (tx, sender)) in txs.into_iter().zip(senders).enumerate() {
            let tx_number = start + offset as u64;
            let (block_number, first_tx_num, _, block_hash, timestamp) = match block_ctx {
                Some(ctx) if tx_number < ctx.2 => ctx,
                _ => {
                    let block = self
                        .inner
                        .blockchain_provider
                        .block_by_transaction_id(tx_number)?
                        .ok_or(ProviderError::TransactionNotFound(tx_number.into()))?;
                    let indices = self
                        .inner
                        .blockchain_provider
                        .block_body_indices(block)?
                        .ok_or(ProviderError::BlockBodyIndicesNotFound(block))?;
                    let header = self
                        .inner
                        .blockchain_provider
                        .sealed_header(block)?
                        .ok_or(ProviderError::HeaderNotFound(block.into()))?;
                    let ctx = (
                        block,
                        indices.first_tx_num(),
                        indices.next_tx_num(),
                        header.hash(),
                        header.timestamp,
                    );
                    block_ctx = Some(ctx);
                    ctx
                }
            };
            entries.push(TxFeedEntry {
                tx_number,
                transaction: Recovered::new_unchecked(tx, sender),
                block_number,
                block_hash,
                timestamp,
                index: tx_number.saturating_sub(first_tx_num),
            });
        }

        Ok(entries)
    }

    /// Return balance, nonce, and code hash for an account at the latest canonical state.
    pub fn retrieve_account(&self, address: &Address) -> TnRethResult<Option<Account>> {
        Ok(self.inner.blockchain_provider.basic_account(address)?)
    }

    /// Return the contract bytecode deployed at `address` at the latest canonical state
    /// (`eth_getCode` equivalent). `None` for EOAs and unknown accounts.
    pub fn account_code(&self, address: &Address) -> TnRethResult<Option<Bytes>> {
        Ok(self.latest()?.account_code(address)?.map(|code| code.original_bytes()))
    }
}
