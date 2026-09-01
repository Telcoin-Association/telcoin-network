//! Helper methods for retrieving state.
//!
//! # Read consistency: two families of reads
//!
//! Two families of read methods coexist on `RethEnv`, and they DISAGREE about which blocks
//! "exist" while a consensus output is mid-canonicalization. The engine advances the canonical
//! in-memory state block-by-block as it executes an output (speculatively, before anything is
//! durable — see `execute_payload` in `tn-engine`'s payload builder), and only afterwards
//! commits the whole output's blocks to the database in a single transaction
//! (`finish_executing_output`). Between those two points:
//!
//! - **In-memory-tip-aware** reads call `blockchain_provider` directly, which overlays the
//!   canonical in-memory chain on the database (reth's `ConsistentProvider`, or the in-memory head
//!   state for `latest`). Verified members: `sealed_header_by_hash`, `header`, `blocks_for_range`,
//!   `canonical_tip`, `latest`. These already see the in-flight blocks of the output being
//!   executed.
//!
//! - **Committed-DB-only** reads go through `database_provider_ro()`, a raw read transaction with
//!   no in-memory overlay. Verified members: `sealed_header_by_number`, `header_by_number`,
//!   `last_block_number`, `last_finalized_block_number`. These see the in-flight blocks only once
//!   the output commits. `lookup_head` also belongs here: reth's `NodeConfig::lookup_head` resolves
//!   the head *number* from the `Finish` stage checkpoint via `database_provider_ro()`, so it
//!   tracks the committed database even though the follow-up header fetch is in-memory-aware.
//!
//! Concretely: `sealed_header_by_hash` can return a header whose number exceeds
//! `last_block_number()`, and `header_by_number(canonical_tip().number)` can return `None`,
//! without anything being wrong. Unifying the two families behind one provider path was
//! deliberately deferred during the env reorg — when adding a read here, pick a family
//! intentionally and mind which one existing callers assume.

use std::{ops::RangeInclusive, sync::Arc};

use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
use reth_chain_state::CanonicalInMemoryState;
use reth_chainspec::BaseFeeParams;
use reth_errors::ProviderError;
use reth_evm::{ConfigureEvm as _, EvmFactory as _};
use reth_primitives_traits::SignerRecoverable as _;
use reth_provider::{
    AccountReader as _, BlockBodyIndicesProvider as _, BlockIdReader as _, BlockNumReader as _,
    BlockReader as _, CanonStateNotificationStream, CanonStateSubscriptions as _, Chain,
    ChainStateBlockReader as _, DatabaseProviderFactory as _, ExecutionOutcome,
    HeaderProvider as _, ReceiptProvider as _, StateProviderBox, StateProviderFactory as _,
    TransactionVariant, TransactionsProvider as _,
};
use reth_revm::context::result::ExecutionResult;
use tn_types::{
    Account, Address, BlockHashOrNumber, BlockNumHash, BlockNumber, Bytes,
    CanonicalExecutionReader, ExecHeader, Receipt, Recovered, SealedBlock, SealedHeader,
    TransactionMeta, TransactionSigned, TxHash, TxNumber, B256,
};

use crate::{
    error::{EvmReadError, EvmReadResult, TnRethError, TnRethResult},
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

    /// TEST-ONLY: the state provider for `hash`, exactly as block building resolves its
    /// parent state (reth's memory-overlay provider over any unpersisted ancestors).
    ///
    /// The #1301 differential tests use it as the oracle: reth's own
    /// `state_root_with_updates` over this provider must equal the layered
    /// `OutputTrieOverlay` root for every block of an output.
    #[cfg(any(feature = "test-utils", test))]
    pub fn state_by_block_hash_for_test(&self, hash: B256) -> TnRethResult<StateProviderBox> {
        Ok(self.inner.blockchain_provider.state_by_block_hash(hash)?)
    }

    /// TEST-ONLY: a read-only database provider over this env's database (the
    /// committed-DB-only family; no in-memory overlay).
    ///
    /// Exposes the raw transaction (`tx_ref`) that the #1301 trie-level differential
    /// test drives both root computations over.
    #[cfg(any(feature = "test-utils", test))]
    pub fn database_provider_ro_for_test(
        &self,
    ) -> TnRethResult<impl reth_provider::DBProvider + use<>> {
        Ok(self.inner.blockchain_provider.database_provider_ro()?)
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
            .map(|b| b.into_sealed_block()))
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

    /// Return the head header per the database's `Finish` stage checkpoint.
    ///
    /// Committed-DB read (see the module docs): reth's `NodeConfig::lookup_head` resolves the
    /// head number from the committed database, not the in-memory tip; the sealed header is
    /// then fetched by that number. If that header cannot be found, this returns a
    /// `ProviderError::HeaderNotFound` error rather than panicking.
    pub fn lookup_head(&self) -> TnRethResult<SealedHeader> {
        let head = self.node_config().lookup_head(&self.inner.blockchain_provider)?;
        let header = self
            .inner
            .blockchain_provider
            .sealed_header(head.number)?
            .ok_or(ProviderError::HeaderNotFound(head.number.into()))?;
        Ok(header)
    }

    /// If a debug max round is set then return it.
    ///
    /// Despite the "round" name, this reads `debug.max_block` from the node config: a block
    /// number used as a debug stopping point.
    pub fn get_debug_max_round(&self) -> Option<u64> {
        self.node_config().debug.max_block
    }

    /// Helper to get a gas price estimate based on the head header.
    ///
    /// RPC-style convenience only: this computes an ETHEREUM next-block base fee
    /// (`BaseFeeParams::ethereum()`, mainnet's EIP-1559 schedule), which is NOT how TN derives
    /// base fees — TN worker base fees come from the on-chain `WorkerConfigs` contract, applied
    /// per epoch (see `worker_fee_configs_inner` in `env/epoch.rs`). Do not use this value for
    /// fee enforcement. The header it prices from is the committed-DB head
    /// ([`Self::lookup_head`]).
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

    /// Return the latest block number committed to the database.
    ///
    /// Committed-DB read (see the module docs): blocks of an in-flight consensus output are not
    /// counted until the output commits, so this can lag [`Self::canonical_tip`]. Provider
    /// errors propagate — there is no fallback value.
    pub fn last_block_number(&self) -> TnRethResult<u64> {
        Ok(self.inner.blockchain_provider.database_provider_ro()?.last_block_number()?)
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

    /// Returns the block number of the last finalized block per the database marker.
    ///
    /// Committed-DB read (see the module docs). A database without a finalized marker (no
    /// consensus output committed yet) yields `Ok(0)`: the `Option` is deliberately collapsed,
    /// so callers cannot distinguish "never finalized" from "finalized at genesis (block 0)".
    /// This is not an error swallow — provider errors still propagate.
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

    /// Execute a read-only contract call at the canonical tip and return the raw ABI-encoded
    /// output bytes.
    ///
    /// `eth_call`-like semantics: caller is the zero address, value 0, gas price 0, nonce and
    /// base fee checks disabled, gas capped at the system-call budget; no state is committed.
    /// Callers decode the returned bytes themselves (e.g. with `SolCall::abi_decode_returns`).
    ///
    /// # Errors
    ///
    /// - [`EvmReadError::Revert`]: the call reverted on-chain. Carries the raw ABI-encoded revert
    ///   bytes plus a best-effort decoded reason string.
    /// - [`EvmReadError::Internal`]: every non-revert failure — state-provider/database faults, EVM
    ///   environment construction, transact-level errors, or the call halting.
    pub fn read_contract(&self, contract: Address, calldata: Bytes) -> EvmReadResult<Bytes> {
        self.read_contract_inner(&self.canonical_tip(), Address::ZERO, contract, calldata)
    }

    /// Same as [`Self::read_contract`], but pinned to the state of the block identified by
    /// `block_hash`.
    ///
    /// # Errors
    ///
    /// Same split as [`Self::read_contract`]; additionally, `block_hash` failing to resolve to
    /// a known sealed header is an [`EvmReadError::Internal`], and so is a pin below a restored
    /// datadir's state floor, refused by `read_only_state_db` (`env/mod.rs`) before any state
    /// is read instead of silently answering from the missing pre-snapshot history (#1136).
    pub fn read_contract_at_block(
        &self,
        block_hash: B256,
        contract: Address,
        calldata: Bytes,
    ) -> EvmReadResult<Bytes> {
        let header = self
            .sealed_header_by_hash(block_hash)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?
            .ok_or_else(|| {
                EvmReadError::Internal(format!(
                    "sealed header not found for block hash {block_hash:?}"
                ))
            })?;
        self.read_contract_inner(&header, Address::ZERO, contract, calldata)
    }

    /// Build an EVM against `header`'s state and execute one read-only call, returning raw
    /// output bytes on success and mapping Revert/Halt like the registry read path.
    ///
    /// # Errors
    ///
    /// On-chain reverts surface as [`EvmReadError::Revert`] (raw output + decoded reason);
    /// everything else — state-db construction, EVM env construction, transact errors, and
    /// `Halt` — collapses into [`EvmReadError::Internal`].
    ///
    /// ARCHIVE-MODE ASSUMPTION: this is a pinned read. It builds its state from
    /// `read_only_state_db` directly rather than through `pinned_state_and_env`, so it
    /// does not inherit that helper's note, but it depends on fully indexed history for exactly
    /// the same reason. `RethConfig::ensure_archive_mode` refuses to start a node whose
    /// configuration requests pruning, and `etc/archive-mode-guard.sh` fails the build if a pruner
    /// entry point is introduced. See `pinned_state_and_env` in `env/epoch.rs` for the normative
    /// note and the three ways "history is missing" can present.
    fn read_contract_inner(
        &self,
        header: &SealedHeader,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> EvmReadResult<Bytes> {
        let mut db =
            self.read_only_state_db(header).map_err(|e| EvmReadError::Internal(e.to_string()))?;
        let evm_env =
            self.evm_config().evm_env(header).map_err(|e| EvmReadError::Internal(e.to_string()))?;
        let mut tn_evm = self.evm_config().evm_factory().create_evm(&mut db, evm_env);

        let result = self
            .read_state_on_chain(&mut tn_evm, caller, contract, calldata)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;

        // surface user-triggerable reverts distinctly from internal node faults
        match result.result {
            ExecutionResult::Success { output, .. } => Ok(output.into_data()),
            ExecutionResult::Revert { output, .. } => Err(EvmReadError::Revert {
                reason: alloy::sol_types::decode_revert_reason(&output),
                output,
            }),
            ExecutionResult::Halt { reason, gas_used } => Err(EvmReadError::Internal(format!(
                "contract call halted: {reason:?} (gas {gas_used})"
            ))),
        }
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

impl CanonicalExecutionReader for RethEnv {
    /// Answer from the committed-DB-only read family (see this module's header): the caller asks
    /// whether `number` is *durably* canonical, so an in-memory-tip-aware read would defeat the
    /// question by reporting speculatively executed blocks as confirmed.
    fn canonical_execution_hash(&self, number: BlockNumber) -> Option<B256> {
        // A read error (e.g. a transient provider/DB error) is treated as "not confirmed" rather
        // than as a canonical match, so the caller keeps its conservative fork handling.
        self.sealed_header_by_number(number)
            .inspect_err(|error| {
                tracing::debug!(
                    target: "tn::reth",
                    ?error,
                    number,
                    "canonical_execution_hash: canonical DB read failed; treating as unconfirmed"
                );
            })
            .ok()
            .flatten()
            .map(|header| header.hash())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        error::EvmReadError,
        payload::TNPayload,
        test_utils::{
            consensus_output_for_tests, execute_payload_and_update_canonical_chain,
            TransactionFactory,
        },
        RethChainSpec,
    };
    use tempfile::TempDir;
    use tn_types::{
        keccak256, test_genesis, Encodable2718 as _, GenesisAccount, TaskManager, U256,
    };

    /// Exercise the tx/receipt/feed read API against a persisted three-block chain:
    /// block 1 holds two transfers, block 2 is empty, block 3 holds one transfer.
    #[tokio::test]
    async fn test_read_api_tx_receipts_and_feed() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        let mut factory = TransactionFactory::new();
        let recipient = Address::from_slice(&[0xcc; 20]);
        let value = U256::from(1_000_000);
        let tx1 =
            factory.create_eip1559(chain.clone(), None, 100, Some(recipient), value, Bytes::new());
        let tx2 =
            factory.create_eip1559(chain.clone(), None, 100, Some(recipient), value, Bytes::new());
        let tx3 =
            factory.create_eip1559(chain.clone(), None, 100, Some(recipient), value, Bytes::new());

        // block 1: two transfers
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![tx1.encoded_2718(), tx2.encoded_2718()],
        )?;
        let block1_header = block1.recovered_block.clone_sealed_header();

        // block 2: empty
        let consensus_output = consensus_output_for_tests(2, 0, 2, false);
        let payload = TNPayload::new_for_test(block1_header.clone(), &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let block2_header = block2.recovered_block.clone_sealed_header();

        // block 3: one transfer
        let consensus_output = consensus_output_for_tests(2, 0, 3, false);
        let payload = TNPayload::new_for_test(block2_header, &consensus_output);
        let block3 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![tx3.encoded_2718()],
        )?;
        let block3_header = block3.recovered_block.clone_sealed_header();

        // tx-by-hash roundtrip for the second transaction in block 1
        let tx2_hash = *tx2.hash();
        let (recovered, meta) = reth_env
            .transaction_by_hash_with_meta(tx2_hash)?
            .expect("mined transaction found by hash");
        assert_eq!(recovered.signer(), factory.address());
        assert_eq!(*recovered.hash(), tx2_hash);
        assert_eq!(meta.block_number, 1);
        assert_eq!(meta.index, 1);
        assert_eq!(meta.block_hash, block1_header.hash());
        assert_eq!(meta.timestamp, block1_header.timestamp);
        // unknown hash is Ok(None), not an error
        assert!(reth_env.transaction_by_hash_with_meta(TxHash::random())?.is_none());

        // receipts by hash: cumulative gas is block-wide, per-tx gas is the delta
        let receipt = reth_env.receipt_by_hash(tx2_hash)?.expect("receipt for mined tx");
        assert!(receipt.success);
        let (receipt, gas_used) = reth_env
            .receipt_by_hash_with_gas_used(tx2_hash)?
            .expect("receipt with gas for mined tx");
        assert_eq!(gas_used, 21_000);
        assert_eq!(receipt.cumulative_gas_used, 42_000);
        // first-in-block transaction: per-tx gas equals its cumulative gas
        let (receipt, gas_used) = reth_env
            .receipt_by_hash_with_gas_used(*tx3.hash())?
            .expect("receipt with gas for mined tx");
        assert_eq!(gas_used, 21_000);
        assert_eq!(receipt.cumulative_gas_used, 21_000);

        // receipts by block: number- and hash-based reads agree
        let by_number =
            reth_env.receipts_by_block(BlockHashOrNumber::Number(1))?.expect("block 1 receipts");
        let by_hash = reth_env
            .receipts_by_block(BlockHashOrNumber::Hash(block1_header.hash()))?
            .expect("block 1 receipts by hash");
        assert_eq!(by_number.len(), 2);
        assert_eq!(by_number, by_hash);
        // known-but-empty block returns Some(empty); unknown block returns None
        assert_eq!(reth_env.receipts_by_block(BlockHashOrNumber::Number(2))?, Some(vec![]));
        assert!(reth_env.receipts_by_block(BlockHashOrNumber::Number(99))?.is_none());

        // chain-wide feed: three transactions total
        assert_eq!(reth_env.total_transactions()?, 3);
        let feed = reth_env.transactions_by_tx_range_with_meta(0..=2)?;
        assert_eq!(feed.len(), 3);
        let tx_numbers: Vec<_> = feed.iter().map(|entry| entry.tx_number).collect();
        assert_eq!(tx_numbers, vec![0, 1, 2]);
        for (entry, tx) in feed.iter().zip([&tx1, &tx2, &tx3]) {
            assert_eq!(*entry.transaction.hash(), *tx.hash());
            assert_eq!(entry.transaction.signer(), factory.address());
        }
        // entries 0-1 come from block 1
        assert_eq!(feed[0].block_number, 1);
        assert_eq!(feed[0].index, 0);
        assert_eq!(feed[0].block_hash, block1_header.hash());
        assert_eq!(feed[1].block_number, 1);
        assert_eq!(feed[1].index, 1);
        // entry 2 comes from block 3 — the empty block 2 is skipped entirely
        assert_eq!(feed[2].block_number, 3);
        assert_eq!(feed[2].index, 0);
        assert_eq!(feed[2].block_hash, block3_header.hash());
        assert_eq!(feed[2].timestamp, block3_header.timestamp);

        // ranges past the newest transaction clamp to what exists
        assert_eq!(reth_env.transactions_by_tx_range_with_meta(0..=99)?.len(), 3);
        // empty range yields an empty vec
        assert!(reth_env.transactions_by_tx_range_with_meta(RangeInclusive::new(1, 0))?.is_empty());

        // newest-first page of two: read a descending window and reverse it
        let total = reth_env.total_transactions()?;
        let mut page = reth_env.transactions_by_tx_range_with_meta(total - 2..=total - 1)?;
        page.reverse();
        let hashes: Vec<_> = page.iter().map(|entry| *entry.transaction.hash()).collect();
        assert_eq!(hashes, vec![*tx3.hash(), *tx2.hash()]);

        Ok(())
    }

    /// Minimal runtime bytecode: `PUSH1 42, PUSH1 0, MSTORE, PUSH1 32, PUSH1 0, RETURN` —
    /// returns `uint256(42)` for any calldata.
    const RETURN_42: &[u8] = &[0x60, 0x2a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3];
    /// Minimal runtime bytecode: `PUSH1 0, PUSH1 0, REVERT` — reverts with empty output.
    const ALWAYS_REVERT: &[u8] = &[0x60, 0x00, 0x60, 0x00, 0xfd];

    /// Exercise account/code reads and the generic read-only contract call against two
    /// bytecode fixtures deployed in genesis.
    #[tokio::test]
    async fn test_read_api_account_code_and_contract_read() -> eyre::Result<()> {
        let return_42_addr = Address::from_slice(&[0xaa; 20]);
        let always_revert_addr = Address::from_slice(&[0xbb; 20]);
        let genesis = test_genesis().extend_accounts([
            (
                return_42_addr,
                GenesisAccount::default().with_code(Some(Bytes::from_static(RETURN_42))),
            ),
            (
                always_revert_addr,
                GenesisAccount::default().with_code(Some(Bytes::from_static(ALWAYS_REVERT))),
            ),
        ]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;
        let genesis_hash = chain.sealed_genesis_header().hash();

        let mut factory = TransactionFactory::new();
        let genesis_balance = reth_env
            .retrieve_account(&factory.address())?
            .expect("factory funded in genesis")
            .balance;

        // execute one transfer so the factory's nonce and balance move
        let recipient = Address::from_slice(&[0xcc; 20]);
        let transfer = factory.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(recipient),
            U256::from(1_000_000),
            Bytes::new(),
        );
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![transfer])?;

        // account state reflects the executed transfer
        let factory_account =
            reth_env.retrieve_account(&factory.address())?.expect("factory account exists");
        assert_eq!(factory_account.nonce, 1);
        assert!(factory_account.balance < genesis_balance);

        // genesis contract account carries the bytecode hash; unknown account is None
        let contract_account =
            reth_env.retrieve_account(&return_42_addr)?.expect("genesis contract account exists");
        assert_eq!(contract_account.bytecode_hash, Some(keccak256(RETURN_42)));
        assert!(reth_env.retrieve_account(&Address::from_slice(&[0xdd; 20]))?.is_none());

        // eth_getCode semantics: byte-exact for contracts, None for EOAs and unknowns
        assert_eq!(reth_env.account_code(&return_42_addr)?, Some(Bytes::from_static(RETURN_42)));
        assert!(reth_env.account_code(&factory.address())?.is_none());
        assert!(reth_env.account_code(&Address::from_slice(&[0xdd; 20]))?.is_none());

        // read-only contract call at the canonical tip
        let output = reth_env
            .read_contract(return_42_addr, Bytes::default())
            .expect("read-only call succeeds");
        assert_eq!(output.len(), 32);
        assert_eq!(U256::from_be_slice(&output), U256::from(42));

        // on-chain revert surfaces as EvmReadError::Revert with the raw output bytes
        let err = reth_env
            .read_contract(always_revert_addr, Bytes::default())
            .expect_err("reverting call must error");
        match err {
            EvmReadError::Revert { output, reason } => {
                assert!(output.is_empty());
                // alloy's `decode_revert_reason` treats any valid-UTF-8 output as a raw-string
                // reason (Vyper convention), so empty revert data yields `Some("")` rather than
                // `None`; assert there is no *meaningful* reason either way
                assert!(reason.as_deref().unwrap_or_default().is_empty());
            }
            other => panic!("expected revert error, got: {other:?}"),
        }

        // historical pinning: the same read against the genesis block's state
        let output = reth_env
            .read_contract_at_block(genesis_hash, return_42_addr, Bytes::default())
            .expect("read-only call at genesis succeeds");
        assert_eq!(U256::from_be_slice(&output), U256::from(42));

        Ok(())
    }
}
