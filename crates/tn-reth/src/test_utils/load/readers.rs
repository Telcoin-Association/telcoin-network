//! The concurrent side load of a busy node: RPC reads, raw transaction submissions and the batch
//! builder's balance polls, each on its own threads against the in-process RPC methods and the
//! pool while the writer executes outputs.
//!
//! - [`Readers`]: `threads` reader threads, each with its own seeded RNG, call the RPC [`Methods`]
//!   that `RethEnv::get_rpc_server` builds for HTTP, with every kind of their [`ReadMix`] equally
//!   likely. [`ReadMix::rpc`] draws the latest-state and recent reads:
//!   - [`ReadKind::Balance`]: `eth_getBalance` of a Zipf-drawn prefilled account at `latest`;
//!   - [`ReadKind::TokenBalance`]: `eth_call` of `balanceOf` on [`STABLECOIN_A`] or
//!     [`STABLECOIN_B`] for a Zipf-drawn prefilled account at `latest`;
//!   - [`ReadKind::Receipt`]: `eth_getTransactionReceipt` of a recent transaction, drawn from a
//!     ring of the hashes in the latest blocks the readers fetched, plus any the driver feeds
//!     ([`Readers::feed_tx_hashes`]); with the ring empty the reader fetches the latest block;
//!   - [`ReadKind::Logs`]: `eth_getLogs` of [`SWAP_ROUTER`]'s `ISwapRouter::Swap` events over the
//!     last [`LOG_BLOCKS`] blocks up to `eth_blockNumber` (that lookup is not timed);
//!   - [`ReadKind::Block`]: `eth_getBlockByNumber("latest", true)`.
//!
//!   [`ReadMix::history`] draws the historical-state and by-hash reads, each past block drawn after
//!   an untimed `eth_blockNumber`, uniformly from the blocks below the tip (`*_chain`) or from the
//!   last [`ReadMix::epoch_blocks`] of them (`*_epoch`):
//!   - [`ReadKind::BalanceAtChain`], [`ReadKind::BalanceAtEpoch`]: `eth_getBalance` of a Zipf-drawn
//!     prefilled account at a past block;
//!   - [`ReadKind::StorageAtChain`], [`ReadKind::StorageAtEpoch`]: `eth_getStorageAt` of a
//!     Zipf-drawn prefilled account's balance slot on [`STABLECOIN_A`] or [`STABLECOIN_B`] at a
//!     past block;
//!   - [`ReadKind::TxByHash`]: `eth_getTransactionByHash` of an executed transaction, drawn from a
//!     uniform sample ([`EXECUTED_TXS`] at most) of every hash the driver fed
//!     ([`Readers::feed_tx_hashes`]); a hash without an answer is an error. With none fed yet the
//!     reader fetches the latest block instead;
//!   - [`ReadKind::TxByHashMiss`]: `eth_getTransactionByHash` of a random hash, which must answer
//!     `null`.
//!
//!   [`ReadMix::all`] draws both sets.
//! - [`Readers::poll_balances`]: one more thread calls the pool's `TxPool::get_account_balances`
//!   for an address set ([`poll_addresses`]) every [`POLL_INTERVAL`], as the batch builder does.
//! - [`Submitter`]: one thread sends queued raw transactions through `eth_sendRawTransaction`, so
//!   the pool's validation reads and its maintenance run as in production.
//!
//! The readers and the poller share one gate ([`Readers::set_on`]): off, they park; on, the
//! readers call back to back. Switching off returns once every call in flight is recorded, so
//! [`Readers::drain`] after it holds exactly the window's samples.
//!
//! Every call runs on its own std thread through `Handle::block_on` on the tokio runtime current
//! at start. The RPC handlers spawn onto that runtime and use its timers, and the state cache and
//! the pool's maintenance run on it, as they do behind the node's server. The runtime must be
//! multi-thread: on a current-thread runtime, `Handle::block_on` from another thread drives
//! neither IO nor timers.
//!
//! Errors: every RPC error is counted per kind with the first message kept, and an answer of the
//! wrong shape (a malformed quantity, a receipt or log that does not match its query) counts as an
//! error too. A submission refused with `nonce too low` (already mined) or `already known`
//! (already pooled) is expected and counted apart from the errors.

use super::{
    state::{
        balance_slot, ISwapRouter, PrefillSpec, IERC20, STABLECOIN_A, STABLECOIN_B, SWAP_ROUTER,
    },
    workload::{WorkloadConfig, Zipf},
};
use crate::{TxPool as _, WorkerTxPool};
use alloy::sol_types::{SolCall as _, SolEvent as _};
use jsonrpsee::{core::params::ArrayParams, rpc_params, Methods};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Mutex, MutexGuard, PoisonError,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tn_types::{keccak256, Address, Bytes, B256, U256};
use tokio::runtime::{Handle, RuntimeFlavor};

/// The blocks a [`ReadKind::Logs`] query spans, ending at the tip.
pub const LOG_BLOCKS: u64 = 64;

/// How often the batch builder polls its senders' balances.
pub const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// The Zipf-drawn prefilled accounts [`poll_addresses`] polls besides the senders.
pub const POLL_ACCOUNTS: usize = 64;

/// The recent transaction hashes [`ReadKind::Receipt`] draws from.
const RECENT_TXS: usize = 1_024;

/// The largest sample of executed transaction hashes [`ReadKind::TxByHash`] draws from.
pub const EXECUTED_TXS: usize = 1 << 16;

/// How long an idle thread parks before it checks the gate again.
const IDLE: Duration = Duration::from_millis(5);

/// What a reader asks the RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReadKind {
    /// `eth_getBalance` of a prefilled account.
    Balance,
    /// `eth_call` of a token's `balanceOf` for a prefilled account.
    TokenBalance,
    /// `eth_getTransactionReceipt` of a recent transaction.
    Receipt,
    /// `eth_getLogs` of the router's swaps over the last [`LOG_BLOCKS`] blocks.
    Logs,
    /// `eth_getBlockByNumber` of the latest block with its transactions.
    Block,
    /// `eth_getBalance` of a prefilled account at a past block of the whole chain.
    BalanceAtChain,
    /// `eth_getBalance` of a prefilled account at a past block of the last epoch's.
    BalanceAtEpoch,
    /// `eth_getStorageAt` of a prefilled account's token balance at a past block of the chain.
    StorageAtChain,
    /// `eth_getStorageAt` of a prefilled account's token balance at a past block of the last
    /// epoch's.
    StorageAtEpoch,
    /// `eth_getTransactionByHash` of an executed transaction.
    TxByHash,
    /// `eth_getTransactionByHash` of a random hash.
    TxByHashMiss,
}

impl ReadKind {
    /// How many kinds there are.
    pub const COUNT: usize = 11;

    /// Every kind, in the order [`ReadSamples`] keeps them.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Balance,
        Self::TokenBalance,
        Self::Receipt,
        Self::Logs,
        Self::Block,
        Self::BalanceAtChain,
        Self::BalanceAtEpoch,
        Self::StorageAtChain,
        Self::StorageAtEpoch,
        Self::TxByHash,
        Self::TxByHashMiss,
    ];

    /// The latest-state and recent reads ([`ReadMix::rpc`]).
    pub const RPC: [Self; 5] =
        [Self::Balance, Self::TokenBalance, Self::Receipt, Self::Logs, Self::Block];

    /// The historical-state and by-hash reads ([`ReadMix::history`]).
    pub const HISTORY: [Self; 6] = [
        Self::BalanceAtChain,
        Self::BalanceAtEpoch,
        Self::StorageAtChain,
        Self::StorageAtEpoch,
        Self::TxByHash,
        Self::TxByHashMiss,
    ];

    /// The RPC method the kind calls.
    pub const fn method(self) -> &'static str {
        match self {
            Self::Balance | Self::BalanceAtChain | Self::BalanceAtEpoch => "eth_getBalance",
            Self::TokenBalance => "eth_call",
            Self::Receipt => "eth_getTransactionReceipt",
            Self::Logs => "eth_getLogs",
            Self::Block => "eth_getBlockByNumber",
            Self::StorageAtChain | Self::StorageAtEpoch => "eth_getStorageAt",
            Self::TxByHash | Self::TxByHashMiss => "eth_getTransactionByHash",
        }
    }

    /// The kind's report label and JSON key.
    pub const fn key(self) -> &'static str {
        match self {
            Self::Balance => "balance",
            Self::TokenBalance => "token_balance",
            Self::Receipt => "receipt",
            Self::Logs => "logs",
            Self::Block => "block",
            Self::BalanceAtChain => "balance_at_chain",
            Self::BalanceAtEpoch => "balance_at_epoch",
            Self::StorageAtChain => "storage_at_chain",
            Self::StorageAtEpoch => "storage_at_epoch",
            Self::TxByHash => "tx_by_hash",
            Self::TxByHashMiss => "tx_by_hash_miss",
        }
    }
}

/// The kinds the readers draw, each equally likely, and the span of the `*_epoch` kinds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadMix {
    /// The kinds.
    pub kinds: &'static [ReadKind],
    /// The blocks below the tip the `*_epoch` kinds draw from: the last epoch's worth.
    pub epoch_blocks: u64,
}

impl ReadMix {
    /// [`ReadKind::RPC`], the latest-state and recent reads.
    pub const fn rpc() -> Self {
        Self { kinds: &ReadKind::RPC, epoch_blocks: 1 }
    }

    /// [`ReadKind::HISTORY`], the past-block and by-hash reads, the `*_epoch` kinds over the last
    /// `epoch_blocks` blocks (at least 1).
    pub const fn history(epoch_blocks: u64) -> Self {
        Self { kinds: &ReadKind::HISTORY, epoch_blocks }
    }

    /// Every kind, [`ReadKind::ALL`].
    pub const fn all(epoch_blocks: u64) -> Self {
        Self { kinds: &ReadKind::ALL, epoch_blocks }
    }
}

/// Where a past-block read draws its block from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Span {
    /// Uniformly from the blocks below the tip.
    Chain,
    /// Uniformly from the last [`ReadMix::epoch_blocks`] blocks below the tip.
    Epoch,
}

impl Span {
    /// The blocks the span covers below `tip`, an epoch being `epoch_blocks` (at least 1): from
    /// the genesis, or the epoch's first, to the block below the tip; the genesis alone while the
    /// tip is the genesis.
    fn blocks(self, tip: u64, epoch_blocks: u64) -> std::ops::RangeInclusive<u64> {
        let last = tip.saturating_sub(1);
        let first = match self {
            Self::Chain => 0,
            Self::Epoch => tip.saturating_sub(epoch_blocks.max(1)),
        };
        first..=last
    }
}

/// One kind's calls in a window.
#[derive(Clone, Debug, Default)]
pub struct KindSamples {
    /// The latency of every answered call (`ok` plus `empty`).
    pub latencies: Vec<Duration>,
    /// Answers with content.
    pub ok: u64,
    /// Right-shaped empty answers: no receipt, no logs, a block without transactions.
    pub empty: u64,
    /// RPC errors and wrong-shaped answers.
    pub errors: u64,
    /// The first error's message.
    pub first_error: Option<String>,
}

impl KindSamples {
    /// Count an error, keeping its message when it is the first.
    fn error(&mut self, message: String) {
        self.errors += 1;
        self.first_error.get_or_insert(message);
    }
}

/// The balance poller's calls in a window.
#[derive(Clone, Debug, Default)]
pub struct PollSamples {
    /// The latency of every `get_account_balances` call.
    pub latencies: Vec<Duration>,
    /// The last call's balances, one per polled address.
    pub last: HashMap<Address, U256>,
}

/// What [`Readers::drain`] returns: each kind's samples and the poller's.
#[derive(Clone, Debug, Default)]
pub struct ReadSamples {
    /// Each kind's samples, in [`ReadKind::ALL`] order.
    kinds: [KindSamples; ReadKind::COUNT],
    /// The balance poller's samples.
    pub polls: PollSamples,
}

impl ReadSamples {
    /// The samples of `kind`.
    pub fn kind(&self, kind: ReadKind) -> &KindSamples {
        &self.kinds[kind as usize]
    }

    /// The errors over every kind.
    pub fn errors(&self) -> u64 {
        self.kinds.iter().map(|kind| kind.errors).sum()
    }
}

/// A right-shaped answer.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Answer {
    /// A native or token balance.
    Balance(U256),
    /// The receipt's block number, or `None` when the node has no receipt.
    Receipt(Option<u64>),
    /// How many logs matched.
    Logs(usize),
    /// The block's number and its transactions' hashes.
    Block(u64, Vec<B256>),
    /// The transaction's block number, or `None` when the node has no such transaction.
    Tx(Option<u64>),
}

impl Answer {
    /// Whether the answer is a right-shaped "nothing".
    fn is_empty(&self) -> bool {
        match self {
            Self::Balance(_) => false,
            Self::Receipt(found) => found.is_none(),
            Self::Logs(count) => *count == 0,
            Self::Block(_, txs) => txs.is_empty(),
            Self::Tx(found) => found.is_none(),
        }
    }
}

/// One query, with what its answer must match.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Read {
    /// The native balance of an account.
    Balance(Address),
    /// The balance of an account (second) on a token (first).
    TokenBalance(Address, Address),
    /// The receipt of a transaction.
    Receipt(B256),
    /// The swaps in blocks `from..=to`.
    Logs(u64, u64),
    /// The latest block.
    Block,
    /// The native balance of an account at a block drawn from a span.
    BalanceAt(Address, u64, Span),
    /// The storage of a token (first) at a slot (second) at a block drawn from a span.
    StorageAt(Address, B256, u64, Span),
    /// A transaction by hash: `true` for an executed one, which must answer, `false` for a
    /// random hash, which must not.
    Tx(B256, bool),
}

impl Read {
    /// The kind the query is counted under.
    fn kind(&self) -> ReadKind {
        match self {
            Self::Balance(_) => ReadKind::Balance,
            Self::TokenBalance(..) => ReadKind::TokenBalance,
            Self::Receipt(_) => ReadKind::Receipt,
            Self::Logs(..) => ReadKind::Logs,
            Self::Block => ReadKind::Block,
            Self::BalanceAt(.., Span::Chain) => ReadKind::BalanceAtChain,
            Self::BalanceAt(.., Span::Epoch) => ReadKind::BalanceAtEpoch,
            Self::StorageAt(.., Span::Chain) => ReadKind::StorageAtChain,
            Self::StorageAt(.., Span::Epoch) => ReadKind::StorageAtEpoch,
            Self::Tx(_, true) => ReadKind::TxByHash,
            Self::Tx(_, false) => ReadKind::TxByHashMiss,
        }
    }

    /// Run the query on `methods` and check its answer.
    fn run(&self, methods: &Methods, handle: &Handle) -> Result<Answer, String> {
        let method = self.kind().method();
        let params = match self {
            Self::Balance(account) => rpc_params![account, "latest"],
            Self::TokenBalance(token, account) => {
                let input = Bytes::from(IERC20::balanceOfCall { account: *account }.abi_encode());
                rpc_params![json!({ "to": token, "input": input }), "latest"]
            }
            Self::Receipt(hash) => rpc_params![hash],
            Self::Logs(from, to) => rpc_params![json!({
                "fromBlock": format!("{from:#x}"),
                "toBlock": format!("{to:#x}"),
                "address": SWAP_ROUTER,
                "topics": [ISwapRouter::Swap::SIGNATURE_HASH],
            })],
            Self::Block => rpc_params!["latest", true],
            Self::BalanceAt(account, block, _) => rpc_params![account, block_tag(*block)],
            Self::StorageAt(token, slot, block, _) => rpc_params![token, slot, block_tag(*block)],
            Self::Tx(hash, _) => rpc_params![hash],
        };
        let value = call(methods, handle, method, params)?;
        self.check(&value).map_err(|e| format!("{method}: {e}"))
    }

    /// Check `value` against the query.
    fn check(&self, value: &Value) -> Result<Answer, String> {
        match self {
            Self::Balance(_) => quantity(value).map(Answer::Balance),
            Self::TokenBalance(..) => {
                let bytes: Bytes = text(value)?.parse().map_err(|e| format!("not bytes: {e}"))?;
                if bytes.len() != 32 {
                    return Err(format!("balanceOf returned {} bytes", bytes.len()));
                }
                Ok(Answer::Balance(U256::from_be_slice(&bytes)))
            }
            Self::Receipt(hash) => {
                if value.is_null() {
                    return Ok(Answer::Receipt(None));
                }
                let found = tx_hash(&value["transactionHash"])?;
                if found != *hash {
                    return Err(format!("receipt of {found}, asked for {hash}"));
                }
                Ok(Answer::Receipt(Some(number(&value["blockNumber"])?)))
            }
            Self::Logs(from, to) => {
                let logs = value.as_array().ok_or("not an array")?;
                for log in logs {
                    let address: Address =
                        text(&log["address"])?.parse().map_err(|e| format!("address: {e}"))?;
                    let block = number(&log["blockNumber"])?;
                    let topic = tx_hash(&log["topics"][0])?;
                    if address != SWAP_ROUTER
                        || topic != ISwapRouter::Swap::SIGNATURE_HASH
                        || !(*from..=*to).contains(&block)
                    {
                        return Err(format!("log outside the filter: {log}"));
                    }
                }
                Ok(Answer::Logs(logs.len()))
            }
            Self::Block => {
                let txs = value["transactions"].as_array().ok_or("no transactions array")?;
                let hashes =
                    txs.iter().map(|tx| tx_hash(&tx["hash"])).collect::<Result<Vec<_>, _>>()?;
                Ok(Answer::Block(number(&value["number"])?, hashes))
            }
            Self::BalanceAt(..) => quantity(value).map(Answer::Balance),
            Self::StorageAt(..) => Ok(Answer::Balance(U256::from_be_bytes(tx_hash(value)?.0))),
            Self::Tx(hash, executed) => {
                if value.is_null() {
                    return if *executed {
                        Err(format!("the executed transaction {hash} has no answer"))
                    } else {
                        Ok(Answer::Tx(None))
                    };
                }
                let found = tx_hash(&value["hash"])?;
                if found != *hash {
                    return Err(format!("transaction {found}, asked for {hash}"));
                }
                if !executed {
                    return Err(format!("the random hash {hash} answered a transaction"));
                }
                Ok(Answer::Tx(Some(number(&value["blockNumber"])?)))
            }
        }
    }
}

/// The RPC block tag of block `number`: its hex quantity.
pub(super) fn block_tag(number: u64) -> String {
    format!("{number:#x}")
}

/// Call `method` with `params` on `methods` from this thread, within `handle`'s runtime.
pub(super) fn call(
    methods: &Methods,
    handle: &Handle,
    method: &str,
    params: ArrayParams,
) -> Result<Value, String> {
    handle.block_on(methods.call::<_, Value>(method, params)).map_err(|e| format!("{method}: {e}"))
}

/// The JSON string `value`.
pub(super) fn text(value: &Value) -> Result<&str, String> {
    value.as_str().ok_or_else(|| format!("not a string: {value}"))
}

/// The `0x`-prefixed hex quantity `value`.
pub(super) fn quantity(value: &Value) -> Result<U256, String> {
    let digits = text(value)?.strip_prefix("0x").ok_or("quantity without 0x")?;
    U256::from_str_radix(digits, 16).map_err(|e| format!("bad quantity {value}: {e}"))
}

/// The hex quantity `value` as a block number.
pub(super) fn number(value: &Value) -> Result<u64, String> {
    u64::try_from(quantity(value)?).map_err(|e| format!("block number {value}: {e}"))
}

/// The 32-byte hex `value`.
pub(super) fn tx_hash(value: &Value) -> Result<B256, String> {
    text(value)?.parse().map_err(|e| format!("bad hash {value}: {e}"))
}

/// Lock `mutex`, taking the data over from a thread that panicked holding it.
pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The current tokio runtime, which must be multi-thread (see the module docs).
pub(super) fn runtime() -> eyre::Result<Handle> {
    let handle = Handle::try_current()
        .map_err(|_| eyre::eyre!("the side load starts within a tokio runtime"))?;
    eyre::ensure!(
        handle.runtime_flavor() == RuntimeFlavor::MultiThread,
        "the side load needs a multi-thread runtime: a current-thread runtime drives neither IO \
         nor timers under another thread's `Handle::block_on`"
    );
    Ok(handle)
}

/// The state the readers, the poller and their handle share.
#[derive(Debug, Default)]
struct Shared {
    /// The gate: the threads call while it is set.
    on: AtomicBool,
    /// Set once by [`Readers::stop`] or a drop; the threads return.
    stop: AtomicBool,
    /// The threads that passed the gate and have not recorded their call yet.
    busy: AtomicUsize,
    /// The window's samples.
    samples: Mutex<ReadSamples>,
    /// The recent transaction hashes, oldest first.
    recent: Mutex<VecDeque<B256>>,
    /// A uniform sample of every executed transaction hash fed.
    executed: Mutex<Reservoir>,
}

/// A uniform sample of at most [`EXECUTED_TXS`] of the hashes added so far (Vitter's algorithm R),
/// its replacements drawn from its own RNG.
#[derive(Debug)]
struct Reservoir {
    /// The sample.
    hashes: Vec<B256>,
    /// The hashes added so far.
    seen: u64,
    /// The replacements' RNG.
    rng: StdRng,
}

impl Default for Reservoir {
    fn default() -> Self {
        Self { hashes: Vec::new(), seen: 0, rng: StdRng::seed_from_u64(0) }
    }
}

impl Reservoir {
    /// Add `hash`: kept while the sample is short, else in place of a random one with probability
    /// `EXECUTED_TXS / seen`.
    fn add(&mut self, hash: B256) {
        if self.hashes.len() < EXECUTED_TXS {
            self.hashes.push(hash);
        } else {
            let slot = self.rng.random_range(0..=self.seen);
            if let Some(kept) = self.hashes.get_mut(slot as usize) {
                *kept = hash;
            }
        }
        self.seen += 1;
    }
}

impl Shared {
    /// Wait at the gate: the thread's place once it may make a call (it is `busy` until the
    /// place drops, so a call that panics leaves too and `Readers::set_on(false)` never waits on
    /// a dead thread), `None` once the threads stop.
    fn enter(&self) -> Option<Entered<'_>> {
        loop {
            if self.stop.load(Ordering::SeqCst) {
                return None;
            }
            if self.on.load(Ordering::SeqCst) {
                self.busy.fetch_add(1, Ordering::SeqCst);
                let entered = Entered(self);
                // recheck after counting: `Readers::set_on(false)` waits only for counted calls,
                // so a thread that counted itself after it looked must see the closed gate
                if self.on.load(Ordering::SeqCst) {
                    return Some(entered);
                }
                drop(entered);
            }
            thread::park_timeout(IDLE);
        }
    }

    /// Record a reader's call of `kind`: its latency and answer, or its error.
    fn record(&self, kind: ReadKind, outcome: Result<(Duration, Answer), String>) {
        let mut samples = lock(&self.samples);
        let samples = &mut samples.kinds[kind as usize];
        match outcome {
            Ok((latency, answer)) => {
                samples.latencies.push(latency);
                if answer.is_empty() {
                    samples.empty += 1;
                } else {
                    samples.ok += 1;
                }
            }
            Err(message) => samples.error(message),
        }
    }

    /// Add `hashes` to the recent transactions, dropping the oldest past [`RECENT_TXS`].
    fn remember(&self, hashes: impl IntoIterator<Item = B256>) {
        let mut recent = lock(&self.recent);
        recent.extend(hashes);
        let excess = recent.len().saturating_sub(RECENT_TXS);
        recent.drain(..excess);
    }

    /// A recent transaction hash drawn uniformly, or `None` with none known.
    fn recent_tx(&self, rng: &mut StdRng) -> Option<B256> {
        let recent = lock(&self.recent);
        (!recent.is_empty()).then(|| recent[rng.random_range(0..recent.len())])
    }

    /// An executed transaction hash drawn uniformly from the sample, or `None` with none fed.
    fn executed_tx(&self, rng: &mut StdRng) -> Option<B256> {
        let executed = lock(&self.executed);
        let hashes = &executed.hashes;
        (!hashes.is_empty()).then(|| hashes[rng.random_range(0..hashes.len())])
    }

    /// Sleep until `deadline`, waking early when the threads stop.
    fn pause(&self, deadline: Instant) {
        while !self.stop.load(Ordering::SeqCst) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return;
            }
            thread::park_timeout(left);
        }
    }
}

/// One reader thread's state.
struct Reader {
    /// The RPC methods it calls.
    methods: Methods,
    /// The runtime its calls run within.
    handle: Handle,
    /// The prefill its accounts come from.
    spec: PrefillSpec,
    /// The account sampler, shared by every reader.
    zipf: Arc<Zipf>,
    /// The kinds it draws and the epoch span.
    mix: ReadMix,
    /// This reader's RNG.
    rng: StdRng,
    /// The gate and the samples.
    shared: Arc<Shared>,
}

/// A thread's place at the gate ([`Shared::enter`]): the call it entered for is in flight, and
/// counted, until this drops.
struct Entered<'a>(&'a Shared);

impl Drop for Entered<'_> {
    fn drop(&mut self) {
        self.0.busy.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Reader {
    /// Call while the gate is open until the readers stop.
    fn run(mut self) {
        let shared = Arc::clone(&self.shared);
        while let Some(entered) = shared.enter() {
            let kind = self.mix.kinds[self.rng.random_range(0..self.mix.kinds.len())];
            self.read(kind);
            drop(entered);
        }
    }

    /// Make one call of `kind`, timed, and record it.
    fn read(&mut self, kind: ReadKind) {
        let read = match self.plan(kind) {
            Ok(read) => read,
            Err(message) => {
                self.shared.record(kind, Err(message));
                return;
            }
        };
        let started = Instant::now();
        let answer = read.run(&self.methods, &self.handle);
        let latency = started.elapsed();
        if let Ok(Answer::Block(_, hashes)) = &answer {
            self.shared.remember(hashes.iter().copied());
        }
        self.shared.record(read.kind(), answer.map(|answer| (latency, answer)));
    }

    /// The query for `kind`: a [`ReadKind::Receipt`] without a recent transaction, or a
    /// [`ReadKind::TxByHash`] without an executed one, fetches the latest block instead.
    fn plan(&mut self, kind: ReadKind) -> Result<Read, String> {
        Ok(match kind {
            ReadKind::Balance => Read::Balance(self.account()),
            ReadKind::TokenBalance => {
                let token = if self.rng.random() { STABLECOIN_A } else { STABLECOIN_B };
                Read::TokenBalance(token, self.account())
            }
            ReadKind::Receipt => {
                self.shared.recent_tx(&mut self.rng).map_or(Read::Block, Read::Receipt)
            }
            ReadKind::Logs => {
                let tip =
                    number(&call(&self.methods, &self.handle, "eth_blockNumber", rpc_params![])?)?;
                Read::Logs(tip.saturating_sub(LOG_BLOCKS - 1), tip)
            }
            ReadKind::Block => Read::Block,
            ReadKind::BalanceAtChain => {
                Read::BalanceAt(self.account(), self.past_block(Span::Chain)?, Span::Chain)
            }
            ReadKind::BalanceAtEpoch => {
                Read::BalanceAt(self.account(), self.past_block(Span::Epoch)?, Span::Epoch)
            }
            ReadKind::StorageAtChain | ReadKind::StorageAtEpoch => {
                let span = if kind == ReadKind::StorageAtChain { Span::Chain } else { Span::Epoch };
                let token = if self.rng.random() { STABLECOIN_A } else { STABLECOIN_B };
                let slot = balance_slot(self.account());
                Read::StorageAt(token, slot, self.past_block(span)?, span)
            }
            ReadKind::TxByHash => self
                .shared
                .executed_tx(&mut self.rng)
                .map_or(Read::Block, |hash| Read::Tx(hash, true)),
            ReadKind::TxByHashMiss => Read::Tx(B256::from(self.rng.random::<[u8; 32]>()), false),
        })
    }

    /// A block below the tip (`eth_blockNumber`, untimed) drawn uniformly from `span`: the whole
    /// chain or the last [`ReadMix::epoch_blocks`] blocks; the genesis while the tip is the
    /// genesis.
    fn past_block(&mut self, span: Span) -> Result<u64, String> {
        let tip = number(&call(&self.methods, &self.handle, "eth_blockNumber", rpc_params![])?)?;
        Ok(self.rng.random_range(span.blocks(tip, self.mix.epoch_blocks)))
    }

    /// A Zipf-drawn prefilled account.
    fn account(&mut self) -> Address {
        self.spec.account(self.zipf.sample(&mut self.rng))
    }
}

/// The prefilled accounts' sampler, as the workload draws its hot recipients.
fn account_sampler(spec: &PrefillSpec) -> eyre::Result<Zipf> {
    eyre::ensure!(spec.accounts > 0, "the side load reads prefilled accounts; the spec has none");
    Ok(Zipf::new(spec.accounts, WorkloadConfig::default().zipf_exponent))
}

/// The addresses the batch builder's poll reads by default: [`POLL_ACCOUNTS`] distinct Zipf-drawn
/// prefilled accounts (every account when the spec has fewer), drawn with an RNG seeded with
/// `seed`, then every sender, in order.
pub fn poll_addresses(spec: &PrefillSpec, seed: u64) -> eyre::Result<Vec<Address>> {
    let zipf = account_sampler(spec)?;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut drawn = HashSet::new();
    let mut addresses = Vec::new();
    while addresses.len() < POLL_ACCOUNTS.min(spec.accounts) {
        let i = zipf.sample(&mut rng);
        if drawn.insert(i) {
            addresses.push(spec.account(i));
        }
    }
    addresses.extend((0..spec.senders).map(|i| spec.sender(i).address()));
    Ok(addresses)
}

/// The reader threads and the optional balance poller, behind one gate (see the module docs).
///
/// Dropping it stops the threads without joining them; [`Self::stop`] joins them.
#[derive(Debug)]
pub struct Readers {
    /// The gate and the samples.
    shared: Arc<Shared>,
    /// The runtime the threads call within.
    handle: Handle,
    /// Every thread, the poller last.
    threads: Vec<JoinHandle<()>>,
}

impl Readers {
    /// Start `threads` readers over `methods` with the gate off, drawing the kinds of `mix` and
    /// accounts of `spec`; reader `i`'s RNG is seeded with `seed + i`.
    ///
    /// Call it within the multi-thread tokio runtime the env's tasks run on.
    pub fn start(
        methods: Methods,
        spec: PrefillSpec,
        mix: ReadMix,
        threads: usize,
        seed: u64,
    ) -> eyre::Result<Self> {
        eyre::ensure!(!mix.kinds.is_empty(), "the readers' mix has no kind");
        let handle = runtime()?;
        let zipf = Arc::new(account_sampler(&spec)?);
        let mut readers = Self { shared: Arc::default(), handle, threads: Vec::new() };
        for i in 0..threads {
            let reader = Reader {
                methods: methods.clone(),
                handle: readers.handle.clone(),
                spec,
                zipf: zipf.clone(),
                mix,
                rng: StdRng::seed_from_u64(seed.wrapping_add(i as u64)),
                shared: readers.shared.clone(),
            };
            readers.spawn(format!("load-reader-{i}"), move || reader.run())?;
        }
        Ok(readers)
    }

    /// Add a thread that, while the gate is open, calls `pool`'s `get_account_balances` for
    /// `addresses` every `interval` (the batch builder's [`POLL_INTERVAL`]); its samples are
    /// [`ReadSamples::polls`].
    pub fn poll_balances(
        &mut self,
        pool: WorkerTxPool,
        addresses: Vec<Address>,
        interval: Duration,
    ) -> eyre::Result<()> {
        let shared = self.shared.clone();
        self.spawn("load-balance-poller".to_string(), move || {
            while let Some(entered) = shared.enter() {
                let started = Instant::now();
                let balances = pool.get_account_balances(&addresses);
                let latency = started.elapsed();
                let mut samples = lock(&shared.samples);
                samples.polls.latencies.push(latency);
                samples.polls.last = balances;
                drop(samples);
                drop(entered);
                shared.pause(started + interval);
            }
        })
    }

    /// Spawn a named thread that the gate wakes.
    fn spawn(&mut self, name: String, body: impl FnOnce() + Send + 'static) -> eyre::Result<()> {
        self.threads.push(thread::Builder::new().name(name).spawn(body)?);
        Ok(())
    }

    /// Add the executed transactions `hashes` to those [`ReadKind::Receipt`] (the recent ones)
    /// and [`ReadKind::TxByHash`] (a uniform sample of all fed) draw from. Feed only executed
    /// transactions: a [`ReadKind::TxByHash`] of a hash without an answer is an error.
    pub fn feed_tx_hashes(&self, hashes: impl IntoIterator<Item = B256>) {
        let hashes: Vec<B256> = hashes.into_iter().collect();
        let mut executed = lock(&self.shared.executed);
        hashes.iter().for_each(|hash| executed.add(*hash));
        drop(executed);
        self.shared.remember(hashes);
    }

    /// Open or close the gate. Closing returns once every call in flight is recorded: no sample
    /// lands after it until the gate opens again.
    pub fn set_on(&self, on: bool) {
        self.shared.on.store(on, Ordering::SeqCst);
        if on {
            self.threads.iter().for_each(|thread| thread.thread().unpark());
            return;
        }
        while self.shared.busy.load(Ordering::SeqCst) > 0 {
            thread::sleep(Duration::from_micros(100));
        }
    }

    /// The samples since the last drain, clearing them.
    pub fn drain(&self) -> ReadSamples {
        std::mem::take(&mut *lock(&self.shared.samples))
    }

    /// Stop and join every thread, then return the samples since the last drain.
    pub fn stop(mut self) -> eyre::Result<ReadSamples> {
        self.halt();
        for thread in std::mem::take(&mut self.threads) {
            thread.join().map_err(|_| eyre::eyre!("a side-load thread panicked"))?;
        }
        Ok(self.drain())
    }

    /// Tell every thread to return.
    fn halt(&self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        self.threads.iter().for_each(|thread| thread.thread().unpark());
    }
}

impl Drop for Readers {
    fn drop(&mut self) {
        self.halt();
    }
}

/// The [`Submitter`]'s calls in a window.
#[derive(Clone, Debug, Default)]
pub struct SubmitSamples {
    /// The latency of every call, refused ones included.
    pub latencies: Vec<Duration>,
    /// Transactions the pool admitted.
    pub accepted: u64,
    /// Refused with `nonce too low`: already mined.
    pub nonce_too_low: u64,
    /// Refused with `already known`: already pooled.
    pub already_known: u64,
    /// Any other refusal, or an answer that is not the transaction's hash.
    pub errors: u64,
    /// The first error's message.
    pub first_error: Option<String>,
}

impl SubmitSamples {
    /// Record one submission of `raw`: its latency and the RPC's answer.
    fn record(&mut self, raw: &[u8], latency: Duration, answer: Result<Value, String>) {
        self.latencies.push(latency);
        let error = match answer.and_then(|value| tx_hash(&value)) {
            Ok(hash) if hash == keccak256(raw) => {
                self.accepted += 1;
                return;
            }
            Ok(hash) => format!("accepted as {hash}, not {}", keccak256(raw)),
            Err(e) if e.contains("nonce too low") => {
                self.nonce_too_low += 1;
                return;
            }
            Err(e) if e.contains("already known") => {
                self.already_known += 1;
                return;
            }
            Err(e) => e,
        };
        self.errors += 1;
        self.first_error.get_or_insert(error);
    }
}

/// A thread that submits raw transactions through `eth_sendRawTransaction`, in the order queued.
///
/// Dropping it lets the thread finish the queue and return without joining it; [`Self::stop`]
/// joins it.
#[derive(Debug)]
pub struct Submitter {
    /// The queue; `None` once stopping.
    queue: Option<mpsc::Sender<Vec<Vec<u8>>>>,
    /// The window's samples.
    samples: Arc<Mutex<SubmitSamples>>,
    /// The submitting thread.
    thread: Option<JoinHandle<()>>,
}

impl Submitter {
    /// Start the thread over `methods`, within the current multi-thread tokio runtime.
    pub fn start(methods: Methods) -> eyre::Result<Self> {
        let handle = runtime()?;
        let (queue, txs) = mpsc::channel::<Vec<Vec<u8>>>();
        let samples = Arc::new(Mutex::new(SubmitSamples::default()));
        let shared = samples.clone();
        let thread =
            thread::Builder::new().name("load-submitter".to_string()).spawn(move || {
                for raw in txs.iter().flatten() {
                    let params = rpc_params![Bytes::copy_from_slice(&raw)];
                    let started = Instant::now();
                    let answer = call(&methods, &handle, "eth_sendRawTransaction", params);
                    lock(&shared).record(&raw, started.elapsed(), answer);
                }
            })?;
        Ok(Self { queue: Some(queue), samples, thread: Some(thread) })
    }

    /// Queue `txs` (EIP-2718 encoded, such as a subset of `LoadOutput::raw_txs`) for submission.
    pub fn submit(&self, txs: Vec<Vec<u8>>) -> eyre::Result<()> {
        self.queue
            .as_ref()
            .ok_or_else(|| eyre::eyre!("the submitter is stopping"))?
            .send(txs)
            .map_err(|_| eyre::eyre!("the submitter thread is gone"))
    }

    /// The samples since the last drain, clearing them.
    pub fn drain(&self) -> SubmitSamples {
        std::mem::take(&mut *lock(&self.samples))
    }

    /// Submit what is queued, join the thread, then return the samples since the last drain.
    pub fn stop(mut self) -> eyre::Result<SubmitSamples> {
        self.queue = None;
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| eyre::eyre!("the submitter thread panicked"))?;
        }
        Ok(self.drain())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{
            execute_payload_and_update_canonical_chain,
            load::{
                open_production_env,
                state::{ACCOUNT_NATIVE, ACCOUNT_TOKENS, SENDER_NATIVE},
                workload::LoadKind,
                LoadOutput, OutputConfig, OutputFactory,
            },
        },
        worker::WorkerNetwork,
        RethEnv,
    };
    use jsonrpsee::RpcModule;
    use reth_provider::StateProvider as _;
    use tn_types::{
        gas_accumulator::{BaseFeeContainer, GasAccumulator},
        max_batch_gas, Hash as _, NodeMode, TaskManager,
    };

    /// A small prefill: 500 accounts, a few senders, two pools.
    fn spec() -> PrefillSpec {
        PrefillSpec { accounts: 500, senders: 8, pools: 2, seed: 21, registry: false }
    }

    /// An env on a prefilled genesis, opened as the node opens it, with its pool and HTTP methods.
    struct Node {
        env: RethEnv,
        pool: WorkerTxPool,
        methods: Methods,
        _tasks: TaskManager,
        _tmp: tempfile::TempDir,
    }

    impl Node {
        /// Open the env and build the pool and the RPC server as the worker does.
        fn open() -> eyre::Result<Self> {
            let tmp = tempfile::tempdir()?;
            let tasks = TaskManager::default();
            let env =
                open_production_env(tmp.path(), Arc::new(spec().genesis().into()), &[], &tasks)?;
            let pool = env.init_txn_pool(BaseFeeContainer::default())?;
            let methods = env
                .get_rpc_server(
                    pool.clone(),
                    WorkerNetwork::new_for_test(env.chainspec()),
                    GasAccumulator::new(1).worker_base_fee(0),
                    || NodeMode::CvvActive,
                    RpcModule::new(()),
                )?
                .http_methods(|_| true)
                .ok_or_else(|| eyre::eyre!("the production env serves HTTP"))?;
            Ok(Self { env, pool, methods, _tasks: tasks, _tmp: tmp })
        }

        /// A factory of two 3M-gas batches per output.
        fn factory(&self) -> eyre::Result<OutputFactory> {
            let config = OutputConfig { batches: 2, batch_gas: 3_000_000, ..Default::default() };
            OutputFactory::new(&self.env, spec(), Default::default(), config)
        }

        /// Execute and persist `load`, one block per batch, as the output tests do.
        fn execute(&self, load: &LoadOutput) -> eyre::Result<()> {
            let output = &load.output;
            for index in 0..load.batches.len() {
                let batch = load.batch(index);
                let digest = output.get_batch_digest(index).expect("a digest per batch");
                let payload = TNPayload::new(
                    self.env.canonical_tip(),
                    batch.beneficiary,
                    index,
                    digest,
                    output,
                    output.digest().into(),
                    batch.base_fee_per_gas,
                    max_batch_gas(output.leader().epoch()),
                    output.prev_randao(index, digest),
                    batch.worker_id,
                );
                execute_payload_and_update_canonical_chain(
                    &self.env,
                    payload,
                    batch.transactions.clone(),
                )?;
            }
            Ok(())
        }

        /// The node after three executed outputs, with the factory for the next.
        fn with_history() -> eyre::Result<(Self, OutputFactory, Vec<LoadOutput>)> {
            let node = Self::open()?;
            let mut factory = node.factory()?;
            let executed = factory.generate(3);
            for load in &executed {
                node.execute(load)?;
            }
            Ok((node, factory, executed))
        }

        /// Run `read` through the readers' own path, off the runtime's worker.
        fn read(&self, read: Read) -> Result<Answer, String> {
            let handle = Handle::current();
            tokio::task::block_in_place(|| read.run(&self.methods, &handle))
        }
    }

    /// Every kind's query answers right on an env with history, and two readers on for 300 ms
    /// record every kind with no errors.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_readers_answer_every_kind() -> eyre::Result<()> {
        let (node, _, executed) = Node::with_history()?;
        let tip = node.env.canonical_tip();
        let state = node.env.latest()?;

        let account = spec().account(0);
        let native = state.account_balance(&account)?.unwrap_or_default();
        assert_eq!(node.read(Read::Balance(account)), Ok(Answer::Balance(native)));
        let held = state.storage(STABLECOIN_B, balance_slot(account))?.unwrap_or_default();
        assert_eq!(node.read(Read::TokenBalance(STABLECOIN_B, account)), Ok(Answer::Balance(held)));

        let last = executed.last().expect("three outputs");
        let raw = last.raw_txs(last.batches.len() - 1);
        let hashes: Vec<B256> = raw.iter().map(keccak256).collect();
        assert_eq!(node.read(Read::Block), Ok(Answer::Block(tip.number, hashes.clone())));
        assert_eq!(node.read(Read::Receipt(hashes[0])), Ok(Answer::Receipt(Some(tip.number))));
        assert_eq!(node.read(Read::Receipt(B256::repeat_byte(7))), Ok(Answer::Receipt(None)));

        let swaps: usize = executed.iter().map(|load| load.count(LoadKind::Swap)).sum();
        assert!(swaps > 0, "the workload swaps");
        assert_eq!(node.read(Read::Logs(1, tip.number)), Ok(Answer::Logs(swaps)));
        assert_eq!(node.read(Read::Logs(0, 0)), Ok(Answer::Logs(0)), "genesis has no swaps");

        let readers = Readers::start(node.methods.clone(), spec(), ReadMix::rpc(), 2, 7)?;
        readers.set_on(true);
        tokio::time::sleep(Duration::from_millis(300)).await;
        readers.set_on(false);
        let samples = readers.stop()?;
        for kind in ReadKind::RPC {
            let calls = samples.kind(kind);
            assert_eq!(calls.errors, 0, "{kind:?}: {:?}", calls.first_error);
            assert!(calls.ok > 0, "{kind:?} answered with content");
            assert_eq!(calls.latencies.len() as u64, calls.ok + calls.empty, "{kind:?}");
        }
        Ok(())
    }

    /// The past-block and by-hash queries answer with the state at their block and the executed
    /// transactions; a random hash answers `null`, and an answer of the wrong kind is an error.
    /// Two readers of the history mix on for 300 ms record every kind with no errors.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_readers_answer_history_kinds() -> eyre::Result<()> {
        let (node, _, executed) = Node::with_history()?;
        let tip = node.env.canonical_tip();
        assert!(tip.number >= 4, "three outputs of two blocks");
        let sender = spec().sender(0).address();
        let account = spec().account(0);
        let past = tip.number - 2;
        let past_hash = node.env.sealed_header_by_number(past)?.expect("a past header").hash();
        let state = node.env.state_by_block_hash_for_test(past_hash)?;
        let native = state.account_balance(&sender)?.unwrap_or_default();
        assert_ne!(native, U256::from(SENDER_NATIVE), "the sender paid for gas by then");

        let at = |account, block| Read::BalanceAt(account, block, Span::Chain);
        assert_eq!(node.read(at(sender, 0)), Ok(Answer::Balance(U256::from(SENDER_NATIVE))));
        assert_eq!(node.read(at(sender, past)), Ok(Answer::Balance(native)));
        let epoch = Read::BalanceAt(account, 0, Span::Epoch);
        assert_eq!(node.read(epoch), Ok(Answer::Balance(U256::from(ACCOUNT_NATIVE))));
        let held = state.storage(STABLECOIN_B, balance_slot(sender))?.unwrap_or_default();
        let slot =
            |holder, block| Read::StorageAt(STABLECOIN_B, balance_slot(holder), block, Span::Chain);
        assert_eq!(node.read(slot(sender, past)), Ok(Answer::Balance(held)));
        assert_eq!(node.read(slot(account, 0)), Ok(Answer::Balance(U256::from(ACCOUNT_TOKENS))));

        let first = executed.first().expect("three outputs");
        let hash = keccak256(&first.raw_txs(0)[0]);
        let random = B256::repeat_byte(7);
        assert_eq!(node.read(Read::Tx(hash, true)), Ok(Answer::Tx(Some(1))));
        assert_eq!(node.read(Read::Tx(random, false)), Ok(Answer::Tx(None)));
        assert!(node.read(Read::Tx(random, true)).is_err(), "an executed hash must answer");
        assert!(node.read(Read::Tx(hash, false)).is_err(), "a random hash must not answer");

        let readers = Readers::start(node.methods.clone(), spec(), ReadMix::history(2), 2, 9)?;
        let hashes = executed.iter().flat_map(|load| {
            (0..load.batches.len()).flat_map(move |i| load.raw_txs(i).iter().map(keccak256))
        });
        readers.feed_tx_hashes(hashes.collect::<Vec<_>>());
        readers.set_on(true);
        tokio::time::sleep(Duration::from_millis(300)).await;
        readers.set_on(false);
        let samples = readers.stop()?;
        for kind in ReadKind::HISTORY {
            let calls = samples.kind(kind);
            assert_eq!(calls.errors, 0, "{kind:?}: {:?}", calls.first_error);
            let answered = if kind == ReadKind::TxByHashMiss { calls.empty } else { calls.ok };
            assert!(answered > 0, "{kind:?} answered");
            assert_eq!(calls.latencies.len() as u64, calls.ok + calls.empty, "{kind:?}");
        }
        for kind in ReadKind::RPC {
            let calls = samples.kind(kind);
            assert_eq!(calls.latencies.len() as u64 + calls.errors, 0, "{kind:?} is not drawn");
        }
        Ok(())
    }

    /// A span covers the blocks below the tip: the whole chain, or the last epoch's blocks.
    #[test]
    fn test_span_blocks() {
        assert_eq!(Span::Chain.blocks(100, 10), 0..=99);
        assert_eq!(Span::Epoch.blocks(100, 10), 90..=99);
        assert_eq!(Span::Epoch.blocks(5, 10), 0..=4, "a chain shorter than an epoch");
        assert_eq!(Span::Epoch.blocks(100, 0), 99..=99, "an epoch of at least one block");
        assert_eq!(Span::Chain.blocks(0, 10), 0..=0, "the genesis while it is the tip");
        assert_eq!(Span::Epoch.blocks(0, 10), 0..=0);
    }

    /// The executed-hash sample keeps every hash while it is short, then stays at
    /// [`EXECUTED_TXS`] with later hashes replacing earlier ones.
    #[test]
    fn test_reservoir_samples_every_hash_fed() {
        let mut reservoir = Reservoir::default();
        let hash = |i: u64| B256::from(U256::from(i));
        (0..EXECUTED_TXS as u64).for_each(|i| reservoir.add(hash(i)));
        assert_eq!(reservoir.hashes, (0..EXECUTED_TXS as u64).map(hash).collect::<Vec<_>>());
        let total = 4 * EXECUTED_TXS as u64;
        (EXECUTED_TXS as u64..total).for_each(|i| reservoir.add(hash(i)));
        assert_eq!(reservoir.hashes.len(), EXECUTED_TXS);
        assert_eq!(reservoir.seen, total);
        let late = reservoir
            .hashes
            .iter()
            .filter(|h| U256::from_be_bytes(h.0) >= U256::from(EXECUTED_TXS))
            .count();
        // uniform: about three quarters of the sample come from the later three quarters
        assert!((late as f64 / EXECUTED_TXS as f64 - 0.75).abs() < 0.02, "{late}");
    }

    /// Every call recorded in `samples`: answered or not, read or poll.
    fn calls(samples: &ReadSamples) -> usize {
        let reads: usize = ReadKind::ALL
            .iter()
            .map(|kind| samples.kind(*kind).latencies.len() + samples.kind(*kind).errors as usize)
            .sum();
        reads + samples.polls.latencies.len()
    }

    /// The gate starts closed, opening it runs the readers and the poller, and once it closes no
    /// sample lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_readers_off_gate_stops_samples() -> eyre::Result<()> {
        let (node, _, _) = Node::with_history()?;
        let mut readers = Readers::start(node.methods.clone(), spec(), ReadMix::rpc(), 2, 3)?;
        let addresses = poll_addresses(&spec(), 3)?;
        readers.poll_balances(node.pool.clone(), addresses, Duration::from_millis(10))?;

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(calls(&readers.drain()), 0, "the gate starts closed");

        readers.set_on(true);
        tokio::time::sleep(Duration::from_millis(150)).await;
        readers.set_on(false);
        let open = readers.drain();
        assert_eq!(open.errors(), 0);
        assert!(calls(&open) > open.polls.latencies.len(), "the readers ran");
        assert!(!open.polls.latencies.is_empty(), "the poller ran");

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(calls(&readers.stop()?), 0, "no call after the gate closed");
        Ok(())
    }

    /// The next output's transactions enter the pool; a resubmission is `already known` and a
    /// mined transaction `nonce too low`, neither an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_submitter_admits_the_next_output() -> eyre::Result<()> {
        let (node, mut factory, executed) = Node::with_history()?;
        let next = factory.next_output();
        let fresh: Vec<Vec<u8>> = next.raw_txs(0).to_vec();
        let mined: Vec<Vec<u8>> = executed[2].raw_txs(0).to_vec();

        let submitter = Submitter::start(node.methods.clone())?;
        submitter.submit(fresh.clone())?;
        submitter.submit(fresh.clone())?;
        submitter.submit(mined.clone())?;
        let samples = submitter.stop()?;

        assert_eq!(samples.errors, 0, "{:?}", samples.first_error);
        assert_eq!(samples.accepted, fresh.len() as u64);
        assert_eq!(samples.already_known, fresh.len() as u64);
        assert_eq!(samples.nonce_too_low, mined.len() as u64);
        assert_eq!(samples.latencies.len(), 2 * fresh.len() + mined.len());
        for raw in &fresh {
            assert!(node.pool.get(&keccak256(raw)).is_some(), "an accepted tx is pooled");
        }
        Ok(())
    }

    /// The poller reads the prefilled balances of its default address set.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_balance_poller_reads_prefilled_balances() -> eyre::Result<()> {
        let node = Node::open()?;
        let addresses = poll_addresses(&spec(), 5)?;
        assert_eq!(addresses.len(), POLL_ACCOUNTS + spec().senders);
        assert_eq!(addresses.iter().collect::<HashSet<_>>().len(), addresses.len(), "distinct");

        let mut readers = Readers::start(node.methods.clone(), spec(), ReadMix::rpc(), 0, 5)?;
        readers.poll_balances(node.pool.clone(), addresses.clone(), Duration::from_millis(20))?;
        readers.set_on(true);
        tokio::time::sleep(Duration::from_millis(150)).await;
        readers.set_on(false);
        let polls = readers.stop()?.polls;

        assert!(polls.latencies.len() >= 2, "polled every interval: {}", polls.latencies.len());
        let (accounts, senders) = addresses.split_at(POLL_ACCOUNTS);
        for account in accounts {
            assert_eq!(polls.last.get(account), Some(&U256::from(ACCOUNT_NATIVE)));
        }
        for sender in senders {
            assert_eq!(polls.last.get(sender), Some(&U256::from(SENDER_NATIVE)));
        }
        Ok(())
    }
}
