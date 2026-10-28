//! The bench's workload: a deterministic stream of busy-node transactions over a
//! [`PrefillSpec`]'s state, cut into batches by expected gas.
//!
//! [`Workload`] plans each transaction serially from a `StdRng` seeded with
//! [`WorkloadConfig::seed`], then signs a batch's transactions in parallel. The bytes depend only
//! on the spec, the config and the senders' starting nonces, never on the thread count.
//!
//! - **Kinds** ([`LoadKind`]) are drawn by count from [`Mix`]: by default 40% native transfers, 40%
//!   ERC20 `transfer`s on [`STABLECOIN_A`] or [`STABLECOIN_B`], 15% swaps through [`SWAP_ROUTER`]
//!   over pools `0..pools`, and 5% contract creations.
//! - **Senders** are uniform over the spec's keyed senders. [`Workload::new`] reads their nonces
//!   once from the env's latest state, so a workload resumes on a datadir that already executed
//!   outputs, and tracks them itself after that.
//! - **Recipients** of native and ERC20 transfers: [`WorkloadConfig::fresh_percent`] of them are
//!   fresh addresses, the rest prefilled accounts `spec.account(i)` with `i` drawn from a Zipf
//!   distribution of exponent [`WorkloadConfig::zipf_exponent`] (hot keys). A fresh address is the
//!   last 20 bytes of `keccak256("fresh" ‖ sender ‖ nonce)`, unique on the chain, so a resumed
//!   workload never reuses one.
//! - **Creations** deploy a runtime of [`WorkloadConfig::create_size`] bytes: `STOP`, the sender
//!   and the nonce, then bytes from the RNG, so every creation adds a new bytecode.
//!
//! Every transaction's gas use is known exactly before it runs: 21,000, plus its calldata (4 per
//! zero byte, 16 per other byte), plus a per-kind cost: nothing for a native transfer,
//! [`ERC20_TO_HOLDER_GAS`] or [`ERC20_TO_FRESH_GAS`] for an ERC20 transfer, [`SWAP_GAS`] for a
//! swap. A creation adds 32,000, 2 per word of init code, the init code's run (`16 + 6w + w²/512`
//! for a runtime of `w` words) and 200 per deployed byte. EIP-7623's calldata floor never binds
//! for these kinds. Each transaction's gas limit is its expected gas plus 10%, so a wrong table
//! shows up as a gas mismatch, not an out-of-gas revert.
//!
//! [`Workload::next_batch`] fills a batch while its expected gas stays within the target and each
//! transaction's gas limit fits [`WorkloadConfig::block_gas_limit`] on top of the expected gas
//! before it. Execution refuses a whole block when a transaction's gas limit exceeds the block's
//! remaining gas, and this rule keeps every batch clear of that. The transaction that would
//! overflow a batch opens the next one.
//!
//! On the test chain (`test_workload_*` measure it), a native transfer uses 21,000 gas, an ERC20
//! transfer about 39k to a holder and 56k to a fresh address, a swap about 79.5k, and a 1 KiB
//! creation about 275k.

use super::state::{
    swap_calldata, transfer_calldata, PrefillSpec, STABLECOIN_A, STABLECOIN_B, SWAP_ROUTER,
};
use crate::{test_utils::TransactionFactory, RethChainSpec, RethEnv};
use rand::{rngs::StdRng, Rng, SeedableRng as _};
use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
use reth_provider::StateProvider as _;
use std::{fmt, sync::Arc};
use tn_types::{keccak256, max_batch_gas, Address, Bytes, U256};

/// The gas every transaction pays before its calldata.
const TX_GAS: u64 = 21_000;

/// The gas a contract-creating transaction pays on top of [`TX_GAS`].
const CREATE_GAS: u64 = 32_000;

/// The gas an ERC20 `transfer` to an existing holder uses beyond 21,000 and its calldata.
pub const ERC20_TO_HOLDER_GAS: u64 = 17_390;

/// The gas an ERC20 `transfer` to a fresh address uses beyond 21,000 and its calldata: the new
/// balance slot costs a zero-to-nonzero `SSTORE`.
pub const ERC20_TO_FRESH_GAS: u64 = 34_490;

/// The gas a swap through [`SWAP_ROUTER`] uses beyond 21,000 and its calldata.
pub const SWAP_GAS: u64 = 58_180;

/// The bytes every created runtime starts with: `STOP`, the sender and the nonce.
pub const CREATE_PREFIX: usize = 1 + 20 + 8;

/// The largest runtime a creation may deploy (EIP-170).
pub const MAX_CODE_SIZE: usize = 24_576;

/// The length of [`init_code`]'s header, where the runtime starts.
const INIT_HEADER: u8 = 10;

/// The largest native transfer, in wei.
const MAX_NATIVE_VALUE: u64 = 10u64.pow(15);

/// The largest ERC20 transfer or swap input, in token units.
const MAX_TOKEN_AMOUNT: u64 = 10u64.pow(18);

/// The smallest swap input: large enough that the pool always pays out a nonzero amount, which
/// keeps the swap's gas constant.
const MIN_SWAP_AMOUNT: u64 = 10u64.pow(12);

/// What a workload transaction does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LoadKind {
    /// A native transfer to a recipient.
    Native,
    /// An ERC20 `transfer` on [`STABLECOIN_A`] or [`STABLECOIN_B`] to a recipient.
    Erc20,
    /// A swap of token A for token B through [`SWAP_ROUTER`].
    Swap,
    /// A contract creation.
    Create,
}

impl LoadKind {
    /// Every kind, in [`Mix`]'s order.
    pub const ALL: [Self; 4] = [Self::Native, Self::Erc20, Self::Swap, Self::Create];
}

/// The relative weights of the kinds, by transaction count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mix {
    /// The weight of [`LoadKind::Native`].
    pub native: u32,
    /// The weight of [`LoadKind::Erc20`].
    pub erc20: u32,
    /// The weight of [`LoadKind::Swap`].
    pub swap: u32,
    /// The weight of [`LoadKind::Create`].
    pub create: u32,
}

impl Default for Mix {
    /// 40% native, 40% ERC20, 15% swaps and 5% creations.
    fn default() -> Self {
        Self { native: 40, erc20: 40, swap: 15, create: 5 }
    }
}

impl Mix {
    /// A mix of `kind` alone.
    pub fn only(kind: LoadKind) -> Self {
        let none = Self { native: 0, erc20: 0, swap: 0, create: 0 };
        match kind {
            LoadKind::Native => Self { native: 1, ..none },
            LoadKind::Erc20 => Self { erc20: 1, ..none },
            LoadKind::Swap => Self { swap: 1, ..none },
            LoadKind::Create => Self { create: 1, ..none },
        }
    }

    /// The weight of `kind`.
    pub fn weight(&self, kind: LoadKind) -> u32 {
        match kind {
            LoadKind::Native => self.native,
            LoadKind::Erc20 => self.erc20,
            LoadKind::Swap => self.swap,
            LoadKind::Create => self.create,
        }
    }

    /// The sum of the weights.
    fn total(&self) -> u32 {
        LoadKind::ALL.iter().map(|kind| self.weight(*kind)).sum()
    }

    /// A kind drawn by weight.
    fn sample(&self, rng: &mut StdRng) -> LoadKind {
        let mut draw = rng.random_range(0..self.total());
        for kind in LoadKind::ALL {
            match draw.checked_sub(self.weight(kind)) {
                Some(rest) => draw = rest,
                None => return kind,
            }
        }
        unreachable!("the draw is below the total weight")
    }
}

/// How a [`Workload`] draws its transactions.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WorkloadConfig {
    /// The seed of the planning RNG.
    pub seed: u64,
    /// The kinds' weights by count.
    pub mix: Mix,
    /// The percentage of transfer recipients that are fresh addresses.
    pub fresh_percent: u32,
    /// The exponent of the Zipf distribution over the prefilled accounts.
    pub zipf_exponent: f64,
    /// The runtime size of each creation, from [`CREATE_PREFIX`] to [`MAX_CODE_SIZE`] bytes.
    pub create_size: usize,
    /// Every transaction's fee cap in wei; the tip is zero, so it pays the block's base fee.
    pub max_fee_per_gas: u128,
    /// The block gas limit no transaction's gas limit may exceed on top of the gas before it.
    pub block_gas_limit: u64,
}

impl Default for WorkloadConfig {
    /// Seed 0, the default [`Mix`], 20% fresh recipients, Zipf exponent 1.0, 1 KiB creations, a
    /// 1 gwei fee cap and the production `max_batch_gas`.
    fn default() -> Self {
        Self {
            seed: 0,
            mix: Mix::default(),
            fresh_percent: 20,
            zipf_exponent: 1.0,
            create_size: 1024,
            max_fee_per_gas: 1_000_000_000,
            block_gas_limit: max_batch_gas(0),
        }
    }
}

/// One batch of a [`Workload`]: its signed transactions and the gas each is expected to use.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoadBatch {
    /// The EIP-2718 encoded transactions, in execution order.
    pub txs: Vec<Vec<u8>>,
    /// Each transaction's kind, parallel to `txs`.
    pub kinds: Vec<LoadKind>,
    /// Each transaction's exact expected gas use, parallel to `txs`.
    pub gas: Vec<u64>,
}

impl LoadBatch {
    /// The gas the batch is expected to use.
    pub fn expected_gas(&self) -> u64 {
        self.gas.iter().sum()
    }

    /// How many of the batch's transactions are of `kind`.
    pub fn count(&self, kind: LoadKind) -> usize {
        self.kinds.iter().filter(|k| **k == kind).count()
    }
}

/// A deterministic generator of busy-node transactions over a [`PrefillSpec`]'s state (see the
/// module docs).
#[derive(Debug)]
pub struct Workload {
    /// The prefilled state the transactions run on.
    spec: PrefillSpec,
    /// How transactions are drawn.
    config: WorkloadConfig,
    /// The chain the transactions are signed for.
    chain: Arc<RethChainSpec>,
    /// The planning RNG.
    rng: StdRng,
    /// A signer for each sender.
    senders: Vec<TransactionFactory>,
    /// Each sender's address.
    addresses: Vec<Address>,
    /// Each sender's next nonce.
    nonces: Vec<u64>,
    /// The sampler of existing recipients.
    zipf: Zipf,
    /// The planned transaction that overflowed the last batch, which opens the next one.
    pending: Option<Planned>,
}

impl Workload {
    /// A workload on `env`'s latest state, with each sender's nonce read from it.
    pub fn new(env: &RethEnv, spec: PrefillSpec, config: WorkloadConfig) -> eyre::Result<Self> {
        let transfers = config.mix.native + config.mix.erc20 > 0;
        eyre::ensure!(spec.senders > 0, "a workload needs senders");
        eyre::ensure!(config.mix.total() > 0, "a workload needs a kind with weight");
        eyre::ensure!(config.mix.swap == 0 || spec.pools > 0, "swaps need pools");
        eyre::ensure!(
            !transfers || config.fresh_percent >= 100 || spec.accounts > 0,
            "existing recipients need accounts"
        );
        eyre::ensure!(
            (CREATE_PREFIX..=MAX_CODE_SIZE).contains(&config.create_size),
            "a creation deploys {CREATE_PREFIX}..={MAX_CODE_SIZE} bytes, not {}",
            config.create_size
        );

        let senders = spec.sender_factories();
        let addresses: Vec<Address> = senders.iter().map(TransactionFactory::address).collect();
        let state = env.latest()?;
        let nonces = addresses
            .iter()
            .map(|address| Ok(state.account_nonce(address)?.unwrap_or_default()))
            .collect::<eyre::Result<_>>()?;
        Ok(Self {
            spec,
            config,
            chain: env.chainspec().reth_chain_spec(),
            rng: StdRng::seed_from_u64(config.seed),
            senders,
            addresses,
            nonces,
            zipf: Zipf::new(spec.accounts, config.zipf_exponent),
            pending: None,
        })
    }

    /// The next batch: transactions in planned order while the expected gas stays within
    /// `target_gas` and each gas limit fits [`WorkloadConfig::block_gas_limit`] on top of the
    /// expected gas before it. Planning is serial; signing runs on rayon.
    pub fn next_batch(&mut self, target_gas: u64) -> LoadBatch {
        let mut planned = Vec::new();
        let mut gas = 0;
        loop {
            let next = self.pending.take().unwrap_or_else(|| self.plan());
            if gas + next.gas > target_gas || gas + next.gas_limit() > self.config.block_gas_limit {
                self.pending = Some(next);
                break;
            }
            gas += next.gas;
            planned.push(next);
        }

        let (chain, senders, fee) = (&self.chain, &self.senders, self.config.max_fee_per_gas);
        let txs = planned
            .par_iter()
            .map(|tx| {
                let mut signer = senders[tx.sender];
                signer.set_nonce(tx.nonce);
                signer.create_eip1559_encoded(
                    chain.clone(),
                    Some(tx.gas_limit()),
                    fee,
                    tx.to,
                    tx.value,
                    tx.input.clone(),
                )
            })
            .collect();
        LoadBatch {
            txs,
            kinds: planned.iter().map(|tx| tx.kind).collect(),
            gas: planned.iter().map(|tx| tx.gas).collect(),
        }
    }

    /// Plan the next transaction: its kind, sender, nonce, recipient and amount.
    fn plan(&mut self) -> Planned {
        let kind = self.config.mix.sample(&mut self.rng);
        let sender = self.rng.random_range(0..self.senders.len());
        let nonce = self.nonces[sender];
        self.nonces[sender] += 1;
        let from = self.addresses[sender];
        let planned = |to, value, input: Bytes, gas| Planned {
            kind,
            sender,
            nonce,
            to,
            value,
            gas: TX_GAS + data_gas(&input) + gas,
            input,
        };
        match kind {
            LoadKind::Native => {
                let (to, _) = self.recipient(from, nonce);
                let value = U256::from(self.rng.random_range(1..=MAX_NATIVE_VALUE));
                planned(Some(to), value, Bytes::new(), 0)
            }
            LoadKind::Erc20 => {
                let token = if self.rng.random() { STABLECOIN_A } else { STABLECOIN_B };
                let (to, fresh) = self.recipient(from, nonce);
                let amount = U256::from(self.rng.random_range(1..=MAX_TOKEN_AMOUNT));
                let gas = if fresh { ERC20_TO_FRESH_GAS } else { ERC20_TO_HOLDER_GAS };
                planned(Some(token), U256::ZERO, transfer_calldata(to, amount), gas)
            }
            LoadKind::Swap => {
                let pool = self.rng.random_range(0..self.spec.pools as u64);
                let amount = U256::from(self.rng.random_range(MIN_SWAP_AMOUNT..=MAX_TOKEN_AMOUNT));
                planned(Some(SWAP_ROUTER), U256::ZERO, swap_calldata(pool, amount), SWAP_GAS)
            }
            LoadKind::Create => {
                let mut runtime = vec![0; self.config.create_size];
                runtime[1..21].copy_from_slice(from.as_slice());
                runtime[21..CREATE_PREFIX].copy_from_slice(&nonce.to_be_bytes());
                self.rng.fill(&mut runtime[CREATE_PREFIX..]);
                let input = init_code(&runtime);
                let gas = CREATE_GAS + 2 * words(input.len()) + run_gas(runtime.len());
                planned(None, U256::ZERO, input, gas)
            }
        }
    }

    /// A transfer recipient for `from`'s transaction at `nonce`, and whether it is fresh.
    fn recipient(&mut self, from: Address, nonce: u64) -> (Address, bool) {
        if self.rng.random_range(0..100) < self.config.fresh_percent {
            let hash =
                keccak256([b"fresh".as_slice(), from.as_slice(), &nonce.to_be_bytes()].concat());
            (Address::from_slice(&hash[12..]), true)
        } else {
            (self.spec.account(self.zipf.sample(&mut self.rng)), false)
        }
    }
}

/// One planned transaction, before signing.
#[derive(Debug)]
struct Planned {
    /// What it does.
    kind: LoadKind,
    /// The index of its sender.
    sender: usize,
    /// Its nonce.
    nonce: u64,
    /// The callee, or `None` for a creation.
    to: Option<Address>,
    /// The native value it moves.
    value: U256,
    /// Its calldata or init code.
    input: Bytes,
    /// The exact gas it is expected to use.
    gas: u64,
}

impl Planned {
    /// The gas limit: the expected gas plus 10%.
    fn gas_limit(&self) -> u64 {
        self.gas + self.gas / 10
    }
}

/// The intrinsic gas of `input` as calldata: 4 per zero byte, 16 per other byte.
fn data_gas(input: &[u8]) -> u64 {
    input.iter().map(|byte| if *byte == 0 { 4 } else { 16 }).sum()
}

/// The 32-byte words covering `len` bytes.
fn words(len: usize) -> u64 {
    len.div_ceil(32) as u64
}

/// The gas of running [`init_code`] for a runtime of `len` bytes and depositing it: 16 for the
/// header's opcodes, `CODECOPY`'s copy and memory (`6w + w²/512` for `w` words), and 200 per
/// deployed byte.
fn run_gas(len: usize) -> u64 {
    let w = words(len);
    16 + 6 * w + w * w / 512 + 200 * len as u64
}

/// Init code that returns `runtime`: `PUSH2 len, DUP1, PUSH1 10, PUSH0, CODECOPY, PUSH0, RETURN`,
/// then the runtime from offset 10.
fn init_code(runtime: &[u8]) -> Bytes {
    let [hi, lo] = u16::try_from(runtime.len()).expect("a runtime fits PUSH2").to_be_bytes();
    let header = [0x61, hi, lo, 0x80, 0x60, INIT_HEADER, 0x5f, 0x39, 0x5f, 0xf3];
    debug_assert_eq!(header.len(), INIT_HEADER as usize);
    [header.as_slice(), runtime].concat().into()
}

/// An exact sampler of the Zipf distribution over `0..n`: index `i` has weight `1 / (i + 1)^s`.
pub(super) struct Zipf {
    /// The cumulative probability of each index; the last is exactly 1.
    cdf: Vec<f64>,
}

impl fmt::Debug for Zipf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Zipf").field("n", &self.cdf.len()).finish()
    }
}

impl Zipf {
    /// The sampler over `0..n` with exponent `s`.
    pub(super) fn new(n: usize, s: f64) -> Self {
        let mut total = 0.0;
        let mut cdf: Vec<f64> = (1..=n)
            .map(|rank| {
                total += (rank as f64).powf(-s);
                total
            })
            .collect();
        cdf.iter_mut().for_each(|p| *p /= total);
        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }
        Self { cdf }
    }

    /// An index drawn from the distribution: the first whose cumulative probability exceeds a
    /// uniform draw from `[0, 1)`.
    pub(super) fn sample(&self, rng: &mut StdRng) -> usize {
        let draw: f64 = rng.random();
        self.cdf.partition_point(|p| *p <= draw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{
            consensus_output_for_tests_at, execute_payload_and_update_canonical_chain,
            load::{
                open_production_env,
                state::{balance_slot, reserve_slots, IERC20, POOL_RESERVE},
            },
        },
        ExecutedBlock,
    };
    use alloy::sol_types::SolCall as _;
    use reth_primitives_traits::SignerRecoverable as _;
    use std::collections::{HashMap, HashSet};
    use tn_types::{
        Decodable2718 as _, Recovered, TaskManager, TransactionSigned, TransactionTrait as _, B256,
    };

    /// The target of the per-kind and mixed batches.
    const TARGET: u64 = 15_000_000;

    /// A small prefill: 2k accounts, a few senders, two pools.
    fn spec() -> PrefillSpec {
        PrefillSpec { accounts: 2_000, senders: 8, pools: 2, seed: 11, registry: false }
    }

    /// An env on a prefilled genesis in a temp dir, opened as the node opens it.
    struct Bench {
        env: RethEnv,
        _tasks: TaskManager,
        _tmp: tempfile::TempDir,
    }

    impl Bench {
        fn open(spec: &PrefillSpec) -> eyre::Result<Self> {
            let tmp = tempfile::tempdir()?;
            let tasks = TaskManager::default();
            let env =
                open_production_env(tmp.path(), Arc::new(spec.genesis().into()), &[], &tasks)?;
            Ok(Self { env, _tasks: tasks, _tmp: tmp })
        }

        /// Execute and persist `batch` as one block on the canonical tip, and check that every
        /// transaction ran, succeeded and used exactly its expected gas.
        fn execute(&self, batch: &LoadBatch) -> eyre::Result<ExecutedBlock> {
            let parent = self.env.canonical_tip();
            let number = parent.number + 1;
            let output = consensus_output_for_tests_at(
                number as u32,
                0,
                number,
                false,
                parent.timestamp + 1,
            );
            let payload = TNPayload::new_for_test(parent, &output);
            let block =
                execute_payload_and_update_canonical_chain(&self.env, payload, batch.txs.clone())?;
            let receipts = &block.execution_output.result.receipts;
            assert_eq!(receipts.len(), batch.txs.len(), "no transaction is skipped");
            let mut before = 0;
            for (i, receipt) in receipts.iter().enumerate() {
                let kind = batch.kinds[i];
                assert!(receipt.success, "tx {i} ({kind:?}) succeeds");
                let used = receipt.cumulative_gas_used - before;
                assert_eq!(used, batch.gas[i], "tx {i} ({kind:?}) uses its expected gas");
                before = receipt.cumulative_gas_used;
            }
            assert_eq!(block.recovered_block.header().gas_used, batch.expected_gas());
            Ok(block)
        }

        /// `address`'s storage at `slot` in the state after block `at`.
        fn storage(&self, at: B256, address: Address, slot: B256) -> eyre::Result<U256> {
            Ok(self.env.state_by_block_hash(at)?.storage(address, slot)?.unwrap_or_default())
        }

        /// `address`'s native balance in the state after block `at`.
        fn balance(&self, at: B256, address: Address) -> eyre::Result<U256> {
            Ok(self.env.state_by_block_hash(at)?.account_balance(&address)?.unwrap_or_default())
        }
    }

    /// A transaction decoded from a batch, with its signer.
    fn decode(raw: &[u8]) -> Recovered<TransactionSigned> {
        TransactionSigned::decode_2718(&mut &raw[..])
            .expect("a workload tx decodes")
            .try_into_recovered()
            .expect("a workload tx recovers")
    }

    /// One `TARGET` batch of `kind` alone, executed on a fresh env: every transaction succeeds
    /// with its exact expected gas, and the batch lands within 5% of the target.
    fn single_kind(kind: LoadKind) -> eyre::Result<(Bench, LoadBatch, ExecutedBlock)> {
        let spec = spec();
        let bench = Bench::open(&spec)?;
        let config = WorkloadConfig { seed: 3, mix: Mix::only(kind), ..Default::default() };
        let batch = Workload::new(&bench.env, spec, config)?.next_batch(TARGET);
        assert!(!batch.txs.is_empty());
        assert_eq!(batch.count(kind), batch.txs.len(), "a single-kind batch");
        assert!(batch.expected_gas() <= TARGET, "the batch stays within its target");
        let block = bench.execute(&batch)?;
        let used = block.recovered_block.header().gas_used;
        assert!(used.abs_diff(TARGET) * 20 <= TARGET, "{used} gas is within 5% of {TARGET}");
        Ok((bench, batch, block))
    }

    /// Native transfers credit their recipients: fresh ones and Zipf-hot prefilled accounts.
    #[tokio::test]
    async fn test_workload_native_transfers() -> eyre::Result<()> {
        let (bench, batch, block) = single_kind(LoadKind::Native)?;
        let spec = spec();
        let (genesis, after) = (block.recovered_block.parent_hash, block.recovered_block.hash());
        let mut credits: HashMap<Address, U256> = HashMap::new();
        let mut fresh = 0;
        for raw in &batch.txs {
            let tx = decode(raw);
            let to = tx.to().expect("a call");
            fresh += usize::from(bench.balance(genesis, to)?.is_zero());
            *credits.entry(to).or_default() += tx.value();
        }
        for (to, credit) in &credits {
            let before = bench.balance(genesis, *to)?;
            assert_eq!(bench.balance(after, *to)?, before + credit, "{to} is credited");
        }
        let share = 100 * fresh / batch.txs.len();
        assert!((15..=25).contains(&share), "{share}% of the recipients are fresh, not 20%");
        let hits =
            |i| batch.txs.iter().filter(|raw| decode(raw).to() == Some(spec.account(i))).count();
        assert!(hits(0) > hits(1), "account 0 is the hottest");
        Ok(())
    }

    /// ERC20 transfers move token balances to fresh and existing holders on both tokens.
    #[tokio::test]
    async fn test_workload_erc20_transfers() -> eyre::Result<()> {
        let (bench, batch, block) = single_kind(LoadKind::Erc20)?;
        let mut credits: HashMap<(Address, Address), U256> = HashMap::new();
        for raw in &batch.txs {
            let tx = decode(raw);
            let call = IERC20::transferCall::abi_decode(tx.input())?;
            *credits.entry((tx.to().expect("a call"), call.to)).or_default() += call.value;
        }
        let (genesis, after) = (block.recovered_block.parent_hash, block.recovered_block.hash());
        let mut fresh = 0;
        for ((token, to), credit) in &credits {
            let before = bench.storage(genesis, *token, balance_slot(*to))?;
            fresh += usize::from(before.is_zero());
            assert_eq!(bench.storage(after, *token, balance_slot(*to))?, before + credit);
        }
        assert!(fresh > 0 && fresh < credits.len(), "fresh and existing holders");
        let tokens: HashSet<Address> = credits.keys().map(|(token, _)| *token).collect();
        assert_eq!(tokens, HashSet::from([STABLECOIN_A, STABLECOIN_B]), "both tokens");
        Ok(())
    }

    /// Swaps move every pool's reserve0 by the inputs swapped into it.
    #[tokio::test]
    async fn test_workload_swaps() -> eyre::Result<()> {
        let (bench, batch, block) = single_kind(LoadKind::Swap)?;
        let mut inputs: HashMap<u64, U256> = HashMap::new();
        for raw in &batch.txs {
            let tx = decode(raw);
            assert_eq!(tx.to(), Some(SWAP_ROUTER));
            let pool = U256::from_be_slice(&tx.input()[..32]).to::<u64>();
            *inputs.entry(pool).or_default() += U256::from_be_slice(&tx.input()[32..]);
        }
        assert_eq!(inputs.len(), spec().pools, "every pool is used");
        let after = block.recovered_block.hash();
        for (pool, input) in inputs {
            let (reserve0, reserve1) = reserve_slots(pool);
            let reserve = U256::from(POOL_RESERVE);
            assert_eq!(bench.storage(after, SWAP_ROUTER, reserve0)?, reserve + input);
            assert!(bench.storage(after, SWAP_ROUTER, reserve1)? < reserve, "reserve1 pays out");
        }
        Ok(())
    }

    /// Creations deploy distinct runtimes of the configured size at the senders' create
    /// addresses.
    #[tokio::test]
    async fn test_workload_creations() -> eyre::Result<()> {
        let (bench, batch, _) = single_kind(LoadKind::Create)?;
        let state = bench.env.latest()?;
        let mut codes = HashSet::new();
        for raw in &batch.txs {
            let tx = decode(raw);
            let created = tx.signer().create(tx.nonce());
            let code = state.account_code(&created)?.expect("code at the created address");
            let code = code.original_bytes();
            assert_eq!(code.len(), WorkloadConfig::default().create_size);
            assert_eq!(&code[1..21], tx.signer().as_slice(), "the runtime embeds its sender");
            assert!(codes.insert(code), "every runtime is new");
        }
        Ok(())
    }

    /// The default mix holds its proportions by count; the same seed gives the same bytes on
    /// any thread count; a second batch continues the nonces and executes in full.
    #[tokio::test]
    async fn test_workload_default_mix() -> eyre::Result<()> {
        let spec = spec();
        let bench = Bench::open(&spec)?;
        let config = WorkloadConfig { seed: 5, ..Default::default() };
        let on_threads = |config, threads| -> eyre::Result<LoadBatch> {
            let mut workload = Workload::new(&bench.env, spec, config)?;
            let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build()?;
            Ok(pool.install(|| workload.next_batch(TARGET)))
        };
        let serial = on_threads(config, 1)?;
        assert_eq!(serial, on_threads(config, 4)?, "the same seed, the same bytes");
        assert_ne!(serial.txs, on_threads(WorkloadConfig { seed: 6, ..config }, 4)?.txs);

        let mut workload = Workload::new(&bench.env, spec, config)?;
        let first = workload.next_batch(TARGET);
        assert_eq!(first, serial);
        bench.execute(&first)?;
        let second = workload.next_batch(TARGET);
        bench.execute(&second)?;
        let latest = bench.env.latest()?;
        for address in &workload.addresses {
            assert!(latest.account_nonce(address)?.unwrap_or_default() > 0, "{address} sent");
        }

        // the shares by count over about 2,800 planned transactions, within 3 points
        let mut planner = Workload::new(&bench.env, spec, config)?;
        let batches: Vec<LoadBatch> = (0..6).map(|_| planner.next_batch(30_000_000)).collect();
        let total: usize = batches.iter().map(|batch| batch.txs.len()).sum();
        for kind in LoadKind::ALL {
            let count: usize = batches.iter().map(|batch| batch.count(kind)).sum();
            let share = 100.0 * count as f64 / total as f64;
            let expected = config.mix.weight(kind) as f64;
            assert!((share - expected).abs() <= 3.0, "{kind:?}: {share:.1}% of {total}");
        }
        Ok(())
    }

    /// A batch's gas limits fit the block gas limit on top of the expected gas before each, with
    /// 10% headroom over each transaction's expected gas; a full 30M batch executes.
    #[tokio::test]
    async fn test_workload_batches_fit_the_block_gas_limit() -> eyre::Result<()> {
        let spec = spec();
        let bench = Bench::open(&spec)?;
        let limit = 10_000_000;
        let config = WorkloadConfig { seed: 4, block_gas_limit: limit, ..Default::default() };
        let mut workload = Workload::new(&bench.env, spec, config)?;
        for _ in 0..4 {
            let batch = workload.next_batch(TARGET);
            let mut before = 0;
            for (raw, gas) in batch.txs.iter().zip(&batch.gas) {
                let gas_limit = decode(raw).gas_limit();
                assert_eq!(gas_limit, gas + gas / 10, "10% headroom");
                assert!(before + gas_limit <= limit, "the gas limit fits the block");
                before += gas;
            }
            assert!(before + 310_000 > limit, "the batch fills the block: {before}");
        }

        let config = WorkloadConfig { seed: 4, ..Default::default() };
        let batch = Workload::new(&bench.env, spec, config)?.next_batch(30_000_000);
        bench.execute(&batch)?;
        Ok(())
    }

    /// A workload created after its senders' transactions executed starts at their on-chain
    /// nonces, and its fresh recipients and runtimes are still new, even with the same seed.
    #[tokio::test]
    async fn test_workload_nonces_from_the_db() -> eyre::Result<()> {
        let spec = spec();
        let bench = Bench::open(&spec)?;
        let mix = Mix { native: 0, erc20: 1, swap: 0, create: 1 };
        let config = WorkloadConfig { seed: 9, mix, fresh_percent: 100, ..Default::default() };
        let mut first = Workload::new(&bench.env, spec, config)?;
        assert!(first.nonces.iter().all(|nonce| *nonce == 0), "genesis nonces");
        let early = first.next_batch(5_000_000);
        bench.execute(&early)?;

        let mut sent = vec![0; spec.senders];
        for raw in &early.txs {
            let signer = decode(raw).signer();
            sent[first.addresses.iter().position(|a| *a == signer).expect("a sender")] += 1;
        }
        let mut resumed = Workload::new(&bench.env, spec, config)?;
        assert_eq!(resumed.nonces, sent, "the on-chain nonces");
        assert!(sent.iter().any(|count| *count > 0), "the senders sent");

        // every ERC20 recipient is still fresh (exact gas) and every runtime is new
        let late = resumed.next_batch(5_000_000);
        let block = bench.execute(&late)?;
        let state = bench.env.state_by_block_hash(block.recovered_block.hash())?;
        let mut codes = HashSet::new();
        for raw in early.txs.iter().chain(&late.txs) {
            let tx = decode(raw);
            if tx.to().is_none() {
                let code = state.account_code(&tx.signer().create(tx.nonce()))?.expect("code");
                assert!(codes.insert(code.hash_slow()), "every runtime is new");
            }
        }
        assert!(late.count(LoadKind::Create) > 0 && late.count(LoadKind::Erc20) > 0);
        Ok(())
    }

    /// The Zipf sampler stays in range, draws index 0 most often, and its frequencies fall with
    /// the index as `1 / (i + 1)`.
    #[test]
    fn test_zipf_favours_low_indices() {
        let (n, draws) = (1_000, 200_000);
        let zipf = Zipf::new(n, 1.0);
        let mut rng = StdRng::seed_from_u64(1);
        let mut counts = vec![0u32; n];
        for _ in 0..draws {
            counts[zipf.sample(&mut rng)] += 1;
        }
        assert_eq!(counts.iter().max(), Some(&counts[0]), "index 0 is the most frequent");
        let harmonic: f64 = (1..=n).map(|rank| 1.0 / rank as f64).sum();
        for i in [0, 1, 9] {
            let expected = draws as f64 / ((i + 1) as f64 * harmonic);
            let ratio = counts[i] as f64 / expected;
            assert!((0.9..1.1).contains(&ratio), "index {i}: {} vs {expected:.0}", counts[i]);
        }
        let mean = |range: std::ops::Range<usize>| {
            counts[range.clone()].iter().sum::<u32>() as f64 / range.len() as f64
        };
        assert!(mean(0..10) > mean(10..100) && mean(10..100) > mean(100..n), "falling");
    }
}
