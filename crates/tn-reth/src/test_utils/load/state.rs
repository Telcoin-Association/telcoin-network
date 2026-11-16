//! The bench's prefilled genesis: funded accounts, two ERC20 tokens with prefilled balances and
//! allowances, and a swap router with prefilled pools, all written into genesis so a large state
//! costs no transactions.
//!
//! [`PrefillSpec`] derives every key, address and amount from its fields, so a workload recreates
//! the senders and the accounts from the spec alone:
//!
//! - **Senders** ([`PrefillSpec::sender`]): `senders` keyed accounts. Sender `i` signs with the key
//!   drawn from a `StdRng` seeded with `keccak256("sender" ‖ seed ‖ i)`. Each holds
//!   [`SENDER_NATIVE`] wei and [`SENDER_TOKENS`] of each token, and allows [`SWAP_ROUTER`] to spend
//!   [`SENDER_TOKENS`] of token A (a finite allowance, so every swap also writes it).
//! - **Accounts** ([`PrefillSpec::account`]): `accounts` plain addresses with no key, the last 20
//!   bytes of `keccak256("account" ‖ seed ‖ i)`. Each holds [`ACCOUNT_NATIVE`] wei and
//!   [`ACCOUNT_TOKENS`] of each token: the population of existing recipients.
//! - **Tokens** [`STABLECOIN_A`] and [`STABLECOIN_B`]: the `Stablecoin` runtime code
//!   (`deployedBytecode.object` of `tn-contracts/artifacts/Stablecoin.json`, which has no
//!   immutables) with OpenZeppelin 5.2's ERC-7201 `ERC20Storage` written directly at base
//!   [`ERC20_STORAGE`] (`S`):
//!   - the balance of `a` at `keccak256(pad32(a) ‖ S)` ([`balance_slot`]);
//!   - the allowance of `owner` for `spender` at `keccak256(pad32(spender) ‖ keccak256(pad32(owner)
//!     ‖ (S + 1)))` ([`allowance_slot`]);
//!   - `_totalSupply` at `S + 2` ([`total_supply_slot`]): the sum of the token's balances.
//!
//!   There is no proxy and no `initialize`: name, symbol, decimals and roles stay empty, and
//!   `transfer`, `transferFrom`, `balanceOf`, `allowance` and `totalSupply` never read them. Every
//!   transfer also reads the `Blacklist` mapping for both parties, where empty means allowed.
//! - **Router** [`SWAP_ROUTER`]: hand-assembled code ([`router_code`]) with an AMM-like footprint.
//!   Its calldata is two words, `(pool, amount_in)`, with no selector ([`swap_calldata`]). It:
//!   1. calls `A.transferFrom(caller, router, amount_in)`, spending the caller's allowance;
//!   2. reads the pool's reserves at `keccak256(pool ‖ 0)` and `keccak256(pool ‖ 1)`
//!      ([`reserve_slots`]), computes `amount_out = amount_in * reserve1 / (reserve0 + amount_in)`,
//!      and writes back `reserve0 + amount_in` and `reserve1 - amount_out`;
//!   3. calls `B.transfer(caller, amount_out)` from its own B balance;
//!   4. emits `ISwapRouter::Swap` (`LOG3`: the event topic, the caller and the pool as topics,
//!      `amount_in` and `amount_out` as data).
//!
//!   It reverts when either token call fails or returns anything but `true`. Genesis gives pools
//!   `0..pools` [`POOL_RESERVE`] of each reserve, and the router [`ROUTER_TOKENS`] of each token.
//!
//! The base is the test genesis at the fixed [`GENESIS_TIMESTAMP`] (`test_genesis_at`), or with
//! `registry`, the genesis of [`test_genesis_with_consensus_registry`] with
//! [`REGISTRY_VALIDATORS`] validators, moved to the same timestamp. Either way the same spec
//! always gives the same genesis hash.
//!
//! On the test chain an ERC20 `transfer` between prefilled holders uses 38,974 gas and a swap
//! 79,520 (`test_transfer_moves_prefilled_balances`, `test_swap_moves_tokens_and_reserves`); a
//! transfer to a fresh recipient pays about 17k more for the new balance slot.
//! [`TRANSFER_GAS_LIMIT`] and [`SWAP_GAS_LIMIT`] cover both with room to spare.

use crate::{
    test_utils::{test_genesis_with_consensus_registry, TransactionFactory},
    RethEnv,
};
use alloy::{
    primitives::b256,
    sol_types::{SolCall as _, SolEvent as _},
};
use secp256k1::rand::{rngs::StdRng, SeedableRng as _};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tn_types::{
    address, keccak256, test_genesis_at, Address, Bytes, Genesis, GenesisAccount, B256, U256,
};

alloy::sol! {
    /// The ERC20 surface the bench calls on [`STABLECOIN_A`] and [`STABLECOIN_B`].
    interface IERC20 {
        /// Emitted by every transfer.
        event Transfer(address indexed from, address indexed to, uint256 value);
        /// Move `value` from the caller to `to`.
        function transfer(address to, uint256 value) external returns (bool);
        /// Move `value` from `from` to `to`, spending the caller's allowance from `from`.
        function transferFrom(address from, address to, uint256 value) external returns (bool);
        /// The balance of `account`.
        function balanceOf(address account) external view returns (uint256);
        /// What `spender` may still move from `owner`.
        function allowance(address owner, address spender) external view returns (uint256);
        /// The sum of all balances.
        function totalSupply() external view returns (uint256);
    }

    /// The event [`SWAP_ROUTER`] emits; its calldata is raw words (see [`swap_calldata`]).
    interface ISwapRouter {
        /// Emitted by every swap.
        event Swap(address indexed sender, uint256 indexed pool, uint256 amountIn, uint256 amountOut);
    }
}

/// Where genesis places token A, which the router takes in.
pub const STABLECOIN_A: Address = address!("0x10ad00000000000000000000000000000000000a");

/// Where genesis places token B, which the router pays out.
pub const STABLECOIN_B: Address = address!("0x10ad00000000000000000000000000000000000b");

/// Where genesis places the swap router.
pub const SWAP_ROUTER: Address = address!("0x10ad000000000000000000000000000000000005");

/// The base slot of OpenZeppelin 5.2's ERC-7201 `ERC20Storage` (`openzeppelin.storage.ERC20`):
/// `_balances`, then `_allowances`, then `_totalSupply`.
pub const ERC20_STORAGE: B256 =
    b256!("0x52c63247e1f47db19d5ce0460030c497f067ca4cebf71ba98eeadabe20bace00");

/// The genesis timestamp, fixed so the same spec always gives the same genesis hash.
pub const GENESIS_TIMESTAMP: u64 = 1_700_000_000;

/// The validators of the `ConsensusRegistry` a `registry` genesis deploys.
pub const REGISTRY_VALIDATORS: usize = 4;

/// Each sender's native balance in wei (one billion TEL).
pub const SENDER_NATIVE: u128 = 10u128.pow(27);

/// Each account's native balance in wei (one TEL).
pub const ACCOUNT_NATIVE: u128 = 10u128.pow(18);

/// Each sender's balance of each token, and its allowance to the router on token A.
pub const SENDER_TOKENS: u128 = 10u128.pow(30);

/// Each account's balance of each token.
pub const ACCOUNT_TOKENS: u128 = 10u128.pow(20);

/// The router's balance of each token: the pools' liquidity.
pub const ROUTER_TOKENS: u128 = 10u128.pow(38);

/// Each prefilled pool's two reserves.
pub const POOL_RESERVE: u128 = 10u128.pow(30);

/// A gas limit that covers an ERC20 `transfer`, to a fresh recipient too.
pub const TRANSFER_GAS_LIMIT: u64 = 100_000;

/// A gas limit that covers a swap through the router.
pub const SWAP_GAS_LIMIT: u64 = 200_000;

/// The prefilled genesis: how many senders, accounts and pools it holds, the seed their keys and
/// addresses derive from, and its base.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefillSpec {
    /// Funded plain addresses with no key ([`Self::account`]): the existing recipients.
    pub accounts: usize,
    /// Funded keyed senders ([`Self::sender`]), each allowing the router to spend its token A.
    pub senders: usize,
    /// Router pools with prefilled reserves, ids `0..pools`.
    pub pools: usize,
    /// The seed every sender key and account address derives from.
    pub seed: u64,
    /// Build on [`test_genesis_with_consensus_registry`] (a `ConsensusRegistry` deployed from the
    /// current artifact, which epoch-closing system calls need) instead of `test_genesis_at`.
    /// [`Self::genesis`] then needs a tokio runtime.
    pub registry: bool,
}

impl PrefillSpec {
    /// The address of account `i`: the last 20 bytes of `keccak256("account" ‖ seed ‖ i)`.
    pub fn account(&self, i: usize) -> Address {
        Address::from_slice(&derive(b"account", self.seed, i)[12..])
    }

    /// A factory signing for sender `i` from nonce 0, its key drawn from a `StdRng` seeded with
    /// `keccak256("sender" ‖ seed ‖ i)`.
    pub fn sender(&self, i: usize) -> TransactionFactory {
        let mut rng = StdRng::from_seed(derive(b"sender", self.seed, i).0);
        TransactionFactory::new_random_from_seed(&mut rng)
    }

    /// A factory for every sender, in order, each from nonce 0.
    pub fn sender_factories(&self) -> Vec<TransactionFactory> {
        (0..self.senders).map(|i| self.sender(i)).collect()
    }

    /// Each token's `_totalSupply`: the senders', the accounts' and the router's balances.
    pub fn total_supply(&self) -> U256 {
        U256::from(SENDER_TOKENS) * U256::from(self.senders)
            + U256::from(ACCOUNT_TOKENS) * U256::from(self.accounts)
            + U256::from(ROUTER_TOKENS)
    }

    /// The prefilled genesis: the base genesis (see the module docs) extended with the senders,
    /// the accounts, both tokens and the router.
    ///
    /// With `registry` this deploys the registry on a temporary chain, so it must run within a
    /// tokio runtime.
    pub fn genesis(&self) -> Genesis {
        let base = if self.registry {
            let mut genesis = test_genesis_with_consensus_registry(REGISTRY_VALIDATORS);
            genesis.timestamp = GENESIS_TIMESTAMP;
            genesis
        } else {
            test_genesis_at(GENESIS_TIMESTAMP)
        };
        base.extend_accounts(self.alloc())
    }

    /// The prefilled accounts and contracts [`Self::genesis`] adds to its base.
    fn alloc(&self) -> Vec<(Address, GenesisAccount)> {
        let senders: Vec<Address> = (0..self.senders).map(|i| self.sender(i).address()).collect();
        let accounts: Vec<Address> = (0..self.accounts).map(|i| self.account(i)).collect();

        // both tokens hold the same balances; token A also holds the senders' allowances
        let token_b: BTreeMap<B256, B256> = senders
            .iter()
            .map(|holder| (balance_slot(*holder), word(SENDER_TOKENS)))
            .chain(accounts.iter().map(|holder| (balance_slot(*holder), word(ACCOUNT_TOKENS))))
            .chain([
                (balance_slot(SWAP_ROUTER), word(ROUTER_TOKENS)),
                (total_supply_slot(), B256::from(self.total_supply())),
            ])
            .collect();
        let mut token_a = token_b.clone();
        token_a.extend(
            senders.iter().map(|owner| (allowance_slot(*owner, SWAP_ROUTER), word(SENDER_TOKENS))),
        );
        let reserves: BTreeMap<B256, B256> = (0..self.pools as u64)
            .flat_map(|pool| {
                let (reserve0, reserve1) = reserve_slots(pool);
                [(reserve0, word(POOL_RESERVE)), (reserve1, word(POOL_RESERVE))]
            })
            .collect();

        let code = stablecoin_code();
        let contract = |code: Bytes, storage| {
            GenesisAccount::default().with_code(Some(code)).with_storage(Some(storage))
        };
        senders
            .into_iter()
            .map(|a| (a, GenesisAccount::default().with_balance(U256::from(SENDER_NATIVE))))
            .chain(
                accounts.into_iter().map(|a| {
                    (a, GenesisAccount::default().with_balance(U256::from(ACCOUNT_NATIVE)))
                }),
            )
            .chain([
                (STABLECOIN_A, contract(code.clone(), token_a)),
                (STABLECOIN_B, contract(code, token_b)),
                (SWAP_ROUTER, contract(router_code(), reserves)),
            ])
            .collect()
    }
}

/// `keccak256(tag ‖ seed ‖ i)`, both numbers as big-endian `u64`.
fn derive(tag: &[u8], seed: u64, i: usize) -> B256 {
    keccak256([tag, &seed.to_be_bytes(), &(i as u64).to_be_bytes()].concat())
}

/// A storage word holding `value`.
fn word(value: u128) -> B256 {
    B256::from(U256::from(value))
}

/// `keccak256(a ‖ b)`: the slot of key `a` in the mapping at slot `b`.
fn mapping_slot(key: B256, base: B256) -> B256 {
    keccak256([key.as_slice(), base.as_slice()].concat())
}

/// `S + offset`: a field of `ERC20Storage`.
fn erc20_field(offset: u64) -> B256 {
    B256::from(U256::from_be_bytes(ERC20_STORAGE.0) + U256::from(offset))
}

/// The slot of `holder`'s balance on either token: `keccak256(pad32(holder) ‖ S)`.
pub fn balance_slot(holder: Address) -> B256 {
    mapping_slot(holder.into_word(), ERC20_STORAGE)
}

/// The slot of what `spender` may move from `owner` on either token:
/// `keccak256(pad32(spender) ‖ keccak256(pad32(owner) ‖ (S + 1)))`.
pub fn allowance_slot(owner: Address, spender: Address) -> B256 {
    mapping_slot(spender.into_word(), mapping_slot(owner.into_word(), erc20_field(1)))
}

/// The slot of either token's `_totalSupply`: `S + 2`.
pub fn total_supply_slot() -> B256 {
    erc20_field(2)
}

/// The router's slots of `pool`'s two reserves: `keccak256(pool ‖ 0)` and `keccak256(pool ‖ 1)`.
pub fn reserve_slots(pool: u64) -> (B256, B256) {
    let pool = B256::from(U256::from(pool));
    (mapping_slot(pool, B256::ZERO), mapping_slot(pool, B256::from(U256::from(1))))
}

/// The calldata of an ERC20 `transfer(to, value)`.
pub fn transfer_calldata(to: Address, value: U256) -> Bytes {
    IERC20::transferCall { to, value }.abi_encode().into()
}

/// The router's calldata: the words `pool` and `amount_in`.
pub fn swap_calldata(pool: u64, amount_in: U256) -> Bytes {
    [B256::from(U256::from(pool)).0, amount_in.to_be_bytes::<32>()].concat().into()
}

/// The artifact genesis takes the tokens' code from (tn-contracts submodule).
pub const STABLECOIN_JSON: &str =
    include_str!("../../../../../tn-contracts/artifacts/Stablecoin.json");

/// The `Stablecoin` runtime code: [`STABLECOIN_JSON`]'s `deployedBytecode.object`.
pub fn stablecoin_code() -> Bytes {
    RethEnv::fetch_value_from_json_str(STABLECOIN_JSON, Some("deployedBytecode.object"))
        .ok()
        .and_then(|object| object.as_str()?.parse().ok())
        .expect("Stablecoin.json holds deployedBytecode.object as hex")
}

/// The EVM opcodes [`router_code`] uses.
mod op {
    pub(super) const STOP: u8 = 0x00;
    pub(super) const ADD: u8 = 0x01;
    pub(super) const MUL: u8 = 0x02;
    pub(super) const SUB: u8 = 0x03;
    pub(super) const DIV: u8 = 0x04;
    pub(super) const EQ: u8 = 0x14;
    pub(super) const ISZERO: u8 = 0x15;
    pub(super) const AND: u8 = 0x16;
    pub(super) const SHL: u8 = 0x1b;
    pub(super) const KECCAK256: u8 = 0x20;
    pub(super) const ADDRESS: u8 = 0x30;
    pub(super) const CALLER: u8 = 0x33;
    pub(super) const CALLDATALOAD: u8 = 0x35;
    pub(super) const MLOAD: u8 = 0x51;
    pub(super) const MSTORE: u8 = 0x52;
    pub(super) const SLOAD: u8 = 0x54;
    pub(super) const SSTORE: u8 = 0x55;
    pub(super) const JUMPI: u8 = 0x57;
    pub(super) const GAS: u8 = 0x5a;
    pub(super) const JUMPDEST: u8 = 0x5b;
    pub(super) const PUSH0: u8 = 0x5f;
    pub(super) const PUSH1: u8 = 0x60;
    pub(super) const PUSH2: u8 = 0x61;
    pub(super) const DUP1: u8 = 0x80;
    pub(super) const DUP2: u8 = 0x81;
    pub(super) const DUP3: u8 = 0x82;
    pub(super) const DUP4: u8 = 0x83;
    pub(super) const DUP5: u8 = 0x84;
    pub(super) const LOG3: u8 = 0xa3;
    pub(super) const CALL: u8 = 0xf1;
    pub(super) const REVERT: u8 = 0xfd;
}

/// A minimal EVM assembler: opcodes, pushes of the narrowest width, and named jump targets.
#[derive(Default)]
struct Asm {
    /// The code so far.
    code: Vec<u8>,
    /// Each label's `JUMPDEST` offset.
    labels: HashMap<&'static str, usize>,
    /// The offset of each `PUSH2` operand that `finish` patches with its label's offset.
    fixups: Vec<(usize, &'static str)>,
}

impl Asm {
    /// Append `op`.
    fn op(&mut self, op: u8) -> &mut Self {
        self.code.push(op);
        self
    }

    /// Push the big-endian `bytes` with the narrowest `PUSHn` (`PUSH0` for zero).
    fn push(&mut self, bytes: &[u8]) -> &mut Self {
        let start = bytes.iter().position(|b| *b != 0).unwrap_or(bytes.len());
        let bytes = &bytes[start..];
        assert!(bytes.len() <= 32, "a push holds at most 32 bytes");
        match bytes.len() {
            0 => self.op(op::PUSH0),
            n => {
                self.op(op::PUSH1 + n as u8 - 1);
                self.code.extend_from_slice(bytes);
                self
            }
        }
    }

    /// Push `value`.
    fn push_u64(&mut self, value: u64) -> &mut Self {
        self.push(&value.to_be_bytes())
    }

    /// Push the offset of `label`, which [`Self::finish`] fills in.
    fn push_label(&mut self, label: &'static str) -> &mut Self {
        self.fixups.push((self.code.len() + 1, label));
        self.op(op::PUSH2);
        self.code.extend_from_slice(&[0, 0]);
        self
    }

    /// Place `label` here, as a `JUMPDEST`.
    fn label(&mut self, label: &'static str) -> &mut Self {
        let previous = self.labels.insert(label, self.code.len());
        assert!(previous.is_none(), "label {label} placed twice");
        self.op(op::JUMPDEST)
    }

    /// The code, with every pushed label's offset filled in.
    fn finish(mut self) -> Bytes {
        for (at, label) in self.fixups {
            let target = self.labels.get(label).unwrap_or_else(|| panic!("label {label} unplaced"));
            let target = u16::try_from(*target).expect("code fits a PUSH2 offset");
            self.code[at..at + 2].copy_from_slice(&target.to_be_bytes());
        }
        self.code.into()
    }

    /// Store the 4-byte `selector` at memory 0, so the call's arguments start at memory 4.
    fn selector(&mut self, selector: [u8; 4]) -> &mut Self {
        self.push(&selector).push_u64(224).op(op::SHL).op(op::PUSH0).op(op::MSTORE)
    }

    /// `CALL(gas, token, 0, 0, args_len, 0, 32)`, then jump to `revert` unless the call succeeded
    /// and returned the word 1 (`true`) into memory 0.
    fn call_returning_true(&mut self, token: Address, args_len: u64) -> &mut Self {
        self.push_u64(32).op(op::PUSH0).push_u64(args_len).op(op::PUSH0).op(op::PUSH0);
        self.push(token.as_slice()).op(op::GAS).op(op::CALL);
        self.op(op::PUSH0).op(op::MLOAD).push_u64(1).op(op::EQ).op(op::AND).op(op::ISZERO);
        self.push_label("revert").op(op::JUMPI)
    }
}

/// The swap router's runtime code (see the module docs). The comments track the stack, top last.
pub fn router_code() -> Bytes {
    use op::*;
    let mut a = Asm::default();

    // 1. A.transferFrom(caller, this, amount_in)
    a.selector(IERC20::transferFromCall::SELECTOR);
    a.op(CALLER).push_u64(0x04).op(MSTORE);
    a.op(ADDRESS).push_u64(0x24).op(MSTORE);
    a.push_u64(0x20).op(CALLDATALOAD).push_u64(0x44).op(MSTORE);
    a.call_returning_true(STABLECOIN_A, 0x64);

    // 2. the reserves: memory 0 holds the pool, memory 32 the mapping slot (0, then 1)
    a.op(PUSH0).op(CALLDATALOAD).op(PUSH0).op(MSTORE);
    a.op(PUSH0).push_u64(0x20).op(MSTORE);
    a.push_u64(0x40).op(PUSH0).op(KECCAK256); // [s0]
    a.push_u64(1).push_u64(0x20).op(MSTORE);
    a.push_u64(0x40).op(PUSH0).op(KECCAK256); // [s0, s1]
    a.op(DUP1).op(SLOAD); // [s0, s1, r1]
    a.op(DUP3).op(SLOAD); // [s0, s1, r1, r0]
    a.push_u64(0x20).op(CALLDATALOAD).op(ADD); // [s0, s1, r1, r0 + in]
    a.op(DUP1).op(DUP5).op(SSTORE); // reserve0 = r0 + in
    a.op(DUP2).push_u64(0x20).op(CALLDATALOAD).op(MUL); // [s0, s1, r1, r0 + in, r1 * in]
    a.op(DIV); // [s0, s1, r1, out]
    a.op(DUP1).op(DUP3).op(SUB); // [s0, s1, r1, out, r1 - out]
    a.op(DUP4).op(SSTORE); // reserve1 = r1 - out; [s0, s1, r1, out]

    // 3. B.transfer(caller, out)
    a.selector(IERC20::transferCall::SELECTOR);
    a.op(CALLER).push_u64(0x04).op(MSTORE);
    a.op(DUP1).push_u64(0x24).op(MSTORE);
    a.call_returning_true(STABLECOIN_B, 0x44);

    // 4. Swap(caller, pool, in, out): data is memory 0..64
    a.push_u64(0x20).op(CALLDATALOAD).op(PUSH0).op(MSTORE);
    a.push_u64(0x20).op(MSTORE); // [s0, s1, r1]
    a.op(PUSH0).op(CALLDATALOAD).op(CALLER);
    a.push(ISwapRouter::Swap::SIGNATURE_HASH.as_slice());
    a.push_u64(0x40).op(PUSH0).op(LOG3).op(STOP);

    a.label("revert").op(PUSH0).op(PUSH0).op(REVERT);
    a.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{
            consensus_output_for_tests_at, execute_payload_and_update_canonical_chain,
            load::open_production_env,
        },
        ExecutedBlock, RethChainSpec, SYSTEM_ADDRESS,
    };
    use alloy::sol_types::SolValue as _;
    use reth_evm::Evm as _;
    use reth_provider::StateProvider as _;
    use reth_revm::context::result::ExecutionResult;
    use serde_json::Value;
    use std::sync::Arc;
    use tn_types::{TaskManager, U256};

    /// The fee cap of every test transaction (the tip is zero, so it pays the base fee).
    const GAS_PRICE: u128 = 1_000_000_000;

    /// A small prefill: 10k accounts, a few senders and pools.
    fn spec() -> PrefillSpec {
        PrefillSpec { accounts: 10_000, senders: 4, pools: 8, seed: 7, registry: false }
    }

    /// An env on a prefilled genesis in a temp dir, opened as the node opens it.
    struct Bench {
        env: RethEnv,
        chain: Arc<RethChainSpec>,
        _tasks: TaskManager,
        _tmp: tempfile::TempDir,
    }

    impl Bench {
        fn open(spec: &PrefillSpec) -> eyre::Result<Self> {
            let tmp = tempfile::tempdir()?;
            let chain: Arc<RethChainSpec> = Arc::new(spec.genesis().into());
            let tasks = TaskManager::default();
            let env = open_production_env(tmp.path(), chain.clone(), &[], &tasks)?;
            Ok(Self { env, chain, _tasks: tasks, _tmp: tmp })
        }

        /// Execute and persist one block of `txs` on the canonical tip.
        fn block(&self, txs: Vec<Vec<u8>>) -> eyre::Result<ExecutedBlock> {
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
            execute_payload_and_update_canonical_chain(&self.env, payload, txs)
        }

        /// A call from `sender` to `to` with `input`.
        fn tx(
            &self,
            sender: &mut TransactionFactory,
            gas_limit: u64,
            to: Address,
            input: Bytes,
        ) -> Vec<u8> {
            sender.create_eip1559_encoded(
                self.chain.clone(),
                Some(gas_limit),
                GAS_PRICE,
                Some(to),
                U256::ZERO,
                input,
            )
        }

        /// `address`'s storage at `slot` in the state after `block`.
        fn storage(&self, block: B256, address: Address, slot: B256) -> eyre::Result<U256> {
            Ok(self.env.state_by_block_hash(block)?.storage(address, slot)?.unwrap_or_default())
        }

        /// The word `call` on `to` returns in the state after `block`.
        fn view(&self, block: B256, to: Address, call: impl alloy::sol_types::SolCall) -> U256 {
            let mut evm = self.env.tn_evm(block).expect("evm at block");
            let result = evm
                .transact_system_call(SYSTEM_ADDRESS, to, call.abi_encode().into())
                .expect("view call")
                .result;
            match result {
                ExecutionResult::Success { output, .. } => {
                    U256::abi_decode(output.data()).expect("one word")
                }
                other => panic!("view call failed: {other:?}"),
            }
        }
    }

    /// The token views read the prefilled storage: balances, allowances and the total supply.
    #[tokio::test]
    async fn test_views_read_the_prefill() -> eyre::Result<()> {
        let spec = spec();
        let bench = Bench::open(&spec)?;
        let genesis = bench.chain.genesis_hash();
        let sender = spec.sender(1).address();
        let account = spec.account(spec.accounts - 1);
        let balance =
            |token, holder| bench.view(genesis, token, IERC20::balanceOfCall { account: holder });
        let allowance = |token, owner| {
            bench.view(genesis, token, IERC20::allowanceCall { owner, spender: SWAP_ROUTER })
        };

        for token in [STABLECOIN_A, STABLECOIN_B] {
            assert_eq!(balance(token, sender), U256::from(SENDER_TOKENS), "a sender's balance");
            assert_eq!(balance(token, account), U256::from(ACCOUNT_TOKENS), "an account's balance");
            assert_eq!(balance(token, SWAP_ROUTER), U256::from(ROUTER_TOKENS), "the router's");
            assert_eq!(bench.view(genesis, token, IERC20::totalSupplyCall {}), spec.total_supply());
        }
        assert_eq!(allowance(STABLECOIN_A, sender), U256::from(SENDER_TOKENS), "A's allowance");
        assert_eq!(allowance(STABLECOIN_B, sender), U256::ZERO, "no allowance on B");
        assert_eq!(
            bench.env.latest()?.account_balance(&account)?,
            Some(U256::from(ACCOUNT_NATIVE))
        );
        Ok(())
    }

    /// A `transfer` moves the prefilled balances and logs one `Transfer`; a sender can move its
    /// whole prefilled balance and not one unit more.
    #[tokio::test]
    async fn test_transfer_moves_prefilled_balances() -> eyre::Result<()> {
        let spec = spec();
        let bench = Bench::open(&spec)?;
        let genesis = bench.chain.genesis_hash();
        let mut senders = spec.sender_factories();
        let (s0, s1, s2) = (senders[0].address(), senders[1].address(), senders[2].address());
        let to = spec.account(3);
        let amount = U256::from(1_000);
        let all = U256::from(SENDER_TOKENS);

        let txs = vec![
            bench.tx(
                &mut senders[0],
                TRANSFER_GAS_LIMIT,
                STABLECOIN_A,
                transfer_calldata(to, amount),
            ),
            bench.tx(&mut senders[1], TRANSFER_GAS_LIMIT, STABLECOIN_B, transfer_calldata(to, all)),
            bench.tx(
                &mut senders[2],
                TRANSFER_GAS_LIMIT,
                STABLECOIN_A,
                transfer_calldata(to, all + U256::from(1)),
            ),
        ];
        let block = bench.block(txs)?;
        let receipts = &block.execution_output.result.receipts;
        assert!(receipts[0].success && receipts[1].success, "transfers within the balance pass");
        assert!(!receipts[2].success, "a transfer past the prefilled balance reverts");
        assert!(receipts[2].logs.is_empty(), "the reverted transfer logs nothing");

        let [log] = receipts[0].logs.as_slice() else { panic!("one log: {:?}", receipts[0].logs) };
        assert_eq!(log.address, STABLECOIN_A);
        assert_eq!(
            log.topics(),
            [IERC20::Transfer::SIGNATURE_HASH, s0.into_word(), to.into_word()]
        );
        assert_eq!(U256::from_be_slice(&log.data.data), amount, "the logged value");

        let after = block.recovered_block.hash();
        let a = |block, holder| bench.storage(block, STABLECOIN_A, balance_slot(holder));
        let b = |block, holder| bench.storage(block, STABLECOIN_B, balance_slot(holder));
        let account = U256::from(ACCOUNT_TOKENS);
        assert_eq!((a(genesis, s0)?, a(after, s0)?), (all, all - amount), "the sender pays");
        assert_eq!((a(genesis, to)?, a(after, to)?), (account, account + amount), "to receives");
        assert_eq!((b(after, s1)?, b(after, to)?), (U256::ZERO, account + all), "a whole balance");
        assert_eq!(a(after, s2)?, all, "the reverted sender keeps its balance");
        assert!(receipts[0].cumulative_gas_used <= TRANSFER_GAS_LIMIT);
        Ok(())
    }

    /// A swap takes token A for the router, writes both reserves, pays token B and logs
    /// `Transfer`, `Transfer`, `Swap`; a caller without an allowance reverts.
    #[tokio::test]
    async fn test_swap_moves_tokens_and_reserves() -> eyre::Result<()> {
        let spec = spec();
        let bench = Bench::open(&spec)?;
        let mut sender = spec.sender(2);
        let caller = sender.address();
        let pool = 5;
        let amount_in = U256::from(10u128.pow(18));
        let reserve = U256::from(POOL_RESERVE);
        let out = amount_in * reserve / (reserve + amount_in);
        let mut stranger = TransactionFactory::new(); // funded natively, holds no tokens

        let txs = vec![
            bench.tx(&mut sender, SWAP_GAS_LIMIT, SWAP_ROUTER, swap_calldata(pool, amount_in)),
            bench.tx(&mut stranger, SWAP_GAS_LIMIT, SWAP_ROUTER, swap_calldata(pool, amount_in)),
        ];
        let block = bench.block(txs)?;
        let receipts = &block.execution_output.result.receipts;
        assert!(receipts[0].success, "the swap succeeds");
        assert!(!receipts[1].success, "a swap without tokens or allowance reverts");
        assert!(receipts[1].logs.is_empty(), "the reverted swap logs nothing");

        let [take, pay, swap] = receipts[0].logs.as_slice() else {
            panic!("three logs: {:?}", receipts[0].logs)
        };
        let transfer = IERC20::Transfer::SIGNATURE_HASH;
        assert_eq!(take.address, STABLECOIN_A);
        assert_eq!(take.topics(), [transfer, caller.into_word(), SWAP_ROUTER.into_word()]);
        assert_eq!(U256::from_be_slice(&take.data.data), amount_in);
        assert_eq!(pay.address, STABLECOIN_B);
        assert_eq!(pay.topics(), [transfer, SWAP_ROUTER.into_word(), caller.into_word()]);
        assert_eq!(U256::from_be_slice(&pay.data.data), out);
        assert_eq!(swap.address, SWAP_ROUTER);
        let pool_topic = B256::from(U256::from(pool));
        assert_eq!(
            swap.topics(),
            [ISwapRouter::Swap::SIGNATURE_HASH, caller.into_word(), pool_topic]
        );
        assert_eq!(swap.data.data, (amount_in, out).abi_encode(), "the swap's amounts");

        let after = block.recovered_block.hash();
        let at = |address, slot| bench.storage(after, address, slot);
        let (r0, r1) = reserve_slots(pool);
        assert_eq!(at(SWAP_ROUTER, r0)?, reserve + amount_in, "reserve0 takes amount_in");
        assert_eq!(at(SWAP_ROUTER, r1)?, reserve - out, "reserve1 pays out");
        let (other0, _) = reserve_slots(pool + 1);
        assert_eq!(at(SWAP_ROUTER, other0)?, reserve, "another pool is untouched");

        let (tokens, router) = (U256::from(SENDER_TOKENS), U256::from(ROUTER_TOKENS));
        assert_eq!(at(STABLECOIN_A, balance_slot(caller))?, tokens - amount_in);
        assert_eq!(at(STABLECOIN_A, balance_slot(SWAP_ROUTER))?, router + amount_in);
        assert_eq!(at(STABLECOIN_A, allowance_slot(caller, SWAP_ROUTER))?, tokens - amount_in);
        assert_eq!(at(STABLECOIN_B, balance_slot(caller))?, tokens + out);
        assert_eq!(at(STABLECOIN_B, balance_slot(SWAP_ROUTER))?, router - out);
        assert!(receipts[0].cumulative_gas_used <= SWAP_GAS_LIMIT);
        Ok(())
    }

    /// The same spec gives the same genesis hash, keys and addresses; another seed differs.
    #[test]
    fn test_genesis_is_deterministic() {
        let spec = PrefillSpec { accounts: 1_000, ..spec() };
        let reseeded = PrefillSpec { seed: spec.seed + 1, ..spec };
        let hash = |spec: &PrefillSpec| RethChainSpec::from(spec.genesis()).genesis_hash();
        assert_eq!(hash(&spec), hash(&spec), "the same spec, the same genesis");
        assert_ne!(hash(&spec), hash(&reseeded), "another seed, another genesis");
        assert_eq!(spec.sender(1).address(), spec.sender(1).address());
        assert_ne!(spec.sender(1).address(), reseeded.sender(1).address());
        assert_ne!(spec.sender(0).address(), spec.sender(1).address());
        assert_ne!(spec.account(0), reseeded.account(0));
        assert_ne!(spec.account(0), spec.account(1));
    }

    /// A `registry` genesis deploys the `ConsensusRegistry`, holds the prefill and is just as
    /// deterministic.
    #[tokio::test]
    async fn test_registry_genesis_holds_the_prefill() {
        let spec = PrefillSpec { accounts: 100, registry: true, ..spec() };
        let genesis = spec.genesis();
        assert_eq!(genesis.timestamp, GENESIS_TIMESTAMP);
        let registry = &genesis.alloc[&crate::system_calls::CONSENSUS_REGISTRY_ADDRESS];
        assert!(registry.code.as_ref().is_some_and(|code| !code.is_empty()), "the registry");
        assert!(registry.storage.as_ref().is_some_and(|s| !s.is_empty()), "a seeded registry");
        assert_eq!(genesis.alloc[&STABLECOIN_A].code, Some(stablecoin_code()));
        assert!(genesis.alloc.contains_key(&spec.account(spec.accounts - 1)));
        let hash = |genesis: Genesis| RethChainSpec::from(genesis).genesis_hash();
        assert_eq!(hash(genesis), hash(spec.genesis()), "the same spec, the same genesis");
    }

    /// The `Stablecoin` artifact has no immutables, so its compile-time code is its runtime code.
    #[test]
    fn test_stablecoin_artifact_has_no_immutables() {
        fn immutable_sites(artifact: &str) -> usize {
            let json: Value = serde_json::from_str(artifact).expect("artifact json");
            json.pointer("/deployedBytecode/immutableReferences")
                .and_then(Value::as_object)
                .map(|refs| refs.values().filter_map(Value::as_array).map(Vec::len).sum())
                .unwrap_or_default()
        }
        assert_eq!(immutable_sites(STABLECOIN_JSON), 0, "Stablecoin has no immutables");
        // the reader finds immutables where an artifact has them
        assert!(immutable_sites(tn_config::CONSENSUS_REGISTRY_JSON) > 0);
        assert!(!stablecoin_code().is_empty());
    }
}
