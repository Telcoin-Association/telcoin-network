//! Telcoin precompile registered as a [`DynPrecompile`] inside a [`PrecompilesMap`].
//!
//! Storage layout at TELCOIN_PRECOMPILE_ADDRESS:
//!   keccak256(abi.encode(recipient, 0)) = pending mint amount
//!   keccak256(abi.encode(recipient, 1)) = unlock timestamp
//!   keccak256(abi.encode(spender, keccak256(abi.encode(owner, 2)))) = allowance
//!   keccak256(abi.encode(owner, 4)) = nonces (EIP-2612 permit nonces)
//!   slot 100 = totalSupply

use alloy::{
    primitives::{address, Signature},
    sol,
    sol_types::{SolCall, SolEvent},
};
use alloy_evm::{
    precompiles::{DynPrecompile, PrecompileInput, PrecompilesMap},
    EvmInternals,
};
use reth_primitives_traits::crypto::SECP256K1N_HALF;
use reth_revm::{
    precompile::{PrecompileError, PrecompileId, PrecompileOutput, PrecompileResult},
    primitives::keccak256,
};
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{Address, Bytes, B256, U256};

// ABI definitions for the Telcoin precompile's external interface.
//
// This generates selector constants and encoding/decoding types for each function and event.
// The interface combines a custom mint/claim/burn lifecycle with a standard ERC-20 surface,
// plus optional role-management functions gated behind the `faucet` feature.
//
// Security notes:
// - `mint` creates a pending mint subject to a timelock; it does NOT credit tokens immediately.
// - `claim` is permissionless — anyone can trigger it once the timelock expires.
// - `burn` destroys tokens held by the precompile account; only governance may call it.
// - `grantMintRole` / `revokeMintRole` are only compiled with `feature = "faucet"`.
sol! {
    function claim(address recipient) external;
    function burn(uint256 amount) external;
    function name() external view returns (string);
    function symbol() external view returns (string);
    function decimals() external view returns (uint8);
    function totalSupply() external view returns (uint256);
    function balanceOf(address account) external view returns (uint256);
    function transfer(address to, uint256 amount) external returns (bool);
    function approve(address spender, uint256 amount) external returns (bool);
    function transferFrom(address from, address to, uint256 amount) external returns (bool);
    function allowance(address owner, address spender) external view returns (uint256);
    function grantMintRole(address addr) external;
    function revokeMintRole(address addr) external;
    function hasMintRole(address addr) external view returns (bool);
    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);
    event Mint(address indexed recipient, uint256 amount, uint256 unlockTimestamp);
    event Claim(address indexed recipient, uint256 amount);
    event Burn(uint256 amount);
    function permit(address owner, address spender, uint256 value, uint256 deadline, uint8 v, bytes32 r, bytes32 s) external;
    function nonces(address owner) external view returns (uint256);
    function DOMAIN_SEPARATOR() external view returns (bytes32);
}

// only mint to governance safe
#[cfg(not(feature = "faucet"))]
sol! {
    function mint(uint256 amount) external;
}

// faucet mints on the fly
#[cfg(feature = "faucet")]
sol! {
    function mint(address recipient, uint256 amount) external;
}

/// The canonical address of the Telcoin precompile: `0x7e1`.
///
/// All TEL token state (pending mints, allowances, total supply) is stored under this address.
/// Native account balances at other addresses represent TEL holdings for those accounts.
/// Calls targeting this address are intercepted by the [`DynPrecompile`] registered via
/// [`add_telcoin_precompile`] and routed to the precompile dispatcher instead of executing EVM
/// bytecode.
pub const TELCOIN_PRECOMPILE_ADDRESS: Address =
    address!("00000000000000000000000000000000000007e1");

/// Timelock duration applied to new mints before they can be claimed.
///
/// - **Production** (`!faucet`): 7 days (604 800 seconds). Provides a window for governance to
///   cancel malicious mints before tokens enter circulation.
/// - **Testnet / faucet** (`faucet`): 0 seconds. Allows instant claim for development convenience.
///
/// # Security invariant
/// This value is set at **compile time**. A production binary must never be built with
/// `feature = "faucet"` enabled, or the timelock protection is silently disabled.
#[cfg(not(feature = "faucet"))]
pub const TIMELOCK_DURATION: u64 = 7 * 24 * 60 * 60; // 604800s = 7 days

/// Fixed storage slot (100) that holds the total circulating supply of TEL.
///
/// Incremented on [`handle_claim`] and decremented on [`handle_burn`].
/// This is a plain slot (not a mapping), so it can be read directly without hashing.
const TOTAL_SUPPLY_SLOT: U256 = U256::from_limbs([100, 0, 0, 0]);

/// EIP-712 domain type hash: `keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")`.
const EIP712_DOMAIN_TYPEHASH: B256 = B256::new([
    0x8b, 0x73, 0xc3, 0xc6, 0x9b, 0xb8, 0xfe, 0x3d, 0x51, 0x2e, 0xcc, 0x4c, 0xf7, 0x59, 0xcc, 0x79,
    0x23, 0x9f, 0x7b, 0x17, 0x9b, 0x0f, 0xfa, 0xca, 0xa9, 0xa7, 0x5d, 0x52, 0x2b, 0x39, 0x40, 0x0f,
]);

/// EIP-2612 permit type hash: `keccak256("Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)")`.
const PERMIT_TYPEHASH: B256 = B256::new([
    0x6e, 0x71, 0xed, 0xae, 0x12, 0xb1, 0xb9, 0x7f, 0x4d, 0x1f, 0x60, 0x37, 0x0f, 0xef, 0x10, 0x10,
    0x5f, 0xa2, 0xfa, 0xae, 0x01, 0x26, 0x11, 0x4a, 0x16, 0x9c, 0x64, 0x84, 0x5d, 0x61, 0x26, 0xc9,
]);

/// Registers the Telcoin ERC20 precompile at [`TELCOIN_PRECOMPILE_ADDRESS`] in the given map.
pub fn add_telcoin_precompile(map: &mut PrecompilesMap) {
    map.extend_precompiles([(
        TELCOIN_PRECOMPILE_ADDRESS,
        DynPrecompile::new_stateful(PrecompileId::Custom("telcoin".into()), telcoin_precompile),
    )]);
}

// --- Storage slot helpers ---
//
// All TEL precompile state is stored under TELCOIN_PRECOMPILE_ADDRESS using
// Solidity-style mapping layouts. Base slot indices:
//   0 → pending mint amounts  (mapping: address → uint256)
//   1 → unlock timestamps     (mapping: address → uint256)
//   2 → allowances            (mapping: address → mapping(address → uint256))
//   3 → mint roles            (mapping: address → bool)  [faucet feature only]
//   4 → nonces                (mapping: address → uint256)  [EIP-2612 permit]
// 100 → totalSupply           (plain slot)

/// Compute the storage slot for a recipient's pending mint amount.
///
/// Layout: `keccak256(abi.encode(recipient, 0))` — standard Solidity `mapping(address => uint256)`
/// at slot 0.
fn amount_slot(recipient: Address) -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(recipient.as_slice());
    // slot index 0 is already zero in buf[32..64]
    U256::from_be_bytes(keccak256(buf).0)
}

/// Compute the storage slot for a recipient's unlock timestamp.
///
/// Layout: `keccak256(abi.encode(recipient, 1))` — `mapping(address => uint256)` at slot 1.
/// A non-zero value means a pending mint exists; the value is the earliest block timestamp
/// at which [`handle_claim`] will succeed.
fn timestamp_slot(recipient: Address) -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(recipient.as_slice());
    buf[63] = 1; // slot index 1
    U256::from_be_bytes(keccak256(buf).0)
}

/// Compute the storage slot for `allowance[owner][spender]`.
///
/// Layout: `keccak256(abi.encode(spender, keccak256(abi.encode(owner, 2))))` — nested
/// Solidity `mapping(address => mapping(address => uint256))` at base slot 2.
///
/// Used by [`handle_approve`], [`handle_transfer_from`], and [`handle_allowance`].
fn allowance_slot(owner: Address, spender: Address) -> U256 {
    // Inner hash: keccak256(abi.encode(owner, 2))
    let mut inner_buf = [0u8; 64];
    inner_buf[12..32].copy_from_slice(owner.as_slice());
    inner_buf[63] = 2; // base slot 2
    let inner_hash = keccak256(inner_buf);

    // Outer hash: keccak256(abi.encode(spender, inner_hash))
    let mut outer_buf = [0u8; 64];
    outer_buf[12..32].copy_from_slice(spender.as_slice());
    outer_buf[32..64].copy_from_slice(&inner_hash.0);
    U256::from_be_bytes(keccak256(outer_buf).0)
}

/// Compute the storage slot for an owner's EIP-2612 permit nonce.
///
/// Layout: `keccak256(abi.encode(owner, 4))` — `mapping(address => uint256)` at slot 4.
fn nonce_slot(owner: Address) -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(owner.as_slice());
    buf[63] = 4; // slot index 4
    U256::from_be_bytes(keccak256(buf).0)
}

/// Compute the EIP-712 domain separator for the given chain ID.
///
/// `keccak256(abi.encode(EIP712_DOMAIN_TYPEHASH, keccak256("Telcoin"), keccak256("1"), chainId, TELCOIN_PRECOMPILE_ADDRESS))`
fn compute_domain_separator(chain_id: u64) -> B256 {
    let mut buf = [0u8; 5 * 32];
    buf[0..32].copy_from_slice(&EIP712_DOMAIN_TYPEHASH.0);
    buf[32..64].copy_from_slice(&keccak256("Telcoin").0);
    buf[64..96].copy_from_slice(&keccak256("1").0);
    buf[96..128].copy_from_slice(&U256::from(chain_id).to_be_bytes::<32>());
    buf[140..160].copy_from_slice(TELCOIN_PRECOMPILE_ADDRESS.as_slice());
    keccak256(buf)
}

/// Compute the storage slot for a dynamically granted mint role.
///
/// Layout: `keccak256(abi.encode(address, 3))` — `mapping(address => bool)` at slot 3.
/// Only compiled with `feature = "faucet"`. A non-zero value means the address may call `mint`.
#[cfg(feature = "faucet")]
fn mint_role_slot(addr: Address) -> U256 {
    let mut buf = [0u8; 64];
    buf[12..32].copy_from_slice(addr.as_slice());
    buf[63] = 3; // base slot 3
    U256::from_be_bytes(keccak256(buf).0)
}

/// Check whether `caller` is authorized to invoke `mint`.
///
/// Returns `true` if **any** of the following hold:
/// 1. `caller == GOVERNANCE_SAFE_ADDRESS` (always, regardless of features).
/// 2. *(faucet only)* The mint-role storage slot for `caller` is non-zero, meaning governance
///    previously called `grantMintRole(caller)`.
///
/// # Security note
/// Without the `faucet` feature, only governance can mint. This is the production invariant.
fn has_mint_role(
    #[cfg(feature = "faucet")] internals: &mut EvmInternals<'_>,
    #[cfg(not(feature = "faucet"))] _internals: &mut EvmInternals<'_>,
    caller: Address,
) -> Result<bool, PrecompileError> {
    if caller == GOVERNANCE_SAFE_ADDRESS {
        return Ok(true);
    }

    // faucet can have more than 1 mint
    #[cfg(feature = "faucet")]
    {
        let slot = mint_role_slot(caller);
        let val = internals
            .sload(TELCOIN_PRECOMPILE_ADDRESS, slot)
            .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
            .data;
        Ok(!val.is_zero())
    }

    #[cfg(not(feature = "faucet"))]
    Ok(false)
}

/// Check whether `caller` is authorized to invoke `burn`.
///
/// Only [`GOVERNANCE_SAFE_ADDRESS`] may burn. Unlike mint roles, burn authority is never
/// dynamically grantable — not even with the `faucet` feature.
fn has_burn_role(caller: Address) -> bool {
    caller == GOVERNANCE_SAFE_ADDRESS
}

// --- ABI encoding helpers ---
//
// Minimal hand-rolled ABI encoders. These exist to avoid pulling in a full ABI codec
// dependency for the small subset of return types the precompile needs.

/// ABI-encode a `bool` as a 32-byte word (`0x00…01` for true, `0x00…00` for false).
fn abi_encode_bool(val: bool) -> Bytes {
    let mut buf = [0u8; 32];
    if val {
        buf[31] = 1;
    }
    Bytes::copy_from_slice(&buf)
}

/// ABI-encode a `U256` as a big-endian 32-byte word.
fn abi_encode_uint256(val: U256) -> Bytes {
    Bytes::copy_from_slice(&val.to_be_bytes::<32>())
}

/// ABI-encode a dynamic `string` value.
///
/// Returns `[offset (0x20)] [length] [padded UTF-8 data]` per the Solidity ABI spec.
fn abi_encode_string(s: &str) -> Bytes {
    let s_bytes = s.as_bytes();
    let padded_len = s_bytes.len().div_ceil(32) * 32;

    // offset (32) + length (32) + padded data
    let mut buf = Vec::with_capacity(64 + padded_len);
    // Offset to string data (0x20 = 32)
    let mut offset_word = [0u8; 32];
    offset_word[31] = 0x20;
    buf.extend_from_slice(&offset_word);
    // String length
    buf.extend_from_slice(&U256::from(s_bytes.len()).to_be_bytes::<32>());
    // Padded string data
    let mut padded = vec![0u8; padded_len];
    padded[..s_bytes.len()].copy_from_slice(s_bytes);
    buf.extend_from_slice(&padded);
    Bytes::from(buf)
}

/// Left-pad a 20-byte address into a 32-byte log topic.
fn address_to_topic(addr: Address) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[12..32].copy_from_slice(addr.as_slice());
    B256::from(bytes)
}

/// Top-level dispatcher for the Telcoin precompile.
///
/// Extracts the 4-byte selector from calldata and routes to the matching handler.
/// State-mutating selectors are rejected when `input.is_static` is set (STATICCALL).
fn telcoin_precompile(mut input: PrecompileInput<'_>) -> PrecompileResult {
    if input.data.len() < 4 {
        return Err(PrecompileError::Other("Invalid input: too short".into()));
    }

    let selector: [u8; 4] = input.data[0..4].try_into().unwrap();
    let calldata = &input.data[4..];

    match selector {
        // State-mutating functions
        mintCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            #[cfg(feature = "faucet")]
            {
                handle_mint_faucet(&mut input.internals, calldata, input.caller, input.gas)
            }
            #[cfg(not(feature = "faucet"))]
            {
                handle_mint(&mut input.internals, calldata, input.caller, input.gas)
            }
        }
        claimCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            handle_claim(&mut input.internals, calldata, input.gas)
        }
        burnCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            handle_burn(&mut input.internals, calldata, input.caller, input.gas)
        }
        transferCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            handle_transfer(&mut input.internals, calldata, input.caller, input.gas)
        }
        approveCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            handle_approve(&mut input.internals, calldata, input.caller, input.gas)
        }
        transferFromCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            handle_transfer_from(&mut input.internals, calldata, input.caller, input.gas)
        }
        permitCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            handle_permit(&mut input.internals, calldata, input.gas)
        }
        // Read-only functions
        nameCall::SELECTOR => handle_name(input.gas),
        symbolCall::SELECTOR => handle_symbol(input.gas),
        decimalsCall::SELECTOR => handle_decimals(input.gas),
        totalSupplyCall::SELECTOR => handle_total_supply(&mut input.internals, input.gas),
        balanceOfCall::SELECTOR => handle_balance_of(&mut input.internals, calldata, input.gas),
        allowanceCall::SELECTOR => handle_allowance(&mut input.internals, calldata, input.gas),
        noncesCall::SELECTOR => handle_nonces(&mut input.internals, calldata, input.gas),
        DOMAIN_SEPARATORCall::SELECTOR => handle_domain_separator(&mut input.internals, input.gas),
        // Faucet feature: role management
        #[cfg(feature = "faucet")]
        grantMintRoleCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            handle_grant_mint_role(&mut input.internals, calldata, input.caller, input.gas)
        }
        #[cfg(feature = "faucet")]
        revokeMintRoleCall::SELECTOR => {
            if input.is_static {
                return Err(PrecompileError::Other("Cannot modify state in static context".into()));
            }
            handle_revoke_mint_role(&mut input.internals, calldata, input.caller, input.gas)
        }
        #[cfg(feature = "faucet")]
        hasMintRoleCall::SELECTOR => {
            handle_has_mint_role(&mut input.internals, calldata, input.gas)
        }
        _ => Err(PrecompileError::Other("Unknown function selector".into())),
    }
}

// --- ERC20 read-only handlers ---

/// `name()` → returns ABI-encoded `"Telcoin"`. Pure; no storage access.
fn handle_name(gas_limit: u64) -> PrecompileResult {
    const GAS_COST: u64 = 200;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    Ok(PrecompileOutput::new(GAS_COST, abi_encode_string("Telcoin")))
}

/// `symbol()` → returns ABI-encoded `"TEL"`. Pure; no storage access.
fn handle_symbol(gas_limit: u64) -> PrecompileResult {
    const GAS_COST: u64 = 200;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    Ok(PrecompileOutput::new(GAS_COST, abi_encode_string("TEL")))
}

/// `decimals()` → returns ABI-encoded `18`. Pure; no storage access.
fn handle_decimals(gas_limit: u64) -> PrecompileResult {
    const GAS_COST: u64 = 200;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    Ok(PrecompileOutput::new(GAS_COST, abi_encode_uint256(U256::from(18))))
}

/// `totalSupply()` → reads [`TOTAL_SUPPLY_SLOT`] (slot 100) and returns the current circulating
/// supply.
fn handle_total_supply(internals: &mut EvmInternals<'_>, gas_limit: u64) -> PrecompileResult {
    const GAS_COST: u64 = 2_100;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    let supply = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    Ok(PrecompileOutput::new(GAS_COST, abi_encode_uint256(supply)))
}

/// `balanceOf(address)` → returns the **native account balance** of the given address.
///
/// TEL balances are stored as native ether-equivalent balances, not in precompile storage.
/// This means `balanceOf` loads the account record, not a storage slot.
fn handle_balance_of(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_600;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("balanceOf: expected 32 bytes (address)".into()));
    }
    let addr = Address::from_slice(&calldata[12..32]);
    let balance = internals
        .load_account(addr)
        .map_err(|e| PrecompileError::Other(format!("load_account failed: {e:?}").into()))?
        .data
        .info
        .balance;
    Ok(PrecompileOutput::new(GAS_COST, abi_encode_uint256(balance)))
}

/// `allowance(address owner, address spender)` → reads the ERC-20 allowance from precompile
/// storage.
fn handle_allowance(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_100;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 64 {
        return Err(PrecompileError::Other(
            "allowance: expected 64 bytes (address,address)".into(),
        ));
    }
    let owner = Address::from_slice(&calldata[12..32]);
    let spender = Address::from_slice(&calldata[44..64]);
    let slot = allowance_slot(owner, spender);
    let value = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    Ok(PrecompileOutput::new(GAS_COST, abi_encode_uint256(value)))
}

// --- EIP-2612 permit handlers ---

/// `nonces(address owner)` → returns the current permit nonce for `owner`.
fn handle_nonces(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_100;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("nonces: expected 32 bytes (address)".into()));
    }
    let owner = Address::from_slice(&calldata[12..32]);
    let nonce = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, nonce_slot(owner))
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    Ok(PrecompileOutput::new(GAS_COST, abi_encode_uint256(nonce)))
}

/// `DOMAIN_SEPARATOR()` → returns the EIP-712 domain separator for the current chain.
fn handle_domain_separator(internals: &mut EvmInternals<'_>, gas_limit: u64) -> PrecompileResult {
    const GAS_COST: u64 = 2_600;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    let ds = compute_domain_separator(internals.chain_id());
    Ok(PrecompileOutput::new(GAS_COST, Bytes::copy_from_slice(&ds.0)))
}

/// `permit(address owner, address spender, uint256 value, uint256 deadline, uint8 v, bytes32 r, bytes32 s)`
///
/// EIP-2612 gasless approval. Verifies an off-chain signature from `owner` and sets
/// `allowance[owner][spender] = value`. Increments the owner's nonce to prevent replay.
///
/// Emits `Approval(owner, spender, value)`.
fn handle_permit(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 72_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 224 {
        return Err(PrecompileError::Other(
            "permit: expected 224 bytes (address,address,uint256,uint256,uint8,bytes32,bytes32)"
                .into(),
        ));
    }

    let owner = Address::from_slice(&calldata[12..32]);
    let spender = Address::from_slice(&calldata[44..64]);
    if spender == Address::ZERO {
        return Err(PrecompileError::Other("permit: cannot approve address(0)".into()));
    }
    let value = U256::from_be_slice(&calldata[64..96]);
    let deadline = U256::from_be_slice(&calldata[96..128]);
    let v = calldata[159];
    let r = U256::from_be_slice(&calldata[160..192]);
    let s = U256::from_be_slice(&calldata[192..224]);
    // Validate s
    if s > SECP256K1N_HALF {
        return Err(PrecompileError::Other("permit: signature malleability".into()));
    }

    // Check deadline
    let current_ts = internals.block_timestamp();
    if current_ts > deadline {
        return Err(PrecompileError::Other("permit: expired deadline".into()));
    }

    // Validate v
    if v != 27 && v != 28 {
        return Err(PrecompileError::Other("permit: invalid v value".into()));
    }

    // Load current nonce
    let nonce_s = nonce_slot(owner);
    let nonce = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, nonce_s)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;

    // Compute EIP-712 struct hash
    let struct_hash = {
        let mut buf = [0u8; 6 * 32];
        buf[0..32].copy_from_slice(&PERMIT_TYPEHASH.0);
        buf[44..64].copy_from_slice(owner.as_slice());
        buf[76..96].copy_from_slice(spender.as_slice());
        buf[96..128].copy_from_slice(&value.to_be_bytes::<32>());
        buf[128..160].copy_from_slice(&nonce.to_be_bytes::<32>());
        buf[160..192].copy_from_slice(&deadline.to_be_bytes::<32>());
        keccak256(buf)
    };

    // Compute EIP-712 digest
    let digest = {
        let domain_separator = compute_domain_separator(internals.chain_id());
        let mut buf = [0u8; 2 + 32 + 32];
        buf[0] = 0x19;
        buf[1] = 0x01;
        buf[2..34].copy_from_slice(&domain_separator.0);
        buf[34..66].copy_from_slice(&struct_hash.0);
        keccak256(buf)
    };

    // Recover signer and verify
    let sig = Signature::new(r, s, v == 28);
    let recovered = sig
        .recover_address_from_prehash(&digest)
        .map_err(|_| PrecompileError::Other("permit: invalid signature".into()))?;
    if recovered != owner {
        return Err(PrecompileError::Other("permit: signer != owner".into()));
    }

    // Increment nonce
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, nonce_s, nonce + U256::from(1))
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Set allowance
    let slot = allowance_slot(owner, spender);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, value)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Approval(owner, spender, value)
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Approval::SIGNATURE_HASH, address_to_topic(owner), address_to_topic(spender)],
        value.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Approval log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

// --- ERC20 state-mutating handlers ---

/// `transfer(address to, uint256 amount)` → moves native balance from `caller` to `to`.
///
/// Emits `Transfer(caller, to, amount)`. Returns ABI-encoded `true` on success.
/// Fails if `caller`'s balance is insufficient.
///
/// Allow transfers to this address for `burn`.
fn handle_transfer(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 12_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 64 {
        return Err(PrecompileError::Other("transfer: expected 64 bytes (address,uint256)".into()));
    }

    let to = Address::from_slice(&calldata[12..32]);
    let amount = U256::from_be_slice(&calldata[32..64]);

    if to == Address::ZERO {
        return Err(PrecompileError::Other("transfer: cannot transfer to address(0)".into()));
    }

    // Transfer native balance
    let transfer_result = internals
        .transfer(caller, to, amount)
        .map_err(|e| PrecompileError::Other(format!("transfer failed: {e:?}").into()))?;

    if let Some(error) = transfer_result {
        return Err(PrecompileError::Other(format!("transfer error: {error:?}").into()));
    }

    // Emit Transfer(from, to, value)
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Transfer::SIGNATURE_HASH, address_to_topic(caller), address_to_topic(to)],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, abi_encode_bool(true)))
}

/// `approve(address spender, uint256 amount)` → sets allowance for `spender` to spend `caller`'s
/// tokens.
///
/// Overwrites any existing allowance (no incremental add/sub). Emits `Approval(caller, spender,
/// amount)`.
///
/// # Security note
/// Subject to the classic ERC-20 approve front-running race. Callers should set allowance
/// to 0 before setting a new non-zero value, or use `transferFrom` patterns that don't rely
/// on allowance deltas.
fn handle_approve(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 22_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 64 {
        return Err(PrecompileError::Other("approve: expected 64 bytes (address,uint256)".into()));
    }

    let spender = Address::from_slice(&calldata[12..32]);
    if spender == Address::ZERO {
        return Err(PrecompileError::Other("approve: cannot approve address(0)".into()));
    }
    let amount = U256::from_be_slice(&calldata[32..64]);

    // Store allowance
    let slot = allowance_slot(caller, spender);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, amount)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Approval(owner, spender, value)
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Approval::SIGNATURE_HASH, address_to_topic(caller), address_to_topic(spender)],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Approval log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, abi_encode_bool(true)))
}

/// `transferFrom(address from, address to, uint256 amount)` → spends allowance and transfers.
///
/// 1. Loads `allowance[from][caller]` and verifies it is `>= amount`.
/// 2. Decrements the allowance by `amount` (skipped for infinite `U256::MAX` allowance).
/// 3. Transfers native balance from `from` to `to`.
/// 4. Emits `Transfer(from, to, amount)`.
///
/// Fails with "insufficient allowance" or a transfer error if balance is too low.
fn handle_transfer_from(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 35_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 96 {
        return Err(PrecompileError::Other(
            "transferFrom: expected 96 bytes (address,address,uint256)".into(),
        ));
    }

    let from = Address::from_slice(&calldata[12..32]);
    let to = Address::from_slice(&calldata[44..64]);
    let amount = U256::from_be_slice(&calldata[64..96]);

    if to == Address::ZERO {
        return Err(PrecompileError::Other("transferFrom: cannot transfer to address(0)".into()));
    }

    // Check allowance
    let slot = allowance_slot(from, caller);
    let current_allowance = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;

    if current_allowance < amount {
        return Err(PrecompileError::Other("transferFrom: insufficient allowance".into()));
    }

    // Decrement allowance (skip for infinite approval)
    let new_allowance = if current_allowance != U256::MAX {
        let new_allowance = current_allowance - amount;
        internals
            .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, new_allowance)
            .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;
        new_allowance
    } else {
        current_allowance
    };

    // Emit Approval(from, caller, new_allowance) — updated allowance after spend
    let approval_log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Approval::SIGNATURE_HASH, address_to_topic(from), address_to_topic(caller)],
        new_allowance.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Approval log".into()))?;
    internals.log(approval_log);

    // Transfer native balance
    let transfer_result = internals
        .transfer(from, to, amount)
        .map_err(|e| PrecompileError::Other(format!("transfer failed: {e:?}").into()))?;

    if let Some(error) = transfer_result {
        return Err(PrecompileError::Other(
            format!("transferFrom transfer error: {error:?}").into(),
        ));
    }

    // Emit Transfer(from, to, value)
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![Transfer::SIGNATURE_HASH, address_to_topic(from), address_to_topic(to)],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, abi_encode_bool(true)))
}

/// `mint(uint256 amount)` — creates a timelocked pending mint to governance.
///
/// Stores `amount` at the governance address's amount slot and
/// `block.timestamp + TIMELOCK_DURATION` at the timestamp slot.
/// **Does not credit the balance** — that happens in [`handle_claim`] after the timelock expires.
///
/// # Access control
/// Requires [`has_mint_role`] — only governance qualifies in production.
///
/// # Security notes
/// - The recipient is always [`GOVERNANCE_SAFE_ADDRESS`]; callers cannot choose a target.
/// - A second `mint` **overwrites** the previous pending amount and resets the timelock.
/// - Emits `Mint(recipient, amount, unlockTimestamp)`.
#[cfg(not(feature = "faucet"))] // see `handle_mint_faucet`
fn handle_mint(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 41_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if !has_mint_role(internals, caller)? {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("mint: expected 32 bytes (uint256)".into()));
    }

    // NOTE: allow mint to be `0` to replace pending mints during timelock
    let amount = U256::from_be_slice(&calldata[0..32]);
    let recipient = GOVERNANCE_SAFE_ADDRESS;

    // Compute unlock timestamp
    let current_ts = internals.block_timestamp();
    let unlock_ts = current_ts + U256::from(TIMELOCK_DURATION);

    // Store amount at keccak256(recipient, 0)
    let amt_slot = amount_slot(recipient);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, amt_slot, amount)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Store unlock timestamp at keccak256(recipient, 1)
    let ts_slot = timestamp_slot(recipient);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, ts_slot, unlock_ts)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Mint(address recipient, uint256 amount, uint256 unlockTimestamp)
    let topic0 = Mint::SIGNATURE_HASH;
    let mut log_data = Vec::with_capacity(64);
    log_data.extend_from_slice(&amount.to_be_bytes::<32>());
    log_data.extend_from_slice(&unlock_ts.to_be_bytes::<32>());

    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0, address_to_topic(recipient)],
        log_data.into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Mint log".into()))?;
    internals.log(log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `mint(address recipient, uint256 amount)` — faucet variant that directly credits the recipient.
///
/// Unlike [`handle_mint`], this does **not** create pending state or require a separate
/// [`handle_claim`] call. The recipient's native balance is incremented immediately and
/// `totalSupply` is updated in the same call.
///
/// # Emits
/// - `Mint(recipient, amount, unlockTimestamp=0)`
/// - `Transfer(address(0), recipient, amount)`
#[cfg(feature = "faucet")]
fn handle_mint_faucet(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 30_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if !has_mint_role(internals, caller)? {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 64 {
        return Err(PrecompileError::Other("mint: expected 64 bytes (address, uint256)".into()));
    }

    let amount = U256::from_be_slice(&calldata[32..64]);
    if amount.is_zero() {
        return Err(PrecompileError::Other("mint: amount must be greater than zero".into()));
    }

    let recipient = Address::from_slice(&calldata[12..32]);

    // Directly credit recipient's native balance
    internals
        .balance_incr(recipient, amount)
        .map_err(|e| PrecompileError::Other(format!("balance_incr failed: {e:?}").into()))?;

    // Increment totalSupply
    let current_supply = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    internals
        .sstore(
            TELCOIN_PRECOMPILE_ADDRESS,
            TOTAL_SUPPLY_SLOT,
            current_supply
                .checked_add(amount)
                .ok_or_else(|| PrecompileError::Other("mint: total supply overflow".into()))?,
        )
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Mint(address recipient, uint256 amount, uint256 unlockTimestamp=0)
    let topic0 = Mint::SIGNATURE_HASH;
    let mut log_data = Vec::with_capacity(64);
    log_data.extend_from_slice(&amount.to_be_bytes::<32>());
    log_data.extend_from_slice(&U256::ZERO.to_be_bytes::<32>());

    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0, address_to_topic(recipient)],
        log_data.into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Mint log".into()))?;
    internals.log(log);

    // Emit Transfer(address(0), recipient, amount) — ERC20 mint event
    let transfer_log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![
            Transfer::SIGNATURE_HASH,
            address_to_topic(Address::ZERO),
            address_to_topic(recipient),
        ],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(transfer_log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `claim(address recipient)` — finalizes a pending mint after the timelock expires.
///
/// Flow:
/// 1. Loads pending `amount` and `unlock_ts` from precompile storage.
/// 2. Verifies `block.timestamp >= unlock_ts` (timelock check).
/// 3. Credits `amount` to the recipient's **native balance** via `balance_incr`.
/// 4. Clears both storage slots (amount and timestamp) to prevent double-claim.
/// 5. Increments `totalSupply`.
/// 6. Emits `Claim(recipient, amount)` and `Transfer(address(0), recipient, amount)`.
///
/// # Access control
/// **Permissionless** — anyone can call `claim` on behalf of any recipient once the
/// timelock has passed.
fn handle_claim(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 25_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("claim: expected 32 bytes (address)".into()));
    }

    let recipient = Address::from_slice(&calldata[12..32]);

    // Load pending amount
    let amt_slot = amount_slot(recipient);
    let amount = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, amt_slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;

    if amount.is_zero() {
        return Err(PrecompileError::Other("claim: no pending mint".into()));
    }

    // Load unlock timestamp
    let ts_slot = timestamp_slot(recipient);
    let unlock_ts = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, ts_slot)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;

    // Check timelock
    let current_ts = internals.block_timestamp();
    if current_ts < unlock_ts {
        return Err(PrecompileError::Other("claim: timelock not expired".into()));
    }

    // Credit recipient
    internals
        .balance_incr(recipient, amount)
        .map_err(|e| PrecompileError::Other(format!("balance_incr failed: {e:?}").into()))?;

    // Clear storage
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, amt_slot, U256::ZERO)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, ts_slot, U256::ZERO)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Increment totalSupply
    let current_supply = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    internals
        .sstore(
            TELCOIN_PRECOMPILE_ADDRESS,
            TOTAL_SUPPLY_SLOT,
            current_supply
                .checked_add(amount)
                .ok_or_else(|| PrecompileError::Other("claim: total supply overflow".into()))?,
        )
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Claim(address recipient, uint256 amount)
    let topic0 = Claim::SIGNATURE_HASH;
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0, address_to_topic(recipient)],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Claim log".into()))?;
    internals.log(log);

    // Emit Transfer(address(0), recipient, amount) — ERC20 mint event
    let transfer_log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![
            Transfer::SIGNATURE_HASH,
            address_to_topic(Address::ZERO),
            address_to_topic(recipient),
        ],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(transfer_log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `burn(uint256 amount)` — destroys tokens held by the precompile account.
///
/// Transfers `amount` from the precompile's native balance to `address(0)` (effectively
/// destroying it), then decrements `totalSupply`.
///
/// # Access control
/// Governance-only via [`has_burn_role`].
fn handle_burn(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 8_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if !has_burn_role(caller) {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("burn: expected 32 bytes (uint256)".into()));
    }

    let amount = U256::from_be_slice(&calldata[0..32]);

    // Transfer from precompile to zero address (burn)
    let transfer_result = internals
        .transfer(TELCOIN_PRECOMPILE_ADDRESS, Address::ZERO, amount)
        .map_err(|e| PrecompileError::Other(format!("transfer failed: {e:?}").into()))?;

    if let Some(error) = transfer_result {
        return Err(PrecompileError::Other(format!("burn transfer error: {error:?}").into()));
    }

    // Decrement totalSupply
    let current_supply = internals
        .sload(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT)
        .map_err(|e| PrecompileError::Other(format!("sload failed: {e:?}").into()))?
        .data;
    let new_supply = current_supply
        .checked_sub(amount)
        .ok_or_else(|| PrecompileError::Other("burn: total supply underflow".into()))?;
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, TOTAL_SUPPLY_SLOT, new_supply)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;

    // Emit Burn(uint256 amount)
    let topic0 = Burn::SIGNATURE_HASH;
    let log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![topic0],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Burn log".into()))?;
    internals.log(log);

    // Emit Transfer(precompile, address(0), amount) — ERC20 burn event
    let transfer_log = reth_revm::primitives::Log::new(
        TELCOIN_PRECOMPILE_ADDRESS,
        vec![
            Transfer::SIGNATURE_HASH,
            address_to_topic(TELCOIN_PRECOMPILE_ADDRESS),
            address_to_topic(Address::ZERO),
        ],
        amount.to_be_bytes_vec().into(),
    )
    .ok_or_else(|| PrecompileError::Other("Failed to create Transfer log".into()))?;
    internals.log(transfer_log);

    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

// --- Faucet feature: role management handlers ---

/// `grantMintRole(address)` — governance-only. Writes `1` to the mint-role storage slot,
/// enabling `addr` to call `mint`.
#[cfg(feature = "faucet")]
fn handle_grant_mint_role(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 22_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if caller != GOVERNANCE_SAFE_ADDRESS {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("grantMintRole: expected 32 bytes (address)".into()));
    }
    let addr = Address::from_slice(&calldata[12..32]);
    let slot = mint_role_slot(addr);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, U256::from(1))
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;
    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `revokeMintRole(address)` — governance-only. Writes `0` to the mint-role storage slot,
/// revoking `addr`'s ability to call `mint`.
#[cfg(feature = "faucet")]
fn handle_revoke_mint_role(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    caller: Address,
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 22_000;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if caller != GOVERNANCE_SAFE_ADDRESS {
        return Err(PrecompileError::Other("unauthorized".into()));
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("revokeMintRole: expected 32 bytes (address)".into()));
    }
    let addr = Address::from_slice(&calldata[12..32]);
    let slot = mint_role_slot(addr);
    internals
        .sstore(TELCOIN_PRECOMPILE_ADDRESS, slot, U256::ZERO)
        .map_err(|e| PrecompileError::Other(format!("sstore failed: {e:?}").into()))?;
    Ok(PrecompileOutput::new(GAS_COST, Bytes::new()))
}

/// `hasMintRole(address)` — read-only query. Returns `true` if `addr` is governance or has
/// been dynamically granted the mint role.
#[cfg(feature = "faucet")]
fn handle_has_mint_role(
    internals: &mut EvmInternals<'_>,
    calldata: &[u8],
    gas_limit: u64,
) -> PrecompileResult {
    const GAS_COST: u64 = 2_100;
    if gas_limit < GAS_COST {
        return Err(PrecompileError::OutOfGas);
    }
    if calldata.len() < 32 {
        return Err(PrecompileError::Other("hasMintRole: expected 32 bytes (address)".into()));
    }
    let addr = Address::from_slice(&calldata[12..32]);
    let has_role = has_mint_role(internals, addr)?;
    Ok(PrecompileOutput::new(GAS_COST, abi_encode_bool(has_role)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::sol_types::SolCall;
    use alloy_evm::precompiles::PrecompilesMap;
    use reth_revm::{
        context::{
            result::{EVMError, ExecutionResult, InvalidTransaction, Output},
            BlockEnv, Context, ContextSetters, Evm, FrameStack, TxEnv,
        },
        context_interface::ContextTr,
        db::InMemoryDB,
        handler::{
            instructions::EthInstructions, EthFrame, EthPrecompiles, Handler, MainnetContext,
            MainnetHandler,
        },
        inspector::NoOpInspector,
        interpreter::interpreter::EthInterpreter,
        primitives::{address, Address, KECCAK_EMPTY},
        state::AccountInfo,
        MainContext,
    };
    use std::collections::HashMap;
    use tn_config::GOVERNANCE_SAFE_ADDRESS;
    use tn_types::{TxKind, U256};

    /// Test EVM type using PrecompilesMap with the Telcoin precompile registered.
    type TestCtx = MainnetContext<InMemoryDB>;

    type TestEvmInner = Evm<
        TestCtx,
        NoOpInspector,
        EthInstructions<EthInterpreter, TestCtx>,
        PrecompilesMap,
        EthFrame<EthInterpreter>,
    >;

    type TestResult =
        Result<ExecutionResult, EVMError<core::convert::Infallible, InvalidTransaction>>;

    const USER: Address = address!("1111100000000000000000000000000000000001");
    const RECIPIENT: Address = address!("2222222000000000000000000000000000000002");

    struct TestEnv {
        evm: TestEvmInner,
        nonces: HashMap<Address, u64>,
    }

    impl TestEnv {
        fn new() -> Self {
            let mut db = InMemoryDB::default();

            db.insert_account_info(
                GOVERNANCE_SAFE_ADDRESS,
                AccountInfo {
                    balance: U256::from(10).pow(U256::from(18)),
                    nonce: 0,
                    code_hash: KECCAK_EMPTY,
                    code: None,
                    ..Default::default()
                },
            );

            db.insert_account_info(
                USER,
                AccountInfo {
                    balance: U256::from(10).pow(U256::from(18)),
                    nonce: 0,
                    code_hash: KECCAK_EMPTY,
                    code: None,
                    ..Default::default()
                },
            );

            db.insert_account_info(
                TELCOIN_PRECOMPILE_ADDRESS,
                AccountInfo {
                    balance: U256::from(1000),
                    nonce: 0,
                    code_hash: KECCAK_EMPTY,
                    code: None,
                    ..Default::default()
                },
            );

            db.insert_account_storage(
                TELCOIN_PRECOMPILE_ADDRESS,
                U256::from(100),
                U256::from(100_000_000_000u128) * U256::from(10).pow(U256::from(18)),
            )
            .unwrap();

            let mut block = BlockEnv::default();
            block.timestamp = U256::from(1000);
            let context = Context::mainnet().with_db(db).with_block(block);

            let mut precompiles = PrecompilesMap::from(EthPrecompiles::default());
            add_telcoin_precompile(&mut precompiles);

            let evm = Evm {
                ctx: context,
                inspector: NoOpInspector,
                instruction: EthInstructions::default(),
                precompiles,
                frame_stack: FrameStack::new(),
            };

            Self { evm, nonces: HashMap::new() }
        }

        fn exec(&mut self, caller: Address, calldata: Vec<u8>, gas_limit: u64) -> TestResult {
            let nonce = self.nonces.entry(caller).or_insert(0);
            self.evm.ctx.set_tx(
                TxEnv::builder()
                    .caller(caller)
                    .kind(TxKind::Call(TELCOIN_PRECOMPILE_ADDRESS))
                    .data(calldata.into())
                    .gas_limit(gas_limit)
                    .nonce(*nonce)
                    .build()
                    .unwrap(),
            );
            *nonce += 1;
            MainnetHandler::default().run(&mut self.evm)
        }

        fn exec_default(&mut self, caller: Address, calldata: Vec<u8>) -> TestResult {
            self.exec(caller, calldata, 100_000)
        }

        #[cfg(not(feature = "faucet"))]
        fn set_timestamp(&mut self, ts: u64) {
            let mut block = BlockEnv::default();
            block.timestamp = U256::from(ts);
            self.evm.ctx.set_block(block);
        }
    }

    fn assert_success(result: &TestResult) -> &ExecutionResult {
        let r = result.as_ref().expect("expected Ok, got Err");
        assert!(matches!(r, ExecutionResult::Success { .. }), "expected Success, got {r:?}");
        r
    }

    fn assert_not_success(result: &TestResult) {
        match result {
            Ok(ExecutionResult::Success { .. }) => panic!("expected non-success, got Success"),
            _ => {}
        }
    }

    fn extract_output_bytes(result: &TestResult) -> Bytes {
        let r = assert_success(result);
        match r {
            ExecutionResult::Success { output, .. } => match output {
                Output::Call(b) => b.clone(),
                Output::Create(b, _) => b.clone(),
            },
            _ => unreachable!(),
        }
    }

    fn decode_u256(result: &TestResult) -> U256 {
        let bytes = extract_output_bytes(result);
        assert!(bytes.len() >= 32, "output too short for U256");
        U256::from_be_slice(&bytes[..32])
    }

    fn decode_bool(result: &TestResult) -> bool {
        !decode_u256(result).is_zero()
    }

    // ==============================
    // Group A: Happy paths
    // ==============================

    #[test]
    fn test_mint_succeeds() {
        let mut env = TestEnv::new();
        #[cfg(not(feature = "faucet"))]
        let data = mintCall { amount: U256::from(500) }.abi_encode();
        #[cfg(feature = "faucet")]
        let data = mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_claim_before_timelock_halts() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        assert_not_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_claim_after_timelock_succeeds() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        assert_success(&result);
    }

    #[test]
    fn test_burn_succeeds() {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(200) }.abi_encode(),
        );
        assert_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_total_supply_after_claim_and_burn() {
        let mut env = TestEnv::new();
        let genesis = U256::from(100_000_000_000u128) * U256::from(10).pow(U256::from(18));
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        )
        .unwrap();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(200) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, totalSupplyCall {}.abi_encode());
        assert_eq!(decode_u256(&result), genesis + U256::from(500) - U256::from(200));
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_balance_of() {
        let mut env = TestEnv::new();
        let initial_balance = U256::from(10).pow(U256::from(18));
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            balanceOfCall { account: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), initial_balance + U256::from(500));
    }

    #[test]
    fn test_transfer() {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            USER,
            transferCall { to: RECIPIENT, amount: U256::from(100) }.abi_encode(),
        );
        assert!(decode_bool(&result));
    }

    #[test]
    fn test_approve() {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            approveCall { spender: USER, amount: U256::from(200) }.abi_encode(),
        );
        assert!(decode_bool(&result));
    }

    #[test]
    fn test_transfer_from() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            approveCall { spender: USER, amount: U256::from(200) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            USER,
            transferFromCall {
                from: GOVERNANCE_SAFE_ADDRESS,
                to: RECIPIENT,
                amount: U256::from(150),
            }
            .abi_encode(),
        );
        assert!(decode_bool(&result));
    }

    #[test]
    fn test_name() {
        let mut env = TestEnv::new();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, nameCall {}.abi_encode());
        let bytes = extract_output_bytes(&result);
        let len = U256::from_be_slice(&bytes[32..64]).to::<usize>();
        assert_eq!(std::str::from_utf8(&bytes[64..64 + len]).unwrap(), "Telcoin");
    }

    #[test]
    fn test_symbol() {
        let mut env = TestEnv::new();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, symbolCall {}.abi_encode());
        let bytes = extract_output_bytes(&result);
        let len = U256::from_be_slice(&bytes[32..64]).to::<usize>();
        assert_eq!(std::str::from_utf8(&bytes[64..64 + len]).unwrap(), "TEL");
    }

    #[test]
    fn test_decimals() {
        let mut env = TestEnv::new();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, decimalsCall {}.abi_encode());
        assert_eq!(decode_u256(&result), U256::from(18));
    }

    #[test]
    fn test_unauthorized_mint() {
        let mut env = TestEnv::new();
        #[cfg(not(feature = "faucet"))]
        let data = mintCall { amount: U256::from(100) }.abi_encode();
        #[cfg(feature = "faucet")]
        let data = mintCall { recipient: RECIPIENT, amount: U256::from(100) }.abi_encode();
        let result = env.exec_default(USER, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_unauthorized_burn() {
        let mut env = TestEnv::new();
        let result = env.exec_default(USER, burnCall { amount: U256::from(100) }.abi_encode());
        assert_not_success(&result);
    }

    // ==============================
    // Group B: Faucet feature tests
    // ==============================

    #[cfg(feature = "faucet")]
    const FAUCET: Address = address!("0000000000000000000000000000000000000F00");

    #[cfg(feature = "faucet")]
    fn fund_faucet(env: &mut TestEnv) {
        env.evm.ctx.db_mut().insert_account_info(
            FAUCET,
            AccountInfo {
                balance: U256::from(10).pow(U256::from(18)),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_grant_mint_role() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        let data = grantMintRoleCall { addr: FAUCET }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_success(&result);
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_faucet_mint() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        let data = mintCall { recipient: RECIPIENT, amount: U256::from(1000) }.abi_encode();
        let result = env.exec_default(FAUCET, data);
        assert_success(&result);
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_revoke_mint_role() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        let data = revokeMintRoleCall { addr: FAUCET }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_success(&result);
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_faucet_mint_after_revoke_fails() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, revokeMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        let data = mintCall { recipient: RECIPIENT, amount: U256::from(1000) }.abi_encode();
        let result = env.exec_default(FAUCET, data);
        assert_not_success(&result);
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_faucet_mint_directly_credits() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        let result = env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
        );
        assert_success(&result);
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            balanceOfCall { account: RECIPIENT }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), U256::from(500));
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_faucet_mint_increments_total_supply() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        let genesis = U256::from(100_000_000_000u128) * U256::from(10).pow(U256::from(18));
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, totalSupplyCall {}.abi_encode());
        assert_eq!(decode_u256(&result), genesis + U256::from(500));
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_faucet_double_mint_is_additive() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(300) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            balanceOfCall { account: RECIPIENT }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), U256::from(800));
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_faucet_claim_after_mint_fails() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        let result = env
            .exec_default(GOVERNANCE_SAFE_ADDRESS, claimCall { recipient: RECIPIENT }.abi_encode());
        assert_not_success(&result);
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_faucet_balance_of() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(750) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            balanceOfCall { account: RECIPIENT }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), U256::from(750));
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_faucet_total_supply_after_mint_and_burn() {
        let mut env = TestEnv::new();
        fund_faucet(&mut env);
        let genesis = U256::from(100_000_000_000u128) * U256::from(10).pow(U256::from(18));
        env.exec_default(GOVERNANCE_SAFE_ADDRESS, grantMintRoleCall { addr: FAUCET }.abi_encode())
            .unwrap();
        env.exec_default(
            FAUCET,
            mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(200) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, totalSupplyCall {}.abi_encode());
        assert_eq!(decode_u256(&result), genesis + U256::from(500) - U256::from(200));
    }

    // ==============================
    // Group C: Edge cases and error paths
    // ==============================

    #[test]
    fn test_input_too_short() {
        let mut env = TestEnv::new();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, vec![0xAB, 0xCD]);
        assert_not_success(&result);
    }

    #[test]
    fn test_unknown_selector() {
        let mut env = TestEnv::new();
        let mut data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        data.extend_from_slice(&[0u8; 32]);
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_mint_short_calldata() {
        let mut env = TestEnv::new();
        #[cfg(not(feature = "faucet"))]
        let short = {
            let full = mintCall { amount: U256::from(500) }.abi_encode();
            full[..4 + 16].to_vec()
        };
        #[cfg(feature = "faucet")]
        let short = {
            let full = mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode();
            full[..4 + 32].to_vec()
        };
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, short);
        assert_not_success(&result);
    }

    #[test]
    fn test_balance_of_short_calldata() {
        let mut env = TestEnv::new();
        let full = balanceOfCall { account: RECIPIENT }.abi_encode();
        let short = full[..4 + 16].to_vec();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, short);
        assert_not_success(&result);
    }

    #[test]
    fn test_allowance_short_calldata() {
        let mut env = TestEnv::new();
        let full = allowanceCall { owner: GOVERNANCE_SAFE_ADDRESS, spender: USER }.abi_encode();
        let short = full[..4 + 32].to_vec();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, short);
        assert_not_success(&result);
    }

    #[test]
    fn test_claim_no_pending_mint() {
        let mut env = TestEnv::new();
        let result = env
            .exec_default(GOVERNANCE_SAFE_ADDRESS, claimCall { recipient: RECIPIENT }.abi_encode());
        assert_not_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_claim_already_claimed() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let claim_data = claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, claim_data.clone());
        assert_success(&result);
        let result2 = env.exec_default(GOVERNANCE_SAFE_ADDRESS, claim_data);
        assert_not_success(&result2);
    }

    #[test]
    fn test_burn_insufficient_balance() {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            burnCall { amount: U256::from(2000) }.abi_encode(),
        );
        assert_not_success(&result);
    }

    #[test]
    fn test_transfer_insufficient_balance() {
        let mut env = TestEnv::new();
        let too_much = U256::from(10).pow(U256::from(18)) + U256::from(1);
        let result =
            env.exec_default(USER, transferCall { to: RECIPIENT, amount: too_much }.abi_encode());
        assert_not_success(&result);
    }

    #[test]
    fn test_transfer_from_insufficient_allowance() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            approveCall { spender: USER, amount: U256::from(100) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            USER,
            transferFromCall {
                from: GOVERNANCE_SAFE_ADDRESS,
                to: RECIPIENT,
                amount: U256::from(200),
            }
            .abi_encode(),
        );
        assert_not_success(&result);
    }

    // --- Out of gas (OOG) ---

    #[test]
    fn test_mint_oog() {
        let mut env = TestEnv::new();
        #[cfg(not(feature = "faucet"))]
        let data = mintCall { amount: U256::from(500) }.abi_encode();
        #[cfg(feature = "faucet")]
        let data = mintCall { recipient: RECIPIENT, amount: U256::from(500) }.abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_claim_oog() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        let data = claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    fn test_burn_oog() {
        let mut env = TestEnv::new();
        let data = burnCall { amount: U256::from(100) }.abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    fn test_transfer_oog() {
        let mut env = TestEnv::new();
        let data = transferCall { to: RECIPIENT, amount: U256::from(100) }.abi_encode();
        let result = env.exec(USER, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    fn test_name_oog() {
        let mut env = TestEnv::new();
        let data = nameCall {}.abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    fn test_total_supply_oog() {
        let mut env = TestEnv::new();
        let data = totalSupplyCall {}.abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    // --- Miscellaneous ---

    #[test]
    fn test_total_supply_zero_no_genesis() {
        let mut env = TestEnv::new();
        env.evm
            .ctx
            .db_mut()
            .insert_account_storage(TELCOIN_PRECOMPILE_ADDRESS, U256::from(100), U256::ZERO)
            .unwrap();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, totalSupplyCall {}.abi_encode());
        assert_eq!(decode_u256(&result), U256::ZERO);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_double_mint_overwrites() {
        let mut env = TestEnv::new();
        let initial_balance = U256::from(10).pow(U256::from(18));
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(300) }.abi_encode(),
        )
        .unwrap();
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            balanceOfCall { account: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), initial_balance + U256::from(300));
    }

    #[test]
    fn test_allowance_after_approve() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            approveCall { spender: USER, amount: U256::from(200) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            allowanceCall { owner: GOVERNANCE_SAFE_ADDRESS, spender: USER }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), U256::from(200));
    }

    #[test]
    fn test_transfer_from_decrements_allowance() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            approveCall { spender: USER, amount: U256::from(200) }.abi_encode(),
        )
        .unwrap();
        env.exec_default(
            USER,
            transferFromCall {
                from: GOVERNANCE_SAFE_ADDRESS,
                to: RECIPIENT,
                amount: U256::from(50),
            }
            .abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            allowanceCall { owner: GOVERNANCE_SAFE_ADDRESS, spender: USER }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), U256::from(150));
    }

    #[test]
    fn test_balance_of_unfunded() {
        let mut env = TestEnv::new();
        let nobody = address!("0000000000000000000000000000000000099999");
        let result = env
            .exec_default(GOVERNANCE_SAFE_ADDRESS, balanceOfCall { account: nobody }.abi_encode());
        assert_eq!(decode_u256(&result), U256::ZERO);
    }

    // ==============================
    // Group D: Security report fixes
    // ==============================

    #[test]
    fn test_transfer_from_infinite_allowance_not_decremented() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            approveCall { spender: USER, amount: U256::MAX }.abi_encode(),
        )
        .unwrap();
        env.exec_default(
            USER,
            transferFromCall {
                from: GOVERNANCE_SAFE_ADDRESS,
                to: RECIPIENT,
                amount: U256::from(50),
            }
            .abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            allowanceCall { owner: GOVERNANCE_SAFE_ADDRESS, spender: USER }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), U256::MAX);
    }

    #[test]
    fn test_transfer_to_zero_address_fails() {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            USER,
            transferCall { to: Address::ZERO, amount: U256::from(100) }.abi_encode(),
        );
        assert_not_success(&result);
    }

    #[test]
    fn test_transfer_from_to_zero_address_fails() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            approveCall { spender: USER, amount: U256::from(200) }.abi_encode(),
        )
        .unwrap();
        let result = env.exec_default(
            USER,
            transferFromCall {
                from: GOVERNANCE_SAFE_ADDRESS,
                to: Address::ZERO,
                amount: U256::from(100),
            }
            .abi_encode(),
        );
        assert_not_success(&result);
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_zero_amount_mint_fails() {
        let mut env = TestEnv::new();
        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { recipient: RECIPIENT, amount: U256::ZERO }.abi_encode(),
        );
        assert_not_success(&result);
    }

    #[test]
    #[cfg(not(feature = "faucet"))]
    fn test_zero_amount_mint_does_not_overwrite_pending() {
        let mut env = TestEnv::new();
        env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            mintCall { amount: U256::from(500) }.abi_encode(),
        )
        .unwrap();
        // Zero-amount mint succeeds and cancels the pending mint
        let result =
            env.exec_default(GOVERNANCE_SAFE_ADDRESS, mintCall { amount: U256::ZERO }.abi_encode());
        assert_success(&result);
        env.set_timestamp(1000 + TIMELOCK_DURATION + 1);
        // Claim fails because pending was overwritten to zero
        let claim_result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            claimCall { recipient: GOVERNANCE_SAFE_ADDRESS }.abi_encode(),
        );
        assert_not_success(&claim_result);
    }

    // --- Faucet error tests (cfg-gated) ---

    #[test]
    #[cfg(feature = "faucet")]
    fn test_grant_role_unauthorized() {
        let mut env = TestEnv::new();
        let data = grantMintRoleCall { addr: USER }.abi_encode();
        let result = env.exec_default(USER, data);
        assert_not_success(&result);
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_revoke_role_unauthorized() {
        let mut env = TestEnv::new();
        let data = revokeMintRoleCall { addr: GOVERNANCE_SAFE_ADDRESS }.abi_encode();
        let result = env.exec_default(USER, data);
        assert_not_success(&result);
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_has_mint_role_governance() {
        let mut env = TestEnv::new();
        let data = hasMintRoleCall { addr: GOVERNANCE_SAFE_ADDRESS }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert!(decode_bool(&result));
    }

    #[test]
    #[cfg(feature = "faucet")]
    fn test_has_mint_role_random() {
        let mut env = TestEnv::new();
        let random = address!("0000000000000000000000000000000000ABCDEF");
        let data = hasMintRoleCall { addr: random }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert!(!decode_bool(&result));
    }

    // ==============================
    // Group E: EIP-2612 Permit
    // ==============================

    /// Fixed secret key for permit signing tests.
    const PERMIT_SECRET: B256 = B256::new([
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 1,
    ]);

    /// Derive the address corresponding to PERMIT_SECRET.
    fn permit_signer_address() -> Address {
        use alloy::signers::local::PrivateKeySigner;
        let signer = PrivateKeySigner::from_slice(&PERMIT_SECRET.0).unwrap();
        signer.address()
    }

    /// Fund the permit signer account in the test environment.
    fn fund_permit_signer(env: &mut TestEnv) -> Address {
        let addr = permit_signer_address();
        env.evm.ctx.db_mut().insert_account_info(
            addr,
            AccountInfo {
                balance: U256::from(10).pow(U256::from(18)),
                nonce: 0,
                code_hash: KECCAK_EMPTY,
                code: None,
                ..Default::default()
            },
        );
        addr
    }

    /// Sign a permit message using PERMIT_SECRET.
    fn sign_permit_test(
        owner: Address,
        spender: Address,
        value: U256,
        nonce: U256,
        deadline: U256,
        chain_id: u64,
    ) -> (u8, B256, B256) {
        use alloy::signers::{local::PrivateKeySigner, SignerSync};

        let signer = PrivateKeySigner::from_slice(&PERMIT_SECRET.0).unwrap();

        let domain_separator = compute_domain_separator(chain_id);

        let struct_hash = {
            let mut buf = [0u8; 6 * 32];
            buf[0..32].copy_from_slice(&PERMIT_TYPEHASH.0);
            buf[44..64].copy_from_slice(owner.as_slice());
            buf[76..96].copy_from_slice(spender.as_slice());
            buf[96..128].copy_from_slice(&value.to_be_bytes::<32>());
            buf[128..160].copy_from_slice(&nonce.to_be_bytes::<32>());
            buf[160..192].copy_from_slice(&deadline.to_be_bytes::<32>());
            keccak256(buf)
        };

        let digest = {
            let mut buf = [0u8; 66];
            buf[0] = 0x19;
            buf[1] = 0x01;
            buf[2..34].copy_from_slice(&domain_separator.0);
            buf[34..66].copy_from_slice(&struct_hash.0);
            keccak256(buf)
        };

        let sig = signer.sign_hash_sync(&digest).unwrap();
        let v = if sig.v() { 28u8 } else { 27u8 };
        let r = B256::from(sig.r().to_be_bytes::<32>());
        let s = B256::from(sig.s().to_be_bytes::<32>());
        (v, r, s)
    }

    #[test]
    fn test_typehash_values() {
        let domain_hash = keccak256(
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        assert_eq!(domain_hash, EIP712_DOMAIN_TYPEHASH);

        let permit_hash = keccak256(
            "Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)",
        );
        assert_eq!(permit_hash, PERMIT_TYPEHASH);
    }

    #[test]
    fn test_permit_sets_allowance() {
        let mut env = TestEnv::new();
        let owner = fund_permit_signer(&mut env);
        let value = U256::from(500);
        let deadline = U256::from(2000);

        let (v, r, s) = sign_permit_test(owner, RECIPIENT, value, U256::ZERO, deadline, 1);
        let data = permitCall { owner, spender: RECIPIENT, value, deadline, v, r, s }.abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_success(&result);

        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            allowanceCall { owner, spender: RECIPIENT }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), value);
    }

    #[test]
    fn test_permit_increments_nonce() {
        let mut env = TestEnv::new();
        let owner = fund_permit_signer(&mut env);
        let deadline = U256::from(2000);

        // First permit (nonce 0)
        let (v, r, s) =
            sign_permit_test(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v, r, s }
                .abi_encode();
        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));

        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, noncesCall { owner }.abi_encode());
        assert_eq!(decode_u256(&result), U256::from(1));

        // Second permit (nonce 1)
        let (v, r, s) =
            sign_permit_test(owner, RECIPIENT, U256::from(200), U256::from(1), deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(200), deadline, v, r, s }
                .abi_encode();
        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));

        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, noncesCall { owner }.abi_encode());
        assert_eq!(decode_u256(&result), U256::from(2));
    }

    #[test]
    fn test_permit_then_transfer_from() {
        let mut env = TestEnv::new();
        let owner = fund_permit_signer(&mut env);
        let value = U256::from(500);
        let deadline = U256::from(2000);

        let (v, r, s) = sign_permit_test(owner, USER, value, U256::ZERO, deadline, 1);
        let data = permitCall { owner, spender: USER, value, deadline, v, r, s }.abi_encode();
        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));

        let result = env.exec_default(
            USER,
            transferFromCall { from: owner, to: RECIPIENT, amount: U256::from(200) }.abi_encode(),
        );
        assert!(decode_bool(&result));

        let result = env.exec_default(
            GOVERNANCE_SAFE_ADDRESS,
            allowanceCall { owner, spender: USER }.abi_encode(),
        );
        assert_eq!(decode_u256(&result), value - U256::from(200));
    }

    #[test]
    fn test_permit_expired_deadline_fails() {
        let mut env = TestEnv::new();
        let owner = fund_permit_signer(&mut env);
        // Block timestamp is 1000, deadline is 999 (already expired)
        let deadline = U256::from(999);
        let (v, r, s) =
            sign_permit_test(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v, r, s }
                .abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_permit_wrong_signer_fails() {
        let mut env = TestEnv::new();
        let signer_addr = fund_permit_signer(&mut env);
        let deadline = U256::from(2000);

        // Sign as the real signer address
        let (v, r, s) =
            sign_permit_test(signer_addr, USER, U256::from(100), U256::ZERO, deadline, 1);
        // Submit with USER as the claimed owner — signature won't match
        let data = permitCall {
            owner: USER,
            spender: RECIPIENT,
            value: U256::from(100),
            deadline,
            v,
            r,
            s,
        }
        .abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_permit_replay_fails() {
        let mut env = TestEnv::new();
        let owner = fund_permit_signer(&mut env);
        let deadline = U256::from(2000);

        let (v, r, s) =
            sign_permit_test(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v, r, s }
                .abi_encode();

        // First call succeeds
        assert_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data.clone()));
        // Replay fails (nonce incremented)
        assert_not_success(&env.exec_default(GOVERNANCE_SAFE_ADDRESS, data));
    }

    #[test]
    fn test_permit_invalid_v_fails() {
        let mut env = TestEnv::new();
        let owner = fund_permit_signer(&mut env);
        let deadline = U256::from(2000);

        let (_, r, s) =
            sign_permit_test(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v: 26, r, s }
                .abi_encode();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, data);
        assert_not_success(&result);
    }

    #[test]
    fn test_permit_short_calldata_fails() {
        let mut env = TestEnv::new();
        let full = permitCall {
            owner: GOVERNANCE_SAFE_ADDRESS,
            spender: USER,
            value: U256::from(100),
            deadline: U256::from(2000),
            v: 27,
            r: B256::ZERO,
            s: B256::ZERO,
        }
        .abi_encode();
        // Truncate to less than 228 bytes (4 selector + 224 calldata)
        let short = full[..4 + 192].to_vec();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, short);
        assert_not_success(&result);
    }

    #[test]
    fn test_permit_oog() {
        let mut env = TestEnv::new();
        let owner = fund_permit_signer(&mut env);
        let deadline = U256::from(2000);
        let (v, r, s) =
            sign_permit_test(owner, RECIPIENT, U256::from(100), U256::ZERO, deadline, 1);
        let data =
            permitCall { owner, spender: RECIPIENT, value: U256::from(100), deadline, v, r, s }
                .abi_encode();
        let result = env.exec(GOVERNANCE_SAFE_ADDRESS, data, 21_000);
        assert_not_success(&result);
    }

    #[test]
    fn test_nonces_initial_zero() {
        let mut env = TestEnv::new();
        let result =
            env.exec_default(GOVERNANCE_SAFE_ADDRESS, noncesCall { owner: USER }.abi_encode());
        assert_eq!(decode_u256(&result), U256::ZERO);
    }

    #[test]
    fn test_nonces_short_calldata_fails() {
        let mut env = TestEnv::new();
        let full = noncesCall { owner: USER }.abi_encode();
        let short = full[..4 + 16].to_vec();
        let result = env.exec_default(GOVERNANCE_SAFE_ADDRESS, short);
        assert_not_success(&result);
    }

    #[test]
    fn test_domain_separator_returns_value() {
        let mut env = TestEnv::new();
        let result =
            env.exec_default(GOVERNANCE_SAFE_ADDRESS, DOMAIN_SEPARATORCall {}.abi_encode());
        let bytes = extract_output_bytes(&result);
        assert_eq!(bytes.len(), 32);
        let expected = compute_domain_separator(1);
        assert_eq!(B256::from_slice(&bytes), expected);
    }

    #[test]
    fn test_domain_separator_deterministic() {
        let mut env = TestEnv::new();
        let r1 = env.exec_default(GOVERNANCE_SAFE_ADDRESS, DOMAIN_SEPARATORCall {}.abi_encode());
        let r2 = env.exec_default(GOVERNANCE_SAFE_ADDRESS, DOMAIN_SEPARATORCall {}.abi_encode());
        assert_eq!(extract_output_bytes(&r1), extract_output_bytes(&r2));
    }

    #[test]
    fn test_const_typehash_matches() {
        // eip712
        let eip712_domain_typehash_expected = keccak256(
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        assert_eq!(EIP712_DOMAIN_TYPEHASH, eip712_domain_typehash_expected);

        // eip2612
        let permit_typehash = keccak256(
            "Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)",
        );
        assert_eq!(PERMIT_TYPEHASH, permit_typehash);
    }
}
