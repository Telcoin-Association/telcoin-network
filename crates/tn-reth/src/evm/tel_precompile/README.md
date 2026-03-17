# TEL Precompile — Native ERC-20 at `0x7e1`

This directory implements a **native ERC-20 precompile** for the Telcoin (TEL) token. Unlike a standard Solidity contract, the precompile operates directly on native account balances — `balanceOf(addr)` returns the same value as `addr.balance`. This makes TEL simultaneously the chain's gas token and its primary ERC-20.

The precompile is registered as a `DynPrecompile` inside reth's `PrecompilesMap` at address `0x00000000000000000000000000000000000007e1`. Any `CALL` or `STATICCALL` targeting this address is intercepted by the dispatcher in `mod.rs` and routed to the appropriate handler based on the 4-byte function selector.

## Module map

| File | Purpose |
|------|---------|
| `mod.rs` | Top-level dispatcher: selector → handler routing, precompile registration |
| `erc20.rs` | Standard ERC-20: `name`, `symbol`, `decimals`, `totalSupply`, `balanceOf`, `transfer`, `approve`, `transferFrom`, `allowance` |
| `eip2612.rs` | EIP-2612 `permit` (gasless approvals), `nonces`, `DOMAIN_SEPARATOR` |
| `burnable.rs` | Timelocked `mint`/`claim` lifecycle + `burn` (production) |
| `faucet.rs` | Instant `mint` with role management (testnet, `faucet` feature) |
| `helpers.rs` | Storage slot derivation + minimal ABI encoders |
| `test_utils.rs` | In-memory EVM test harness (gated behind `#[cfg(test)]` / `test-utils` feature) |

## Storage layout

All precompile-managed state lives under the precompile address (`0x7e1`) using Solidity-compatible mapping layouts:

| Base slot | Type | Description |
|-----------|------|-------------|
| 0 | `mapping(address => uint256)` | Pending mint amounts |
| 1 | `mapping(address => uint256)` | Unlock timestamps (block.timestamp after which `claim` succeeds) |
| 2 | `mapping(address => mapping(address => uint256))` | ERC-20 allowances |
| 3 | `mapping(address => bool)` | Mint roles (`faucet` feature only) |
| 4 | `mapping(address => uint256)` | EIP-2612 permit nonces |
| 100 | `uint256` (plain slot) | Total circulating supply |

Slot derivation follows standard Solidity rules — e.g., `allowance[owner][spender]` is at `keccak256(abi.encode(spender, keccak256(abi.encode(owner, 2))))`. See `helpers.rs` for the implementations.

**Important:** Token balances are **not** in precompile storage. They are native account balances (`account.balance`), which is what makes `balanceOf` equivalent to checking the account's ether balance.

## Token lifecycle

### Production (`!faucet`)

```
mint(amount)  →  pending_amount[governance] = amount
                 unlock_ts[governance] = block.timestamp + 7 days

claim(recipient)  →  [if block.timestamp >= unlock_ts]
                     recipient.balance += amount
                     totalSupply += amount
                     clear pending slots

burn(amount)  →  precompile.balance -= amount  (sent to address(0))
                 totalSupply -= amount
```

- **`mint`**: Governance-only. Creates a pending mint with a 7-day timelock. A second `mint` overwrites the previous pending amount (can be used to cancel by minting 0).
- **`claim`**: Permissionless. Anyone can finalize a pending mint once the timelock expires.
- **`burn`**: Governance-only. Destroys tokens held by the precompile's own account.

### Testnet (`faucet` feature)

```
mint(recipient, amount)  →  recipient.balance += amount
                            totalSupply += amount
```

No pending state, no timelock. Mint roles can be granted/revoked by governance.

## Access control

| Function | Who can call |
|----------|-------------|
| `mint` (production) | Governance only |
| `mint` (faucet) | Governance + dynamically granted mint-role holders |
| `claim` | Anyone (after timelock) |
| `burn` | Governance only |
| `grantMintRole` / `revokeMintRole` | Governance only (faucet feature) |
| `transfer`, `approve`, `transferFrom`, `permit` | Any account |
| All view functions | Any account |

Governance is identified by `GOVERNANCE_SAFE_ADDRESS` from `tn-config`.

## Security considerations

### Timelock bypass (`faucet` feature)
The `faucet` feature **removes the 7-day timelock** on minting. A production binary must never be compiled with this feature enabled. The feature is set at compile time — there is no runtime toggle.

### ERC-20 approve race condition
`approve` overwrites the existing allowance without checking the current value. This is the standard ERC-20 behavior and is subject to the well-known front-running race. Users should set allowance to 0 before setting a new non-zero value, or use `permit` for atomic approval.

### Signature malleability (EIP-2612)
`permit` rejects signatures where `s > SECP256K1N_HALF` to prevent signature malleability. The `v` value must be exactly 27 or 28.

### Double-claim prevention
After `claim` succeeds, both the amount and timestamp storage slots are zeroed, preventing re-entry into the same pending mint.

### Native balance equivalence
Since `balanceOf` reads native account balances, any direct ETH-style transfer (e.g., `CALL` with value) changes the TEL balance without going through the precompile. The precompile's `Transfer` event is only emitted for calls routed through `transfer`/`transferFrom`/`claim`. Off-chain indexers must account for both native transfers and precompile events.

### Total supply accounting
`totalSupply` is only updated by `claim` (increment) and `burn` (decrement). It does **not** account for native balance changes outside the precompile (e.g., gas fees, coinbase rewards). The genesis value must be set correctly at chain initialization.

## Gas costs

Each handler has a fixed gas cost charged upfront:

| Function | Gas |
|----------|-----|
| `name`, `symbol`, `decimals` | 200 |
| `totalSupply`, `allowance`, `nonces`, `hasMintRole` | 2,100 |
| `balanceOf`, `DOMAIN_SEPARATOR` | 2,600 |
| `burn` | 8,000 |
| `transfer` | 12,000 |
| `approve`, `grantMintRole`, `revokeMintRole` | 22,000 |
| `claim` | 25,000 |
| `mint` (faucet) | 30,000 |
| `transferFrom` | 35,000 |
| `mint` (production) | 41,000 |
| `permit` | 72,000 |

These are rough estimates covering storage reads/writes and do not include the base transaction cost (21,000).

## Testing

Test infrastructure lives in `test_utils.rs` and is the single source of truth for both unit tests (in each module's `#[cfg(test)] mod tests`) and integration tests (in `crates/tn-reth/tests/it/`).

```bash
# Unit tests (production mint)
cargo test -p tn-reth --lib tel_precompile

# Unit tests (faucet mint)
cargo test -p tn-reth --lib tel_precompile --features faucet

# Integration tests
cargo test -p tn-reth --features test-utils --test it -- tel_precompile

# Integration tests with faucet
cargo test -p tn-reth --features "test-utils,faucet" --test it -- tel_precompile
```
