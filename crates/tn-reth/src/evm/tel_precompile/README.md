# TEL Precompile — Native ERC-20 at `0x7e1`

This directory implements a **native ERC-20 precompile** for the Telcoin (TEL) token. Unlike a standard Solidity contract, the precompile operates directly on native account balances — `balanceOf(addr)` returns the same value as `addr.balance`. This makes TEL simultaneously the chain's gas token and its primary ERC-20.

The precompile is registered as a `DynPrecompile` inside reth's `PrecompilesMap` at address `0x00000000000000000000000000000000000007e1`. Any `CALL` or `STATICCALL` targeting this address is intercepted by the dispatcher in `mod.rs` and routed to the appropriate handler based on the 4-byte function selector.

## Module map

| File            | Purpose                                                                                                                       |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `mod.rs`        | Top-level dispatcher: selector → handler routing, precompile registration                                                     |
| `erc20.rs`      | Standard ERC-20: `name`, `symbol`, `decimals`, `totalSupply`, `balanceOf`, `transfer`, `approve`, `transferFrom`, `allowance` |
| `eip2612.rs`    | EIP-2612 `permit` (gasless approvals), `nonces`, `DOMAIN_SEPARATOR`                                                           |
| `burnable.rs`   | Timelocked `mint`/`claim` lifecycle + `burn` (production)                                                                     |
| `faucet.rs`     | Instant `mint` with role management (testnet, `faucet` feature)                                                               |
| `helpers.rs`    | Storage slot derivation + minimal ABI encoders                                                                                |
| `test_utils.rs` | In-memory EVM test harness (gated behind `#[cfg(test)]` / `test-utils` feature)                                               |

## Storage layout

All precompile-managed state lives under the precompile address (`0x7e1`) using Solidity-compatible mapping layouts:

| Base slot | Type                                              | Description                                                      |
| --------- | ------------------------------------------------- | ---------------------------------------------------------------- |
| 0         | `mapping(address => uint256)`                     | Pending mint amounts                                             |
| 1         | `mapping(address => uint256)`                     | Unlock timestamps (block.timestamp after which `claim` succeeds) |
| 2         | `mapping(address => mapping(address => uint256))` | ERC-20 allowances                                                |
| 3         | `mapping(address => bool)`                        | Mint roles (`faucet` feature only)                               |
| 4         | `mapping(address => uint256)`                     | EIP-2612 permit nonces                                           |
| 100       | `uint256` (plain slot)                            | Total circulating supply                                         |

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

| Function                                        | Who can call                                       |
| ----------------------------------------------- | -------------------------------------------------- |
| `mint` (production)                             | Governance only                                    |
| `mint` (faucet)                                 | Governance + dynamically granted mint-role holders |
| `claim`                                         | Anyone (after timelock)                            |
| `burn`                                          | Governance only                                    |
| `grantMintRole` / `revokeMintRole`              | Governance only (faucet feature)                   |
| `transfer`, `approve`, `transferFrom`, `permit` | Any account                                        |
| All view functions                              | Any account                                        |

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

Each handler charges a fixed gas amount upfront. The tables below compare each constant against the worst-case solidity-equivalent cost.

These costs do **not** include the base transaction cost (21,000) or calldata costs; those are charged by the EVM before the precompile runs.

### EVM gas reference (Cancun)

| Operation            | Condition             | Gas                   |
| -------------------- | --------------------- | --------------------- |
| SLOAD                | Cold                  | 2,100                 |
| SLOAD                | Warm                  | 100                   |
| SSTORE               | Cold, 0→nonzero       | 22,100                |
| SSTORE               | Cold, nonzero→nonzero | 5,000                 |
| SSTORE               | Warm, 0→nonzero       | 20,000                |
| SSTORE               | Warm, nonzero→nonzero | 2,900                 |
| SSTORE               | Warm, nonzero→0       | 2,900 (+4,800 refund) |
| Account access       | Cold                  | 2,600                 |
| Account access       | Warm                  | 100                   |
| LOG base             | —                     | 375                   |
| LOG per topic        | —                     | 375                   |
| LOG per data byte    | —                     | 8                     |
| ECRECOVER precompile | —                     | 3,000                 |

### View functions

| Function                                            | Gas   | Notes                        |
| --------------------------------------------------- | ----- | ---------------------------- |
| `name`, `symbol`, `decimals`                        | 200   | Pure return, no state access |
| `totalSupply`, `allowance`, `nonces`, `hasMintRole` | 2,100 | 1 cold SLOAD                 |
| `balanceOf`, `DOMAIN_SEPARATOR`                     | 2,600 | 1 cold account access        |

### `transfer` — 12,000 gas

| Operation             | Access | Gas       |
| --------------------- | ------ | --------- |
| load_account(from)    | cold   | 2,600     |
| load_account(to)      | cold   | 2,600     |
| LOG3 (Transfer, 32 B) | —      | 1,756     |
| **Total**             |        | **6,956** |

**Status: OK** — 1.72× headroom. Gas constant covers worst-case EVM cost with margin.

### `approve` — 22,000 gas

| Operation             | Access          | Gas        |
| --------------------- | --------------- | ---------- |
| SSTORE allowance      | cold, 0→nonzero | 22,100     |
| LOG3 (Approval, 32 B) | —               | 1,756      |
| **Total**             |                 | **23,856** |

**Status: Undercharged** — 0.92× headroom. Worst case (new approval, 0→nonzero) exceeds the gas constant by 1,856. Overwrites (nonzero→nonzero) cost only 6,756, well within budget.

### `transferFrom` — 35,000 gas

| Operation             | Access                | Gas        |
| --------------------- | --------------------- | ---------- |
| SLOAD allowance       | cold                  | 2,100      |
| SSTORE allowance      | warm, nonzero→nonzero | 2,900      |
| load_account(from)    | cold                  | 2,600      |
| load_account(to)      | cold                  | 2,600      |
| LOG3 (Transfer, 32 B) | —                     | 1,756      |
| **Total**             |                       | **11,956** |

**Status: OK** — 2.93× headroom. SSTORE is warm (same slot as prior SLOAD). Skipped entirely for infinite allowance.

### `mint` (production) — 41,000 gas

| Operation             | Access          | Gas        |
| --------------------- | --------------- | ---------- |
| SSTORE amount slot    | cold, 0→nonzero | 22,100     |
| SSTORE timestamp slot | cold, 0→nonzero | 22,100     |
| LOG2 (Mint, 64 B)     | —               | 1,637      |
| **Total**             |                 | **45,837** |

**Status: Undercharged** — 0.89× headroom. First mint (both slots 0→nonzero) exceeds gas constant by 4,837. Subsequent mints (overwriting pending amounts) cost only 11,637, well within budget.

### `claim` — 25,000 gas

| Operation               | Access                | Gas        |
| ----------------------- | --------------------- | ---------- |
| SLOAD amount slot       | cold                  | 2,100      |
| SLOAD timestamp slot    | cold                  | 2,100      |
| load_account(recipient) | cold                  | 2,600      |
| SSTORE amount slot      | warm, nonzero→0       | 2,900      |
| SSTORE timestamp slot   | warm, nonzero→0       | 2,900      |
| SLOAD totalSupply       | cold                  | 2,100      |
| SSTORE totalSupply      | warm, nonzero→nonzero | 2,900      |
| LOG2 (Claim, 32 B)      | —                     | 1,381      |
| LOG3 (Transfer, 32 B)   | —                     | 1,756      |
| **Total**               |                       | **20,737** |

**Status: Tight** — 1.21× headroom. Barely covers worst-case cost. The nonzero→0 SSTOREs produce 9,600 in refunds at transaction end, but refunds don't reduce upfront gas requirements.

### `burn` — 8,000 gas

| Operation                | Access                | Gas        |
| ------------------------ | --------------------- | ---------- |
| load_account(precompile) | cold                  | 2,600      |
| SLOAD totalSupply        | cold                  | 2,100      |
| SSTORE totalSupply       | warm, nonzero→nonzero | 2,900      |
| LOG1 (Burn, 32 B)        | —                     | 1,006      |
| LOG3 (Transfer, 32 B)    | —                     | 1,756      |
| **Total**                |                       | **10,362** |

**Status: Undercharged** — 0.77× headroom. Gas constant is 2,362 below worst-case EVM cost.

### `permit` — 72,000 gas

| Operation             | Access          | Gas        |
| --------------------- | --------------- | ---------- |
| SLOAD nonce           | cold            | 2,100      |
| ECRECOVER             | —               | 3,000      |
| SSTORE nonce          | warm, 0→nonzero | 20,000     |
| SSTORE allowance      | cold, 0→nonzero | 22,100     |
| LOG3 (Approval, 32 B) | —               | 1,756      |
| **Total**             |                 | **48,956** |

**Status: OK** — 1.47× headroom. Worst case is first permit (nonce 0→1). Subsequent permits cost only 31,856.

### `mint` (faucet) — 30,000 gas

| Operation               | Access                | Gas        |
| ----------------------- | --------------------- | ---------- |
| SLOAD mint-role slot    | cold                  | 2,100      |
| load_account(recipient) | cold                  | 2,600      |
| SLOAD totalSupply       | cold                  | 2,100      |
| SSTORE totalSupply      | warm, nonzero→nonzero | 2,900      |
| LOG2 (Mint, 64 B)       | —                     | 1,637      |
| LOG3 (Transfer, 32 B)   | —                     | 1,756      |
| **Total**               |                       | **13,093** |

**Status: OK** — 2.29× headroom. Role-check SLOAD included (non-governance caller worst case).

### `grantMintRole` (faucet) — 22,000 gas

| Operation        | Access          | Gas        |
| ---------------- | --------------- | ---------- |
| SSTORE role slot | cold, 0→nonzero | 22,100     |
| **Total**        |                 | **22,100** |

**Status: Undercharged** — 1.00× headroom. Exceeds gas constant by 100 in worst case (new grant). Re-grants (nonzero→nonzero) cost only 5,000.

### `revokeMintRole` (faucet) — 22,000 gas

| Operation        | Access          | Gas       |
| ---------------- | --------------- | --------- |
| SSTORE role slot | cold, nonzero→0 | 5,000     |
| **Total**        |                 | **5,000** |

**Status: OK** — 4.40× headroom. The nonzero→0 SSTORE produces a 4,800 refund at transaction end.

### Status key

- **Undercharged** (headroom < 1.0×): The gas constant is lower than the worst-case EVM cost. The precompile charges less gas than an equivalent Solidity contract would consume. The operation is subsidized relative to EVM costs.
- **Tight** (headroom 1.0×–1.2×): The gas constant barely covers the worst-case EVM cost. No margin for implementation overhead or future gas schedule changes.
- **OK** (headroom > 1.2×): Sufficient margin above worst-case EVM cost.

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
