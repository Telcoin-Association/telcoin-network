# TEL Precompile — Native token issuance at `0x7e1`

This directory implements a **native token-issuance precompile** for the Telcoin (TEL) token. The precompile owns the on-chain mint/claim/burn lifecycle and exposes a single read-only view (`totalSupply`). It does **not** expose an ERC-20 transfer/approve/permit surface — those flows live in user-space contracts and rely on native value transfers, which are equivalent to ERC-20 transfers because TEL balances are native account balances.

The precompile is registered as a `DynPrecompile` inside reth's `PrecompilesMap` at address `0x00000000000000000000000000000000000007e1`. Any `CALL` or `STATICCALL` targeting this address is intercepted by the dispatcher in `mod.rs` and routed to the appropriate handler based on the 4-byte function selector.

## Module map

| File            | Purpose                                                                                       |
| --------------- | --------------------------------------------------------------------------------------------- |
| `mod.rs`        | Top-level dispatcher: selector → handler routing, precompile registration                      |
| `burnable.rs`   | Timelocked `mint`/`claim` lifecycle, `burn`, and the `totalSupply()` view (mainnet)            |
| `faucet.rs`     | Instant `mint` with role management (testnet, `faucet` feature)                                |
| `helpers.rs`    | Storage slot derivation + balance manipulation helpers                                         |
| `test_utils.rs` | In-memory EVM test harness (gated behind `#[cfg(test)]` / `test-utils` feature)                |

## Storage layout

All precompile-managed state lives under the precompile address (`0x7e1`) using Solidity-compatible mapping layouts:

| Base slot | Type                          | Description                                                      |
| --------- | ----------------------------- | ---------------------------------------------------------------- |
| 0         | `mapping(address => uint256)` | Pending mint amounts                                             |
| 1         | `mapping(address => uint256)` | Unlock timestamps (block.timestamp after which `claim` succeeds) |
| 3         | `mapping(address => bool)`    | Mint roles (`faucet` feature only)                               |
| 100       | `uint256` (plain slot)        | Total circulating supply                                         |

Slot derivation follows standard Solidity rules. See `helpers.rs` for the implementations.

**Important:** Token balances are **not** in precompile storage. They are native account balances (`account.balance`).

## Token lifecycle

### mainnet (`!faucet`)

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
- **`claim`**: Governance-only. Finalizes the pending mint after the timelock has expired.
- **`burn`**: Governance-only. Destroys tokens held by the precompile's own account.

### Testnet (`faucet` feature)

```
mint(recipient, amount)  →  recipient.balance += amount
                            totalSupply += amount
```

No pending state, no timelock. Mint roles can be granted/revoked by governance.

## Access control

| Function                           | Who can call                                       |
| ---------------------------------- | -------------------------------------------------- |
| `mint` (mainnet)                   | Governance only                                    |
| `mint` (faucet)                    | Governance + dynamically granted mint-role holders |
| `claim`                            | Governance only (after timelock)                   |
| `burn`                             | Governance only                                    |
| `grantMintRole` / `revokeMintRole` | Governance only (faucet feature)                   |
| `hasMintRole` / `totalSupply`      | Any account (read-only)                            |

Governance is identified by `GOVERNANCE_SAFE_ADDRESS` from `tn-config`.

## Security considerations

### No ERC-20 / EIP-2612 surface on the precompile

The precompile previously exposed `transfer`, `approve`, `transferFrom`, `permit`, `nonces`, `allowance`, `name`, `symbol`, `decimals`, `balanceOf`, and `DOMAIN_SEPARATOR`. That surface has been removed.

The reason is that `DELEGATECALL` reads storage from the precompile's own account but executes against the calling contract's storage layout. If a malicious contract `DELEGATECALL`s into `0x7e1`, every `SSTORE` the precompile performs writes into the **caller's** storage at the same slot index. With the old ERC-20/permit surface, slots 2 (allowances) and 4 (nonces) became attacker-controlled write primitives in any contract that delegated to the precompile, putting downstream contract state at risk.

Removing the mutating selectors closes that attack class entirely. `totalSupply()` is intentionally retained because it only performs an `SLOAD`: under `DELEGATECALL` it reads slot 100 from the caller's own storage (typically zero) and cannot be used to mutate caller state. ERC-20-style transfers remain available natively — `CALL <addr> <value>` moves TEL between accounts because TEL **is** the native gas token, so user-space contracts can implement allowance-and-permit semantics on top of native value transfers without exposing them at the protocol level.

### Timelock bypass (`faucet` feature)

The `faucet` feature **removes the 7-day timelock** on minting. A mainnet binary must never be compiled with this feature enabled. The feature is set at compile time — there is no runtime toggle.

### Double-claim prevention

After `claim` succeeds, both the amount and timestamp storage slots are zeroed, preventing re-entry into the same pending mint.

### Native balance equivalence

Token holdings are native account balances, so any direct value transfer (e.g., `CALL` with value) changes a holder's TEL balance without going through the precompile. Off-chain indexers that track issuance/destruction must watch the precompile's `Mint`, `Claim`, `Burn`, and `Transfer(0x0,…)`/`Transfer(…,0x0)` events; ordinary user-to-user TEL movement is observable as native value transfers in transaction traces.

### Total supply accounting

`totalSupply` is only updated by `claim` (increment) and `burn` (decrement). It does **not** account for native balance changes outside the precompile (e.g., gas fees, coinbase rewards). The genesis value must be set correctly at chain initialization.

## Gas costs

Each handler charges a fixed gas amount upfront. The tables below compare each constant against the worst-case Solidity-equivalent cost.

These costs do **not** include the base transaction cost (21,000) or calldata costs; those are charged by the EVM before the precompile runs.

### EVM gas reference (Cancun)

| Operation         | Condition             | Gas                   |
| ----------------- | --------------------- | --------------------- |
| SLOAD             | Cold                  | 2,100                 |
| SLOAD             | Warm                  | 100                   |
| SSTORE            | Cold, 0→nonzero       | 22,100                |
| SSTORE            | Cold, nonzero→nonzero | 5,000                 |
| SSTORE            | Warm, 0→nonzero       | 20,000                |
| SSTORE            | Warm, nonzero→nonzero | 2,900                 |
| SSTORE            | Warm, nonzero→0       | 2,900 (+4,800 refund) |
| Account access    | Cold                  | 2,600                 |
| Account access    | Warm                  | 100                   |
| LOG base          | —                     | 375                   |
| LOG per topic     | —                     | 375                   |
| LOG per data byte | —                     | 8                     |

### View functions

| Function                      | Gas   | Notes        |
| ----------------------------- | ----- | ------------ |
| `totalSupply`, `hasMintRole`  | 2,100 | 1 cold SLOAD |

### `mint` (mainnet) — 41,000 gas

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

Example: read totalSupply by calling `0x18160ddd` with no arguments.

```bash
# Unit tests (mainnet mint)
cargo test -p tn-reth --lib tel_precompile

# Unit tests (faucet mint)
cargo test -p tn-reth --lib tel_precompile --features faucet

# Integration tests
cargo test -p tn-reth --features test-utils --test it -- tel_precompile

# Integration tests with faucet
cargo test -p tn-reth --features "test-utils,faucet" --test it -- tel_precompile
```
