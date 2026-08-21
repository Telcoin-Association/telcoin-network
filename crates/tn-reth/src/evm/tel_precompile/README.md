# TEL Precompile — Native token issuance at `0x7e1`

This directory implements a **native token-issuance precompile** for the Telcoin (TEL) token. The precompile owns the on-chain mint/claim/burn lifecycle and exposes a single read-only view (`totalSupply`). It does **not** expose an ERC-20 transfer/approve/permit surface — those flows live in user-space contracts and rely on native value transfers, which are equivalent to ERC-20 transfers because TEL balances are native account balances.

The precompile is registered as a `DynPrecompile` inside reth's `PrecompilesMap` at address `0x00000000000000000000000000000000000007e1`. Any `CALL` or `STATICCALL` targeting this address is intercepted by the dispatcher in `mod.rs`, which routes on the 4-byte function selector. Routing is not unconditional for a `STATICCALL`: only the read-only selectors are served in a static frame — `totalSupply`, plus `hasMintRole` under the `faucet` feature — and every state-mutating selector is refused before dispatch. See "`STATICCALL` write protection" under Security considerations.

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
| Any selector inside a `STATICCALL` | Read-only only — `totalSupply` (plus `hasMintRole` under `faucet`); mutating selectors refused regardless of caller |

Governance is identified by `GOVERNANCE_SAFE_ADDRESS` from `tn-config`.

## Security considerations

### No ERC-20 / EIP-2612 surface on the precompile

The precompile previously exposed `transfer`, `approve`, `transferFrom`, `permit`, `nonces`, `allowance`, `name`, `symbol`, `decimals`, `balanceOf`, and `DOMAIN_SEPARATOR`. That surface has been removed.

The rationale is that none of those selectors needed to live at the protocol level:

- TEL **is** the native gas token. Moving TEL between accounts is `CALL <addr> <value>` — a native value transfer that updates `account.balance` directly. A precompile-level `transfer` / `transferFrom` was redundant with this primitive.
- Allowances and nonces are user-space concerns. Any contract that wants ERC-20-style approvals or EIP-2612 permits can layer them on top of native value transfers without the protocol managing the underlying maps.
- The remaining selectors (`mint` / `claim` / `burn` / `totalSupply`, plus `grantMintRole` / `revokeMintRole` / `hasMintRole` under the `faucet` feature) are the only ones that genuinely require protocol-level state — issuance authority and the supply counter cannot live in user space.

Shrinking the surface to issuance-only is the actual reason for the deletion; the resulting protocol is simpler and exposes less authority than a full ERC-20 implementation would.

### `DELEGATECALL` semantics under revm

For completeness: a contract that `DELEGATECALL`s into `0x7e1` runs the precompile's logic, but every `SSTORE` the precompile performs targets the literal address argument it passes — `TELCOIN_PRECOMPILE_ADDRESS` — not the calling contract's storage. The precompile dispatcher routes through `EvmInternals::sstore(TELCOIN_PRECOMPILE_ADDRESS, …)`, and revm's journaled state writes to the address argument verbatim with no `DELEGATECALL`-aware rewrite. A regression test in `crates/tn-reth/tests/it/tel_precompile_props.rs` (`test_delegatecall_writes_target_precompile_storage`) pins this behaviour against future revm upgrades.

### `STATICCALL` write protection

Registering `0x7e1` as a `DynPrecompile` short-circuits bytecode execution: revm's handler runs the precompile before it loads any bytecode for the target, so the `require_non_staticcall!` check that `SSTORE` and `LOG` expand through never runs for calls routed into the dispatcher, and the journal beneath it carries no static-context flag that would catch the writes instead. Write protection for a precompile is the precompile's own responsibility.

The dispatcher therefore classifies selectors itself, and it classifies by *read-only* rather than by *mutating*. `totalSupply` — plus `hasMintRole` under the `faucet` feature — stay callable inside a `STATICCALL` frame; every other selector is refused with `static call: state mutation not permitted` before its handler is reached. Default-deny is the point: a selector added to the dispatcher later is guarded unless someone explicitly names it read-only, so the direction of a future mistake is a refused read rather than an unguarded write. One consequence is that an unrecognised selector inside a static frame reports that same rejection rather than `Unknown function selector`; both revert the frame.

This matters because staticcall-into-precompile is a live pattern in this codebase — `ConsensusRegistry` reaches the sibling BLS precompile that way — and because without the check an authorized caller's `STATICCALL` into `mint` or `grantMintRole` was accepted and wrote storage.

Four regression tests pin both directions, reaching the precompile through the `STATICCALL` relay in `crates/tn-reth/tests/it/precompile_relays.rs`: `test_staticcall_cannot_mutate_precompile_state` and `test_staticcall_still_serves_read_only_selectors` (mainnet, in `tel_precompile_props.rs`), and `test_staticcall_cannot_grant_mint_role` and `test_staticcall_still_serves_has_mint_role` (`faucet`, in `tel_precompile_faucet_props.rs`).

### Rejection semantics: halt, not revert

Every rejection the precompile issues — the `STATICCALL` refusal above, the `call value: selector is not payable` refusal, an unrecognised selector, and each handler's own `unauthorized`, calldata-length, and arithmetic errors — is a `PrecompileError`. revm maps that to `InstructionResult::PrecompileError`, which is a **halt**, not a revert, and a halt consumes every unit of gas the frame was given: unspent gas is returned only for results that are `is_ok_or_revert()`, and a precompile error is neither.

So a rejected call is not a cheap failure:

- A **sub-call** into `0x7e1` loses the entire 63/64 of the caller's gas that was forwarded to it, leaving the calling frame the 1/64 it withheld.
- A **top-level transaction** is charged its full `gas_limit` and reported as `Halt`, not `Revert`.

The `false` a rejected `CALL` pushes on the stack is therefore not usefully actionable. A contract written as "forward `msg.value` into `0x7e1`, then branch on the returned boolean" reaches the branch with 1/64 of its gas and, in practice, dies there — a pattern worth noting because a value-bearing call that used to succeed (for example `0x7e1.call{value: v}(totalSupply())`, which cost ~2,100 before the payability gate) now consumes the caller's whole budget.

This behaviour is not new with the payability gate; the `STATICCALL` refusal has always had it, and the gates match each other on purpose. Consuming the limit is the EVM's ordinary price for an invalid operation, and reaching either gate means the caller violated a documented rule. Callers should satisfy those rules up front — do not attach value to anything but `burn`, do not reach mutating selectors from a static frame — rather than expecting to detect the rejection and continue.

### Timelock bypass (`faucet` feature)

The `faucet` feature **removes the 7-day timelock** on minting. A mainnet binary must never be compiled with this feature enabled. The feature is set at compile time — there is no runtime toggle.

### Double-claim prevention

After `claim` succeeds, both the amount and timestamp storage slots are zeroed, preventing re-entry into the same pending mint.

### Native balance equivalence

Token holdings are native account balances, so any direct value transfer (e.g., `CALL` with value) changes a holder's TEL balance without going through the precompile. Off-chain indexers that track issuance/destruction must watch the precompile's `Mint`, `Claim`, `Burn`, and `Transfer(0x0,…)`/`Transfer(…,0x0)` events; ordinary user-to-user TEL movement is observable as native value transfers in transaction traces.

One movement into the precompile does have an event: `burn` is payable, and the wei it receives arrives through the EVM's own transfer, which emits nothing. `handle_burn` therefore emits `Transfer(caller, 0x7e1, msg.value)` before the burn events whenever the call carries value, so an indexer reconstructing TEL as an ERC-20 from these logs never sees the pool pay out wei it was not seen receiving. The mirror reports the attached value, which may exceed the burned amount — the excess stays in the pool and is destroyed by a later `burn`.

### Total supply accounting

`totalSupply` is only updated by `claim` (increment) and `burn` (decrement). It does **not** account for native balance changes outside the precompile (e.g., gas fees, coinbase rewards). The genesis value must be set correctly at chain initialization.

## Gas costs

Each handler charges a fixed gas amount upfront. The tables below compare each constant against the worst-case Solidity-equivalent cost.

These costs do **not** include the base transaction cost (21,000) or calldata costs; those are charged by the EVM before the precompile runs.

Native-balance mutations (`balance_incr` / `balance_decr` in `claim`, `burn`, and the faucet `mint`) are priced as account **accesses** only, never as writes — balances are not precompile storage, and the account is already touched by the call. Every headroom figure below rests on that assumption.

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

**Status: Tight** — 1.00× headroom. Exactly covers the single cold SLOAD. `hasMintRole` is only compiled with the `faucet` feature; a query for the governance address short-circuits before the read and so overpays by the full 2,100.

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

| Operation                     | Access                | Gas        |
| ----------------------------- | --------------------- | ---------- |
| load_account(precompile)      | cold                  | 2,600      |
| SLOAD totalSupply             | cold                  | 2,100      |
| SSTORE totalSupply            | warm, nonzero→nonzero | 2,900      |
| LOG3 (inbound Transfer, 32 B) | value-funded only     | 1,756      |
| LOG1 (Burn, 32 B)             | —                     | 1,006      |
| LOG3 (Transfer, 32 B)         | —                     | 1,756      |
| **Total**                     |                       | **12,118** |

**Status: Undercharged** — 0.66× headroom. Gas constant is 4,118 below worst-case EVM cost. A
zero-value burn skips the inbound `Transfer` and costs 10,362 (0.77× headroom). Burning is
governance-only, so the subsidy is not reachable by untrusted callers.

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

**Status: Undercharged** — 0.995× headroom. Exceeds gas constant by 100 in worst case (new grant). Re-grants (nonzero→nonzero) cost only 5,000.

### `revokeMintRole` (faucet) — 22,000 gas

| Operation        | Access          | Gas       |
| ---------------- | --------------- | --------- |
| SSTORE role slot | cold, nonzero→0 | 5,000     |
| **Total**        |                 | **5,000** |

**Status: OK** — 4.40× headroom. The nonzero→0 SSTORE produces a 4,800 refund at transaction end.

### Status key

- **Undercharged** (headroom < 1.0×): The gas constant is lower than the worst-case EVM cost. The precompile charges less gas than an equivalent Solidity contract would consume. The operation is subsidized relative to EVM costs.
- **Tight** (headroom 1.0×–1.25×): The gas constant barely covers the worst-case EVM cost. No margin for implementation overhead or future gas schedule changes.
- **OK** (headroom > 1.25×): Sufficient margin above worst-case EVM cost.

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
