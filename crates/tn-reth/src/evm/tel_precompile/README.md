# TEL Precompile — Native token issuance at `0x7e1`

This directory implements a **native token-issuance precompile** for the Telcoin (TEL) token. The precompile owns the on-chain mint/claim/burn lifecycle and exposes a single read-only view (`totalSupply`). It does **not** expose an ERC-20 transfer/approve/permit surface — those flows live in user-space contracts and rely on native value transfers, which are equivalent to ERC-20 transfers because TEL balances are native account balances.

The precompile is registered as a `DynPrecompile` inside reth's `PrecompilesMap` at address `0x00000000000000000000000000000000000007e1`. Every call frame whose code address is `0x7e1` is intercepted by the dispatcher in `mod.rs`. revm keys that lookup on the frame's `bytecode_address` alone, so `DELEGATECALL` and `CALLCODE` frames arrive there too; the dispatcher refuses those before doing anything else and serves only a direct `CALL` or `STATICCALL`, on which it routes on the 4-byte function selector (see "Direct-call guard" under Security considerations). Routing is not unconditional for a `STATICCALL`: only the read-only selectors are served in a static frame — `totalSupply`, plus `hasMintRole` under the `faucet` feature — and every state-mutating selector is refused before dispatch. See "`STATICCALL` write protection" under Security considerations.

## Module map

| File            | Purpose                                                                                       |
| --------------- | --------------------------------------------------------------------------------------------- |
| `mod.rs`        | Top-level dispatcher: selector → handler routing, precompile registration                      |
| `burnable.rs`   | Timelocked `mint`/`claim` lifecycle, `burn`, and the `totalSupply()` view (mainnet)            |
| `faucet.rs`     | Instant `mint` with role management (testnet, `faucet` feature)                                |
| `helpers.rs`    | Storage slot derivation + balance manipulation helpers                                         |
| `test_utils.rs` | TEL-specific test helpers on top of the shared harness in `../precompile_test_utils.rs` (gated behind `#[cfg(test)]` / `test-utils` feature) |

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

burn(amount)  →  [msg.value, if attached, was already credited to precompile.balance]
                 precompile.balance -= amount  (sent to address(0))
                 totalSupply -= amount
```

- **`mint`**: Governance-only. Creates a pending mint with a 7-day timelock. A second `mint` overwrites the previous pending amount (can be used to cancel by minting 0).
- **`claim`**: Governance-only. Finalizes the pending mint after the timelock has expired.
- **`burn`**: Governance-only, and the only payable selector. Destroys tokens held by the precompile's own account. Attached `msg.value` is credited to that account by the EVM before the handler runs, so it tops up the pool the burn draws from: `msg.value == amount` funds and burns in one transaction, and any excess stays in the pool for a later `burn`. See "Native balance equivalence" for the log that mirrors the top-up.

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
| Any selector via `DELEGATECALL` / `CALLCODE` | Refused before dispatch, regardless of caller and selector; only a direct `CALL`/`STATICCALL` to `0x7e1` reaches a handler |

Governance is identified by `GOVERNANCE_SAFE_ADDRESS` from `tn-config`.

## Security considerations

### No ERC-20 / EIP-2612 surface on the precompile

The precompile previously exposed `transfer`, `approve`, `transferFrom`, `permit`, `nonces`, `allowance`, `name`, `symbol`, `decimals`, `balanceOf`, and `DOMAIN_SEPARATOR`. That surface has been removed.

The rationale is that none of those selectors needed to live at the protocol level:

- TEL **is** the native gas token. Moving TEL between accounts is `CALL <addr> <value>` — a native value transfer that updates `account.balance` directly. A precompile-level `transfer` / `transferFrom` was redundant with this primitive.
- Allowances and nonces are user-space concerns. Any contract that wants ERC-20-style approvals or EIP-2612 permits can layer them on top of native value transfers without the protocol managing the underlying maps.
- The remaining selectors (`mint` / `claim` / `burn` / `totalSupply`, plus `grantMintRole` / `revokeMintRole` / `hasMintRole` under the `faucet` feature) are the only ones that genuinely require protocol-level state — issuance authority and the supply counter cannot live in user space.

Shrinking the surface to issuance-only is the actual reason for the deletion; the resulting protocol is simpler and exposes less authority than a full ERC-20 implementation would.

### Direct-call guard (`DELEGATECALL` / `CALLCODE`)

revm dispatches a precompile by the frame's `bytecode_address` alone (`PrecompilesMap::get(&inputs.bytecode_address)`), with no check on the call scheme, so a contract that `DELEGATECALL`s or `CALLCODE`s into `0x7e1` runs the dispatcher just as a `CALL` does. Two things make that frame shape unsafe to serve:

- **The `caller` is not the contract in front of the precompile.** `DELEGATECALL` preserves the parent frame's `msg.sender`, so a contract `A` that governance chose to `CALL` presents `caller == GOVERNANCE_SAFE_ADDRESS` when it delegates into `0x7e1`, and every governance gate passes. (`CALLCODE` presents `A` itself as the caller, so it spoofs nothing on its own, but it is the same indirect frame shape and is refused for the same reason.)
- **The storage does not follow `DELEGATECALL` semantics.** Every handler passes the literal `TELCOIN_PRECOMPILE_ADDRESS` to `EvmInternals::sstore` / `sload` and the balance helpers, and revm's journaled state writes to that address verbatim with no `DELEGATECALL`-aware rewrite. An indirect frame therefore acts on the **canonical** `0x7e1` ledger, not on the delegating contract's storage as it would against ordinary bytecode.

On mainnet the combination is latent rather than live: `mint` hardcodes governance as the recipient and `claim` reverts on an empty pending slot, so a spoofed governance caller cannot direct funds to itself, and presenting a governance `caller` at all requires governance to execute the transaction (at which point a direct call to `0x7e1` is equally available to it). Under the `faucet` feature it is live: `mint(address, uint256)` credits a calldata-chosen recipient, so any contract a mint-role holder chose to `CALL` could mint to an address of its choosing. Either way the authorization gates were trusting a spoofable identity.

The dispatcher therefore refuses every frame that is not a direct call, as its first check and ahead of selector dispatch: `PrecompileInput::is_direct_call()` (`target_address == bytecode_address`) holds for a direct `CALL`/`STATICCALL`, which set both to `0x7e1`, and fails for `DELEGATECALL`/`CALLCODE`, which keep the calling contract's own address as `target_address`. Read-only selectors are refused too; an indirect frame has no legitimate use of this precompile.

Regression tests reach the precompile through the `DELEGATECALL` and `CALLCODE` relays in `crates/tn-reth/tests/it/precompile_relays.rs`: `test_delegatecall_cannot_reach_precompile`, `test_delegatecall_refuses_read_only_selectors_too` and `test_callcode_cannot_reach_precompile` (mainnet, in `tel_precompile_props.rs`), and `test_delegatecall_cannot_mint_faucet` and `test_callcode_cannot_mint_faucet` (`faucet`, in `tel_precompile_faucet_props.rs`). The `CALL` and `STATICCALL` positive controls in the static-call tests below prove the guard leaves direct calls untouched.

### `STATICCALL` write protection

Registering `0x7e1` as a `DynPrecompile` short-circuits bytecode execution: revm's handler runs the precompile before it loads any bytecode for the target, so the `require_non_staticcall!` check that `SSTORE` and `LOG` expand through never runs for calls routed into the dispatcher, and the journal beneath it carries no static-context flag that would catch the writes instead. Write protection for a precompile is the precompile's own responsibility.

The dispatcher therefore classifies selectors itself, and it classifies by *read-only* rather than by *mutating*. `totalSupply` — plus `hasMintRole` under the `faucet` feature — stay callable inside a `STATICCALL` frame; every other selector is refused with `static call: state mutation not permitted` before its handler is reached. Default-deny is the point: a selector added to the dispatcher later is guarded unless someone explicitly names it read-only, so the direction of a future mistake is a refused read rather than an unguarded write. One consequence is that an unrecognised selector inside a static frame reports that same rejection rather than `Unknown function selector`; both revert the frame.

This matters because staticcall-into-precompile is a live pattern in this codebase — `ConsensusRegistry` reaches the sibling BLS precompile that way — and because without the check an authorized caller's `STATICCALL` into `mint` or `grantMintRole` was accepted and wrote storage.

Four regression tests pin both directions, reaching the precompile through the `STATICCALL` relay in `crates/tn-reth/tests/it/precompile_relays.rs`: `test_staticcall_cannot_mutate_precompile_state` and `test_staticcall_still_serves_read_only_selectors` (mainnet, in `tel_precompile_props.rs`), and `test_staticcall_cannot_grant_mint_role` and `test_staticcall_still_serves_has_mint_role` (`faucet`, in `tel_precompile_faucet_props.rs`).

### Rejection semantics: halt, not revert

Every rejection the precompile issues — the `STATICCALL` refusal above, the `call value: selector is not payable` refusal, an unrecognised selector, and each handler's own gas-limit precheck, `unauthorized`, calldata-length, and arithmetic errors — is a `PrecompileError`. The provider that runs `0x7e1` is `alloy-evm`'s `PrecompilesMap` — `add_telcoin_precompile` registers into that map, and the factory names it as the EVM's `Precompiles` type — so `PrecompilesMap::run` (`alloy-evm`'s `precompiles.rs`) is what maps `PrecompileError::OutOfGas`, which is what the `gas_limit < GAS_COST` precheck in each handler returns, to `InstructionResult::PrecompileOOG`, and every other variant to `InstructionResult::PrecompileError`. Both are a **halt**, not a revert, and a halt consumes every unit of gas the frame was given: unspent gas is returned only for results that are `is_ok_or_revert()`, and neither halt is.

Which provider runs is load-bearing rather than a citation detail. revm's own `EthPrecompiles` makes the identical split, but it additionally stashes the error string for a call at journal depth 1, which `revm-handler`'s `post_execution.rs` promotes to `HaltReason::PrecompileErrorWithContext(message)`. `PrecompilesMap` stashes nothing, so a rejected top-level call here halts with the bare `HaltReason::PrecompileError` and surfaces no reason on the receipt at all — which is what the unit tests in `mod.rs` assert, and what makes the "the halt carries no message" claim in `docs/src/evm-compatibility.md` true.

So a rejected call is not a cheap failure:

- A **sub-call** into `0x7e1` loses the entire 63/64 of the caller's gas that was forwarded to it, leaving the calling frame the 1/64 it withheld.
- A **top-level transaction** is charged its full `gas_limit` and reported as `Halt`, not `Revert`.

The `false` a rejected `CALL` pushes on the stack is therefore not usefully actionable. A contract written as "forward `msg.value` into `0x7e1`, then branch on the returned boolean" reaches the branch with 1/64 of its gas and, in practice, dies there. The change bites hardest on a value-bearing call that used to succeed: `0x7e1.call{value: v}(totalSupply())` cost on the order of 9,000 gas before the payability gate, and now consumes the caller's whole budget.

The precompile's own 2,100 charge is the small part of that 9,000. `CALL` charges 100 for the account access (revm pre-warms every registered precompile address, so `0x7e1` is never cold) plus a flat 9,000 for attaching nonzero value. That 9,000 buys the callee a 2,300 gas stipend on top of the forwarded gas, which more than covers the 2,100, so 200 of it comes back unspent: `100 + 9,000 + 2,100 - 2,300 = 8,900` net, before the caller's own memory and calldata costs. The 25,000 empty-account surcharge never applies, because the genesis `0x7e1` account carries code (see "Genesis account and the `0xfe` code" below).

This behaviour is not new with the payability gate; the `STATICCALL` refusal has always had it, and the gates match each other on purpose. Consuming the limit is the EVM's ordinary price for an invalid operation, and reaching either gate means the caller violated a documented rule. Callers should satisfy those rules up front — do not attach value to anything but `burn`, do not reach mutating selectors from a static frame — rather than expecting to detect the rejection and continue.

### Timelock bypass (`faucet` feature)

The `faucet` feature **removes the 7-day timelock** on minting. A mainnet binary must never be compiled with this feature enabled. The feature is set at compile time — there is no runtime toggle.

### Double-claim prevention

After `claim` succeeds, both the amount and timestamp storage slots are zeroed, preventing re-entry into the same pending mint.

### Native balance equivalence

Token holdings are native account balances, so any direct value transfer (e.g., `CALL` with value) changes a holder's TEL balance without going through the precompile. Off-chain indexers that track issuance/destruction must watch the precompile's `Mint`, `Claim`, `Burn`, the `Transfer(0x0,…)`/`Transfer(…,0x0)` mint and burn legs, and the inbound `Transfer(caller, 0x7e1, msg.value)` a value-funded `burn` emits (below — neither of its legs is `0x0`); ordinary user-to-user TEL movement is observable as native value transfers in transaction traces.

One movement into the precompile does have an event: `burn` is payable, and the wei it receives arrives through the EVM's own transfer, which emits nothing. `handle_burn` therefore emits `Transfer(caller, 0x7e1, msg.value)` before the burn events whenever the call carries value, so an indexer reconstructing TEL as an ERC-20 from these logs never sees the pool pay out wei it was not seen receiving. The mirror reports the attached value, which may exceed the burned amount — the excess stays in the pool and is destroyed by a later `burn`.

That mirror makes the log stream a complete account of the pool's balance across every path that runs precompile code, which is not the same as every path. `SELFDESTRUCT` remains a logless inlet: under EIP-6780 (active from genesis, `cancunTime: 0`) the beneficiary credit still happens — only the account deletion was removed — so a contract self-destructing to `0x7e1` tops the pool up with no log and no precompile frame to observe it, and a later `burn` of that wei emits an outbound `Transfer` for wei nothing was seen sending. The precompile cannot see such a credit, so there is nothing to emit; an indexer should reconcile against the account's native balance rather than treat the log stream as closed.

### Total supply accounting

`totalSupply` is only updated by `claim` (increment) and `burn` (decrement). It does **not** account for native balance changes outside the precompile (e.g., gas fees, coinbase rewards). The genesis value must be set correctly at chain initialization.

### Genesis account and the `0xfe` code

The genesis account at `0x7e1` is `nonce: "0x0"`, `balance: "0x0"`, `code: "0xfe"` (`chain-configs/mainnet/genesis.yaml:46-49`). That code is a single `0xfe` (`INVALID`) byte, defined once as `PRECOMPILE_GENESIS_BYTECODE` in `crates/tn-reth/src/system_calls.rs:47-54`. The precompile map short-circuits before any bytecode load, so the byte never executes; it is there for what its presence does to the account:

- It keeps the account non-empty, so EIP-158 state clearing never prunes it. An account with zero nonce, zero balance, and no code is deleted at the end of any block that touches it, which would silently wipe the `totalSupply` slot at slot 100 and every pending mint. The precompile's balance legitimately drains to zero after a `burn` of the whole pool, so this is a reachable state, not a theoretical one.
- It makes a call that bypasses precompile dispatch fail rather than succeed against an EOA. With no code, `0x7e1` is an EOA and a plain call to it returns success; `INVALID` halts the frame instead. The constant's own doc also notes that reth skips calls to accounts with no bytecode, which the code field avoids.

Anything that seeds state under `0x7e1` — a faucet mint role, for example — must extend this account rather than replace it; dropping the `code` field reintroduces the pruning bug. `faucet_mint_role_slot` in `mod.rs` carries the same warning, and the in-memory harness gives its precompile account the identical shape — reading the same `PRECOMPILE_GENESIS_BYTECODE` rather than restating the byte — so tests that burn the pool empty exercise the production account shape (`TestEnv::new_with_balances` in `crates/tn-reth/src/evm/precompile_test_utils.rs`).

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

| Operation                     | Access                       | Gas       |
| ----------------------------- | ---------------------------- | --------- |
| load_account(precompile)      | warm (pre-warmed precompile) | 100       |
| SLOAD totalSupply             | cold                         | 2,100     |
| SSTORE totalSupply            | warm, nonzero→nonzero        | 2,900     |
| LOG3 (inbound Transfer, 32 B) | value-funded only            | 1,756     |
| LOG1 (Burn, 32 B)             | —                            | 1,006     |
| LOG3 (Transfer, 32 B)         | —                            | 1,756     |
| **Total**                     |                              | **9,618** |

**Status: Undercharged on a value-funded burn only** — 0.83× headroom, 1,618 below worst case. A
zero-value burn skips the inbound `Transfer`, costs 7,862, and is fully covered (1.02×). This is
the one table whose `load_account` targets the precompile's own account rather than a recipient:
revm pre-warms every registered precompile address before any frame runs, so `0x7e1` is never cold
here. Burning is governance-only, so the subsidy is not reachable by untrusted callers.

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

The shared in-memory EVM harness lives in `crates/tn-reth/src/evm/precompile_test_utils.rs`; `test_utils.rs` in this directory layers the TEL token helpers (`mint`, `get_total_supply`, `set_total_supply`, `GENESIS_SUPPLY`) onto it. Together they are the single source of truth for both unit tests (in each module's `#[cfg(test)] mod tests`) and integration tests (in `crates/tn-reth/tests/it/`).

Example: read totalSupply by calling `0x18160ddd` with no arguments.

CI and `make test` both run the suite through nextest (`.github/workflows/pr.yaml`, `Makefile:111`), so use nextest here too:

```bash
# Unit tests (mainnet)
cargo nextest run -p tn-reth --features test-utils --lib -E 'test(tel_precompile)'

# Integration tests (mainnet)
cargo nextest run -p tn-reth --features test-utils --test it -E 'test(tel_precompile)'

# Unit tests (faucet)
cargo nextest run -p tn-reth --features "test-utils,faucet" --lib -E 'test(tel_precompile)'

# Integration tests (faucet)
cargo nextest run -p tn-reth --features "test-utils,faucet" --test it -E 'test(tel_precompile)'
```

`-E 'test(tel_precompile)'` matches the full test path, so `--lib` selects the `evm::tel_precompile::*` unit tests and `--test it` selects the matching modules under `crates/tn-reth/tests/it/`. All four commands are needed because the `faucet` feature changes which tests compile rather than merely adding to them. The integration sets are disjoint — `tel_precompile_props` and `pipeline_tel_precompile_props` compile only on mainnet, `tel_precompile_faucet_props` only under `faucet` — and the unit sets overlap without either containing the other, since some mainnet unit tests do not compile under `faucet`.
