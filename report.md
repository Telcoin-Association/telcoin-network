# Code Review: Native TEL ERC-20 Precompile
Date: 2026-03-18
Scope: Branch `native-tel-erc20-precompile` — all commits since `main` (10 commits, +5523 lines across 23 files)

## Summary
Well-structured precompile with good module separation and solid test coverage. Of 10 initial findings, 6 were confirmed or partially valid after subagent evaluation. Key issues: documentation contradicts implementation on `claim` access control, `balance_incr` lacks overflow checking, `transferFrom` emits a non-standard `Approval` event, and two gas costs are tight.

| # | Title | Severity | Category | Status |
|---|-------|----------|----------|--------|
| 4 | `claim` is governance-only despite docs saying "permissionless" | High | Bugs | Confirmed |
| 3 | `balance_incr` unchecked overflow on native balance | Medium | State & Funds | Confirmed |
| 7 | Transfer to `TELCOIN_PRECOMPILE_ADDRESS` locks tokens for non-governance users | Medium | State & Funds | Confirmed |
| 9 | `handle_claim` and `handle_burn` gas costs may be too low | Medium | Optimization | Partially Valid |
| 10 | `transferFrom` emits non-standard `Approval` event | Medium | Bugs | Confirmed |
| 8 | `permit` does not validate `owner != address(0)` | Low | Security | Partially Valid |

## Findings

### 4. `claim` is governance-only despite docs saying "permissionless"
- **Severity**: High
- **Category**: Bugs
- **Location**: `crates/tn-reth/src/evm/tel_precompile/burnable.rs:38,196-209`
- **Description**: Three places document `claim` as permissionless:
  1. `burnable.rs:6` — "anyone can finalize the mint after the timelock expires"
  2. `burnable.rs:38` — sol! doc: "Permissionless"
  3. `mod.rs:44` — "`claim` is also permissionless once the timelock expires"

  But `handle_claim` at line 207 checks `has_governance_role(caller)` and rejects non-governance callers. This is a direct contradiction between docs and implementation.
- **Impact**: Either users will be blocked from claiming (if the intent was permissionless), or external integrators will misunderstand the access model (if governance-only is correct). Needs a decision: fix the code or fix the docs.
- **Analysis**: The implementation itself is defensible — governance-only claiming is safer. But the documentation is wrong. No test exercises `claim` from a non-governance caller expecting success.
- **Proposed Fix (if governance-only is intended)**: Update all three doc locations to say "Governance-only":
  ```rust
  // burnable.rs sol! doc
  /// Finalize a pending mint after the timelock expires. Governance-only.
  function claim(address recipient) external;

  // burnable.rs module doc line 6
  // 2. **`claim(address)`** — governance finalizes the mint after the timelock expires

  // mod.rs line 44 — remove claim from "Any account" list
  ```

### 3. `balance_incr` unchecked overflow on native balance
- **Severity**: Medium
- **Category**: State & Funds
- **Location**: `crates/tn-reth/src/evm/tel_precompile/helpers.rs:140`
- **Description**: `balance_incr` does `acc.info.balance += amount` without overflow checking. Both callers (`handle_claim` at `burnable.rs:241` and `handle_mint_faucet` at `faucet.rs:88`) call `balance_incr` **before** the `totalSupply.checked_add()` check, so the totalSupply guard does not protect the balance mutation.
- **Impact**: U256 overflow is practically impossible (2^256 is astronomically large), but the code is inconsistent — `totalSupply` gets overflow protection while the individual balance does not. This violates defense-in-depth.
- **Analysis**: In `handle_claim` (burnable.rs), line 241 calls `balance_incr` and only then lines 252-264 do `totalSupply.checked_add`. Same pattern in `faucet.rs:88` vs `faucet.rs:91-103`. If `balance_incr` overflowed, the totalSupply check would still pass (they're independent values).
- **Proposed Fix**:
  ```rust
  pub(super) fn balance_incr(
      internals: &mut EvmInternals<'_>,
      addr: Address,
      amount: U256,
  ) -> Result<(), PrecompileError> {
      let acc = internals
          .load_account(addr)
          .map_err(|e| PrecompileError::Other(format!("load_account failed: {e:?}")))?
          .data;
      acc.info.balance = acc.info.balance
          .checked_add(amount)
          .ok_or_else(|| PrecompileError::Other("balance overflow".into()))?;
      acc.mark_touch();
      Ok(())
  }
  ```

### 7. Transfer to `TELCOIN_PRECOMPILE_ADDRESS` locks tokens for non-governance users
- **Severity**: Medium
- **Category**: State & Funds
- **Location**: `crates/tn-reth/src/evm/tel_precompile/erc20.rs:158-178`
- **Description**: `handle_transfer` blocks transfers to `address(0)` but allows any account to transfer to `TELCOIN_PRECOMPILE_ADDRESS`. The comment at line 158 says "Allow transfers to this address for `burn`", confirming this is the intended burn lifecycle (transfer to precompile → governance calls burn). However, non-governance users who transfer tokens there have no way to recover them — only governance can `burn`.
- **Impact**: Accidental transfers by non-governance users permanently lock tokens. This is analogous to sending ETH to a contract without a withdraw function — a known risk but one that could be mitigated.
- **Analysis**: The integration test at `pipeline_tel_precompile_props.rs:289-304` confirms `transfer(TELCOIN_PRECOMPILE_ADDRESS, amount)` succeeds. The flow is intentional for governance but creates a foot-gun for regular users.
- **Proposed Fix**: Either restrict transfers to the precompile to governance-only:
  ```rust
  if to == TELCOIN_PRECOMPILE_ADDRESS && !has_governance_role(caller) {
      return Err(PrecompileError::Other(
          "transfer: only governance can transfer to precompile".into()
      ));
  }
  ```
  Or document the risk prominently and accept it as a design decision (similar to ERC-20 tokens sent to contract addresses).

### 9. `handle_claim` and `handle_burn` gas costs may be too low
- **Severity**: Medium
- **Category**: Optimization
- **Location**: `burnable.rs:203` (claim: 25,000), `burnable.rs:300` (burn: 8,000)
- **Description**: Gas cost analysis against EVM baselines:

  | Handler | Gas | Operations | Min EVM cost | Headroom |
  |---------|-----|------------|-------------|----------|
  | `claim` | 25,000 | 4 SLOAD + 3 SSTORE + 1 load_account + 2 logs | ~24,150 | 1.03x |
  | `burn` | 8,000 | 1 load_account + 1 SLOAD + 1 SSTORE + 2 logs | ~6,350 | 1.26x |

  Other handlers have 3-6x headroom. These two are notably tight, especially `claim` which has virtually no margin.
- **Impact**: Under-charged gas could enable denial-of-service if the actual computational cost exceeds the charged amount. The tight margin on `claim` means cold storage access patterns could push actual cost above the charged amount.
- **Proposed Fix**: Increase to provide consistent headroom:
  - `handle_claim`: 25,000 → 32,000
  - `handle_burn`: 8,000 → 12,000

### 10. `transferFrom` emits non-standard `Approval` event
- **Severity**: Medium
- **Category**: Bugs
- **Location**: `crates/tn-reth/src/evm/tel_precompile/erc20.rs:299-306`
- **Description**: `handle_transfer_from` emits an `Approval` event with the updated allowance after spending. Standard OpenZeppelin ERC-20 does **not** emit `Approval` in `transferFrom` — it only emits `Transfer`. The `Approval` event should only be emitted by `approve()` and `permit()`.
- **Impact**: Off-chain indexers and wallets that track allowance changes via `Approval` events will see unexpected events from `transferFrom` calls. This breaks the standard ERC-20 event contract.
- **Analysis**: Looking at OpenZeppelin's ERC-20 `_spendAllowance` internal function — it decrements allowance but does NOT emit `Approval`. Only `_approve` emits it. The precompile's behavior is non-standard.
- **Proposed Fix**: Remove the `Approval` event emission from `handle_transfer_from` (lines 299-306):
  ```rust
  // Remove this block:
  // let approval_log = reth_revm::primitives::Log::new(
  //     TELCOIN_PRECOMPILE_ADDRESS,
  //     vec![Approval::SIGNATURE_HASH, from.into_word(), caller.into_word()],
  //     new_allowance.to_be_bytes_vec().into(),
  // )
  // ...
  ```

### 8. `permit` does not validate `owner != address(0)`
- **Severity**: Low
- **Category**: Security
- **Location**: `crates/tn-reth/src/evm/tel_precompile/eip2612.rs:117`
- **Description**: The permit handler validates `spender != Address::ZERO` (line 119) but does not check `owner != Address::ZERO`. While `recover_address_from_prehash` will never return `Address::ZERO` for a valid signature (making this cryptographically infeasible to exploit), it's inconsistent with the spender check and deviates from OpenZeppelin's implementation.
- **Impact**: Practically zero risk. Defense-in-depth measure only.
- **Proposed Fix**:
  ```rust
  let owner = Address::from_slice(&calldata[12..32]);
  if owner == Address::ZERO {
      return Err(PrecompileError::Other("permit: owner cannot be address(0)".into()));
  }
  ```

## False Positives (removed from findings)

| # | Title | Reason |
|---|-------|--------|
| 1 | `totalSupply` not updated on transfers | Standard ERC-20 behavior — transfers don't change supply. Correct. |
| 2 | `burn` relies on precompile balance | Intentional design — mint→claim→transfer→burn lifecycle is correct, just underdocumented. |
| 5 | `DOMAIN_SEPARATOR` encoding | `bytes32` ABI encoding is identical to raw 32 bytes. Technically correct, just stylistically inconsistent. |
| 6 | Address `0x7e1` collision risk | Current Ethereum precompiles are 0x01-0x0a. Custom chain can manage independently. Non-issue. |
