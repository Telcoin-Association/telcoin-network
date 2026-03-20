## Native TEL ERC-20 Precompile at `0x7e1`

### Summary

Implements a native ERC-20 precompile for the Telcoin (TEL) token, registered as a `DynPrecompile` at address `0x7e1`. Unlike a Solidity contract, this precompile operates directly on **native account balances** — `balanceOf(addr)` returns the same value as `addr.balance`, making TEL simultaneously the chain's gas token and its primary ERC-20.

### What's included

**Core ERC-20** (`erc20.rs`)
- `name`, `symbol`, `decimals`, `totalSupply`, `balanceOf`
- `transfer`, `approve`, `transferFrom`, `allowance`
- Emits standard `Transfer` and `Approval` events

**EIP-2612 Permit** (`eip2612.rs`)
- Gasless approvals via `permit(owner, spender, value, deadline, v, r, s)`
- `nonces`, `DOMAIN_SEPARATOR`
- Signature malleability protection (`s > SECP256K1N_HALF` rejected)

**Timelocked Mint/Burn lifecycle** (`burnable.rs`)
- `mint(uint256)` — governance-only, creates pending mint with 7-day timelock
- `claim(address)` — finalizes pending mint after timelock, credits native balance
- `burn(uint256)` — governance-only, destroys tokens held by the precompile account
- Second `mint` call overwrites pending (can cancel by minting 0)

**Testnet Faucet** (`faucet.rs`, behind `faucet` feature flag)
- `mint(address, uint256)` — instant mint, no timelock
- `grantMintRole` / `revokeMintRole` / `hasMintRole` — role management
- **Must never be enabled in production** — removes timelock safety

**EVM Integration** (`factory.rs`, `context.rs`)
- Precompile registered in `TNEvmFactory` via `add_telcoin_precompile()`
- Renamed `MainnetEvm` → `TelcoinEvm` for clarity

### Storage layout (under `0x7e1`)

| Slot | Type | Description |
|------|------|-------------|
| 0 | `mapping(address => uint256)` | Pending mint amounts |
| 1 | `mapping(address => uint256)` | Unlock timestamps |
| 2 | `mapping(address => mapping(address => uint256))` | ERC-20 allowances |
| 3 | `mapping(address => bool)` | Mint roles (faucet only) |
| 4 | `mapping(address => uint256)` | EIP-2612 nonces |
| 100 | `uint256` | Total circulating supply |

Slot derivation follows standard Solidity mapping rules (keccak256-based), so storage is compatible with tooling that reads Solidity storage layouts.

### Access control

| Function | Who can call |
|----------|-------------|
| `mint` (production) | Governance only |
| `mint` (faucet) | Governance + mint-role holders |
| `claim` | Anyone (after timelock) |
| `burn` | Governance only |
| `grantMintRole` / `revokeMintRole` | Governance only (faucet) |
| `transfer`, `approve`, `transferFrom`, `permit` | Any account |

Governance = `GOVERNANCE_SAFE_ADDRESS` from `tn-config`.

### Testing

~2,100 lines of tests across 4 test files + shared test infrastructure:
- **Unit tests** — per-module tests for each handler (erc20, burnable, eip2612, faucet)
- **Integration tests** — end-to-end precompile execution through the EVM
- **Pipeline integration tests** — full block execution pipeline with precompile transactions
- Test semaphore to prevent memory exhaustion from libmdbx in parallel test runs
- Both production (`!faucet`) and faucet feature paths are covered

### Files changed

- **+5,664 / -19** across 23 files
- New module: `crates/tn-reth/src/evm/tel_precompile/` (7 source files + README)
- New integration tests: `crates/tn-reth/tests/it/` (4 test files + helpers)
