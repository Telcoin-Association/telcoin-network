# EVM Compatibility

### Overview

Telcoin Network runs a **standard Ethereum Virtual Machine**. There are no custom opcodes, no modified gas costs, and no disabled instructions. Contracts written in Solidity or Vyper deploy and execute identically to Ethereum. Standard tooling, including Hardhat, Foundry, ethers.js, viem, etc., work without modification.

TN supports all standard transaction types:

* Legacy transactions
* EIP-2930 (access list)
* EIP-1559 (type 2)

All Ethereum hardfork rules through Prague/Pectra are active.

The sections below cover the areas where TN diverges from mainnet Ethereum behavior.

### Custom Precompiles

TN registers two additional precompiles beyond the standard Ethereum set: a TEL token-issuance precompile at `0x7e1` and a BLS12-381 signature verifier at `0xb151`. The standard precompiles (`0x01`-`0x0a` etc.) are unchanged.

Both custom addresses hold a single `0xfe` (`INVALID`) byte of code in genesis, so `eth_getCode` returns `0xfe` for them, not `0x`. Tooling that infers "not a contract" from empty code will be wrong about these two addresses. The nonzero code is there for two reasons: it keeps the accounts non-empty, which exempts them from EIP-158 state clearing (an account holding only storage counts as empty and would be deleted, wiping the TEL supply counter), and it makes any call that bypasses precompile dispatch fail on the invalid instruction instead of succeeding against an EOA.

### TEL Precompile: Token Issuance

The **TEL precompile** at address `0x00000000000000000000000000000000000007e1` owns the native token's supply lifecycle: minting, claiming, and burning, plus a `totalSupply()` view.

> [!WARNING]
> The precompile is **not** an ERC-20 contract. An earlier design exposed `transfer`, `approve`, `transferFrom`, `permit`, `balanceOf`, and the other ERC-20/EIP-2612 selectors at `0x7e1`; that surface has been removed. Calling any of them now reverts with `Unknown function selector`. Do not configure `0x7e1` as a token contract in wallets, bridges, or indexers.

#### What This Means

TEL **is** the chain's native asset. Balances are native account balances, and moving TEL between accounts is an ordinary value transfer (`CALL` with value) — exactly like moving ETH on Ethereum, and equivalent to what an ERC-20 `transfer` would have done. Allowances, permits, and other ERC-20 semantics are user-space concerns: protocols that need an ERC-20 representation of TEL wrap it, just as ETH is wrapped into WETH. A canonical wrapper, **WTEL**, is deployed at `0x239c9fa0a4bfb9b71304e20c094738debfd7e2b0`.

Only issuance authority and the supply counter require protocol-level state, so those are all the precompile exposes.

#### Interface

| Function | Access | Gas | Behavior |
| -------- | ------ | --- | -------- |
| `mint(uint256 amount)` | Governance only | 41,000 | Creates a pending mint under a **7-day timelock**. A second `mint` overwrites the pending amount (minting 0 cancels) |
| `claim(address recipient)` | Governance only | 25,000 | After the timelock expires: credits `recipient`'s native balance, increments `totalSupply`, clears the pending slots |
| `burn(uint256 amount)` | Governance only | 8,000 | Destroys `amount` from the precompile's own balance and decrements `totalSupply`. The only payable selector: attached value funds the pool it draws from |
| `totalSupply()` | Any caller | 2,100 | Returns the current circulating supply. Callable from `STATICCALL` |

`burn` is the chain's only payable inlet at `0x7e1`, and like every other issuance selector it is governance-gated. The EVM credits `msg.value` to the precompile account before the handler runs, so attaching value tops up the pool the burn destroys from. Calling `burn(amount)` with `msg.value == amount` funds and burns in a single transaction. Any excess stays in the precompile's balance for a later burn: it is not refunded, and no selector ever pays balance back out. The genesis account starts at zero balance, a plain value send with empty calldata is refused, and every other selector rejects nonzero value, so a governance `burn` is the only way an ordinary call can add TEL to the pool.

On testnet builds compiled with the `faucet` feature, `mint` becomes an instant `mint(address recipient, uint256 amount)` with no timelock, and governance can delegate minting via `grantMintRole(address)` / `revokeMintRole(address)` / `hasMintRole(address)`. Mainnet binaries are never built with this feature.

Governance is the on-chain governance safe at `0x00000000000000000000000000000000000007a0`.

#### Notable Behaviors

* **Direct calls only:** `DELEGATECALL` and `CALLCODE` into `0x7e1` still reach the precompile, because revm looks a precompile up by the frame's code address alone. The dispatcher refuses them with `telcoin precompile: only a direct call to 0x7e1 is permitted`. Under `DELEGATECALL` the caller identity the access gates would check is inherited from the parent frame, while the handlers still read and write the canonical `0x7e1` ledger, so authorizing such a frame would mean trusting a spoofable address. The check runs before the calldata length check and before selector dispatch, so read-only selectors are refused through those opcodes too. Only a plain `CALL` or `STATICCALL` reaches a handler.
* **`STATICCALL` write protection:** inside a static frame only the read-only selectors are served (`totalSupply`, plus `hasMintRole` on faucet builds); every state-mutating selector is refused with `static call: state mutation not permitted`.
* **Non-payable by default:** every selector except `burn` rejects nonzero call value with `call value: selector is not payable`, so stray TEL cannot be stranded at the precompile.
* **Rejections consume all gas:** every refusal above, plus every `unauthorized`, decode, or arithmetic failure inside a handler, is a **halt**, not a revert. revm returns unspent gas only for calls that succeed or revert, and a precompile error is neither. A rejected sub-call therefore loses every unit of gas it was forwarded. Under EIP-150's default forwarding that is 63/64 of the caller's remaining gas, leaving the calling frame only the 1/64 it withheld. A rejected top-level transaction is charged its full `gas_limit`. The zero such a call pushes on the stack (`false` to a Solidity caller) is not a result to branch on: the gas needed to act on it has already been spent. Probing `0x7e1` with a value-carrying call to see what it does costs the whole gas budget. Satisfy the direct-call, access, static, and payability rules up front rather than planning to recover from a refusal. Neither form surfaces the reason: the call returns no data, and the halt carries no message, so a caller cannot tell from the receipt which rule it broke.
* **Events:** `Mint(recipient, amount, unlockTimestamp)`, `Claim(recipient, amount)`, `Burn(amount)`, plus ERC-20-style `Transfer` events: from `address(0)` on claim and faucet-mint, to `address(0)` on burn, and, when a burn is funded with call value, an inbound `Transfer(caller, 0x7e1, msg.value)` that mirrors the value transfer the EVM performs silently. That inbound log is the one `Transfer` where neither leg is `address(0)`, and its data field carries the attached value, not the burned amount; the two need not match. A value-funded burn emits three logs in this order: `Transfer(caller -> 0x7e1, msg.value)`, `Burn(amount)`, `Transfer(0x7e1 -> address(0), amount)`. Ordinary native TEL transfers do **not** emit `Transfer` events.
* **Supply accounting:** `totalSupply` changes only via `claim` and `burn`. It does not track native balance movement such as gas fees or validator rewards.

#### Implications for Integrations

* Wallets and explorers should treat TEL as the **native asset** (like ETH), never as a token at `0x7e1`.
* Bridges and DeFi protocols that need ERC-20 semantics should integrate **WTEL**, exactly as they would WETH on Ethereum.
* Indexers tracking supply should watch the precompile's `Mint`/`Claim`/`Burn`/`Transfer` events; ordinary TEL movement is visible as native value transfers in transaction traces, not as log events.
* Indexers that reconstruct TEL as an ERC-20 balance sheet from those `Transfer` logs must credit the inbound `Transfer(caller, 0x7e1, msg.value)` on a value-funded burn. It exists for exactly this audience: skip it and the precompile's balance drifts negative, because tokens leave a pool they were never seen entering.
* Even with that log credited, the `Transfer` stream is a complete account of the precompile's balance only across paths that run precompile code. `SELFDESTRUCT` naming `0x7e1` as its beneficiary still transfers the balance under EIP-6780 — only the account deletion was removed — and does so with no log and no precompile frame, so the precompile cannot mirror it. Reconcile against `0x7e1`'s native balance rather than treating the log stream as closed.

### BLS Precompile: Signature Verification

The **BLS precompile** at address `0x000000000000000000000000000000000000b151` verifies BLS12-381 signatures using the same `blst` (`min_sig`) implementation the consensus layer uses to produce them, so on-chain verification can never diverge from what validators sign. Its ABI matches `IBlsG1.sol` in tn-contracts.

#### Interface

```
blsVerify(bytes signature, bytes pubkey, bytes message) → bool
```

* `signature` — 48-byte **compressed G1** point.
* `pubkey` — 96-byte **compressed G2** point.
* `message` — raw bytes, at most 4,096 bytes; hash-to-curve with the protocol's domain-separation tag happens inside the verifier.

These are the protocol's own compressed encodings — the identical bytes validators submit as proof-of-possession signatures to `stake`/`delegateStake`. Only exact 48/96-byte compressed encodings are accepted; uncompressed points are rejected.

#### Behavior

* Returns ABI-encoded `true`/`false`. Malformed points, wrong lengths, and failed verifications all return `false` — the precompile never reverts on bad cryptographic input, matching `BlsG1.blsVerify`'s boolean contract.
* Callable from `STATICCALL` (it is a pure view): `ConsensusRegistry` reaches it that way to verify validator proof-of-possession on staking.
* **Gas:** 150,000 base plus 12 per 32-byte word of the message (rounded up).

### Fee Distribution

TN modifies where transaction fees are sent compared to Ethereum.

| Fee Component      | Ethereum          | Telcoin Network                          |
| ------------------ | ----------------- | ---------------------------------------- |
| Priority fee (tip) | Block proposer    | Batch producer (validator)               |
| Base fee           | Burned (EIP-1559) | Credited to the base-fee address (for governance processing) |
| Gas limit penalty  | N/A               | Credited to the base-fee address (for governance processing) |

The base fee is **not removed from circulation** on TN. It is credited to the chain's base-fee address (the governance safe by default) for protocol use. See [basefees](basefees.md) for details on how the base fee adjusts per epoch, and [penalties](gas-limit-penalty.md) for the quadratic gas limit penalty mechanism.

### Chain IDs

| Network | Chain ID |
| ------- | -------- |
| Mainnet | `487`    |
| Testnet | `2017`   |

### Blob Transactions (EIP-4844)

TN does not support blob transactions yet. While the EIP-4844 transaction type is recognized, blob gas pricing is effectively disabled. Applications should use standard EIP-1559 transactions.

### Block Header Differences

TN repurposes several Ethereum block header fields to carry consensus-layer metadata. These differences do not affect contract execution but are relevant for indexing tools, block explorers, or services that read block headers directly.

#### Repurposed Fields

| Field                      | Ethereum Meaning             | TN Meaning                                                                                 |
| -------------------------- | ---------------------------- | ------------------------------------------------------------------------------------------ |
| `nonce`                    | PoW mining nonce             | Epoch and consensus round, packed as `(epoch << 32) \| round`                              |
| `difficulty`               | Network difficulty           | Worker ID and batch index, packed as `(batch_index << 16) \| worker_id`                    |
| `mix_hash`                 | PoW mix digest               | `keccak256("TN_PREVRANDAO_V1" \|\| epoch seed chain value as of this commit \|\| consensus block number \|\| batch index)`, the latter two little-endian `u64`. Before the PREVRANDAO fork epoch: consensus output digest XOR'd with batch digest (just the output digest if no batches) |
| `ommers_hash`              | Uncle block hash             | Digest of the consensus `Batch` executed to produce this block. `B256::ZERO` if no batches |
| `parent_beacon_block_root` | Beacon chain parent root     | Digest of the `ConsensusHeader` that committed the transactions                            |
| `extra_data`               | Arbitrary miner data         | Committee-shuffle seed (the epoch seed chain value as of the closing commit) at epoch boundaries, empty bytes otherwise |
| `base_fee_per_gas`         | Adjusts per block (EIP-1559) | Fixed for the entire epoch, adjusts at epoch boundaries. See [basefees](basefees.md)       |
| `withdrawals`              | Beacon chain withdrawals     | Validator reward records at epoch boundaries, empty otherwise                              |

The `mix_hash` derivation changed at a hard fork. Blocks from epochs before the PREVRANDAO fork epoch use
the legacy `output_digest ^ batch_digest` form; blocks from that epoch onward use the seed-chain
derivation. Indexers that reproduce `mix_hash` from consensus data must branch on the block's epoch (the
upper 32 bits of `nonce`).

The seed chain value in that hash is the block's own commit-level value, and the chain advances at every
commit. It matches the `extra_data` of the epoch's closing block only for that closing block itself; mid-epoch
values appear on no executed header. To reproduce `mix_hash` for any other block, fold the epoch's seed chain
forward from consensus data — the epoch root, then each commit's leader round and seed signature — up to the
commit that produced the block.

#### Fixed / Unused Fields

| Field             | Value                 | Notes                              |
| ----------------- | --------------------- | ---------------------------------- |
| `requests_hash`   | `EMPTY_REQUESTS_HASH` | EIP-7685 deposit requests not used |
| `excess_blob_gas` | `0`                   | Blob transactions not used         |

#### Decoding the `difficulty` Field

The `difficulty` field packs two values:

```
difficulty = (batch_index << 16) | worker_id
```

* **Bits 0-15** (lower 16 bits): `worker_id`
* **Bits 16+** (upper bits): `batch_index`

To extract them:

```
worker_id  = difficulty & 0xFFFF
batch_index = difficulty >> 16
```

#### Decoding the `nonce` Field

The `nonce` field packs epoch and round:

```
nonce = (epoch << 32) | round
```

* **Upper 32 bits**: epoch number
* **Lower 32 bits**: consensus round number

### Summary

| Property             | Ethereum                             | Telcoin Network               |
| -------------------- | ------------------------------------ | ----------------------------- |
| EVM opcodes          | Standard                             | Standard (identical)          |
| Gas costs            | Standard                             | Standard (identical)          |
| Transaction types    | Legacy, EIP-2930, EIP-1559, EIP-4844 | Legacy, EIP-2930, EIP-1559    |
| Native asset ERC-20  | Requires WETH wrapper                | Requires WTEL wrapper         |
| Custom precompiles   | None                                 | TEL issuance at `0x7e1`, BLS verify at `0xb151` (both report `0xfe` code) |
| Base fee destination | Burned                               | Base-fee address (governance) |
| Blob transactions    | Supported                            | Not used                      |
| Contract languages   | Solidity, Vyper, etc.                | Same                          |
| Tooling              | Hardhat, Foundry, ethers.js, viem    | Same                          |
| Chain IDs            | 1 (mainnet)                          | 487 (mainnet), 2017 (testnet) |
