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
| `burn(uint256 amount)` | Governance only | 8,000 | Destroys tokens held by the precompile's own account and decrements `totalSupply`. The only payable selector |
| `totalSupply()` | Any caller | 2,100 | Returns the current circulating supply. Callable from `STATICCALL` |

On testnet builds compiled with the `faucet` feature, `mint` becomes an instant `mint(address recipient, uint256 amount)` with no timelock, and governance can delegate minting via `grantMintRole(address)` / `revokeMintRole(address)` / `hasMintRole(address)`. Mainnet binaries are never built with this feature.

Governance is the on-chain governance safe at `0x00000000000000000000000000000000000007a0`.

#### Notable Behaviors

* **`STATICCALL` write protection:** inside a static frame only the read-only selectors are served (`totalSupply`, plus `hasMintRole` on faucet builds); every state-mutating selector is refused with `static call: state mutation not permitted`.
* **Non-payable by default:** every selector except `burn` rejects nonzero call value with `call value: selector is not payable`, so stray TEL cannot be stranded at the precompile.
* **Events:** `Mint(recipient, amount, unlockTimestamp)`, `Claim(recipient, amount)`, `Burn(amount)`, plus ERC-20-style `Transfer` events from `address(0)` on claim/faucet-mint and to `address(0)` on burn. Ordinary native TEL transfers do **not** emit `Transfer` events.
* **Supply accounting:** `totalSupply` changes only via `claim` and `burn`. It does not track native balance movement such as gas fees or validator rewards.

#### Implications for Integrations

* Wallets and explorers should treat TEL as the **native asset** (like ETH), never as a token at `0x7e1`.
* Bridges and DeFi protocols that need ERC-20 semantics should integrate **WTEL**, exactly as they would WETH on Ethereum.
* Indexers tracking supply should watch the precompile's `Mint`/`Claim`/`Burn`/`Transfer` events; ordinary TEL movement is visible as native value transfers in transaction traces, not as log events.

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
| `mix_hash`                 | PoW mix digest               | Consensus output digest XOR'd with batch digest. If no batches, just the output digest     |
| `ommers_hash`              | Uncle block hash             | Digest of the consensus `Batch` executed to produce this block. `B256::ZERO` if no batches |
| `parent_beacon_block_root` | Beacon chain parent root     | Digest of the `ConsensusHeader` that committed the transactions                            |
| `extra_data`               | Arbitrary miner data         | Committee-shuffle seed (the epoch seed chain value) at epoch boundaries, empty bytes otherwise |
| `base_fee_per_gas`         | Adjusts per block (EIP-1559) | Fixed for the entire epoch, adjusts at epoch boundaries. See [basefees](basefees.md)       |
| `withdrawals`              | Beacon chain withdrawals     | Validator reward records at epoch boundaries, empty otherwise                              |

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
| Custom precompiles   | None                                 | TEL issuance at `0x7e1`, BLS verify at `0xb151` |
| Base fee destination | Burned                               | Base-fee address (governance) |
| Blob transactions    | Supported                            | Not used                      |
| Contract languages   | Solidity, Vyper, etc.                | Same                          |
| Tooling              | Hardhat, Foundry, ethers.js, viem    | Same                          |
| Chain IDs            | 1 (mainnet)                          | 487 (mainnet), 2017 (testnet) |
