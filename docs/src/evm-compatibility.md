# EVM Compatibility

### Overview

Telcoin Network runs a **standard Ethereum Virtual Machine**. There are no custom opcodes, no modified gas costs, and no disabled instructions. Contracts written in Solidity or Vyper deploy and execute identically to Ethereum. Standard tooling, including Hardhat, Foundry, ethers.js, viem, etc., work without modification.

TN supports all standard transaction types:

* Legacy transactions
* EIP-2930 (access list)
* EIP-1559 (type 2)

All Ethereum hardfork rules through Prague/Pectra are active.

The sections below cover the areas where TN diverges from mainnet Ethereum behavior.

### TEL Precompile: Native ERC-20 Interface

The most significant difference is the **TEL precompile** at address `0x7e1`. This precompile gives the native TEL token a full ERC-20 interface, making TEL simultaneously the chain's gas token and a standard ERC-20.

#### What This Means

On Ethereum, the native asset (ETH) has no ERC-20 interface. Protocols that need an ERC-20 representation of ETH must use a wrapper contract like WETH. On TN, this is unnecessary because TEL is natively accessible as an ERC-20 at `0x7e1`.

Calling `balanceOf(address)` on the precompile returns the address's **native account balance**. A native TEL transfer (via `CALL` with value) and an ERC-20 `transfer()` call to `0x7e1` both modify the same underlying balance.

#### Supported Interfaces

**ERC-20 (full standard)**

| Function                                  | Behavior                                                  |
| ----------------------------------------- | --------------------------------------------------------- |
| `name()`                                  | Returns `"Telcoin"`                                       |
| `symbol()`                                | Returns `"TEL"`                                           |
| `decimals()`                              | Returns `18`                                              |
| `totalSupply()`                           | Returns current circulating supply                        |
| `balanceOf(address)`                      | Returns native account balance                            |
| `transfer(address, uint256)`              | Moves native balance; reverts on transfer to `address(0)` |
| `approve(address, uint256)`               | Sets allowance; overwrites any existing value             |
| `allowance(address, address)`             | Returns current allowance                                 |
| `transferFrom(address, address, uint256)` | Transfers on behalf of owner using allowance              |

**EIP-2612 (permit)**

The precompile supports gasless approvals via `permit(owner, spender, value, deadline, v, r, s)`. This allows off-chain signatures to set allowances without requiring the token holder to submit a transaction.

* `permit()` — set allowance via EIP-712 typed signature
* `nonces(address)` — monotonic nonce for replay protection
* `DOMAIN_SEPARATOR()` — EIP-712 domain hash (chain-aware)

#### Notable Behaviors

* **Infinite allowance:** If allowance is set to `type(uint256).max`, `transferFrom` does not decrement it. This saves gas on repeated transfers from the same spender.
* **No transfers to zero address:** Both `transfer()` and `transferFrom()` revert if the recipient is `address(0)`.
* **Standard events:** The precompile emits `Transfer` and `Approval` events per the ERC-20 specification. However, native TEL transfers (via `CALL` with value) do **not** emit `Transfer` events — only ERC-20 calls through `0x7e1` do.

#### Implications for Bridge Integrations

* TEL can be bridged as an ERC-20 without wrapping. Use address `0x7e1` as the token contract.
* `balanceOf` reflects the exact native balance, including gas refunds and validator rewards.
* Standard `approve` + `transferFrom` patterns work as expected.
* `permit()` is available for gasless approval flows.

### Fee Distribution

TN modifies where transaction fees are sent compared to Ethereum.

| Fee Component      | Ethereum          | Telcoin Network                          |
| ------------------ | ----------------- | ---------------------------------------- |
| Priority fee (tip) | Block proposer    | Batch producer (validator)               |
| Base fee           | Burned (EIP-1559) | Burned then minted to governance address |
| Gas limit penalty  | N/A               | Burned then minted to governance address |

The base fee is **not removed from circulation** on TN. It is collected by the governance address for protocol use. See [basefees](basefees.md) for details on how the base fee adjusts per epoch, and [penalties](gas-limit-penalty.md) for the quadratic gas limit penalty mechanism.

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
| `extra_data`               | Arbitrary miner data         | `keccak256(BLS aggregate signature)` at epoch boundaries, empty bytes otherwise            |
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
| Native asset ERC-20  | Requires WETH wrapper                | Built-in at `0x7e1`           |
| Base fee destination | Burned                               | Governance address            |
| Blob transactions    | Supported                            | Not used                      |
| Contract languages   | Solidity, Vyper, etc.                | Same                          |
| Tooling              | Hardhat, Foundry, ethers.js, viem    | Same                          |
| Chain IDs            | 1 (mainnet)                          | 487 (mainnet), 2017 (testnet) |
