---
description: >-
  This guide covers the process for node operators to stake and activate a
  validator node on Telcoin Network.
---

# How to Stake

### Overview

Becoming an active validator requires three on-chain transactions:

1. **Governance Approval** - Governance issues a ConsensusNFT to whitelist operator's validator address.
2. **Stake** - Operator submits uncompressed BLS public key, uncompressed proof of possession, and stake amount.
3. **Activate** - Once the node is synced, operator submits an activation transaction to become eligible for inclusion in future committees at the next epoch boundary.

### Prerequisites

#### [Hardware Requirements](how-to-stake.md#hardware-requirements)

| Component     | Minimum                        | Recommended                            |
| ------------- | ------------------------------ | -------------------------------------- |
| CPU           | 16 cores / 32 threads, x86/x64 | 32 cores, higher clock speed preferred |
| CPU Benchmark | 4000+ Single Thread (PassMark) | -                                      |
| Memory        | 128 GB DDR4/DDR5 ECC RDIMM     | 128 GB+ (highest MT/s possible)        |
| Storage       | 4 TB TLC NVMe SSD              | 7.5 TB TLC NVMe SSD                    |

#### Network Requirements

* Static public IP address
* Open UDP ports for P2P communication (default: primary and worker ports)
* Reliable, low-latency internet connection

#### Software Requirements

* `telcoin-network` binary built and installed
* Access to an Ethereum wallet with sufficient TEL for staking

### Step 1: Generate Validator Keys

Generate BLS keys and node identity information using the CLI.

```bash
telcoin-network keytool generate validator \
  --address <ADDRESS_FOR_TEL_REWARDS> \
  --external-primary-addr /ip4/<YOUR_PUBLIC_IP>/udp/<PRIMARY_PORT>/quic-v1 \
  --external-worker-addrs /ip4/<YOUR_PUBLIC_IP>/udp/<WORKER_PORT>/quic-v1 \
  --datadir /path/to/node/data
```

**Parameters:**

* `--address` - The validator's Ethereum-style address (receives block rewards). This does not need to be the same address that holds the validator NFT.
* `--external-primary-addr` - Public multiaddr for primary P2P network
* `--external-worker-addrs` - Public multiaddr(s) for worker P2P network
* `--datadir` - Directory to store node data and keys

**BLS Key Passphrase:**

Operators will be prompted to enter a passphrase to encrypt their BLS key on the local filesystem. Alternatively, set the passphrase via:

* Environment variable: `TN_BLS_PASSPHRASE`
* Stdin: Use `--bls-passphrase-source stdin`
* Interactive prompt: Use `--bls-passphrase-source ask`

This command generates:

* BLS keypair (encrypted with your passphrase)
* Network keypairs for P2P communication
* `node-info.yaml` containing your public keys and proof of possession

### Step 2: Request Governance Approval

Before staking, your validator address must be whitelisted by governance.

1. Submit the validator's ECDSA address to governance for approval
2. Governance performs off-chain verification
3. Upon approval, governance calls `mint(validatorAddress)` on the ConsensusRegistry contract

**ConsensusRegistry Address:** `0x07E17e17E17e17E17e17E17E17E17e17e17E17e1`

Operators can verify their whitelist status by checking if they own a ConsensusNFT at the registry address.

```bash
# Check NFT balance (returns 1 if whitelisted, 0 if not)
cast call 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "balanceOf(address)(uint256)" \
  <VALIDATOR_ADDRESS> \
  --rpc-url <RPC_URL>
```

### Step 3: Stake

Once whitelisted, submit the stake transaction to the ConsensusRegistry.

#### Contract Function

```solidity
function stake(
    bytes calldata blsPubkey,
    ProofOfPossession memory proofOfPossession
) external payable
```

**Parameters:**

* `blsPubkey` - The compressed BLS public key (96 bytes)
* `proofOfPossession` - Struct containing:
  * `uncompressedPubkey` - Your uncompressed BLS public key (192 bytes)
  * `uncompressedSignature` - BLS signature proving ownership of the secret key

**Value:** Send the exact stake amount required by the current stake configuration. 1M TEL for initial mainnet launch.

#### Reading Your Keys

The node's BLS public key and proof of possession are stored in `node-info.yaml` after key generation:

```yaml
bls_public_key: <compressed-96-byte-key>
proof_of_possession: <signature>
```

#### Example Using Cast

```bash
# Read current stake amount from contract
cast call 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "getCurrentStakeConfig()" \
  --rpc-url <RPC_URL>

# Submit stake transaction
cast send 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "stake(bytes,(bytes,bytes))" \
  <BLS_PUBKEY_COMPRESSED> \
  "(<UNCOMPRESSED_PUBKEY>,<UNCOMPRESSED_SIGNATURE>)" \
  --value <STAKE_AMOUNT> \
  --trezor \
  --rpc-url <RPC_URL>
```

After staking, the validator's status changes to `Staked`.

### Step 4: Start and Sync The Node

Start the validator node and wait for it to sync with the network. This step can be started before staking is complete.

```bash
telcoin-network node \
  --datadir /path/to/node/data \
  --http
```

**Passphrase options:**

* Environment: `TN_BLS_PASSPHRASE=<passphrase> telcoin-network node ...`
* Stdin: `echo "<passphrase>" | telcoin-network --bls-passphrase-source stdin node ...`
* Interactive: `telcoin-network --bls-passphrase-source ask node ...`

#### Verify Sync Status

Use the `tn_syncing` RPC method to check if the node is synced:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"tn_syncing","params":[],"id":1}' \
  http://localhost:8545
```

Wait until the node reports it is fully synced with the current epoch before proceeding.

### Step 5: Activate

Once synced, submit an activation transaction to signal readiness for committee participation. This does not guarantee immediate inclusion. It indicates the validator is ready to be included in future committee selection process starting at the next epoch boundary.

#### Contract Function

```solidity
function activate() external
```

This function must be called from the validator address (the address that owns the ConsensusNFT and submitted the stake).

#### Example Using Cast

```bash
cast send 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "activate()" \
  --trezor \
  --rpc-url <RPC_URL>
```

After activation:

1. Node status changes to `PendingActivation`
2. At the next epoch boundary, node status changes to `Active`
3. Node becomes eligible for committee selection in subsequent epochs

### Validator Lifecycle States

| Status              | Description                                                             |
| ------------------- | ----------------------------------------------------------------------- |
| `Undefined`         | Address has ConsensusNFT but has not staked                             |
| `Staked`            | Validator has staked but not yet activated                              |
| `PendingActivation` | Validator called activate(), waiting for epoch boundary                 |
| `Active`            | Validator is active and eligible for committee selection                |
| `PendingExit`       | Validator requested exit, waiting for committee obligations to complete |
| `Exited`            | Validator has exited and can unstake after one epoch                    |

### Delegated Staking (Institutional Onboarding)

For institutional or white-glove onboarding scenarios, a delegator can stake on behalf of a validator.

#### Contract Function

```solidity
function delegateStake(
    bytes calldata blsPubkey,
    ProofOfPossession memory proofOfPossession,
    address validatorAddress,
    bytes calldata validatorEIP712Signature
) external payable
```

**Additional Parameters:**

* `validatorAddress` - The address that owns the ConsensusNFT
* `validatorEIP712Signature` - EIP-712 signature from the validator authorizing the delegation

#### Obtaining the Delegation Digest

The validator must sign an EIP-712 typed data message. Get the digest to sign:

```solidity
function delegationDigest(
    bytes memory blsPubkey,
    address validatorAddress,
    address delegator
) external view returns (bytes32)
```

The validator signs this digest, and the delegator includes the signature when calling `delegateStake`.

**Note:** Governance-initiated delegations (where `msg.sender` is the contract owner) do not require the validator's EIP-712 signature.

### Exiting and Unstaking

#### Begin Exit

To leave the active validator set:

```bash
cast send 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "beginExit()" \
  --trezor \
  --rpc-url <RPC_URL>
```

Your status changes to `PendingExit`. The protocol will automatically exit you once you're no longer needed for any committee assignments (current epoch + 2 future epochs).

#### Unstake

After exiting and waiting one full epoch, reclaim your stake and rewards:

```bash
cast send 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "unstake(address)" \
  <VALIDATOR_ADDRESS> \
  --trezor \
  --rpc-url <RPC_URL>
```

This returns your initial stake plus any accrued rewards to either:

* The validator address (if self-staked)
* The delegator address (if delegated staking was used)

#### Claim Rewards Without Exiting

Active validators can claim accrued rewards without exiting:

```bash
cast send 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "claimStakeRewards(address)" \
  <VALIDATOR_ADDRESS> \
  --trezor \
  --rpc-url <RPC_URL>
```

### Checking Validator Status

Query your validator information:

```bash
cast call 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "getValidator(address)" \
  <VALIDATOR_ADDRESS> \
  --rpc-url <RPC_URL>
```

Query current epoch information:

```bash
cast call 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "getCurrentEpochInfo()" \
  --rpc-url <RPC_URL>
```

### Troubleshooting

#### Common Errors

| Error                      | Cause                               | Solution                                            |
| -------------------------- | ----------------------------------- | --------------------------------------------------- |
| `RequiresConsensusNFT`     | Address not whitelisted             | Request governance approval first                   |
| `InvalidStatus`            | Wrong validator state for operation | Check current status with `getValidator()`          |
| `InvalidStakeAmount`       | Incorrect stake value sent          | Query `getCurrentStakeConfig()` for required amount |
| `InvalidProofOfPossession` | BLS signature verification failed   | Regenerate keys and ensure correct address          |
| `DuplicateBLSPubkey`       | BLS key already registered          | Generate new keys with `--force` flag               |

#### Key Management

* **Lost passphrase:** BLS keys cannot be recovered without the passphrase. Generate new keys.
* **Regenerating keys:** Use `--force` flag to overwrite existing keys (old keys will be lost).
* **Backing up keys:** Securely backup the contents of your data directory, especially the encrypted key files.
