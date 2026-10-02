# How to Stake

### Overview

Becoming an active validator requires three on-chain transactions:

1. **Governance Approval** - Governance issues a ConsensusNFT to whitelist operator's validator address.
2. **Stake** - Operator submits the compressed BLS public key (96 bytes), the compressed proof of possession (48 bytes), and the stake amount.
3. **Activate** - Once the node is synced, operator submits an activation transaction to become eligible for inclusion in future committees at the next epoch boundary.

### Prerequisites

#### Hardware Requirements

Size the validator host from [Hardware Requirements](../getting-started/hardware-requirements.md).

#### Network Requirements

* Static public IP address
* Open UDP ports for P2P communication: the primary and worker ports you pass at key generation (49590 and 49594 by convention; the node has no default ports)
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

* `--address` - The validator's Ethereum-style address (receives block rewards). The BLS proof of possession commits to this address, so it must be the same address that governance whitelists with the ConsensusNFT.
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
* `proofOfPossession` - Struct with one field:
  * `signature` - The compressed proof of possession (48 bytes), which binds the BLS key to the validator address

**Value:** Send exactly the `stakeAmount` of the current epoch's stake version. Read the version with `getCurrentStakeVersion()`, then its config with `stakeConfig(uint8)`. On the Adiri testnet the amount is 1,000,000 TEL today.

#### Reading Your Keys

The node's BLS public key and proof of possession are stored in `node-info.yaml` after key generation:

```yaml
bls_public_key: <compressed-96-byte-key>
proof_of_possession: <signature>
```

The file stores both values in base58. `keytool export-staking-args` reads them and prints the two `stake()` arguments in hex, or the complete transaction calldata with `--calldata`. It needs only `node-info.yaml`: no BLS key, passphrase, or datadir.

#### Example Using Cast

```bash
# Read the current epoch's stake version
cast call 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "getCurrentStakeVersion()(uint8)" \
  --rpc-url <RPC_URL>

# Read that version's config; the first value is the stake amount in wei
cast call 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "stakeConfig(uint8)(uint256,uint256,uint256,uint32)" \
  <STAKE_VERSION> \
  --rpc-url <RPC_URL>

# Build the stake(bytes,(bytes)) calldata from node-info.yaml
CALLDATA=$(telcoin-network keytool export-staking-args \
  --node-info /path/to/node/data/node-info.yaml \
  --calldata)

# Submit stake transaction
cast send 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "$CALLDATA" \
  --value <STAKE_AMOUNT> \
  --trezor \
  --rpc-url <RPC_URL>
```

Do not read the amount from `getCurrentStakeConfig()`. It returns the newest config governance has written, and in an epoch where governance writes a new version, that config only takes effect at the next epoch. `stake()` checks the value against the current epoch's version.

After staking, the validator's status changes to `Staked`.

### Step 4: Start and Sync The Node

Start the validator node and wait for it to sync with the network. This step can be started before staking is complete.

```bash
telcoin-network node \
  --datadir /path/to/node/data \
  --chain adiri \
  --http
```

`--chain adiri` loads the Adiri testnet genesis built into the binary. Build the binary with the `adiri` feature: without it, the node refuses to start on the Adiri chain (chain ID 2017).

**Passphrase options:**

* Environment: `TN_BLS_PASSPHRASE=<passphrase> telcoin-network node ...`
* Stdin: `echo "<passphrase>" | telcoin-network --bls-passphrase-source stdin node ...`
* Interactive: `telcoin-network --bls-passphrase-source ask node ...`

#### Verify Sync Status

There is no dedicated syncing RPC method (`eth_syncing` always returns `false` on Telcoin Network). Compare the node's latest block number against a public RPC endpoint:

```bash
# local node
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  http://localhost:8545

# network
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
  <RPC_URL>
```

Wait until the node's block number matches the network's and keeps advancing with it before proceeding.

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
    bytes calldata validatorEIP712Signature,
    uint256 deadline
) external payable
```

**Additional Parameters:**

* `validatorAddress` - The address that owns the ConsensusNFT
* `validatorEIP712Signature` - EIP-712 signature from the validator authorizing the delegation
* `deadline` - Unix timestamp after which the signature is rejected with `DelegationExpired`

#### Obtaining the Delegation Digest

The validator must sign an EIP-712 typed data message. Get the digest to sign:

```solidity
function delegationDigest(
    bytes memory blsPubkey,
    address validatorAddress,
    address delegator,
    uint256 deadline
) external view returns (bytes32)
```

The validator signs this digest, and the delegator includes the signature and the same `deadline` when calling `delegateStake`. The digest also covers the current epoch's stake version and the validator's delegation nonce, so the signature stops working if either changes before `delegateStake` runs. A validator can revoke a signature it has given by calling `increaseNonce()`.

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

The validator address or its delegator can call `unstake`. The registry accepts it in two cases:

* the validator is `Staked` (it staked but never activated), or
* the validator is `Exited` and at least one epoch has passed since its exit epoch (`currentEpoch >= exitEpoch + 1`).

```bash
cast send 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 \
  "unstake(address,bool)" \
  <VALIDATOR_ADDRESS> \
  false \
  --trezor \
  --rpc-url <RPC_URL>
```

This returns your initial stake plus any accrued rewards to either:

* The validator address (if self-staked)
* The delegator address (if delegated staking was used)

If the recipient rejects the transfer, the amount is credited instead, and the recipient withdraws it with `claimRefund()`.

The second argument is `acceptRewardShortfall`. With `false`, the call reverts if the Issuance contract cannot cover the accrued rewards. With `true`, the stake is still returned in full, rewards are paid up to the Issuance balance, and the unpaid rewards are forfeited.

Unstaking retires the validator address for good: governance cannot issue it a ConsensusNFT again.

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
| `InvalidStakeAmount`       | Incorrect stake value sent          | Send `stakeAmount` from `stakeConfig(getCurrentStakeVersion())` |
| `InvalidProofOfPossession` | BLS signature verification failed   | The proof was usually signed for a different address. Re-sign it for the staking address with `keytool generate pop --address <ADDRESS>`, then export the staking arguments again |
| `DuplicateBLSPubkey`       | BLS key already registered          | A registered key is never released, not even after unstaking. Generate a new validator's keys in a new datadir, and never overwrite the keys of a validator that has staked |

#### Key Management

* **Lost passphrase:** BLS keys cannot be recovered without the passphrase. Before staking, generate new keys. After staking, the registered key cannot be replaced: exit and unstake from the validator address, then onboard a new validator address with new keys.
* **Regenerating keys:** Never run `keytool generate validator --force` for a validator that has staked. The registry binds the BLS key to that validator for good and has no way to replace it, so `--force` overwrites the only key that can sign for the validator. If the proof of possession names the wrong address, re-sign it with `keytool generate pop --address <ADDRESS>` instead: it keeps every key and rewrites only `execution_address` and `proof_of_possession` in `node-info.yaml`.
* **Backing up keys:** Securely backup the contents of your data directory, especially the encrypted key files.
