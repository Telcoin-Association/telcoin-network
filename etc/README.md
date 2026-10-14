# etc files

The directory is home for developer tools, such as docker compose, Dockerfile, and executable bash scripts.

## Docker Compose

`compose.yaml` is a Docker Compose V2 file that can be brought up using `make up` and down with `make down`.

Using these commands will erase all data between up/down.

The compose spins up 4 containers to create the necessary validator information using mounted volumes.
The `setup_validator.sh` script is used to facilitate all the necessary commands.

A committee service generates genesis and distributes the `committee.yaml`, `genesis.yaml`, and `parameters.yaml` files.
Finally, validator services are launched to start the network.

RPCs can be reached at:

- validator 1: 127.0.0.1:8545
- validator 2: 127.0.0.1:8544
- validator 3: 127.0.0.1:8543
- validator 4: 127.0.0.1:8542

## Local Testnet Script

`local-testnet.sh` creates a 4-validator local network with an observer node. It generates keys inside `local-validators/` so the dev can start nodes in separate terminal windows.

### Flags

| Flag | Required | Description |
|------|----------|-------------|
| `--dev-funds ADDRESS` | Yes (first run) | Address funded with 1 billion TEL at genesis |
| `--governance ADDRESS` | No | Owner of ConsensusRegistry (can mint ConsensusNFTs). Defaults to `--dev-funds` |
| `--basefee-address ADDRESS` | No | Recipient of transaction base fees |
| `--start` | No | Start the validators after generating config |

### Quick Start

```bash
# First run: generate config and start
./etc/local-testnet.sh --start --dev-funds 0xYOUR_ADDRESS

# Subsequent runs (config already exists): just start
./etc/local-testnet.sh --start
```

With a separate governance address:

```bash
./etc/local-testnet.sh --start \
    --dev-funds 0xFUNDED_ADDRESS \
    --governance 0xGOVERNANCE_ADDRESS
```

### RPC Endpoints

| Node | Endpoint |
|------|----------|
| validator-1 | http://127.0.0.1:8545 |
| validator-2 | http://127.0.0.1:8544 |
| validator-3 | http://127.0.0.1:8543 |
| validator-4 | http://127.0.0.1:8542 |
| observer | http://127.0.0.1:8541 |

### Shutdown

```bash
killall telcoin-network
```

To reset and reconfigure, delete `./local-validators/` and rerun the script with `--dev-funds`.

## Testing Validator Onboarding

This walkthrough demonstrates the complete flow to onboard a new (5th) validator on a running local testnet. It uses [Foundry's `cast`](https://book.getfoundry.sh/reference/cast/) for on-chain interactions.

### Prerequisites

- A running local testnet (see Quick Start above)
- [Foundry](https://book.getfoundry.sh/getting-started/installation) installed
- The private key for your `--dev-funds` address (this is the ConsensusRegistry owner unless `--governance` was set separately)

### Setup

```bash
# ConsensusRegistry system address
REGISTRY=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1

# RPC endpoint (any running validator)
RPC=http://127.0.0.1:8545

# Private key for the governance address (ConsensusRegistry owner)
GOV_KEY=0xYOUR_GOVERNANCE_PRIVATE_KEY

# Address for the new validator
NEW_VALIDATOR=0xNEW_VALIDATOR_ADDRESS

# Passphrase that encrypts the new validator's BLS key
export TN_BLS_PASSPHRASE='YOUR_STRONG_PASSPHRASE'
```

Pick a strong value for `TN_BLS_PASSPHRASE`. `keytool generate` and `node` read it by default (`--bls-passphrase-source env`) and exit with "passphrase is required" when it is unset, and `node` needs the same value that encrypted the key. Run the steps below in this shell. If the node runs in a container, pass the variable through the container's environment, as `compose.yaml` does for its services.

### Step 1: Generate keys for the new validator

```bash
# Encrypts the BLS key with TN_BLS_PASSPHRASE from Setup
target/release/telcoin-network keytool generate validator \
    --datadir ./new-validator \
    --address $NEW_VALIDATOR
```

This creates `./new-validator/node-info.yaml` containing the BLS public key and proof of possession, and `./new-validator/node-keys/bls.kw`, the BLS key encrypted with `TN_BLS_PASSPHRASE`.

### Step 2: Export staking calldata

```bash
# Human-readable output (inspect the values)
target/release/telcoin-network --bls-passphrase-source no-passphrase \
    keytool export-staking-args \
    --node-info ./new-validator/node-info.yaml

# Raw calldata for use with cast (-q keeps log lines out of the capture)
STAKE_CALLDATA=$(target/release/telcoin-network -q --bls-passphrase-source no-passphrase \
    keytool export-staking-args \
    --node-info ./new-validator/node-info.yaml \
    --calldata)
```

`export-staking-args` never reads the BLS key, but the binary requires a passphrase source for every keytool command except `set-rpc`, hence `--bls-passphrase-source no-passphrase`, which lets these commands run whether or not `TN_BLS_PASSPHRASE` is set.

### Step 3: Query the required stake amount

`stake()` checks the value against the stake version of the current epoch. Read the version, then that version's config:

```bash
STAKE_VERSION=$(cast call $REGISTRY "getCurrentStakeVersion()(uint8)" --rpc-url $RPC)

cast call $REGISTRY \
    "stakeConfig(uint8)(uint256,uint256,uint256,uint32)" \
    $STAKE_VERSION \
    --rpc-url $RPC
```

The first value is `stakeAmount` in wei. The default genesis config is **1,000,000 TEL**:

```bash
STAKE_AMOUNT=1000000ether
```

### Step 4: Mint a ConsensusNFT for the new validator

Only the ConsensusRegistry owner (governance) can call `mint`. This whitelists the address to stake.

```bash
cast send $REGISTRY \
    "mint(address)" \
    $NEW_VALIDATOR \
    --private-key $GOV_KEY \
    --rpc-url $RPC
```

### Step 5: Fund the new validator

Transfer TEL from the dev-funds account so the validator can pay for staking and gas:

```bash
cast send $NEW_VALIDATOR \
    --value 1000001ether \
    --private-key $GOV_KEY \
    --rpc-url $RPC
```

> Send slightly more than the stake amount so the validator has gas for the `stake()` and `activate()` transactions.

### Step 6: Stake

The validator calls `stake()` with BLS key arguments and sends the exact stake amount as `msg.value`:

```bash
# NEW_VAL_KEY is the private key for $NEW_VALIDATOR
cast send $REGISTRY \
    "$STAKE_CALLDATA" \
    --value $STAKE_AMOUNT \
    --private-key $NEW_VAL_KEY \
    --rpc-url $RPC
```

### Step 7: Activate

Signal readiness for committee selection:

```bash
cast send $REGISTRY \
    "activate()" \
    --private-key $NEW_VAL_KEY \
    --rpc-url $RPC
```

The validator enters `PendingActivation` and becomes `Active` at the next epoch transition. The earliest committee it can be selected for starts two epochs after that.

### Step 8: Start the new validator node

Copy genesis files from an existing validator, then start:

```bash
mkdir -p ./new-validator/genesis
cp ./local-validators/genesis/genesis.yaml ./new-validator/genesis/
cp ./local-validators/genesis/committee.yaml ./new-validator/genesis/
cp ./local-validators/parameters.yaml ./new-validator/

# Decrypts the BLS key with TN_BLS_PASSPHRASE from Setup (the value used in Step 1)
# Instance 6 avoids port conflicts (1-4 = validators, 5 = observer)
target/release/telcoin-network node \
    --datadir ./new-validator \
    --instance 6 \
    --log.stdout.format log-fmt \
    -vvv \
    --http
```

The RPC endpoint will be at `http://127.0.0.1:8540`.

### Verifying on-chain state

```bash
# Check ConsensusNFT ownership
cast call $REGISTRY "balanceOf(address)(uint256)" $NEW_VALIDATOR --rpc-url $RPC

# Read validator info
cast call $REGISTRY "getValidator(address)" $NEW_VALIDATOR --rpc-url $RPC
```
