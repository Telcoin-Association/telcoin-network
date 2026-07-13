# Telcoin Network

Consensus layer (CL) is an implementation of Narwhal and Bullshark.
Execution layer (EL) produces EVM blocks compatible with Ethereum.

Requires Rust 1.94

### Supported Platforms

The Telcoin Network protocol client supports Linux and MacOS operating systems. For Windows users, use WSL to run a Linux environment in which the client compiles and runs properly.

## Quick Start

Check out the repo and update the submodules:
`make init-submodules` or `git submodule update --init --recursive`

### Run an observer against testnet

Build a release version of the node software:
`cargo build -p telcoin-network --bin telcoin-network --release`

Generate a config and keys for your observer node:
`target/release/telcoin-network keytool generate observer --datadir DATADIR --address 0x4444444444444444444444444444444444444444 --bls-passphrase-source ask`

This will use DATADIR for storage and set your "execution" address to 0x4444444444444444444444444444444444444444. Note an observer does not recieve credit for execution but this option needs to be set anyway (at time of writing). Use an address you control or a dummy like above. This will also ask for the password for your nodes BLS key, this will need to be entered when started (or it can be put in an ENV var for injection).

Start your observer node:
`target/release/telcoin-network node -vvv --http --observer --chain adiri --bls-passphrase-source ask --datadir DATADIR`

Make sure DATADIR matches the config command above and use the same password for reading the key.

### Run a validator

#### 1. Generate validator keys

```bash
telcoin-network --datadir DATADIR --bls-passphrase-source ask \
  keytool generate validator \
  --address 0xYOUR_EXECUTION_ADDRESS \
  --external-primary-addr /ip4/YOUR_IP/udp/PORT/quic-v1
```

This generates a BLS keypair, network keys, proof-of-possession, and a `node-info.yaml` file in `DATADIR`.

- `--address` is the execution layer address that receives fees
- `--external-primary-addr` should be set to the node's public IP and port

#### 2. Export staking arguments

```bash
telcoin-network keytool export-staking-args --node-info DATADIR/node-info.yaml
```

This reads the public `node-info.yaml` and outputs the BLS public key and proof-of-possession in the format required by `ConsensusRegistry.stake()`.
No BLS private key or passphrase is needed.

Output modes:

- **Default**: human-readable display of each argument
- `--json`: JSON object for programmatic use
- `--calldata`: hex-encoded calldata for direct transaction submission

#### 3. Stake on-chain

**Prerequisite:** Governance must mint a ConsensusNFT for your execution address before you can stake.

Governance mints the NFT:

```bash
# local testnet example
cast send $CONSENSUS_REGISTRY "mint(address)" 0xVALIDATOR_EXECUTION_ADDRESS --trezor --rpc-url RPC_URL
```

The `ConsensusRegistry` is deployed at a fixed system address:

```bash
CONSENSUS_REGISTRY=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1
```

The `stake()` function is payable.
Users must send the exact required stake amount as the transaction value.
Query the current required stake amount:

```bash
cast call $CONSENSUS_REGISTRY "getCurrentStakeConfig()(uint256,uint256,uint256,uint32)" --rpc-url RPC_URL
```

The first value returned is `stakeAmount` (in wei). Use it as the `--value` in the stake transaction:

```bash
CALLDATA=$(telcoin-network keytool export-staking-args --node-info DATADIR/node-info.yaml --calldata)
cast send $CONSENSUS_REGISTRY $CALLDATA --value <STAKE_AMOUNT> --trezor --rpc-url RPC_URL
```

Other signing options: `--ledger` for Ledger hardware wallets, `--interactive` to enter a private key securely at a prompt.

After staking, signal that your validator is ready for committee selection:

```bash
cast send $CONSENSUS_REGISTRY "activate()" --trezor --rpc-url RPC_URL
```

#### 4. Start the validator node

```bash
telcoin-network node -vvv --http --chain adiri --bls-passphrase-source ask --datadir DATADIR
```

Make sure `DATADIR` matches the directory used during key generation and use the same BLS passphrase.

### Start a local development network

Run the test network script to start four local validators and begin advancing the chain:
`etc/local-testnet.sh --start --dev-funds 0xADDRESS`
Note: the script will compile a release build, which may take a few minutes.
This configures and creates genesis for a new network and starts it.
See the output for the RPC endpoints.
0xADDRESS above should be a valid address prepended with 0x.
Make sure you have the key for this address, it will be funded with 1billion TEL on your test network.
After configuration you can run the script with just the --start option (--dev-funds is only used when configuring and CAN NOT be used later to fund an account).
Nodes run in the backgound and should be killed with the `kill telcoin-network` or `killall telcoin-network` commands.

The best docs for running a test network will currently be this script.
It is short and pretty basic, it configures each node, brings together the configs to create genesis and then shares this with each node.
This is the same basic procedure used to create nodes on diffent machines (NOTE- do not use the instance option if not running on the same machine, it is to avoid port conflicts).

Once started you can use the RPC endpoint for any node with your favorate Ethereum tooling to test.
You will have test funds in your dev funds account set during config.
The network can be restarted by shutting down, `killall telcoin-network` is good for this, deleting ./local-validators/ and rerunning the script.

The defaults should build a block roughly every 10 seconds, see comments on the script if you want to speed this up for testing.

## CLI Usage

The CLI is used to create validator information, join a committee, and start the network.
The following `.env` variables are useful but not required:

- `PRIMARY_LISTENER_MULTIADDR`: The multi address of the primary libp2p network.
- `WORKER_LISTENER_MULTIADDR`: The multi address of the worker libp2p network.
  All of these multi addresses will default to /ip4/127.0.0.1/udp/[PORT]/quic-v1 with an unused port for PORT. This is really only useful for tests (but is very useful for testing).
  You MUST supply quic-v1 and udp to work with the telcoin-network (although if you were setting up your own network other protocols may work but are untested).
  References for multiaddr:
  https://github.com/multiformats/multiaddr
  https://github.com/multiformats/rust-multiaddr
  These are used with libp2p2 so also see the Rust libp2p docs.

## Snapshots

Cloud snapshots let a fresh node start at the last epoch boundary instead of replaying consensus from genesis. An observer node publishes a signed, state-only snapshot to object storage at each epoch boundary; a new node downloads one and verifies it against its LOCAL `genesis`/`committee.yaml` before installing a single byte. Verification is always full and trustless — the bucket is never a trust root, so the only thing an operator must protect is the local committee file.

### Publishing (observer nodes only)

Run an observer with an upload target and it publishes one snapshot per epoch boundary:

```bash
telcoin-network node --observer --snapshot-upload <URL> --chain adiri --datadir DATADIR
```

- `--snapshot-upload` (env `TN_SNAPSHOT_UPLOAD`) is **observer-only**. A validator started with it refuses to boot: the uploader must never spend the epoch-boundary quiet window publishing.
- `--snapshot-keep-last N` (env `TN_SNAPSHOT_KEEP_LAST`, default `3`) retains the N most-recent snapshots and prunes older ones.

Supported URL schemes and the credentials each reads from the process environment:

| Scheme | Example | Credentials |
| --- | --- | --- |
| `s3://` (`s3a://`) | `s3://bucket/prefix` | `AWS_*` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, …) |
| `gs://` | `gs://bucket/prefix` | `GOOGLE_*` (service-account key or path) |
| `az://` (`azure://`, `abfs://`, …) | `az://container/prefix` | `AZURE_*` (`AZURE_STORAGE_ACCOUNT_NAME` plus an access or SAS key) |
| `http(s)://` | `https://host/path` | none (public, pre-signed, or reverse-proxied; a `user:pass@` in the URL is honored) |
| `file://` | `file:///abs/path` | none (a local directory; for tests and local runs) |

Operational notes:

- Run **one uploader per bucket prefix**. Publishing is last-writer-wins on `latest.json`, so two uploaders pointed at the same prefix will race the pointer.
- Set a bucket **lifecycle rule to abort/expire incomplete multipart uploads**. A cancelled upload can leave orphaned parts that snapshot pruning does not clean up.

### Restoring

Restore into a datadir that already holds `genesis`/`committee.yaml` but **no chain data**:

```bash
telcoin-network snapshot restore --source <URL> [--epoch N] --datadir DATADIR
```

Or auto-restore on a fresh start — the node downloads, verifies, and installs the newest snapshot before opening its databases, and skips silently if the datadir already holds chain data:

```bash
telcoin-network node --observer --snapshot-source <URL> --chain adiri --datadir DATADIR
```

- Verification is **always full**; the trust root is the LOCAL `committee.yaml`, never the bucket. Without a local genesis/committee, restore refuses with a missing-trust-root error.
- The restored epoch is **logged prominently**. A malicious bucket cannot forge a snapshot for the wrong committee, but it can withhold the newest one and serve an older, still-valid snapshot. Passing `--epoch N` pins the choice, bypasses the `latest.json` pointer, and defeats withholding.
- `telcoin-network snapshot verify --source <URL> [--epoch N]` runs the same download-and-verify path but installs nothing, so a bucket can be vetted before committing to a restore. By default it also recomputes the state root the way a restore would — a dry-run rebuild of the plain-state dump into a throwaway datadir it discards afterward — so a chunk that hashes correctly but decodes to the wrong state is still caught. That rebuild costs roughly a restore's worth of disk and time; pass `--skip-state-root` to check only each chunk's sha256 and skip it.
- `telcoin-network snapshot create --out <URL>` exports and uploads a snapshot from a **stopped** node whose tip sits exactly on an epoch boundary — an out-of-band alternative to the background uploader.

### Caveats

- **State-only, by design.** A restored node does not serve pre-snapshot history: it holds the final EVM state and a bounded header window, not the full chain.
- **Fee-derivability skips.** The uploader skips any epoch whose EIP-1559 base fees are not derivable from the captured state (a fee-configured worker that produced no block that epoch). Such an epoch is never published, because a restored node could not resume fee calculation from it.
- **Quarantine after a failed restore.** A failed restore leaves a `.restore-incomplete` marker in the datadir's `snapshots/` directory. While it is present, a manual `snapshot restore` refuses (`restore did not complete`) and a node started with `--snapshot-source` refuses to boot rather than run on partial state. Recovery is manual: clear the datadir before retrying.

## Example RPC request

### get chain id

curl 127.0.0.1:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_chainId","params":[],"id":1,"jsonrpc":"2.0"}'

## TN-Contracts Submodule

Telcoin-Network infrastructure makes use of several onchain contracts which serve as critical infrastructure for the network.
These include a validator staking contract, bridged token module, testnet faucet, CI attestation interface, and several others like Uniswap and various liquid stablecoins.
All onchain infrastructure is housed in a Foundry smart contract submodule called `tn-contracts`.
The repo is publicly available [here](https://github.com/Telcoin-Association/tn-contracts).

### Initialize `tn-contracts`

After cloning `telcoin-network`, initialize the `tn-contracts` submodule using the following make command:

```bash
make init-submodules
```

### Updating the tn-contracts submodule

The tn-contracts submodule is pinned to a commit on its master branch, please note that this pinned commit may not always be the most recent as it is only updated on large version updates for stability. The instructions below detail the update process for updating the submodule, assuming the changes have been merged into the tn-contracts master branch:

**Create a new branch for the submodule update**

`git checkout -b feat/update-submodule`

**Update the submodule reference in the telcoin-network parent repository**

```bash
make update-tn-contracts
```

**Push the new submodule reference and create a PR**

```bash
git add tn-contracts
git commit -m "Update tn-contracts submodule"
git push -u origin feat/update-submodule
# Now create a PR using the pushed branch
```

**After merging the PR, all telcoin-network developers must update their local copy of telcoin-network with the new submodule commit**

```bash
git pull
make init-submodules
```

This will install Foundry to the submodule but will not initialize its dependencies.
Please see `tn-contracts` repo for instructions on how to initialize its dependencies.
Devs do not need to initialize or install `tn-contract` dependencies to operate in this repo.

## Acknowledgements

Telcoin Network is an EVM-compatible blockchain built with DAG-based consensus.
While building the protocol, we studied and explored many different projects to identify what worked well and where we could make improvements.

We want to extend our sincere appreciation to the following teams:

- [reth](https://github.com/paradigmxyz/reth): Reth stands out for their dedication to implementing the Ethereum protocol with clean, well-written code. Their unwavering commitment to building a strong open-source community has reached far beyond the Ethereum ecosystem. We are truly grateful for their leadership and the inspiration they continue to provide.
- [sui](https://github.com/MystenLabs/sui): Telcoin Network uses a version of Bullshark that was heavily derived from Mysten Lab's Sui codebase under Apache 2.0 license. Because this code was already released under the Apache License, we decided to start with a derivation of their work to iterate more quickly. We thank the Mysten Labs team for pioneering BFT consensus protocols and publishing their libraries.
