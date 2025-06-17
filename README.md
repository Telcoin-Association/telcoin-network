# Telcoin Network

Consensus layer (CL) is an implementation of Narwhal and Bullshark.
Execution layer (EL) produces EVM blocks compatible with Ethereum.

Requires Rust 1.86

### Supported Platforms

The Telcoin Network protocol client supports Linux and MacOS operating systems. For Windows users, use WSL to run a Linux environment in which the client compiles and runs properly.

## Quick Start

Check out the repo and update the submodules:
`make init-submodules` or `git submodule update --init --recursive`

Run the test network script to start four local validators and begin advancing the chain:
`etc/local-testnet.sh --start --dev-funds 0xADDRESS`
Note: the script will compile a release build, which may take a few minutes.
This configures and creates genesis for a new network and starts it. See the output for the RPC endpoints.
0xADDRESS above should be a valid address prepended with 0x. Make sure you have the key for this address,
it will be funded with 1billion TEL on your test network. After configuration you can run the script with
just the --start option (--dev-funds is only used when configuring and CAN NOT be used later to fund
an account). Nodes run in the backgound and should be killed with the `kill` or `killall` commands.

The best docs for running a test network will currently be this script. It is short and pretty basic,
it configures each node, brings together the configs to create genesis and then shares this with each node.
This is the same basic procedure used to create nodes on diffent machines (NOTE- do not use the instance
option if not running on the same machine, it is to avoid port conflicts).

Once started you can use the RPC endpoint for any node with your favorate Ethereum tooling to test.
You will have test funds in your dev funds account set during config. The network can be restarted
by shutting down, `killall telcoin-network` is good for this, deleting ./local-validators/ and
rerunning the script.

The defaults should build a block roughly every 10 seconds, see comments on the script if you want to
speed this up for testing.

## CLI Usage

The CLI is used to create validator information, join a committee, and start the network.
The following `.env` variables are useful but not required:

- `TN_PRIMARY_ADDR`: The multi address of the primary libp2p network.
- `TN_WORKER_ADDRS`: The multi address(es) of the worker libp2p networks.  This is a comma seperated list.
All of these multi addresses will default to /ip4/127.0.0.1/udp/[PORT]/quic-v1 with an unused port for PORT.  This is really only useful for tests (but is very useful for testing).
You MUST supply quic-v1 and udp to work with the telcoin-network (although if you were setting up your own network other protocols may work but are untested).
References for multiaddr:
https://github.com/multiformats/multiaddr
https://github.com/multiformats/rust-multiaddr
These are used with libp2p2 so also see the Rust libp2p docs.

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
