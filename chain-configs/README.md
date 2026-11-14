# Chain Configs

This directory contains the genesis and general configurations for testnet and mainnet.
These are built into the telcoin-network binary for use with the `--chain` flag.

Four files of each network are built into the binary:

- `genesis.yaml`: the EVM genesis block.
- `committee.yaml`: the genesis committee, the validators of epoch 0. `--chain` writes it to the datadir's `genesis/committee.yaml` if that file is missing.
- `parameters.yaml`: consensus parameters. `--chain` reads them from the binary, never from the datadir.
- `bootstrap_servers.yaml`: the servers a new node dials to join the network, keyed by BLS public key. `--chain` reads them from the binary, never from the datadir's committee file.

## Bootstrap servers

The bootstrap servers are dial hints for a node's first contact with the network. Once a node has peers, kademlia and the peer records it learns take over.
A server needs no committee seat: any reachable node can serve, and the list need not match the genesis committee, so `committee.yaml` lists none (`bootstrap_servers: {}`; the empty key stays because older binaries refuse a committee file without it).
To change the list, edit `bootstrap_servers.yaml` and build a new binary. A node takes the new list when it restarts on that binary; nothing else changes, and no coordinated upgrade is needed.
An operator can override the list without a new binary: `bootstrap_peers` in the datadir's `network-config`, or `--bootstrap-peers` for one run (`docs/src/getting-started/bootstrap-peers.md`).

Each entry lists the server's primary and its workers in worker id order, and every address ends with `/p2p/<peer id>` of the entry's network key.
`bundled_bootstrap_servers_are_dialable` in `crates/config/src/node.rs` checks both files: each parses, lists at least one server, names no BLS key twice, and passes the node's startup checks on bootstrap entries; the testnet list may hold no loopback address.

A network started from datadir genesis files instead of `--chain` (a local testnet, or the output of `telcoin-network genesis`) has no built-in list, and takes its bootstrap servers from the `bootstrap_servers` of its `genesis/committee.yaml`.

## testnet

`testnet/` is the genesis configuration of the running Adiri testnet (chain ID 2017).
`--chain adiri` and `--chain test-net` load it, and the binary must be built with the `adiri` feature to start on it.

Its `genesis.yaml` predates the ConsensusRegistry fork.
The `ConsensusRegistry` and `WorkerConfigs` code in it is the pre-fork code, not what `tn-contracts` builds today.
In the block that closes epoch 406, builds with the `adiri` feature replace the code of both contracts in place, keeping their storage, and run a one-time migration of the registry's validator sets (`CONSENSUS_REGISTRY_FORK_EPOCH` is 407 in `crates/types/src/forks.rs`). The live chain has run the upgraded registry since epoch 407.
A node syncing from genesis runs the pre-fork code up to that boundary and then applies the same swap.

Do not edit `testnet/genesis.yaml` to match the current contracts.
Any change to its values changes the genesis block, and a node built from the edited file no longer follows the live chain.
The swap also refuses to run unless the registry still carries the pinned pre-fork code hash.
For the registry ABI that is live today, read the `tn-contracts` sources or the staking docs, not this file.

## mainnet

`mainnet/` (chain ID 731) holds placeholders, not the real mainnet configuration.
Its validators are throwaway keys from the ignored test `regenerate_mainnet_chain_configs` in `crates/telcoin-network-cli/src/genesis/mod.rs`, which is rerun after a `tn-contracts` artifact bump.
That test rewrites `genesis.yaml`, `committee.yaml` and `parameters.yaml` only. `bootstrap_servers.yaml` holds an earlier placeholder set on loopback addresses and is edited by hand.
The real files will come from the mainnet genesis ceremony; the real bootstrap servers are set in `bootstrap_servers.yaml` before launch.
