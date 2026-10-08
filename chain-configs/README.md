# Chain Configs

This directory contains the genesis and general configurations for testnet and mainnet.
These are built into the telcoin-network binary for use with the `--chain` flag.

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

`mainnet/` (chain ID 487) holds placeholders, not the real mainnet configuration.
Its validators are throwaway keys from the ignored test `regenerate_mainnet_chain_configs` in `crates/telcoin-network-cli/src/genesis/mod.rs`, which is rerun after a `tn-contracts` artifact bump.
The real files will come from the mainnet genesis ceremony.
