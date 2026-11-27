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

### The validator files

`mainnet/validators/` holds the inputs of the genesis ceremony: one node-info file per genesis validator, in the format `telcoin-network keytool generate validator` writes as `node-info.yaml`.
The files are named `validator-00.yaml` to `validator-09.yaml`.
File N belongs to the validator that stakes under genesis validator Safe N, the Nth address of `tn-contracts/deployments/genesis/validator-safes.yaml` (`tn_config::genesis_validator_safes()`).
Its `execution_address` must be that Safe, and its proof of possession must be signed over it.

Today the ten files are throwaway placeholders from the ignored test `mint_mainnet_placeholder_validators` in `crates/telcoin-network-cli/src/genesis/mod.rs`.
Their names start with `placeholder-validator-`, their addresses are in 192.0.2.0/24 (a documentation range that is never routed), and their private keys were discarded when the test finished.
The test refuses to replace a file that is not one of its placeholders.

### Regenerating the configs

The ignored test `regenerate_mainnet_chain_configs` in the same file runs the genesis ceremony over the ten validator files and rewrites `genesis.yaml`, `committee.yaml` and `parameters.yaml`.
It mints no keys.
Before the ceremony it refuses a directory that does not hold exactly the ten files, a file whose `execution_address` is not its Safe, a proof of possession that does not verify for the file's BLS key and Safe, and a BLS key listed in two files.
Rerun it after a `tn-contracts` artifact bump and after any change to the validator files:

```
cargo test -p telcoin-network-cli regenerate_mainnet_chain_configs -- --ignored
```

The ceremony stamps the current time as the genesis timestamp, so two runs over the same files differ only in the `timestamp` line of `genesis.yaml`.
Every regeneration therefore changes the genesis hash: commit the regenerated files together with the new `MAINNET_GENESIS_HASH` and `MAINNET_GENESIS_TIMESTAMP` of `crates/types/src/genesis.rs`, which `mainnet_genesis_hash_is_pinned` prints.
Run each ignored test by name: an unfiltered `-- --ignored` run starts both at once.

`bootstrap_servers.yaml` is not written by the ceremony. It holds an earlier placeholder set on loopback addresses and is edited by hand; the real bootstrap servers are set there before launch.

### Final data

The ignored test `mainnet_final_data_is_complete` in `crates/telcoin-network-cli/src/genesis/mod.rs` fails while any placeholder is left: the genesis timestamp, the placeholder validator files, the placeholder owner, dev-funded account and basefee recipient, and unreachable validator or bootstrap server addresses.
Run it by name with `cargo nextest run -p telcoin-network-cli --run-ignored ignored-only -E 'test(mainnet_final_data_is_complete)'`.

The final mainnet data is installed in this order, in one commit:

1. Confirm each operator's BLS public key out of band.
   The operator reads back the `bls_public_key` of their file over a second channel and confirms the Safe index they were assigned.
   The proof of possession shows that the file's author holds the key, not that the key belongs to the operator of that Safe.
2. Replace `mainnet/validators/validator-NN.yaml` with the file of the operator assigned Safe NN, keeping the file names.
   The regeneration refuses a directory that does not hold exactly the ten files.
3. Set the launch values in the ceremony arguments of `regenerate_mainnet_chain_configs`: the owner of the ConsensusRegistry and WorkerConfigs (`--consensus-registry-owner`), the dev-funded account (`--dev-funded-account`, or no flag for none) and the basefee recipient (`--basefee-address`).
   Today these arguments pass the constants `MAINNET_PLACEHOLDER_OWNER`, `MAINNET_PLACEHOLDER_DEV_FUNDED_ACCOUNT` and `MAINNET_PLACEHOLDER_BASEFEE_ADDRESS`.
   Replace the arguments and keep the constants, which the final-data gate refuses to find in the genesis.
   Check the remaining arguments at the same time: the stake and minimum withdrawal stay on the CLI defaults, and the epoch duration, the epoch issuance, the Issuance contract's balance (`--issuance-balance`) and worker 0's fee config (`--worker-fee-config`) come from constants in the same file.
   These four are decided: 6-hour epochs, 136,986 TEL for each epoch, 200,000,000 TEL on the Issuance contract, and one worker with the EIP-1559 fee strategy and a gas target of 1,000,000 gas.
   The header and batch delays are literals in the same argument list (`--max-header-delay-ms 1000`, `--min-header-delay-ms 350`, `--max-batch-delay-ms 250`), and the regeneration writes them to `parameters.yaml`.
4. Fill `mainnet/bootstrap_servers.yaml` with the real bootstrap servers, the nodes that answer a new node's first contact, which need not be validators.
   Each entry lists the server's BLS key, its primary and worker addresses, and the `/p2p/<peer id>` of its network key.
   Every address must be a public IP address, not a DNS name, which the node does not resolve.
   A node that can dial none of the listed servers starts without peers unless its operator names other bootstrap peers.
5. Run `regenerate_mainnet_chain_configs` with the genesis timestamp fixed at 1796214660 (2026-12-02 12:31:00 UTC).
6. Update `MAINNET_GENESIS_HASH` and `MAINNET_GENESIS_TIMESTAMP` in `crates/types/src/genesis.rs` to the values that `mainnet_genesis_hash_is_pinned` prints.
   Update any other pin the launch values move, such as the funded accounts or the stake: each failing test names the value it pins.
7. Remove the `#[ignore]` of `mainnet_final_data_is_complete`.
8. Run the mainnet tests: `cargo nextest run -p tn-types -p tn-config -p telcoin-network-cli`, which now includes the final-data gate, and `cargo nextest run -p tn-reth -E 'test(mainnet)'`.
9. Commit the ten validator files, `bootstrap_servers.yaml`, the regenerated `genesis.yaml`, `committee.yaml` and `parameters.yaml`, the ceremony arguments, the pinned constants and the gate together.

Some of these steps cannot be done yet.
Steps 1 and 2 wait for the operators' node-info files.
The owner, dev-funded account and basefee recipient of step 3 and the servers of step 4 are launch decisions that have not been made yet, so the committed files keep their placeholders.
Step 5 waits for a ceremony option that fixes the genesis timestamp: today the ceremony stamps the wall clock, so it is run with the fixed launch timestamp once that option is available.
