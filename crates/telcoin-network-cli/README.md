# Validator Node Operator Guide

Technical reference for running a Telcoin Network validator node. Covers key generation, genesis ceremony, node startup, configuration, networking, monitoring, and troubleshooting.

The `telcoin-network` binary has three subcommands:

- keytool: generate cryptographic keys and export staking arguments
- genesis: produce the chain genesis from validator configs (only applicable for new network deployments)
- node: run the validator or observer node

## Prerequisites

Hardware: [Hardware requirements](../../docs/src/getting-started/hardware-requirements.md) has the sizing model, the benchmark results and the observer profiles.
The validator figures below are the minimum and recommended tiers from that page, set from the 2026-09 benchmark; the page says which figures are measured and which are modelled.
CPU and memory are twice the measured single-worker figures, a margin for the multi-worker rollout.

| Resource | Minimum | Recommended |
| -------- | ------- | ----------- |
| CPU      | 8 physical cores | 16 physical cores, PassMark single-thread 3,500+ |
| RAM      | 32 GB ECC, no swap | 64 GB ECC, no swap |
| Disk     | 2 TB TLC NVMe SSD, 10,000+ sustained write IOPS, 300+ MB/s, rated 2+ DWPD | 4 TB TLC NVMe SSD, 20,000+ sustained IOPS, 500+ MB/s, rated 1+ DWPD |
| Network  | 200 Mbps symmetric | 1 Gbps |

Software:

- Linux (Debian/Ubuntu recommended) or macOS
- Rust 1.94+ toolchain (if building from source)
- Docker 24+ (if using container deployment)
- `cmake`, `libclang-16-dev`, `pkg-config`, `libssl-dev`, `libapr1-dev` (build dependencies on Debian)

Build from source:

```bash
cargo build -p telcoin-network --bin telcoin-network --release
```

The binary lands at `target/release/telcoin-network`.

## Key generation

Generate validator credentials with the `keytool generate` subcommand. This creates a BLS keypair, derives network identity keys, and writes a `node-info.yaml` file describing the node's public identity.

```bash
telcoin-network keytool generate validator \
    --datadir /var/lib/telcoin \
    --address 0xYOUR_EXECUTION_ADDRESS
```

For observer nodes (no consensus participation):

```bash
telcoin-network keytool generate observer \
    --datadir /var/lib/telcoin \
    --address 0xYOUR_EXECUTION_ADDRESS
```

### keytool generate flags

| Flag                               | Default                    | Description                                                                                                           |
| ---------------------------------- | -------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `--address`, `--execution-address` | required                   | EVM address for fee recipient. Pass `0` for the zero address. Env: `EXECUTION_ADDRESS`                                |
| `--workers`                        | `1`                        | Number of workers for the primary (range: 1-4, must be 1 currently)                                                   |
| `--force`, `--overwrite`           | `false`                    | Overwrite existing keys. Existing keys are lost permanently                                                           |
| `--name`                           | auto-derived               | Human-readable node name written to `node-info.yaml` (logging/RPC only). Defaults to `node-<bs58 of BLS key>`         |
| `--external-primary-addr`          | localhost with random port | External multiaddr for the primary P2P network. Format: `/ip4/HOST/udp/PORT/quic-v1`. Env: `TN_EXTERNAL_PRIMARY_ADDR` |
| `--external-worker-addrs`          | localhost with random port | Comma-separated multiaddrs for worker P2P networks. Env: `TN_EXTERNAL_WORKER_ADDRS`                                   |

### BLS passphrase handling

The `--bls-passphrase-source` global flag controls how the BLS private key is encrypted at rest. It applies to `keytool generate` (encryption) and `node` (decryption).

| Value           | Behavior                                                                                                                                      |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `env` (default) | Read passphrase from `TN_BLS_PASSPHRASE` environment variable. The variable is cleared from the process environment immediately after reading |
| `stdin`         | Read the first line from stdin until EOF                                                                                                      |
| `ask`           | Prompt interactively on the terminal (foreground TTY required)                                                                                |
| `no-passphrase` | Store the key unencrypted. Testing only; never use in production                                                                              |

Encrypted keys use AES-256-GCM-SIV with PBKDF2-HMAC-SHA256 key derivation (1,000,000 iterations). The encrypted file is saved as `node-keys/bls.kw`; unencrypted keys are saved as `node-keys/bls.key`.

The current binary decrypts the BLS key into node process memory and does not provide an HSM or remote signer interface. See [Validator production operations](../../docs/src/getting-started/validator-operations.md#bls-key-custody) for production custody, backup, and isolation guidance.

### Generated files

After running `keytool generate`, the data directory contains:

```
<datadir>/
  node-info.yaml           # public node identity (safe to share)
  node-keys/
    bls.key  or  bls.kw    # BLS private key (plain or encrypted)
    primary.seed            # seed for primary network key derivation
    worker.seed             # seed for worker network key derivation
```

### node-info.yaml

Contains the node's public identity. This file is shared with other validators during the genesis ceremony. Nothing in it is secret.

Fields:

- `name`: human-readable identifier (e.g. `node-JMQq7ZqVT`)
- `bls_public_key`: Base58-encoded BLS12-381 public key (96 bytes compressed)
- `p2p_info.primary`: primary network multiaddr and Ed25519 public key
- `p2p_info.workers`: one entry per worker, in worker ID order, each with the worker's network multiaddr, Ed25519 public key, and optional `rpc` endpoint (see [Advertising a JSON-RPC endpoint](#advertising-a-json-rpc-endpoint))
- `execution_address`: EVM address that receives block rewards
- `proof_of_possession`: BLS signature binding the public key to the execution address

Files written by v0.14.0-adiri and earlier have a single `worker:` map in place of `workers:`. The node and keytool still read that shape, and a keytool command that rewrites the file (`set-rpc`, `generate pop`) saves it as a `workers:` list.

Example:

```yaml
name: "node-JMQq7ZqVT"
bls_public_key: "mCss5AWBd69e6Na..."
p2p_info:
  primary:
    network_address: "/ip4/34.31.250.229/udp/49590/quic-v1/p2p/12D3KooW..."
    network_key: "4XTTM1f3EZanf..."
    rpc: ~
  workers:
    - network_address: "/ip4/34.31.250.229/udp/49594/quic-v1/p2p/12D3KooW..."
      network_key: "4XTTMD3rST8E7..."
      rpc: ~ # set with --rpc-http at generation or with keytool set-rpc
execution_address: "0xefaacf04b92298a88200aa50aa6bb7bfce587b17"
proof_of_possession: "kFa9r..."
```

### Rotating the execution address

To stake or earn rewards under a different execution address, the proof of possession has to be re-signed. The PoP commits to the execution address, so the original signature fails on-chain `stake()` verification once the address changes (the symptom is a "proof of possession is incorrect" revert).

`keytool generate pop` re-signs the proof of possession for a new address using the node's *existing* keys. It never generates or overwrites keys — the BLS key, network identity keys, p2p peer IDs, and node name stay byte-for-byte identical. Only `execution_address` and `proof_of_possession` in `node-info.yaml` change.

```bash
telcoin-network keytool generate pop \
    --datadir /var/lib/telcoin \
    --address 0xNEW_EXECUTION_ADDRESS
```

The command requires existing keys and a `node-info.yaml` under `--datadir`; it errors if either is missing (run `keytool generate validator|observer` first). When the new address differs from the current one it logs a warning, re-signs, and prints the new proof of possession. The `proof-of-possession` alias and the `EXECUTION_ADDRESS` env var both work, mirroring `generate validator|observer`.

After rotating, re-export the staking arguments for the new address (see [Staking registration](#staking-registration)):

```bash
telcoin-network keytool export-staking-args \
    --node-info /var/lib/telcoin/node-info.yaml
```

### Advertising a JSON-RPC endpoint

A node can advertise an optional JSON-RPC endpoint to peers over Kademlia so wallets and dapps can discover where to submit transactions. The endpoint is stored in `node-info.yaml` under `p2p_info.workers[0].rpc` (worker 0's entry) and advertised by the worker network when the node runs.

`keytool set-rpc` sets or clears that endpoint. It is a config-only edit — no keys are read and the BLS passphrase is ignored — so it requires an existing `node-info.yaml` under `--datadir`; run `keytool generate validator|observer` first (it errors with that hint otherwise).

```bash
telcoin-network keytool set-rpc \
    --datadir /var/lib/telcoin \
    --http https://validator.example.com:8545/ \
    --ws wss://validator.example.com:8546/
```

`--http` is the required HTTP/HTTPS endpoint; `--ws` is the optional WebSocket endpoint. Both are validated with the same check node startup applies — `--http` must use the `http` or `https` scheme and `--ws` must use `ws` or `wss` — so a bad scheme fails immediately instead of being advertised and rejected by peers.

Remove a previously-advertised endpoint with `--clear`:

```bash
telcoin-network keytool set-rpc --datadir /var/lib/telcoin --clear
```

`--clear` conflicts with `--http`/`--ws`, and omitting all flags is an error (`--http` is required unless `--clear`).

Validators should set this endpoint, because observers forward the transactions they accept to it. Advertise an `https://` URL served by a TLS reverse proxy, keep the node's RPC server on loopback, and never advertise a private address: observers refuse to dial one. [Validator production operations](../../docs/src/getting-started/validator-operations.md#advertising-an-rpc-endpoint) has the full rules.

## Genesis ceremony

The genesis ceremony runs once per network. One coordinator collects all validators' `node-info.yaml` files, runs the `genesis` command, and distributes the output to every participant.
For new nodes joining an existing network (testnet or mainnet), this step should be skipped.

### Directory layout before genesis

Collect all validator node-info files into a `genesis/validators/` directory under the shared datadir:

```
<shared-datadir>/
  genesis/
    validators/
      validator-1.yaml     # node-info.yaml from validator 1
      validator-2.yaml     # node-info.yaml from validator 2
      validator-3.yaml     # ...
      validator-N.yaml
```

### Running genesis

```bash
telcoin-network genesis \
    --datadir <shared-datadir> \
    --chain-id 2017 \
    --consensus-registry-owner 0xGOVERNANCE_MULTISIG \
    --basefee-address 0xBASEFEE_RECIPIENT \
    --initial-stake-per-validator 1000000 \
    --epoch-duration-in-secs 21600  # 6 hours, as on mainnet and testnet
```

### genesis flags

| Flag                                                 | Default        | Description                                                                               |
| ---------------------------------------------------- | -------------- | ----------------------------------------------------------------------------------------- |
| `--chain-id`                                         | `911329` (0xde7e1) | Numeric chain ID. Accepts decimal or `0x`-prefixed hex                                |
| `--consensus-registry-owner`                         | `0x...07a0`    | Owner address for the ConsensusRegistry contract. Use a governance multisig in production |
| `--basefee-address`                                  | `0x...07a0`    | Address that receives all transaction base fees                                           |
| `--initial-stake-per-validator`, `--stake`           | `1000000`      | TEL staked per validator at genesis (input in whole TEL, stored as wei)                   |
| `--min-withdraw-amount`, `--min_withdraw`            | `1000`         | Minimum TEL withdrawal amount                                                             |
| `--epoch-block-rewards`, `--block_rewards_per_epoch` | `25806`        | Total block rewards per epoch in TEL                                                      |
| `--epoch-duration-in-secs`, `--epoch_length`         | `28800`        | Epoch duration in seconds (default: 8 hours; mainnet and testnet use `21600`, 6 hours)    |
| `--max-header-delay-ms`                              | none           | Max delay between header proposals (milliseconds)                                         |
| `--min-header-delay-ms`                              | none           | Min delay between header proposals (milliseconds)                                         |
| `--max-batch-delay-ms`                               | none           | Max delay before a worker seals a batch of pending transactions (milliseconds)            |
| `--dev-funded-account`                               | none           | Fund a deterministic test account. Never use in production                                |
| `--accounts`                                         | none           | Path to a YAML file mapping addresses to genesis accounts                                 |

### Genesis output files

The command produces three files:

1. `genesis/genesis.yaml`: EVM genesis block with chain config, hardforks, and alloc (includes ConsensusRegistry contract and precompiles)
2. `parameters.yaml`: consensus protocol parameters
3. `genesis/committee.yaml`: committee membership with BLS public keys and bootstrap network addresses

### Distributing genesis

Copy the three output files to each validator's data directory:

```bash
for VALIDATOR in validator-1 validator-2 validator-3 validator-4; do
    mkdir -p "./$VALIDATOR/genesis"
    cp genesis/genesis.yaml  "./$VALIDATOR/genesis/"
    cp genesis/committee.yaml "./$VALIDATOR/genesis/"
    cp parameters.yaml       "./$VALIDATOR/"
done
```

Each validator's data directory should now look like:

```
<validator-datadir>/
  node-info.yaml
  node-keys/
    bls.kw
    primary.seed
    worker.seed
  genesis/
    genesis.yaml
    committee.yaml
  parameters.yaml
```

## Starting the node

### Joining a named chain

For public networks with embedded genesis configs:

```bash
telcoin-network node \
    --datadir /var/lib/telcoin \
    --chain adiri \
    --http \
    --metrics 127.0.0.1:9101
```

Available named chains: `adiri` and `test-net`, which both load the Adiri testnet config, and `main-net`.

The `--chain` flag overrides local genesis files with the embedded config for that network.

#### Run an observer against testnet

Build a release version of the node software with the `adiri` feature (required to join the
adiri testnet — the node refuses the `--chain adiri` flag at startup without it):
`cargo build -p telcoin-network --bin telcoin-network --release --features adiri`

Generate a config and keys for your observer node:
`target/release/telcoin-network keytool generate observer --datadir DATADIR --address 0x4444444444444444444444444444444444444444 --bls-passphrase-source ask`

This will use DATADIR for storage and set your "execution" address to 0x4444444444444444444444444444444444444444. Note an observer does not recieve credit for execution but this option needs to be set anyway (at time of writing). Use an address you control or a dummy like above. This will also ask for the password for your nodes BLS key, this will need to be entered when started (or it can be put in an ENV var for injection).

Start your observer node:
`target/release/telcoin-network node -vvv --http --chain adiri --bls-passphrase-source ask --datadir DATADIR`

Make sure DATADIR matches the config command above and use the same password for reading the key.

Node role is derived from committee membership: a key outside the current committee runs as an
observer. To take a validator out of consensus, exit it on chain.


### Using local config

When running a private network or local testnet, omit `--chain` and point `--datadir` at a directory containing the genesis files:

```bash
telcoin-network node \
    --datadir /var/lib/telcoin \
    --http \
    --metrics 127.0.0.1:9101
```

### node flags

| Flag                  | Default        | Description                                                                                    |
| --------------------- | -------------- | ---------------------------------------------------------------------------------------------- |
| `--chain`             | none           | Join a named network (`adiri`, `test-net`, `main-net`)                                         |
| `--instance`          | none           | Instance number (1-200) for port offsetting. See [Multi-instance setup](#multi-instance-setup) |
| `--metrics`           | none           | Enable Prometheus metrics at this socket address (e.g. `127.0.0.1:9101`)                       |
| `--healthcheck`       | none           | TCP health check port. Env: `HEALTHCHECK_TCP_PORT`                                             |
| `--node-name`         | auto-generated | Name for OpenTelemetry service identification                                                  |
| `--tracing-url`       | none           | OpenTelemetry collector URL (e.g. `http://192.168.1.2:4317`). Env: `TN_TRACING_URL`            |
| `--with-unused-ports` | `false`        | Let the OS assign all ports (mutually exclusive with `--instance`)                             |

### global flags

These flags apply to all subcommands:

| Flag                      | Default                 | Description                                                              |
| ------------------------- | ----------------------- | ------------------------------------------------------------------------ |
| `--datadir`               | OS-specific (see below) | Path to the data directory                                               |
| `--bls-passphrase-source` | `env`                   | How to obtain the BLS passphrase: `env`, `stdin`, `ask`, `no-passphrase` |
| `-v` ... `-vvvvv`         | none                    | Verbosity level (info, debug, trace, etc.)                               |
| `--log.stdout.format`     | default                 | Log output format (e.g. `log-fmt`)                                       |
| `--color`                 | `auto`                  | Color mode: `always`, `auto`, `never`                                    |

Default data directory by platform:

| Platform | Path                                                                       |
| -------- | -------------------------------------------------------------------------- |
| Linux    | `$XDG_DATA_HOME/telcoin-network/` or `$HOME/.local/share/telcoin-network/` |
| macOS    | `$HOME/Library/Application Support/telcoin-network/`                       |
| Windows  | `{FOLDERID_RoamingAppData}/telcoin-network/`                               |

## Configuration reference

### Environment variables

| Variable                   | Used by          | Description                                                |
| -------------------------- | ---------------- | ---------------------------------------------------------- |
| `TN_BLS_PASSPHRASE`        | keytool, node    | BLS key passphrase (cleared from env after read)           |
| `EXECUTION_ADDRESS`        | keytool generate | Fee recipient address                                      |
| `TN_EXTERNAL_PRIMARY_ADDR` | keytool generate | External multiaddr for primary P2P                         |
| `TN_EXTERNAL_WORKER_ADDRS` | keytool generate | External multiaddrs for worker P2P (comma-separated)       |
| `TN_TRACING_URL`           | node             | OpenTelemetry collector endpoint                           |
| `HEALTHCHECK_TCP_PORT`     | node             | TCP health check port                                      |
| `RUST_LOG`                 | all              | Standard Rust log filter directive (e.g. `info,evm=debug`) |

## Data directory layout

Complete tree after the node has run:

```
<datadir>/
  node-info.yaml                  # node public identity
  parameters.yaml                 # consensus parameters
  network-config                  # network settings (YAML), written with defaults on first start
  node-keys/                      # private key material
    bls.key  or  bls.kw           #   BLS keypair (plain or encrypted)
    primary.seed                  #   primary network key seed
    worker.seed                   #   worker network key seed
  genesis/                        # chain genesis data
    genesis.yaml                  #   EVM genesis block
    committee.yaml                #   committee membership + bootstrap peers
  db/                             # execution layer database (Reth/MDBX)
    static_files/                 #   static block data
  consensus-db/                   # consensus protocol database
    epoch/                        #   per-epoch consensus data (certs, votes, payloads)
    epochs/                       #   epoch pack files for archive storage
```

## RPC configuration

Enable the HTTP and WebSocket RPC servers with `--http` and `--ws`. By default, both bind to `127.0.0.1`.

### RPC flags

| Flag                                     | Default       | Description                              |
| ---------------------------------------- | ------------- | ---------------------------------------- |
| `--http`                                 | disabled      | Enable the HTTP-RPC server               |
| `--http.addr`                            | `127.0.0.1`   | HTTP listen address                      |
| `--http.port`                            | `8545`        | HTTP listen port                         |
| `--http.api`                             | none          | RPC modules to enable (see below)        |
| `--http.corsdomain`                      | none          | Allowed CORS origins                     |
| `--ws`                                   | disabled      | Enable the WebSocket-RPC server          |
| `--ws.addr`                              | `127.0.0.1`   | WebSocket listen address                 |
| `--ws.port`                              | `8546`        | WebSocket listen port                    |
| `--ws.api`                               | none          | RPC modules to enable                    |
| `--ws.origins`                           | none          | Allowed WebSocket origins                |
| `--ipcdisable`                           | `false`       | Disable the IPC-RPC server               |
| `--ipcpath`                              | `/tmp/tn.ipc` | IPC socket path                          |
| `--rpc.jwtsecret`                        | none          | Hex-encoded JWT secret for RPC auth      |
| `--rpc.max-request-size`                 | `15` (MB)     | Max request payload size                 |
| `--rpc.max-response-size`                | `160` (MB)    | Max response payload size                |
| `--rpc.max-subscriptions-per-connection` | `1024`        | Max subscriptions per connection         |
| `--rpc.max-connections`                  | `500`         | Max concurrent RPC connections           |
| `--rpc.max-tracing-requests`             | CPU-dependent | Max concurrent tracing requests          |
| `--rpc.gascap`                           | Reth default  | Max gas for `eth_call`                   |
| `--rpc.txfeecap`                         | `0` (no cap)  | Max transaction fee via RPC (0 = no cap) |

### available RPC modules

`eth`, `net`, `web3`, `debug`, `trace`, `rpc`

`--http.api all` (and `--ws.api all`) enables `eth`, `net`, `web3`, `rpc`. The `debug` and
`trace` modules are expensive to serve on an archive node and are never part of `all`: name
them explicitly (for example `--http.api eth,debug,trace`) to enable them, which logs a
warning at startup. A selection whose first entry is `all` (for example `all,debug`) parses
as plain `all` and the rest of the list is ignored, so list every module by name instead.

The IPC endpoint (`--ipcpath`, enabled unless `--ipcdisable`) serves the same module set as
`all`.

The `admin` and `txpool` modules are not available at this time; they are dropped from any
selection with a warning.

### Transaction pool

| Flag                         | Default | Description                                                                                |
| ---------------------------- | ------- | ------------------------------------------------------------------------------------------ |
| `--txpool.max-account-slots` | `256`   | Max pending transactions per sender. Telcoin Network raises Reth's default of 16 to 256; any explicit value is honored, including 16 |

## Networking

### P2P transport

All peer-to-peer communication uses QUIC (v1) over UDP, managed by libp2p. Each node runs two QUIC endpoints:

| Endpoint | Conventional port | Purpose                                |
| -------- | ----------------- | -------------------------------------- |
| Primary  | UDP 49590         | Consensus headers, certificates, votes |
| Worker   | UDP 49594         | Transaction batches                    |

The node has no default for these ports. It takes them from the addresses recorded in `node-info.yaml` at key generation (see [External address configuration](#external-address-configuration)), or from `PRIMARY_LISTENER_MULTIADDR` and `WORKER_LISTENER_MULTIADDR` (worker 0) when set. Validators use the ports above by convention; other free UDP ports work if peers can reach them.

### External address configuration

On a public-facing machine, set the external addresses during key generation so other validators can reach your node:

```bash
telcoin-network keytool generate validator \
    --datadir /var/lib/telcoin \
    --address 0xYOUR_ADDRESS \
    --external-primary-addr /ip4/YOUR_PUBLIC_IP/udp/49590/quic-v1 \
    --external-worker-addrs /ip4/YOUR_PUBLIC_IP/udp/49594/quic-v1
```

If not set, addresses default to `127.0.0.1` with a random port (only useful for local testing).

### Peer discovery

Nodes discover each other through Kademlia DHT (libp2p). Bootstrap peers are loaded from the committee configuration at each epoch. Key parameters:

- K-bucket size: 20
- Record TTL: 48 hours
- Publication interval: 12 hours
- Query timeout: 60 seconds

### QUIC transport settings

| Parameter              | Value      |
| ---------------------- | ---------- |
| Handshake timeout      | 65 seconds |
| Max idle timeout       | 30 seconds |
| Keep-alive interval    | 5 seconds  |
| Max concurrent streams | 10,000     |
| Max stream data        | 50 MiB     |
| Max connection data    | 100 MiB    |

### Firewall requirements

Inbound (must be open; the P2P rows use the conventional ports, so substitute the ports in your `node-info.yaml` if you chose others):

| Port  | Protocol | Service                                                       |
| ----- | -------- | ------------------------------------------------------------- |
| 49590 | UDP      | Primary consensus P2P                                         |
| 49594 | UDP      | Worker consensus P2P                                          |
| 8545  | TCP      | HTTP RPC (if enabled; restrict to trusted sources)            |
| 8546  | TCP      | WebSocket RPC (if enabled; restrict to trusted sources)       |
| 9101  | TCP      | Prometheus metrics (if enabled; restrict to monitoring infra) |

Outbound: Unrestricted UDP for QUIC connections to peers.

These are application port requirements, not a complete production perimeter. The node does not configure host firewall rules. See [Validator production operations](../../docs/src/getting-started/validator-operations.md#firewall-configuration) for the recommended validator, sentry, observer, and management separation. Never create firewall rules from DHT or peer exchange data.

## Consensus parameters

The `parameters.yaml` file controls consensus timing and behavior. The node reads it at startup and refuses to start when the file is missing or fails to parse, or when a value fails one of the [checks that stop the node](#checks-that-stop-the-node). Duration values accept human-readable strings (e.g. `3s`, `500ms`).

| Field                                   | Default  | Description                                         |
| --------------------------------------- | -------- | --------------------------------------------------- |
| `header_num_of_batches_threshold`       | `5`      | Batch digests needed before proposing a header      |
| `max_header_num_of_batches`             | `10`     | Maximum batch digests per header                    |
| `max_header_delay`                      | `2500ms` | Maximum wait time between header proposals          |
| `min_header_delay`                      | `1000ms` | Minimum wait time; allows early header proposal     |
| `vote_timeout`                          | `5s`     | Voter-side limit per vote request; at least `max_header_delay` + [`max_header_time_drift_tolerance`](#max_header_time_drift_tolerance-network-config) (rounded up to whole seconds pre-fork) and below the 10 s libp2p request timeout |
| `gc_depth`                              | `50`     | Consensus rounds retained before garbage collection |
| `sync_retry_delay`                      | `5s`     | Delay before retrying sync requests                 |
| `sync_retry_nodes`                      | `3`      | Number of random committee nodes to query on retry  |
| `max_batch_delay`                       | `1s`     | Worker timeout before sealing a batch               |
| `max_concurrent_requests`               | `500000` | Max concurrent requests from untrusted entities     |
| `batch_vote_timeout`                    | `10s`    | Timeout for batch voting requests                   |
| `basefee_address`                       | required | Base-fee recipient; must match every peer           |
| `parallel_fetch_request_delay_interval` | `5s`     | Delay between parallel certificate fetch requests   |
| `allow_private_forward_targets`         | `false`  | Let observer forwarding dial non-public RPC hosts   |

### `allow_private_forward_targets`

An observer forwards each transaction it accepts to the JSON-RPC endpoint the owning validator advertised on its node record, so the dial target is chosen by a committee member rather than by this node. Left at the default `false`, an advertised endpoint on a loopback, private (RFC 1918), link-local, unique-local, shared-address-space or unspecified address is refused and logged once at `warn` with the advertising validator's BLS key, so a committee member cannot direct this node's outbound HTTP at hosts inside its own perimeter.

The check reads the host as written and never resolves DNS. It refuses IP literals in every spelling (dotted-quad, decimal, octal, hex, IPv4-mapped and NAT64/6to4 IPv6) and the names reserved to resolve locally (`localhost`, `*.localhost`, `*.local`), but a hostname that merely resolves to a private address is still dialed. Treat this as a guard against an advertised internal address, not as complete egress filtering. Operators who need the stronger property should restrict outbound traffic from observer nodes at the network layer.

Set it to `true` only when every committee member is under the same operator as this node - single-host and docker-compose deployments, where validators legitimately advertise `127.0.0.1`. On a public network it re-enables dialing arbitrary internal addresses.

`basefee_address` is the one field here without a default. It is consensus-critical: the
EVM credits this account on every transaction, so its balance enters the state root and every node
on the network must hold the same value. A parameters file that omits the key fails to parse, so
the node refuses to start rather than falling back in silence. The `genesis` commands write the
key for you, and the `mainnet` and `adiri` chain presets carry their own value.

Example, the testnet preset (`chain-configs/testnet/parameters.yaml`). It leaves `vote_timeout` at the 5 s default:

```yaml
---
header_num_of_batches_threshold: 5
max_header_num_of_batches: 10
max_header_delay: 3s
min_header_delay: 1s
gc_depth: 50
sync_retry_delay: 5s
sync_retry_nodes: 3
max_batch_delay: 1s
max_concurrent_requests: 500000
batch_vote_timeout:
  secs: 10
  nanos: 0
basefee_address: "0x00000000000000000000000000000000000007a0"
parallel_fetch_request_delay_interval:
  secs: 5
  nanos: 0
```

### Checks that stop the node

Beyond parsing `parameters.yaml`, the node checks the values below each time it sets up consensus for an epoch: as it starts, at every epoch boundary, and when it re-enters the current epoch after its role changes.
The two `vote_timeout` checks are the exception: they bound the node's own votes, so they run only for an epoch the node can still vote in.
An epoch that its committee has already closed, shown by a stored epoch record carrying that committee's certificate, is replayed without them; a node catching up from genesis or from an old snapshot passes through such epochs.
Validators and observers run the same checks.
When one fails, the node logs `epoch returned error` and then `Error running node:` with a message that names the field, and exits.
Fix the file and restart.

The node reads `parameters.yaml` and `network-config` only at startup, so a bad value stops the node while it starts, as it enters its first epoch.
A node that is catching up skips the `vote_timeout` checks for every epoch whose certified record it holds; records sync ahead of execution, so a bad `vote_timeout` normally stops it at the first epoch it can still vote in.
The only input to these checks that changes between epochs is whether the sub-second timestamp fork is active, and that only loosens the `vote_timeout` bound, so a value that passes them for one epoch passes them at every later epoch boundary.

| Requirement                                                              | Why                                                                                                                                                                                                  |
| ------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gc_depth` above 10                                                      | The node's activity window is `gc_depth` minus 10 rounds; at 10 or less the window is empty and the node cannot stay active                                                                         |
| `gc_depth` at most 50                                                    | The consensus-pack reader is sized for this bound, so a deeper setting could commit output that no node can reconstruct later                                                                        |
| `max_header_num_of_batches` from 1 to 10                                 | At 0 the proposer puts no batches in a header and drains no transactions; above 10, as with `gc_depth`, output could exceed what the reader reconstructs                                              |
| `header_num_of_batches_threshold` from 1 to `max_header_num_of_batches`  | At 0 the proposer seals empty headers; above the maximum the two limits contradict each other                                                                                                        |
| `min_header_delay` at most `max_header_delay`                            | The minimum is an early-proposal point inside the maximum's window; inverted, the maximum always expires first and the minimum never takes effect                                                   |
| `vote_timeout` at least `max_header_delay` plus the voter's longest drift wait | A vote request must stay open for a full header cadence plus the time the voter may spend waiting out a future-dated header. The drift wait is `max_header_time_drift_tolerance` once the sub-second timestamp fork is active for the epoch, and the tolerance rounded up to whole seconds before it (1 s for the 250 ms default) |
| `vote_timeout` below 10 s                                                | The libp2p request timeout is 10 s and covers the whole exchange; at or above it the transport cancels a slow vote before `vote_timeout` fires                                                        |

With `--chain adiri` or `--chain mainnet`, the node takes these parameters from the preset built into the binary and does not read the datadir's `parameters.yaml`, so the drift tolerance in `network-config` is the only value in these checks an operator sets.
Neither preset sets `vote_timeout`, so it is 5 s.
The testnet preset's `max_header_delay` is 3 s, which leaves room for a tolerance of up to 2 s; the mainnet preset's is 1 s, which leaves room for up to 4 s.
The default `250ms` and a legacy `1` pass with both.

One condition only logs a warning: `max_header_delay` below 1 s while the sub-second timestamp fork is not active for the epoch.
Header timestamps are then still whole seconds, so rounds can stall at second boundaries.

A `network-config` that fails to parse, including a `max_header_time_drift_tolerance` in a form the node does not accept, stops the node earlier in startup, when it reads that file.

### `max_header_time_drift_tolerance` (network-config)

How far a header's creation time may run ahead of this node's clock before the node stops waiting it out.
It is not in `parameters.yaml`.
It lives in the network config, the file `network-config` in the data directory (YAML, no file extension), under `sync_config`:

```yaml
sync_config:
  max_header_time_drift_tolerance: 250ms
```

The default is `250ms`.
Write the value as a humantime string such as `250ms`, `1s` or `1s 500ms`.
A bare whole number is read as seconds (`1` is one second); that is the format older binaries wrote, and the node logs a warning each time it reads one.
Anything else fails to parse and the node does not start: a negative number, a fraction such as `1.5`, text humantime cannot read, or the `secs:` / `nanos:` form other durations in the file use.
The node also logs a warning at startup when the value is above 1 s, because a vote can wait that long.

When a validator gets a vote request, it measures how far the header's creation time is ahead of its own clock:

- Within the tolerance, it waits out the difference, then decides whether to vote.
- Beyond the tolerance but within the tolerance plus `vote_timeout` (5.25 s at the defaults), it does not vote yet. It answers with a retryable response, the proposer is not penalized, and the proposer retries the request; by then the difference may be back within the tolerance. The validator counts these deferrals in `tn_primary_votes_deferred_future_header_total` and logs a warning once per proposer and round.
- Further ahead, it rejects the header and gives the proposer a severe peer penalty.

Before the sub-second timestamp fork is active for an epoch, header timestamps are whole seconds and the first check compares whole seconds against the tolerance rounded up.
The 250 ms default then waits out a difference of up to 1 s, the same as `1`; the rejection point is still the tolerance plus `vote_timeout`, measured in milliseconds.

The tolerance is local.
Validators with different values agree on every block; the value only changes when this validator votes and when it penalizes a proposer.
`vote_timeout` has to cover `max_header_delay` plus the longest of these waits, or the node stops (see [Checks that stop the node](#checks-that-stop-the-node)).

#### Datadirs from older binaries keep one second

The node writes `network-config` only when the file is missing, and never rewrites an existing one.
A datadir first started by a binary from before sub-second timestamps, such as `v0.15.0-adiri`, holds the whole number that binary wrote, `max_header_time_drift_tolerance: 1`, and a newer binary keeps reading it as one second.
The 250 ms default reaches only datadirs a newer binary creates.

To see the value:

```bash
grep -n max_header_time_drift_tolerance <datadir>/network-config
```

To use the default, delete that line: a missing key takes the default.
To choose a value, write it as a humantime string, for example `max_header_time_drift_tolerance: 250ms`.
The node reads the file once, at startup, so restart it after an edit.

#### Rolling back to an older binary

Binaries from before sub-second timestamps, such as `v0.15.0-adiri`, read this field as a whole number of seconds and reject a string.
A `network-config` holding a humantime value (every datadir a newer binary created, and any file edited to a value such as `250ms`) fails to parse on an older binary, and that binary does not start.
Before rolling back, do one of these:

- Set the line to a whole number of seconds, for example `max_header_time_drift_tolerance: 1`.
- Delete the line. Each binary then runs its own default: one second on the older binary, 250 ms on a newer one. The file then works with both binaries.
- Remove the file. The older binary writes a new one with its defaults, which also resets every other network setting in it.

## Monitoring

### Prometheus metrics

Enable with `--metrics <ADDR:PORT>`:

```bash
telcoin-network node --metrics 127.0.0.1:9101
```

The endpoint serves the Prometheus text format (`Content-Type: text/plain; version=0.0.4`)
and exposes two namespaces:

- `tn_*` — telcoin-network instrumentation: consensus (`tn_primary_*`), batches
  (`tn_worker_*`, `tn_batch_builder_*`), execution (`tn_engine_*`, `tn_executor_*`),
  networking (`tn_network_*`, labeled `network={primary,worker}`), epoch lifecycle
  (`tn_epoch_*`), and node health (`tn_node_*`, including `tn_node_mode` and
  `tn_node_consensus_sync_distance`).
- `reth_*` — reth's built-in instrumentation (database, transaction pool, provider) plus
  process metrics (`reth_process_*`), named identically to a stock reth node so upstream
  dashboards work unchanged.

A Grafana dashboard covering both namespaces ships at
`etc/grafana/telcoin-node-metrics.json`.

If the address cannot be bound, node startup fails — the flag is an explicit request for
the endpoint, not best-effort.

Warning: like the health check, the endpoint answers any connection without limits or
authentication. Bind to loopback and relay with a local collector (Prometheus, Grafana
Alloy), or place the port behind a firewall.

### OpenTelemetry tracing

Send traces to an OpenTelemetry collector (Jaeger, Grafana Tempo, etc.):

```bash
telcoin-network node \
    --tracing-url http://192.168.1.2:4317 \
    --node-name my-validator-01
```

Traces are exported every 30 seconds. Only spans with targets prefixed `telcoin` are included. The `--node-name` value becomes the OpenTelemetry service name.

### Health check endpoint

Enable a TCP health check for load balancers and monitoring:

```bash
telcoin-network node --healthcheck 8080
```

The endpoint binds to `0.0.0.0` on the specified port. Any TCP connection receives an `HTTP/1.1 200 OK` response with body `OK`, then the connection closes.

Warning: This endpoint has no connection limits or rate limiting. Place it behind a firewall and do not expose it to the public internet.

### Log verbosity

| Flag     | Level |
| -------- | ----- |
| `-vvv`   | INFO  |
| `-vvvv`  | DEBUG |
| `-vvvvv` | TRACE |

Use `RUST_LOG` for fine-grained control:

```bash
RUST_LOG=info,consensus=debug,evm=trace telcoin-network node ...
```

## Multi-instance setup

The `--instance` flag adjusts port numbers so multiple nodes can run on the same machine without conflicts. Instance numbers range from 1 to 200; the CLI rejects 0 before the node starts (earlier releases accepted 0 and derived conflicting ports from it). This configuration is only recommended for spawning local networks and should not be used in production environments.

Note: `--instance` does NOT offset `--metrics` (or `--healthcheck`) — pass a distinct
address per instance, as in the example below.

### Port offset formula

| Port          | Formula              | Instance 1      | Instance 2      | Instance 3      |
| ------------- | -------------------- | --------------- | --------------- | --------------- |
| HTTP RPC      | `8545 - N + 1`       | 8545            | 8544            | 8543            |
| WebSocket RPC | `8546 + (N * 2 - 2)` | 8546            | 8548            | 8550            |
| IPC path      | `/tmp/tn.ipc-{N}`    | `/tmp/tn.ipc-1` | `/tmp/tn.ipc-2` | `/tmp/tn.ipc-3` |

Example starting four validators on one machine:

```bash
for i in 1 2 3 4; do
    telcoin-network node \
        --datadir ./validators/validator-$i \
        --instance $i \
        --metrics "127.0.0.1:910$i" \
        --http \
        -vvv &
done
```

## Docker deployment

### Building the image

From the repo root:

```bash
docker build -f etc/Dockerfile -t telcoin-network:latest .
```

The build uses a two-stage Dockerfile:

1. Builder (rust:1.94-slim-bookworm): compiles the binary with `--release`
2. Production (debian:bookworm-slim): minimal image with the binary at `/usr/local/bin/telcoin`

The production image runs as a non-root user (UID 1101). The binary is installed as `/usr/local/bin/telcoin` (renamed from `telcoin-network`). The default entrypoint is `telcoin node`.

### Docker Compose

The repository includes a compose file at `etc/compose.yaml` that runs a 4-validator local network. It orchestrates setup, genesis, and node containers on a bridge network (`10.10.0.0/16`).

Key environment variables for containerized deployment:

| Variable                     | Example                             | Purpose                           |
| ---------------------------- | ----------------------------------- | --------------------------------- |
| `PRIMARY_LISTENER_MULTIADDR` | `/ip4/10.10.0.21/udp/49590/quic-v1` | QUIC endpoint for primary network |
| `WORKER_LISTENER_MULTIADDR`  | `/ip4/10.10.0.21/udp/49595/quic-v1` | QUIC endpoint for worker network  |
| `EXECUTION_ADDRESS`          | `0x1111...`                         | Fee recipient                     |
| `TN_BLS_PASSPHRASE`          | (secret)                            | BLS key passphrase                |
| `RUST_LOG`                   | `info`                              | Log level                         |

Running:

```bash
cd etc
docker compose up
```

Validators are accessible on host ports 8545-8542 (mapped from container port 8545).

## Staking registration

After key generation, export the staking arguments needed to call `ConsensusRegistry.stake()` on-chain.

```bash
telcoin-network keytool export-staking-args \
    --node-info /var/lib/telcoin/node-info.yaml
```

This command reads only public data from `node-info.yaml`. No private key, passphrase, or data directory is needed.

### Output formats

Default (human-readable):

Prints the two arguments with byte lengths and `0x`-prefixed hex values.

JSON (`--json`):

```json
{
	"blsPubkey": "0x...",
	"signature": "0x..."
}
```

Raw calldata (`--calldata`):

Single `0x`-prefixed hex string containing ABI-encoded calldata ready to submit as transaction data to `ConsensusRegistry.stake()`.

### Contract function signature

```solidity
function stake(
    bytes calldata blsPubkey,
    ProofOfPossession calldata proofOfPossession
) external payable

struct ProofOfPossession {
    bytes signature; // 48 bytes (compressed G1)
}
```

The compressed BLS public key is 96 bytes and the proof-of-possession signature is 48 bytes. The proof of possession binds the BLS key to the validator's execution address; the native precompile verifies the signature directly against the compressed `blsPubkey`.

The transaction value must equal the `stakeAmount` of the current epoch's stake version: read the version with `getCurrentStakeVersion()`, then the config with `stakeConfig(uint8)`. See [How to Stake](../../docs/src/staking/how-to-stake.md) for the full sequence.

## Observer mode

With a key outside the current committee, a node follows consensus and executes blocks without
participating in voting or block production. Role is derived from committee membership:

```bash
telcoin-network node \
    --datadir /var/lib/telcoin \
    --http
```

### Differences from a validator

| Capability                  | Validator | Observer |
| --------------------------- | --------- | -------- |
| Receive and validate blocks | Yes       | Yes      |
| Execute EVM transactions    | Yes       | Yes      |
| Serve RPC requests          | Yes       | Yes      |
| Propose consensus headers   | Yes       | No       |
| Vote on certificates        | Yes       | No       |
| Produce transaction batches | Yes       | No       |
| Committee membership        | Yes       | No       |

Observers still require key generation (`keytool generate observer`) and the genesis files. They need the same genesis config and parameters as validators.

An observer generates its own network identity keys for P2P connectivity. If its key joins the
committee, the node takes the validator role and participates once caught up. To take a validator
out of consensus, exit it on chain.

## Security considerations

### BLS key protection

- Use a strong passphrase. The default mode (`--bls-passphrase-source env`) reads from `TN_BLS_PASSPHRASE` and clears it from the process environment immediately after reading, before any threads start.
- Encryption details are described in [BLS passphrase handling](#bls-passphrase-handling).
- Set restrictive file permissions on the key directory:

```bash
umask 0077
telcoin-network keytool generate validator ...
# Or after the fact:
chmod 700 /var/lib/telcoin/node-keys
chmod 600 /var/lib/telcoin/node-keys/*
```

### Passphrase management

- Production: Use `--bls-passphrase-source env` with a secrets manager that injects `TN_BLS_PASSPHRASE` into the process environment.
- Interactive: Use `--bls-passphrase-source ask` for manual startup (requires a foreground TTY).
- CI/automated: Use `--bls-passphrase-source stdin` to pipe the passphrase from a secure source.
- Never use `--bls-passphrase-source no-passphrase` outside of testing.

### Network security

- RPC endpoints: Bind to `127.0.0.1` (the default) unless you need external access. If exposing RPC, use a reverse proxy with authentication and rate limiting. The endpoint a validator advertises is the exception: observers call it without credentials, so serve it over HTTPS with rate limiting only (see [Advertising a JSON-RPC endpoint](#advertising-a-json-rpc-endpoint)).
- Health check: The `--healthcheck` endpoint has no rate limiting (see [Health check endpoint](#health-check-endpoint)). Keep it behind a firewall.
- Metrics: Restrict Prometheus metrics to your monitoring infrastructure. Do not expose port 9101 publicly.
- P2P ports: the primary and worker UDP ports from `node-info.yaml` (49590 and 49594 by convention) must be reachable by other validators. All other ports should be firewalled.

### Proof of possession

The proof of possession (PoP) cryptographically binds a validator's BLS public key to their execution address. This prevents a malicious actor from registering someone else's BLS key with their own address. The PoP is verified during genesis and on-chain during staking registration.
