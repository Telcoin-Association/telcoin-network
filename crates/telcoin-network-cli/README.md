# Validator Node Operator Guide

Technical reference for running a Telcoin Network validator node. Covers key generation, genesis ceremony, node startup, configuration, networking, monitoring, and troubleshooting.

The `telcoin-network` binary has three subcommands:

- keytool: generate cryptographic keys and export staking arguments
- genesis: produce the chain genesis from validator configs (only applicable for new network deployments)
- node: run the validator or observer node

## Prerequisites

Hardware (recommended minimums):

| Resource | Minimum    | Recommended   |
| -------- | ---------- | ------------- |
| CPU      | 4 cores    | 8+ cores      |
| RAM      | 16 GB      | 32 GB         |
| Disk     | 500 GB SSD | 1 TB NVMe SSD |
| Network  | 100 Mbps   | 1 Gbps        |

Software:

- Linux (Debian/Ubuntu recommended) or macOS
- Rust 1.94+ toolchain (if building from source)
- Docker 24+ (if using container deployment)
- `cmake`, `libclang-16-dev`, `pkg-config`, `libssl-dev`, `libapr1-dev` (build dependencies on Debian)

Build from source:

```bash
cargo build --bin telcoin-network --release
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
- `p2p_info.worker`: worker network multiaddr and Ed25519 public key
- `execution_address`: EVM address that receives block rewards
- `proof_of_possession`: BLS signature binding the public key to the execution address

Example:

```yaml
name: "node-JMQq7ZqVT"
bls_public_key: "mCss5AWBd69e6Na..."
p2p_info:
  primary:
    network_address: "/ip4/34.31.250.229/udp/49590/quic-v1/p2p/12D3KooW..."
    network_key: "4XTTM1f3EZanf..."
  worker:
    network_address: "/ip4/34.31.250.229/udp/49594/quic-v1/p2p/12D3KooW..."
    network_key: "4XTTMD3rST8E7..."
execution_address: "0xefaacf04b92298a88200aa50aa6bb7bfce587b17"
proof_of_possession: "kFa9r..."
```

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
    --epoch-duration-in-secs 86400
```

### genesis flags

| Flag                                                 | Default        | Description                                                                               |
| ---------------------------------------------------- | -------------- | ----------------------------------------------------------------------------------------- |
| `--chain-id`                                         | `2017` (0x7e1) | Numeric chain ID. Accepts decimal or `0x`-prefixed hex                                    |
| `--consensus-registry-owner`                         | `0x...07a0`    | Owner address for the ConsensusRegistry contract. Use a governance multisig in production |
| `--basefee-address`                                  | `0x...07a0`    | Address that receives all transaction base fees                                           |
| `--initial-stake-per-validator`, `--stake`           | `1000000`      | TEL staked per validator at genesis (input in whole TEL, stored as wei)                   |
| `--min-withdraw-amount`, `--min_withdraw`            | `1000`         | Minimum TEL withdrawal amount                                                             |
| `--epoch-block-rewards`, `--block_rewards_per_epoch` | `25806`        | Total block rewards per epoch in TEL                                                      |
| `--epoch-duration-in-secs`, `--epoch_length`         | `86400`        | Epoch duration in seconds (default: 24 hours)                                             |
| `--max-header-delay-ms`                              | none           | Max delay between header proposals (milliseconds)                                         |
| `--min-header-delay-ms`                              | none           | Min delay between header proposals (milliseconds)                                         |
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

Available named chains: `adiri` (alias: `testnet`), `mainnet`.

The `--chain` flag overrides local genesis files with the embedded config for that network.

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
| `--chain`             | none           | Join a named network (`adiri`, `testnet`, `mainnet`)                                           |
| `--instance`          | none           | Instance number (0-200) for port offsetting. See [Multi-instance setup](#multi-instance-setup) |
| `--observer`          | `false`        | Run as an observer (no consensus participation)                                                |
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
| `--rpc.txfeecap`                         | `1.0` (ETH)   | Max transaction fee via RPC (0 = no cap) |

### available RPC modules

`eth`, `net`, `web3`, `debug`, `trace`, `rpc`

The `admin` and `txpool` modules are not available at this time.

### Transaction pool

| Flag                         | Default | Description                                                                                |
| ---------------------------- | ------- | ------------------------------------------------------------------------------------------ |
| `--txpool.max-account-slots` | `256`   | Max pending transactions per sender (Reth default is 16; Telcoin Network overrides to 256) |

## Networking

### P2P transport

All peer-to-peer communication uses QUIC (v1) over UDP, managed by libp2p. Each node runs two QUIC endpoints:

| Endpoint | Default port | Purpose                                |
| -------- | ------------ | -------------------------------------- |
| Primary  | UDP 49590    | Consensus headers, certificates, votes |
| Worker   | UDP 49595    | Transaction batches                    |

### External address configuration

On a public-facing machine, set the external addresses during key generation so other validators can reach your node:

```bash
telcoin-network keytool generate validator \
    --datadir /var/lib/telcoin \
    --address 0xYOUR_ADDRESS \
    --external-primary-addr /ip4/YOUR_PUBLIC_IP/udp/49590/quic-v1 \
    --external-worker-addrs /ip4/YOUR_PUBLIC_IP/udp/49595/quic-v1
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

Inbound (must be open):

| Port  | Protocol | Service                                                       |
| ----- | -------- | ------------------------------------------------------------- |
| 49590 | UDP      | Primary consensus P2P                                         |
| 49595 | UDP      | Worker consensus P2P                                          |
| 8545  | TCP      | HTTP RPC (if enabled; restrict to trusted sources)            |
| 8546  | TCP      | WebSocket RPC (if enabled; restrict to trusted sources)       |
| 9101  | TCP      | Prometheus metrics (if enabled; restrict to monitoring infra) |

Outbound: Unrestricted UDP for QUIC connections to peers.

## Consensus parameters

The `parameters.yaml` file controls consensus timing and behavior. If the file is absent, defaults are used. Duration values accept human-readable strings (e.g. `3s`, `500ms`).

| Field                                   | Default  | Description                                         |
| --------------------------------------- | -------- | --------------------------------------------------- |
| `header_num_of_batches_threshold`       | `5`      | Batch digests needed before proposing a header      |
| `max_header_num_of_batches`             | `10`     | Maximum batch digests per header                    |
| `max_header_delay`                      | `2500ms` | Maximum wait time between header proposals          |
| `min_header_delay`                      | `1000ms` | Minimum wait time; allows early header proposal     |
| `gc_depth`                              | `50`     | Consensus rounds retained before garbage collection |
| `sync_retry_delay`                      | `5s`     | Delay before retrying sync requests                 |
| `sync_retry_nodes`                      | `3`      | Number of random committee nodes to query on retry  |
| `max_batch_delay`                       | `1s`     | Worker timeout before sealing a batch               |
| `max_concurrent_requests`               | `500000` | Max concurrent requests from untrusted entities     |
| `batch_vote_timeout`                    | `10s`    | Timeout for batch voting requests                   |
| `basefee_address`                       | none     | Address that receives transaction base fees         |
| `parallel_fetch_request_delay_interval` | `5s`     | Delay between parallel certificate fetch requests   |

Example (testnet configuration):

```yaml
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

## Monitoring

### Prometheus metrics

Enable with `--metrics <ADDR:PORT>`:

```bash
telcoin-network node --metrics 127.0.0.1:9101
```

Scrape the endpoint with Prometheus or any compatible collector.

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

The `--instance` flag adjusts port numbers so multiple nodes can run on the same machine without conflicts. Instance numbers range from 0 to 200.

### Port offset formula

| Port          | Formula              | Instance 1      | Instance 2      | Instance 3      |
| ------------- | -------------------- | --------------- | --------------- | --------------- |
| HTTP RPC      | `8545 - N + 1`       | 8545            | 8544            | 8543            |
| WebSocket RPC | `8546 + (N * 2 - 2)` | 8546            | 8548            | 8550            |
| IPC path      | `/tmp/tn-{N}.ipc`    | `/tmp/tn-1.ipc` | `/tmp/tn-2.ipc` | `/tmp/tn-3.ipc` |

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

Prints the three arguments with byte lengths and `0x`-prefixed hex values.

JSON (`--json`):

```json
{
	"blsPubkey": "0x...",
	"uncompressedPubkey": "0x...",
	"uncompressedSignature": "0x..."
}
```

Raw calldata (`--calldata`):

Single `0x`-prefixed hex string containing ABI-encoded calldata ready to submit as transaction data to `ConsensusRegistry.stake()`.

### Contract function signature

```solidity
function stake(
    bytes calldata blsPubkey,
    ProofOfPossession calldata proofOfPossession
) public

struct ProofOfPossession {
    bytes uncompressedPubkey;    // 192 bytes
    bytes uncompressedSignature; // 96 bytes
}
```

The compressed BLS public key is 96 bytes. The proof of possession binds the BLS key to the validator's execution address.

## Observer mode

Run a node that follows consensus and executes blocks without participating in voting or block production:

```bash
telcoin-network node \
    --datadir /var/lib/telcoin \
    --observer \
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

An observer generates its own network identity keys for P2P connectivity but never participates in the consensus committee, regardless of whether it is registered on-chain.

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

- RPC endpoints: Bind to `127.0.0.1` (the default) unless you need external access. If exposing RPC, use a reverse proxy with authentication and rate limiting.
- Health check: The `--healthcheck` endpoint has no rate limiting (see [Health check endpoint](#health-check-endpoint)). Keep it behind a firewall.
- Metrics: Restrict Prometheus metrics to your monitoring infrastructure. Do not expose port 9101 publicly.
- P2P ports: UDP 49590 and 49595 must be reachable by other validators. All other ports should be firewalled.

### Proof of possession

The proof of possession (PoP) cryptographically binds a validator's BLS public key to their execution address. This prevents a malicious actor from registering someone else's BLS key with their own address. The PoP is verified during genesis and on-chain during staking registration.
