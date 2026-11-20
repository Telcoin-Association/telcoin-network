# etc files

The directory is home for developer tools, such as docker compose, Dockerfile, and executable bash scripts.

## Docker Compose

`compose.yaml` is a Docker Compose V2 file for a local testnet: `make up` starts it and `make down` removes it.
It runs the mainnet topology: every validator behind its own worker gateway, and an observer node that answers the reads.
Clients talk to the gateways on host ports 8545-8542, not to the validators.

### Topology

| Service | Bridge address | Role |
| --- | --- | --- |
| `validator1`..`validator4` | `10.10.0.21`-`10.10.0.24` | The committee. |
| `observer` | `10.10.0.31` | Follows the chain outside the committee and answers every call the gateways do not send to a validator. |
| `worker-gateway1`..`worker-gateway4` | `10.10.0.41`-`10.10.0.44` | One in front of each validator: transaction submissions go to its validator, every other call to the observer (`--redirect-queries http://10.10.0.31:8545`). |
| `node-record-api` | `10.10.0.51` | Reads the validators' signed node records from the worker DHT and serves them, with the RPC URLs they advertise, as JSON. |

Before the nodes start, one-shot services prepare them.
`telcoin-base` builds the node image.
`setup1`..`setup4` and `setup-observer` run `setup_validator.sh` to generate each node's keys and `node-info.yaml`, once per volume.
`comittee` (container `genesis`) runs `genesis.sh`: it writes the genesis from the four validators' `node-info.yaml` and gives every node the chain config.
The observer's `node-info.yaml` never enters the genesis, so it is not a committee member.

Each validator advertises its gateway's bridge address (`http://10.10.0.41:8545/` for validator1, and so on) in its node record, as a mainnet validator advertises its public gateway.
That URL is written with the keys at the first `make up`, so a change to `RPC_HTTP` in `compose.yaml` takes effect after `make down`.

### Endpoints

| Host address | Service | What answers |
| --- | --- | --- |
| `8545`, `8544`, `8543`, `8542`, every interface | `worker-gateway1`..`worker-gateway4` | JSON-RPC over HTTP for clients, and the gateway's `/health`, `/ready` and `/ready/any` |
| `127.0.0.1:8541` | `observer` | The observer's own JSON-RPC, `tn_*` included |
| `127.0.0.1:18545`, `18544`, `18543`, `18542` | `validator1`..`validator4` | Debug ports: the validator's own JSON-RPC, bypassing its gateway |
| `127.0.0.1:8090` | `node-record-api` | `/healthz`, `/readyz`, `/v1/records`, `/v1/rpcs` |
| `127.0.0.1:9090`, `127.0.0.1:3000`, `127.0.0.1:9100` | Prometheus, Grafana, node_exporter | Only with `--profile observability` |

Use `127.0.0.1` for the loopback ports, not `localhost`, which may resolve to `::1`, where only the gateway ports listen.
Use the debug ports to compare a validator's own answer with what clients get; a test of the client path goes through a gateway.

Nothing else is published to the host.
The validators and the observer serve health on `<bridge address>:8080`, and every node, gateway and `node-record-api` serves metrics on `<bridge address>:9101/metrics`.
On a Linux host the bridge addresses are reachable from the host, as in `curl -s http://10.10.0.41:9101/metrics`.
A gateway's probe listener is `127.0.0.1:8546` inside its container, where the compose healthcheck reads `/ready/any`.

### Starting and stopping

`make up` runs `docker compose -f ./etc/compose.yaml up --build --remove-orphans --detach`.
It builds three images from the working tree (the node, `worker-gateway` and `node-record-api`), so the first run takes a while and later runs reuse Docker's build cache.
`genesis` waits for the four validator setups to exit 0, and the validators and the observer wait for `genesis` to exit 0, so a failed one-shot makes `make up` fail and the nodes after it do not start.
`make up` returns once the containers have started, before they are ready.

Wait for the services to report `healthy` before sending traffic:

```sh
docker inspect -f '{{.State.Health.Status}}' worker-gateway1
```

The gateways turn healthy about 12 seconds after `make up` returns; until then a gateway's `/ready` answers `503`.
`node-record-api` turns healthy within a few seconds, and its `/readyz` then reports `{"status":"ready","cycles_completed":1,"records_cached":4}`.

These are normal and not failures:

- `docker compose -f etc/compose.yaml ps -a` lists `etc-telcoin-base-1` as `Exited (1)`: that service only builds the node image.
  The setup services and `genesis` show `Exited (0)`.
- Each gateway logs one INFO line at startup, `could not compare the chain ids of --redirect-queries and the worker; continuing without the check`, when its validator was not listening yet.
- `node-record-api` logs one WARN at startup, `key source refresh failed; reusing its last good set source="rpc"`, because the observer's RPC is not answering yet; the next refresh succeeds.

The chain is slow when idle.
Epochs last 60 seconds, and with no transactions block 3 appears about three minutes after start.
A transaction sent through a gateway is mined within about two seconds, so a test that needs blocks should send transactions rather than wait.

A second `make up` keeps the genesis and the chain; the `genesis` container logs `keeping the genesis in /home/nonroot/data/genesis`.
Because `make up` passes `--build`, a second `make up` after the source tree changed rebuilds the node image and recreates the validators and the observer, which restart on their existing data.

`make down` removes the containers and the volumes: the keys, the genesis and the chain are gone, and the next `make up` starts a new network.
A stack started with the observability profile comes down with `docker compose -f etc/compose.yaml --profile observability down --remove-orphans -v`.

### Routing through a gateway

A gateway sends `eth_sendRawTransaction` and `eth_sendRawTransactionSync` to its validator, and every other call to the observer, `tn_*` calls included.
`bin/worker-gateway/README.md` gives the full rules ("Query redirect", "Reads kept on the worker" and "Split routing"); these are the ones a test is most likely to meet.

- Reads come from the observer, which follows the chain from the validators, so a read can lag what the validators have executed.
- The observer has not seen the validator's transaction pool.
  So `eth_getTransactionCount` at `"pending"`, and `eth_getTransactionByHash` or `eth_getTransactionReceipt` for a hash the same gateway forwarded in the last 60 seconds, go to the validator.
  Every other read at `pending` (`eth_call`, `eth_getBalance`, `eth_estimateGas`) and a nonce read at `latest` go to the observer and do not see transactions still in the pool.
  A client that sends several transactions in a row should read its nonce at `"pending"` or track it.
- A batch goes to the validator only when every call in it is a submission.
  Any other batch goes whole to the observer, a mixed batch's submissions included.
- A batch longer than 50 calls, or a request body over 1 MiB, gets `413` / `-32003` and is not forwarded.
  The limits being off does not lift these caps (see [Edge limits](#edge-limits)).
- With the observer down, a read through a gateway fails with HTTP `504` and `{"error":{"code":-32002,"message":"upstream request timed out"},...}`.
  It is never retried on the validator.
  A pending-nonce read still answers, from the validator, and `/ready/any` stays `200`, because the gateway can still take submissions.
  Only submissions and those reads reach the validator, so a client that reads the chain id or fee data before it signs fails on those reads.
- The observer is the one read backend of all four gateways.
  When reads fail under load, check the observer before the gateways' own limits: `tn_worker_gateway_rejections_total{reason="rate_limited"}` counts a gateway's own `429`s, and `{reason="overloaded"}` its capacity `503`s.

A transaction sent to the observer on `127.0.0.1:8541` reaches a validator as it would on mainnet.
The observer forwards it to the RPC URL advertised by the validator that owns the sender, here that validator's gateway, and the gateway passes it to the validator: only that gateway's `route="worker"` counter moves, by one per transaction.
This works only because `genesis.sh` sets `allow_private_forward_targets: true` in the observer's own `parameters.yaml`.
The gateways' addresses are private, and without the setting the observer refuses to forward to them and logs `refusing to forward to a non-public advertised RPC endpoint`.
The setting is for this local bridge network only: never copy it into a real node's `parameters.yaml`.
On a public network it lets a committee member point the node's outbound HTTP at hosts inside the node's own network.
The validators keep the default, `false`.

`docker compose stop observer` also stops the services that depend on it; to take the observer away from a running stack, use `docker stop observer` and `docker start observer`.

### Edge limits

The gateways run with their rate limits and their per-client connection cap off.
`WORKER_GATEWAY_RATE_LIMIT_PER_IP`, `WORKER_GATEWAY_RATE_LIMIT_GLOBAL` and `WORKER_GATEWAY_MAX_CONNECTIONS_PER_IP` default to `0` in `compose.yaml`, because every client on this host reaches a gateway from one address, which the production per-client limits would throttle.
A gateway logs its setting at startup, `rate_limiting=false` and `max_connections_per_ip=None` with the limits off.

The production values are 100 requests per second per client, 3000 per gateway, and 32 connections per client.
To run with them, set the three variables in the shell or in `etc/.env` before `make up`, or recreate gateways on a running stack:

```sh
WORKER_GATEWAY_RATE_LIMIT_PER_IP=100 WORKER_GATEWAY_RATE_LIMIT_GLOBAL=3000 WORKER_GATEWAY_MAX_CONNECTIONS_PER_IP=32 \
    docker compose -f etc/compose.yaml up -d --no-deps worker-gateway1
```

`--no-deps` leaves the validators and the observer running; name more gateways to recreate them too.
The recreated gateway logs `rate_limiting=true` and `max_connections_per_ip=Some(32)`.
The same command without the variables turns its limits off again.

With the limits off, a burst of 2000 `eth_chainId` calls at 64 in parallel through one gateway gets 2000 answers.
With the production values the same burst got 923 answers and 1077 `429`s, and `tn_worker_gateway_rejections_total{reason="rate_limited"}` read 1077, while `worker-gateway2`, left with the limits off, answered every call of a 500-call burst.
The split depends on how fast the burst runs: the per-client bucket holds 200 calls and refills at 100 a second.

The capacity caps keep the gateway's defaults whatever the three variables say:

| Flag | Default | Over it |
| --- | --- | --- |
| `--max-connections` | 500 connections | New connections wait in the kernel's accept backlog |
| `--max-inflight-submissions` | 256 requests | `503` / `-32009` |
| `--max-inflight-queries` | 256 requests | `503` / `-32009` |
| `--max-upstream-inflight` | 100 requests to the validator, of which the pending-nonce and recent-hash reads may hold 20 | `503` / `-32009` |
| `--max-batch-len` | 50 calls | `413` / `-32003` |
| `--max-request-bytes` | 1 MiB | `413` / `-32003` |
| `--max-inflight-request-bytes` | 512 MiB across requests in flight | `503` / `-32010` |

A load test that gets `503` / `-32009` has reached a capacity cap, not a rate limit.

### Funded account

The genesis funds one account with 1 billion TEL, chosen by `TN_DEV_FUNDED_ACCOUNT` in the shell or in `etc/.env`.
The default, `0x3DCc9a6f3A71F0A6C8C659c65558321c374E917a`, has no key in this repository, so to sign transactions, fund an address whose key you hold:

```sh
make down
TN_DEV_FUNDED_ACCOUNT=0xYOUR_ADDRESS make up
```

The value goes to `telcoin genesis --dev-funded-account`: a `0x` address is funded as written, and any other word funds an address derived from it deterministically.
A value that starts with `0x` but is not a valid address stops genesis: the `genesis` container exits 101 with `not a valid account!`, `make up` fails, and no validator starts.
The variable applies to a new genesis only; on a stack that already has one, `make up` keeps it, so run `make down` first.
The ConsensusRegistry owner stays `0x3DCc9a6f3A71F0A6C8C659c65558321c374E917a` whatever the funded account.

### Checking the gateway path

These checks confirm that clients reach the validators through the gateways and that reads come from the observer.
They read the gateways' metrics on their bridge addresses, so run them on a Linux host.
`ADDR` and `KEY` are the funded account (see [Funded account](#funded-account)) and its private key.

```sh
ADDR=0xYOUR_ADDRESS
KEY=0xYOUR_PRIVATE_KEY
rpc() { curl -s -m 8 -H 'content-type: application/json' -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$2\",\"params\":${3:-[]}}" "http://127.0.0.1:$1"; echo; }
```

Every gateway is ready, and can serve both routes:

```sh
for p in 8545 8544 8543 8542; do curl -s -o /dev/null -w '%{http_code}\n' "http://127.0.0.1:$p/ready"; done
curl -s http://127.0.0.1:8545/ready/any
```

Each `/ready` prints `200`, and `/ready/any` prints `{"ready":true,"submissions":true,"queries":true}`.

A read goes to the observer:

```sh
rpc 8545 eth_chainId
curl -s http://10.10.0.41:9101/metrics | grep 'routed_requests_total{'
rpc 8545 tn_getCurrentEpochInfo
```

The chain id is `0x1e7`.
On a fresh stack the metrics then show one line, `tn_worker_gateway_routed_requests_total{route="query",result="forwarded"} 1`: a read moves the `query` count and never the `worker` count.
`tn_getCurrentEpochInfo` answers with a result object, from the observer.

A submission goes to the validator:

```sh
docker run --rm --network host ghcr.io/foundry-rs/foundry:stable \
    "cast send --rpc-url http://127.0.0.1:8545 --private-key $KEY --value 1wei --timeout 180 --json 0x000000000000000000000000000000000000dEaD"
curl -s http://10.10.0.41:9101/metrics | grep 'routed_requests_total{'
```

The receipt carries `"status":"0x1"`, and the transaction is mined within about two seconds.
The `route="worker"` count of `worker-gateway1` rises by 11 or 12: besides the submission, `cast` reads its nonce at `pending` and polls the receipt of the hash it sent, and those reads go to the validator too.
Repeat with ports 8544, 8543 and 8542, and metrics on `10.10.0.42` to `10.10.0.44`, for the other gateways.

The debug ports are on loopback and answer directly:

```sh
docker port validator1 8545/tcp
rpc 18545 eth_blockNumber
```

The port prints `127.0.0.1:18545`.

Reads fail without the observer, and are never sent to the validator instead:

```sh
docker stop observer
rpc 8545 eth_blockNumber
rpc 8545 eth_getTransactionCount "[\"$ADDR\",\"pending\"]"
docker start observer
```

The block-number read gets HTTP `504` and `{"error":{"code":-32002,"message":"upstream request timed out"},"id":1,"jsonrpc":"2.0"}`.
The pending-nonce read still answers, from the validator.
After `docker start observer` the observer is healthy again in about 12 seconds, and reads through the gateway answer again.

A transaction sent to the observer reaches a validator through that validator's gateway:

```sh
docker run --rm --network host ghcr.io/foundry-rs/foundry:stable \
    "cast send --rpc-url http://127.0.0.1:8541 --private-key $KEY --value 1wei --timeout 180 --json 0x000000000000000000000000000000000000dEaD"
curl -s http://10.10.0.31:9101/metrics | grep -i 'forward' | grep -v '^#'
docker logs observer 2>&1 | grep -c 'refusing to forward'
for n in 1 2 3 4; do curl -s "http://10.10.0.4$n:9101/metrics" | grep 'routed_requests_total{'; done
```

The receipt carries `"status":"0x1"`.
The observer's `tn_reth_forwarded_txns_queued_total` reads 1, every `tn_reth_forwarded_txns_dropped_total` reads 0, and the log has no `refusing to forward` line (`0`).
One gateway's `route="worker"` count has risen by one.

`node-record-api` lists the four validators and their gateways:

```sh
curl -s http://127.0.0.1:8090/readyz
curl -s http://127.0.0.1:8090/v1/records
curl -s http://127.0.0.1:8090/v1/rpcs
```

`/readyz` prints `{"status":"ready","cycles_completed":1,"records_cached":4}` after the first refresh, and `cycles_completed` counts up with each 30-second refresh after that.
`/v1/records` has `chain_id` 487, `keys_tracked` 4 and four records, none of them stale.
`/v1/rpcs` lists the same four records, whose `rpc.http` values are `http://10.10.0.41:8545/` to `http://10.10.0.44:8545/`.
It keeps answering while the observer is stopped.

The limits are off, and the production values throttle one client:

```sh
seq 2000 | xargs -P 64 -I{} curl -s -m 20 -o /dev/null -w '%{http_code}\n' -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' http://127.0.0.1:8545 | sort | uniq -c
WORKER_GATEWAY_RATE_LIMIT_PER_IP=100 WORKER_GATEWAY_RATE_LIMIT_GLOBAL=3000 WORKER_GATEWAY_MAX_CONNECTIONS_PER_IP=32 \
    docker compose -f etc/compose.yaml up -d --no-deps worker-gateway1
docker inspect -f '{{.State.Health.Status}}' worker-gateway1
seq 2000 | xargs -P 64 -I{} curl -s -m 20 -o /dev/null -w '%{http_code}\n' -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}' http://127.0.0.1:8545 | sort | uniq -c
curl -s http://10.10.0.41:9101/metrics | grep -E 'rejections_total|requests_total|connections_refused_total' | grep -v '^#'
```

The first burst prints one line, `2000 200`: every call was answered.
Once the recreated gateway is `healthy`, the second burst prints a count of `200` and a count of `429` (`923 200` and `1077 429` in one run), and the metrics show `tn_worker_gateway_rejections_total{reason="rate_limited"}` and `tn_worker_gateway_requests_total{outcome="rejected"}` at the `429` count.
The share of `429` depends on how fast the host sends the burst: another run got `292 200` and `1708 429`.
The per-IP connection cap shows only when one client holds more connections open than the cap: with 48 connections held open from the host, the gateway served 32 and refused 16, and `tn_worker_gateway_connections_refused_total{reason="per_ip_cap"}` read 16.
Recreating the gateway without the three variables turns the limits off again.

`make down` ends the check and removes everything it created.

### Metrics and dashboards

The `observability` profile adds node_exporter, Prometheus and Grafana, all in the host network namespace and bound to loopback:

```sh
docker compose -f etc/compose.yaml --profile observability up --build --remove-orphans --detach
curl -s --get --data-urlencode 'query=up' http://127.0.0.1:9090/api/v1/query
curl -s -u admin:telcoin-local http://127.0.0.1:3000/api/search
```

Prometheus scrapes 11 targets, each with `up` at 1: the four validators and the observer in job `telcoin-node`, the four gateways in job `worker-gateway`, `node-record-api`, and node_exporter in job `node`.
Grafana provisions the "Telcoin Node Metrics" and "Telcoin Worker Gateway" dashboards; its admin password is `GF_SECURITY_ADMIN_PASSWORD`, `telcoin-local` by default.
The observer reports `tn_network_committee_peers_expected` 0, so the committee alerts skip it.
See [Host network observability](../docs/src/getting-started/host-network-observability.md) and [Private observability listeners](../docs/src/getting-started/observability-listeners.md).

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
