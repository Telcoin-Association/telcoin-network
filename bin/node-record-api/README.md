# Node record API

`node-record-api` is a read-only HTTP directory of BLS-signed node records fetched from one
Telcoin DHT. It uses `tn-kad-client`, which depends on the shared `tn-node-record` schema
instead of the node's storage, metrics or peer manager.

Run it with bootstrap addresses for the selected chain and role. Each address must end in
`/quic-v1/p2p/PEER_ID`. IPv4, IPv6 and DNS hosts are supported. Replace the placeholders below
with an actual worker bootstrap address and an RPC endpoint on the same chain:

```sh
cargo run --release --bin node-record-api -- \
  --chain-id 2017 \
  --network worker:0 \
  --bootstrap /ip4/ADDRESS/udp/PORT/quic-v1/p2p/PEER_ID \
  --rpc-url https://YOUR_RPC_ENDPOINT \
  --keys-file validators.json \
  --bind 127.0.0.1:8080
```

The DHT is keyed by the validator's raw 96-byte compressed BLS public key. It cannot enumerate
validators. Configure at least one source:

- `--key HEX`: an explicit BLS public key, optionally prefixed with `0x`. Repeat for more keys.
- `--keys-file FILE`: a JSON array of those hex strings, reloaded each refresh.
- `--committee-file FILE`: a standard Telcoin committee YAML file, also reloaded each refresh.
- `--rpc-url URL`: query `eth_chainId`, `tn_getCurrentEpoch` and `tn_getCommitteeBlsPubkeys` over
  plain JSON-RPC. Repeat to union multiple independent live sources.

All sources are unioned. Live committees omit staked validators that are outside the committee,
so retain explicit keys or a file as a static floor when those validators must be listed. Each
reloadable source keeps its own last successful set in memory. A failed request, malformed key,
invalid file or wrong RPC chain leaves that set intact and reports an error in `sources`.
A successful new set replaces that source's previous set, including when the new set is empty.

`--network primary` reads the primary DHT. `--network worker:ID` reads that worker's DHT;
validators advertise public RPC endpoints in worker records. One daemon serves one chain and
role, and its bootstrap addresses must belong to that network.

## HTTP endpoints

- `GET /v1/records`: the complete cached snapshot, including `chain_id`, `network`,
  `refreshed_at`, source health and tracked records.
- `GET /v1/records/HEX_KEY`: one tracked key and its latest lookup status. Unknown keys return
  404; malformed keys return 400. A tracked key with no discovered record still returns 200
  with its lookup status and `record: null`.
- `GET /healthz`: 503 before the first refresh completes, then 200. This is refresh readiness;
  inspect the snapshot's source and lookup statuses for upstream failures.

Requests serve cached data and never initiate network lookups. HTTP write methods have no
route. By default, refreshes run every 60 seconds and each network operation has a 10-second
deadline. Configure these with `--refresh-seconds` and `--timeout-seconds`. Lookups run
sequentially; a slow refresh skips missed interval ticks. Each completed refresh is published
atomically, so HTTP remains responsive while the next refresh is in progress.

Each entry has a hex `key`, an optional signed `record`, its `verified_at` admission time, and
a `lookup` status (`found`, `missing` or `failed`). On a lookup failure, a previously verified
record remains available with the failure explicitly reported. Older signed copies cannot
replace a newer cached copy. A successful clean miss clears that key's cached record. Keys
no longer present in any successful or retained source set leave the directory.

## Verification and network behavior

Every returned DHT copy must match the requested key, verify under the configured chain and
role, fit `MAX_ADVERTISED_MULTIADDRS`, and have a publisher matching the signed network identity.
Multiple verified copies are folded newest-first by signed timestamp; equal timestamps retain
the first copy. Copies that fail verification are an error, distinct from a successful empty
lookup. Signatures authenticate the advertisement, not the endpoint's availability or the
validator's current stake. Advertised RPC URLs are returned as data and are never fetched.

The client generates an ephemeral identity, opens no listener, uses client-mode Kademlia,
filters inbound pushes, and disables record publication and write-back caching. It negotiates
the chain's gossipsub protocol without subscribing or publishing, avoiding validators'
`GossipsubNotSupported` penalty. No validator key or node database is required.

All retained source sets and cached records are in memory and start empty after a restart.

## Library use

External consumers can use `tn-node-record` for the schema and domain-scoped verification, or
`tn-kad-client` for the complete verified read path. For example, within a Tokio runtime:

```rust,no_run
use libp2p::Multiaddr;
use std::time::Duration;
use tn_kad_client::{Client, Error};
use tn_node_record::{NetworkType, NodeRecord};
use tn_types::BlsPublicKey;

async fn lookup(
    bootstrap: Vec<Multiaddr>,
    key: BlsPublicKey,
) -> Result<Option<NodeRecord>, Error> {
    Client::new(2017, NetworkType::Worker(0), bootstrap, Duration::from_secs(10))?
        .lookup(&key)
        .await
}
```
