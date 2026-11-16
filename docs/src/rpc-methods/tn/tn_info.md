# tn\_info

Returns the identity a node publishes about itself: the chain it belongs to, its software version and name, its BLS public key and authority id, the execution address that receives its rewards, and the network keys and external addresses peers use to reach its primary and worker. The answer does not change while the node runs; the node's current consensus participation mode is served by [tn\_nodeMode](tn_nodemode.md) instead.

Each worker runs its own RPC server, and `worker_network_key` and `worker_external_address` belong to the worker whose server answered. Every other field is the same on all of a node's workers.

#### Parameters

`None`

#### Returns

`Object` - The node information object. Unlike most `tn` results, its field names are snake\_case:

* `chain_id`: `Number` - The chain id, as a JSON number.
* `version`: `String` - The node's software version, `<crate version> (<git commit sha>)`.
* `name`: `String` - The node's name. Defaults to `node-` followed by the base58 encoding of the first 8 bytes of the BLS public key; the operator can set another name.
* `bls_public_key`: `String` - The node's BLS12-381 public key, base58 encoding of its 96-byte compressed form.
* `authority_id`: `String` - The node's consensus authority id, a 32-byte hash of its BLS public key, base58-encoded.
* `execution_address`: `DATA`, 20 bytes - The address that receives the node's rewards when it serves in a committee.
* `primary_network_key`: `String` - The public key of the node's primary network identity, base58 encoding of the libp2p protobuf key encoding.
* `worker_network_key`: `String` - The public key of this worker's network identity, in the same encoding.
* `primary_external_address`: `String` - The multiaddr peers use to reach the node's primary.
* `worker_external_address`: `String` - The multiaddr peers use to reach this worker.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_info","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "chain_id": 2017,
    "version": "0.1.0 (5736cc30012c5ff25913898e318a74df308f13d9)",
    "name": "node-WmSeweurYc6",
    "bls_public_key": "24JaKrebALhvhXxsAabjd5ZK22k1W4zKwGepKD5pcobQwBkZyiruYyhRM3hkLap8Ssc4qUdcRUJSrs2389hUoV7G83HKYrmkz2pTzCJiQURV5vtkrh8gFRKBQWKFTWLWah9s",
    "authority_id": "G6A8BRn31vofiVH8KZzETW2kcPsbomNTQYMgvZg52jTg",
    "execution_address": "0x3518b301b86ceb53b5a3dff62e55cd43ef59d024",
    "primary_network_key": "4XTTMJFzopXoTVayf2m4wE69GCqMXW3wUnimiU3CdK5zHL36j",
    "worker_network_key": "4XTTM5m63vU9mmcv8yAmFEpc946PWotoFWDb4TyqRz7ayKJUJ",
    "primary_external_address": "/ip4/34.102.122.126/udp/49590/quic-v1/p2p/12D3KooWSwK9yPLNtDr116cX74FS4Ehs2Nx2fWAUjpnsGrNkNGKd",
    "worker_external_address": "/ip4/34.102.122.126/udp/49594/quic-v1/p2p/12D3KooWESQQ5KghAFnUwWJq7niJuVjrLDooNzypjmRfwsySMXhC"
  }
}
```
