# web3

The `web3` namespace has two methods: one returns the node's version string, the other hashes data with Keccak-256. It is on by default and costs little to serve. Every node role can serve it, including a validator's minimal `eth,net,web3` selection; see [Enabling Namespaces](../enabling-namespaces.md) for the flags.

On Telcoin Network `web3_clientVersion` returns the Telcoin Network release and commit the node was built from, not a geth or reth client string.

Example values are illustrative; field names and types match what the node returns.

| Method | Description |
| --- | --- |
| [web3\_clientVersion](web3_clientversion.md) | The node's Telcoin Network version and commit sha |
| [web3\_sha3](web3_sha3.md) | Keccak-256 hash of the given data |
