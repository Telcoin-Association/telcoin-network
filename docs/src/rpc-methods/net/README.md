# net

The `net` namespace reports basic facts about the node's network: the chain id, whether the node is listening, and how many peers it is connected to. It is on by default and costs little to serve, since every answer comes from memory. Every node role can serve it, including a validator's minimal `eth,net,web3` selection; see [Enabling Namespaces](../enabling-namespaces.md) for the flags.

Telcoin Network does not run devp2p, so two methods differ from Ethereum clients: `net_peerCount` counts the peers connected to the node's worker network, and `net_listening` always returns `true`.

Example values are illustrative; field names and types match what the node returns.

| Method | Description |
| --- | --- |
| [net\_listening](net_listening.md) | Always `true` |
| [net\_peerCount](net_peercount.md) | Number of peers connected to the node's worker network, refreshed every 15 seconds |
| [net\_version](net_version.md) | Chain id as a decimal string |
