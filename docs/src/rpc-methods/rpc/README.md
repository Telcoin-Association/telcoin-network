# rpc

The `rpc` namespace has one method, `rpc_modules`, which lists the namespaces the node serves. It is on by default and costs almost nothing to serve, since the node builds the answer once when the RPC server starts. Public RPC nodes and archive nodes should keep it so clients and operators can check what a node serves; a validator's minimal `eth,net,web3` selection leaves it out. See [Enabling Namespaces](../enabling-namespaces.md) for the flags.

Example values are illustrative; field names and types match what the node returns.

| Method | Description |
| --- | --- |
| [rpc\_modules](rpc_modules.md) | The namespaces the node serves, each with the version `"1.0"` |
