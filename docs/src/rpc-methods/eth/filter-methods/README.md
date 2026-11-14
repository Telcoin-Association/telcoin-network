# Filter Methods

Telcoin Network supports the following filter methods over HTTP. Filters are held in the memory of the node that created them, so subsequent polls must go to the same node.

Filters that are not polled using [`eth_getFilterChanges`](eth_getfilterchanges.md) will be automatically expired after five minutes of inactivity.

Each worker's RPC endpoint keeps at most `--rpc.max-active-filters` filters (1,000 by default) across all its clients, first come, first served: one client can install them all. Once that many are installed, creating another fails with a server-busy error (-32009) until a filter is uninstalled or expires. Each worker's transaction pool also bounds the listeners that pending-transaction filters and `newPendingTransactions` subscriptions register (1,024 of each kind, the node's own listeners included, also first come, first served), with the same error.

A log filter may name at most 1,000 addresses and at most 1,000 values in each topic position, the limit geth applies by default. `eth_newFilter`, `eth_getLogs` and `logs` subscriptions refuse a larger filter with an invalid-params error (-32602).
