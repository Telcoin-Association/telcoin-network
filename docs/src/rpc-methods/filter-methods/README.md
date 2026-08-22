# Filter Methods

Telcoin Network supports the following filter methods over HTTP. The filter IDs can be shared by any connection using the same API key.

Filters that are not polled using [`eth_getFilterChanges`](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/filter-methods/eth\_getfilterchanges) will be automatically expired after fifteen minutes of inactivity.
