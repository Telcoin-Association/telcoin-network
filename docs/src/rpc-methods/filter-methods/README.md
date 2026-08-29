# Filter Methods

Telcoin Network supports the following filter methods over HTTP. Filters are held in the memory of the node that created them, so subsequent polls must go to the same node.

Filters that are not polled using [`eth_getFilterChanges`](eth_getfilterchanges.md) will be automatically expired after five minutes of inactivity.
