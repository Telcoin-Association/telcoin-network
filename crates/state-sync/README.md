# State-Sync

This lib is used by the node to start each epoch in the correct state.

When nodes are participating in consensus as active committee-voting validators (CVV), they only use the `consensus/executor/subscriber`.
For syncing CVVs (inactive) and observers, state-sync is responsible for querying peers to fetch the latest output from consensus.
