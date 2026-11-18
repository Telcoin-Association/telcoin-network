#!/bin/bash
# Generates a node's keys and node-info.yaml in its datadir, once. NODE_TYPE is `validator` (the
# default) or `observer`. Both get the same kind of keys: a node joins the committee only when
# genesis.sh puts its node-info into the genesis.
set -euo pipefail

USER_ID=1101
NODE_TYPE="${NODE_TYPE:-validator}"

# Ensure the data directory exists and is owned by nonroot user
if [ ! -d /home/nonroot/data/node-keys ]; then
    echo "creating $NODE_TYPE keys"
    export TN_BLS_PASSPHRASE="local"
    /usr/local/bin/telcoin keytool generate "$NODE_TYPE" --datadir /home/nonroot/data --address "${EXECUTION_ADDRESS}" --external-primary-addr "${PRIMARY_LISTENER_MULTIADDR}" --external-worker-addrs "${WORKER_LISTENER_MULTIADDR}"

    chown -R "${USER_ID}:${USER_ID}" /home/nonroot/data

    echo "Keys generated and ownership/permissions set"

    ls -la /home/nonroot/data/
    ls -la /home/nonroot/data/node-keys/
else
    echo "Setup already complete"
    ls -la /home/nonroot/data/
    ls -la /home/nonroot/data/node-keys/ 2>/dev/null || echo "node-keys directory not found"
fi
