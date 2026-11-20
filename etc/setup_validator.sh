#!/bin/bash
# Generates a node's keys and node-info.yaml in its datadir, once. NODE_TYPE is `validator` (the
# default) or `observer`. Both get the same kind of keys: a node joins the committee only when
# genesis.sh puts its node-info into the genesis.
#
# RPC_HTTP, when set, is the JSON-RPC URL the node advertises in its node record: observers forward
# the transactions they accept to the advertised URL of the validator that owns the sender. It is
# written into node-info.yaml with the keys, so a datadir that has keys keeps its URL until
# `make down`, or until `telcoin keytool set-rpc` rewrites node-info.yaml and the node restarts.
set -euo pipefail

USER_ID=1101
NODE_TYPE="${NODE_TYPE:-validator}"
NODE_INFO=/home/nonroot/data/node-info.yaml

rpc_args=()
if [ -n "${RPC_HTTP:-}" ]; then
    rpc_args=(--rpc-http "$RPC_HTTP")
fi

# Ensure the data directory exists and is owned by nonroot user
if [ ! -d /home/nonroot/data/node-keys ]; then
    echo "creating $NODE_TYPE keys"
    export TN_BLS_PASSPHRASE="local"
    /usr/local/bin/telcoin keytool generate "$NODE_TYPE" --datadir /home/nonroot/data --address "${EXECUTION_ADDRESS}" --external-primary-addr "${PRIMARY_LISTENER_MULTIADDR}" --external-worker-addrs "${WORKER_LISTENER_MULTIADDR}" "${rpc_args[@]}"

    chown -R "${USER_ID}:${USER_ID}" /home/nonroot/data

    echo "Keys generated and ownership/permissions set"

    ls -la /home/nonroot/data/
    ls -la /home/nonroot/data/node-keys/
else
    echo "Setup already complete"
    ls -la /home/nonroot/data/
    ls -la /home/nonroot/data/node-keys/ 2>/dev/null || echo "node-keys directory not found"
    # RPC_HTTP is applied with new keys only, so say so when this datadir does not advertise it.
    if [ -n "${RPC_HTTP:-}" ] && ! grep -qF "$RPC_HTTP" "$NODE_INFO" 2>/dev/null; then
        echo "$NODE_INFO does not advertise $RPC_HTTP, which applies to new keys only; make down starts a new network" >&2
    fi
fi
