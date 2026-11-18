#!/bin/bash
# Creates the genesis of the compose testnet and gives every node's datadir the chain config.
# `docker compose up` runs this again on a stack that is already up, so an existing genesis is
# kept: a new one would rewrite the chain under nodes that are running.
set -euo pipefail

DATA=/home/nonroot/data
GENESIS="$DATA/genesis"
NODE_UID=1101

# The address funded at genesis; compose.yaml passes it, with a default.
: "${TN_DEV_FUNDED_ACCOUNT:?not set}"

# Copies a file unless the destination exists, so a repeated run never rewrites config under a
# running node.
copy_once() {
    [ -e "$2" ] || cp "$1" "$2"
}

# Gives a node's datadir the chain config, owned by the user the node runs as. Only these files
# are chowned: the datadir may hold a running node's database.
install_chain_config() {
    local node=$1
    mkdir -p "$node/genesis"
    copy_once "$GENESIS/genesis.yaml" "$node/genesis/genesis.yaml"
    copy_once "$GENESIS/committee.yaml" "$node/genesis/committee.yaml"
    copy_once "$GENESIS/parameters.yaml" "$node/parameters.yaml"
    chown -R "$NODE_UID:$NODE_UID" "$node/genesis" "$node/parameters.yaml"
}

if [ -f "$GENESIS/genesis.yaml" ]; then
    echo "keeping the genesis in $GENESIS (make down removes it; TN_DEV_FUNDED_ACCOUNT applies to a new genesis only)"
    if [ ! -f "$GENESIS/parameters.yaml" ]; then
        echo "$GENESIS has no parameters.yaml; run make down to start a new network" >&2
        exit 1
    fi
else
    mkdir -p "$GENESIS/validators"
    for i in 1 2 3 4; do
        cp "$DATA/validator-$i/node-info.yaml" "$GENESIS/validators/validator-$i.yaml"
    done

    /usr/local/bin/telcoin genesis \
        --datadir "$DATA" \
        --chain-id 0x1e7 \
        --epoch-duration-in-secs 60 \
        --dev-funded-account "$TN_DEV_FUNDED_ACCOUNT" \
        --max-header-delay-ms 1000 \
        --min-header-delay-ms 1000 \
        --consensus-registry-owner 0x3DCc9a6f3A71F0A6C8C659c65558321c374E917a

    # The datadir root lives in the container's own layer, which is lost when compose recreates
    # the container; the shared volume keeps parameters.yaml beside the rest of the genesis.
    cp "$DATA/parameters.yaml" "$GENESIS/parameters.yaml"
fi
chown -R "$NODE_UID:$NODE_UID" "$GENESIS"

for i in 1 2 3 4; do
    install_chain_config "$DATA/validator-$i"
done

# The observer follows the chain from the same config. Its node-info never goes into
# genesis/validators, so it is not in the committee and finds the validators through the
# committee's bootstrap servers. Skipped when compose mounts no observer volume here.
if [ -d "$DATA/observer" ]; then
    install_chain_config "$DATA/observer"
fi

echo "done"
