#!/bin/bash

USER_ID=1101

# Ensure the data directory exists and is owned by nonroot user
if [ ! -d /home/nonroot/data/node-keys ]; then
    echo "creating validator keys"
    export TN_BLS_PASSPHRASE="local"
    /usr/local/bin/telcoin keytool generate validator --datadir /home/nonroot/data --address "${EXECUTION_ADDRESS}"

    chown -R ${USER_ID}:${USER_ID} /home/nonroot/data

    echo "Keys generated and ownership/permissions set"

    ls -la /home/nonroot/data/
    ls -la /home/nonroot/data/node-keys/
else
    echo "Setup already complete"
    ls -la /home/nonroot/data/
    ls -la /home/nonroot/data/node-keys/ 2>/dev/null || echo "node-keys directory not found"
fi
