#!/bin/bash

# Ensure the data directory exists and is owned by nonroot user
if [ ! -d /home/nonroot/data/node-keys ]; then
    echo "creating validator keys"
    chown -R ${USER_ID}:${USER_ID} /home/nonroot/data
    export TN_BLS_PASSPHRASE="local"
    /usr/local/bin/telcoin keytool generate validator --datadir /home/nonroot/data --address "${EXECUTION_ADDRESS}"
else
    echo "Setup already complete"
fi
