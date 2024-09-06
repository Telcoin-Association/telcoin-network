#!/bin/bash

# Ensure the data directory exists and is owned by nonroot user
if [ ! -d /home/nonroot/data/validator-keys ]; then
    echo "creating validator keys"
    chown -R ${USER_ID}:${USER_ID} /home/nonroot/data
    /usr/local/bin/telcoin genesis init --datadir /home/nonroot/data
    /usr/local/bin/telcoin keytool generate validator --datadir /home/nonroot/data --address "${EXECUTION_ADDRESS}"
    /usr/local/bin/telcoin genesis add-validator --datadir /home/nonroot/data
else
    echo "Setup already complete"
fi
