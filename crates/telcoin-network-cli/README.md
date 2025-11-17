# Telcoin Network Binary

The main binary for running a node.

# Validator Instructions

Validators use the CLI to create credentials, sign genesis, and join the network.

## Steps

1. `telcoin-network keytool generate validator --address <ADDRESS>`
2. `telcoin-network genesis` // this is only run once per network - creates genesis.yaml, committee.yaml, and parameters.yaml
3. `telcoin-network node`

See `etc/local-testnet.sh` for working example.
