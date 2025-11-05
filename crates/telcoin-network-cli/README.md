# Telcoin Network Binary
The main binary for running a node.

# Validator Instructions
Validators use the CLI to create credentials, sign genesis, and join the network.

## Steps
1. `telcoin-network keytool generate validator --address <ADDRESS>`
2. `telcoin-network genesis init` // this is only run once per network
3. `telcoin-network genesis add-validator` // every validator adds their credentials to genesis
4. `telcoin-network genesis finalize` // create committee and worker cache - this can be shared after created
5. `telcoin-network node`

## Steps with optional args
1. `telcoin-network keytool generate validator --address <ADDRESS> --datadir my-dir`
2. `telcoin-network genesis init --datadir my-dir` // this is only run once per network
3. `telcoin-network genesis add-validator --datadir my-dir` // every validator adds their credentials to genesis
4. `telcoin-network genesis finalize --data-dir my-dir` // create committee and worker cache - this can be shared after created
5. `telcoin-network node --data-dir my-dir`
