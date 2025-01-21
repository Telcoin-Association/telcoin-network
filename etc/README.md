# etc files
The directory is home for developer tools, such as docker compose, Dockerfile, and executable bash scripts.

## Docker Compose
`compose.yaml` is a Docker Compose V2 file that can be brought up using `make up` and down with `make down`.

Using these commands will erase all data between up/down.

The compose spins up 4 containers to create the necessary validator information using mounted volumes.
The `setup_validator.sh` script is used to facilitate all the necessary commands.

A committee service generates genesis and distributes the `committee.yaml` and `worker_cache.yaml` files.
Finally, validator services are launched to start the network.

RPCs can be reached at:
- validator 1: 127.0.0.1:8545
- validator 2: 127.0.0.1:8544
- validator 3: 127.0.0.1:8543
- validator 4: 127.0.0.1:8542

## Scripts
A different approach to create all the necessary information to run a local testnet is `local-testnet.sh`.

This executable will generate keys inside `local-validators/` so the dev can start nodes in separate terminal windows.
