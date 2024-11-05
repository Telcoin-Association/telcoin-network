# Telcoin Network

Consensus layer (CL) is an implemntation of Narwhal and Bullshark.
Execution layer (EL) is an implementation of reth.

Requires Rust 1.79

## CLI Usage

The CLI is used to create validator information, join a committee, and start the network. The following `.env` variables are useful but not required:

- `NARWHAL_HOST`: The ipv4 address of the host running the node. Narwhal uses this address for consensus messages.

## Helpful CURL commands

### faucet request

curl https://adiri.tel \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"faucet_transfer","params":["0xffE2815E73f7E0f30892f38b724651D663D3978e", "0xb7bE13B047e1151649191593C8f7719BB0563609"],"id":1,"jsonrpc":"2.0" }'

### get chain id

curl localhost:8544 \
curl 35.208.38.251:8544 \
curl 34.122.21.94:8544 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_chainId","params":[],"id":1,"jsonrpc":"2.0"}'

### get block number

curl localhost:8544 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_blockNumber","params":[],"id":1,"jsonrpc":"2.0"}'

### get block by number

curl 35.208.38.251:8544 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_getBlockByNumber","params":["latest"],"id":1,"jsonrpc":"2.0"}'

### get gas price

curl http://localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_gasPrice","params":[],"id":1,"jsonrpc":"2.0"}'

### get balance for test wallet

curl https://adiri.tel \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_getBalance","params":["0x911C4954D645258e68E71de59A41066C55bd9966", "latest"],"id":1,"jsonrpc":"2.0"}'

### transactions pool status

curl localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"txpool_status","params":[],"id":1,"jsonrpc":"2.0"}'

### transactions content

curl localhost:8545 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"txpool_content","params":[],"id":1,"jsonrpc":"2.0"}'

### transactions by hash

curl localhost:8544 \
-X POST \
-H "Content-Type: application/json" \
--data '{"method":"eth_getTransactionByHash","params":["0x78469857b25968c53c6e78ef3559396767ac470517e4af59c196e5e0bd65dac4"],"id":1,"jsonrpc":"2.0"}'

### Get transaction receipt

curl localhost:8544 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"method":"eth_getTransactionReceipt","params": ["0x5340fbcd36fffe11d4e8da962c54446a6fb9a43fc9a2b69d14a2430888b9e69c"],"id":1,"jsonrpc":"2.0"}'

## TN-Contracts Submodule

Telcoin-Network infrastructure makes use of several onchain contracts which serve as critical infrastructure for the network. These include a validator staking contract, bridged token module, testnet faucet, CI attestation interface, and several others like Uniswap and various liquid stablecoins. All onchain infrastructure is housed in a Foundry smart contract submodule called `tn-contracts`.

### Initialize `tn-contracts`

After cloning `telcoin-network`, initialize the `tn-contracts` submodule using the following make command:

```makefile
make init-submodules
```

This will install Foundry to the submodule but will not initialize its dependencies.

### Install `tn-contracts` dependencies using NPM

Further work in `tn-contracts` such as contract compilation via `solc` requires developers to pull dependencies which are managed using `npm`. Note that this deviates from Foundry's standard dependency management which normally makes use of its `lib` dir as defined in `foundry.toml`; in our case we use NPM's `node_modules` for clean compatibility with Axelar infrastructure which uses Hardhat rather than Foundry.

To install `tn-contracts` dependencies, simply run the following NPM command:

```bash
npm install
```

Now `tn-contracts` is fully initialized; to compile run `forge build` and to run the fuzz tests run `forge test`. Contract information is best viewed in each contract's interface where its API is defined and documented in NatSpec format.
