# Telcoin Network

Consensus layer (CL) is an implemntation of Narwhal and Bullshark.
Execution layer (EL) is an implementation of reth.

Requires Rust 1.79


## Quick Start

Check out the repo and update the submodules:
```make init-submodules``` or ```git submodule update --init --recursive```

Run the test network script to start four local validators and begin advancing the chain:
```etc/local-testnet.sh --start --dev-funds 0xADDRESS```
Note: the script will compile a release build, which may take a few minutes.
This configures and create genesis for a new network and starts it.  See the output for the RPC endpoints.
0xADDRESS above should be a valid address prepended with 0x.  Make sure you have the key for this address,
it will be funded with 1billion TEL on your test network.  After configuration you can run the script with
just the --start option (--dev-funds is only used when configuring and CAN NOT be used later to fund
an account).  Nodes run in the backgound and should be killed with the ```kill``` or ```killall``` commands.

The best docs for running a test network will currently be this script.  It is short and pretty basic,
it configures each node, brings together the configs to create genesis and then shares this with each node.
This is the same basic procedure used to create nodes on diffent machines (NOTE- do not use the instance
option if not running on the same machine, it is to avoid port conflicts).

Note that network operation is under heavy dev and somewhat rough right now but it should be possible to
bring up a reliable test network.

Once started you can use the RPC endpoint for any node with you favorate Ethereum tooling to test.
You will have test funds in your dev funds account set during config.  The network can be restarted
by shutting down, ```killall telcoin-network``` is good for this, deleting ./local-validators/ and
rerunning the script.

The defaults should build a block roughly every 10 seconds, see comments on the script if you want to
speed this up for testing.


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

### Build the contracts using Foundry

Now that `tn-contracts` is initialized, compile the contracts to populate the required `out` dir by running

```bash
forge build
```

### Optional: Foundry tests

To run the fuzz tests run `forge test`. Note that contract information is best viewed in each contract's interface where its API is defined and documented in NatSpec format.


## Acknowledgements

Telcoin Network is an EVM-compatible blockchain built with DAG-based consensus.
While building the protocol, we studied and explored many different projects to identify what worked well and where we could make improvements.

We want to extend our sincere appreciation to the following teams:
- [reth](https://github.com/paradigmxyz/reth): Reth stands out for their dedication to implementing the Ethereum protocol with clean, well-written code. Their unwavering commitment to building a strong open-source community has reached far beyond the Ethereum ecosystem. We are truly grateful for their leadership and the inspiration they continue to provide.
- [sui](https://github.com/MystenLabs/sui): Telcoin Network uses a version of Bullshark that was heavily derived from Mysten Lab's Sui codebase under Apache 2.0 license. Because this code was already released under the Apache License, we decided to start with a derivation of their work to iterate more quickly. We thank the Mysten Labs team for pioneering BFT consensus protocols and publishing their libraries.
