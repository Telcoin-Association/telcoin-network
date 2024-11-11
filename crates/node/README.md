# Telcoin Network Node Implementation

The engine crate strives to maintain compatibility with Ethereum.

## Implementation Details

The `engine` mod uses `reth` as a library. Starting with reth's beta release, custom nodes are supported. This README is to document design decisions for the custom `TelcoinNode` implementation.

### Worker and Primary Crossover

The worker and primary "nodes" need access to the same infromation at times. These are the components and why they need to stay in-sync.

###### Blockchain Provider

This type is how the worker/primary interact with the Blockchain Tree.

The blockchain provider is the main type for interacting with the blockchain. This type serves as the main entry point for interacting with the blockchain and provides data from database storage and from the blockchain tree (pending state etc.) It is a simple wrapper type that holds an instance of the database and the blockchain tree.

###### Blockchain Tree

The blockchain tree is used to track the canonical chain.

The primary adds "canonical" blocks to the tree after consensus is reached.

The worker adds "uncle" blocks to the tree for each batch it produces and validates. The worker reads from the canonical tip for managing batches.

The blockchain tree is owned by the Blockchain Provider.

###### Provider

The blockchain provider fetches data from a database or static file. This provider implements most provider or provider factory traits.

### Worker and Primary Differences

The worker and primary "nodes" need access to the different infromation at times. These are the components and why they need to remain isolated.

###### Transaction Pool

The worker needs the transaction pool to be full of user-submitted transactions.

The primary does not need a transaction pool. It simply executes all transactions received from `ConsensusOutput`.

###### Payload Builder

The worker needs the payload builder to build from the transaction pool.

The primary does not need a payload builder. It simply executes all transactions received from `ConsensusOutput`. The CL Executor is basically the payload

###### RPC

The worker needs the RPC for receiving transactions and responding to blockchain inquiries.

The primary only facilitates consensus and does not need access to the RPC server.

###### Network

The worker uses it's own network of peers, controlled on the consensus layer.

The primary needs the concept of "network" to run the beacon engine until custom consensus is supported.

###### Pipeline

The worker only needs access to the canonical tip for producing new batches and the db for validating batches.

The primary needs the concept of "pipeline" to run the beacon engine until custom consensus is supported.

###### Beacon Engine

The worker uses a different type of engine to execute state on-top of the canonical tip.

The primary needs the concept of "beacon engine" to run the beacon engine until custom consensus is supported.
