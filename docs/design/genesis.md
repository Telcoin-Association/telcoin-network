# Genesis
Validators must agree to an initial state for the blockchain, also known as genesis.

## Testnet
Genesis is initialized in the Execution layer before any batches are created by workers. The genesis block contains all the information needed to start the first batch. 

## Rationale
Telcoin Network is a consortium with known validators. Therefore, it is possible to agree on an initial genesis through social governance instead of relying exclusively on trustless cryptography.

If a validator is byzantine, it's proposed batch and proposed header will fail peer validation because the parent hash will be incorrect. Currently, this is irrecoverable without reinitializing genesis.

## Alternative
The EL produces batches for workers to broadcast. If genesis block was shared among all workers first, there would be no way to validate it. This would require additional logic with a new message type for the engine - "Genesis". Each worker would validate their peer's genesis matches their own. However, this is more work for seemingly no additional benefit.

The only additional consideration is how the CL handles genesis.
