# Telcoin Network Synchronization Strategy

Telcoin Network can synchronize a node trustlessly from genesis (the genesis committee is the prerequisite).
It implements two sets of metadata, basically mini chains themselves to achieve this.

## The Epoch Chain

See struct EpochHeader in crates/types/src/primay/epoch.rs

This is composed of epoch records that contain:
- Epoch number (block number for this "chain")
- The BLS public keys of that epoch's committee
- The BLS public keys of the next epoch's committee
- The hash of the previous EpochRecord (creating a chain)
- The block number and hash of the final execution state of the epoch
- The hash of the final consensus output for the epoch (see the consensus chain)

These records are generated at the END of each epoch and the outgoing committee will publish their signatures of the record at the START of the next epoch.
This allows the network to continue while this is being done.
All caught up nodes are expected to consolidate these signatures and store them as a certificate containing at least 2/3 + 1 committee signatures from the epoch.
Note that these certificates could contain different signatures depending on the node but all that matters is they have enough to prove the epoch record is valid.
Each node is also expected to "serve" the epoch records and certificates on request.

This information is not needed by caught up committee members to move forward,
this is why we can continue the network while this happens in the background.

With these records and certs any node that knows the genesis committee can do the following to trustlessly acquire all the committees for all current epochs.
This is important to allow consensus output to be validated before the execution state has caught up.
- Request epoch 0/cert from a peer (any peer with a valid cert will do)
- If valid save this record/cert (and also become a source for other nodes as well)
- Request epoch 1/cert from a peer
- Use the "next" committee of epoch 0 to verify that the epoch 1 committee is correct
- Verify the record is signed by that committee
- Repeat this until no more epoch records are available

This will leave the node with knowledge of all committees for all epochs.
The current epoch will not have a record yet however the previous epoch's record will contain the current epoch's committee.

Note: the onchain committee state is the source of truth for committee members but this is used to acquire this information without the execution state.
It is possible for an epoch record to contain a committee with a validator that was dropped,
this should be exceptionally rare but needs to be accounted for when validating next committees.
With a quorum of 2/3 + 1 this should not cause a problem.

## The Consensus Chain

See struct ConsensusHeader crates/types/src/primay/block.rs

This is composed of consensus records that contain:
- The hash of the previous consensus header (creating a chain)
- The number of the record, an incrementing counter as records are generated (i.e. block height for the consensus chain)
- The committed sub dag that can be executed to extend the execution chain (this is the consensus the committee came to)

Starting from genesis (or the last verified execution state) one can execute consensus output to rebuild (sync) the execution chain.
It is important to use consensus output that has been verified.
The actual headers include certificates that by definition can be verified but this alone is not sufficient to guarantee a given output is valid (what if a bad actor omits or reorders certs for instance).
Sources for verified consensus header hashes are:
- Execution blocks will store the hash that generated the block.  This will be too late to verify incoming consensus output however (but can be used to identify a fork quickly).
- Each epoch record (Epoch Chain) will include the final consensus header hash of that epoch.
- Current committee members will gossip consensus output with a signature and this can be used to verify consensus output and obtain the hash of the latest consensus header.  Note we need to know the current committee for this to work, this the prime job of the Epoch Chain.

Once a verified hash is obtained then a peer can be queried to provide the consensus header and verify its hash.
This will contain its parent record which can be queried and its hash verified as well.
Repeat until all missing output has been retrieved trustlessly.
Once all consensus headers are obtained they can be executed from lowest to highest to re-create the execution chain.
Consensus headers have to be retrieved in "reverse" from a newer hash backwards but executed "forwards".

## Using The Metadata Chains.

- First download and verify all available epoch records.
  - This will provide all committees
  - There should be one epoch per day so the burden of doing this should not be great.
- Once verified hashes for consensus headers are obtained then download consensus headers.
  - This can be done in parallel (as epoch records come in for instance)
  - Once the latest committee is known then the node can download the latest output as it received
- Once an unbroken chain of consensus output has been retrieved then start executing it in order to create the execution chain.
