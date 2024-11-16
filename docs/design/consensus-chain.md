# Consensus Chain

In addition to the execution chain we will maintain a "consensus" chain.  This is a simple chain that contains
Committed Sub Dags.  The execution block header will also use the beacon field to store the hash of consensus
chain header that build that block.

The consensus chain header is simple, it contains:
- parent hash: the hash of the previous header
- committed sub dag: the committed sub dag for this output
- number: the height of the consensus chain

As consensus output is recieved the consensus chain will be built including saving the committed sub dag as well working blocks it references.

## Using consensus chain with a light client

A strategy to implement a light client with the help of the consensus chain:
- Monitor the execution chain's log bloom for events of interest
- When found use a signed (consensus certificate) header emmitted by a primary that will contain the last know execution header hash
  - These can be obtained via the committed sub dag's from the consensus chain (note that you will need to wait for your hash to make it to a future commit).
  - One could also monitor the consesus p2p network for a signed primary header that contained your blocks hash (or a decendant).  This will be faster but more complex.

Note that the latest execution header needs to be added to the primary headers.  This can also be used to validate headers.
