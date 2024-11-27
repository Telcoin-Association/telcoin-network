# TN Network - libp2p

Telcoin Network requires peer-to-peer network communication to achieve consensus.

Consensus p2p networking was initially built using [anemo](https://github.com/mystenlabs/anemo.git).
However, the protocol is moving to libp2p for all network communication.

This crate is separate while there are two existing network solutions in the codebase.
Eventually, this crate will replace tn-network and be the only network interface for p2p communication.

## General setup

Committee-voting validators (CVVs) use [flood publishing](https://github.com/libp2p/specs/blob/master/pubsub/gossipsub/gossipsub-v1.1.md#flood-publishing) to broadcast messages to peers.
Non-voting validators (NVVs) subscribe to topics and gossip the messages received from CVVs.
CVVs do not subscribe to topics used to propogate network consensus. Instead, they use reliable broadcasting to other CVVs using a request/response model.
CVVs use anemo for p2p consensus networking at this time, but will eventually transition to libp2p as well.

## Worker Publish

Workers publish blocks that reach quorum.

## Primary Publish

Primaries publish certificates.

## Subscriber Publish

Subscriber publishes committed subdag results as `ConsensusHeader`s. These headers comprise the consensus chain.
Clients use results from worker and primary topics to execute consensus headers.
