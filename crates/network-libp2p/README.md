# TN Network - libp2p

Telcoin Network requires peer-to-peer network communication to achieve consensus.

## General setup

Nodes use a combination of request-response behaviour (for reliable broadcast) and gossipsub (unreliable).
Only validators in the current committee have publishing rights on gossipsub topics related to consensus.

Primary and worker each have their own instance of `ConsensusNetwork`.
These nodes in the network publish `NodeRecord` using the primary's BLS public key as the records key.
The records are queried at the start of each epoch to retrieve network information for the next committee.
There is no identify behaviour.
Peer identity resolves through these signed `NodeRecord`s, which bind a peer id to the primary's BLS public key.
Addresses are never taken from a peer's own unsigned claim about itself.

## NodeRecord freshness and repair

NodeRecord signatures cover the original advertised timestamp, network key, addresses and RPC information. Admission verifies those signed bytes and the publisher before using the record. Local freshness metadata never changes the signed payload or the bytes returned by the DHT.

The local time source is `tn_types::now()`, Unix time in seconds from the system clock. A signed timestamp at most five minutes ahead retains ordinary strict timestamp ordering. A larger timestamp is accepted and usable even when the local clock lags, but its unsigned comparison ceiling is fixed to local admission time plus five minutes, using saturating arithmetic.

A distinct authenticated record within the skew tolerance immediately replaces a cached value whose timestamp is still beyond the tolerance, including a lower signed timestamp. If both values are clamped, increasing signed timestamps retain order; a lower correction becomes eligible once the cached admission ceiling passes. Equal signed timestamps never replace a cached record. Ordinary older records remain stale. Strict signed ordering resumes as soon as the cached signed timestamp comes within the local tolerance. Ordering is deliberately reset when repairing an out-of-policy value, so replay protection for such values is limited to equal timestamps and the bounded admission window. A backward clock adjustment that makes the retained local ceiling implausible triggers the legacy repair policy.

The DHT persists admission metadata in a local BCS envelope alongside the original record and its normal expiry. Byte-identical republishes preserve the admission ceiling. Primary and each worker namespace restore this metadata after restart. Pre-upgrade rows still decode; an implausible legacy timestamp receives a zero comparison ceiling because its original local admission time is unknown. Downgrading to software that only understands the old local envelope requires rebuilding the DHT record cache.

Query winners, committee records, pinned operator-provisioned peers and the known-peer cache share this policy. Admission reuses retained metadata for the same signed timestamp, and inbound puts use one clock observation for every comparison and destination. A winning query carries its original admission metadata into the cache. Cache replacement updates the peer's live identity, addresses and RPC URL together. Committee pruning also prunes the metadata; operator-provisioned stubs remain exempt until a signed record replaces them.

To repair a mapping, correct the publishing node's clock and republish its newly signed NodeRecord with the intended key, addresses and RPC information. A receiving node with a reasonable clock accepts that correction immediately. If its clock also lags, a lower future correction becomes eligible after the retained five-minute ceiling passes; restart does not renew that ceiling. Legacy poisoned rows repair on the next distinct authenticated record. No database deletion or signature bypass is required.

## Behaviours

### Gossipsub

Used to gossip small messages to indicate events for nodes to start downloading.

### Request/Response

Used to reliably message peers directly and exchange messages of large size (>20kb).

### Peer Manager

The `PeerManager` is controls connectivity for the swarm.
If a peer receives enough penalties, they are disconnected and temporarily banned.
Some penalties result in a permanent ban.

### Kademlia

Distributed hash table for publishing node records.
These records are used to find committee validators.

## Notes on Implementation

### Keep Alive

Connections between peers rely on QUIC transport for idle and keep alive messages.

### Message Verification

Staked validators are the only publishers on the gossipsub network.
The source of the message is used to verify a staked node signed the message before propagating to other peers.
If a peer sends an invalid message, the `PeerManager` assesses a penalty.

Messages are decoded in the application layer, and penalties are reported for peers who broadcast messages that fail to decode properly.
