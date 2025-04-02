# How it works

## Peer DB

BanOperation::TemporaryBan is handled by

Here's the strategy:

- AllPeers heartbeat updates scores
  - AllPeers manages connection status
- Pass these reputation updates to the peer manager
- Peer manager decides what action to take
  - Manager only makes decisions based on reported penalties and reputation updates

Problem: unban needs ip addresses, which only the db manager has

- but reputation change comes from score

Solution:

- AllPeers captures current score
- Calls update on peer scores
- Creates the `ReputationUpdate` to share with PeerManager
  - This isolates logic so AllPeers can include the unbanned IP addresses
