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

## Peer policy

Admission eligibility, connection retention, and load scoring are independent policy dimensions.
The peer policy composes every live trust basis instead of selecting a single exemption.

| Peer | Configured admission | Population pruning and mesh | Load penalties | Protocol penalties |
| --- | --- | --- | --- | --- |
| Ordinary | Public discovery policy | Ordinary | Scored | Scored |
| Bootstrap or explicit discovery | Authorized | Ordinary | Scored | Scored |
| Operator trusted | Authorized | Protected | Exempt | Scored |
| Previous, current, or next committee | Authorized | Protected | Exempt | Scored |

Configured admission eligibility is recorded separately from the existing public discovery
admission policy. This change supplies the policy dimensions for a configured topology.
Committee privileges follow the union of the three authoritative slots. Exiting the final slot
revokes only committee privileges; operator trust remains attached to the configured peer.
Connected peers' explicit gossip status is reconciled when committee slots change.

Timeouts, slow gossip delivery, stream rate excess, and Kademlia rate excess use `Penalty::Load`.
Malformed payloads, cryptographic failures, and application validation failures retain their
protocol severity and source or author attribution. A protocol failure remains scoreable for
every peer and can ban a trusted or committee peer. Committee updates, pre-dial recovery,
rediscovery, network-key rotation, and adding trust preserve recorded protocol history.
Ordinary score decay remains the recovery mechanism for protocol bans.

Load exemption changes scoring only. Connection caps, observed-IP and address caps, stream
budgets, message-size bounds, and Kademlia work limits still apply. Over-budget Kademlia work
is shed before cryptographic verification or storage. Separate message-class windows remain
independent, so exhausting a bulk record allowance does not exhaust another class's allowance.
