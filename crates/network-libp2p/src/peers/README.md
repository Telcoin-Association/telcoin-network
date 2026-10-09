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

Admission, retention, load scoring, and protocol scoring are independent policy dimensions.
The peer policy composes every live trust basis instead of selecting a single exemption.

| Peer | Configured admission | Population pruning and mesh | Load penalties | Protocol penalties |
| --- | --- | --- | --- | --- |
| Ordinary | Public discovery policy | Ordinary | Scored | Scored |
| Bootstrap or explicit discovery | Authorized | Ordinary | Scored | Scored |
| Configured DAO observer | Authorized | Protected | Scored | Scored |
| Operator trusted | Authorized | Protected | Exempt | Scored |
| Previous, current, or next committee | Authorized | Protected | Exempt | Exempt |

Configured admission eligibility is recorded separately from the existing public discovery
admission policy. This change supplies the policy dimensions for a configured topology.
Committee privileges follow the union of the three authoritative slots. Exiting the final slot
revokes only committee privileges; operator trust remains attached to the configured peer.
Connected peers' explicit gossip status is reconciled when committee slots change.

Timeouts, slow gossip delivery, stream rate excess, and Kademlia rate excess use `Penalty::Load`.
Malformed payloads, cryptographic failures, and application validation failures retain their
protocol severity and source or author attribution. Operator trust alone does not suppress these
penalties, and trust reload preserves protocol bans. Committee members remain exempt from all
score penalties for launch: a scoring bug or benign mismatch must not remove peers required for
consensus liveness. Committee promotion, pre-dial recovery, and discovery of a committee identity
forgive existing reputation bans and prime the score. Exiting the last committee slot restores
protocol scoring even if operator trust remains.

This exemption affects reputation, not validity: invalid messages and votes are still rejected.
Known validator identities support operational accountability, but do not rule out compromised
keys, malicious operators, or shared implementation bugs. Automatic committee exclusion needs a
separate liveness and recovery design before it can safely replace this launch policy.

Scoring exemptions change reputation only. Connection caps, observed-IP and address caps, stream
budgets, message-size bounds, and Kademlia work limits still apply. Over-budget Kademlia work
is shed before cryptographic verification or storage. Separate message-class windows remain
independent, so exhausting a bulk record allowance does not exhaust another class's allowance.
