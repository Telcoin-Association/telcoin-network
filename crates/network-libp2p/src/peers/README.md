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

### Collateral address penalties

An authenticated identity with `Admission::Authorized` is exempt from collateral IP bans. The
exemption uses the same live policy snapshot as configured admission: operator trust, pinned
bootstrap or explicit discovery peers, and the previous/current/next committee union. Advertising
a committee address, a trusted peer's `/p2p` component, or another identity's address grants no
trust or quota. The offending identity's reputation and protocol bans remain enforceable for
every trust basis.

Pending inbound connections have no authenticated PeerId. They receive ordinary finite slots
before collateral filtering, so an admitted identity sharing a banned address can authenticate.
The source ceiling is `max_priority_peers * MAX_ESTABLISHED_CONNECTIONS_PER_PEER`, clamped to
the existing aggregate pending ceiling. This accommodates configured priority peers behind one
NAT without allocating any identity privilege before authentication. IPv4 and IPv4-mapped IPv6
sources share a scope; IPv6 sources share their /64. QUIC Retry validates addresses before
acceptance by default. Disabling Retry retains all finite slot and transport memory bounds but
removes that source-validation guarantee.

Authentication and every listen failure release the connection's slot exactly once, including
denial by another behaviour, failed upgrades, cancellation, and transport timeouts. Identity
penalties and live collateral policy are checked in both established connection directions.
Collateral bans use only actually observed IPs, not advertised addresses, and apply to the
current connection or proposed dial addresses. Historical IP evidence is retained for ban
contributions without stranding a peer that moves to an unbanned address. Committee rotation
changes the exemption without deleting shared-address counters; ordinary unban and pruning
remove only the departing banned identity's contributions. Persistent IP penalties use exact
observed addresses; the /64 scope bounds pending work, without penalizing authenticated IPv6
neighbours solely for sharing a prefix.

| Peer | Configured admission | Population pruning and mesh | Load penalties | Protocol penalties |
| --- | --- | --- | --- | --- |
| Ordinary | Public discovery policy | Ordinary | Scored | Scored |
| Bootstrap or explicit discovery | Authorized | Ordinary | Scored | Scored |
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
