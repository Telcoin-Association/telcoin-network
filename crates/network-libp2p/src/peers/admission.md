# Connection admission and recovery

`network-config.yaml` defaults to `admission.mode: Open`. The primary and every configured worker
swarm use the same admission predicate, with their own transport identity maps. A worker swarm
outside the on-chain worker range keeps its listener and gets the same window. Open preserves existing
discovery behavior. Grace also permits ordinary discovery and keeps live connections while
operators repair or observe inputs. Neither mode grants admission privileges to ordinary peers.

```yaml
admission:
  mode: Grace
  snapshot_max_age_secs: 300
```

Closed permits the union of previous, current, and next committee identities, explicitly trusted
peers, and configured bootstrap peers. Generic pinned dial hints do not become operator grants.
Committee identity mappings come from the existing signature-checked record cache; trusted and
bootstrap mappings come from local operator commands. A claimed BLS key in a certificate or an
unauthenticated connection never creates an admission grant. Pending outbound checks run before
the registered-dial shortcut, including Kademlia dials. Unknown pending identities are denied
in Closed. Both established hooks apply the latest policy again after transport authentication.
Denied establishment never registers an established peer.

## Snapshot ordering and close condition

The epoch-start owner publishes an immutable previous/current/next window to each swarm using
`update_committees_at`. Each swarm includes every configured worker swarm outside the on-chain
worker range: it keeps listening, so it gets the window and its renewal, but no worker node. The
epoch is its revision. A higher revision replaces the complete
window. The epoch-start send always applies the complete committee update, as the unversioned
update did: ban forgiveness, slot update, pruning, record retention, explicit-peer refresh, and
missing-record queries. A same-epoch re-entry gets the same side effects. A renewal with an
identical window only renews the lease, except when it restores membership after an unversioned
compatibility update. Grace and Closed renewals retry unresolved records across the complete
window, including previous-committee identities needed during cold recovery. An older revision or different same-revision window cannot
replace the accepted membership. Unversioned compatibility updates invalidate Closed.

Closed requires a nonempty current committee, a live monotonic renewal lease, and at least
`n - floor((n - 1) / 3)` authenticated current records. The local node's
authenticated identity counts as resolved, but does not count as a connected remote peer. Config
stubs alone do not count as authenticated records. In addition to the quorum, every identity in
the previous/current/next union must resolve. This conservative launch decision prevents an
unresolved retiring or joining committee identity from being denied solely because it is unknown.
Grace enforces nothing beyond Open. It differs only in the reported status and gauges. So one
window identity without a cached record, for example an offline member, keeps a Closed swarm in
Grace. Watch `admission_fallback` (unresolved = 4) before relying on Closed.
The noncurrent sets may be empty when authoritative state explicitly supplies an empty set.

## Fallback and recovery decisions

These are the concrete launch semantics implemented by this change, for maintainer review:

| Input fault | Effective mode | Recovery |
| --- | --- | --- |
| Missing snapshot, empty current committee, or unversioned update | Open | Complete versioned authoritative window |
| Expired renewal lease, zero lease, or older revision | Open | Consistent accepted-revision renewal or newer complete window, with a nonzero lease |
| Different window at the same revision | Open | Original accepted window renewed, or a newer complete window |
| One PeerId claimed by different BLS identities, or a configured identity conflicting with a learned mapping | Unchanged | No mode change; counted in `admission_contradicted` |
| Incomplete committee identity union or insufficient authenticated current records | Grace | Resolve records through existing validation and discovery paths |

A contradicted identity binding does not change the effective mode. A record signature proves
the BLS key, not the advertised network key. So a window member can publish a record that claims
another identity's network key. Closed keeps every resolved PeerId and every configured trusted or
bootstrap PeerId authorized. A forged record can add at most the one PeerId that its own BLS key
claims. It cannot open the swarm, and it cannot remove the grant of the identity it collides with.
`AdmissionStatus::contradicted` and the `admission_contradicted` gauge count these identities.

The default lease is 300 seconds. Each epoch-scoped task renews at one third of the configured
lease, with a minimum interval of one second. Epoch shutdown cancels renewal. The swarm checks
expiry on every admission decision, so a delayed heartbeat cannot extend Closed. Renewals use
the epoch-start-pinned authoritative committee window, not newly received certificate contents.
An older epoch task cannot roll back membership. Its stale command triggers fallback until the
current epoch renews. No wall-clock peer timestamp is used to renew authoritative policy.

Open and Grace preserve live connections. On recovery to Closed, and on Closed rotation, the
manager disconnects live peers outside the accepted union without a reputation penalty. A newly
resolved record is evaluated by the next connection decision, and live revocation occurs no
later than the next peer-manager heartbeat. In-flight and resumed connections must pass the
latest established hook. Previous-committee peers remain admitted throughout their boundary
window. Committee and operator grants bypass ordinary discovery targets only while Closed is
effective. All finite transport, pending-handshake, per-peer connection, stream, message, and
rate budgets continue to apply. Existing signature, record validity, self, IP, and ban checks
remain active in every mode. Admission itself never grants a score or penalty exemption.

## Network key rotation

A network key changes only when an operator edits the `primary.seed` or `worker.seed` file. Under
Closed, a peer cannot learn a rotated key by connection. The new PeerId is denied before the record
exchange runs, and the old PeerId stays authorized while its cached record resolves.

Each accepted window update queries kad again for every window key whose cached record was signed
more than two record publication intervals ago (`kad_publication_interval`, 12 hours by default).
A live owner re-signs its record every interval. When an authorized peer holds the newer record,
the query returns it and the newer record replaces the old binding.

To rotate a network key without this delay, use one of these procedures on each Closed peer:

- Switch to Grace before the rotation. Switch back to Closed after the new binding resolves.
- Configure the new binding as a `trusted_nodes` hub. A configured hub wins over a cached record.

## Scheduled and eligible tracking

At each epoch start the epoch manager publishes two key sets to the primary swarm and to every configured worker swarm, including the swarms outside the on-chain worker range.
`scheduled` is the committee two epochs ahead.
`eligible` is the validator pool the committee draw reads: pending activation, active and pending exit.
Both are read at the epoch-start pin, the same header the committee window comes from.
A historical epoch publishes nothing, so a catching-up node does not read the pool once per replayed epoch.
The pool read is not retried.
A failed read publishes nothing and each swarm keeps its last publication, so its tracked epoch stays behind the admission epoch until a later epoch start succeeds.
A failed read of the scheduled committee publishes the pool with an empty `scheduled` set.

The publication is its own command, `NetworkHandle::update_tracked_keys`, never `update_committees_at`, so the window, its revision and its renewal are unchanged by it.
An update for an epoch older than the held one is ignored, and an update for the same epoch replaces it.
A key an update no longer lists loses its retained record and its lookup schedule.

Tracking admits nothing.
A tracked record lives in its own map in the peer manager, never in `known_peers`, the peer store or the BLS-to-PeerId binding, so admission, gossip attribution and request delivery behave exactly as they would without it.
The one exception is the window after a restart.
The startup restore loads every persisted kad row into `known_peers` with its binding before any committee or tracked update exists, and the store persists tracked rows too (see Kad store below), so until the first committee update a restored tracked key resolves like any other restored record.
That update drops every restored key outside the window and the pinned set, moves the record of a key that is still tracked into the tracked map, and unbinds every dropped key whose peer has not been dialed or connected since startup.
A tracked key gets no admission grant, no score or penalty exemption and no pin against eviction.
Tracked records never count toward the resolved-record quorum or the complete-union check, so they never change the effective mode or the fallback in the table above.
A window key whose only record is its tracked one, for example a key that has just moved from `eligible` into `next`, still counts as unresolved, and a Closed swarm reports Grace until that key's committee lookup lands.

### Cap

One swarm tracks at most 1024 distinct keys across both sets.
Past the cap every scheduled key is kept first and eligible keys fill the remaining room in ascending key order, so nodes that read the same pool keep the same keys.
The keys left out are counted in one bounded warning per update.
The local key may appear in the sets, but it is never looked up, never listed in the snapshot and never counted by the tracked gauges.

### Ingest

A verified record for a tracked key outside every committee slot and the pinned set is retained from both kad paths: a query result, and an inbound put, whether the publisher's own or relayed.
A record that contradicts a configured `trusted_nodes` binding or a `committee_peers` launch binding, in either direction, is refused, as on the committee cache path.
A record that does not supersede the held one under the shared freshness rule is dropped, so a replayed older copy never regresses it.
A retained record drops its RPC endpoint.
When a committee rotation evicts a `known_peers` entry whose key is still tracked, its learned record moves into the tracked map.
A config stub is not a record and never moves, and a tracked record is never promoted back into `known_peers`.

### Lookups

On each peer-manager heartbeat (30 seconds by default) a swarm looks up the tracked keys that have no retained record, or whose record was signed more than two record publication intervals ago, the age limit window records use.
A key in a committee slot and a pinned key are left to the committee and hub lookups and are never queried as tracked keys.
At most 16 keys are queried per heartbeat, keys never looked up first and then the keys due the longest, so a cold pool of 500 keys is covered in about 16 minutes.
Each key backs off from 30 seconds, doubling up to 30 minutes while it stays unresolved, so a key that never resolves costs at most one query per 30 minutes.
The schedule resets once the key no longer needs a lookup.

A tracked lookup never displaces a committee lookup.
A committee lookup for a key that already has a tracked query in flight joins that query instead of issuing a second one, and marks it.
A record the joined query finds is routed by the key's membership when the query closes, so a key now in the window is cached as a committee record.
When a marked query closes without a record, the swarm re-issues the committee lookup through `find_authorities`, which skips the key if a record has been learned in the meantime.
A joined committee lookup therefore waits at most for the tracked query already in flight, which the 60-second kad query timeout bounds.
Every path that brings a key into the window, the committee update and its window lookups, issues a committee lookup, so a key that moves from `eligible` into the window while its tracked query runs is looked up again this way.

### Kad store

The kad store keeps rows for tracked keys as a class of its own.
A tracked row is served to peers like a committee row, so once a committee node has fetched an eligible validator's record, it can answer for that validator even after the validator loses its hub connection.
A fetched copy expires no later than `kad_record_ttl` (48 hours by default) after it arrived.
Tracked rows are never queried as missing required records.
They share the process-wide required-record allowance (1,024 rows across the primary and every worker swarm) but take only headroom that no required key holds, so they never make a committee reservation fail.
The store follows only an update the peer manager accepted, with the keys the peer manager kept under its cap.
When those keys exceed the headroom, the store keeps what fits, scheduled keys first and then eligible keys, each in key order, and counts the keys it left out in one bounded warning per update.
A tracked key the same store already requires, as its own, a pinned or a committee key, takes no headroom and is always kept; if it later leaves the committees, its row stays without a slot until the next tracked update.
When committee keys later grow into headroom that tracked rows hold, the next tracked update gives it back.
The peer manager's own tracked records do not depend on the store.
An update prunes only the rows of keys it stops tracking.
After a restart the first committee update prunes persisted tracked rows, unless a tracked update has listed them again before it.
Until that update the restored rows are also in `known_peers` with their PeerId binding, and the same update removes them there and unbinds the keys whose peer has not been dialed or connected since startup (see Tracking admits nothing above).

### Reachability under Closed

A Closed swarm narrows its connections to the authorized set and denies dials to other identities, so its kad queries reach only committee peers and configured hubs.
A responder serves only the rows it retains, and a hub retains a peer's own record only while that peer stays connected.
An eligible validator's record is therefore reachable from a Closed committee only through a hub it stays connected to, or through a committee node that has already fetched it.
Run the hubs in Open or Grace, and configure a joining validator with those hubs in `trusted_nodes` and keep them connected from its first start.

A Closed swarm refuses the two connection directions at different points.
It refuses its own dial to an identity outside the authorized set before any transport work, because the dial already names the PeerId.
An inbound connection names its PeerId only when the QUIC/TLS handshake completes, so the swarm refuses it after the handshake, and the remote's own dial can complete before the connection closes.
The node's gate therefore never saves the host an inbound handshake, and the host firewall is the cost filter.
Because that filter drops a peer before the node can see it, it must lead the node's view: it opens to scheduled and eligible keys before the node admits them, and an eligible peer it passes costs one handshake before the node refuses it.

### Export

`NetworkHandle::admission_snapshot` returns one swarm's admission status with one entry per key of the committee window, the scheduled and eligible sets, and the trusted and bootstrap sets, except the swarm's own key.
Each entry carries its slots, the addresses the swarm would use for it, where those addresses came from, and whether the key is admitted.
`admitted` restates the admission predicate, so a key held only as scheduled or eligible is never admitted, even when its record claims an authorized PeerId.
The node serves the snapshots of all its swarms as the `tn_admissionSnapshot` RPC method.
The `tn_network.tracked_required_members` and `tn_network.tracked_unresolved_members` gauges, labelled by `network` and by `slot` (`scheduled` or `eligible`), and `tn_network.tracked_records{network}` report tracking progress apart from the three committee slots.

## Operational rollout

Start in Open, populate configured bootstrap and trusted identities on every swarm, and inspect
the window in Grace before requesting Closed. Repair stale, contradictory, or unresolved inputs
before enabling it. `NetworkHandle::admission_status` exposes configured/effective modes, epoch,
fallback cause, authenticated current-record count, required-record quorum, connected current
peers, and contradicted bindings separately. The `tn_network.admission_*` gauges carry the
existing primary/worker labels. `admission_mode` is Open = 0, Grace = 1, Closed = 2.
`admission_fallback` is healthy = 0, missing = 1, stale = 2, contradictory = 3, unresolved = 4.
Contradictory means a different window at the same revision. `admission_contradicted` counts
identities with a contradicted PeerId binding.

Resolved records prove a usable identity map, connections prove transport reachability, and
node consensus readiness proves the node's separate consensus prerequisites. None substitutes
for another. Requesting Closed cannot force it active while decision inputs remain unresolved.
During an incident, select Open or Grace in configuration and restart, or restore the current
authoritative renewal and validated records to recover automatically.
