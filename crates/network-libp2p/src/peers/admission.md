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
