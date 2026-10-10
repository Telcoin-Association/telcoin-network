# Connection admission and recovery

`network-config.yaml` defaults to `admission.mode: Open`. The primary and every worker use the
same admission predicate, with their own transport identity maps. Open preserves existing
discovery behavior. Grace also permits ordinary discovery and keeps live connections while
operators repair or observe inputs. Neither mode grants admission privileges to ordinary peers.

```yaml
admission:
  mode: Grace
  snapshot_max_age_secs: 300
  transition_grace_secs: 30
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
`update_committees_at`. The epoch is its revision. A higher revision replaces the complete
window. An identical same-revision update renews the lease without reapplying committee ban
forgiveness or changing membership, except when restoring membership after an unversioned
compatibility update. Grace and Closed renewals retry unresolved records across the complete
window, including previous-committee identities needed during cold recovery. An older revision or different same-revision window cannot
replace the accepted membership. Unversioned compatibility updates invalidate Closed.

Closed requires a nonempty current committee, a live monotonic renewal lease, consistent identity
mappings, and at least `n - floor((n - 1) / 3)` authenticated current records. The local node's
authenticated identity counts as resolved, but does not count as a connected remote peer. Config
stubs alone do not count as authenticated records. In addition to the quorum, every identity in
the previous/current/next union must resolve. This conservative launch decision prevents an
unresolved retiring or joining committee identity from being denied solely because it is unknown.
The noncurrent sets may be empty when authoritative state explicitly supplies an empty set.

## Fallback and recovery decisions

These are the concrete launch semantics implemented by this change, for maintainer review:

| Input fault | Effective mode | Recovery |
| --- | --- | --- |
| Missing snapshot, empty current committee, or unversioned update | Open | Complete versioned authoritative window |
| Expired renewal lease, zero lease, or older revision | Open | Consistent accepted-revision renewal or newer complete window, with a nonzero lease |
| Different window at the same revision | Open | Original accepted window renewed, or a newer complete window |
| One PeerId claimed by different authorized BLS identities, or a configured identity conflicting with a learned mapping | Open | Repair configuration or obtain consistent signature-checked records |
| Incomplete committee identity union or insufficient authenticated current records | Grace | Resolve records through existing validation and discovery paths |

## Automatic transition Grace

When Closed is requested, every newly accepted epoch snapshot enters Grace for at least
`transition_grace_secs` (30 seconds by default). The interval starts when that swarm accepts the
snapshot, using its monotonic clock, even if all records already resolve. Identical renewals
refresh only the policy lease. They cannot postpone closure by restarting the interval. A newer
epoch atomically replaces the entire window and starts its own interval. An older or contradictory
update preserves the accepted window and timer while taking the documented Open fallback.
Consistent authoritative renewal repairs that fault without restarting the accepted epoch's timer.

Elapsed time is a minimum wait, never a close authorization. With unavailable hubs or delayed
records, Grace continues indefinitely while the authoritative lease is renewed. Closure requires
the current snapshot's authenticated-record quorum and every identity in the complete boundary
window to resolve, as described above. This retains the conservative launch contract: a quorum
alone does not deny unresolved boundary identities. Connections, consensus readiness, operator
stubs, and records from superseded windows cannot replace these inputs. Once records arrive
through the existing signature/protocol checks, the next admission decision may close if the
minimum wait has elapsed and the snapshot is valid. The heartbeat then revokes ordinary live peers.

Unknown authenticated Grace peers remain ordinary discovery peers. Grace does not make them
important, operator trusted, pinned in the gossip mesh, or exempt from finite service budgets.
They continue through the same transport, ban, signature, protocol, and work-rate checks. No
attacker is required for the connectivity failure this interval prevents, and an attacker cannot
extend it with an identical committee renewal or acquire privileges by connecting during Grace.

The timer is not persisted. Restart requires a fresh authoritative snapshot and a full new
interval, even with a restored record cache. An unversioned compatibility update also discards the
timer; recovery through a consistent versioned window begins a new interval. Explicit Open and
Grace configurations keep their requested rollout behavior. A zero minimum interval permits
immediate closure only when every existing validity and resolution condition holds.

`AdmissionStatus` exposes the accepted epoch, remaining minimum interval, effective mode,
fallback reason, and independent resolved/required/connected current counts. Metrics publish
`tn_network_admission_transition_remaining_seconds`; fallback value 5 denotes an active minimum
interval with otherwise valid inputs. Unresolved or invalid inputs take precedence over that
reason so operators can see why waiting alone will not close the policy.

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

## Operational rollout

Start in Open, populate configured bootstrap and trusted identities on every swarm, and inspect
the window in Grace before requesting Closed. Repair stale, contradictory, or unresolved inputs
before enabling it. `NetworkHandle::admission_status` exposes configured/effective modes, epoch,
fallback cause, authenticated current-record count, required-record quorum, and connected current
peers separately. The `tn_network.admission_*` gauges carry the existing primary/worker labels.
`admission_mode` is Open = 0, Grace = 1, Closed = 2. `admission_fallback` is healthy = 0,
missing = 1, stale = 2, contradictory = 3, unresolved = 4.

Resolved records prove a usable identity map, connections prove transport reachability, and
node consensus readiness proves the node's separate consensus prerequisites. None substitutes
for another. Requesting Closed cannot force it active while decision inputs remain unresolved.
During an incident, select Open or Grace in configuration and restart, or restore the current
authoritative renewal and validated records to recover automatically.
