# Network diagnostic budgets

Recurring warnings and errors in `tn-network-libp2p` use per-call-site budgets. Each
`bounded_warn!` or `bounded_error!` call site admits four detailed diagnostics per 60-second
observation window across all primary and worker swarms. One call site cannot use the budget of
another call site, so a remotely driven site cannot suppress the first occurrence of an unrelated
local failure. The next window restores the burst. No address, peer, message, record key, RPC URL,
or error string creates limiter state. Each call site owns one static window that contains only an
optional start time and two counters, and the flush registry holds at most one entry per call site.

The first events retain the original diagnostic fields and severity. Suppressed events do not
evaluate tracing fields or format errors. A summary reports `event_class`, `site` (the `file:line` of the
call site), `emitted`, `suppressed`, and the actual `window_seconds`. The
`tn_network.diagnostics_suppressed_total` counter counts suppressed events with `class` and `site`
labels, both from compile-time vocabularies. A call site in shared code, such as the Kademlia store
or the peer manager, has one window for all swarms, so a summary does not name the swarm. Each active network loop polls a reporting timer, with missed
ticks skipped. Shared windows ensure multiple swarms cannot duplicate a summary. The event path
also rolls expired windows, so a busy event stream does not depend on timer scheduling. A quiet
tail is reported on the next timer poll, at most one reporting interval after its window expires
when the loop is being scheduled. Neither formatting nor the tracing sink holds the counter lock.

The limit is four details and at most one suppression summary per call site per observation window.
The existing connection-limit aggregate warning is a separate, already bounded per-swarm window.
No attacker is required to trigger repeated diagnostics: an unavailable database or application
consumer can produce the same amplification as malformed remote input.

## Controlled sites

The class argument at every `bounded_warn!` or `bounded_error!` call names the kind of failure in
summaries and metrics. The budget belongs to the call site. Recoverable `SwarmEvent::ListenerError`
events use the per-swarm listener error log in `consensus.rs` instead of a budget.

| Class | Controlled paths |
| --- | --- |
| `Swarm` | Outer network-event failures and mismatched external-address reports. |
| `Command` | Command failures and penalties for unresolved application peers. |
| `Gossip` | Validation-result reporting, rejected gossip penalties, and application forwarding. |
| `Request` | Request forwarding, duplicate request IDs, codec I/O, and unsupported protocols. |
| `Stream` | Unknown inbound stream peers and failed application forwarding. |
| `Kademlia` | Startup record scrubbing, local publication, and query/provider failures. |
| `Record` | Invalid peer records, banned PUT sources/publishers, retention failures, and rejected provider writes. |
| `Store` | All Kademlia store warnings and errors, including database failures and corrupt provider rows. |
| `PeerAdmission` | Source admission failures and banned/disconnecting established-peer inconsistencies. |
| `PeerState` | Reputation, penalties, bans, dialing, committee/trust unbanning, and lifecycle inconsistencies. |
| `PeerRecord` | Confirmed-identity repair, invalid initial peer state, configured binding conflicts, and invalid RPC advertisements. |
| `Reply` | Failed oneshot replies through `send_or_log_error!`, including dial replies. |

The exported `send_or_log_error!` macro retains its existing API. Its caller-resolved `error!` is
bound to `bounded_reply_error` in each production module that invokes it: `consensus`,
`peers::all_peers`, and `peers::manager`.

## Immediate transitions and other levels

`ListenerClosed` bypasses suppression for both its unexpected-close cause and terminal loss of
all listeners. These are finite local listener lifecycle transitions. Creating another listener
requires a local command; remote identities cannot allocate listeners. Terminal loss returns
`AllListenersClosed` immediately. Ordinary shutdown, connection, and routing transitions retain
their existing info/debug/trace output. Debug/trace output is explicitly opt-in and outside this
warning/error budget. The existing connection-limit warning reports aggregate denials at most
once per 60 seconds per configured swarm and therefore needs no additional gate.

No recurring production warning/error site in the peer and record paths is left uncontrolled.
Logging suppression never changes penalties, admission decisions, storage results, or replies.
`listener_errors_total`, gossip rejection counters, inbound denial counters, and connection-limit
rejection counters are updated outside the logging gate, so their values remain exact.

## Metric-label audit

`metrics.rs` resolves production handles using configured `network` values (`primary`,
`worker-{id}`) and fixed enum vocabularies for service class, shed/failure reason, outcome,
connection limit, penalty severity, and source direction. Worker IDs come from local configuration.
`tn_network.listener_errors_total` has the configured `network` label and a fixed `reason` label.
`tn_network.diagnostics_suppressed_total` has a fixed `class` label and a `site` label whose values
are the compile-time `file:line` call sites of this crate.
Existing QUIC observations are cumulative decisions and occupancy gauges with no address labels.

There are no production per-address or per-peer metric labels in this crate. Peer IDs, BLS keys,
raw errors, record keys, RPC endpoints, and remote topic strings remain diagnostic fields only;
they never become metric label values. Thus committee rotation or trust removal has no per-peer
metric series to clean up, and changing remote identities cannot increase metric cardinality.
The `event_class` summary field is selected exclusively from the fixed Rust enum, and the `site`
summary field is a compile-time constant.

## Regression coverage

The production window and emission tests supply time explicitly. They cover the exact interval
boundary, burst exhaustion, suppressed counts, quiet-tail summaries, delayed polls, low-rate
diagnostics, recovery, saturating counts, independent windows for call sites in one class, lazy formatting of
changing remote errors, and calling sinks outside the limiter lock.

The network regression feeds repeated listener errors, malformed gossip, and malformed PUTs
through the actual handlers, checks exact listener/gossip counters with only configured labels,
processes a subsequent command, and confirms that terminal listener loss logs immediately.
