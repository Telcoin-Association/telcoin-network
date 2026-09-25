# Validated handshake-start accounting

`tn_network_libp2p::admission` provides the accounting contract for
[issue #1434](https://github.com/Telcoin-Association/telcoin-network/issues/1434).
It does not install a budget, change the shipped transport, or select production rates.
The current libp2p pending-inbound swarm hook supplies an address without explicit
address-validation evidence. Calling the accountant there would not establish the required
boundary before expensive handshake work.

## Transport integration contract

The future transport adapter must verify an address-bound QUIC Retry token, construct
`ValidatedSource` from that result, and call `HandshakeStartBudget::admit` once before
starting the expensive handshake. Merely receiving a packet, an address, a claimed PeerId,
or a swarm event cannot supply `ValidationProvenance::QuicRetry`. The provenance type
records a trusted in-process assertion; it does not verify tokens or make an arbitrary
caller trustworthy. A changed source address needs its own validation.

Install one explicit policy at process startup. Give the same `HandshakeStartBudget`
reference to the primary and every worker transport. Additional listeners and restarted
swarms obtain the installed reference through `process()`; a second installation fails.
Proceed only on `Ok(StartOutcome::Admitted)`. Every other outcome or accounting error
rejects the start. No PeerId or address allowlist bypasses any allowance. In particular,
Retry proves reachability, not committee membership. This contract supplies no priority
class or exemption for authenticated traffic.

## Accounting and bounded state

Each admitted start atomically spends one process, source, and prefix allowance. A source
key ignores ports and peer identities, canonicalizes IPv4-mapped IPv6 to IPv4, and groups
IPv4 by /24 and native IPv6 by /64. Pending-occupancy integration must reuse `SourceKey`
and `SourcePrefix` so it has identical normalization, while keeping its reservations
separate. Closing a connection releases occupancy; it never refunds a handshake start.
Those prefix widths are the accounting contract, not calibrated production rate values.

`StartRate` takes a burst and a duration to replenish one start. Debt uses monotonic
time with checked arithmetic, retaining fractional refill time. The process allowance
starts empty, including after a full restart. After one aggregate refill interval one
start becomes eligible; after a full burst duration the whole burst becomes eligible.
Per-source and per-prefix allowances start full but remain inside the physical process
allowance. Ordinary reconnect bursts need no attacker to hit an undersized policy.

Each source/prefix table has a separate explicit capacity. Each entry has one debt record
and one ordered expiry record. Admission and eviction take O(log capacity) work without
scanning the table. Only completely refilled debt may be evicted. If every entry still
has debt, a new key is rejected without changing any allowance. Existing keys can still
spend their remaining credit. Table churn cannot discard debt to regain an early burst.

Policy replacement is rejected until **all** process, source, and prefix debt has refilled.
A successful replacement clears only refilled entries, preserves counters, and starts
the new aggregate allowance empty. Continuous traffic can therefore defer a policy change;
operators must quiesce admission to guarantee an update. There is no reset or refund API.
Future occupancy integration must also respect live reservations when transitioning policy.

`snapshot()` exports six saturating process counters and two retained-entry counts.
`StartOutcome::label()` supplies the fixed metric label vocabulary; the accountant emits
no per-attempt logs and exports no source/identity labels. A metrics adapter should export
one process snapshot, rather than summing duplicate snapshots from every swarm. A poisoned
accounting lock fails closed with `PolicyError::Unavailable`.

## Calibration and remaining acceptance work

The deterministic tests exercise the contract using synthetic rates. They cover shared
NATs and mapped addresses, IPv4 and IPv6 prefixes, concurrent callers, bounded churn,
refill boundaries, restarts, policy changes, and fixed-cardinality telemetry. They are
not a QUIC reconnect benchmark, proof of the transport boundary, or hardware qualification.

Before enabling a production policy:

1. Integrate the minimum Retry transport build at the validated pre-handshake boundary.
   Share normalized keys and transition rules with pending-occupancy accounting.
2. Measure honest reconnect bursts with primary and worker swarms, the supported worker
   count, shared NATs, multiple prefixes, restarts, and policy changes.
3. Agree on peer population, network conditions, CPU/state budgets, and reconnect
   failure/time bounds, then select explicit rates, bursts, and table capacities.
4. Repeat reconnect acceptance with those values. Table saturation is deliberately
   fail-closed and does not guarantee fairness under unlimited source diversity.
5. Qualify the validator profile on representative hardware with host firewall
   enforcement disabled. Include source diversity and compare received traffic with
   generator limits. Treat the larger hub/shared-NAT profile as separate capacity work.

The accounting contract alone does not complete those acceptance criteria or close #1434.
