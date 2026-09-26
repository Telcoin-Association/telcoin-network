# Early identity refusal

## Decision

Keep identity refusal at the existing established-connection checks. Do not add
an earlier TLS admission callback for
[#1435](https://github.com/Telcoin-Association/telcoin-network/issues/1435).
This is the issue's explicit no-change outcome, reviewed against Telcoin Network
[`5736cc30`](https://github.com/Telcoin-Association/telcoin-network/commit/5736cc30012c5ff25913898e318a74df308f13d9)
on 2026-09-24.

An earlier check can only save work for identities the admission policy already
rejects. The current inbound identity check rejects self-connections and banned
peers. It does not require an identity to belong to a committee, trusted-peer
set, or bootstrap set. Moving that check into TLS would therefore still admit a
fresh, unbanned identity at an otherwise admissible address with available
connection capacity. It would not establish an authoritative admission policy.

No current-provider benchmark was run for this decision, and no handshake CPU,
latency, or throughput savings are asserted. Potential savings on existing
refusal cases remain unmeasured. The decision is to avoid adding a second policy
enforcement point without a separately defined and tested admission contract or
evidence that its benefit justifies the maintenance cost.

**This decision does not complete the independent launch requirement for
authoritative admission.** That requirement needs its own implementation and
tests. QUIC Retry, connection limits, and this investigation's closure do not
satisfy it.

## Source evidence

The following observations are static source checks, not deployment or
performance qualification:

| Boundary | Observed behavior | Source |
|---|---|---|
| Established inbound identity | Rejects the local PeerId and banned peers, then returns a connection handler. There is no committee/trusted/bootstrap membership check in this hook. | [`PeerManager::handle_established_inbound_connection`][inbound] |
| Established connection count | The connection-limit behavior caps connections per PeerId. A new identity has a separate count. | [`connection_limits`][limits] |
| QUIC configuration | Telcoin configures the QUIC transport through `with_quic_config`. The pinned transport builds its TLS configurations using libp2p-tls; its TLS configuration fields are private. | [Telcoin swarm construction][swarm], [libp2p-quic configuration][quic-config] |
| TLS provider | libp2p-tls selects the AWS-LC provider for both client and server configurations and installs its certificate verifier. | [libp2p-tls configuration][tls-config] |
| Certificate and transcript checks | The verifier parses the presented certificate and verifies the TLS 1.3 handshake signature. An early refusal must not turn a claimed identity into authenticated identity. | [libp2p-tls verifier][tls-verifier] |

The [lockfile][lockfile] pins libp2p 0.57.0, libp2p-quic 0.14.0,
libp2p-tls 0.7.0, quinn 0.11.9, quinn-proto 0.11.18, and rustls 0.23.37.
These versions and the provider must accompany any later profile. A result from
a different provider or dependency revision is not evidence for this baseline.

The established identity hook is only one part of connection management.
Address screening, capacity checks, scoring, and subsequent disconnection still
apply as described in [Peers](peers.md). This decision does not characterize
every unknown peer as guaranteed a usable connection.

## Requirements before reconsidering

### Authoritative admission first

Define and test the established-connection admission contract separately before
introducing an optimization of that contract. Its tests must settle:

- The allowed identities for each node role, including how committee, trusted,
  and bootstrap sets combine and behave across committee rotation.
- How policy revisions become visible to connection admission, including a
  revision that changes while a handshake is in progress.
- Whether existing live connections survive a policy change, and how any
  required disconnections are enforced.
- The behavior for stale or missing policy inputs, the launch fallback, the
  explicit condition that closes that fallback, and recovery after stale input.

The established gate must remain authoritative if an earlier refusal is added.
Both enforcement points must use the same contract. An early pass is not a
reservation or permission to bypass the current policy at connection admission.

### One shared handshake profile

Use one current-provider baseline and workload definition with
[the certificate-verification investigation][certificate-work] and
[the key-exchange investigation][group-work]. Record the Telcoin commit,
lockfile, provider, build options, hardware, instrumentation, workload counts,
and raw results so the three investigations do not attribute the same saved work
to different changes.

Measure these boundaries and cases separately:

| Dimension | Required evidence |
|---|---|
| Before a peer certificate is available | Listener and handshake work already performed before an identity-based refusal could run. Do not credit this work as saved by a certificate-stage check. |
| After a peer certificate is available | Identity extraction and policy lookup cost, certificate/extension verification, TLS transcript verification, and remaining work until the established gate. Measure the candidate's actual refusal point. |
| Refusal frequency | Counts and reasons for policy refusals, including fresh unknown identities, self-connections, banned peers, and capacity refusals. Keep malformed authentication separate from policy denial. |
| Full and resumed handshakes | Counts, CPU cost, and latency for each path. Determine whether resumption occurs with the shipped configuration; report an unexercised path explicitly rather than assuming coverage. |
| Honest operation | Initial connections and realistic reconnects, including restart and committee rotation, with acceptance rate and latency distributions as well as refused-handshake cost. |

Estimate the benefit using measured refusal frequencies and the work actually
avoided after the candidate check, including the extra lookup cost on accepted
connections. Set an acceptance threshold before comparing the candidate with
the baseline. Publish the result even if it supports keeping this no-change
decision. A Retry result alone supplies neither identity admission nor evidence
of savings from earlier identity refusal.

### Conditions for a later implementation

If the admission contract and profile justify a change, require all of the
following before shipping it:

- Treat an identity extracted before signature verification as untrusted. A
  claimed allowed identity without valid proof must receive no authentication,
  trusted scheduling, or quota exemption.
- Preserve certificate, libp2p identity-extension, and TLS transcript
  verification for accepted peers. Test invalid proofs claiming an allowed
  identity as well as valid allowed and valid refused identities.
- Apply current admission policy to resumed connections, including sessions
  created before a ban, committee rotation, or other policy revision. Test stale
  input fallback and recovery on the applicable full and resumed paths.
- Demonstrate the measured benefit while preserving the required stock-peer
  interoperability matrix and honest reconnect behavior. Coordinate that
  coverage with [the QUIC interoperability work][interop-work].
- Record the owner, upstream baseline, regression coverage, and upgrade checks
  for every additionally modified TLS or transport crate. Do not assume the
  existing QUIC tuning callback exposes a certificate-verifier hook.

[inbound]: https://github.com/Telcoin-Association/telcoin-network/blob/5736cc30012c5ff25913898e318a74df308f13d9/crates/network-libp2p/src/peers/behavior.rs#L87-L108
[limits]: https://github.com/Telcoin-Association/telcoin-network/blob/5736cc30012c5ff25913898e318a74df308f13d9/crates/network-libp2p/src/consensus.rs#L133-L138
[swarm]: https://github.com/Telcoin-Association/telcoin-network/blob/5736cc30012c5ff25913898e318a74df308f13d9/crates/network-libp2p/src/consensus.rs#L563-L582
[quic-config]: https://docs.rs/crate/libp2p-quic/0.14.0/source/src/config.rs
[tls-config]: https://docs.rs/crate/libp2p-tls/0.7.0/source/src/lib.rs
[tls-verifier]: https://docs.rs/crate/libp2p-tls/0.7.0/source/src/verifier.rs
[lockfile]: https://github.com/Telcoin-Association/telcoin-network/blob/5736cc30012c5ff25913898e318a74df308f13d9/Cargo.lock
[certificate-work]: https://github.com/libp2p/rust-libp2p/issues/6633
[group-work]: https://github.com/Telcoin-Association/telcoin-network/issues/1436
[interop-work]: https://github.com/Telcoin-Association/telcoin-network/issues/1432
