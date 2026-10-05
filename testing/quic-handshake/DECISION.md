# Retain the current QUIC key-exchange groups

Decision for [#1436](https://github.com/Telcoin-Association/telcoin-network/issues/1436):
retain X25519MLKEM768, X25519, secp256r1 and secp384r1 for this rollout. Preserve
the resolved provider's default ordering and post-quantum support. No production
configuration or authentication change is selected.

The measurements establish that group costs differ. They do not establish a
deployment capacity problem, an acceptable rejection cost for stock peers, or a
security-policy justification for narrowing support. Server-list reordering
also failed to change selection with the same client offer. A future narrowing
proposal needs representative concurrent workloads, explicit acceptance
criteria, removed-support and security consequences, and maintainer and
cryptographic review before implementation.

## Current-provider evidence

Measured on 2026-09-24, macOS 26.4 arm64, using Rust 1.94 release builds and the
node lockfile at `5736cc30012c5ff25913898e318a74df308f13d9`: libp2p-tls 0.7.0,
rustls 0.23.37, aws-lc-rs 1.16.2, aws-lc-sys 0.39.1 and rustls-webpki 0.103.10.
The profile uses the same aws-lc, std and prefer-post-quantum rustls features as
libp2p-tls. No historical ring measurement is used as a current-provider result.

The [machine-readable profile](evidence/2026-09-24-macos-arm64.json) contains
compiler details, dependency and source hashes, the binary hash, raw-sample
hashes, medians, p95 values and total child-process CPU time. Measurements were
taken from the pre-commit working tree identified by those source hashes.
There are 1,000 samples in each of 14 scenarios. The tables below use the 999
warm samples; the first sample remains in the raw evidence. This is one serial
run on a development machine, with no claim of isolated CPU scheduling.

Direct provider operations, median elapsed microseconds:

| Group | Client start | Server exchange | Client completion |
| --- | ---: | ---: | ---: |
| X25519 | 11.08 | 36.25 | 25.33 |
| secp256r1 | 14.42 | 60.17 | 45.79 |
| secp384r1 | 56.92 | 208.25 | 151.88 |
| X25519MLKEM768 | 26.63 | 51.63 | 39.33 |

Each direct exchange requires matching client/server shared secrets. The P-384
server operation was approximately four times the hybrid operation in this run.
This comparison isolates group work; it is not an estimate of listener capacity.

Instrumented in-memory QUIC TLS, median elapsed microseconds:

| Scenario | Negotiated group | Listener first flight | Both-peer handshake |
| --- | --- | ---: | ---: |
| Default | X25519MLKEM768 | 106.08 | 1030.17 |
| Server order reversed | X25519MLKEM768 | 110.75 | 1075.79 |
| Client order reversed | secp384r1 | 274.63 | 1348.13 |
| X25519 only | X25519 | 78.33 | 889.38 |
| P-256 only | secp256r1 | 123.50 | 1130.25 |
| P-384 only | secp384r1 | 278.92 | 1382.29 |
| Hybrid only | X25519MLKEM768 | 103.96 | 1014.17 |
| Stock resumed reconnect | X25519MLKEM768 | 76.21 | 326.46 |

Every successful sample verified the group on both peers, authenticated TLS
exporters, the listener's extracted client PeerId and the expected full/resumed
state. Pure MLKEM768-only offers were rejected by the unchanged default listener
in all 1,000 attempts. All 1,000 wrong-expected-PeerId attempts were rejected.

The shared verification profile observed three certificate parses, three
certificate self-signature checks, three identity-extension checks and one
transcript check per full inbound handshake, including final PeerId extraction.
Resumed reconnects observed one parse, one self-signature check and one extension
check during extraction of the authenticated cached certificate. The runner
checks these counts, rather than assuming resumption eliminated particular work.
Per-phase elapsed times are retained for the certificate and early-refusal
investigations. No certificate, extension or transcript verification is removed.

The listener first flight occurs before the client certificate is available.
It remains work that identity-based early refusal cannot avoid at that point.
This study does not measure refusal frequency, another identity/signature scheme,
adversarial concurrency, committee bursts or an authoritative admission policy.
The early-refusal launch prerequisites remain independent of this decision.

## Required stock releases

The [stock interoperability evidence](evidence/2026-09-24-stock-interop.json) uses
the unchanged fixtures from [PR #1449](https://github.com/Telcoin-Association/telcoin-network/pull/1449),
commit `c9ae96a57efe3cd38f8f345539925f159e90c2a9`. Each release has an independently
resolved lockfile and debug binary. All four pairings passed three connections
with one dialer identity, nine verified 32 KiB bidirectional streams and three
listener-observed closures. The controller's stalled-handshake checks also passed.

Dialer-observed loopback connection times, milliseconds, in connection order:

| Dialer | Listener | First connection | Reconnect 1 | Reconnect 2 |
| --- | --- | ---: | ---: | ---: |
| 0.13.1 | 0.13.1 | 2.973 | 1.642 | 1.547 |
| 0.14.0 | 0.13.1 | 32.696 | 1.665 | 1.642 |
| 0.13.1 | 0.14.0 | 41.022 | 3.085 | 2.235 |
| 0.14.0 | 0.14.0 | 52.975 | 2.159 | 1.747 |

These are debug-build loopback observations, separately scoped from the release
TLS microbenchmarks. They establish honest interoperability, not a latency SLO
or representative-NIC qualification. Both mixed directions remain required;
this result retires neither release.

## Reproduction and maintenance

Follow the [harness instructions](README.md). CI runs every experiment and
uploads raw samples, without enforcing timing thresholds. The separately locked,
hash-checked instrumentation stays outside the node's workspace and is generated
on demand. This no-change decision introduces no production TLS fork, patch
maintenance obligation for a modified node dependency, or permanent metric.

The harness checks were confirmed with four temporary mutations: ignoring client
groups, ignoring server order, accepting the real identity in the wrong-peer
experiment, and disabling reconnect resumption. Each failed at its intended
invariant. The source was restored and all scenarios passed again. Rust 1.94 and
nightly clippy, both nightly feature lanes, formatting, Python compilation and
whitespace checks passed. Whole-node attestation remains the repository's
separate post-push gate.
