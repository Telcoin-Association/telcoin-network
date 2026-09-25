# QUIC release fixtures

This is the first stage of [issue #1432](https://github.com/Telcoin-Association/telcoin-network/issues/1432).
It establishes reusable transport fixtures and ordinary CI evidence. Retry policy,
committee behavior, source-spoofing defenses and production resilience remain
separate acceptance work.

## What runs

Two independent Cargo workspaces build the same process fixture against
libp2p-quic 0.13.1 (libp2p 0.56.0) and 0.14.0 (libp2p 0.57.0). Each has its own
committed lockfile and target directory. No process links both transport versions.
The current fixture's libp2p, QUIC, TLS, Quinn, quinn-proto and rustls versions must
match the node's root lockfile. The controller fails on drift.

Both binaries compile the production `QuicConfig` source, including defaults and
the exact mapping called by `ConsensusNetwork`. They accept a JSON configuration
file with the same fields as the node's `quic_config` section. An empty object
selects production defaults. This isolates the transport from consensus, storage,
peer management and swarm policy; it does not instantiate a complete node.

The four-direction matrix includes both mixed-release directions and the two
same-release controls. Each run uses one dialer identity and transport for three
fresh authenticated connections to the listener. Each connection verifies three
32 KiB bidirectional stream exchanges. Application acknowledgements synchronize
closure so reconnects do not depend on sleeps. JSON events retain per-connection
and per-stream elapsed time; no benchmark threshold is inferred from loopback.

A bound UDP relay then forwards the client's datagrams and drops server replies,
observing the resolved transport's outbound timeout and, when an Incoming event
exists, its inbound timeout. Every observed failure must be a QUIC timeout.
The harness records absence of an Incoming event explicitly, since a listener
that sends Retry can fail to produce an accepted-connection event. This fixture
does not require unconditional acceptance. A watchdog bounds the harness;
elapsed deadlines are evidence, not assertions about wall-clock scheduling.

The default handshake timeout is 65 seconds and the idle timeout is 30 seconds.
Both configured and applied values are captured. The effective deadline depends
on the resolved implementation and handshake path; inspect the recorded timeout
observations rather than assuming that the configured handshake timeout wins.

## Run locally

Use the toolchain selected by the repository's `rust-toolchain.toml`:

```sh
cargo build --locked --manifest-path testing/quic-interop/releases/0.13.1/Cargo.toml
cargo build --locked --manifest-path testing/quic-interop/releases/0.14.0/Cargo.toml
python3 testing/quic-interop/run.py \
  --listener "$PWD/testing/quic-interop/releases/0.14.0/target/debug/quic-interop" \
  --dialer "$PWD/testing/quic-interop/releases/0.13.1/target/debug/quic-interop" \
  --listener-release 0.14.0 --dialer-release 0.13.1 \
  --output /tmp/quic-0.13.1-to-0.14.0
```

Python 3.11 or newer is required. Swap the binaries and release labels for the
reverse direction. Build artifacts are separate from the node's target directory.
When updating the node's transport dependencies, update the current fixture lock
as part of the same change. Preserve the old fixture lock as rollout evidence.

## Cadence and environments

| Lane | Cadence | Coverage and artifacts |
| --- | --- | --- |
| Ordinary CI | Every PR and merge group, weekly, and manual dispatch | Four directions on loopback; config mapping test; connections, streams, reconnects, timeout observations; JSON events, binary hashes, candidate commit, toolchain, dependency trees and lockfiles |
| Isolated privileged networking | Before Retry feature release and on relevant transport changes | Pending: controlled spoofing, packet loss/reordering, token scenarios, Accept/Retry/Refuse/Ignore, bounded polling and wakeups |
| Representative NICs | Deployment qualification and hardware/network changes | Pending: NIC/driver/offload settings, bandwidth/RTT/load, resource budget and honest committee reconnect acceptance criteria |

The ordinary runner records observed interfaces, capability bits and availability
of `ip`, `tc` and `ethtool`. When `unshare` exists, it probes an isolated network
namespace and records success or failure. It never enables spoofing or modifies
host networking. A successful namespace probe is not proof that all privileged
tests are supported. Hosted-runner loopback is not representative NIC evidence.

Keep both mixed-release directions throughout the 0.13.1 to 0.14.0 rollout and
while supported deployments can still run 0.13.1. Remove that coverage only in an
explicit maintenance PR after transport owners confirm the supported deployment
inventory has no old-release peers and document the end of the compatibility
window. This fixture PR sets no calendar retirement date.

## Remaining release evidence

Keep issue #1432 open after this stage. The Retry feature needs resolved-semantics
tests for valid, expired, malformed, replayed and incorrectly bound tokens,
including restart/key changes, loss and reordering. Do not assume tokens are
single-use. It also needs all listener outcomes, including no-accept paths,
bounded polling with preserved wakeups, honest committee reconnects in both
supported directions, and the additional checks tracked in the related security
advisory.

For every later run, retain the candidate commit and binary hashes, test cadence,
environment and raw result artifacts. Complete those checks before feature
release. Passing these fixtures alone is not production deployment qualification.
