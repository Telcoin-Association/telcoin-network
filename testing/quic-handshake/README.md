# QUIC handshake cost profile

This shared profiling harness supports [#1436](https://github.com/Telcoin-Association/telcoin-network/issues/1436),
the certificate work in [rust-libp2p#6633](https://github.com/libp2p/rust-libp2p/issues/6633),
and the early-refusal investigation in [#1435](https://github.com/Telcoin-Association/telcoin-network/issues/1435).
It preserves the node's accepted groups and authentication policy.

The [measured decision](DECISION.md) retains all current groups and includes the
current-provider results and all four required stock release pairings.

## Reproduce

Use Rust 1.94 and Python 3.12 or newer, from the repository root:

```sh
python3 -P testing/quic-handshake/prepare.py
CARGO_TARGET_DIR="$PWD/testing/quic-handshake/target" \
  cargo +1.94 build --release --locked -j 2 \
  --manifest-path testing/quic-handshake/Cargo.toml
python3 -P testing/quic-handshake/run.py \
  --samples 1000 --output testing/quic-handshake/results/local
```

Preparation downloads the published libp2p-tls 0.7.0 archive, checks its checksum
against the node's lockfile, checks the three instrumented source hashes, and
copies it into the ignored `generated` directory. For an offline preparation,
pass `--tls-source /path/to/cargo/registry/src/.../libp2p-tls-0.7.0`.
The registry is never edited. This standalone Cargo workspace is outside the
node's dependency graph. Its patch and counters are disposable profiling tools,
not a production TLS fork or permanent negotiated-group metric.

The runner refuses version drift for libp2p-tls, rustls, aws-lc-rs, aws-lc-sys and
rustls-webpki. Commit any intentional lockfile update with new measurements and
updated source hashes. CI builds and checks this harness, runs every scenario,
and retains raw samples. It has no timing threshold.

## Experiments and measurements

The four `kx-*` experiments measure the current provider's client key generation,
server exchange and client completion directly, and require identical shared
secrets. They isolate group operations from certificate work and TLS bookkeeping.

Every successful sample completes both rustls QUIC TLS state machines, checks
the negotiated group on both peers, compares authenticated TLS exporters, and
revalidates the listener's client certificate to extract the expected PeerId,
as libp2p-quic does after establishment. Stock certificate and session caches are
retained for reconnects. Other scenarios disable client resumption explicitly
so that a warm session cannot hide full-handshake costs.

| Scenario | Client | Listener | Required outcome |
| --- | --- | --- | --- |
| `default` | Default groups | Default groups | Full handshake, default first group |
| `reverse-server` | Default groups | Default groups in reverse order | Same group as `default` |
| `reverse-client` | Default groups in reverse order | Default groups | Client's first group |
| `x25519`, `p256`, `p384`, `hybrid` | One named group | Default groups | That group succeeds |
| `reconnect` | Stock session cache | Stock session cache | First full, then resumed |
| `incompatible` | Pure MLKEM768 only | Default groups | Incompatible-group rejection |
| `wrong-peer` | Requires another PeerId | Default groups | Certificate rejection |

The listener measurements separate its first ClientHello processing, which
precedes a client certificate, from total TLS input processing and the final
PeerId extraction. Temporary instrumentation measures parsing, certificate
self-signatures, identity-extension signatures and transcript signatures without
changing their results or error propagation. Phase counts must match the
expected full/resumed path or the run fails. Failed handshakes never become
successful timing samples.

`report.json` contains median and p95 elapsed times, child-process CPU time,
dependency versions, platform, compiler, binary and source hashes. The adjacent
JSONL files retain every sample, including the first cold sample. Summaries omit
that first sample; reconnect summaries contain only resumed handshakes.

Timing scope matters: the profile transfers TLS handshake bytes directly through
rustls's QUIC API. It includes both peers but no UDP, loss, QUIC packet protection,
swarm scheduling or node workload. Listener timings are monotonic elapsed time,
not thread CPU time. Child CPU includes both peers, initialization, instrumentation
and JSON output. Do not interpret it as listener-only cost, deployment capacity,
an attack rate, or a measured benefit from early refusal. Ed25519 identities and
the stock generated certificate are the measured authentication workload;
other accepted identity/signature schemes require separate samples.

## Stock release interoperability

Reuse the independent fixtures from
[PR #1449](https://github.com/Telcoin-Association/telcoin-network/pull/1449)
for real QUIC connection, stream and reconnect timings. Build its separately
locked 0.13.1 and 0.14.0 binaries, then run all four listener/dialer pairings with
`testing/quic-interop/run.py`. Its README contains the exact invocation and
fixture boundaries. Record the fixture commit, both binary hashes and all four
result files alongside this profile. Neither a same-release pass nor one mixed
direction replaces the other mixed direction.

These stock fixtures measure honest loopback reconnects independently from the
instrumented TLS profile. They do not qualify representative NICs, network loss,
committee reconnect bursts, sustained concurrency or production capacity.
Keep both releases in both directions until the rollout's retirement decision.

## Decision boundary

Group cost alone does not authorize removing a supported group or post-quantum
protection. Reversing the server list does not implement server preference in
the resolved TLS 1.3 selection path. Any future narrowing must identify its
purpose, removed support, security and interoperability consequences, and the
maintainer/cryptographic-review decision. It must separately qualify the
selected CPU/timing criteria and extend maintenance coverage for any production
TLS/configuration patch. This harness itself changes no listener policy.
