# Carried libp2p-quic maintenance record

This record qualifies the active vendored Retry and incoming queue bounds patch.
It applies to the default and Adiri Linux release dependency graphs. The
[TLS verification candidate](../libp2p-tls-6634/README.md) remains a separate,
unadopted candidate. Normal contribution review, release checks and security
reporting still apply.

## Source identity

| Field | Shipped source |
| --- | --- |
| Upstream | https://github.com/libp2p/rust-libp2p, `transports/quic` |
| Release | crates.io `libp2p-quic` 0.14.0 |
| Release tag | No `libp2p-quic-v0.14.0` tag was published in the upstream refs checked on 2026-09-30. The immutable registry archive and its VCS base identify the release. |
| Upstream base | `7171dce2f90c05ba7892d4ba926abb1881db27c7` from `.cargo_vcs_info.json` |
| Archive SHA-256 | `4f78ca359466657b380e469fe8c04df2f4447d1430838e6c3cef4a1c51ccb2ee` |
| Maintained mirror | [Telcoin's tracked vendored tree](../../../patches/libp2p-quic) |
| Exact patch revision | [`a740a484e4944871f8a81e825ce84dd1559ee158`](https://github.com/Telcoin-Association/telcoin-network/commit/a740a484e4944871f8a81e825ce84dd1559ee158) |
| Override | Root `[patch.crates-io]`, `libp2p-quic = { path = "patches/libp2p-quic" }`, excluded from workspace membership |
| Complete normalized diff | [upstream.diff](../../../patches/libp2p-quic/upstream.diff), SHA-256 `70bcf353a43adf946424c40b663fe02e871a1b0b6ad346c2f19ecfbb5c92973c` |
| Runtime tree fingerprint | `51fa07a9f61861af00723b4b96f8e68deece21a021b5cee870903ebf669d5f26` |

The patch revision identifies the runtime source introduced on main. This PR
updates documentation and registry crypto pins without changing that source.
The fingerprint excludes only `PATCH.md` and `upstream.diff`; per-file hashes
are retained in the source evidence. Cargo selects the path package, not an
unused override. `libp2p-tls`, `quinn`, `quinn-proto` and crypto dependencies
remain registry packages. No source replacement configuration is involved.

[sources.py](../../../etc/transport-patches/sources.py) verifies the official
archive checksum, normalizes `Cargo.toml.orig`, removes registry-only files,
and compares the regenerated diff byte for byte with the tracked diff. It
records actual package IDs, sources and dependencies with `cargo +1.94 metadata
--locked --format-version 1 --filter-platform x86_64-unknown-linux-gnu`, and
release features with `cargo +1.94 tree --locked --target
x86_64-unknown-linux-gnu -p telcoin-network -e normal,build --prefix none
--format '{p}|{f}'`. Both commands run separately with the default features
and with `--features telcoin-network/adiri`.

## Accepted ownership and update plan

On 2026-09-30, @MavenRain explicitly accepted both maintenance and advisory
responsibility, including weekly advisory review and review on each transport
dependency update. @MavenRain is the approving core maintainer for this
maintenance process. Approval of a particular release still follows the
repository's PR and attestation gates.

The next target is the first upstream `libp2p-quic` release after 0.14.0, or an
earlier upstream revision implementing the required incoming decision API.
By 2026-10-07, @MavenRain will coordinate disclosure through [SECURITY.md](../../../SECURITY.md)
and submit or link an upstream change exposing Retry, Refuse and Ignore before
accept, queue bounds and a bounded listener poll. No public security details
beyond the existing patch are needed for the submission record.

On every transport dependency update, @MavenRain must:

1. Compare the maintained diff against the new upstream source and check every
   overridden package and version constraint.
2. Refresh the source revision, archive checksum, diff, fingerprints, default
   and Adiri resolution evidence, and provider comparison.
3. Reassess upstream advisories against both the base and the diff. Rerun the
   affected registry/path controls and review Dependabot settings and graph.
4. Rerun the required stock-peer matrix, identity and key logging checks.
   Review failures and changed defaults before approving the update.

Remove the vendored tree and manifest override together when a released
upstream API supplies the incoming decision and queue bounds with the required
behavior. Set the same node values through that API, update the lockfile,
verify registry resolution, pass the same checks and archive this record with
the replacement release. Upstream acceptance alone does not satisfy removal.
Any future non-registry TLS, Quinn or crypto patch must extend this record,
its source-form controls and the source checker before it ships.

## Advisory coverage and measured gaps

The selected tools are `cargo-audit` 0.22.2 and `cargo-deny` 0.20.2 with the
RustSec database. [advisories.py](../../../etc/transport-patches/advisories.py)
retains tool versions, exact commands, configurations, actual Cargo package
IDs, reports, exit statuses and hashes. The initial database revision is
`9b3a3b73a7f42606494c943e95f8196e9994df46`.

The control uses the real published `quinn-proto` 0.11.6 source, archive SHA-256
`ba92fb39ec7ad06ca2582c0ca834dfeadcaf06ddfc8e635c80aa7e1c05315fdd`.
[RUSTSEC-2024-0373](https://rustsec.org/advisories/RUSTSEC-2024-0373.html)
affects its `Endpoint::retry` behavior. The same archived crate is resolved
once from the registry and once by a path override, matching the carried
source form without changing the node lockfile.

| Source form | Expected finding | cargo-audit | cargo-deny |
| --- | --- | --- | --- |
| Registry `quinn-proto` 0.11.6 | RUSTSEC-2024-0373 | Detected, exit 1 | Detected, exit 1 |
| Identical path `quinn-proto` 0.11.6 | RUSTSEC-2024-0373 | Missed, exit 0 | Missed, exit 0 |

This proves a path coverage gap in both tools. The source-neutral manual match
checks the advisory's package and affected/fixed ranges against actual
resolution and the carried diff. The node resolves registry `quinn-proto`
0.11.18, which is outside the affected range (fixed in 0.11.7). The vendored
diff delegates Retry to Quinn and carries no Quinn backport. That advisory
therefore does not apply to this carried build. The control and match are
repeated in required CI; an empty scanner report cannot replace them.

The fresh scan also identified [GHSA-2mjx-qc3c-rqvc](https://github.com/advisories/GHSA-2mjx-qc3c-rqvc),
RUSTSEC-2026-0285, affecting the old rustls 0.23.37 pin. This PR updates rustls
to the fixed 0.23.45, AWS-LC to 1.18.1 / 0.45.0 and rustls-webpki to 0.103.15.
The scoped updated transport scan has no findings. The workspace scan exits
1 with seven findings outside this transport scope; this record does not
claim the entire workspace is advisory-free.

Dependabot was checked separately on 2026-09-30:

| Capability | Observed result |
| --- | --- |
| Dependency graph | Registry Quinn, TLS and rustls packages recognized; vendored `libp2p-quic` absent from the main branch SBOM |
| Advisory alerts | Repository vulnerability alerts enabled (API HTTP 204); actual alert delivery for a vulnerable path override is unverified |
| Security updates | Disabled (`enabled: false`, `paused: false`) |
| Version updates | No `.github/dependabot.yml` configuration |

These are owned gaps, not evidence of non-registry coverage. @MavenRain reviews
RustSec and upstream rust-libp2p, Quinn, rustls and AWS-LC advisories weekly,
on each transport dependency update, and when an advisory arrives. Review
the actual base, package identity, enabled features and diff, record
applicability, then open a dependency update or escalate through the existing
security process. Next scheduled review: 2026-10-07. Any temporary exception
must name its owner and review deadline in this record. Monitoring and updates
use this process even when Dependabot cannot recognize the carried crate.

## Compatibility, crypto and production key logging

[qualify.py](../../../etc/transport-patches/qualify.py) builds three independent,
locked binaries: carried 0.14.0 with the node's Retry and transport defaults,
registry 0.14.0, and registry 0.13.1 (the preceding compatible QUIC release).
The stock manifests have no patch section. Their resolved IDs, lockfiles,
features and binary hashes are retained. This tests the transport rather than
a stock node build, since the node uses APIs added by the carried patch.

Each stock version listens and dials against the carried binary over both
IPv4 and IPv6. Each of the eight rows performs two authenticated 32-byte
echoes through the same listener identity, proving disconnect and reconnect.
A wrong expected PeerId must fail at the authenticated transport output
before application bytes are sent. This asserts the application identity
boundary, not a promise that a bare transport dial enforces a `/p2p` suffix.

The unchanged source baseline passed all eight rows and the identity control
locally on macOS. An initial run timed out; failed receipts were retained,
and bounded subprocess deadlines and two Tokio workers were used for the
successful repeat. The final updated stack and node configuration passed all
eight rows and sixteen exchanges in the required Linux CI lane on 2026-09-30.

The actual default and Adiri graphs retain the same feature selection when
compared with the registry-source control. `libp2p-tls` 0.7.0 explicitly uses
the AWS-LC provider, its libp2p TLS 1.3 cipher suite list and TLS 1.3 only.
Quinn selects `rustls-aws-lc-rs`; ring is also enabled in the resolved graph.
The rustls features retain `aws_lc_rs`, `logging`, `prefer-post-quantum`,
`ring`, `std` and `tls12`. Enabling `tls12` in rustls does not change the
libp2p TLS 1.3 protocol selection.

The baseline provider groups are `X25519MLKEM768`, `X25519`, `secp256r1` and
`secp384r1`. The complete ordered baseline provider cipher suite list is
retained beside the evidence. The required lane compares the updated provider
dump against that baseline and passed for all three peers. Any change requires
explicit review. Independent peers use the node's exact
rustls, AWS-LC and webpki pins and provider features rather than claiming
equivalence from crate versions alone.

Production policy: **`SSLKEYLOGFILE` must be unset in every node process.**
The stock TLS implementation installs `rustls::KeyLogFile`; the environment
variable can enable secret logging even without a Telcoin logging option.
The matrix runs with that variable removed. A separate positive control
enables it for ephemeral test identities, requires TLS traffic-secret labels,
then deletes the secret file and retains only labels. Secret files are never
uploaded as CI artifacts. The Linux positive control recorded handshake and
application traffic-secret labels and confirmed deletion of the file. This PR
does not enable production key logging.

## Required gate and evidence

The reusable [transport workflow](../../../.github/workflows/transport-patches.yaml)
runs for changes to transport source, dependencies, configuration, this
record or its recipes. It is a required dependency of `ci-success` on PRs
and merge groups, including maintainer and draft paths. For unrelated changes
the job succeeds after its scope check. A skipped, failed or cancelled job
cannot pass aggregation. The existing lint, tests, archive and attestation
requirements are retained.

The [Linux qualification run](https://github.com/Telcoin-Association/telcoin-network/actions/runs/36759742506/job/110038954563)
passed on commit `f6a36fb7b5bf1a96e2d82c4722515b9fcc5ec2fe`. It includes the
source/default/Adiri checks, both scanner source-form controls, eight peer rows,
identity rejection, provider comparison, key logging control, CI aggregation
tests and killed traffic/identity mutations. Both scanner database revisions
were `9b3a3b73a7f42606494c943e95f8196e9994df46`.
[ci-linux.json](evidence/ci-linux.json) retains the actual package IDs/features,
binary and report hashes, outcomes, immutable artifact ID and archive digest.
The existing on-chain attestation gate failed because this commit has no
attestation; full workspace validation and attestation remain required for merge.

Run from the repository root:

```sh
python3 -I etc/transport-patches/qualify.py --output /tmp/transport-peers
python3 -I etc/transport-patches/sources.py --output /tmp/transport-sources
python3 -I etc/transport-patches/mutations.py --peers /tmp/transport-peers
git clone https://github.com/RustSec/advisory-db.git /tmp/transport-db
python3 -I etc/transport-patches/advisories.py --output /tmp/transport-advisories \
  --database /tmp/transport-db --audit /path/to/cargo-audit --deny /path/to/cargo-deny
python3 -I -m unittest discover -s etc/transport-patches -p 'test_*.py' -v
```

The scripts preserve full command receipts, metadata, feature trees, scanner
reports and failed attempts in the output directories. CI uploads these as
immutable run artifacts for 30 days. Keep a tracked summary and hashes beside
this record; archive full artifacts before expiry if they are needed for a
release audit. The [evidence directory](evidence) contains the initial source,
advisory and baseline crypto observations. Final CI results must identify the
tested commit and immutable run URL. This qualification does not claim
deployment, throughput or packet-flood benchmarking; those remain separate
release checks and issue #1432 coverage.
