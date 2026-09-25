# Candidate: libp2p-tls transcript verification

Status: proposed, not carried or release-qualified. Checked on 2026-09-24
(America/Vancouver). This record is preparation for
[Telcoin #1431](https://github.com/Telcoin-Association/telcoin-network/issues/1431);
it does not complete that issue's acceptance criteria.

## Purpose and provenance

[Upstream PR #6634](https://github.com/libp2p/rust-libp2p/pull/6634) changes the TLS
1.3 transcript callback to avoid repeating certificate self-signature and libp2p
identity-extension checks. The callback continues to verify the transcript
signature. The intended contract preserves certificate authentication, expected
PeerId checks and connection isolation for untrusted transport peers.

| Field | Candidate value |
| --- | --- |
| Crate | `libp2p-tls` 0.7.1 |
| Upstream repository | <https://github.com/libp2p/rust-libp2p> |
| Upstream base commit | `70bf5e2f5c64cfa36405ccf9f4f1690afd8abf66` |
| Mirror | <https://github.com/MavenRain/rust-libp2p> |
| Exact patch revision | `626725357c653f189e74dfe1baf62c25bc559e17` |
| Upstream submission | [#6634](https://github.com/libp2p/rust-libp2p/pull/6634), open at the check date |
| Exact upstream diff | [Base to candidate](https://github.com/MavenRain/rust-libp2p/compare/70bf5e2f5c64cfa36405ccf9f4f1690afd8abf66...626725357c653f189e74dfe1baf62c25bc559e17) |
| Release tag and carried source form | Pending selection and verification |

The candidate's [TLS manifest](https://github.com/MavenRain/rust-libp2p/blob/626725357c653f189e74dfe1baf62c25bc559e17/transports/tls/Cargo.toml)
inherits dependencies from its [workspace manifest](https://github.com/MavenRain/rust-libp2p/blob/626725357c653f189e74dfe1baf62c25bc559e17/Cargo.toml).
For example, `libp2p-core` is a workspace path dependency. The eventual carrying
PR must demonstrate the graph Cargo resolves for its chosen override, including
package source identities; matching crate versions alone does not establish an
unchanged graph. Track the complete diff and any packaging/backport changes then.

## Current Telcoin source

At Telcoin commit `5736cc30012c5ff25913898e318a74df308f13d9`, the lockfile selects
the following crates from `registry+https://github.com/rust-lang/crates.io-index`:

| Package | Version |
| --- | --- |
| `libp2p` | 0.57.0 |
| `libp2p-quic` | 0.14.0 |
| `libp2p-tls` | 0.7.0 |
| `quinn` | 0.11.9 |
| `quinn-proto` | 0.11.18 |
| `rustls` | 0.23.37 |

This record does not change the manifest or lockfile. No candidate git/path source,
resolved release feature set or crypto-provider comparison has been qualified.

## Ownership, update and removal

Maintenance and advisory owners are unassigned. The upstream author is
`@MavenRain`; authorship is not recorded acceptance of either maintenance role.
A core maintainer must approve the named owners and their review cadence before
adoption. Confidential impact follows [SECURITY.md](../../../SECURITY.md).

The next dependency update must check the status of upstream #6634 and whether a
released `libp2p-tls` contains the change. Do not assume a version numbered 0.7.1
contains it without checking the source. If the patch is carried before a suitable
release, remove its override only after the replacement release passes the same
advisory, feature/provider and stock peer checks. Record that replacement and its
resolution evidence in the removal PR.

## Evidence still required before adoption

- [ ] Select and resolve the actual Cargo source form, pin its provenance, and
  commit its complete patch/packaging diff and resolved source/feature evidence.
- [ ] Name maintenance and advisory owners, record acceptance, and set the next
  upstream update target, advisory cadence and release approval.
- [ ] Demonstrate a relevant advisory match using that source form, with a known
  affected control and the selected scanners' versions, database and reports.
  Record Dependabot graph, alert, security-update and version-update behavior
  separately, with an owned process for any coverage gap.
- [ ] Preserve and compare Telcoin's resolved crypto provider/features. Upstream
  profiling used rustls 0.23.45; the Telcoin baseline above uses 0.23.37.
- [ ] Run independently built supported QUIC peers in both directions, including
  the 0.13.1/0.14.0 release pair specified by
  [#1432](https://github.com/Telcoin-Association/telcoin-network/issues/1432), with
  production listener configuration and the patch's authentication regressions.
- [ ] Record and verify the production TLS key-logging decision, including the
  effect of `SSLKEYLOGFILE`, and complete the normal repository release gates.

The upstream PR reports TLS tests, profiling and mixed TLS-version tests on a
common current QUIC engine. Those are upstream reports, not runs performed for
this Telcoin record, and are not the independently built QUIC release matrix
required above. No scanner, stock peer or deployment result is claimed here.
