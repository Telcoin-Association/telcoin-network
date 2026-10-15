# Quinn protocol observation maintenance record

Issue [#1680](https://github.com/Telcoin-Association/telcoin-network/issues/1680) needs decisions
that Quinn handles before the application hook. The optional atomic observer records those
decisions, actual incoming occupancy and effective live server ceilings. It shares the existing
libp2p observation and recorder. The saturation observation is at the existing cheap-first gate,
before Initial-key derivation. Label vocabularies are fixed and independent of remote traffic.

## Source identity

| Field | Recorded value |
| --- | --- |
| Package | Published `quinn-proto` 0.11.18 |
| Upstream | https://github.com/quinn-rs/quinn, `quinn-proto` |
| Release identity | Immutable crates.io 0.11.18 archive and its VCS base; no mutable tag is used |
| Upstream base | `eaec0db4bcb698f76df736d89938743c88a2ad5f` |
| Archive SHA-256 | `a9746dbde176634f4f2f1faf2404e30a31b2bc1e9cafb5329c95d8177a18c9fc` |
| Maintained mirror | [Tracked source tree](../../../patches/quinn-proto) |
| Exact observation revision | [`372fa302bbb85758c9341b2c6b2247db3e9601ca`](https://github.com/Telcoin-Association/telcoin-network/commit/372fa302bbb85758c9341b2c6b2247db3e9601ca) |
| Cargo source | Root `[patch.crates-io]`, `quinn-proto = { path = "patches/quinn-proto" }`, excluded from workspace membership |
| Complete diff | [upstream.diff](../../../patches/quinn-proto/upstream.diff), SHA-256 `219d1f7a04a2e2433815ba393110c16e40afa2016a7d02d27af68ec0d268a369` |
| Source tree fingerprint | `f171721216180561d13a478d675b85504190ac29cde9031bc38c8c444b3274e2` |
| Resolution evidence | [source.json](evidence/source.json), default and Adiri Linux graphs, package IDs, dependency edges, enabled features and per-file source hashes |

The normalized registry manifest, dependencies, features, crypto providers and both licenses
are preserved. The production lockfile selects the path package at the same version. The
standalone test lockfile is separate. Source fingerprints exclude maintenance prose, the
generated diff and the standalone lockfile. Full-index diff headers make reproduction
independent of Git object abbreviation settings.

`etc/transport-patches/sources.py` verifies both archive checksums and reproduces both source
diffs. It permits only the recorded libp2p-quic and quinn-proto paths and versions. Source
identity normalization retains dependency edges, versions and feature checks. Independent
release peers retain registry packages; the carried peer uses both recorded overrides.

## Ownership and upstream replacement

This companion uses the active [transport maintenance process](../libp2p-quic/README.md).
That record identifies @MavenRain's explicit acceptance on 2026-09-30 of maintenance and
advisory responsibility, including weekly review and review at every transport dependency
update, and identifies @MavenRain as the approving core maintainer for the process. Release
approval remains subject to the repository's PR and attestation gates.

Evaluate the next quinn-proto release after 0.11.18, or an earlier revision exposing equivalent
incoming observations. The 2026-10-15 submission plan is to prepare an observation-only upstream
proposal from this reviewed delta after Telcoin review and record the upstream link here.
Remove the companion when the upstream API can provide these observations with the same stages,
units, bounded vocabulary and lifecycle guarantees.

On an update, compare the complete source delta, token fallback behavior, both incoming-slab
removal sites, saturation ordering, all accept-error returns, server-config replacement and
endpoint destruction. Refresh source identities, feature evidence, archive/diff fingerprints,
advisory applicability and compatibility evidence together.

## Advisory and qualification coverage

The companion adds observation statements to the pinned protocol decisions. The existing
registry/path scanner controls and transitive advisory-closure checks cover this source in
the carried transport workflow. Path-source scanner limitations described in the active
transport record still require manual upstream advisory review; a missing scanner finding
does not establish that a path package is unaffected.

Local pinned-toolchain verification on 2026-10-15:

- Protocol crate: all 308 library tests pass, including real token returns, invalid and opaque
  token outcomes, refusal, ignore, accept timeout, queue release, effective ceilings, shutdown
  and saturation across 1,000 source addresses.
- Candidate: ten supported localhost tests pass. The existing `127.0.0.2` wrong-IP fixture is
  run by Linux CI because this macOS host cannot bind that address. Candidate Clippy and
  repository-pinned formatting checks pass.
- Default and Adiri Linux source graphs and both archive/diff reproductions pass.
- Eight maintenance tests pass. [Protocol](evidence/protocol-mutations.json),
  [candidate](evidence/candidate-mutations.json) and [source-control](evidence/source-mutations.json)
  receipts record nine observed regression failures when their corresponding observations or
  version checks are mutated.
- The [native-count conversion receipt](evidence/gauge-value.json) records a direct pinned-Rust
  test of values above u32 and at the native maximum, plus its failing truncation mutation.
  The metrics CI job also runs this regression in the complete crate.

The staging workflow runs the full Linux candidate suite, primary/multiple-worker label and
shutdown tests, counter restart tests, their mutation witnesses, workspace linting, carried
transport qualification and the independent release interoperability matrix. The full command
output and mutation failures are retained as CI artifacts. Release approval requires those
gates and the repository's normal pinned attestation.

The [metrics contract](../../quic-incoming-metrics.md) records stage, unit, scope, reset,
sampling, fixed labels, unavailable packet-loss observations and operator queries.
