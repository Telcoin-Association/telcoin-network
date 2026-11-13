# Carried patch: quinn-proto incoming observations

This is the published quinn-proto 0.11.19 source, including both upstream licenses.
Its registry archive SHA-256 is
`0e750cca55fe4f0439a15d0bb529da9651e79993e8e72c61a899a36d462befbe`.
The upstream revision is `8192ed399a26a6e8f0aba43d10ec8badaa5b905e`.
It moved from 0.11.18 to 0.11.19 for RUSTSEC-2026-0523 and RUSTSEC-2026-0524; Telcoin's diff
is unchanged apart from line offsets (see the maintenance record).
The normalized registry Cargo.toml, dependencies, features and crypto providers are preserved.
The standalone test lockfile is independent of the production workspace lockfile.

The additive `IncomingStats` observer is optional in `EndpointConfig`. It measures actual slab
insertion/removal, effective live server ceilings, validated Retry and NEW_TOKEN Initials,
token failures, explicit API dispositions, failed accepts and cap drops. It has a fixed enum
vocabulary, no recorder dependency and no remote identity storage. Observations at the existing
accept-error returns preserve protocol decisions and packet generation.

The protocol harness exercises actual decision sites without sockets or wall-clock waits.
Upstream source files unrelated to the observation are unchanged. `rustfmt.toml` uses upstream's
`style_edition = "2024"`. Maintenance artifacts and the standalone lockfile are excluded from
the reproduced source diff. `etc/transport-patches/sources.py` verifies the registry checksum,
reproduces `upstream.diff`, and rejects unrecorded path or version overrides. Carried release
peers use this source while independent stock peers retain the registry dependency.

Track removal or upstream replacement through issue #1680. On an upgrade, recheck token
fallback semantics, both incoming-slab removal sites, saturation ordering, server-config
replacement, endpoint destruction and the deterministic regression tests. Regenerate the diff
against the checksum-identified registry source before updating the maintenance record.

See [the observations contract](../../docs/quic-incoming-metrics.md) for units, stages, resets,
scope, availability gaps and operational queries.
