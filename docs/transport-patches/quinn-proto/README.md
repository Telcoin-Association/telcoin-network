# Quinn protocol observation source

Production selects the checksum-identified quinn-proto 0.11.18 source under
[`patches/quinn-proto`](../../../patches/quinn-proto/PATCH.md). The additional observer exposes
pre-application decisions that the pinned public API cannot report. It changes no protocol,
cryptographic or queue policy. It is shared with the existing libp2p QUIC observations.

Source verification permits only this exact path and version, reproduces the upstream diff,
and retains dependency-edge, feature and advisory checks. The carried peer recipe applies
both recorded overrides; independent peers keep their registry sources. Observation tests
and Retry interoperability tests run together, including on staging PRs.

The [metrics contract](../../quic-incoming-metrics.md) records stage, unit, scope, reset,
sampling, fixed labels, unavailable packet-loss observations and operator queries.
