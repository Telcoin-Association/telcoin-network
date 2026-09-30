# Carried transport patches

Use this process when shipping local changes to `libp2p-quic`, `libp2p-tls`,
`quinn`, or their transport and cryptographic dependencies. It extends the normal
[contribution review](../../CONTRIBUTING.md) and
[security reporting process](../../SECURITY.md). It does not replace their checks
or create a separate vulnerability disclosure channel.

The first real patch must include a completed maintenance record before it ships.
Upstream submission or acceptance does not waive this requirement. Preparation
and independent node fixes can proceed before that patch is available.

## Patch records

Copy [TEMPLATE.md](TEMPLATE.md) to a directory named for the patch, as `README.md`,
and keep its evidence beside it. Link the record here in the same PR that changes
the dependency. A template with pending fields is not release approval.

There are no carried transport patches yet. The
[libp2p-tls upstream candidate](libp2p-tls-6634/README.md) records a concrete patch
and the checks still required before adopting it in a Telcoin release.

## Source and resolution

A reviewer must be able to identify the shipped code from tracked files:

- Record the upstream repository, release tag, full base commit, maintained mirror,
  exact 40-character patch revision, and the complete diff from that base. Include
  every changed transport crate, even when the node reaches it through `libp2p`.
- For a git override, commit the workspace manifest override with a full `rev`
  and the resulting lockfile. A branch or tag alone is insufficient. Check that
  Cargo actually selects the patched package; an unused patch is not evidence.
- For a path or vendored override, commit the source tree, provenance and diff,
  plus the manifests and any source-replacement configuration used to build it.
  Record its upstream and mirror revisions as well as the tracked source path.
  A developer's untracked local directory cannot supply a release dependency.
- For a registry source, record the registry, package name, version and lockfile
  checksum, and connect the published source to the upstream base and patch diff.

Use Cargo's [workspace patch rules](https://doc.rust-lang.org/cargo/reference/overriding-dependencies.html#the-patch-section).
Capture the resolved transport package IDs, source identities, dependencies and
enabled features, not just the declared versions. `cargo metadata --locked
--format-version 1` and `cargo tree --locked -e features -i <package>` can supply
this evidence. Record the full commands, target and release feature selection;
use the repository's pinned toolchain. Resolve each supported release configuration
separately, and keep a tracked summary and hashes of the complete outputs.

## Ownership and updates

Name a maintenance owner and an advisory owner by GitHub handle, and record their
acceptance and the approving core maintainer. A shared owner is allowed if both
responsibilities are explicit. Do not infer acceptance from an upstream author.

The maintenance owner records the next upstream release or revision to evaluate,
the upstream submission link or a dated submission plan, and removal criteria.
On the next dependency update, that owner must compare the patch against the new
upstream source, check every overridden crate and its version constraints, refresh
the diff and resolution evidence, and rerun the advisory and compatibility checks.
An update must not silently leave an unused override or an obsolete patch behind.

The advisory owner records which upstream advisories they monitor, the review
cadence and the escalation route through the existing security process. They
reassess applicability whenever an advisory arrives or the source, version or
feature selection changes. Record gaps and their compensating review explicitly.

Remove the override when a released dependency contains the required fix and
passes the same checks. Commit the manifest and lockfile changes together, verify
that resolution returns to the intended source, and archive the record with the
replacement version and evidence. An accepted upstream PR alone is not removal.

## Advisory coverage for the actual source

Reuse the dependency scanners and advisory process selected by the maintainers.
Record each tool's version, configuration, advisory database revision, command,
exit status and report. A clean scan alone does not prove that a non-registry
dependency was examined.

Before shipping, demonstrate detection or explicit matching of a published,
relevant advisory for the actual git, path, vendored or registry source form:

1. Identify the advisory, affected upstream package and vulnerable base. Capture
   the package identity Cargo resolves in the check. A small isolated fixture may
   use a known vulnerable revision with the same source form and package identity;
   keep it separate from the release lockfile.
2. Record the expected and observed result from every selected scanner. Include
   a known affected control so an empty report cannot count as proof of coverage.
   Document source, version or package-name mismatches that prevent detection.
3. Match the advisory to the carried source and patch diff. A retained upstream
   version can still trigger an alert after a backport; record the applicability
   decision and evidence instead of treating either an alert or its absence as
   the final decision. Any exception must have an owner and a review deadline.
4. Check Dependabot separately: dependency-graph recognition, advisory alerts,
   security updates and version updates are distinct results. Record the actual
   repository settings/configuration and dated evidence from the same source
   form. [Cargo ecosystem support](https://docs.github.com/en/code-security/reference/supply-chain-security/supported-ecosystems-and-repositories)
   by itself is not evidence for a particular override. Mark unavailable or
   unsupported checks as gaps, and document a demonstrated manual advisory match
   and an owned monitoring/update procedure for them.

Keep a concise, reproducible evidence summary in the record, with immutable links
or hashes for larger reports. Never commit TLS secrets or private advisory details;
use the existing security reporting process for confidential material. A large
synthetic no-op patch exercise is not required for unrelated transport work.

## Compatibility and cryptography

Transport peers are untrusted. A carried optimization must preserve peer identity
authentication and certificate rejection behavior unless an explicit protocol
change is separately reviewed. Capture the resolved crypto provider and features
before and after the patch, including the relevant `libp2p-tls`, `quinn`,
`quinn-proto` and `rustls` packages. Explain any difference; do not silently change
accepted groups, algorithms or defaults as part of carrying a patch.

Record successful tests against an unmodified upstream peer and every supported
release peer required for rollout. Identify both binaries' revisions, toolchains,
targets, features and commands. Exercise patched dialer to stock listener and
stock dialer to patched listener, over the supported QUIC listener/address forms,
with authenticated handshakes, application traffic and reconnects. Include the
rejection/regression cases affected by the patch. Link the applicable
[interoperability coverage](https://github.com/Telcoin-Association/telcoin-network/issues/1432)
and preserve results; a link to planned coverage is not a passing run.

Record the production TLS key-logging decision, the effective configuration and
the check that proves it. The production decision must account for an inherited
`SSLKEYLOGFILE` as well as explicit transport configuration. Diagnostic opt-in
needs a separate, bounded procedure that keeps key material out of logs and
artifacts. Documentation alone does not change the runtime behavior.

If a later change also patches TLS or quinn, extend the record to cover those
sources, owners, advisory checks, crypto features and compatibility results before
shipping it. Run the normal repository PR and release gates in addition to this
patch-specific evidence.
