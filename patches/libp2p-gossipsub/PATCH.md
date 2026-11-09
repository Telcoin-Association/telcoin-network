# Carried patch: libp2p-gossipsub

This is the production source of `libp2p-gossipsub` 0.50.0 with a promise-expiry fix, a per-peer promise filter and an outbound upgrade that reports a lazily negotiated protocol rejection as unsupported.
The root `[patch.crates-io]` selects this copy for the existing libp2p 0.57.0 stack.
The crate is excluded from the workspace, like the carried libp2p-quic and
libp2p-connection-limits patches, to preserve upstream lints and style. The
`gossipsub-patch` CI job explicitly runs its library tests and is required by
`CI Success`, including for maintainer PRs.

## Provenance

- Published crate: `libp2p-gossipsub` 0.50.0.
- crates.io archive checksum from the original root `Cargo.lock`:
  `1b625461b45e5b36facf07ff3585b831bcaf26cab8907ac8f47d1e8e999999e0`.
- Published VCS revision: `7171dce2f90c05ba7892d4ba926abb1881db27c7`, path
  `protocols/gossipsub`, from the archive's `.cargo_vcs_info.json`.
- Original `src/behaviour.rs` SHA-256: `17e54f69957af01b02f19036ec5f5b02cea36d319b782042df6b8e8c539df462`.
- Original `src/gossip_promises.rs` SHA-256: `7b6132b9a61ccee78654b09a405c95898f4d3fe551d102ec7f9f877a96cb6990`.
- Original `src/behaviour/tests/gossip.rs` SHA-256: `2c5685d3513ed174e83b809432bfa3ea91c426173aac0707a014406d49a454ed`.
- Original `src/protocol.rs` SHA-256: `3d3796536850af98cba80710fb26f66224eee6f4d1bfc7d05a84215ebe28074c`.
- Original `src/handler.rs` SHA-256: `571f103dd76d59a81bcefea3f85b30159d73c6a793d30b49d941fa031111bb8b`.
- The carried source is the archive's complete `src/` tree with five changes: the heartbeat expiry and the per-peer IHAVE filter in `src/behaviour.rs`, the per-peer promise lookup in `src/gossip_promises.rs`, one regression test appended to `src/behaviour/tests/gossip.rs`, the mapping of a lazily negotiated protocol rejection to `ProtocolUnsupported` in `src/handler.rs`, and in `src/protocol.rs` a zero-length read in the outbound upgrade plus the removal of the `mod tests` block at the end of the file.
  The removed `mod tests` block's five codec tests need the rust-libp2p workspace-only `quickcheck` dev-dependency, which the published manifest does not carry.
  Upstream's `tests/smoke.rs` needs the workspace-only `libp2p-swarm-test` crate and is not carried either.
  Every carried file keeps its MIT copyright and permission notice.
- Runtime dependencies, versions and features match the published manifest. The root
  lockfile changes only this crate's source and checksum, selecting the path package
  without moving any dependency version. There is no separate patch lockfile.
- The published `[dev-dependencies]` table (`tokio`, `tracing-subscriber` and a second
  `libp2p-core` entry) served only the uncarried `tests/smoke.rs` and is dropped. Cargo
  refuses to test a non-member package that declares dev-dependencies, and it never
  resolves a non-member's dev-dependencies, so the root lockfile is unchanged by the drop
  and the library tests run with `--locked` against it.

## Behavior and ownership

`Behaviour::apply_iwant_penalties` runs on every heartbeat. Upstream calls
`gossip_promises.get_broken_promises()`, which removes the promises whose
`iwant_followup_time` deadline has passed, only inside the `PeerScoreState::Active`
branch. The node constructs its gossipsub behaviours without `with_peer_score`, so
upstream never removes a promise for a message that does not arrive: one entry per
advertised id, for as long as the node runs, surviving the advertiser's disconnect.

The carried copy drains the broken promises first and keeps only the per-peer penalty
and its metric behind the scoring check. With scoring disabled the promise map is now
bounded by the number of ids the node asks for per peer per heartbeat times the
`iwant_followup_time` window, and a later IHAVE for an expired id is answered with an
IWANT again. With scoring enabled the behaviour is unchanged.

Upstream's `handle_ihave` also skips every advertised id that has a pending promise from
any peer, so one peer's promise silences the IWANT to every other advertiser of that id
until it is answered or expires, and with scoring disabled nothing discourages that peer
from renewing it with another IHAVE. A peer that advertises predictable ids it never
delivers could hold the IWANT recovery of those ids for as long as it keeps advertising.
The carried copy keys the filter on the (id, peer) pair: `GossipPromises::contains_peer`
replaces `contains`, and an advertised id is skipped only when the same peer already
holds a pending promise for it. Another advertiser is asked at once and records its own
promise, so the extra requests are bounded by the per-peer IHAVE and IWANT caps, by
IDONTWANT, and by the follow-up window. With scoring enabled a broken promise still
costs the promiser its penalty.

The node's swarm negotiates outbound substreams with multistream-select `V1Lazy`, which applies only to the last protocol a dialer proposes.
Gossipsub proposes `/1.1.0` and then `/1.0.0`, so an honest peer still confirms `/1.1.0` under strict negotiation and nothing changes for it.
A peer that speaks neither id rejects `/1.1.0`, and the `/1.0.0` fallback is then proposed lazily and handed back as fully negotiated before the listener answers.
Upstream's outbound upgrade cannot fail, so its handler never sees `StreamUpgradeError::NegotiationFailed` for that peer, never disables itself as `ProtocolUnsupported`, and the behaviour never reports `GossipsubNotSupported`, the event the node answers with a fatal penalty.
The carried copy's outbound upgrade performs a zero-length read before framing the stream, which drives the negotiation to completion without consuming data, and its error type becomes `io::Error` instead of `Infallible`; the inbound upgrade is unchanged.
The listener's rejection then fails the upgrade with an `io::Error` wrapping `NegotiationError::Failed`, reported as `StreamUpgradeError::Apply`, and the handler maps it to `DisabledHandler::ProtocolUnsupported` exactly like `NegotiationFailed`; any other io error from the upgrade is logged like a negotiation io error.
An honest peer pays nothing: its stream is already complete after the strict `/1.1.0` negotiation, and the empty read returns at once.
A listener that closes the stream after the fallback proposal instead of answering `na` is not mapped: under lazy negotiation that is a plain `UnexpectedEof`, which the handler logs and retries until it disables itself as `MaxSubstreamAttempts` without reporting the peer as unsupported, where strict negotiation reported `NegotiationFailed`; stock libp2p listeners always answer `na`, so only a non-conforming peer takes this path.

Telcoin's network maintainers own this carried patch with the `network-libp2p` dependency stack.
Tracking is GHSA-779j-phh4-5v67 for the promise changes and issue #1780 for the negotiation change.
Keep the provenance and regression evidence current when updating this dependency.

## Upstream resolution and retirement

As of 2026-10-31, upstream master at
[`c449090f49b5ae2338e8f0727be98e0a739ba135`](https://github.com/libp2p/rust-libp2p/blob/c449090f49b5ae2338e8f0727be98e0a739ba135/protocols/gossipsub/src/behaviour.rs)
still calls `get_broken_promises()` only inside the `PeerScoreState::Active` branch of
`apply_iwant_penalties`, so unanswered promises still never expire while peer scoring
is disabled. The published 0.50.0 keys the `handle_ihave` promise filter on the message
id alone.
No public upstream issue or PR for this expiry, or released version containing it, was
found. Upstream submission and its released version remain tracked in
GHSA-779j-phh4-5v67; this patch does not assign an unannounced release number or claim
an upstream resolution.

Retire the carried patch only after a published libp2p-gossipsub release expires
promises on every heartbeat independently of peer scoring, no longer lets one peer's
pending promise suppress the IWANT to other advertisers of the same id, and is
compatible with the resolved libp2p stack. Record its public upstream issue/PR, release and exact lockfile
revision here. Run the regression against that release, then remove the path override,
exclusion and carried source together. Preserve the promise-expiry coverage in the
network tests or upstream tests when removing the dedicated CI job.

Upstream's handler assumes strict `V1` negotiation: its outbound upgrade is infallible, so under `V1Lazy` a rejected fallback proposal is never reported as unsupported.
An upstream fix would be the same zero-length read in the outbound upgrade, or a `NegotiationFailed` surfaced through `Apply`.
Retiring the patch therefore also needs a release that reports a lazily negotiated rejection as unsupported, or the node's swarm returning to strict negotiation.
`test_peer_without_gossipsub_is_banned_under_lazy_negotiation` in `crates/network-libp2p/src/tests/network_tests.rs` covers this path; the library tests do not.

## Regression command

From the repository root, using its lockfile and pinned toolchain:

```sh
cargo +1.94 test --locked -p libp2p-gossipsub --lib
```

`test_broken_promises_expire_without_scoring` in `src/behaviour/tests/gossip.rs` builds a
behaviour with scoring disabled, records a promise from one peer's IHAVE, confirms that a
second peer's IHAVE for the same id is answered with its own IWANT while the first
promise is pending and that the same peer's repeated IHAVE asks for nothing, lets the
follow-up deadline pass, and checks that the next heartbeat drops both promises and that
the id advertised again is answered with an IWANT. Restoring the upstream scoring branch
around `get_broken_promises` must fail the expiry assertions, restoring the id-keyed
filter in `handle_ihave` must fail the second-advertiser assertion, while the upstream
`test_iwant_penalties` with scoring enabled must keep passing.
