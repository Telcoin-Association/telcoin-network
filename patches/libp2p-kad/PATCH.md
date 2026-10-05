# Carried patch: libp2p-kad

This is the authenticated published `libp2p-kad` 0.49.0 package. The root
`[patch.crates-io]` selects this copy for the existing libp2p stack. The crate is
excluded from the workspace to retain upstream lints and style. The existing
`connection-limits-patch` CI job additionally runs its isolated library tests;
the original connection-limits step, conditions and aggregate gates remain required.

## Provenance

- Published package: `libp2p-kad` 0.49.0.
- Original root `Cargo.lock` archive checksum:
  `973caa45045e53f3cf1cf3d596888b82602b30640fd75485836c81aa66e7c38a`.
- Published VCS revision: `7171dce2f90c05ba7892d4ba926abb1881db27c7`, path
  `protocols/kad`, from `.cargo_vcs_info.json`.
- Original `src/behaviour.rs` SHA256:
  `bc29c4d2714b3c0c6a2079bcbb1fb183d8418ef76d88541662d8654dc3765d16`.
- All 29 published members were copied byte for byte before applying the delta.
  Source copyright notices, licensing metadata, integration tests and every runtime
   and dev dependency are retained. The published `Cargo.lock` is retained byte for
   byte and controls the excluded crate's isolated unit tests.

The root lockfile selects the path package without changing the Kad version or
runtime dependency versions. Root workspace integration tests use that root lock.
The additional unit-test command selects the excluded crate with `--manifest-path`,
using its authenticated upstream lock and unchanged dev dependency graph. Mutation
controls for the carried crate use the same standalone manifest selection. Neither
lane drops dev dependencies or upstream tests, and both require `--locked`.

## Runtime behavior

`record_received` still computes the same density-attenuated expiry. With
`StoreInserts::FilterBoth`, it emits the complete attenuated record for application
authentication even after the DHT lifetime has expired. It does not insert the
record automatically. `Unfiltered` still discards expired records, and the existing
protocol acknowledgment, local-publisher guard, provider handling, TTL and replication
settings are unchanged.

The Telcoin consumer separately accepts an expired identity proof only after its
existing ban, rate, key, domain, signature and publisher checks. The source must be
physically connected, have manager status `Connected`, and own the signed advertised
identity. Admission is limited to a still-anonymous public source and a BLS key absent
from confirmed bindings, known records, pinned configuration and committee slots.
Expired proofs cannot rotate existing identities or refresh configured/cache metadata,
and do not gain DHT storage or connected-record retention. The dedicated promotion uses
the verified transport key with no advertised addresses, preserving existing observed
address and connection state without importing expired RPC data. Nonexpired handling is unchanged.

## Test-only delta and regressions

`src/handler.rs` adds a `cfg(test)` request-ID fixture constructor, since upstream's
request identifier has a private handler-local field. The private behavior regression
calls the real `record_received` method without sockets or sleeps. It requires filtered
expired delivery with the attenuated expiry and original acknowledgment, no automatic storage,
and expired unfiltered rejection. Restoring the original expiry gate must fail the
filtered-delivery regression. Network consumer regressions exercise real physical
connections and reject expired relayed, invalid, banned, rate-shed and closing sources.

Remote commands, from the repository root with its pinned toolchain and lockfile:

```sh
cargo +1.94 test --locked --manifest-path patches/libp2p-kad/Cargo.toml --lib
cargo +1.94 nextest run --locked -p tn-network-libp2p -E 'test(expired_kad_)' --no-tests fail
```

## Maintenance and retirement

Telcoin network maintainers own this carried patch with the libp2p dependency stack;
its qualification evidence is tracked in
[telcoin-network #1476](https://github.com/Telcoin-Association/telcoin-network/issues/1476).
No upstream submission or published resolution is claimed here. Record the upstream
issue or PR when submitted, and keep archive, VCS, source and regression provenance
current on upgrades.

Retire the override only after a compatible published Kad release delivers expiry-
attenuated filtered records independently of DHT storage expiry while preserving
unfiltered expiry and protocol behavior. Run these regressions against that release,
then remove the override, workspace exclusion, carried source and isolated CI step
together. Preserve the consumer's authenticated live-self and no-storage regressions.
