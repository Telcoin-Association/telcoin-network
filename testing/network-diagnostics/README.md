# Network diagnostic regression fixture

This small standalone crate compiles the production limiter, tracing wrappers, and their Rust
tests directly from `crates/network-libp2p/src`. It keeps time under test control and needs only
the workspace's pinned tracing version. It has no copy of the implementation.

From the repository root:

```sh
testledger -- cargo +1.94 test --locked --manifest-path testing/network-diagnostics/Cargo.toml -j 2
```

The full `tn-network-libp2p` test suite additionally checks listener/gossip counters, malformed
record handlers, subsequent command progress, and immediate terminal listener loss. This fixture
provides fast limiter regression and mutation checks; workspace CI covers the integration path.
