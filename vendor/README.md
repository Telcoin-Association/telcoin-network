# vendor

Third-party code that Telcoin Network carries as source and changes freely, unlike `patches/`,
which holds small, temporary patches to crates.io crates.

- `reth/`: TN's copy of [reth](https://github.com/paradigmxyz/reth), the execution layer's base.
  It is a nested cargo workspace with its own tooling for upstream syncs. Start with
  `reth/README.md`; agents and reviewers start with `reth/CLAUDE.md`.
