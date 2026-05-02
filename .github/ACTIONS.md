# How TN CI Works

## On-chain commit attestation (`maintainer-verify.yaml`)

GitHub actions takes over 45 minutes to build and test the workspace (before fmt or clippy lint checks).

Instead of relying on cloud infrastructure, the core team is responsible for submitting an attestation transaction to the git commit hash attestation contract (currently deployed at `0xde9700e89e0999854e5bfd7357a803d8fc476bb0`).

The worker takes the HEAD commit hash and verifies an attestation was pushed on-chain.

### Environment
Attesting devs must have "MAINTAINER" role to update contract state.

The local `test-and-attest.sh` script requires Foundry's cast.

See https://book.getfoundry.sh/getting-started/installation for installation instructions.

Add `GITHUB_ATTESTATION_PRIVATE_KEY` to a `.env` file in the project. This is the private key (without "0x" prefix) associated with the "MAINTAINER" role address.

## Public release pipeline (`release.yaml`)

Triggered on tag push (`v*.*.*` for mainnet, `v*.*.*-adiri` for testnet).
Independent of the on-chain attestation flow above — the two serve different
audiences.

The workflow:

1. `meta` — derives channel + image tag list from the git tag.
2. `build-binary` — matrix on x86_64 + aarch64 linux runners, runs
   `cargo build --release`, strips, tars, sha256sums, and emits SLSA build
   provenance via `actions/attest-build-provenance@v2`.
3. `build-image` — `docker buildx` multi-arch (linux/amd64 + linux/arm64) push
   to `ghcr.io/telcoin-association/telcoin-network`, with provenance + SBOM
   pushed alongside.
4. `draft-release` — aggregates `SHA256SUMS`, creates a **draft** GitHub
   Release containing the tarballs, the checksums file, and the image digest
   in the release body.

The draft sits unpublished until two maintainers run `make release-sign` from
their laptops with their respective YubiKeys. See
[`docs/RELEASING.md`](../docs/RELEASING.md) for the full runbook and
[`docs/INSTALL.md`](../docs/INSTALL.md) for the operator-side verification
commands.

The maintainer countersignature certs live in [`release-keys/`](release-keys/).
They are pinned at the tag — operators clone the repo at `$TAG` and use those
certs as the trust anchor.

## Why both?

The on-chain attestation is governance evidence ("the team approved this
commit"). The release pipeline produces verifiable artifacts ("anyone can
prove the binary they downloaded came from this commit, signed by the team").
They overlap zero. Don't conflate them.
