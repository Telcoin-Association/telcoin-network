# Maintainers

These pages are for maintainers who cut releases or hold a release signing key.

- [Releasing](releasing.md) walks through the seven steps from the release PR to a published and verified release, and what to do when a step fails.
- [YubiKey signing setup](yubikey-setup.md) gets your OpenPGP key, generated on a YubiKey that it never leaves, onto the allowlist in `tn-release-signing` and into this repository's releases, and sets up registry access on the build server.

Operators install a release with [Installing a release](../getting-started/installing-a-release.md) and read what changed in [Release notes](../getting-started/release-notes.md).

## What a release contains

Every release ships the node: its tarball, its image in the network's package, `tn-mainnet` for mainnet tags and `tn-testnet` for Adiri tags, `IMAGE_DIGEST`, which names that image by digest, `SHA256SUMS` and `SHA256SUMS.asc`.
A release also ships the edge services, `worker-gateway` and `node-record-api`, when `etc/release-components` at the tagged commit names them; a tag without that file ships the node alone.
Each edge service ships a tarball, an `IMAGE_DIGEST-<service>` file and an image in a package of its own, `ghcr.io/telcoin-association/tn-worker-gateway` or `ghcr.io/telcoin-association/tn-node-record-api`, which mainnet and Adiri tags share; publishing moves its `:latest` to the newest mainnet final and its `:adiri` to the newest Adiri final.
The node's image is built from `etc/Dockerfile`, the edge services' from `bin/worker-gateway/Dockerfile` and `bin/node-record-api/Dockerfile`.
`SHA256SUMS` lists only the node's files and the chain-config files, the shape the deployment scripts in `tn-node-deployment` accept.
The edge files are listed in a second manifest, `SHA256SUMS-edge`, which the same maintainer keys sign in `SHA256SUMS-edge.asc` under the same threshold.
The deployment scripts install and verify the node from a release; they set up the worker gateway from an image digest, on testnet and mainnet only the one that their own signed script release pins, read no edge-service file of a release, and do not install `node-record-api`, so [Installing a release](../getting-started/installing-a-release.md#edge-services-worker-gateway-and-node-record-api) has the operator check for the edge services.

`make docker-worker-gateway` and `make docker-node-record-api` push unsigned ad-hoc images to `ghcr.io/telcoin-association/tn-dev` as `dev-<binary>-<TAG>`, as `make docker-adiri` and `make docker-devnet` do for the node.
`tn-dev` holds no release, and there is no devnet release channel.

## How a release is trusted

The keys that can sign a release are in [`tn-release-signing`](https://github.com/Telcoin-Association/tn-release-signing), the repository of maintainer keys that every signed Telcoin project shares, one file per maintainer in `keys/`, named after their GitHub handle.
This repository carries it as the `release` submodule, and only the keys at the commit `main` records for `release` count.
Every key in that shared allowlist can sign a release of this repository.
A key on that allowlist signs the git tag, and a key on it, usually the same one, signs `SHA256SUMS.asc`, the detached signature over the checksums of the tarball and of the image digest.
One signature is required: `KEYS.md` in `tn-release-signing` states the threshold, `Signatures required: 1`, and `MIN_SIGNATURES` in `etc/release.sh` may raise it but never lower it.
In a release that ships the edge services the same key also signs `SHA256SUMS-edge.asc`, the detached signature over `SHA256SUMS-edge`, which needs as many signatures as `SHA256SUMS.asc`.
CI, the release scripts and the operator check in [Installing a release](../getting-started/installing-a-release.md) read the allowlist at that commit, never from the tagged tree or from the submodule's checked-out files, so a tag cannot bring its own key.
A key reaches this repository's releases only through a pull request that moves the `release` pointer, which needs the review of a maintainer who holds a release key; [Moving the submodule pointer](releasing.md#moving-the-submodule-pointer) has the steps.
The node deployment scripts carry their own copies of the keys in each signed script release, so a release installs on an operator's host only when the script release that host runs carries the key of one of its signers, and a mainnet host builds a tag from source only when it carries the key that signed the tag.

Signing happens on a maintainer's macOS laptop, with a key that was generated on a YubiKey and never leaves it.
Building happens on the build server, an Ubuntu or Debian Linux x86_64 host that holds the registry credential and never sees the YubiKey.
A release that passes `make release-verify` needs both machines: the build server builds and pushes the images, and the YubiKey signs the checksums.

The signature shows that a maintainer approved the files.
It does not show that they were built from the tagged source: release builds are not reproducible, and there is no SBOM or third-party build provenance.
The moving image tags `:latest` and `:adiri` are not signed, and whoever holds the registry credential can move them, which is why operators pin the digest from `IMAGE_DIGEST` or `IMAGE_DIGEST-<service>`.

## Where things live

| Path | What it holds |
| --- | --- |
| [`release/`](https://github.com/Telcoin-Association/tn-release-signing) | The `tn-release-signing` submodule: the allowlist in `keys/`, one `<handle>.asc` public key per maintainer, `yubikey-setup.sh`, which `make yubikey-setup` runs, and the shared maintainer guides in `docs/` |
| [`release/KEYS.md`](https://github.com/Telcoin-Association/tn-release-signing/blob/main/KEYS.md) | The maintainer key table that operators check fingerprints against, the signature threshold, and the rules for changing them |
| [`.gitmodules`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.gitmodules) and [`.github/CODEOWNERS`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/CODEOWNERS) | Where the `release` submodule is fetched from, and the rule that puts every change to it or to the `release` pointer in front of a maintainer who holds a release key |
| [`SECURITY.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/SECURITY.md#verifying-releases) | How releases are signed and verified, for reporters and operators |
| [`CHANGELOG.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/CHANGELOG.md) and [`cliff.toml`](https://github.com/Telcoin-Association/telcoin-network/blob/main/cliff.toml) | The release notes git-cliff generates from commit messages, and its configuration |
| [`etc/release.sh`](https://github.com/Telcoin-Association/telcoin-network/blob/main/etc/release.sh) | Every release step; the `release-*` Make targets call it |
| `etc/release-components` | The components a tag ships, one name per line: `node`, which every release ships, and `worker-gateway` and `node-record-api` when the release ships them; a tag without the file ships the node alone |
| [`etc/Dockerfile`](https://github.com/Telcoin-Association/telcoin-network/blob/main/etc/Dockerfile), [`bin/worker-gateway/Dockerfile`](https://github.com/Telcoin-Association/telcoin-network/blob/main/bin/worker-gateway/Dockerfile) and [`bin/node-record-api/Dockerfile`](https://github.com/Telcoin-Association/telcoin-network/blob/main/bin/node-record-api/Dockerfile) | The release images of the node and of the two edge services, which `make release-build` builds from the tagged commit |
| [`Makefile`](https://github.com/Telcoin-Association/telcoin-network/blob/main/Makefile) | `release-prep`, `release-tag`, `release-build`, `release-sign`, `release-verify`, `release-publish`, `docker-login` and `yubikey-setup` |
| [`.github/workflows/release.yaml`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/workflows/release.yaml) | CI that validates a pushed tag, creates the draft release, and verifies a published release |
| [`.github/ACTIONS.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/ACTIONS.md#release-workflow) | How CI works, including the release workflow and the attestation that the merge queue relies on |
