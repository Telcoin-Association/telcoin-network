# Maintainers

These pages are for maintainers who cut releases or hold a release signing key.

- [Releasing](releasing.md) walks through the seven steps from the release PR to a published and verified release, and what to do when a step fails.
- [YubiKey signing setup](yubikey-setup.md) creates an OpenPGP key with its signing subkey on a YubiKey, adds it to the allowlist, and sets up registry access on the build host.

Operators install a release with [Installing a release](../getting-started/installing-a-release.md) and read what changed in [Release notes](../getting-started/release-notes.md).

## How a release is trusted

Only keys in `.github/maintainer-gpg-keys/` on `main` can sign a release, one file per maintainer, named after their GitHub handle.
The same key signs the git tag and `SHA256SUMS.asc`, the detached signature over the checksums of the tarball and of the image digest.
One signature is required (`MIN_SIGNATURES` in `etc/release.sh`).
CI, the release scripts and the operator check in [Installing a release](../getting-started/installing-a-release.md) read the allowlist from `main`, never from the tagged tree, so a tag cannot bring its own key.

Signing happens on a maintainer's macOS laptop, where the signing subkey lives on a YubiKey.
Building happens on xerxes, a Linux x86_64 host that holds the registry credential and never sees the YubiKey.
A release that passes `make release-verify` needs both machines: xerxes builds and pushes the image, and the YubiKey signs the checksums.

The signature shows that a maintainer approved the files.
It does not show that they were built from the tagged source: release builds are not reproducible, and there is no SBOM or third-party build provenance.
The moving image tags `:latest` and `:adiri` are not signed, and whoever holds the registry credential can move them, which is why operators pin the digest from `IMAGE_DIGEST`.

## Where things live

| Path | What it holds |
| --- | --- |
| [`.github/maintainer-gpg-keys/`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/maintainer-gpg-keys/README.md) | The allowlist: one `<handle>.asc` public key per maintainer |
| [`SECURITY.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/SECURITY.md#maintainer-release-keys) | The maintainer release keys table that operators check fingerprints against |
| [`CHANGELOG.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/CHANGELOG.md) and [`cliff.toml`](https://github.com/Telcoin-Association/telcoin-network/blob/main/cliff.toml) | The release notes git-cliff generates from commit messages, and its configuration |
| [`etc/release.sh`](https://github.com/Telcoin-Association/telcoin-network/blob/main/etc/release.sh) | Every release step; the `release-*` Make targets call it |
| [`Makefile`](https://github.com/Telcoin-Association/telcoin-network/blob/main/Makefile) | `release-prep`, `release-tag`, `release-build`, `release-sign`, `release-verify`, `release-publish` and `docker-login` |
| [`.github/workflows/release.yaml`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/workflows/release.yaml) | CI that validates a pushed tag, creates the draft release, and verifies a published release |
| [`.github/ACTIONS.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/ACTIONS.md#release-workflow) | How CI works, including the release workflow and the attestation that the merge queue relies on |
