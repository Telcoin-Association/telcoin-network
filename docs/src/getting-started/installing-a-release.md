# Installing a release

This page is for operators who run Telcoin Network on Linux x86_64.
Each release is a signed git tag plus these files on the tag's [GitHub release](https://github.com/Telcoin-Association/telcoin-network/releases):

| File | Content |
| --- | --- |
| `telcoin-network-<TAG>-x86_64-unknown-linux-gnu.tar.gz` | The `telcoin-network` binary, with `LICENSE-APACHE`, `LICENSE-MIT` and `NOTICE` |
| `IMAGE_DIGEST` | The `linux/amd64` image at `ghcr.io/telcoin-association/telcoin-network`, named by its content digest |
| `SHA256SUMS` | The SHA-256 of the tarball and of `IMAGE_DIGEST` |
| `SHA256SUMS.asc` | A maintainer's detached OpenPGP signature over `SHA256SUMS` |

There are no release builds for other platforms.
On those, build from source as the [README](https://github.com/Telcoin-Association/telcoin-network/blob/main/README.md#quick-start) describes.

## Release channels

The tag name sets the network, the build features and the image tags.

| Tag | Network | Build features | Image tags |
| --- | --- | --- | --- |
| `vX.Y.Z` | mainnet | none | `:vX.Y.Z`, `:latest` |
| `vX.Y.Z-rcN` | mainnet release candidate | none | `:vX.Y.Z-rcN` only |
| `vX.Y.Z-adiri` | Adiri testnet | `adiri` | `:vX.Y.Z-adiri`, `:adiri` |
| `vX.Y.Z-adiri-rcN` | Adiri release candidate | `adiri` | `:vX.Y.Z-adiri-rcN` only |

Running with `--chain adiri` needs a build with the `adiri` feature, so Adiri nodes use the `-adiri` releases.
Release candidates are for testing and do not belong in production.
`:latest` and `:adiri` move to the newest release in their channel when it is published, so deployments should pin the image digest instead (see [Path B](#path-b-docker-image)).
GitHub marks every Adiri release and every release candidate as a pre-release, so the repository's "Latest" release never points at one of them.
[Release notes](release-notes.md) lists the changes in each release.

## What the signature proves

`IMAGE_DIGEST` names the image by its content digest, and `SHA256SUMS` lists the SHA-256 of `IMAGE_DIGEST` and of the tarball, so one signature over `SHA256SUMS` covers both the binary and the image.
`SHA256SUMS.asc` is made with a maintainer's OpenPGP key whose signing subkey is held on a YubiKey.
The same key signs the git tag.
The keys allowed to sign are in [`.github/maintainer-gpg-keys/`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/maintainer-gpg-keys/README.md), and their fingerprints are in the maintainer release keys table in [`SECURITY.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/SECURITY.md#maintainer-release-keys).

A good signature proves that a maintainer on that list approved these exact files.
It does not prove that the binary was built from the tagged source.
Release builds are not reproducible, and releases have no SBOM and no third-party build provenance.
The `Commit SHA` that `--version` prints is written in by the build, so it names the commit the build was given; it is not evidence that the binary came from that commit.
Before a release is published, the maintainers' tooling checks that the binary in the tarball is byte-identical to the one in the image and that `--version` names the tagged commit.
Image tags such as `:adiri` are not signed; only the digest in `IMAGE_DIGEST` is.

## Before you start

You need `curl`, `git` and GnuPG (`gpg` and `gpgv`), plus Docker for Path B.
On Debian or Ubuntu:

```sh
sudo apt-get install -y curl git gnupg
```

Start each path in a new, empty directory and run its commands in one shell.
Set `TAG` on the first line to the release you are installing.

## Path A: tarball

These commands fetch the maintainer keys from the tag, download the four release files, check the signature over `SHA256SUMS`, and check both hashes:

```sh
TAG=v0.17.0-adiri   # the release you are installing
REPO=Telcoin-Association/telcoin-network
BASE="https://github.com/$REPO/releases/download/$TAG"
git clone --quiet --depth 1 --branch "$TAG" "https://github.com/$REPO.git" tn-release
for k in tn-release/.github/maintainer-gpg-keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg
gpg --show-keys --with-fingerprint tn-release/.github/maintainer-gpg-keys/*.asc
curl -fsSL --remote-name-all "$BASE/SHA256SUMS" "$BASE/SHA256SUMS.asc" "$BASE/IMAGE_DIGEST" "$BASE/telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz"
gpgv --keyring ./tn-release-keys.gpg SHA256SUMS.asc SHA256SUMS
sha256sum --check SHA256SUMS
```

Expect:

- `gpgv` to print `Good signature from` followed by a maintainer's name, and to exit with status 0;
- `sha256sum` to print `IMAGE_DIGEST: OK` and `telcoin-network-<TAG>-x86_64-unknown-linux-gnu.tar.gz: OK`.

### Check the key fingerprints

The keys came from the tag, so a tag signed with the wrong key would also carry the wrong key.
Check every primary key fingerprint that `gpg --show-keys` printed against two sources the tag cannot change:

- the maintainer release keys table in [`SECURITY.md` on `main`](https://github.com/Telcoin-Association/telcoin-network/blob/main/SECURITY.md#maintainer-release-keys);
- the maintainer's GitHub account at `https://github.com/<handle>.gpg`, where `<handle>` is the key file's name without `.asc`.

This loop prints the keys GitHub has for each handle:

```sh
for k in tn-release/.github/maintainer-gpg-keys/*.asc; do
  h=$(basename "$k" .asc)
  echo "== $h"
  curl -fsSL "https://github.com/$h.gpg" | gpg --show-keys --with-fingerprint
done
```

Each fingerprint must appear in both places.
If one is missing or different, stop and report it.

### Check the binary

```sh
tar -xzf "telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz"
cd "telcoin-network-$TAG-x86_64-unknown-linux-gnu"
./telcoin-network --version
git -C ../tn-release rev-parse HEAD
```

The tarball holds one directory, `telcoin-network-<TAG>-x86_64-unknown-linux-gnu/`, with `telcoin-network`, `LICENSE-APACHE`, `LICENSE-MIT` and `NOTICE` in it.
The `--version` output must include these lines:

- `Version: X.Y.Z` on the first line, after the program name, where `X.Y.Z` is the tag without its leading `v` and without `-adiri` or `-rcN` (`telcoin-network-cli Version: 0.17.0` for `v0.17.0-adiri`);
- `Commit SHA:` followed by the commit that `git rev-parse` printed;
- `Build Features:` containing `adiri` for an `-adiri` tag, and not containing it for any other tag.

Releases cut before this process print `Version: 0.1.0`.

Install the binary:

```sh
sudo install -m 0755 telcoin-network /usr/local/bin/telcoin-network
```

Record the tarball's SHA-256 from `SHA256SUMS` and the `--version` output in your operator inventory, as the [release and network update process](validator-operations.md#release-and-network-update-process) asks.

## Path B: Docker image

The image needs only three of the release files: `SHA256SUMS`, `SHA256SUMS.asc` and `IMAGE_DIGEST`.
The first six lines are the same as in Path A.

```sh
TAG=v0.17.0-adiri   # the release you are installing
REPO=Telcoin-Association/telcoin-network
BASE="https://github.com/$REPO/releases/download/$TAG"
git clone --quiet --depth 1 --branch "$TAG" "https://github.com/$REPO.git" tn-release
for k in tn-release/.github/maintainer-gpg-keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg
gpg --show-keys --with-fingerprint tn-release/.github/maintainer-gpg-keys/*.asc
curl -fsSL --remote-name-all "$BASE/SHA256SUMS" "$BASE/SHA256SUMS.asc" "$BASE/IMAGE_DIGEST"
gpgv --keyring ./tn-release-keys.gpg SHA256SUMS.asc SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
docker pull "$(cat IMAGE_DIGEST)"
docker run --rm --network none "$(cat IMAGE_DIGEST)" telcoin --version
```

Check the key fingerprints as in [Path A](#check-the-key-fingerprints).
`sha256sum` prints `IMAGE_DIGEST: OK`; `--ignore-missing` skips the tarball, which this path does not download.
In the image the binary is `/usr/local/bin/telcoin`, and its `--version` output must show the same three lines as in [Path A](#check-the-binary).

Docker checks an image pulled by digest against that digest, so the image you pulled is the one the signature covers.
Put the full content of `IMAGE_DIGEST` (`ghcr.io/telcoin-association/telcoin-network@sha256:...`) in compose files and systemd units, not a tag.
The image is `linux/amd64` only.

If you already pulled by tag, compare the two lines this prints; they must be equal:

```sh
docker image inspect --format '{{index .RepoDigests 0}}' "ghcr.io/telcoin-association/telcoin-network:$TAG"
cat IMAGE_DIGEST
```

## Optional: check the tag signature

The tag carries its own signature from the same key.
To check it in a throwaway keyring:

```sh
GNUPGHOME=$(mktemp -d)
export GNUPGHOME
gpg --quiet --import tn-release/.github/maintainer-gpg-keys/*.asc
git -C tn-release verify-tag "$TAG"
unset GNUPGHOME
```

`git verify-tag` prints `Good signature from` and a warning that the key is not certified with a trusted signature.
The warning is expected in a fresh keyring; the fingerprint check is what ties the key to a maintainer.

## When verification fails

Never run a binary or image that failed one of these checks.

| Symptom | Meaning | Action |
| --- | --- | --- |
| `git clone` cannot find the tag, or `curl` fails with `404` | `TAG` is wrong, or the release is not published yet | Check `TAG` against the [releases page](https://github.com/Telcoin-Association/telcoin-network/releases). |
| `gpgv` prints `BAD signature` | `SHA256SUMS` changed after it was signed | Stop and report it. |
| `gpgv` prints `Can't check signature: No public key` | The signing key is not in the tag's allowlist, or the files belong to another release | Check `TAG`; if it is right, stop and report it. |
| `gpgv` reports a good signature and notes that the key has expired | The key expired after the release was signed | Acceptable for an older release if the fingerprint matches `SECURITY.md`. |
| A fingerprint is missing from, or differs from, `SECURITY.md` or GitHub | The keys in the tag are not the published maintainer keys | Stop and report it. |
| `sha256sum` prints `FAILED` | A file differs from its signed hash | Download it again once; if it still fails, report it. |
| `sha256sum` prints `no file was verified` | None of the files named in `SHA256SUMS` is present | Check `TAG` and the downloaded file names. |
| `Commit SHA` differs from the tag's commit, or `Build Features` lacks `adiri` on an `-adiri` tag | The binary does not match the release | Stop and report it. |
| `exec format error`, or Docker warns that the image platform does not match the host | The image is `linux/amd64` only | Use an x86_64 host, or build from source. |
| `docker pull` fails with `denied` or `unauthorized` | The image is not publicly readable | Tell the maintainers. |

Report a failed check through the [security policy](https://github.com/Telcoin-Association/telcoin-network/blob/main/SECURITY.md).
