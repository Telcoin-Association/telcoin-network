# Installing a release

This page is for operators who run Telcoin Network on Linux x86_64.
Each release is a signed git tag plus these files on the tag's [GitHub release](https://github.com/Telcoin-Association/telcoin-network/releases):

| File | Content |
| --- | --- |
| `telcoin-network-<TAG>-x86_64-unknown-linux-gnu.tar.gz` | The `telcoin-network` binary, with `LICENSE-APACHE`, `LICENSE-MIT` and `NOTICE` |
| `IMAGE_DIGEST` | The `linux/amd64` image, named by its content digest, in the network's package: `ghcr.io/telcoin-association/tn-mainnet` for a mainnet tag, `ghcr.io/telcoin-association/tn-testnet` for an Adiri tag |
| `SHA256SUMS` | The SHA-256 of the tarball, of `IMAGE_DIGEST`, and of the network's three chain-config files |
| `SHA256SUMS.asc` | A maintainer's detached OpenPGP signature over `SHA256SUMS` |
| `worker-gateway-<TAG>-x86_64-unknown-linux-gnu.tar.gz` | Releases that ship the edge services: the `worker-gateway` binary, with `LICENSE-APACHE`, `LICENSE-MIT` and `NOTICE` |
| `node-record-api-<TAG>-x86_64-unknown-linux-gnu.tar.gz` | Releases that ship the edge services: the `node-record-api` binary, with `LICENSE-APACHE`, `LICENSE-MIT` and `NOTICE` |
| `IMAGE_DIGEST-worker-gateway`, `IMAGE_DIGEST-node-record-api` | Releases that ship the edge services: the service's `linux/amd64` image, named by its content digest, in the service's own package, `ghcr.io/telcoin-association/tn-worker-gateway` or `ghcr.io/telcoin-association/tn-node-record-api`, which mainnet and Adiri tags share |
| `SHA256SUMS-edge` | Releases that ship the edge services: the SHA-256 of each edge tarball and each `IMAGE_DIGEST-<service>` file |
| `SHA256SUMS-edge.asc` | Releases that ship the edge services: the maintainers' detached OpenPGP signatures over `SHA256SUMS-edge`, by the same keys as `SHA256SUMS.asc` |

Every release ships the node.
A release also ships `worker-gateway`, `node-record-api` or both when `etc/release-components` at the tagged commit names them; a tag without that file ships the node alone.
[Edge services](#edge-services-worker-gateway-and-node-record-api) verifies their files.

There are no release builds for other platforms.
On those, build from source as the [README](https://github.com/Telcoin-Association/telcoin-network/blob/main/README.md#quick-start) describes.

## Release channels

The tag name sets the network, the build features, the image package and the image tags.

| Tag | Network | Build features | Image package | Image tags |
| --- | --- | --- | --- | --- |
| `vX.Y.Z` | mainnet | none | `tn-mainnet` | `:vX.Y.Z`, `:latest` |
| `vX.Y.Z-rcN` | mainnet release candidate | none | `tn-mainnet` | `:vX.Y.Z-rcN` only |
| `vX.Y.Z-adiri` | Adiri testnet | `adiri` | `tn-testnet` | `:vX.Y.Z-adiri`, `:adiri` |
| `vX.Y.Z-adiri-rcN` | Adiri release candidate | `adiri` | `tn-testnet` | `:vX.Y.Z-adiri-rcN` only |

Both packages are under `ghcr.io/telcoin-association/`.
A mainnet release never has its image in `tn-testnet`, and an Adiri release never in `tn-mainnet`; the image check below refuses either.
The edge services' images are in neither: each service has one package that both networks share, as [Edge services](#edge-services-worker-gateway-and-node-record-api) describes.

Running with `--chain adiri` needs a build with the `adiri` feature, so Adiri nodes use the `-adiri` releases.
Release candidates are for testing and do not belong in production.
`:latest` and `:adiri` move to the newest release in their channel when it is published, so deployments should pin the image digest instead (see [Path B](#path-b-docker-image)).
GitHub marks every Adiri release and every release candidate as a pre-release, so the repository's "Latest" release never points at one of them.
[Release notes](release-notes.md) lists the changes in each release.

## What the signature proves

`SHA256SUMS` has five lines: `IMAGE_DIGEST`, which names the image by its content digest, the tarball, and the network's `committee.yaml`, `genesis.yaml` and `parameters.yaml` by their path in the repository, under `chain-configs/testnet/` for an `-adiri` tag and `chain-configs/mainnet/` for any other.
One signature over `SHA256SUMS` therefore covers the binary, the image and the chain configuration.
The edge services' files are not in `SHA256SUMS`; a second signed manifest, `SHA256SUMS-edge`, covers them, as [Edge services](#edge-services-worker-gateway-and-node-record-api) describes.
The blocks below extract the chain-config files from the tag, so Path A checks all five lines and Path B checks every line but the tarball's.
`SHA256SUMS.asc` is made with a maintainer's OpenPGP signing key, held on a YubiKey, which it never leaves.
The git tag carries a signature by a key on the same allowlist.
The keys allowed to sign are in `keys/` of [`tn-release-signing`](https://github.com/Telcoin-Association/tn-release-signing), the repository that holds the maintainer keys every signed Telcoin project shares.
This repository carries it as the `release` submodule, and the keys that count are the ones at the commit `main` records for `release`.
Their fingerprints are in the key table of `KEYS.md` in that same commit.

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
The first command of each block stops on a directory that is not empty, so a file left from an earlier download is never checked in place of this release's.
Set `TAG` on the first line to the release you are installing.

Both paths take the maintainer keys from `main`, never from the release's tag: they clone `main`, check out its `release` submodule at the commit `main` records, and read the keys there.
A tag therefore cannot bring its own key, and moving the `release` pointer on `main` needs a review from a maintainer who holds a release key.
A key removed from the allowlist stops verifying as soon as `main` records a `release` commit without it.
The tag is fetched into the same checkout by its full name, `refs/tags/<TAG>`, because `git clone --branch <TAG>` prefers a branch of the same name, if one exists, and says nothing.

## Path A: tarball

In a new, empty directory, these commands clone `main` and its `release` submodule for the maintainer keys, fetch the tag, extract the chain-config files from it, download the four release files, stopping at the first download that fails, check the signature over `SHA256SUMS`, and check every hash in it:

```sh
TAG=v0.17.0-adiri
REPO=Telcoin-Association/telcoin-network
BASE="https://github.com/$REPO/releases/download/$TAG"
if [ -n "$(ls -A)" ]; then echo "Run these commands in a new, empty directory" >&2; false; fi &&
git clone --quiet --depth 1 --branch main "https://github.com/$REPO.git" tn-main &&
git -C tn-main submodule update --init --depth 1 -- release &&
git -C tn-main fetch --quiet --depth 1 origin "refs/tags/$TAG:refs/tags/$TAG" &&
git -C tn-main archive "refs/tags/$TAG" chain-configs | tar -xf - &&
for k in tn-main/release/keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg &&
gpg --show-keys --with-fingerprint tn-main/release/keys/*.asc &&
curl -fsSL --fail-early --remote-name-all "$BASE/SHA256SUMS" "$BASE/SHA256SUMS.asc" "$BASE/IMAGE_DIGEST" "$BASE/telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz" &&
gpgv --status-fd 1 --keyring ./tn-release-keys.gpg SHA256SUMS.asc SHA256SUMS |
awk '$2 ~ /^(BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { bad = 1 } $2 ~ /^(GOODSIG|BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { s = $2; print $2 } $2 == "VALIDSIG" && s == "GOODSIG" { print "signed by primary key " $NF; n++ } END { exit (n && !bad) ? 0 : 1 }' &&
sha256sum --check SHA256SUMS &&
echo "Signature and hashes verified"
```

Each command runs only if the one before it passed.
Expect:

- `GOODSIG`, then `signed by primary key` followed by a fingerprint;
- `sha256sum` to print `OK` after `IMAGE_DIGEST`, after the three `chain-configs/` files and after `telcoin-network-<TAG>-x86_64-unknown-linux-gnu.tar.gz`;
- `Signature and hashes verified` as the last line.

gpgv prints `Good signature from` and exits 0 even when the signing key is revoked or expired, so the block reads gpgv's status lines instead: it counts only a `GOODSIG`, and it fails when any signature in `SHA256SUMS.asc` is `BADSIG`, `ERRSIG`, `EXPSIG`, `EXPKEYSIG` or `REVKEYSIG`, even if another one is good.
The release pipeline applies the same rule before a maintainer signs and before a release is published.
If the last line `Signature and hashes verified` is missing, the files are not verified; stop, do not extract or run anything from this release, and report it.
The lines above it show which check failed, and [When verification fails](#when-verification-fails) explains each one.

### Chain-config files

`SHA256SUMS` lists the network's `committee.yaml`, `genesis.yaml` and `parameters.yaml` by their path in the repository: `chain-configs/testnet/` for an `-adiri` tag and `chain-configs/mainnet/` for any other.
These are the files built into the release's binary for `--chain` that a datadir can also hold. The binary also carries the network's `bootstrap_servers.yaml`, the servers a `--chain` node dials to join; the node never reads that file from disk, so it is checked as part of the binary, by the tarball's hash.
The block extracts them from the tag into `chain-configs/`, so `sha256sum` checks them against their signed hashes, and you can use these copies.
To check copies you already have, run this from the directory that holds their `chain-configs/` folder, such as the root of a checkout of the tag, with the path of the verified `SHA256SUMS`:

```sh
grep '  chain-configs/' /path/to/SHA256SUMS | sha256sum --check
```

It must print `OK` for all three files.

### Check the key fingerprints

The keys come from `main`, so the tag cannot change them.
Check that they are the maintainers' keys: every primary key fingerprint that `gpg --show-keys` printed, including the one after `signed by primary key`, must appear in both of these places:

- the key table in `tn-main/release/KEYS.md`, from the same `main` checkout, with a Status that is not `revoked`;
- the maintainer's GitHub account at `https://github.com/<handle>.gpg`, where `<handle>` is the key file's name without `.asc`.

`git -C tn-main submodule status -- release` prints the commit of `tn-release-signing` that the keys and the table came from.
This loop prints the keys GitHub has for each handle:

```sh
for k in tn-main/release/keys/*.asc; do
  h=$(basename "$k" .asc)
  echo "== $h"
  curl -fsSL "https://github.com/$h.gpg" | gpg --show-keys --with-fingerprint
done
```

If a fingerprint is missing from either place, is different, or has the Status `revoked`, stop and report it.

### Check the binary

Run this only after the last line was `Signature and hashes verified` and the fingerprints matched:

```sh
tar -xzf "telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz" &&
cd "telcoin-network-$TAG-x86_64-unknown-linux-gnu" &&
./telcoin-network --version &&
git -C ../tn-main rev-parse "refs/tags/$TAG^{commit}"
```

The tarball holds one directory, `telcoin-network-<TAG>-x86_64-unknown-linux-gnu/`, with `telcoin-network`, `LICENSE-APACHE`, `LICENSE-MIT` and `NOTICE` in it.
The `--version` output must include these lines:

- `telcoin-network-cli Version: X.Y.Z` as the first line, where `X.Y.Z` is the tag without its leading `v` and without `-adiri` or `-rcN` (`telcoin-network-cli Version: 0.17.0` for `v0.17.0-adiri`);
- `Commit SHA:` followed by the commit that `git rev-parse` printed, which is the commit the tag points to;
- `Build Features:` with nothing after it for a mainnet tag (`vX.Y.Z` or `vX.Y.Z-rcN`); for an `-adiri` tag, `adiri`, possibly with `faucet`, which the `adiri` feature implies, and nothing else.

Releases cut before this process print `Version: 0.1.0` after the program name.

Install the binary:

```sh
sudo install -m 0755 telcoin-network /usr/local/bin/telcoin-network
```

Record the tarball's SHA-256 from `SHA256SUMS` and the `--version` output in your operator inventory, as the [release and network update process](validator-operations.md#release-and-network-update-process) asks.

## Path B: Docker image

The image needs only three of the release files, `SHA256SUMS`, `SHA256SUMS.asc` and `IMAGE_DIGEST`, and the chain-config files from the tag.
In a new, empty directory, run the Path A block without the tarball.
Its last check drops only the tarball's line from `SHA256SUMS` with `grep`, so `sha256sum` checks `IMAGE_DIGEST` and the three chain-config files and fails when one of them is missing:

```sh
TAG=v0.17.0-adiri
REPO=Telcoin-Association/telcoin-network
BASE="https://github.com/$REPO/releases/download/$TAG"
if [ -n "$(ls -A)" ]; then echo "Run these commands in a new, empty directory" >&2; false; fi &&
git clone --quiet --depth 1 --branch main "https://github.com/$REPO.git" tn-main &&
git -C tn-main submodule update --init --depth 1 -- release &&
git -C tn-main fetch --quiet --depth 1 origin "refs/tags/$TAG:refs/tags/$TAG" &&
git -C tn-main archive "refs/tags/$TAG" chain-configs | tar -xf - &&
for k in tn-main/release/keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg &&
gpg --show-keys --with-fingerprint tn-main/release/keys/*.asc &&
curl -fsSL --fail-early --remote-name-all "$BASE/SHA256SUMS" "$BASE/SHA256SUMS.asc" "$BASE/IMAGE_DIGEST" &&
gpgv --status-fd 1 --keyring ./tn-release-keys.gpg SHA256SUMS.asc SHA256SUMS |
awk '$2 ~ /^(BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { bad = 1 } $2 ~ /^(GOODSIG|BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { s = $2; print $2 } $2 == "VALIDSIG" && s == "GOODSIG" { print "signed by primary key " $NF; n++ } END { exit (n && !bad) ? 0 : 1 }' &&
grep -v -F "  telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz" SHA256SUMS | sha256sum --check &&
echo "Signature and hashes verified"
```

Expect `GOODSIG`, then `signed by primary key` followed by a fingerprint, then `OK` after `IMAGE_DIGEST` and after the three `chain-configs/` files, and `Signature and hashes verified` as the last line.
If the last line `Signature and hashes verified` is missing, the files are not verified; stop, do not pull or run the image, and report it.
Then check the key fingerprints as in [Path A](#check-the-key-fingerprints), and stop if one is missing, different or revoked.

### Check the image

Run this only after the signature, the hash and the fingerprints have passed.
It takes the image package from `TAG`, `tn-testnet` for an Adiri tag and `tn-mainnet` otherwise, and refuses an `IMAGE_DIGEST` that does not name an image in that package by digest, such as one in the other network's package:

```sh
case "$TAG" in
  *-adiri | *-adiri-rc*) PACKAGE=tn-testnet ;;
  *) PACKAGE=tn-mainnet ;;
esac
IMAGE_REF=$(cat IMAGE_DIGEST)
case "$IMAGE_REF" in
  "ghcr.io/telcoin-association/$PACKAGE@sha256:"*) docker pull "$IMAGE_REF" && docker run --rm --network none --entrypoint /usr/local/bin/telcoin "$IMAGE_REF" --version ;;
  *) echo "IMAGE_DIGEST does not name an image in ghcr.io/telcoin-association/$PACKAGE by digest: $IMAGE_REF" >&2 ;;
esac
```

In the image the binary is `/usr/local/bin/telcoin`, and its `--version` output must show the same three lines as in [Path A](#check-the-binary), with the commit that `git -C tn-main rev-parse "refs/tags/$TAG^{commit}"` prints.

Docker checks an image pulled by digest against that digest, so the image you pulled is the one the signature covers.
Put the full content of `IMAGE_DIGEST` (`ghcr.io/telcoin-association/tn-mainnet@sha256:...` for mainnet, `ghcr.io/telcoin-association/tn-testnet@sha256:...` for Adiri) in compose files and systemd units, not a tag.
The image is `linux/amd64` only.

If you already pulled by tag, compare the two lines this prints, with `PACKAGE` set as in the block above; they must be equal:

```sh
docker image inspect --format '{{index .RepoDigests 0}}' "ghcr.io/telcoin-association/$PACKAGE:$TAG"
cat IMAGE_DIGEST
```

## Edge services: worker gateway and node-record API

Some releases also ship the edge services, built from the same commit as the node:

- `worker-gateway`: a mostly stateless reverse proxy in front of a worker's JSON-RPC endpoint, which forwards the calls its method policy allows to a ready worker.
- `node-record-api`: a read-only HTTP/JSON directory of the JSON-RPC endpoints that validators advertise, which it reads from their signed node records in the worker DHT.

The notes of such a release have an "Edge services" section with a row for each service it ships and the blocks below, with `TAG` set and only those services' files.

Each service's images are in a package of its own, `ghcr.io/telcoin-association/tn-worker-gateway` and `ghcr.io/telcoin-association/tn-node-record-api`, which mainnet and Adiri releases share, so an image's tag, the release tag, says which release it belongs to.
In each of these packages `:latest` and `:adiri` move to the newest mainnet and Adiri release when it is published, as the node's do, and a release candidate moves neither; pin the digest from `IMAGE_DIGEST-<service>` instead.

The services' files are listed in a second manifest, `SHA256SUMS-edge`, which the same maintainer keys sign in `SHA256SUMS-edge.asc`, and a release needs as many signatures on it as on `SHA256SUMS`.
`SHA256SUMS` lists none of these files, so Path A and Path B do not cover them.
[What the signature proves](#what-the-signature-proves) holds for `SHA256SUMS-edge` as for `SHA256SUMS`: before a release is published, the maintainers' tooling checks that each service's binary in its tarball is byte-identical to the one in its image and that its `--version` names the tagged commit.

The blocks below are for a release that ships both services.
For a release that ships one of them, leave the other's files out of the `curl` line and the `printf` list of names, as that release's notes do; a file the release does not have stops the block at `curl` with `404`.

Run each edge block in a new, empty directory of its own, not in the directory where Path A or Path B ran.
Like the node's blocks, its first command stops on a directory that is not empty, and it clones `main` and its `release` submodule for the maintainer keys and fetches the tag itself, so it checks against the keys on `main` when it runs.

### Edge tarballs

This block downloads `SHA256SUMS-edge`, `SHA256SUMS-edge.asc`, each `IMAGE_DIGEST-<service>` and each tarball, stopping at the first download that fails, checks the signatures over `SHA256SUMS-edge` with the same rule as Path A, checks that `SHA256SUMS-edge` names exactly this release's edge files, and checks every hash in it:

```sh
TAG=v0.17.0-adiri
REPO=Telcoin-Association/telcoin-network
BASE="https://github.com/$REPO/releases/download/$TAG"
if [ -n "$(ls -A)" ]; then echo "Run these commands in a new, empty directory" >&2; false; fi &&
git clone --quiet --depth 1 --branch main "https://github.com/$REPO.git" tn-main &&
git -C tn-main submodule update --init --depth 1 -- release &&
git -C tn-main fetch --quiet --depth 1 origin "refs/tags/$TAG:refs/tags/$TAG" &&
for k in tn-main/release/keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg &&
gpg --show-keys --with-fingerprint tn-main/release/keys/*.asc &&
curl -fsSL --fail-early --remote-name-all "$BASE/SHA256SUMS-edge" "$BASE/SHA256SUMS-edge.asc" "$BASE/IMAGE_DIGEST-worker-gateway" "$BASE/worker-gateway-$TAG-x86_64-unknown-linux-gnu.tar.gz" "$BASE/IMAGE_DIGEST-node-record-api" "$BASE/node-record-api-$TAG-x86_64-unknown-linux-gnu.tar.gz" &&
gpgv --status-fd 1 --keyring ./tn-release-keys.gpg SHA256SUMS-edge.asc SHA256SUMS-edge |
awk '$2 ~ /^(BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { bad = 1 } $2 ~ /^(GOODSIG|BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { s = $2; print $2 } $2 == "VALIDSIG" && s == "GOODSIG" { print "signed by primary key " $NF; n++ } END { exit (n && !bad) ? 0 : 1 }' &&
if [ "$(awk 'length($1) == 64 && /^[0-9a-f]+  [^ ]+$/ { print $2; next } { print "malformed" }' SHA256SUMS-edge | LC_ALL=C sort)" != "$(printf '%s\n' "IMAGE_DIGEST-node-record-api" "IMAGE_DIGEST-worker-gateway" "node-record-api-$TAG-x86_64-unknown-linux-gnu.tar.gz" "worker-gateway-$TAG-x86_64-unknown-linux-gnu.tar.gz")" ]; then echo "SHA256SUMS-edge does not name exactly the edge files of $TAG" >&2; false; fi &&
sha256sum --check SHA256SUMS-edge &&
echo "Edge signature and hashes verified"
```

Expect:

- `GOODSIG`, then `signed by primary key` followed by a fingerprint;
- `sha256sum` to print `OK` after each `IMAGE_DIGEST-<service>` file and each tarball;
- `Edge signature and hashes verified` as the last line.

The same maintainer keys sign the node's `SHA256SUMS` and the `SHA256SUMS-edge` of every release, so a good signature alone does not show that the file is this release's edge manifest.
The `if` line before `sha256sum` does: it reads the names in `SHA256SUMS-edge` and stops unless every line is `<64 hex>  <name>` and the names are exactly this release's `IMAGE_DIGEST-<service>` files and tarballs, each once.
If the last line `Edge signature and hashes verified` is missing, the services' files are not verified; stop, do not extract or run any of them, and report it.
Then check the key fingerprints as in [Path A](#check-the-key-fingerprints), with this directory's `tn-main`.

### Check the edge binaries

Run this only after the last line was `Edge signature and hashes verified` and the fingerprints matched:

```sh
tar -xzf "worker-gateway-$TAG-x86_64-unknown-linux-gnu.tar.gz" &&
"./worker-gateway-$TAG-x86_64-unknown-linux-gnu/worker-gateway" --version &&
tar -xzf "node-record-api-$TAG-x86_64-unknown-linux-gnu.tar.gz" &&
"./node-record-api-$TAG-x86_64-unknown-linux-gnu/node-record-api" --version &&
git -C tn-main rev-parse "refs/tags/$TAG^{commit}"
```

Each tarball holds one directory, `<service>-<TAG>-x86_64-unknown-linux-gnu/`, with the service's binary, `LICENSE-APACHE`, `LICENSE-MIT` and `NOTICE` in it.
Each `--version` prints these two lines and nothing else, shown here for `v0.17.0-adiri`, where `<commit>` is the full commit that `git rev-parse` printed:

```text
tn-worker-gateway Version: 0.17.0
Commit SHA: <commit>

tn-node-record-api Version: 0.17.0
Commit SHA: <commit>
```

The version is the tag without its leading `v` and without `-adiri` or `-rcN`, as for the node, and there is no `Build Features:` line.

Install the binaries:

```sh
sudo install -m 0755 "worker-gateway-$TAG-x86_64-unknown-linux-gnu/worker-gateway" /usr/local/bin/worker-gateway &&
sudo install -m 0755 "node-record-api-$TAG-x86_64-unknown-linux-gnu/node-record-api" /usr/local/bin/node-record-api
```

Record each tarball's SHA-256 from `SHA256SUMS-edge` and each `--version` output in your operator inventory, as for the node.

### Edge images

The images need only `SHA256SUMS-edge`, `SHA256SUMS-edge.asc` and the `IMAGE_DIGEST-<service>` files.
Run the [Edge tarballs](#edge-tarballs) block without the tarballs, in a new, empty directory of its own.
It checks the names in `SHA256SUMS-edge` as that block does, the tarballs' included.
Its last check then drops only the tarballs' lines from `SHA256SUMS-edge` with `grep`, so `sha256sum` checks every `IMAGE_DIGEST-<service>` file and fails when one of them is missing:

```sh
TAG=v0.17.0-adiri
REPO=Telcoin-Association/telcoin-network
BASE="https://github.com/$REPO/releases/download/$TAG"
if [ -n "$(ls -A)" ]; then echo "Run these commands in a new, empty directory" >&2; false; fi &&
git clone --quiet --depth 1 --branch main "https://github.com/$REPO.git" tn-main &&
git -C tn-main submodule update --init --depth 1 -- release &&
git -C tn-main fetch --quiet --depth 1 origin "refs/tags/$TAG:refs/tags/$TAG" &&
for k in tn-main/release/keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg &&
gpg --show-keys --with-fingerprint tn-main/release/keys/*.asc &&
curl -fsSL --fail-early --remote-name-all "$BASE/SHA256SUMS-edge" "$BASE/SHA256SUMS-edge.asc" "$BASE/IMAGE_DIGEST-worker-gateway" "$BASE/IMAGE_DIGEST-node-record-api" &&
gpgv --status-fd 1 --keyring ./tn-release-keys.gpg SHA256SUMS-edge.asc SHA256SUMS-edge |
awk '$2 ~ /^(BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { bad = 1 } $2 ~ /^(GOODSIG|BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { s = $2; print $2 } $2 == "VALIDSIG" && s == "GOODSIG" { print "signed by primary key " $NF; n++ } END { exit (n && !bad) ? 0 : 1 }' &&
if [ "$(awk 'length($1) == 64 && /^[0-9a-f]+  [^ ]+$/ { print $2; next } { print "malformed" }' SHA256SUMS-edge | LC_ALL=C sort)" != "$(printf '%s\n' "IMAGE_DIGEST-node-record-api" "IMAGE_DIGEST-worker-gateway" "node-record-api-$TAG-x86_64-unknown-linux-gnu.tar.gz" "worker-gateway-$TAG-x86_64-unknown-linux-gnu.tar.gz")" ]; then echo "SHA256SUMS-edge does not name exactly the edge files of $TAG" >&2; false; fi &&
grep -v -F -e "  worker-gateway-$TAG-x86_64-unknown-linux-gnu.tar.gz" -e "  node-record-api-$TAG-x86_64-unknown-linux-gnu.tar.gz" SHA256SUMS-edge | sha256sum --check &&
echo "Edge signature and hashes verified"
```

Expect `GOODSIG`, then `signed by primary key` followed by a fingerprint, then `OK` after each `IMAGE_DIGEST-<service>` file, and `Edge signature and hashes verified` as the last line.
If the last line `Edge signature and hashes verified` is missing, the files are not verified; stop, do not pull or run the images, and report it.
Then check the key fingerprints as in [Path A](#check-the-key-fingerprints), with this directory's `tn-main`, and stop if one is missing, different or revoked.

### Check the edge images

Run this only after the signature, the hashes and the fingerprints have passed.
It refuses an `IMAGE_DIGEST-<service>` that does not name an image in that service's package by digest; each service's images are published there and nowhere else:

```sh
IMAGE_REF=$(cat IMAGE_DIGEST-worker-gateway) &&
case "$IMAGE_REF" in
  ghcr.io/telcoin-association/tn-worker-gateway@sha256:*) docker pull "$IMAGE_REF" && docker run --rm --network none --entrypoint /usr/local/bin/worker-gateway "$IMAGE_REF" --version ;;
  *) echo "IMAGE_DIGEST-worker-gateway does not name an image in ghcr.io/telcoin-association/tn-worker-gateway by digest: $IMAGE_REF" >&2; false ;;
esac &&
IMAGE_REF=$(cat IMAGE_DIGEST-node-record-api) &&
case "$IMAGE_REF" in
  ghcr.io/telcoin-association/tn-node-record-api@sha256:*) docker pull "$IMAGE_REF" && docker run --rm --network none --entrypoint /usr/local/bin/node-record-api "$IMAGE_REF" --version ;;
  *) echo "IMAGE_DIGEST-node-record-api does not name an image in ghcr.io/telcoin-association/tn-node-record-api by digest: $IMAGE_REF" >&2; false ;;
esac &&
echo "Edge images pulled by digest"
```

In each image the binary is `/usr/local/bin/<service>`, and its `--version` output must be the same two lines as its tarball's in [Check the edge binaries](#check-the-edge-binaries), with the commit that `git -C tn-main rev-parse "refs/tags/$TAG^{commit}"` prints.
If the last line is not `Edge images pulled by digest`, do not run the images; the lines above it say which check failed.
Put the full content of each `IMAGE_DIGEST-<service>` (`ghcr.io/telcoin-association/tn-worker-gateway@sha256:...` or `ghcr.io/telcoin-association/tn-node-record-api@sha256:...`) in compose files and systemd units, not a tag.
The images are `linux/amd64` only.

### When an edge check fails

Never extract, install or run any of the services' files when one of these checks failed.
[When verification fails](#when-verification-fails) applies, with `SHA256SUMS-edge`, `SHA256SUMS-edge.asc`, `IMAGE_DIGEST-<service>` and `Edge signature and hashes verified` in place of `SHA256SUMS`, `SHA256SUMS.asc`, `IMAGE_DIGEST` and `Signature and hashes verified`.
Its table also has rows for the edge blocks' digest-file refusal, `IMAGE_DIGEST-<service> does not name an image in ghcr.io/telcoin-association/tn-<service> by digest`, and for an edge service's `--version`.
Two more failures belong to the edge blocks alone:

- `SHA256SUMS-edge does not name exactly the edge files of <TAG>`: the signed manifest lacks one of this release's edge files, names another file or holds a malformed line, as the node's `SHA256SUMS` or another release's `SHA256SUMS-edge` would; it is not this release's edge manifest, so stop and report it.
- `curl` fails with `404` for an edge file: besides a wrong `TAG`, the release may not ship that service; its notes and its files on the release page show which services it ships.

## Optional: check the tag signature

The tag carries its own signature, by one key on the allowlist.
To check it in a throwaway keyring, from the directory you started in:

```sh
GNUPGHOME=$(mktemp -d)
export GNUPGHOME
gpg --quiet --import tn-main/release/keys/*.asc
git -C tn-main verify-tag "$TAG"
unset GNUPGHOME
```

`git verify-tag` prints `Good signature from` and a warning that the key is not certified with a trusted signature.
The warning is expected in a fresh keyring; the fingerprint check is what ties the key to a maintainer.

## When verification fails

Never extract, install or run anything from a release that failed one of these checks.

| Symptom | Meaning | Action |
| --- | --- | --- |
| The last line is not `Signature and hashes verified` | A step of the block failed, and the lines above say which | Find that line in this table; do not extract or run anything from the release. |
| `Run these commands in a new, empty directory` | The directory holds files already, possibly from another download | Start again in a new, empty directory. |
| `git fetch` cannot find `refs/tags/<TAG>`, or `curl` fails with `404` | `TAG` is wrong, or the release is not published yet | Check `TAG` against the [releases page](https://github.com/Telcoin-Association/telcoin-network/releases). |
| `BADSIG` (gpgv prints `BAD signature`) | `SHA256SUMS` changed after it was signed | Stop and report it. |
| `ERRSIG` (gpgv prints `Can't check signature: No public key`) | The signing key is not in the allowlist on `main`: `TAG` is wrong, the files belong to another release, or the key was removed after this release was signed | Check `TAG`; if it is right, stop and report it. |
| `REVKEYSIG` (gpgv still prints `Good signature`) | The signing key was revoked, for example after a YubiKey was lost | Stop and report it. A release signed before the revocation can no longer be verified against `main`, which is intended. |
| `EXPKEYSIG` (gpgv still prints `Good signature`) | The signing key has expired | Stop and report it. An older release signed before the expiry cannot be verified against `main` until a maintainer extends the key and `main` records a `release` commit that carries the extended key. |
| `EXPSIG` | The signature itself has expired | Stop and report it. |
| One of the rows above next to a `GOODSIG` | Another signature in `SHA256SUMS.asc` is bad, unverifiable, expired or revoked, and the block refuses the file as a whole | Stop and report it. |
| `sha256sum` prints `FAILED` | A file differs from its signed hash | Check `TAG` and run the block again in a new, empty directory; if it fails again, stop and report it. |
| `sha256sum` prints `FAILED open or read` for a `chain-configs/` file | `git archive` did not extract it from the tag | Check that the tag was fetched; if it was, stop and report it. |
| A fingerprint is missing from, or differs from, `tn-main/release/KEYS.md` or GitHub, or its `KEYS.md` Status is `revoked` | The keys on `main` are not the current maintainer keys | Stop and report it. |
| `IMAGE_DIGEST does not name an image in ghcr.io/telcoin-association/<package> by digest` | The signed `IMAGE_DIGEST` points outside the tag's package, for example at the other network's package | Stop and report it. |
| `IMAGE_DIGEST-<service> does not name an image in ghcr.io/telcoin-association/tn-<service> by digest` | The signed digest file of an edge service points outside that service's package, for example at the node's package | Stop and report it. |
| `Commit SHA` differs from the tag's commit, `Build Features` is not empty on a mainnet tag, or on an `-adiri` tag lacks `adiri` or lists anything but `adiri` and `faucet` | The binary does not match the release | Stop and report it. |
| An edge service's `--version` prints anything but its two lines, `tn-<service> Version: X.Y.Z` and `Commit SHA:` with the tag's commit, such as another version, another commit or a `Build Features:` line | The service's binary does not match the release | Stop and report it. |
| `exec format error`, or Docker warns that the image platform does not match the host | The release is built for Linux x86_64 only | Use an x86_64 host, or build from source. |
| `docker pull` fails with `denied` or `unauthorized` | The image is not publicly readable | Tell the maintainers. |

Report a failed check through the [security policy](https://github.com/Telcoin-Association/telcoin-network/blob/main/SECURITY.md).
