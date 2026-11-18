# Releasing

A release goes from a release PR to a published GitHub release in seven steps.
CI validates the signed tag and creates a draft release; it never builds anything.
The maintainer builds on the build server, signs on the laptop, and publishes from the build server once every check passes.
A release ships the node, and also the two edge services, the worker gateway and the node-record API, when the tagged commit names them, as [Edge services](#edge-services) describes.
Each step is a `make` target that calls [`etc/release.sh`](https://github.com/Telcoin-Association/telcoin-network/blob/main/etc/release.sh).
`release-tag`, `release-sign` and `release-publish` show what they are about to do and wait for you to type `yes`.
The examples use `v0.17.0-adiri`.

## At a glance

| Step | Where | Command | Result |
| --- | --- | --- | --- |
| 1. Release PR | build server | `make release-prep TAG=v0.17.0-adiri`, a PR merged alone, then `ALLOW_STALE_BASE=1 make attest` on the release commit | The attested `release: v0.17.0-adiri` commit on `main` |
| 2. Tag | laptop | `make release-tag TAG=v0.17.0-adiri` | A signed tag on GitHub |
| 3. Validate and draft | CI | runs when the tag is pushed | A draft release with generated notes |
| 4. Build | build server | `make release-build TAG=v0.17.0-adiri` | The node image pushed to `tn-testnet` as `:v0.17.0-adiri`, and each edge service's image to its own package under the same tag; the tarballs, the digest files, `SHA256SUMS` and, with edge services, `SHA256SUMS-edge` on the draft |
| 5. Sign | laptop | `make release-sign TAG=v0.17.0-adiri` | `SHA256SUMS.asc`, and with edge services `SHA256SUMS-edge.asc`, on the draft |
| 6. Verify and publish | build server | `make release-verify TAG=v0.17.0-adiri`, then `make release-publish TAG=v0.17.0-adiri` | The published release, and the channel aliases moved if it is the newest release |
| 7. Post-publish check | CI | runs when the release is published | The same verification, repeated on a GitHub runner |

## Release channels

| Tag | Network | Build features | Image package | Image tags | GitHub release |
| --- | --- | --- | --- | --- | --- |
| `vX.Y.Z` | mainnet | none | `tn-mainnet` | `:vX.Y.Z`, `:latest` | Latest, or a normal release if a higher mainnet version, tagged after it, was published first |
| `vX.Y.Z-rcN` | mainnet release candidate | none | `tn-mainnet` | `:vX.Y.Z-rcN` only | Pre-release |
| `vX.Y.Z-adiri` | Adiri testnet | `adiri` | `tn-testnet` | `:vX.Y.Z-adiri`, `:adiri` | Pre-release |
| `vX.Y.Z-adiri-rcN` | Adiri release candidate | `adiri` | `tn-testnet` | `:vX.Y.Z-adiri-rcN` only | Pre-release |

Each network has its own image package: `ghcr.io/telcoin-association/tn-mainnet` for mainnet tags and `ghcr.io/telcoin-association/tn-testnet` for Adiri tags.
`etc/release.sh` takes the package from the tag alone, and `./etc/release.sh parse <TAG>` prints it as `PACKAGE`; `RELEASE_REPO` changes only the repository owner in it.
Every step that reads or writes `IMAGE_DIGEST` requires exactly one line, `<package image>@sha256:<64 hex>`, naming the tag's package: `release-build`'s own check of the draft, `release-sign`, which checks again right before it signs, `release-verify`, `release-publish`, and in CI `draft-release` and `verify-release`.
Each refuses an image in another package, several lines, a line without a digest and a malformed digest alike, and the refusal says what the file holds, the tag's channel, its package and the line expected, as in `IMAGE_DIGEST names an image in ghcr.io/telcoin-association/tn-mainnet, but v0.17.0-adiri is on the adiri channel, whose images are published only in ghcr.io/telcoin-association/tn-testnet (expected the one line ghcr.io/telcoin-association/tn-testnet@sha256:<64 hex>)`.
`make docker-adiri` and `make docker-devnet` push their ad-hoc `dev-<flavor>-<TAG>` images to `ghcr.io/telcoin-association/tn-dev`, never to a release package; the earlier package `ghcr.io/telcoin-association/telcoin-network` never held a release and is no longer used.
There is no devnet release channel: devnet images exist only as these unsigned ad-hoc builds.

`release-build` pushes the node's image to the tag's package under the tag name.
`release-publish` moves `:latest` or `:adiri`, inside the same package, only when the release is the highest published final version in its channel, so publishing a lower version after a higher one does not move the alias back.
It marks a release Latest only when it is a mainnet final release with the highest published mainnet version, and every release candidate and Adiri release a pre-release.
The version rules below refuse a final version that is not higher than every final version already tagged in its channel, so a patch to an earlier minor version, such as `v1.1.1` once `v1.2.0` exists, cannot be released.
A mainnet final is published as a normal release that is not Latest only when a higher version, tagged after it, was published first.
`etc/release.sh` rejects any tag outside this grammar, so neither CI nor the `make` targets accept one; numbers have no leading zeros, and `N` starts at 1.

The tag checks also enforce these version rules:

- the release commit sets `[workspace.package].version` in `Cargo.toml` to `X.Y.Z`, with no suffix;
- a final version must be higher than every other final version in the same channel;
- a release candidate's version must be higher than every final version in the same channel, so none can be tagged once its own final release exists;
- mainnet and Adiri may use the same `X.Y.Z`;
- `telcoin-network/vX.Y.Z/linux` must fit in the 32-byte block extra-data field.

A tag is validated against every release tag that exists when it is pushed.
`release-prep` and `release-tag` apply the version rules to the tags on GitHub, read with `git ls-remote`, because a clone's own tags can be out of date.
When the tag is pushed, CI compares it with every `v*` tag in its checkout; a re-run of that check compares it with the tags created before GitHub created the run, the time of the push.
Every later step fetches the `v*` tags from `origin`, and stops when a local one differs from `origin`'s, then compares with the tags dated before the tag's tagger date, on any commit; that date must not be earlier than the release commit's committer date or more than five minutes ahead of the host's clock.
So an older release still verifies after newer ones exist, while a lower version tagged after a higher one, or a release candidate tagged after its final release, fails.

### Edge services

Two edge services can ship with the node: `worker-gateway`, the reverse proxy in front of a worker's JSON-RPC endpoint, and `node-record-api`, the directory of the JSON-RPC endpoints that validators advertise.
`etc/release-components` at the tagged commit decides what a tag ships: one component name per line, out of `node`, `worker-gateway` and `node-record-api`, with `node` required; blank lines and `#` comments are ignored.
A tagged commit without that file ships the node alone, so every tag made before the file existed releases and verifies as it always did.
The file is the only thing the tag decides: every check of what it names comes from `main`, and the tag checks read it only after the tag's other checks passed.
A file that names an unknown component, names one twice, leaves out `node` or holds a malformed line fails the tag checks, so that tag can never be released; correct the file on `main` and tag a new release commit.
A defect in an edge service's build blocks the release of the tag that ships it.
A new release commit can drop that service from `etc/release-components`, and its tag then releases the node, and any other service the file still names, without it.

| Service | Image package | Image tags | Tarball | Digest file |
| --- | --- | --- | --- | --- |
| `worker-gateway` | `tn-worker-gateway` | `:<TAG>` for every tag that ships it, `:latest`, `:adiri` | `worker-gateway-<TAG>-x86_64-unknown-linux-gnu.tar.gz` | `IMAGE_DIGEST-worker-gateway` |
| `node-record-api` | `tn-node-record-api` | `:<TAG>` for every tag that ships it, `:latest`, `:adiri` | `node-record-api-<TAG>-x86_64-unknown-linux-gnu.tar.gz` | `IMAGE_DIGEST-node-record-api` |

Each service has one image package, `ghcr.io/telcoin-association/tn-worker-gateway` and `ghcr.io/telcoin-association/tn-node-record-api`, which mainnet and Adiri tags share; their tags never collide, because an Adiri tag carries `-adiri`.
In each package, `:latest` follows the mainnet finals and `:adiri` follows the Adiri finals.
`release-publish` moves a service's alias under the same rule and in the same run as the node's, so a release that moves `tn-testnet:adiri` also moves `:adiri` in the package of each edge service it ships.
Neither service has cargo features, so a mainnet tag and an Adiri tag build them the same way, and their `--version` prints no `Build Features` line.
Every step that reads or writes an `IMAGE_DIGEST-<service>` file, the same steps as for `IMAGE_DIGEST`, requires exactly one line that names an image in that service's package by digest, `<package image>@sha256:<64 hex>`, whatever the tag's channel.
Each refuses an image in another package, several lines, a line without a digest and a malformed digest alike, as in `IMAGE_DIGEST-worker-gateway names an image in ghcr.io/telcoin-association/tn-testnet, but worker-gateway images are published only in ghcr.io/telcoin-association/tn-worker-gateway, for every channel (expected the one line ghcr.io/telcoin-association/tn-worker-gateway@sha256:<64 hex>)`.
The services' files have a signed manifest of their own, `SHA256SUMS-edge`, with its signatures in `SHA256SUMS-edge.asc`, and `SHA256SUMS` lists the node's files alone, in the shape [the deployment scripts](#what-the-deployment-scripts-check) accept.
`make docker-worker-gateway` and `make docker-node-record-api` push unsigned ad-hoc images of the services to `ghcr.io/telcoin-association/tn-dev` as `dev-worker-gateway-<TAG>` and `dev-node-record-api-<TAG>`, never to a release package.

## Machines and prerequisites

### The `release` submodule

The maintainer keys that may sign a release and the signature threshold live in [`tn-release-signing`](https://github.com/Telcoin-Association/tn-release-signing), the repository that every signed Telcoin project shares.
This repository carries it as the git submodule `release/`: each key is `release/keys/<handle>.asc`, and [`KEYS.md`](https://github.com/Telcoin-Association/tn-release-signing/blob/main/KEYS.md) holds the key table and the line `Signatures required: N`.
Every step that needs a key reads the keys and that line at the commit `origin/main` records for `release`, from the submodule's object store, never from its checked-out files, from the tag or from this repository's own tree.
When the submodule lacks that commit, the step fetches it from the submodule's `origin`.
A tag therefore cannot introduce a key, and the keys change only when a PR moves the `release` pointer on `main`, which needs the review of a maintainer who holds a release key (see [Moving the submodule pointer](#moving-the-submodule-pointer)).
Every key in the shared allowlist can sign a release of this repository.

Every checkout that runs a step, on the laptop and on the build server, needs the submodule checked out.
Run this after each pull of `main`:

```sh
git submodule update --init -- release
```

Without it, every step that reads the keys stops with exit 2, and `release-prep` stops when the submodule is at another commit than `main` records, because it requires a clean tracked tree.
On a checkout whose `HEAD` predates the submodule, the command fails with a pathspec error; pull `main` first.
The release workflow checks the repository out with its submodules, so CI needs nothing more.

### Laptop (macOS)

- [YubiKey signing setup](yubikey-setup.md) is done, and your key is in the allowlist at the `release` commit that `main` records.
- `gpg --card-status` shows the YubiKey.
- `git config --get user.signingkey` prints your signing key fingerprint followed by `!`; `RELEASE_GPG_KEY` overrides it for one command.
- `gh auth status` shows a login with the `repo` scope, which `release-sign` needs to read the draft and upload to it.
- Docker runs `linux/amd64` images, as Docker Desktop does on any Mac: `release-sign` pulls each release image by digest, compares its binary with its tarball's and runs its `--version`.
- The repository is cloned and on an up-to-date `main`, with the [`release` submodule](#the-release-submodule) checked out.
- Foundry's `cast` is optional: with it, `release-tag` and `release-sign` also check the on-chain attestation, and without it they print a notice and leave that check to CI.

### Build server (Ubuntu or Debian, Linux x86_64)

Any Ubuntu or Debian server on Linux x86_64 can be the build server; `release-build` refuses any other platform.

- `docker buildx version` works, and `docker buildx inspect default` reports the `docker` driver; `RELEASE_BUILDER` selects another builder with that driver.
- `gh auth status` shows a login with the `repo` scope, to upload to the draft and edit it.
- Docker is logged in to `ghcr.io` through `make docker-login`; [Registry access on the build server](yubikey-setup.md#registry-access-on-the-build-server) covers the first login.
- `cast --version` works, and `.env` holds `GITHUB_ATTESTATION_PRIVATE_KEY` for an address with the MAINTAINER role (see the [CI environment notes](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/ACTIONS.md#environment)).
- `make init-submodules` has been run; it checks out `tn-contracts` and `release`.
- The checkout is on an up-to-date `main` with a clean tracked tree, and the [`release` submodule](#the-release-submodule) is at the commit `main` records.
  `release-prep` requires `HEAD` to be `origin/main`.
  `release-build` and `release-publish` refuse to run when `etc/release.sh` or `.github/scripts/verify_commit_hash.sh` differ from `origin/main`.

The YubiKey never goes to the build server, and the registry token never goes on the laptop.

### One-time setup

- `main` must record the `release` submodule, and the allowlist at that commit must hold a provisioned key; every check fails without the submodule, and on a placeholder file, which it names.
  `MIN_SIGNATURES` in `etc/release.sh` must be at least the `Signatures required` line of `KEYS.md` at that commit.
- A repository admin sets up the tag ruleset on `v*`, as [Settings the release workflow requires](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/ACTIONS.md#settings-the-release-workflow-requires) describes: only maintainers can create a `v*` tag, and it must be signed; nobody, maintainers included, can update or delete one.
  The scripts cannot see the ruleset, and the signatures do not depend on it: every step that reads a tag requires exactly one signature on it, made by a key in the allowlist on `main`, and publishing requires at least `MIN_SIGNATURES` maintainer signatures over `SHA256SUMS`, and as many over `SHA256SUMS-edge` when the release has it.
  The ruleset covers what signatures cannot: GitHub runs `release.yaml` from the tagged commit, so the ruleset decides who can start the workflow for a release tag, and it keeps a tag from being moved or deleted after it was validated.
- Each release package must be public, because the release scripts and operators read it without credentials, and only the release maintainer account may write to it.
  The release packages are the node's, `tn-mainnet` and `tn-testnet`, and the edge services', `tn-worker-gateway` and `tn-node-record-api`.
  A ghcr package exists only after its first push, and only `release-build` pushes to the release packages, so this setup happens at the first release that pushes to each: `tn-testnet` at the first Adiri tag, `tn-mainnet` at the first mainnet tag, and `tn-worker-gateway` and `tn-node-record-api` at the first release that ships the edge services, whichever channel it is on:
  1. Run step 4 for that tag.
     Before anything is built, `release-build` asks the registry for the tag in each of the tag's packages; ghcr answers 403 for a package that does not exist yet, as it does for a private one, so `release-build` asks again with your Docker login.
     The login does not see the tag in a package that does not exist yet, and `release-build` then stops and asks for `RELEASE_FIRST_PUSH`, the comma-separated list of the new packages.
     At the first Adiri tag, run `RELEASE_FIRST_PUSH=tn-testnet make release-build TAG=<tag>` (`tn-mainnet` at the first mainnet tag).
     At the first release that ships the edge services, run `RELEASE_FIRST_PUSH=tn-worker-gateway,tn-node-record-api make release-build TAG=<tag>`, and add the network's package when that release is also the network's first, as in `RELEASE_FIRST_PUSH=tn-testnet,tn-worker-gateway,tn-node-record-api`.
     Every name in the list must be a package of the tag, and `RELEASE_YES=1` never stands in for it.
     `release-build` builds and checks every image of the tag, pushes them all, the first image of each new package among them, and then stops because the new packages are not public.
  2. Open each new package's settings, `https://github.com/orgs/Telcoin-Association/packages/container/<package>/settings`, such as `.../tn-testnet/settings` or `.../tn-worker-gateway/settings`.
     Under "Danger Zone", change the visibility to public.
     Under "Manage access", turn off the access inherited from the repository and give write access to the release maintainer account only.
     Under "Manage Actions access", leave this repository out or give it the Read role: no workflow asks for `packages` permission, so the Actions token cannot write to the package, and this keeps a workflow added on a branch from doing so.
     [Settings the release workflow requires](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/ACTIONS.md#settings-the-release-workflow-requires) explains each list.
  3. Run step 4 again with `RELEASE_REBUILD=1`, because the first images are now in the registry, and without `RELEASE_FIRST_PUSH`.
     This is a full rebuild of every image of the tag, and the images it pushes replace the first ones under the tag before anything is signed.
     So the first release of each network builds twice, and so does the first release that ships the edge services.

## Cut a release

### 1. Prepare and merge the release PR (build server)

```sh
git switch main && git pull --ff-only && git submodule update --init -- release
make release-prep TAG=v0.17.0-adiri
```

`release-prep` checks that the tracked tree is clean, that `HEAD` is `origin/main`, and that the tag exists neither locally nor on GitHub.
It sets `[workspace.package].version` to `0.17.0`, runs `cargo update --workspace`, and generates the new `CHANGELOG.md` section with git-cliff in Docker.
The section records the `main` commit it was generated on in a `<!-- release-base: <sha> -->` line under its heading.
It prints the section and a `Next:` list that starts with these commands:

```sh
git switch -c release/v0.17.0-adiri
git commit -am "release: v0.17.0-adiri"
gh pr create --title "release: v0.17.0-adiri" --body "Version 0.17.0 and the CHANGELOG section for v0.17.0-adiri."
```

Review the section, but do not edit it; `CHANGELOG.md` sections are generated, never written by hand.
Notes for operators, such as a required resync, a configuration change or an activation epoch, go in the GitHub release in step 4.

Attest the PR head with `make attest` as for any PR, then merge it through the merge queue on its own.
The tag checks require the release commit's parent to be the `release-base` commit, so the PR must land alone and before anything else reaches `main`.
If `main` moves before the PR merges, close it and repeat this step on the new `main`.
If the release commit landed in a batch with another PR, repeat this step on the new `main`; `release-prep` replaces the section it wrote earlier, since that section is still the newest one.

When the PR has merged, attest the release commit, which is the commit you will tag:

```sh
git switch main && git pull --ff-only && git submodule update --init -- release
RELEASE_SHA="$(git log -1 --format=%H --grep='^release: v0\.17\.0-adiri (#' origin/main)"
git switch --detach "$RELEASE_SHA" && ALLOW_STALE_BASE=1 make attest
git switch main
```

`RELEASE_SHA` is the newest commit on `main` whose subject is `release: v0.17.0-adiri (#<PR>)`, which is the release commit you just merged; it is the tip of `main` unless another PR has landed since.
The pattern ends at ` (#`, which starts the pull request number of the squash commit, so it matches neither a candidate's commit, `release: v0.17.0-adiri-rc1 (#<PR>)`, nor, for a mainnet tag such as `v0.17.0`, the Adiri commit of the same version, `release: v0.17.0-adiri (#<PR>)`.
`make attest` normally refuses a commit that is behind `origin/main`, because a PR attested on an old base is not what the merge queue merges.
The release commit is already on `main` and is the exact commit that gets tagged and built, so `ALLOW_STALE_BASE=1` is safe here.
If `make attest` reports that the commit is already attested, there is nothing more to do.
Switch back to `main` afterwards, because `release-build` and `release-publish` compare the release scripts in the checkout with `origin/main`.

The new section shows up on the docs site's [Release notes](../getting-started/release-notes.md) as soon as the PR merges.

### 2. Sign and push the tag (laptop)

```sh
git switch main && git pull --ff-only && git submodule update --init -- release
make release-tag TAG=v0.17.0-adiri
```

`release-tag` tags the tip of `origin/main`.
If another PR has landed since the release commit, point it at the release commit instead:

```sh
RELEASE_COMMIT="$(git log -1 -E --format=%H --grep='^release: v0\.17\.0-adiri(-rc[1-9][0-9]*)? \(#' origin/main)" make release-tag TAG=v0.17.0-adiri
```

This pattern also matches a release candidate's commit of the same version and channel, such as `release: v0.17.0-adiri-rc1 (#<PR>)`, and `-1` picks the newest, so it finds the candidate's commit when the final release tags the same commit.
It ends at ` (#` as well, so for a mainnet tag it never matches the Adiri commit of the same version.
The release commit must already be attested, as in step 1.

Before signing, it checks the Cargo version, the `CHANGELOG.md` section and `etc/release-components` on that commit, and the attestation too when `cast` is installed.
It signs with `RELEASE_GPG_KEY` if set and `git config user.signingkey` otherwise, and refuses a key whose primary key is not on the allowlist.
The tag message is `Release v0.17.0-adiri`.
It verifies the new tag the way CI will, shows what it is about to push, the image of each component the tag ships included, and pushes the tag after you type `yes`.
Expect one touch, and a PIN prompt if this is the first signature since the YubiKey was plugged in.

### 3. CI validates the tag and creates the draft

Pushing the tag starts [`release.yaml`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/workflows/release.yaml).
Its `validate-tag` job runs `etc/release.sh check-tag` with the scripts and the allowlist from `main`, and checks that:

- the tag is annotated and carries exactly one signature, made by an allowlisted key;
- the tagged commit is on `main`;
- `Cargo.toml` has the tag's version and the version rules hold;
- `CHANGELOG.md` has the release's section with the right `release-base`;
- the commit is attested on-chain;
- `etc/release-components`, when the commit has it, follows the rules in [Edge services](#edge-services).

The job writes the generated release notes to its summary.
The `draft-release` job then creates a draft release with those notes.
`release-build` refuses a draft that `github-actions[bot]` did not create, which catches a draft made by hand.
This is not a security check, because anyone with write access can edit a draft or create one from a workflow of their own, so the later steps re-check the tag, the assets and the signatures instead of trusting the draft.

```sh
gh run list --workflow release.yaml --limit 3
gh run watch
gh release view v0.17.0-adiri
```

### 4. Build (build server)

```sh
git switch main && git pull --ff-only && git submodule update --init -- release
make release-build TAG=v0.17.0-adiri
```

`release-build` checks the host, the ghcr login, that the release scripts match `origin/main`, every tag check from step 3, and that the draft exists, has no signature yet and holds no file that is not one of the tag's release files.
Before it builds anything, it asks the registry for every image the tag ships: `ghcr.io/telcoin-association/tn-testnet:v0.17.0-adiri`, and `ghcr.io/telcoin-association/tn-worker-gateway:v0.17.0-adiri` and `ghcr.io/telcoin-association/tn-node-record-api:v0.17.0-adiri` when the tag ships the edge services.
It stops when any of them is in the registry already, and names every one; `RELEASE_REBUILD=1` rebuilds them all.
A package that does not exist yet stops it too, at the first release of a network and at the first release that ships the edge services; [One-time setup](#one-time-setup) says what to do then.
It builds every image for `linux/amd64` from a temporary worktree of the tag, without the build cache, the edge services first, so that a defect in one of them stops the build early.
For each image it checks the image's `ENTRYPOINT` and `CMD`, copies the binary out, runs `--version` in the image with the checks of step 6, and packs the binary into its tarball.
Only when every image is built and checked does it push any: the node's to `tn-testnet` (a mainnet tag's to `tn-mainnet`) and each edge service's to its own package, all as `:v0.17.0-adiri`.
A defect in any build or check, an edge service's included, stops `release-build` before it pushes anything and blocks the release of that tag; when the defect is in the tagged tree, follow [Before publish](#before-publish) and release a new commit, whose `etc/release-components` may leave the broken service out.

After the push it writes the digest files and the manifests next to the tarballs in `target/release-artifacts/v0.17.0-adiri/`: `IMAGE_DIGEST` and `SHA256SUMS` for the node, and `IMAGE_DIGEST-worker-gateway`, `IMAGE_DIGEST-node-record-api` and `SHA256SUMS-edge` for the edge services.
`SHA256SUMS` lists `IMAGE_DIGEST`, the node's tarball, and the network's `committee.yaml`, `genesis.yaml` and `parameters.yaml` by their repository path (`chain-configs/testnet/` for an Adiri tag, `chain-configs/mainnet/` otherwise), hashed as committed at the tag.
`SHA256SUMS-edge` lists each edge service's digest file and tarball, and nothing else.
It uploads the tarballs, the digest files and the manifests to the draft in one upload, rewrites the draft's notes to include the image digests, and then runs every check from step 6 except the signature count.
The last two lines it prints are the SHA-256 of each manifest, the node's first:

```text
SHA256SUMS sha256: <hex>
SHA256SUMS-edge sha256: <hex>
```

Write both values down; step 5 asks for both and refuses to sign when a manifest on the draft has another hash.
A tag that ships the node alone has no `SHA256SUMS-edge`, and its last line is `SHA256SUMS sha256: <hex>`.

Because `release-build` rewrites the draft's notes, add the operator notes after it, above the generated notes, and leave the generated notes as they are: `release-publish` refuses a body that does not end with them.

```sh
gh release view v0.17.0-adiri --json body --jq .body > notes.md
# put the operator notes at the top of notes.md
gh release edit v0.17.0-adiri --notes-file notes.md
```

### 5. Sign (laptop)

```sh
make release-sign TAG=v0.17.0-adiri
```

`release-sign` repeats the tag checks and downloads the draft's files into `target/release-artifacts/v0.17.0-adiri/`.
It then runs the file checks of `release-verify` in step 6 before anything is signed: the downloads are the assets GitHub lists, the hashes in each manifest match, the chain-config ones in `SHA256SUMS` against the tagged commit, each digest file matches what its package serves for `:v0.17.0-adiri`, each tarball holds the four expected files, its binary is byte-identical to the one in its image pulled by digest, and that binary's `--version` names the release.
It checks that your key is allowlisted, and that every signature already in `SHA256SUMS.asc` and `SHA256SUMS-edge.asc` is a good one by an allowlisted key.
It signs only the manifests your key has not signed yet, and stops when your key has signed every one.
It shows the tag, the commit, every image, and the content of each manifest and its SHA-256.

On every run it asks for the SHA-256 of each manifest, `SHA256SUMS` first, as `release-build` printed it in step 4, even for a manifest you signed in an earlier run.
It reads them from `RELEASE_SUMS_SHA256` and `RELEASE_EDGE_SUMS_SHA256` when they are set, and otherwise asks at a prompt for each: paste the value or the whole line, or paste the two lines `release-build` printed, together, at the first prompt, which answers both prompts.
It refuses a line pasted at the other manifest's prompt, and stops without signing when a hash differs.
`RELEASE_YES=1` answers only the `yes` prompt that follows, never these, so a run without a terminal needs `RELEASE_SUMS_SHA256`, and `RELEASE_EDGE_SUMS_SHA256` as well when the tag ships the edge services; on a tag that ships the node alone, `RELEASE_EDGE_SUMS_SHA256` is refused.
Right before it signs, it checks once more that each manifest lists the digest files beside it and that each names an image in its own package.
`IMAGE_DIGEST` must name an image in the tag's package, `tn-testnet` here, and it stops without signing when it names any other package, `tn-mainnet` included; each `IMAGE_DIGEST-<service>` must name an image in that service's package.

It signs each manifest with the key `release-tag` would use, and makes every signature before it uploads any, so a run that stops between two signatures has uploaded nothing.
It adds each signature to the manifest's `.asc` file, uploads the files in one upload, then downloads each manifest it signed and its `.asc` again and checks that each `.asc` is the file it uploaded and carries your signature.
If the upload stops part way, run `make release-sign` again: it signs only the manifest that still lacks your signature.
Sign one at a time: two maintainers uploading at once replace each other's file, and `release-sign` stops with an error for the one whose signature was lost.
Expect one touch per manifest it signs, so two for a release that ships the edge services; [PIN and touch during signing](yubikey-setup.md#pin-and-touch-during-signing) covers the prompts and retries.

### 6. Verify and publish (build server)

```sh
make release-verify TAG=v0.17.0-adiri
make release-publish TAG=v0.17.0-adiri
docker logout ghcr.io
```

`release-verify` checks the whole release:

- every tag check from step 3;
- the draft holds exactly the release files of the components the tag ships: the node's tarball, `IMAGE_DIGEST`, `SHA256SUMS` and `SHA256SUMS.asc`, and for a tag that ships the edge services also `SHA256SUMS-edge`, `SHA256SUMS-edge.asc`, and each service's tarball and `IMAGE_DIGEST-<service>`;
- `SHA256SUMS` names exactly `IMAGE_DIGEST`, the node's tarball and the network's three chain-config files, `SHA256SUMS-edge` names exactly each edge service's digest file and tarball, and every hash matches, the chain-config ones against the tagged commit;
- enough distinct allowlisted maintainers signed each manifest: `RELEASE_SIG_THRESHOLD` if set, otherwise `MIN_SIGNATURES`, which is 1, and the threshold holds for `SHA256SUMS` and `SHA256SUMS-edge` separately;
- every signature in `SHA256SUMS.asc` and `SHA256SUMS-edge.asc` is a `GOODSIG` with a `VALIDSIG` by an allowlisted key: one that is bad, unverifiable, expired or revoked, or by a key that is not on the allowlist, refuses the release even next to good ones, which is the rule of the operator check on the install page;
- `IMAGE_DIGEST` names an image in the tag's package, `tn-testnet` for an Adiri tag and `tn-mainnet` for a mainnet tag, each `IMAGE_DIGEST-<service>` names an image in that service's package, each matches what its package serves for the tag, and its manifest lists it before any signature is counted;
- each tarball holds exactly the four expected files in its directory;
- the binary in each tarball is byte-identical to the one in its image pulled by digest;
- `--version` in the node's image shows the version and the tagged commit, and its `Build Features` line is empty for a mainnet tag and, for an Adiri tag, lists `adiri` and at most `faucet` besides, which `adiri` implies;
- `--version` in each edge service's image prints exactly two lines and nothing else, `tn-worker-gateway Version: 0.17.0` (`tn-node-record-api Version: 0.17.0` for the node-record API) and `Commit SHA:` with the full tagged commit, so no `Build Features` line; `release-build` runs the same `--version` checks before it pushes the images.

For the node it prints a line like `verified v0.17.0-adiri commit=... signatures=1/1 image=...`.
For a tag that ships the edge services, one more line per service follows it, like `verified v0.17.0-adiri component=worker-gateway manifest=SHA256SUMS-edge signatures=1/1 image=...`.

`release-publish` runs the same checks, and also checks the ghcr login when it will move the channel aliases, before it asks for `yes`.
While the release is a draft, it checks that the release body ends with the notes generated for the tagged commit and the image digests, line for line, and shows the operator notes above them for you to review.
After you type `yes`, it checks that the draft's files are still the ones it verified, by name, asset id and digest, and that the body has not changed, and publishes the draft as Latest, as a normal release or as a pre-release, following the channel table.
It checks the files and the body once more after publishing, before it moves the aliases.
If the release is the highest published final version in its channel, it moves the channel alias in each package the tag ships to: `:adiri` in `tn-testnet` here, and in `tn-worker-gateway` and `tn-node-record-api` when the tag ships the edge services.
It first checks that every one of those packages still serves the tag at its verified digest, and moves no alias until all of them do.
It then points the edge services' aliases at their digests first and the node's last, each inside its own package, and confirms that the registry agrees, so once the node's alias has moved, every alias has.
It skips work that is already done, so it is safe to run again: when a run stopped after publishing, run `make release-publish` again, and on the release that is now public it keeps the body as it is, without comparing it with the generated notes, and moves the aliases that have not moved yet.
After the `verified` lines it prints the node's `published` line, like `published v0.17.0-adiri image=... alias=adiri`, and one more line per edge service, like `published v0.17.0-adiri component=worker-gateway image=... alias=adiri`.
`docker logout ghcr.io` removes the registry credential from the build server until the next release.

### 7. CI verifies the published release

Publishing with your own `gh` login, as `release-publish` does, starts the `verify-release` job, which runs `etc/release.sh verify` on a GitHub runner with the scripts and the allowlist from `main` and without registry credentials, so it also fails when `IMAGE_DIGEST` names an image outside the tag's package, or an `IMAGE_DIGEST-<service>` one outside that service's package.
The check is detective only: the release is already public when it runs, so a failure means following [After publish](#after-publish).
GitHub starts no workflow for a publish made with a `GITHUB_TOKEN`, so a release that someone publishes from a workflow is never checked by this job.
After every publish, check that a `verify-release` run exists for the tag:

```sh
gh run list --workflow release.yaml --event release --limit 3
```

If none of the runs is for `v0.17.0-adiri`, start one, and use the same command to run the check again later:

```sh
gh workflow run release.yaml -f tag=v0.17.0-adiri -f mode=verify
```

Then follow [Installing a release](../getting-started/installing-a-release.md) on a clean host, and announce the release with a link to [Release notes](../getting-started/release-notes.md).

## Release candidates

A candidate goes through the same seven steps with an `-rcN` tag, for example `v0.17.0-adiri-rc1`.
`release-prep` heads its `CHANGELOG.md` section with the final version, `v0.17.0-adiri`, because candidates have no section of their own.
A later candidate, or the final release, can tag the same commit: start at step 2, setting `RELEASE_COMMIT` if `main` has moved.
If fixes have landed since the candidate, start at step 1 with the new tag; `release-prep` replaces the candidate's section as long as it is still the newest one.
Publishing a candidate never moves an image alias.

## Dry run

`RELEASE_DRY_RUN=1` runs the steps of `etc/release.sh` as a rehearsal: it calls neither `gh` nor `git push`, and it pushes images only to a registry on `localhost` or `127.0.0.1`.
Only a dry run accepts the variables that stand in for the draft release, the registry, `main`, its allowlist and the attestation check, and every other run refuses them: `RELEASE_IMAGE`, `RELEASE_ALLOWLIST_DIR`, `RELEASE_MAIN_REF`, `RELEASE_ATTESTATION=skip` and `RELEASE_DIR`, which the header of `etc/release.sh` describes.
`RELEASE_IMAGE`, a repository such as `127.0.0.1:5000/tn`, stays the node's image, and replaces its whole name, package included.
Each other component's image is the value `$RELEASE_IMAGE-<component>`, here `127.0.0.1:5000/tn-worker-gateway` and `127.0.0.1:5000/tn-node-record-api`, so the rehearsal of a tag that ships the edge services pushes three images to the local registry.
`RELEASE_DIR`, a directory outside `target/release-artifacts`, stands in for the GitHub draft release.

## What the deployment scripts check

The node deployment scripts in [tn-node-deployment](https://github.com/Telcoin-Association/tn-node-deployment) install and update nodes from these releases with no person reading the output.
They take a release only under a tag in the form of the host's network: `vX.Y.Z` on mainnet, and `vX.Y.Z-adiri` or, when the operator names it, `vX.Y.Z-adiri-rcN` on Adiri.
They refuse a mainnet release candidate, `vX.Y.Z-rcN`, before they download anything.
They check every release they take the same way:

1. They download `SHA256SUMS`, `SHA256SUMS.asc` and `IMAGE_DIGEST` from the release, and the host's tarball when they install the binary.
2. They build a keyring from the `*.asc` files in `lib/release-keys/` of the signed script release the host runs, not from this repository, and run `gpgv --status-fd 1 --keyring <keyring> SHA256SUMS.asc SHA256SUMS`.
3. They judge its status lines, and take the release when at least one signature is good and no line refuses it.
   A signature is good when a `GOODSIG` line is followed by a `VALIDSIG` line whose last field, the 40-character fingerprint of the primary key, is one of their keys; one is enough, and they count no further.
   A `BADSIG` line, whatever key made it, an `EXPKEYSIG`, `REVKEYSIG` or `EXPSIG` line, and an `ERRSIG` line for any reason but a missing key each refuse the release, even beside a good signature.
   A `NO_PUBKEY` line, or an `ERRSIG` line for a missing key, is a signature by a key they do not ship, and refuses nothing by itself: beside a good signature they take the release and print a warning that names that key, and without one they refuse the release and name the key.
4. They require `SHA256SUMS` to hold only `<64 hex>  <name>` lines that name exactly `IMAGE_DIGEST`, one node tarball and the network's `committee.yaml`, `genesis.yaml` and `parameters.yaml` under their repository paths, each once.
   The three chain-config lines must carry the hashes that their own script release pins for the network, and they refuse the release when a hash differs or none is pinned.
5. Before anything is installed, they check `IMAGE_DIGEST`, which must be one `<repository>@sha256:<64 hex>` line, and the tarball against their lines in `SHA256SUMS`, and they run the image only from the repository they take for the network.
6. When a host builds the node from source instead, they check the signature on the release tag by the same rule.
   A tag whose signature is bad, expired or revoked stops the build on any network; a tag they cannot verify, such as one signed by a key they do not ship, stops it on mainnet and only prints a warning on testnet.

For maintainers this means:

- The release files keep their names and formats: `SHA256SUMS` in `sha256sum` format, naming exactly `IMAGE_DIGEST`, the node's tarball and the chain-config files under their repository paths, a detached ASCII-armored `SHA256SUMS.asc`, the tarball and `IMAGE_DIGEST`.
  A renamed or reformatted file, or one more line in `SHA256SUMS`, stops every node update.
- A release whose `committee.yaml`, `genesis.yaml` or `parameters.yaml` changed installs only on hosts whose script release pins the new hashes.
  A change to one of those files on `main` therefore needs a signed script release with the new hashes before a node release cut from that commit installs anywhere.
- A mainnet release candidate installs on no host the scripts manage; an operator installs one by hand, as [Installing a release](../getting-started/installing-a-release.md) describes.
- A key signs a node release, or its tag, only once a signed script release ships it, whether it is a primary key that signs, as every key on the allowlist does today, or a subkey with the sign capability.
  Hosts run a signed script release, not the scripts' `main`, and keep an older one until they update with `update-scripts.sh`, so compare `lib/release-keys/` in the latest signed script release with `KEYS.md` before a key signs.
  A host whose script release does not ship the key refuses a node release that only that key signed, and names the key; when a key it ships signed the release too, it takes the release and warns about the other signature.
  A mainnet host that builds from source refuses a tag signed by a key it does not ship.
  A signature by a key that the host's copy shows expired or revoked refuses the release there even beside a good one, as it does here, so a key's new expiry date also has to reach the scripts in a signed script release.
- The scripts tolerate a signature by a key they do not ship, but a release made here never carries one from outside the allowlist: `release-sign`, `release-verify` and `release-publish` refuse any signature in `SHA256SUMS.asc` that is not a good one by an allowlisted key.
  So there is one rule to follow: every signature is a good one by an allowlisted key, and every key that signs ships in a signed script release before it signs.
- The scripts count no signatures on a node release, so one good signature by a key they ship is enough there.
  Raising `MIN_SIGNATURES` here raises what every release carries, not what the scripts check.

### `SHA256SUMS-edge`

The deployment scripts accept only the node's `SHA256SUMS` shape, which is why the edge services' files have a manifest of their own.
`SHA256SUMS` never lists an edge service's file, and `SHA256SUMS-edge` never lists a node file or a chain-config file.

The scripts set up the worker gateway with `setup-gateway.sh`, but not from a release: they read neither `SHA256SUMS-edge` nor `IMAGE_DIGEST-worker-gateway`.
They take the gateway's image as one `<repository>@sha256:<64 hex>` reference and refuse a tag.
On devnet they take an image from the repositories on an allow-list of their own.
On testnet and mainnet they take only the image that their signed script release pins for the network, and both pins are empty, so there they refuse every image.
They pull the image by digest and run its `--help`, and refuse an image whose help lacks a flag that their service unit passes.
The value to pin for a network is the `IMAGE_DIGEST-worker-gateway` line of a signed release of that network, once that release's `SHA256SUMS-edge` and its signatures verify: the scripts check nothing else about the image, so their own signed script release is what carries that trust to hosts.
They do not install `node-record-api`.

A tool that installs an edge service from a release can rely on these rules, which maintainers keep like the node's:

- `SHA256SUMS-edge` and `SHA256SUMS-edge.asc` are on every release whose tagged commit's `etc/release-components` names an edge service, and on no other release.
- `SHA256SUMS-edge` is in `sha256sum` format, `<64 hex>  <name>` lines sorted by name in the C locale, and lists exactly the `IMAGE_DIGEST-<service>` file and the `<service>-<TAG>-x86_64-unknown-linux-gnu.tar.gz` tarball of each service the release ships, by file name.
  For a release of `v0.17.0-adiri` that ships both services it reads:

  ```text
  <hex>  IMAGE_DIGEST-node-record-api
  <hex>  IMAGE_DIGEST-worker-gateway
  <hex>  node-record-api-v0.17.0-adiri-x86_64-unknown-linux-gnu.tar.gz
  <hex>  worker-gateway-v0.17.0-adiri-x86_64-unknown-linux-gnu.tar.gz
  ```

- `SHA256SUMS-edge.asc` holds detached ASCII-armored signatures over `SHA256SUMS-edge`, as `SHA256SUMS.asc` does over `SHA256SUMS`.
  The same allowlist signs both, a release needs as many signatures on `SHA256SUMS-edge` as on `SHA256SUMS`, and every signature in either file is a good one by an allowlisted key.
- `IMAGE_DIGEST-<service>` is one line that names an image in that service's package by digest, `ghcr.io/telcoin-association/tn-worker-gateway@sha256:<64 hex>` or `ghcr.io/telcoin-association/tn-node-record-api@sha256:<64 hex>`, on mainnet and Adiri releases alike.
- Each tarball holds one directory, `<service>-<TAG>-x86_64-unknown-linux-gnu/`, with exactly the binary `<service>` (mode 0755), and `LICENSE-APACHE`, `LICENSE-MIT` and `NOTICE` (mode 0644).
  The binary is byte for byte `/usr/local/bin/<service>` in the image that the digest file names, which is what the image's `ENTRYPOINT` runs.
- The binary's `--version` prints two lines, `tn-<service> Version: X.Y.Z` and `Commit SHA: <commit>`, with the full tagged commit, as in `tn-worker-gateway Version: 0.17.0`.

[Edge services: worker gateway and node-record API](../getting-started/installing-a-release.md#edge-services-worker-gateway-and-node-record-api) on the install page has the checks an operator runs on these files.

## When a step fails

Every step prints `error:` and the reason when it stops.
`etc/release.sh` exits with 1 when a check fails, 2 for a usage error or an unmet precondition, and 3 when GitHub, the registry, the RPC endpoint or a `git` or `docker` network call failed; `make` shows the code as `Error N`.
GitHub API calls, `git fetch` and `git ls-remote`, the fetch of the `release` commit that `main` records, registry lookups, the pull of the git-cliff image and the attestation check are retried before a step gives up with 3.
A request that GitHub refuses, with an HTTP 4xx other than 429, is not retried and exits with 1 or 2, and so does a tag push that `origin` rejects.
`git push`, `docker push`, `docker pull`, `cargo update`, the image build, the `tn-contracts` submodule fetch and the alias move are tried once, and exit with 3 when they fail.
A signature that gpg cannot make, usually because the YubiKey is not plugged in, exits with 2.
After an exit code of 3, run the same command again.
If `cargo update` or the image build fails the same way a second time, the cause is not an outage: the output above it shows it, such as a version conflict or a compile error.
If `release-build` had already pushed the images, the second run stops because `:<TAG>` is in the registry, and names every image that is; rebuild with `RELEASE_REBUILD=1`, which rebuilds every image of the tag, as the table below says.
If `release-sign` had already uploaded your signatures, the second run stops because your key has signed every manifest, and nothing more is needed; if only one of the two signature files reached the draft, the second run signs the manifest that still lacks your signature.

| Step | Symptom | Fix |
| --- | --- | --- |
| 1 | `release-prep` refuses to start | The message names the precondition: a clean tracked tree, `HEAD` at `origin/main`, or a tag that already exists. |
| 1 | `tracked files have local changes; prep needs a clean checkout of main`, and `git status --short` shows ` M release` | The `release` submodule is at another commit than `main` records, usually after a pull that moved the pointer. Run `git submodule update --init -- release`, then `release-prep` again. |
| 1 | `release-prep` stops after it set the version | It restores `Cargo.toml`, `Cargo.lock` and `CHANGELOG.md` from `HEAD` as it exits, so fix the cause and run it again. |
| 1 | `release-prep` finds the version's section lower down in `CHANGELOG.md` | That version was already released; choose the next one. |
| 2, 3 | The `CHANGELOG.md` check fails on `release-base` | If you are tagging a later commit, set `RELEASE_COMMIT` to the release commit. If the release commit did not land alone, repeat step 1 on the new `main`. |
| 2, 3 | The signing key is not on the allowlist | Check `git config --get user.signingkey` and `RELEASE_GPG_KEY`, and that your file at the `release` commit `main` records holds your current signing key (`gpg --show-keys release/keys/<handle>.asc` after `git submodule update --init -- release`). A key merged in `tn-release-signing` can sign only once `main` moves the pointer to it. |
| 2 to 7 | `submodule layout: release is a submodule on refs/remotes/origin/main but is not checked out here; run git submodule update --init -- release` | Run `git submodule update --init -- release`, then the step again. If git answers `pathspec 'release' did not match any file(s) known to git`, the checkout's `HEAD` predates the submodule: run `git switch main && git pull --ff-only` first. |
| 2 to 7 | `main must record the release submodule (tn-release-signing), but refs/remotes/origin/main has no submodule at release; ...` | No step can read a key until `main` records the `release` submodule again; keys are never read from this repository's own tree. |
| 2 to 7 | `submodule layout: MIN_SIGNATURES=<n> in etc/release.sh is below the <N> signatures KEYS.md requires in release at <commit>, ...` | The pointer on `main` names a `KEYS.md` that requires more signatures than `etc/release.sh`. Raise `MIN_SIGNATURES` to at least `<N>` in a PR; it belongs in the PR that moves the pointer, as [Moving the submodule pointer](#moving-the-submodule-pointer) says. |
| 2 to 7 | `submodule layout: release at <commit>, ... has no KEYS.md to read the signature threshold from`, or `submodule layout: KEYS.md in release at <commit>, ... must state the threshold on one line 'Signatures required: N'` | The pointer on `main` names a `tn-release-signing` commit without a usable threshold line. Fix `KEYS.md` there, then move the pointer to the fixed commit. |
| 2 to 7 | `submodule layout: refs/remotes/origin/main records <commit> for release, but neither its checkout nor its origin has that commit` | The pointer names a commit that `tn-release-signing` does not have, such as one never pushed there. Move the pointer to a commit on its `main`. |
| 2 to 7 | `git fetch in release from its origin failed after 3 attempts` | The step needed the `release` commit `main` records and could not fetch it. Run the step again; `git submodule update --init -- release` fetches that commit too. |
| 2, 3 | The commit is not attested | On the build server, run `git switch --detach <commit> && ALLOW_STALE_BASE=1 make attest`, then `git switch main`. If CI failed on it, also run `gh run rerun <run-id>`. |
| 2, 3 | `etc/release-components at <commit> has the malformed line '<line>'; ...`, `... names '<name>', which etc/release.sh does not know (it knows: node worker-gateway node-record-api); ...`, `... names <name> twice; ...`, `... does not name node; every release ships the node; ...` or `... is a <type> of git mode <mode>, not a regular file; ...`, each ending `; correct it on main and tag a new release commit` | The commit's `etc/release-components` breaks the rules in [Edge services](#edge-services), so no tag of that commit can be released. When `release-tag` stops, nothing is tagged yet: correct the file on `main`, repeat step 1 on the new `main`, and tag the new release commit. When CI stops, the tag exists and can never be released: follow [Before publish](#before-publish) and release the next version from a commit whose file is correct. |
| 2 to 7 | `cannot read etc/release-components at <commit>: git cannot read that commit's tree; fetch it again, or check the repository with git fsck`, or `cannot read etc/release-components at <commit> (blob <id>); ...` | The checkout lacks an object of that commit, or holds a damaged one; the file itself may be fine. Run `git fetch origin`, and `git fsck` if that does not help, then the step again. |
| 3 | The tag is wrong | The tag stays: only maintainers can create a `v*` tag, and it must be signed; nobody, maintainers included, can update or delete one. Follow [Before publish](#before-publish) and release the next version. |
| 4, 6 | `run make docker-login` | Run `make docker-login` on the build server. |
| 4 | `RELEASE_FIRST_PUSH names '<name>', which is not a package of <TAG> (its packages: <package>, <package>, <package>)`, or `RELEASE_FIRST_PUSH is <value>, but <TAG> is on the <channel> channel, whose package is <package>` | An entry of `RELEASE_FIRST_PUSH` is not a package the tag pushes to: a typo, an empty entry such as a trailing comma, an edge package on a tag that ships the node alone, or the other network's package. Nothing was asked of the registry or built. Run step 4 again with only the new packages, each one of those the message lists. |
| 4 | `<image> refuses anonymous reads and the docker login does not see <image>:<TAG>: if this is the first release image of the package <package>, run again with RELEASE_FIRST_PUSH=<packages>; otherwise make the package public (maintainer guide, one-time setup)`, or `<image> and <image> refuse anonymous reads, and the docker login sees <TAG> in none of them: ...` | Nothing was built. At the first release of a network, or the first release that ships the edge services, run step 4 again with the `RELEASE_FIRST_PUSH=<packages>` the message names, which lists every new package of the tag, as the [one-time setup](#one-time-setup) says. On any later release the package was made private, or your Docker login cannot read it: make the package public and check `make docker-login`; do not set `RELEASE_FIRST_PUSH`. |
| 4 | `<image> is private, not new, and already holds <image>:<TAG> (<digest>): do the one-time setup first (make the package public and set its access list, as the maintainer guide says), then run make release-build TAG=<TAG> again, with RELEASE_REBUILD=1 if the tag must be rebuilt` | The package refuses anonymous reads but your Docker login sees the tag in it, and nothing was built. Do the [one-time setup](#one-time-setup) step 2 first (make the package public and set its access lists), then run step 4 again, with `RELEASE_REBUILD=1` if the tag must be rebuilt. |
| 4 | `<image>:<TAG> already exists (<digest>); set RELEASE_REBUILD=1 to rebuild it`, or `<image>:<TAG> (<digest>) and <image>:<TAG> (<digest>) already exist; set RELEASE_REBUILD=1 to rebuild them` | An earlier build pushed them, and nothing was built this time. Rebuild with `RELEASE_REBUILD=1 make release-build TAG=<TAG>`, which rebuilds and pushes every image of the tag, not only the ones named; the new images have other digests, because builds are not reproducible. |
| 4 | An edge service's image fails a check, and the message ends `<TAG> cannot be released; correct <Dockerfile> on main and tag a new release commit` or `<TAG> cannot be released; correct <Dockerfile> or the <crate> crate on main and tag a new release commit`, as in `<image>:<TAG> has ENTRYPOINT and CMD <found>; expected ENTRYPOINT ["worker-gateway"] and no CMD; ...` or `<image>:<TAG> reports '<line>' on the first line of --version; expected 'tn-worker-gateway Version: <X.Y.Z>'; ...` | The image that the tag's own Dockerfile and source build is wrong, so every build of the tag fails the same way, and the tag can never be released. Every image is built and checked before the first push, so this run pushed nothing. Follow [Before publish](#before-publish), then release a new commit that corrects the Dockerfile or the crate, or whose `etc/release-components` leaves that service out, so that its release ships without it. |
| 4 | `<image>:<TAG> is pushed, but the package is not public: do its one-time setup (maintainer guide), then run make release-build TAG=<TAG> again with RELEASE_REBUILD=1`, or `<image>:<TAG> and <image>:<TAG> are pushed, but the packages <package> and <package> are not public: ...` | Every image of the tag is pushed, and the packages named refuse anonymous reads. At the first release of a network, or the first release that ships the edge services, do the [one-time setup](#one-time-setup) of each package named, then run step 4 again with `RELEASE_REBUILD=1` and without `RELEASE_FIRST_PUSH`. Otherwise someone made the package private: make it public again, then run step 4 again with `RELEASE_REBUILD=1`. |
| 4 | The release scripts differ from `origin/main` | Run `git switch main && git pull --ff-only`, then the build again. |
| 4 to 7 | `IMAGE_DIGEST ..., but <TAG> is on the ... channel, whose images are published only in ...` | The draft's `IMAGE_DIGEST` names an image in another package, such as the other network's, or is not one well-formed line, so it is not the file `release-build` wrote for this tag. Do not sign or publish; rebuild the draft as for a failed `release-verify` below. If the release is public, follow [After publish](#after-publish). |
| 4 to 7 | `IMAGE_DIGEST-<service> ..., but <service> images are published only in ..., for every channel (expected the one line ...)` | The draft's digest file of that service names an image in another package, such as the node's, or is not one well-formed line, so it is not the file `release-build` wrote for this tag. Act as for `IMAGE_DIGEST` in the row above. An edge image whose `ENTRYPOINT`, `CMD` or `--version` is refused in step 5 or later, with the advice `rebuild the draft with make release-build TAG=<TAG>, or report it if the release is published`, means the same: `release-build` checked every image it pushed, so the draft names another image. |
| 5 | `<manifest> on the draft has sha256 <hex>, but make release-build printed <hex>; do not sign, find out what changed the draft` | Nothing was signed. Every run compares the hash of every manifest, one you signed in an earlier run included, so a mismatch there means the draft changed after your first signature. A hash pasted alone at the other manifest's prompt ends here too, so compare it with the two lines `release-build` printed. Otherwise find out what changed the draft's files before going further. |
| 5 | `you pasted the SHA256SUMS-edge line at the SHA256SUMS prompt; paste the SHA256SUMS line of make release-build, or its hash alone`, or `RELEASE_SUMS_SHA256 holds the SHA256SUMS-edge line of make release-build; set it to the SHA256SUMS sha256 that make release-build printed`, or either of them the other way round | Nothing was signed. Run `release-sign` again and give each manifest its own line: at the first prompt the `SHA256SUMS sha256:` line, or both lines as `release-build` printed them, `SHA256SUMS` first. When `the SHA256SUMS-edge sha256 from make release-build must be 64 hex characters, got ''` follows a paste of both lines, the paste carried Windows line ends; paste each line at its own prompt. |
| 5 | `RELEASE_EDGE_SUMS_SHA256 is set, but <TAG> ships the node alone, so it has no SHA256SUMS-edge to compare it with; unset RELEASE_EDGE_SUMS_SHA256` | The variable is left over from a release that shipped the edge services, and nothing was read or signed. Unset it and run `release-sign` again. |
| 5 | `release-sign` stops on a file check | Nothing was signed; the message names the check, as for `release-verify` in step 6. |
| 5 | `gpg could not sign <manifest> (nothing was uploaded; run make release-sign again): is the YubiKey that holds <fingerprint> plugged in, and does gpg --card-status show it?`, or for a tag that ships the node alone `gpg could not sign SHA256SUMS: is the YubiKey that holds <fingerprint> plugged in, and does gpg --card-status show it?` | gpg could not make a signature, often because a touch was missed. Every signature is made before any is uploaded, so nothing reached the draft, not even a signature made before the failed one. Check `gpg --card-status`, then run `release-sign` again: it signs every manifest that still lacks your signature on the draft, so expect a touch for each again. |
| 5 | PIN or touch errors | See [PIN and touch during signing](yubikey-setup.md#pin-and-touch-during-signing). |
| 5 | `SHA256SUMS for <TAG> is already signed by @<handle>`, or `SHA256SUMS and SHA256SUMS-edge for <TAG> are already signed by @<handle>` | Nothing to do. With a threshold above 1, another maintainer signs next. |
| 5 to 7 | `<manifest>.asc holds a signature that is not a good one by an allowlisted maintainer key, so the file is refused: ...`, for `SHA256SUMS.asc` or `SHA256SUMS-edge.asc` | The reason after the colon names each refused signature. `BADSIG: signature by key <id> does not match <manifest>` means the file holds a signature over another file, such as `SHA256SUMS.asc` uploaded as `SHA256SUMS-edge.asc`: on a draft, delete it with `gh release delete-asset <TAG> <manifest>.asc --yes`, and each maintainer whose signature it held runs `make release-sign` again, which signs only the manifests that lack their signature. For a signature by an expired or revoked key, see [Rotating or revoking a key](#rotating-or-revoking-a-key). If the release is public, follow [After publish](#after-publish). |
| 5 to 7 | `<image>: the registry answered HTTP <code>: package is not public, or has never been pushed (one-time setup after its first push: make it public, as the maintainer guide says)` | Someone made that package private after the build. Make it public again, as step 2 of the [one-time setup](#one-time-setup) says, then run the step again. |
| 6 | `release-verify` fails | Do not publish; the message names the failed check. To rebuild under the same tag, delete the draft with `gh release delete <TAG> --yes`, recreate it with `gh run rerun <run-id>` on the tag's CI run, run step 4 with `RELEASE_REBUILD=1`, then step 5. |
| 6 | `<manifest> carries <n> of the <T> required maintainer signatures` | Each manifest needs the threshold on its own, and this one has fewer signatures, such as `SHA256SUMS-edge` after a `release-sign` run whose upload stopped part way. Each maintainer whose signature it lacks runs `make release-sign`, which signs only the manifests that lack their signature. |
| 6 | `release-publish` stops because the release's files or body changed after they were verified | Someone changed the draft while `release-publish` ran. If the release is still a draft, find out what changed before you run `release-publish` again; if it is public, follow [After publish](#after-publish). |
| 6 | `release-publish` stops because the body of the draft does not end with the generated notes | Someone edited the generated part, or the allowlist or the release scripts on `main` changed since the build. Re-run the tag's CI run with `gh run rerun <run-id>`, which writes the generated notes again, add the operator notes above them, and run `release-publish` again. |
| 6 | `<image>:<TAG> is <digest>, not <digest>; <image>:<alias> moves only to the image this package serves for <TAG>`, or `<image>:<TAG> is missing, not <digest>; ...` | The release is public by now, and no alias has moved: after this run verified the image, someone pushed another one under `:<TAG>` in that package, or deleted it. Only the release maintainer account can write to the package, so find out who did and why before anything else. The signed digest files still name the verified images. Unless the change is explained, follow [After publish](#after-publish). |
| 6 | `release-publish` stops after publishing, at the alias step, such as with `cannot move <image>:<alias> (if docker reported unauthorized or denied, run make docker-login and publish again)` or `<image>:<alias> points at <digest>, expected <digest>` | The edge services' aliases move first and the node's last, so until the node's has moved, some aliases point at this release and the rest at the previous one. Run `make release-publish` again; on the published release it skips publishing and the body comparison, leaves each alias that already points at its digest, and moves the rest. |
| 7 | CI verification fails after publishing | Follow [After publish](#after-publish). |
| any | Unsure where things stand | Check `gh release view <TAG>`, `gh run list --workflow release.yaml` and the files in `target/release-artifacts/<TAG>/`. |

## Rollback and yanking

### Before publish

Until the release is published, the public parts are the tag, the `:<TAG>` images in the packages `release-build` pushed to, and the release commit on `main`, whose `CHANGELOG.md` section is already on the docs site's [Release notes](../getting-started/release-notes.md) page.
The tag stays: only maintainers can create a `v*` tag, and it must be signed; nobody, maintainers included, can update or delete one.
Delete the draft, if CI created one, and the tag's image version in every package `release-build` pushed to, running the `gh` commands on the build server, where the package scopes belong:

```sh
gh release delete v0.17.0-adiri --yes
for PACKAGE in tn-testnet tn-worker-gateway tn-node-record-api; do
  ID=$(GH_TOKEN="$PACKAGES_PAT" gh api --paginate "/orgs/Telcoin-Association/packages/container/$PACKAGE/versions" \
    --jq '.[] | select(.metadata.container.tags | any(. == "v0.17.0-adiri")) | .id')
  if [ -n "$ID" ]; then GH_TOKEN="$PACKAGES_PAT" gh api -X DELETE "/orgs/Telcoin-Association/packages/container/$PACKAGE/versions/$ID"; fi
done
```

The loop lists the node's package, `tn-mainnet` for a mainnet tag, and both edge packages; leave out an edge package the tag does not ship.
A package with no version under the tag, such as one that a push failing part way never reached, is skipped, and so is one that does not exist yet, after `gh` reports it as not found.

The packages API accepts only a classic personal access token with the `read:packages` and `delete:packages` scopes, so put one in `PACKAGES_PAT` for those calls and revoke it afterwards; each package's Versions page on GitHub can delete the version instead.

Then release the next version from step 1, which supersedes the tag: the next release candidate after a withdrawn candidate (`v0.17.0-adiri-rc2` after `v0.17.0-adiri-rc1`, or the final `v0.17.0-adiri`), and the next patch version after a withdrawn final release (`v0.17.1-adiri` after `v0.17.0-adiri`).
The version rules refuse a final release or a release candidate whose version is not above every final release in its channel, so a withdrawn final's version is never released; a withdrawn candidate's version can still be released, by a later candidate or by its final.
Leave `CHANGELOG.md` as it is.
`release-prep` replaces a withdrawn candidate's section while it is the newest one; a withdrawn final keeps its section, and the next section lists the commits after its tag.
Say in the next release's operator notes that the version was withdrawn.

### After publish

Never delete a published tag, release or image, because operators may have verified and pinned them.
Instead:

1. Put a warning at the top of the release notes with `gh release edit <TAG> --notes-file notes.md`.
2. Mark the release as a pre-release with `gh release edit <TAG> --prerelease`.
3. If the channel alias points at it, move the alias back to the previous good release from the build server, after `make docker-login`:

   ```sh
   PREV=vX.Y.Z-adiri
   ALIAS=ghcr.io/telcoin-association/tn-testnet:adiri
   make release-verify TAG="$PREV" &&
   REF=$(curl -fsSL "https://github.com/Telcoin-Association/telcoin-network/releases/download/$PREV/IMAGE_DIGEST") &&
   case "$REF" in "${ALIAS%:*}@sha256:"*) true ;; *) echo "not an image in ${ALIAS%:*}: $REF" >&2; false ;; esac &&
   docker buildx imagetools create --prefer-index=false --tag "$ALIAS" "$REF" &&
   docker buildx imagetools inspect "$ALIAS" --format '{{println .Manifest.Digest}}'
   ```

   Set `PREV` to the previous good release.
   For a mainnet release, set `ALIAS` to `ghcr.io/telcoin-association/tn-mainnet:latest`; the `case` line stops when `$REF` is not in the alias's package, so the alias never points at the other network's image.
   `--prefer-index=false` copies the image manifest as it is, as `release-publish` does, so the last command prints the digest after the `@` in `$REF`.

   If the release shipped the edge services and moved their aliases, move each of them back the same way, with the digest from the earlier release's `IMAGE_DIGEST-<service>`:

   ```sh
   PREV=vX.Y.Z-adiri
   SERVICE=worker-gateway
   ALIAS=ghcr.io/telcoin-association/tn-$SERVICE:adiri
   make release-verify TAG="$PREV" &&
   REF=$(curl -fsSL "https://github.com/Telcoin-Association/telcoin-network/releases/download/$PREV/IMAGE_DIGEST-$SERVICE") &&
   case "$REF" in "${ALIAS%:*}@sha256:"*) true ;; *) echo "not an image in ${ALIAS%:*}: $REF" >&2; false ;; esac &&
   docker buildx imagetools create --prefer-index=false --tag "$ALIAS" "$REF" &&
   docker buildx imagetools inspect "$ALIAS" --format '{{println .Manifest.Digest}}'
   ```

   Run it for each service the release shipped, with `SERVICE` set to `worker-gateway` or `node-record-api`, and with `:latest` in place of `:adiri` for a mainnet release.
   Set `PREV` to the newest earlier good release in the channel that shipped that service, which can be older than the node's `PREV`; `curl` stops with `404` on a release that did not ship it.
   When no earlier release in the channel shipped the service, there is nothing to roll back to: leave its alias where it is, and say in the warning of step 1 that it still points at this release.
4. Ship a fixed patch release through all seven steps; publishing it moves the aliases forward again, an edge service's only when the fixed release ships that service.
5. Tell operators the affected version and the version to roll back to, following the [release and network update process](../getting-started/validator-operations.md#release-and-network-update-process).

## Adding a maintainer or changing the threshold

- The new maintainer follows [YubiKey signing setup](yubikey-setup.md).
- One PR to `tn-release-signing` adds `keys/<handle>.asc` and the maintainer's row in the `KEYS.md` table, following the rules in [Changing this table](https://github.com/Telcoin-Association/tn-release-signing/blob/main/KEYS.md#changing-this-table).
- An existing maintainer reviews it and confirms the fingerprint with the new maintainer over a separate channel, such as a call, and against `https://github.com/<handle>.gpg`.
- Once it merges there, a PR here moves the `release` pointer to that commit, as [Moving the submodule pointer](#moving-the-submodule-pointer) describes.
  The key can sign a release as soon as that PR merges, because every check reads the keys at the pointer on `main`.
- The node deployment scripts carry maintainer keys of their own, so a new key also has to reach them before it signs a node release, as [What the deployment scripts check](#what-the-deployment-scripts-check) says.
  A host trusts the keys in `lib/release-keys/` of the signed script release it runs, so a release installs there only with a good signature by one of those keys; compare the fingerprints in the latest signed script release, not in the scripts' `main`, with `KEYS.md` before a key signs a node release.

`KEYS.md` states the threshold for every project that shares the allowlist, on its line `Signatures required: N`, currently 1.
`MIN_SIGNATURES` in `etc/release.sh`, currently 1, may be higher than that line but never lower: every step that reads the keys reads the line at the commit `main` records for `release`, and stops with exit 1 when `MIN_SIGNATURES` is below it or the line is missing.
`RELEASE_SIG_THRESHOLD` raises it for a single command, and can go neither below `MIN_SIGNATURES` nor above the number of allowlisted maintainers.
To raise it for every release, raise `MIN_SIGNATURES` in a PR that also tells operators on [Installing a release](../getting-started/installing-a-release.md) how many good signatures to expect.
When the `Signatures required` line goes up in `tn-release-signing`, raise `MIN_SIGNATURES` no later than the PR that moves the pointer to that commit, because every step stops with exit 1 from then on until it does.
Signatures count per maintainer handle, so several keys in one maintainer's file count once.
With a threshold above 1, each signer runs `make release-sign` on their own laptop before anyone runs `make release-publish`.

## Rotating or revoking a key

[Expiry, loss and rotation](https://github.com/Telcoin-Association/tn-release-signing/blob/main/docs/yubikey-setup.md#expiry-loss-and-rotation) in the shared YubiKey guide has the commands, and [Rotating or revoking a key](https://github.com/Telcoin-Association/tn-release-signing/blob/main/docs/signing.md#rotating-or-revoking-a-key) says how `keys/` and `KEYS.md` change in `tn-release-signing`.
Every such change reaches this repository only when a PR moves the `release` pointer to a commit that carries it, as [Moving the submodule pointer](#moving-the-submodule-pointer) describes.
After a revocation, move the pointer at once: releases here accept the revoked key until that PR merges.
The deployment scripts carry their own copies of the keys, so a key that is added, replaced or removed also needs a signed script release there, and so does a key whose expiry date is extended.

`release-verify` refuses a signature file that carries a signature from an expired or revoked key, and it reads the keys at the pointer on `main`.
So once a key expires, or the pointer moves past its revocation, CI verification of the older releases it signed fails.
Extend keys before they expire.
After revoking a compromised key, tell operators which releases are no longer trusted.

## Moving the submodule pointer

A change to the keys or the threshold in `tn-release-signing` reaches this repository's releases only when `main` records a commit of it that carries the change.
To move the pointer, on a branch that starts at an up-to-date `main`:

```sh
git submodule update --init -- release &&
git -C release fetch --quiet origin main &&
git -C release checkout --quiet --detach FETCH_HEAD &&
git diff --submodule=log -- release &&
git -C release diff "$(git rev-parse HEAD:release)" HEAD -- keys KEYS.md yubikey-setup.sh &&
git add release
```

The two `diff` commands show the commits the move brings in and their changes to the keys, the key table and the setup script, which is what the reviewer checks.
The reviewer also checks that the new commit is on `main` of `tn-release-signing`, for example with `git -C release fetch --quiet origin main && git -C release merge-base --is-ancestor <commit> origin/main`, which exits 0 only then.
The pointer shows only a commit id, and GitHub serves any commit of the repository by its id, an unmerged pull request's head included, so a pointer to such a commit would bring in keys that never reached `main` there.
Being on that `main` shows that a maintainer with write access merged the commit, not that a second maintainer reviewed it, so the reviewer of the move reads the key changes in the two diffs whatever protection `tn-release-signing` has.
If the new commit raises `Signatures required` above `MIN_SIGNATURES`, raise `MIN_SIGNATURES` in `etc/release.sh` in the same PR.
Commit, and open a PR.
[`CODEOWNERS`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/CODEOWNERS) assigns `/release` and `/.gitmodules` to the maintainers who hold release keys, and the `main` ruleset requires a code owner's approval, so the move needs a key holder's review.
The repository's admin role can bypass that ruleset, so the review is a check on every account but those, as [`.github/ACTIONS.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/ACTIONS.md) says of every path `CODEOWNERS` lists.
Once it merges, every machine that runs a step runs `git submodule update --init -- release` after its next pull of `main`.
A move that changes the keys between `release-build` and `release-publish` also changes the key table in the generated release notes, so `release-publish` refuses the draft's body until the notes are generated again, as the failure table says.
