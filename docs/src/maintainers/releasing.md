# Releasing

A release goes from a release PR to a published GitHub release in seven steps.
CI validates the signed tag and creates a draft release; it never builds anything.
The maintainer builds on the build server, signs on the laptop, and publishes from the build server once every check passes.
Each step is a `make` target that calls [`etc/release.sh`](https://github.com/Telcoin-Association/telcoin-network/blob/main/etc/release.sh).
`release-tag`, `release-sign` and `release-publish` show what they are about to do and wait for you to type `yes`.
The examples use `v0.17.0-adiri`.

## At a glance

| Step | Where | Command | Result |
| --- | --- | --- | --- |
| 1. Release PR | build server | `make release-prep TAG=v0.17.0-adiri`, a PR merged alone, then `ALLOW_STALE_BASE=1 make attest` on the release commit | The attested `release: v0.17.0-adiri` commit on `main` |
| 2. Tag | laptop | `make release-tag TAG=v0.17.0-adiri` | A signed tag on GitHub |
| 3. Validate and draft | CI | runs when the tag is pushed | A draft release with generated notes |
| 4. Build | build server | `make release-build TAG=v0.17.0-adiri` | The image pushed to `tn-testnet` as `:v0.17.0-adiri`; the tarball, `IMAGE_DIGEST` and `SHA256SUMS` on the draft |
| 5. Sign | laptop | `make release-sign TAG=v0.17.0-adiri` | `SHA256SUMS.asc` on the draft |
| 6. Verify and publish | build server | `make release-verify TAG=v0.17.0-adiri`, then `make release-publish TAG=v0.17.0-adiri` | The published release, and the channel alias moved if it is the newest release |
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

`release-build` pushes the image to the tag's package under the tag name.
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
- Docker runs `linux/amd64` images, as Docker Desktop does on any Mac: `release-sign` pulls the release image by digest, compares its binary with the tarball's and runs its `--version`.
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
  The scripts cannot see the ruleset, and the signatures do not depend on it: every step that reads a tag requires exactly one signature on it, made by a key in the allowlist on `main`, and publishing requires at least `MIN_SIGNATURES` maintainer signatures over `SHA256SUMS`.
  The ruleset covers what signatures cannot: GitHub runs `release.yaml` from the tagged commit, so the ruleset decides who can start the workflow for a release tag, and it keeps a tag from being moved or deleted after it was validated.
- Each release package must be public, because the release scripts and operators read it without credentials, and only the release maintainer account may write to it.
  A ghcr package exists only after its first push, and only `release-build` pushes to the release packages, so this setup happens at the first release of each network, `tn-testnet` at the first Adiri tag and `tn-mainnet` at the first mainnet tag:
  1. Run step 4 for that tag.
     Before the push, ghcr answers 403 for the package, as it does for a private one, so `release-build` asks the registry again with your Docker login.
     The login does not see the tag in a package that does not exist yet, and `release-build` then stops and asks for `RELEASE_FIRST_PUSH`: run `RELEASE_FIRST_PUSH=tn-testnet make release-build TAG=<tag>` (`tn-mainnet` for a mainnet tag; the value must be the tag's package, and `RELEASE_YES=1` never stands in for it).
     It pushes the package's first image and then stops with `package is not public`.
  2. Open the package's settings, `https://github.com/orgs/Telcoin-Association/packages/container/tn-testnet/settings` or `.../tn-mainnet/settings`.
     Under "Danger Zone", change the visibility to public.
     Under "Manage access", turn off the access inherited from the repository and give write access to the release maintainer account only.
     Under "Manage Actions access", leave this repository out or give it the Read role: no workflow asks for `packages` permission, so the Actions token cannot write to the package, and this keeps a workflow added on a branch from doing so.
     [Settings the release workflow requires](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/ACTIONS.md#settings-the-release-workflow-requires) explains each list.
  3. Run step 4 again with `RELEASE_REBUILD=1`, because the first image is now in the registry, and without `RELEASE_FIRST_PUSH`.
     This is a full rebuild, so the first release of each network builds twice; the image it pushes replaces the first one under the tag before anything is signed.

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

Before signing, it checks the Cargo version and the `CHANGELOG.md` section on that commit, and the attestation too when `cast` is installed.
It signs with `RELEASE_GPG_KEY` if set and `git config user.signingkey` otherwise, and refuses a key whose primary key is not on the allowlist.
The tag message is `Release v0.17.0-adiri`.
It verifies the new tag the way CI will, shows what it is about to push, and pushes the tag after you type `yes`.
Expect one touch, and a PIN prompt if this is the first signature since the YubiKey was plugged in.

### 3. CI validates the tag and creates the draft

Pushing the tag starts [`release.yaml`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/workflows/release.yaml).
Its `validate-tag` job runs `etc/release.sh check-tag` with the scripts and the allowlist from `main`, and checks that:

- the tag is annotated and carries exactly one signature, made by an allowlisted key;
- the tagged commit is on `main`;
- `Cargo.toml` has the tag's version and the version rules hold;
- `CHANGELOG.md` has the release's section with the right `release-base`;
- the commit is attested on-chain.

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

`release-build` checks the host, the ghcr login, that the release scripts match `origin/main`, every tag check from step 3, that the draft exists and has no signature yet, and that `ghcr.io/telcoin-association/tn-testnet:v0.17.0-adiri` is not in the registry yet.
On the first release of a network, that package does not exist yet; [One-time setup](#one-time-setup) says what `release-build` does then.
It builds the `linux/amd64` image from a temporary worktree of the tag, without the build cache, and copies the binary out of the image.
It runs `--version` in the image as a smoke test, packs the tarball, and pushes the image as `ghcr.io/telcoin-association/tn-testnet:v0.17.0-adiri`; a mainnet tag's image goes to `ghcr.io/telcoin-association/tn-mainnet`.
It writes `IMAGE_DIGEST` and `SHA256SUMS` next to the tarball in `target/release-artifacts/v0.17.0-adiri/`, uploads the three files to the draft, and rewrites the draft's notes to include the image digest.
`SHA256SUMS` lists `IMAGE_DIGEST`, the tarball, and the network's `committee.yaml`, `genesis.yaml` and `parameters.yaml` by their repository path (`chain-configs/testnet/` for an Adiri tag, `chain-configs/mainnet/` otherwise), hashed as committed at the tag.
It then runs every check from step 6 except the signature count.
The last line it prints is `SHA256SUMS sha256: <hex>`.
Write that value down; step 5 asks for it and refuses to sign when the draft's `SHA256SUMS` has another hash.

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
It then runs the file checks of `release-verify` in step 6 before anything is signed: the downloads are the assets GitHub lists, the hashes in `SHA256SUMS` match, the chain-config ones against the tagged commit, `IMAGE_DIGEST` matches what the registry reports for `:v0.17.0-adiri`, the tarball holds the four expected files, its binary is byte-identical to the one in the image pulled by digest, and that binary's `--version` names the release.
It checks that your key is allowlisted and has not signed this release yet, and that every signature already in `SHA256SUMS.asc` is a good one by an allowlisted key, and shows the tag, the commit, the image, the content of `SHA256SUMS` and its SHA-256.
It asks for the SHA-256 that `release-build` printed in step 4 (paste the value or the whole line), or reads it from `RELEASE_SUMS_SHA256`, and stops without signing when it differs.
`RELEASE_YES=1` answers only the `yes` prompt that follows, never this one, so a run without a terminal needs `RELEASE_SUMS_SHA256`.
Right before it signs, it checks once more that `SHA256SUMS` lists the `IMAGE_DIGEST` beside it and that this names an image in the tag's package, `tn-testnet` here, and stops without signing when it names any other package, `tn-mainnet` included.
It signs `SHA256SUMS` with the key `release-tag` would use, adds the signature to `SHA256SUMS.asc`, uploads it, then downloads it again and checks that it is the file it uploaded and that it carries your signature.
Sign one at a time: two maintainers uploading at once replace each other's file, and `release-sign` stops with an error for the one whose signature was lost.
Expect one touch; [PIN and touch during signing](yubikey-setup.md#pin-and-touch-during-signing) covers the prompts and retries.

### 6. Verify and publish (build server)

```sh
make release-verify TAG=v0.17.0-adiri
make release-publish TAG=v0.17.0-adiri
docker logout ghcr.io
```

`release-verify` checks the whole release:

- every tag check from step 3;
- the draft holds exactly the tarball, `IMAGE_DIGEST`, `SHA256SUMS` and `SHA256SUMS.asc`;
- `SHA256SUMS` names exactly `IMAGE_DIGEST`, the tarball and the network's three chain-config files, and every hash matches, the chain-config ones against the tagged commit;
- enough distinct allowlisted maintainers signed: `RELEASE_SIG_THRESHOLD` if set, otherwise `MIN_SIGNATURES`, which is 1;
- every signature in `SHA256SUMS.asc` is a `GOODSIG` with a `VALIDSIG` by an allowlisted key: one that is bad, unverifiable, expired or revoked, or by a key that is not on the allowlist, refuses the release even next to good ones, which is the rule of the operator check on the install page;
- `IMAGE_DIGEST` names an image in the tag's package, `tn-testnet` for an Adiri tag and `tn-mainnet` for a mainnet tag, and matches what that package serves for the tag, and `SHA256SUMS` lists that `IMAGE_DIGEST` before any signature is counted;
- the tarball holds exactly the four expected files in its directory;
- the binary in the tarball is byte-identical to the one in the image pulled by digest;
- `--version` in the image shows the version and the tagged commit, and its `Build Features` line is empty for a mainnet tag and, for an Adiri tag, lists `adiri` and at most `faucet` besides, which `adiri` implies; `release-build` runs the same check before it pushes the image.

It ends with a line like `verified v0.17.0-adiri commit=... signatures=1/1 image=...`.

`release-publish` runs the same checks, and also checks the ghcr login when it will move the channel alias, before it asks for `yes`.
While the release is a draft, it checks that the release body ends with the notes generated for the tagged commit and the image digest, line for line, and shows the operator notes above them for you to review.
After you type `yes`, it checks that the draft's files are still the ones it verified, by name, asset id and digest, and that the body has not changed, and publishes the draft as Latest, as a normal release or as a pre-release, following the channel table.
It checks the files and the body once more after publishing, before it moves the alias.
If the release is the highest published final version in its channel, it checks that the package still serves the tag at the verified digest, points the channel alias in the same package at that digest, and confirms that the registry agrees.
It skips work that is already done, so it is safe to run again: when a run stopped after publishing, run `make release-publish` again, and on the release that is now public it keeps the body as it is, without comparing it with the generated notes, and moves the alias.
`docker logout ghcr.io` removes the registry credential from the build server until the next release.

### 7. CI verifies the published release

Publishing with your own `gh` login, as `release-publish` does, starts the `verify-release` job, which runs `etc/release.sh verify` on a GitHub runner with the scripts and the allowlist from `main` and without registry credentials, so it also fails when `IMAGE_DIGEST` names an image outside the tag's package.
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

## When a step fails

Every step prints `error:` and the reason when it stops.
`etc/release.sh` exits with 1 when a check fails, 2 for a usage error or an unmet precondition, and 3 when GitHub, the registry, the RPC endpoint or a `git` or `docker` network call failed; `make` shows the code as `Error N`.
GitHub API calls, `git fetch` and `git ls-remote`, the fetch of the `release` commit that `main` records, registry lookups, the pull of the git-cliff image and the attestation check are retried before a step gives up with 3.
A request that GitHub refuses, with an HTTP 4xx other than 429, is not retried and exits with 1 or 2, and so does a tag push that `origin` rejects.
`git push`, `docker push`, `docker pull`, `cargo update`, the image build, the `tn-contracts` submodule fetch and the alias move are tried once, and exit with 3 when they fail.
A signature that gpg cannot make, usually because the YubiKey is not plugged in, exits with 2.
After an exit code of 3, run the same command again.
If `cargo update` or the image build fails the same way a second time, the cause is not an outage: the output above it shows it, such as a version conflict or a compile error.
If `release-build` had already pushed the image, the second run stops because `:<TAG>` is in the registry; rebuild with `RELEASE_REBUILD=1` as the table below says.
If `release-sign` had already uploaded the signature, the second run stops because your key has already signed, and nothing more is needed.

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
| 3 | The tag is wrong | The tag stays: only maintainers can create a `v*` tag, and it must be signed; nobody, maintainers included, can update or delete one. Follow [Before publish](#before-publish) and release the next version. |
| 4, 6 | `run make docker-login` | Run `make docker-login` on the build server. |
| 4 | `package is not public` | On a network's first release, after the push: do the [one-time setup](#one-time-setup) and run step 4 again with `RELEASE_REBUILD=1`. Otherwise someone made the package private: make it public again. |
| 4 | `release-build` stops and asks for `RELEASE_FIRST_PUSH` | On the first release of that network, run it again with `RELEASE_FIRST_PUSH=<package>` as the [one-time setup](#one-time-setup) says. On any later release the package was made private, or your Docker login cannot read it: make the package public and check `make docker-login`; do not set `RELEASE_FIRST_PUSH`. |
| 4 | `release-build` stops before the build: the package is private, not new, and already holds `:<TAG>` | The package refuses anonymous reads but your Docker login sees the tag in it. Do the [one-time setup](#one-time-setup) step 2 first (make the package public and set its access lists), then run step 4 again, with `RELEASE_REBUILD=1` if the tag must be rebuilt. |
| 4 to 7 | `IMAGE_DIGEST ..., but <TAG> is on the ... channel, whose images are published only in ...` | The draft's `IMAGE_DIGEST` names an image in another package, such as the other network's, or is not one well-formed line, so it is not the file `release-build` wrote for this tag. Do not sign or publish; rebuild the draft as for a failed `release-verify` below. If the release is public, follow [After publish](#after-publish). |
| 4 | The image `:<TAG>` is already in the registry | An earlier build pushed it. Rebuild with `RELEASE_REBUILD=1 make release-build TAG=<TAG>`; the new image has a different digest, because builds are not reproducible. |
| 4 | The release scripts differ from `origin/main` | Run `git switch main && git pull --ff-only`, then the build again. |
| 5 | `release-sign` stops because the SHA-256 differs from what `release-build` printed | Nothing was signed. Find out what changed the draft's files before going further. |
| 5 | `release-sign` stops on a file check | Nothing was signed; the message names the check, as for `release-verify` in step 6. |
| 5 | PIN or touch errors | See [PIN and touch during signing](yubikey-setup.md#pin-and-touch-during-signing). |
| 5 | Your key has already signed this release | Nothing to do. With a threshold above 1, another maintainer signs next. |
| 6 | `release-verify` fails | Do not publish; the message names the failed check. To rebuild under the same tag, delete the draft with `gh release delete <TAG> --yes`, recreate it with `gh run rerun <run-id>` on the tag's CI run, run step 4 with `RELEASE_REBUILD=1`, then step 5. |
| 6 | `release-publish` stops because the release's files or body changed after they were verified | Someone changed the draft while `release-publish` ran. If the release is still a draft, find out what changed before you run `release-publish` again; if it is public, follow [After publish](#after-publish). |
| 6 | `release-publish` stops because the body of the draft does not end with the generated notes | Someone edited the generated part, or the allowlist or the release scripts on `main` changed since the build. Re-run the tag's CI run with `gh run rerun <run-id>`, which writes the generated notes again, add the operator notes above them, and run `release-publish` again. |
| 6 | `release-publish` stops after publishing, at the alias step | Run `make release-publish` again; on the published release it skips publishing and the body comparison and moves the alias. |
| 7 | CI verification fails after publishing | Follow [After publish](#after-publish). |
| any | Unsure where things stand | Check `gh release view <TAG>`, `gh run list --workflow release.yaml` and the files in `target/release-artifacts/<TAG>/`. |

## Rollback and yanking

### Before publish

Until the release is published, the public parts are the tag, the `:<TAG>` image in the tag's package, and the release commit on `main`, whose `CHANGELOG.md` section is already on the docs site's [Release notes](../getting-started/release-notes.md) page.
The tag stays: only maintainers can create a `v*` tag, and it must be signed; nobody, maintainers included, can update or delete one.
Delete the draft, if CI created one, and the image version, if `release-build` pushed one, running the `gh` commands on the build server, where the package scopes belong:

```sh
gh release delete v0.17.0-adiri --yes
PACKAGE=tn-testnet
ID=$(GH_TOKEN="$PACKAGES_PAT" gh api --paginate "/orgs/Telcoin-Association/packages/container/$PACKAGE/versions" \
  --jq '.[] | select(.metadata.container.tags | any(. == "v0.17.0-adiri")) | .id')
GH_TOKEN="$PACKAGES_PAT" gh api -X DELETE "/orgs/Telcoin-Association/packages/container/$PACKAGE/versions/$ID"
```

Set `PACKAGE` to `tn-mainnet` for a mainnet tag.

The packages API accepts only a classic personal access token with the `read:packages` and `delete:packages` scopes, so put one in `PACKAGES_PAT` for those two calls and revoke it afterwards; the package's Versions page on GitHub can delete the version instead.

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
4. Ship a fixed patch release through all seven steps; publishing it moves the alias forward again.
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
If the new commit raises `Signatures required` above `MIN_SIGNATURES`, raise `MIN_SIGNATURES` in `etc/release.sh` in the same PR.
Commit, and open a PR.
[`CODEOWNERS`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/CODEOWNERS) assigns `/release` and `/.gitmodules` to the maintainers who hold release keys, and the `main` ruleset requires a code owner's approval, so the move needs a key holder's review.
The repository's admin role can bypass that ruleset, so the review is a check on every account but those, as [`.github/ACTIONS.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/ACTIONS.md) says of every path `CODEOWNERS` lists.
Once it merges, every machine that runs a step runs `git submodule update --init -- release` after its next pull of `main`.
A move that changes the keys between `release-build` and `release-publish` also changes the key table in the generated release notes, so `release-publish` refuses the draft's body until the notes are generated again, as the failure table says.
