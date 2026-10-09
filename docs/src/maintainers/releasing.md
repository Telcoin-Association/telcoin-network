# Releasing

A release goes from a release PR to a published GitHub release in seven steps.
CI validates the signed tag and creates a draft release; it never builds anything.
The maintainer builds on xerxes, signs on the laptop, and publishes from xerxes once every check passes.
Each step is a `make` target that calls [`etc/release.sh`](https://github.com/Telcoin-Association/telcoin-network/blob/main/etc/release.sh).
`release-tag`, `release-sign` and `release-publish` show what they are about to do and wait for you to type `yes`.
The examples use `v0.17.0-adiri`.

## At a glance

| Step | Where | Command | Result |
| --- | --- | --- | --- |
| 1. Release PR | xerxes | `make release-prep TAG=v0.17.0-adiri`, a PR merged alone, then `make attest` | The attested `release: v0.17.0-adiri` commit on `main` |
| 2. Tag | laptop | `make release-tag TAG=v0.17.0-adiri` | A signed tag on GitHub |
| 3. Validate and draft | CI | runs when the tag is pushed | A draft release with generated notes |
| 4. Build | xerxes | `make release-build TAG=v0.17.0-adiri` | The image pushed as `:v0.17.0-adiri`; the tarball, `IMAGE_DIGEST` and `SHA256SUMS` on the draft |
| 5. Sign | laptop | `make release-sign TAG=v0.17.0-adiri` | `SHA256SUMS.asc` on the draft |
| 6. Verify and publish | xerxes | `make release-verify TAG=v0.17.0-adiri`, then `make release-publish TAG=v0.17.0-adiri` | The published release, and the channel alias moved if it is the newest release |
| 7. Post-publish check | CI | runs when the release is published | The same verification, repeated on a GitHub runner |

## Release channels

| Tag | Network | Build features | Image tags | GitHub release |
| --- | --- | --- | --- | --- |
| `vX.Y.Z` | mainnet | none | `:vX.Y.Z`, `:latest` | Latest |
| `vX.Y.Z-rcN` | mainnet release candidate | none | `:vX.Y.Z-rcN` only | Pre-release |
| `vX.Y.Z-adiri` | Adiri testnet | `adiri` | `:vX.Y.Z-adiri`, `:adiri` | Pre-release |
| `vX.Y.Z-adiri-rcN` | Adiri release candidate | `adiri` | `:vX.Y.Z-adiri-rcN` only | Pre-release |

`release-build` pushes the image under the tag name.
`release-publish` moves `:latest` or `:adiri` only when the release is the highest published final version in its channel, so publishing an older patch later does not move the alias back.
It marks a release Latest only when it is a mainnet final release with the highest published mainnet version, and publishes everything else as a pre-release.
`etc/release.sh` rejects any tag outside this grammar, so neither CI nor the `make` targets accept one; numbers have no leading zeros, and `N` starts at 1.

The tag checks also enforce these version rules:

- the release commit sets `[workspace.package].version` in `Cargo.toml` to `X.Y.Z`, with no suffix;
- a final version must be higher than every other final version in the same channel;
- no release candidate can be tagged once the final release of the same version exists;
- mainnet and Adiri may use the same `X.Y.Z`;
- `telcoin-network/vX.Y.Z/linux` must fit in the 32-byte block extra-data field.

## Machines and prerequisites

### Laptop (macOS)

- [YubiKey signing setup](yubikey-setup.md) is done, and your key is on the allowlist on `main`.
- `gpg --card-status` shows the YubiKey.
- `git config --get user.signingkey` prints your signing subkey fingerprint followed by `!`; `RELEASE_GPG_KEY` overrides it for one command.
- `gh auth status` shows a login with the `repo` scope, which `release-sign` needs to read the draft and upload to it.
- The repository is cloned and on an up-to-date `main`.
- Foundry's `cast` is optional: with it, `release-tag` and `release-sign` also check the on-chain attestation, and without it they print a notice and leave that check to CI.

### xerxes (Linux x86_64)

- `docker buildx version` works, and `docker buildx inspect default` reports the `docker` driver; `RELEASE_BUILDER` selects another builder with that driver.
- `gh auth status` shows a login with the `repo` scope, to upload to the draft and edit it.
- Docker is logged in to `ghcr.io` through `make docker-login`; [Registry access on xerxes](yubikey-setup.md#registry-access-on-xerxes) covers the first login.
- `cast --version` works, and `.env` holds `GITHUB_ATTESTATION_PRIVATE_KEY` for an address with the MAINTAINER role (see the [CI environment notes](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/ACTIONS.md#environment)).
- `make init-submodules` has been run.
- The checkout is on an up-to-date `main` with a clean tracked tree.
  `release-prep` requires `HEAD` to be `origin/main`.
  `release-build` and `release-publish` refuse to run when `etc/release.sh` or `.github/scripts/verify_commit_hash.sh` differ from `origin/main`.

The YubiKey never goes to xerxes, and the registry token never goes on the laptop.

### One-time setup

- The allowlist on `main` must hold a provisioned key; every check fails on a placeholder file and names it.
- The ghcr package must be public, because the release scripts and operators read it without credentials.
  Push any image to it first (for example with `make docker-adiri`), then set its visibility to public at `https://github.com/orgs/Telcoin-Association/packages/container/telcoin-network/settings`.
  Until then, `release-build` stops with `package is not public`.

## Cut a release

### 1. Prepare and merge the release PR (xerxes)

```sh
git switch main && git pull --ff-only
make release-prep TAG=v0.17.0-adiri
```

`release-prep` checks that the tracked tree is clean, that `HEAD` is `origin/main`, and that the tag exists neither locally nor on GitHub.
It sets `[workspace.package].version` to `0.17.0`, runs `cargo update --workspace`, and generates the new `CHANGELOG.md` section with git-cliff in Docker.
The section records the `main` commit it was generated on in a `<!-- release-base: <sha> -->` line under its heading.
It prints the section, then the commands to run next:

```sh
git switch -c release/v0.17.0-adiri
git commit -am "release: v0.17.0-adiri"
gh pr create --title "release: v0.17.0-adiri"
```

Review the section, but do not edit it; `CHANGELOG.md` sections are generated, never written by hand.
Notes for operators, such as a required resync, a configuration change or an activation epoch, go in the GitHub release in step 4.

Attest the PR head with `make attest` as for any PR, then merge it through the merge queue on its own.
The tag checks require the release commit's parent to be the `release-base` commit, so the PR must land alone and before anything else reaches `main`.
If `main` moves before the PR merges, close it and repeat this step on the new `main`.
If the release commit landed in a batch with another PR, repeat this step on the new `main`; `release-prep` replaces the section it wrote earlier, since that section is still the newest one.

When the PR has merged, attest the commit that landed on `main`, which is the commit you will tag:

```sh
git switch main && git pull --ff-only
make attest
```

The new section shows up on the docs site's [Release notes](../getting-started/release-notes.md) as soon as the PR merges.

### 2. Sign and push the tag (laptop)

```sh
git switch main && git pull --ff-only
make release-tag TAG=v0.17.0-adiri
```

`release-tag` tags the tip of `origin/main`.
If another PR has landed since the release commit, point it at the release commit instead:

```sh
RELEASE_COMMIT="$(git log -1 --format=%H --grep='^release: v0.17.0-adiri' origin/main)" make release-tag TAG=v0.17.0-adiri
```

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
The later steps only accept a draft that `github-actions[bot]` created.

```sh
gh run list --workflow release.yaml --limit 3
gh run watch
gh release view v0.17.0-adiri
```

### 4. Build (xerxes)

```sh
git switch main && git pull --ff-only
make release-build TAG=v0.17.0-adiri
```

`release-build` checks the host, the ghcr login, that the release scripts match `origin/main`, every tag check from step 3, that the draft exists and has no signature yet, and that `:v0.17.0-adiri` is not in the registry yet.
It builds the `linux/amd64` image from a temporary worktree of the tag, without the build cache, and copies the binary out of the image.
It runs `--version` in the image as a smoke test, packs the tarball, and pushes the image as `ghcr.io/telcoin-association/telcoin-network:v0.17.0-adiri`.
It writes `IMAGE_DIGEST` and `SHA256SUMS` next to the tarball in `target/release-artifacts/v0.17.0-adiri/`, uploads the three files to the draft, and rewrites the draft's notes to include the image digest.
It then runs every check from step 6 except the signature count.
The last line it prints is `SHA256SUMS sha256: <hex>`.
Write that value down; step 5 shows it again before you sign.

Because `release-build` rewrites the draft's notes, add the operator notes after it:

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
It checks the hashes in `SHA256SUMS`, checks that `IMAGE_DIGEST` matches what the registry reports for `:v0.17.0-adiri`, and checks that your key is allowlisted and has not signed this release yet.
It then shows the tag, the commit, the content of `SHA256SUMS` and its SHA-256, and waits for `yes`.
Compare that SHA-256 with the value `release-build` printed in step 4 before you type `yes`, and stop if they differ.
It signs `SHA256SUMS` with the key `release-tag` would use, adds the signature to `SHA256SUMS.asc`, uploads it, then downloads it again and counts the signatures.
Expect one touch; [PIN and touch during signing](yubikey-setup.md#pin-and-touch-during-signing) covers the prompts and retries.

### 6. Verify and publish (xerxes)

```sh
make release-verify TAG=v0.17.0-adiri
make release-publish TAG=v0.17.0-adiri
docker logout ghcr.io
```

`release-verify` checks the whole release:

- every tag check from step 3;
- the draft holds exactly the tarball, `IMAGE_DIGEST`, `SHA256SUMS` and `SHA256SUMS.asc`;
- the hashes in `SHA256SUMS` match the files;
- enough distinct allowlisted maintainers signed: `RELEASE_SIG_THRESHOLD` if set, otherwise `MIN_SIGNATURES`, which is 1;
- `IMAGE_DIGEST` matches the registry;
- the tarball holds exactly the four expected files in its directory;
- the binary in the tarball is byte-identical to the one in the image pulled by digest;
- `--version` in the image shows the version, the tagged commit, and the `adiri` feature exactly when the tag is an Adiri tag.

It ends with a line like `verified v0.17.0-adiri commit=... signatures=1/1 image=...`.

`release-publish` runs the same checks, asks for `yes`, and publishes the draft as Latest or as a pre-release, following the channel table.
If the release is the highest published final version in its channel, it points the channel alias at the image digest and confirms that the registry agrees.
It skips work that is already done, so it is safe to run again.
`docker logout ghcr.io` removes the registry credential from xerxes until the next release.

### 7. CI verifies the published release

Publishing starts the `verify-release` job, which runs `etc/release.sh verify` on a GitHub runner with the scripts and the allowlist from `main` and without registry credentials.
The check is detective only: the release is already public when it runs, so a failure means following [After publish](#after-publish).
To run it again later:

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

## When a step fails

Every step prints `error:` and the reason when it stops.
It exits with 1 when a check fails, 2 for a usage error or an unmet precondition, and 3 when GitHub, the registry or the RPC endpoint stayed unreachable after retries.
After an exit code of 3, run the same command again.

| Step | Symptom | Fix |
| --- | --- | --- |
| 1 | `release-prep` refuses to start | The message names the precondition: a clean tracked tree, `HEAD` at `origin/main`, or a tag that already exists. |
| 1 | `release-prep` finds the version's section lower down in `CHANGELOG.md` | That version was already released; choose the next one. |
| 2, 3 | The `CHANGELOG.md` check fails on `release-base` | If you are tagging a later commit, set `RELEASE_COMMIT` to the release commit. If the release commit did not land alone, repeat step 1 on the new `main`. |
| 2, 3 | The signing key is not on the allowlist | Check `git config --get user.signingkey` and `RELEASE_GPG_KEY`, and that your file on `main` holds your current signing subkey (`gpg --show-keys .github/maintainer-gpg-keys/<handle>.asc`). |
| 3 | The commit is not attested | Run `make attest` on that commit on xerxes, then `gh run rerun <run-id>`. |
| 3 | The tag is wrong and has to go | If CI created no draft and nothing was built, delete it with `git push --delete origin <TAG>` and `git tag -d <TAG>`, then tag again. Otherwise follow [Before publish](#before-publish). |
| 4 | `run make docker-login` | Run `make docker-login` on xerxes. |
| 4 | `package is not public` | Do the [one-time setup](#one-time-setup). |
| 4 | The image `:<TAG>` is already in the registry | An earlier build pushed it. Rebuild with `RELEASE_REBUILD=1 make release-build TAG=<TAG>`; the new image has a different digest, because builds are not reproducible. |
| 4 | The release scripts differ from `origin/main` | Run `git switch main && git pull --ff-only`, then the build again. |
| 5 | The SHA-256 differs from what `release-build` printed | Do not sign. Find out what changed the draft's files before going further. |
| 5 | PIN or touch errors | See [PIN and touch during signing](yubikey-setup.md#pin-and-touch-during-signing). |
| 5 | Your key has already signed this release | Nothing to do. With a threshold above 1, another maintainer signs next. |
| 6 | `release-verify` fails | Do not publish; the message names the failed check. To rebuild under the same tag, delete the draft with `gh release delete <TAG> --yes`, recreate it with `gh run rerun <run-id>` on the tag's CI run, run step 4 with `RELEASE_REBUILD=1`, then step 5. |
| 6 | `release-publish` stops at the alias step | Run it again; it skips the steps already done. |
| 7 | CI verification fails after publishing | Follow [After publish](#after-publish). |
| any | Unsure where things stand | Check `gh release view <TAG>`, `gh run list --workflow release.yaml` and the files in `target/release-artifacts/<TAG>/`. |

## Rollback and yanking

### Before publish

Until the release is published, only the tag and the `:<TAG>` image are public.
Delete the draft, the image version and the tag, running the `gh` commands on xerxes, where the package scopes belong:

```sh
gh release delete v0.17.0-adiri --yes
gh auth refresh -h github.com -s read:packages,delete:packages
ID=$(gh api --paginate /orgs/Telcoin-Association/packages/container/telcoin-network/versions \
  --jq '.[] | select(.metadata.container.tags | any(. == "v0.17.0-adiri")) | .id')
gh api -X DELETE "/orgs/Telcoin-Association/packages/container/telcoin-network/versions/$ID"
git push --delete origin v0.17.0-adiri
git tag -d v0.17.0-adiri
```

Run `git tag -d` on every machine that has the tag.
Then cut the next release candidate or patch version; reuse a tag name only if CI never created a draft for it and no image was pushed.

### After publish

Never delete a published tag, release or image, because operators may have verified and pinned them.
Instead:

1. Put a warning at the top of the release notes with `gh release edit <TAG> --notes-file notes.md`.
2. Mark the release as a pre-release with `gh release edit <TAG> --prerelease`.
3. If the channel alias points at it, move the alias back to the previous good release from xerxes, after `make docker-login`:

   ```sh
   PREV=vX.Y.Z-adiri   # the previous good release
   make release-verify TAG="$PREV"
   docker buildx imagetools create --tag ghcr.io/telcoin-association/telcoin-network:adiri \
     "$(curl -fsSL "https://github.com/Telcoin-Association/telcoin-network/releases/download/$PREV/IMAGE_DIGEST")"
   ```

   For a mainnet release, the alias is `:latest`.
4. Ship a fixed patch release through all seven steps; publishing it moves the alias forward again.
5. Tell operators the affected version and the version to roll back to, following the [release and network update process](../getting-started/validator-operations.md#release-and-network-update-process).

## Adding a maintainer or changing the threshold

- The new maintainer follows [YubiKey signing setup](yubikey-setup.md).
- One PR adds `.github/maintainer-gpg-keys/<handle>.asc` and the maintainer's row in the `SECURITY.md` key table.
- An existing maintainer reviews it and confirms the fingerprint with the new maintainer over a separate channel, such as a call, and against `https://github.com/<handle>.gpg`.
- The key can sign as soon as the PR merges, because every check reads the allowlist from `main`.

The threshold is `MIN_SIGNATURES` in `etc/release.sh`, currently 1.
`RELEASE_SIG_THRESHOLD` raises it for a single command, and can go neither below `MIN_SIGNATURES` nor above the number of allowlisted maintainers.
To raise it for every release, change `MIN_SIGNATURES` in a PR that also updates the signatures-required line in `SECURITY.md` and tells operators on [Installing a release](../getting-started/installing-a-release.md) how many good signatures to expect.
Signatures count per maintainer handle, so several keys in one maintainer's file count once.
With a threshold above 1, each signer runs `make release-sign` on their own laptop before anyone runs `make release-publish`.

## Rotating or revoking a key

[Expiry, loss and rotation](yubikey-setup.md#expiry-loss-and-rotation) has the commands.
The allowlist and `SECURITY.md` change like this:

- for an extended expiry, a new signing subkey or a revoked subkey, update `<handle>.asc` in place, and update the serial and date in the `SECURITY.md` row when the YubiKey changes;
- for a revoked primary key, delete `<handle>.asc` and mark the `SECURITY.md` row `revoked YYYY-MM-DD`;
- never add a placeholder file for a new maintainer; the file is added when the key exists.

`release-verify` does not count a signature from an expired or revoked key, and it reads the keys from `main`.
So once a key expires or is revoked, CI verification of the older releases it signed fails.
Extend keys before they expire.
After revoking a compromised key, tell operators which releases are no longer trusted.
