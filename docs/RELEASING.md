# Releasing telcoin-network

This document describes how a maintainer ships a public release. Operators
who want to *consume* a release should read [INSTALL.md](INSTALL.md) instead.

A release goes through five stages:

1. Bump the workspace version on `main`.
2. Tag the merge commit. CI builds binaries + image, drafts the release.
3. Maintainer 1 countersigns the draft from their laptop.
4. Maintainer 2 countersigns from their laptop.
5. Either maintainer publishes the draft.

The draft never flips to published until both signatures land. This is
two-of-two: one compromised key cannot ship a release.

## Channels

Tag pattern controls the channel:

| Pattern | Channel | Image tags |
|---------|---------|------------|
| `vX.Y.Z`        | mainnet | `:vX.Y.Z`, `:latest` |
| `vX.Y.Z-adiri`  | adiri (testnet) | `:vX.Y.Z-adiri`, `:adiri` |

The two channels coexist on the same commit if you tag twice, but normally
mainnet releases come from a different commit than adiri releases.

## One-time setup per maintainer

Provision a release-signing YubiKey:

```bash
make release-yubikey-init
```

That prints the `ykman` commands. Follow them, then open a PR adding your
exported cert to `.github/release-keys/<handle>.pem` and your fingerprint to
`SECURITY.md`. The *other* maintainer reviews and merges.

Install cosign locally:

```bash
brew install cosign        # macOS
# or grab the latest release binary on Linux
```

## Cutting a release

### 1. Bump the version

The workspace version is **not** bumped automatically. The maintainer cutting
the release opens a PR that updates `[workspace.package].version` in the root
`Cargo.toml`. CHANGELOG entries are auto-generated from conventional commits:

```bash
make release-changelog
```

That regenerates `CHANGELOG.md` between the previous tag and `HEAD`.

### 2. Tag and push

After the version-bump PR is merged into `main`:

```bash
git checkout main
git pull
# mainnet
git tag -s v0.6.0 -m "v0.6.0"
git push origin v0.6.0
# OR adiri
git tag -s v0.6.0-adiri -m "v0.6.0-adiri"
git push origin v0.6.0-adiri
```

Use `-s` to GPG-sign the tag if you have a GPG key configured. Tag protection
on the repo (recommended setting) requires signed tags and forbids
delete/recreate.

The push triggers `.github/workflows/release.yaml`. Watch it run with:

```bash
gh run watch
```

When CI finishes you'll see a **draft** release at
`https://github.com/telcoin-association/telcoin-network/releases` containing:

- `telcoin-network-vX.Y.Z-x86_64-unknown-linux-gnu.tar.gz` (+ `.sha256`)
- `telcoin-network-vX.Y.Z-aarch64-unknown-linux-gnu.tar.gz` (+ `.sha256`)
- `SHA256SUMS`
- A note pointing at the multi-arch image digest on `ghcr.io`
- Two `actions/attest-build-provenance@v2` attestations on Rekor (one per
  tarball, one for the image)

### 3. Countersign

Plug in your YubiKey. Then, from the repo root:

```bash
make release-sign TAG=v0.6.0 SIGNER=<your-handle>
```

The script:

- pulls the tarballs + `SHA256SUMS` from the draft into a tempdir
- prompts for your PIV PIN
- requires a YubiKey touch for **each** signature (one per tarball, one per
  `SHA256SUMS`, one for the image manifest)
- uploads `<asset>.<your-handle>.sig` and `<asset>.<your-handle>.pem` back to
  the draft release

The image signature is stored as an additional cosign layer at
`ghcr.io/telcoin-association/telcoin-network:sha256-<digest>.sig`. Multiple
maintainer signatures coexist there; verifiers separate them by cert.

Tell the other maintainer the draft is ready. They run:

```bash
make release-sign TAG=v0.6.0 SIGNER=<their-handle>
```

### 4. Verify and publish

Either maintainer runs:

```bash
make release-verify TAG=v0.6.0
```

That checks all three things:

1. CI build provenance for both tarballs and the image
2. Both maintainer blob signatures on every tarball + `SHA256SUMS`
3. Both maintainer image signatures on the manifest digest

If any check fails, the script exits non-zero. Do not flip the draft until it
passes.

```bash
make release-publish TAG=v0.6.0
```

That re-runs `release-verify` and only flips the draft to published if it
passes. The release is now public.

## Rotating a key

If a YubiKey is lost, replaced, or suspected of compromise:

1. Provision a new key on a new YubiKey (`make release-yubikey-init`).
2. Open a PR adding `.github/release-keys/<handle>-2.pem` (do **not**
   overwrite the existing file — past releases must remain verifiable).
3. Update `etc/scripts/release-sign.sh` and `release-verify.sh` to point at
   `<handle>-2.pem` for current signing/verification.
4. Update `SECURITY.md` with the new fingerprint and the date the previous
   key is retired.
5. The other maintainer reviews and merges.

## Out of scope

- Windows, macOS, and musl static binary targets — add later if operator
  demand surfaces.
- Pushing to `crates.io` — not part of this pipeline.
- Modifying `make docker-adiri` — the GCP path stays exactly as it was.
- Bumping `[workspace.package].version` automatically — staying mechanical
  here would mask version mistakes.
