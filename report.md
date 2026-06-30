# Code Review: releases-infra branch

Date: 2026-05-20
Scope: 20 files added/modified vs `main` introducing a public release pipeline (cosign + YubiKey two-of-two countersigning, SLSA provenance, multi-arch tarballs + ghcr.io images), plus hardening of the existing on-chain commit-attestation workflow.
Verification: Phase 3 (findings-verifier) complete — 7 of 9 confirmed actionable, 2 informational pass-through, 0 refuted.

## Summary

The branch lands a well-engineered release pipeline with strong defense-in-depth (CI provenance + two-of-two human countersignatures, certs pinned in-repo at the tag).
The on-chain attestation hardening is solid: strict-mode shell, input validation, full-word equality check.
Remaining gaps are supply-chain and operational, not consensus or crypto.
The most consequential issue is Finding 1 (silent `:latest` overwrite from pre-release tags); Findings 2 + 4 share root cause (sign-time trust placed in attacker-mutable GH artifacts) and should be fixed together.

| # | Title | Severity | Category | Status |
|---|-------|----------|----------|--------|
| 1 | Pre-release/RC tags publish to mainnet `:latest` | High | Bugs / Release Routing | **Confirmed** |
| 2 | Image digest read from mutable release body before signing | Medium | Security / Supply Chain | **Confirmed** |
| 3 | Third-party GitHub Actions pinned to mutable major-version tags | Medium | Security / Supply Chain | **Confirmed** |
| 4 | `release-sign.sh` does not verify CI provenance before signing | Medium | Security / Supply Chain | **Confirmed** |
| 5 | `docker buildx ls` precheck is effectively a no-op | Low | Bugs / DX | **Confirmed** |
| 6 | Placeholder PEMs would produce confusing verification failures | Low | Bugs / Release Readiness | **Confirmed** |
| 7 | Release workflow does not enforce GPG-signed tags | Low | Security / Defense-in-depth | **Confirmed** |
| 8 | `actions/checkout` major-version inconsistency | Informational | Style / Consistency | Pass-through |
| 9 | Maintainer signatures deliberately skip Rekor | Informational | Security / Design Tradeoff | Pass-through |

## Recommended action order

1. **Finding 1 (High)** — tag-routing fix.
Highest impact, smallest diff.
2. **Finding 6 (Low)** — placeholder-PEM guard.
One-line guard; land before the next release tag.
3. **Findings 2 + 4 (Medium, shared root cause)** — sign-time provenance + registry-side digest resolution.
4. **Finding 3 (Medium)** — SHA-pin every third-party action.
Start with `dtolnay/rust-toolchain@stable`.
5. **Finding 5 (Low)** — fix `docker buildx ls` precheck.
6. **Finding 7 (Low)** — either workflow-side `git tag -v` or documented tag-protection.
7. **Finding 8 (Informational)** — align `actions/checkout` versions once SHA-pinning lands.
8. **Finding 9 (Informational)** — confirm and document the Rekor-skip tradeoff in `SECURITY.md`.

## Findings

### 1. Pre-release/RC tags publish to mainnet `:latest`

- **Severity (verified)**: High
- **Category**: Bugs / Release Routing
- **Location**: `.github/workflows/release.yaml:3-7,21-50`
- **Status**: Confirmed
- **Evidence**: The trigger filter `v*.*.*` matches any tag with at least three dot-separated components, including `v0.6.0-rc1`, `v0.6.0-beta`, `v0.6.0-foo`, `v1.2.3.4`.
The meta job's only discriminator is `if [[ "$TAG" == *-adiri ]]` (line 34); anything else falls into the mainnet branch (line 39-43) and emits `${REGISTRY}/${IMAGE_NAME}:latest`.
Consequence: `git push origin v0.6.0-rc1` would build, draft, and replace production `:latest` with the RC image.
`docs/RELEASING.md:21-24` only documents `vX.Y.Z` and `vX.Y.Z-adiri`; the Makefile's local guard is a permissive shell glob and doesn't protect CI.
- **Proposed fix** — apply both:

  (a) Tighten the tag trigger in `.github/workflows/release.yaml:3-7`:

  ```yaml
  on:
    push:
      tags:
        - 'v[0-9]+.[0-9]+.[0-9]+'         # mainnet: strict semver, no suffix
        - 'v[0-9]+.[0-9]+.[0-9]+-adiri'   # adiri (testnet)
        - 'v[0-9]+.[0-9]+.[0-9]+-rc[0-9]+' # OPTIONAL: rc channel, no :latest
  ```

  (b) Replace the meta job conditional at `release.yaml:34-43` with an explicit case that fails on unknown shape:

  ```bash
  case "$TAG" in
    v[0-9]*.[0-9]*.[0-9]*-adiri)
      CHANNEL=adiri
      VERSION="${TAG%-adiri}"; VERSION="${VERSION#v}"
      DOCKER_TAGS="${REGISTRY}/${IMAGE_NAME}:${TAG}"$'\n'"${REGISTRY}/${IMAGE_NAME}:adiri"
      ;;
    v[0-9]*.[0-9]*.[0-9]*-rc[0-9]*)
      CHANNEL=rc
      VERSION="${TAG#v}"
      DOCKER_TAGS="${REGISTRY}/${IMAGE_NAME}:${TAG}"   # never :latest
      ;;
    v[0-9]*.[0-9]*.[0-9]*)
      CHANNEL=mainnet
      VERSION="${TAG#v}"
      DOCKER_TAGS="${REGISTRY}/${IMAGE_NAME}:${TAG}"$'\n'"${REGISTRY}/${IMAGE_NAME}:latest"
      ;;
    *)
      echo "::error::tag $TAG does not match any release channel pattern" >&2
      exit 1
      ;;
  esac
  ```

  Mirror the same case logic in `etc/scripts/release-image.sh:28-32`.

### 2. Image digest read from mutable release body before signing

- **Severity (verified)**: Medium
- **Category**: Security / Supply Chain
- **Location**: `etc/scripts/release-sign.sh:108-109`, `etc/scripts/release-verify.sh:81-82`
- **Status**: Confirmed
- **Evidence**: Both scripts grep the release body for `sha256:[a-f0-9]{64}`.
The body is built in `release.yaml:178-195` from `needs.build-image.outputs.digest` (trusted at write time) but is editable post-creation by anyone with `contents: write` via `PATCH /repos/{owner}/{repo}/releases/{release_id}`.
An attacker with a compromised maintainer token (or via a poisoned third-party action — see Finding 3) can substitute the digest between CI completion and signing; both maintainers would then sign the attacker-chosen digest.
The downstream `gh attestation verify` in `release-verify.sh:88-92` would catch the swap and block publish — but the maintainer countersignatures already exist on the wrong digest, breaking the "two humans approved this exact image" assertion.
- **Proposed fix** — replace the digest-extraction block at `release-sign.sh:108-109` (and the same block at `release-verify.sh:81-82`):

  ```bash
  # Resolve image digest from the registry directly. cosign signs by digest,
  # so locking to the registry's current value eliminates the editable-body
  # attack surface. Then verify the digest is bound to the CI build at this
  # tag before signing.
  DIGEST="$(docker buildx imagetools inspect "${IMAGE}:${TAG}" \
    --format '{{ json .Manifest }}' | jq -r .digest)"
  if [[ -z "$DIGEST" || "$DIGEST" != sha256:* ]]; then
    echo "could not resolve image digest for ${IMAGE}:${TAG} from registry" >&2
    exit 2
  fi
  echo ">>> verifying provenance on ${IMAGE}@${DIGEST} before signing"
  gh attestation verify \
    --owner telcoin-association \
    --repo "$REPO" \
    "oci://${IMAGE}@${DIGEST}"
  ```

  This shares root cause with Finding 4 — fix together.

### 3. Third-party GitHub Actions pinned to mutable major-version tags

- **Severity (verified)**: Medium
- **Category**: Security / Supply Chain
- **Location**: `.github/workflows/release.yaml:65,76,95,100,112,117,120,123,131,146,157,160`
- **Status**: Confirmed
- **Evidence**: All 12 third-party `uses:` references use mutable refs.
The worst case is `dtolnay/rust-toolchain@stable` (line 76) — a branch ref, not even a major-version tag.
The workflow holds `contents: write`, `packages: write`, `id-token: write`, `attestations: write` (release.yaml:9-13) — the exact configuration where SHA-pinning is the SLSA L3 baseline.
`.github/dependabot.yml` already tracks `github-actions` weekly, so SHA-bumping is automated and low-friction.
- **Proposed fix** — SHA-pin every third-party action, preserving the version comment Dependabot recognizes:

  ```yaml
  - uses: actions/checkout@b4ffde65f46336ab88eb53be808477a3936bae11 # v4.1.1
  - uses: dtolnay/rust-toolchain@<SHA>                              # stable @ <date>
  - uses: actions/attest-build-provenance@<SHA>                     # v2.2.0
  - uses: actions/upload-artifact@<SHA>                             # v4.4.0
  - uses: docker/setup-qemu-action@<SHA>                            # v3.2.0
  - uses: docker/setup-buildx-action@<SHA>                          # v3.6.1
  - uses: docker/login-action@<SHA>                                 # v3.3.0
  - uses: docker/build-push-action@<SHA>                            # v6.7.0
  - uses: actions/download-artifact@<SHA>                           # v4.1.8
  ```

  Apply the same to `.github/workflows/maintainer-verify.yaml:22` (`actions/checkout@v5`) and `:23` (`foundry-rs/foundry-toolchain@v1`).
Tools: `pinact` or `gh actions-pin` can do the conversion in one shot.
Prioritize `dtolnay/rust-toolchain@stable` — branch refs are the worst case.

### 4. `release-sign.sh` does not verify CI provenance before signing

- **Severity (verified)**: Medium
- **Category**: Security / Supply Chain
- **Location**: `etc/scripts/release-sign.sh:74-103`
- **Status**: Confirmed
- **Evidence**: `gh release download` (lines 75-79) pulls the assets and the signing loop (lines 87-103) signs them immediately.
There is no `gh attestation verify` between download and sign.
The verify-time check at `release-verify.sh:48-54` runs only after both signatures are attached.
The `isDraft` check at line 69 is mutable by anyone with `contents: write` and doesn't bind tarball contents to the CI build.
A credentialed attacker can swap tarballs on the draft; both maintainers blindly countersign; verify-time check catches the swap and blocks publish — but the countersignatures already exist on artifacts the humans never reviewed.
- **Proposed fix** — insert after `release-sign.sh:79` (before the signing loop):

  ```bash
  # Verify CI build provenance on every downloaded tarball BEFORE signing.
  # Binds the artifacts the maintainer is about to sign to the CI build at
  # the tagged commit. Without this, a credentialed attacker could swap
  # tarballs on the draft between CI completion and countersignature.
  for tarball in "$WORK"/*.tar.gz; do
    [[ -f "$tarball" ]] || continue
    echo ">>> verifying provenance on $(basename "$tarball") before signing"
    gh attestation verify \
      --owner telcoin-association \
      --repo "$REPO" \
      "$tarball"
  done
  ```

  Combined with the Finding 2 fix, the flow becomes: confirm draft → download → verify tarball provenance → resolve image digest from registry → verify image provenance → sign.

### 5. `docker buildx ls` precheck is effectively a no-op

- **Severity (verified)**: Low
- **Category**: Bugs / DX
- **Location**: `etc/scripts/release-image.sh:34-37`
- **Status**: Confirmed
- **Evidence**: `docker buildx ls --format '{{.Name}}' | grep -q .` always matches because every Docker install ships a `default` builder.
The `default` builder uses the `docker` driver, which does not support `--platform linux/amd64,linux/arm64`.
The precheck passes; `docker buildx build --platform linux/amd64,linux/arm64 --push .` (line 39-47) then fails with `ERROR: Multi-platform build is not supported for the docker driver`.
The intended "no docker buildx builder found" error is never seen.
The Makefile's `docker-builder` target creates a `tn-builder` using `docker-container`, which is the driver the precheck should require.
- **Proposed fix** — replace `release-image.sh:34-37`:

  ```bash
  if ! docker buildx ls | awk 'NR>1 && $2 ~ /(docker-container|kubernetes)/ {found=1} END {exit !found}'; then
    echo "no multi-platform-capable buildx builder found — run 'make docker-builder' first." >&2
    exit 2
  fi
  ```

### 6. Placeholder PEMs would produce confusing verification failures

- **Severity (verified)**: Low
- **Category**: Bugs / Release Readiness
- **Location**: `.github/release-keys/grantkee.pem`, `.github/release-keys/sstanfield.pem`
- **Status**: Confirmed
- **Evidence**: Both PEMs contain `-----BEGIN PLACEHOLDER-----` plaintext.
`grep -rn "PLACEHOLDER" etc/scripts/ .github/workflows/` returns no hits — no guard anywhere in the pipeline.
If a release is tagged today: `release.yaml` runs to completion (never reads the PEMs); `release-sign.sh` signs with the YubiKey (never reads committed PEMs); `release-verify.sh:67-77` calls `cosign verify-blob --key <placeholder>` and fails with an opaque OpenSSL parse error.
Operators following `docs/INSTALL.md` see the same opaque error with no "release not yet provisioned" signal.
- **Proposed fix** — two cheap guards:

  (a) Add a guard step at the top of the `meta` job in `release.yaml`:

  ```yaml
  - name: checkout
    uses: actions/checkout@<SHA>
  - name: guard against placeholder release keys
    run: |
      set -euo pipefail
      for f in .github/release-keys/grantkee.pem .github/release-keys/sstanfield.pem; do
        if [[ ! -s "$f" ]]; then
          echo "::error file=$f::Release-key file is missing or empty." >&2
          exit 1
        fi
        if grep -q 'PLACEHOLDER' "$f"; then
          echo "::error file=$f::Release-key file is still a placeholder. Provision a real PEM before tagging a release." >&2
          exit 1
        fi
      done
  ```

  (b) Add an early check in `release-sign.sh` and `release-verify.sh`:

  ```bash
  for signer in "${SIGNERS[@]}"; do
    cert=".github/release-keys/${signer}.pem"
    if [[ ! -s "$cert" ]] || grep -q 'PLACEHOLDER' "$cert"; then
      echo "ERROR: committed cert $cert is a placeholder — this release predates key provisioning." >&2
      exit 1
    fi
  done
  ```

### 7. Release workflow does not enforce GPG-signed tags

- **Severity (verified)**: Low
- **Category**: Security / Defense-in-depth
- **Status**: Confirmed (severity medium-confidence)
- **Location**: `.github/workflows/release.yaml:3-7`
- **Evidence**: Trigger on `v*.*.*` and `v*.*.*-adiri` with no `git tag -v` step anywhere.
`docs/RELEASING.md:77` notes "Tag protection on the repo (recommended setting) requires signed tags" — enforcement delegated to a repo setting that may or may not be configured.
Severity is Low because the two-of-two countersignature is the actual defense; the risk is "attacker with push access pushes an unsigned tag, triggers a draft, social-engineers a countersignature."
- **Proposed fix** — two acceptable approaches:

  **Approach A (encoded in CI)**: Add a `git tag -v` step at the top of the `meta` job that imports an in-repo allowlist of maintainer GPG public keys (`.github/maintainer-gpg-keys/<handle>.asc`) and rejects any tag without a "Good signature" from one of them.
Requires committing maintainer public keys.

  **Approach B (documentation + audit)**: Add to `SECURITY.md`: "Tag-protection rule on `v*` is required; tag pushes by anyone without a configured signing key MUST be rejected at the API."
Do a one-time audit confirming the setting is enabled.
Cheaper, but enforcement lives outside the repo.

  Approach A makes the policy auditable from the repo.

### 8. `actions/checkout` major-version inconsistency

- **Severity (verified)**: Informational
- **Category**: Style / Consistency
- **Status**: Pass-through
- **Location**: `.github/workflows/release.yaml:65,112,157` (`@v4`); `.github/workflows/maintainer-verify.yaml:22` (`@v5`)
- **Proposed fix**: Once Finding 3's SHA-pinning lands, align both workflows to the same SHA.

### 9. Maintainer signatures deliberately skip Rekor (`--tlog-upload=false`)

- **Severity (verified)**: Informational
- **Category**: Security / Design Tradeoff
- **Status**: Pass-through
- **Location**: `etc/scripts/release-sign.sh:94,118`, `etc/scripts/release-verify.sh:75,99`
- **Evidence**: All `cosign` invocations use `--tlog-upload=false` / `--insecure-ignore-tlog`.
Documented as deliberate in `docs/INSTALL.md:75-77`.
Tradeoff: simpler operator UX vs. no public log of maintainer signing events.
- **Proposed fix**: Confirm this is a conscious choice and document the rationale in `SECURITY.md` so future maintainers don't change it by accident.
