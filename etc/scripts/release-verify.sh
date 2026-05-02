#!/usr/bin/env bash
# Verify provenance + both maintainer countersignatures on a release.
#
# Three checks:
#   1. CI build provenance (gh attestation verify)        — bound to workflow + commit
#   2. Both maintainers' blob signatures (cosign verify-blob) — tarballs + SHA256SUMS
#   3. Both maintainers' image signatures (cosign verify) — multi-arch manifest digest
#
# `make release-publish` calls this as a pre-flight; operators run it from
# docs/INSTALL.md after downloading a release.
#
# Usage: ./etc/scripts/release-verify.sh <TAG>

set -euo pipefail

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
  echo "usage: $0 <TAG>" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

REPO="telcoin-association/telcoin-network"
REGISTRY="ghcr.io"
IMAGE="${REGISTRY}/${REPO}"
SIGNERS=(grantkee sstanfield)

for cmd in cosign gh; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing required command: $cmd" >&2
    exit 2
  fi
done

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
echo ">>> verifying release $TAG"

# Pull every asset attached to the release (tarballs, sigs, certs, sums).
gh release download "$TAG" \
  --repo "$REPO" \
  --dir "$WORK" \
  --pattern '*'

# 1) CI build provenance for each tarball.
for tarball in "$WORK"/*.tar.gz; do
  echo ">>> [provenance] $(basename "$tarball")"
  gh attestation verify \
    --owner telcoin-association \
    --repo "$REPO" \
    "$tarball"
done

# 2) Maintainer blob signatures.
for asset in "$WORK"/*.tar.gz "$WORK"/SHA256SUMS; do
  [[ -f "$asset" ]] || continue
  base="$(basename "$asset")"
  for signer in "${SIGNERS[@]}"; do
    sig="$WORK/${base}.${signer}.sig"
    cert=".github/release-keys/${signer}.pem"
    if [[ ! -f "$sig" ]]; then
      echo "missing signature: $(basename "$sig")" >&2
      exit 1
    fi
    if [[ ! -f "$cert" ]]; then
      echo "missing committed cert: $cert" >&2
      exit 1
    fi
    echo ">>> [blob] $base — $signer"
    cosign verify-blob \
      --key "$cert" \
      --signature "$sig" \
      --insecure-ignore-tlog \
      "$asset"
  done
done

# 3) Image signatures by digest.
DIGEST="$(gh release view "$TAG" --repo "$REPO" --json body --jq .body \
  | grep -oE 'sha256:[a-f0-9]{64}' | head -n1)"
if [[ -z "$DIGEST" ]]; then
  echo "could not extract image digest from release body" >&2
  exit 1
fi

echo ">>> [image provenance] ${IMAGE}@${DIGEST}"
gh attestation verify \
  --owner telcoin-association \
  --repo "$REPO" \
  "oci://${IMAGE}@${DIGEST}"

for signer in "${SIGNERS[@]}"; do
  cert=".github/release-keys/${signer}.pem"
  echo ">>> [image] ${IMAGE}@${DIGEST} — $signer"
  cosign verify \
    --key "$cert" \
    --insecure-ignore-tlog \
    "${IMAGE}@${DIGEST}"
done

echo ">>> all checks passed for $TAG"
