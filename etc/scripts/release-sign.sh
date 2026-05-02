#!/usr/bin/env bash
# Countersign a draft GitHub Release with a maintainer's YubiKey.
#
# Downloads the tarballs and SHA256SUMS that CI produced for $TAG, signs each
# with cosign via PKCS#11 against the YubiKey in slot 9c, signs the multi-arch
# image manifest by digest, and uploads the resulting `.sig` + `.pem` files
# back to the draft release. Output filenames are namespaced by $SIGNER so
# multiple maintainers don't collide.
#
# Requires:
#   - cosign (>= 2.x) on $PATH
#   - libykcs11.{so,dylib} present (env override: LIBYKCS11)
#   - gh CLI authenticated as a user with write access to the repo
#   - YubiKey inserted; PIN entered at the prompt; touch confirmed per signature
#
# Usage: ./etc/scripts/release-sign.sh <TAG> <SIGNER>
# Example: ./etc/scripts/release-sign.sh v0.6.0 grantkee

set -euo pipefail

TAG="${1:-}"
SIGNER="${2:-}"
if [[ -z "$TAG" || -z "$SIGNER" ]]; then
  echo "usage: $0 <TAG> <SIGNER>" >&2
  echo "example: $0 v0.6.0 grantkee" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

REPO="telcoin-association/telcoin-network"
REGISTRY="ghcr.io"
IMAGE="${REGISTRY}/${REPO}"

# Locate libykcs11. Default search covers macOS Homebrew + Linux distros.
LIBYKCS11="${LIBYKCS11:-}"
if [[ -z "$LIBYKCS11" ]]; then
  for candidate in \
    /opt/homebrew/lib/libykcs11.dylib \
    /usr/local/lib/libykcs11.dylib \
    /usr/lib/x86_64-linux-gnu/libykcs11.so \
    /usr/lib/aarch64-linux-gnu/libykcs11.so \
    /usr/lib64/libykcs11.so \
    /usr/lib/libykcs11.so; do
    if [[ -f "$candidate" ]]; then
      LIBYKCS11="$candidate"
      break
    fi
  done
fi
if [[ -z "$LIBYKCS11" || ! -f "$LIBYKCS11" ]]; then
  echo "could not locate libykcs11; install yubico-piv-tool or set LIBYKCS11=/path/to/libykcs11.{so,dylib}" >&2
  exit 2
fi

for cmd in cosign gh; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing required command: $cmd" >&2
    exit 2
  fi
done

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
echo ">>> work dir: $WORK"

# Confirm the release exists and is still a draft.
if ! gh release view "$TAG" --repo "$REPO" --json isDraft --jq .isDraft | grep -q true; then
  echo "release $TAG is not a draft (or does not exist) — refusing to sign" >&2
  exit 2
fi

# Pull the tarballs + checksum file CI uploaded.
gh release download "$TAG" \
  --repo "$REPO" \
  --dir "$WORK" \
  --pattern '*.tar.gz' \
  --pattern 'SHA256SUMS'

# PIV slot 9c on the YubiKey (digital-signature key).
COSIGN_KEY="pkcs11:slot-id=0;object=SIGN%20key?module-path=${LIBYKCS11}"

# --- Blob signatures ---
# One .sig + .pem per asset, namespaced by signer so two maintainers can each
# upload without overwriting each other.
for asset in "$WORK"/*.tar.gz "$WORK"/SHA256SUMS; do
  [[ -f "$asset" ]] || continue
  base="$(basename "$asset")"
  echo ">>> signing $base"
  cosign sign-blob \
    --yes \
    --key "$COSIGN_KEY" \
    --tlog-upload=false \
    --output-signature "$WORK/${base}.${SIGNER}.sig" \
    --output-certificate "$WORK/${base}.${SIGNER}.pem" \
    "$asset"
  gh release upload "$TAG" \
    --repo "$REPO" \
    --clobber \
    "$WORK/${base}.${SIGNER}.sig" \
    "$WORK/${base}.${SIGNER}.pem"
done

# --- Image signature ---
# cosign signs the multi-arch manifest by digest. Multiple maintainer
# signatures coexist on the same image; verifiers distinguish by cert.
DIGEST="$(gh release view "$TAG" --repo "$REPO" --json body --jq .body \
  | grep -oE 'sha256:[a-f0-9]{64}' | head -n1)"
if [[ -z "$DIGEST" ]]; then
  echo "could not extract image digest from release body" >&2
  exit 2
fi
echo ">>> signing image ${IMAGE}@${DIGEST}"
cosign sign \
  --yes \
  --key "$COSIGN_KEY" \
  --tlog-upload=false \
  "${IMAGE}@${DIGEST}"

echo ">>> done. Two-of-two: confirm the other maintainer has signed, then run 'make release-publish TAG=$TAG'."
