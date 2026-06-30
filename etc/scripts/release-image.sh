#!/usr/bin/env bash
# Local helper: build & push a multi-arch image to ghcr.io.
#
# CI does this automatically on tag push (see .github/workflows/release.yaml).
# This script is the manual fallback — used when re-pushing an image without
# re-creating the tag, or when iterating on Dockerfile changes from a laptop.
#
# Requires:
#   - docker buildx with a builder that supports linux/amd64 + linux/arm64
#   - `gh auth token` (or `docker login ghcr.io`) for push credentials
#
# Usage: ./etc/scripts/release-image.sh <TAG>

set -euo pipefail

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
  echo "usage: $0 <TAG>" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

REGISTRY="ghcr.io"
IMAGE="telcoin-association/telcoin-network"

case "$TAG" in
  v[0-9]*.[0-9]*.[0-9]*-adiri)
    TAGS=("--tag" "${REGISTRY}/${IMAGE}:${TAG}" "--tag" "${REGISTRY}/${IMAGE}:adiri")
    ;;
  v[0-9]*.[0-9]*.[0-9]*-rc[0-9]*)
    TAGS=("--tag" "${REGISTRY}/${IMAGE}:${TAG}")
    ;;
  v[0-9]*.[0-9]*.[0-9]*)
    TAGS=("--tag" "${REGISTRY}/${IMAGE}:${TAG}" "--tag" "${REGISTRY}/${IMAGE}:latest")
    ;;
  *)
    echo "tag $TAG does not match any release channel pattern (vX.Y.Z, vX.Y.Z-adiri, vX.Y.Z-rcN)" >&2
    exit 1
    ;;
esac

# The default builder uses the `docker` driver which does not support
# --platform linux/amd64,linux/arm64. Require a builder backed by the
# docker-container or kubernetes driver — that is what `make docker-builder`
# creates.
if ! docker buildx ls | awk 'NR>1 && $2 ~ /(docker-container|kubernetes)/ {found=1} END {exit !found}'; then
  echo "no multi-platform-capable buildx builder found — run 'make docker-builder' first." >&2
  exit 2
fi

docker buildx build \
  --file etc/Dockerfile \
  --platform linux/amd64,linux/arm64 \
  --label "org.opencontainers.image.source=https://github.com/${IMAGE}" \
  --label "org.opencontainers.image.revision=$(git rev-parse HEAD)" \
  --label "org.opencontainers.image.version=${TAG}" \
  "${TAGS[@]}" \
  --push \
  .
