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

if [[ "$TAG" == *-adiri ]]; then
  TAGS=("--tag" "${REGISTRY}/${IMAGE}:${TAG}" "--tag" "${REGISTRY}/${IMAGE}:adiri")
else
  TAGS=("--tag" "${REGISTRY}/${IMAGE}:${TAG}" "--tag" "${REGISTRY}/${IMAGE}:latest")
fi

if ! docker buildx ls --format '{{.Name}}' | grep -q .; then
  echo "no docker buildx builder found — run 'make docker-builder' first." >&2
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
