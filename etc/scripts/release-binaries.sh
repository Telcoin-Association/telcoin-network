#!/usr/bin/env bash
# Local parity build for the linux release tarballs.
#
# Mirrors the matrix build in .github/workflows/release.yaml. Useful for
# reproducing CI output without pushing a tag — the tarballs land in dist/.
#
# Usage: ./etc/scripts/release-binaries.sh <TAG>

set -euo pipefail

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
  echo "usage: $0 <TAG>" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

case "$(uname -m)" in
  x86_64|amd64) HOST_TARGET="x86_64-unknown-linux-gnu" ;;
  arm64|aarch64) HOST_TARGET="aarch64-unknown-linux-gnu" ;;
  *)
    echo "unsupported host arch: $(uname -m)" >&2
    exit 2
    ;;
esac

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "release-binaries.sh requires Linux (CI uses ubuntu runners)." >&2
  echo "Run on a linux box, or push the tag and let the workflow build for you." >&2
  exit 2
fi

mkdir -p dist

cargo build --bin telcoin-network --release --target "$HOST_TARGET"

BIN="target/${HOST_TARGET}/release/telcoin-network"
strip "$BIN"

NAME="telcoin-network-${TAG}-${HOST_TARGET}"
tar -C "$(dirname "$BIN")" -czf "dist/${NAME}.tar.gz" "$(basename "$BIN")"
( cd dist && sha256sum "${NAME}.tar.gz" > "${NAME}.tar.gz.sha256" )

echo "wrote dist/${NAME}.tar.gz"
