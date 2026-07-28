#!/usr/bin/env bash
# Run etc/fork-test/patch-genesis.py inside a throwaway container, so the fork-test harness needs
# Docker rather than a host `pip install ruamel.yaml`. patch-genesis.py is unmodified and still
# runs directly under a host python3 that does have ruamel.yaml.
#
# Usage:
#   patch-genesis.sh --check                    # verify Docker is usable (prereq gate)
#   patch-genesis.sh <source.yaml> <target.yaml>  # build image if needed, then patch in place
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE=tn-genesis-patch:local

check_docker() {
    if ! command -v docker >/dev/null 2>&1; then
        echo "docker is required to patch genesis (it runs ruamel.yaml in a container so you do not"
        echo "have to install it on the host): https://docs.docker.com/get-docker/"
        return 1
    fi
    if ! docker info >/dev/null 2>&1; then
        echo "the Docker daemon is not reachable -- start Docker Desktop (or run 'colima start')"
        echo "before ./etc/local-testnet.sh."
        return 1
    fi
}

if [ "${1:-}" = "--check" ]; then
    check_docker
    exit $?
fi

if [ $# -ne 2 ]; then
    echo "usage: $0 <source-genesis.yaml> <target-genesis.yaml>" >&2
    echo "       $0 --check" >&2
    exit 1
fi

check_docker || exit 1

# Map a host path to its repo-relative form, since only $REPO is mounted in the container.
# Both arguments exist at call time (the target is patched in place), so realpath always resolves.
to_repo_rel() {
    local abs
    if ! abs="$(realpath "$1" 2>/dev/null)"; then
        echo "no such file: $1" >&2; exit 1
    fi
    case "$abs" in
        "$REPO"/*) printf '%s' "${abs#"$REPO"/}" ;;
        *) echo "path is outside the repo ($REPO), cannot mount: $1" >&2; exit 1 ;;
    esac
}

SRC_REL="$(to_repo_rel "$1")"
TGT_REL="$(to_repo_rel "$2")"

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "building $IMAGE (one time)"
    docker build -q -t "$IMAGE" -f "$REPO/etc/fork-test/patch.Dockerfile" "$REPO/etc/fork-test"
fi

# --user keeps the in-place rewrite owned by the caller, so later `cp`/`rm -rf local-validators`
# steps in local-testnet.sh do not hit root-owned files. (This is also why ruamel is baked into
# the image at build time -- a non-root user cannot write to site-packages.)
# --network none: the script only touches two local files; no pip fetch can sneak in.
# HOME=/tmp: the mapped uid has no passwd entry or home directory in the image.
docker run --rm --network none \
    --user "$(id -u):$(id -g)" -e HOME=/tmp \
    -v "$REPO:/work" -w /work \
    "$IMAGE" python etc/fork-test/patch-genesis.py "$SRC_REL" "$TGT_REL"
