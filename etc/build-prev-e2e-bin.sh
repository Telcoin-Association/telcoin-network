#!/bin/bash
#
# Build the e2e node binary of an OLDER commit, for the mixed-binary (rolling upgrade) e2e tests
# that run two node versions side by side (TN_BIN_PATH_PREV). Run through
# `make build-e2e-bin-prev`, which passes every path:
#
#   etc/build-prev-e2e-bin.sh <ref> <source-worktree> <target-root> <binary>
#
#   <ref>              any commit-ish (TN_PREV_REF), resolved in this clone
#   <source-worktree>  the detached worktree the old sources are checked out in (E2E_PREV_SRC)
#   <target-root>      the old build's CARGO_TARGET_DIR (E2E_TARGET_ROOT_PREV)
#   <binary>           where that build leaves the node binary (E2E_BIN_PREV)
#
# The Makefile derives <binary> from <target-root>, so the path checked below is the path to export
# as TN_BIN_PATH_PREV. Every step is idempotent, so the target can run before every mixed-binary
# test run:
#
# 1. Reuse <source-worktree> when it is a worktree of this clone, checking out <ref> when it holds
#    another commit; otherwise add it. `cargo clean` deletes the directory but git keeps its
#    registration. Without --force git refuses to add that path again, and when the parent
#    directory is gone as well git registers the same path a second time, so `git worktree list`
#    shows it twice. Creating the parent first and passing --force replaces the stale entry.
#    A directory there that is NOT a worktree of this clone is refused, never checked out: git run
#    inside a plain directory finds the enclosing checkout and would move that checkout's HEAD.
# 2. Initialize the submodules with --reference to this checkout's tn-contracts repository, so the
#    contract history comes from local objects instead of a second full clone.
# 3. Run the ref's own `make build-e2e-bin` with CARGO_TARGET_DIR=<target-root>: the binary is
#    built with that ref's Makefile, Cargo.lock and .cargo/config.toml. Cargo also reads the
#    .cargo/config.toml of every parent directory, so when <source-worktree> sits inside this
#    checkout, settings in this checkout's file that the ref's file lacks still apply. The file is
#    identical at ba7654d8a and v0.15.0-adiri, the refs this was written for.
#    A rerun with nothing changed still recompiles the CLI and binary crates (about 45 s): the CLI
#    build script watches ../../.git/HEAD, which a worktree does not have (.git is a file there),
#    so cargo reruns it every time and it emits a new build timestamp.
# 4. Fail unless <binary> exists and its `--version` names the commit <ref> resolves to. A ref
#    whose Makefile ignores CARGO_TARGET_DIR would otherwise leave an earlier ref's binary in place
#    and TN_BIN_PATH_PREV would name the wrong node.

set -euo pipefail

if [ "$#" -ne 4 ]; then
    echo "usage: $0 <ref> <source-worktree> <target-root> <binary>" >&2
    exit 2
fi
ref=$1
src=$2
target_root=$3
bin=$4

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() {
    echo "build-e2e-bin-prev: $*" >&2
    exit 1
}

# physical path of an existing directory, so symlinked spellings of one path compare equal
phys() {
    (cd "$1" && pwd -P)
}

sha=$(git -C "$ROOT_DIR" rev-parse --verify --quiet "${ref}^{commit}") ||
    fail "'$ref' does not name a commit in $ROOT_DIR"

# 1. the source worktree
common_dir=$(phys "$(git -C "$ROOT_DIR" rev-parse --path-format=absolute --git-common-dir)")
if [ -e "$src" ]; then
    top=$(git -C "$src" rev-parse --show-toplevel 2>/dev/null) || top=
    src_common=$(git -C "$src" rev-parse --path-format=absolute --git-common-dir 2>/dev/null) ||
        src_common=
    if [ -z "$top" ] || [ -z "$src_common" ] ||
        [ "$(phys "$top")" != "$(phys "$src")" ] || [ "$(phys "$src_common")" != "$common_dir" ]; then
        fail "$src exists but is not a worktree of $ROOT_DIR; remove the directory and rerun"
    fi
    if [ "$(git -C "$src" rev-parse HEAD)" != "$sha" ]; then
        echo "build-e2e-bin-prev: checking out $ref ($sha) in $src"
        git -C "$src" checkout --quiet --detach "$sha"
    fi
else
    echo "build-e2e-bin-prev: adding worktree $src at $ref ($sha)"
    mkdir -p "$(dirname "$src")"
    git -C "$ROOT_DIR" worktree add --quiet --force --detach "$src" "$sha"
fi

# 2. submodules, from local objects when this checkout has them
reference=$(git -C "$ROOT_DIR" rev-parse --path-format=absolute --git-path modules/tn-contracts)
if [ -d "$reference" ]; then
    git -C "$src" submodule update --init --reference "$reference"
else
    git -C "$src" submodule update --init
fi

# 3. the ref's own e2e build, into its own target root
make -C "$src" build-e2e-bin CARGO_TARGET_DIR="$target_root"

# 4. the binary TN_BIN_PATH_PREV will name is the one just built from <ref>
[ -x "$bin" ] || fail "the build of $ref left no binary at $bin"
version=$("$bin" --version) || fail "$bin --version failed"
built=$(printf '%s\n' "$version" | sed -n 's/^Commit SHA: *//p')
[ -n "$built" ] || fail "$bin --version prints no 'Commit SHA:' line"
case "$sha" in
    "$built"*) ;;
    *) fail "$bin reports commit $built, but $ref is $sha" ;;
esac
echo "build-e2e-bin-prev: $bin is $ref ($built)"
