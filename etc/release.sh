#!/usr/bin/env bash
#
# Release pipeline for telcoin-network. CI, the build server and the maintainer laptop
# all run this one file, so the tag grammar, the maintainer allowlist, the signature counter
# and the registry lookup exist exactly once. Maintainer guide:
# docs/src/maintainers/releasing.md.
#
# Usage: etc/release.sh <subcommand> <TAG> [options]   (or: make release-<subcommand> TAG=...)
#
# Flow:
#   1. prep TAG        server  set [workspace.package].version to X.Y.Z, cargo update, splice
#                              the git-cliff section into CHANGELOG.md; open the PR
#                              "release: TAG", merge it alone through the queue, `make attest`
#   2. tag TAG         laptop  sign TAG on the release commit with an allowlisted key, push it
#   3. check-tag TAG   CI      validate the pushed tag; `notes` renders the release body and
#                              `draft` creates the draft release
#   4. build TAG       server  build each component's image and tarball from the tag, push the
#                              images, upload the IMAGE_DIGEST files, the manifests (SHA256SUMS,
#                              and SHA256SUMS-edge when the tag ships more than the node) and
#                              the tarballs to the draft
#   5. sign TAG        laptop  re-check the draft's files as verify does, compare each manifest's
#                              hash with the one build printed, append a detached signature over
#                              each manifest to its signature file (SHA256SUMS.asc,
#                              SHA256SUMS-edge.asc)
#   6. verify TAG      server  re-check tag, assets, signatures, images and binaries, then
#      publish TAG             check the release body, publish the draft and move the
#                              channel alias of each image, the node's last
#   7. verify TAG      CI      re-check the published release (detective only)
#
# Subcommands: parse, check-tag, notes [--image-digest sha256:HEX], draft, prep, tag, build,
# sign, verify [--allow-unsigned], publish. `help` or no arguments prints the usage.
#
# Exit codes:
#   0  ok
#   1  a policy or verification check failed
#   2  usage or precondition: bad TAG, missing tool, wrong host, missing draft, a release
#      submodule that is not checked out, or a prompt with no TTY and no RELEASE_YES=1
#   3  an external service (adiri RPC, GitHub API, registry) stayed unavailable after retries
#
# Components: a release is a set of components, each with its own image, tarball and image
# digest file: node, worker-gateway and node-record-api. The file etc/release-components at the
# tagged commit lists the ones the tag ships, one name per line; a tag without it ships the
# node alone. It is the one file of the tagged tree that steers this script, and it only picks
# which of the artifact sets this script knows the release must hold. Every step that builds,
# signs, verifies or publishes reads it after the tag's signature and attestation checks pass,
# and every check still comes from main. notes and draft read it once the tag resolves to a
# commit, with no other check, but only render the release body's text from it; the release
# workflow runs them after its own tag check.
# SHA256SUMS lists the node's files (IMAGE_DIGEST, its tarball, the chain configs) and is signed
# in SHA256SUMS.asc. A second manifest, SHA256SUMS-edge, lists the other components' tarballs
# and IMAGE_DIGEST-<component> files and is signed in SHA256SUMS-edge.asc with the same keys;
# each manifest needs the full threshold of signatures on its own.
#
# Images: the node's image goes to one ghcr package per network, chosen by the tag alone. A
# mainnet tag (vX.Y.Z or vX.Y.Z-rcN) is pushed to ghcr.io/<owner>/tn-mainnet and an adiri tag
# (vX.Y.Z-adiri or vX.Y.Z-adiri-rcN) to ghcr.io/<owner>/tn-testnet, where <owner> is the owner of
# the GitHub repository in lower case. `parse` prints the package as PACKAGE. worker-gateway and
# node-record-api images go to ghcr.io/<owner>/tn-worker-gateway and
# ghcr.io/<owner>/tn-node-record-api, which every channel shares, so an image's tag says which
# release it belongs to. build, sign, verify, publish and draft refuse an image digest file
# that names an image outside its component's package, and the channel alias (latest, adiri)
# moves only inside each package. A package comes into existence with its first push, which
# build makes only for the packages RELEASE_FIRST_PUSH names.
#
# Maintainer keys: the allowlist (release/keys/*.asc) and the signature threshold (the line
# "Signatures required: N" of release/KEYS.md) come from the tn-release-signing submodule at
# release/, at the commit MAIN_REF records for it. They are read from the submodule's object
# store, which fetches that commit from the submodule's origin when it lacks it, never from the
# checked-out files, the tag or this repository's own tree; a MAIN_REF without the submodule
# stops every step that needs a key. A tag therefore cannot introduce a key: the pointer is
# read from main, and moving it needs a code owner's review. MIN_SIGNATURES may exceed the
# threshold KEYS.md states but not fall below it (exit 1). Each checkout needs
# `git submodule update --init -- release` after pulling main.
#
# Environment (all optional):
#   RELEASE_REPO                   owner/repo of the GitHub repository; default
#                                  Telcoin-Association/telcoin-network, GITHUB_REPOSITORY in
#                                  CI. Of the image name it sets only the owner
#   RELEASE_COMMIT                 tag: commit to tag instead of the origin/main tip
#   RELEASE_GPG_KEY                tag, sign: signing key; default `git config user.signingkey`
#   RELEASE_SIG_THRESHOLD          maintainer signatures required; never below MIN_SIGNATURES
#   RELEASE_BUILDER                build: buildx builder, default "default" (driver docker)
#   RELEASE_REBUILD=1              build: rebuild a tag the registry already has, every image
#                                  of it, not only the ones it lacks
#   RELEASE_FIRST_PUSH=PACKAGE[,PACKAGE...]
#                                  build: push the first image of each package named that
#                                  refuses anonymous reads and whose TAG the docker login cannot
#                                  see either; every entry must be the package of a component
#                                  the tag ships, and RELEASE_YES never stands in for it
#   RELEASE_ALLOW_LOCAL_SCRIPTS=1  build, publish: allow this script or verify_commit_hash.sh
#                                  to differ from main
#   RELEASE_SUMS_SHA256            sign: the SHA256SUMS sha256 that build printed; asked for at
#                                  a prompt when unset
#   RELEASE_EDGE_SUMS_SHA256       sign: the SHA256SUMS-edge sha256 that build printed; asked
#                                  for at a prompt when unset, and refused on a tag that ships
#                                  the node alone
#   RELEASE_YES=1                  answer yes to every confirmation prompt; it answers neither
#                                  hash prompt (RELEASE_SUMS_SHA256, RELEASE_EDGE_SUMS_SHA256)
#   GH_TOKEN                       read by gh (CI); locally `gh auth login` is used
#
# Dry run, for rehearsals (RELEASE_DRY_RUN=1). The variables below are refused without it.
# A dry run never calls gh or git push and pushes images only to RELEASE_IMAGE and the names
# derived from it.
#   RELEASE_IMAGE                  image repository on localhost:PORT/ or 127.0.0.1:PORT/; it
#                                  replaces the node image's whole name, package included, and
#                                  each other component's image is the value with
#                                  -<component> appended (RELEASE_IMAGE-worker-gateway)
#   RELEASE_ALLOWLIST_DIR          directory of *.asc used instead of main's allowlist; needs
#                                  no submodule, and KEYS.md's threshold is not read
#   RELEASE_MAIN_REF               ref used instead of refs/remotes/origin/main; no fetch
#   RELEASE_ATTESTATION=skip       skip the on-chain attestation check
#   RELEASE_DIR                    directory that stands in for the GitHub draft release
#
# Portable to macOS's bash 3.2: no associative arrays, mapfile, ${x,,}, sed -i or GNU date.

set -euo pipefail
# Keeps -e inside $(...) on bash >= 4.4. bash 3.2 has no such option, so every function whose
# output is captured checks its own failures and calls die.
shopt -s inherit_errexit 2>/dev/null || true

# ---------------------------------------------------------------------------------------------
# Constants

DEFAULT_REPO=Telcoin-Association/telcoin-network
TRIPLE=x86_64-unknown-linux-gnu
MIN_SIGNATURES=1
MAIN_REF=refs/remotes/origin/main
# The tn-release-signing submodule and the allowlist inside it, the values the shared
# release/release.sh takes as SELF_REPO_PATH and KEYS_PATH in its submodule layout.
SIGNING_PATH=release
ALLOWLIST_PATH=$SIGNING_PATH/keys
GIT_CLIFF_IMAGE=ghcr.io/orhun/git-cliff/git-cliff:2.10.1@sha256:6ba0d1fcb051bd7b154cfb19c4b2b3bfa2c22c475f5285fc30606777b6573119
ARTIFACT_ROOT=target/release-artifacts
# The file at the tagged commit that names the components the tag ships (load_components).
COMPONENTS_FILE=etc/release-components
# Every component this script can release, in release order. Never remove a name: an older tag
# whose COMPONENTS_FILE lists it must keep verifying.
KNOWN_COMPONENTS='node worker-gateway node-record-api'
# The signed manifest of every component but the node.
EDGE_SUMS=SHA256SUMS-edge

TAG_RE='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-adiri)?(-rc[1-9][0-9]*)?$'
DIGEST_RE='^sha256:[0-9a-f]{64}$'
HANDLE_FILE_RE='^[A-Za-z0-9-]+\.asc$'
HEX64_RE='^[0-9a-f]{64}$'
COMPONENT_RE='^[a-z][a-z0-9-]*$'
REPO_RE='^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'
LOCAL_HOST_RE='^(localhost|127\.0\.0\.1):[0-9]+$'
LOCAL_PATH_RE='^[a-z0-9._/-]+$'
BOT_LOGIN='github-actions[bot]'
ATTEST_SCRIPT=.github/scripts/verify_commit_hash.sh
ANCHOR='<!-- ANCHOR: releases -->'
MANIFEST_TYPES='application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json'

# State. parse_tag sets TAG VERSION CHANNEL PACKAGE RC BASE FEATURES PRERELEASE ALIAS TARBALL
# CHAIN_CONFIGS; the functions named in the comments own the rest.
TAG='' VERSION='' CHANNEL='' PACKAGE='' RC='' BASE='' FEATURES='' PRERELEASE='' ALIAS='' TARBALL='' CHAIN_CONFIGS=''
WORK=            # setup: private temp dir, removed on exit
ORIG_PWD=        # setup: directory the script was started from
SELF=            # setup: absolute path of this script
DRY_RUN=0        # init_env
REPO=            # init_env: owner/repo on GitHub
OWNER=           # init_env: the owner of REPO in lower case, the ghcr namespace of the images
IMAGE=           # init_env: ghcr.io/<owner>/PACKAGE, or RELEASE_IMAGE in a dry run; no tag
COMPONENTS=node  # load_components: the components the release ships, in release order
MANIFESTS=SHA256SUMS # load_components: the signed manifests, each signed in <manifest>.asc
DIGESTS=         # record_digest: "component<TAB>sha256:<hex>" for each image digest known
THRESHOLD=$MIN_SIGNATURES
ALLOW_GNUPGHOME= # load_allowlist: keyring holding only main's allowlisted keys
ALLOW_LIST=      # load_allowlist: "handle<TAB>primary fingerprint" lines
COMMIT=          # check_tag: the commit TAG points at
SIGNER=          # check_tag_signature: handle that signed TAG
SIGNER_FPR=      # check_tag_signature: primary fingerprint that signed TAG
SIGN_KEY=        # resolve_signing_key: key spec handed to gpg / git tag -u
SIGN_FPR=        # resolve_signing_key: its primary fingerprint
SIGN_HANDLE=     # resolve_signing_key: its allowlist handle
D=               # check_image_digest_file: sha256:<hex> of IMAGE:TAG, the node's C_D
# component_set: one component's row of the table, valid until the next component_set.
C_NAME='' C_PACKAGE='' C_IMAGE='' C_DOCKERFILE='' C_BIN='' C_TAR_BIN='' C_TARDIR='' C_TARBALL=''
C_DIGEST_FILE='' C_MANIFEST='' C_CONFIG='' C_CONFIG_TEXT='' C_VERSION_NAME='' C_D=''
REL_EXISTS=0     # release_state: GitHub release (or dry-run RELEASE_DIR) facts
REL_DRAFT=
REL_PRERELEASE=
REL_AUTHOR=
REL_ASSETS=
REL_ASSET_IDS=   # release_state: "name<TAB>asset id<TAB>digest" per asset, sorted
FETCHED=0        # fetch_tag ran
BUILD_WT=        # build: detached worktree of the tag, removed on exit
CONTAINERS=      # containers from docker create, removed on exit
CREATED_TAG=     # tag: local tag deleted again on failure (until it is pushed)
PREP_RESTORE=0   # prep: 1 from the version bump until every check passed; cleanup then
                 # restores Cargo.toml, Cargo.lock and CHANGELOG.md from HEAD
BODY_HEX=        # check_release_body: sha256 of the release body that publish checked

# ---------------------------------------------------------------------------------------------
# Helpers

die() {
    local code=$1
    shift
    printf 'error: %s\n' "$*" >&2
    if [ "${GITHUB_ACTIONS:-}" = true ]; then printf '::error::%s\n' "$*" >&2; fi
    exit "$code"
}

warn() {
    printf 'warning: %s\n' "$*" >&2
    if [ "${GITHUB_ACTIONS:-}" = true ]; then printf '::warning::%s\n' "$*" >&2; fi
}

# Progress goes to stderr so stdout carries only results (parse, notes, digests, summaries).
info() { printf '%s\n' "$*" >&2; }

need() {
    local tool
    for tool in "$@"; do
        command -v "$tool" >/dev/null 2>&1 || die 2 "missing required tool: $tool"
    done
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}

sha256_check() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum --check --strict "$@"
    else
        shasum -a 256 --check --strict "$@"
    fi
}

sha256_hex() {
    local out
    out=$(sha256 "$1") || die 2 "cannot hash $1"
    printf '%s\n' "${out%% *}"
}

# Prompts that change shared state (tag, sign, publish) need an explicit "yes".
confirm() {
    local answer=
    if [ "${RELEASE_YES:-}" = 1 ]; then
        info "RELEASE_YES=1: continuing without a prompt"
        return 0
    fi
    [ -t 0 ] || die 2 "confirmation needed but stdin is not a TTY (set RELEASE_YES=1 to skip it)"
    printf '%s Type yes to continue: ' "$1" >&2
    read -r answer || true
    [ "$answer" = yes ] || die 1 "aborted"
}

lower() { printf '%s\n' "$1" | tr '[:upper:]' '[:lower:]'; }

# $1 with every byte outside printable ASCII shown as ?, to quote output in a refusal.
printable() { printf '%s' "$1" | LC_ALL=C tr -c '[:print:]' '?'; }

count_lines() { awk 'NF { n++ } END { print n + 0 }'; }

# The non-empty lines of $1 as one phrase: "a", "a and b", "a, b and c".
and_list() {
    printf '%s\n' "$1" | awk 'NF { w[++n] = $0 } END { for (i = 1; i <= n; i++) printf "%s%s", (i == 1 ? "" : (i == n ? " and " : ", ")), w[i]; print "" }'
}

# Highest X.Y.Z on stdin. Numeric per field, so it orders like `sort -V` without needing it.
version_max() { awk NF | LC_ALL=C sort -t. -k1,1n -k2,2n -k3,3n | tail -n 1; }

release_dir() { printf '%s/%s\n' "$ARTIFACT_ROOT" "$1"; }

abspath() {
    case "$1" in
        /*) printf '%s\n' "$1" ;;
        *) printf '%s/%s\n' "$ORIG_PWD" "$1" ;;
    esac
}

# Physical path (symlinks, "." and ".." resolved) of the absolute directory path $1, which may
# not exist yet: its deepest existing ancestor is resolved and the rest appended, and that
# rest may not contain "." or "..".
physical_dir() {
    local head=$1 rest='' leaf
    while [ "$head" != / ] && [ "${head%/}" != "$head" ]; do head=${head%/}; done
    while [ ! -d "$head" ]; do
        leaf=${head##*/}
        case "$leaf" in '' | . | ..) die 2 "cannot resolve the directory $1" ;; esac
        rest="/$leaf$rest"
        head=${head%/*}
        if [ -z "$head" ]; then head=/; fi
    done
    head=$(cd "$head" && pwd -P) || die 2 "cannot resolve the directory $1"
    printf '%s%s\n' "${head%/}" "$rest"
}

# $1 is a dry-run image repository: localhost:PORT or 127.0.0.1:PORT, then a lowercase
# repository path with no tag, digest or "..". Anything looser (such as
# "localhost:5000@host/x") could send registry_digest's requests to another host.
is_local_image() {
    local host=${1%%/*} path=${1#*/}
    [[ "$host" =~ $LOCAL_HOST_RE ]] || return 1
    [ "$path" != "$1" ] || return 1
    [[ "$path" =~ $LOCAL_PATH_RE ]] || return 1
    case "$path" in *..*) return 1 ;; esac
    return 0
}

require_local_image_in_dry_run() {
    if [ "$DRY_RUN" = 1 ] && ! is_local_image "$IMAGE"; then
        die 2 "dry run: set RELEASE_IMAGE to a localhost: or 127.0.0.1: repository"
    fi
}

# Before the first docker call of build, sign and verify, so a stopped daemon is a precondition
# (exit 2) and not read as a registry outage.
require_docker() {
    docker info >/dev/null 2>&1 || die 2 "docker does not answer: start Docker (Docker Desktop on a Mac) and run again"
}

require_main_ref() {
    git rev-parse --verify --quiet "$MAIN_REF^{commit}" >/dev/null ||
        die 2 "$MAIN_REF does not exist; fetch main first"
}

# build pushes to ghcr.io and publish moves an alias there, so docker needs its credential.
# A dry run uses a local registry without one.
require_registry_login() {
    local cfg
    if [ "$DRY_RUN" = 1 ]; then return 0; fi
    cfg="${DOCKER_CONFIG:-$HOME/.docker}/config.json"
    grep -q '"ghcr.io"' "$cfg" 2>/dev/null || die 2 "docker is not logged in to ghcr.io: run make docker-login"
}

# ---------------------------------------------------------------------------------------------
# The tn-release-signing submodule
#
# See "Maintainer keys" at the top: MAIN_REF records the submodule at SIGNING_PATH as a gitlink
# (mode 160000), and load_allowlist reads from the submodule's object store at that commit.

# The commit MAIN_REF records for SIGNING_PATH when that is a gitlink, else nothing. Call after
# require_main_ref. Its output is captured, so callers pass a die on with `|| exit`.
main_gitlink() {
    local entry
    entry=$(git ls-tree "$MAIN_REF" "$SIGNING_PATH") || die 2 "cannot list $SIGNING_PATH on $MAIN_REF"
    printf '%s\n' "$entry" | awk '$1 == "160000" && $2 == "commit" { print $3 }'
}

# git -C on the empty directory of a submodule that is not checked out would read this
# repository instead, so the submodule must be the top of a work tree of its own.
require_submodule_checkout() {
    local prefix
    if prefix=$(git -C "$SIGNING_PATH" rev-parse --show-prefix 2>/dev/null) && [ -z "$prefix" ]; then
        return 0
    fi
    die 2 "submodule layout: $SIGNING_PATH is a submodule on $MAIN_REF but is not checked out here; run git submodule update --init -- $SIGNING_PATH"
}

# KEYS.md at commit $1 of the submodule states the signature threshold, on its one line
# "Signatures required: N", for every project that records that commit; MIN_SIGNATURES may
# require more, never fewer. Call after submodule_fetch.
check_keys_threshold() {
    local gitlink=$1 sub=$SIGNING_PATH text required
    text=$(git -C "$sub" show "$gitlink:KEYS.md" 2>/dev/null) ||
        die 1 "submodule layout: $sub at $gitlink, the commit $MAIN_REF records, has no KEYS.md to read the signature threshold from"
    required=$(printf '%s\n' "$text" | awk '
        /^Signatures required: *[0-9]+\.?[ \t]*$/ { n++; v = $0; sub(/^Signatures required: */, "", v); sub(/\.?[ \t]*$/, "", v) }
        END { if (n == 1) print v + 0 }')
    [ -n "$required" ] ||
        die 1 "submodule layout: KEYS.md in $sub at $gitlink, the commit $MAIN_REF records, must state the threshold on one line 'Signatures required: N'"
    [ "$MIN_SIGNATURES" -ge "$required" ] ||
        die 1 "submodule layout: MIN_SIGNATURES=$MIN_SIGNATURES in etc/release.sh is below the $required signatures KEYS.md requires in $sub at $gitlink, the commit $MAIN_REF records; raise it to at least $required"
}

# Makes commit $1 of the submodule present in its object store. A checkout made before main
# moved the pointer, or a shallow one (CI), lacks it: fetch it from the submodule's origin by
# id, or the origin's branches where a server refuses an id it does not advertise. Retried
# like git_fetch.
submodule_fetch() {
    local sha=$1 sub=$SIGNING_PATH attempt=1
    if git -C "$sub" cat-file -e "$sha^{commit}" 2>/dev/null; then return 0; fi
    git -C "$sub" remote get-url origin >/dev/null 2>&1 ||
        die 2 "submodule layout: $sub lacks $sha, the commit $MAIN_REF records, and has no remote named origin to fetch it from"
    info "submodule layout: fetching $sha, the commit $MAIN_REF records, into $sub from its origin"
    until git -C "$sub" fetch --quiet --no-tags origin "$sha" 2>/dev/null ||
        git -C "$sub" fetch --quiet --no-tags origin; do
        [ "$attempt" -lt 3 ] || die 3 "git fetch in $sub from its origin failed after 3 attempts"
        attempt=$((attempt + 1))
        sleep 5
    done
    git -C "$sub" cat-file -e "$sha^{commit}" 2>/dev/null ||
        die 1 "submodule layout: $MAIN_REF records $sha for $sub, but neither its checkout nor its origin has that commit"
}

# ---------------------------------------------------------------------------------------------
# Tag grammar

# Splits TAG into the variables `parse` prints. Exit 2 for anything but a release tag.
parse_tag() {
    local rc_suffix
    TAG=$1
    [[ "$TAG" =~ $TAG_RE ]] ||
        die 2 "not a release tag: '$TAG' (expected vX.Y.Z, vX.Y.Z-rcN, vX.Y.Z-adiri or vX.Y.Z-adiri-rcN)"
    VERSION="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
    rc_suffix=${BASH_REMATCH[5]}
    RC=${rc_suffix#-rc}
    BASE=${TAG%"$rc_suffix"}
    # CHAIN_CONFIGS: the directory of the channel's genesis, committee and parameters files,
    # whose hashes SHA256SUMS carries. Adiri is chain-configs/testnet (chain-configs/README.md).
    # PACKAGE: the ghcr package that holds the channel's images, and no other channel's.
    if [ -n "${BASH_REMATCH[4]}" ]; then
        CHANNEL=adiri
        PACKAGE=tn-testnet
        FEATURES=adiri
        CHAIN_CONFIGS=chain-configs/testnet
    else
        CHANNEL=mainnet
        PACKAGE=tn-mainnet
        FEATURES=
        CHAIN_CONFIGS=chain-configs/mainnet
    fi
    # Only a mainnet final is a full release; aliases move only for finals.
    PRERELEASE=true
    ALIAS=
    if [ -z "$RC" ]; then
        if [ "$CHANNEL" = mainnet ]; then
            PRERELEASE=false
            ALIAS=latest
        else
            ALIAS=adiri
        fi
    fi
    TARBALL="telcoin-network-$TAG-$TRIPLE.tar.gz"
}

print_parse() {
    printf 'TAG=%s\nVERSION=%s\nCHANNEL=%s\nPACKAGE=%s\nRC=%s\nBASE=%s\nFEATURES=%s\nPRERELEASE=%s\nALIAS=%s\nTARBALL=%s\nCHAIN_CONFIGS=%s\n' \
        "$TAG" "$VERSION" "$CHANNEL" "$PACKAGE" "$RC" "$BASE" "$FEATURES" "$PRERELEASE" "$ALIAS" "$TARBALL" "$CHAIN_CONFIGS"
}

# X.Y.Z of the final (non-rc) tags among $1 (one per line) in TAG's channel, TAG excluded.
channel_finals() {
    local t t_channel
    while IFS= read -r t; do
        if [ "$t" != "$TAG" ] && [[ "$t" =~ $TAG_RE ]] && [ -z "${BASH_REMATCH[5]}" ]; then
            t_channel=mainnet
            if [ -n "${BASH_REMATCH[4]}" ]; then t_channel=adiri; fi
            if [ "$t_channel" = "$CHANNEL" ]; then
                printf '%s.%s.%s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}"
            fi
        fi
    done <<EOF
$1
EOF
}

# ---------------------------------------------------------------------------------------------
# Components
#
# A release is a set of components, each an image, a tarball holding the image's binary and a
# digest file naming the image by digest, listed in a signed manifest. COMPONENTS names them in
# release order. Every step that acts on an image, tarball, digest file or manifest reads it
# from this table, the node's included.

# load_components COMMIT: sets COMPONENTS (in KNOWN_COMPONENTS order) and MANIFESTS from
# COMPONENTS_FILE as committed at COMMIT. A commit without the file ships the node alone, as
# every tag made before the file existed does. Blank lines and lines starting with # are
# ignored; every other line is one known name, listed once, and node is one of them.
# The file is the one input read from the tagged tree, and it only chooses which artifacts the
# release must hold: every check of them is this script's, from main. check_tag reads it after
# the tag's signature and attestation passed, and tag before it signs, so a tag whose list is
# refused is never pushed; notes and draft read it after tag_commit alone, to render text. It
# needs git alone (no TAG, WORK or network): a guard sources this file and runs
# `load_components HEAD` from the repository root.
load_components() {
    local commit=$1 where="$COMPONENTS_FILE at $1" fix entry mode kind id rest text line shown
    local seen=' ' from list='' manifests='' c m
    fix="correct it on main and tag a new release commit"
    # ls-tree prints nothing for a missing file and fails on a commit or tree git cannot read,
    # so a read error never passes for a tag without the file.
    entry=$(git ls-tree --full-tree "$commit" -- "$COMPONENTS_FILE" 2>/dev/null) ||
        die 1 "cannot read $where: git cannot read that commit's tree; fetch it again, or check the repository with git fsck"
    if [ -z "$entry" ]; then
        seen=' node '
        from="no $where, so the node alone"
    else
        read -r mode kind id rest <<EOF
$entry
EOF
        case "$mode" in
            100644 | 100755) ;;
            *) die 1 "$where is a $kind of git mode $mode, not a regular file; $fix" ;;
        esac
        # bash drops NUL bytes from a variable; as \001 they stay a byte no name may hold.
        text=$(git cat-file blob "$id" 2>/dev/null | tr '\000' '\001') ||
            die 1 "cannot read $where (blob $id); fetch it again, or check the repository with git fsck"
        while IFS= read -r line; do
            case "${line#"${line%%[![:space:]]*}"}" in '' | '#'*) continue ;; esac
            # The name rule first: a line such as "node worker-gateway" would pass the
            # space-delimited membership test below.
            if ! [[ "$line" =~ $COMPONENT_RE ]]; then
                shown=$(printf '%s' "$line" | LC_ALL=C tr -c '[:print:]' '?')
                die 1 "$where has the malformed line '$shown'; expected one component name per line, a blank line or a # comment; $fix"
            fi
            case " $KNOWN_COMPONENTS " in
                *" $line "*) ;;
                *) die 1 "$where names '$line', which etc/release.sh does not know (it knows: $KNOWN_COMPONENTS); $fix" ;;
            esac
            case "$seen" in *" $line "*) die 1 "$where names $line twice; $fix" ;; esac
            seen="$seen$line "
        done <<EOF
$text
EOF
        case "$seen" in *" node "*) ;; *) die 1 "$where does not name node; every release ships the node; $fix" ;; esac
        from=$where
    fi
    for c in $KNOWN_COMPONENTS; do
        case "$seen" in *" $c "*) ;; *) continue ;; esac
        list="$list${list:+ }$c"
        m=$(manifest_of "$c") || exit
        case " $manifests " in *" $m "*) ;; *) manifests="$manifests${manifests:+ }$m" ;; esac
    done
    COMPONENTS=$list
    MANIFESTS=$manifests
    info "components: $COMPONENTS ($from)"
}

# The signed manifest that lists component $1's digest file and tarball: SHA256SUMS for the
# node, EDGE_SUMS for the others. Its output is captured, so callers pass a die on with
# `|| exit`.
manifest_of() {
    case "$1" in
        node) printf '%s\n' SHA256SUMS ;;
        worker-gateway | node-record-api) printf '%s\n' "$EDGE_SUMS" ;;
        *) die 2 "internal error: no manifest for the component '$1'" ;;
    esac
}

# record_digest NAME DIGEST keeps the image digest (sha256:<hex>) of component NAME once it is
# pushed, verified, or read from a draft's digest file that check_digest_package accepted;
# digest_of NAME prints it, or nothing while it is unknown.
record_digest() {
    DIGESTS=$(printf '%s\n' "$DIGESTS" | awk -F'\t' -v n="$1" -v d="$2" 'NF && $1 != n { print } END { print n "\t" d }')
}

digest_of() {
    printf '%s\n' "$DIGESTS" | awk -F'\t' -v n="$1" '$1 == n { d = $2 } END { print d }'
}

# Fills the C_* fields with component $1's row of the table. They hold until the next
# component_set, so a loop body sets them before it reads them, and a function that calls
# component_set overwrites its caller's fields unless its output is captured (a subshell).
# One arm per component, so any field of a component may differ from the others'.
# The node's image is IMAGE, its channel's package. The other components' packages serve every
# channel; in a dry run with RELEASE_IMAGE, as for IMAGE in init_env, each one's image is
# RELEASE_IMAGE-<component>, which is a local image whenever RELEASE_IMAGE is.
# C_VERSION_NAME starts the first line of the component's --version; the node's is empty, as
# its version line has a rule of its own.
component_set() {
    case "$1" in
        node)
            C_PACKAGE=$PACKAGE
            C_IMAGE=$IMAGE
            C_DOCKERFILE=etc/Dockerfile
            C_BIN=telcoin
            C_TAR_BIN=telcoin-network
            C_TARDIR=telcoin-network-$TAG-$TRIPLE
            C_TARBALL=$TARBALL
            C_DIGEST_FILE=IMAGE_DIGEST
            C_CONFIG='null ["telcoin","node"]'
            C_CONFIG_TEXT='no ENTRYPOINT and CMD ["telcoin","node"]'
            C_VERSION_NAME=
            ;;
        worker-gateway)
            C_PACKAGE=tn-worker-gateway
            C_IMAGE=ghcr.io/$OWNER/tn-worker-gateway
            if [ -n "${RELEASE_IMAGE:-}" ]; then C_IMAGE=$RELEASE_IMAGE-worker-gateway; fi
            C_DOCKERFILE=bin/worker-gateway/Dockerfile
            C_BIN=worker-gateway
            C_TAR_BIN=worker-gateway
            C_TARDIR=worker-gateway-$TAG-$TRIPLE
            C_TARBALL=worker-gateway-$TAG-$TRIPLE.tar.gz
            C_DIGEST_FILE=IMAGE_DIGEST-worker-gateway
            C_CONFIG='["worker-gateway"] null'
            C_CONFIG_TEXT='ENTRYPOINT ["worker-gateway"] and no CMD'
            C_VERSION_NAME=tn-worker-gateway
            ;;
        node-record-api)
            C_PACKAGE=tn-node-record-api
            C_IMAGE=ghcr.io/$OWNER/tn-node-record-api
            if [ -n "${RELEASE_IMAGE:-}" ]; then C_IMAGE=$RELEASE_IMAGE-node-record-api; fi
            C_DOCKERFILE=bin/node-record-api/Dockerfile
            C_BIN=node-record-api
            C_TAR_BIN=node-record-api
            C_TARDIR=node-record-api-$TAG-$TRIPLE
            C_TARBALL=node-record-api-$TAG-$TRIPLE.tar.gz
            C_DIGEST_FILE=IMAGE_DIGEST-node-record-api
            C_CONFIG='["node-record-api"] null'
            C_CONFIG_TEXT='ENTRYPOINT ["node-record-api"] and no CMD'
            C_VERSION_NAME=tn-node-record-api
            ;;
        *) die 2 "internal error: no component named '$1'" ;;
    esac
    C_NAME=$1
    C_MANIFEST=$(manifest_of "$1") || exit
    C_D=$(digest_of "$1") || exit
}

# ---------------------------------------------------------------------------------------------
# Allowlist and signatures

allowlist_name() {
    [[ "$1" =~ $HANDLE_FILE_RE ]] ||
        die 1 "$2/$1: allowlist files must be named <GitHub login>.asc"
}

# Fingerprint of every primary key in a gpg --with-colons listing on stdin.
primary_fprs() {
    awk -F: '$1 == "pub" { want = 1; next } $1 == "fpr" && want { print $10 } { want = 0 }'
}

# Imports the maintainer keys from main (never from the tagged tree) into a private keyring.
# main holds no key files, only the gitlink of the submodule at SIGNING_PATH, so they are read
# from the submodule's object store at the commit MAIN_REF records, never from its checkout:
# neither a tag nor a checkout can add a key. The threshold KEYS.md states at that commit is
# checked there too (check_keys_threshold).
# Sets ALLOW_GNUPGHOME and ALLOW_LIST ("handle<TAB>primary fingerprint", one line per primary
# key; a file may hold several during a rotation). Thresholds count distinct handles.
# ALLOW_LIST is in the byte order of the handles, each file's keys in the file's order: a glob
# sorts in the locale's collation, and render_notes prints ALLOW_LIST as the body's key table,
# which publish compares line for line with the body another machine wrote.
load_allowlist() {
    local src where names name file fprs fpr handles repo rev gitlink
    if [ -n "$ALLOW_GNUPGHOME" ]; then return 0; fi
    need gpg
    src=$WORK/allowlist
    mkdir "$src"
    if [ -n "${RELEASE_ALLOWLIST_DIR:-}" ]; then
        where=$RELEASE_ALLOWLIST_DIR
        [ -d "$where" ] || die 2 "RELEASE_ALLOWLIST_DIR=$where is not a directory"
        for file in "$where"/*.asc; do
            if [ -e "$file" ]; then
                allowlist_name "${file##*/}" "$where"
                cp "$file" "$src/${file##*/}"
            fi
        done
    else
        require_main_ref
        gitlink=$(main_gitlink) || exit
        # No fallback to a directory of this repository: keys committed here would bypass the
        # review that moving the submodule pointer gets.
        [ -n "$gitlink" ] ||
            die 1 "main must record the $SIGNING_PATH submodule (tn-release-signing), but $MAIN_REF has no submodule at $SIGNING_PATH; maintainer keys are never read from this repository's own tree"
        require_submodule_checkout
        submodule_fetch "$gitlink"
        check_keys_threshold "$gitlink"
        repo=$SIGNING_PATH
        rev="$gitlink:${ALLOWLIST_PATH#"$SIGNING_PATH"/}"
        where="$SIGNING_PATH submodule $rev"
        info "submodule layout: maintainer keys from $SIGNING_PATH at $gitlink, the commit $MAIN_REF records"
        names=$(git -C "$repo" ls-tree --name-only "$rev" 2>/dev/null) ||
            die 1 "$ALLOWLIST_PATH does not exist on $MAIN_REF (submodule layout: $SIGNING_PATH at $gitlink has no ${ALLOWLIST_PATH#"$SIGNING_PATH"/})"
        while IFS= read -r name; do
            case "$name" in *.asc*) ;; *) continue ;; esac
            allowlist_name "$name" "$where"
            git -C "$repo" show "$rev/$name" >"$src/$name" || die 1 "cannot read $where/$name"
        done <<EOF
$names
EOF
    fi

    # Short path: gpg keeps its sockets in the homedir and socket paths are limited to ~100 bytes.
    ALLOW_GNUPGHOME=$WORK/g
    mkdir -m 700 "$ALLOW_GNUPGHOME"
    printf 'no-autostart\n' >"$ALLOW_GNUPGHOME/gpg.conf"
    : >"$WORK/allowlist.tsv"
    for file in "$src"/*.asc; do
        [ -e "$file" ] || continue
        name=${file##*/}
        if ! grep -q -e '-----BEGIN PGP PUBLIC KEY BLOCK-----' "$file"; then
            die 1 "$where/$name is not a provisioned OpenPGP public key (placeholder?)"
        fi
        fprs=$(gpg --homedir "$ALLOW_GNUPGHOME" --batch --with-colons \
            --import-options show-only --import "$file" 2>/dev/null | primary_fprs) ||
            die 1 "cannot read the keys in $where/$name"
        [ -n "$fprs" ] || die 1 "$where/$name holds no OpenPGP primary key"
        while IFS= read -r fpr; do
            if awk -F'\t' -v f="$fpr" '$2 == f { hit = 1 } END { exit !hit }' "$WORK/allowlist.tsv"; then
                die 1 "primary key $fpr appears in two allowlist files (second: $where/$name)"
            fi
            printf '%s\t%s\n' "${name%.asc}" "$fpr" >>"$WORK/allowlist.tsv"
        done <<EOF
$fprs
EOF
        gpg --homedir "$ALLOW_GNUPGHOME" --batch --quiet --import "$file" >/dev/null 2>&1 ||
            die 1 "cannot import $where/$name"
    done
    ALLOW_LIST=$(LC_ALL=C sort -s -t $'\t' -k1,1 "$WORK/allowlist.tsv")
    [ -n "$ALLOW_LIST" ] || die 1 "no maintainer keys in $where"
    handles=$(cut -f1 "$WORK/allowlist.tsv" | LC_ALL=C sort -u | count_lines)
    [ "$THRESHOLD" -le "$handles" ] ||
        die 1 "signature threshold $THRESHOLD exceeds the $handles allowlisted maintainer(s)"
}

handle_of() {
    printf '%s\n' "$ALLOW_LIST" | awk -F'\t' -v f="$1" '$2 == f { print $1 }'
}

# Reads gpg status output (--status-fd) on stdin and judges each signature block, split on
# NEWSIG. A block is good only with GOODSIG and VALIDSIG, and its signer is the VALIDSIG
# primary fingerprint (last field) mapped through the allowlist. Prints per block
# "good<TAB>handle<TAB>fpr", "bad<TAB>keyid" or "skip<TAB>reason", the reason starting with
# the gpg status, then "blocks<TAB>N".
sig_blocks() {
    awk -v allow="$WORK/allowlist.tsv" '
        BEGIN {
            while ((getline line < allow) > 0) { split(line, f, "\t"); handle[f[2]] = f[1] }
        }
        function open_block() {
            if (!inblock) { inblock = 1; n++; good = 0; valid = ""; note = ""; bad = 0; nokey = 0 }
        }
        function close_block() {
            if (!inblock) return
            if (good && valid != "") {
                if (valid in handle) print "good\t" handle[valid] "\t" valid
                else print "skip\tGOODSIG by key " valid ", which is not on the allowlist"
            } else if (note != "") print "skip\t" note (nokey ? " (NO_PUBKEY: the key is not on the allowlist)" : "")
            else if (!bad) print "skip\tsignature without GOODSIG and VALIDSIG"
            inblock = 0
        }
        $1 != "[GNUPG:]" { next }
        $2 == "NEWSIG" { close_block(); open_block(); next }
        $2 == "GOODSIG" { open_block(); good = 1; next }
        $2 == "VALIDSIG" { open_block(); valid = $NF; next }
        $2 == "BADSIG" { open_block(); bad = 1; print "bad\t" $3; next }
        $2 == "ERRSIG" { open_block(); note = "ERRSIG: cannot check the signature by key " $3; next }
        $2 == "NO_PUBKEY" { open_block(); nokey = 1; next }
        $2 == "EXPKEYSIG" { open_block(); note = "EXPKEYSIG: signature by expired key " $3; next }
        $2 == "REVKEYSIG" { open_block(); note = "REVKEYSIG: signature by revoked key " $3; next }
        $2 == "EXPSIG" { open_block(); note = "EXPSIG: expired signature by key " $3; next }
        END { close_block(); print "blocks\t" n + 0 }
    '
}

# count_sigs FILE SIG: prints the distinct allowlisted handles with a good signature over FILE
# in SIG (concatenated armored detached signatures), one per line; the count is the line
# count. Every signature in SIG must be a GOODSIG with a VALIDSIG by an allowlisted primary
# key, the rule of the operator check on the install page: one that is bad, unverifiable,
# expired, revoked or by a key off the allowlist refuses the whole file (exit 1), even next to
# good ones. Its output is captured, so callers pass a die on with `|| exit`.
count_sigs() {
    local file=$1 sig=$2 status results refused
    if [ ! -f "$file" ] || [ ! -f "$sig" ]; then die 2 "count_sigs: $file or $sig does not exist"; fi
    status=$(gpg --homedir "$ALLOW_GNUPGHOME" --batch --status-fd 1 --verify "$sig" "$file" 2>/dev/null || true)
    results=$(printf '%s\n' "$status" | sig_blocks) || die 1 "cannot judge the signatures in ${sig##*/}"
    refused=$(printf '%s\n' "$results" | awk -F'\t' -v f="${file##*/}" '
        $1 == "bad" { msg = msg sep "BADSIG: signature by key " $2 " does not match " f; sep = "; " }
        $1 == "skip" { msg = msg sep $2; sep = "; " }
        END { print msg }')
    [ -z "$refused" ] ||
        die 1 "${sig##*/} holds a signature that is not a good one by an allowlisted maintainer key, so the file is refused: $refused"
    printf '%s\n' "$results" | awk -F'\t' '$1 == "good" { print $2 }' | LC_ALL=C sort -u
}

# check-tag step (3): exactly one signature on the tag, good, by an allowlisted primary key.
# Sets SIGNER and SIGNER_FPR.
check_tag_signature() {
    local status results blocks good
    load_allowlist
    status=$(GNUPGHOME=$ALLOW_GNUPGHOME git -c gpg.format=openpgp -c gpg.program=gpg \
        -c gpg.openpgp.program=gpg verify-tag --raw "refs/tags/$TAG" 2>&1 >/dev/null || true)
    results=$(printf '%s\n' "$status" | sig_blocks)
    blocks=$(printf '%s\n' "$results" | awk -F'\t' '$1 == "blocks" { print $2 }')
    [ "$blocks" -ge 1 ] || die 1 "tag $TAG is not signed"
    if printf '%s\n' "$results" | awk -F'\t' '$1 == "bad" { hit = 1 } END { exit !hit }'; then
        die 1 "tag $TAG has a bad signature"
    fi
    [ "$blocks" = 1 ] || die 1 "tag $TAG carries $blocks signatures; exactly one is allowed"
    good=$(printf '%s\n' "$results" | awk -F'\t' '$1 == "good" { print $2 " " $3 }')
    if [ -z "$good" ]; then
        die 1 "tag $TAG is not signed by an allowlisted maintainer key ($(printf '%s\n' "$results" | awk -F'\t' '$1 == "skip" { print $2 }'))"
    fi
    SIGNER=${good%% *}
    SIGNER_FPR=${good#* }
}

# The key `tag` and `sign` use: RELEASE_GPG_KEY, else git's user.signingkey. A subkey resolves
# to its primary, which must be allowlisted. Sets SIGN_KEY, SIGN_FPR and SIGN_HANDLE.
resolve_signing_key() {
    local listing count
    SIGN_KEY=${RELEASE_GPG_KEY:-}
    if [ -z "$SIGN_KEY" ]; then SIGN_KEY=$(git config --get user.signingkey || true); fi
    [ -n "$SIGN_KEY" ] || die 2 "no signing key: set RELEASE_GPG_KEY or git config user.signingkey"
    listing=$(gpg --batch --with-colons --list-keys -- "${SIGN_KEY%!}" 2>/dev/null) ||
        die 2 "signing key ${SIGN_KEY%!} is not in your GnuPG keyring"
    SIGN_FPR=$(printf '%s\n' "$listing" | primary_fprs)
    count=$(printf '%s\n' "$SIGN_FPR" | count_lines)
    [ "$count" = 1 ] || die 2 "signing key ${SIGN_KEY%!} matches $count primary keys; give its fingerprint"
    SIGN_HANDLE=$(handle_of "$SIGN_FPR")
    [ -n "$SIGN_HANDLE" ] || die 1 "signing key $SIGN_FPR is not in the maintainer allowlist on $MAIN_REF"
}

# ---------------------------------------------------------------------------------------------
# Registry

# HEAD request for a manifest. Writes the response headers to $2 and prints the HTTP status.
manifest_head() {
    local url=$1 out=$2 token=$3
    set -- -sS -I --connect-timeout 10 --max-time 60 -o "$out" -w '%{http_code}' \
        -H "Accept: $MANIFEST_TYPES"
    if [ -n "$token" ]; then set -- "$@" -H "Authorization: Bearer $token"; fi
    curl "$@" "$url"
}

# registry_digest REPO REF [first-push]: prints the digest the registry serves for REPO:REF, or
# nothing when that tag does not exist. ghcr.io needs an anonymous pull token, which only works
# for a public package; the dry-run localhost registry is plain http without a token. ghcr
# answers 403 alike for a private package and for one that has never been pushed, so with
# first-push an answer of 401 or 403 prints the word denied instead of stopping.
registry_digest() {
    local repo=$1 ref=$2 mode=${3:-} host path url token code digest attempt=1 out
    need curl
    host=${repo%%/*}
    path=${repo#*/}
    out=$WORK/registry.out
    case "$host" in
        localhost:* | 127.0.0.1:*) url="http://$host/v2/$path/manifests/$ref" ;;
        ghcr.io) url="https://ghcr.io/v2/$path/manifests/$ref" ;;
        *) die 2 "registry_digest: unsupported registry $host" ;;
    esac
    while :; do
        token=
        code=200
        if [ "$host" = ghcr.io ]; then
            code=$(curl -sS --connect-timeout 10 --max-time 60 -o "$out" -w '%{http_code}' \
                "https://ghcr.io/token?scope=repository:$path:pull") || code=000
            if [ "$code" = 200 ]; then token=$(sed -n 's/.*"token" *: *"\([^"]*\)".*/\1/p' "$out"); fi
        fi
        if [ "$code" = 200 ]; then code=$(manifest_head "$url" "$out" "$token") || code=000; fi
        case "$code" in
            200)
                digest=$(awk 'tolower($1) == "docker-content-digest:" { print $2 }' "$out" | tr -d '\r')
                if [[ "$digest" =~ $DIGEST_RE ]]; then
                    printf '%s\n' "$digest"
                    return 0
                fi
                code="200 without a valid docker-content-digest"
                ;;
            404) return 0 ;;
            401 | 403)
                if [ "$mode" = first-push ]; then
                    printf 'denied\n'
                    return 0
                fi
                die 2 "$repo: the registry answered HTTP $code: package is not public, or has never been pushed (one-time setup after its first push: make it public, as the maintainer guide says)"
                ;;
        esac
        [ "$attempt" -lt 3 ] || die 3 "$repo:$ref: registry unavailable after 3 attempts (last answer: $code)"
        attempt=$((attempt + 1))
        sleep 5
    done
}

# login_digest REPO: build, after ghcr refused an anonymous read of REPO: the digest of REPO:TAG
# as the docker login sees it, or nothing (with the answer on stderr) when the registry answers
# that the tag is not found or access is denied (403 Forbidden included: ghcr answers that for
# a package never pushed). Any other failure, such as DNS, a timeout or an expired login, is
# retried and then stops with 3, so it never reads as "the login does not see TAG".
login_digest() {
    local repo=$1 out digest last attempt=1
    while :; do
        if out=$(docker buildx imagetools inspect "$repo:$TAG" --format '{{println .Manifest.Digest}}' 2>&1); then
            digest=$(printf '%s\n' "$out" | awk 'NF { d = $1 } END { print d }')
            [[ "$digest" =~ $DIGEST_RE ]] ||
                die 1 "docker buildx imagetools inspect $repo:$TAG answered without a digest: $out"
            printf '%s\n' "$digest"
            return 0
        fi
        last=$(printf '%s\n' "$out" | awk 'NF { l = $0 } END { print l }')
        case "$(printf '%s\n' "$out" | tr '[:upper:]' '[:lower:]')" in
            *"permission denied"*) ;;
            *"not found"* | *"manifest unknown"* | *"name unknown"* | *denied* | *"403 forbidden"*)
                info "with the docker login, $repo:$TAG does not resolve: $last"
                return 0
                ;;
        esac
        [ "$attempt" -lt 3 ] ||
            die 3 "cannot ask the registry for $repo:$TAG with the docker login after 3 attempts: $last (check the network, and run make docker-login if the login expired)"
        attempt=$((attempt + 1))
        sleep 5
    done
}

# ---------------------------------------------------------------------------------------------
# Tag, version and CHANGELOG checks

# A missing origin is a precondition, not an outage, so it is not retried.
require_origin() {
    git remote get-url origin >/dev/null 2>&1 || die 2 "this checkout has no git remote named origin"
}

# Retried because a fetch is the first network call on every host. git fetch exits 1 when it
# refuses a ref update (here only the unforced tags: a local one differs from origin's), 128 on
# other errors.
git_fetch() {
    local attempt=1 rc
    require_origin
    while :; do
        git fetch --quiet --no-tags origin "$@" && rc=0 || rc=$?
        case "$rc" in
            0) return 0 ;;
            1) die 1 "git fetch refused to update a local ref from origin ($*); a local tag differs from the one on origin: find out why before you delete it" ;;
        esac
        [ "$attempt" -lt 3 ] || die 3 "git fetch from origin failed after 3 attempts"
        attempt=$((attempt + 1))
        sleep 5
    done
}

# Object id of refs/tags/$1 on origin, or nothing.
ls_remote_tag() {
    local out attempt=1
    require_origin
    until out=$(git ls-remote origin "refs/tags/$1"); do
        [ "$attempt" -lt 3 ] || die 3 "git ls-remote origin failed after 3 attempts"
        attempt=$((attempt + 1))
        sleep 5
    done
    printf '%s\n' "$out" | awk -v r="refs/tags/$1" '$2 == r { print $1 }'
}

# check-tag step (1), outside CI and dry run: fetch main, this tag and every other v* tag,
# unforced, so a local tag that differs from origin is an error rather than silently replaced,
# and the version rules see the tags origin has.
fetch_tag() {
    local remote local_obj
    if [ "$FETCHED" = 1 ] || [ "$DRY_RUN" = 1 ] || [ "${GITHUB_ACTIONS:-}" = true ]; then return 0; fi
    remote=$(ls_remote_tag "$TAG")
    [ -n "$remote" ] || die 2 "tag $TAG does not exist on origin"
    if local_obj=$(git rev-parse --verify --quiet "refs/tags/$TAG"); then
        [ "$local_obj" = "$remote" ] || die 1 "local tag $TAG ($local_obj) differs from origin ($remote)"
    fi
    git_fetch +refs/heads/main:refs/remotes/origin/main "refs/tags/$TAG:refs/tags/$TAG" 'refs/tags/v*:refs/tags/v*'
    FETCHED=1
}

# check-tag step (2): TAG is an annotated tag object that names itself and points at a commit.
# Prints that commit.
tag_commit() {
    local tag=$1 type obj header_tag header_type
    git rev-parse --verify --quiet "refs/tags/$tag" >/dev/null || die 2 "tag $tag does not exist locally"
    type=$(git cat-file -t "refs/tags/$tag") || die 1 "cannot read refs/tags/$tag"
    [ "$type" = tag ] || die 1 "$tag is a lightweight tag (it names a $type); release tags are signed annotated tags"
    obj=$(git cat-file tag "refs/tags/$tag") || die 1 "cannot read the tag object of $tag"
    header_tag=$(printf '%s\n' "$obj" | awk 'NF == 0 { body = 1 } !body && $1 == "tag" { print $2 }')
    header_type=$(printf '%s\n' "$obj" | awk 'NF == 0 { body = 1 } !body && $1 == "type" { print $2 }')
    [ "$header_tag" = "$tag" ] || die 1 "refs/tags/$tag holds a tag object named '$header_tag'"
    [ "$header_type" = commit ] || die 1 "tag $tag points at a $header_type, not a commit"
    git rev-parse --verify --quiet "refs/tags/$tag^{commit}" || die 1 "cannot resolve $tag to a commit"
}

# check-tag step (4).
check_on_main() {
    local rc
    require_main_ref
    git merge-base --is-ancestor "$1" "$MAIN_REF" && rc=0 || rc=$?
    case "$rc" in
        0) ;;
        1) die 1 "$1 is not on main ($MAIN_REF)" ;;
        *) die 2 "git merge-base failed for $1" ;;
    esac
}

# The first `version = "..."` inside [workspace.package] of the Cargo.toml on stdin.
workspace_version() {
    awk '
        /^\[/ { in_ws = ($0 ~ /^\[workspace\.package\][ \t]*$/) }
        in_ws && !found && /^[ \t]*version[ \t]*=/ {
            v = $0; sub(/^[^"]*"/, "", v); sub(/".*$/, "", v); print v; found = 1
        }
    '
}

# Tagger time (epoch seconds) of the annotated tag $1, from its tag object; empty for a
# lightweight tag.
tagger_time() {
    local raw
    raw=$(git for-each-ref --format='%(taggerdate:raw)' "refs/tags/$1") || die 1 "cannot read refs/tags/$1"
    printf '%s\n' "${raw%% *}"
}

# Names of the annotated v* tags on origin, the ones ls-remote lists with a peeled ^{} line;
# a lightweight tag counts for no version rule, as on the cutoff path. Read with ls-remote
# because the local tags can be stale. Retried like ls_remote_tag.
remote_tags() {
    local out attempt=1
    require_origin
    until out=$(git ls-remote --tags origin 'refs/tags/v*'); do
        [ "$attempt" -lt 3 ] || die 3 "git ls-remote --tags origin failed after 3 attempts"
        attempt=$((attempt + 1))
        sleep 5
    done
    printf '%s\n' "$out" | awk '$2 ~ /\^\{\}$/ { n = $2; sub(/^refs\/tags\//, "", n); sub(/\^\{\}$/, "", n); if (n != "") print n }'
}

# Names of the annotated v* tags that count for the version rules. Without a cutoff (TAG does
# not exist yet, or CI checks the push of TAG), every one on origin, read with ls-remote, or
# every local one in a dry run and in CI, whose checkout holds origin's tags. With a cutoff
# (epoch seconds), every local one whose tagger date is strictly earlier, whatever commit it
# names.
# Its output is captured, and bash 3.2 runs $(...) without -e, so a failure dies explicitly
# and the caller passes the status on with `|| exit`.
tags_before() {
    local out
    if [ -n "$1" ]; then
        out=$(git for-each-ref --format='%(refname:strip=2) %(taggerdate:raw)' 'refs/tags/v*') ||
            die 2 "cannot list the v* tags"
        printf '%s\n' "$out" | awk -v c="$1" '$2 != "" && $2 + 0 < c + 0 { print $1 }'
    elif [ "$DRY_RUN" = 1 ] || [ "${GITHUB_ACTIONS:-}" = true ]; then
        out=$(git for-each-ref --format='%(objecttype) %(refname:strip=2)' 'refs/tags/v*') ||
            die 2 "cannot list the v* tags"
        printf '%s\n' "$out" | awk '$1 == "tag" { print $2 }'
    else
        remote_tags
    fi
}

# The release is monotonic in its channel among the tags that tags_before CUTOFF names: a
# final's version is above every other final's, and a release candidate's version is above
# every final's, its own final included.
check_version_order() {
    local earlier others highest
    earlier=$(tags_before "$1") || exit
    if [ -n "$RC" ]; then
        case $'\n'"$earlier"$'\n' in
            *$'\n'"$BASE"$'\n'*) die 1 "$BASE already exists; no release candidate may follow its final release" ;;
        esac
    fi
    others=$(channel_finals "$earlier")
    highest=$(printf '%s\n%s\n' "$others" "$VERSION" | version_max)
    [ "$highest" = "$VERSION" ] ||
        die 1 "$TAG is not newer than the $CHANNEL release v$highest; $CHANNEL versions only go up"
}

# Epoch seconds at which GitHub created this workflow run: the push of the tag created it, and
# a re-run keeps it. Its output is captured, so a failure dies explicitly.
ci_run_created() {
    local t
    [[ "${GITHUB_RUN_ID:-}" =~ ^[0-9]+$ ]] || die 2 "GITHUB_RUN_ID is not set"
    t=$(gh_run api "repos/$REPO/actions/runs/$GITHUB_RUN_ID" --jq '.created_at | fromdateiso8601') || exit
    [[ "$t" =~ ^[0-9]+$ ]] || die 3 "GitHub returned no creation time for workflow run $GITHUB_RUN_ID"
    printf '%s\n' "$t"
}

# The cutoff for the version rules once TAG exists, empty for none. A tag is validated against
# every release tag that exists when it is pushed:
# - CI, on the push of TAG: the first run compares with every tag; a re-run with the tags
#   created before GitHub created the run, the time of the push.
# - Everywhere else (a dispatch, build, sign, verify, publish): the tags dated before TAG's
#   tagger date, which must lie between its commit's committer date and five minutes from now.
#   The check of the push in CI is what creates the draft that these steps need.
# Its output is captured, so a failure dies explicitly.
tag_cutoff() {
    local commit=$1 tagged committed now
    if [ "${GITHUB_ACTIONS:-}" = true ] && [ "${GITHUB_EVENT_NAME:-}" = push ]; then
        if [ "${GITHUB_RUN_ATTEMPT:-1}" != 1 ]; then ci_run_created || exit; fi
        return 0
    fi
    tagged=$(tagger_time "$TAG") || exit
    [ -n "$tagged" ] || die 1 "tag $TAG has no tagger date"
    committed=$(git log -1 --format=%ct "$commit") || die 1 "cannot read the committer date of $commit"
    [ "$tagged" -ge "$committed" ] ||
        die 1 "tag $TAG is dated $tagged (epoch seconds), before its commit $commit was made ($committed); a release tag is made after its commit"
    now=$(date +%s) || die 2 "cannot read the clock"
    [ "$tagged" -le $((now + 300)) ] ||
        die 1 "tag $TAG is dated $tagged (epoch seconds), more than five minutes after this host's clock ($now)"
    printf '%s\n' "$tagged"
}

# check-tag step (5): the Cargo version is X.Y.Z, the extradata string fits 32 bytes, and the
# release is monotonic in its channel, with no release candidate after its final.
check_version_at() {
    local commit=$1 cargo_version extradata cutoff=''
    cargo_version=$(git show "$commit:Cargo.toml" 2>/dev/null | workspace_version) ||
        die 1 "cannot read Cargo.toml at $commit"
    [ -n "$cargo_version" ] || die 1 "no [workspace.package] version in Cargo.toml at $commit"
    [ "$cargo_version" = "$VERSION" ] ||
        die 1 "Cargo.toml at $commit has version $cargo_version; $TAG needs $VERSION (run make release-prep)"
    extradata="telcoin-network/v$VERSION/linux"
    [ "${#extradata}" -le 32 ] || die 1 "'$extradata' is longer than the 32-byte extradata limit"

    # Before TAG exists (`tag` is about to create it) the rules compare with every tag on origin.
    # Once it exists they compare with the tags tag_cutoff selects, on any commit: an older
    # release stays verifiable after newer ones exist, while a lower version tagged after a
    # higher one, or a release candidate tagged after its final, fails. The other tags are not
    # verified (the tags before this process are unsigned), so the v* tag ruleset keeps bogus
    # tags out. Tags are immutable under that ruleset: a wrong tag stays, and the next version
    # supersedes it.
    if git rev-parse --verify --quiet "refs/tags/$TAG" >/dev/null; then
        cutoff=$(tag_cutoff "$commit") || exit
    fi
    check_version_order "$cutoff"
}

# Applies the CHANGELOG rules of check-tag step (6) to file $1: exactly one "## [BASE] - date"
# heading, at least one entry, and exactly one release-base marker naming $2. $3 names the
# file in messages.
check_changelog_file() {
    local file=$1 parent=$2 what=$3 stats headings entries markers sha
    stats=$(awk -v base="$BASE" '
        BEGIN { head = "## [" base "] - " }
        index($0, "## [") == 1 {
            insec = 0
            if (index($0, head) == 1 && substr($0, length(head) + 1) ~ /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$/) {
                headings++; insec = 1
            }
            next
        }
        !insec { next }
        /^- / { entries++ }
        /^<!-- release-base: [0-9a-f]+ -->$/ {
            markers++; sha = $0; sub(/^<!-- release-base: /, "", sha); sub(/ -->$/, "", sha)
        }
        END { printf "%d %d %d %s\n", headings, entries, markers, sha }
    ' "$file")
    read -r headings entries markers sha <<EOF
$stats
EOF
    [ "$headings" -ge 1 ] || die 1 "$what has no '## [$BASE] - YYYY-MM-DD' section (run make release-prep)"
    [ "$headings" = 1 ] || die 1 "$what has $headings '## [$BASE]' sections"
    [ "$entries" -ge 1 ] || die 1 "the [$BASE] section of $what has no entries"
    [ "$markers" = 1 ] || die 1 "the [$BASE] section of $what needs exactly one release-base marker (found $markers)"
    [ "${#sha}" = 40 ] || die 1 "the release-base marker in $what is not a full commit id: $sha"
    [ "$sha" = "$parent" ] ||
        die 1 "$what was generated on $sha but the release commit's parent is $parent; something landed in between, re-run make release-prep"
}

# check-tag step (6). The marker equal to COMMIT^ proves nothing landed between
# `make release-prep` and the release commit (merge queue batching included).
check_changelog_at() {
    local commit=$1 parent
    git show "$commit:CHANGELOG.md" >"$WORK/changelog-at.md" 2>/dev/null ||
        die 1 "CHANGELOG.md does not exist at $commit"
    parent=$(git rev-parse --verify --quiet "$commit^") || die 1 "$commit has no parent commit"
    check_changelog_file "$WORK/changelog-at.md" "$parent" "CHANGELOG.md at $commit"
}

# check-tag step (7). MODE "always" needs cast; "if-cast" (the laptop) leaves it to CI.
check_attestation() {
    local commit=$1 mode=$2 rc
    if [ "${RELEASE_ATTESTATION:-}" = skip ]; then
        warn "RELEASE_ATTESTATION=skip: the on-chain attestation of $commit is not checked"
        return 0
    fi
    if ! command -v cast >/dev/null 2>&1; then
        if [ "$mode" = if-cast ]; then
            info "notice: cast is not installed, so the attestation of $commit is not checked here; CI enforces it"
            return 0
        fi
        die 2 "missing required tool: cast (Foundry), needed for the attestation check"
    fi
    [ -f "$ATTEST_SCRIPT" ] || die 2 "$ATTEST_SCRIPT is missing"
    bash "$ATTEST_SCRIPT" "$commit" >&2 && rc=0 || rc=$?
    case "$rc" in
        0) ;;
        2) die 3 "the attestation registry did not answer for $commit" ;;
        *) die 1 "$commit is not attested on chain; on the build server run: git switch --detach $commit && ALLOW_STALE_BASE=1 make attest" ;;
    esac
}

# Full check-tag, steps (1)-(8). ATTEST is "always" or "if-cast". Sets COMMIT, SIGNER,
# SIGNER_FPR, COMPONENTS and MANIFESTS, and prints the summary line. The component list is read
# from the tagged tree only once the tag is known to be signed, on main and attested.
check_tag() {
    local attest=${1:-always}
    fetch_tag
    COMMIT=$(tag_commit "$TAG")
    check_tag_signature
    check_on_main "$COMMIT"
    check_version_at "$COMMIT"
    check_changelog_at "$COMMIT"
    check_attestation "$COMMIT" "$attest"
    load_components "$COMMIT"
    printf 'commit=%s signer=%s fingerprint=%s channel=%s version=%s features=%s prerelease=%s\n' \
        "$COMMIT" "$SIGNER" "$SIGNER_FPR" "$CHANNEL" "$VERSION" "$FEATURES" "$PRERELEASE"
}

# build and publish must run main's copy of this script and of the attestation check, so an
# edited or stale checkout cannot weaken them.
check_scripts_from_main() {
    local mine theirs file
    if [ "${RELEASE_ALLOW_LOCAL_SCRIPTS:-}" = 1 ]; then
        warn "RELEASE_ALLOW_LOCAL_SCRIPTS=1: not comparing etc/release.sh and $ATTEST_SCRIPT with $MAIN_REF"
        return 0
    fi
    require_main_ref
    for file in etc/release.sh "$ATTEST_SCRIPT"; do
        if [ "$file" = etc/release.sh ]; then
            mine=$(git hash-object -- "$SELF") || die 2 "cannot read $SELF"
        else
            mine=$(git hash-object -- "$file") ||
                die 2 "$file is missing from this checkout; update it or set RELEASE_ALLOW_LOCAL_SCRIPTS=1"
        fi
        theirs=$(git rev-parse --verify --quiet "$MAIN_REF:$file") || theirs=missing
        [ "$mine" = "$theirs" ] ||
            die 2 "$file differs from $MAIN_REF; update the checkout or set RELEASE_ALLOW_LOCAL_SCRIPTS=1"
    done
}

# ---------------------------------------------------------------------------------------------
# The GitHub release, or RELEASE_DIR in a dry run

# After a failed gh call: missing auth is a precondition (2). An HTTP 4xx answer other than a
# rate limit is final: 422 is a refused change (1), the rest a precondition such as the token's
# permissions or the repository name (2). Only transport errors, 5xx and rate limits are
# retried, three attempts in all, then exit 3.
gh_failed() {
    local rc=$1 attempt=$2 what=$3
    cat "$WORK/gh.err" >&2
    if [ "$rc" = 4 ]; then die 2 "$what: gh is not authenticated (gh auth login, or GH_TOKEN in CI)"; fi
    if ! grep -q -i -e 'HTTP 429' -e 'rate limit' "$WORK/gh.err"; then
        if grep -q 'HTTP 422' "$WORK/gh.err"; then die 1 "$what: GitHub refused the request (HTTP 422)"; fi
        if grep -q 'HTTP 4[0-9][0-9]' "$WORK/gh.err"; then
            die 2 "$what: GitHub refused the request; check the token's permissions and RELEASE_REPO"
        fi
    fi
    [ "$attempt" -lt 3 ] || die 3 "$what failed after 3 attempts"
    sleep 5
}

# gh with retries; stdout passes through.
gh_run() {
    local attempt=1 out rc
    need gh
    while :; do
        out=$(gh "$@" 2>"$WORK/gh.err") && rc=0 || rc=$?
        if [ "$rc" = 0 ]; then
            if [ -n "$out" ]; then printf '%s\n' "$out"; fi
            return 0
        fi
        gh_failed "$rc" "$attempt" "gh $1 $2"
        attempt=$((attempt + 1))
    done
}

# Sets REL_EXISTS, REL_DRAFT, REL_PRERELEASE, REL_AUTHOR, REL_ASSETS (one name per line) and
# REL_ASSET_IDS. GitHub gives a replaced asset a new id and digest. A dry run's RELEASE_DIR is
# always a draft created by the bot, and its files' hashes stand in for the ids and digests.
release_state() {
    local out rc attempt=1 file hex
    REL_EXISTS=0 REL_DRAFT='' REL_PRERELEASE='' REL_AUTHOR='' REL_ASSETS='' REL_ASSET_IDS=''
    if [ "$DRY_RUN" = 1 ]; then
        [ -n "${RELEASE_DIR:-}" ] || die 2 "dry run: set RELEASE_DIR to the directory that stands in for the release"
        [ -d "$RELEASE_DIR" ] || return 0
        REL_EXISTS=1 REL_DRAFT=true REL_PRERELEASE=$PRERELEASE REL_AUTHOR=$BOT_LOGIN
        for file in "$RELEASE_DIR"/*; do
            [ -f "$file" ] || continue
            hex=$(sha256_hex "$file")
            REL_ASSETS="$REL_ASSETS${file##*/}"$'\n'
            REL_ASSET_IDS="$REL_ASSET_IDS${file##*/}"$'\t-\tsha256:'"$hex"$'\n'
        done
        REL_ASSET_IDS=$(printf '%s' "$REL_ASSET_IDS" | LC_ALL=C sort)
        return 0
    fi
    need gh
    while :; do
        out=$(gh release view "$TAG" --repo "$REPO" --json isDraft,isPrerelease,author,assets \
            --jq '.isDraft, .isPrerelease, .author.login, (.assets[] | [.name, .id, (.digest // "")] | @tsv)' \
            2>"$WORK/gh.err") && rc=0 || rc=$?
        if [ "$rc" = 0 ]; then break; fi
        if grep -q 'release not found' "$WORK/gh.err"; then return 0; fi
        gh_failed "$rc" "$attempt" "gh release view $TAG"
        attempt=$((attempt + 1))
    done
    REL_EXISTS=1
    REL_DRAFT=$(printf '%s\n' "$out" | sed -n 1p)
    REL_PRERELEASE=$(printf '%s\n' "$out" | sed -n 2p)
    REL_AUTHOR=$(printf '%s\n' "$out" | sed -n 3p)
    REL_ASSET_IDS=$(printf '%s\n' "$out" | sed 1,3d | LC_ALL=C sort)
    REL_ASSETS=$(printf '%s\n' "$REL_ASSET_IDS" | cut -f1)
}

has_asset() {
    case $'\n'"$REL_ASSETS"$'\n' in *$'\n'"$1"$'\n'*) return 0 ;; esac
    return 1
}

# check_asset_set REQUIRED... -- OPTIONAL...: the release holds every required asset, and
# nothing that is neither required nor optional.
check_asset_set() {
    local name allowed=$'\n' optional=0 asset
    for name in "$@"; do
        if [ "$name" = -- ]; then
            optional=1
            continue
        fi
        if [ "$optional" = 0 ] && ! has_asset "$name"; then die 1 "release $TAG lacks the asset $name"; fi
        allowed="$allowed$name"$'\n'
    done
    while IFS= read -r asset; do
        [ -n "$asset" ] || continue
        case "$allowed" in *$'\n'"$asset"$'\n'*) ;; *) die 1 "release $TAG has an unexpected asset: $asset" ;; esac
    done <<EOF
$REL_ASSETS
EOF
}

# The release's files other than the signature files, one per line. For each manifest in
# MANIFESTS: the digest files of the components it covers, the manifest, then their tarballs,
# the order in which check_release_asset_set names the first one missing (for the node:
# IMAGE_DIGEST, SHA256SUMS, its tarball). It calls component_set, so its output is always
# captured, and callers pass a die on with `|| exit`.
release_files() {
    local manifest c
    for manifest in $MANIFESTS; do
        for c in $COMPONENTS; do
            component_set "$c"
            if [ "$C_MANIFEST" = "$manifest" ]; then printf '%s\n' "$C_DIGEST_FILE"; fi
        done
        printf '%s\n' "$manifest"
        for c in $COMPONENTS; do
            component_set "$c"
            if [ "$C_MANIFEST" = "$manifest" ]; then printf '%s\n' "$C_TARBALL"; fi
        done
    done
}

# The signature file of each manifest in MANIFESTS, one per line.
release_sigs() {
    local manifest
    for manifest in $MANIFESTS; do printf '%s.asc\n' "$manifest"; done
}

# check_release_asset_set MODE: the release holds release_files and nothing but them and
# release_sigs. MODE signed (verify): every signature file is required too, each named right
# after its manifest; unsigned (sign, the verify of an unsigned draft): signature files are
# optional; build: nothing is required yet, as build writes the files.
check_release_asset_set() {
    local mode=$1 files sigs name
    files=$(release_files) || exit
    sigs=$(release_sigs) || exit
    # The list grows behind MODE, which the shift below drops: bash 3.2 under set -u refuses
    # "$@" while there are no arguments.
    case "$mode" in
        signed | unsigned) ;;
        build) set -- "$@" -- ;;
        *) die 2 "internal error: check_release_asset_set takes signed, unsigned or build, not '$mode'" ;;
    esac
    while IFS= read -r name; do
        set -- "$@" "$name"
        if [ "$mode" = signed ]; then
            case " $MANIFESTS " in *" $name "*) set -- "$@" "$name.asc" ;; esac
        fi
    done <<EOF
$files
EOF
    if [ "$mode" = unsigned ]; then set -- "$@" --; fi
    if [ "$mode" != signed ]; then
        while IFS= read -r name; do set -- "$@" "$name"; done <<EOF
$sigs
EOF
    fi
    shift
    check_asset_set "$@"
}

# check_assets_unchanged WHAT: the release still holds exactly the assets, by name, id and
# digest, that release_state listed for the last verification, and the body that
# check_release_body checked, once it has run. WHAT ends the error message.
check_assets_unchanged() {
    local verified=$REL_ASSET_IDS
    release_state
    [ "$REL_EXISTS" = 1 ] || die 1 "release $TAG is gone; $1"
    [ "$REL_ASSET_IDS" = "$verified" ] || die 1 "the assets of $TAG changed after they were verified; $1"
    if [ -n "$BODY_HEX" ]; then
        release_body "$WORK/body.now"
        [ "$(sha256_hex "$WORK/body.now")" = "$BODY_HEX" ] || die 1 "the body of release $TAG changed after it was checked; $1"
    fi
    if [ "$REL_DRAFT" = false ] && [ "$REL_PRERELEASE" != "$PRERELEASE" ]; then
        die 1 "release $TAG is published with prerelease=$REL_PRERELEASE; expected $PRERELEASE"
    fi
}

# check_fetched_digests DIR: every listed asset, as downloaded into DIR, has the sha256 GitHub
# reports for it, so the files checked are the assets listed (nothing swapped in between).
check_fetched_digests() {
    local dir=$1 name id digest hex
    while IFS=$'\t' read -r name id digest; do
        [ -n "$name" ] || continue
        hex=$(sha256_hex "$dir/$name")
        [ -n "$digest" ] ||
            die 2 "GitHub reported no sha256 digest for $name (asset $id) on release $TAG, so the download cannot be compared with it; update gh if it is old"
        [ "$digest" = "sha256:$hex" ] || die 1 "$name on release $TAG (asset $id) changed between listing and download"
    done <<EOF
$REL_ASSET_IDS
EOF
}

# release_fetch DIR NAME...: copies the named assets into DIR.
release_fetch() {
    local dir=$1 name n attempt=1 rc
    shift
    mkdir -p "$dir"
    if [ "$DRY_RUN" = 1 ]; then
        for name in "$@"; do
            cp "$RELEASE_DIR/$name" "$dir/$name" || die 1 "cannot copy $name from $RELEASE_DIR"
        done
        return 0
    fi
    n=$#
    while [ "$n" -gt 0 ]; do
        set -- "$@" --pattern "$1"
        shift
        n=$((n - 1))
    done
    need gh
    until gh release download "$TAG" --repo "$REPO" --dir "$dir" --clobber "$@" 2>"$WORK/gh.err"; do
        rc=$?
        gh_failed "$rc" "$attempt" "gh release download $TAG"
        attempt=$((attempt + 1))
    done
}

# fetch_release_assets DIR: release_fetch into DIR of each of release_files and release_sigs
# that the release holds. Run after check_release_asset_set, so only signature files may be
# absent.
fetch_release_assets() {
    local files sigs name
    files=$(release_files) || exit
    sigs=$(release_sigs) || exit
    # The names follow DIR in the arguments, as release_fetch takes them.
    while IFS= read -r name; do
        if has_asset "$name"; then set -- "$@" "$name"; fi
    done <<EOF
$files
$sigs
EOF
    # gh release download without a pattern would fetch every asset.
    [ $# -gt 1 ] || die 1 "release $TAG holds none of its release files"
    release_fetch "$@"
}

# release_upload FILE...: replaces same-named assets.
release_upload() {
    local file
    if [ "$DRY_RUN" = 1 ]; then
        for file in "$@"; do
            if [ ! "$file" -ef "$RELEASE_DIR/${file##*/}" ]; then
                cp "$file" "$RELEASE_DIR/${file##*/}" || die 1 "cannot copy $file to $RELEASE_DIR"
            fi
        done
        info "dry run: assets written to $RELEASE_DIR"
        return 0
    fi
    gh_run release upload "$TAG" --repo "$REPO" --clobber "$@" >&2
}

# Markdown on stdin without carriage returns and without trailing blank lines, so a body
# GitHub returns compares equal to the file it was set from.
normalize_body() {
    tr -d '\r' | awk '{ line[NR] = $0 } END { n = NR; while (n > 0 && line[n] ~ /^[ \t]*$/) n--; for (i = 1; i <= n; i++) print line[i] }'
}

# release_body FILE: the release body, normalized, written to FILE; a dry run reads the copy
# release_set_notes keeps next to the artifacts.
release_body() {
    local file
    if [ "$DRY_RUN" = 1 ]; then
        file="$(pwd)/$ARTIFACT_ROOT/$TAG.notes.md"
        [ -f "$file" ] || die 2 "dry run: $file does not exist; run the build first"
    else
        file=$WORK/body.raw
        gh_run release view "$TAG" --repo "$REPO" --json body --jq .body >"$file"
    fi
    normalize_body <"$file" >"$1"
}

# publish: the release body ends with the notes render_notes writes for COMMIT and D, line for
# line; anything above them is the operator notes a maintainer added after the build, which are
# shown for review. Sets BODY_HEX, so check_assets_unchanged also catches a later edit.
check_release_body() {
    local want=$WORK/body.want have=$WORK/body.have n_want n_have
    render_notes "$D" >"$WORK/body.rendered"
    normalize_body <"$WORK/body.rendered" >"$want"
    release_body "$have"
    n_want=$(awk 'END { print NR }' "$want")
    n_have=$(awk 'END { print NR }' "$have")
    if [ "$n_have" -lt "$n_want" ] || ! tail -n "$n_want" "$have" | cmp -s - "$want"; then
        die 1 "the body of release $TAG does not end with the notes generated for $COMMIT and $D; re-run the tag's release workflow (gh run rerun <run-id>) to write them again, add the operator notes above them, and publish again"
    fi
    if [ "$n_have" -gt "$n_want" ]; then
        info "operator notes above the generated release notes:"
        head -n "$((n_have - n_want))" "$have" | sed 's/^/  | /' >&2
    else
        info "the release body is the generated release notes, with no operator notes"
    fi
    BODY_HEX=$(sha256_hex "$have")
}

# publish on a release that is public already, after a publish that stopped before the alias
# step: its body may carry a warning added since, and the draft job no longer writes the notes
# of a published release, so the body is recorded as it is (BODY_HEX, which
# check_assets_unchanged compares) and not compared with the generated notes.
record_release_body() {
    release_body "$WORK/body.have"
    BODY_HEX=$(sha256_hex "$WORK/body.have")
    info "release $TAG is public already: its body is kept as it is, not compared with the generated notes"
}

# The release body; a dry run keeps it next to the artifacts instead.
release_set_notes() {
    local dest
    if [ "$DRY_RUN" = 1 ]; then
        dest="$(pwd)/$ARTIFACT_ROOT/$TAG.notes.md"
        mkdir -p "${dest%/*}"
        cp "$1" "$dest"
        info "dry run: release body written to $dest"
        return 0
    fi
    gh_run release edit "$TAG" --repo "$REPO" --notes-file "$1" >&2
}

# Empties the local artifact dir $1. In a dry run the stand-in release must be neither that
# dir nor inside it; both are compared as physical paths at the moment of deletion, since
# init_env's check could be outdated by a symlink created since.
clear_release_dir() {
    local rd=$1 here stand_in
    if [ "$DRY_RUN" = 1 ] && [ -n "${RELEASE_DIR:-}" ]; then
        here=$(physical_dir "$rd") || exit
        stand_in=$(physical_dir "$RELEASE_DIR") || exit
        case "$stand_in/" in
            "$here/"*) die 2 "RELEASE_DIR=$RELEASE_DIR lies inside $rd, which this step empties; move it outside $ARTIFACT_ROOT" ;;
        esac
    fi
    rm -rf "$rd"
}

# ---------------------------------------------------------------------------------------------
# Artifact checks shared by build, sign and verify

# The channel's chain-config files, by repository path, one per line.
chain_config_files() {
    local f
    for f in committee genesis parameters; do printf '%s/%s.yaml\n' "$CHAIN_CONFIGS" "$f"; done
}

# Every name the manifest $1 lists, one per line: the digest file and the tarball of each
# component it covers, and for SHA256SUMS the chain-config files. The chain-config lines carry
# repository paths, so `sha256sum --check` run from the root of a checkout of the tag checks
# the files there. EDGE_SUMS lists no chain-config file: its binaries serve every network.
# It calls component_set, so its output is always captured (a subshell keeps the caller's C_*
# fields), and callers pass a die on with `|| exit`.
manifest_names() {
    local manifest=$1 c
    for c in $COMPONENTS; do
        component_set "$c"
        if [ "$C_MANIFEST" = "$manifest" ]; then printf '%s\n' "$C_DIGEST_FILE" "$C_TARBALL"; fi
    done
    if [ "$manifest" = SHA256SUMS ]; then chain_config_files; fi
}

# Writes the chain-config files as committed at COMMIT under $1, at their repository paths.
# cat-file gives the committed bytes, with no checkout filter or textconv applied.
stage_chain_configs() {
    local dir=$1 path
    while IFS= read -r path; do
        mkdir -p "$dir/${path%/*}"
        git cat-file blob "$COMMIT:$path" >"$dir/$path" 2>/dev/null || die 1 "$path does not exist at $COMMIT"
    done <<EOF
$(chain_config_files)
EOF
}

# "<hex>  <path>" for each chain-config file at COMMIT, staged under $1 first. Its output is
# captured, and bash 3.2 runs $(...) without -e, so a failed hash exits explicitly.
chain_config_sums() {
    local dir=$1 path hex
    stage_chain_configs "$dir"
    while IFS= read -r path; do
        hex=$(sha256_hex "$dir/$path") || exit
        printf '%s  %s\n' "$hex" "$path"
    done <<EOF
$(chain_config_files)
EOF
}

# write_sums DIR MANIFEST: DIR/MANIFEST holds "<hex>  <name>" for every name manifest_names
# lists, C-locale sorted by name, each hashed in DIR; for SHA256SUMS the chain-config files as
# committed at COMMIT are staged into DIR first.
write_sums() {
    local dir=$1 manifest=$2 names name hex lines=''
    names=$(manifest_names "$manifest") || exit
    if [ "$manifest" = SHA256SUMS ]; then stage_chain_configs "$dir"; fi
    while IFS= read -r name; do
        hex=$(sha256_hex "$dir/$name") || exit
        lines="$lines$hex  $name"$'\n'
    done <<EOF
$names
EOF
    printf '%s' "$lines" | LC_ALL=C sort -k2 >"$dir/$manifest"
}

# check_sums_shape FILE MANIFEST: FILE is in the strict two-space format and names exactly what
# manifest_names lists for MANIFEST, each once. For SHA256SUMS that is IMAGE_DIGEST, the
# tarball and the channel's three chain-config files; for EDGE_SUMS, the digest file and the
# tarball of each other component the tag ships.
check_sums_shape() {
    local sums=$1 manifest=$2 names want
    awk '!/^[0-9a-f]+  [^ ]+$/ || length($1) != 64 { bad = 1 } END { exit bad }' "$sums" ||
        die 1 "$manifest has a malformed line"
    names=$(awk '{ print $2 }' "$sums" | LC_ALL=C sort)
    want=$(manifest_names "$manifest") || exit
    want=$(printf '%s\n' "$want" | LC_ALL=C sort)
    if [ "$names" != "$want" ]; then
        if [ "$manifest" = SHA256SUMS ]; then
            die 1 "SHA256SUMS must name exactly IMAGE_DIGEST, $TARBALL and the committee, genesis and parameters files in $CHAIN_CONFIGS"
        fi
        die 1 "$manifest must name exactly $(printf '%s\n' "$want" | awk '{ printf "%s%s", sep, $0; sep = ", " } END { print "" }')"
    fi
}

# check_sums DIR MANIFEST: DIR/MANIFEST has the right shape, and every hash matches the file
# beside it in DIR; for SHA256SUMS the chain-config files as committed at COMMIT, staged into
# DIR.
check_sums() {
    local dir=$1 manifest=$2
    check_sums_shape "$dir/$manifest" "$manifest"
    if [ "$manifest" = SHA256SUMS ]; then stage_chain_configs "$dir"; fi
    (cd "$dir" && sha256_check "$manifest") >&2 || die 1 "$manifest does not match the files"
}

# $1 is the digest file of the component that component_set chose (the node's IMAGE_DIGEST). It
# must hold exactly one line "C_IMAGE@sha256:<64 hex>", where C_IMAGE is the component's image
# (for the node, the package of the tag's channel, or the dry run's RELEASE_IMAGE): a mainnet
# tag never names an image in tn-testnet, an adiri tag never one in tn-mainnet, no digest file
# names the package of another component or any other repository, and the digest is well
# formed. Every refusal has one shape: what the file holds, why only one package fits (for the
# node, the tag's channel; any other component has one package for every channel) and the
# expected line. $2, when given, replaces the advice at the end of the refusal.
check_digest_package() {
    local file=$1 advice=${2:-rebuild the draft with make release-build TAG=$TAG, or report it if the release is published} lines line repo digest problem=
    lines=$(awk 'END { print NR }' "$file") || die 1 "cannot read $file"
    line=$(cat "$file") || die 1 "cannot read $file"
    repo=${line%%@*}
    digest=${line#*@}
    if [ "$lines" != 1 ]; then
        problem="holds $lines lines"
    elif [ "$repo" = "$line" ]; then
        problem="names '$line', which is not an image by digest"
    elif [ "$repo" != "$C_IMAGE" ]; then
        problem="names an image in $repo"
    elif ! [[ "$digest" =~ $DIGEST_RE ]]; then
        problem="names $C_IMAGE with the malformed digest '$(printf '%s' "$digest" | tr -c '[:print:]' '?')'"
    fi
    [ -n "$problem" ] || return 0
    if [ "$C_NAME" = node ]; then
        die 1 "$C_DIGEST_FILE $problem, but $TAG is on the $CHANNEL channel, whose images are published only in $C_IMAGE (expected the one line $C_IMAGE@sha256:<64 hex>); $advice"
    fi
    die 1 "$C_DIGEST_FILE $problem, but $C_NAME images are published only in $C_IMAGE, for every channel (expected the one line $C_IMAGE@sha256:<64 hex>); $advice"
}

# check_sums_package DIR MANIFEST [ADVICE]: sign before it signs, verify before it counts
# signatures. For each component MANIFEST covers, MANIFEST in DIR lists its digest file exactly
# once, that line hashes the digest file beside it, and the file names an image in the
# component's package. A maintainer signature therefore never covers an image in the other
# network's package. ADVICE is passed on to check_digest_package. It calls component_set, so
# the caller's C_* fields do not survive it.
check_sums_package() {
    local dir=$1 manifest=$2 advice=${3:-} c listed have
    for c in $COMPONENTS; do
        component_set "$c"
        [ "$C_MANIFEST" = "$manifest" ] || continue
        listed=$(awk -v f="$C_DIGEST_FILE" '$2 == f { n++; h = $1 } END { if (n == 1) print h }' "$dir/$manifest") ||
            die 1 "cannot read $dir/$manifest"
        have=$(sha256_hex "$dir/$C_DIGEST_FILE") || exit
        if [ -z "$listed" ] || [ "$listed" != "$have" ]; then
            die 1 "$manifest does not list the $C_DIGEST_FILE beside it exactly once with its hash"
        fi
        check_digest_package "$dir/$C_DIGEST_FILE" "$advice"
    done
}

# The digest file of the component that component_set chose, in DIR $1, is one line
# "C_IMAGE@sha256:<hex>" and the registry still serves C_IMAGE:TAG at that digest. Sets C_D and
# records it; for the node, D too.
check_image_digest_file() {
    local file=$1/$C_DIGEST_FILE line reg
    check_digest_package "$file"
    line=$(cat "$file")
    C_D=${line#"$C_IMAGE@"}
    reg=$(registry_digest "$C_IMAGE" "$TAG")
    [ "$reg" = "$C_D" ] || die 1 "$C_DIGEST_FILE says $C_D but the registry serves $C_IMAGE:$TAG as ${reg:-nothing}"
    record_digest "$C_NAME" "$C_D"
    if [ "$C_NAME" = node ]; then D=$C_D; fi
}

# The tarball $1 of the component that component_set chose lists exactly the four files under
# its top-level directory C_TARDIR (an exact comparison, so nothing absolute and no ".."): the
# binary C_TAR_BIN and the three license files, and they extract as regular files into $2.
check_tarball() {
    local tarball=$1 dest=$2 dir=$C_TARDIR listing want f
    listing=$(tar -tzf "$tarball") || die 1 "cannot list $C_TARBALL"
    listing=$(printf '%s\n' "$listing" | awk -v d="$dir/" '$0 != d' | LC_ALL=C sort)
    want=$(printf '%s\n' "$dir/LICENSE-APACHE" "$dir/LICENSE-MIT" "$dir/NOTICE" "$dir/$C_TAR_BIN" | LC_ALL=C sort)
    [ "$listing" = "$want" ] ||
        die 1 "$C_TARBALL must hold exactly $C_TAR_BIN, LICENSE-APACHE, LICENSE-MIT and NOTICE under $dir/"
    mkdir -p "$dest"
    tar -xzf "$tarball" -C "$dest" || die 1 "cannot extract $C_TARBALL"
    for f in "$C_TAR_BIN" LICENSE-APACHE LICENSE-MIT NOTICE; do
        if [ ! -f "$dest/$dir/$f" ] || [ -L "$dest/$dir/$f" ]; then die 1 "$dir/$f in $C_TARBALL is not a regular file"; fi
    done
    [ -x "$dest/$dir/$C_TAR_BIN" ] || die 1 "$dir/$C_TAR_BIN in $C_TARBALL is not executable"
}

# Copies /usr/local/bin/C_BIN out of image $1 to $2, after checking that the image runs that
# file: the ENTRYPOINT and CMD of the component's Dockerfile (C_CONFIG; for the node no
# ENTRYPOINT and CMD ["telcoin","node"], as in etc/Dockerfile), and no C_BIN in
# /usr/local/sbin, the one PATH entry ahead of /usr/local/bin. Another component's image in
# this component's package fails on its ENTRYPOINT and CMD.
# For any component but the node, these two refusals end with advice, as check_version_output's
# do: $3 when given, by default the build's. The node's texts are fixed and carry none.
image_binary() {
    local ref=$1 dest=$2 ctr cfg
    local advice=${3:-$TAG cannot be released; correct $C_DOCKERFILE on main and tag a new release commit} suffix=
    if [ "$C_NAME" != node ]; then suffix="; $advice"; fi
    cfg=$(docker image inspect --format '{{json .Config.Entrypoint}} {{json .Config.Cmd}}' "$ref") ||
        die 1 "docker image inspect $ref failed"
    [ "$cfg" = "$C_CONFIG" ] ||
        die 1 "$ref has ENTRYPOINT and CMD $cfg; expected $C_CONFIG_TEXT$suffix"
    ctr=$(docker create --platform linux/amd64 "$ref") || die 1 "docker create $ref failed"
    CONTAINERS="$CONTAINERS$ctr"$'\n'
    if docker cp "$ctr:/usr/local/sbin/$C_BIN" - >/dev/null 2>&1; then
        die 1 "$ref has a second $C_BIN in /usr/local/sbin, which PATH finds first$suffix"
    fi
    docker cp "$ctr:/usr/local/bin/$C_BIN" "$dest" >/dev/null || die 1 "cannot copy /usr/local/bin/$C_BIN out of $ref"
    docker rm "$ctr" >/dev/null 2>&1 || true
}

# verify step (i): the image's long version names this release's version, commit and channel
# (crates/telcoin-network-cli/src/version.rs). "Build Features" lists the cli crate's cargo
# features: a mainnet release lists none; an Adiri release lists adiri, and may list faucet,
# which the adiri feature implies; anything else is refused.
# A component with a C_VERSION_NAME prints two exact lines instead (check_exact_version). Its
# refusals end with advice, $2 when given: by default the advice for the build, which checks
# the image it built from the tag before pushing it, so a refusal there is the tag's own.
check_version_output() {
    local ref=$1 out version sha features n_lines feature has_adiri=no
    local advice=${2:-$TAG cannot be released; correct $C_DOCKERFILE or the $C_VERSION_NAME crate on main and tag a new release commit}
    # --entrypoint: run the very file image_binary copied, not whatever ENTRYPOINT or PATH picks.
    out=$(docker run --rm --network none --platform linux/amd64 \
        --entrypoint "/usr/local/bin/$C_BIN" "$ref" --version) ||
        die 1 "$ref: /usr/local/bin/$C_BIN --version failed"
    if [ -n "$C_VERSION_NAME" ]; then
        check_exact_version "$ref" "$out" "$advice"
        return
    fi
    # clap starts the first line with the program name: "telcoin-network-cli Version: X.Y.Z".
    version=$(printf '%s\n' "$out" | sed -n 's/^\([^ ]* \)\{0,1\}Version: //p')
    sha=$(printf '%s\n' "$out" | sed -n 's/^Commit SHA: //p')
    n_lines=$(printf '%s\n' "$out" | awk '/^Build Features:/ { n++ } END { print n + 0 }')
    features=$(printf '%s\n' "$out" | sed -n 's/^Build Features:[[:space:]]*//p')
    [ "$version" = "$VERSION" ] || die 1 "$ref reports 'Version: $version'; expected $VERSION"
    if [ "${#sha}" -lt 7 ]; then die 1 "$ref reports 'Commit SHA: $sha'; expected $COMMIT"; fi
    case "$COMMIT" in
        "$sha"*) ;;
        *) die 1 "$ref reports 'Commit SHA: $sha'; expected $COMMIT" ;;
    esac
    [ "$n_lines" = 1 ] || die 1 "$ref prints $n_lines 'Build Features:' lines; expected one"
    while IFS= read -r feature; do
        [ -n "$feature" ] || continue
        if [ "$CHANNEL" = mainnet ]; then
            die 1 "$ref is a mainnet release but was built with cargo features (Build Features: $features); a mainnet release has none"
        fi
        case "$feature" in
            adiri) has_adiri=yes ;;
            faucet) ;;
            *) die 1 "$ref is an Adiri release but was built with the feature $feature (Build Features: $features); expected adiri, and faucet at most" ;;
        esac
    done <<EOF
$(printf '%s\n' "$features" | awk -F '[, \t]+' '{ for (i = 1; i <= NF; i++) if ($i != "") print $i }')
EOF
    if [ "$CHANNEL" = adiri ] && [ "$has_adiri" = no ]; then
        die 1 "$ref was built without the adiri feature (Build Features: $features)"
    fi
    info "$ref: Version $version, Commit SHA $sha, Build Features ${features:-none}"
}

# check_exact_version REF OUT ADVICE: OUT, the --version of the component that component_set
# chose, is exactly "C_VERSION_NAME Version: VERSION" then "Commit SHA: COMMIT"
# (bin/<component>/src/version.rs), the two lines the release notes tell operators to expect.
# Stricter than the node's rule on purpose: the Dockerfile is given the full commit, so no
# prefix passes, and only the node's version lists cargo features. A node image put in this
# component's package fails on the first line.
check_exact_version() {
    local ref=$1 out=$2 advice=$3 want_version want_sha first n line
    want_version="$C_VERSION_NAME Version: $VERSION"
    want_sha="Commit SHA: $COMMIT"
    first=$(printf '%s\n' "$out" | sed -n 1p)
    [ "$first" = "$want_version" ] ||
        die 1 "$ref reports '$(printable "$first")' on the first line of --version; expected '$want_version'; $advice"
    n=$(printf '%s\n' "$out" | awk '/^Build Features:/ { n++ } END { print n + 0 }')
    [ "$n" = 0 ] ||
        die 1 "$ref prints a 'Build Features:' line; expected none, as only the node's --version lists cargo features; $advice"
    n=$(printf '%s\n' "$out" | awk '/^Commit SHA:/ { n++ } END { print n + 0 }')
    [ "$n" = 1 ] || die 1 "$ref prints $n 'Commit SHA:' lines; expected one, '$want_sha'; $advice"
    line=$(printf '%s\n' "$out" | awk '/^Commit SHA:/')
    [ "$line" = "$want_sha" ] ||
        die 1 "$ref reports '$(printable "$line")'; expected '$want_sha', the full commit $TAG points at; $advice"
    if [ "$out" != "$want_version"$'\n'"$want_sha" ]; then
        # Reads to the end (no exit): under pipefail an early exit can fail printf on a full pipe.
        line=$(printf '%s\n' "$out" | awk -v c="$want_sha" 'NR > 1 && $0 != c && !found { print; found = 1 }')
        die 1 "$ref prints '$(printable "$line")' besides its version and commit; expected only '$want_version' and '$want_sha'; $advice"
    fi
    info "$ref: $C_VERSION_NAME Version $VERSION, Commit SHA $COMMIT"
}

# The release files that release_fetch put in DIR, before anyone counts or adds a signature:
# each is the asset GitHub listed, and every manifest matches them (SHA256SUMS also the
# chain-config files at COMMIT). Then for each component: its digest file names what the
# registry serves for TAG, its tarball holds the four files, the tarball's binary is byte for
# byte /usr/local/bin/C_BIN in the image pulled by digest (the node's: telcoin), and that binary
# reports this release's version, commit and features. sign and verify both run it. Records
# every component's digest and sets D. It calls component_set, so the caller's C_* fields do
# not survive it. The build checked every image it pushed, so when image_binary or
# check_version_output refuses one here the draft names another image: their advice is then
# check_digest_package's, not the build's.
check_release_files() {
    local dir=$1 manifest c tar_bin_hex image_bin_hex
    local advice="rebuild the draft with make release-build TAG=$TAG, or report it if the release is published"
    check_fetched_digests "$dir"
    for manifest in $MANIFESTS; do check_sums "$dir" "$manifest"; done
    for c in $COMPONENTS; do
        component_set "$c"
        check_image_digest_file "$dir"
        check_tarball "$dir/$C_TARBALL" "$dir/x"
        docker pull --platform linux/amd64 "$C_IMAGE@$C_D" >&2 || die 3 "docker pull $C_IMAGE@$C_D failed"
        image_binary "$C_IMAGE@$C_D" "$dir/image-$C_BIN" "$advice"
        image_bin_hex=$(sha256_hex "$dir/image-$C_BIN")
        tar_bin_hex=$(sha256_hex "$dir/x/$C_TARDIR/$C_TAR_BIN")
        [ "$image_bin_hex" = "$tar_bin_hex" ] ||
            die 1 "the tarball's $C_TAR_BIN differs from /usr/local/bin/$C_BIN in $C_IMAGE@$C_D"
        check_version_output "$C_IMAGE@$C_D" "$advice"
    done
}

# ---------------------------------------------------------------------------------------------
# Build and publish steps of the component that component_set chose

# build: builds C_IMAGE:TAG from C_DOCKERFILE in the tag's worktree BUILD_WT, without cache or
# provenance, labelled with TAG and COMMIT. The cargo features select the node's network, so
# only the node gets CARGO_FEATURES; every image gets GIT_SHA.
build_image() {
    set -- --builder "$RELEASE_BUILDER" --platform linux/amd64 -f "$BUILD_WT/$C_DOCKERFILE"
    if [ "$C_NAME" = node ]; then
        info "building $C_IMAGE:$TAG (features: ${FEATURES:-none})"
        set -- "$@" --build-arg "CARGO_FEATURES=$FEATURES"
    else
        info "building $C_IMAGE:$TAG"
    fi
    docker buildx build "$@" --build-arg "GIT_SHA=$COMMIT" \
        --label "org.opencontainers.image.version=$TAG" \
        --label "org.opencontainers.image.revision=$COMMIT" \
        --provenance=false --sbom=false --pull --no-cache --load \
        -t "$C_IMAGE:$TAG" "$BUILD_WT" >&2 ||
        die 3 "docker buildx build failed; a failed download (base image, apt, crates.io) is the usual cause, so run make release-build again, and report it if the output above shows a build error instead"
}

# pack_tarball RD STAGE: build. STAGE/C_TARDIR holds the binary C_TAR_BIN, which the caller
# copied out of the image with image_binary, so the tarball carries the very bytes that are in
# the image. Adds the license files at TAG and packs the directory into RD/C_TARBALL with fixed
# modes, owners, order and mtime (COMMIT's committer date), so the same tag packs the same bytes.
pack_tarball() {
    local rd=$1 stage=$2 dir=$C_TARDIR mtime
    cp "$BUILD_WT/LICENSE-APACHE" "$BUILD_WT/LICENSE-MIT" "$BUILD_WT/NOTICE" "$stage/$dir/" ||
        die 1 "license files are missing at $TAG"
    chmod 0755 "$stage/$dir" "$stage/$dir/$C_TAR_BIN"
    chmod 0644 "$stage/$dir/LICENSE-APACHE" "$stage/$dir/LICENSE-MIT" "$stage/$dir/NOTICE"
    mtime=$(git log -1 --format=%ct "$COMMIT")
    LC_ALL=C tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@$mtime" --format=gnu \
        -cf - -C "$stage" "$dir" | gzip -n -9 >"$rd/$C_TARBALL" || die 1 "packaging $C_TARBALL failed"
}

# publish: points C_IMAGE:ALIAS at C_IMAGE@C_D, unless it points there already. The caller has
# checked that the registry serves C_IMAGE:TAG at C_D, so the alias moves only inside the
# component's package and only to the image that package serves for TAG, never to an image of
# the other network.
move_alias() {
    local current
    current=$(registry_digest "$C_IMAGE" "$ALIAS")
    if [ "$current" = "$C_D" ]; then
        info "$C_IMAGE:$ALIAS already points at $C_D"
        return 0
    fi
    # --prefer-index=false copies the manifest as is, so the alias keeps digest C_D.
    docker buildx imagetools create --prefer-index=false --tag "$C_IMAGE:$ALIAS" "$C_IMAGE@$C_D" >&2 ||
        die 3 "cannot move $C_IMAGE:$ALIAS (if docker reported unauthorized or denied, run make docker-login and publish again)"
    current=$(registry_digest "$C_IMAGE" "$ALIAS")
    [ "$current" = "$C_D" ] || die 1 "$C_IMAGE:$ALIAS points at ${current:-nothing}, expected $C_D"
}

# ---------------------------------------------------------------------------------------------
# Release body

# Body of the "## [BASE]" section of the CHANGELOG on stdin: heading and HTML comments
# removed, surrounding blank lines trimmed.
section_body() {
    awk -v base="$BASE" '
        BEGIN { head = "## [" base "] - " }
        index($0, "## [") == 1 { if (insec) done = 1; else if (!done && index($0, head) == 1) insec = 1; next }
        !insec || done { next }
        {
            line = $0; out = ""
            while (line != "") {
                if (incomment) {
                    p = index(line, "-->")
                    if (p == 0) { line = "" } else { line = substr(line, p + 3); incomment = 0 }
                } else {
                    p = index(line, "<!--")
                    if (p == 0) { out = out line; line = "" } else { out = out substr(line, 1, p - 1); line = substr(line, p + 4); incomment = 1 }
                }
            }
            if (out != $0 && out ~ /^[ \t]*$/) next
            lines[++n] = out
        }
        END {
            first = 1; while (first <= n && lines[first] ~ /^[ \t]*$/) first++
            last = n; while (last >= first && lines[last] ~ /^[ \t]*$/) last--
            for (i = first; i <= last; i++) print lines[i]
        }
    '
}

# Renders the release body for TAG at COMMIT to stdout; $1 is the node's image digest, if
# known, and the other components' digests are the ones DIGESTS holds. It calls component_set
# for the node, so the caller's C_* fields do not survive it.
render_notes() {
    local digest=$1 body handle fpr digest_cell features_cell alias_cell cc_lines
    component_set node
    git show "$COMMIT:CHANGELOG.md" >"$WORK/notes-changelog.md" 2>/dev/null ||
        die 1 "CHANGELOG.md does not exist at $COMMIT"
    body=$(section_body <"$WORK/notes-changelog.md")
    [ -n "$body" ] || die 1 "CHANGELOG.md at $COMMIT has no entries for [$BASE]"
    if [ -n "$RC" ]; then
        printf "Release candidate %s for \`%s\`. Prerelease; publishing it moves no image alias.\n\n" "$RC" "$BASE"
    fi
    printf '%s\n\n' "$body"

    digest_cell="added by \`make release-build\`"
    if [ -n "$digest" ]; then digest_cell="\`$digest\`"; fi
    features_cell=none
    if [ -n "$FEATURES" ]; then features_cell="\`$FEATURES\`"; fi
    alias_cell="none: a release candidate moves no image alias"
    if [ -n "$ALIAS" ]; then
        alias_cell="publishing moves \`$C_IMAGE:$ALIAS\` to this image when it is the newest $CHANNEL release"
    fi
    printf '## Artifacts\n\n| Item | Value |\n|---|---|\n'
    printf '| %s | %s |\n' \
        Channel "\`$CHANNEL\`" \
        Commit "\`$COMMIT\`" \
        "Cargo features" "$features_cell" \
        Image "\`$C_IMAGE:$TAG\`" \
        "Image digest" "$digest_cell" \
        Tarball "\`$C_TARBALL\` (Linux x86_64)" \
        Alias "$alias_cell"
    printf '\n'

    # The chain-config lines of SHA256SUMS depend only on COMMIT, so the notes list them before
    # the build has run.
    cc_lines=$(chain_config_sums "$WORK/notes-chain-configs") || exit
    sed -e "s|@CHAIN_CONFIGS@|$CHAIN_CONFIGS|g" <<'EOF'
`SHA256SUMS` also lists the network's chain-config files, `committee.yaml`, `genesis.yaml` and `parameters.yaml` in `@CHAIN_CONFIGS@`, as committed at the tag and by their path in the repository:

```text
EOF
    printf '%s\n' "$cc_lines"
    cat <<'EOF'
```

The commands below extract these files from the tag and check them with the release files. To check copies you already have, run `grep '  chain-configs/' /path/to/SHA256SUMS | sha256sum --check` with the verified `SHA256SUMS`, from the directory that holds their `chain-configs/` folder, such as the root of a checkout of the tag; it must print `OK` for all three files.

EOF

    # The verification block is the canonical one (the install page's Path A), with TAG and REPO
    # filled in. The heredocs are quoted, so $TAG, $2 and $NF reach the reader as written; only
    # the @NAME@ placeholders are substituted.
    sed -e "s|@TAG@|$TAG|g" -e "s|@REPO@|$REPO|g" <<'EOF'
## Verify

Check the signed checksums before you extract or run anything. In a new, empty directory, run these commands in one shell; the first one stops on a directory that is not empty, so no file from an earlier download is checked in place of this release's. They take the maintainer keys from `main`, never from the tag, fetch the tag by its full name, extract the chain-config files from it, download the release files, stopping at the first download that fails, check the signature over `SHA256SUMS`, and check the hashes:

```sh
TAG=@TAG@
REPO=@REPO@
BASE="https://github.com/$REPO/releases/download/$TAG"
if [ -n "$(ls -A)" ]; then echo "Run these commands in a new, empty directory" >&2; false; fi &&
git clone --quiet --depth 1 --branch main "https://github.com/$REPO.git" tn-main &&
git -C tn-main submodule update --init --depth 1 -- release &&
git -C tn-main fetch --quiet --depth 1 origin "refs/tags/$TAG:refs/tags/$TAG" &&
git -C tn-main archive "refs/tags/$TAG" chain-configs | tar -xf - &&
for k in tn-main/release/keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg &&
gpg --show-keys --with-fingerprint tn-main/release/keys/*.asc &&
curl -fsSL --fail-early --remote-name-all "$BASE/SHA256SUMS" "$BASE/SHA256SUMS.asc" "$BASE/IMAGE_DIGEST" "$BASE/telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz" &&
gpgv --status-fd 1 --keyring ./tn-release-keys.gpg SHA256SUMS.asc SHA256SUMS |
awk '$2 ~ /^(BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { bad = 1 } $2 ~ /^(GOODSIG|BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { s = $2; print $2 } $2 == "VALIDSIG" && s == "GOODSIG" { print "signed by primary key " $NF; n++ } END { exit (n && !bad) ? 0 : 1 }' &&
sha256sum --check SHA256SUMS &&
echo "Signature and hashes verified"
```

If the last line `Signature and hashes verified` is missing, the files are not verified; stop, do not extract or run anything from this release, and report it.

Then check the key fingerprints: every primary key fingerprint `gpg --show-keys` printed, including the one after `signed by primary key`, must appear in the key table of `tn-main/release/KEYS.md` (the same `main` checkout) with a Status that is not `revoked`, and in `https://github.com/<handle>.gpg`, where `<handle>` is the key file's name without `.asc`. Stop on any mismatch.

Maintainer release keys on `main` when these notes were written:

| Maintainer | Primary key fingerprint |
|---|---|
EOF
    while IFS=$'\t' read -r handle fpr; do
        if [ -n "$handle" ]; then printf '| @%s | %s |\n' "$handle" "\`$fpr\`"; fi
    done <<EOF
$ALLOW_LIST
EOF
    sed -e "s|@TAG@|$TAG|g" -e "s|@REPO@|$REPO|g" -e "s|@VERSION@|$VERSION|g" -e "s|@IMAGE@|$C_IMAGE|g" \
        -e "s|@CHANNEL@|$CHANNEL|g" <<'EOF'

Tarball, only after the last line was `Signature and hashes verified` and the fingerprints matched:

```sh
tar -xzf "telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz" &&
cd "telcoin-network-$TAG-x86_64-unknown-linux-gnu" &&
./telcoin-network --version &&
git -C ../tn-main rev-parse "refs/tags/$TAG^{commit}"
```

The first `--version` line is `telcoin-network-cli Version: @VERSION@`. `Commit SHA:` must be the commit that `git rev-parse` printed. `Build Features:` must be empty for a mainnet tag; for an `-adiri` tag it lists `adiri`, and may also list `faucet`, which the `adiri` feature implies, and nothing else.

Docker image: it needs only `SHA256SUMS`, `SHA256SUMS.asc`, `IMAGE_DIGEST` and the chain-config files. In a new, empty directory, run the block above without the tarball; its last check drops the tarball's line from `SHA256SUMS` with `grep`, so `sha256sum` checks every other file and fails when one of them is missing:

```sh
TAG=@TAG@
REPO=@REPO@
BASE="https://github.com/$REPO/releases/download/$TAG"
if [ -n "$(ls -A)" ]; then echo "Run these commands in a new, empty directory" >&2; false; fi &&
git clone --quiet --depth 1 --branch main "https://github.com/$REPO.git" tn-main &&
git -C tn-main submodule update --init --depth 1 -- release &&
git -C tn-main fetch --quiet --depth 1 origin "refs/tags/$TAG:refs/tags/$TAG" &&
git -C tn-main archive "refs/tags/$TAG" chain-configs | tar -xf - &&
for k in tn-main/release/keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg &&
gpg --show-keys --with-fingerprint tn-main/release/keys/*.asc &&
curl -fsSL --fail-early --remote-name-all "$BASE/SHA256SUMS" "$BASE/SHA256SUMS.asc" "$BASE/IMAGE_DIGEST" &&
gpgv --status-fd 1 --keyring ./tn-release-keys.gpg SHA256SUMS.asc SHA256SUMS |
awk '$2 ~ /^(BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { bad = 1 } $2 ~ /^(GOODSIG|BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { s = $2; print $2 } $2 == "VALIDSIG" && s == "GOODSIG" { print "signed by primary key " $NF; n++ } END { exit (n && !bad) ? 0 : 1 }' &&
grep -v -F "  telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz" SHA256SUMS | sha256sum --check &&
echo "Signature and hashes verified"
```

Only after its last line was `Signature and hashes verified` and the fingerprints matched, check the image. This refuses an `IMAGE_DIGEST` that does not name an image in `@IMAGE@` by digest; @CHANNEL@ images are published there and nowhere else:

```sh
IMAGE_REF=$(cat IMAGE_DIGEST)
case "$IMAGE_REF" in
  @IMAGE@@sha256:*) docker pull "$IMAGE_REF" && docker run --rm --network none --entrypoint /usr/local/bin/telcoin "$IMAGE_REF" --version ;;
  *) echo "IMAGE_DIGEST does not name an image in @IMAGE@ by digest: $IMAGE_REF" >&2 ;;
esac
```

Its `--version` output must show the same three lines as the tarball's, with the commit that `git -C tn-main rev-parse "refs/tags/$TAG^{commit}"` prints.

When a check fails, do not extract, install or run anything from this release:

- The last line is not `Signature and hashes verified`: a step failed, and the lines above it say which.
- `BADSIG`: `SHA256SUMS` changed after it was signed. Report it.
- `Run these commands in a new, empty directory`: the directory holds files already. Start again in a new, empty directory.
- `curl` reports an error, such as `404`: a release file did not download, and the block stopped there. Check `TAG`; if it is right, report it.
- `ERRSIG`: the signing key is not in the allowlist on `main`, or `TAG` is wrong. Check `TAG`; if it is right, report it.
- `EXPKEYSIG` or `REVKEYSIG`: the signing key has expired or was revoked, although gpgv still prints `Good signature`. Report it; a release signed before that can no longer be verified against `main`, which is intended.
- `EXPSIG`: the signature itself has expired. Report it.
- Any of `BADSIG`, `ERRSIG`, `EXPSIG`, `EXPKEYSIG` or `REVKEYSIG` stops the block even when another signature in `SHA256SUMS.asc` is good.
- `sha256sum` prints `FAILED`: a file differs from its signed hash. Check `TAG` and run the block again in a new, empty directory; if it fails again, report it.
- `sha256sum` prints `FAILED open or read` for a `chain-configs/` file: `git archive` did not extract it. Check that the tag was fetched; if it was, report it.
- A fingerprint is missing from `tn-main/release/KEYS.md` or GitHub, differs, or has the Status `revoked`. Report it.
- `IMAGE_DIGEST does not name an image in @IMAGE@ by digest`: it names another package, such as the other network's, or no digest. Report it.
- `Commit SHA` is not the tag's commit, or `Build Features` does not match the tag. Report it.
- `exec format error`, or Docker warns that the image platform does not match the host: the release is Linux x86_64 only. Use an x86_64 host, or build from source.
- `docker pull` fails with `denied` or `unauthorized`: the image is not publicly readable. Tell the maintainers.

Report a failed check through the [security policy](https://github.com/@REPO@/blob/main/SECURITY.md).
EOF
    # The other components' part sits between the node's verify section and the closing links.
    # A tag that ships the node alone gets none of it.
    case " $MANIFESTS " in
        *" $EDGE_SUMS "*) (render_edge_notes) || exit ;;
    esac
    cat <<'EOF'

- [Installing a release](https://docs.telcoin.network/getting-started/installing-a-release.html)
- [Release notes](https://docs.telcoin.network/getting-started/release-notes.html)
EOF
}

# The release body's part for the components besides the node, the ones EDGE_SUMS lists: what
# each is, its row of the table, and the blocks that verify its files, which name only the
# components the tag ships. The blocks follow the node's: a new, empty directory, the keyring
# from a fresh clone of main, the same rule over the signatures, the manifest's names checked
# against this tag's edge files and every line of it checked, so neither the node's manifest
# nor one replayed from another release passes. A component's digest is the one DIGESTS holds
# (build and verify record it, draft reads it back from the release), or none before the
# build. It calls component_set, so render_notes runs it in a subshell.
render_edge_notes() {
    local c about digest_cell alias_cell sh_tarball sh_tardir first=1
    cat <<'EOF'

## Edge services

This release also ships the following, built from the same commit as the node:

EOF
    for c in $COMPONENTS; do
        component_set "$c"
        [ "$C_MANIFEST" = "$EDGE_SUMS" ] || continue
        case "$C_NAME" in
            worker-gateway) about="a mostly stateless reverse proxy in front of a worker's JSON-RPC endpoint, which forwards the calls its method policy allows to a ready worker." ;;
            node-record-api) about="a read-only HTTP/JSON directory of the JSON-RPC endpoints that validators advertise, which it reads from their signed node records in the worker DHT." ;;
            *) die 2 "internal error: the release notes do not describe the component '$C_NAME'" ;;
        esac
        printf -- "- \`%s\`: %s\n" "$C_NAME" "$about"
    done
    cat <<'EOF'

Each service's image is in a package of its own, which mainnet and Adiri releases share, so an image's tag says which release it belongs to. The services' files are listed in a second manifest, `SHA256SUMS-edge`, which the same maintainer keys sign in `SHA256SUMS-edge.asc`, and a release needs as many signatures on it as on `SHA256SUMS`. `SHA256SUMS` lists none of these files, so the node's checks above do not cover them.

| Service | Image | Image digest | Tarball | Alias |
|---|---|---|---|---|
EOF
    for c in $COMPONENTS; do
        component_set "$c"
        [ "$C_MANIFEST" = "$EDGE_SUMS" ] || continue
        digest_cell="added by \`make release-build\`"
        if [ -n "$C_D" ]; then digest_cell="\`$C_D\`"; fi
        alias_cell="none: a release candidate moves no image alias"
        if [ -n "$ALIAS" ]; then
            alias_cell="publishing moves \`$C_IMAGE:$ALIAS\` to this image when it is the newest $CHANNEL release"
        fi
        printf '| %s | %s | %s | %s | %s |\n' "\`$C_NAME\`" "\`$C_IMAGE:$TAG\`" "$digest_cell" \
            "\`$C_TARBALL\` (Linux x86_64)" "$alias_cell"
    done

    cat <<'EOF'

### Verify the edge services

Check `SHA256SUMS-edge` before you extract or run any of these files. Run the block in a new, empty directory of its own, not the node's: like the node's block, its first command stops on a directory that is not empty, and it clones `main` and fetches the tag itself, so the keys are the ones on `main` when it runs. It downloads the services' files, stopping at the first download that fails, checks the signatures over `SHA256SUMS-edge` with the node's rule, checks that `SHA256SUMS-edge` names exactly this release's edge files, so that neither the node's `SHA256SUMS` nor another release's manifest passes in its place, and checks every hash in it:

EOF
    edge_verify_block tarball
    cat <<'EOF'

If the last line `Edge signature and hashes verified` is missing, the services' files are not verified; stop, do not extract or run any of them, and report it. Then check the key fingerprints as for the node's block, with this directory's `tn-main`.

Tarballs, only after the last line was `Edge signature and hashes verified` and the fingerprints matched:

```sh
EOF
    for c in $COMPONENTS; do
        component_set "$c"
        [ "$C_MANIFEST" = "$EDGE_SUMS" ] || continue
        sh_tarball=$(shell_tag_name "$C_TARBALL") || exit
        sh_tardir=$(shell_tag_name "$C_TARDIR") || exit
        printf 'tar -xzf "%s" &&\n"./%s/%s" --version &&\n' "$sh_tarball" "$sh_tardir" "$C_TAR_BIN"
    done
    cat <<'EOF'
git -C tn-main rev-parse "refs/tags/$TAG^{commit}"
```

Each `--version` prints these two lines and nothing else, with the commit that `git rev-parse` printed:

```text
EOF
    for c in $COMPONENTS; do
        component_set "$c"
        [ "$C_MANIFEST" = "$EDGE_SUMS" ] || continue
        if [ "$first" = 0 ]; then printf '\n'; fi
        first=0
        printf '%s Version: %s\nCommit SHA: %s\n' "$C_VERSION_NAME" "$VERSION" "$COMMIT"
    done
    cat <<'EOF'
```

Images only: they need only `SHA256SUMS-edge`, `SHA256SUMS-edge.asc` and the `IMAGE_DIGEST-<service>` files. Run the block above without the tarballs, in a new, empty directory of its own. It checks the names in `SHA256SUMS-edge` as above, the tarballs' included, then drops the tarballs' lines with `grep`, so `sha256sum` checks every digest file and fails when one of them is missing:

EOF
    edge_verify_block image
    cat <<'EOF'

Only after its last line was `Edge signature and hashes verified` and the fingerprints matched, check the images. This refuses an `IMAGE_DIGEST-<service>` that does not name an image in that service's package by digest; each service's images are published there and nowhere else:

```sh
EOF
    for c in $COMPONENTS; do
        component_set "$c"
        [ "$C_MANIFEST" = "$EDGE_SUMS" ] || continue
        sed -e "s|@DIGEST_FILE@|$C_DIGEST_FILE|g" -e "s|@IMAGE@|$C_IMAGE|g" -e "s|@BIN@|$C_BIN|g" <<'EOF'
IMAGE_REF=$(cat @DIGEST_FILE@) &&
case "$IMAGE_REF" in
  @IMAGE@@sha256:*) docker pull "$IMAGE_REF" && docker run --rm --network none --entrypoint /usr/local/bin/@BIN@ "$IMAGE_REF" --version ;;
  *) echo "@DIGEST_FILE@ does not name an image in @IMAGE@ by digest: $IMAGE_REF" >&2; false ;;
esac &&
EOF
    done
    sed -e "s|@TAG@|$TAG|g" <<'EOF'
echo "Edge images pulled by digest"
```

Each image's `--version` output must be the same two lines as its tarball's. If the last line is not `Edge images pulled by digest`, do not run the images; the lines above it say which check failed.

When an edge check fails, the node's list above applies, with `SHA256SUMS-edge`, `SHA256SUMS-edge.asc`, `IMAGE_DIGEST-<service>` and `Edge signature and hashes verified` in place of `SHA256SUMS`, `SHA256SUMS.asc`, `IMAGE_DIGEST` and `Signature and hashes verified`, and a `--version` other than the two lines above is a failed check. `SHA256SUMS-edge does not name exactly the edge files of @TAG@` means that the signed manifest lacks one of this release's edge files, names another file, or holds a malformed line, as the node's `SHA256SUMS` or another release's manifest would. Report it.
EOF
}

# $1, a release file name that holds TAG, with TAG written as the shell variable $TAG, which
# is how the verify blocks name the release files.
shell_tag_name() {
    printf '%s\n' "${1%%"$TAG"*}\$TAG${1#*"$TAG"}"
}

# edge_verify_block PATH: a verify block of render_edge_notes, for PATH tarball (every file
# EDGE_SUMS lists) or image (all but the tarballs, whose lines its last check drops by name).
# It runs only in a new, empty directory, where it clones main and fetches the tag as the
# node's block does, so the keyring is main's when it runs and no file of another download is
# checked in place of this release's. A signature over the node's SHA256SUMS, or over another
# release's EDGE_SUMS, is good too, so before any hash it checks check_sums_shape's rule: every
# line is "<64 hex>  <name>", and the names are exactly what manifest_names lists for EDGE_SUMS,
# the tarballs included on both paths. It calls component_set, so it runs only in
# render_edge_notes's subshell.
edge_verify_block() {
    local path=$1 c sh_tarball files drop='' names name want=''
    files="\"\$BASE/$EDGE_SUMS\" \"\$BASE/$EDGE_SUMS.asc\""
    for c in $COMPONENTS; do
        component_set "$c"
        [ "$C_MANIFEST" = "$EDGE_SUMS" ] || continue
        sh_tarball=$(shell_tag_name "$C_TARBALL") || exit
        files="$files \"\$BASE/$C_DIGEST_FILE\""
        if [ "$path" = tarball ]; then files="$files \"\$BASE/$sh_tarball\""; fi
        drop="$drop -e \"  $sh_tarball\""
    done
    # The names in the order the block's `LC_ALL=C sort` gives the manifest's, written as the
    # block writes release files.
    names=$(manifest_names "$EDGE_SUMS") || exit
    names=$(printf '%s\n' "$names" | LC_ALL=C sort)
    while IFS= read -r name; do
        case "$name" in *"$TAG"*) name=$(shell_tag_name "$name") || exit ;; esac
        want="$want \"$name\""
    done <<EOF
$names
EOF
    sed -e "s|@TAG@|$TAG|g" -e "s|@REPO@|$REPO|g" <<'EOF'
```sh
TAG=@TAG@
REPO=@REPO@
BASE="https://github.com/$REPO/releases/download/$TAG"
if [ -n "$(ls -A)" ]; then echo "Run these commands in a new, empty directory" >&2; false; fi &&
git clone --quiet --depth 1 --branch main "https://github.com/$REPO.git" tn-main &&
git -C tn-main submodule update --init --depth 1 -- release &&
git -C tn-main fetch --quiet --depth 1 origin "refs/tags/$TAG:refs/tags/$TAG" &&
for k in tn-main/release/keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg &&
gpg --show-keys --with-fingerprint tn-main/release/keys/*.asc &&
EOF
    printf 'curl -fsSL --fail-early --remote-name-all %s &&\n' "$files"
    sed -e "s|@WANT@|$want|" <<'EOF'
gpgv --status-fd 1 --keyring ./tn-release-keys.gpg SHA256SUMS-edge.asc SHA256SUMS-edge |
awk '$2 ~ /^(BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { bad = 1 } $2 ~ /^(GOODSIG|BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG)$/ { s = $2; print $2 } $2 == "VALIDSIG" && s == "GOODSIG" { print "signed by primary key " $NF; n++ } END { exit (n && !bad) ? 0 : 1 }' &&
if [ "$(awk 'length($1) == 64 && /^[0-9a-f]+  [^ ]+$/ { print $2; next } { print "malformed" }' SHA256SUMS-edge | LC_ALL=C sort)" != "$(printf '%s\n'@WANT@)" ]; then echo "SHA256SUMS-edge does not name exactly the edge files of $TAG" >&2; false; fi &&
EOF
    if [ "$path" = tarball ]; then
        printf 'sha256sum --check %s &&\n' "$EDGE_SUMS"
    else
        printf 'grep -v -F%s %s | sha256sum --check &&\n' "$drop" "$EDGE_SUMS"
    fi
    cat <<'EOF'
echo "Edge signature and hashes verified"
```
EOF
}

# ---------------------------------------------------------------------------------------------
# prep helpers

# Rewrites the version string on the first `version =` line inside [workspace.package]
# (temp file + mv, no sed -i).
set_workspace_version() {
    local tmp=Cargo.toml.release-tmp
    awk -v v="$1" '
        /^\[/ { in_ws = ($0 ~ /^\[workspace\.package\][ \t]*$/) }
        in_ws && !done && /^[ \t]*version[ \t]*=/ { sub(/"[^"]*"/, "\"" v "\""); done = 1 }
        { print }
        END { if (!done) exit 3 }
    ' Cargo.toml >"$tmp" || {
        rm -f "$tmp"
        die 1 "no version line in [workspace.package] of Cargo.toml"
    }
    mv "$tmp" Cargo.toml
}

# Makes the pinned image $1 available locally, pulling it with retries; exit 3 when the
# registry stays unavailable.
pull_image() {
    local attempt=1
    if docker image inspect "$1" >/dev/null 2>&1; then return 0; fi
    until docker pull --quiet "$1" >&2; do
        [ "$attempt" -lt 3 ] || die 3 "docker pull $1 failed after 3 attempts"
        attempt=$((attempt + 1))
        sleep 5
    done
}

# Writes the commits since the previous final release as the "## [BASE]" section to $1, with
# the pinned git-cliff image. The range starts at the nearest final tag of either channel
# reachable from HEAD (the whole history when there is none), not at --unreleased, which
# would start after an ignored rc tag and drop the rc's changes from the final's section.
# A worktree's .git points into the common dir and a shared clone borrows objects through
# alternates, so each of those is mounted read-only at its own path.
cliff_section() {
    local out=$1 top common alt prev
    top=$(pwd)
    common=$(git rev-parse --path-format=absolute --git-common-dir) || die 2 "git rev-parse --git-common-dir failed"
    prev=$(git describe --tags --abbrev=0 --match 'v*' --exclude '*-rc[0-9]*' HEAD 2>/dev/null) || prev=
    set -- -v "$top:$top:ro" -v "$common:$common:ro"
    if [ -f "$common/objects/info/alternates" ]; then
        while IFS= read -r alt; do
            case "$alt" in /*) set -- "$@" -v "$alt:$alt:ro" ;; esac
        done <"$common/objects/info/alternates"
    fi
    set -- "$@" -w "$top" "$GIT_CLIFF_IMAGE" --config cliff.toml --tag "$BASE" --strip all
    if [ -n "$prev" ]; then
        info "CHANGELOG section for $BASE: commits since $prev"
        set -- "$@" "$prev..HEAD"
    else
        info "CHANGELOG section for $BASE: no earlier final release tag, so the whole history"
    fi
    pull_image "$GIT_CLIFF_IMAGE"
    docker run --rm --network none -u "$(id -u):$(id -g)" -e HOME=/tmp "$@" >"$out" ||
        die 1 "git-cliff failed"
}

# Splices section file $1 into CHANGELOG.md right after the anchor line, replacing a top
# section already headed "## [BASE]". A "## [BASE]" further down means the history is wrong
# and is an error.
splice_changelog() {
    local section=$1 rc
    awk -v base="$BASE" -v anchor="$ANCHOR" -v section="$section" '
        function is_base(l) { return index(l, "## [" base "] ") == 1 || l == "## [" base "]" }
        state == "" {
            print
            if ($0 == anchor) { while ((getline s < section) > 0) print s; state = "top" }
            next
        }
        state == "skip" { if (index($0, "## [") != 1) next; state = "rest" }
        state == "top" && index($0, "## [") == 1 { if (is_base($0)) { state = "skip"; next } state = "rest" }
        state == "rest" && is_base($0) { lower = 1 }
        { print }
        END { if (lower) exit 3 }
    ' CHANGELOG.md >"$WORK/CHANGELOG.new" && rc=0 || rc=$?
    case "$rc" in
        0) cat "$WORK/CHANGELOG.new" >CHANGELOG.md ;;
        3) die 1 "CHANGELOG.md already has a [$BASE] section below the top one" ;;
        *) die 1 "cannot rewrite CHANGELOG.md" ;;
    esac
}

# ---------------------------------------------------------------------------------------------
# Subcommands

# validate-tag in CI runs this. The parse goes to its log first, so the log shows the package
# that the tag's image belongs in; once the tag passed, the log shows what each component it
# ships puts on the release.
cmd_check_tag() {
    local c
    print_parse >&2
    info "IMAGE=$IMAGE"
    check_tag always
    for c in $COMPONENTS; do
        component_set "$c"
        info "component $C_NAME: image $C_IMAGE:$TAG, $C_DIGEST_FILE and $C_TARBALL in $C_MANIFEST"
    done
}

# --image-digest is the node's. Nothing here knows another component's digest, so their rows
# say the build adds it; the bodies that build and draft write carry every digest.
cmd_notes() {
    local digest=$1
    if [ -n "$digest" ] && ! [[ "$digest" =~ $DIGEST_RE ]]; then die 2 "--image-digest must be sha256:<64 hex>"; fi
    COMMIT=$(tag_commit "$TAG")
    load_components "$COMMIT"
    load_allowlist
    render_notes "$digest"
}

# gh release create is not idempotent, and it can fail after GitHub created the draft. Look
# again before each retry, so a retry cannot leave two drafts for TAG.
create_draft() {
    local notes=$1 attempt=1 rc
    need gh
    set -- release create "$TAG" --repo "$REPO" --draft --verify-tag --title "$TAG" --notes-file "$notes"
    if [ "$PRERELEASE" = true ]; then set -- "$@" --prerelease; fi
    while :; do
        gh "$@" >&2 2>"$WORK/gh.err" && rc=0 || rc=$?
        if [ "$rc" = 0 ]; then return 0; fi
        gh_failed "$rc" "$attempt" "gh release create $TAG"
        attempt=$((attempt + 1))
        release_state
        if [ "$REL_EXISTS" = 1 ]; then
            [ "$REL_DRAFT" = true ] || die 1 "release $TAG is already published"
            [ "$REL_AUTHOR" = "$BOT_LOGIN" ] || die 1 "the draft for $TAG was created by $REL_AUTHOR, not $BOT_LOGIN"
            info "the draft for $TAG exists after the failed call; not creating it again"
            return 0
        fi
    done
}

# CI only: create the draft, or refresh the body of a draft the bot created earlier.
cmd_draft() {
    local notes=$WORK/notes.md digest c line
    [ "${GITHUB_ACTIONS:-}" = true ] || die 2 "draft runs only in CI (GITHUB_ACTIONS=true)"
    COMMIT=$(tag_commit "$TAG")
    load_components "$COMMIT"
    load_allowlist
    release_state
    if [ "$REL_EXISTS" = 1 ]; then
        [ "$REL_DRAFT" = true ] || die 1 "release $TAG is already published"
        [ "$REL_AUTHOR" = "$BOT_LOGIN" ] || die 1 "the draft for $TAG was created by $REL_AUTHOR, not $BOT_LOGIN"
        # A re-run after the build keeps the digests the build put into the body, and stops on
        # a digest file that is not one well-formed line in its component's package.
        for c in $COMPONENTS; do
            component_set "$c"
            if has_asset "$C_DIGEST_FILE"; then
                release_fetch "$WORK/draft" "$C_DIGEST_FILE"
                check_digest_package "$WORK/draft/$C_DIGEST_FILE"
                line=$(cat "$WORK/draft/$C_DIGEST_FILE")
                record_digest "$C_NAME" "${line#"$C_IMAGE@"}"
            fi
        done
    fi
    digest=$(digest_of node) || exit
    render_notes "$digest" >"$notes"
    if [ "$DRY_RUN" = 1 ]; then
        mkdir -p "$RELEASE_DIR"
        release_set_notes "$notes"
        return 0
    fi
    if [ "$REL_EXISTS" = 1 ]; then
        gh_run release edit "$TAG" --repo "$REPO" --notes-file "$notes" >&2
    else
        create_draft "$notes"
    fi
    info "draft release $TAG is ready"
}

# build server: on a clean checkout of main's tip, bump the version and splice the CHANGELOG
# section. The marker records main's tip so check-tag can prove nothing landed in between.
cmd_prep() {
    local main_sha remote anchors changed name tag_re
    need docker cargo
    [ -z "$(git status --porcelain --untracked-files=no)" ] || die 2 "tracked files have local changes; prep needs a clean checkout of main"
    if [ "$DRY_RUN" != 1 ]; then git_fetch +refs/heads/main:refs/remotes/origin/main; fi
    require_main_ref
    main_sha=$(git rev-parse "$MAIN_REF^{commit}")
    [ "$(git rev-parse HEAD)" = "$main_sha" ] || die 2 "HEAD is not the tip of main ($MAIN_REF = $main_sha)"
    if git rev-parse --verify --quiet "refs/tags/$TAG" >/dev/null; then die 1 "tag $TAG already exists locally"; fi
    if [ "$DRY_RUN" != 1 ]; then
        remote=$(ls_remote_tag "$TAG")
        [ -z "$remote" ] || die 1 "tag $TAG already exists on origin"
    fi
    # The version rules of tag and check-tag, against origin's tags, before anything is written:
    # a release candidate after its final would replace the released section at the top of
    # CHANGELOG.md.
    check_version_order ''
    [ -f cliff.toml ] || die 2 "cliff.toml is missing"
    [ -f CHANGELOG.md ] || die 2 "CHANGELOG.md is missing"
    anchors=$(awk -v a="$ANCHOR" '$0 == a { n++ } END { print n + 0 }' CHANGELOG.md)
    [ "$anchors" = 1 ] || die 1 "CHANGELOG.md must contain the line '$ANCHOR' exactly once (found $anchors)"

    # From here until the checks below pass, a failure restores the three files prep writes, so
    # the next run finds a clean tree.
    PREP_RESTORE=1
    set_workspace_version "$VERSION"
    info "cargo update --workspace"
    cargo update --workspace >&2 ||
        die 3 "cargo update --workspace failed; a failed download from crates.io is the usual cause, so run make release-prep again, and if it fails the same way, the output above shows the cause, such as a version conflict"

    cliff_section "$WORK/section.raw"
    awk -v base="$BASE" -v m="$main_sha" '
        NF == 0 && !started { next }
        { started = 1; print }
        !marked && index($0, "## [" base "] - ") == 1 { print "<!-- release-base: " m " -->"; marked = 1 }
    ' "$WORK/section.raw" >"$WORK/section.md"
    splice_changelog "$WORK/section.md"
    check_changelog_file CHANGELOG.md "$main_sha" CHANGELOG.md
    [ "$(workspace_version <Cargo.toml)" = "$VERSION" ] || die 1 "Cargo.toml does not read back version $VERSION"

    changed=$(git diff --name-only HEAD)
    while IFS= read -r name; do
        case "$name" in
            '' | Cargo.toml | Cargo.lock | CHANGELOG.md) ;;
            *) die 1 "prep changed $name; only Cargo.toml, Cargo.lock and CHANGELOG.md may change" ;;
        esac
    done <<EOF
$changed
EOF
    PREP_RESTORE=0

    # The squash commit's subject is "release: TAG (#PR)"; dots escaped, so v1.2.3 cannot
    # match v1x2x3, and " (#" keeps v1.2.3 from matching v1.2.3-rc1.
    tag_re=$(printf '%s\n' "$TAG" | sed 's/\./\\./g')
    cat "$WORK/section.md"
    cat <<EOF

Next:
  git switch -c release/$TAG
  git commit -am "release: $TAG"
  gh pr create --title "release: $TAG" --body "Version $VERSION and the CHANGELOG section for $BASE."
  Attest the PR head with make attest, then merge it through the queue alone (no batching).
  The queue lands a new squash commit that the PR's attestation does not cover; attest it here:
    git switch main && git pull --ff-only
    RELEASE_SHA="\$(git log -1 --format=%H --grep='^release: $tag_re (#' origin/main)"
    git switch --detach "\$RELEASE_SHA" && ALLOW_STALE_BASE=1 make attest
    git switch main
  Then, on the signing laptop: make release-tag TAG=$TAG
  (with RELEASE_COMMIT=<RELEASE_SHA> if main has moved past the release commit)
EOF
}

# laptop: sign the tag on main's tip (or RELEASE_COMMIT) and push it.
cmd_tag() {
    local target remote rc c
    need gpg
    if [ "$DRY_RUN" != 1 ]; then git_fetch +refs/heads/main:refs/remotes/origin/main; fi
    require_main_ref
    if [ -n "${RELEASE_COMMIT:-}" ]; then
        target=$(git rev-parse --verify --quiet "$RELEASE_COMMIT^{commit}") ||
            die 2 "RELEASE_COMMIT=$RELEASE_COMMIT is not a commit"
    else
        target=$(git rev-parse --verify "$MAIN_REF^{commit}")
    fi
    check_on_main "$target"
    if git rev-parse --verify --quiet "refs/tags/$TAG" >/dev/null; then die 1 "tag $TAG already exists locally"; fi
    if [ "$DRY_RUN" != 1 ]; then
        remote=$(ls_remote_tag "$TAG")
        [ -z "$remote" ] || die 1 "tag $TAG already exists on origin"
    fi
    load_allowlist
    check_version_at "$target"
    check_changelog_at "$target"
    check_attestation "$target" if-cast
    # Before the signature: a tag is immutable once pushed, so a list check-tag would refuse
    # must stop here.
    load_components "$target"
    resolve_signing_key

    CREATED_TAG=$TAG
    git -c gpg.format=openpgp tag -s -u "$SIGN_KEY" -m "Release $TAG" "$TAG" "$target" ||
        die 2 "git tag -s failed: is the YubiKey that holds $SIGN_FPR plugged in, and does gpg --card-status show it?"
    COMMIT=$(tag_commit "$TAG")
    [ "$COMMIT" = "$target" ] || die 1 "the new tag points at $COMMIT, not $target"
    check_tag_signature
    [ "$SIGNER" = "$SIGN_HANDLE" ] || die 1 "the tag verifies as @$SIGNER, expected @$SIGN_HANDLE"
    check_on_main "$COMMIT"

    info "tag      $TAG ($CHANNEL, features: ${FEATURES:-none}, prerelease: $PRERELEASE)"
    info "commit   $COMMIT"
    info "signer   @$SIGNER $SIGNER_FPR"
    for c in $COMPONENTS; do
        component_set "$c"
        info "image    $C_IMAGE:$TAG"
    done
    confirm "Push $TAG to origin?"
    if [ "$DRY_RUN" = 1 ]; then
        info "dry run: not pushing; $TAG stays a local tag"
        CREATED_TAG=
        return 0
    fi
    git push origin "refs/tags/$TAG" 2>"$WORK/push.err" && rc=0 || rc=$?
    cat "$WORK/push.err" >&2
    if [ "$rc" != 0 ]; then
        # A ruleset (GH013), a protected ref or an existing tag is a refusal, not an outage.
        if grep -q -i -e 'rejected\]' -e GH013 -e protected "$WORK/push.err"; then
            die 1 "origin refused $TAG (tag ruleset, protected ref, or the tag exists there); see the git output above"
        fi
        die 3 "git push origin refs/tags/$TAG failed"
    fi
    CREATED_TAG=
    info "pushed $TAG; CI now validates it and creates the draft release"
}

# RELEASE_FIRST_PUSH names the packages whose first image build may push, comma separated.
first_push_named() {
    case ",${RELEASE_FIRST_PUSH:-}," in *",$1,"*) return 0 ;; esac
    return 1
}

# build, before the registry is asked: every entry of RELEASE_FIRST_PUSH is the package of a
# component TAG ships, so a typo stops here and never reads as a package left unnamed. It calls
# component_set, so the caller's C_* fields do not survive it.
check_first_push_names() {
    local packages='' rest name found p c
    [ -n "${RELEASE_FIRST_PUSH:-}" ] || return 0
    for c in $COMPONENTS; do
        component_set "$c"
        packages="$packages${packages:+ }$C_PACKAGE"
    done
    # The trailing comma makes every entry, an empty one too, end in one.
    rest=$RELEASE_FIRST_PUSH,
    while [ -n "$rest" ]; do
        name=${rest%%,*}
        rest=${rest#*,}
        found=no
        for p in $packages; do
            if [ "$p" = "$name" ]; then found=yes; fi
        done
        [ "$found" = no ] || continue
        case "$name" in
            tn-mainnet | tn-testnet) die 2 "RELEASE_FIRST_PUSH is $RELEASE_FIRST_PUSH, but $TAG is on the $CHANNEL channel, whose package is $PACKAGE" ;;
        esac
        die 2 "RELEASE_FIRST_PUSH names '$(printable "$name")', which is not a package of $TAG (its packages: ${packages// /, })"
    done
}

# build server: build every component's image and tarball from the tag, push the images, attach
# the artifacts.
cmd_build() {
    local driver manifest c order existing rd stage want got repo_digests sums_hex files name changed
    local images packages n_new=0 new_images='' new_packages='' first='' n_have=0 have='' have_one=''
    local n_private=0 private_images='' private_packages=''
    if [ "$(uname -s)" != Linux ] || [ "$(uname -m)" != x86_64 ]; then
        die 2 "build runs on Linux x86_64 (this host is $(uname -s) $(uname -m))"
    fi
    need docker tar gzip curl
    require_docker
    require_local_image_in_dry_run
    driver=$(docker buildx inspect "$RELEASE_BUILDER" 2>/dev/null | awk '$1 == "Driver:" && !seen { print $2; seen = 1 }') ||
        die 2 "docker buildx builder '$RELEASE_BUILDER' does not exist"
    [ "$driver" = docker ] ||
        die 2 "buildx builder '$RELEASE_BUILDER' uses the '$driver' driver; release builds need the docker driver"
    require_registry_login
    fetch_tag
    check_scripts_from_main
    check_tag always
    release_state
    [ "$REL_EXISTS" = 1 ] || die 2 "no draft release for $TAG yet; CI creates it when the tag is pushed"
    [ "$REL_DRAFT" = true ] || die 1 "release $TAG is already published"
    [ "$REL_AUTHOR" = "$BOT_LOGIN" ] || die 1 "the draft for $TAG was created by $REL_AUTHOR, not $BOT_LOGIN"
    for manifest in $MANIFESTS; do
        if has_asset "$manifest.asc"; then
            die 1 "the draft for $TAG already carries $manifest.asc; a rebuild would void those signatures"
        fi
    done
    # A stray file on the draft stops the build now, not after the images are built.
    check_release_asset_set build
    check_first_push_names

    # The registry is asked about every image before any is built, and a refusal names every
    # image or package it concerns, so one rerun can fix them all.
    for c in $COMPONENTS; do
        component_set "$c"
        existing=$(registry_digest "$C_IMAGE" "$TAG" first-push) || exit
        if [ "$existing" = denied ]; then
            # ghcr refuses anonymous reads of a private package and of one never pushed alike,
            # so the docker login looks again. When it sees TAG, the package is private and not
            # new, and a rebuild now would only stop again after its push: the setup comes
            # first. Only when the registry answers that TAG is not found (or denied) is this a
            # first push, which needs the package named in RELEASE_FIRST_PUSH.
            existing=$(login_digest "$C_IMAGE") || exit
            if [ -n "$existing" ]; then
                die 2 "$C_IMAGE is private, not new, and already holds $C_IMAGE:$TAG ($existing): do the one-time setup first (make the package public and set its access list, as the maintainer guide says), then run make release-build TAG=$TAG again, with RELEASE_REBUILD=1 if the tag must be rebuilt"
            fi
            first="$first${first:+,}$C_PACKAGE"
            if first_push_named "$C_PACKAGE"; then
                warn "RELEASE_FIRST_PUSH=$C_PACKAGE: pushing $C_IMAGE:$TAG as the first image of the package; build stops after the push until the package is public"
            else
                n_new=$((n_new + 1))
                new_images="$new_images$C_IMAGE"$'\n'
                new_packages="$new_packages$C_PACKAGE"$'\n'
            fi
        elif [ -n "$existing" ]; then
            if [ "${RELEASE_REBUILD:-}" = 1 ]; then
                warn "RELEASE_REBUILD=1: rebuilding $C_IMAGE:$TAG (the registry has $existing)"
            else
                n_have=$((n_have + 1))
                have="$have$C_IMAGE:$TAG ($existing)"$'\n'
                have_one="$C_IMAGE:$TAG already exists ($existing)"
            fi
        fi
    done
    # The RELEASE_FIRST_PUSH to pass names every package found new, the ones named already too.
    images=$(and_list "$new_images")
    packages=$(and_list "$new_packages")
    if [ "$n_new" = 1 ]; then
        die 2 "$images refuses anonymous reads and the docker login does not see $images:$TAG: if this is the first release image of the package $packages, run again with RELEASE_FIRST_PUSH=$first; otherwise make the package public (maintainer guide, one-time setup)"
    elif [ "$n_new" -gt 1 ]; then
        die 2 "$images refuse anonymous reads, and the docker login sees $TAG in none of them: if these are the first release images of the packages $packages, run again with RELEASE_FIRST_PUSH=$first; otherwise make those packages public (maintainer guide, one-time setup)"
    fi
    if [ "$n_have" = 1 ]; then
        die 1 "$have_one; set RELEASE_REBUILD=1 to rebuild it"
    elif [ "$n_have" -gt 1 ]; then
        die 1 "$(and_list "$have") already exist; set RELEASE_REBUILD=1 to rebuild them"
    fi

    # Build from a pristine checkout of the tag, never from the working tree.
    umask 022
    BUILD_WT=$WORK/src
    git worktree add --detach "$BUILD_WT" "refs/tags/$TAG" >&2 || die 2 "git worktree add failed"
    [ "$(git -C "$BUILD_WT" rev-parse HEAD)" = "$COMMIT" ] || die 1 "the build worktree is not at $COMMIT"
    git -C "$BUILD_WT" submodule update --init tn-contracts >&2 || die 3 "cannot fetch the tn-contracts submodule"
    want=$(git -C "$BUILD_WT" rev-parse "HEAD:tn-contracts")
    got=$(git -C "$BUILD_WT/tn-contracts" rev-parse HEAD)
    [ "$want" = "$got" ] || die 1 "tn-contracts is at $got but $TAG records $want"

    # Every image is built and checked before any is pushed, so a defect leaves the registry
    # as it was. The node's image, much the slowest to build, comes last, so a defect in another
    # component's image stops the build early. One build: each tarball carries the very bytes
    # that are in its image.
    order=
    for c in $COMPONENTS; do
        if [ "$c" != node ]; then order="$order$c "; fi
    done
    order="${order}node"
    rd="$(pwd)/$(release_dir "$TAG")"
    clear_release_dir "$rd"
    mkdir -p "$rd"
    stage=$WORK/stage
    for c in $order; do
        component_set "$c"
        build_image
        mkdir -p "$stage/$C_TARDIR"
        image_binary "$C_IMAGE:$TAG" "$stage/$C_TARDIR/$C_TAR_BIN"
        check_version_output "$C_IMAGE:$TAG"
        pack_tarball "$rd" "$stage"
    done

    # A maintainer can sign, and anyone with write access upload, while the images build. The
    # draft is read again right before the first push, which would replace the images of a
    # release signed in the meantime; nothing is pushed yet, so a refusal leaves the registry as
    # it was.
    changed="the draft for $TAG changed while the images were built, and nothing was pushed"
    release_state
    if [ "$REL_EXISTS" != 1 ] || [ "$REL_DRAFT" != true ] || [ "$REL_AUTHOR" != "$BOT_LOGIN" ]; then
        die 1 "$changed: it is no longer a draft that $BOT_LOGIN created"
    fi
    for manifest in $MANIFESTS; do
        if has_asset "$manifest.asc"; then
            die 1 "$changed: it carries $manifest.asc now, and a rebuild would void those signatures"
        fi
    done
    # The subshell lets this refusal follow check_release_asset_set's own, which names the asset.
    (check_release_asset_set build) || die 1 "$changed"

    for c in $order; do
        component_set "$c"
        info "pushing $C_IMAGE:$TAG"
        docker push "$C_IMAGE:$TAG" >&2 || die 3 "docker push $C_IMAGE:$TAG failed"
    done
    # A first push leaves its package private. Every image is pushed by now, so the one rerun
    # after the setup of each such package rebuilds them all.
    for c in $order; do
        component_set "$c"
        C_D=$(registry_digest "$C_IMAGE" "$TAG" first-push) || exit
        if [ "$C_D" = denied ]; then
            n_private=$((n_private + 1))
            private_images="$private_images$C_IMAGE:$TAG"$'\n'
            private_packages="$private_packages$C_PACKAGE"$'\n'
            continue
        fi
        [ -n "$C_D" ] || die 1 "the registry has no $C_IMAGE:$TAG after the push"
        repo_digests=$(docker image inspect --format '{{json .RepoDigests}}' "$C_IMAGE:$TAG") ||
            die 1 "docker image inspect $C_IMAGE:$TAG failed"
        case "$repo_digests" in
            *"\"$C_IMAGE@$C_D\""*) ;;
            *) die 1 "the registry serves $C_D for $C_IMAGE:$TAG, which is not the image just built ($repo_digests)" ;;
        esac
        record_digest "$C_NAME" "$C_D"
        if [ "$C_NAME" = node ]; then D=$C_D; fi
        # cmd_verify below reads the digest file back from the draft and refuses it unless it
        # names an image in C_IMAGE, the component's package.
        printf '%s@%s\n' "$C_IMAGE" "$C_D" >"$rd/$C_DIGEST_FILE"
    done
    images=$(and_list "$private_images")
    packages=$(and_list "$private_packages")
    if [ "$n_private" = 1 ]; then
        die 2 "$images is pushed, but the package is not public: do its one-time setup (maintainer guide), then run make release-build TAG=$TAG again with RELEASE_REBUILD=1"
    elif [ "$n_private" -gt 1 ]; then
        die 2 "$images are pushed, but the packages $packages are not public: do the one-time setup of each (maintainer guide), then run make release-build TAG=$TAG again with RELEASE_REBUILD=1"
    fi
    for manifest in $MANIFESTS; do write_sums "$rd" "$manifest"; done

    # One upload of every release file. The list grows behind a placeholder, which the shift
    # drops: bash 3.2 under set -u refuses "$@" while there are no arguments.
    files=$(release_files) || exit
    set -- -
    while IFS= read -r name; do set -- "$@" "$rd/$name"; done <<EOF
$files
EOF
    shift
    release_upload "$@"
    render_notes "$D" >"$WORK/notes.md"
    release_set_notes "$WORK/notes.md"
    cmd_verify 1
    # The self-verify checked the draft's manifests, which it fetched into $WORK/verify; the
    # hashes printed below are of the ones this build wrote, so the two must be the same files.
    for manifest in $MANIFESTS; do
        sums_hex=$(sha256_hex "$rd/$manifest") || exit
        got=$(sha256_hex "$WORK/verify/$manifest") || exit
        [ "$got" = "$sums_hex" ] ||
            die 1 "$manifest on the draft is not the file this build wrote (sha256 $got, not $sums_hex); do not sign, find out what changed the draft"
    done
    for manifest in $MANIFESTS; do
        sums_hex=$(sha256_hex "$rd/$manifest")
        printf '%s sha256: %s\n' "$manifest" "$sums_hex"
    done
}

# sign: $2 is the sha256 of the manifest $1 on the draft. It must equal the value build printed
# for it, taken from RELEASE_SUMS_SHA256 (SHA256SUMS) or RELEASE_EDGE_SUMS_SHA256 (EDGE_SUMS),
# or typed at a prompt: the hash alone or the whole printed line "<manifest> sha256: <hex>",
# whose label must name $1. Build prints one line per manifest, so pasting all of them at the
# first prompt answers the next ones too: each read takes one line, and the rest wait in the
# terminal. RELEASE_YES=1 answers none of these prompts, and a mismatch always stops.
check_build_sums_hash() {
    local manifest=$1 have=$2 var want from label
    case "$manifest" in
        SHA256SUMS) var=RELEASE_SUMS_SHA256 want=${RELEASE_SUMS_SHA256:-} ;;
        "$EDGE_SUMS") var=RELEASE_EDGE_SUMS_SHA256 want=${RELEASE_EDGE_SUMS_SHA256:-} ;;
        *) die 2 "internal error: build prints no sha256 for the manifest '$manifest'" ;;
    esac
    from=$var
    if [ -z "$want" ]; then
        [ -t 0 ] || die 2 "set $var to the $manifest sha256 that make release-build printed (stdin is not a TTY)"
        if [ "$MANIFESTS" = SHA256SUMS ]; then
            printf 'Paste the SHA256SUMS sha256 from the last line of make release-build: ' >&2
        elif [ "$manifest" = SHA256SUMS ]; then
            printf 'Paste the SHA256SUMS sha256 from make release-build, or its last two lines at once: ' >&2
        else
            printf 'Paste the %s sha256 from make release-build: ' "$manifest" >&2
        fi
        read -r want || true
        from="the $manifest prompt"
    fi
    # The last non-blank line counts, so a trailing blank or CR does not matter: its last word
    # is the hash, and the word before "sha256:" in it, if any, is the label build printed.
    label=$(printf '%s\n' "$want" | tr -d '\r' | awk 'NF { l = ""; for (i = 2; i <= NF; i++) if ($i == "sha256:") l = $(i - 1) } END { print l }')
    want=$(printf '%s\n' "$want" | tr -d '\r' | awk 'NF { w = $NF } END { print tolower(w) }')
    if [ -n "$label" ] && [ "$label" != "$manifest" ]; then
        case " SHA256SUMS $EDGE_SUMS " in
            *" $label "*)
                if [ "$from" = "$var" ]; then
                    die 2 "$var holds the $label line of make release-build; set it to the $manifest sha256 that make release-build printed"
                fi
                die 2 "you pasted the $label line at the $manifest prompt; paste the $manifest line of make release-build, or its hash alone"
                ;;
        esac
    fi
    [[ "$want" =~ $HEX64_RE ]] || die 2 "the $manifest sha256 from make release-build must be 64 hex characters, got '$want'"
    [ "$want" = "$have" ] ||
        die 1 "$manifest on the draft has sha256 $have, but make release-build printed $want; do not sign, find out what changed the draft"
    info "$manifest sha256 matches the value from make release-build"
}

# The count last recorded for manifest $2 in $1, lines "<manifest> <count>" (cmd_sign,
# cmd_verify).
recorded_count() { printf '%s\n' "$1" | awk -v m="$2" '$1 == m { n = $2 } END { print n }'; }

# laptop: re-check the draft's files as verify does, compare the hash of every manifest with the
# one build printed, then append this maintainer's detached signature to the signature file of
# each manifest that lacks it. Every signature is made before any is uploaded, so a failed
# touch of the key leaves the draft as it was; a rerun after the upload signs only what still
# lacks this maintainer's signature.
cmd_sign() {
    local rd c manifest before after recount n_before n_after n_back sums_hex up_hex back_hex
    local counts='' mine='' to_sign='' over='' unsent='' summary=''
    need docker tar gzip curl gpg
    require_docker
    check_tag if-cast
    case " $MANIFESTS " in
        *" $EDGE_SUMS "*) ;;
        *)
            [ -z "${RELEASE_EDGE_SUMS_SHA256:-}" ] ||
                die 2 "RELEASE_EDGE_SUMS_SHA256 is set, but $TAG ships the node alone, so it has no $EDGE_SUMS to compare it with; unset RELEASE_EDGE_SUMS_SHA256"
            ;;
    esac
    release_state
    [ "$REL_EXISTS" = 1 ] || die 2 "no draft release for $TAG"
    [ "$REL_DRAFT" = true ] || die 1 "release $TAG is already published; signatures go on the draft"
    check_release_asset_set unsigned
    rd="$(pwd)/$(release_dir "$TAG")"
    clear_release_dir "$rd"
    fetch_release_assets "$rd"
    check_release_files "$rd"
    resolve_signing_key
    # A manifest this key has signed is not signed again, but its hash is still compared with
    # build's below, so a rerun proves that nothing changed since the first run.
    for manifest in $MANIFESTS; do
        before=
        if [ -f "$rd/$manifest.asc" ]; then before=$(count_sigs "$rd/$manifest" "$rd/$manifest.asc") || exit; fi
        n_before=$(printf '%s\n' "$before" | count_lines)
        counts="$counts$manifest $n_before"$'\n'
        case $'\n'"$before"$'\n' in
            *$'\n'"$SIGN_HANDLE"$'\n'*) mine="$mine$manifest"$'\n' ;;
            *) to_sign="$to_sign$manifest"$'\n' ;;
        esac
    done
    if [ -z "$to_sign" ]; then
        [ "$MANIFESTS" != SHA256SUMS ] || die 1 "SHA256SUMS for $TAG is already signed by @$SIGN_HANDLE"
        die 1 "$(and_list "$mine") for $TAG are already signed by @$SIGN_HANDLE"
    fi

    info "tag        $TAG"
    info "commit     $COMMIT"
    info "signer     @$SIGN_HANDLE $SIGN_FPR"
    if [ "$MANIFESTS" = SHA256SUMS ]; then
        info "signatures $(recorded_count "$counts" SHA256SUMS) so far, $THRESHOLD required"
    else
        for manifest in $MANIFESTS; do
            info "signatures $(recorded_count "$counts" "$manifest") so far on $manifest, $THRESHOLD required"
        done
        if [ -n "$mine" ]; then
            info "already signed by @$SIGN_HANDLE, so checked again but not signed again: $(and_list "$mine")"
        fi
    fi
    for c in $COMPONENTS; do
        component_set "$c"
        info "image      $C_IMAGE@$C_D"
    done
    # Nothing between two prompts reads stdin, so the lines pasted at once reach them in turn.
    for manifest in $MANIFESTS; do
        sums_hex=$(sha256_hex "$rd/$manifest")
        info "$manifest sha256: $sums_hex"
        info "$(cat "$rd/$manifest")"
        check_build_sums_hash "$manifest" "$sums_hex"
    done
    for manifest in $MANIFESTS; do
        check_sums_package "$rd" "$manifest" "do not sign it; rebuild the draft with make release-build TAG=$TAG"
    done
    confirm "Sign $(and_list "$to_sign") for $TAG?"
    if [ "$MANIFESTS" != SHA256SUMS ]; then unsent=' (nothing was uploaded; run make release-sign again)'; fi
    for manifest in $to_sign; do
        gpg --armor --detach-sign --local-user "$SIGN_KEY" --output "$WORK/new-$manifest.asc" "$rd/$manifest" ||
            die 2 "gpg could not sign $manifest$unsent: is the YubiKey that holds $SIGN_FPR plugged in, and does gpg --card-status show it?"
    done
    for manifest in $to_sign; do
        if [ "$MANIFESTS" != SHA256SUMS ]; then over=" over $manifest"; fi
        n_before=$(recorded_count "$counts" "$manifest")
        if [ -f "$rd/$manifest.asc" ]; then
            cat "$rd/$manifest.asc" "$WORK/new-$manifest.asc" >"$WORK/combined-$manifest.asc"
        else
            cp "$WORK/new-$manifest.asc" "$WORK/combined-$manifest.asc"
        fi
        after=$(count_sigs "$rd/$manifest" "$WORK/combined-$manifest.asc") || exit
        n_after=$(printf '%s\n' "$after" | count_lines)
        [ "$n_after" = $((n_before + 1)) ] || die 1 "the new signature$over did not count (before $n_before, after $n_after)"
        case $'\n'"$after"$'\n' in
            *$'\n'"$SIGN_HANDLE"$'\n'*) ;;
            *) die 1 "the new signature$over does not verify as @$SIGN_HANDLE" ;;
        esac
        counts="$counts$manifest $n_after"$'\n'
    done
    # One upload of every new signature file. The list grows behind a placeholder, which the
    # shift drops: bash 3.2 under set -u refuses "$@" while there are no arguments.
    set -- -
    for manifest in $to_sign; do
        mv "$WORK/combined-$manifest.asc" "$rd/$manifest.asc"
        set -- "$@" "$rd/$manifest.asc"
    done
    shift
    release_upload "$@"

    # Read them back from the release, so what operators download is what was counted. A
    # maintainer signing at the same moment replaces a whole file (assets have no
    # compare-and-swap), so each must be byte for byte the file just uploaded.
    set -- "$WORK/readback"
    for manifest in $to_sign; do set -- "$@" "$manifest" "$manifest.asc"; done
    release_fetch "$@"
    for manifest in $to_sign; do
        up_hex=$(sha256_hex "$rd/$manifest.asc")
        back_hex=$(sha256_hex "$WORK/readback/$manifest.asc")
        [ "$back_hex" = "$up_hex" ] ||
            die 1 "$manifest.asc on the release is not the file just uploaded (did another maintainer sign at the same time?); run make release-sign again"
        recount=$(count_sigs "$WORK/readback/$manifest" "$WORK/readback/$manifest.asc") || exit
        case $'\n'"$recount"$'\n' in
            *$'\n'"$SIGN_HANDLE"$'\n'*) ;;
            *) die 1 "the uploaded $manifest.asc carries no signature by @$SIGN_HANDLE" ;;
        esac
        n_back=$(printf '%s\n' "$recount" | count_lines)
        n_after=$(recorded_count "$counts" "$manifest")
        [ "$n_back" = "$n_after" ] || die 1 "the uploaded $manifest.asc counts $n_back signatures, expected $n_after"
    done
    if [ "$MANIFESTS" = SHA256SUMS ]; then
        printf 'signed %s as @%s: %s/%s signatures\n' "$TAG" "$SIGN_HANDLE" "$(recorded_count "$counts" SHA256SUMS)" "$THRESHOLD"
        return 0
    fi
    for manifest in $MANIFESTS; do
        summary="$summary${summary:+, }$manifest $(recorded_count "$counts" "$manifest")/$THRESHOLD"
    done
    printf 'signed %s as @%s: %s signatures\n' "$TAG" "$SIGN_HANDLE" "$summary"
}

# build server, CI, operators: re-check everything about a release. $1 = 1 allows missing
# signature files (build's own self-check before anyone has signed). Each manifest is signed on
# its own, so the threshold holds for each: signatures over SHA256SUMS vouch for nothing that
# SHA256SUMS-edge lists.
cmd_verify() {
    local allow_unsigned=$1 dir manifest handles n counts='' c
    need docker tar gzip curl gpg
    require_docker
    check_tag always
    release_state
    [ "$REL_EXISTS" = 1 ] || die 2 "no GitHub release for $TAG"
    if [ "$allow_unsigned" = 1 ] && [ "$REL_DRAFT" != true ]; then
        die 2 "release $TAG is published; --allow-unsigned applies only to a draft"
    fi
    if [ "$REL_DRAFT" = false ] && [ "$REL_PRERELEASE" != "$PRERELEASE" ]; then
        die 1 "release $TAG is published with prerelease=$REL_PRERELEASE; expected $PRERELEASE"
    fi
    if [ "$allow_unsigned" = 1 ]; then
        check_release_asset_set unsigned
    else
        check_release_asset_set signed
    fi
    dir=$WORK/verify
    rm -rf "$dir"
    fetch_release_assets "$dir"
    check_release_files "$dir"
    for manifest in $MANIFESTS; do check_sums_package "$dir" "$manifest"; done
    # Every signature file is judged before any count is compared with the threshold, so a bad
    # signature anywhere (a signature file swapped with another manifest's) is what stops it.
    for manifest in $MANIFESTS; do
        n=0
        if [ -f "$dir/$manifest.asc" ]; then
            handles=$(count_sigs "$dir/$manifest" "$dir/$manifest.asc") || exit
            n=$(printf '%s\n' "$handles" | count_lines)
        fi
        counts="$counts$manifest $n
"
    done
    if [ "$allow_unsigned" != 1 ]; then
        for manifest in $MANIFESTS; do
            n=$(recorded_count "$counts" "$manifest")
            [ "$n" -ge "$THRESHOLD" ] ||
                die 1 "$manifest carries $n of the $THRESHOLD required maintainer signatures"
        done
    fi
    printf 'verified %s commit=%s signatures=%s/%s image=%s@%s\n' "$TAG" "$COMMIT" "$(recorded_count "$counts" SHA256SUMS)" "$THRESHOLD" "$IMAGE" "$D"
    for c in $COMPONENTS; do
        if [ "$c" = node ]; then continue; fi
        component_set "$c"
        printf 'verified %s component=%s manifest=%s signatures=%s/%s image=%s@%s\n' "$TAG" "$C_NAME" "$C_MANIFEST" \
            "$(recorded_count "$counts" "$C_MANIFEST")" "$THRESHOLD" "$C_IMAGE" "$C_D"
    done
}

# Versions of the published final releases in TAG's channel, TAG excluded. A dry run has no
# GitHub to ask and treats TAG as the only release. Its output is captured, and bash 3.2 runs
# $(...) without -e, so `|| exit` passes on the status of a die inside gh_run.
published_versions() {
    local tags
    if [ "$DRY_RUN" = 1 ]; then return 0; fi
    tags=$(gh_run release list --repo "$REPO" --limit 1000 --json tagName,isDraft \
        --jq '.[] | select(.isDraft | not) | .tagName') || exit
    channel_finals "$tags"
}

# build server: verify, publish the draft, move the channel alias of each component's image.
# Safe to re-run: an alias that points at its verified digest already is left as it is.
cmd_publish() {
    local published highest latest=--latest=false alias_moves=0 c order current alias=none
    need docker
    require_local_image_in_dry_run
    fetch_tag
    check_scripts_from_main
    cmd_verify 0
    if [ "$REL_DRAFT" = true ]; then check_release_body; else record_release_body; fi
    published=$(published_versions) || exit
    highest=$(printf '%s\n%s\n' "$published" "$VERSION" | version_max)
    if [ -z "$RC" ] && [ "$highest" = "$VERSION" ]; then
        alias_moves=1
        alias=$ALIAS
        if [ "$CHANNEL" = mainnet ]; then latest=--latest; fi
    fi
    # Checked before anything is made public: the alias move needs the registry credential.
    if [ "$alias_moves" = 1 ]; then require_registry_login; fi
    # The aliases move in this order, the node's last, so a node alias at the new digest means
    # that every alias of the release moved; a rerun after a failed move moves the rest.
    order=
    for c in $COMPONENTS; do
        if [ "$c" != node ]; then order="$order$c "; fi
    done
    order="${order}node"

    info "tag      $TAG (commit $COMMIT)"
    for c in $COMPONENTS; do
        component_set "$c"
        info "image    $C_IMAGE@$C_D"
    done
    if [ "$REL_DRAFT" = true ]; then
        info "release  draft -> published, prerelease=$PRERELEASE, $latest"
    else
        info "release  already published"
    fi
    if [ "$alias_moves" = 1 ]; then
        for c in $order; do
            component_set "$c"
            info "alias    $C_IMAGE:$ALIAS -> $C_D"
        done
    else
        info "alias    none moves (release candidate, or not the newest $CHANNEL release)"
    fi
    confirm "Publish $TAG?"
    # Anyone with write access can change a draft's assets while the prompt waits, and
    # publishing takes whatever the draft holds at that moment.
    check_assets_unchanged "run make release-publish again to verify them"

    if [ "$REL_DRAFT" = true ]; then
        if [ "$DRY_RUN" = 1 ]; then
            info "dry run: would run gh release edit $TAG --draft=false --prerelease=$PRERELEASE $latest"
        else
            gh_run release edit "$TAG" --repo "$REPO" --draft=false "--prerelease=$PRERELEASE" "$latest" >&2
        fi
    fi
    # Catches a change between the check above and the edit, before an alias points at a digest.
    check_assets_unchanged "the release is public: follow After publish in the maintainer guide"
    if [ "$alias_moves" = 1 ]; then
        # Every package still serves TAG at the verified digest before the first alias moves,
        # so no alias moves while another package's image is in doubt.
        for c in $order; do
            component_set "$c"
            current=$(registry_digest "$C_IMAGE" "$TAG")
            [ "$current" = "$C_D" ] ||
                die 1 "$C_IMAGE:$TAG is ${current:-missing}, not $C_D; $C_IMAGE:$ALIAS moves only to the image this package serves for $TAG"
        done
        for c in $order; do
            component_set "$c"
            move_alias
        done
    fi
    printf 'published %s image=%s@%s alias=%s\n' "$TAG" "$IMAGE" "$D" "$alias"
    for c in $COMPONENTS; do
        if [ "$c" = node ]; then continue; fi
        component_set "$c"
        printf 'published %s component=%s image=%s@%s alias=%s\n' "$TAG" "$C_NAME" "$C_IMAGE" "$C_D" "$alias"
    done
}

# ---------------------------------------------------------------------------------------------
# Setup and dispatch

usage() {
    cat >&2 <<'EOF'
usage: etc/release.sh <subcommand> <TAG> [options]

  parse TAG                          print TAG's channel, the node's image package, features,
                                     alias and file names
  check-tag TAG                      validate a pushed release tag (CI, build server, laptop)
  notes TAG [--image-digest D]       render the release body (CI, build server)
  draft TAG                          create or refresh the draft release (CI only)
  prep TAG                           bump the version and write the CHANGELOG section (build server)
  tag TAG                            sign and push the release tag (laptop)
  build TAG                          build, push and attach the artifacts (build server)
  sign TAG                           add a maintainer signature to SHA256SUMS.asc, and to
                                     SHA256SUMS-edge.asc when the tag ships more than the
                                     node (laptop)
  verify TAG [--allow-unsigned]      check a release end to end (build server, CI, operators)
  publish TAG                        verify, publish the draft, move the aliases (build server)

TAG is vX.Y.Z, vX.Y.Z-rcN, vX.Y.Z-adiri or vX.Y.Z-adiri-rcN. A mainnet tag's node image goes
to the ghcr package tn-mainnet, an adiri tag's to tn-testnet.
etc/release-components at the tagged commit lists the components the tag ships (node,
worker-gateway, node-record-api); a tag without it ships the node alone. The worker-gateway
and node-record-api images go to the packages tn-worker-gateway and tn-node-record-api, which
every channel shares, and their files are listed in SHA256SUMS-edge, which the same keys sign.
The maintainer keys (release/keys/*.asc) and the signature threshold (release/KEYS.md) come
from the tn-release-signing submodule at the commit main records for release/, so a tag cannot
introduce a key, and moving that pointer needs a code owner's review. After pulling main, run
git submodule update --init -- release in this checkout.
See the header of this file for the environment variables and exit codes.
EOF
    exit 2
}

reject_unless_dry() {
    if [ -n "$2" ]; then die 2 "$1 is honoured only in a dry run (RELEASE_DRY_RUN=1)"; fi
}

# Reads and checks the environment once; see the header for each variable.
init_env() {
    local artifacts
    case "${RELEASE_DRY_RUN:-}" in
        '' | 0) DRY_RUN=0 ;;
        1) DRY_RUN=1 ;;
        *) die 2 "RELEASE_DRY_RUN must be 1 or unset" ;;
    esac
    if [ "$DRY_RUN" = 0 ]; then
        reject_unless_dry RELEASE_IMAGE "${RELEASE_IMAGE+set}"
        reject_unless_dry RELEASE_ALLOWLIST_DIR "${RELEASE_ALLOWLIST_DIR+set}"
        reject_unless_dry RELEASE_MAIN_REF "${RELEASE_MAIN_REF+set}"
        reject_unless_dry RELEASE_ATTESTATION "${RELEASE_ATTESTATION+set}"
        reject_unless_dry RELEASE_DIR "${RELEASE_DIR+set}"
    else
        warn "dry run: no gh calls, no git push, images go only to RELEASE_IMAGE"
    fi

    if [ -n "${RELEASE_REPO:-}" ]; then
        REPO=$RELEASE_REPO
    elif [ "${GITHUB_ACTIONS:-}" = true ] && [ -n "${GITHUB_REPOSITORY:-}" ]; then
        REPO=$GITHUB_REPOSITORY
    else
        REPO=$DEFAULT_REPO
    fi
    [[ "$REPO" =~ $REPO_RE ]] || die 2 "RELEASE_REPO must look like owner/repo, got '$REPO'"
    if [ "$REPO" != "$DEFAULT_REPO" ]; then warn "using repository $REPO"; fi

    # The image is the tag's package under the repository owner: RELEASE_REPO and
    # GITHUB_REPOSITORY name the GitHub repository and give the image only their owner.
    [ -n "$PACKAGE" ] || die 2 "internal error: init_env runs only after parse_tag"
    OWNER=$(lower "${REPO%%/*}")
    if [ -n "${RELEASE_IMAGE:-}" ]; then
        IMAGE=$RELEASE_IMAGE
        is_local_image "$IMAGE" ||
            die 2 "RELEASE_IMAGE must be localhost:PORT/ or 127.0.0.1:PORT/ and a lowercase repository path without a tag or digest, e.g. 127.0.0.1:5000/tn"
    else
        IMAGE=ghcr.io/$OWNER/$PACKAGE
    fi

    if [ -n "${RELEASE_MAIN_REF:-}" ]; then MAIN_REF=$RELEASE_MAIN_REF; fi
    if [ -n "${RELEASE_ATTESTATION:-}" ] && [ "$RELEASE_ATTESTATION" != skip ]; then
        die 2 "RELEASE_ATTESTATION accepts only 'skip'"
    fi
    if [ -n "${RELEASE_DIR:-}" ]; then
        # build and sign empty the local artifact dir, which must not delete the stand-in
        # release, however either path is spelled (./, .., symlinks).
        RELEASE_DIR=$(physical_dir "$(abspath "$RELEASE_DIR")") || exit
        artifacts=$(physical_dir "$(pwd)/$ARTIFACT_ROOT") || exit
        case "$RELEASE_DIR/" in
            "$artifacts/"*) die 2 "RELEASE_DIR must lie outside $ARTIFACT_ROOT" ;;
        esac
    fi
    if [ -n "${RELEASE_ALLOWLIST_DIR:-}" ]; then RELEASE_ALLOWLIST_DIR=$(abspath "$RELEASE_ALLOWLIST_DIR"); fi

    THRESHOLD=${RELEASE_SIG_THRESHOLD:-$MIN_SIGNATURES}
    case "$THRESHOLD" in
        '' | *[!0-9]*) die 2 "RELEASE_SIG_THRESHOLD must be a whole number" ;;
    esac
    [ "$THRESHOLD" -ge "$MIN_SIGNATURES" ] ||
        die 2 "RELEASE_SIG_THRESHOLD=$THRESHOLD is below MIN_SIGNATURES=$MIN_SIGNATURES"
    RELEASE_BUILDER=${RELEASE_BUILDER:-default}
}

cleanup() {
    local ctr
    if [ "$PREP_RESTORE" = 1 ]; then
        if git checkout HEAD -- Cargo.toml Cargo.lock CHANGELOG.md 2>/dev/null; then
            info "prep stopped: restored Cargo.toml, Cargo.lock and CHANGELOG.md from HEAD"
        else
            warn "prep stopped and could not restore its files: run git checkout HEAD -- Cargo.toml Cargo.lock CHANGELOG.md"
        fi
    fi
    if [ -n "$CREATED_TAG" ] && git tag -d "$CREATED_TAG" >/dev/null 2>&1; then
        info "removed the unpushed local tag $CREATED_TAG"
    fi
    while IFS= read -r ctr; do
        if [ -n "$ctr" ]; then docker rm -f "$ctr" >/dev/null 2>&1 || true; fi
    done <<EOF
$CONTAINERS
EOF
    if [ -n "$BUILD_WT" ]; then git worktree remove --force --force "$BUILD_WT" >/dev/null 2>&1 || true; fi
    if [ -n "$ALLOW_GNUPGHOME" ] && command -v gpgconf >/dev/null 2>&1; then
        gpgconf --homedir "$ALLOW_GNUPGHOME" --kill all >/dev/null 2>&1 || true
    fi
    if [ -n "$WORK" ]; then rm -rf "$WORK"; fi
}

# Runs from the repository root of the current directory, with a private temp dir.
setup() {
    local top
    # git reads a replace ref (refs/replace/<id>) in place of the object it names. One in the
    # release submodule's repository would substitute the commit main records, so the maintainer
    # keys and the threshold would come from a commit nobody reviewed. Every git command of this
    # run, its children included, reads objects as they are.
    export GIT_NO_REPLACE_OBJECTS=1
    need git awk
    ORIG_PWD=$(pwd)
    SELF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
    top=$(git rev-parse --show-toplevel 2>/dev/null) || die 2 "not inside a git checkout"
    cd "$top"
    WORK=$(mktemp -d "${TMPDIR:-/tmp}/tn-release.XXXXXX") || die 2 "mktemp failed"
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    init_env
}

main() {
    local cmd digest='' allow_unsigned=0
    [ $# -ge 1 ] || usage
    cmd=$1
    shift
    case "$cmd" in
        parse | check-tag | notes | draft | prep | tag | build | sign | verify | publish) ;;
        *) usage ;;
    esac
    [ $# -ge 1 ] || usage
    parse_tag "$1"
    shift
    while [ $# -gt 0 ]; do
        case "$cmd:$1" in
            notes:--image-digest)
                [ $# -ge 2 ] || usage
                digest=$2
                shift 2
                ;;
            verify:--allow-unsigned)
                allow_unsigned=1
                shift
                ;;
            *) die 2 "unknown option for $cmd: $1" ;;
        esac
    done

    # parse needs neither git nor the network.
    if [ "$cmd" = parse ]; then
        print_parse
        return 0
    fi
    setup
    case "$cmd" in
        check-tag) cmd_check_tag ;;
        notes) cmd_notes "$digest" ;;
        draft) cmd_draft ;;
        prep) cmd_prep ;;
        tag) cmd_tag ;;
        build) cmd_build ;;
        sign) cmd_sign ;;
        verify) cmd_verify "$allow_unsigned" ;;
        publish) cmd_publish ;;
    esac
}

# Sourcing the file (tests) defines the functions without running anything.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    main "$@"
fi
