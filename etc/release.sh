#!/usr/bin/env bash
#
# Release pipeline for telcoin-network. CI, the build host (xerxes) and the maintainer laptop
# all run this one file, so the tag grammar, the maintainer allowlist, the signature counter
# and the registry lookup exist exactly once. Maintainer guide:
# docs/src/maintainers/releasing.md.
#
# Usage: etc/release.sh <subcommand> <TAG> [options]   (or: make release-<subcommand> TAG=...)
#
# Flow:
#   1. prep TAG        xerxes  set [workspace.package].version to X.Y.Z, cargo update, splice
#                              the git-cliff section into CHANGELOG.md; open the PR
#                              "release: TAG", merge it alone through the queue, `make attest`
#   2. tag TAG         laptop  sign TAG on the release commit with an allowlisted key, push it
#   3. check-tag TAG   CI      validate the pushed tag; `notes` renders the release body and
#                              `draft` creates the draft release
#   4. build TAG       xerxes  build image and tarball from the tag, push the image, upload
#                              IMAGE_DIGEST, SHA256SUMS and the tarball to the draft
#   5. sign TAG        laptop  append a detached signature over SHA256SUMS to SHA256SUMS.asc
#   6. verify TAG      xerxes  re-check tag, assets, signatures, image and binary, then
#      publish TAG             publish the draft and move the channel alias
#   7. verify TAG      CI      re-check the published release (detective only)
#
# Subcommands: parse, check-tag, notes [--image-digest sha256:HEX], draft, prep, tag, build,
# sign, verify [--allow-unsigned], publish. `help` or no arguments prints the usage.
#
# Exit codes:
#   0  ok
#   1  a policy or verification check failed
#   2  usage or precondition: bad TAG, missing tool, wrong host, missing draft, or a prompt
#      with no TTY and no RELEASE_YES=1
#   3  an external service (adiri RPC, GitHub API, registry) stayed unavailable after retries
#
# Environment (all optional):
#   RELEASE_REPO                   owner/repo; default Telcoin-Association/telcoin-network,
#                                  GITHUB_REPOSITORY in CI
#   RELEASE_COMMIT                 tag: commit to tag instead of the origin/main tip
#   RELEASE_GPG_KEY                tag, sign: signing key; default `git config user.signingkey`
#   RELEASE_SIG_THRESHOLD          maintainer signatures required; never below MIN_SIGNATURES
#   RELEASE_BUILDER                build: buildx builder, default "default" (driver docker)
#   RELEASE_REBUILD=1              build: rebuild a tag the registry already has
#   RELEASE_ALLOW_LOCAL_SCRIPTS=1  build, publish: allow this script or verify_commit_hash.sh
#                                  to differ from main
#   RELEASE_YES=1                  answer yes to every confirmation prompt
#   GH_TOKEN                       read by gh (CI); locally `gh auth login` is used
#
# Dry run, for rehearsals (RELEASE_DRY_RUN=1). The variables below are refused without it.
# A dry run never calls gh or git push and pushes images only to RELEASE_IMAGE.
#   RELEASE_IMAGE                  image repository on localhost:PORT/ or 127.0.0.1:PORT/
#   RELEASE_ALLOWLIST_DIR          directory of *.asc used instead of main's allowlist
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
ALLOWLIST_PATH=.github/maintainer-gpg-keys
GIT_CLIFF_IMAGE=ghcr.io/orhun/git-cliff/git-cliff:2.10.1@sha256:6ba0d1fcb051bd7b154cfb19c4b2b3bfa2c22c475f5285fc30606777b6573119
ARTIFACT_ROOT=target/release-artifacts

TAG_RE='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-adiri)?(-rc[1-9][0-9]*)?$'
DIGEST_RE='^sha256:[0-9a-f]{64}$'
HANDLE_FILE_RE='^[A-Za-z0-9-]+\.asc$'
REPO_RE='^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'
LOCAL_HOST_RE='^(localhost|127\.0\.0\.1):[0-9]+$'
LOCAL_PATH_RE='^[a-z0-9._/-]+$'
BOT_LOGIN='github-actions[bot]'
ATTEST_SCRIPT=.github/scripts/verify_commit_hash.sh
ANCHOR='<!-- ANCHOR: releases -->'
MANIFEST_TYPES='application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json'

# State. parse_tag sets TAG VERSION CHANNEL RC BASE FEATURES PRERELEASE ALIAS TARBALL; the
# functions named in the comments own the rest.
TAG='' VERSION='' CHANNEL='' RC='' BASE='' FEATURES='' PRERELEASE='' ALIAS='' TARBALL=''
WORK=            # setup: private temp dir, removed on exit
ORIG_PWD=        # setup: directory the script was started from
SELF=            # setup: absolute path of this script
DRY_RUN=0        # init_env
REPO=            # init_env: owner/repo on GitHub
IMAGE=           # init_env: image repository, without tag
THRESHOLD=$MIN_SIGNATURES
ALLOW_GNUPGHOME= # load_allowlist: keyring holding only main's allowlisted keys
ALLOW_LIST=      # load_allowlist: "handle<TAB>primary fingerprint" lines
COMMIT=          # check_tag: the commit TAG points at
SIGNER=          # check_tag_signature: handle that signed TAG
SIGNER_FPR=      # check_tag_signature: primary fingerprint that signed TAG
SIGN_KEY=        # resolve_signing_key: key spec handed to gpg / git tag -u
SIGN_FPR=        # resolve_signing_key: its primary fingerprint
SIGN_HANDLE=     # resolve_signing_key: its allowlist handle
D=               # check_image_digest_file: sha256:<hex> of IMAGE:TAG
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

count_lines() { awk 'NF { n++ } END { print n + 0 }'; }

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

# A dry-run image repository: localhost:PORT or 127.0.0.1:PORT, then a lowercase repository
# path with no tag, digest or "..". Anything looser (such as "localhost:5000@host/x") could
# send registry_digest's requests to another host.
is_local_image() {
    local host=${IMAGE%%/*} path=${IMAGE#*/}
    [[ "$host" =~ $LOCAL_HOST_RE ]] || return 1
    [ "$path" != "$IMAGE" ] || return 1
    [[ "$path" =~ $LOCAL_PATH_RE ]] || return 1
    case "$path" in *..*) return 1 ;; esac
    return 0
}

require_local_image_in_dry_run() {
    if [ "$DRY_RUN" = 1 ] && ! is_local_image; then
        die 2 "dry run: set RELEASE_IMAGE to a localhost: or 127.0.0.1: repository"
    fi
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
    if [ -n "${BASH_REMATCH[4]}" ]; then
        CHANNEL=adiri
        FEATURES=adiri
    else
        CHANNEL=mainnet
        FEATURES=
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
    printf 'TAG=%s\nVERSION=%s\nCHANNEL=%s\nRC=%s\nBASE=%s\nFEATURES=%s\nPRERELEASE=%s\nALIAS=%s\nTARBALL=%s\n' \
        "$TAG" "$VERSION" "$CHANNEL" "$RC" "$BASE" "$FEATURES" "$PRERELEASE" "$ALIAS" "$TARBALL"
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
# Sets ALLOW_GNUPGHOME and ALLOW_LIST ("handle<TAB>primary fingerprint", one line per primary
# key; a file may hold several during a rotation). Thresholds count distinct handles.
load_allowlist() {
    local src where names name file fprs fpr handles
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
        where="$MAIN_REF:$ALLOWLIST_PATH"
        names=$(git ls-tree --name-only "$where" 2>/dev/null) ||
            die 1 "$ALLOWLIST_PATH does not exist on $MAIN_REF"
        while IFS= read -r name; do
            case "$name" in *.asc*) ;; *) continue ;; esac
            allowlist_name "$name" "$where"
            git show "$where/$name" >"$src/$name" || die 1 "cannot read $where/$name"
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
    ALLOW_LIST=$(cat "$WORK/allowlist.tsv")
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
# "good<TAB>handle<TAB>fpr", "bad<TAB>keyid" or "skip<TAB>reason", then "blocks<TAB>N".
sig_blocks() {
    awk -v allow="$WORK/allowlist.tsv" '
        BEGIN {
            while ((getline line < allow) > 0) { split(line, f, "\t"); handle[f[2]] = f[1] }
        }
        function open_block() {
            if (!inblock) { inblock = 1; n++; good = 0; valid = ""; note = ""; bad = 0 }
        }
        function close_block() {
            if (!inblock) return
            if (good && valid != "") {
                if (valid in handle) print "good\t" handle[valid] "\t" valid
                else print "skip\tsignature by key " valid " is not allowlisted"
            } else if (note != "") print "skip\t" note
            else if (!bad) print "skip\tsignature without GOODSIG and VALIDSIG"
            inblock = 0
        }
        $1 != "[GNUPG:]" { next }
        $2 == "NEWSIG" { close_block(); open_block(); next }
        $2 == "GOODSIG" { open_block(); good = 1; next }
        $2 == "VALIDSIG" { open_block(); valid = $NF; next }
        $2 == "BADSIG" { open_block(); bad = 1; print "bad\t" $3; next }
        $2 == "ERRSIG" { open_block(); note = "unverifiable signature by key " $3 " (not allowlisted?)"; next }
        $2 == "EXPKEYSIG" { open_block(); note = "signature by expired key " $3; next }
        $2 == "REVKEYSIG" { open_block(); note = "signature by revoked key " $3; next }
        $2 == "EXPSIG" { open_block(); note = "expired signature by key " $3; next }
        END { close_block(); print "blocks\t" n + 0 }
    '
}

# count_sigs FILE SIG: prints the distinct allowlisted handles with a good signature over FILE
# in SIG (concatenated armored detached signatures), one per line; the count is the line
# count. Any BADSIG is fatal (exit 1); unverifiable, expired or revoked ones are warned about
# and not counted.
count_sigs() {
    local file=$1 sig=$2 status results bad kind detail
    if [ ! -f "$file" ] || [ ! -f "$sig" ]; then die 2 "count_sigs: $file or $sig does not exist"; fi
    status=$(gpg --homedir "$ALLOW_GNUPGHOME" --batch --status-fd 1 --verify "$sig" "$file" 2>/dev/null || true)
    results=$(printf '%s\n' "$status" | sig_blocks) || die 1 "cannot judge the signatures in ${sig##*/}"
    bad=$(printf '%s\n' "$results" | awk -F'\t' '$1 == "bad" { printf "%s ", $2 }')
    [ -z "$bad" ] || die 1 "BADSIG in ${sig##*/} from key ${bad% }: ${file##*/} does not match its signature"
    while IFS=$'\t' read -r kind detail; do
        if [ "$kind" = skip ]; then warn "${sig##*/}: $detail; not counted"; fi
    done <<EOF
$results
EOF
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

# registry_digest REF: prints the digest the registry serves for IMAGE:REF, or nothing when
# that tag does not exist. ghcr.io needs an anonymous pull token, which only works for a public
# package; the dry-run localhost registry is plain http without a token.
registry_digest() {
    local ref=$1 host path url token code digest attempt=1 out
    need curl
    host=${IMAGE%%/*}
    path=${IMAGE#*/}
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
                die 2 "$IMAGE: the registry answered HTTP $code: package is not public (one-time setup: make the ghcr package public)"
                ;;
        esac
        [ "$attempt" -lt 3 ] || die 3 "$IMAGE:$ref: registry unavailable after 3 attempts (last answer: $code)"
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
# refuses a ref update (here only the unforced tag: it moved on origin), 128 on other errors.
git_fetch() {
    local attempt=1 rc
    require_origin
    while :; do
        git fetch --quiet --no-tags origin "$@" && rc=0 || rc=$?
        case "$rc" in
            0) return 0 ;;
            1) die 1 "git fetch refused to update a local ref from origin ($*); did the tag move on origin?" ;;
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

# check-tag step (1), outside CI and dry run: fetch main and exactly this tag, unforced, so a
# local tag that differs from origin is an error rather than silently replaced.
fetch_tag() {
    local remote local_obj
    if [ "$FETCHED" = 1 ] || [ "$DRY_RUN" = 1 ] || [ "${GITHUB_ACTIONS:-}" = true ]; then return 0; fi
    remote=$(ls_remote_tag "$TAG")
    [ -n "$remote" ] || die 2 "tag $TAG does not exist on origin"
    if local_obj=$(git rev-parse --verify --quiet "refs/tags/$TAG"); then
        [ "$local_obj" = "$remote" ] || die 1 "local tag $TAG ($local_obj) differs from origin ($remote)"
    fi
    git_fetch +refs/heads/main:refs/remotes/origin/main "refs/tags/$TAG:refs/tags/$TAG"
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

# Names of the refs/tags/v* tags that count for the version rules. Without a cutoff, every
# tag; with a cutoff (epoch seconds) and commit $2, only annotated tags whose tagger date is
# strictly earlier and whose commit is $2 or one of its ancestors. Returns git's status:
# bash 3.2 runs $(...) without -e, so the caller checks it.
tags_before() {
    local name
    if [ -z "$1" ]; then
        git tag -l 'v*'
    else
        git for-each-ref --format='%(refname:strip=2) %(taggerdate:raw)' 'refs/tags/v*' |
            awk -v c="$1" '$2 != "" && $2 + 0 < c + 0 { print $1 }' |
            while IFS= read -r name; do
                if git merge-base --is-ancestor "refs/tags/$name^{commit}" "$2" 2>/dev/null; then
                    printf '%s\n' "$name"
                fi
            done
    fi
}

# The release is monotonic in its channel, with no release candidate after its final, among
# the tags that tags_before CUTOFF COMMIT names.
check_version_order() {
    local earlier others highest
    earlier=$(tags_before "$1" "$2") || die 2 "cannot list the v* tags"
    if [ -n "$RC" ]; then
        case $'\n'"$earlier"$'\n' in
            *$'\n'"$BASE"$'\n'*) die 1 "$BASE already exists; no release candidate may follow its final release" ;;
        esac
        return 0
    fi
    others=$(channel_finals "$earlier")
    highest=$(printf '%s\n%s\n' "$others" "$VERSION" | version_max)
    [ "$highest" = "$VERSION" ] ||
        die 1 "$TAG is not newer than the $CHANNEL release v$highest; $CHANNEL versions only go up"
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

    # Before TAG exists (`tag` is about to create it) the rules compare with every existing tag.
    # Once it exists they compare only with annotated tags dated before it on COMMIT or its
    # ancestors, so an older release stays verifiable after newer ones exist. Only TAG's date
    # is signed; the other tags are not verified (the tags before this process are unsigned),
    # so the v* tag ruleset keeps bogus tags out, and a wrong tag has to be deleted.
    if git rev-parse --verify --quiet "refs/tags/$TAG" >/dev/null; then
        cutoff=$(tagger_time "$TAG")
        [ -n "$cutoff" ] || die 1 "tag $TAG has no tagger date"
    fi
    check_version_order "$cutoff" "$commit"
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
        *) die 1 "$commit is not attested on chain; on xerxes run: git switch --detach $commit && ALLOW_STALE_BASE=1 make attest" ;;
    esac
}

# Full check-tag, steps (1)-(8). ATTEST is "always" or "if-cast". Sets COMMIT, SIGNER and
# SIGNER_FPR, and prints the summary line.
check_tag() {
    local attest=${1:-always}
    fetch_tag
    COMMIT=$(tag_commit "$TAG")
    check_tag_signature
    check_on_main "$COMMIT"
    check_version_at "$COMMIT"
    check_changelog_at "$COMMIT"
    check_attestation "$COMMIT" "$attest"
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

# check_assets_unchanged WHAT: the release still holds exactly the assets, by name, id and
# digest, that release_state listed for the last verification. WHAT ends the error message.
check_assets_unchanged() {
    local verified=$REL_ASSET_IDS
    release_state
    [ "$REL_EXISTS" = 1 ] || die 1 "release $TAG is gone; $1"
    [ "$REL_ASSET_IDS" = "$verified" ] || die 1 "the assets of $TAG changed after they were verified; $1"
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
        if [ -n "$digest" ] && [ "$digest" != "sha256:$hex" ]; then
            die 1 "$name on release $TAG (asset $id) changed between listing and download"
        fi
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

# SHA256SUMS: "<hex>  <name>" for IMAGE_DIGEST and the tarball, C-locale sorted by name.
write_sums() {
    local dir=$1 digest_hex tar_hex
    digest_hex=$(sha256_hex "$dir/IMAGE_DIGEST")
    tar_hex=$(sha256_hex "$dir/$TARBALL")
    printf '%s  %s\n%s  %s\n' "$digest_hex" IMAGE_DIGEST "$tar_hex" "$TARBALL" |
        LC_ALL=C sort -k2 >"$dir/SHA256SUMS"
}

# SHA256SUMS names exactly IMAGE_DIGEST and the tarball, in the strict two-space format, and
# every hash matches the file beside it.
check_sums() {
    local dir=$1 names want
    awk '!/^[0-9a-f]+  [^ ]+$/ || length($1) != 64 { bad = 1 } END { exit bad }' "$dir/SHA256SUMS" ||
        die 1 "SHA256SUMS has a malformed line"
    names=$(awk '{ print $2 }' "$dir/SHA256SUMS" | LC_ALL=C sort)
    want=$(printf '%s\n' IMAGE_DIGEST "$TARBALL" | LC_ALL=C sort)
    [ "$names" = "$want" ] || die 1 "SHA256SUMS must name exactly IMAGE_DIGEST and $TARBALL"
    (cd "$dir" && sha256_check SHA256SUMS) >&2 || die 1 "SHA256SUMS does not match the files"
}

# IMAGE_DIGEST is one line "IMAGE@sha256:<hex>" and the registry still serves IMAGE:TAG at that
# digest. Sets D.
check_image_digest_file() {
    local file=$1/IMAGE_DIGEST line lines reg
    lines=$(awk 'END { print NR }' "$file")
    line=$(cat "$file")
    [ "$lines" = 1 ] || die 1 "IMAGE_DIGEST must be a single line"
    case "$line" in
        "$IMAGE@"*) ;;
        *) die 1 "IMAGE_DIGEST names '$line'; expected $IMAGE@sha256:<hex>" ;;
    esac
    D=${line#"$IMAGE@"}
    [[ "$D" =~ $DIGEST_RE ]] || die 1 "IMAGE_DIGEST holds a malformed digest: $D"
    reg=$(registry_digest "$TAG")
    [ "$reg" = "$D" ] || die 1 "IMAGE_DIGEST says $D but the registry serves $IMAGE:$TAG as ${reg:-nothing}"
}

# The tarball lists exactly the four files under its top-level directory (an exact
# comparison, so nothing absolute and no ".."), and they extract as regular files into $2.
check_tarball() {
    local tarball=$1 dest=$2 dir listing want f
    dir="telcoin-network-$TAG-$TRIPLE"
    listing=$(tar -tzf "$tarball") || die 1 "cannot list $TARBALL"
    listing=$(printf '%s\n' "$listing" | awk -v d="$dir/" '$0 != d' | LC_ALL=C sort)
    want=$(printf '%s\n' "$dir/LICENSE-APACHE" "$dir/LICENSE-MIT" "$dir/NOTICE" "$dir/telcoin-network" | LC_ALL=C sort)
    [ "$listing" = "$want" ] ||
        die 1 "$TARBALL must hold exactly telcoin-network, LICENSE-APACHE, LICENSE-MIT and NOTICE under $dir/"
    mkdir -p "$dest"
    tar -xzf "$tarball" -C "$dest" || die 1 "cannot extract $TARBALL"
    for f in telcoin-network LICENSE-APACHE LICENSE-MIT NOTICE; do
        if [ ! -f "$dest/$dir/$f" ] || [ -L "$dest/$dir/$f" ]; then die 1 "$dir/$f in $TARBALL is not a regular file"; fi
    done
    [ -x "$dest/$dir/telcoin-network" ] || die 1 "$dir/telcoin-network in $TARBALL is not executable"
}

# Copies /usr/local/bin/telcoin out of image $1 to $2, after checking that the image runs that
# file: no ENTRYPOINT, CMD ["telcoin","node"] as in etc/Dockerfile, and no telcoin in
# /usr/local/sbin, the one PATH entry ahead of /usr/local/bin.
image_binary() {
    local ref=$1 dest=$2 ctr cfg
    cfg=$(docker image inspect --format '{{json .Config.Entrypoint}} {{json .Config.Cmd}}' "$ref") ||
        die 1 "docker image inspect $ref failed"
    [ "$cfg" = 'null ["telcoin","node"]' ] ||
        die 1 "$ref has ENTRYPOINT and CMD $cfg; expected no ENTRYPOINT and CMD [\"telcoin\",\"node\"]"
    ctr=$(docker create --platform linux/amd64 "$ref") || die 1 "docker create $ref failed"
    CONTAINERS="$CONTAINERS$ctr"$'\n'
    if docker cp "$ctr:/usr/local/sbin/telcoin" - >/dev/null 2>&1; then
        die 1 "$ref has a second telcoin in /usr/local/sbin, which PATH finds first"
    fi
    docker cp "$ctr:/usr/local/bin/telcoin" "$dest" >/dev/null || die 1 "cannot copy /usr/local/bin/telcoin out of $ref"
    docker rm "$ctr" >/dev/null 2>&1 || true
}

# verify step (i): the image's long version names this release's version, commit and channel
# (crates/telcoin-network-cli/src/version.rs).
check_version_output() {
    local ref=$1 out version sha features flist has_adiri=no
    # --entrypoint: run the very file image_binary copied, not whatever ENTRYPOINT or PATH picks.
    out=$(docker run --rm --network none --platform linux/amd64 \
        --entrypoint /usr/local/bin/telcoin "$ref" --version) ||
        die 1 "$ref: /usr/local/bin/telcoin --version failed"
    # clap starts the first line with the program name: "telcoin-network-cli Version: X.Y.Z".
    version=$(printf '%s\n' "$out" | sed -n 's/^\([^ ]* \)\{0,1\}Version: //p')
    sha=$(printf '%s\n' "$out" | sed -n 's/^Commit SHA: //p')
    features=$(printf '%s\n' "$out" | sed -n 's/^Build Features: //p')
    [ "$version" = "$VERSION" ] || die 1 "$ref reports 'Version: $version'; expected $VERSION"
    if [ "${#sha}" -lt 7 ]; then die 1 "$ref reports 'Commit SHA: $sha'; expected $COMMIT"; fi
    case "$COMMIT" in
        "$sha"*) ;;
        *) die 1 "$ref reports 'Commit SHA: $sha'; expected $COMMIT" ;;
    esac
    flist=$(printf '%s' "$features" | tr ' ' ',')
    case ",$flist," in *,adiri,*) has_adiri=yes ;; esac
    if [ "$CHANNEL" = adiri ] && [ "$has_adiri" = no ]; then
        die 1 "$ref was built without the adiri feature (Build Features: $features)"
    fi
    if [ "$CHANNEL" = mainnet ] && [ "$has_adiri" = yes ]; then
        die 1 "$ref is a mainnet release but was built with the adiri feature"
    fi
    info "$ref: Version $version, Commit SHA $sha, Build Features ${features:-none}"
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

# Renders the release body for TAG at COMMIT to stdout; $1 is the image digest, if known.
render_notes() {
    local digest=$1 body handle fpr digest_cell features_cell alias_cell
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
        alias_cell="publishing moves \`$IMAGE:$ALIAS\` to this image when it is the newest $CHANNEL release"
    fi
    printf '## Artifacts\n\n| Item | Value |\n|---|---|\n'
    printf '| %s | %s |\n' \
        Channel "\`$CHANNEL\`" \
        Commit "\`$COMMIT\`" \
        "Cargo features" "$features_cell" \
        Image "\`$IMAGE:$TAG\`" \
        "Image digest" "$digest_cell" \
        Tarball "\`$TARBALL\` (Linux x86_64)" \
        Alias "$alias_cell"
    printf '\n'

    # The verification block is the canonical one (the install page's Path A), with TAG and REPO
    # filled in. The heredocs are quoted, so $TAG, $2 and $NF reach the reader as written; only
    # the @NAME@ placeholders are substituted.
    sed -e "s|@TAG@|$TAG|g" -e "s|@REPO@|$REPO|g" <<'EOF'
## Verify

Check the signed checksums before you extract or run anything. These commands take the maintainer keys from `main`, never from the tag, fetch the tag by its full name, download the release files, check the signature over `SHA256SUMS`, and check the hashes:

```sh
TAG=@TAG@
REPO=@REPO@
BASE="https://github.com/$REPO/releases/download/$TAG"
git clone --quiet --depth 1 --branch main "https://github.com/$REPO.git" tn-main &&
git -C tn-main fetch --quiet --depth 1 origin "refs/tags/$TAG:refs/tags/$TAG" &&
for k in tn-main/.github/maintainer-gpg-keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg &&
gpg --show-keys --with-fingerprint tn-main/.github/maintainer-gpg-keys/*.asc &&
curl -fsSL --remote-name-all "$BASE/SHA256SUMS" "$BASE/SHA256SUMS.asc" "$BASE/IMAGE_DIGEST" "$BASE/telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz" &&
gpgv --status-fd 1 --keyring ./tn-release-keys.gpg SHA256SUMS.asc SHA256SUMS |
awk '$2 == "BADSIG" { bad = 1 } $2 ~ /^(GOODSIG|EXPKEYSIG|REVKEYSIG|BADSIG|ERRSIG)$/ { s = $2; print $2 } $2 == "VALIDSIG" && s == "GOODSIG" { print "signed by primary key " $NF; n++ } END { exit (n && !bad) ? 0 : 1 }' &&
sha256sum --check SHA256SUMS &&
echo "Signature and hashes verified"
```

If the last line `Signature and hashes verified` is missing, the files are not verified; stop, do not extract or run anything from this release, and report it.

Then check the key fingerprints: every primary key fingerprint `gpg --show-keys` printed, including the one after `signed by primary key`, must appear in the maintainer release keys table in `tn-main/SECURITY.md` (the same `main` checkout, also [on GitHub](https://github.com/@REPO@/blob/main/SECURITY.md#maintainer-release-keys)) with a Status that is not `revoked`, and in `https://github.com/<handle>.gpg`, where `<handle>` is the key file's name without `.asc`. Stop on any mismatch.

Maintainer release keys on `main` when these notes were written:

| Maintainer | Primary key fingerprint |
|---|---|
EOF
    while IFS=$'\t' read -r handle fpr; do
        if [ -n "$handle" ]; then printf '| @%s | %s |\n' "$handle" "\`$fpr\`"; fi
    done <<EOF
$ALLOW_LIST
EOF
    sed -e "s|@TAG@|$TAG|g" -e "s|@REPO@|$REPO|g" -e "s|@VERSION@|$VERSION|g" -e "s|@IMAGE@|$IMAGE|g" <<'EOF'

Tarball, only after the last line was `Signature and hashes verified` and the fingerprints matched:

```sh
tar -xzf "telcoin-network-$TAG-x86_64-unknown-linux-gnu.tar.gz" &&
cd "telcoin-network-$TAG-x86_64-unknown-linux-gnu" &&
./telcoin-network --version &&
git -C ../tn-main rev-parse "refs/tags/$TAG^{commit}"
```

The first `--version` line is `telcoin-network-cli Version: @VERSION@`. `Commit SHA:` must be the commit that `git rev-parse` printed, and `Build Features:` must contain `adiri` for an `-adiri` tag and not for any other.

Docker image: it needs only `SHA256SUMS`, `SHA256SUMS.asc` and `IMAGE_DIGEST`. These commands are the block above without the tarball, and with `--ignore-missing`, which makes `sha256sum` skip the tarball's line:

```sh
TAG=@TAG@
REPO=@REPO@
BASE="https://github.com/$REPO/releases/download/$TAG"
git clone --quiet --depth 1 --branch main "https://github.com/$REPO.git" tn-main &&
git -C tn-main fetch --quiet --depth 1 origin "refs/tags/$TAG:refs/tags/$TAG" &&
for k in tn-main/.github/maintainer-gpg-keys/*.asc; do gpg --dearmor < "$k"; done > tn-release-keys.gpg &&
gpg --show-keys --with-fingerprint tn-main/.github/maintainer-gpg-keys/*.asc &&
curl -fsSL --remote-name-all "$BASE/SHA256SUMS" "$BASE/SHA256SUMS.asc" "$BASE/IMAGE_DIGEST" &&
gpgv --status-fd 1 --keyring ./tn-release-keys.gpg SHA256SUMS.asc SHA256SUMS |
awk '$2 == "BADSIG" { bad = 1 } $2 ~ /^(GOODSIG|EXPKEYSIG|REVKEYSIG|BADSIG|ERRSIG)$/ { s = $2; print $2 } $2 == "VALIDSIG" && s == "GOODSIG" { print "signed by primary key " $NF; n++ } END { exit (n && !bad) ? 0 : 1 }' &&
sha256sum --check --ignore-missing SHA256SUMS &&
echo "Signature and hashes verified"
```

Only after its last line was `Signature and hashes verified` and the fingerprints matched, check the image. This refuses an `IMAGE_DIGEST` that does not name the release repository by digest:

```sh
IMAGE_REF=$(cat IMAGE_DIGEST)
case "$IMAGE_REF" in
  @IMAGE@@sha256:*) docker pull "$IMAGE_REF" && docker run --rm --network none --entrypoint /usr/local/bin/telcoin "$IMAGE_REF" --version ;;
  *) echo "IMAGE_DIGEST does not name a telcoin-network image by digest: $IMAGE_REF" >&2 ;;
esac
```

Its `--version` output must show the same three lines as the tarball's, with the commit that `git -C tn-main rev-parse "refs/tags/$TAG^{commit}"` prints.

When a check fails, do not extract, install or run anything from this release:

- The last line is not `Signature and hashes verified`: a step failed, and the lines above it say which.
- `BADSIG`: `SHA256SUMS` changed after it was signed. Report it.
- `ERRSIG`: the signing key is not in the allowlist on `main`, or `TAG` is wrong. Check `TAG`; if it is right, report it.
- `EXPKEYSIG` or `REVKEYSIG`: the signing subkey has expired or was revoked, although gpgv still prints `Good signature`. Report it; a release signed before that can no longer be verified against `main`, which is intended.
- `sha256sum` prints `FAILED` or `no file was verified`: a file differs from its signed hash, or none of the files is present. Check `TAG` and download once more; if it still fails, report it.
- A fingerprint is missing from `SECURITY.md` or GitHub, differs, or has the Status `revoked`. Report it.
- `IMAGE_DIGEST does not name a telcoin-network image by digest`. Report it.
- `Commit SHA` is not the tag's commit, or `Build Features` does not match the tag. Report it.
- `exec format error`, or Docker warns that the image platform does not match the host: the release is Linux x86_64 only. Use an x86_64 host, or build from source.
- `docker pull` fails with `denied` or `unauthorized`: the image is not publicly readable. Tell the maintainers.

Report a failed check through the [security policy](https://github.com/@REPO@/blob/main/SECURITY.md).

- [Installing a release](https://docs.telcoin.network/getting-started/installing-a-release.html)
- [Release notes](https://docs.telcoin.network/getting-started/release-notes.html)
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

cmd_check_tag() { check_tag always; }

cmd_notes() {
    local digest=$1
    if [ -n "$digest" ] && ! [[ "$digest" =~ $DIGEST_RE ]]; then die 2 "--image-digest must be sha256:<64 hex>"; fi
    COMMIT=$(tag_commit "$TAG")
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
    local notes=$WORK/notes.md digest='' line
    [ "${GITHUB_ACTIONS:-}" = true ] || die 2 "draft runs only in CI (GITHUB_ACTIONS=true)"
    COMMIT=$(tag_commit "$TAG")
    load_allowlist
    release_state
    if [ "$REL_EXISTS" = 1 ]; then
        [ "$REL_DRAFT" = true ] || die 1 "release $TAG is already published"
        [ "$REL_AUTHOR" = "$BOT_LOGIN" ] || die 1 "the draft for $TAG was created by $REL_AUTHOR, not $BOT_LOGIN"
        # A re-run after the build keeps the digest the build put into the body.
        if has_asset IMAGE_DIGEST; then
            release_fetch "$WORK/draft" IMAGE_DIGEST
            line=$(cat "$WORK/draft/IMAGE_DIGEST")
            digest=${line#"$IMAGE@"}
            if ! [[ "$digest" =~ $DIGEST_RE ]]; then digest=; fi
        fi
    fi
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

# xerxes: on a clean checkout of main's tip, bump the version and splice the CHANGELOG
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
    # The version rules of tag and check-tag, before anything is written: a release candidate
    # after its final would replace the released section at the top of CHANGELOG.md. prep
    # fetches no tags, so the final is also looked up on origin.
    check_version_order '' "$main_sha"
    if [ -n "$RC" ] && [ "$DRY_RUN" != 1 ]; then
        remote=$(ls_remote_tag "$BASE")
        [ -z "$remote" ] || die 1 "$BASE already exists on origin; no release candidate may follow its final release"
    fi
    [ -f cliff.toml ] || die 2 "cliff.toml is missing"
    [ -f CHANGELOG.md ] || die 2 "CHANGELOG.md is missing"
    anchors=$(awk -v a="$ANCHOR" '$0 == a { n++ } END { print n + 0 }' CHANGELOG.md)
    [ "$anchors" = 1 ] || die 1 "CHANGELOG.md must contain the line '$ANCHOR' exactly once (found $anchors)"

    set_workspace_version "$VERSION"
    info "cargo update --workspace"
    cargo update --workspace >&2 || die 1 "cargo update --workspace failed"

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
    local target remote rc
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
    resolve_signing_key

    CREATED_TAG=$TAG
    git -c gpg.format=openpgp tag -s -u "$SIGN_KEY" -m "Release $TAG" "$TAG" "$target" ||
        die 1 "git tag -s failed"
    COMMIT=$(tag_commit "$TAG")
    [ "$COMMIT" = "$target" ] || die 1 "the new tag points at $COMMIT, not $target"
    check_tag_signature
    [ "$SIGNER" = "$SIGN_HANDLE" ] || die 1 "the tag verifies as @$SIGNER, expected @$SIGN_HANDLE"
    check_on_main "$COMMIT"

    info "tag      $TAG ($CHANNEL, features: ${FEATURES:-none}, prerelease: $PRERELEASE)"
    info "commit   $COMMIT"
    info "signer   @$SIGNER $SIGNER_FPR"
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

# xerxes: build image and tarball from the tag, push the image, attach the artifacts.
cmd_build() {
    local driver existing rd stage dir mtime want got repo_digests sums_hex
    if [ "$(uname -s)" != Linux ] || [ "$(uname -m)" != x86_64 ]; then
        die 2 "build runs on Linux x86_64 (this host is $(uname -s) $(uname -m))"
    fi
    need docker tar gzip curl
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
    if has_asset SHA256SUMS.asc; then
        die 1 "the draft for $TAG already carries SHA256SUMS.asc; a rebuild would void those signatures"
    fi
    existing=$(registry_digest "$TAG")
    if [ -n "$existing" ]; then
        [ "${RELEASE_REBUILD:-}" = 1 ] || die 1 "$IMAGE:$TAG already exists ($existing); set RELEASE_REBUILD=1 to rebuild it"
        warn "RELEASE_REBUILD=1: rebuilding $IMAGE:$TAG (the registry has $existing)"
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

    info "building $IMAGE:$TAG (features: ${FEATURES:-none})"
    docker buildx build --builder "$RELEASE_BUILDER" --platform linux/amd64 \
        -f "$BUILD_WT/etc/Dockerfile" \
        --build-arg "CARGO_FEATURES=$FEATURES" --build-arg "GIT_SHA=$COMMIT" \
        --label "org.opencontainers.image.version=$TAG" \
        --label "org.opencontainers.image.revision=$COMMIT" \
        --provenance=false --sbom=false --pull --no-cache --load \
        -t "$IMAGE:$TAG" "$BUILD_WT" >&2 || die 1 "docker buildx build failed"

    # One build: the tarball carries the very bytes that are in the image.
    rd="$(pwd)/$(release_dir "$TAG")"
    clear_release_dir "$rd"
    mkdir -p "$rd"
    dir="telcoin-network-$TAG-$TRIPLE"
    stage=$WORK/stage
    mkdir -p "$stage/$dir"
    image_binary "$IMAGE:$TAG" "$stage/$dir/telcoin-network"
    check_version_output "$IMAGE:$TAG"
    cp "$BUILD_WT/LICENSE-APACHE" "$BUILD_WT/LICENSE-MIT" "$BUILD_WT/NOTICE" "$stage/$dir/" ||
        die 1 "license files are missing at $TAG"
    chmod 0755 "$stage/$dir" "$stage/$dir/telcoin-network"
    chmod 0644 "$stage/$dir/LICENSE-APACHE" "$stage/$dir/LICENSE-MIT" "$stage/$dir/NOTICE"
    mtime=$(git log -1 --format=%ct "$COMMIT")
    LC_ALL=C tar --sort=name --owner=0 --group=0 --numeric-owner --mtime="@$mtime" --format=gnu \
        -cf - -C "$stage" "$dir" | gzip -n -9 >"$rd/$TARBALL" || die 1 "packaging $TARBALL failed"

    info "pushing $IMAGE:$TAG"
    docker push "$IMAGE:$TAG" >&2 || die 3 "docker push $IMAGE:$TAG failed"
    D=$(registry_digest "$TAG")
    [ -n "$D" ] || die 1 "the registry has no $IMAGE:$TAG after the push"
    repo_digests=$(docker image inspect --format '{{json .RepoDigests}}' "$IMAGE:$TAG") ||
        die 1 "docker image inspect $IMAGE:$TAG failed"
    case "$repo_digests" in
        *"\"$IMAGE@$D\""*) ;;
        *) die 1 "the registry serves $D for $IMAGE:$TAG, which is not the image just built ($repo_digests)" ;;
    esac
    printf '%s@%s\n' "$IMAGE" "$D" >"$rd/IMAGE_DIGEST"
    write_sums "$rd"

    release_upload "$rd/IMAGE_DIGEST" "$rd/SHA256SUMS" "$rd/$TARBALL"
    render_notes "$D" >"$WORK/notes.md"
    release_set_notes "$WORK/notes.md"
    cmd_verify 1
    sums_hex=$(sha256_hex "$rd/SHA256SUMS")
    printf 'SHA256SUMS sha256: %s\n' "$sums_hex"
}

# laptop: append this maintainer's detached signature over SHA256SUMS to SHA256SUMS.asc.
cmd_sign() {
    local rd before after recount n_before n_after n_back sums_hex up_hex back_hex
    need gpg curl
    check_tag if-cast
    release_state
    [ "$REL_EXISTS" = 1 ] || die 2 "no draft release for $TAG"
    [ "$REL_DRAFT" = true ] || die 1 "release $TAG is already published; signatures go on the draft"
    check_asset_set IMAGE_DIGEST SHA256SUMS "$TARBALL" -- SHA256SUMS.asc
    rd="$(pwd)/$(release_dir "$TAG")"
    clear_release_dir "$rd"
    if has_asset SHA256SUMS.asc; then
        release_fetch "$rd" IMAGE_DIGEST SHA256SUMS "$TARBALL" SHA256SUMS.asc
    else
        release_fetch "$rd" IMAGE_DIGEST SHA256SUMS "$TARBALL"
    fi
    check_sums "$rd"
    check_image_digest_file "$rd"
    resolve_signing_key
    before=
    if [ -f "$rd/SHA256SUMS.asc" ]; then before=$(count_sigs "$rd/SHA256SUMS" "$rd/SHA256SUMS.asc"); fi
    n_before=$(printf '%s\n' "$before" | count_lines)
    case $'\n'"$before"$'\n' in
        *$'\n'"$SIGN_HANDLE"$'\n'*) die 1 "SHA256SUMS for $TAG is already signed by @$SIGN_HANDLE" ;;
    esac

    sums_hex=$(sha256_hex "$rd/SHA256SUMS")
    info "tag        $TAG"
    info "commit     $COMMIT"
    info "signer     @$SIGN_HANDLE $SIGN_FPR"
    info "signatures $n_before so far, $THRESHOLD required"
    info "SHA256SUMS sha256: $sums_hex (compare with the last line of make release-build)"
    info "$(cat "$rd/SHA256SUMS")"
    confirm "Sign SHA256SUMS for $TAG?"
    gpg --armor --detach-sign --local-user "$SIGN_KEY" --output "$WORK/new.asc" "$rd/SHA256SUMS" ||
        die 1 "gpg could not sign SHA256SUMS"
    if [ -f "$rd/SHA256SUMS.asc" ]; then
        cat "$rd/SHA256SUMS.asc" "$WORK/new.asc" >"$WORK/combined.asc"
    else
        cp "$WORK/new.asc" "$WORK/combined.asc"
    fi
    after=$(count_sigs "$rd/SHA256SUMS" "$WORK/combined.asc")
    n_after=$(printf '%s\n' "$after" | count_lines)
    [ "$n_after" = $((n_before + 1)) ] || die 1 "the new signature did not count (before $n_before, after $n_after)"
    case $'\n'"$after"$'\n' in
        *$'\n'"$SIGN_HANDLE"$'\n'*) ;;
        *) die 1 "the new signature does not verify as @$SIGN_HANDLE" ;;
    esac
    mv "$WORK/combined.asc" "$rd/SHA256SUMS.asc"
    release_upload "$rd/SHA256SUMS.asc"

    # Read it back from the release, so what operators download is what was counted. A
    # maintainer signing at the same moment replaces the whole file (assets have no
    # compare-and-swap), so it must be byte for byte the file just uploaded.
    release_fetch "$WORK/readback" SHA256SUMS SHA256SUMS.asc
    up_hex=$(sha256_hex "$rd/SHA256SUMS.asc")
    back_hex=$(sha256_hex "$WORK/readback/SHA256SUMS.asc")
    [ "$back_hex" = "$up_hex" ] ||
        die 1 "SHA256SUMS.asc on the release is not the file just uploaded (did another maintainer sign at the same time?); run make release-sign again"
    recount=$(count_sigs "$WORK/readback/SHA256SUMS" "$WORK/readback/SHA256SUMS.asc")
    case $'\n'"$recount"$'\n' in
        *$'\n'"$SIGN_HANDLE"$'\n'*) ;;
        *) die 1 "the uploaded SHA256SUMS.asc carries no signature by @$SIGN_HANDLE" ;;
    esac
    n_back=$(printf '%s\n' "$recount" | count_lines)
    [ "$n_back" = "$n_after" ] || die 1 "the uploaded SHA256SUMS.asc counts $n_back signatures, expected $n_after"
    printf 'signed %s as @%s: %s/%s signatures\n' "$TAG" "$SIGN_HANDLE" "$n_after" "$THRESHOLD"
}

# xerxes, CI, operators: re-check everything about a release. $1 = 1 allows a missing
# SHA256SUMS.asc (build's own self-check before anyone has signed).
cmd_verify() {
    local allow_unsigned=$1 dir handles n=0 tar_bin_hex image_bin_hex
    need docker tar gzip curl gpg
    check_tag always
    release_state
    [ "$REL_EXISTS" = 1 ] || die 2 "no GitHub release for $TAG"
    if [ "$REL_DRAFT" = false ] && [ "$REL_PRERELEASE" != "$PRERELEASE" ]; then
        die 1 "release $TAG is published with prerelease=$REL_PRERELEASE; expected $PRERELEASE"
    fi
    if [ "$allow_unsigned" = 1 ]; then
        check_asset_set IMAGE_DIGEST SHA256SUMS "$TARBALL" -- SHA256SUMS.asc
    else
        check_asset_set IMAGE_DIGEST SHA256SUMS SHA256SUMS.asc "$TARBALL"
    fi
    dir=$WORK/verify
    rm -rf "$dir"
    if has_asset SHA256SUMS.asc; then
        release_fetch "$dir" IMAGE_DIGEST SHA256SUMS SHA256SUMS.asc "$TARBALL"
    else
        release_fetch "$dir" IMAGE_DIGEST SHA256SUMS "$TARBALL"
    fi
    check_fetched_digests "$dir"
    check_sums "$dir"
    if [ -f "$dir/SHA256SUMS.asc" ]; then
        handles=$(count_sigs "$dir/SHA256SUMS" "$dir/SHA256SUMS.asc")
        n=$(printf '%s\n' "$handles" | count_lines)
    fi
    if [ "$allow_unsigned" != 1 ] && [ "$n" -lt "$THRESHOLD" ]; then
        die 1 "SHA256SUMS carries $n of the $THRESHOLD required maintainer signatures"
    fi
    check_image_digest_file "$dir"
    check_tarball "$dir/$TARBALL" "$dir/x"
    docker pull --platform linux/amd64 "$IMAGE@$D" >&2 || die 3 "docker pull $IMAGE@$D failed"
    image_binary "$IMAGE@$D" "$dir/image-telcoin"
    image_bin_hex=$(sha256_hex "$dir/image-telcoin")
    tar_bin_hex=$(sha256_hex "$dir/x/telcoin-network-$TAG-$TRIPLE/telcoin-network")
    [ "$image_bin_hex" = "$tar_bin_hex" ] || die 1 "the tarball's telcoin-network differs from /usr/local/bin/telcoin in $IMAGE@$D"
    check_version_output "$IMAGE@$D"
    printf 'verified %s commit=%s signatures=%s/%s image=%s@%s\n' "$TAG" "$COMMIT" "$n" "$THRESHOLD" "$IMAGE" "$D"
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

# xerxes: verify, publish the draft, move the channel alias. Safe to re-run.
cmd_publish() {
    local published highest latest=--latest=false move_alias=0 current
    need docker
    require_local_image_in_dry_run
    fetch_tag
    check_scripts_from_main
    cmd_verify 0
    published=$(published_versions) || exit
    highest=$(printf '%s\n%s\n' "$published" "$VERSION" | version_max)
    if [ -z "$RC" ] && [ "$highest" = "$VERSION" ]; then
        move_alias=1
        if [ "$CHANNEL" = mainnet ]; then latest=--latest; fi
    fi
    # Checked before anything is made public: the alias move needs the registry credential.
    if [ "$move_alias" = 1 ]; then require_registry_login; fi

    info "tag      $TAG (commit $COMMIT)"
    info "image    $IMAGE@$D"
    if [ "$REL_DRAFT" = true ]; then
        info "release  draft -> published, prerelease=$PRERELEASE, $latest"
    else
        info "release  already published"
    fi
    if [ "$move_alias" = 1 ]; then
        info "alias    $IMAGE:$ALIAS -> $D"
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
    # Catches a change between the check above and the edit, before the alias points at D.
    check_assets_unchanged "the release is public: follow After publish in the maintainer guide"
    if [ "$move_alias" = 1 ]; then
        current=$(registry_digest "$ALIAS")
        if [ "$current" = "$D" ]; then
            info "$IMAGE:$ALIAS already points at $D"
        else
            # --prefer-index=false copies the manifest as is, so the alias keeps digest D.
            docker buildx imagetools create --prefer-index=false --tag "$IMAGE:$ALIAS" "$IMAGE@$D" >&2 ||
                die 3 "cannot move $IMAGE:$ALIAS (if docker reported unauthorized or denied, run make docker-login and publish again)"
            current=$(registry_digest "$ALIAS")
            [ "$current" = "$D" ] || die 1 "$IMAGE:$ALIAS points at ${current:-nothing}, expected $D"
        fi
    fi
    printf 'published %s image=%s@%s alias=%s\n' "$TAG" "$IMAGE" "$D" "$(if [ "$move_alias" = 1 ]; then printf '%s' "$ALIAS"; else printf none; fi)"
}

# ---------------------------------------------------------------------------------------------
# Setup and dispatch

usage() {
    cat >&2 <<'EOF'
usage: etc/release.sh <subcommand> <TAG> [options]

  parse TAG                          print TAG's channel, features, alias and file names
  check-tag TAG                      validate a pushed release tag (CI, xerxes, laptop)
  notes TAG [--image-digest D]       render the release body (CI, xerxes)
  draft TAG                          create or refresh the draft release (CI only)
  prep TAG                           bump the version and write the CHANGELOG section (xerxes)
  tag TAG                            sign and push the release tag (laptop)
  build TAG                          build, push and attach the artifacts (xerxes)
  sign TAG                           add a maintainer signature to SHA256SUMS.asc (laptop)
  verify TAG [--allow-unsigned]      check a release end to end (xerxes, CI, operators)
  publish TAG                        verify, publish the draft, move the alias (xerxes)

TAG is vX.Y.Z, vX.Y.Z-rcN, vX.Y.Z-adiri or vX.Y.Z-adiri-rcN.
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

    if [ -n "${RELEASE_IMAGE:-}" ]; then
        IMAGE=$RELEASE_IMAGE
        is_local_image ||
            die 2 "RELEASE_IMAGE must be localhost:PORT/ or 127.0.0.1:PORT/ and a lowercase repository path without a tag or digest, e.g. 127.0.0.1:5000/tn"
    else
        IMAGE=ghcr.io/$(lower "$REPO")
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
