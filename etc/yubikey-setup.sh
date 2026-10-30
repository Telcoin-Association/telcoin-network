#!/usr/bin/env bash
#
# One-time YubiKey signing setup for telcoin-network maintainers. Runs the steps of
# docs/src/maintainers/yubikey-setup.md from "Install the tools" through "Test signing" on a
# Mac, reading what is already in place before each step and asking only for what is missing,
# so a run can be repeated or resumed. The release signing key is generated inside the
# YubiKey and never exists anywhere else. The spare YubiKey, extending the expiry and the
# loss and rotation flows stay manual; so do the optional kdf-setup and card url.
#
# Usage: etc/yubikey-setup.sh check | run | help   (or: make yubikey-setup)
#
# Flow (run):
#   1. tools      brew gnupg, pinentry-mac and ykman (>= 5.6.0); ~/.gnupg and gpg-agent.conf
#   2. yubikey    exactly one YubiKey, firmware >= 5.2.3, OpenPGP enabled; find an existing key
#                 (the card counts as new only when all three key slots are empty)
#   3. pins       change the PIN and Admin PIN (ykman prompts for them)
#   4. identity   GitHub login (gh, else git config github.user, else a prompt), name and
#                 email for the user ID
#   5. key        generated on the YubiKey: gpg --card-edit admin, key-attr (ECC, Curve 25519
#                 for all three keys), generate (no off-card backup, 2 years); revocation
#                 certificate kept; touch policy on
#   6. export     ~/<handle>.asc
#   7. allowlist  .github/maintainer-gpg-keys/<handle>.asc, the SECURITY.md row and the
#                 allowlist README row on the current branch, staged but not committed
#   8. git        user.signingkey (FPR!) and gpg.program
#   9. github     the public key on the GitHub account, with a verified email; gh is optional
#                 (an OAuth login uploads the key, anything else is done by hand), and the
#                 read-back uses https://github.com/<handle>.gpg and the public API
#  10. test       a signed tag and a detached signature, verified locally
#
# The key is one OpenPGP key: the primary key (sign and certify, ed25519) in the card's
# signature slot, an encryption subkey (cv25519) and an authentication subkey (ed25519) in the
# other two slots, all expiring in 2 years. The primary key is the signing key, so its
# fingerprint (FPR) is what git, SECURITY.md, the allowlist and etc/release.sh match. A lost or
# broken YubiKey, or a blocked Admin PIN, means a new key ("Lost or broken YubiKey" in the
# runbook).
#
# check runs the detection of every step and prints one line per item (ok, todo, missing,
# unknown). It writes nothing, except that gpg may start its daemons and create an empty
# keybox in ~/.gnupg.
#
# Exit codes:
#   0  ok
#   1  a check failed, the Mac is in a state the script does not handle, or the user declined
#   2  usage or precondition: wrong OS, missing tool, no TTY, a forbidden environment variable
#      or gpg.conf option
#
# Security rules (enforced below, not configurable):
#   - PINs are typed only into pinentry-mac or ykman's hidden prompt. The script never reads
#     them and never passes --pin, --new-pin, --admin-pin or --passphrase.
#   - The private key is generated inside the YubiKey and never leaves it: the script never
#     calls --export-secret-keys, keytocard or gpg's off-card backup (it answers no to "Make
#     off-card backup of encryption key?").
#   - gpg's card editor is driven by answering each question on its status fd by keyword. An
#     unexpected question, including any GET_HIDDEN, stops gpg while it waits for the answer,
#     so no question is ever left to gpg's default.
#   - Every mutating step is read back from the card, the keyring, the file or the API.
#   - Every ykman call carries --device SERIAL; gpg calls that may prompt carry
#     --pinentry-mode ask.
#   - The script refuses GH_TOKEN, GITHUB_TOKEN and GH_HOST, and a gpg.conf that sets
#     pinentry-mode loopback, batch, yes or a passphrase option.
#   - Destructive or irreversible actions wait for a typed "yes": OpenPGP reset, key
#     generation (the user ID and the card are then committed), replacing a git or gpg-agent
#     setting, the optional scratch-repository test.
#   - The OpenPGP reset is never offered for a card whose key is in the allowlist on
#     origin/main or on the GitHub account, nor when main cannot be fetched from origin, its
#     allowlist cannot be read, or the key list of a known GitHub login cannot be downloaded.
#   - The script never commits, pushes or opens a pull request.
#
# State: none. Everything is read from the card, ~/.gnupg, the checkout and GitHub.
#
# Portable to macOS's bash 3.2: no associative arrays, mapfile, ${x,,}, sed -i, coproc or GNU
# date.

set -euo pipefail
# Keeps -e inside $(...) on bash >= 4.4. bash 3.2 has no such option, so every function whose
# output is captured checks its own failures and calls die.
shopt -s inherit_errexit 2>/dev/null || true
umask 077

# ---------------------------------------------------------------------------------------------
# Constants

MIN_GPG=2.2.0
# ykman 5.6.0 is the first release with `ykman openpgp keys info`, which set_touch_policy reads;
# `openpgp access change-pin` and `change-admin-pin` date from 5.0.0.
MIN_YKMAN=5.6.0
MIN_FIRMWARE=5.2.3
KEY_EXPIRY=2y
# Right after generation the key must expire between these many days from now (2y is 730).
EXPIRY_MIN_DAYS=600
EXPIRY_MAX_DAYS=740
# The RAM disk the earlier version of this script worked in.
OLD_RAMDISK=/Volumes/tn-gpg
ALLOWLIST_PATH=.github/maintainer-gpg-keys
SECURITY_FILE=SECURITY.md
DOC=docs/src/maintainers/yubikey-setup.md
GPG_KEYS_URL='https://github.com/settings/gpg/new'
EMAILS_URL='https://github.com/settings/emails'

HANDLE_RE='^[A-Za-z0-9-]+$'
FPR_RE='^[0-9A-F]{40}$'
SERIAL_RE='^[1-9][0-9]*$'
AID_RE='^D27600012401[0-9A-F]{20}$'
VERSION_RE='^[0-9]+\.[0-9]+\.[0-9]+$'
EMAIL_RE='^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]+$'
# gpg wants 5 or more characters, not starting with a digit; < > ( ) : would change how the
# user ID reads back.
NAME_RE='^[^<>():0-9][^<>():]{4,}$'

# State. The functions named in the comments own each variable.
MODE=             # main: check or run
WORK=             # setup: private temp dir, removed on exit
HOME_GNUPG=$HOME/.gnupg
BREW_BIN=         # stage_tools
GPG=              # stage_tools: brew's gpg, gpgconf, ykman and pinentry-mac
GPGCONF=
YKMAN=
PINENTRY=
SERIAL=           # stage_card: the YubiKey's decimal serial
FIRMWARE=         # stage_card
CARD_AID=         # read_card_slot: OpenPGP application id (32 hex)
CARD_SIG_FPR=     # read_card_slot: fingerprint in the card's signature slot, empty when none
CARD_ENC_FPR=     # read_card_slot: the same for the encryption slot
CARD_AUT_FPR=     # read_card_slot: the same for the authentication slot
CARD_COLONS=      # read_card_slot: the gpg --card-status --with-colons output it read
PINS_DEFAULT=0    # stage_card_state: the card was just reset, so the PINs are the defaults
PHASE=            # discover_key: fresh, import, ready or foreign
PHASE_DETAIL=     # discover_key: text for foreign
KEYFILE=          # discover_key: the file holding the public key of the card's key, for import
HANDLE=           # stage_identity
NAME=             # stage_identity
EMAIL=            # stage_identity / discover_key
FPR=              # discover_key / stage_key: the key's fingerprint, in the signature slot
PUBFILE=          # stage_export: ~/<handle>.asc
TOKEN_TYPE=       # stage_github: oauth, fine-grained-pat, classic-pat, unknown, or none (no gh login)
STAGED=           # stage_allowlist: files handed to git add
EDITOR_PID=       # drive_gpg_editor: the gpg it runs in the background, for cleanup
EDITOR_KEYWORD=   # drive_gpg_editor: the question it did not answer
GENERATED=0       # generate_key: the key was generated in this run

# ---------------------------------------------------------------------------------------------
# Helpers

die() {
    local code=$1
    shift
    printf 'error: %s\n' "$*" >&2
    exit "$code"
}

warn() { printf 'warning: %s\n' "$*" >&2; }

# Progress goes to stderr so stdout carries only results (check lines, fingerprints, summary).
info() { printf '%s\n' "$*" >&2; }

step() { if [ "$MODE" = run ]; then printf '\n== %s\n' "$*" >&2; fi; }

need() {
    local tool
    for tool in "$@"; do
        command -v "$tool" >/dev/null 2>&1 || die 2 "missing required tool: $tool"
    done
}

matches() { [[ "$1" =~ $2 ]]; }

lower() { printf '%s\n' "$1" | tr '[:upper:]' '[:lower:]'; }

count_lines() { awk 'NF { n++ } END { print n + 0 }'; }

# check mode: one line per item on stdout.
report() { printf '%-18s %-8s %s\n' "$1" "$2" "${3:-}"; }

ok() { if [ "$MODE" = check ]; then report "$1" ok "${2:-}"; fi; }

# In check mode: report the item as todo and return 1. In run mode: return 0, so the caller
# acts. Usage: if todo item "detail"; then <act>; fi
todo() {
    if [ "$MODE" = check ]; then
        report "$1" todo "${2:-}"
        return 1
    fi
    return 0
}

require_tty() { [ -t 0 ] || die 2 "a prompt is needed but stdin is not a terminal"; }

# Prompts that guard a destructive or irreversible action need an explicit "yes".
confirm() {
    local answer=
    require_tty
    printf '%s Type yes to continue: ' "$1" >&2
    read -r answer || true
    [ "$answer" = yes ] || die 1 "aborted"
}

ask_yn() {
    local answer=
    require_tty
    while :; do
        printf '%s [y/n] ' "$1" >&2
        read -r answer || die 1 "aborted"
        case "$answer" in
            y | Y | yes) return 0 ;;
            n | N | no) return 1 ;;
        esac
    done
}

ask() {
    local answer=
    require_tty
    printf '%s: ' "$1" >&2
    read -r answer || die 1 "aborted"
    printf '%s\n' "$answer"
}

pause() {
    local answer=
    require_tty
    printf '%s Press Enter to continue. ' "$1" >&2
    read -r answer || die 1 "aborted"
}

# version_ge A B: dotted versions, numeric per field, missing fields count as 0.
version_ge() {
    local i x y
    for i in 1 2 3; do
        x=$(printf '%s' "$1" | cut -d. -f"$i")
        y=$(printf '%s' "$2" | cut -d. -f"$i")
        matches "$x" '^[0-9]+$' || x=0
        matches "$y" '^[0-9]+$' || y=0
        if [ "$x" -gt "$y" ]; then return 0; fi
        if [ "$x" -lt "$y" ]; then return 1; fi
    done
    return 0
}

resolve_path() {
    local p=$1 t i
    for i in 1 2 3 4 5 6 7 8; do
        t=$(readlink "$p" 2>/dev/null) || break
        case "$t" in
            /*) p=$t ;;
            *) p=$(dirname "$p")/$t ;;
        esac
    done
    printf '%s/%s\n' "$(cd "$(dirname "$p")" 2>/dev/null && pwd -P)" "$(basename "$p")"
}

# ---------------------------------------------------------------------------------------------
# Parsers. Each reads one command's output on stdin, so tests can feed fixtures.

# `ykman list`: the serial of the single listed device; fails unless exactly one line.
ykman_serial() {
    awk 'NF { n++; if (match($0, /Serial: [0-9]+$/)) s = substr($0, RSTART + 8, RLENGTH - 8) }
         END { if (n == 1 && s != "") print s; else exit 1 }'
}

# ykman_field NAME: the value of the top-level "NAME:" line of ykman info, openpgp info or
# keys info output.
ykman_field() {
    awk -v k="$1:" 'index($0, k) == 1 {
        v = substr($0, length(k) + 1); sub(/^[[:space:]]+/, "", v); sub(/[[:space:]]+$/, "", v)
        print v; exit }'
}

# ykman_app_enabled APP (`ykman info`): the USB column of APP in the Applications table.
ykman_app_enabled() {
    awk -v app="$1" 'apps && $1 == app { print $2; exit } /^Applications/ { apps = 1 }'
}

# card_field RECORD [N] (`gpg --card-status --with-colons`): field N (default 2) of RECORD.
# The fpr record holds the signature, encryption and authentication slots in fields 2, 3, 4.
card_field() {
    awk -F: -v r="$1" -v n="${2:-2}" '$1 == r { print $n; exit }'
}

# card_keyattr N (`gpg --card-status --with-colons`): "ALGO:ATTR" of key slot N (1 signature,
# 2 encryption, 3 authentication). ALGO is the OpenPGP algorithm number (1 RSA, 18 ECDH,
# 22 EdDSA); ATTR is the RSA size or the curve name.
card_keyattr() {
    awk -F: -v n="$1" '$1 == "keyattr" && $2 == n { print $3 ":" $4; exit }'
}

# The OpenPGP application id on the Reader: line of `gpg --card-status --with-colons`.
card_aid() {
    awk -F: '$1 == "Reader" {
        for (i = 2; i <= NF; i++)
            if (length($i) == 32 && $i ~ /^D27600012401[0-9A-F]+$/) { print $i; exit } }'
}

# aid_serial AID: the decimal serial in characters 21-28 of the application id.
aid_serial() {
    local s=${1:20:8}
    s=${s#"${s%%[!0]*}"}
    printf '%s\n' "${s:-0}"
}

# key_field FPR N (`--list-keys`, `--list-secret-keys` or `--show-keys` --with-colons): field N
# of the pub/sub/sec/ssb record whose fingerprint is FPR. Field 2 holds the validity ("u" for
# ultimate), field 4 the algorithm, field 7 the expiry, field 12 the usage letters, field 15
# the secret key location: "#" for a stub, "+" for a secret key on this Mac, the card's serial
# number for a key on a card.
key_field() {
    awk -F: -v f="$1" -v n="$2" '
        $1 == "pub" || $1 == "sub" || $1 == "sec" || $1 == "ssb" { rec = $0; next }
        $1 == "fpr" && $10 == f { split(rec, a, ":"); print a[n]; exit }'
}

# key_usage FPR: the key's own usage letters (lowercase), for example "sc" or "e".
key_usage() { key_field "$1" 12 | tr -cd '[:lower:]'; }

# list_fprs TYPE: fingerprints of every pub, sub, sec or ssb record on stdin.
list_fprs() {
    awk -F: -v t="$1" '
        $1 == t { want = 1; next }
        $1 == "pub" || $1 == "sub" || $1 == "sec" || $1 == "ssb" { want = 0; next }
        $1 == "fpr" && want { print $10; want = 0 }'
}

# primary_of FPR: the primary fingerprint of the key that holds FPR as primary or subkey.
primary_of() {
    awk -F: -v f="$1" '
        $1 == "pub" || $1 == "sec" { want = 1; next }
        $1 == "fpr" { if (want) { prim = $10; want = 0 } if ($10 == f) { print prim; exit } }'
}

# key_block FPR: the records of the key whose primary fingerprint is FPR, from its pub or sec
# record up to the next key. A file can hold several keys (a spare YubiKey's key, or every key
# of a GitHub account).
key_block() {
    awk -F: -v f="$1" '
        $1 == "pub" || $1 == "sec" { head = $0; want = 1; inkey = 0; next }
        want && $1 == "fpr" { want = 0; inkey = ($10 == f); if (inkey) { print head; print }; next }
        inkey { print }'
}

# The email of the first user ID.
uid_email() {
    awk -F: '$1 == "uid" && match($10, /<[^<>]+>$/) { print substr($10, RSTART + 1, RLENGTH - 2); exit }'
}

# The first user ID, as gpg prints it in field 10.
uid_text() { awk -F: '$1 == "uid" { print $10; exit }'; }

# `ykman openpgp keys info sig`: Off, On, On (fixed), Cached or Cached (fixed).
touch_policy() { ykman_field "Touch policy"; }

# `gh auth status` text: the kind of credential behind the active login.
gh_token_type() {
    awk '/^[[:space:]]*-[[:space:]]*Token:/ { t = $NF }
         END {
             if (t ~ /^github_pat_/) print "fine-grained-pat"
             else if (t ~ /^ghp_/) print "classic-pat"
             else if (t ~ /^gh[ou]_/) print "oauth"
             else print "unknown" }'
}

# `gh auth status` text: the scopes of the active login, space separated; empty for a
# fine-grained token, which shows no scopes line.
gh_scopes() {
    awk -F'Token scopes:' '/Token scopes:/ {
        s = $2; gsub(/[\047,]/, "", s); sub(/^[[:space:]]+/, "", s); sub(/[[:space:]]+$/, "", s)
        print s; exit }'
}

# mounted_at DIR (`mount`): succeeds when a line of the mount table has DIR as mount point.
mounted_at() { awk -v d="$1" '$2 == "on" && $3 == d { found = 1 } END { exit !found }'; }

# conf_value FILE OPTION: the value of the last uncommented OPTION line of a gpg-style config
# file ("option value" or "option=value"); fails when the option is absent.
conf_value() {
    [ -f "$1" ] || return 1
    awk -v o="$2" '
        /^[[:space:]]*#/ { next }
        $1 == o { v = substr($0, index($0, o) + length(o)); found = 1 }
        index($1, o "=") == 1 { v = substr($0, index($0, o "=") + length(o) + 1); found = 1 }
        END { if (!found) exit 1; sub(/^[[:space:]]+/, "", v); sub(/[[:space:]]+$/, "", v); print v }' "$1"
}

conf_has() { conf_value "$1" "$2" >/dev/null; }

# replace_table_row FILE KEY ROW: replace the markdown table row whose first cell is KEY.
# Returns 0 when one row was replaced, 1 when there is none, 2 when there are several.
replace_table_row() {
    local file=$1 tmp=$WORK/row.tmp n
    n=$(KEY=$2 ROW=$3 awk -v out="$tmp" '
        BEGIN { prefix = "| " ENVIRON["KEY"] " |" }
        index($0, prefix) == 1 { print ENVIRON["ROW"] > out; n++; next }
        { print > out }
        END { print n + 0 }' "$file") || return 2
    case "$n" in
        1) cp "$tmp" "$file" ;;
        0) return 1 ;;
        *) return 2 ;;
    esac
}

# ---------------------------------------------------------------------------------------------
# gpg and ykman wrappers

gpg_home() { "$GPG" --homedir "$HOME_GNUPG" "$@"; }

# Only one scdaemon can hold the card, and ykman needs it free.
kill_scdaemons() {
    [ -n "$GPGCONF" ] || return 0
    "$GPGCONF" --homedir "$HOME_GNUPG" --kill scdaemon >/dev/null 2>&1 || true
}

yk_list() {
    kill_scdaemons
    "$YKMAN" list
}

yk() {
    kill_scdaemons
    "$YKMAN" --device "$SERIAL" "$@"
}

read_card_slot() {
    CARD_COLONS=$(gpg_home --card-status --with-colons 2>/dev/null) || die 1 "gpg cannot read the YubiKey; see Troubleshooting in $DOC"
    CARD_AID=$(card_aid <<<"$CARD_COLONS")
    matches "$CARD_AID" "$AID_RE" || die 1 "gpg --card-status shows no OpenPGP application id"
    [ "$(aid_serial "$CARD_AID")" = "$SERIAL" ] ||
        die 1 "gpg sees the card $CARD_AID (serial $(aid_serial "$CARD_AID")) but ykman reports serial $SERIAL; unplug every other card"
    CARD_SIG_FPR=$(card_field fpr 2 <<<"$CARD_COLONS")
    CARD_ENC_FPR=$(card_field fpr 3 <<<"$CARD_COLONS")
    CARD_AUT_FPR=$(card_field fpr 4 <<<"$CARD_COLONS")
    if [ -n "$CARD_SIG_FPR" ]; then
        matches "$CARD_SIG_FPR" "$FPR_RE" || die 1 "unexpected signature fingerprint on the card: $CARD_SIG_FPR"
    fi
}

# card_empty: none of the card's three key slots holds a key.
card_empty() { [ -z "$CARD_SIG_FPR$CARD_ENC_FPR$CARD_AUT_FPR" ]; }

# on_card LOCATION: the secret key location field (key_field 15) names this YubiKey.
on_card() {
    [ -n "$CARD_AID" ] && [ -n "$SERIAL" ] || return 1
    case "$1" in
        *"$CARD_AID"* | *"$SERIAL"*) return 0 ;;
    esac
    return 1
}

assert_this_card() {
    local s
    s=$(yk_list 2>/dev/null | ykman_serial) || die 1 "ykman no longer lists exactly one YubiKey"
    [ "$s" = "$SERIAL" ] || die 1 "the plugged-in YubiKey is $s, not $SERIAL"
}

# ---------------------------------------------------------------------------------------------
# 0. preflight

preflight() {
    local v conf=$HOME_GNUPG/gpg.conf opt
    [ "$(uname -s)" = Darwin ] || die 2 "this script runs on macOS only"
    [ "$(id -u)" != 0 ] || die 2 "do not run this script as root"
    case "${GNUPGHOME:-}" in
        '' | "$HOME_GNUPG" | "$HOME_GNUPG/") ;;
        *) die 2 "GNUPGHOME is set to $GNUPGHOME; unset it, the script names every keyring explicitly" ;;
    esac
    unset GNUPGHOME
    for v in GH_TOKEN GITHUB_TOKEN GH_HOST; do
        [ -z "${!v:-}" ] || die 2 "$v is set; unset it so gh uses your keyring login"
    done
    if [ -f "$conf" ]; then
        for opt in batch yes passphrase passphrase-fd passphrase-file passphrase-repeat; do
            if conf_has "$conf" "$opt"; then die 2 "$conf sets '$opt', which lets gpg skip or answer prompts; remove it"; fi
        done
        if [ "$(conf_value "$conf" pinentry-mode 2>/dev/null || true)" = loopback ]; then
            die 2 "$conf sets pinentry-mode loopback; remove it so PINs go through pinentry-mac"
        fi
    fi
    if [ "$MODE" = run ]; then [ -t 0 ] || die 2 "run needs a terminal"; fi
    need git awk brew date mktemp mkfifo mount ps cmp tr cut sed readlink curl
    git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die 2 "not inside the telcoin-network checkout"
    check_old_ramdisk
}

# The earlier version of this script built a key in a RAM disk at /Volumes/tn-gpg. That key
# was never used and is not needed; the disk only has to be closed.
check_old_ramdisk() {
    local close="gpgconf --homedir $OLD_RAMDISK/gnupg --kill all; diskutil eject $OLD_RAMDISK"
    [ -d "$OLD_RAMDISK" ] || return 0
    if ! mount | mounted_at "$OLD_RAMDISK"; then return 0; fi
    if [ "$MODE" = check ]; then
        report ramdisk todo "leftover RAM disk from the earlier version of this script; close it: $close"
        return 0
    fi
    die 1 "$OLD_RAMDISK is a RAM disk left by the earlier version of this script. The key in it was never used and is not needed. Close it, then run again: $close"
}

# ---------------------------------------------------------------------------------------------
# 1. tools

stage_tools() {
    local missing='' entry formula tool ver ykver mode agentconf cur want tmp
    step "1. tools"
    BREW_BIN=$(brew --prefix)/bin
    for entry in gnupg:gpg gnupg:gpgconf pinentry-mac:pinentry-mac ykman:ykman; do
        formula=${entry%%:*}
        tool=${entry#*:}
        if [ ! -x "$BREW_BIN/$tool" ]; then
            case " $missing " in *" $formula "*) ;; *) missing="$missing $formula" ;; esac
        fi
    done
    if [ -n "$missing" ]; then
        if todo tools "brew install$missing"; then
            confirm "Install with brew:$missing."
            # shellcheck disable=SC2086
            brew install $missing || die 2 "brew install failed"
            for entry in gnupg:gpg gnupg:gpgconf pinentry-mac:pinentry-mac ykman:ykman; do
                [ -x "$BREW_BIN/${entry#*:}" ] || die 2 "$BREW_BIN/${entry#*:} is still missing after brew install"
            done
        else
            return 1
        fi
    fi
    GPG=$BREW_BIN/gpg
    GPGCONF=$BREW_BIN/gpgconf
    YKMAN=$BREW_BIN/ykman
    PINENTRY=$BREW_BIN/pinentry-mac
    ver=$("$GPG" --version | awk 'NR == 1 { print $3 }') || die 2 "$GPG --version failed"
    matches "$ver" "$VERSION_RE" || die 2 "cannot parse the gpg version from '$ver'"
    version_ge "$ver" "$MIN_GPG" || die 2 "gpg $ver is older than $MIN_GPG"
    ykver=$("$YKMAN" --version 2>/dev/null | awk 'NR == 1 { print $NF }') ||
        die 2 "$YKMAN --version failed; run it to see why, or reinstall ykman with brew reinstall ykman"
    matches "$ykver" "$VERSION_RE" || die 2 "cannot parse the ykman version from '$ykver'"
    version_ge "$ykver" "$MIN_YKMAN" || die 2 "ykman $ykver is older than $MIN_YKMAN; run brew upgrade ykman"
    ok tools "gpg $ver, ykman $ykver, pinentry-mac in $BREW_BIN"

    if [ ! -d "$HOME_GNUPG" ]; then
        if todo gnupg-dir "create $HOME_GNUPG"; then mkdir -m 700 "$HOME_GNUPG"; fi
    fi
    if [ -d "$HOME_GNUPG" ]; then
        mode=$(stat -f %Lp "$HOME_GNUPG")
        if [ "$mode" != 700 ]; then
            if todo gnupg-dir "mode $mode, want 700"; then chmod 700 "$HOME_GNUPG"; fi
        else
            ok gnupg-dir "$HOME_GNUPG mode 700"
        fi
    fi

    agentconf=$HOME_GNUPG/gpg-agent.conf
    want=$PINENTRY
    cur=$(conf_value "$agentconf" pinentry-program 2>/dev/null || true)
    tmp=$WORK/agent.conf
    if [ -z "$cur" ]; then
        if todo agent-pinentry "add pinentry-program $want to $agentconf"; then
            printf 'pinentry-program %s\n' "$want" >>"$agentconf"
        fi
    elif [ "$cur" != "$want" ]; then
        if todo agent-pinentry "pinentry-program is $cur, want $want"; then
            confirm "Replace pinentry-program '$cur' with '$want' in $agentconf."
            awk -v o=pinentry-program '/^[[:space:]]*#/ { print; next } $1 == o || index($1, o "=") == 1 { next } { print }' "$agentconf" >"$tmp"
            printf 'pinentry-program %s\n' "$want" >>"$tmp"
            cp "$tmp" "$agentconf"
        fi
    else
        ok agent-pinentry "$agentconf: pinentry-program $want"
    fi
    if ! conf_has "$agentconf" no-allow-external-cache; then
        if todo agent-cache "add no-allow-external-cache to $agentconf (no 'Save in Keychain' box)"; then
            printf 'no-allow-external-cache\n' >>"$agentconf"
        fi
    else
        ok agent-cache "$agentconf: no-allow-external-cache"
    fi
    if [ "$MODE" = run ]; then
        [ "$(conf_value "$agentconf" pinentry-program)" = "$want" ] || die 2 "$agentconf does not name $want after the edit"
        conf_has "$agentconf" no-allow-external-cache || die 2 "$agentconf lacks no-allow-external-cache after the edit"
        "$GPGCONF" --homedir "$HOME_GNUPG" --kill gpg-agent >/dev/null 2>&1 || true
    fi
}

# ---------------------------------------------------------------------------------------------
# 2. yubikey

stage_card() {
    local list ykinfo app pginfo try
    step "2. yubikey"
    list=$(yk_list 2>/dev/null || true)
    if ! SERIAL=$(ykman_serial <<<"$list"); then
        SERIAL=
        if [ "$MODE" = check ]; then
            report yubikey missing "plug in exactly one YubiKey (ykman list: ${list:-nothing})"
            return 1
        fi
        die 2 "plug in exactly one YubiKey; ykman list printed: ${list:-nothing}"
    fi
    matches "$SERIAL" "$SERIAL_RE" || die 2 "unexpected serial '$SERIAL'"
    ykinfo=$(yk info) || die 2 "ykman info failed"
    FIRMWARE=$(ykman_field "Firmware version" <<<"$ykinfo")
    matches "$FIRMWARE" "$VERSION_RE" || die 2 "cannot read the firmware version from ykman info"
    version_ge "$FIRMWARE" "$MIN_FIRMWARE" || die 1 "firmware $FIRMWARE is older than $MIN_FIRMWARE, which ed25519 keys need"
    ok yubikey "serial $SERIAL, firmware $FIRMWARE"

    app=$(ykman_app_enabled OpenPGP <<<"$ykinfo")
    if [ "$app" != Enabled ]; then
        if todo openpgp-app "${app:-absent}; ykman config usb --enable openpgp"; then
            confirm "Enable the OpenPGP application over USB on YubiKey $SERIAL."
            yk config usb --enable openpgp -f || die 1 "ykman config usb failed"
            for try in 1 2 3 4 5; do
                sleep 2
                if ykinfo=$(yk info 2>/dev/null); then break; fi
                [ "$try" != 5 ] || die 2 "ykman cannot read the YubiKey after enabling OpenPGP; unplug it, plug it in again and run again"
            done
            [ "$(ykman_app_enabled OpenPGP <<<"$ykinfo")" = Enabled ] || die 1 "OpenPGP is still not enabled"
            info "OpenPGP application enabled"
        else
            return 1
        fi
    else
        ok openpgp-app enabled
    fi

    read_card_slot
    if [ "$MODE" = check ]; then
        pginfo=$(yk openpgp info 2>/dev/null || true)
        report kdf ok "$(ykman_field "KDF enabled" <<<"$pginfo")"
        report pin-retries ok "PIN $(ykman_field "PIN tries remaining" <<<"$pginfo"), Admin PIN $(ykman_field "Admin PIN tries remaining" <<<"$pginfo")"
        report signature-slot ok "${CARD_SIG_FPR:-empty}"
    fi
}

# Settles the case from the card and what this Mac holds, before anything touches the card:
#   fresh    all three key slots are empty
#   ready    ~/.gnupg links the key in the signature slot to this YubiKey
#   import   the key in the slot is in ~/.gnupg but not linked to this card, or its public key
#            is in ~/*.asc or the allowlist and not yet in ~/.gnupg
#   foreign  the signature slot holds a key that matches nothing here, or it is empty while
#            the encryption or authentication slot holds a key
# A key whose secret part sits in ~/.gnupg itself has existed outside the card; it is refused.
discover_key() {
    local secrets f loc keys
    PHASE=fresh
    PHASE_DETAIL=
    secrets=$(gpg_home --list-secret-keys --with-colons 2>/dev/null || true)
    if [ -z "$CARD_SIG_FPR" ] && ! card_empty; then
        PHASE=foreign
        PHASE_DETAIL="YubiKey $SERIAL has no signature key, but its encryption slot holds '${CARD_ENC_FPR:-nothing}' and its authentication slot '${CARD_AUT_FPR:-nothing}'; generating a key would replace them."
        return 0
    fi
    if [ -z "$CARD_SIG_FPR" ]; then
        for f in $(list_fprs sec <<<"$secrets"); do
            if on_card "$(key_field "$f" 15 <<<"$secrets")"; then
                warn "$HOME_GNUPG lists $f as a key on YubiKey $SERIAL, but the card's signature slot is empty: that key is gone (see 'Lost or broken YubiKey' in $DOC)"
            fi
        done
        return 0
    fi
    if list_fprs sec <<<"$secrets" | grep -qx "$CARD_SIG_FPR"; then
        FPR=$CARD_SIG_FPR
        loc=$(key_field "$FPR" 15 <<<"$secrets")
        if [ "$loc" = + ]; then
            die 1 "$HOME_GNUPG holds the secret key $FPR itself, so this key has existed outside the YubiKey; this setup only accepts a key generated on the card. Deal with it by hand (see 'Lost or broken YubiKey' and 'Key compromised' in $DOC)."
        fi
        if on_card "$loc"; then PHASE=ready; else PHASE=import; fi
        return 0
    fi
    for f in "$HOME"/*.asc "$ALLOWLIST_PATH"/*.asc; do
        [ -f "$f" ] || continue
        keys=$("$GPG" --show-keys --with-colons "$f" 2>/dev/null) || continue
        list_fprs pub <<<"$keys" | grep -qx "$CARD_SIG_FPR" || continue
        FPR=$CARD_SIG_FPR
        EMAIL=$(key_block "$FPR" <<<"$keys" | uid_email)
        matches "$EMAIL" "$EMAIL_RE" || die 1 "$f holds the YubiKey's key $FPR, but its user ID has no usable email"
        KEYFILE=$f
        PHASE=import
        return 0
    done
    PHASE=foreign
    PHASE_DETAIL="YubiKey $SERIAL holds a signature key $CARD_SIG_FPR that matches nothing in $HOME_GNUPG, $HOME/*.asc or $ALLOWLIST_PATH."
}

# all_fprs (`--show-keys --with-colons`): every primary and subkey fingerprint, one per line.
all_fprs() { awk -F: '$1 == "pub" || $1 == "sub" { want = 1; next } $1 == "fpr" && want { print $10 } { want = 0 }'; }

# has_any_fpr LIST FPR...: one of FPR... is a line of LIST.
has_any_fpr() {
    local list=$1 fpr
    shift
    for fpr in "$@"; do
        if [ -n "$fpr" ] && grep -qx "$fpr" <<<"$list"; then return 0; fi
    done
    return 1
}

# card_key_published FPR...: where a key holding one of FPR... (as its primary key or a subkey)
# is published, one place per line: an allowlist file on origin/main, which release.sh treats
# as the source of truth, and https://github.com/<login>.gpg when the GitHub login is known
# without asking. main is fetched from origin first, and the fetch must succeed: returns 2 when
# it fails or the allowlist cannot be read (a match in the local origin/main is still listed),
# and 3 when the login is known but its GitHub key list cannot be downloaded. Run mode only:
# the fetch writes refs/remotes/origin/main.
card_key_published() {
    local ref=refs/remotes/origin/main names name fprs login keys rc=0
    if ! git remote get-url origin >/dev/null 2>&1; then
        warn "this checkout has no git remote named origin"
        rc=2
    elif ! git fetch --quiet --no-tags origin "+refs/heads/main:$ref" 2>/dev/null; then
        warn "cannot fetch main from origin"
        rc=2
    fi
    if names=$(git ls-tree --name-only "$ref:$ALLOWLIST_PATH" 2>/dev/null) && [ -n "$names" ]; then
        while IFS= read -r name; do
            case "$name" in *.asc) ;; *) continue ;; esac
            fprs=$(git show "$ref:$ALLOWLIST_PATH/$name" 2>/dev/null | "$GPG" --show-keys --with-colons 2>/dev/null | all_fprs || true)
            if has_any_fpr "$fprs" "$@"; then printf 'origin/main:%s/%s\n' "$ALLOWLIST_PATH" "$name"; fi
        done <<EOF
$names
EOF
    else
        rc=2
    fi
    login=$(gh_login || git config --global --get github.user 2>/dev/null || true)
    if matches "$login" "$HANDLE_RE"; then
        if keys=$(curl -fsS --max-time 30 "https://github.com/$login.gpg" 2>/dev/null); then
            fprs=$(printf '%s\n' "$keys" | "$GPG" --show-keys --with-colons 2>/dev/null | all_fprs || true)
            if has_any_fpr "$fprs" "$@"; then printf 'https://github.com/%s.gpg\n' "$login"; fi
        else
            warn "cannot download https://github.com/$login.gpg"
            if [ "$rc" = 0 ]; then rc=3; fi
        fi
    fi
    return "$rc"
}

# run: a card holding a key that matches nothing here is either the maintainer's own key from
# another Mac, or something to wipe. A key published on main's allowlist or on GitHub is never
# wiped here, and neither is a key when main cannot be fetched from origin, its allowlist cannot
# be read, or the key list of a known GitHub login cannot be downloaded.
stage_card_state() {
    local found rc=0
    [ "$PHASE" = foreign ] || return 0
    info "$PHASE_DETAIL"
    found=$(card_key_published "$CARD_SIG_FPR" "$CARD_ENC_FPR" "$CARD_AUT_FPR") || rc=$?
    if [ -n "$found" ]; then
        die 1 "the key on YubiKey $SERIAL is published at $(printf '%s' "$found" | tr '\n' ' '), so it may be an allowlisted release key; not offering ykman openpgp reset. Put its public key in ~/<handle>.asc, for example with git show origin/main:$ALLOWLIST_PATH/<handle>.asc > ~/<handle>.asc, and run again."
    fi
    case "$rc" in
        0) ;;
        3) die 2 "could not read your GitHub key list, so the key on YubiKey $SERIAL may be a published release key; not offering ykman openpgp reset. Check the network and run again." ;;
        *) die 2 "could not read the allowlist on origin/main, so the key on YubiKey $SERIAL may be an allowlisted release key; not offering ykman openpgp reset. Check that git fetch origin main works and run again." ;;
    esac
    info "If it is your own key from another Mac, decline below, put its public key in ~/<handle>.asc (copy the file from that Mac, or curl -fsS https://github.com/<handle>.gpg -o ~/<handle>.asc when GitHub holds only that key) and run again."
    info "Otherwise the card can be wiped: every key on it is lost and the PINs return to the defaults."
    confirm "Wipe the OpenPGP application of YubiKey $SERIAL (ykman openpgp reset)."
    yk openpgp reset -f || die 1 "ykman openpgp reset failed"
    read_card_slot
    card_empty || die 1 "the card still holds a key after the reset (signature '$CARD_SIG_FPR', encryption '$CARD_ENC_FPR', authentication '$CARD_AUT_FPR')"
    PINS_DEFAULT=1
    PHASE=fresh
    info "OpenPGP application reset; the PINs are the defaults again"
}

# ---------------------------------------------------------------------------------------------
# 3. pins

stage_pins() {
    step "3. pins"
    if [ -n "$CARD_SIG_FPR" ]; then
        ok pins "the card holds a key, so its PINs were set earlier"
        return 0
    fi
    if [ "$MODE" = check ]; then
        report pins unknown "not detectable; run asks"
        return 0
    fi
    if [ "$PINS_DEFAULT" != 1 ]; then
        if ask_yn "Have the PIN and Admin PIN on YubiKey $SERIAL been changed from the defaults (123456 and 12345678)?"; then
            info "keeping the current PINs"
            return 0
        fi
    fi
    info "The optional kdf-setup and card url from $DOC are not scripted. If you want them, stop now with Ctrl-C, do them by hand while the PINs are still the defaults, and run again."
    info "ykman asks for the current PIN, then the new PIN twice (6 or more characters)."
    yk openpgp access change-pin || die 1 "the PIN was not changed"
    info "ykman asks for the current Admin PIN, then the new Admin PIN twice (8 or more characters)."
    yk openpgp access change-admin-pin || die 1 "the Admin PIN was not changed"
    PINS_DEFAULT=0
    info "PINs changed. Store the Admin PIN in your password manager: without it the key cannot be extended, and three wrong tries lock it for good."
}

# ---------------------------------------------------------------------------------------------
# 4. identity

# The login of the active gh account, when gh is installed, logged in and online.
gh_login() {
    local login
    command -v gh >/dev/null 2>&1 || return 1
    login=$(gh api user --jq .login 2>/dev/null) || return 1
    matches "$login" "$HANDLE_RE" || return 1
    printf '%s\n' "$login"
}

# Sets HANDLE: from gh when it is logged in, else from git config github.user, else by asking
# (run offers to save the answer as github.user). gh is optional throughout the script.
resolve_handle() {
    local from_gh from_git login src
    from_gh=$(gh_login || true)
    from_git=$(git config --global --get github.user 2>/dev/null || true)
    if [ -n "$from_git" ] && ! matches "$from_git" "$HANDLE_RE"; then
        die 1 "git config github.user is '$from_git', which is not a GitHub login"
    fi
    if [ -n "$from_gh" ] && [ -n "$from_git" ] && [ "$from_gh" != "$from_git" ]; then
        die 1 "gh is logged in as $from_gh but git config github.user is $from_git; fix one of them"
    fi
    if [ -n "$from_gh" ]; then
        login=$from_gh
        src=gh
    elif [ -n "$from_git" ]; then
        login=$from_git
        src="git config github.user"
    else
        if [ "$MODE" = check ]; then
            report github-login todo "gh is not logged in and git config github.user is unset; run asks"
            return 1
        fi
        info "gh is not logged in and git config github.user is unset."
        login=
        while ! matches "$login" "$HANDLE_RE"; do login=$(ask "Your GitHub login"); done
        if ask_yn "Save it as git config --global github.user so later runs find it?"; then
            git config --global github.user "$login"
        fi
        src=prompt
    fi
    if [ -n "$HANDLE" ] && [ "$HANDLE" != "$login" ]; then
        die 1 "the GitHub login is $login ($src), but the key file belongs to $HANDLE"
    fi
    HANDLE=$login
    ok github-login "$HANDLE ($src)"
}

stage_identity() {
    local verified base
    step "4. identity"
    resolve_handle || return 1
    if [ -n "$KEYFILE" ]; then
        # The file only supplies the public key; a spare's file is <handle>-spare.asc.
        base=$(basename "$KEYFILE" .asc)
        base=${base%-spare}
        if [ "$(lower "$base")" != "$(lower "$HANDLE")" ]; then
            warn "the YubiKey's public key was found in $KEYFILE, whose name does not match your GitHub login $HANDLE"
        fi
        ok identity "$HANDLE, $EMAIL (from $KEYFILE)"
        return 0
    fi
    if [ "$PHASE" = ready ] || [ "$PHASE" = import ]; then
        EMAIL=$(gpg_home --list-keys --with-colons "$FPR" 2>/dev/null | key_block "$FPR" | uid_email)
        matches "$EMAIL" "$EMAIL_RE" || die 1 "cannot read the email from the user ID of $FPR"
        ok identity "$HANDLE, $EMAIL (from the key)"
        return 0
    fi

    NAME=$(git config --global --get user.name || true)
    EMAIL=$(git config --global --get user.email || true)
    if [ "$MODE" = check ]; then
        if matches "$NAME" "$NAME_RE" && matches "$EMAIL" "$EMAIL_RE"; then
            report git-identity ok "$NAME <$EMAIL>"
        else
            report git-identity todo "git config --global user.name / user.email unset or unusable for a user ID; run asks"
        fi
        return 0
    fi
    while ! matches "$NAME" "$NAME_RE"; do NAME=$(ask "Your name for the key's user ID (5 or more characters, no < > ( ) :)"); done
    while ! matches "$EMAIL" "$EMAIL_RE"; do EMAIL=$(ask "Your email for the key's user ID (verified on GitHub)"); done
    verified=$(gh api user/emails --jq '.[] | [.email, (.verified | tostring)] | @tsv' 2>/dev/null |
        awk -F'\t' -v e="$EMAIL" '$1 == e { print $2; exit }' || true)
    case "$verified" in
        true) info "$EMAIL is verified on GitHub" ;;
        false) die 1 "$EMAIL is not verified on your GitHub account; verify it at $EMAILS_URL or use another address (git config --global user.email)" ;;
        *)
            info "Without a gh login that can list your emails, check at $EMAILS_URL that $EMAIL is verified on your GitHub account."
            ask_yn "Is $EMAIL verified on GitHub?" || die 1 "use a verified address: git config --global user.email <address>"
            ;;
    esac
    info "The key's user ID will be: $NAME <$EMAIL>"
}

# ---------------------------------------------------------------------------------------------
# 5. key

# card_ecc_ok (`gpg --card-status --with-colons`): the three key slots are set to Curve 25519,
# EdDSA (22) for the signature and authentication keys and ECDH (18) for the encryption key.
card_ecc_ok() {
    awk -F: '$1 == "keyattr" { a[$2] = $3 ":" tolower($4) }
        END { exit !(a[1] ~ /^22:.*25519$/ && a[2] ~ /^18:.*25519$/ && a[3] ~ /^22:.*25519$/) }'
}

# card_attrs (`gpg --card-status --with-colons`): the key attributes of the three slots, for
# messages, for example "rsa2048 rsa2048 rsa2048" or "ed25519 cv25519 ed25519".
card_attrs() {
    awk -F: '$1 == "keyattr" { printf "%s%s%s", sep, ($3 == 1 ? "rsa" : ""), $4; sep = " " }
        END { print "" }'
}

# key_expiry_date: FPR's expiry date as YYYY-MM-DD from ~/.gnupg, or "unknown".
key_expiry_date() {
    local exp
    exp=$(gpg_home --list-keys --with-colons "$FPR" 2>/dev/null | key_field "$FPR" 7)
    if matches "$exp" '^[0-9]+$'; then date -r "$exp" +%Y-%m-%d; else printf 'unknown\n'; fi
}

# other_keys FILE: the primary fingerprints in FILE other than FPR, space separated; nothing
# for a missing file or one that holds no OpenPGP public key, armored or binary (empty, or any
# other text).
other_keys() {
    [ -f "$1" ] || return 0
    "$GPG" --show-keys --with-colons "$1" 2>/dev/null | list_fprs pub | awk -v f="$FPR" '$0 != f' | tr '\n' ' ' | sed 's/ $//' || true
}

# drive_gpg_editor TABLE PREFIX ARG...: runs "$GPG" --no-tty --command-fd 4 --status-fd 5 ARG...
# in the background and answers each question gpg asks on its status fd (GET_LINE or GET_BOOL
# KEYWORD) with the output of "TABLE KEYWORD". gpg is stopped with SIGTERM while it waits for
# the answer when TABLE has no answer (it returns non-zero), when the question is a GET_HIDDEN,
# and when a keyword comes twice in a row (gpg rejected the answer). Closing the command fd
# would not cancel: gpg reads EOF as an empty answer, which takes the default, and the default
# of the off-card backup question is yes. Status lines go to PREFIX.status, gpg's own output to
# PREFIX.out.
# Returns 0 when gpg exits 0, 3 when it was stopped (EDITOR_KEYWORD names the question), and
# gpg's exit status otherwise. EDITOR_PID holds gpg's pid while it runs, for cleanup.
drive_gpg_editor() {
    local table=$1 prefix=$2 line kind kw prev='' answer rc=0
    shift 2
    EDITOR_KEYWORD=
    rm -f "$WORK/cmd" "$WORK/status"
    mkfifo -m 600 "$WORK/cmd" "$WORK/status" || die 1 "mkfifo failed in $WORK"
    : >"$prefix.status"
    "$GPG" --no-tty --command-fd 4 --status-fd 5 "$@" 4<"$WORK/cmd" 5>"$WORK/status" >"$prefix.out" 2>&1 &
    EDITOR_PID=$!
    # gpg's redirections open cmd, then status; opening them in the same order here keeps
    # either side from waiting on a FIFO the other has not opened yet.
    exec 7>"$WORK/cmd" 8<"$WORK/status"
    # If gpg dies between a question and its answer, the write fails instead of killing us.
    trap '' PIPE
    while IFS= read -r line <&8; do
        printf '%s\n' "$line" >>"$prefix.status"
        case "$line" in
            '[GNUPG:] GET_LINE '* | '[GNUPG:] GET_BOOL '* | '[GNUPG:] GET_HIDDEN '*) ;;
            *) continue ;;
        esac
        kind=${line#'[GNUPG:] '}
        kind=${kind%% *}
        kw=${line##* }
        if [ "$kind" = GET_HIDDEN ]; then
            EDITOR_KEYWORD="$kw (a hidden entry)"
        elif [ "$kw" = "$prev" ]; then
            EDITOR_KEYWORD="$kw (asked again, so gpg rejected the answer)"
        elif ! answer=$("$table" "$kw"); then
            EDITOR_KEYWORD=$kw
        elif ! printf '%s\n' "$answer" >&7; then
            EDITOR_KEYWORD="$kw (gpg stopped reading answers)"
        fi
        if [ -n "$EDITOR_KEYWORD" ]; then
            kill -TERM "$EDITOR_PID" 2>/dev/null || true
            break
        fi
        prev=$kw
    done
    exec 7>&- 8<&-
    # 2>/dev/null drops bash's "Terminated" job notice after a stop.
    wait "$EDITOR_PID" 2>/dev/null || rc=$?
    EDITOR_PID=
    trap - PIPE
    rm -f "$WORK/cmd" "$WORK/status"
    if [ -n "$EDITOR_KEYWORD" ]; then return 3; fi
    return "$rc"
}

# Answer tables for drive_gpg_editor: print the answer to the question KEYWORD, or return 1 for
# a question the command is not expected to ask.

# key-attr asks, for each of the three keys, the algorithm ((2) ECC) and the curve ((1) Curve
# 25519). RSA would add cardedit.genkeys.size, which is never expected.
keyattr_answer() {
    case "$1" in
        cardedit.genkeys.algo) printf '2\n' ;;
        keygen.curve) printf '1\n' ;;
        *) return 1 ;;
    esac
}

# generate asks whether to keep an off-card backup of the encryption key (no), the expiry and
# the user ID. gpg skips keygen.valid.okay and keygen.userid.cmd when it reads its answers from
# a command fd; they are answered in case a version asks them. A slot that already holds a key
# makes gpg ask cardedit.genkeys.replace_keys, which is not answered.
generate_answer() {
    case "$1" in
        cardedit.genkeys.backup_enc) printf 'n\n' ;;
        keygen.valid) printf '%s\n' "$KEY_EXPIRY" ;;
        keygen.valid.okay) printf 'y\n' ;;
        keygen.name) printf '%s\n' "$NAME" ;;
        keygen.email) printf '%s\n' "$EMAIL" ;;
        keygen.comment) printf '\n' ;;
        keygen.userid.cmd) printf 'O\n' ;;
        *) return 1 ;;
    esac
}

# card_editor COMMAND TABLE FAILURE: gpg --card-edit admin COMMAND quit, driven with TABLE.
# Dies with gpg's last lines and FAILURE when gpg fails or asks a question TABLE does not
# answer.
card_editor() {
    local cmd=$1 table=$2 failure=$3 prefix=$WORK/card-$1 rc=0
    drive_gpg_editor "$table" "$prefix" --no-options --homedir "$HOME_GNUPG" --pinentry-mode ask \
        --card-edit admin "$cmd" quit || rc=$?
    [ "$rc" != 0 ] || return 0
    info "gpg's last lines:"
    tail -n 8 "$prefix.out" >&2 || true
    if [ "$rc" = 3 ]; then
        die 1 "gpg --card-edit $cmd asked '$EDITOR_KEYWORD', which this script does not answer, so it stopped gpg at that question. $failure"
    fi
    die 1 "gpg --card-edit $cmd failed (exit $rc). $failure"
}

# card_key_problem: what is wrong with the key FPR on this YubiKey and in ~/.gnupg, or nothing
# when the signature slot holds FPR, ~/.gnupg links FPR and its two subkeys to this card, the
# key can sign and certify, and it has not expired. Reads CARD_COLONS from read_card_slot.
card_key_problem() {
    local secrets loc use exp slot sub n name
    if [ "$CARD_SIG_FPR" != "$FPR" ]; then
        printf "the card's signature slot holds %s, not %s\n" "${CARD_SIG_FPR:-nothing}" "$FPR"
        return 0
    fi
    secrets=$(gpg_home --list-secret-keys --with-colons "$FPR" 2>/dev/null | key_block "$FPR")
    if [ -z "$secrets" ]; then
        printf '%s does not list %s as a secret key\n' "$HOME_GNUPG" "$FPR"
        return 0
    fi
    loc=$(key_field "$FPR" 15 <<<"$secrets")
    if ! on_card "$loc"; then
        printf '%s does not link %s to YubiKey %s (location field "%s")\n' "$HOME_GNUPG" "$FPR" "$SERIAL" "$loc"
        return 0
    fi
    use=$(key_usage "$FPR" <<<"$secrets")
    case "$use" in
        *s*c* | *c*s*) ;;
        *)
            printf '%s does not both sign and certify (usage "%s")\n' "$FPR" "$use"
            return 0
            ;;
    esac
    exp=$(key_field "$FPR" 7 <<<"$secrets")
    if ! matches "$exp" '^[0-9]+$'; then
        printf '%s has no expiry date\n' "$FPR"
        return 0
    fi
    if [ "$exp" -le "$(date +%s)" ]; then
        printf '%s expired on %s; see Extend the expiry in %s\n' "$FPR" "$(date -r "$exp" +%Y-%m-%d)" "$DOC"
        return 0
    fi
    n=$(list_fprs ssb <<<"$secrets" | count_lines)
    if [ "$n" != 2 ]; then
        printf '%s has %s subkeys, not the encryption and authentication subkeys generate makes\n' "$FPR" "$n"
        return 0
    fi
    for slot in 3 4; do
        if [ "$slot" = 3 ]; then name=encryption; else name=authentication; fi
        sub=$(card_field fpr "$slot" <<<"$CARD_COLONS")
        if ! list_fprs ssb <<<"$secrets" | grep -qx "$sub"; then
            printf "the card's %s slot holds '%s', which is not a subkey of %s\n" "$name" "$sub" "$FPR"
            return 0
        fi
        if ! on_card "$(key_field "$sub" 15 <<<"$secrets")"; then
            printf '%s does not link the subkey %s to YubiKey %s\n' "$HOME_GNUPG" "$sub" "$SERIAL"
            return 0
        fi
    done
}

# Keys this script would have to refuse once the new key exists, checked before generation:
# ~/<handle>.asc or the allowlist file already holding a key.
assert_first_key() {
    local f keys
    for f in "$HOME/$HANDLE.asc" "$(find_allowlist_file)"; do
        keys=$(other_keys "$f")
        [ -z "$keys" ] || die 1 "$f already holds the key $keys. This script sets up the first key only; for a second YubiKey see Spare YubiKey in $DOC, and to replace a lost key see Lost or broken YubiKey."
    done
}

generate_key() {
    local tries secrets exp now uid problem
    local partial="The card may now hold a partial key: run again, and if the card holds a key that matches nothing here the script offers ykman openpgp reset."
    if ! matches "$HANDLE" "$HANDLE_RE" || ! matches "$NAME" "$NAME_RE" || ! matches "$EMAIL" "$EMAIL_RE"; then
        die 1 "the GitHub login, name and email must be known before the key is generated"
    fi
    assert_first_key
    assert_this_card
    read_card_slot
    card_empty || die 1 "the card holds a key (signature '$CARD_SIG_FPR', encryption '$CARD_ENC_FPR', authentication '$CARD_AUT_FPR'); refusing to generate over it"
    tries=$(card_field pinretry 4 <<<"$CARD_COLONS")
    case "$tries" in
        0) die 1 "the Admin PIN of YubiKey $SERIAL is blocked, so no key can be generated on it. Only ykman openpgp reset clears it (it wipes the OpenPGP application and restores the default PINs); then run again." ;;
        1 | 2) warn "the Admin PIN of YubiKey $SERIAL has $tries tries left. A blocked Admin PIN cannot be unblocked, so type it carefully." ;;
    esac
    info "The key is generated inside YubiKey $SERIAL and never leaves it: a signing primary key (ed25519) in the signature slot, with encryption (cv25519) and authentication (ed25519) subkeys in the other two slots, all valid $KEY_EXPIRY ('Create the key' in $DOC)."
    info "pinentry-mac asks for the Admin PIN, for the key attributes and the generation, then for the PIN, with which the card signs the user ID and the revocation certificate. There is no passphrase, and generation takes under a minute."
    confirm "Generate the release signing key on YubiKey $SERIAL for \"$NAME <$EMAIL>\", valid $KEY_EXPIRY. The user ID is permanent and the key never leaves the card."
    # Started here so that gpg does not start it from the card editor, with the FIFOs open.
    "$GPGCONF" --homedir "$HOME_GNUPG" --launch gpg-agent >/dev/null 2>&1 || die 1 "gpg-agent does not start"

    if card_ecc_ok <<<"$CARD_COLONS"; then
        info "the key attributes are already ECC, Curve 25519"
    else
        info "Setting the key attributes from $(card_attrs <<<"$CARD_COLONS") to ECC, Curve 25519: pinentry-mac asks for the Admin PIN."
        card_editor key-attr keyattr_answer "Nothing was generated; run again."
        read_card_slot
        card_ecc_ok <<<"$CARD_COLONS" ||
            die 1 "the key attributes read back as $(card_attrs <<<"$CARD_COLONS") after key-attr, not ECC, Curve 25519; nothing was generated. See the key-attr row of Troubleshooting in $DOC"
        info "key attributes set to $(card_attrs <<<"$CARD_COLONS")"
    fi
    info "Generating the key: pinentry-mac asks for the Admin PIN unless the card still holds it, then for the PIN."
    card_editor generate generate_answer "$partial"

    kill_scdaemons
    read_card_slot
    [ -n "$CARD_SIG_FPR" ] || die 1 "gpg finished, but the card's signature slot is empty. $partial"
    FPR=$CARD_SIG_FPR
    problem=$(card_key_problem)
    [ -z "$problem" ] || die 1 "$problem. $partial"
    secrets=$(gpg_home --list-secret-keys --with-colons "$FPR" 2>/dev/null | key_block "$FPR")
    [ "$(key_field "$FPR" 2 <<<"$secrets")" = u ] ||
        die 1 "$HOME_GNUPG does not trust $FPR ultimately (validity '$(key_field "$FPR" 2 <<<"$secrets")'), as it does a key gpg generated"
    exp=$(key_field "$FPR" 7 <<<"$secrets")
    now=$(date +%s)
    if [ "$exp" -lt $((now + EXPIRY_MIN_DAYS * 86400)) ] || [ "$exp" -gt $((now + EXPIRY_MAX_DAYS * 86400)) ]; then
        die 1 "$FPR expires on $(date -r "$exp" +%Y-%m-%d), not about $KEY_EXPIRY from now"
    fi
    uid=$(uid_text <<<"$secrets")
    [ "$uid" = "$NAME <$EMAIL>" ] || die 1 "the key's user ID reads back as '$uid', not '$NAME <$EMAIL>'"
    PHASE=ready
    GENERATED=1
    info "key $FPR generated on YubiKey $SERIAL"
}

# The key on the card is known (from ~/.gnupg, ~/*.asc or the allowlist) but ~/.gnupg does not
# link it to this YubiKey yet.
stage_import() {
    local secrets
    if [ -n "$KEYFILE" ]; then
        if grep -q PRIVATE "$KEYFILE"; then die 1 "$KEYFILE holds a private key block; refusing to import it"; fi
        gpg_home --import "$KEYFILE" 2>/dev/null || die 1 "importing $KEYFILE into $HOME_GNUPG failed"
        info "imported the public key of $FPR from $KEYFILE"
    fi
    kill_scdaemons
    # Reading the card makes gpg-agent record that the card holds the secret keys.
    gpg_home --card-status >/dev/null 2>&1 || die 1 "gpg --card-status failed after the import"
    printf '%s:6:\n' "$FPR" | gpg_home --import-ownertrust 2>/dev/null || die 1 "gpg --import-ownertrust failed"
    secrets=$(gpg_home --list-secret-keys --with-colons "$FPR" 2>/dev/null | key_block "$FPR")
    on_card "$(key_field "$FPR" 15 <<<"$secrets")" ||
        die 1 "$HOME_GNUPG still does not link $FPR to YubiKey $SERIAL after gpg --card-status"
    [ "$(key_field "$FPR" 2 <<<"$secrets")" = u ] || die 1 "$FPR is not ultimately trusted after gpg --import-ownertrust"
    PHASE=ready
    info "$HOME_GNUPG links $FPR to YubiKey $SERIAL"
}

check_revocation_cert() {
    local rev=$HOME_GNUPG/openpgp-revocs.d/$FPR.rev
    if [ -f "$rev" ]; then
        ok revocation-cert "$rev"
        [ "$MODE" = run ] || return 0
        if [ "$GENERATED" = 1 ]; then
            info "gpg wrote the key's revocation certificate to $rev ('Store the revocation certificate' in $DOC)."
            pause "Store it in your password manager, or print it and keep the paper apart from the YubiKey, now: it is the only way to revoke the key once the YubiKey is lost."
        else
            info "revocation certificate: $rev; keep it stored apart from the YubiKey"
        fi
    elif todo revocation-cert "$rev missing; store the one from the Mac that generated the key"; then
        warn "there is no revocation certificate at $rev. If the key was generated on another Mac, it is there: make sure it is stored. Otherwise make one at that path with the YubiKey plugged in, mkdir -m 700 -p $HOME_GNUPG/openpgp-revocs.d && gpg --output $rev --gen-revoke $FPR, and store it ('Store the revocation certificate' in $DOC)."
    fi
}

set_touch_policy() {
    local pol
    pol=$(yk openpgp keys info sig 2>/dev/null | touch_policy || true)
    case "$pol" in
        On | "On (fixed)")
            ok touch-policy "$pol"
            return 0
            ;;
        "Cached (fixed)")
            warn "the signature touch policy is '$pol' and cannot be changed without resetting the OpenPGP application; it still needs a touch"
            ok touch-policy "$pol"
            return 0
            ;;
    esac
    if todo touch-policy "${pol:-unknown}; ykman openpgp keys set-touch sig on"; then
        info "Setting the signature touch policy to on ('Require a touch to sign' in $DOC). ykman asks for the Admin PIN."
        yk openpgp keys set-touch sig on -f || die 1 "ykman openpgp keys set-touch failed"
        pol=$(yk openpgp keys info sig 2>/dev/null | touch_policy || true)
        [ "$pol" = On ] || die 1 "the touch policy reads '$pol' after set-touch"
        info "every signature now needs a touch"
    fi
}

stage_key() {
    local problem
    step "5. key"
    case "$PHASE" in
        fresh)
            if [ "$MODE" = check ]; then
                report key todo "none yet; run generates one on the YubiKey"
                if card_ecc_ok <<<"$CARD_COLONS"; then
                    report key-attr ok "ECC, Curve 25519"
                else
                    report key-attr todo "$(card_attrs <<<"$CARD_COLONS"); run sets ECC, Curve 25519"
                fi
                report revocation-cert todo "after the key exists"
                report touch-policy todo "after the key exists"
                return 0
            fi
            generate_key
            ;;
        import)
            if [ "$MODE" = check ]; then
                report key todo "$FPR is on YubiKey $SERIAL; run ${KEYFILE:+imports its public key from $KEYFILE and }links it in $HOME_GNUPG"
                report revocation-cert todo "after the import"
                set_touch_policy
                return 0
            fi
            stage_import
            ;;
        ready) ;;
        *)
            [ "$MODE" = check ] || die 1 "unexpected phase '$PHASE'"
            report key todo "$PHASE_DETAIL run explains the choices"
            return 0
            ;;
    esac
    problem=$(card_key_problem)
    if [ -n "$problem" ]; then
        [ "$MODE" = check ] || die 1 "$problem"
        report key todo "$problem"
    else
        ok key "$FPR on YubiKey $SERIAL, expires $(key_expiry_date)"
        if [ "$MODE" = run ] && [ "$GENERATED" != 1 ]; then info "key $FPR is on YubiKey $SERIAL"; fi
    fi
    check_revocation_cert
    set_touch_policy
}

# ---------------------------------------------------------------------------------------------
# 6. export

# public_file_ok FILE: an armored public key holding FPR and no other key.
public_file_ok() {
    local keys
    [ -s "$1" ] || return 1
    [ "$(head -n 1 "$1")" = '-----BEGIN PGP PUBLIC KEY BLOCK-----' ] || return 1
    if grep -q PRIVATE "$1"; then return 1; fi
    keys=$("$GPG" --show-keys --with-colons "$1" 2>/dev/null) || return 1
    [ "$(list_fprs pub <<<"$keys")" = "$FPR" ]
}

stage_export() {
    local pub=$WORK/pub.asc keys
    step "6. export"
    PUBFILE=$HOME/$HANDLE.asc
    if public_file_ok "$PUBFILE"; then
        ok public-key "$PUBFILE holds $FPR"
        return 0
    fi
    keys=$(other_keys "$PUBFILE")
    if [ -n "$keys" ]; then
        if [ "$MODE" = check ]; then
            report public-key todo "$PUBFILE holds another key ($keys); this script sets up the first key only"
            return 0
        fi
        die 1 "$PUBFILE holds another key ($keys): this script sets up the first key only; see Spare YubiKey in $DOC"
    fi
    if todo public-key "$PUBFILE missing or not an armored export of $FPR; run exports it"; then
        gpg_home --armor --export --export-options export-minimal "$FPR" >"$pub" || die 1 "gpg --export of $FPR failed"
        public_file_ok "$pub" || die 1 "the export of $FPR is not one armored public key"
        cp "$pub" "$PUBFILE"
        chmod 644 "$PUBFILE"
        public_file_ok "$PUBFILE" || die 1 "$PUBFILE does not read back as the public key of $FPR"
        info "public key written to $PUBFILE ('Export the public key' in $DOC)"
    fi
}

# ---------------------------------------------------------------------------------------------
# 7. allowlist

# The allowlist file for HANDLE: an existing file whose name matches it in any case, else
# <handle>.asc.
find_allowlist_file() {
    local want f
    want=$(lower "$HANDLE.asc")
    for f in "$ALLOWLIST_PATH"/*.asc; do
        [ -f "$f" ] || continue
        if [ "$(lower "$(basename "$f")")" = "$want" ]; then
            printf '%s\n' "$f"
            return 0
        fi
    done
    printf '%s\n' "$ALLOWLIST_PATH/$HANDLE.asc"
}

require_clean() {
    local f
    for f in "$@"; do
        [ -z "$(git status --porcelain -- "$f")" ] || die 1 "$f has uncommitted changes; commit or stash them first"
    done
}

stage_allowlist() {
    local today target readme base existing row key branch file_todo=0 sec_todo=0 readme_todo=0 to_clean='' rc
    step "7. allowlist"
    today=$(date +%Y-%m-%d)
    target=$(find_allowlist_file)
    base=$(basename "$target")
    readme=$ALLOWLIST_PATH/README.md
    if [ -f "$target" ] && grep -q 'BEGIN PGP PUBLIC KEY BLOCK' "$target"; then
        existing=$("$GPG" --show-keys --with-colons "$target" 2>/dev/null | list_fprs pub || true)
        if grep -qx "$FPR" <<<"$existing"; then
            ok allowlist "$target holds $FPR"
        elif [ "$MODE" = check ]; then
            report allowlist todo "$target holds another key ($(printf '%s' "$existing" | tr '\n' ' ')); replacing it is a manual change"
            return 0
        else
            die 1 "$target holds another key ($(printf '%s' "$existing" | tr '\n' ' ')); replacing a maintainer's key is a manual change, see $readme and Lost or broken YubiKey in $DOC"
        fi
    else
        file_todo=1
    fi
    if grep -q "^| @$HANDLE |.*$FPR" "$SECURITY_FILE"; then ok security-row "$SECURITY_FILE row carries $FPR"; else sec_todo=1; fi
    if grep -q "^| \`$base\` |.*Active since" "$readme"; then ok readme-row "$readme row is active"; else readme_todo=1; fi
    if [ "$MODE" = check ]; then
        [ "$file_todo" = 0 ] || report allowlist todo "$target is missing or a placeholder"
        [ "$sec_todo" = 0 ] || report security-row todo "$SECURITY_FILE row for @$HANDLE"
        [ "$readme_todo" = 0 ] || report readme-row todo "$readme row for $base"
        return 0
    fi
    if [ "$file_todo$sec_todo$readme_todo" = 000 ]; then
        info "the allowlist already carries the key"
        return 0
    fi
    [ "$file_todo" = 0 ] || to_clean="$to_clean $target"
    [ "$sec_todo" = 0 ] || to_clean="$to_clean $SECURITY_FILE"
    [ "$readme_todo" = 0 ] || to_clean="$to_clean $readme"
    # shellcheck disable=SC2086
    require_clean $to_clean
    branch=$(git rev-parse --abbrev-ref HEAD)
    if [ "$branch" = main ] || [ "$branch" = HEAD ]; then
        warn "you are on '$branch'; create a branch before committing (git switch -c add-$HANDLE-key)"
    fi

    if [ "$file_todo" = 1 ]; then
        cp "$PUBFILE" "$target"
        chmod 644 "$target"
        public_file_ok "$target" || die 1 "$target does not read back as the public key of $FPR"
        STAGED="$STAGED $target"
        info "wrote $target"
    fi
    if [ "$sec_todo" = 1 ]; then
        row="| @$HANDLE | $FPR | $SERIAL | $today | active |"
        rc=0
        replace_table_row "$SECURITY_FILE" "@$HANDLE" "$row" || rc=$?
        case "$rc" in
            0)
                grep -qF "$row" "$SECURITY_FILE" || die 1 "$SECURITY_FILE does not contain the new row"
                STAGED="$STAGED $SECURITY_FILE"
                info "filled in your row in $SECURITY_FILE"
                ;;
            1) printf 'Add this row to the maintainer release keys table in %s:\n%s\n' "$SECURITY_FILE" "$row" ;;
            *) die 1 "$SECURITY_FILE has several rows for @$HANDLE" ;;
        esac
    fi
    if [ "$readme_todo" = 1 ]; then
        key="\`$base\`"
        row="| $key | @$HANDLE | Active since $today. |"
        rc=0
        replace_table_row "$readme" "$key" "$row" || rc=$?
        case "$rc" in
            0)
                grep -qF "$row" "$readme" || die 1 "$readme does not contain the new row"
                STAGED="$STAGED $readme"
                info "filled in your row in $readme"
                ;;
            1) printf 'Add this row to the Files table in %s:\n%s\n' "$readme" "$row" ;;
            *) die 1 "$readme has several rows for $key" ;;
        esac
    fi
    if [ -n "$STAGED" ]; then
        # shellcheck disable=SC2086
        git add -- $STAGED
        git diff --cached --stat
    fi
    printf 'Key fingerprint for the reviewer to confirm with you over a call: %s\n' "$FPR"
    printf 'The script never commits or pushes. When ready:\n'
    printf '  git commit -m "chore(release): add @%s'"'"'s release signing key"\n' "$HANDLE"
    printf '  git push -u origin %s\n' "$branch"
    printf '  gh pr create --title "chore(release): add @%s'"'"'s release signing key"\n' "$HANDLE"
}

# ---------------------------------------------------------------------------------------------
# 8. git config

set_global() {
    local key=$1 want=$2 cur
    cur=$(git config --global --get "$key" || true)
    if [ -z "$cur" ]; then
        if todo "git-$key" "unset; want $want"; then
            git config --global "$key" "$want"
            info "set $key"
        fi
    elif [ "$cur" = "$want" ] || { [ "$key" = gpg.program ] && [ "$(resolve_path "$cur")" = "$(resolve_path "$want")" ]; }; then
        ok "git-$key" "$cur"
    else
        if todo "git-$key" "is '$cur', want '$want'"; then
            confirm "Replace the global git $key '$cur' with '$want'."
            git config --global "$key" "$want"
            info "set $key"
        fi
    fi
    if [ "$MODE" = run ]; then
        cur=$(git config --global --get "$key" || true)
        [ "$cur" = "$want" ] || { [ "$key" = gpg.program ] && [ "$(resolve_path "$cur")" = "$(resolve_path "$want")" ]; } ||
            die 1 "git $key reads back as '$cur'"
    fi
}

stage_gitconfig() {
    local k v cur
    step "8. git config"
    for k in user.signingkey gpg.program gpg.openpgp.program gpg.format; do
        if v=$(git config --local --get "$k" 2>/dev/null); then
            if todo "git-local-$k" "$v; remove it: git config --unset $k"; then
                die 1 "this checkout sets $k=$v locally, which would override the global setting; remove it with 'git config --unset $k'"
            fi
        fi
    done
    set_global user.signingkey "${FPR}!"
    set_global gpg.program "$GPG"
    cur=$(git config --global --get gpg.format || true)
    case "$cur" in
        '' | openpgp) ok git-gpg.format "${cur:-unset (openpgp)}" ;;
        *)
            if todo git-gpg.format "is '$cur', want openpgp"; then
                confirm "Replace the global git gpg.format '$cur' with 'openpgp'."
                git config --global gpg.format openpgp
            fi
            ;;
    esac
    cur=$(git config --global --get user.email || true)
    if [ "$cur" != "$EMAIL" ]; then
        warn "git user.email is '${cur:-unset}' but the key's user ID is $EMAIL; GitHub shows tags as Unverified unless they match (git config --global user.email $EMAIL)"
    fi
}

# ---------------------------------------------------------------------------------------------
# 9. github

# The keys GitHub serves for the account include FPR as a primary key. Needs no login.
github_lists_key() {
    local keys
    keys=$(curl -fsS --max-time 30 "https://github.com/$HANDLE.gpg" 2>/dev/null | "$GPG" --show-keys --with-colons 2>/dev/null) || return 1
    list_fprs pub <<<"$keys" | grep -qx "$FPR"
}

# "true" or "false" for EMAIL on the account's GPG keys from the public API, which prints one
# field per line; nothing when the API or the email is not reachable. Needs no login.
github_email_verified() {
    curl -fsS --max-time 30 -H 'Accept: application/vnd.github+json' "https://api.github.com/users/$HANDLE/gpg_keys" 2>/dev/null |
        awk -v e="$EMAIL" '
            BEGIN { needle = "\"email\": \"" e "\"" }
            index($0, needle) { want = 1; next }
            want && /"verified":/ { print ($0 ~ /true/) ? "true" : "false"; exit }'
}

stage_github() {
    local status scopes verified login try
    step "9. github"
    status=
    if command -v gh >/dev/null 2>&1; then
        status=$(gh auth status --active --hostname github.com 2>&1) || status=
    fi
    if [ -n "$status" ]; then TOKEN_TYPE=$(gh_token_type <<<"$status"); else TOKEN_TYPE=none; fi
    if [ "$MODE" = check ]; then
        case "$TOKEN_TYPE" in
            none) report gh-login ok "none (optional); the key is added on GitHub by hand" ;;
            oauth) report gh-login ok "oauth; scopes: $(gh_scopes <<<"$status")" ;;
            *) report gh-login ok "$TOKEN_TYPE; the key is added on GitHub by hand (a token cannot add GPG keys through gh)" ;;
        esac
        if [ -z "$FPR" ] || [ -z "$HANDLE" ]; then
            report github-key todo "after the key exists"
        elif github_lists_key; then
            verified=$(github_email_verified)
            report github-key ok "$FPR on github.com/$HANDLE.gpg; $EMAIL verified: ${verified:-unknown}"
        else
            report github-key todo "$FPR not on github.com/$HANDLE.gpg"
        fi
        return 0
    fi
    if github_lists_key; then
        info "https://github.com/$HANDLE.gpg already lists $FPR"
    elif [ "$TOKEN_TYPE" = oauth ]; then
        login=$(gh_login) || die 2 "gh cannot read your GitHub login; is the Mac online?"
        [ "$login" = "$HANDLE" ] || die 1 "gh is logged in as $login, not $HANDLE"
        scopes=$(gh_scopes <<<"$status")
        case " $scopes " in *" write:packages "*) warn "this gh login has write:packages; keep that scope off every gh login (see 'Registry access on the build server' in $DOC)" ;; esac
        case " $scopes " in
            *" write:gpg_key "*) ;;
            *)
                info "gh opens a browser to add the write:gpg_key scope."
                gh auth refresh -h github.com -s write:gpg_key || die 1 "gh auth refresh failed"
                ;;
        esac
        gh gpg-key add "$PUBFILE" || die 1 "gh gpg-key add failed"
        for try in 1 2 3 4 5; do
            if github_lists_key; then break; fi
            [ "$try" != 5 ] || die 1 "https://github.com/$HANDLE.gpg does not list $FPR after gh gpg-key add; if GitHub holds an older export of the key, delete it there (gh gpg-key list, gh gpg-key delete <key-id>) and run again"
            sleep 3
        done
    else
        if [ "$TOKEN_TYPE" = none ]; then
            info "gh is not logged in, so add the key on GitHub by hand:"
        else
            info "Your gh login is a personal access token, which cannot add GPG keys, so add the key on GitHub by hand:"
        fi
        info "  open $GPG_KEYS_URL and paste the contents of $PUBFILE (pbcopy < $PUBFILE copies it)"
        pause "Add the key on GitHub first."
        while ! github_lists_key; do
            ask_yn "https://github.com/$HANDLE.gpg does not list $FPR yet. Check again?" ||
                die 1 "add $PUBFILE at $GPG_KEYS_URL, then run again"
        done
    fi
    info "https://github.com/$HANDLE.gpg lists $FPR"
    verified=$(github_email_verified)
    case "$verified" in
        true) info "GitHub lists the key with $EMAIL verified" ;;
        false) die 1 "GitHub does not show $EMAIL as verified for this key; verify it at $EMAILS_URL, or the tags show as Unverified" ;;
        *) warn "could not read the email status from api.github.com; check at $EMAILS_URL that $EMAIL is verified, or the tags show as Unverified" ;;
    esac
}

# ---------------------------------------------------------------------------------------------
# 10. test

github_sig_test() {
    local repo=$1 passed=1 verified reason
    if gh repo view "$HANDLE/gpg-sigtest" >/dev/null 2>&1; then die 1 "$HANDLE/gpg-sigtest exists; delete it first or skip this test"; fi
    (cd "$repo" && gh repo create "$HANDLE/gpg-sigtest" --private --source=. --push) || die 1 "gh repo create failed"
    if git -C "$repo" push -q origin sigtest; then
        verified=$(gh api "repos/$HANDLE/gpg-sigtest/git/tags/$(git -C "$repo" rev-parse sigtest)" --jq .verification.verified 2>/dev/null) || verified=error
        if [ "$verified" = true ]; then
            info "GitHub verifies the test tag"
            passed=0
        else
            reason=$(gh api "repos/$HANDLE/gpg-sigtest/git/tags/$(git -C "$repo" rev-parse sigtest)" --jq .verification.reason 2>/dev/null || true)
            warn "GitHub does not verify the test tag: ${reason:-$verified}"
        fi
    else
        warn "pushing the test tag failed"
    fi
    info "Deleting the scratch repository; gh opens a browser for the delete_repo scope."
    if gh auth refresh -h github.com -s delete_repo && gh repo delete "$HANDLE/gpg-sigtest" --yes; then
        info "deleted $HANDLE/gpg-sigtest"
    else
        warn "delete $HANDLE/gpg-sigtest by hand: gh repo delete $HANDLE/gpg-sigtest --yes"
    fi
    gh auth refresh -h github.com --remove-scopes delete_repo ||
        warn "remove the delete_repo scope by hand: gh auth refresh -h github.com --remove-scopes delete_repo"
    [ "$passed" = 0 ] || die 1 "GitHub did not verify the test tag; see the last row of Troubleshooting in $DOC"
}

stage_test() {
    local repo=$WORK/sigtest raw signer name email
    step "10. test"
    if [ "$MODE" = check ]; then
        report test todo "run signs a test tag and a file"
        return 0
    fi
    # The test tag carries the identity make release-tag uses in this checkout: git's own
    # user.name and user.email here, not the key's user ID.
    name=$(git config --get user.name || true)
    email=$(git config --get user.email || true)
    if [ -z "$name" ] || [ -z "$email" ]; then
        die 2 "git config user.name and user.email must be set for the test tag, as make release-tag uses them (git config --global user.email $EMAIL)"
    fi
    mkdir "$repo"
    git -C "$repo" init -q
    git -C "$repo" -c user.name="$name" -c user.email="$email" commit -q --allow-empty -m init
    info "Signing a test tag as $name <$email>: pinentry-mac asks for the YubiKey's PIN unless the card holds it, then the YubiKey blinks until you touch it ('PIN and touch during signing' in $DOC)."
    git -C "$repo" -c user.name="$name" -c user.email="$email" tag -s sigtest -m sigtest || die 1 "git tag -s failed; see Troubleshooting in $DOC"
    raw=$(git -C "$repo" verify-tag --raw sigtest 2>&1) || die 1 "git verify-tag failed: $raw"
    signer=$(awk '$2 == "VALIDSIG" { print $3; exit }' <<<"$raw")
    [ "$signer" = "$FPR" ] || die 1 "the test tag was signed by '$signer', not $FPR"
    info "test tag signed by $FPR and verified"
    printf 'test\n' >"$repo/t"
    info "Making a detached signature like make release-sign: touch the YubiKey again."
    gpg_home --armor --detach-sign --local-user "${FPR}!" --output "$repo/t.asc" "$repo/t" || die 1 "detached signature failed"
    gpg_home --verify "$repo/t.asc" "$repo/t" 2>/dev/null || die 1 "the detached signature does not verify"
    info "detached signature verified"
    if [ "$TOKEN_TYPE" = oauth ]; then
        if ask_yn "Also push the test tag to a private scratch repository $HANDLE/gpg-sigtest and ask GitHub whether it verifies? It is created and deleted again, with a browser login for the delete_repo scope."; then
            github_sig_test "$repo"
        fi
    else
        info "Without an OAuth gh login the GitHub check is manual. From a throwaway repository with a signed tag, as in 'Test signing' in $DOC (or push the tag to a private repository of yours and look for its Verified badge):"
        info "  gh repo create $HANDLE/gpg-sigtest --private --source=. --push"
        info "  git push origin sigtest"
        info "  gh api repos/$HANDLE/gpg-sigtest/git/tags/\$(git rev-parse sigtest) --jq .verification.verified"
        info "  gh repo delete $HANDLE/gpg-sigtest --yes"
    fi
}

# ---------------------------------------------------------------------------------------------
# Summary

summary() {
    local expiry rev=$HOME_GNUPG/openpgp-revocs.d/$FPR.rev
    expiry=$(key_expiry_date)
    printf '\nYubiKey signing setup complete\n'
    printf '  YubiKey          serial %s, firmware %s\n' "$SERIAL" "$FIRMWARE"
    printf '  signing key      %s (generated on the YubiKey, never left it; expires %s)\n' "$FPR" "$expiry"
    printf '  public key       %s\n' "$PUBFILE"
    if [ -n "$STAGED" ]; then printf '  staged           %s\n' "${STAGED# }"; fi
    printf 'Reminders (sections of %s):\n' "$DOC"
    printf '  - extend the key before %s with the YubiKey plugged in (Extend the expiry):\n' "$expiry"
    printf '      gpg --quick-set-expire %s %s\n' "$FPR" "$KEY_EXPIRY"
    printf "      gpg --quick-set-expire %s %s '*'\n" "$FPR" "$KEY_EXPIRY"
    printf '    then export it again, update %s in a PR and replace the key on GitHub\n' "$(find_allowlist_file)"
    if [ -f "$rev" ]; then
        printf '  - keep the revocation certificate %s in your password manager or on paper, apart from the YubiKey (Store the revocation certificate)\n' "$rev"
    else
        printf '  - keep the revocation certificate from the Mac that generated the key in your password manager or on paper (Store the revocation certificate)\n'
    fi
    printf '  - a lost or broken YubiKey, or a blocked Admin PIN, means a new key, a new allowlist PR and a new GitHub key (Lost or broken YubiKey); a spare YubiKey with a key of its own limits that (Spare YubiKey)\n'
    printf '  - on the build server, set up registry access (Registry access on the build server)\n'
}

# ---------------------------------------------------------------------------------------------
# Setup and dispatch

usage() {
    local text='usage: etc/yubikey-setup.sh check | run | help

  check   report what is already set up, without changing anything
  run     set up the release signing key on a YubiKey, asking only for what is missing
  help    print this text

Runbook: docs/src/maintainers/yubikey-setup.md. See the header of this file for the flow,
the exit codes and the security rules.'
    if [ "${1:-2}" = 0 ]; then printf '%s\n' "$text"; else printf '%s\n' "$text" >&2; fi
    exit "${1:-2}"
}

cleanup() {
    if [ -n "$EDITOR_PID" ]; then
        kill -TERM "$EDITOR_PID" 2>/dev/null || true
        info "stopped gpg's card editor; check the YubiKey with gpg --card-status before running again"
    fi
    exec 7>&- 8<&-
    if [ -n "$WORK" ]; then rm -rf "$WORK"; fi
}

setup() {
    local top
    need git awk
    top=$(git rev-parse --show-toplevel 2>/dev/null) || die 2 "not inside a git checkout"
    cd "$top"
    WORK=$(mktemp -d "${TMPDIR:-/tmp}/tn-yubikey.XXXXXX") || die 2 "mktemp failed"
    trap cleanup EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
}

cmd_check() {
    stage_tools || return 0
    stage_card || return 0
    discover_key
    stage_pins
    stage_identity || true
    stage_key
    if [ -n "$FPR" ] && [ -n "$HANDLE" ]; then
        stage_export
        stage_allowlist
    else
        report public-key todo "after the key exists"
        report allowlist todo "after the key exists"
    fi
    if [ -n "$FPR" ]; then stage_gitconfig; else report git-config todo "after the key exists"; fi
    stage_github
    stage_test
}

cmd_run() {
    stage_tools
    stage_card
    discover_key
    stage_card_state
    stage_pins
    stage_identity
    stage_key
    [ "$PHASE" = ready ] || die 1 "unexpected phase '$PHASE' after the key stage"
    stage_export
    stage_allowlist
    stage_gitconfig
    stage_github
    stage_test
    summary
}

main() {
    [ $# -eq 1 ] || usage
    case "$1" in
        check | run) MODE=$1 ;;
        help | -h | --help) usage 0 ;;
        *) usage ;;
    esac
    setup
    preflight
    if [ "$MODE" = check ]; then cmd_check; else cmd_run; fi
}

# Sourcing the file (tests) defines the functions without running anything.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    main "$@"
fi
