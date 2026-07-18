#!/bin/bash
#
# stake-validator.sh <n>  —  stake -> activate -> seat a non-genesis validator via cast.
#
# <n> selects the stakeable node created by local-testnet.sh:
#   5 -> observer1 (RPC :8541)   6 -> observer2 (RPC :8540)
#
# The SAME script onboards node 5 (pre-fork bytecode) and node 6 (post-fork bytecode): mint /
# stake / activate / getCurrentStakeConfig / setNextCommitteeSize are byte-identical across the
# ConsensusRegistry fork, so reusing it is itself a check that staking behaves the same on both.
#
# Only fork-AGNOSTIC reads are used for the committee-grow step: tn_getValidators() reverts
# pre-fork (its getValidatorsInfo backing selector does not exist in the pre-fork bytecode), so we
# count Active validators via tn_getValidator(address), which is identical across the fork.
#
# Requires: Foundry `cast`, the built `telcoin-network` binary, and $LOCAL_PK in the environment
# (the governance / dev-funds private key; `source .env` provides it).
#
# Usage: ./etc/fork-test/stake-validator.sh 5

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

N="${1:-}"
if [ "$N" != "5" ] && [ "$N" != "6" ]; then
    echo "usage: $0 <5|6>   (5 = observer1 / pre-fork, 6 = observer2 / post-fork)"
    exit 1
fi

REG=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1
CHAIN_ID=2017
NAME="observer$((N - 4))"          # 5 -> observer1, 6 -> observer2
RPC_PORT=$((8546 - N))             # 5 -> 8541,      6 -> 8540
RPC="http://localhost:${RPC_PORT}"
DATADIR="${ROOT_DIR}/local-validators/${NAME}"
EOA_FILE="${ROOT_DIR}/local-validators/${NAME}.eoa"
BIN="${ROOT_DIR}/target/release/telcoin-network"
export TN_BLS_PASSPHRASE="${TN_BLS_PASSPHRASE:-local}"

# Genesis committee validator addresses (from local-testnet.sh) + both operator EOAs make up the
# full known validator set, used to count Active validators for the committee-grow step.
GENESIS_ADDRS=(
    0x1111111111111111111111111111111111111111
    0x2222222222222222222222222222222222222222
    0x3333333333333333333333333333333333333333
    0x4444444444444444444444444444444444444444
)

# ---- preflight ------------------------------------------------------------------------------
[ -n "${LOCAL_PK:-}" ] || { echo "LOCAL_PK is not set (run: source .env)"; exit 1; }
[ -x "$BIN" ]          || { echo "telcoin-network binary not found at ${BIN} (build it first)"; exit 1; }
[ -f "$EOA_FILE" ]     || { echo "operator EOA file ${EOA_FILE} not found (run local-testnet.sh first)"; exit 1; }
command -v cast >/dev/null 2>&1 || { echo "Foundry 'cast' is required"; exit 1; }

# EOA_ADDR / EOA_PK for this node.
# shellcheck disable=SC1090
source "$EOA_FILE"
[ -n "${EOA_ADDR:-}" ] && [ -n "${EOA_PK:-}" ] || { echo "malformed ${EOA_FILE}"; exit 1; }

# operator EOAs (for the Active count) — read both, whichever exist.
obs_eoa() { grep '^EOA_ADDR=' "${ROOT_DIR}/local-validators/$1.eoa" 2>/dev/null | cut -d= -f2; }
KNOWN_ADDRS=("${GENESIS_ADDRS[@]}")
for o in observer1 observer2; do a="$(obs_eoa "$o")"; [ -n "$a" ] && KNOWN_ADDRS+=("$a"); done

echo "=== staking ${NAME} (node ${N}) EOA ${EOA_ADDR} via ${RPC} ==="

# ---- helpers --------------------------------------------------------------------------------
# read JSON from stdin, print a python expression over `d`
jval() {
    python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
print(eval(sys.argv[1]))
' "$1"
}

# currentStatus string for an address (fork-agnostic getValidator), or empty if unregistered.
node_status() {
    cast rpc tn_getValidator "\"$1\"" --rpc-url "$RPC" 2>/dev/null | jval 'd["currentStatus"]' 2>/dev/null
}

# cast send wrapper that surfaces the revert reason on failure.
send() {
    local desc="$1"; shift
    echo "  -> ${desc}"
    local out
    if ! out="$(cast send "$@" --rpc-url "$RPC" --chain-id "$CHAIN_ID" 2>&1)"; then
        echo "  FAILED: ${desc}"
        echo "$out" | sed 's/^/     /'
        exit 1
    fi
}

# committee size to request: count of Active validators, floored at (current committee + 1) so we
# always grow past the present committee to seat the new node. The floor is <= true Active count
# (the current committee was selected before this node activated), so it never over-requests.
target_committee_size() {
    local n=0 a st
    for a in "${KNOWN_ADDRS[@]}"; do
        [ -z "$a" ] && continue
        st="$(node_status "$a")"
        [ "$st" = "Active" ] && n=$((n + 1))
    done
    local clen
    clen="$(cast rpc tn_getCurrentEpochInfo --rpc-url "$RPC" 2>/dev/null | jval 'len(d["committee"])' 2>/dev/null)"
    clen="${clen:-0}"
    local floor=$((clen + 1))
    [ "$n" -lt "$floor" ] && n="$floor"
    echo "$n"
}

# ---- 0. wait for this node's RPC to be live -------------------------------------------------
# The target node may still be starting/syncing (esp. when auto-invoked right after boot). Poll
# its own RPC until it serves a block before submitting transactions.
echo "waiting for ${NAME} RPC (${RPC}) to be live..."
READY=false
for _ in $(seq 1 40); do
    if cast block-number --rpc-url "$RPC" >/dev/null 2>&1; then READY=true; break; fi
    sleep 2
done
[ "$READY" = true ] || { echo "${NAME} RPC ${RPC} never came up"; exit 1; }

# ---- 1. required stake amount ---------------------------------------------------------------
# getCurrentStakeConfig() -> (stakeAmount, minWithdrawAmount, epochIssuance, epochDuration).
STAKE="$(cast call "$REG" "getCurrentStakeConfig()(uint256,uint256,uint256,uint32)" --rpc-url "$RPC" 2>/dev/null | head -1 | awk '{print $1}')"
case "$STAKE" in
    ''|*[!0-9]*) echo "could not read stake amount from getCurrentStakeConfig() (is the RPC up?)"; exit 1 ;;
esac
echo "stake amount: ${STAKE} wei ($(cast from-wei "$STAKE" 2>/dev/null || echo '?') TEL)"

# ---- 2. fund the operator EOA (stake + gas) from governance ---------------------------------
FUND="$(python3 -c "print(int('$STAKE') + 5*10**18)")"   # + 5 TEL for gas (stake + activate)
send "fund ${NAME} EOA with ${FUND} wei" "$EOA_ADDR" --value "$FUND" --private-key "$LOCAL_PK"

# ---- 3. mint the ConsensusNFT (governance-only whitelist) ------------------------------------
send "mint ConsensusNFT for ${EOA_ADDR}" "$REG" "mint(address)" "$EOA_ADDR" --private-key "$LOCAL_PK"

# ---- 4. stake (operator EOA sends BLS args + stakeAmount as value) --------------------------
CALLDATA="$("$BIN" keytool export-staking-args --node-info "$DATADIR" --calldata)"
case "$CALLDATA" in
    0x*) : ;;
    *) echo "export-staking-args did not return calldata: ${CALLDATA}"; exit 1 ;;
esac
send "stake ${STAKE} wei" "$REG" "$CALLDATA" --value "$STAKE" --private-key "$EOA_PK"

# ---- 5. activate (signal readiness for committee selection) ----------------------------------
send "activate()" "$REG" "activate()" --private-key "$EOA_PK"
echo "  ${NAME} is now PendingActivation; it becomes Active at the next epoch boundary."

# ---- 6. grow the committee to seat it (retry across epoch boundaries) ------------------------
# Deterministic seating: with >genesis-size eligible validators the next committee is a random
# 4-of-N shuffle, so governance must call setNextCommitteeSize to seat the new node. The pre-fork
# contract validates newSize <= count(Active); a freshly-activated node is PendingActivation until
# the NEXT boundary, so retry until it is Active, then request the full Active count.
echo "growing committee (waiting for ${NAME} to reach Active)..."
TRIES=6
SEATED=false
for ((t = 1; t <= TRIES; t++)); do
    ST="$(node_status "$EOA_ADDR")"
    echo "  [${t}/${TRIES}] ${NAME} status: ${ST:-<unregistered>}"
    if [ "$ST" = "Active" ]; then
        SIZE="$(target_committee_size)"
        echo "  requesting setNextCommitteeSize(${SIZE})"
        if out="$(cast send "$REG" "setNextCommitteeSize(uint16)" "$SIZE" \
                    --private-key "$LOCAL_PK" --rpc-url "$RPC" --chain-id "$CHAIN_ID" 2>&1)"; then
            echo "  setNextCommitteeSize(${SIZE}) accepted -- ${NAME} will seat at an upcoming boundary."
            SEATED=true
            break
        fi
        echo "  setNextCommitteeSize reverted; retrying after an epoch"
    fi
    sleep 16
done

if [ "$SEATED" = true ]; then
    echo "=== ${NAME} staked, activated, and committee grown. ==="
    echo "    Watch it join: cast rpc tn_getCommitteeValidators \$(cast rpc tn_getCurrentEpoch --rpc-url ${RPC}) --rpc-url ${RPC}"
else
    echo "=== ${NAME} staked + activated, but committee grow did not confirm after ${TRIES} tries. ==="
    echo "    It may still be PendingActivation; re-run this script or call setNextCommitteeSize manually."
    exit 1
fi
