#!/bin/bash
#
# sanity-check.sh — prove the ConsensusRegistry survived the bytecode fork on the local testnet.
#
# Run AFTER the fork fires (current epoch >= CONSENSUS_REGISTRY_FORK_EPOCH == 5). It checks, across
# all six node RPCs:
#   1. Swap fired          — registry code hash != pinned pre-fork hash.
#   2. Fleet agreement     — identical block hash at a post-fork height across all six nodes.
#   3. Migration / new ABI — tn_getValidators("Any") populated+deduped; per-status sets sum to it;
#                            getValidatorsInfo-backed per-status reads succeed; "Undefined" reverts;
#                            getEligibleValidatorCount() matches the eligible set.
#   4. Epoch info          — tn_getCurrentEpochInfo: epochDuration==15, committee populated,
#                            epochId cross-checks the contract getCurrentEpoch().
#   5. Scalars sane        — tn_getNextCommitteeSize / tn_getCurrentStakeVersion /
#                            tn_undistributedIssuance readable, cross-checked vs eth_call.
#   6. Pre-fork onboarding — observer1 (node 5) EOA appears in a pre-fork committee (epoch 3/4)
#                            and reads back as a registered validator.
#   7. Observer2 (node 6)  — informational: post-fork onboarding status.
#
# Usage: ./etc/fork-test/sanity-check.sh
# Depends on: Foundry `cast`, python3.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

REG=0x07E17e17E17e17E17e17E17E17E17e17e17E17e1
# keccak256 of the pinned pre-fork ConsensusRegistry runtime code (forks.rs).
PRE_FORK_HASH=0x5318ebc5cd8123cfb0808fac0f3c0b95ed6f45f67c0853fea0766b52035fea53
FORK_EPOCH=5

# All six node RPCs: validators 1-4 (8545-8542), observer1 (8541), observer2 (8540).
PORTS=(8545 8544 8543 8542 8541 8540)
PRIMARY_RPC="http://localhost:8545"

PASS=0
FAIL=0
pass() { echo "  ✅ $1"; PASS=$((PASS + 1)); }
fail() { echo "  ❌ $1"; FAIL=$((FAIL + 1)); }
info() { echo "  •  $1"; }

# Read JSON from stdin, print a python expression evaluated over `d` (the parsed JSON).
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

rpc()  { cast rpc "$@" --rpc-url "$PRIMARY_RPC" 2>/dev/null; }
call() { cast call "$REG" "$@" --rpc-url "$PRIMARY_RPC" 2>/dev/null; }

# addr_in_list <address> — stdin is a JSON array of ValidatorInfo; prints True/False for membership.
addr_in_list() {
    python3 -c '
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print("False"); sys.exit(0)
target = sys.argv[1].lower()
print(any(v.get("validatorAddress", "").lower() == target for v in d))
' "$1"
}

# Operator EOAs recorded by local-testnet.sh (EOA_ADDR=0x.. lines).
eoa_addr() {
    grep '^EOA_ADDR=' "${ROOT_DIR}/local-validators/$1.eoa" 2>/dev/null | cut -d= -f2
}
OBS1_EOA="$(eoa_addr observer1)"
OBS2_EOA="$(eoa_addr observer2)"

echo "=== ConsensusRegistry fork sanity check ==="

# ---- Pre-flight -------------------------------------------------------------------------------
CUR_EPOCH="$(rpc tn_getCurrentEpoch | tr -d '"')"
if [ -z "$CUR_EPOCH" ]; then
    echo "FATAL: no response from ${PRIMARY_RPC} (is the network up?)"
    exit 1
fi
echo "current epoch: ${CUR_EPOCH} (fork epoch: ${FORK_EPOCH})"
if [ "$CUR_EPOCH" -lt "$FORK_EPOCH" ] 2>/dev/null; then
    echo "WARNING: current epoch < fork epoch — the fork has not fired yet; run this later."
fi
echo

# ---- 1. Swap fired ----------------------------------------------------------------------------
echo "[1] registry code swap"
CODE="$(cast code "$REG" --rpc-url "$PRIMARY_RPC" 2>/dev/null)"
if [ -z "$CODE" ] || [ "$CODE" = "0x" ]; then
    fail "registry has no deployed code at ${REG}"
else
    CODE_HASH="$(cast keccak "$CODE")"
    if [ "$CODE_HASH" != "$PRE_FORK_HASH" ]; then
        pass "registry code swapped away from pre-fork (hash ${CODE_HASH})"
    else
        fail "registry code still equals pre-fork hash — swap did NOT fire"
    fi
fi
echo

# ---- 2. Fleet agreement -----------------------------------------------------------------------
echo "[2] fleet agreement (identical block hash across all six nodes)"
HEIGHT="$(cast block-number --rpc-url "$PRIMARY_RPC" 2>/dev/null)"
if [ -z "$HEIGHT" ]; then
    fail "could not read block height from primary"
else
    # Back off a few blocks so every node has certainly imported the target height.
    CHECK_HEIGHT=$((HEIGHT > 5 ? HEIGHT - 5 : HEIGHT))
    REF_HASH=""
    AGREE=true
    for p in "${PORTS[@]}"; do
        H="$(cast block "$CHECK_HEIGHT" --rpc-url "http://localhost:$p" --json 2>/dev/null | jval 'd["hash"]' 2>/dev/null)"
        if [ -z "$H" ]; then
            info "node :$p did not return block $CHECK_HEIGHT (behind or down)"
            AGREE=false
            continue
        fi
        if [ -z "$REF_HASH" ]; then
            REF_HASH="$H"
        elif [ "$H" != "$REF_HASH" ]; then
            info "node :$p hash $H != $REF_HASH"
            AGREE=false
        fi
    done
    if [ "$AGREE" = true ] && [ -n "$REF_HASH" ]; then
        pass "all six nodes agree on block $CHECK_HEIGHT hash ($REF_HASH)"
    else
        fail "nodes DIVERGE (or lag) at block $CHECK_HEIGHT — see notes above"
    fi
fi
echo

# ---- 3. Migration correct / post-fork ABI live -----------------------------------------------
echo "[3] migration + post-fork getValidatorsInfo ABI"
ANY_JSON="$(rpc tn_getValidators '"Any"')"
ANY_LEN="$(echo "$ANY_JSON" | jval 'len(d)' 2>/dev/null)"
UNIQ_LEN="$(echo "$ANY_JSON" | jval 'len({v["validatorAddress"].lower() for v in d})' 2>/dev/null)"
if [ -n "$ANY_LEN" ] && [ "$ANY_LEN" -gt 0 ] 2>/dev/null; then
    pass "tn_getValidators(\"Any\") returned $ANY_LEN validators"
    if [ "$ANY_LEN" = "$UNIQ_LEN" ]; then
        pass "no duplicate validatorAddress in \"Any\" set"
    else
        fail "duplicate validators in \"Any\" set ($ANY_LEN entries, $UNIQ_LEN unique)"
    fi
else
    fail "tn_getValidators(\"Any\") empty or errored"
fi

# per-status reads (each hits the post-fork getValidatorsInfo ABI) and sum == Any
PER_STATUS_TOTAL=0
STATUS_OK=true
ELIGIBLE_FROM_STATUS=0
for st in Staked PendingActivation Active PendingExit Exited; do
    R="$(rpc tn_getValidators "\"$st\"")"
    L="$(echo "$R" | jval 'len(d)' 2>/dev/null)"
    if [ -z "$L" ]; then
        fail "tn_getValidators(\"$st\") errored (post-fork ABI missing?)"
        STATUS_OK=false
        continue
    fi
    info "$st: $L"
    PER_STATUS_TOTAL=$((PER_STATUS_TOTAL + L))
    case "$st" in
        PendingActivation | Active | PendingExit) ELIGIBLE_FROM_STATUS=$((ELIGIBLE_FROM_STATUS + L)) ;;
    esac
done
if [ "$STATUS_OK" = true ]; then
    pass "all five per-status getValidatorsInfo reads succeeded"
    if [ -n "$ANY_LEN" ] && [ "$PER_STATUS_TOTAL" = "$ANY_LEN" ]; then
        pass "per-status sets sum to \"Any\" ($PER_STATUS_TOTAL == $ANY_LEN)"
    else
        fail "per-status sum ($PER_STATUS_TOTAL) != \"Any\" ($ANY_LEN)"
    fi
fi

# Undefined must revert (negative check).
if rpc tn_getValidators '"Undefined"' >/dev/null 2>&1; then
    fail "tn_getValidators(\"Undefined\") did NOT revert"
else
    pass "tn_getValidators(\"Undefined\") reverts as expected"
fi

# getEligibleValidatorCount() eth_call == count(PendingActivation + Active + PendingExit).
ELIG="$(call 'getEligibleValidatorCount()(uint256)')"
ELIG="${ELIG%% *}" # strip any trailing unit annotation
if [ -n "$ELIG" ] && [ "$ELIG" = "$ELIGIBLE_FROM_STATUS" ]; then
    pass "getEligibleValidatorCount() == eligible per-status count ($ELIG)"
elif [ -n "$ELIG" ]; then
    fail "getEligibleValidatorCount() ($ELIG) != eligible per-status count ($ELIGIBLE_FROM_STATUS)"
else
    fail "getEligibleValidatorCount() eth_call failed"
fi
echo

# ---- 4. Current epoch info -------------------------------------------------------------------
echo "[4] tn_getCurrentEpochInfo"
EI="$(rpc tn_getCurrentEpochInfo)"
EDUR="$(echo "$EI" | jval 'd["epochDuration"]' 2>/dev/null)"
CSIZE="$(echo "$EI" | jval 'len(d["committee"])' 2>/dev/null)"
EPID="$(echo "$EI" | jval 'd["epochId"]' 2>/dev/null)"
if [ "$EDUR" = "15" ]; then
    pass "epochDuration == 15"
else
    fail "epochDuration is '${EDUR}', expected 15"
fi
if [ -n "$CSIZE" ] && [ "$CSIZE" -gt 0 ] 2>/dev/null; then
    pass "committee populated ($CSIZE members)"
else
    fail "committee empty in tn_getCurrentEpochInfo"
fi
# Cross-check tn_ struct epochId against the contract's getCurrentEpoch().
EPOCH_ETH="$(call 'getCurrentEpoch()(uint32)')"
if [ -n "$EPID" ] && [ "$EPID" = "$EPOCH_ETH" ]; then
    pass "epochId ($EPID) cross-checks contract getCurrentEpoch()"
else
    fail "tn_ epochId ($EPID) != contract getCurrentEpoch() ($EPOCH_ETH)"
fi
echo

# ---- 5. Scalar getters sane (RPC vs eth_call) ------------------------------------------------
echo "[5] scalar registry getters"
NCS_RPC="$(rpc tn_getNextCommitteeSize | tr -d '"')"
NCS_ETH="$(call 'getNextCommitteeSize()(uint16)')"
if [ -n "$NCS_RPC" ] && [ "$NCS_RPC" = "$NCS_ETH" ]; then
    pass "tn_getNextCommitteeSize ($NCS_RPC) == contract getNextCommitteeSize()"
else
    fail "next committee size mismatch (rpc '$NCS_RPC' vs eth '$NCS_ETH')"
fi
SV_RPC="$(rpc tn_getCurrentStakeVersion | tr -d '"')"
SV_ETH="$(call 'getCurrentStakeVersion()(uint8)')"
if [ -n "$SV_RPC" ] && [ "$SV_RPC" = "$SV_ETH" ]; then
    pass "tn_getCurrentStakeVersion ($SV_RPC) == contract getCurrentStakeVersion()"
else
    fail "stake version mismatch (rpc '$SV_RPC' vs eth '$SV_ETH')"
fi
UI="$(rpc tn_undistributedIssuance | tr -d '"')"
if [ -n "$UI" ]; then
    pass "tn_undistributedIssuance readable ($(cast to-dec "$UI" 2>/dev/null || echo "$UI"))"
else
    fail "tn_undistributedIssuance failed"
fi
echo

# ---- 6. Pre-fork onboarding: observer1 (node 5) ----------------------------------------------
echo "[6] pre-fork onboarding — observer1 (node 5)"
if [ -z "$OBS1_EOA" ]; then
    info "no observer1.eoa found — skipping (was the network generated by local-testnet.sh?)"
else
    info "observer1 EOA: $OBS1_EOA"
    # Any committee in [2, FORK_EPOCH-1] proves observer1 was seated while the PRE-fork bytecode
    # was still live (a range, not just {3,4}, tolerates epoch-timing jitter).
    SEEN_IN_COMMITTEE=false
    for e in $(seq 2 $((FORK_EPOCH - 1))); do
        C="$(rpc tn_getCommitteeValidators "$e")"
        HIT="$(echo "$C" | addr_in_list "$OBS1_EOA")"
        if [ "$HIT" = "True" ]; then
            SEEN_IN_COMMITTEE=true
            info "present in committee for epoch $e"
        fi
    done
    if [ "$SEEN_IN_COMMITTEE" = true ]; then
        pass "observer1 seated in a PRE-fork committee (epoch 2..$((FORK_EPOCH - 1))) — staking works pre-fork"
    else
        fail "observer1 not found in any pre-fork committee (epoch 2..$((FORK_EPOCH - 1))) — pre-fork onboarding did not complete"
    fi
    ST="$(rpc tn_getValidator "\"$OBS1_EOA\"" | jval 'd["currentStatus"]' 2>/dev/null)"
    if [ -n "$ST" ]; then
        pass "tn_getValidator(observer1) status: $ST"
    else
        fail "tn_getValidator(observer1) errored"
    fi
fi
echo

# ---- 7. Post-fork onboarding: observer2 (node 6) — informational -----------------------------
echo "[7] post-fork onboarding — observer2 (node 6) [informational]"
if [ -z "$OBS2_EOA" ]; then
    info "no observer2.eoa found — skipping"
else
    info "observer2 EOA: $OBS2_EOA"
    ST2="$(rpc tn_getValidator "\"$OBS2_EOA\"" | jval 'd["currentStatus"]' 2>/dev/null)"
    if [ -n "$ST2" ]; then
        info "tn_getValidator(observer2) status: $ST2"
        IN2="$(rpc tn_getCommitteeValidators "$CUR_EPOCH" | addr_in_list "$OBS2_EOA")"
        if [ "$IN2" = "True" ]; then
            info "observer2 IS in the current committee (epoch $CUR_EPOCH) — post-fork seating done"
        else
            info "observer2 not yet in the current committee (run stake-validator.sh 6, wait ~epochs)"
        fi
    else
        info "observer2 not registered yet (run ./etc/fork-test/stake-validator.sh 6 to onboard it)"
    fi
fi
echo

# ---- Summary ----------------------------------------------------------------------------------
echo "=== summary: ${PASS} passed, ${FAIL} failed ==="
if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
