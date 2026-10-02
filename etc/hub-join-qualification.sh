#!/usr/bin/env bash
# Qualify cold joins on each swarm and actual governance activation in a disposable Linux runner.
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export RUSTFLAGS="-D warnings -D unused_extern_crates" RUST_BACKTRACE=1
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-target}"
evidence_dir=hub-join-evidence
mkdir -p "$evidence_dir"
{
  git rev-parse HEAD
  git submodule status
  uname -a
  rustc -Vv
} > "$evidence_dir/environment.txt"
cargo nextest run --no-run --locked --workspace
for attempt in 1 2 3 4 5; do
  echo "qualification_attempt=$attempt"
  cargo nextest run --locked -p tn-network-libp2p -E 'test(hub_join_)' \
    --success-output immediate --failure-output immediate
done 2>&1 | tee "$evidence_dir/swarms.log"
make build-e2e-bin
export TN_BIN_PATH
TN_BIN_PATH="$(realpath "$CARGO_TARGET_DIR/e2e/telcoin-network")"
for attempt in 1 2 3 4 5; do
  export HUB_JOIN_QUALIFICATION_ATTEMPT="$attempt"
  echo "qualification_attempt=$attempt"
  cargo nextest run --locked -p e2e-tests -E 'test(hub_join_governance_two_workers)' \
    --run-ignored only --success-output immediate --failure-output immediate
done 2>&1 | tee "$evidence_dir/governance.log"
python3 etc/hub-join-summary.py "$evidence_dir" > "$evidence_dir/summary.json"
python3 etc/hub-join-mutation.py 2>&1 | tee "$evidence_dir/mutation.log"
