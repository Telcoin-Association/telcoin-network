#!/bin/bash
#
# Archive-mode guard: fail the build if a pruning entry point enters the node.
#
# Every pinned consensus-registry read (committee membership, epoch records, epoch state, worker
# fee configs) resolves historical state through the one `pinned_state_and_env` helper in
# crates/tn-reth/src/env/epoch.rs. Reth serves that state from the pinned block only while the
# history indices covering it survive. Once pruning removes them the read either fails with
# `StateAtBlockPruned` (pinned block below the prune watermark) or, worse, silently serves TIP
# state (at or above the watermark, with the key's own shards gone). The loud half still lands
# mid-consensus (committee construction, epoch entry), so the reachable outcomes there are a
# stalled epoch transition or a halted node; the silent half corrupts the read outright.
#
# `RethConfig::ensure_archive_mode` (crates/tn-reth/src/cli.rs) already refuses to start a node
# whose CONFIGURATION requests pruning. This guard covers the two things a configuration check
# cannot see:
#
#   1. a pruner driven straight against the provider, bypassing `NodeConfig` entirely;
#   2. a reth.toml `[prune]` section, which reth's own launcher merges *behind*
#      `NodeConfig::prune_config` (TN hand-rolls its init path today and never loads one).
#
# Either is a deliberate semantic change to every pinned read, so it fails here and becomes a
# visible review decision instead of a silent one.
#
# Fails closed: `set -euo pipefail` plus an explicit check on grep's exit status means a missing
# tool or an unreadable tree aborts rather than reporting success. Resolves nothing and compiles
# nothing.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Pruner entry points, plus the reth.toml loader. None has a legitimate use while pinned reads
# depend on fully indexed history.
FORBIDDEN='PrunerBuilder|Pruner::new|with_prune_modes|set_prune_modes|PruneModes::|reth_config::Config'

# `grep` exits 0 on a match, 1 on no match, and >=2 on an error (unreadable tree, bad regex).
# Capture the status explicitly rather than testing it with `if`, which would fold an ERROR into
# the same branch as "no match" and let the guard pass on a scan that never happened.
# Scan every workspace member holding Rust sources, not just crates/: the node binary itself
# (bin/telcoin-network) is exactly where a pruner would most plausibly be wired up.
SCAN_ROOTS="crates bin examples"

status=0
hits=$(grep -rEn --include='*.rs' "$FORBIDDEN" $SCAN_ROOTS) || status=$?

if [ "$status" -gt 1 ]; then
    echo "archive-mode guard FAILED: could not scan ${SCAN_ROOTS} (grep exit ${status})."
    exit 1
fi

if [ "$status" -eq 0 ]; then
    echo "archive-mode guard FAILED: a pruning entry point was introduced into the node."
    echo
    echo "$hits"
    echo
    cat <<'EOF'
Pinned consensus-registry reads (committee membership, epoch records, epoch state, worker fee
configs) resolve historical state through `state_by_block_hash`. Pruning that history makes reth
fail those reads with StateAtBlockPruned, mid-consensus, once a pinned block falls below the
history watermark.

If this is intentional, the change is not just this line: audit the ARCHIVE-MODE note on
`pinned_state_and_env` in crates/tn-reth/src/env/epoch.rs, decide what a pinned read means under
pruning, and update `RethConfig::ensure_archive_mode` (crates/tn-reth/src/cli.rs) and this guard
together.
EOF
    exit 1
fi

echo "archive-mode guard passed: no pruning entry point in the node"
