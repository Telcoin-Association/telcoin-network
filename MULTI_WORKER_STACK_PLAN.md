# Multi-Worker Stacked Branch Plan

This file tracks the branch/PR stacking strategy for the multi-worker implementation.

## Objective

Implement multi-worker support through stacked PRs with one branch per PR, each branch based on the previous one.

## Branch Naming and Stack Order

1. `stack/554-config-foundation` (base: `main`)
2. `stack/555-per-worker-swarms` (base: `stack/554-config-foundation`)
3. `stack/556-per-worker-local-network` (base: `stack/555-per-worker-swarms`)
4. `stack/557-epoch-manager-multi-worker-loop` (base: `stack/556-per-worker-local-network`)
5. `stack/558-engine-initialization-multi-worker` (base: `stack/557-epoch-manager-multi-worker-loop`)
6. `stack/559-multi-worker-tests` (base: `stack/558-engine-initialization-multi-worker`)

## PR Mapping

- Branch `stack/554-config-foundation` -> Issue #554
- Branch `stack/555-per-worker-swarms` -> Issue #555
- Branch `stack/556-per-worker-local-network` -> Issue #556
- Branch `stack/557-epoch-manager-multi-worker-loop` -> Issue #557
- Branch `stack/558-engine-initialization-multi-worker` -> Issue #558
- Branch `stack/559-multi-worker-tests` -> Issue #559

## Scope Per Branch

### #554 Config Foundation
- `NodeP2pInfo` transition from single worker to `workers: Vec<_>` with compatibility for old serialized format.
- `NodeInfo` worker accessors by `WorkerId`.
- Add `parameters.num_workers` (default `1`) and `Config::num_workers`.
- Per-worker topic function signatures in `LibP2pConfig`.
- Genesis validation for consistent worker count across validators.
- Compile-safe callsite adjustments while runtime behavior remains equivalent for worker `0`.

### #555 Per-Worker Swarms
- Spawn one worker swarm per worker id.
- Maintain `Vec<WorkerNetworkHandle>` and per-worker event streams.
- Use worker-specific network key/address and worker-specific gossip topics.

### #556 Per-Worker LocalNetwork
- Replace single `LocalNetwork` with `Vec<LocalNetwork>`.
- Ensure primary registers handlers on every worker local network.
- Route worker construction with local network indexed by worker id.

### #557 EpochManager Multi-Worker Loop
- Loop worker creation and epoch orchestration over `0..num_workers`.
- Return `Vec<WorkerNode>` from consensus creation path.
- Update batch builder startup/orphan handling to iterate over workers.
- Remove hardcoded `worker_id = 0` manager flow.

### #558 Engine Initialization Multi-Worker
- Initialize worker components per worker.
- Update worker network task respawn interfaces to take all worker handles.
- Preserve faucet attachment to worker `0` only.

### #559 Multi-Worker Tests
- Add/expand multi-worker tests and integration scenarios.
- Keep this branch as the main testing consolidation branch for the epic.

## Quality Gates

- Compile checks after each branch update.
- Targeted tests where relevant for changed area.
- Consolidated broad tests in #559.

## Notes

- Keep diffs minimal and scoped to the branch issue.
- Preserve backward behavior when `num_workers = 1`.
- Do not perform unrelated refactors.
