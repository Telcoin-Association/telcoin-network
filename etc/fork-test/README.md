# ConsensusRegistry fork — local test harness

A **throwaway** local harness that proves the pending `ConsensusRegistry` bytecode fork works
end-to-end on a multi-node network before it ships to `adiri`. Nothing here is meant to be
committed.

At the epoch-closing block of `CONSENSUS_REGISTRY_FORK_EPOCH - 1` the protocol swaps the deployed
registry's runtime code from the pinned **pre-fork** bytecode to the current **post-fork** bytecode
and runs a one-time `migrateValidatorSets()` back-fill (see `crates/tn-reth/src/evm/block.rs` and
`crates/types/src/forks.rs`). This harness drives that boundary locally and verifies:

- the swap fires and migration is correct,
- the fleet stays in consensus across the boundary (no divergence),
- validators can stake / activate / rotate into the committee under **both** the pre-fork and
  post-fork bytecode.

## What it changes

| File | Change |
|------|--------|
| `crates/types/src/forks.rs` | **(uncommitted)** `CONSENSUS_REGISTRY_FORK_EPOCH = 5` (was `u32::MAX`). The only Rust edit. |
| `etc/local-testnet.sh` | Builds `--features adiri`, chain-id `2017`, 15 s epochs, two non-genesis **stakeable** validators (observer1/observer2, no `--observer`), a genesis-patch step, and auto pre-fork staking of node 5. |
| `etc/fork-test/patch-genesis.py` | Splices the pinned pre-fork registry `code` + the BlsG1 library account from the committed testnet genesis into the freshly-generated local genesis. |
| `etc/fork-test/stake-validator.sh` | Reusable stake → activate → grow-committee for node 5 (pre-fork) and node 6 (post-fork). |
| `etc/fork-test/sanity-check.sh` | Verifies swap fired, fleet agreement, migration/ABI, and registry health across all six node RPCs. |

## Why the genesis patch is needed

A fresh local genesis **deploys the current (post-fork) contract** and snapshots its storage, so the
fork's fail-closed swap gate — which only runs over a deployment whose code hash equals the pinned
pre-fork hash `0x5318ebc5…` — would abort. `patch-genesis.py` overwrites just the registry `code`
with the pinned pre-fork bytecode (keeping the fresh storage; `migrateValidatorSets()` is idempotent
over it) and adds the `0xce69…0fc7` BlsG1 library the pre-fork registry `DELEGATECALL`s for
proof-of-possession. All nodes receive one byte-identical patched `genesis.yaml`, so the genesis hash
matches and consensus holds.

## Prerequisites

- **Foundry `cast`** — https://book.getfoundry.sh/getting-started/installation
- **python3 + ruamel.yaml** — `pip install ruamel.yaml`
- **`.env`** providing `LOCAL_ADDRESS` (dev/governance account, funded 1B TEL at genesis) and
  `LOCAL_PK` (its private key). `source .env` before running.

## Run it

```bash
source .env

# 1. Build (adiri), generate 4 committee validators + 2 non-committee validators, patch genesis
#    (pre-fork code + BlsG1), start 6 nodes, auto-stake node 5 pre-fork.
./etc/local-testnet.sh --start --dev-funds $LOCAL_ADDRESS --governance $LOCAL_ADDRESS

# 2. Watch node 5 join the committee pre-fork (~epoch 3) and the fork fire at epoch 5:
tail -f local-validators/validator-1.log
cast call 0x07E17e17E17e17E17e17E17E17E17e17e17E17e1 "getCurrentEpoch()(uint32)" --rpc-url http://localhost:8545

# 3. After epoch 5: verify swap, migration, fleet agreement, registry health.
./etc/fork-test/sanity-check.sh

# 4. Post-fork onboarding: stake + activate node 6 under the NEW bytecode.
./etc/fork-test/stake-validator.sh 6

# 5. Re-verify (node 6 seats ~epoch 8).
./etc/fork-test/sanity-check.sh
```

### Node layout

| Node | Instance | RPC | Role |
|------|----------|-----|------|
| validator-1..4 | 1–4 | 8545–8542 | genesis committee |
| observer1 | 5 | 8541 | non-genesis, staked **pre**-fork (node 5) |
| observer2 | 6 | 8540 | non-genesis, staked **post**-fork (node 6) |

`killall telcoin-network` to bring the network down.

## Success criteria

- **Pre-fork onboarding**: `tn_getCommitteeValidators` for a pre-fork epoch includes node 5.
- **Fork fires**: a validator log shows the fork/`migrateValidatorSets` line at the epoch-4 close;
  `cast code` hash of the registry changes away from `0x5318ebc5…`.
- **No divergence**: all six nodes report identical block hashes at post-fork heights.
- **Migration correct**: `tn_getValidators("Any")` / `getEligibleValidatorCount()` reflect every
  staked validator; `getValidatorsInfo`-backed per-status reads succeed.
- **Post-fork onboarding**: node 6 stakes, activates, and appears in `tn_getCommitteeValidators`.

## Gotchas

- **Re-runs**: `local-testnet.sh` skips config if `local-validators/` exists. Delete that directory
  (it also holds each node's `db/`/`consensus-db/`) before regenerating — a stale genesis hash in
  the DB will contradict a re-patched genesis. Auto pre-fork staking runs **only** on a fresh config,
  never on a restart.
- **Do not** pass `--chain adiri/testnet` to the nodes — that loads the embedded genesis and bypasses
  the patched datadir `genesis.yaml`.
- `tn_getValidators()` **reverts pre-fork** (its `getValidatorsInfo` backing selector does not exist
  in the pre-fork bytecode). The grow-committee step therefore counts Active validators via
  `tn_getValidator(address)`, which is identical across the fork. Run `sanity-check.sh`'s per-status
  reads only **after** the fork (epoch ≥ 5).
- **Fidelity gap (acceptable)**: unlike the live chain (whose pre-fork registry has *empty* appended
  slots), the local patched genesis keeps the fresh constructor's *populated* `validatorSets`; the
  idempotent migration converges to the same end state. The one documented non-consensus difference
  is `tn_isValidator` keying (masked-x vs keccak) for keys registered while pre-fork code is live
  (RPC-only, matching `forks.rs`).
