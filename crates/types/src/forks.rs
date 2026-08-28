//! Code to support various chain forks.

use crate::Epoch;
use alloy::primitives::{b256, B256};

/// Keccak-256 hash of the pre-fork `ConsensusRegistry` runtime bytecode deployed on the live
/// adiri testnet (the registry account's `code` in the committed
/// `chain-configs/testnet/genesis.yaml`).
///
/// This constant pins the code the [`CONSENSUS_REGISTRY_FORK_EPOCH`] upgrade expects to find
/// on-chain, and is load-bearing in two places (both in `tn-reth::evm::block`):
/// - **Legacy-read routing:** while the deployed registry still carries this code, the
///   epoch-conclusion path reads the committee-eligible pool via the pre-fork `getValidators(uint8)
///   -> ValidatorInfo[]` ABI instead of the post-fork `getValidatorsInfo` queries, so pre-fork
///   epoch closes (fresh-node onboarding, full resync) execute byte-identically to the historical
///   chain.
/// - **Fail-closed swap gate:** the in-place code swap at the fork boundary refuses to run over any
///   deployment whose code hash differs from this value, rather than migrating over an unknown
///   storage layout.
///
/// Unconditional (not `adiri`-gated) so the pin test guarding it runs in default-feature CI.
pub const CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH: B256 =
    b256!("0x5318ebc5cd8123cfb0808fac0f3c0b95ed6f45f67c0853fea0766b52035fea53");

/// Keccak-256 hash of the upgraded (post-fork) `ConsensusRegistry` runtime bytecode the
/// [`CONSENSUS_REGISTRY_FORK_EPOCH`] boundary swaps in: the embedded `ConsensusRegistry.json`
/// `deployedBytecode.object` (`tn-config`'s `CONSENSUS_REGISTRY_JSON`, loaded by
/// `tn-reth::evm::block::consensus_registry_runtime_code`).
///
/// Unlike the pre-fork pins this is not a gate — nothing compares against it at runtime. It exists
/// so an artifact bump is caught at compile-and-test time rather than at replay time: once the
/// fork has run live, re-executing the fork block must swap in these exact bytes, and a
/// tn-contracts submodule bump would silently change them and break historical state roots.
///
/// Unconditional (not `adiri`-gated) so the pin test guarding it runs in default-feature CI.
pub const CONSENSUS_REGISTRY_POST_FORK_CODE_HASH: B256 =
    b256!("0xbd1ade0e07b6827794e1e0cb181e756ff3b6916535c81941b634cd783a9cd8ce");

/// Keccak-256 hash of the pre-fork `WorkerConfigs` runtime bytecode deployed on the live adiri
/// testnet (the worker-configs account's `code` in the committed
/// `chain-configs/testnet/genesis.yaml`).
///
/// The live deployment is an old, pre-#161 tn-contracts build: its `WorkerConfig.data` field is a
/// `uint128`, and it exposes none of the `setWorkerConfigsData`, `setWorkerConfigsValue`, or
/// `setMaxStrategy` selectors the current artifact defines. The protocol therefore has no way to
/// write worker fee state back to the deployed contract, which is why the fork splices the current
/// artifact's runtime code over it at [`CONSENSUS_REGISTRY_FORK_EPOCH`], alongside the registry
/// swap.
///
/// That splice is gated fail-closed on this hash: if the on-chain `WorkerConfigs` code hashes to
/// anything else, the block aborts rather than upgrading over an unknown storage layout.
///
/// Unconditional (not `adiri`-gated) so the pin test guarding it runs in default-feature CI.
pub const WORKER_CONFIGS_PRE_FORK_CODE_HASH: B256 =
    b256!("0x5e8a93f4eb1b5d645f32e5b8615463a996aaf4d8af2a90a444378a2d4b4b3bf2");

/// Keccak-256 hash of the upgraded (post-fork) `WorkerConfigs` runtime bytecode the
/// [`CONSENSUS_REGISTRY_FORK_EPOCH`] boundary splices over the deployed contract: the embedded
/// `WorkerConfigs.json` `deployedBytecode.object` (`tn-config`'s `WORKER_CONFIGS_JSON`, loaded by
/// `tn-reth::evm::block::worker_configs_runtime_code`).
///
/// The splice rides the same boundary block as the registry swap, so it carries the same replay
/// constraint for the same reason: see [`CONSENSUS_REGISTRY_POST_FORK_CODE_HASH`].
///
/// Unconditional (not `adiri`-gated) so the pin test guarding it runs in default-feature CI.
pub const WORKER_CONFIGS_POST_FORK_CODE_HASH: B256 =
    b256!("0x58304c00bbfaa7e348220efb95843614756207311245abc4949f91bb3ddb2ff7");

#[cfg(feature = "adiri")]
/// The epoch below which Adiri testnet may have had duplicate batches.
pub const ADIRI_DUP_BATCH_EPOCH: Epoch = 160;

#[cfg(feature = "adiri")]
/// First epoch that runs on the upgraded `ConsensusRegistry` bytecode.
///
/// The epoch-closing block of `CONSENSUS_REGISTRY_FORK_EPOCH - 1` swaps the deployed registry
/// code to the upgraded version (preserving all existing storage) and runs the one-time
/// `migrateValidatorSets()` that back-fills the appended per-status `validatorSets` and the cached
/// `eligibleValidatorCount`. From the first block of `CONSENSUS_REGISTRY_FORK_EPOCH` onward the
/// protocol runs on the new code with populated sets. See
/// `tn-reth::evm::block::apply_consensus_registry_fork`.
///
/// Scope: an Adiri-testnet-only, in-place upgrade of an already-deployed registry (the whole
/// mechanism is `#[cfg(feature = "adiri")]`, so non-adiri/mainnet builds exclude it) — not a
/// general registry-upgrade path. The fork only exists in binaries compiled with the `adiri`
/// feature, so the activation-epoch PR must ship alongside a confirmed fork-capable node build.
///
/// Accepted, documented behavior across the fork: the new contract keys its
/// `blsPubkeyHashToValidator` dedup map by a masked-x `_blsKeyId` rather than `keccak(full key)`,
/// and the migration does not re-key legacy entries. The only effects are `isValidator(legacyKey)`
/// returning `false` (RPC-only, not consensus-critical) and a weakened cross-fork duplicate-key
/// check (governance-gated NFT minting prevents abuse on the permissioned testnet).
///
/// Armed for adiri (chain 2017) at epoch 407, so both code swaps execute one boundary earlier, in
/// the epoch-closing block of 406. Projected boundary: 2026-08-19 15:29 UTC, give or take about
/// 26 minutes at 1σ. Derived by binary-searching first-block timestamps over epochs 398→404 on the
/// live chain (mean epoch length 21,985 s, σ 902 s), snapshotted at epoch 404 / block 317826 on
/// 2026-08-18.
///
/// That projection is a live measurement, NOT the fixed
/// `T(E) = 2026-08-11T02:20:52Z + (E - 373) × 6h` grid earlier revisions of this comment used. A
/// roughly six-hour halt during the epoch-383 recovery pushed every real boundary later than that
/// grid, which now runs about an hour fast, so a future retarget must re-derive from live boundary
/// timestamps rather than extrapolate a fixed cadence.
///
/// Nothing machine-checks that this epoch is still in the future — no const-assert, no test — so
/// manual re-verification is the only guard. Re-verify against the live chain at merge time. If
/// epoch 407 has already begun, raise the constant in the same PR: the trigger
/// (`concluding_epoch + 1 == CONSENSUS_REGISTRY_FORK_EPOCH`) cannot fire retroactively, so the
/// live fleet would skip the swap for good while a node replaying that boundary on this build
/// applies it and diverges from canonical history.
///
/// Rollout sequence (standard hard-fork rule): every validator must run a fork-capable build
/// (compiled `--features adiri` — verify the deploy image — and including the epoch-setting PR)
/// **before** the epoch-closing block of `CONSENSUS_REGISTRY_FORK_EPOCH - 1` executes. Nodes
/// still on older builds never apply the swap at that boundary, reject the fork block, and
/// diverge from the canonical chain.
///
/// Deploying the fork-capable build EARLY is safe for the registry-read path: the
/// committee-pool read is gated on the deployed registry's code hash
/// ([`CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`]), so every pre-fork epoch close speaks the legacy
/// registry ABI and derives byte-identical committees (the legacy single-call pool order feeds
/// the shuffle exactly as the historical chain computed it). The same gate keeps pre-fork
/// history re-executable, so fresh-node onboarding and full resync from genesis work across the
/// fork on one binary. Scope: this covers the registry reads only — full old-binary ↔
/// fork-build live mixed-fleet compatibility depends on everything else shipped since and is
/// confirmed by the operator dry-run below, not promised here.
///
/// Pre-deploy checklist for the epoch-setting PR:
/// - **DONE** — the swapped-in (post-fork) runtime code hashes of **both** contracts are pinned the
///   same way [`CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`] and [`WORKER_CONFIGS_PRE_FORK_CODE_HASH`]
///   pin the pre-fork code, with pin tests against the embedded `ConsensusRegistry.json` and
///   `WorkerConfigs.json` artifacts: after the fork runs live, a tn-contracts artifact bump would
///   otherwise change the bytes re-execution swaps in and break historical state roots. Re-pinned
///   against tn-contracts `0fb6b01`, which moved the registry hash only — the `WorkerConfigs`
///   artifact is byte-identical across that bump, so [`WORKER_CONFIGS_POST_FORK_CODE_HASH`] did not
///   move;
/// - **DONE** — the LIVE deployed `WorkerConfigs` was read on 2026-08-18: `numWorkers() == 1`, and
///   a storage probe of `_workerConfigSet[0]` (the mapping is internal) reads `1`, so every row
///   below `numWorkers` is set and the first post-fork closing block's `setWorkerConfigsData`
///   system call cannot revert `MissingWorkerConfig` and abort that one-shot close. Worker 0 reads
///   back as `Eip1559 { target_gas: u64::MAX }` with `data == 0`, so the entry read prices the
///   first post-fork epoch from MIN and no governance write has landed on any row (see the rollout
///   constraint below). The embedded artifact's post-fork `WorkerConfigs` splice hashes to
///   `0x58304c00bbfaa7e348220efb95843614756207311245abc4949f91bb3ddb2ff7`, reproduced here for
///   readability; the binding copy is [`WORKER_CONFIGS_POST_FORK_CODE_HASH`], guarded by
///   `test_post_fork_worker_configs_code_hash_pinned`;
/// - the `WorkerConfigs` bytecode swap ships at this same fork epoch (see
///   [`WORKER_CONFIGS_PRE_FORK_CODE_HASH`]) — both swaps land in the epoch-closing block of
///   `CONSENSUS_REGISTRY_FORK_EPOCH - 1`, so a build applying one but not the other diverges;
/// - **DONE** — the live ConsensusNFT count is 5 (`totalSupply()` on 2026-08-18), so the one-shot
///   `migrateValidatorSets()` walk covers five entries: orders of magnitude of headroom under the
///   100M system-call gas cap that bounds it;
/// - **OPEN** — operator dry-run: resync a fork-build node against a live adiri archive across the
///   fork boundary and confirm matching state roots (also measures the live migration gas). This is
///   the last unfinished item on this checklist;
/// - both swaps fail closed on their pre-fork pin ([`CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`],
///   [`WORKER_CONFIGS_PRE_FORK_CODE_HASH`]): an unexpected on-chain deployment aborts the block
///   (fatal error) rather than migrating over an incompatible layout. Both gates still held on
///   2026-08-18, when the live registry and `WorkerConfigs` code hashes each matched their pinned
///   pre-fork value;
/// - adiri rollout constraint: until this fork epoch has passed on a fleet running the entry-read
///   build, governance must not touch ANY `WorkerConfigs` row — no `setWorkerConfig` writes, no
///   strategy flips, no `data` writes. The deployed pre-fork contract's
///   `setWorkerConfig(uint16,uint8,uint64,uint128)` writes the same `_workerConfigs` rows the entry
///   read consumes and pre-fork closes never overwrite `data`, so a non-zero word landed pre-fork
///   has entry-read nodes pricing the epoch from it while older builds scan-derive MIN (the
///   exact-equality basefee check splits the fleet), and a word above `u64::MAX` fail-hards the
///   whole entry-read fleet at the same entry — chain halted, unrecoverable by any governance
///   transaction, coordinated binary patch only. A strategy flip is no safer even when it looks
///   fee-neutral: `Static { S }` → `Eip1559 { u64::MAX }` (neither a `target_gas` move nor a
///   `Static` fee change) has older builds deriving ~0.875·S from the header scan while entry-read
///   nodes read `data == 0` → MIN;
/// - post-fork governance runbook: the `WorkerConfigs` swap replaces code only, so the appended
///   `maxStrategy` slot (slot 4) stays `0` and every owner call assigning `Static` (strategy id 1)
///   reverts `InvalidStrategy`. The owner must send one `setMaxStrategy(1)` transaction after the
///   fork before any Static assignment. This is not urgent: neither the protocol write path
///   (`setWorkerConfigsData` / `setWorkerConfigsValue`) nor the epoch-boundary read path consults
///   `maxStrategy`, so a zeroed ceiling gates future governance actions only.
pub const CONSENSUS_REGISTRY_FORK_EPOCH: Epoch = 407;

#[cfg(feature = "adiri")]
/// First epoch whose `Header`s carry the `seed_signature` field on the wire (#1032).
///
/// Headers of earlier epochs serialize the seven legacy fields byte-identically to the
/// pre-fork binary and keep the legacy leader-aggregate committee-shuffle seed, so a binary
/// with this gate is wire-indistinguishable from the historical chain for every pre-fork
/// epoch: deploys roll gradually across a mixed fleet with no protocol bump, no migration,
/// and no coordination window. From this epoch onward the eighth field is written (and
/// required) on the wire, voters verify it, and the epoch seed chain
/// ([`EpochSeedChainValue`](crate::EpochSeedChainValue)) replaces the leader-aggregate seed.
///
/// The gate ([`seed_signature_active`]) always reads the epoch carried inside the value
/// being encoded or decoded — never node-local committee state — so mixed-epoch containers
/// (certificate vectors, sub-DAGs, pack records) decode correctly at any nesting depth, and
/// historical digests are preserved end to end.
///
/// Set to epoch 383 for the #1086 rollout (PR-2, adjusted from the initial 400).
/// Adjustment-time snapshot: live adiri epoch 379 on 2026-08-12 (latest block 313478,
/// nonce `>> 32` via rpc.adiri.tel), so 383 begins at the next epoch boundary. That is
/// inside the plan's floor of current + 8: every adiri node must run this build before
/// that boundary closes. No test or CI re-checks the margin: re-verify it against the
/// live chain at merge time. If epoch 383 has already begun, raise the constant in the
/// same PR: headers committed at or past the fork epoch in the legacy seven-field layout
/// do not decode under this build. The full fork schedule is logged at startup so
/// operators can diff it across the fleet; a compile-time constant that differs between
/// binaries has no other in-protocol detection.
///
/// Rollout sequence (standard hard-fork rule): deploy the gate-capable build fleet-wide
/// first (safe indefinitely while dormant), then land the epoch-setting PR fleet-wide before
/// the fork epoch begins. A straggler still on an old build past the boundary fails to
/// decode post-fork headers loudly and drops out rather than silently diverging: the field
/// is covered by the header digest, and decode failures charge `Penalty::Fatal` to the
/// author while committee authors stay exempt from bans.
///
/// Non-adiri builds (mainnet) have no dormant period: the field is active from genesis and
/// this constant does not exist there.
pub const SEED_SIGNATURE_FORK_EPOCH: Epoch = 383;

/// Whether `Header`s of `epoch` carry the `seed_signature` field on the wire and the epoch
/// seed chain drives the epoch-close committee shuffle (#1032).
///
/// Gates both directions of serialization plus every consumer of the seed (proposer signing,
/// vote verification, sub-DAG randomness). Callers MUST pass the epoch carried inside the
/// value being encoded or decoded (e.g. `HeaderInner::epoch`, `leader.epoch()`), never
/// `Committee::epoch()` or other node-local state, so that historical values keep their
/// historical layout at any nesting depth.
///
/// Adiri builds activate at [`SEED_SIGNATURE_FORK_EPOCH`]; all other builds are active from
/// genesis (mainnet never carries the legacy layout). Under `test-utils`, an explicit
/// `TN_SEED_SIGNATURE_FORK_EPOCH` override takes precedence over both (see
/// [`seed_signature_fork_epoch_override`]), so a test states the fork point it means rather
/// than inheriting whichever one its feature set happens to select.
#[inline]
pub fn seed_signature_active(epoch: Epoch) -> bool {
    #[cfg(feature = "test-utils")]
    {
        seed_signature_fork_epoch_override()
            .map_or_else(|| build_fork_active(epoch), |fork| epoch >= fork)
    }
    #[cfg(not(feature = "test-utils"))]
    {
        build_fork_active(epoch)
    }
}

/// This build's compile-time fork point, with no test override applied.
///
/// Unchanged from [`SEED_SIGNATURE_FORK_EPOCH`]'s documented contract: adiri (testnet, which
/// carries pre-fork history) is dormant before the fork epoch and active from it, and every
/// other build (mainnet, which never carries the legacy layout) is active from genesis.
#[inline]
const fn build_fork_active(epoch: Epoch) -> bool {
    #[cfg(feature = "adiri")]
    {
        epoch >= SEED_SIGNATURE_FORK_EPOCH
    }
    #[cfg(not(feature = "adiri"))]
    {
        let _ = epoch;
        true
    }
}

/// Test-only override of the effective seed-signature fork epoch, read once from
/// `TN_SEED_SIGNATURE_FORK_EPOCH` (`4294967295` for "never fires", `0` for "active from
/// genesis").
///
/// An environment variable rather than a process-global setter because e2e tests drive real
/// node processes spawned via `TN_BIN_PATH`, which share no memory with the harness: a static
/// would silently reach only the in-process tests, and the multi-node tests that actually
/// exercise epoch close would keep inheriting the build default.
///
/// Compiled out entirely without `test-utils`, so a production binary keeps the compile-time
/// constant and cannot be repointed at runtime by its environment. An unparseable value is
/// ignored rather than defaulted, leaving the build's own fork point in force.
#[cfg(feature = "test-utils")]
pub fn seed_signature_fork_epoch_override() -> Option<Epoch> {
    static OVERRIDE: std::sync::OnceLock<Option<Epoch>> = std::sync::OnceLock::new();
    *OVERRIDE.get_or_init(|| {
        std::env::var("TN_SEED_SIGNATURE_FORK_EPOCH").ok().and_then(|raw| raw.trim().parse().ok())
    })
}

/// First epoch whose executed blocks derive `mix_hash` (EVM `PREVRANDAO`) from the epoch
/// seed chain instead of the transaction-dependent `output_digest ^ batch_digest` (#1247).
///
/// Both XORed digests commit to transaction bytes and transaction ordering, so a committing
/// leader can enumerate otherwise-valid payload constructions after the causal DAG is known
/// and publish the one whose `PREVRANDAO` favors it. From this epoch onward the value is
/// `keccak256(domain || seed chain value || consensus block number || batch index)` (see
/// [`ConsensusOutput::prev_randao`](crate::ConsensusOutput::prev_randao)): every input is
/// pinned by the committed order and the digest-pinned seed chain, so payload construction
/// cannot generate new candidates.
///
/// This narrows grinding to the single propose-or-withhold choice the committing leader
/// always has; it does not make `PREVRANDAO` unbiasable. See
/// [`EpochSeedChainValue`](crate::EpochSeedChainValue) on accepted last-actor bias.
///
/// `Epoch::MAX` is the dormant placeholder of the standard two-step hard-fork rule (the
/// same sequence [`SEED_SIGNATURE_FORK_EPOCH`] followed): deploy this gate-capable build
/// fleet-wide first (safe indefinitely while dormant on adiri), then land the epoch-setting
/// PR fleet-wide before the fork epoch begins. The rollout PR MUST set a value at or above
/// the live adiri epoch plus deployment margin, and at or above
/// [`SEED_SIGNATURE_FORK_EPOCH`]: [`prevrandao_seed_active`] additionally requires
/// [`seed_signature_active`], so a lower value silently stays dormant until the seed fork
/// fires instead of hashing the forkable legacy leader-aggregate seed (#1032).
///
/// Pre-fork epochs keep the XOR derivation byte-identical so replaying already-executed
/// history reproduces the same headers. Non-adiri builds carry no such history and are
/// active from genesis, exactly as with the seed-signature fork (like
/// [`SEED_SIGNATURE_FORK_EPOCH`], this constant does not exist there). The full fork
/// schedule is logged at startup so operators can diff it across the fleet; a compile-time
/// constant that differs between binaries has no other in-protocol detection.
#[cfg(feature = "adiri")]
pub const PREVRANDAO_FORK_EPOCH: Epoch = Epoch::MAX;

/// Compile-time enforcement of the rollout-order contract documented on
/// [`PREVRANDAO_FORK_EPOCH`]: a rollout PR that sets the PREVRANDAO fork below the seed
/// fork fails to compile instead of shipping a gate that silently stays dormant until the
/// seed fork fires.
#[cfg(feature = "adiri")]
#[expect(
    clippy::absurd_extreme_comparisons,
    reason = "always true only while PREVRANDAO_FORK_EPOCH is the `Epoch::MAX` placeholder; \
              once the rollout PR lowers the constant the comparison becomes live and this \
              expectation flags itself for removal"
)]
const _: () = assert!(PREVRANDAO_FORK_EPOCH >= SEED_SIGNATURE_FORK_EPOCH);

/// Whether executed blocks of `epoch` derive `PREVRANDAO` from the epoch seed chain (#1247).
///
/// Callers MUST pass the epoch carried inside the output being executed (the committing
/// leader's epoch), never node-local committee state, so historical outputs keep their
/// historical derivation during replay.
///
/// Requires [`seed_signature_active`] as a fail-closed conjunct: the new derivation hashes
/// the sub-dag `randomness`, which is the digest-pinned epoch seed chain value only once the
/// seed fork is active. If an override or a future fork schedule orders the two forks the
/// other way, the gate stays on the legacy XOR (the status quo) instead of promoting the
/// forkable legacy leader-aggregate seed into `PREVRANDAO`.
#[inline]
pub fn prevrandao_seed_active(epoch: Epoch) -> bool {
    seed_signature_active(epoch) && prevrandao_fork_point_active(epoch)
}

/// This build's effective PREVRANDAO fork point (any `test-utils` override applied),
/// without the [`seed_signature_active`] conjunct [`prevrandao_seed_active`] enforces.
#[inline]
fn prevrandao_fork_point_active(epoch: Epoch) -> bool {
    #[cfg(feature = "test-utils")]
    {
        prevrandao_fork_epoch_override()
            .map_or_else(|| prevrandao_build_fork_active(epoch), |fork| epoch >= fork)
    }
    #[cfg(not(feature = "test-utils"))]
    {
        prevrandao_build_fork_active(epoch)
    }
}

/// This build's compile-time PREVRANDAO fork point, with no test override applied.
///
/// Same contract as [`build_fork_active`]: adiri (testnet, which carries pre-fork executed
/// history) is dormant before [`PREVRANDAO_FORK_EPOCH`] and active from it; every other
/// build is active from genesis.
#[inline]
const fn prevrandao_build_fork_active(epoch: Epoch) -> bool {
    #[cfg(feature = "adiri")]
    #[expect(
        clippy::absurd_extreme_comparisons,
        reason = "PREVRANDAO_FORK_EPOCH is an `Epoch::MAX` placeholder; `>=` (not `==`) is \
                  the gate the future epoch-setting PR relies on, and this expectation flags \
                  itself for removal once that PR lowers the constant"
    )]
    {
        epoch >= PREVRANDAO_FORK_EPOCH
    }
    #[cfg(not(feature = "adiri"))]
    {
        let _ = epoch;
        true
    }
}

/// Test-only override of the effective PREVRANDAO fork epoch, read once from
/// `TN_PREVRANDAO_FORK_EPOCH` (`4294967295` for "never fires", `0` for "active from
/// genesis", both subject to the [`seed_signature_active`] conjunct).
///
/// An environment variable for the same reason as [`seed_signature_fork_epoch_override`]:
/// e2e tests drive real node processes spawned via `TN_BIN_PATH`, which share no memory
/// with the harness, so a process-global setter would silently reach only in-process tests.
/// Compiled out entirely without `test-utils`, so a production binary keeps the
/// compile-time constant and cannot be repointed at runtime by its environment. An
/// unparseable value is ignored rather than defaulted, leaving the build's own fork point
/// in force.
#[cfg(feature = "test-utils")]
pub fn prevrandao_fork_epoch_override() -> Option<Epoch> {
    static OVERRIDE: std::sync::OnceLock<Option<Epoch>> = std::sync::OnceLock::new();
    *OVERRIDE.get_or_init(|| {
        std::env::var("TN_PREVRANDAO_FORK_EPOCH").ok().and_then(|raw| raw.trim().parse().ok())
    })
}

#[cfg(feature = "adiri")]
/// First epoch whose [`Committee`](crate::Committee) is bcs-encoded in the multi-worker layout
/// (#554).
///
/// Two fields move at this boundary, both inside the `Committee` value itself:
/// - each `BootstrapServer` writes `workers`, a length-prefixed sequence of `P2pNode`, where the
///   legacy layout wrote a single unprefixed `worker`;
/// - `CommitteeInner` gains a trailing `num_workers`.
///
/// bcs is not self-describing, so neither change is backward compatible — `#[serde(default)]`
/// buys nothing on a binary codec, and a legacy value read as the new layout consumes the
/// worker's first byte as a sequence length. `Committee` is embedded in `EpochMeta`, the first
/// record of every consensus pack, so an un-gated layout change bricks decode of every pack
/// already on disk: an adiri node restarting on the new build cannot read its own history. Below
/// this epoch the encoder writes the legacy single-worker shape byte-identically to the pre-#554
/// binary, so packs stay readable in both directions across a mixed fleet.
///
/// The gate ([`multi_workers_fork_active`]) always reads the epoch carried inside the value being
/// encoded or decoded — never node-local committee state — so mixed-epoch containers (pack
/// records, epoch records, state-sync payloads) decode correctly at any nesting depth and
/// historical digests are preserved end to end.
///
/// PLACEHOLDER: `u32::MAX` practically never fires. Set a concrete future epoch in a dedicated
/// epoch-setting PR only after every validator and observer runs a gate-capable build. The full
/// fork schedule is logged at startup so operators can diff it across the fleet; a compile-time
/// constant that differs between binaries has no other in-protocol detection.
///
/// Arming constraint: the concrete epoch must be at least [`CONSENSUS_REGISTRY_FORK_EPOCH`]
/// (407). Committees below 407 are structurally single-worker — the deployed pre-fork registry
/// exposes no governance path that raises the worker count — so the legacy layout is lossless
/// for every epoch this gate leaves dormant. From 407 onward that guarantee becomes operational
/// rather than structural: the worker count must stay at one until this fork epoch has begun, or
/// a multi-worker committee gets written in a layout that cannot represent it.
///
/// Rollout sequence (standard hard-fork rule): deploy the gate-capable build fleet-wide first
/// (safe indefinitely while dormant, since it writes and reads the legacy layout for every epoch
/// below the constant), then land the epoch-setting PR fleet-wide before the fork epoch begins. A
/// straggler still on an old build past the boundary fails to decode post-fork committees loudly
/// and drops out rather than silently diverging.
///
/// Non-adiri builds (mainnet) have no dormant period: the multi-worker layout is active from
/// genesis and this constant does not exist there.
pub const MULTI_WORKERS_FORK_EPOCH: Epoch = u32::MAX;

/// Whether the [`Committee`](crate::Committee) of `epoch` is bcs-encoded in the multi-worker
/// layout (#554).
///
/// Gates both directions of serialization. Callers MUST pass the epoch carried inside the value
/// being encoded or decoded (the committee's own epoch, the epoch of the pack record being read),
/// never the running node's `Committee::epoch()` or any other node-local state, so that
/// historical values keep their historical layout at any nesting depth.
///
/// Adiri builds activate at [`MULTI_WORKERS_FORK_EPOCH`]; all other builds are active from
/// genesis (mainnet never carries the legacy layout). Under `test-utils`, an explicit
/// `TN_MULTI_WORKERS_FORK_EPOCH` override takes precedence over both (see
/// [`multi_workers_fork_epoch_override`]), so a test states the fork point it means rather than
/// inheriting whichever one its feature set happens to select.
#[inline]
pub fn multi_workers_fork_active(epoch: Epoch) -> bool {
    #[cfg(feature = "test-utils")]
    {
        multi_workers_fork_epoch_override()
            .map_or_else(|| multi_workers_build_fork_active(epoch), |fork| epoch >= fork)
    }
    #[cfg(not(feature = "test-utils"))]
    {
        multi_workers_build_fork_active(epoch)
    }
}

/// This build's compile-time fork point for the multi-workers fork, with no test override
/// applied.
///
/// Spelled out in full rather than sharing [`build_fork_active`] (the seed-signature gate)
/// because the two forks arm independently and must never be tied to one constant.
///
/// Unchanged from [`MULTI_WORKERS_FORK_EPOCH`]'s documented contract: adiri (testnet, which
/// carries pre-#554 packs on disk) stays dormant until the constant is lowered, and every other
/// build is active from genesis. The genesis default rests on an assumption worth stating: no
/// non-adiri network holds packs written by a pre-#554 binary, so no such build ever has to read
/// the legacy single-worker layout. A non-adiri deployment that predates #554 would need its own
/// dormant period here instead.
#[inline]
const fn multi_workers_build_fork_active(epoch: Epoch) -> bool {
    #[cfg(feature = "adiri")]
    #[expect(
        clippy::absurd_extreme_comparisons,
        reason = "MULTI_WORKERS_FORK_EPOCH is a `u32::MAX` placeholder; `>=` (not `==`) is \
                  the gate the future epoch-setting PR relies on, and this expectation flags \
                  itself for removal once that PR lowers the constant"
    )]
    {
        epoch >= MULTI_WORKERS_FORK_EPOCH
    }
    #[cfg(not(feature = "adiri"))]
    {
        let _ = epoch;
        true
    }
}

/// Test-only override of the effective multi-workers fork epoch, read once from
/// `TN_MULTI_WORKERS_FORK_EPOCH` (`4294967295` for "never fires", `0` for "active from
/// genesis").
///
/// An environment variable rather than a process-global setter because e2e tests drive real node
/// processes spawned via `TN_BIN_PATH`, which share no memory with the harness: a static would
/// silently reach only the in-process tests, and the multi-node tests that actually exercise
/// epoch close would keep inheriting the build default.
///
/// Compiled out entirely without `test-utils`, so a production binary keeps the compile-time
/// constant and cannot be repointed at runtime by its environment. An unparseable value is
/// ignored rather than defaulted, leaving the build's own fork point in force.
#[cfg(feature = "test-utils")]
pub fn multi_workers_fork_epoch_override() -> Option<Epoch> {
    static OVERRIDE: std::sync::OnceLock<Option<Epoch>> = std::sync::OnceLock::new();
    *OVERRIDE.get_or_init(|| {
        std::env::var("TN_MULTI_WORKERS_FORK_EPOCH").ok().and_then(|raw| raw.trim().parse().ok())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{address, keccak256};

    /// The `ConsensusRegistry` build artifact whose `deployedBytecode.object` the fork boundary
    /// swaps in.
    ///
    /// Re-embedded from the tn-contracts submodule rather than read through `tn-config`'s
    /// `CONSENSUS_REGISTRY_JSON`, because tn-config depends on tn-types and the reverse edge would
    /// be circular — the same reason the pre-fork tests below hardcode their contract addresses.
    /// It is the identical file, so the bytes are identical by construction; keep this path in
    /// step with `tn-config::genesis` if that constant is ever repointed.
    const CONSENSUS_REGISTRY_ARTIFACT_JSON: &str =
        include_str!("../../../tn-contracts/artifacts/ConsensusRegistry.json");

    /// The `WorkerConfigs` build artifact whose `deployedBytecode.object` the fork boundary
    /// splices in. Embedded on the same terms as [`CONSENSUS_REGISTRY_ARTIFACT_JSON`].
    const WORKER_CONFIGS_ARTIFACT_JSON: &str =
        include_str!("../../../tn-contracts/artifacts/WorkerConfigs.json");

    /// Pin [`CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`] to the registry code committed in
    /// `chain-configs/testnet/genesis.yaml`.
    ///
    /// Unconditional (not `adiri`-gated) so it runs in default-feature CI even though the fork
    /// machinery consuming the constant is `adiri`-only.
    #[test]
    fn test_pre_fork_consensus_registry_code_hash_pinned() {
        let genesis = crate::adiri_genesis();
        // `tn-reth::system_calls::CONSENSUS_REGISTRY_ADDRESS`, hardcoded because tn-types cannot
        // depend on tn-reth.
        let registry = address!("0x07E17e17E17e17E17e17E17E17E17e17e17E17e1");
        let code = genesis
            .alloc
            .get(&registry)
            .and_then(|account| account.code.as_ref())
            .expect("testnet genesis must allocate ConsensusRegistry runtime code");
        assert_eq!(
            keccak256(code),
            CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH,
            "CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH mirrors the LIVE adiri deployment — do not \
             blindly update this constant to make the test pass; if genesis.yaml was regenerated, \
             reassess the fork plan and `CONSENSUS_REGISTRY_FORK_EPOCH` first",
        );
    }

    /// Pin the seed-signature gate to the rollout contract this build actually implements.
    ///
    /// #1032 was reviewed on the claim that the gate is dormant before
    /// [`SEED_SIGNATURE_FORK_EPOCH`] (then the `u32::MAX` placeholder, now a concrete epoch).
    /// That holds only under `adiri`. Every other build — including the default
    /// one that produces both the shipped node binary and the e2e binary — is active from
    /// genesis, so epoch 1 takes the post-fork anchor path in production. Nothing
    /// asserted either half, so the "dormant everywhere" reading survived review; this states
    /// it outright so no future reader infers dormancy from the constant alone.
    ///
    /// Asserts against [`build_fork_active`], the override-free decision, so the result does
    /// not depend on whether `test-utils` was unified into this build.
    #[test]
    fn build_fork_gate_matches_this_builds_rollout_contract() {
        #[cfg(not(feature = "adiri"))]
        [0, 1, 2, u32::MAX].into_iter().for_each(|epoch| {
            assert!(
                build_fork_active(epoch),
                "non-adiri builds carry no legacy layout and are active from genesis; epoch \
                 {epoch} must be post-fork",
            );
        });
        #[cfg(feature = "adiri")]
        {
            [0, 1, 2, SEED_SIGNATURE_FORK_EPOCH - 1].into_iter().for_each(|epoch| {
                assert!(
                    !build_fork_active(epoch),
                    "adiri stays dormant before SEED_SIGNATURE_FORK_EPOCH; epoch {epoch} must \
                     be pre-fork",
                );
            });
            [SEED_SIGNATURE_FORK_EPOCH, u32::MAX].into_iter().for_each(|epoch| {
                assert!(
                    build_fork_active(epoch),
                    "the gate must fire from the fork epoch onward (`>=`, not `>`); epoch \
                     {epoch} must be post-fork",
                );
            });
        }
    }

    /// With no `TN_SEED_SIGNATURE_FORK_EPOCH` in the environment, the test override must be
    /// completely inert: the gate answers exactly as the compile-time contract does.
    #[cfg(feature = "test-utils")]
    #[test]
    fn seed_signature_override_is_inert_when_unset() {
        // The override latches in a process-wide `OnceLock`, so a harness launched WITH the
        // variable set cannot observe the unset behaviour. Fail loudly rather than assert a
        // property this process cannot hold — a silent skip here would read as a pass.
        assert!(
            seed_signature_fork_epoch_override().is_none(),
            "this test requires a process without TN_SEED_SIGNATURE_FORK_EPOCH set; the \
             override is OnceLock-latched, so run the unset case in its own process",
        );
        [0, 1, 2, u32::MAX].into_iter().for_each(|epoch| {
            assert_eq!(
                seed_signature_active(epoch),
                build_fork_active(epoch),
                "an unset override must not shift the gate at epoch {epoch}",
            );
        });
    }

    /// Pin the PREVRANDAO gate to the rollout contract this build actually implements,
    /// mirroring [`build_fork_gate_matches_this_builds_rollout_contract`]: non-adiri builds
    /// are active from genesis, adiri stays dormant before [`PREVRANDAO_FORK_EPOCH`] and
    /// fires from it (`>=`, not `>`).
    ///
    /// Asserts against [`prevrandao_build_fork_active`], the override-free decision, so the
    /// result does not depend on whether `test-utils` was unified into this build.
    #[test]
    fn prevrandao_build_fork_gate_matches_this_builds_rollout_contract() {
        #[cfg(not(feature = "adiri"))]
        [0, 1, 2, u32::MAX].into_iter().for_each(|epoch| {
            assert!(
                prevrandao_build_fork_active(epoch),
                "non-adiri builds carry no pre-fork executed history and are active from \
                 genesis; epoch {epoch} must be post-fork",
            );
        });
        #[cfg(feature = "adiri")]
        {
            [0, 1, 2, PREVRANDAO_FORK_EPOCH - 1].into_iter().for_each(|epoch| {
                assert!(
                    !prevrandao_build_fork_active(epoch),
                    "adiri stays dormant before PREVRANDAO_FORK_EPOCH; epoch {epoch} must \
                     be pre-fork",
                );
            });
            [PREVRANDAO_FORK_EPOCH, u32::MAX].into_iter().for_each(|epoch| {
                assert!(
                    prevrandao_build_fork_active(epoch),
                    "the gate must fire from the fork epoch onward (`>=`, not `>`); epoch \
                     {epoch} must be post-fork",
                );
            });
        }
    }

    /// With no `TN_PREVRANDAO_FORK_EPOCH` in the environment, the test override must be
    /// completely inert: the fork point answers exactly as the compile-time contract does.
    #[cfg(feature = "test-utils")]
    #[test]
    fn prevrandao_override_is_inert_when_unset() {
        // The override latches in a process-wide `OnceLock`, so a harness launched WITH the
        // variable set cannot observe the unset behaviour. Fail loudly rather than assert a
        // property this process cannot hold; a silent skip here would read as a pass.
        assert!(
            prevrandao_fork_epoch_override().is_none(),
            "this test requires a process without TN_PREVRANDAO_FORK_EPOCH set; the \
             override is OnceLock-latched, so run the unset case in its own process",
        );
        [0, 1, 2, u32::MAX].into_iter().for_each(|epoch| {
            assert_eq!(
                prevrandao_fork_point_active(epoch),
                prevrandao_build_fork_active(epoch),
                "an unset override must not shift the fork point at epoch {epoch}",
            );
        });
    }

    /// Pin the multi-workers gate to the rollout contract this build actually implements.
    ///
    /// Carries the same asymmetry [`build_fork_gate_matches_this_builds_rollout_contract`] states
    /// for the seed-signature gate: "dormant while the constant is `u32::MAX`" holds only under
    /// `adiri`. Every other build — including the default one that produces both the shipped node
    /// binary and the e2e binary — is active from genesis, so epoch 1 already uses the
    /// multi-worker layout there.
    ///
    /// Asserts against [`multi_workers_build_fork_active`], the override-free decision, so the
    /// result does not depend on whether `test-utils` was unified into this build. The grid is
    /// derived from the constant, so arming the fork does not require editing this test.
    #[test]
    fn multi_workers_build_fork_gate_matches_this_builds_rollout_contract() {
        #[cfg(not(feature = "adiri"))]
        [0, 1, 2, u32::MAX].into_iter().for_each(|epoch| {
            assert!(
                multi_workers_build_fork_active(epoch),
                "non-adiri builds carry no legacy committee layout and are active from genesis; \
                 epoch {epoch} must be post-fork",
            );
        });
        #[cfg(feature = "adiri")]
        {
            [0, 1, 2, MULTI_WORKERS_FORK_EPOCH - 1].into_iter().for_each(|epoch| {
                assert!(
                    !multi_workers_build_fork_active(epoch),
                    "adiri stays dormant before MULTI_WORKERS_FORK_EPOCH; epoch {epoch} must be \
                     pre-fork",
                );
            });
            [MULTI_WORKERS_FORK_EPOCH, u32::MAX].into_iter().for_each(|epoch| {
                assert!(
                    multi_workers_build_fork_active(epoch),
                    "the gate must fire from the fork epoch onward (`>=`, not `>`); epoch \
                     {epoch} must be post-fork",
                );
            });
        }
    }

    /// With no `TN_MULTI_WORKERS_FORK_EPOCH` in the environment, the test override must be
    /// completely inert: the gate answers exactly as the compile-time contract does.
    #[cfg(feature = "test-utils")]
    #[test]
    fn multi_workers_override_is_inert_when_unset() {
        // The override latches in a process-wide `OnceLock`, so a harness launched WITH the
        // variable set cannot observe the unset behaviour. Fail loudly rather than assert a
        // property this process cannot hold — a silent skip here would read as a pass.
        assert!(
            multi_workers_fork_epoch_override().is_none(),
            "this test requires a process without TN_MULTI_WORKERS_FORK_EPOCH set; the override \
             is OnceLock-latched, so run the unset case in its own process",
        );
        [0, 1, 2, u32::MAX].into_iter().for_each(|epoch| {
            assert_eq!(
                multi_workers_fork_active(epoch),
                multi_workers_build_fork_active(epoch),
                "an unset override must not shift the gate at epoch {epoch}",
            );
        });
    }

    /// Pin [`WORKER_CONFIGS_PRE_FORK_CODE_HASH`] to the worker-configs code committed in
    /// `chain-configs/testnet/genesis.yaml`.
    ///
    /// Unconditional (not `adiri`-gated) so it runs in default-feature CI even though the fork
    /// machinery consuming the constant is `adiri`-only.
    #[test]
    fn test_pre_fork_worker_configs_code_hash_pinned() {
        let genesis = crate::adiri_genesis();
        // `tn-config::WORKER_CONFIGS_ADDRESS`, hardcoded because tn-config depends on tn-types
        // and the reverse edge would be circular.
        let worker_configs = address!("0xFee0FEe0fee0fEE0FEe0fee0FEE0fEe0feE0FEe0");
        let code = genesis
            .alloc
            .get(&worker_configs)
            .and_then(|account| account.code.as_ref())
            .expect("testnet genesis must allocate WorkerConfigs runtime code");
        assert_eq!(
            keccak256(code),
            WORKER_CONFIGS_PRE_FORK_CODE_HASH,
            "WORKER_CONFIGS_PRE_FORK_CODE_HASH mirrors the LIVE adiri deployment — do not \
             blindly update this constant to make the test pass; if genesis.yaml was regenerated, \
             reassess the fork plan and `CONSENSUS_REGISTRY_FORK_EPOCH` first",
        );
    }

    /// Pin [`CONSENSUS_REGISTRY_POST_FORK_CODE_HASH`] to the embedded `ConsensusRegistry.json`
    /// artifact the fork boundary swaps in.
    ///
    /// Unconditional (not `adiri`-gated) so it runs in default-feature CI even though the fork
    /// machinery consuming the constant is `adiri`-only.
    #[test]
    fn test_post_fork_consensus_registry_code_hash_pinned() {
        assert_eq!(
            deployed_bytecode_hash(CONSENSUS_REGISTRY_ARTIFACT_JSON),
            CONSENSUS_REGISTRY_POST_FORK_CODE_HASH,
            "the embedded ConsensusRegistry artifact drifted from the pinned post-fork code hash \
             — do not blindly update this constant to make the test pass; these are the bytes the \
             fork boundary swaps in, so once the fork has run live, shipping a different artifact \
             breaks historical state roots. Reassess the tn-contracts submodule pin first",
        );
    }

    /// Pin [`WORKER_CONFIGS_POST_FORK_CODE_HASH`] to the embedded `WorkerConfigs.json` artifact
    /// the fork boundary splices in.
    ///
    /// Unconditional (not `adiri`-gated) so it runs in default-feature CI even though the fork
    /// machinery consuming the constant is `adiri`-only.
    #[test]
    fn test_post_fork_worker_configs_code_hash_pinned() {
        assert_eq!(
            deployed_bytecode_hash(WORKER_CONFIGS_ARTIFACT_JSON),
            WORKER_CONFIGS_POST_FORK_CODE_HASH,
            "the embedded WorkerConfigs artifact drifted from the pinned post-fork code hash — do \
             not blindly update this constant to make the test pass; these are the bytes the fork \
             boundary splices in, so once the fork has run live, shipping a different artifact \
             breaks historical state roots. Reassess the tn-contracts submodule pin first",
        );
    }

    /// Keccak-256 of an artifact's hex-encoded `deployedBytecode.object`.
    ///
    /// Reproduces what the fork boundary pins against: `Bytecode::new_raw(bytes).hash_slow()` over
    /// these same decoded bytes, which for raw (unanalyzed) legacy bytecode is plain keccak-256 of
    /// the runtime code — the same value the EVM stores as the account's `code_hash`.
    ///
    /// Decoded through `alloy::hex` (not the `hex` crate) to match the runtime loaders in
    /// `tn-reth::evm::block` byte for byte: the artifact's object string is `0x`-prefixed and only
    /// alloy's decoder strips that prefix.
    fn deployed_bytecode_hash(artifact_json: &str) -> B256 {
        let artifact: serde_json::Value =
            serde_json::from_str(artifact_json).expect("embedded artifact json is valid");
        let hex_str = artifact["deployedBytecode"]["object"]
            .as_str()
            .expect("artifact deployedBytecode.object is a string");
        keccak256(
            alloy::hex::decode(hex_str).expect("artifact deployedBytecode.object is valid hex"),
        )
    }
}
