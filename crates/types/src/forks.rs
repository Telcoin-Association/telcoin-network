//! Code to support various chain forks.

use crate::Epoch;
use alloy::primitives::{address, b256, Address, B256};

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
/// [`SEED_SIGNATURE_FORK_EPOCH`], this constant does not exist there).
///
/// The full fork schedule is logged at startup so operators can diff it across the fleet; a
/// compile-time constant that differs between binaries has no other in-protocol detection.
///
/// Accepted residual (#1247): the seed chain closes payload grinding, but the committing
/// leader can still compute every `PREVRANDAO` its commit will produce before broadcasting
/// and withhold the proposal if it dislikes them . . . one propose-or-withhold choice per
/// commit. This fork promotes that bias into an opcode contracts can read; contracts that
/// need unbiasable randomness must not use `PREVRANDAO` alone.
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

#[cfg(feature = "adiri")]
/// First epoch whose committed sub-DAGs order same-round certificates by the leader-seeded
/// tie-break in `order_dag` (#1260).
///
/// Within a round, the legacy commit order is the DFS discovery order, which is derived from
/// the digest-ordered parent sets of the sub-DAG. A header digest is a blake3 hash over
/// proposer-controlled fields (the `payload` insertion order and `created_at`), so a Byzantine
/// proposer can grind its own digest to steer its certificate toward either end of its round,
/// and that order reaches execution with no re-sort. Post-fork, `order_dag` keys the
/// intra-round order on `blake3(domain || seed || certificate_digest)` instead, where `seed`
/// is the epoch seed chain value the commit folds into `CommittedSubDag::randomness`: the
/// chain value is fixed by the previous published commit and the leader's contribution is its
/// deterministic BLS seed signature, so no proposer, the committing leader included, can
/// enumerate candidate orders after observing the sub-DAG, while the order stays a pure
/// function of committed data that every honest node derives identically. The accepted
/// residual is the seed chain's own propose-or-withhold last-actor bias; `order_dag`'s
/// `intra_round_key` documents it.
///
/// The gate ([`leader_seeded_ordering_active`]) always reads the epoch carried inside the
/// committed leader, never node-local committee state, so replay of a historical commit
/// reproduces the historical execution sequence.
///
/// # Arming constraints (compile-time asserted below)
///
/// - **At or above [`SEED_SIGNATURE_FORK_EPOCH`].** The seeded key folds the leader's seed
///   signature into the epoch seed chain, which only carries digest-pinned values once the seed
///   fork is active. [`leader_seeded_ordering_active`] additionally conjoins
///   [`seed_signature_active`] fail-closed, so a mis-ordered schedule stays on the legacy order
///   instead of keying commits on a forkable value.
/// - **Strictly above [`ADIRI_DUP_BATCH_EPOCH`].** Duplicate-batch attribution is
///   intra-round-position-sensitive for adiri epochs at or below that cutoff: a batch referenced by
///   two headers is credited to whichever header comes first in sequence and dropped from the
///   second (`subscriber.rs`, mirrored in `consensus_pack.rs`). This fork permutes exactly that
///   sequence, so arming it at or below the cutoff would change replayed attribution on adiri, a
///   resync divergence rather than just a reordering. The epoch-setting PR is the place this bites,
///   and that PR will not be looking at the dup-batch interaction; the assert makes it look.
///
/// PLACEHOLDER: `u32::MAX` practically never fires. Set a concrete future epoch in a dedicated
/// epoch-setting PR only after every validator and observer runs a gate-capable build. The full
/// fork schedule is logged at startup so operators can diff it across the fleet; a compile-time
/// constant that differs between binaries has no other in-protocol detection.
///
/// Rollout sequence (standard hard-fork rule): deploy the gate-capable build fleet-wide first
/// (safe indefinitely while dormant, since the legacy order stays in force for every epoch
/// below the constant), then land the epoch-setting PR fleet-wide before the fork epoch
/// begins. A straggler still on an old build past the boundary orders the same certificates
/// differently, executes them in a different sequence, and forks away from the upgraded fleet
/// at its next commit. That divergence is loud (its executed state stops matching the fleet's)
/// but it is a fork, not a decode error, so the fleet must be fully upgraded before the epoch
/// is armed.
///
/// Non-adiri builds (mainnet) have no dormant period: the seeded order is active from genesis
/// and this constant does not exist there.
pub const LEADER_SEEDED_ORDERING_FORK_EPOCH: Epoch = u32::MAX;

/// Compile-time enforcement of the first arming constraint documented on
/// [`LEADER_SEEDED_ORDERING_FORK_EPOCH`]: a rollout PR that sets this fork below the seed
/// fork fails to compile instead of shipping a gate that silently stays dormant until the
/// seed fork fires (the [`leader_seeded_ordering_active`] conjunct).
#[cfg(feature = "adiri")]
#[expect(
    clippy::absurd_extreme_comparisons,
    reason = "always true only while LEADER_SEEDED_ORDERING_FORK_EPOCH is the `u32::MAX` \
              placeholder; once the rollout PR lowers the constant the comparison becomes \
              live and this expectation flags itself for removal"
)]
const _: () = assert!(LEADER_SEEDED_ORDERING_FORK_EPOCH >= SEED_SIGNATURE_FORK_EPOCH);

/// Compile-time enforcement of the second arming constraint documented on
/// [`LEADER_SEEDED_ORDERING_FORK_EPOCH`]: arming the fork at or below
/// [`ADIRI_DUP_BATCH_EPOCH`] would permute duplicate-batch attribution on replay (the
/// same relation `consensus_pack.rs` pins for its shared-batch scenarios).
#[cfg(feature = "adiri")]
const _: () = assert!(LEADER_SEEDED_ORDERING_FORK_EPOCH > ADIRI_DUP_BATCH_EPOCH);

/// Whether the committed sub-DAG of a leader of `epoch` orders same-round certificates by the
/// seeded tie-break `blake3(domain || seed || certificate_digest)` instead of the legacy
/// DFS discovery order (#1260).
///
/// Gates the linearization in `order_dag`. Callers MUST pass the epoch carried inside the
/// committed leader (`leader.epoch()`), never `Committee::epoch()` or other node-local state,
/// so that a replayed historical commit keeps its historical order.
///
/// Requires [`seed_signature_active`] as a fail-closed conjunct, exactly as
/// [`prevrandao_seed_active`] does: the seeded key folds the leader's seed signature into the
/// epoch seed chain, which is digest-pinned only once the seed fork is active. If an override
/// or a future fork schedule orders the two forks the other way, the gate stays on the legacy
/// order (the status quo) instead of leaving `order_dag` to degrade on a missing signature.
///
/// Adiri builds activate at [`LEADER_SEEDED_ORDERING_FORK_EPOCH`]; all other builds are active
/// from genesis (mainnet never produced a legacy-ordered commit). Under `test-utils`, an
/// explicit `TN_LEADER_SEEDED_ORDERING_FORK_EPOCH` override takes precedence over both (see
/// [`leader_seeded_ordering_fork_epoch_override`]), so a test states the fork point it means
/// rather than inheriting whichever one its feature set happens to select.
#[inline]
pub fn leader_seeded_ordering_active(epoch: Epoch) -> bool {
    seed_signature_active(epoch) && leader_seeded_ordering_fork_point_active(epoch)
}

/// This build's effective leader-seeded-ordering fork point (any `test-utils` override
/// applied), without the [`seed_signature_active`] conjunct [`leader_seeded_ordering_active`]
/// enforces.
#[inline]
fn leader_seeded_ordering_fork_point_active(epoch: Epoch) -> bool {
    #[cfg(feature = "test-utils")]
    {
        leader_seeded_ordering_fork_epoch_override()
            .map_or_else(|| leader_seeded_ordering_build_fork_active(epoch), |fork| epoch >= fork)
    }
    #[cfg(not(feature = "test-utils"))]
    {
        leader_seeded_ordering_build_fork_active(epoch)
    }
}

/// This build's compile-time fork point for the leader-seeded-ordering fork, with no test
/// override applied.
///
/// Spelled out in full rather than sharing another gate's helper because the forks arm
/// independently and must never be tied to one constant.
///
/// Unchanged from [`LEADER_SEEDED_ORDERING_FORK_EPOCH`]'s documented contract: adiri (testnet,
/// which carries legacy-ordered commits in its history) stays dormant until the constant is
/// lowered, and every other build is active from genesis.
#[inline]
const fn leader_seeded_ordering_build_fork_active(epoch: Epoch) -> bool {
    #[cfg(feature = "adiri")]
    #[expect(
        clippy::absurd_extreme_comparisons,
        reason = "LEADER_SEEDED_ORDERING_FORK_EPOCH is a `u32::MAX` placeholder; `>=` (not \
                  `==`) is the gate the future epoch-setting PR relies on, and this expectation \
                  flags itself for removal once that PR lowers the constant"
    )]
    {
        epoch >= LEADER_SEEDED_ORDERING_FORK_EPOCH
    }
    #[cfg(not(feature = "adiri"))]
    {
        let _ = epoch;
        true
    }
}

/// Test-only override of the effective leader-seeded-ordering fork epoch, read once from
/// `TN_LEADER_SEEDED_ORDERING_FORK_EPOCH` (`4294967295` for "never fires", `0` for "active
/// from genesis").
///
/// An environment variable rather than a process-global setter because e2e tests drive real
/// node processes spawned via `TN_BIN_PATH`, which share no memory with the harness: a static
/// would silently reach only the in-process tests.
///
/// Compiled out entirely without `test-utils`, so a production binary keeps the compile-time
/// constant and cannot be repointed at runtime by its environment. An unparseable value is
/// ignored rather than defaulted, leaving the build's own fork point in force.
#[cfg(feature = "test-utils")]
pub fn leader_seeded_ordering_fork_epoch_override() -> Option<Epoch> {
    static OVERRIDE: std::sync::OnceLock<Option<Epoch>> = std::sync::OnceLock::new();
    *OVERRIDE.get_or_init(|| {
        std::env::var("TN_LEADER_SEEDED_ORDERING_FORK_EPOCH")
            .ok()
            .and_then(|raw| raw.trim().parse().ok())
    })
}

/// Every `test-utils` fork-epoch override actually in force in this process, for the startup
/// fork-schedule log (#1086).
///
/// Callable from any build so the CLI logs the effective schedule without having to know whether
/// `tn-types`' `test-utils` was unified into the binary it is compiled into. Always empty in a
/// production binary, where the overrides are compiled out and no environment variable can
/// repoint a fork; empty in a `test-utils` build too when nothing is exported. Without this the
/// startup line reports the compiled constants only, and a `test-utils` binary whose forks its
/// harness pinned elsewhere (the e2e harness holds all but the leader-seeded fork dormant) logs
/// "active from genesis" while executing the legacy derivations — the shape of a `mix_hash`
/// mismatch that is otherwise invisible in the logs.
///
/// Only variables that parsed are listed, so an entry means "pinned here", absence means "using
/// this build's own fork point". A row is carried here under the same cfg as the gate that
/// consumes it, so absence also covers a fork this build cannot honor at all: reporting one would
/// name a pin nothing reads. [`governance_safe_fork_epoch`] is `adiri`-only, so its row is too,
/// and a non-adiri `test-utils` binary inheriting `TN_GOVERNANCE_SAFE_FORK_EPOCH` from a Makefile
/// lane stays silent about it rather than warn-logging a schedule change that never happens. Every
/// entry is therefore genuinely in force on the build that printed it. Values latch on first read
/// like the individual overrides do.
pub fn fork_epoch_overrides() -> Vec<(&'static str, Epoch)> {
    #[cfg(feature = "test-utils")]
    {
        // Carried under its consumer's cfg per the paragraph above. An attribute cannot sit on an
        // array element, so the row joins the unconditional ones as a chained `Option`.
        #[cfg(feature = "adiri")]
        let governance_safe =
            Some(("TN_GOVERNANCE_SAFE_FORK_EPOCH", governance_safe_fork_epoch_override()));
        #[cfg(not(feature = "adiri"))]
        let governance_safe: Option<(&'static str, Option<Epoch>)> = None;

        [
            ("TN_SEED_SIGNATURE_FORK_EPOCH", seed_signature_fork_epoch_override()),
            ("TN_PREVRANDAO_FORK_EPOCH", prevrandao_fork_epoch_override()),
            ("TN_MULTI_WORKERS_FORK_EPOCH", multi_workers_fork_epoch_override()),
            ("TN_LEADER_SEEDED_ORDERING_FORK_EPOCH", leader_seeded_ordering_fork_epoch_override()),
        ]
        .into_iter()
        .chain(governance_safe)
        .filter_map(|(var, fork_epoch)| fork_epoch.map(|fork_epoch| (var, fork_epoch)))
        .collect()
    }
    #[cfg(not(feature = "test-utils"))]
    Vec::new()
}

/// Keccak-256 hash of the governance Safe proxy runtime bytecode deployed on the live adiri
/// testnet (the `0x…07a0` account's `code` in the committed `chain-configs/testnet/genesis.yaml`
/// — an 81-byte solc-0.8.26 recompile of `SafeProxy`, not the canonical 171-byte build).
///
/// Pins the code the [`GOVERNANCE_SAFE_FORK_EPOCH`] migration expects to find at the governance
/// address. The fork rewrites the proxy's singleton slot (slot 0) and fallback-handler slot in
/// place; doing that over any other deployment risks corrupting an unknown layout, so the
/// migration fails closed unless the on-chain code hashes to this value AND slot 0 still holds
/// the pre-fork L1 `Safe` singleton.
///
/// Unconditional (not `adiri`-gated) so the pin test guarding it runs in default-feature CI.
pub const GOVERNANCE_SAFE_PROXY_PRE_FORK_CODE_HASH: B256 =
    b256!("0xfe74fcea823036dfc874205a4198185eedae92b256d956cbf805c6c0dc2fd184");

/// Keccak-256 hash of the pre-fork `Safe` singleton runtime bytecode deployed on the live adiri
/// testnet at the canonical `0x41675C09…` address (a 12,180-byte solc-0.8.26 recompile; the
/// canonical v1.4.1 build is 8,640 bytes of solc-0.7.6 output).
///
/// Pins the code the [`GOVERNANCE_SAFE_FORK_EPOCH`] swap expects to find before replacing it
/// with the canonical bytes: the code-only swap preserves the account's storage (`threshold = 1`
/// from the recompiled constructor), which is only sound over the pinned layout — Safe v1.4.1
/// storage is identical between the recompile and the canonical build, but an unknown deployment
/// gets no such guarantee, so the swap fails closed on any other hash.
///
/// Unconditional (not `adiri`-gated) so the pin test guarding it runs in default-feature CI.
pub const SAFE_SINGLETON_PRE_FORK_CODE_HASH: B256 =
    b256!("0xcebd258f55cc264ff411b2797a0a1609764f47076a34cef429264a3e2ea96b77");

/// Keccak-256 hash of the pre-fork `SafeProxyFactory` runtime bytecode deployed on the live
/// adiri testnet at the canonical `0x4e1DCf7A…` address (a solc-0.8.26 recompile).
///
/// The recompiled factory is the reason counterfactual Safe creations land at non-canonical
/// addresses on adiri: `createProxyWithNonce` derives the proxy address via CREATE2 over the
/// factory's **embedded proxy creation code**, and the recompile embeds different bytes than
/// every other chain's canonical deployment. The [`GOVERNANCE_SAFE_FORK_EPOCH`] swap replaces it
/// with the canonical bytes, restoring cross-chain address parity for every Safe created after
/// the boundary; proxies the recompiled factory already created (their addresses and code) are
/// untouched. Fails closed on any other hash.
///
/// Unconditional (not `adiri`-gated) so the pin test guarding it runs in default-feature CI.
pub const SAFE_PROXY_FACTORY_PRE_FORK_CODE_HASH: B256 =
    b256!("0x7c62c68777c4f5d3a736cabf479a12f2fc043ef7016fad20da5a9bb0c68433fc");

/// The canonical Safe v1.4.1 suite the [`GOVERNANCE_SAFE_FORK_EPOCH`] boundary installs:
/// `(vendored-file stem, canonical cross-chain address, keccak-256 of the runtime bytecode)`.
///
/// One row per contract in mainnet genesis' Safe suite — the full 12-contract
/// safe-deployments v1.4.1 registry plus the Safe Singleton Factory — sourced from
/// `tn-contracts/deployments/genesis/canonical-bytecode/` (provenance and per-file hashes in
/// its README; the file stem names the vendored `<stem>.hex`). `tn-reth`'s fork machinery
/// embeds those files and refuses to etch any byte string that does not hash to its row here,
/// and after the fork has run live these values carry the replay constraint documented on
/// [`CONSENSUS_REGISTRY_POST_FORK_CODE_HASH`]: re-executing the boundary must install these
/// exact bytes, so a tn-contracts bump that changes a vendored file is caught by the pin test
/// instead of breaking historical state roots.
///
/// Unconditional (not `adiri`-gated) so the pin test guarding it runs in default-feature CI.
pub const GOVERNANCE_SAFE_FORK_CANONICAL_SUITE: [(&str, Address, B256); 13] = [
    (
        "Safe",
        address!("0x41675C099F32341bf84BFc5382aF534df5C7461a"),
        b256!("0x1fe2df852ba3299d6534ef416eefa406e56ced995bca886ab7a553e6d0c5e1c4"),
    ),
    (
        "SafeL2",
        address!("0x29fcB43b46531BcA003ddC8FCB67FFE91900C762"),
        b256!("0xb1f926978a0f44a2c0ec8fe822418ae969bd8c3f18d61e5103100339894f81ff"),
    ),
    (
        "SafeProxyFactory",
        address!("0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67"),
        b256!("0x50c3cdc4074750a7a974204a716c999edd37482f907608d960b2b025ee0b3317"),
    ),
    (
        "CompatibilityFallbackHandler",
        address!("0xfd0732Dc9E303f09fCEf3a7388Ad10A83459Ec99"),
        b256!("0x7c6007a5d711cea8dfd5d91f5940ec29c7f200fe511eb1fc1397b367af3c42f9"),
    ),
    (
        "SafeToL2Setup",
        address!("0xBD89A1CE4DDe368FFAB0eC35506eEcE0b1fFdc54"),
        b256!("0x2f25df28caf984366ee584e13241707e85dcd5a6ea0c14267928dafc1fd6274b"),
    ),
    (
        "MultiSend",
        address!("0x38869bf66a61cF6bDB996A6aE40D5853Fd43B526"),
        b256!("0x0e4f7fc66550a322d1e7688e181b75e217e662a4f3f4d6a29b22bc61217c4b77"),
    ),
    (
        "MultiSendCallOnly",
        address!("0x9641d764fc13c8B624c04430C7356C1C7C8102e2"),
        b256!("0xecd5bd14a08c5d2122379900b2f272bdf107a7e92423c10dd5fe3254386c9939"),
    ),
    (
        "SignMessageLib",
        address!("0xd53cd0aB83D845Ac265BE939c57F53AD838012c9"),
        b256!("0x525c754a46b79e05543a59bb61e8de3c9eee0d955a59352409cbe67ea1077528"),
    ),
    (
        "CreateCall",
        address!("0x9b35Af71d77eaf8d7e40252370304687390A1A52"),
        b256!("0x2b3060c55fcb8275653e99ad511a71f67ba76934ed66a7d74d6e68b52afff889"),
    ),
    (
        "SimulateTxAccessor",
        address!("0x3d4BA2E0884aa488718476ca2FB8Efc291A46199"),
        b256!("0x91f82615581fc73b190b83d72e883608b25e392f72322035df1b13d51766cf8d"),
    ),
    (
        "SafeSingletonFactory",
        address!("0x914d7Fec6aaC8cd542e72Bca78B30650d45643d7"),
        b256!("0x2fa86add0aed31f33a762c9d88e807c475bd51d0f52bd0955754b2608f7e4989"),
    ),
    (
        "SafeMigration",
        address!("0x526643F69b81B008F46d95CD5ced5eC0edFFDaC6"),
        b256!("0xc00d7921460cd5a05393e7772e634bd7d212f356356aa3a77f0120a9b8e25e99"),
    ),
    (
        "SafeToL2Migration",
        address!("0xfF83F6335d8930cBad1c0D439A841f01888D9f69"),
        b256!("0xa83e7be2fa20c96dc9575e3937239d552f3831ea437d7c96397eec8736f0cba0"),
    ),
];

/// The canonical address of the named [`GOVERNANCE_SAFE_FORK_CANONICAL_SUITE`] row, or `None`
/// when the suite carries no such contract.
///
/// Row order in the table is load-bearing — `tn-reth` zips it against the vendored bytecode
/// list, so the two must stay positionally aligned — which makes an index the wrong handle for
/// callers that mean one *specific* contract. They resolve it by name here, the same way the
/// fork's installer keys its per-contract special cases (pre-fork pins, the SafeL2 threshold
/// seed) off the row name.
pub fn governance_safe_fork_canonical_address(name: &str) -> Option<Address> {
    GOVERNANCE_SAFE_FORK_CANONICAL_SUITE
        .iter()
        .find_map(|(row, address, _)| (row == &name).then_some(*address))
}

#[cfg(feature = "adiri")]
/// First epoch that begins with the canonical Safe v1.4.1 suite installed and the governance
/// Safe migrated onto `SafeL2`.
///
/// The epoch-closing block that concludes `GOVERNANCE_SAFE_FORK_EPOCH - 1` fires
/// `tn-reth::evm::block::apply_governance_safe_fork` exactly once (one-shot `==` trigger, the
/// same shape as [`CONSENSUS_REGISTRY_FORK_EPOCH`]), bringing live adiri's Safe stack to parity
/// with mainnet genesis:
/// - **etch** the eleven [`GOVERNANCE_SAFE_FORK_CANONICAL_SUITE`] contracts adiri lacks (SafeL2 +
///   fallback handler + libraries + both migration helpers + the singleton factory), forcing the
///   canonical bytes over whatever the address holds — empty on the live chain, already canonical
///   if someone deployed the suite through the singleton factory, and an unknown occupant only at
///   `warn!` rather than a fleet-wide abort;
/// - **swap** the two recompiled deployments — the `Safe` singleton and the `SafeProxyFactory` — to
///   the canonical bytes, each gated fail-closed on its pre-fork pin
///   ([`SAFE_SINGLETON_PRE_FORK_CODE_HASH`], [`SAFE_PROXY_FACTORY_PRE_FORK_CODE_HASH`]), preserving
///   balance, nonce, and all storage;
/// - **migrate** the governance Safe proxy: slot 0 (singleton) from the L1 `Safe` to `SafeL2` and
///   the fallback-handler slot from unset to the canonical `CompatibilityFallbackHandler`, gated
///   fail-closed on [`GOVERNANCE_SAFE_PROXY_PRE_FORK_CODE_HASH`] and on slot 0 still holding the L1
///   singleton. Owners, threshold, the Safe nonce, and the TEL balance are untouched.
///
/// Why a protocol fork instead of a governance transaction: the sanctioned in-Safe path,
/// `SafeToL2Migration.migrateToL2`, requires the Safe's storage nonce to be exactly 1 (its
/// `onlyNonceZero` guard runs after `execTransaction` increments), i.e. it must be the Safe's
/// first transaction ever. Adiri governance sits at nonce 2, so no transaction it can ever sign
/// performs the migration; `SafeMigration.migrateL2Singleton` has no nonce guard but leaves the
/// missing suite and the recompiled factory in place. The fork does the whole job atomically
/// behind the gates above — fail-closed wherever a pin vouches for a storage layout the write
/// preserves (every gate is a pure function of committed state, so the fleet passes or aborts in
/// lockstep).
///
/// Scope: adiri-only, like every constant in this family — mainnet genesis already carries the
/// full canonical suite with governance on SafeL2, so non-adiri builds exclude the mechanism
/// entirely.
///
/// Armed for adiri (chain 2017) at epoch 554, so the migration executes one boundary earlier, in
/// the epoch-closing block of 553. Measured boundary: Fri 2026-09-25 08:32 UTC (03:32 CDT).
/// Derived by binary-searching first-block timestamps over epochs 536→544 on the live chain (mean
/// epoch length 21,602 s, σ 58 s, so about 6.001 h per epoch), snapshotted at epoch 544 / block
/// 408941 on 2026-09-22 22:08 UTC via rpc.adiri.tel.
///
/// Re-verify at merge and at tag time. If epoch 554 has begun, raise the constant in the same PR:
/// the trigger (`concluding_epoch + 1 == GOVERNANCE_SAFE_FORK_EPOCH`) cannot fire retroactively,
/// so the live fleet would skip the migration for good while a node replaying that boundary on
/// this build applies it and diverges from canonical history. The const assert below rejects only
/// a value at or below [`CONSENSUS_REGISTRY_FORK_EPOCH`]; a stale epoch above that floor still
/// compiles, so this re-verification is the guard.
///
/// Live pre-fork state, re-read on 2026-09-22 (first sampled 2026-08-28): the three pre-fork pins
/// ([`GOVERNANCE_SAFE_PROXY_PRE_FORK_CODE_HASH`], [`SAFE_SINGLETON_PRE_FORK_CODE_HASH`],
/// [`SAFE_PROXY_FACTORY_PRE_FORK_CODE_HASH`]) match the live deployments; the governance proxy's
/// slot 0 holds the L1 `Safe` singleton and its fallback-handler slot is unset; the Safe nonce
/// is 2; the Safe Singleton Factory deployer's nonce is 0; all eleven etch targets are empty;
/// and the registry carries the post-fork code (the epoch-407 fork ran). A mismatch at
/// re-verification means adiri's Safe state moved: reassess before tagging, and do not update
/// pins to make gates pass.
///
/// Rollout sequence: the standard two-step rule (gate-capable build fleet-wide first, arming later)
/// is compressed into one release here because the fleet's current build, 5f1c0b49
/// (v0.14.0-adiri), predates every one of the fork gates that release arms. Every validator,
/// observer and RPC node must run that release before the closing block of epoch 553 executes; a
/// node still on 5f1c0b49 closes that epoch without the migration and diverges from the canonical
/// chain.
///
/// Under `test-utils`, `TN_GOVERNANCE_SAFE_FORK_EPOCH` overrides the constant (see
/// [`governance_safe_fork_epoch_override`]). Honoring it also takes `adiri`: every piece of this
/// fork is behind that feature and `make build-e2e-bin` omits it, so the variable is inert on the
/// default e2e lanes. `make test-e2e-governance-safe` is the one invocation that arms it on
/// spawned nodes — it builds the `adiri` e2e binary and runs
/// `crates/e2e-tests/tests/it/governance_safe_fork.rs`, which rewrites its genesis into the live
/// adiri pre-fork Safe state and asserts the transition over RPC. Arming the variable on any other
/// lane is a named test failure there rather than a silent no-op.
pub const GOVERNANCE_SAFE_FORK_EPOCH: Epoch = 554;

/// Compile-time floor for [`GOVERNANCE_SAFE_FORK_EPOCH`]: adiri has already crossed
/// [`CONSENSUS_REGISTRY_FORK_EPOCH`], so a value at or below it would be a retroactive
/// arming, and a one-shot `==` trigger can never fire for a boundary that has already closed.
/// Only this floor is machine-checked; whether the armed epoch is still in the future is the
/// manual re-verification documented on the constant.
#[cfg(feature = "adiri")]
const _: () = assert!(GOVERNANCE_SAFE_FORK_EPOCH > CONSENSUS_REGISTRY_FORK_EPOCH);

/// This build's effective governance-Safe fork epoch: the `TN_GOVERNANCE_SAFE_FORK_EPOCH`
/// override when compiled with `test-utils` and set, otherwise
/// [`GOVERNANCE_SAFE_FORK_EPOCH`].
///
/// The boundary trigger in `tn-reth::evm::block` compares the concluding epoch + 1 against
/// this value (one-shot `==`), so tests arm the fork by environment variable without touching
/// the production constant.
#[cfg(feature = "adiri")]
pub fn governance_safe_fork_epoch() -> Epoch {
    #[cfg(feature = "test-utils")]
    if let Some(fork) = governance_safe_fork_epoch_override() {
        return fork;
    }
    GOVERNANCE_SAFE_FORK_EPOCH
}

/// Test-only override of the effective governance-Safe fork epoch, read once from
/// `TN_GOVERNANCE_SAFE_FORK_EPOCH` (`4294967295` for "never fires"; a small value plus a
/// consensus output concluding `value - 1` drives the boundary in-process).
///
/// An environment variable for the same reason as [`seed_signature_fork_epoch_override`]: e2e
/// tests drive real node processes spawned via `TN_BIN_PATH`, which share no memory with the
/// harness, so a process-global setter would silently reach only in-process tests. Compiled
/// out entirely without `test-utils`, so a production binary keeps the compile-time constant
/// and cannot be repointed at runtime by its environment. An unparseable value is ignored
/// rather than defaulted, leaving the build's own fork point in force.
#[cfg(feature = "test-utils")]
pub fn governance_safe_fork_epoch_override() -> Option<Epoch> {
    static OVERRIDE: std::sync::OnceLock<Option<Epoch>> = std::sync::OnceLock::new();
    *OVERRIDE.get_or_init(|| {
        std::env::var("TN_GOVERNANCE_SAFE_FORK_EPOCH").ok().and_then(|raw| raw.trim().parse().ok())
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

    /// Pin every [`GOVERNANCE_SAFE_FORK_CANONICAL_SUITE`] hash to the vendored canonical
    /// bytecode file it names.
    ///
    /// The files are the byte-exact Ethereum-mainnet-captured Safe v1.4.1 runtime bytes that
    /// mainnet genesis etches and the [`GOVERNANCE_SAFE_FORK_EPOCH`] boundary installs on
    /// adiri. Once the fork has run live, re-executing the boundary must install these exact
    /// bytes, so a tn-contracts submodule bump that changes a vendored file must fail here
    /// rather than silently changing historical state roots.
    ///
    /// Unconditional (not `adiri`-gated) so it runs in default-feature CI even though the fork
    /// machinery consuming the table is `adiri`-only.
    #[test]
    fn test_governance_safe_fork_canonical_suite_pinned() {
        // embedded on the same terms as CONSENSUS_REGISTRY_ARTIFACT_JSON above: tn-types cannot
        // read these through tn-config without a circular edge, and the include is the identical
        // file tn-reth's fork machinery embeds
        const VENDORED: [(&str, &str); 13] = [
            ("Safe", include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/Safe.hex")),
            ("SafeL2", include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/SafeL2.hex")),
            (
                "SafeProxyFactory",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/SafeProxyFactory.hex"),
            ),
            (
                "CompatibilityFallbackHandler",
                include_str!(
                    "../../../tn-contracts/deployments/genesis/canonical-bytecode/CompatibilityFallbackHandler.hex"
                ),
            ),
            (
                "SafeToL2Setup",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/SafeToL2Setup.hex"),
            ),
            (
                "MultiSend",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/MultiSend.hex"),
            ),
            (
                "MultiSendCallOnly",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/MultiSendCallOnly.hex"),
            ),
            (
                "SignMessageLib",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/SignMessageLib.hex"),
            ),
            (
                "CreateCall",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/CreateCall.hex"),
            ),
            (
                "SimulateTxAccessor",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/SimulateTxAccessor.hex"),
            ),
            (
                "SafeSingletonFactory",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/SafeSingletonFactory.hex"),
            ),
            (
                "SafeMigration",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/SafeMigration.hex"),
            ),
            (
                "SafeToL2Migration",
                include_str!("../../../tn-contracts/deployments/genesis/canonical-bytecode/SafeToL2Migration.hex"),
            ),
        ];

        for ((table_name, _, expected), (file_name, hex)) in
            GOVERNANCE_SAFE_FORK_CANONICAL_SUITE.iter().zip(VENDORED)
        {
            assert_eq!(
                *table_name, file_name,
                "the vendored-file list must stay in table order so every row is checked",
            );
            let bytes = alloy::hex::decode(hex.trim())
                .unwrap_or_else(|e| panic!("vendored {file_name}.hex must be valid hex: {e}"));
            assert_eq!(
                keccak256(&bytes),
                *expected,
                "{file_name}: GOVERNANCE_SAFE_FORK_CANONICAL_SUITE pins the vendored canonical \
                 bytecode — a tn-contracts bump changed the file; after the fork runs live these \
                 bytes are locked by replay, so reassess the fork plan rather than re-pinning",
            );
        }
    }

    /// Pin the three governance-Safe-fork pre-fork hashes to the LIVE adiri deployments (the
    /// committed `chain-configs/testnet/genesis.yaml`), and the governance proxy's singleton
    /// slot to the L1 `Safe` — the exact state the fork's fail-closed gates expect.
    ///
    /// Mirrors [`test_pre_fork_consensus_registry_code_hash_pinned`], and unconditional for the
    /// same reason. Do not blindly update these constants to make the test pass: if
    /// genesis.yaml was regenerated, the fixture no longer mirrors the live chain the fork
    /// targets — reassess the fork plan first.
    #[test]
    fn test_governance_safe_fork_pre_fork_pins_match_adiri_genesis() {
        let genesis = crate::adiri_genesis();
        // `tn-config::GOVERNANCE_SAFE_ADDRESS` and the canonical Safe addresses, hardcoded
        // because tn-types cannot depend on tn-config/tn-reth
        let cases = [
            (
                "governance SafeProxy",
                address!("0x00000000000000000000000000000000000007a0"),
                GOVERNANCE_SAFE_PROXY_PRE_FORK_CODE_HASH,
            ),
            (
                "Safe singleton (recompile)",
                address!("0x41675C099F32341bf84BFc5382aF534df5C7461a"),
                SAFE_SINGLETON_PRE_FORK_CODE_HASH,
            ),
            (
                "SafeProxyFactory (recompile)",
                address!("0x4e1DCf7AD4e460CfD30791CCC4F9c8a4f820ec67"),
                SAFE_PROXY_FACTORY_PRE_FORK_CODE_HASH,
            ),
        ];
        for (name, addr, expected) in cases {
            let code = genesis
                .alloc
                .get(&addr)
                .and_then(|account| account.code.as_ref())
                .unwrap_or_else(|| panic!("testnet genesis must allocate {name} runtime code"));
            assert_eq!(
                keccak256(code),
                expected,
                "{name}: pre-fork pin mirrors the LIVE adiri deployment",
            );
        }

        // second half of the governance gate: slot 0 must still hold the L1 Safe singleton
        let slot0 = genesis
            .alloc
            .get(&address!("0x00000000000000000000000000000000000007a0"))
            .and_then(|account| account.storage.as_ref())
            .and_then(|storage| storage.get(&B256::ZERO))
            .expect("adiri governance proxy must carry singleton storage at slot 0");
        assert_eq!(
            Address::from_word(*slot0),
            address!("0x41675C099F32341bf84BFc5382aF534df5C7461a"),
            "adiri governance proxy slot 0 must hold the pre-fork L1 Safe singleton",
        );
    }

    /// The governance-Safe fork trigger is one-shot: with the concluding epoch `e`, the
    /// boundary fires iff `e + 1 == GOVERNANCE_SAFE_FORK_EPOCH` — exactly once, never
    /// retroactively, and (unlike the `>=` layout gates) never for any later epoch. On adiri the
    /// fork fires only in the epoch-closing block that concludes epoch 553, the boundary that opens
    /// [`GOVERNANCE_SAFE_FORK_EPOCH`]; the boundary before it, the one after it, and every later
    /// one stay silent. The closure mirrors the trigger in `tn-reth::evm::block`, `checked_add`
    /// included: a concluding epoch of `u32::MAX` has no successor, so it neither fires nor
    /// overflows.
    #[cfg(feature = "adiri")]
    #[test]
    fn governance_safe_fork_boundary_is_one_shot() {
        let fires =
            |concluding: Epoch| concluding.checked_add(1) == Some(GOVERNANCE_SAFE_FORK_EPOCH);

        for concluding in
            [0, 1, 2, GOVERNANCE_SAFE_FORK_EPOCH - 2, GOVERNANCE_SAFE_FORK_EPOCH, u32::MAX]
        {
            assert!(!fires(concluding), "concluding epoch {concluding} must not fire the fork");
        }
        assert!(
            fires(GOVERNANCE_SAFE_FORK_EPOCH - 1),
            "the boundary concluding GOVERNANCE_SAFE_FORK_EPOCH - 1 is the single firing point",
        );
    }

    /// With no `TN_GOVERNANCE_SAFE_FORK_EPOCH` in the environment, the test override must be
    /// completely inert: the effective fork epoch is exactly the compile-time constant.
    #[cfg(all(feature = "adiri", feature = "test-utils"))]
    #[test]
    fn governance_safe_override_is_inert_when_unset() {
        // The override latches in a process-wide `OnceLock`, so a harness launched WITH the
        // variable set cannot observe the unset behaviour. Fail loudly rather than assert a
        // property this process cannot hold; a silent skip here would read as a pass.
        assert!(
            governance_safe_fork_epoch_override().is_none(),
            "this test requires a process without TN_GOVERNANCE_SAFE_FORK_EPOCH set; the \
             override is OnceLock-latched, so run the unset case in its own process",
        );
        assert_eq!(
            governance_safe_fork_epoch(),
            GOVERNANCE_SAFE_FORK_EPOCH,
            "an unset override must not shift the effective fork epoch",
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

    /// Sentinel selecting the child of
    /// [`prevrandao_stays_legacy_while_seed_fork_is_dormant`], independent of the fork overrides
    /// so a lane-exported override cannot be mistaken for a child spawn.
    #[cfg(feature = "test-utils")]
    const TN_TEST_PREVRANDAO_CONJUNCT_CHILD: &str = "TN_TEST_PREVRANDAO_CONJUNCT_CHILD";

    /// The fail-closed conjunction of [`prevrandao_seed_active`]: epochs where the seed
    /// fork is dormant stay on the legacy arm regardless of the PREVRANDAO fork point.
    ///
    /// Pins the seed fork to "never fires" and the PREVRANDAO fork point to "active from
    /// genesis", the exact ordering the conjunct exists for. If the gate ever consulted the
    /// fork point alone, every seed-dormant epoch would promote the forkable legacy
    /// leader-aggregate seed into `PREVRANDAO`; this test observes that ordering directly
    /// instead of relying on the compile-time `>=` assertion between the two constants.
    #[cfg(feature = "test-utils")]
    #[test]
    fn prevrandao_stays_legacy_while_seed_fork_is_dormant() -> std::io::Result<()> {
        // Both overrides latch in process-wide `OnceLock`s. Set them in the child environment
        // before any read, keeping the parent process's unset-override tests independent.
        let exe = std::env::current_exe()?;
        let fn_name = "child_prevrandao_seed_conjunct_blocks";
        let name = module_path!()
            .split_once("::")
            .map_or_else(|| fn_name.to_string(), |(_, module)| format!("{module}::{fn_name}"));
        let output = std::process::Command::new(exe)
            .args(["--exact", name.as_str(), "--ignored", "--nocapture"])
            .env(TN_TEST_PREVRANDAO_CONJUNCT_CHILD, "1")
            .env("TN_SEED_SIGNATURE_FORK_EPOCH", u32::MAX.to_string())
            .env("TN_PREVRANDAO_FORK_EPOCH", "0")
            .output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success() && stdout.contains("1 passed"),
            "child test {name} did not pass exactly once; status {:?}\nstdout:\n{stdout}\n\
             stderr:\n{stderr}",
            output.status,
        );
        Ok(())
    }

    /// Child of [`prevrandao_stays_legacy_while_seed_fork_is_dormant`], with the seed fork
    /// dormant and the PREVRANDAO fork point active from genesis so only the seed conjunct
    /// can keep the public gate closed.
    #[cfg(feature = "test-utils")]
    #[test]
    #[ignore = "spawned by prevrandao_stays_legacy_while_seed_fork_is_dormant with a controlled env"]
    fn child_prevrandao_seed_conjunct_blocks() {
        assert!(
            std::env::var_os(TN_TEST_PREVRANDAO_CONJUNCT_CHILD).is_some(),
            "this child runs only under prevrandao_stays_legacy_while_seed_fork_is_dormant, \
             which pins both fork overrides in the spawn env",
        );
        assert_eq!(
            seed_signature_fork_epoch_override(),
            Some(u32::MAX),
            "this child requires TN_SEED_SIGNATURE_FORK_EPOCH=4294967295 latched from its \
             spawn env; the override is OnceLock-latched, so it cannot be set after startup",
        );
        assert_eq!(
            prevrandao_fork_epoch_override(),
            Some(0),
            "this child requires TN_PREVRANDAO_FORK_EPOCH=0 latched from its spawn env; \
             the override is OnceLock-latched, so it cannot be set after startup",
        );
        // The seed gate is `>=`, so `u32::MAX` itself fires it: the dormant grid stops one
        // below. Every epoch on it has the fork point active (anti-vacuity: the conjunction
        // is actually being exercised) yet must stay on the legacy arm.
        [0, 1, 2, u32::MAX - 1].into_iter().for_each(|epoch| {
            assert!(
                prevrandao_fork_point_active(epoch),
                "anti-vacuity: the pinned fork point must be active at epoch {epoch}",
            );
            assert!(
                !seed_signature_active(epoch),
                "the seed fork must be dormant at epoch {epoch} under the never-fires pin",
            );
            assert!(
                !prevrandao_seed_active(epoch),
                "seed-dormant epoch {epoch} must stay on the legacy arm even with the \
                 PREVRANDAO fork point active: the gate fails closed",
            );
        });
    }

    /// [`fork_epoch_overrides`] reports exactly the pins in force, and nothing else.
    ///
    /// Runs in every feature set: without `test-utils` the list is unconditionally empty, and with
    /// it the list is empty in a process that exported nothing. That second case is why the
    /// startup log can print an entry and mean it — the same reason every
    /// `*_override_is_inert_when_unset` test needs a variable-free process, and the same loud
    /// failure if one latched first.
    ///
    /// The `pinned` list below must name every fork that has a `test-utils` override, each under
    /// the same cfg [`fork_epoch_overrides`] carries it: a fork whose gate is `adiri`-only is
    /// listed only under `adiri`, because only an `adiri` build reports it. A fork added to
    /// [`fork_epoch_overrides`] but not here — or carrying a cfg there that is not mirrored here —
    /// fails the length assert only in a process that exports it.
    #[test]
    fn fork_epoch_overrides_lists_only_the_pins_in_force() {
        #[cfg(not(feature = "test-utils"))]
        assert!(
            fork_epoch_overrides().is_empty(),
            "a build without test-utils compiles the overrides out and can never report one",
        );
        #[cfg(feature = "test-utils")]
        {
            let reported = fork_epoch_overrides();
            // Mirrors the cfg on the governance-Safe row in `fork_epoch_overrides`, restated
            // rather than shared so this stays an independent statement of what it reports.
            #[cfg(feature = "adiri")]
            let governance_safe =
                Some(("TN_GOVERNANCE_SAFE_FORK_EPOCH", governance_safe_fork_epoch_override()));
            #[cfg(not(feature = "adiri"))]
            let governance_safe: Option<(&'static str, Option<Epoch>)> = None;

            let pinned: Vec<_> = [
                ("TN_SEED_SIGNATURE_FORK_EPOCH", seed_signature_fork_epoch_override()),
                ("TN_PREVRANDAO_FORK_EPOCH", prevrandao_fork_epoch_override()),
                ("TN_MULTI_WORKERS_FORK_EPOCH", multi_workers_fork_epoch_override()),
                (
                    "TN_LEADER_SEEDED_ORDERING_FORK_EPOCH",
                    leader_seeded_ordering_fork_epoch_override(),
                ),
            ]
            .into_iter()
            .chain(governance_safe)
            .collect();
            pinned.iter().for_each(|(var, fork_epoch)| {
                assert_eq!(
                    reported.iter().find(|(name, _)| name == var).map(|(_, epoch)| *epoch),
                    *fork_epoch,
                    "{var} must be reported exactly when it is pinned, at the pinned value",
                );
            });
            assert_eq!(
                reported.len(),
                pinned.iter().filter(|(_, fork_epoch)| fork_epoch.is_some()).count(),
                "no fork may be reported that is not pinned: {reported:?}",
            );
        }
    }

    /// Seed-dormant epochs never take the seeded arm: the observable half of the fail-closed
    /// conjunct documented on [`prevrandao_seed_active`].
    ///
    /// This pins the outcome, not the conjunct in isolation. Catching a deleted
    /// `seed_signature_active(epoch) &&` needs an epoch where the fork point is active while the
    /// seed fork is not, and no such epoch exists here: the const assert above forces
    /// `PREVRANDAO_FORK_EPOCH >= SEED_SIGNATURE_FORK_EPOCH`, so every seed-dormant epoch is
    /// fork-point-dormant too — asserted below so the limitation stays visible rather than
    /// implied. The ordering assert is what enforces the contract for the shipped constants; the
    /// runtime conjunct is the backstop for an override-driven schedule, and it is observable
    /// only in a process that sets `TN_PREVRANDAO_FORK_EPOCH` below the seed fork.
    #[cfg(feature = "adiri")]
    #[test]
    fn prevrandao_seed_active_stays_legacy_on_seed_dormant_epochs() {
        // `saturating_sub` rather than `- 1` so a rollout that ever put the seed fork at genesis
        // fails through the assertion message below instead of an underflow panic here.
        [0, 1, SEED_SIGNATURE_FORK_EPOCH.saturating_sub(1)].into_iter().for_each(|epoch| {
            assert!(
                !seed_signature_active(epoch),
                "epoch {epoch} must be seed-dormant for this test to mean anything",
            );
            assert!(
                !prevrandao_seed_active(epoch),
                "epoch {epoch} is seed-dormant, so the PREVRANDAO gate must stay closed \
                 regardless of the fork point",
            );
            // the vacuity this test cannot escape, stated rather than left for a reader to
            // rediscover: both conjuncts are false here, so either one alone would satisfy the
            // assertion above
            assert!(
                !prevrandao_fork_point_active(epoch),
                "epoch {epoch} is expected to be fork-point-dormant as well; if a schedule ever \
                 makes it active here, this test becomes a live check of the seed conjunct and \
                 this assertion is what should be deleted",
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

    /// Pin the leader-seeded-ordering gate to the rollout contract this build actually
    /// implements.
    ///
    /// Carries the same asymmetry [`build_fork_gate_matches_this_builds_rollout_contract`]
    /// states for the seed-signature gate: "dormant while the constant is `u32::MAX`" holds
    /// only under `adiri`. Every other build, including the default one that produces both the
    /// shipped node binary and the e2e binary, is active from genesis, so epoch 0 already
    /// orders sub-DAGs with the leader-seeded tie-break there.
    ///
    /// Asserts against [`leader_seeded_ordering_build_fork_active`], the override-free
    /// decision, so the result does not depend on whether `test-utils` was unified into this
    /// build. The grid is derived from the constant, so arming the fork does not require
    /// editing this test.
    #[test]
    fn leader_seeded_ordering_build_fork_gate_matches_this_builds_rollout_contract() {
        #[cfg(not(feature = "adiri"))]
        [0, 1, 2, u32::MAX].into_iter().for_each(|epoch| {
            assert!(
                leader_seeded_ordering_build_fork_active(epoch),
                "non-adiri builds never produced a legacy-ordered commit and are active from \
                 genesis; epoch {epoch} must be post-fork",
            );
        });
        #[cfg(feature = "adiri")]
        {
            [0, 1, 2, LEADER_SEEDED_ORDERING_FORK_EPOCH.saturating_sub(1)]
                .into_iter()
                .filter(|epoch| *epoch < LEADER_SEEDED_ORDERING_FORK_EPOCH)
                .for_each(|epoch| {
                    assert!(
                        !leader_seeded_ordering_build_fork_active(epoch),
                        "adiri stays dormant before LEADER_SEEDED_ORDERING_FORK_EPOCH; epoch \
                         {epoch} must be pre-fork",
                    );
                });
            [LEADER_SEEDED_ORDERING_FORK_EPOCH, u32::MAX].into_iter().for_each(|epoch| {
                assert!(
                    leader_seeded_ordering_build_fork_active(epoch),
                    "the gate must fire from the fork epoch onward (`>=`, not `>`); epoch \
                     {epoch} must be post-fork",
                );
            });
        }
    }

    /// With no `TN_LEADER_SEEDED_ORDERING_FORK_EPOCH` in the environment, the test override
    /// must be completely inert: the gate answers exactly as the compile-time contract does.
    #[cfg(feature = "test-utils")]
    #[test]
    fn leader_seeded_ordering_override_is_inert_when_unset() {
        // The override latches in a process-wide `OnceLock`, so a harness launched WITH the
        // variable set cannot observe the unset behaviour. Fail loudly rather than assert a
        // property this process cannot hold; a silent skip here would read as a pass.
        assert!(
            leader_seeded_ordering_fork_epoch_override().is_none(),
            "this test requires a process without TN_LEADER_SEEDED_ORDERING_FORK_EPOCH set; \
             the override is OnceLock-latched, so run the unset case in its own process",
        );
        [0, 1, 2, u32::MAX].into_iter().for_each(|epoch| {
            // The fork point is the half that carries the override; the public gate wraps it
            // in the `seed_signature_active` conjunct, which is orthogonal to the override.
            assert_eq!(
                leader_seeded_ordering_fork_point_active(epoch),
                leader_seeded_ordering_build_fork_active(epoch),
                "an unset override must not shift the fork point at epoch {epoch}",
            );
            assert_eq!(
                leader_seeded_ordering_active(epoch),
                seed_signature_active(epoch) && leader_seeded_ordering_build_fork_active(epoch),
                "the public gate must be exactly the fail-closed conjunction at epoch {epoch}",
            );
        });
    }

    /// Sentinel selecting the child dispatch of
    /// [`leader_seeded_conjunct_blocks_when_seed_fork_is_later`]: a dedicated variable rather
    /// than the fork overrides themselves, so lane-exported fork variables cannot be mistaken
    /// for a child spawn.
    #[cfg(feature = "test-utils")]
    const TN_TEST_SEED_CONJUNCT_CHILD: &str = "TN_TEST_LEADER_SEEDED_CONJUNCT_CHILD";

    /// The `seed_signature_active` conjunct in [`leader_seeded_ordering_active`] blocks the
    /// gate when the seed fork is scheduled later than the leader-seeded fork point (#1260).
    ///
    /// This is the only configuration where the conjunct is observable: the compile-time
    /// assert `LEADER_SEEDED_ORDERING_FORK_EPOCH >= SEED_SIGNATURE_FORK_EPOCH` makes the fork
    /// point imply the conjunct whenever the overrides are unset, so the in-process "exactly
    /// the fail-closed conjunction" assert above cannot catch deletion of the
    /// `seed_signature_active &&` term. Spawns THIS test binary with the seed fork pinned
    /// dormant (`4294967295`) and the leader-seeded fork point pinned to `0`: both overrides
    /// latch in process-wide `OnceLock`s, so the crossed schedule needs its own process. The
    /// child's harness output must report exactly one passed test: a drifted name would match
    /// nothing and still exit 0, so exit status alone would be a vacuous pass.
    #[cfg(feature = "test-utils")]
    #[test]
    fn leader_seeded_conjunct_blocks_when_seed_fork_is_later() {
        let exe = std::env::current_exe().expect("test binary path");
        let fn_name = "child_leader_seeded_conjunct_blocks";
        let name = module_path!()
            .split_once("::")
            .map_or_else(|| fn_name.to_string(), |(_, module)| format!("{module}::{fn_name}"));
        let mut command = std::process::Command::new(exe);
        command.args(["--exact", name.as_str(), "--ignored", "--nocapture"]);
        command.env(TN_TEST_SEED_CONJUNCT_CHILD, "1");
        command.env("TN_SEED_SIGNATURE_FORK_EPOCH", u32::MAX.to_string());
        command.env("TN_LEADER_SEEDED_ORDERING_FORK_EPOCH", "0");
        let output = command.output().expect("spawn child test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success() && stdout.contains("1 passed"),
            "child test {name} did not pass exactly once; status {:?}\nstdout:\n{stdout}\n\
             stderr:\n{stderr}",
            output.status,
        );
    }

    /// Child of [`leader_seeded_conjunct_blocks_when_seed_fork_is_later`], spawned with the
    /// seed fork dormant and the leader-seeded fork point active from genesis: the fork point
    /// says yes at every probed epoch, so any `false` from the public gate is attributable to
    /// the `seed_signature_active` conjunct alone.
    #[cfg(feature = "test-utils")]
    #[test]
    #[ignore = "spawned by leader_seeded_conjunct_blocks_when_seed_fork_is_later with a \
                controlled env"]
    fn child_leader_seeded_conjunct_blocks() {
        assert!(
            std::env::var_os(TN_TEST_SEED_CONJUNCT_CHILD).is_some(),
            "this child runs only under leader_seeded_conjunct_blocks_when_seed_fork_is_later, \
             which pins both fork overrides in the spawn env",
        );
        // Both overrides latch in process-wide `OnceLock`s, so a child launched without them
        // in its env cannot observe the crossed schedule. Fail loudly rather than assert a
        // property this process cannot hold; a silent skip here would read as a pass.
        assert_eq!(
            seed_signature_fork_epoch_override(),
            Some(u32::MAX),
            "this child requires TN_SEED_SIGNATURE_FORK_EPOCH=4294967295 latched from its \
             spawn env; the override is OnceLock-latched, so it cannot be set after startup",
        );
        assert_eq!(
            leader_seeded_ordering_fork_epoch_override(),
            Some(0),
            "this child requires TN_LEADER_SEEDED_ORDERING_FORK_EPOCH=0 latched from its \
             spawn env; the override is OnceLock-latched, so it cannot be set after startup",
        );
        // 383 is adiri's SEED_SIGNATURE_FORK_EPOCH, written as a literal because the
        // constant does not exist on non-adiri builds and this child is not adiri-gated.
        [0, 1, 383, u32::MAX - 1].into_iter().for_each(|epoch| {
            assert!(
                leader_seeded_ordering_fork_point_active(epoch),
                "the fork point is pinned to 0, so it must be active at epoch {epoch}; \
                 otherwise the gate assertion below would not isolate the conjunct",
            );
            assert!(
                !leader_seeded_ordering_active(epoch),
                "the dormant seed fork must block the public gate at epoch {epoch} even \
                 though the leader-seeded fork point is active from 0",
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
