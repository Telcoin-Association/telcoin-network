//! Code to support various chain forks.

#[cfg(feature = "adiri")]
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
/// PLACEHOLDER: `u32::MAX` practically never fires (the trigger would need epoch `u32::MAX - 1`,
/// ~4.29e9 epochs away). Set to a concrete future epoch with ample lead time in a dedicated
/// epoch-setting PR before deploy.
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
/// - pin the swapped-in (post-fork) runtime code hashes of **both** contracts the same way
///   [`CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`] and [`WORKER_CONFIGS_PRE_FORK_CODE_HASH`] pin the
///   pre-fork code, with pin tests against the embedded `ConsensusRegistry.json` and
///   `WorkerConfigs.json` artifacts: after the fork runs live, a tn-contracts artifact bump would
///   otherwise change the bytes re-execution swaps in and break historical state roots;
/// - read the LIVE deployed `WorkerConfigs` and confirm `numWorkers()` and, for every `i <
///   numWorkers`, `_workerConfigSet[i] == true` (a storage probe — the mapping is internal): the
///   first post-fork closing block's `setWorkerConfigsData` system call reverts
///   `MissingWorkerConfig` on any unset row, aborting the one-shot fork-boundary close.
///   Additionally confirm `data == 0` for every `Eip1559` row on the live contract — the entry read
///   prices epochs from that word (see the rollout constraint below). Do this alongside the
///   post-fork hash re-pinning above, since an artifact rebuild silently moves those hashes.
///   Informational reference only, NOT a compiled constant: at the time of writing, the embedded
///   artifact's post-fork `WorkerConfigs` splice hashes to
///   `0x58304c00bbfaa7e348220efb95843614756207311245abc4949f91bb3ddb2ff7`;
/// - the `WorkerConfigs` bytecode swap ships at this same fork epoch (see
///   [`WORKER_CONFIGS_PRE_FORK_CODE_HASH`]) — both swaps land in the epoch-closing block of
///   `CONSENSUS_REGISTRY_FORK_EPOCH - 1`, so a build applying one but not the other diverges;
/// - confirm the live validator/ConsensusNFT count leaves headroom under the 100M system-call gas
///   cap that bounds the one-shot `migrateValidatorSets()` walk;
/// - operator dry-run: resync a fork-build node against a live adiri archive across the fork
///   boundary and confirm matching state roots (also measures the live migration gas);
/// - both swaps fail closed on their pre-fork pin ([`CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`],
///   [`WORKER_CONFIGS_PRE_FORK_CODE_HASH`]): an unexpected on-chain deployment aborts the block
///   (fatal error) rather than migrating over an incompatible layout;
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
pub const CONSENSUS_REGISTRY_FORK_EPOCH: Epoch = u32::MAX;

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::{address, keccak256};

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
}
