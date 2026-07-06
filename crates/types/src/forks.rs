//! Code to support various chain forks.

#[cfg(feature = "adiri")]
use crate::Epoch;

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
/// one-line PR before deploy (mirror the `ADIRI_DUP_BATCH_EPOCH` rollout). Pre-deploy checklist:
/// - every node must run a **fork-capable build** (compiled with the `adiri` feature — verify the
///   deploy image is built `--features adiri`) before this epoch, or it will reject the fork block
///   and fork off the network;
/// - confirm the live validator/ConsensusNFT count leaves headroom under the 30M system-call gas
///   cap that bounds the one-shot `migrateValidatorSets()` walk;
/// - consider gating the in-protocol swap on a confirmed expected pre-fork registry code hash so an
///   unexpected on-chain deployment fails closed rather than migrating over an incompatible layout.
pub const CONSENSUS_REGISTRY_FORK_EPOCH: Epoch = u32::MAX;
