//! Methods for node startup and epoch boundaries.
//!
//! Two families live here: startup marker maintenance
//! ([`RethEnv::heal_finalized_to_persisted_tip`],
//! [`RethEnv::finalized_block_hash_number_for_startup`]) and read-only reads of on-chain state
//! from the [`ConsensusRegistry`] and [`WorkerConfigs`] contracts.
//!
//! Every contract read executes as a read-only EVM system call: `transact_system_call` (see
//! `evm/mod.rs`) from `SYSTEM_ADDRESS`, each call under its own fresh system-call gas budget,
//! with gas price 0 and the nonce check disabled. Nothing commits — the returned data is decoded
//! and the touched state discarded.
//!
//! # Pinned reads vs tip reads
//!
//! Contract reads come in two flavors, and picking the wrong one is a consensus hazard:
//!
//! - **Tip reads** — [`RethEnv::epoch_state_from_canonical_tip`],
//!   [`RethEnv::read_consensus_registry`] and its batch form — run against the MUTABLE canonical
//!   tip. Point-in-time only: two nodes (or one node at two moments) may decode different answers,
//!   so they are unsafe for consensus decisions. No per-epoch committee reader is a tip read: the
//!   unpinned `validators_for_epoch` / `bls_pubkeys_for_epoch` pair was removed, so every committee
//!   read must name its pin.
//! - **Pinned reads** — the `*_at_header` / `*_at_block` variants — run against one explicit block;
//!   every node issuing the same read at the same block decodes identical bytes. The per-epoch
//!   validator-info variant (`validators_for_epoch_at_block`) is `test-utils`-gated and lives in
//!   `test_utils.rs`; production reads the committee through
//!   [`RethEnv::epoch_state_at_epoch_start`].
//! - [`RethEnv::epoch_state_at_epoch_start`] pins to the entered epoch's start state: the previous
//!   epoch's closing block (`epoch_info.blockHeight - 1`), or genesis for epoch 0. It samples the
//!   canonical tip to bootstrap that pin; callers that must prove every attempt of a retried read
//!   resolves the SAME pin sample the tip themselves and use
//!   [`RethEnv::epoch_state_at_epoch_start_from_tip`].
//!
//! Pinning matters because the registry's committee arrays mutate MID-epoch: a governance
//! `burn` swap-and-pops the ejected validator out of the CURRENT epoch's stored committee
//! arrays immediately, so tip reads before and after the burn disagree (see
//! [`RethEnv::epoch_state_from_canonical_tip`]). Epoch-scoped consensus reads — committee
//! construction, rewards seeding, quorum — must use a pinned read.
//!
//! # Archive-mode assumption (every pinned read)
//!
//! A pinned read is only as deterministic as the history under it. This node never constructs
//! a pruner — `PruningArgs` in `cli.rs` is built with every field disabled — so historical
//! state always resolves fully indexed. If pruning were ever enabled, reth's historical
//! provider could silently fall back to TIP state for a pruned block, turning a "pinned" read
//! into exactly the nondeterminism pinning exists to prevent. Every pinned read here builds its
//! state through the one `pinned_state_and_env` helper, which carries the normative ARCHIVE-MODE
//! note; revisit it before enabling pruning.
//!
//! # Error classification
//!
//! Methods returning [`StateReadResult`] classify failures by committee determinism:
//! [`StateReadError::Provider`] is a node-local fault (this node's provider/database — peers
//! reading the same block may succeed; retry or halt, never fail open), while
//! [`StateReadError::ChainGlobal`] is a deterministic product of the pinned block and calldata
//! (revert, halt, decode failure, absent contract — identical on every node, so failing open
//! stays fleet-consistent). The boundary lives in the private `classified_system_call` helper:
//! `EVMError::Database` maps to `Provider`; every other transact failure maps to `ChainGlobal`.
//!
//! Every consensus-critical pinned read is classified. Epoch state:
//! [`RethEnv::epoch_state_at_header`], [`RethEnv::epoch_state_at_epoch_start`] and its
//! [`_from_tip`](RethEnv::epoch_state_at_epoch_start_from_tip) form, and
//! [`RethEnv::get_current_epoch_info_at_header`]. Committee pubkeys:
//! [`RethEnv::bls_pubkeys_for_epoch_at_header`], [`RethEnv::bls_pubkeys_for_epochs_at_header`],
//! and their by-hash siblings [`RethEnv::bls_pubkeys_for_epoch_at_block`] /
//! [`RethEnv::bls_pubkeys_for_epochs_at_block`]. Worker fees:
//! [`RethEnv::get_worker_fee_configs_at_block`].
//!
//! [`RethEnv::epoch_state_from_canonical_tip`] deliberately stays `eyre`: it is a tip read, so no
//! caller can act on the classification (a tip read is not committee-deterministic to begin
//! with). The `read_consensus_registry*` family stays [`EvmReadResult`] because the RPC layer
//! maps [`EvmReadError::Revert`]'s raw output bytes into an eth_call-style error, which
//! [`StateReadError`]'s string-only variants cannot carry.

use reth_errors::ProviderError;
use reth_eth_wire::BlockHashNumber;
use reth_evm::{ConfigureEvm as _, Evm as _, EvmEnv, EvmFactory as _};
use reth_provider::{
    BlockIdReader as _, BlockNumReader as _, ChainStateBlockReader as _,
    ChainStateBlockWriter as _, DBProvider as _, DatabaseProviderFactory as _, HeaderProvider as _,
    StateProviderBox,
};
use reth_revm::{
    context::result::{EVMError, ExecutionResult, ResultAndState},
    database::StateProviderDatabase,
    State,
};
use tn_config::WORKER_CONFIGS_ADDRESS;
use tn_types::{
    gas_accumulator::WorkerFeeConfig, Address, Bytes, Epoch, ExecHeader, SealedHeader,
    SolCall as _, B256,
};
use tracing::{debug, error, warn};

use crate::{
    error::{
        EvmReadError, EvmReadResult, StateReadError, StateReadResult, TnRethError, TnRethResult,
    },
    evm::TNEvm,
    system_calls::{ConsensusRegistry, EpochState, WorkerConfigs, CONSENSUS_REGISTRY_ADDRESS},
    RethEnv, SYSTEM_ADDRESS,
};

impl RethEnv {
    /// Advance a lagging finalized marker to the persisted canonical tip.
    ///
    /// Blocks and the finalized/safe markers commit in a single database transaction in
    /// [`Self::finish_executing_output`], so a crash can no longer leave the marker behind the
    /// persisted tip — the two-transaction crash window this heal was written for does not
    /// exist for new writes. The heal is retained as defense-in-depth for databases written by
    /// pre-fix versions, which committed the markers separately in `finalize_block`: a crash
    /// between the two transactions restarted the node with `finalized marker < persisted
    /// canonical tip`, hiding the gap blocks from every marker reader (accumulator catchup,
    /// the tx-pool's last-seen seed, the RPC `finalized`/`safe` tags).
    ///
    /// Advancing the marker is sound because every persisted canonical block is consensus-final
    /// by construction: blocks only enter the canonical database from committed consensus
    /// output, so there is no speculative or reorgable segment the marker could overrun. The
    /// lag is purely a persistence artifact of the pre-fix two-transaction design.
    ///
    /// The heal persists the marker itself ([`Self::finalize_block`] no longer writes the
    /// database) and then routes through `finalize_block` for the in-memory half: the
    /// finalized/safe watches are seeded from the (stale) database at provider construction,
    /// and startup marker readers like the accumulator catchup consult the watch — a DB-only
    /// write would leave them reading the stale value.
    ///
    /// Replay is unaffected: the missed-consensus watermark derives from the persisted
    /// execution tip (which this method never moves), not the finalized marker, so healing
    /// cannot cause `replay_missed_consensus` to re-forward committed output — no
    /// double-execution is possible.
    ///
    /// Call once at startup, before anything reads the marker and before any consensus output
    /// executes. A marker ahead of the tip fails with
    /// [`TnRethError::FinalizedMarkerAheadOfTip`]: the execution database lost blocks this node
    /// already attested as final.
    ///
    /// Like every startup tip reader, this trusts the persisted tip itself; a torn write
    /// between MDBX and static files below the provider is a pre-existing exposure this method
    /// neither adds to nor guards against.
    pub fn heal_finalized_to_persisted_tip(&self) -> TnRethResult<()> {
        // One RO provider = one consistent MDBX snapshot for all three reads. Read errors
        // PROPAGATE: the public last_block_number()/last_finalized_block_number() wrappers
        // unwrap_or(0) and would misdiagnose a failed read as tip=0 here.
        let provider = self.inner.blockchain_provider.database_provider_ro()?;
        let tip = provider.last_block_number()?;
        // None = never finalized (fresh genesis). Some(0) is unreachable in production (the
        // first marker write covers block >= 1), so collapsing None to 0 is safe.
        let finalized = provider.last_finalized_block_number()?.unwrap_or(0);

        if finalized > tip {
            return Err(TnRethError::FinalizedMarkerAheadOfTip { finalized, tip });
        }
        if finalized == tip {
            // Normal restart (marker caught up), or fresh genesis (marker never written and
            // tip = 0): genesis intentionally stays unfinalized so
            // finalized_block_hash_number_for_startup keeps its genesis fallback.
            return Ok(());
        }

        let header =
            provider.sealed_header(tip)?.ok_or(ProviderError::HeaderNotFound(tip.into()))?;
        drop(provider);
        warn!(
            target: "tn::reth",
            finalized,
            tip,
            "finalized marker lags the persisted canonical tip (database written by a pre-fix \
             version that committed blocks and the marker separately); healing marker to tip"
        );
        // durably fix the marker on disk before touching the watches: finalize_block only
        // updates in-memory state, so a lagging pre-fix database needs its rows rewritten here
        let provider_rw = self.inner.blockchain_provider.database_provider_rw()?;
        provider_rw.save_finalized_block_number(tip)?;
        provider_rw.save_safe_block_number(tip)?;
        provider_rw.commit()?;
        self.finalize_block(header)
    }

    /// Return the block number and hash of the finalized block on node startup.
    ///
    /// This method adds additional fallbacks to ensure genesis is used when the network is starting
    /// because the genesis block is not initialized as `finalized`. Nodes that start on genesis
    /// will resync with the network if it exists.
    pub fn finalized_block_hash_number_for_startup(&self) -> TnRethResult<BlockHashNumber> {
        let hash = self
            .inner
            .blockchain_provider
            .finalized_block_hash()?
            .unwrap_or_else(|| self.node_config().chain.sealed_genesis_header().hash());
        let number = self.inner.blockchain_provider.finalized_block_number()?.unwrap_or_default();
        Ok(BlockHashNumber { hash, number })
    }

    /// Read the latest committee and epoch information from the [ConsensusRegistry] on-chain.
    ///
    /// The protocol needs the BLS pubkey for the authorities.
    /// - get current epoch info
    /// - getValidator token id by address
    /// - getValidator info by token id
    ///
    /// The committee arrays this returns mutate mid-epoch: a governance `burn` swap-and-pops the
    /// ejected validator out of the CURRENT epoch's stored committees immediately, so tip reads
    /// before and after the burn disagree. Epoch-scoped consensus reads (committee construction,
    /// rewards seeding, quorum) must use [`Self::epoch_state_at_epoch_start`] instead; the tip
    /// read remains correct for point-in-time queries.
    pub fn epoch_state_from_canonical_tip(&self) -> eyre::Result<EpochState> {
        let canonical_tip = self.canonical_tip();
        debug!(target: "engine", ?canonical_tip, "retrieving epoch state from canonical tip");
        // Stays `eyre` on purpose: this is a TIP read, so its result is not
        // committee-deterministic to begin with and no caller can act on the classification.
        Ok(self.epoch_state_at_header(&canonical_tip)?)
    }

    /// Read the committee and epoch information from the [ConsensusRegistry] at `header`.
    ///
    /// The registry state, the EVM environment, and therefore the returned epoch, epoch info,
    /// and committee all derive from this ONE header. Recovery paths that scan
    /// `epoch_info.blockHeight..=header.number` (catchup and epoch-entry base-fee seeding) rely
    /// on this pin: reading the range start from a different header (e.g. the canonical tip)
    /// could yield a silently empty range if finality ever lags the canonical tip.
    ///
    /// Failures are classified per [`StateReadError`] so consensus-critical callers can retry a
    /// node-local provider fault instead of halting on it.
    pub fn epoch_state_at_header(&self, header: &SealedHeader) -> StateReadResult<EpochState> {
        // create EVM with the state at the pinned header
        let (mut db, evm_env) = self.pinned_state_and_env(header)?;
        debug!(target: "engine", state=?db.bundle_state, hashes=?db.block_hashes, "retrieving epoch state at header");
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        // current epoch number
        let epoch = Self::classified_registry_read::<_, u32>(
            &mut tn_evm,
            ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into(),
        )?;

        // current epoch info
        let epoch_info = Self::classified_registry_read::<_, ConsensusRegistry::EpochInfo>(
            &mut tn_evm,
            ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into(),
        )?;
        debug!(target: "engine", ?epoch, ?epoch_info, "retrieved epoch info at header");

        // retrieve closing timestamp for previous epoch. On the epoch-start path the pin IS the
        // closing block, so reuse the held header instead of re-fetching it by number; tip
        // callers keep the fetch branch. Value-preserving for both: equal numbers on the
        // canonical chain resolve to this same header, and the
        // `epoch_state_at_epoch_start_pins_pre_burn_committee` test pins the tip/pinned
        // `epoch_start` equality.
        //
        // Both failures in the fetch branch are node-local (Provider), not chain-global:
        // `header_by_number` reads the DATABASE while the caller's header may come from the
        // in-memory canonical state, so an executed-but-not-yet-persisted closing block can
        // legitimately miss on this node while every peer resolves it.
        let closing_number = epoch_info.blockHeight.saturating_sub(1);
        let epoch_start = if closing_number == header.number {
            header.timestamp
        } else {
            self.header_by_number(closing_number)
                .map_err(|e| {
                    StateReadError::Provider(format!("closing header {closing_number} lookup: {e}"))
                })?
                .ok_or_else(|| {
                    StateReadError::Provider(format!(
                        "failed to retrieve closing epoch information: missing block \
                         {closing_number}"
                    ))
                })?
                .timestamp
        };

        // retrieve the committee
        let validators = Self::classified_registry_read::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
            &mut tn_evm,
            ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into(),
        )?;
        let bls_pubkeys = Self::classified_registry_read::<_, Vec<alloy::primitives::Bytes>>(
            &mut tn_evm,
            ConsensusRegistry::getCommitteeBlsPubkeysCall { epoch }.abi_encode().into(),
        )?;
        let epoch_state = EpochState { epoch, epoch_info, validators, bls_pubkeys, epoch_start };
        debug!(target: "engine", ?epoch_state, "returning epoch state at header");

        Ok(epoch_state)
    }

    /// Read the CURRENT epoch's [`EpochState`] pinned to the epoch's start state — the previous
    /// epoch's closing block (genesis for epoch 0) — returning the pin header alongside it.
    ///
    /// Bootstrapping reads the current epoch number and its
    /// [`EpochInfo`](ConsensusRegistry::EpochInfo) at the canonical tip. This is deterministic at
    /// ANY tip because `concludeEpoch` writes the epoch number and the `EpochInfo` scalars this
    /// method consumes (`blockHeight`) exactly once at the boundary and never mid-epoch — only
    /// the committee ARRAYS mutate mid-epoch (governance `burn` swap-and-pops immediately,
    /// including the committee embedded in `EpochInfo`), which is why the full state read below
    /// is pinned instead of taken at the tip.
    ///
    /// The pin: `epoch_info.blockHeight` is the entered epoch's first block, so `blockHeight - 1`
    /// is the previous epoch's closing block — the block whose execution ran `concludeEpoch` and
    /// seated the entered committee. For epoch 0 the pin is genesis, whose execution seeds the
    /// registry: genesis state IS epoch 0's start state.
    ///
    /// Every field of the returned state derives from the previous epoch's closing block, so any
    /// node entering (or re-entering) the same epoch — fresh boundary crossing, crash-restart
    /// replay, or ModeChange re-entry, before or after a mid-epoch burn — derives an IDENTICAL
    /// epoch view (committee membership included).
    ///
    /// Tripwire: `concludeEpoch` runs INSIDE the closing block, so the pinned read must already
    /// report the entered epoch. If the pinned epoch disagrees with the tip's, the registry's
    /// `blockHeight == closing block + 1` convention broke; this method fails hard rather than
    /// return a possibly-stale committee.
    ///
    /// Samples the canonical tip itself, so two calls can bootstrap off two different tips. A
    /// caller that RETRIES this read must sample the tip once and use
    /// [`Self::epoch_state_at_epoch_start_from_tip`] instead — see that method's docs for why the
    /// difference is load-bearing.
    pub fn epoch_state_at_epoch_start(&self) -> StateReadResult<(EpochState, SealedHeader)> {
        self.epoch_state_at_epoch_start_from_tip(&self.canonical_tip())
    }

    /// [`Self::epoch_state_at_epoch_start`] with the bootstrap tip supplied by the caller.
    ///
    /// The pin this resolves is a function of `tip`: the bootstrap read takes the current epoch
    /// number and its `blockHeight` AT `tip`, and `concludeEpoch` rewrites both at EVERY epoch
    /// boundary (they are written once per epoch, not once ever). A caller that re-samples the
    /// canonical tip per attempt could therefore resolve a different pin on a later attempt; one
    /// that samples `tip` ONCE outside its retry loop proves every attempt resolves the same
    /// header. Retrying callers must use this form for that reason — see
    /// `manager::node::start_epoch`'s epoch-entry read.
    ///
    /// `tip` need not be the live canonical tip, only a header whose registry state names the
    /// epoch to enter; passing an older header pins that header's epoch instead.
    pub fn epoch_state_at_epoch_start_from_tip(
        &self,
        tip: &SealedHeader,
    ) -> StateReadResult<(EpochState, SealedHeader)> {
        // boundary-written-once identity read: deterministic at any tip
        let (epoch, epoch_info) = self.get_current_epoch_info_at_header(tip)?;

        let pin_header = if epoch == 0 {
            // The pin headers are node-local lookups: the block exists on the committee by
            // construction, so a miss or an error reflects THIS node's database, not the chain.
            self.sealed_header_by_number(0)
                .map_err(|e| StateReadError::Provider(format!("genesis header lookup: {e}")))?
                .ok_or_else(|| StateReadError::Provider("missing genesis header".into()))?
        } else {
            // A `blockHeight` of 0 for a non-zero epoch is the registry contradicting its own
            // `blockHeight == closing block + 1` convention: chain-global, identical on every
            // node, and no retry can change it.
            let closing_number = epoch_info.blockHeight.checked_sub(1).ok_or_else(|| {
                StateReadError::ChainGlobal(format!("current epoch {epoch} reports blockHeight 0"))
            })?;
            self.sealed_header_by_number(closing_number)
                .map_err(|e| {
                    StateReadError::Provider(format!("closing header {closing_number} lookup: {e}"))
                })?
                .ok_or_else(|| {
                    StateReadError::Provider(format!(
                        "missing closing block {closing_number} for current epoch {epoch}"
                    ))
                })?
        };
        debug!(
            target: "engine",
            ?epoch,
            pin_number = pin_header.number,
            pin_hash = ?pin_header.hash(),
            "retrieving epoch state at epoch start"
        );

        // The pinned EVM re-reads the epoch number and `EpochInfo` inside this call. That
        // re-read is load-bearing, not redundant with the bootstrap read above: every field of
        // the returned state must derive from the pin (provenance), and the re-read is the
        // input the tripwire below checks against the tip's view.
        let state = self.epoch_state_at_header(&pin_header)?;

        // Tripwire: `concludeEpoch` executes INSIDE the closing block, so the registry at the
        // closing header already reports the entered epoch. This can only fire if the registry's
        // `blockHeight == closing block + 1` convention ever breaks — running a stale committee
        // is a consensus-safety failure, so fail hard instead of returning the mismatched state.
        // Chain-global: both epochs are deterministic products of their pinned blocks, so every
        // node reading the same pair observes the same disagreement and no retry can clear it.
        if state.epoch != epoch {
            error!(
                target: "engine",
                pin_number = pin_header.number,
                pinned_epoch = state.epoch,
                tip_epoch = epoch,
                "epoch-start pin and canonical tip disagree on the current epoch"
            );
            return Err(StateReadError::ChainGlobal(format!(
                "epoch state pinned to block {} reports epoch {} but the canonical tip reports \
                 epoch {epoch}",
                pin_header.number, state.epoch
            )));
        }

        Ok((state, pin_header))
    }

    /// Read the BLS pubkeys for the committee of the provided epoch from the
    /// [ConsensusRegistry], pinned to the state of the block identified by `block_hash`.
    ///
    /// Every node issuing this read at the same block decodes the identical key set — even
    /// after a mid-epoch governance `burn` swap-and-pops the stored committee arrays; an
    /// unpinned canonical-tip read would not.
    ///
    /// Failures are classified per [`StateReadError`]; `block_hash` failing to resolve is
    /// [`StateReadError::Provider`] (see [`Self::pinned_header_by_hash`]).
    pub fn bls_pubkeys_for_epoch_at_block(
        &self,
        epoch: u32,
        block_hash: B256,
    ) -> StateReadResult<Vec<alloy::primitives::Bytes>> {
        let header = self.pinned_header_by_hash(block_hash)?;
        self.bls_pubkeys_for_epoch_at_header(epoch, &header)
    }

    /// Read the BLS pubkeys for several epochs' committees from the [ConsensusRegistry], pinned
    /// to the state of the block identified by `block_hash`.
    ///
    /// The by-hash sibling of [`Self::bls_pubkeys_for_epochs_at_header`], for callers holding
    /// only a block hash (the epoch-record close, which pins to the epoch-closing block it
    /// records as `final_state`). ONE header lookup and ONE pinned EVM serve the whole batch, so
    /// every set in the result provably derives from the same block — two separate single-epoch
    /// by-hash reads would each resolve the header independently.
    pub fn bls_pubkeys_for_epochs_at_block(
        &self,
        epochs: &[Epoch],
        block_hash: B256,
    ) -> StateReadResult<Vec<Vec<alloy::primitives::Bytes>>> {
        let header = self.pinned_header_by_hash(block_hash)?;
        self.bls_pubkeys_for_epochs_at_header(epochs, &header)
    }

    /// Read the BLS pubkeys for the committee of the provided epoch from the
    /// [ConsensusRegistry], pinned to `header`'s state.
    ///
    /// Convenience wrapper over [`Self::bls_pubkeys_for_epochs_at_header`] for the common
    /// single-epoch case: callers already holding the pin header skip the by-hash header lookup
    /// [`Self::bls_pubkeys_for_epoch_at_block`] performs.
    pub fn bls_pubkeys_for_epoch_at_header(
        &self,
        epoch: u32,
        header: &SealedHeader,
    ) -> StateReadResult<Vec<alloy::primitives::Bytes>> {
        // An arity mismatch on a one-element batch is a bug in this node's code, not a transient
        // fault, so it is chain-global: retrying cannot change it.
        self.bls_pubkeys_for_epochs_at_header(&[epoch], header)?.pop().ok_or_else(|| {
            StateReadError::ChainGlobal("consensus registry batch read returned no result".into())
        })
    }

    /// Read the BLS pubkeys for several epochs' committees from the [ConsensusRegistry], pinned
    /// to `header`'s state.
    ///
    /// One `getCommitteeBlsPubkeys` call per epoch, all executed against ONE pinned EVM via
    /// [`Self::classified_registry_batch_at_header`]; the returned key sets are ordered to
    /// match `epochs`.
    pub fn bls_pubkeys_for_epochs_at_header(
        &self,
        epochs: &[Epoch],
        header: &SealedHeader,
    ) -> StateReadResult<Vec<Vec<alloy::primitives::Bytes>>> {
        let calldatas = epochs
            .iter()
            .map(|&epoch| {
                ConsensusRegistry::getCommitteeBlsPubkeysCall { epoch }.abi_encode().into()
            })
            .collect();
        self.classified_registry_batch_at_header(header, calldatas)
    }

    /// Read the [`ConsensusRegistry`] [`EpochInfo`](ConsensusRegistry::EpochInfo) for `epoch` at
    /// the block identified by `block_hash`.
    ///
    /// Builds an EVM pinned to `block_hash`'s state and issues a single `getEpochInfo(uint32)`
    /// call. The registry serves the window `[current - 3, current + 2]` and reverts
    /// (`InvalidEpoch`) outside it — but a FUTURE epoch inside the window does not revert: it
    /// returns a synthesized projection whose `blockHeight` is 0 (the interface's "not yet
    /// begun" sentinel) with config fields as they would be stamped if the epoch began now.
    /// This method exists to recover a *begun* epoch's record (the epoch manager derives the
    /// previous epoch's block range from its closing block when seeding next-epoch base fees),
    /// so it rejects non-identity records rather than handing callers a sentinel `blockHeight`
    /// they would use as a block-scan floor: the returned `epochId` must equal the request
    /// (re-checked here so a binding drift cannot mislabel a record), and `blockHeight == 0` is
    /// only legitimate for epoch 0, whose first block is genesis.
    ///
    /// Fails with a descriptive error if `block_hash` does not resolve to a sealed header, the
    /// registry call does not succeed, or the returned record is synthesized/mismatched.
    pub fn get_epoch_info_at_block(
        &self,
        epoch: Epoch,
        block_hash: B256,
    ) -> eyre::Result<ConsensusRegistry::EpochInfo> {
        let header = self
            .sealed_header_by_hash(block_hash)?
            .ok_or_else(|| eyre::eyre!("sealed header not found for block hash {block_hash:?}"))?;
        let (mut db, evm_env) = self.pinned_state_and_env(&header)?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        let calldata = ConsensusRegistry::getEpochInfoCall { epoch }.abi_encode().into();
        let info =
            self.call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut tn_evm, calldata)?;

        if info.epochId != epoch {
            eyre::bail!(
                "getEpochInfo({epoch}) at block {block_hash:?} returned a record for epoch {}",
                info.epochId
            );
        }
        if info.blockHeight == 0 && epoch != 0 {
            eyre::bail!(
                "getEpochInfo({epoch}) at block {block_hash:?} returned a not-yet-begun record \
                 (blockHeight 0): epoch {epoch} had not started as of this block"
            );
        }

        Ok(info)
    }

    /// Read worker fee configs from the [`WorkerConfigs`] contract at the block identified by
    /// `block_hash`.
    ///
    /// Returns the on-chain worker count and one [`WorkerFeeConfig`] per worker. Failures are
    /// classified per [`StateReadError`] (see `worker_fee_configs_inner`); `block_hash`
    /// failing to resolve to a sealed header is [`StateReadError::Provider`] — the pinned block
    /// exists on the committee by construction, so a miss reflects this node's local view, not a
    /// chain-global fact.
    pub fn get_worker_fee_configs_at_block(
        &self,
        block_hash: B256,
    ) -> StateReadResult<(usize, Vec<WorkerFeeConfig>)> {
        let header = self.pinned_header_by_hash(block_hash)?;
        self.worker_fee_configs_inner(&header)
    }

    /// Resolve `block_hash` to its sealed header for a pinned read, classified per
    /// [`StateReadError`].
    ///
    /// Both the lookup error and a `None` are [`StateReadError::Provider`]: the pinned block
    /// exists on the committee by construction, so failing to resolve it reflects THIS node's
    /// local view (a provider fault, or a block this node has not indexed yet), not a
    /// chain-global fact. A peer issuing the same read may succeed, so callers must retry or
    /// halt rather than fail open.
    fn pinned_header_by_hash(&self, block_hash: B256) -> StateReadResult<SealedHeader> {
        self.sealed_header_by_hash(block_hash)
            .map_err(|e| {
                StateReadError::Provider(format!("header lookup for {block_hash:?}: {e}"))
            })?
            .ok_or_else(|| {
                StateReadError::Provider(format!(
                    "sealed header not found for block hash {block_hash:?}"
                ))
            })
    }

    /// Read fee configs for all workers from the [`WorkerConfigs`] contract at the given header.
    ///
    /// Builds an EVM against `header`'s state and issues a single `getAllWorkerConfigs()` call.
    /// Returns the on-chain worker count alongside the decoded [`WorkerFeeConfig`]s.
    ///
    /// Failures are classified per [`StateReadError`]. The classification boundary: the state
    /// provider construction and any database fault the EVM hits while lazily reading state are
    /// [`StateReadError::Provider`] (node-local — peers reading the same block may succeed);
    /// everything downstream of a successfully-executing EVM — contract absent (empty return
    /// data), revert, halt, ABI decode, arity mismatch — plus EVM environment construction is
    /// [`StateReadError::ChainGlobal`] (a deterministic product of the pinned block, identical on
    /// every node).
    ///
    /// Fail-open on unknown strategy ids: a strategy this node does not recognize (only possible
    /// when a future contract version introduces one before this node is upgraded) is NOT an
    /// error — it falls back to [`WorkerFeeConfig::Eip1559`] with a warning, preserving liveness
    /// instead of halting all validators on the unrecognized id.
    pub(crate) fn worker_fee_configs_inner(
        &self,
        header: &SealedHeader,
    ) -> StateReadResult<(usize, Vec<WorkerFeeConfig>)> {
        let (mut db, evm_env) = self.pinned_state_and_env(header)?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        let calldata = WorkerConfigs::getAllWorkerConfigsCall {}.abi_encode().into();
        let result = Self::classified_system_call(
            &mut tn_evm,
            SYSTEM_ADDRESS,
            WORKER_CONFIGS_ADDRESS,
            calldata,
        )?;
        let data = match result.result {
            ExecutionResult::Success { output, .. } => output.into_data(),
            e => {
                return Err(StateReadError::ChainGlobal(format!(
                    "failed to read worker configs: {e:?}"
                )))
            }
        };
        let ret =
            <WorkerConfigs::getAllWorkerConfigsCall as alloy::sol_types::SolCall>::abi_decode_returns(
                &data,
            )
            .map_err(|e| {
                StateReadError::ChainGlobal(format!(
                    "worker configs return decode failed (contract absent at this block?): {e}"
                ))
            })?;

        let num_workers = ret.count as usize;
        if ret.strategies.len() != num_workers
            || ret.values.len() != num_workers
            || ret.datas.len() != num_workers
        {
            return Err(StateReadError::ChainGlobal(format!(
                "worker config arity mismatch: count={num_workers}, strategies={}, values={}, datas={}",
                ret.strategies.len(),
                ret.values.len(),
                ret.datas.len(),
            )));
        }

        let mut configs = Vec::with_capacity(num_workers);
        for (worker_id, (&strategy, &value)) in
            ret.strategies.iter().zip(ret.values.iter()).enumerate()
        {
            let config = match strategy {
                0 => WorkerFeeConfig::Eip1559 { target_gas: value },
                1 => WorkerFeeConfig::Static { fee: value },
                s => {
                    // The contract rejects unknown strategies, so this branch only fires when a
                    // future contract version introduces a strategy this node hasn't been
                    // updated to understand. Fall back to EIP-1559 to preserve liveness instead
                    // of halting all validators.
                    tracing::warn!(
                        target: "tn::reth",
                        worker_id,
                        strategy = s,
                        "unknown fee strategy; falling back to strategy 0 (Eip1559)"
                    );
                    WorkerFeeConfig::Eip1559 { target_gas: value }
                }
            };
            configs.push(config);
        }

        Ok((num_workers, configs))
    }

    /// Build an EVM at the canonical tip, execute a read-only [ConsensusRegistry] call, and
    /// decode the returned data to `T`.
    ///
    /// Convenience wrapper over [`Self::read_consensus_registry_batch`] for the common
    /// single-read case (one pinned EVM, one call).
    pub fn read_consensus_registry<T>(&self, calldata: Bytes) -> EvmReadResult<T>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        self.read_consensus_registry_batch(vec![calldata])?.pop().ok_or_else(|| {
            EvmReadError::Internal("consensus registry batch read returned no result".into())
        })
    }

    /// Build an EVM at `header`'s state, execute a read-only [ConsensusRegistry] call, and
    /// decode the returned data to `T`.
    ///
    /// Convenience wrapper over [`Self::read_consensus_registry_batch_at_header`] for the common
    /// single-read case (one pinned EVM, one call).
    pub fn read_consensus_registry_at_header<T>(
        &self,
        header: &SealedHeader,
        calldata: Bytes,
    ) -> EvmReadResult<T>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        self.read_consensus_registry_batch_at_header(header, vec![calldata])?.pop().ok_or_else(
            || EvmReadError::Internal("consensus registry batch read returned no result".into()),
        )
    }

    /// Build a single EVM at the canonical tip and execute several read-only [ConsensusRegistry]
    /// calls against it, decoding each result to `T`.
    ///
    /// Thin specialization of [`Self::read_consensus_registry_batch_at_header`] that pins the
    /// batch to the canonical tip.
    pub fn read_consensus_registry_batch<T>(&self, calldatas: Vec<Bytes>) -> EvmReadResult<Vec<T>>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let canonical_tip = self.canonical_tip();
        debug!(target: "engine", ?canonical_tip, "reading consensus registry batch at canonical tip");
        self.read_consensus_registry_batch_at_header(&canonical_tip, calldatas)
    }

    /// Build a single EVM at `header`'s state and execute several read-only [ConsensusRegistry]
    /// calls against it, decoding each result to `T`.
    ///
    /// Every calldata in a batch must decode to the same Solidity type `T`. The current caller is
    /// the tip-pinned five `getValidatorsInfo(status)` reads → `Vec<ValidatorInfo>` (through
    /// [`Self::read_consensus_registry_batch`]). Consensus-critical pinned batches use the
    /// classified sibling [`Self::classified_registry_batch_at_header`] instead; this form stays
    /// for the RPC layer, which maps [`EvmReadError::Revert`]'s raw output bytes into an
    /// eth_call-style error that [`StateReadError`]'s string-only variants cannot carry.
    ///
    /// All calls observe ONE pinned state snapshot, so a multi-call query (e.g. unioning
    /// per-status validator sets) cannot straddle a block commit and double-count or drop a
    /// validator that changes status between reads. Each call still runs under its own fresh
    /// system-call gas budget (`transact_system_call`), so splitting a large query across calls
    /// keeps gas bounded per call.
    pub fn read_consensus_registry_batch_at_header<T>(
        &self,
        header: &SealedHeader,
        calldatas: Vec<Bytes>,
    ) -> EvmReadResult<Vec<T>>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        // Downgrading the classification is lossless here: both setup failures already collapsed
        // into `Internal` before the classified helper existed.
        let (mut db, evm_env) =
            self.pinned_state_and_env(header).map_err(|e| EvmReadError::Internal(e.to_string()))?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        // reuse the one pinned EVM for every read; `call_consensus_registry` is non-committing,
        // so each read sees the same base state.
        calldatas
            .into_iter()
            .map(|calldata| self.call_consensus_registry(&mut tn_evm, calldata))
            .collect()
    }

    /// Build a single EVM at `header`'s state and execute several read-only [ConsensusRegistry]
    /// calls against it, decoding each result to `T`, with failures classified per
    /// [`StateReadError`].
    ///
    /// The [`StateReadResult`]-typed sibling of
    /// [`Self::read_consensus_registry_batch_at_header`]: same ONE-pinned-EVM batching and same
    /// result ordering, but each call routes through [`Self::classified_registry_read`] so a
    /// node-local provider fault stays distinguishable from a chain-global failure. Every
    /// consensus-critical pinned batch — the per-epoch committee reads
    /// ([`Self::bls_pubkeys_for_epochs_at_header`]) — uses this form.
    fn classified_registry_batch_at_header<T>(
        &self,
        header: &SealedHeader,
        calldatas: Vec<Bytes>,
    ) -> StateReadResult<Vec<T>>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let (mut db, evm_env) = self.pinned_state_and_env(header)?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        // reuse the one pinned EVM for every read; `classified_registry_read` is non-committing,
        // so each read sees the same base state.
        calldatas
            .into_iter()
            .map(|calldata| Self::classified_registry_read(&mut tn_evm, calldata))
            .collect()
    }

    /// Build the state database and EVM environment pinned to `header`, classified per
    /// [`StateReadError`].
    ///
    /// Returns the `(db, env)` pair rather than a constructed EVM because `create_evm(&mut db,
    /// env)` borrows `db`, so the caller must own both. This is the ONE place a pinned read's
    /// state is built — every `*_at_header` / `*_at_block` reader here routes through it.
    ///
    /// The classification split: state-provider construction is [`StateReadError::Provider`]
    /// (node-local — this node's database failing to resolve the pinned block's state, where a
    /// peer issuing the same read may succeed), while EVM environment construction is
    /// [`StateReadError::ChainGlobal`] (a deterministic function of the header's own fields and
    /// the chain spec, identical on every node).
    ///
    /// ARCHIVE-MODE ASSUMPTION: this node never constructs a pruner (`PruningArgs` are built
    /// with every field disabled and no `PrunerBuilder` exists in the repo), so
    /// `state_by_block_hash` always resolves fully indexed history. If pruning is ever enabled,
    /// reth's `HistoricalStateProvider` can hit a missing history shard and return
    /// `HistoryInfo::MaybeInPlainState`, silently falling back to TIP state for this "pinned"
    /// read — exactly the nondeterminism pinning exists to prevent. Revisit every pinned read
    /// before enabling pruning.
    fn pinned_state_and_env(
        &self,
        header: &SealedHeader,
    ) -> StateReadResult<(State<StateProviderDatabase<StateProviderBox>>, EvmEnv)> {
        let db = self.read_only_state_db(header.hash()).map_err(|e| {
            StateReadError::Provider(format!("state provider at {}: {e}", header.hash()))
        })?;
        let evm_env = self.inner.evm_config.evm_env(header).map_err(|e| {
            StateReadError::ChainGlobal(format!("evm env for {}: {e}", header.hash()))
        })?;
        Ok((db, evm_env))
    }

    /// Extract the epoch number from a header's nonce.
    pub fn extract_epoch_from_header(header: &ExecHeader) -> Epoch {
        let nonce: u64 = header.nonce.into();
        (nonce >> 32) as u32
    }

    /// Read the CURRENT epoch number and [`EpochInfo`](ConsensusRegistry::EpochInfo) from the
    /// [`ConsensusRegistry`] at `header`, with failures classified per [`StateReadError`].
    ///
    /// This is the close-time identity read for the epoch manager's `adjust_base_fees`: at an
    /// epoch's closing block the registry state has already crossed to the entered epoch
    /// (`concludeEpoch` ran inside that block), so the returned info is the entered epoch's record
    /// and its `blockHeight` must equal `header.number + 1`. It reads exactly what
    /// [`Self::epoch_state_at_header`] reads for the same check but skips the committee/BLS/
    /// epoch-start lookups (a gating check needs only the epoch identity) and — unlike that
    /// method, whose failures collapse into `eyre` strings — keeps node-local provider faults
    /// (NOT committee-deterministic: retry or halt) distinguishable from chain-global failures
    /// (committee-deterministic: fail-open stays consistent).
    pub fn get_current_epoch_info_at_header(
        &self,
        header: &SealedHeader,
    ) -> StateReadResult<(Epoch, ConsensusRegistry::EpochInfo)> {
        let (mut db, evm_env) = self.pinned_state_and_env(header)?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        // both reads observe the ONE pinned EVM state
        let epoch: Epoch = Self::classified_registry_read(
            &mut tn_evm,
            ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into(),
        )?;
        let epoch_info: ConsensusRegistry::EpochInfo = Self::classified_registry_read(
            &mut tn_evm,
            ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into(),
        )?;
        Ok((epoch, epoch_info))
    }

    /// Execute a read-only [`ConsensusRegistry`] call on `evm` and decode the result, classifying
    /// failures per [`StateReadError`].
    ///
    /// The [`StateReadError`]-typed sibling of [`Self::call_consensus_registry`]: revert, halt,
    /// and decode failures are all deterministic products of the pinned chain state
    /// ([`StateReadError::ChainGlobal`]); only a database fault inside the EVM (via
    /// [`Self::classified_system_call`]) is node-local ([`StateReadError::Provider`]).
    fn classified_registry_read<DB, T>(evm: &mut TNEvm<DB>, calldata: Bytes) -> StateReadResult<T>
    where
        DB: alloy_evm::Database,
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let state = Self::classified_system_call(
            evm,
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        )?;
        match state.result {
            ExecutionResult::Success { output, .. } => {
                alloy::sol_types::SolValue::abi_decode(&output.into_data()).map_err(|e| {
                    StateReadError::ChainGlobal(format!(
                        "registry return decode failed (contract absent at this block?): {e}"
                    ))
                })
            }
            ExecutionResult::Revert { output, .. } => Err(StateReadError::ChainGlobal(format!(
                "registry call reverted: {:?}",
                alloy::sol_types::decode_revert_reason(&output)
            ))),
            ExecutionResult::Halt { reason, gas_used } => Err(StateReadError::ChainGlobal(
                format!("registry call halted: {reason:?} (gas {gas_used})"),
            )),
        }
    }

    /// Execute a read-only system call on `evm`, classifying failures per [`StateReadError`].
    ///
    /// An [`EVMError::Database`] is a node-local provider fault surfaced by the EVM's lazy state
    /// reads (the state provider is only CONSTRUCTED up front; account/storage/bytecode loads
    /// happen during execution, so an MDBX/provider I/O fault lands here) — classified
    /// [`StateReadError::Provider`]. Every other transact failure derives deterministically from
    /// the pinned block and calldata, so it is [`StateReadError::ChainGlobal`].
    fn classified_system_call<DB>(
        evm: &mut TNEvm<DB>,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> StateReadResult<ResultAndState>
    where
        DB: alloy_evm::Database,
    {
        evm.transact_system_call(caller, contract, calldata).map_err(|e| match e {
            EVMError::Database(db_err) => {
                StateReadError::Provider(format!("system call state read failed: {db_err}"))
            }
            other => {
                StateReadError::ChainGlobal(format!("system call failed reading state: {other}"))
            }
        })
    }

    /// Helper function to call `ConsensusRegistry` state on-chain.
    pub(crate) fn call_consensus_registry<DB, T>(
        &self,
        evm: &mut TNEvm<DB>,
        calldata: Bytes,
    ) -> EvmReadResult<T>
    where
        DB: alloy_evm::Database,
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let state = self
            .read_state_on_chain(evm, SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;

        // retrieve data from state, distinguishing user-triggerable reverts from node faults
        match state.result {
            ExecutionResult::Success { output, .. } => {
                let data = output.into_data();
                // use SolValue to decode the result
                alloy::sol_types::SolValue::abi_decode(&data).map_err(|e| {
                    EvmReadError::Internal(format!("registry return decode failed: {e}"))
                })
            }
            ExecutionResult::Revert { output, .. } => Err(EvmReadError::Revert {
                reason: alloy::sol_types::decode_revert_reason(&output),
                output,
            }),
            ExecutionResult::Halt { reason, gas_used } => Err(EvmReadError::Internal(format!(
                "registry call halted: {reason:?} (gas {gas_used})"
            ))),
        }
    }

    /// Read state on-chain.
    pub(crate) fn read_state_on_chain<DB>(
        &self,
        evm: &mut TNEvm<DB>,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> TnRethResult<ResultAndState>
    where
        DB: alloy_evm::Database,
    {
        // read from state
        let res = match evm.transact_system_call(caller, contract, calldata) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", ?caller, ?contract, "failed to read state: {}", e);
                return Err(TnRethError::EVMCustom(format!(
                    "system call failed reading state: {e}"
                )));
            }
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        system_calls::ConsensusRegistry::ValidatorStatus,
        test_utils::{
            consensus_output_for_tests, create_committee_from_state,
            execute_payload_and_update_canonical_chain, governance_burn_tx,
            governance_owner_factory, test_genesis_with_consensus_registry, TransactionFactory,
        },
        RethChainSpec,
    };
    use alloy::primitives::utils::parse_ether;
    use rand::{rngs::StdRng, SeedableRng as _};
    use reth_revm::{
        cached::CachedReads, database::StateProviderDatabase, DatabaseCommit as _, State,
    };
    use std::sync::Arc;
    use tempfile::TempDir;
    use tn_config::NodeInfo;
    use tn_types::{
        gas_accumulator::RewardsCounter, generate_proof_of_possession_bls_for_test, BlsKeypair,
        GenesisAccount, NodeP2pInfo, TaskManager, U256,
    };

    /// In-protocol `ConsensusRegistry` fork over the PRE-fork testnet registry.
    ///
    /// `test_genesis()` embeds the committed testnet `genesis.yaml`, whose `ConsensusRegistry`
    /// account carries the pre-fork runtime code and validator storage with NO per-status sets —
    /// the exact on-chain shape the fork upgrades in place. The epoch-closing block that
    /// concludes `FORK_EPOCH - 1` swaps in the new runtime and runs `migrateValidatorSets()`
    /// FIRST, then the rewards + conclude calls run on the new code over the byte-identical
    /// preserved storage.
    ///
    /// Asserts: the pre-fork code does not answer the new-ABI eligible-count call; post-fork the
    /// new code is live and the migration populated a non-empty eligible set; a preserved BLS
    /// pubkey survives the swap as 96-byte compressed; and the fork block's `state_root` is
    /// identical across two independent executions (determinism — every node re-derives the
    /// same root).
    ///
    /// NOTE: the fixture is the LIVE pre-fork deployment, pinned by
    /// `tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH` and its tn-types pin test. If
    /// `chain-configs/testnet/genesis.yaml` is ever regenerated from the current (post-fork)
    /// artifact, the `pre.is_err()` probe below fails first — that means the fixture no longer
    /// mirrors the chain this fork targets; reassess the fork plan rather than updating the
    /// probes.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_consensus_registry_fork_swaps_code_and_migrates() -> eyre::Result<()> {
        // pre-fork fixture: old registry code + validator storage, no per-status sets
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let genesis_header = chain.sealed_genesis_header();

        // fork fires when the concluding epoch + 1 == FORK_EPOCH
        let concluding_epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH - 1;

        // one payload, cloned across both executions, so the determinism check compares
        // byte-identical inputs (`new_for_test` otherwise randomizes
        // beneficiary/mix_hash/digest per call)
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        // --- env 1: pre-fork state must NOT answer the new-ABI eligible-count call (old code) ---
        let tmp1 = TempDir::new().unwrap();
        let tm1 = TaskManager::new("fork test env1");
        let env1 = RethEnv::new_for_temp_chain(chain.clone(), tmp1.path(), &tm1, None).unwrap();
        {
            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .evm_config()
                .evm_factory()
                .create_evm(&mut db, env1.evm_config().evm_env(genesis_header.header())?);
            let pre = env1.call_consensus_registry::<_, U256>(
                &mut evm,
                ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
            );
            assert!(
                pre.is_err(),
                "pre-fork registry (old code) must not expose getEligibleValidatorCount"
            );
        }

        // --- produce the fork boundary block on the production path ---
        let block = execute_payload_and_update_canonical_chain(&env1, payload.clone(), vec![])?;
        let header = block.recovered_block.clone_sealed_header();
        let produced_state_root = header.state_root;

        // --- post-fork: new code is live and the sets are migrated ---
        {
            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .evm_config()
                .evm_factory()
                .create_evm(&mut db, env1.evm_config().evm_env(header.header())?);

            let eligible = env1
                .call_consensus_registry::<_, U256>(
                    &mut evm,
                    ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
                )
                .expect("post-fork getEligibleValidatorCount must succeed on the swapped-in code");
            assert!(eligible > U256::ZERO, "migration must populate a non-zero eligible count");

            // getValidators(Active) now returns address[] (new ABI) from the migrated set
            let active = env1
                .call_consensus_registry::<_, Vec<Address>>(
                    &mut evm,
                    ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                        .abi_encode()
                        .into(),
                )
                .expect("getValidators(Active) must succeed on new code");
            assert!(!active.is_empty(), "migrated Active set must be non-empty");

            // stored BLS pubkey survives the code swap untouched: still 96-byte compressed
            let bls = env1
                .call_consensus_registry::<_, Bytes>(
                    &mut evm,
                    ConsensusRegistry::getBlsPubkeyCall { validatorAddress: active[0] }
                        .abi_encode()
                        .into(),
                )
                .expect("getBlsPubkey must succeed");
            assert_eq!(bls.len(), 96, "preserved BLS pubkey must remain 96-byte compressed");
        }

        // --- determinism: an independent execution of the identical block yields the same root ---
        let tmp2 = TempDir::new().unwrap();
        let tm2 = TaskManager::new("fork test env2");
        let env2 = RethEnv::new_for_temp_chain(chain.clone(), tmp2.path(), &tm2, None).unwrap();
        let block2 = execute_payload_and_update_canonical_chain(&env2, payload, vec![])?;
        assert_eq!(
            block2.recovered_block.clone_sealed_header().state_root,
            produced_state_root,
            "fork block state_root must be identical across independent executions"
        );

        Ok(())
    }

    /// Pre-fork epoch conclusion over the LIVE adiri registry code must speak the legacy ABI.
    ///
    /// `test_genesis()` embeds the committed testnet `genesis.yaml` — the registry account
    /// carries the pre-fork runtime code (pinned by `CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`)
    /// whose `getValidators(uint8)` returns `ValidatorInfo[]` and which has no
    /// `getValidatorsInfo`. Concluding a NORMAL (non-fork-boundary) epoch on this state
    /// exercises the code-hash gate in `read_committee_eligible_pool`: without the gate the
    /// close reverts fatally (the post-fork ABI is absent on-chain) — the exact path a fresh
    /// node onboarding to adiri or a full resync executes for every pre-fork epoch boundary.
    ///
    /// Asserts: the closing block executes; the registry code hash is untouched (no swap —
    /// epoch 3 is far from the fork boundary); the post-fork-only eligible-count call still
    /// fails; and the block's `state_root` is identical across two independent executions.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_pre_fork_epoch_close_uses_legacy_registry_read() -> eyre::Result<()> {
        // pre-fork fixture: the committed adiri genesis (old registry code + validator storage)
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let genesis_header = chain.sealed_genesis_header();

        // a normal epoch close, far from the fork boundary (`u32::MAX - 1`), so no swap can fire
        let concluding_epoch = 3;
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        let tmp1 = TempDir::new().unwrap();
        let tm1 = TaskManager::new("legacy read test env1");
        let env1 = RethEnv::new_for_temp_chain(chain.clone(), tmp1.path(), &tm1, None).unwrap();

        // --- pre-probes: readable failures if the committed genesis fixture is regenerated ---
        {
            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .evm_config()
                .evm_factory()
                .create_evm(&mut db, env1.evm_config().evm_env(genesis_header.header())?);

            // legacy ABI: getValidators(Active) folds the committee-eligible pool into one
            // ValidatorInfo[] response
            let pool = env1
                .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                    &mut evm,
                    ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                        .abi_encode()
                        .into(),
                )
                .expect("pre-fork registry must answer the legacy getValidators(Active) read");
            assert!(!pool.is_empty(), "legacy eligible pool must be non-empty");

            let committee_size = env1
                .call_consensus_registry::<_, u16>(
                    &mut evm,
                    ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
                )
                .expect("pre-fork registry must answer getNextCommitteeSize");
            assert!(
                committee_size as usize <= pool.len(),
                "genesis fixture must hold enough eligible validators ({}) for the next \
                 committee ({committee_size}) — was chain-configs/testnet/genesis.yaml \
                 regenerated?",
                pool.len(),
            );
        }

        // --- the epoch-closing block executes via the legacy read (without the gate: fatal) ---
        let block = execute_payload_and_update_canonical_chain(&env1, payload.clone(), vec![])?;
        let header = block.recovered_block.clone_sealed_header();
        let produced_state_root = header.state_root;

        // --- post: still the pre-fork code — no swap fired, post-fork ABI still absent ---
        {
            use reth_provider::StateProvider as _;
            let code = env1
                .latest()?
                .account_code(&CONSENSUS_REGISTRY_ADDRESS)?
                .expect("registry account must have code");
            assert_eq!(
                code.0.hash_slow(),
                tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH,
                "a normal pre-fork epoch close must not swap the registry code"
            );

            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .evm_config()
                .evm_factory()
                .create_evm(&mut db, env1.evm_config().evm_env(header.header())?);
            let eligible = env1.call_consensus_registry::<_, U256>(
                &mut evm,
                ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
            );
            assert!(
                eligible.is_err(),
                "post-fork-only getEligibleValidatorCount must still fail on the pre-fork code"
            );
        }

        // --- determinism: an independent execution of the identical block yields the same root ---
        let tmp2 = TempDir::new().unwrap();
        let tm2 = TaskManager::new("legacy read test env2");
        let env2 = RethEnv::new_for_temp_chain(chain.clone(), tmp2.path(), &tm2, None).unwrap();
        let block2 = execute_payload_and_update_canonical_chain(&env2, payload, vec![])?;
        assert_eq!(
            block2.recovered_block.clone_sealed_header().state_root,
            produced_state_root,
            "pre-fork epoch-close state_root must be identical across independent executions"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_close_epochs() -> eyre::Result<()> {
        let validator_1 = Address::from_slice(&[0x11; 20]);
        let validator_3 = Address::from_slice(&[0x33; 20]);
        let validator_4 = Address::from_slice(&[0x44; 20]);
        let validator_5 = Address::from_slice(&[0x55; 20]);

        // create validator wallet for staking later
        let mut new_validator_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));

        // create validator wallet for exiting later
        let mut validator_2_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(2));
        let validator_2_address = validator_2_eoa.address();

        // create initial validators for testing
        let all_validators = [
            validator_1,
            validator_2_address,
            validator_3,
            validator_4,
            validator_5,
            new_validator_eoa.address(),
        ];

        // create validator info objects for each address
        let mut validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                // use deterministic seed
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        debug!(target: "engine", "created validators for consensus registry {:#?}", validators);

        let epoch_duration = 60 * 60 * 24; // 24hrs
        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: epoch_duration,
        };

        // create genesis with funded governance safe
        let mut governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([
            (
                governance,
                GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)), // 50mil TEL
            ),
            (
                new_validator_eoa.address(),
                GenesisAccount::default()
                    .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))), // double stake
            ),
            (
                validator_2_address,
                GenesisAccount::default()
                    .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))), // double stake
            ),
        ]);

        // remove last validator so only 5 form the initial committees
        let new_validator = validators.pop().expect("six validators");

        // update genesis with consensus registry storage
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            vec![(0u8, 30_000_000u64)],
        )?;

        // update genesis again to include stake for new validator
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let calldata =
            ConsensusRegistry::mintCall { validatorAddress: new_validator.execution_address }
                .abi_encode()
                .into();
        let mint_nft = governance_multisig.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            calldata,
        );
        let proof = ConsensusRegistry::ProofOfPossession {
            signature: new_validator.proof_of_possession.to_bytes().into(),
        };
        let calldata = ConsensusRegistry::stakeCall {
            blsPubkey: new_validator.bls_public_key.to_bytes().into(),
            proofOfPossession: proof,
        }
        .abi_encode()
        .into();
        let stake_tx = new_validator_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            initial_stake_config.stakeAmount,
            calldata,
        );
        let calldata = ConsensusRegistry::activateCall {}.abi_encode().into();
        let activate_tx = new_validator_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            calldata,
        );

        // create new env with initialized consensus registry for tests
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let mut expected_epoch = 0;
        let expected_committee = validators.iter().map(|v| v.execution_address).collect();
        let mut expected_epoch_info = ConsensusRegistry::EpochInfo {
            committee: expected_committee,
            blockHeight: 0,
            epochId: 0,
            epochDuration: epoch_duration,
            epochIssuance: initial_stake_config.epochIssuance,
            stakeVersion: 0,
        };

        // assert epoch state is correct
        let EpochState { epoch, epoch_info, validators: committee, bls_pubkeys, epoch_start } =
            reth_env.epoch_state_from_canonical_tip()?;
        debug!(target:"evm", ?epoch, ?epoch_info, ?committee, ?epoch, "original epoch state from canonical tip in genesis");
        assert_eq!(epoch, expected_epoch);
        assert_eq!(epoch_start, chain.genesis_timestamp());
        assert_eq!(epoch_info, expected_epoch_info);

        // assert committee matches validator args for constructor
        for v in &validators {
            let idx = committee
                .iter()
                .position(|info| info.validatorAddress == v.execution_address)
                .expect("validator on-chain");
            assert_eq!(bls_pubkeys[idx].as_ref(), v.bls_public_key.to_bytes());
            let on_chain = &committee[idx];
            assert_eq!(on_chain.activationEpoch, epoch);
            assert_eq!(on_chain.exitEpoch, 0);
            assert!(!on_chain.isRetired);
            assert_eq!(on_chain.stakeVersion, 0);
        }

        // close epoch with deterministic signature as source of randomness
        // and execute the first block with txs for new validator to stake
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![mint_nft, stake_tx, activate_tx],
        )?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // now close the first epoch
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block2.recovered_block.clone_sealed_header();

        // system-call state hygiene through the close: SYSTEM_ADDRESS is a caller convention
        // and must never enter the bundle (the executor strips it before every commit), and the
        // closing block carries exactly one reverts entry — the wrapper's single post-finish
        // merge (an in-finish merge would push a phantom second entry)
        assert!(
            !block2.execution_output.state.state.contains_key(&SYSTEM_ADDRESS),
            "SYSTEM_ADDRESS must not enter the epoch-closing bundle"
        );
        assert_eq!(
            block2.execution_output.state.reverts.len(),
            1,
            "one merged reverts entry per closing block"
        );

        // a default (empty) RewardsCounter close allocates nothing: the boundary's
        // applyIncentives ran with an empty rewardInfos array, so every genesis committee
        // member still reads zero rewards
        for v in &validators {
            let rewards = reth_env.read_consensus_registry::<U256>(
                ConsensusRegistry::getRewardsCall { validatorAddress: v.execution_address }
                    .abi_encode()
                    .into(),
            )?;
            assert!(rewards.is_zero(), "empty-counter close must allocate no rewards");
        }

        // now close the second epoch so the new validator is active
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 3, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block3 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block3.recovered_block.clone_sealed_header();

        // read new epoch state
        let EpochState { epoch, epoch_info, validators: committee, bls_pubkeys, epoch_start } =
            reth_env.epoch_state_from_canonical_tip()?;
        debug!(target: "evm", ?epoch, ?epoch_info, ?committee, ?epoch, "new epoch state from canonical tip");
        // assert epoch info updated
        expected_epoch_info.blockHeight = 4;
        expected_epoch_info.epochId = expected_epoch;
        assert_eq!(expected_epoch, epoch);
        assert_eq!(epoch_start, canonical_header.timestamp);
        assert_eq!(epoch_info, expected_epoch_info);

        // create evm to read custom contract call
        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let mut tn_evm = reth_env
            .evm_config()
            .evm_factory()
            .create_evm(&mut db, reth_env.evm_config().evm_env(canonical_header.header())?);

        // read new committee (always 2 epochs ahead)
        let calldata = ConsensusRegistry::getEpochInfoCall { epoch: epoch + 1 }.abi_encode().into();
        let new_epoch_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut tn_evm, calldata)?;

        // replay-critical order normalization: the installed committee is address-sorted
        // (generate_conclude_epoch_calldata sorts after the shuffle) — asserted as a property,
        // independent of the exact-membership golden below
        assert!(new_epoch_info.committee.is_sorted(), "installed committee must be address-sorted");

        // ensure validators in increasing order by address
        let expected_new_committee = vec![
            validator_1,
            validator_3,
            validator_4,
            validator_2_address,
            new_validator.execution_address,
        ];

        let expected = ConsensusRegistry::EpochInfo {
            committee: expected_new_committee,
            blockHeight: 0,
            epochId: expected_epoch + 1,
            // future epoch info is projected from the latest stake config
            epochDuration: epoch_duration,
            epochIssuance: initial_stake_config.epochIssuance,
            stakeVersion: 0,
        };

        debug!(target: "engine", "new epoch info:{:#?}", new_epoch_info);
        assert_eq!(new_epoch_info, expected);

        // assert new committee matches validator args for constructor
        // this should be the case for the first 3 epochs
        for v in &validators {
            let idx = committee
                .iter()
                .position(|info| info.validatorAddress == v.execution_address)
                .expect("validator on-chain");
            assert_eq!(bls_pubkeys[idx].as_ref(), v.bls_public_key.to_bytes());
            let on_chain = &committee[idx];
            assert_eq!(on_chain.activationEpoch, 0);
            assert_eq!(on_chain.exitEpoch, 0);
            assert!(!on_chain.isRetired);
            assert_eq!(on_chain.stakeVersion, 0);
        }

        // submit validator 2 exit request
        let calldata = ConsensusRegistry::beginExitCall {}.abi_encode().into();
        let begin_exit_tx = validator_2_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            calldata,
        );
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 4, false);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block4 =
            execute_payload_and_update_canonical_chain(&reth_env, payload, vec![begin_exit_tx])?;
        let canonical_header = block4.recovered_block.clone_sealed_header();

        // close epoch
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 5, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block5 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block5.recovered_block.clone_sealed_header();

        // create evm to read latest state
        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let mut tn_evm = reth_env
            .evm_config()
            .evm_factory()
            .create_evm(&mut db, reth_env.evm_config().evm_env(canonical_header.header())?);

        // assert validator 2 is pending exit
        let calldata =
            ConsensusRegistry::getValidatorCall { validatorAddress: validator_2_address }
                .abi_encode()
                .into();
        let validator_2_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::ValidatorInfo>(
                &mut tn_evm,
                calldata,
            )?;
        debug!(target: "engine", ?validator_2_info, "getting validator 2 info");
        assert_eq!(validator_2_info.currentStatus, ValidatorStatus::PendingExit);

        // With the per-status sets, `getValidatorsInfo(Active)` returns strictly-active validators;
        // the committee-eligible pool is the union of Active/PendingActivation/PendingExit, so the
        // pending-exit validator is queried separately rather than partitioned out of `Active`.
        let active_validators = reth_env
            .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                &mut tn_evm,
                ConsensusRegistry::getValidatorsInfoCall { status: ValidatorStatus::Active.into() }
                    .abi_encode()
                    .into(),
            )?;
        assert_eq!(active_validators.len(), 5);

        // validator 2 should be the single pending-exit validator
        let pending_exit = reth_env
            .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                &mut tn_evm,
                ConsensusRegistry::getValidatorsInfoCall {
                    status: ValidatorStatus::PendingExit.into(),
                }
                .abi_encode()
                .into(),
            )?;
        assert_eq!(pending_exit.len(), 1);
        assert_eq!(
            pending_exit.first().expect("one pending validator").validatorAddress,
            validator_2_address
        );

        // close epoch again to exit validator
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 6, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block6 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block6.recovered_block.clone_sealed_header();
        // close epoch again
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 7, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block7 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block7.recovered_block.clone_sealed_header();

        // create evm to read latest state
        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let mut tn_evm = reth_env
            .evm_config()
            .evm_factory()
            .create_evm(&mut db, reth_env.evm_config().evm_env(canonical_header.header())?);

        // assert validator 2 is pending exit
        let calldata =
            ConsensusRegistry::getValidatorCall { validatorAddress: validator_2_address }
                .abi_encode()
                .into();
        let validator_2_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::ValidatorInfo>(
                &mut tn_evm,
                calldata,
            )?;
        debug!(target: "engine", ?validator_2_info, "getting validator 2 info");
        assert_eq!(validator_2_info.currentStatus, ValidatorStatus::Exited);

        // read all active validators from consensus registry
        let calldata =
            ConsensusRegistry::getValidatorsInfoCall { status: ValidatorStatus::Active.into() }
                .abi_encode()
                .into();
        let eligible_validators = reth_env
            .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                &mut tn_evm,
                calldata,
            )?;

        assert_eq!(eligible_validators.len(), 5);

        // ensure validator 2 has fully exited
        let (pending_exit, active_validators): (Vec<_>, Vec<_>) = eligible_validators
            .into_iter()
            .partition(|v| v.currentStatus == ValidatorStatus::PendingExit);

        assert_eq!(pending_exit.len(), 0);
        assert_eq!(active_validators.len(), 5);
        for v in active_validators {
            assert!(v.validatorAddress != validator_2_address);
        }

        Ok(())
    }

    /// Governance `burn` forcibly ejects a current-committee validator mid-epoch.
    ///
    /// Pins the on-chain behavior the node's epoch-record layer must tolerate: the stored
    /// committee arrays for the current and both future epochs shrink via swap-and-pop (the
    /// last element moves into the ejected slot — order is NOT preserved), the next committee
    /// size auto-decrements to the eligible count, the validator is permanently retired with
    /// its stake confiscated, and the epoch still closes cleanly on-chain (the boundary
    /// `concludeEpoch` system call succeeds over the shrunken committee). A direct
    /// `applyIncentives` + `concludeEpoch` sequence afterwards exercises the `isRetired` skip
    /// branch in `applyIncentives`: the burned validator earns nothing while a surviving
    /// validator accrues rewards.
    #[tokio::test]
    async fn test_burn_ejects_current_committee_validator_mid_epoch() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Burn Eject Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // genesis committee of 5 for epoch 0
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 0);
        assert_eq!(committee.len(), 5);

        // capture pre-burn committee pubkeys for the current and both future epochs
        let pre_burn = (0u32..=2)
            .map(|e| reth_env.bls_pubkeys_for_epoch_at_block(e, reth_env.canonical_tip().hash()))
            .collect::<StateReadResult<Vec<_>>>()?;

        // eject a middle slot so the swap-and-pop reorder is visible
        let target = committee[1].validatorAddress;
        let target_bls = pre_burn[0][1].clone();

        // pre-burn: full stake outstanding, no rewards
        let (outstanding, initial_stake, rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: target }
                    .abi_encode()
                    .into(),
            )?;
        assert_eq!(outstanding, initial_stake);
        assert!(rewards.is_zero());

        // block 1: governance burns the validator mid-epoch
        let mut governance = governance_owner_factory();
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // the validator is permanently retired with its stake confiscated
        let retired = reth_env.read_consensus_registry::<bool>(
            ConsensusRegistry::isRetiredCall { validatorAddress: target }.abi_encode().into(),
        )?;
        assert!(retired, "burned validator is permanently retired");
        let (outstanding, _initial, rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: target }
                    .abi_encode()
                    .into(),
            )?;
        assert!(outstanding.is_zero(), "outstanding stake confiscated to issuance");
        assert!(rewards.is_zero());

        // current + both future committees shrink with EXACT swap-and-pop order: the last
        // element moves into the burned slot and the array truncates by one
        for e in 0u32..=2 {
            let post =
                reth_env.bls_pubkeys_for_epoch_at_block(e, reth_env.canonical_tip().hash())?;
            let mut expected = pre_burn[e as usize].clone();
            let idx = expected
                .iter()
                .position(|k| k == &target_bls)
                .expect("burned validator in every pre-burn committee");
            let last = expected.len() - 1;
            expected.swap(idx, last);
            expected.truncate(last);
            assert_eq!(post.len(), 4);
            assert!(!post.contains(&target_bls));
            assert_eq!(post, expected, "swap-and-pop order for epoch {e}");
        }

        // positional zip pin: the address committee and pubkey committee stay index-aligned
        // (the node zips these arrays by position to build its committee)
        for e in 0u32..=2 {
            let infos =
                reth_env.validators_for_epoch_at_block(e, reth_env.canonical_tip().hash())?;
            let keys =
                reth_env.bls_pubkeys_for_epoch_at_block(e, reth_env.canonical_tip().hash())?;
            assert_eq!(infos.len(), keys.len());
            for (info, key) in infos.iter().zip(keys.iter()) {
                let direct =
                    reth_env.get_bls_pubkey(canonical_header.hash(), info.validatorAddress)?;
                assert_eq!(&direct, key, "epoch {e} committee arrays zip positionally");
            }
        }

        // committee size auto-decrements to the eligible count
        let next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(next_size, 4);
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(4));

        // block 2: close the epoch — the boundary concludeEpoch system call must
        // succeed over the shrunken committee (on-chain close survives mid-epoch ejection)
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block2.recovered_block.clone_sealed_header();

        // post-close: epoch 1 runs the 4-member committee; the newly shuffled committee
        // (two epochs ahead) is also 4 members and excludes the burned validator
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 1);
        assert_eq!(committee.len(), 4);
        assert!(committee.iter().all(|v| v.validatorAddress != target));
        let shuffled =
            reth_env.bls_pubkeys_for_epoch_at_block(3, reth_env.canonical_tip().hash())?;
        assert_eq!(shuffled.len(), 4);
        assert!(!shuffled.contains(&target_bls));

        // applyIncentives with rewards for the burned + a surviving validator: the isRetired
        // branch skips the burned validator while the survivor accrues rewards. Then
        // concludeEpoch over the shrunken committee. (A hand-rolled pair, not the protocol's
        // boundary sequence - the executor issues three calls with applySlashes interposed; see
        // test_epoch_boundary_slash_ejects_through_production_close.)
        let alive = committee[0].validatorAddress;
        let mut tn_evm = reth_env.tn_evm(canonical_header.hash())?;
        let mut new_committee: Vec<Address> =
            committee.iter().map(|v| v.validatorAddress).collect();
        new_committee.sort();

        let incentives_calldata = ConsensusRegistry::applyIncentivesCall {
            rewardInfos: vec![
                ConsensusRegistry::RewardInfo {
                    validatorAddress: target,
                    consensusHeaderCount: U256::from(5),
                },
                ConsensusRegistry::RewardInfo {
                    validatorAddress: alive,
                    consensusHeaderCount: U256::from(5),
                },
            ],
        }
        .abi_encode()
        .into();
        let mut res = tn_evm.transact_system_call(
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            incentives_calldata,
        )?;
        assert!(res.result.is_success(), "applyIncentives succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);

        let conclude_calldata =
            ConsensusRegistry::concludeEpochCall { newCommittee: new_committee }
                .abi_encode()
                .into();
        let mut res = tn_evm.transact_system_call(
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            conclude_calldata,
        )?;
        assert!(res.result.is_success(), "concludeEpoch succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);
        let burned_rewards = reth_env.call_consensus_registry::<_, U256>(
            &mut tn_evm,
            ConsensusRegistry::getRewardsCall { validatorAddress: target }.abi_encode().into(),
        )?;
        let alive_rewards = reth_env.call_consensus_registry::<_, U256>(
            &mut tn_evm,
            ConsensusRegistry::getRewardsCall { validatorAddress: alive }.abi_encode().into(),
        )?;
        assert!(burned_rewards.is_zero(), "retired validator skipped by the rewards stage");
        assert!(alive_rewards > U256::ZERO, "surviving validator accrues rewards");

        Ok(())
    }

    /// The epoch-boundary slashing sequence: a slash-to-zero ejection lands mid-sequence and the
    /// close still succeeds because the committee is assembled from post-slash state.
    ///
    /// Hand-rolls the three-call boundary order - `applyIncentives`, then `applySlashes`, then
    /// `concludeEpoch` - on a raw `tn_evm`, with a slash that empties a validator's stake. Pins
    /// the CONTRACT-side outcome an automated slash producer relies on: the committee size and
    /// eligible pool reflect the ejection once `applySlashes` commits, so a post-slash committee
    /// passes `concludeEpoch` validation. (The ordering as the block executor actually issues it
    /// is pinned end-to-end by `test_epoch_boundary_slash_ejects_through_production_close`.)
    /// Also pins the preserved reward economics:
    /// incentives run before slashes, so the closing epoch's rewards are weighted on pre-slash
    /// collateral and even the about-to-be-ejected validator collects its final epoch.
    #[tokio::test]
    async fn test_slash_to_zero_ejection_with_post_slash_committee() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Slash Ejection Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // close epoch 0 through the production payload path so the sequence below runs at a
        // realistic mid-chain boundary rather than directly on genesis state
        let consensus_output = consensus_output_for_tests(2, 0, 1, true);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 1);
        assert_eq!(committee.len(), 5);
        let victim = committee[0].validatorAddress;
        let survivor = committee[1].validatorAddress;

        let mut tn_evm = reth_env.tn_evm(canonical_header.hash())?;

        // stage 1: incentives, with rewards for the victim and a survivor
        let calldata = ConsensusRegistry::applyIncentivesCall {
            rewardInfos: vec![
                ConsensusRegistry::RewardInfo {
                    validatorAddress: victim,
                    consensusHeaderCount: U256::from(5),
                },
                ConsensusRegistry::RewardInfo {
                    validatorAddress: survivor,
                    consensusHeaderCount: U256::from(5),
                },
            ],
        }
        .abi_encode()
        .into();
        let mut res =
            tn_evm.transact_system_call(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        assert!(res.result.is_success(), "applyIncentives succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);

        // the victim collects its final epoch's rewards on pre-slash collateral
        let victim_rewards = reth_env.call_consensus_registry::<_, U256>(
            &mut tn_evm,
            ConsensusRegistry::getRewardsCall { validatorAddress: victim }.abi_encode().into(),
        )?;
        assert!(victim_rewards > U256::ZERO, "pre-slash rewards accrue to the victim");

        // stage 2: slash the victim's full outstanding stake, ejecting it at zero balance
        let (outstanding, _initial, _rewards) = reth_env
            .call_consensus_registry::<_, (U256, U256, U256)>(
                &mut tn_evm,
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: victim }
                    .abi_encode()
                    .into(),
            )?;
        let calldata = ConsensusRegistry::applySlashesCall {
            slashes: vec![ConsensusRegistry::Slash {
                validatorAddress: victim,
                amount: outstanding,
            }],
        }
        .abi_encode()
        .into();
        let mut res =
            tn_evm.transact_system_call(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        assert!(res.result.is_success(), "applySlashes succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);

        // the ejection is observable BEFORE the close: retired, eligible 5 -> 4, size clamped
        let retired = reth_env.call_consensus_registry::<_, bool>(
            &mut tn_evm,
            ConsensusRegistry::isRetiredCall { validatorAddress: victim }.abi_encode().into(),
        )?;
        assert!(retired, "slash-to-zero permanently retires the validator");
        let eligible = reth_env.call_consensus_registry::<_, U256>(
            &mut tn_evm,
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(4));
        let next_size = reth_env.call_consensus_registry::<_, u16>(
            &mut tn_evm,
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(next_size, 4, "committee size clamps to the post-slash eligible count");

        // stage 3: assemble the committee from POST-slash state, exactly as the client does,
        // and conclude - the size check validates against the post-ejection state and passes
        let mut new_committee: Vec<Address> = committee
            .iter()
            .map(|v| v.validatorAddress)
            .filter(|address| *address != victim)
            .collect();
        new_committee.sort();
        let calldata = ConsensusRegistry::concludeEpochCall { newCommittee: new_committee }
            .abi_encode()
            .into();
        let mut res =
            tn_evm.transact_system_call(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        assert!(res.result.is_success(), "concludeEpoch succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);

        let current_epoch = reth_env.call_consensus_registry::<_, u32>(
            &mut tn_evm,
            ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into(),
        )?;
        assert_eq!(current_epoch, 2, "epoch rotates over the post-slash committee");

        Ok(())
    }

    /// The production close path ejects a slash-to-zero validator: a slash injected through the
    /// `TNPayload` seam rides `finish()`'s boundary sequence and the close succeeds because the
    /// committee is assembled from post-slash state.
    ///
    /// This is the end-to-end pin for the executor's call ordering (`applyIncentives` →
    /// `applySlashes` → committee reads → `concludeEpoch`), driving the real
    /// `apply_closing_epoch_contract_call` rather than hand-rolling the sequence like the
    /// siblings above. Each assertion kills a distinct mis-ordering:
    /// - the block succeeding at all: the slash landed before the committee reads (a stale
    ///   five-member committee would revert `InvalidCommitteeSize` on-chain and error the block);
    /// - `exitEpoch == 1`: the slash landed BEFORE `concludeEpoch` rotated the epoch
    ///   (`_consensusBurn` records the current epoch at slash time; a slash issued after the
    ///   rotation would record 2, and close-success alone cannot discriminate that reorder);
    /// - the shrunken current + shuffled committees: the ejection propagated into committee
    ///   assembly.
    ///
    /// The inverted-order negative variant is deliberately absent: the fixed production path
    /// cannot issue the calls out of order, and the contract half (stale committee ⇒
    /// `InvalidCommitteeSize` revert) is already pinned by
    /// `test_stale_pre_slash_committee_reverts_close`.
    #[tokio::test]
    async fn test_epoch_boundary_slash_ejects_through_production_close() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Seam Slash Ejection Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // close epoch 0 through the production payload path so the seam-injected close below
        // runs at a realistic mid-chain boundary rather than directly on genesis state
        let consensus_output = consensus_output_for_tests(2, 0, 1, true);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 1);
        assert_eq!(committee.len(), 5);
        let victim = committee[0].validatorAddress;

        // slash sizing: the env runs a default (empty) RewardsCounter, so the boundary's
        // `applyIncentives` credits nothing and the pre-boundary outstanding balance equals the
        // balance `applySlashes` compares — a full-outstanding slash is a slash-to-zero
        let (outstanding, _initial, _rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: victim }
                    .abi_encode()
                    .into(),
            )?;

        // close epoch 1 through the PRODUCTION path with the slash injected through the seam:
        // finish() must sequence it between applyIncentives and the committee reads
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output)
            .with_epoch_boundary_slashes(vec![ConsensusRegistry::Slash {
                validatorAddress: victim,
                amount: outstanding,
            }]);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;

        // the slash landed: retired, ejected from the eligible pool, size clamped
        let retired = reth_env.read_consensus_registry::<bool>(
            ConsensusRegistry::isRetiredCall { validatorAddress: victim }.abi_encode().into(),
        )?;
        assert!(retired, "slash-to-zero through the production close retires the validator");
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(4));
        let next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(next_size, 4);

        // ordering discriminator: `_consensusBurn` → `_exit(validator, currentEpoch)` records
        // the epoch at slash time. The close rotated 1 → 2, so a slash issued after
        // `concludeEpoch` would record 2; only the slash-before-conclude order records 1.
        let victim_info = reth_env.read_consensus_registry::<ConsensusRegistry::ValidatorInfo>(
            ConsensusRegistry::getValidatorCall { validatorAddress: victim }.abi_encode().into(),
        )?;
        assert_eq!(victim_info.exitEpoch, 1, "slash must land before the epoch rotates");

        // the epoch rotated over a post-slash committee that excludes the victim
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 2);
        assert_eq!(committee.len(), 4);
        assert!(committee.iter().all(|v| v.validatorAddress != victim));

        // the committee this concludeEpoch installed (stored at newEpoch + 2 = 4) was shuffled
        // from the post-slash pool: four members, victim absent
        let shuffled =
            reth_env.validators_for_epoch_at_block(4, reth_env.canonical_tip().hash())?;
        assert_eq!(shuffled.len(), 4);
        assert!(shuffled.iter().all(|v| v.validatorAddress != victim));

        Ok(())
    }

    /// A committee sized from PRE-slash state makes `concludeEpoch` revert with
    /// `InvalidCommitteeSize` when a slash-to-zero ejection lands in between.
    ///
    /// The registry deliberately validates the committee against post-slash
    /// `nextCommitteeSize`, so a slash producer that reads the size before `applySlashes`
    /// commits produces a fatal on-chain revert (a deterministic fleet halt), not a silent
    /// mis-seat. The block executor's read-after-slash sequencing exists to prevent exactly
    /// this; this test pins the CONTRACT half (the revert and its selector), while the
    /// executor's own ordering is pinned through the production close path by
    /// `test_epoch_boundary_slash_ejects_through_production_close`.
    #[tokio::test]
    async fn test_stale_pre_slash_committee_reverts_close() -> eyre::Result<()> {
        use reth_revm::context::result::ExecutionResult;

        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Stale Committee Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        let committee =
            reth_env.validators_for_epoch_at_block(0, reth_env.canonical_tip().hash())?;
        assert_eq!(committee.len(), 5);
        let victim = committee[0].validatorAddress;

        // the stale view: a full-size committee assembled before the slash commits
        let mut stale_committee: Vec<Address> =
            committee.iter().map(|v| v.validatorAddress).collect();
        stale_committee.sort();

        let mut tn_evm = reth_env.tn_evm(chain.sealed_genesis_header().hash())?;

        let (outstanding, _initial, _rewards) = reth_env
            .call_consensus_registry::<_, (U256, U256, U256)>(
                &mut tn_evm,
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: victim }
                    .abi_encode()
                    .into(),
            )?;
        let calldata = ConsensusRegistry::applySlashesCall {
            slashes: vec![ConsensusRegistry::Slash {
                validatorAddress: victim,
                amount: outstanding,
            }],
        }
        .abi_encode()
        .into();
        let mut res =
            tn_evm.transact_system_call(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        assert!(res.result.is_success(), "applySlashes succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);

        // the stale five-member committee no longer matches the post-slash size of four
        let calldata = ConsensusRegistry::concludeEpochCall { newCommittee: stale_committee }
            .abi_encode()
            .into();
        let res =
            tn_evm.transact_system_call(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        match res.result {
            ExecutionResult::Revert { output, .. } => {
                let selector = &alloy::primitives::keccak256(
                    "InvalidCommitteeSize(uint256,uint256)".as_bytes(),
                )[..4];
                assert_eq!(
                    output.get(..4).expect("revert output carries at least a 4-byte selector"),
                    selector,
                    "stale committee must revert with InvalidCommitteeSize"
                );
            }
            other => panic!("expected InvalidCommitteeSize revert, got: {other:?}"),
        }

        Ok(())
    }

    /// A partial slash decrements the outstanding balance without ejection, and the standard
    /// boundary sequence with the full committee still succeeds.
    #[tokio::test]
    async fn test_partial_slash_keeps_validator_eligible() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Partial Slash Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        let committee =
            reth_env.validators_for_epoch_at_block(0, reth_env.canonical_tip().hash())?;
        assert_eq!(committee.len(), 5);
        let victim = committee[0].validatorAddress;
        let mut tn_evm = reth_env.tn_evm(chain.sealed_genesis_header().hash())?;

        let (outstanding_before, _initial, _rewards) = reth_env
            .call_consensus_registry::<_, (U256, U256, U256)>(
                &mut tn_evm,
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: victim }
                    .abi_encode()
                    .into(),
            )?;
        let slash_amount = outstanding_before.checked_div(U256::from(2)).expect("nonzero divisor");
        let calldata = ConsensusRegistry::applySlashesCall {
            slashes: vec![ConsensusRegistry::Slash {
                validatorAddress: victim,
                amount: slash_amount,
            }],
        }
        .abi_encode()
        .into();
        let mut res =
            tn_evm.transact_system_call(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        assert!(res.result.is_success(), "applySlashes succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);

        // balance reduced, but the validator stays eligible and the size is untouched
        let retired = reth_env.call_consensus_registry::<_, bool>(
            &mut tn_evm,
            ConsensusRegistry::isRetiredCall { validatorAddress: victim }.abi_encode().into(),
        )?;
        assert!(!retired, "partial slash must not retire the validator");
        let (outstanding_after, _initial, _rewards) = reth_env
            .call_consensus_registry::<_, (U256, U256, U256)>(
                &mut tn_evm,
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: victim }
                    .abi_encode()
                    .into(),
            )?;
        assert_eq!(outstanding_after, outstanding_before - slash_amount);
        let next_size = reth_env.call_consensus_registry::<_, u16>(
            &mut tn_evm,
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(next_size, 5, "partial slash leaves the committee size unchanged");

        // the full five-member committee still concludes the epoch
        let mut new_committee: Vec<Address> =
            committee.iter().map(|v| v.validatorAddress).collect();
        new_committee.sort();
        let calldata = ConsensusRegistry::concludeEpochCall { newCommittee: new_committee }
            .abi_encode()
            .into();
        let mut res =
            tn_evm.transact_system_call(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        assert!(res.result.is_success(), "concludeEpoch succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);

        Ok(())
    }

    /// Boundary-block determinism at default features: two independent executions of the
    /// byte-identical epoch-closing payload derive the same `state_root` and install the same
    /// next-epoch state.
    ///
    /// Every node must re-derive the identical boundary block or the fleet forks — the core
    /// safety property of the epoch close. Until now it was pinned only inside two
    /// `adiri`-gated tests that CI never compiles, so a default-build regression (e.g. a
    /// nondeterministic iteration order feeding `applyIncentives`, or an RNG-draw reorder in
    /// the committee shuffle) had no tripwire.
    #[tokio::test]
    async fn test_epoch_close_deterministic_across_envs() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // one payload, cloned across both executions: `new_for_test` randomizes
        // beneficiary/mix_hash/digest per call, and mix_hash seeds the committee shuffle
        let output = consensus_output_for_tests(2, 0, 1, true);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &output);

        let tmp1 = TempDir::new().unwrap();
        let tm1 = TaskManager::new("determinism env1");
        let env1 = RethEnv::new_for_temp_chain(chain.clone(), tmp1.path(), &tm1, None)?;
        let block1 = execute_payload_and_update_canonical_chain(&env1, payload.clone(), vec![])?;
        let header1 = block1.recovered_block.clone_sealed_header();

        let tmp2 = TempDir::new().unwrap();
        let tm2 = TaskManager::new("determinism env2");
        let env2 = RethEnv::new_for_temp_chain(chain.clone(), tmp2.path(), &tm2, None)?;
        let block2 = execute_payload_and_update_canonical_chain(&env2, payload, vec![])?;
        let header2 = block2.recovered_block.clone_sealed_header();

        assert_eq!(
            header1.state_root, header2.state_root,
            "epoch-closing state_root must be identical across independent executions"
        );

        // the installed next-epoch state matches read-for-read: same epoch record, same
        // shuffled committee (stored at newEpoch + 2), same keys
        let state1 = env1.epoch_state_from_canonical_tip()?;
        let state2 = env2.epoch_state_from_canonical_tip()?;
        assert_eq!(state1.epoch, 1);
        assert_eq!(state1.epoch, state2.epoch);
        assert_eq!(state1.epoch_info, state2.epoch_info);
        assert_eq!(
            env1.validators_for_epoch_at_block(3, env1.canonical_tip().hash())?,
            env2.validators_for_epoch_at_block(3, env2.canonical_tip().hash())?
        );
        assert_eq!(
            env1.bls_pubkeys_for_epoch_at_block(3, env1.canonical_tip().hash())?,
            env2.bls_pubkeys_for_epoch_at_block(3, env2.canonical_tip().hash())?
        );

        Ok(())
    }

    /// Reward accounting through the production close: seeded leader counts flow
    /// `RewardsCounter` → `applyIncentives(RewardInfo[])` → on-chain balances, with the epoch
    /// issuance split exactly between equal-count leaders.
    ///
    /// Pins the `BTreeMap<Address, u32>` → `RewardInfo[]` mapping in
    /// `apply_closing_epoch_contract_call` with a NON-empty rewards array at unit level —
    /// previously exercised only by the engine e2e suite. The fixture's `epochIssuance`
    /// (25,806 TEL) is even, so two equal-weight leaders each collect exactly half
    /// (`div_ceil(2)` matches the engine assertions and is exact here).
    #[tokio::test]
    async fn test_rewards_counter_flows_through_production_close() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Rewards Close Test");
        let counter = RewardsCounter::default();
        let reth_env = RethEnv::new_for_temp_chain(
            chain.clone(),
            tmp_dir.path(),
            &task_manager,
            Some(counter.clone()),
        )?;

        // seed the counter exactly as run_epoch does at epoch entry: the genesis committee,
        // then equal leader tallies for two members
        let epoch_state = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch_state.validators.len(), 5);
        let member_addresses: Vec<Address> =
            epoch_state.validators.iter().map(|v| v.validatorAddress).collect();
        let issuance = epoch_state.epoch_info.epochIssuance;
        let committee = create_committee_from_state(epoch_state).await?;
        counter.set_committee(committee.clone());
        let authorities: Vec<_> = committee.authorities().into_iter().collect();
        let (leader_a, leader_b) = (&authorities[0], &authorities[1]);
        for _ in 0..3 {
            counter.inc_leader_count(&leader_a.id());
            counter.inc_leader_count(&leader_b.id());
        }
        assert_eq!(counter.get_address_counts().len(), 2);

        // close epoch 0 through the production path; finish() drains the counter into
        // applyIncentives(RewardInfo[]) with a two-entry array
        let consensus_output = consensus_output_for_tests(2, 0, 1, true);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;

        // equal weights split the (even) issuance exactly; non-leaders collect nothing
        let expected_each = issuance.div_ceil(U256::from(2));
        assert_eq!(expected_each * U256::from(2), issuance, "fixture issuance splits exactly");
        for leader in [leader_a, leader_b] {
            let rewards = reth_env.read_consensus_registry::<U256>(
                ConsensusRegistry::getRewardsCall { validatorAddress: leader.execution_address() }
                    .abi_encode()
                    .into(),
            )?;
            assert_eq!(rewards, expected_each, "equal-count leader collects an exact half");
        }
        let non_leader = member_addresses
            .iter()
            .find(|address| {
                **address != leader_a.execution_address()
                    && **address != leader_b.execution_address()
            })
            .expect("five members, two leaders");
        let rewards = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getRewardsCall { validatorAddress: *non_leader }.abi_encode().into(),
        )?;
        assert!(rewards.is_zero(), "non-leader collects nothing");

        Ok(())
    }

    /// Governance `burn` of a validator seated only in FUTURE committees leaves the current
    /// committee untouched.
    ///
    /// A sixth validator stakes and activates, so committees shuffled after its activation may
    /// seat it while the current committee predates it. Burning it mid-epoch mutates only the
    /// future committee arrays it occupies (swap-and-pop), leaves the running committee
    /// byte-identical (so the node's epoch-record comparison cannot diverge for future-only
    /// ejection, even without the mid-epoch-ejection tolerance fix), keeps
    /// `nextCommitteeSize` at 5 (eligible count drops 6 -> 5, so no auto-decrement fires),
    /// and the following epochs close cleanly.
    #[tokio::test]
    async fn test_burn_future_only_validator() -> eyre::Result<()> {
        // the sixth validator's EOA signs its own stake + activate txs
        let mut newval_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
        let newval_addr = newval_eoa.address();
        // BLS seeds 0..=4 are taken by the 5 genesis validators
        let newval_bls = BlsKeypair::generate(&mut StdRng::seed_from_u64(5));
        let newval_pop = generate_proof_of_possession_bls_for_test(&newval_bls, &newval_addr)
            .expect("pop generation failed");

        let stake_amount = U256::from(parse_ether("1_000_000")?);
        let genesis = test_genesis_with_consensus_registry(5).extend_accounts([(
            newval_addr,
            GenesisAccount::default().with_balance(stake_amount.saturating_mul(U256::from(2))),
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Future Only Burn Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // block 1 (epoch 0): governance mints the NFT, the new validator stakes and activates
        let mut governance = governance_owner_factory();
        let mint_tx = governance.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::mintCall { validatorAddress: newval_addr }.abi_encode().into(),
        );
        let stake_tx = newval_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            stake_amount,
            ConsensusRegistry::stakeCall {
                blsPubkey: newval_bls.public().to_bytes().into(),
                proofOfPossession: ConsensusRegistry::ProofOfPossession {
                    signature: newval_pop.to_bytes().into(),
                },
            }
            .abi_encode()
            .into(),
        );
        let activate_tx = newval_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::activateCall {}.abi_encode().into(),
        );
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![mint_tx, stake_tx, activate_tx],
        )?;
        let mut canonical_header = block1.recovered_block.clone_sealed_header();

        // six validators are now committee-eligible (5 active + 1 pending activation)
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(6));

        // close epochs until some validator sits in a future committee but not the current
        // one (with 6 eligible validators and 5 seats every shuffled committee excludes
        // exactly one; the shuffle is seed-deterministic so the arrangement is stable)
        let committee_addrs = |e: u32| -> eyre::Result<Vec<Address>> {
            Ok(reth_env
                .validators_for_epoch_at_block(e, reth_env.canonical_tip().hash())?
                .into_iter()
                .map(|v| v.validatorAddress)
                .collect())
        };
        let mut current_epoch = 0u32;
        let mut subdag_index = 2u64;
        let mut arrangement = None;
        while arrangement.is_none() && current_epoch < 6 {
            current_epoch += 1;
            let consensus_output = consensus_output_for_tests(2, current_epoch, subdag_index, true);
            subdag_index += 1;
            let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
            let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
            canonical_header = block.recovered_block.clone_sealed_header();

            let current = committee_addrs(current_epoch)?;
            let future: Vec<Address> = committee_addrs(current_epoch + 1)?
                .into_iter()
                .chain(committee_addrs(current_epoch + 2)?)
                .collect();
            // prefer the never-seated new validator; any future-only member proves the property
            arrangement = if !current.contains(&newval_addr) && future.contains(&newval_addr) {
                Some(newval_addr)
            } else {
                future.iter().copied().find(|v| !current.contains(v))
            };
        }
        let target = arrangement
            .expect("deterministic shuffle seats a validator in a future committee only");
        let w = current_epoch;
        let target_bls = reth_env.get_bls_pubkey(canonical_header.hash(), target)?;

        // pre-burn snapshots of the running + both future committees
        let pre_current =
            reth_env.bls_pubkeys_for_epoch_at_block(w, reth_env.canonical_tip().hash())?;
        let pre_next =
            reth_env.bls_pubkeys_for_epoch_at_block(w + 1, reth_env.canonical_tip().hash())?;
        let pre_subsequent =
            reth_env.bls_pubkeys_for_epoch_at_block(w + 2, reth_env.canonical_tip().hash())?;
        assert!(!pre_current.contains(&target_bls));
        assert!(
            pre_next.contains(&target_bls) || pre_subsequent.contains(&target_bls),
            "target seated in a future committee"
        );
        let pre_next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(pre_next_size, 5);

        // burn the future-only validator mid-epoch W
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, w, subdag_index, false);
        subdag_index += 1;
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;
        canonical_header = block.recovered_block.clone_sealed_header();

        // the running committee is byte-identical: future-only ejection cannot perturb the
        // current epoch (so on-chain reads for epoch W match any pre-burn snapshot exactly)
        assert_eq!(
            reth_env.bls_pubkeys_for_epoch_at_block(w, reth_env.canonical_tip().hash())?,
            pre_current
        );

        // future committees shrink via swap-and-pop exactly where the target was seated
        for (e, pre) in [(w + 1, &pre_next), (w + 2, &pre_subsequent)] {
            let post =
                reth_env.bls_pubkeys_for_epoch_at_block(e, reth_env.canonical_tip().hash())?;
            if let Some(idx) = pre.iter().position(|k| k == &target_bls) {
                let mut expected = pre.to_vec();
                let last = expected.len() - 1;
                expected.swap(idx, last);
                expected.truncate(last);
                assert_eq!(post, expected, "swap-and-pop order for future epoch {e}");
            } else {
                assert_eq!(&post, pre, "future committee {e} untouched");
            }
            assert!(!post.contains(&target_bls));
        }

        // no auto-decrement: 5 remaining eligible validators still cover committee size 5
        let post_next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(post_next_size, 5);
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(5));
        let retired = reth_env.read_consensus_registry::<bool>(
            ConsensusRegistry::isRetiredCall { validatorAddress: target }.abi_encode().into(),
        )?;
        assert!(retired);
        let (outstanding, _initial, rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: target }
                    .abi_encode()
                    .into(),
            )?;
        assert!(outstanding.is_zero(), "outstanding stake confiscated to issuance");
        assert!(rewards.is_zero());

        // the epoch closes cleanly and the next two committees seat without the target,
        // including the (possibly shrunken) subsequent committee going current
        for close in [w + 1, w + 2] {
            let consensus_output = consensus_output_for_tests(2, close, subdag_index, true);
            subdag_index += 1;
            let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
            let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
            canonical_header = block.recovered_block.clone_sealed_header();

            let EpochState { epoch, validators: committee, .. } =
                reth_env.epoch_state_from_canonical_tip()?;
            assert_eq!(epoch, close);
            assert!(committee.iter().all(|v| v.validatorAddress != target));
        }

        Ok(())
    }

    /// `epoch_state_at_epoch_start` pins every read to the previous epoch's closing block, so a
    /// mid-epoch governance `burn` — which swap-and-pops the CURRENT epoch's stored committee
    /// arrays immediately — moves the canonical-tip view but never the epoch-start view. A node
    /// entering (or re-entering) the epoch before or after the burn derives the identical
    /// committee.
    #[tokio::test]
    async fn epoch_state_at_epoch_start_pins_pre_burn_committee() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Epoch Start Pin Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // block 1: plain epoch-0 block
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let header1 = block1.recovered_block.clone_sealed_header();

        // block 2: close epoch 0 — `concludeEpoch` executes inside this block, making it epoch
        // 0's closing block (the header that rules all of epoch 1)
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(header1, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let closing_header = block2.recovered_block.clone_sealed_header();

        // snapshot A: epoch 1's start state, pinned to epoch 0's closing block
        let (snapshot_a, pin_a) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(snapshot_a.epoch, 1);
        assert_eq!(snapshot_a.validators.len(), 5, "full pre-burn committee");
        assert_eq!(snapshot_a.bls_pubkeys.len(), 5);
        assert_eq!(pin_a.hash(), closing_header.hash(), "pin is epoch 0's closing block");

        // block 3: governance burns a committee member mid-epoch-1
        let target = snapshot_a.validators[1].validatorAddress;
        let target_bls = snapshot_a.bls_pubkeys[1].clone();
        let mut governance = governance_owner_factory();
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, 1, 3, false);
        let payload = TNPayload::new_for_test(closing_header, &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;

        // the tip view shrinks immediately (swap-and-pop)
        let tip_state = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(tip_state.epoch, 1);
        assert_eq!(tip_state.validators.len(), 4, "tip committee shrank post-burn");
        assert_eq!(tip_state.bls_pubkeys.len(), 4);
        assert!(tip_state.validators.iter().all(|v| v.validatorAddress != target));
        assert!(!tip_state.bls_pubkeys.contains(&target_bls));

        // the epoch-start view still returns the full pre-burn set, byte-identical to snapshot A
        let (snapshot_b, pin_b) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(pin_b.hash(), pin_a.hash(), "pin unchanged by the burn");
        assert_eq!(snapshot_b.epoch, snapshot_a.epoch);
        assert_eq!(snapshot_b.epoch_info, snapshot_a.epoch_info);
        assert_eq!(snapshot_b.epoch_start, snapshot_a.epoch_start);
        assert_eq!(snapshot_b.validators, snapshot_a.validators);
        assert_eq!(snapshot_b.bls_pubkeys, snapshot_a.bls_pubkeys);
        assert!(snapshot_b.validators.iter().any(|v| v.validatorAddress == target));
        assert!(snapshot_b.bls_pubkeys.contains(&target_bls));

        // the boundary-written-once EpochInfo scalars agree between the tip and pinned reads;
        // the committee array embedded in EpochInfo is exactly what the burn mutates, so it is
        // the only field that diverges
        assert_eq!(tip_state.epoch, snapshot_b.epoch);
        assert_eq!(tip_state.epoch_info.blockHeight, snapshot_b.epoch_info.blockHeight);
        assert_eq!(tip_state.epoch_info.epochId, snapshot_b.epoch_info.epochId);
        assert_eq!(tip_state.epoch_info.epochDuration, snapshot_b.epoch_info.epochDuration);
        assert_eq!(tip_state.epoch_info.epochIssuance, snapshot_b.epoch_info.epochIssuance);
        assert_eq!(tip_state.epoch_info.stakeVersion, snapshot_b.epoch_info.stakeVersion);
        assert_eq!(tip_state.epoch_start, snapshot_b.epoch_start);
        assert_eq!(tip_state.epoch_info.committee.len(), 4, "burn mutates EpochInfo's committee");
        assert_eq!(snapshot_b.epoch_info.committee.len(), 5);

        Ok(())
    }

    /// Next-committee prefetches pinned to the epoch-start header read the PRE-burn future
    /// committees. At epoch 0's closing block the registry already reports epoch 1 and serves
    /// `getCommitteeValidators`/`getCommitteeBlsPubkeys` for epochs 2 and 3 (genesis seeds
    /// epochs 0-2; `concludeEpoch` writes epoch 3's committee inside the closing block), so a
    /// node re-entering epoch 1 after a mid-epoch burn derives the same future sets it would
    /// have derived entering before the burn. The tip variants of the same reads observe the
    /// post-burn (shrunken) sets, proving the pin is what makes the difference.
    #[tokio::test]
    async fn pinned_next_committee_prefetch_reads_pre_burn_sets() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Pinned Prefetch Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // block 1: plain epoch-0 block
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let header1 = block1.recovered_block.clone_sealed_header();

        // block 2: close epoch 0 (epoch 0's closing block)
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(header1, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let closing_header = block2.recovered_block.clone_sealed_header();

        // pre-burn snapshots of the epoch 2 and 3 committees (the tip IS the closing block here)
        let pre_v2 = reth_env.validators_for_epoch_at_block(2, reth_env.canonical_tip().hash())?;
        let pre_v3 = reth_env.validators_for_epoch_at_block(3, reth_env.canonical_tip().hash())?;
        let pre_b2 = reth_env.bls_pubkeys_for_epoch_at_block(2, reth_env.canonical_tip().hash())?;
        let pre_b3 = reth_env.bls_pubkeys_for_epoch_at_block(3, reth_env.canonical_tip().hash())?;
        assert_eq!(pre_v2.len(), 5);
        assert_eq!(pre_v3.len(), 5);

        // block 3: governance burns a committee member mid-epoch-1 (with exactly 5 eligible
        // validators every committee seats all 5, so the target sits in epochs 1, 2, and 3)
        let EpochState { validators, bls_pubkeys, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        let target = validators[1].validatorAddress;
        let target_bls = bls_pubkeys[1].clone();
        let mut governance = governance_owner_factory();
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, 1, 3, false);
        let payload = TNPayload::new_for_test(closing_header.clone(), &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;

        // pinned prefetches at the epoch-start header: epoch+1 and epoch+2 relative to the
        // running epoch 1 still return the PRE-burn sets
        let (state, pin) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(state.epoch, 1);
        assert_eq!(pin.hash(), closing_header.hash());
        let pinned_v2 = reth_env.validators_for_epoch_at_block(2, pin.hash())?;
        let pinned_v3 = reth_env.validators_for_epoch_at_block(3, pin.hash())?;
        assert_eq!(pinned_v2, pre_v2, "epoch 2 committee at the pin is the pre-burn set");
        assert_eq!(pinned_v3, pre_v3, "epoch 3 committee at the pin is the pre-burn set");
        assert!(pinned_v2.iter().any(|v| v.validatorAddress == target));
        assert!(pinned_v3.iter().any(|v| v.validatorAddress == target));
        let pinned_b2 = reth_env.bls_pubkeys_for_epoch_at_block(2, pin.hash())?;
        let pinned_b3 = reth_env.bls_pubkeys_for_epoch_at_block(3, pin.hash())?;
        assert_eq!(pinned_b2, pre_b2, "epoch 2 pubkeys at the pin are the pre-burn set");
        assert_eq!(pinned_b3, pre_b3, "epoch 3 pubkeys at the pin are the pre-burn set");
        // the batch variants resolve both epochs through ONE pinned EVM and order their results
        // to match the input: batch == the single pinned reads, by header and by hash
        let batched = reth_env.bls_pubkeys_for_epochs_at_header(&[2, 3], &pin)?;
        assert_eq!(batched, vec![pinned_b2.clone(), pinned_b3.clone()]);
        let batched_by_hash = reth_env.bls_pubkeys_for_epochs_at_block(&[2, 3], pin.hash())?;
        assert_eq!(
            batched_by_hash, batched,
            "the by-hash batch resolves the same header as the by-header batch"
        );
        assert!(pinned_b2.contains(&target_bls));
        assert!(pinned_b3.contains(&target_bls));

        // the same read pinned to the post-burn canonical tip shows the post-burn set: the pin
        // block is what makes the difference
        let tip_v2 = reth_env.validators_for_epoch_at_block(2, reth_env.canonical_tip().hash())?;
        assert_eq!(tip_v2.len(), 4);
        assert!(tip_v2.iter().all(|v| v.validatorAddress != target));

        Ok(())
    }

    /// In epoch 0 the pin is genesis: genesis execution seeds the registry, so genesis state IS
    /// epoch 0's start state. The pinned view matches the canonical-tip view field for field at
    /// genesis and keeps pinning genesis after the chain advances within the epoch.
    #[tokio::test]
    async fn epoch_state_at_epoch_start_epoch_zero_pins_genesis() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Epoch Zero Pin Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // fresh chain still in epoch 0: the pin is the genesis header
        let (state, pin) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(pin.number, 0, "epoch 0 pins genesis");
        assert_eq!(pin.hash(), chain.sealed_genesis_header().hash());

        // at genesis the tip and the pin are the same block, so the views agree field for field
        let tip_state = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(state.epoch, 0);
        assert_eq!(state.epoch, tip_state.epoch);
        assert_eq!(state.epoch_info, tip_state.epoch_info);
        assert_eq!(state.epoch_start, tip_state.epoch_start);
        assert_eq!(state.validators, tip_state.validators);
        assert_eq!(state.bls_pubkeys, tip_state.bls_pubkeys);
        assert_eq!(state.validators.len(), 5);

        // the pin stays genesis after the chain advances within epoch 0
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let (state_after, pin_after) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(pin_after.number, 0);
        assert_eq!(state_after.epoch, 0);
        assert_eq!(state_after.validators, state.validators);
        assert_eq!(state_after.bls_pubkeys, state.bls_pubkeys);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_worker_fee_configs() -> eyre::Result<()> {
        // minimal validator set (5 validators)
        let all_validators: Vec<Address> =
            (1..=5).map(|i| Address::from_slice(&[i * 0x11; 20])).collect();

        let validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: 28800,
        };

        let governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([(
            governance,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
        )]);

        // deploy with 2 workers: worker 0 = EIP-1559 (strategy 0, target 30M),
        // worker 1 = Static (strategy 1, fee 500)
        let worker_configs = vec![(0u8, 30_000_000u64), (1u8, 500u64)];
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            worker_configs,
        )?;

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Worker Fee Config Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();

        // read back worker configs from chain state - the returned length IS the on-chain
        // numWorkers()
        let configs = reth_env.get_worker_fee_configs()?;
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0], WorkerFeeConfig::Eip1559 { target_gas: 30_000_000 });
        assert_eq!(configs[1], WorkerFeeConfig::Static { fee: 500 });

        // the block-pinned read primitive reports the same count at genesis
        let (num_workers, configs_at_block) =
            reth_env.get_worker_fee_configs_at_block(chain.sealed_genesis_header().hash())?;
        assert_eq!(num_workers, 2);
        assert_eq!(configs_at_block, configs);

        Ok(())
    }

    /// Pins the per-epoch worker-count read rule: the count for epoch E is the `WorkerConfigs`
    /// state at E's first block's parent (= E-1's closing block). Governance submits
    /// `setWorkerConfig` + `setNumWorkers` during epoch 0; the count read at epoch 1's
    /// start-parent block reflects the change, while the count read at epoch 0's start-parent
    /// (genesis) still reports the original single worker.
    #[tokio::test]
    async fn test_worker_count_read_at_epoch_start_parent() -> eyre::Result<()> {
        // minimal validator set (5 validators)
        let all_validators: Vec<Address> =
            (1..=5).map(|i| Address::from_slice(&[i * 0x11; 20])).collect();

        let validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: 28800,
        };

        // governance owns the WorkerConfigs contract and signs the config txs
        let mut governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([(
            governance,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
        )]);

        // deploy with a single worker (the canonical existing-chain shape)
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            vec![(0u8, 30_000_000u64)],
        )?;

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Worker Count Epoch Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();

        // sanity: epoch 0 starts at blockHeight 0, so its start-parent (saturating) is genesis
        let EpochState { epoch, epoch_info, .. } = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 0);
        let epoch_zero_read_block = epoch_info.blockHeight.saturating_sub(1);
        assert_eq!(epoch_zero_read_block, 0);

        // governance grows the worker set mid-epoch: config worker 1 (Static 500), then grow
        // the count (setWorkerConfig must precede setNumWorkers per the contract)
        let set_config_tx = governance_multisig.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(WORKER_CONFIGS_ADDRESS),
            U256::ZERO,
            WorkerConfigs::setWorkerConfigCall {
                workerId: 1,
                strategy: 1,
                value: 500,
                data: alloy::primitives::aliases::U184::ZERO,
            }
            .abi_encode()
            .into(),
        );
        let set_num_workers_tx = governance_multisig.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(WORKER_CONFIGS_ADDRESS),
            U256::ZERO,
            WorkerConfigs::setNumWorkersCall { numWorkers_: 2 }.abi_encode().into(),
        );

        // block 1 (mid-epoch-0): the governance txs land
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![set_config_tx, set_num_workers_tx],
        )?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // block 2 closes epoch 0 -> epoch 1's first block will be 3, start-parent = block 2
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let close_block_hash = block2.recovered_block.clone_sealed_header().hash();

        // the new epoch's info points its start-parent at the closing block
        let EpochState { epoch, epoch_info, .. } = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 1);
        let epoch_one_read_block = epoch_info.blockHeight.saturating_sub(1);
        let epoch_one_read_header = reth_env
            .sealed_header_by_number(epoch_one_read_block)?
            .expect("epoch 1 start-parent header");
        assert_eq!(epoch_one_read_header.hash(), close_block_hash);

        // epoch 1's count (read at its start-parent) reflects the governance change...
        let (num_workers, configs) =
            reth_env.get_worker_fee_configs_at_block(epoch_one_read_header.hash())?;
        assert_eq!(num_workers, 2);
        assert_eq!(configs[1], WorkerFeeConfig::Static { fee: 500 });

        // ...while epoch 0's count (read at genesis) still reports the original single worker
        let genesis_hash = reth_env
            .sealed_header_by_number(epoch_zero_read_block)?
            .expect("genesis header")
            .hash();
        let (num_workers, _configs) = reth_env.get_worker_fee_configs_at_block(genesis_hash)?;
        assert_eq!(num_workers, 1);

        Ok(())
    }

    /// Classification pin: the pinned-block read paths distinguish node-local provider faults
    /// (the pinned hash/header not resolving on THIS node - peers may read fine) from
    /// chain-global failures (contract absent at the block - identical on every node). The
    /// close-time base-fee compute keys retry-then-halt vs keep-current off this split.
    #[tokio::test]
    async fn test_state_read_classifies_provider_vs_chain_global() -> eyre::Result<()> {
        // healthy provider/database, but with the system contracts stripped from the alloc so
        // the contract reads fail deterministically at every block
        let mut genesis = tn_types::test_genesis();
        genesis.alloc.remove(&WORKER_CONFIGS_ADDRESS);
        genesis.alloc.remove(&CONSENSUS_REGISTRY_ADDRESS);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("State Read Classification Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // PROVIDER: an unknown/random block hash on a healthy env is a node-local resolution
        // failure (the fault never reaches the contract)
        let err = reth_env
            .get_worker_fee_configs_at_block(B256::random())
            .expect_err("unknown block hash must fail");
        assert!(
            matches!(err, StateReadError::Provider(_)),
            "unknown block hash must classify as Provider, got: {err}"
        );

        // CHAIN-GLOBAL: the hash resolves (genesis) but the WorkerConfigs contract is absent -
        // the call succeeds with empty return data and the decode fails deterministically
        let err = reth_env
            .get_worker_fee_configs_at_block(chain.sealed_genesis_header().hash())
            .expect_err("absent contract must fail");
        assert!(
            matches!(err, StateReadError::ChainGlobal(_)),
            "absent contract must classify as ChainGlobal, got: {err}"
        );

        // the close-time identity read classifies the same way: a header this node cannot
        // resolve state for is Provider...
        let phantom = SealedHeader::new(ExecHeader::default(), B256::random());
        let err = reth_env
            .get_current_epoch_info_at_header(&phantom)
            .expect_err("phantom header must fail");
        assert!(
            matches!(err, StateReadError::Provider(_)),
            "unresolvable header state must classify as Provider, got: {err}"
        );

        // ...while an absent ConsensusRegistry at a resolvable block is ChainGlobal
        let err = reth_env
            .get_current_epoch_info_at_header(&chain.sealed_genesis_header())
            .expect_err("absent registry must fail");
        assert!(
            matches!(err, StateReadError::ChainGlobal(_)),
            "absent registry must classify as ChainGlobal, got: {err}"
        );

        // the full epoch-state read splits the same way: unresolvable pin state is Provider...
        let err = reth_env.epoch_state_at_header(&phantom).expect_err("phantom header must fail");
        assert!(
            matches!(err, StateReadError::Provider(_)),
            "epoch state at an unresolvable header must classify as Provider, got: {err}"
        );

        // ...and an absent registry at a resolvable block is ChainGlobal
        let err = reth_env
            .epoch_state_at_header(&chain.sealed_genesis_header())
            .expect_err("absent registry must fail");
        assert!(
            matches!(err, StateReadError::ChainGlobal(_)),
            "epoch state with an absent registry must classify as ChainGlobal, got: {err}"
        );

        // the pinned committee-pubkey reads, single and batched, split identically: an
        // unresolvable block hash never reaches the contract, so it is Provider...
        let err = reth_env
            .bls_pubkeys_for_epoch_at_block(0, B256::random())
            .expect_err("unknown block hash must fail");
        assert!(
            matches!(err, StateReadError::Provider(_)),
            "committee read at an unknown block hash must classify as Provider, got: {err}"
        );
        let err = reth_env
            .bls_pubkeys_for_epochs_at_block(&[0, 1], B256::random())
            .expect_err("unknown block hash must fail");
        assert!(
            matches!(err, StateReadError::Provider(_)),
            "batched committee read at an unknown block hash must classify as Provider, got: {err}"
        );

        // ...while the absent registry at genesis is ChainGlobal for both shapes
        let genesis_hash = chain.sealed_genesis_header().hash();
        let err = reth_env
            .bls_pubkeys_for_epoch_at_block(0, genesis_hash)
            .expect_err("absent registry must fail");
        assert!(
            matches!(err, StateReadError::ChainGlobal(_)),
            "committee read with an absent registry must classify as ChainGlobal, got: {err}"
        );
        let err = reth_env
            .bls_pubkeys_for_epochs_at_block(&[0, 1], genesis_hash)
            .expect_err("absent registry must fail");
        assert!(
            matches!(err, StateReadError::ChainGlobal(_)),
            "batched committee read with an absent registry must classify as ChainGlobal, got: \
             {err}"
        );

        Ok(())
    }
}
