// SPDX-License-Identifier: MIT or Apache-2.0
//! RPC request handle for state sync requests from peers.

mod error;
mod rpc_ext;

pub use rpc_ext::{TelcoinNetworkRpcExt, TelcoinNetworkRpcExtApiServer};
use serde::{Deserialize, Serialize};
use std::future::Future;
use thiserror::Error;
use tn_types::{
    Address, AuthorityIdentifier, BlsPublicKey, Bytes, ConsensusHeader, ConsensusHeaderDigest,
    Epoch, EpochCertificate, EpochDigest, EpochRecord, Multiaddr, NetworkPublicKey, NodeMode,
    WorkerId,
};

/// Contain the node's identifying info to provide over RPC.
#[derive(PartialEq, Serialize, Clone, Debug)]
pub struct RpcNodeInfo {
    /// Chain id this node is part of.
    pub chain_id: u64,
    /// The version of the running software.
    pub version: &'static str,
    /// The name for the validator. The default value
    /// is the base58 encoding of the first 8 bytes of the BLS public key
    /// prepended with 'node-'. The operator can overwrite
    /// this value since it is not used when writing to file.
    pub name: String,
    /// The node's BLS public key.
    pub bls_public_key: BlsPublicKey,
    /// The node's authority id (hash of BLS key).
    /// Used for some tables (like reputation).
    pub authority_id: AuthorityIdentifier,
    /// Address that will receive rewards if this node participates in consensus.
    pub execution_address: Address,
    /// Network public key for the primary network.
    pub primary_network_key: NetworkPublicKey,
    /// Network public key for the workers network.
    pub worker_network_key: NetworkPublicKey,
    /// Network external address for the primary network.
    pub primary_external_address: Multiaddr,
    /// Network external address for the worker network.
    pub worker_external_address: Multiaddr,
}

/// Admission view of every swarm this node runs, served by `tn_admissionSnapshot`.
///
/// Host tooling reads it to keep a P2P firewall allowlist a superset of the peers the node admits.
/// Each entry is a BLS key a swarm tracks, with the slots it holds and the addresses the swarm
/// would reach it at. Listing a key grants it nothing: `scheduled` and `eligible` keys are tracked
/// for export only, and [`RpcAdmissionEntry::admitted`] reports whether the key's PeerId is in the
/// authorized set that `Closed` admission enforces.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcAdmissionSnapshot {
    /// Schema version, [`Self::VERSION`] for this build.
    ///
    /// A consumer must not interpret a snapshot whose version it does not know.
    pub version: u32,
    /// The primary swarm's latest accepted admission epoch, or `None` before one is accepted.
    pub epoch: Option<u64>,
    /// One report per swarm: the primary network and each worker network.
    pub swarms: Vec<RpcSwarmAdmission>,
}

impl RpcAdmissionSnapshot {
    /// The schema version this build serves.
    pub const VERSION: u32 = 1;

    /// Create a snapshot at the current schema [`Self::VERSION`].
    pub fn new(epoch: Option<u64>, swarms: Vec<RpcSwarmAdmission>) -> Self {
        Self { version: Self::VERSION, epoch, swarms }
    }
}

/// Admission report of one swarm in an [`RpcAdmissionSnapshot`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcSwarmAdmission {
    /// The swarm's metrics label: `primary`, or `worker-N` for worker `N`.
    pub network: String,
    /// The worker id of a worker swarm, or `None` for the primary.
    pub worker_id: Option<WorkerId>,
    /// Whether the swarm answered.
    ///
    /// A swarm that did not answer in time is still listed, with `available` set to `false` and
    /// no `admission`; its other fields then describe nothing and must not be read as an empty
    /// tracked set.
    pub available: bool,
    /// Epoch of the latest scheduled and eligible key publication the swarm accepted, or `None`
    /// before the first.
    pub tracked_epoch: Option<u64>,
    /// The swarm's connection admission status, `None` (serialized as `null`) exactly when
    /// `available` is `false`.
    ///
    /// A swarm that did not answer has no status to report, so a consumer checks `available`
    /// before it reads the status.
    pub admission: Option<RpcAdmissionStatus>,
    /// Every key the swarm tracks, sorted by BLS key bytes.
    pub entries: Vec<RpcAdmissionEntry>,
}

/// Connection admission status of one swarm.
///
/// The counts are observations of the current committee; they do not imply consensus readiness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcAdmissionStatus {
    /// The admission mode the operator configured.
    pub configured: RpcAdmissionMode,
    /// The admission mode applied to connections, below `configured` while a fallback holds.
    pub effective: RpcAdmissionMode,
    /// The latest accepted committee window epoch, or `None` before one is accepted.
    pub epoch: Option<u64>,
    /// Why `Closed` cannot be enforced, or `None` when nothing prevents it.
    ///
    /// Reported in every configured mode, so an operator can check a window before switching to
    /// `Closed`.
    pub fallback: Option<RpcAdmissionFallback>,
    /// Current committee identities with authenticated records, including the local identity.
    pub resolved_current: u64,
    /// The resolved-record quorum `Closed` requires: n minus floor((n - 1) / 3).
    pub required_current: u64,
    /// Connected current committee peers, excluding the local identity.
    pub connected_current: u64,
    /// Identities whose PeerId another identity also claims, or whose PeerId differs from the
    /// configured binding.
    pub contradicted: u64,
}

/// One tracked BLS key in a swarm's [`RpcSwarmAdmission::entries`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcAdmissionEntry {
    /// The key's 96-byte compressed BLS public key, hex encoded with a `0x` prefix.
    pub bls_pubkey: Bytes,
    /// Every slot the key holds in this swarm.
    pub slots: Vec<RpcAdmissionSlot>,
    /// The base58 PeerId of `network_pubkey`, or `None` when `source` is
    /// [`RpcEntrySource::Unresolved`].
    pub peer_id: Option<String>,
    /// The network public key of the held record or configured binding, or `None` when `source`
    /// is [`RpcEntrySource::Unresolved`].
    pub network_pubkey: Option<NetworkPublicKey>,
    /// The addresses the swarm uses for the key: the configured launch address when `source` is
    /// [`RpcEntrySource::Stub`], the configured binding's addresses when it is
    /// [`RpcEntrySource::Config`], the signed record's when it is [`RpcEntrySource::Record`], and
    /// none when it is [`RpcEntrySource::Unresolved`].
    pub multiaddrs: Vec<String>,
    /// The signed record's own addresses, present only when a configured launch address replaces
    /// them in `multiaddrs`, a signed record for the key is held, and its addresses differ from
    /// `multiaddrs`.
    ///
    /// The two lists can differ when the operator's inventory is stale or the validator moved; a
    /// firewall that must not cut a live peer allows both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_multiaddrs: Option<Vec<String>>,
    /// The signed record's timestamp in seconds since the Unix epoch, or `None` while no signed
    /// record is held.
    pub record_timestamp: Option<u64>,
    /// Where `multiaddrs` came from.
    pub source: RpcEntrySource,
    /// Whether the key's PeerId is in the authorized set that `Closed` admission enforces.
    ///
    /// True only for a key in the admission window or with an operator grant whose PeerId is
    /// authorized. A key held only as `scheduled` or `eligible` is never admitted, even when its
    /// record claims an authorized PeerId. The flag does not depend on the effective mode: under
    /// `Open` or `Grace` a peer outside the authorized set may still connect.
    pub admitted: bool,
}

/// Connection admission mode of a swarm.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcAdmissionMode {
    /// Unrestricted discovery under the existing identity and ban checks.
    Open,
    /// Discovery stays open while operators observe and repair the admission inputs.
    Grace,
    /// Only window, trusted and bootstrap identities are admitted.
    Closed,
}

/// Why a swarm cannot currently enforce `Closed` admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcAdmissionFallback {
    /// No authoritative versioned committee window has arrived.
    Missing,
    /// The window's lease expired or an older epoch attempted to replace it.
    Stale,
    /// One epoch supplied different committees.
    Contradictory,
    /// Committee records or identity mappings are still unresolved.
    Unresolved,
}

/// Why a swarm tracks a key; an entry lists every slot that applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RpcAdmissionSlot {
    /// Member of the previous epoch's committee in the admission window.
    Previous,
    /// Member of the current epoch's committee in the admission window.
    Current,
    /// Member of the next epoch's committee in the admission window.
    Next,
    /// Member of the committee two epochs ahead; tracked for export, never admitted for it.
    Scheduled,
    /// In the committee-eligible pool (pending activation, active or pending exit); tracked for
    /// export, never admitted for it.
    Eligible,
    /// Configured as a trusted node.
    Trusted,
    /// Configured as a bootstrap peer.
    Bootstrap,
}

/// Where an [`RpcAdmissionEntry`]'s addresses came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RpcEntrySource {
    /// A signed node record the swarm authenticated.
    Record,
    /// An operator-provisioned peer (trusted, bootstrap or explicit) with no signed record yet.
    Config,
    /// A `committee_peers` launch address, which overrides any record's addresses.
    Stub,
    /// No record is held; the entry has no addresses.
    Unresolved,
}

/// Trait used to get primary data for our RPC extension (tn namespace).
pub trait EngineToPrimary {
    /// Retrieve the latest consensus block.
    fn get_latest_consensus_block(&self) -> ConsensusHeader;
    /// Get an epoch header if found.
    fn epoch(
        &self,
        epoch: Option<Epoch>,
        hash: Option<EpochDigest>,
    ) -> impl Future<Output = Option<(EpochRecord, EpochCertificate)>> + Send;
    /// Get the consensus header with `digest` from `epoch`'s consensus pack.
    ///
    /// Returns `Ok(None)` when this node does not hold the header: the digest is unknown or the
    /// epoch's pack is absent. The RPC layer reports that as "not found" and asks again on the
    /// next request, so the header resolves once its pack arrives.
    ///
    /// Returns [`ConsensusStorageError`] when storage fails to answer, for example for a sealed
    /// epoch whose pack files are on disk but cannot be opened. The error carries no detail
    /// because RPC callers must never see storage internals, so implementations log the cause
    /// where it happens. The RPC layer reports the failure as an internal error and, for a short
    /// time afterwards, answers requests for that epoch without calling this method, so a caller
    /// that repeats the request does not reopen the pack and log the failure every time.
    fn consensus_header_by_digest(
        &self,
        epoch: Epoch,
        digest: ConsensusHeaderDigest,
    ) -> impl Future<Output = Result<Option<ConsensusHeader>, ConsensusStorageError>> + Send;
    /// Return the node's static information.
    fn node_info(&self) -> &RpcNodeInfo;
    /// Return the node's current consensus participation mode.
    ///
    /// Read live so callers can observe transient modes (e.g. `CvvInactive` while a restarted node
    /// catches up); it is not part of the static [`RpcNodeInfo`].
    fn node_mode(&self) -> NodeMode;
    /// Return the admission view of every swarm this node runs.
    ///
    /// Returns `None` until the node's networks have started; the RPC layer reports that as "not
    /// found". A swarm that does not answer in time is reported unavailable rather than delaying
    /// the snapshot, and an implementation may serve a briefly cached snapshot so that RPC callers
    /// cannot load the swarms' command channels.
    fn admission_snapshot(&self) -> impl Future<Output = Option<RpcAdmissionSnapshot>> + Send;
}

/// Consensus storage failed to answer an [`EngineToPrimary`] lookup.
///
/// Carries no detail: the implementation that hit the failure logs its cause, and the RPC layer
/// only needs to tell a failed lookup apart from a header that is not there.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("consensus storage failed to answer the lookup")]
pub struct ConsensusStorageError;
