//! Committee of validators reach consensus.

use crate::{
    crypto::{BlsPublicKey, NetworkPublicKey},
    forks::committee_workers_active,
    Address, Multiaddr, WorkerId,
};
use serde::{ser::SerializeStruct, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Formatter},
    num::{NonZeroU64, NonZeroUsize},
    str::FromStr,
    sync::Arc,
};
use thiserror::Error;
use url::Url;

/// The epoch number.
/// Becomes the upper 32 bits of a nonce (with rounds the low bits).
pub type Epoch = u32;

/// The voting power an authority has within the committee.
pub type VotingPower = u64;
/// All authorities have equal voting power in consensus.
pub const EQUAL_VOTING_POWER: VotingPower = 1;

/// Maximum byte length of an advertised RPC endpoint URL.
pub const MAX_RPC_URL_LEN: usize = 2048;

/// Optional JSON-RPC endpoint metadata for a validator worker.
///
/// Advertised through the kademlia node record so peers can discover where to
/// submit transactions to the network. These are application-layer URLs consumed
/// by external clients (wallets/dapps); they are never dialed by the libp2p swarm.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcInfo {
    /// Required HTTP(S) JSON-RPC endpoint, e.g. `https://validator.example.com:8545/`.
    pub http: Url,
    /// Optional WebSocket JSON-RPC endpoint, e.g. `wss://validator.example.com:8546/`.
    pub ws: Option<Url>,
}

impl RpcInfo {
    /// Reject endpoints whose scheme is not appropriate for the field
    /// (`http`/`https` for [`Self::http`]; `ws`/`wss` for [`Self::ws`]).
    pub fn validate(&self) -> Result<(), RpcInfoError> {
        if self.http.as_str().len() > MAX_RPC_URL_LEN {
            return Err(RpcInfoError::UrlTooLong(self.http.as_str().len()));
        }
        match self.http.scheme() {
            "http" | "https" => {}
            scheme => {
                return Err(RpcInfoError::InvalidHttpScheme(scheme.to_string()));
            }
        }
        if let Some(ws) = &self.ws {
            if ws.as_str().len() > MAX_RPC_URL_LEN {
                return Err(RpcInfoError::UrlTooLong(ws.as_str().len()));
            }
            match ws.scheme() {
                "ws" | "wss" => {}
                scheme => {
                    return Err(RpcInfoError::InvalidWsScheme(scheme.to_string()));
                }
            }
        }
        Ok(())
    }
}

/// Error returned when validating an [`RpcInfo`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RpcInfoError {
    /// The `http` endpoint scheme is not `http` or `https`.
    #[error("invalid http endpoint scheme `{0}`, expected `http` or `https`")]
    InvalidHttpScheme(String),
    /// The `ws` endpoint scheme is not `ws` or `wss`.
    #[error("invalid ws endpoint scheme `{0}`, expected `ws` or `wss`")]
    InvalidWsScheme(String),
    /// An endpoint URL exceeds [`MAX_RPC_URL_LEN`].
    #[error("rpc endpoint URL length {0} exceeds maximum of {max} bytes", max = MAX_RPC_URL_LEN)]
    UrlTooLong(usize),
}

/// A multiaddr and network public key for a libp2p node.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct P2pNode {
    /// The network address of the node.
    pub network_address: Multiaddr,
    /// Network key of the node.
    pub network_key: NetworkPublicKey,
    /// Optional JSON-RPC endpoint advertised over kademlia (worker nodes only).
    ///
    /// Set on the worker [P2pNode] when the operator wants peers to be able to
    /// discover this validator's JSON-RPC endpoint. `None` on primary nodes and on
    /// worker nodes that do not expose RPC publicly.
    #[serde(default)]
    pub rpc: Option<RpcInfo>,
}

impl From<(Multiaddr, NetworkPublicKey)> for P2pNode {
    fn from(value: (Multiaddr, NetworkPublicKey)) -> Self {
        Self { network_address: value.0, network_key: value.1, rpc: None }
    }
}

impl From<(NetworkPublicKey, Multiaddr)> for P2pNode {
    fn from(value: (NetworkPublicKey, Multiaddr)) -> Self {
        Self { network_address: value.1, network_key: value.0, rpc: None }
    }
}

/// The current on-disk shape of a primary plus its worker list: `{ primary, workers }`.
///
/// Shared by [BootstrapServer] and `NodeP2pInfo`. This is the only shape written today and the
/// only shape binary codecs (bcs: the consensus store and pack encoding) read.
#[derive(Deserialize)]
pub(crate) struct PrimaryWorkersCurrent {
    /// The p2p info of the primary.
    pub(crate) primary: P2pNode,
    /// The p2p info for each worker, never empty.
    #[serde(deserialize_with = "deserialize_non_empty_workers")]
    pub(crate) workers: Vec<P2pNode>,
}

/// The legacy single-worker shape: `{ primary, worker }`.
///
/// Still accepted from human-readable files written before the worker list existed.
#[derive(Deserialize)]
pub(crate) struct PrimaryWorkersLegacy {
    /// The p2p info of the primary.
    pub(crate) primary: P2pNode,
    /// The p2p info of the single worker.
    pub(crate) worker: P2pNode,
}

/// Human-readable (YAML, JSON) representations of a primary plus its worker list.
///
/// Both variants are boxed so the enum stays small (clippy `large_enum_variant`; `P2pNode` is
/// wide). Untagged: serde tries [PrimaryWorkersCurrent] first, then [PrimaryWorkersLegacy].
/// Untagged enums buffer the input through `deserialize_any`, which non-self-describing codecs
/// (bcs) do not support, so this enum is only used when the deserializer reports
/// `is_human_readable()`; see [deserialize_primary_workers].
#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum PrimaryWorkersRepr {
    /// The current shape: `workers: [..]`.
    Current(Box<PrimaryWorkersCurrent>),
    /// The legacy shape: `worker: {..}`.
    Legacy(Box<PrimaryWorkersLegacy>),
}

impl From<PrimaryWorkersRepr> for (P2pNode, Vec<P2pNode>) {
    fn from(value: PrimaryWorkersRepr) -> Self {
        match value {
            PrimaryWorkersRepr::Current(current) => {
                let PrimaryWorkersCurrent { primary, workers } = *current;
                (primary, workers)
            }
            PrimaryWorkersRepr::Legacy(legacy) => {
                let PrimaryWorkersLegacy { primary, worker } = *legacy;
                (primary, vec![worker])
            }
        }
    }
}

/// Deserialize the `(primary, workers)` pair shared by [BootstrapServer] and `NodeP2pInfo`.
///
/// Human-readable formats (the YAML and JSON config and committee files) accept both the
/// current `workers: [..]` list and the legacy single `worker: {..}` map. Binary formats (bcs,
/// used by the consensus store and the consensus pack) carry only the current shape and are read
/// directly: the untagged legacy fallback needs `deserialize_any`, which bcs rejects.
pub(crate) fn deserialize_primary_workers<'de, D>(
    deserializer: D,
) -> Result<(P2pNode, Vec<P2pNode>), D::Error>
where
    D: serde::Deserializer<'de>,
{
    if deserializer.is_human_readable() {
        PrimaryWorkersRepr::deserialize(deserializer).map(Into::into)
    } else {
        PrimaryWorkersCurrent::deserialize(deserializer)
            .map(|PrimaryWorkersCurrent { primary, workers }| (primary, workers))
    }
}

/// Deserialize a worker list and reject an empty one.
///
/// A node must run at least one worker, so an empty list is a configuration error rather than a
/// valid value.
pub(crate) fn deserialize_non_empty_workers<'de, D>(
    deserializer: D,
) -> Result<Vec<P2pNode>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let workers = Vec::<P2pNode>::deserialize(deserializer)?;
    (!workers.is_empty())
        .then_some(workers)
        .ok_or_else(|| serde::de::Error::custom("`workers` must contain at least one entry"))
}

/// Bootstrap p2p server info to join the network.
///
/// `workers` holds one [P2pNode] per worker, indexed by [WorkerId], and is never empty. It is a
/// dial-hint list, not a validated invariant: a bootstrap entry may advertise fewer workers than
/// [Committee::number_of_workers] (today every entry advertises exactly one). Per-worker
/// bootstrap addresses arrive with the per-worker swarms.
///
/// Serialization: the current on-disk shape is `workers: [..]`. The legacy single-worker shape
/// `worker: {..}` is still accepted on read so existing committee files load unchanged; it is
/// written back in the new shape.
#[derive(Clone, Serialize, Debug, Eq, PartialEq)]
pub struct BootstrapServer {
    /// The p2p info the primary.
    pub primary: P2pNode,
    /// The p2p info for each worker, indexed by [WorkerId].
    pub workers: Vec<P2pNode>,
}

impl<'de> Deserialize<'de> for BootstrapServer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_primary_workers(deserializer)
            .map(|(primary, workers)| Self { primary, workers })
    }
}

impl From<PrimaryWorkersLegacy> for BootstrapServer {
    fn from(value: PrimaryWorkersLegacy) -> Self {
        let PrimaryWorkersLegacy { primary, worker } = value;
        Self { primary, workers: vec![worker] }
    }
}

impl BootstrapServer {
    /// Create a new [BootstrapServer] with one [P2pNode] per worker.
    pub fn new(primary_node: P2pNode, worker_nodes: Vec<P2pNode>) -> Self {
        Self { primary: primary_node, workers: worker_nodes }
    }

    /// Return the [P2pNode] for `worker_id`, or `None` if the server has no such worker.
    pub fn worker(&self, worker_id: WorkerId) -> Option<&P2pNode> {
        self.workers.get(usize::from(worker_id))
    }

    /// Return the number of workers this bootstrap server advertises.
    pub fn num_workers(&self) -> usize {
        self.workers.len()
    }
}

/// The default worker count for a committee: one worker per validator.
const ONE_WORKER: NonZeroUsize = NonZeroUsize::MIN;

/// Serde default for [CommitteeInner::num_workers] so committee files without the field load.
fn default_num_workers() -> NonZeroUsize {
    ONE_WORKER
}

/// Immutable authority data.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
struct AuthorityInner {
    /// The authority's main BlsPublicKey which is used to verify the content they sign.
    protocol_key: BlsPublicKey,
    /// The execution address for the authority.
    /// This address will be used as the suggested fee recipient.
    execution_address: Address,
}

/// An Authority, a member of the committee.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Authority {
    inner: Arc<AuthorityInner>,
}

impl Authority {
    /// The constructor is not public by design. Everyone who wants to create authorities should do
    /// it via Committee (more specifically can use [CommitteeBuilder]). As some internal properties
    /// of Authority are initialised via the Committee, to ensure that the user will not
    /// accidentally use stale Authority data, should always derive them via the Commitee.
    fn new(protocol_key: BlsPublicKey, execution_address: Address) -> Self {
        Self { inner: Arc::new(AuthorityInner { protocol_key, execution_address }) }
    }

    /// Version of new that can be called directly.  Useful for testing, if you are calling this
    /// outside of a test you are wrong (see comment on new).
    pub fn new_for_test(protocol_key: BlsPublicKey, execution_address: Address) -> Self {
        Self { inner: Arc::new(AuthorityInner { protocol_key, execution_address }) }
    }

    pub fn id(&self) -> AuthorityIdentifier {
        let bytes = self.inner.protocol_key.to_bytes();
        let mut hasher = crate::DefaultHashFunction::new();
        hasher.update(&bytes);
        AuthorityIdentifier(Arc::new(*hasher.finalize().as_bytes()))
    }

    pub fn protocol_key(&self) -> &BlsPublicKey {
        &self.inner.protocol_key
    }

    pub fn voting_power(&self) -> VotingPower {
        EQUAL_VOTING_POWER
    }

    pub fn execution_address(&self) -> Address {
        self.inner.execution_address
    }
}

impl Serialize for Authority {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ok = self.inner.serialize(serializer)?;
        Ok(ok)
    }
}

impl<'de> Deserialize<'de> for Authority {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = AuthorityInner::deserialize(deserializer)?;
        Ok(Self { inner: Arc::new(inner) })
    }
}

/// The committee lists all validators that participate in consensus.
///
/// Deliberately carries no serde derives: the binary wire layout is epoch-gated (#554, gated by
/// [`crate::forks::committee_workers_active`]), so every encode and decode path routes through the
/// hand-written impls below. Human-readable formats keep the derive's exact behavior through
/// [`CommitteeInnerHr`].
#[derive(Debug, Eq)]
struct CommitteeInner {
    /// The authorities of epoch.
    authorities: BTreeMap<BlsPublicKey, Authority>,
    /// Keeps and index of the Authorities by their respective identifier
    /// This is a helper struct, not included in serde or equality.
    authorities_by_id: BTreeMap<AuthorityIdentifier, Authority>,
    /// The epoch number of this committee
    epoch: Epoch,
    /// The quorum threshold (2f+1)
    /// Derived from the authorities, never serialized.
    quorum_threshold: VotingPower,
    /// The validity threshold (f+1)
    /// Derived from the authorities, never serialized.
    validity_threshold: VotingPower,
    /// The bootstrap servers to initially join a network (probably the initial committee).
    /// Note, not included in partial eq since they are not relevand to overall committee equality.
    bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
    /// The number of workers every validator in this committee runs.
    ///
    /// This is a protocol-level value: all nodes must agree on it. Its source of truth is the
    /// on-chain `WorkerConfigs` contract, read at the previous epoch's closing block when the
    /// committee for an epoch is created. Committee files without the field default to one.
    num_workers: NonZeroUsize,
}

impl Default for CommitteeInner {
    fn default() -> Self {
        Self {
            authorities: BTreeMap::default(),
            authorities_by_id: BTreeMap::default(),
            epoch: Epoch::default(),
            quorum_threshold: VotingPower::default(),
            validity_threshold: VotingPower::default(),
            bootstrap_servers: BTreeMap::default(),
            num_workers: ONE_WORKER,
        }
    }
}

impl PartialEq for CommitteeInner {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch
            && self.quorum_threshold == other.quorum_threshold
            && self.validity_threshold == other.validity_threshold
            && self.num_workers == other.num_workers
            && self.authorities.eq(&other.authorities)
    }
}

impl CommitteeInner {
    /// Assemble the wire fields of a committee into a [CommitteeInner].
    ///
    /// The three derived indexes are left at their defaults: they are absent from every wire
    /// layout in both directions, and [`Committee::deserialize`] rebuilds them by calling
    /// [`CommitteeInner::load`].
    fn from_wire_fields(
        authorities: BTreeMap<BlsPublicKey, Authority>,
        epoch: Epoch,
        bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
        num_workers: NonZeroUsize,
    ) -> Self {
        Self {
            authorities,
            authorities_by_id: BTreeMap::default(),
            epoch,
            quorum_threshold: VotingPower::default(),
            validity_threshold: VotingPower::default(),
            bootstrap_servers,
            num_workers,
        }
    }

    /// Updates the committee internal secondary indexes.
    fn load(&mut self) {
        self.authorities_by_id = self
            .authorities
            .values()
            .map(|authority| {
                let id = authority.id();
                (id, authority.clone())
            })
            .collect();

        self.validity_threshold = self.calculate_validity_threshold().get();
        self.quorum_threshold = self.calculate_quorum_threshold().get();
        assert!(self.authorities_by_id.len() > 1, "committee size must be larger that 1");
    }

    fn calculate_quorum_threshold(&self) -> NonZeroU64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: VotingPower = self.total_voting_power();
        NonZeroU64::new(2 * total_votes / 3 + 1).expect("arithmetic always produces result above 0")
    }

    fn calculate_validity_threshold(&self) -> NonZeroU64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: VotingPower = self.total_voting_power();
        NonZeroU64::new(total_votes.div_ceil(3)).unwrap_or(NonZeroU64::new(1).expect("1 is NOT 0!"))
    }

    fn total_voting_power(&self) -> VotingPower {
        self.authorities.len() as VotingPower
    }
}

/// Number of `CommitteeInner` wire fields on the legacy (pre-multi-worker) layout.
const COMMITTEE_FIELDS_LEGACY: usize = 3;
/// Number of `CommitteeInner` wire fields once the multi-worker layout is active for the
/// committee's epoch.
const COMMITTEE_FIELDS_V1: usize = 4;
/// Field names for [`serde::Deserializer::deserialize_struct`], superset (post-fork) layout.
///
/// Naming all four fields is load-bearing, not cosmetic: bcs sizes its field sequence from this
/// list, so a three-entry list would make the post-fork read of the trailing `num_workers` return
/// `None` instead of decoding it. The legacy arm simply stops after three fields, which bcs
/// permits — it never checks that the sequence was drained.
const COMMITTEE_FIELD_NAMES: [&str; COMMITTEE_FIELDS_V1] =
    ["authorities", "epoch", "bootstrap_servers", "num_workers"];

/// Human-readable (YAML, JSON) representation of [`CommitteeInner`], carrying the exact field set
/// and attributes the removed derive had.
///
/// The epoch gate is binary-only. Human-readable formats name their fields, so a committee file
/// written by any build loads on any other: `num_workers` defaults when absent (as in every
/// `chain-configs/*/committee.yaml` today), and each [`BootstrapServer`] still accepts the legacy
/// `worker:` key through its own deserializer. Both are covered by the tests below.
#[derive(Deserialize)]
#[serde(rename = "CommitteeInner")]
struct CommitteeInnerHr {
    /// The authorities of epoch.
    authorities: BTreeMap<BlsPublicKey, Authority>,
    /// The epoch number of this committee.
    epoch: Epoch,
    /// The bootstrap servers to initially join a network.
    bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
    /// The number of workers every validator in this committee runs.
    #[serde(default = "default_num_workers")]
    num_workers: NonZeroUsize,
}

/// Borrowed serialization view writing a [`BootstrapServer`] in the legacy single-worker shape
/// (`{ primary, worker }`), byte-identical to the pre-#554 derive.
///
/// Used only by the pre-fork arm of [`CommitteeInner`]'s binary serializer. The shape cannot
/// represent more than one worker, so the view fails closed rather than silently dropping the
/// rest: a committee that the legacy layout cannot express must be unrepresentable, not
/// mis-encoded. Callers below the fork epoch are structurally single-worker (the pre-fork
/// on-chain registry exposes no path that raises the count), so this error means a bug or a
/// governance action taken too early, never routine operation.
struct BootstrapServerLegacyRef<'a>(&'a BootstrapServer);

impl Serialize for BootstrapServerLegacyRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let [worker] = self.0.workers.as_slice() else {
            return Err(serde::ser::Error::custom(format!(
                "the pre-fork layout holds exactly one worker per bootstrap server, got {}",
                self.0.workers.len()
            )));
        };
        // serialize_struct mirrors the pre-#554 derive exactly: bcs emits no framing for structs,
        // so the wire bytes are the concatenated fields in declaration order.
        let mut state = serializer.serialize_struct("BootstrapServer", 2)?;
        state.serialize_field("primary", &self.0.primary)?;
        state.serialize_field("worker", worker)?;
        state.end()
    }
}

/// Extracts the next element of the committee field sequence, converting an early end of input
/// into a field-labeled error (bcs would otherwise surface only a distal `Eof`).
fn next_committee_field<'de, A, T>(seq: &mut A, field: &'static str) -> Result<T, A::Error>
where
    A: serde::de::SeqAccess<'de>,
    T: Deserialize<'de>,
{
    seq.next_element()?.ok_or_else(|| serde::de::Error::missing_field(field))
}

impl Serialize for CommitteeInner {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // human-readable formats name their fields, so the committee file is never gated: it
        // carries the current shape at every epoch, exactly as the derive wrote it. for binary
        // formats the gate reads the epoch inside this committee, never node-local state, so a
        // historical committee keeps its historical layout at any nesting depth (pack records,
        // epoch records, state-sync payloads).
        if serializer.is_human_readable() || committee_workers_active(self.epoch) {
            let mut state = serializer.serialize_struct("CommitteeInner", COMMITTEE_FIELDS_V1)?;
            state.serialize_field("authorities", &self.authorities)?;
            state.serialize_field("epoch", &self.epoch)?;
            state.serialize_field("bootstrap_servers", &self.bootstrap_servers)?;
            state.serialize_field("num_workers", &self.num_workers)?;
            return state.end();
        }

        // fail closed: the legacy layout has no field to carry a worker count, so encoding a
        // multi-worker committee below the fork epoch would silently write a committee that
        // decodes as single-worker on every node, including this one.
        if self.num_workers != ONE_WORKER {
            return Err(serde::ser::Error::custom(format!(
                "committee for epoch {} runs {} workers, which the pre-fork layout cannot hold",
                self.epoch, self.num_workers
            )));
        }
        let legacy_servers: BTreeMap<&BlsPublicKey, BootstrapServerLegacyRef<'_>> = self
            .bootstrap_servers
            .iter()
            .map(|(key, server)| (key, BootstrapServerLegacyRef(server)))
            .collect();
        let mut state = serializer.serialize_struct("CommitteeInner", COMMITTEE_FIELDS_LEGACY)?;
        state.serialize_field("authorities", &self.authorities)?;
        state.serialize_field("epoch", &self.epoch)?;
        state.serialize_field("bootstrap_servers", &legacy_servers)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for CommitteeInner {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let CommitteeInnerHr { authorities, epoch, bootstrap_servers, num_workers } =
                CommitteeInnerHr::deserialize(deserializer)?;
            return Ok(Self::from_wire_fields(authorities, epoch, bootstrap_servers, num_workers));
        }

        /// Reads the two ungated fields, then branches on the just-decoded `epoch`: pre-fork
        /// values carry one unprefixed `worker` per bootstrap server and no `num_workers`.
        struct CommitteeInnerVisitor;

        impl<'de> serde::de::Visitor<'de> for CommitteeInnerVisitor {
            type Value = CommitteeInner;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "a CommitteeInner: authorities and epoch, then bootstrap servers in the \
                     layout that epoch selects, plus num_workers once the multi-worker fork is \
                     active",
                )
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let authorities = next_committee_field(&mut seq, "authorities")?;
                let epoch: Epoch = next_committee_field(&mut seq, "epoch")?;
                if committee_workers_active(epoch) {
                    let bootstrap_servers = next_committee_field(&mut seq, "bootstrap_servers")?;
                    let num_workers = next_committee_field(&mut seq, "num_workers")?;
                    return Ok(CommitteeInner::from_wire_fields(
                        authorities,
                        epoch,
                        bootstrap_servers,
                        num_workers,
                    ));
                }

                // the legacy value type is the whole discriminator: the two shapes are not
                // self-describing under bcs (`primary ++ worker` against `primary ++ ULEB128(n)
                // ++ n worker`), so sniffing the bytes is unsound and only the epoch decides.
                let legacy: BTreeMap<BlsPublicKey, PrimaryWorkersLegacy> =
                    next_committee_field(&mut seq, "bootstrap_servers")?;
                let bootstrap_servers =
                    legacy.into_iter().map(|(key, server)| (key, server.into())).collect();
                Ok(CommitteeInner::from_wire_fields(
                    authorities,
                    epoch,
                    bootstrap_servers,
                    ONE_WORKER,
                ))
            }
        }

        deserializer.deserialize_struct(
            "CommitteeInner",
            &COMMITTEE_FIELD_NAMES,
            CommitteeInnerVisitor,
        )
    }
}

/// The committee lists all validators that participate in consensus.
#[derive(Clone, Debug, Default)]
pub struct Committee {
    inner: Arc<CommitteeInner>,
}

impl Serialize for Committee {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ok = self.inner.serialize(serializer)?;
        Ok(ok)
    }
}

impl<'de> Deserialize<'de> for Committee {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut inner = CommitteeInner::deserialize(deserializer)?;
        inner.load();
        Ok(Self { inner: Arc::new(inner) })
    }
}

impl PartialEq for Committee {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for Committee {}

// Every authority gets uniquely identified by the AuthorityIdentifier
// The type can be easily swapped without needing to change anything else in the implementation.
// Currently it is the hash of the authorities BLS key (which will be stable).
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Hash)]
pub struct AuthorityIdentifier(Arc<[u8; 32]>);

impl Serialize for AuthorityIdentifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            // JSON: serialize as bs58 string
            serializer.serialize_str(&self.to_string())
        } else {
            // Binary: serialize as raw bytes for backward compatibility
            self.0.as_ref().serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for AuthorityIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        if deserializer.is_human_readable() {
            // JSON: deserialize from bs58 string
            let s = String::deserialize(deserializer)?;
            s.parse().map_err(D::Error::custom)
        } else {
            // Binary: deserialize from raw bytes
            let bytes = <[u8; 32]>::deserialize(deserializer)?;
            Ok(Self::from_bytes(bytes))
        }
    }
}

impl AuthorityIdentifier {
    /// Create an `AuthorityIdentifier` from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Arc::new(bytes))
    }

    pub fn dummy_for_test(byte: u8) -> Self {
        Self(Arc::new([byte; 32]))
    }
}

impl From<BlsPublicKey> for AuthorityIdentifier {
    fn from(value: BlsPublicKey) -> Self {
        let bytes = value.to_bytes();
        let mut hasher = crate::DefaultHashFunction::new();
        hasher.update(&bytes);
        AuthorityIdentifier(Arc::new(*hasher.finalize().as_bytes()))
    }
}

impl Default for AuthorityIdentifier {
    fn default() -> Self {
        Self(Arc::new([0_u8; 32]))
    }
}

impl Display for AuthorityIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&bs58::encode(&*self.0).into_string())
    }
}

impl std::fmt::Debug for AuthorityIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&bs58::encode(&*self.0).into_string())
    }
}

/// Error when parsing an `AuthorityIdentifier` from a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseAuthorityIdentifierError {
    /// Invalid bs58 encoding.
    InvalidBs58(String),
    /// Invalid length (expected 32 bytes).
    InvalidLength { expected: usize, actual: usize },
}

impl std::fmt::Display for ParseAuthorityIdentifierError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBs58(msg) => write!(f, "invalid bs58 encoding: {msg}"),
            Self::InvalidLength { expected, actual } => {
                write!(f, "invalid length: expected {expected} bytes, got {actual}")
            }
        }
    }
}

impl std::error::Error for ParseAuthorityIdentifierError {}

impl FromStr for AuthorityIdentifier {
    type Err = ParseAuthorityIdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = bs58::decode(s)
            .into_vec()
            .map_err(|e| ParseAuthorityIdentifierError::InvalidBs58(e.to_string()))?;
        let bytes: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
            ParseAuthorityIdentifierError::InvalidLength { expected: 32, actual: v.len() }
        })?;
        Ok(Self::from_bytes(bytes))
    }
}

impl Committee {
    /// Any committee should be created via the [CommitteeBuilder] - this is intentionally
    /// a private method.
    fn new(
        authorities: BTreeMap<BlsPublicKey, Authority>,
        epoch: Epoch,
        bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
        num_workers: NonZeroUsize,
    ) -> Self {
        let mut committee = CommitteeInner {
            authorities,
            epoch,
            authorities_by_id: Default::default(),
            validity_threshold: 0,
            quorum_threshold: 0,
            bootstrap_servers,
            num_workers,
        };
        committee.load();

        // Some sanity checks to ensure that we'll not end up in invalid state
        assert_eq!(committee.authorities_by_id.len(), committee.authorities.len());

        assert_eq!(committee.validity_threshold, committee.calculate_validity_threshold().get());
        assert_eq!(committee.quorum_threshold, committee.calculate_quorum_threshold().get());

        Self { inner: Arc::new(committee) }
    }

    /// Expose new for tests.  If you are calling this outside of a test you are wrong, see comment
    /// on new.
    ///
    /// Pass an optional epoch_boundary timestamp. Defaults to u64::MAX to disable epoch
    /// transitions. The committee has one worker; use [Committee::with_num_workers] to widen it.
    pub fn new_for_test(
        authorities: BTreeMap<BlsPublicKey, Authority>,
        epoch: Epoch,
        bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
    ) -> Self {
        let mut committee = CommitteeInner {
            authorities,
            epoch,
            authorities_by_id: Default::default(),
            validity_threshold: 0,
            quorum_threshold: 0,
            bootstrap_servers,
            num_workers: ONE_WORKER,
        };

        committee.authorities_by_id = committee
            .authorities
            .values()
            .map(|authority| (authority.id(), authority.clone()))
            .collect();
        committee.validity_threshold = committee.calculate_validity_threshold().get();
        committee.quorum_threshold = committee.calculate_quorum_threshold().get();
        assert!(committee.authorities_by_id.len() > 1, "committee size must be larger that 1");
        // Some sanity checks to ensure that we'll not end up in invalid state
        assert_eq!(committee.authorities_by_id.len(), committee.authorities.len());
        committee.load();

        Self { inner: Arc::new(committee) }
    }

    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.inner.epoch
    }

    /// Provided an identifier it returns the corresponding authority
    pub fn authority(&self, identifier: &AuthorityIdentifier) -> Option<Authority> {
        self.inner.authorities_by_id.get(identifier).cloned()
    }

    pub fn authority_by_key(&self, key: &BlsPublicKey) -> Option<Authority> {
        self.inner.authorities.get(key).cloned()
    }

    pub fn authorities(&self) -> Vec<Authority> {
        // Return sorted by id (using the id keyed BTree) since this may be important to some code.
        self.inner.authorities_by_id.values().cloned().collect()
    }

    /// Return true if the authority for id is in the committee.
    pub fn is_authority(&self, id: &AuthorityIdentifier) -> bool {
        // Return sorted by id (using the id keyed BTree) since this may be important to some code.
        self.inner.authorities_by_id.contains_key(id)
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.inner.authorities.len()
    }

    /// Return the voting power of a specific authority.
    pub fn voting_power(&self, name: &BlsPublicKey) -> VotingPower {
        self.inner.authorities.get(&name.clone()).map_or_else(|| 0, |_| EQUAL_VOTING_POWER)
    }

    pub fn voting_power_by_id(&self, id: &AuthorityIdentifier) -> VotingPower {
        self.inner.authorities_by_id.get(id).map_or_else(|| 0, |_| EQUAL_VOTING_POWER)
    }

    /// Returns the voting power required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> VotingPower {
        self.inner.quorum_threshold
    }

    /// Returns the voting power required to reach availability (f+1).
    pub fn validity_threshold(&self) -> VotingPower {
        self.inner.validity_threshold
    }

    /// Returns true if the provided stake has reached quorum (2f+1)
    pub fn reached_quorum(&self, voting_power: VotingPower) -> bool {
        voting_power >= self.quorum_threshold()
    }

    /// Returns true if the provided stake has reached availability (f+1)
    pub fn reached_validity(&self, voting_power: VotingPower) -> bool {
        voting_power >= self.validity_threshold()
    }

    pub fn total_voting_power(&self) -> VotingPower {
        self.inner.total_voting_power()
    }

    /// Return all the network addresses in the committee.
    pub fn others_primaries_by_id(
        &self,
        myself: Option<&AuthorityIdentifier>,
    ) -> Vec<(AuthorityIdentifier, BlsPublicKey)> {
        self.inner
            .authorities
            .iter()
            .filter(
                |(_, authority)| {
                    if let Some(myself) = myself {
                        &authority.id() != myself
                    } else {
                        true
                    }
                },
            )
            .map(|(_, authority)| (authority.id(), *authority.protocol_key()))
            .collect()
    }

    /// Returns the bls keys of all members except `myself`.
    pub fn others_keys_except(&self, myself: &BlsPublicKey) -> Vec<BlsPublicKey> {
        self.inner
            .authorities
            .values()
            .filter_map(|authority| {
                if authority.protocol_key() == myself {
                    None
                } else {
                    Some(*authority.protocol_key())
                }
            })
            .collect()
    }

    /// Returns all the bls keys of all members.
    /// Return as a BTreeSet to inforce an order.
    pub fn bls_keys(&self) -> BTreeSet<BlsPublicKey> {
        self.inner.authorities.values().map(|authority| *authority.protocol_key()).collect()
    }

    /// Return the bootstrap record for key if it exists.
    pub fn get_bootstrap(&self, key: &BlsPublicKey) -> Option<BootstrapServer> {
        self.inner.bootstrap_servers.get(key).cloned()
    }

    /// Return the map of bootstrap servers.
    pub fn bootstrap_servers(&self) -> BTreeMap<BlsPublicKey, BootstrapServer> {
        self.inner.bootstrap_servers.clone()
    }

    /// Used for testing - not recommended to use for any other case.
    /// It creates a new instance with updated epoch
    pub fn advance_epoch_for_test(&self, new_epoch: Epoch) -> Committee {
        Committee::new_for_test(
            self.inner.authorities.clone(),
            new_epoch,
            self.inner.bootstrap_servers.clone(),
        )
    }

    /// Return the number of workers that are in use for this committee.
    ///
    /// This is a protocol level value, all nodes have to agree on this and be
    /// running the required number of workers. The source of truth is the on-chain
    /// `WorkerConfigs` contract at the previous epoch's closing block, so a change takes
    /// effect at an epoch boundary. Committees built without an explicit count have one worker.
    pub fn number_of_workers(&self) -> usize {
        self.inner.num_workers.get()
    }

    /// Return a copy of this committee with the worker count set to `num_workers`.
    ///
    /// The epoch-0 committee is loaded from the committee file, whose count is a default; the
    /// epoch manager uses this to stamp the on-chain count onto it. Every other field, including
    /// the derived indexes and thresholds, is copied as-is: this does not re-run
    /// `CommitteeInner::load`, so it is safe on a default (empty) committee as well.
    pub fn with_num_workers(&self, num_workers: NonZeroUsize) -> Committee {
        let inner = CommitteeInner {
            authorities: self.inner.authorities.clone(),
            authorities_by_id: self.inner.authorities_by_id.clone(),
            epoch: self.inner.epoch,
            quorum_threshold: self.inner.quorum_threshold,
            validity_threshold: self.inner.validity_threshold,
            bootstrap_servers: self.inner.bootstrap_servers.clone(),
            num_workers,
        };
        Committee { inner: Arc::new(inner) }
    }
}

impl std::fmt::Display for Committee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Committee E{}: {:?}",
            self.epoch(),
            self.inner
                .authorities
                .keys()
                .map(|x| {
                    if let Some(k) = x.encode_base58().get(0..16) {
                        k.to_owned()
                    } else {
                        format!("Invalid key: {x}")
                    }
                })
                .collect::<Vec<_>>()
        )
    }
}

/// Type for building committees.
#[derive(Debug)]
pub struct CommitteeBuilder {
    /// The epoch for the committee.
    epoch: Epoch,
    /// The map of [BlsPublicKey] for each [Authority] in the committee.
    authorities: BTreeMap<BlsPublicKey, Authority>,
    /// The map of [BlsPublicKey] for each [BootstrapServer].
    bootstrap_server: BTreeMap<BlsPublicKey, BootstrapServer>,
    /// The number of workers every validator runs (defaults to one).
    num_workers: NonZeroUsize,
}

impl CommitteeBuilder {
    /// Create a new instance of [CommitteeBuilder] for making a new [Committee].
    pub fn new(epoch: Epoch) -> Self {
        Self {
            epoch,
            authorities: BTreeMap::default(),
            bootstrap_server: BTreeMap::default(),
            num_workers: ONE_WORKER,
        }
    }

    /// Set the number of workers every validator in the committee runs.
    pub fn with_num_workers(mut self, num_workers: NonZeroUsize) -> Self {
        self.num_workers = num_workers;
        self
    }

    /// Add an authority and bootstrap server to the committee builder.
    pub fn add_authority_and_bootstrap(
        &mut self,
        protocol_key: BlsPublicKey,
        primary_node: P2pNode,
        worker_nodes: Vec<P2pNode>,
        execution_address: Address,
    ) {
        let authority = Authority::new(protocol_key, execution_address);
        self.authorities.insert(protocol_key, authority);
        let bootstrap = BootstrapServer::new(primary_node, worker_nodes);
        self.bootstrap_server.insert(protocol_key, bootstrap);
    }

    /// Add an authority to the committee builder.
    pub fn add_authority(&mut self, protocol_key: BlsPublicKey, execution_address: Address) {
        let authority = Authority::new(protocol_key, execution_address);
        self.authorities.insert(protocol_key, authority);
    }

    /// Add a bootstrap server to the committee builder.
    pub fn add_bootstrap_server(
        &mut self,
        protocol_key: BlsPublicKey,
        primary_node: P2pNode,
        worker_nodes: Vec<P2pNode>,
    ) {
        let bootstrap = BootstrapServer::new(primary_node, worker_nodes);
        self.bootstrap_server.insert(protocol_key, bootstrap);
    }

    /// Build the [Committee].
    pub fn build(self) -> Committee {
        Committee::new(self.authorities, self.epoch, self.bootstrap_server, self.num_workers)
    }
}

/// The quorum threshold (2f+1)
/// This assumes all committee members have the same voting power of 1.
pub fn quorum_threshold(committee_members: u64) -> u64 {
    ((2 * committee_members) / 3) + 1
}

#[cfg(test)]
mod tests {
    use crate::{
        encode, try_decode, Address, Authority, AuthorityIdentifier, BlsKeypair, BlsPublicKey,
        BootstrapServer, Committee, Epoch, Multiaddr, NetworkKeypair, P2pNode,
        ParseAuthorityIdentifierError, ReputationScores, RpcInfo, EQUAL_VOTING_POWER,
    };
    use rand::rng;
    use serde::{Deserialize, Serialize};
    use std::{collections::BTreeMap, num::NonZeroUsize};

    #[test]
    fn committee_load() {
        // GIVEN
        let mut rng = rng();
        let num_of_authorities = 10;

        let authorities = (0..num_of_authorities)
            .enumerate()
            .map(|(i, _)| {
                let keypair = BlsKeypair::generate(&mut rng);
                let execution_address = Address::repeat_byte(i as u8);

                let a = Authority::new(*keypair.public(), execution_address);

                (*keypair.public(), a)
            })
            .collect::<BTreeMap<BlsPublicKey, Authority>>();

        let bootstrap_servers = authorities
            .keys()
            .map(|key| {
                let primary_keypair = NetworkKeypair::generate_ed25519();
                let worker_keypair = NetworkKeypair::generate_ed25519();

                let b = BootstrapServer::new(
                    (Multiaddr::empty(), primary_keypair.public().clone().into()).into(),
                    vec![(Multiaddr::empty(), worker_keypair.public().clone().into()).into()],
                );

                (*key, b)
            })
            .collect::<BTreeMap<BlsPublicKey, BootstrapServer>>();

        // WHEN
        let committee = Committee::new(authorities, 10, bootstrap_servers, super::ONE_WORKER);

        // THEN
        assert_eq!(committee.inner.authorities_by_id.len() as u64, num_of_authorities);
        assert_eq!(committee.inner.authorities.len() as u64, num_of_authorities);

        for (identifier, authority) in committee.inner.authorities_by_id.iter() {
            assert_eq!(*identifier, authority.id());
        }

        // AND ensure thresholds are calculated correctly
        assert_eq!(committee.quorum_threshold(), 7);
        assert_eq!(committee.validity_threshold(), 4);

        let guard = committee.inner;
        // AND ensure authorities are in both maps
        let mut total = 0;
        for ((public_key, authority_1), (boot_key, _)) in
            guard.authorities.iter().zip(guard.bootstrap_servers.iter())
        {
            assert_eq!(public_key, authority_1.protocol_key());
            assert_eq!(public_key, boot_key);
            let authority_2 = guard.authorities_by_id.get(&authority_1.id()).unwrap();
            assert_eq!(authority_1, authority_2);
            total += 1;
        }
        assert_eq!(total, num_of_authorities);
    }

    /// A [BootstrapServer] with fresh keys and `num_workers` workers.
    fn bootstrap_with_workers(num_workers: usize) -> BootstrapServer {
        let primary_keypair = NetworkKeypair::generate_ed25519();
        BootstrapServer::new(
            (Multiaddr::empty(), primary_keypair.public().clone().into()).into(),
            (0..num_workers)
                .map(|_| {
                    let keypair = NetworkKeypair::generate_ed25519();
                    (Multiaddr::empty(), keypair.public().clone().into()).into()
                })
                .collect(),
        )
    }

    /// Indent every line of a YAML fragment by two spaces so it nests under a key (the document
    /// start marker is dropped).
    fn indent(fragment: &str) -> String {
        fragment
            .lines()
            .filter(|l| *l != "---")
            .map(|l| format!("  {l}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn bootstrap_server_new_shape_round_trips() {
        let bootstrap = bootstrap_with_workers(2);
        let yaml = serde_yaml::to_string(&bootstrap).expect("serialize");
        assert!(yaml.contains("workers:"), "new shape must serialize `workers`: {yaml}");
        assert!(!yaml.contains("\nworker:"), "new shape must not serialize `worker`: {yaml}");
        let decoded: BootstrapServer = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(decoded, bootstrap);
        assert_eq!(decoded.num_workers(), 2);
        assert_eq!(decoded.worker(1), bootstrap.workers.get(1));
        assert!(decoded.worker(2).is_none());
    }

    #[test]
    fn bootstrap_server_legacy_shape_decodes_as_one_worker() {
        let expected = bootstrap_with_workers(1);
        let worker = expected.worker(0).expect("one worker").clone();
        let primary_yaml = serde_yaml::to_string(&expected.primary).expect("serialize primary");
        let worker_yaml = serde_yaml::to_string(&worker).expect("serialize worker");
        let legacy =
            format!("primary:\n{}\nworker:\n{}\n", indent(&primary_yaml), indent(&worker_yaml));
        let decoded: BootstrapServer = serde_yaml::from_str(&legacy).expect("legacy deserialize");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn bootstrap_server_empty_workers_rejected() {
        let bootstrap = bootstrap_with_workers(1);
        let primary_yaml = serde_yaml::to_string(&bootstrap.primary).expect("serialize primary");
        let doc = format!("primary:\n{}\nworkers: []\n", indent(&primary_yaml));
        let result: Result<BootstrapServer, _> = serde_yaml::from_str(&doc);
        assert!(result.is_err(), "empty workers must be rejected");
    }

    #[test]
    fn bootstrap_server_bcs_round_trips() {
        // bcs is not self-describing: the legacy-tolerant untagged path cannot run there, so the
        // binary codec must read the current shape directly.
        let bootstrap = bootstrap_with_workers(2);
        let bytes = crate::encode(&bootstrap);
        let decoded: BootstrapServer = crate::try_decode(&bytes).expect("bcs deserialize");
        assert_eq!(decoded, bootstrap);
    }

    /// A committee at `epoch` with four authorities whose bootstrap servers each advertise
    /// `workers_per_server` workers, with the committee's worker count set to match.
    #[cfg(feature = "adiri")]
    fn committee_with_workers(epoch: crate::Epoch, workers_per_server: usize) -> Committee {
        let mut rng = rng();
        let authorities = (0..4u8)
            .map(|i| {
                let keypair = BlsKeypair::generate(&mut rng);
                (*keypair.public(), Authority::new(*keypair.public(), Address::repeat_byte(i)))
            })
            .collect::<BTreeMap<BlsPublicKey, Authority>>();
        let bootstrap_servers = authorities
            .keys()
            .map(|key| (*key, bootstrap_with_workers(workers_per_server)))
            .collect::<BTreeMap<BlsPublicKey, BootstrapServer>>();
        Committee::new(
            authorities,
            epoch,
            bootstrap_servers,
            std::num::NonZeroUsize::new(workers_per_server).expect("worker count is not 0"),
        )
    }

    /// Below the fork epoch a committee is encoded in the legacy single-worker layout, which has
    /// no field for a worker count: the encoder refuses a committee it cannot represent rather
    /// than write one that decodes as single-worker on every node.
    ///
    /// The adiri fork epoch is a `u32::MAX` placeholder, so every epoch is pre-fork in this lane.
    /// `TN_COMMITTEE_WORKERS_FORK_EPOCH` is deliberately not used to stage that: the override's
    /// `OnceLock` is process-wide and the whole test binary shares one process.
    #[cfg(feature = "adiri")]
    #[test]
    fn committee_bcs_legacy_layout_refuses_multi_worker() {
        // bcs directly rather than `crate::encode`, which panics on a serializer error
        let multi_worker = committee_with_workers(7, 2);
        assert!(
            bcs::to_bytes(&multi_worker).is_err(),
            "the pre-fork layout cannot represent a two-worker committee"
        );

        // a single-worker committee round-trips, and re-encoding the decoded value reproduces the
        // exact bytes: a pre-fork pack survives the decode/re-append cycle unchanged
        let committee = committee_with_workers(7, 1);
        let bytes = bcs::to_bytes(&committee).expect("legacy encode");
        let decoded: Committee = crate::try_decode(&bytes).expect("bcs deserialize");
        assert_eq!(decoded, committee);
        assert_eq!(decoded.number_of_workers(), 1);
        assert_eq!(decoded.bootstrap_servers().len(), 4);
        assert!(decoded.bootstrap_servers().values().all(|server| server.num_workers() == 1));
        assert_eq!(bcs::to_bytes(&decoded).expect("legacy re-encode"), bytes);
    }

    /// Default builds have the multi-worker layout active from genesis, so this exercises the
    /// post-fork arm; the adiri lane covers the legacy arm above.
    #[cfg(not(feature = "adiri"))]
    #[test]
    fn committee_bcs_round_trips_with_bootstrap_servers() {
        // The consensus store and the consensus pack persist committees with bcs.
        let mut rng = rng();
        let authorities = (0..4u8)
            .map(|i| {
                let keypair = BlsKeypair::generate(&mut rng);
                (*keypair.public(), Authority::new(*keypair.public(), Address::repeat_byte(i)))
            })
            .collect::<BTreeMap<BlsPublicKey, Authority>>();
        let bootstrap_servers = authorities
            .keys()
            .map(|key| (*key, bootstrap_with_workers(2)))
            .collect::<BTreeMap<BlsPublicKey, BootstrapServer>>();
        let committee = Committee::new(
            authorities,
            7,
            bootstrap_servers,
            std::num::NonZeroUsize::new(2).expect("2 is not 0"),
        );
        let bytes = crate::encode(&committee);
        let decoded: Committee = crate::try_decode(&bytes).expect("bcs deserialize");
        assert_eq!(decoded, committee);
        assert_eq!(decoded.number_of_workers(), 2);
        assert_eq!(decoded.bootstrap_servers().len(), 4);
        assert!(decoded.bootstrap_servers().values().all(|server| server.num_workers() == 2));
    }

    /// Legacy (pre-#554) shadow of the [`BootstrapServer`] wire layout: a primary plus a single
    /// unprefixed `worker`.
    ///
    /// Derived serde, so it is byte-identical to the `origin/main` `BootstrapServer` derive output
    /// BY CONSTRUCTION — same field types, same order, same attributes. #554 left [`P2pNode`]
    /// untouched, so the real type is reused here rather than shadowed: only the field that
    /// changed shape gets a shadow.
    #[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
    struct BootstrapReprLegacy {
        /// The p2p info the primary.
        primary: P2pNode,
        /// The p2p info the worker.
        worker: P2pNode,
    }

    impl From<BootstrapReprLegacy> for BootstrapServer {
        fn from(value: BootstrapReprLegacy) -> Self {
            let BootstrapReprLegacy { primary, worker } = value;
            Self { primary, workers: vec![worker] }
        }
    }

    /// Legacy (pre-#554) shadow of the [`CommitteeInner`] wire layout: its three serialized
    /// fields, with derived serde, so it is byte-identical to the `origin/main` derive output BY
    /// CONSTRUCTION.
    ///
    /// `origin/main` interleaved three `#[serde(skip)]` helper fields between these
    /// (`authorities_by_id`, `quorum_threshold`, `validity_threshold`). Skipped fields never reach
    /// the wire, so omitting them preserves both the field set and its order. Names are absent
    /// too: bcs writes neither struct nor field names, so only the types and their order decide
    /// the bytes and no `#[serde(rename)]` is load-bearing here.
    #[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
    struct CommitteeReprLegacy {
        /// The authorities of epoch.
        authorities: BTreeMap<BlsPublicKey, Authority>,
        /// The epoch number of this committee.
        epoch: Epoch,
        /// The bootstrap servers to initially join a network.
        bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapReprLegacy>,
    }

    /// Build the real [`Committee`] a legacy shadow describes.
    ///
    /// The worker count is one: the legacy layout holds exactly one worker per bootstrap server
    /// and carries no count field, so a single-worker committee is the only thing it can express.
    fn committee_from_legacy_repr(repr: &CommitteeReprLegacy) -> Committee {
        let bootstrap_servers = repr
            .bootstrap_servers
            .iter()
            .map(|(key, server)| (*key, server.clone().into()))
            .collect();
        Committee::new(repr.authorities.clone(), repr.epoch, bootstrap_servers, super::ONE_WORKER)
    }

    /// Project a [`Committee`] onto its legacy shadow, so both sides of the differential can also
    /// be derived from a single committee rather than only from a single fixture.
    ///
    /// # Panics
    ///
    /// If any bootstrap server advertises anything other than exactly one worker. The legacy
    /// layout has no field to hold a second worker, so such a committee has no legacy shadow at
    /// all — the same value the pre-fork encoder refuses to write rather than truncate.
    fn legacy_repr_from_committee(committee: &Committee) -> CommitteeReprLegacy {
        let bootstrap_servers = committee
            .inner
            .bootstrap_servers
            .iter()
            .map(|(key, server)| {
                let [worker] = server.workers.as_slice() else {
                    panic!(
                        "the legacy layout holds exactly one worker per bootstrap server, got {}",
                        server.workers.len()
                    );
                };
                let repr =
                    BootstrapReprLegacy { primary: server.primary.clone(), worker: worker.clone() };
                (*key, repr)
            })
            .collect();
        CommitteeReprLegacy {
            authorities: committee.inner.authorities.clone(),
            epoch: committee.inner.epoch,
            bootstrap_servers,
        }
    }

    /// Epoch of the legacy-layout fixtures: 407, which is `CONSENSUS_REGISTRY_FORK_EPOCH` and the
    /// documented arming floor of the committee worker-list fork.
    ///
    /// It sits below the worker fork epoch under every build, so `adiri` encodes it in the legacy
    /// layout. Choosing the floor rather than an arbitrary number puts the fixture on the first
    /// epoch whose single-worker guarantee is operational rather than structural — the pre-fork
    /// epoch with the least margin for error.
    const LEGACY_FIXTURE_EPOCH: Epoch = 407;

    /// Seed tag marking a fixture primary's network key, so no primary shares a key with a worker.
    const PRIMARY_SEED_TAG: u8 = 0xB0;
    /// Seed tag marking a fixture worker's network key.
    const WORKER_SEED_TAG: u8 = 0xC0;

    /// Deterministic BLS keypair for fixture authority slot `slot`.
    ///
    /// Built from a fixed scalar rather than a seeded rng, so the derived public key — and with it
    /// the `authorities` map order and every encoded byte — is stable across `rand` version bumps
    /// as well as across runs. The leading bytes stay zero, which keeps the scalar far below the
    /// BLS12-381 group order and nonzero, the only two values `blst` rejects.
    fn fixture_bls_keypair(slot: u8) -> BlsKeypair {
        let mut scalar = [0_u8; 32];
        scalar[30] = slot;
        scalar[31] = 0x2A;
        BlsKeypair::from_bytes(&scalar).expect("fixture bls scalar is a valid private key")
    }

    /// A fixed 32-byte ed25519 secret seed identifying one fixture node.
    fn fixture_seed(tag: u8, authority: u8, worker: u8) -> [u8; 32] {
        let mut seed = [0_u8; 32];
        seed[0] = tag;
        seed[1] = authority;
        seed[2] = worker;
        seed
    }

    /// A [`P2pNode`] from a fixed ed25519 seed and port.
    ///
    /// ed25519 secret keys *are* 32-byte seeds, so a fixed seed yields a fixed public key with no
    /// rng in the path; the multiaddr comes from a literal rather than an OS-assigned port.
    fn fixture_p2p_node(seed: [u8; 32], port: u16, rpc: Option<RpcInfo>) -> P2pNode {
        P2pNode {
            network_address: format!("/ip4/127.0.0.1/udp/{port}/quic-v1")
                .parse()
                .expect("fixture multiaddr parses"),
            network_key: NetworkKeypair::ed25519_from_bytes(seed)
                .expect("a 32-byte array is a valid ed25519 secret seed")
                .public()
                .clone()
                .into(),
            rpc,
        }
    }

    /// The primary node of fixture authority `authority`. Primaries never advertise rpc.
    fn fixture_primary_node(authority: u8) -> P2pNode {
        fixture_p2p_node(
            fixture_seed(PRIMARY_SEED_TAG, authority, 0),
            40_000 + u16::from(authority),
            None,
        )
    }

    /// Worker `worker` of fixture authority `authority`.
    ///
    /// The first worker of the first authority advertises an rpc endpoint and no other worker
    /// does, so both arms of [`P2pNode::rpc`]'s `Option` appear in the encoded fixture.
    fn fixture_worker_node(authority: u8, worker: u8) -> P2pNode {
        let rpc = (authority == 0 && worker == 0).then(|| RpcInfo {
            http: "https://validator0.example.com:8545/".parse().expect("fixture http url"),
            ws: Some("wss://validator0.example.com:8546/".parse().expect("fixture ws url")),
        });
        fixture_p2p_node(
            fixture_seed(WORKER_SEED_TAG, authority, worker),
            41_000 + u16::from(authority) * 8 + u16::from(worker),
            rpc,
        )
    }

    /// Shape of a deterministic committee fixture for wire-layout tests.
    ///
    /// Every value it produces — BLS keys, network keys, multiaddrs, execution addresses, rpc
    /// endpoints — is derived from a constant plus a slot index, with no rng, no clock and no
    /// OS-assigned port, so the encoded bytes are reproducible across runs, machines and
    /// dependency bumps. Here that matters more than realism: these fixtures anchor the
    /// differential assertions below and, later, frozen golden byte vectors.
    #[derive(Clone, Copy, Debug)]
    struct CommitteeWireFixture {
        /// The committee's epoch, which is the only input the wire-layout gate reads.
        epoch: Epoch,
        /// Number of authorities. Must be at least two: `CommitteeInner::load` asserts a committee
        /// larger than one.
        authorities: u8,
        /// Number of bootstrap servers, attached to authority slots `0..bootstrap_servers`.
        ///
        /// Tracked separately from [`Self::authorities`] because the two maps are independent on
        /// the wire: a committee may carry fewer bootstrap hints than it has authorities. Letting
        /// the two lengths differ gives the maps distinct ULEB128 length prefixes, so a layout
        /// that read one where the other belongs would not line up.
        bootstrap_servers: u8,
        /// Workers each bootstrap server advertises, which is also the committee's worker count.
        workers_per_server: u8,
    }

    impl CommitteeWireFixture {
        /// A single-worker fixture: the only shape the legacy layout can express.
        const fn single_worker(epoch: Epoch, authorities: u8, bootstrap_servers: u8) -> Self {
            Self { epoch, authorities, bootstrap_servers, workers_per_server: 1 }
        }

        /// The committee-level worker count this fixture describes.
        fn num_workers(self) -> NonZeroUsize {
            NonZeroUsize::new(usize::from(self.workers_per_server))
                .expect("a fixture runs at least one worker per server")
        }

        /// The fixture's authorities, keyed by BLS public key.
        ///
        /// Shared by both builders below: #554 left this field's layout untouched, and a byte
        /// comparison is only meaningful when both sides hold the same values. Only the fields
        /// whose shape changed are built twice.
        fn authority_map(self) -> BTreeMap<BlsPublicKey, Authority> {
            (0..self.authorities)
                .map(|slot| {
                    let key = *fixture_bls_keypair(slot).public();
                    (key, Authority::new(key, Address::repeat_byte(slot)))
                })
                .collect()
        }

        /// The `(slot, bls key)` pairs that carry a bootstrap server.
        ///
        /// # Panics
        ///
        /// If the fixture has more bootstrap servers than authorities: servers attach to authority
        /// slots, so a wider fixture would key them off nonexistent authorities.
        fn bootstrap_slots(self) -> impl Iterator<Item = (u8, BlsPublicKey)> {
            assert!(
                self.bootstrap_servers <= self.authorities,
                "fixture has {} bootstrap servers but only {} authorities to attach them to",
                self.bootstrap_servers,
                self.authorities
            );
            (0..self.bootstrap_servers).map(|slot| (slot, *fixture_bls_keypair(slot).public()))
        }

        /// Build the [`Committee`] this fixture describes.
        fn committee(self) -> Committee {
            let bootstrap_servers = self
                .bootstrap_slots()
                .map(|(slot, key)| {
                    let workers = (0..self.workers_per_server)
                        .map(|worker| fixture_worker_node(slot, worker))
                        .collect();
                    (key, BootstrapServer::new(fixture_primary_node(slot), workers))
                })
                .collect();
            Committee::new(self.authority_map(), self.epoch, bootstrap_servers, self.num_workers())
        }

        /// Build the legacy shadow this fixture describes.
        ///
        /// Reaches the shadow reprs straight from the slot-derived keys and nodes, never through
        /// [`Self::committee`], so a bug in the gated encoder cannot make the two sides of the
        /// differential agree.
        ///
        /// # Panics
        ///
        /// If the fixture runs more than one worker per server. The legacy layout carries exactly
        /// one unprefixed `worker` per bootstrap server and no count field, so a wider fixture has
        /// no legacy shadow at all.
        fn legacy_repr(self) -> CommitteeReprLegacy {
            assert_eq!(
                self.workers_per_server, 1,
                "the legacy layout holds exactly one worker per bootstrap server"
            );
            let bootstrap_servers = self
                .bootstrap_slots()
                .map(|(slot, key)| {
                    let repr = BootstrapReprLegacy {
                        primary: fixture_primary_node(slot),
                        worker: fixture_worker_node(slot, 0),
                    };
                    (key, repr)
                })
                .collect();
            CommitteeReprLegacy {
                authorities: self.authority_map(),
                epoch: self.epoch,
                bootstrap_servers,
            }
        }
    }

    /// The legacy shadow reprs are self-consistent under bcs, reproducible, and describe the same
    /// value as the [`Committee`] the same fixture builds.
    ///
    /// Runs on every lane so the reprs, the fixture and the conversions stay compiled and
    /// exercised in builds whose gate never selects the legacy layout (non-adiri is post-fork from
    /// genesis). The byte-level differential against the gated encoder is adiri-only, below.
    #[test]
    fn legacy_committee_repr_round_trips() {
        let fixture = CommitteeWireFixture::single_worker(LEGACY_FIXTURE_EPOCH, 4, 3);
        let repr = fixture.legacy_repr();
        let bytes = encode(&repr);

        // a second, independent build of the same fixture encodes to the same bytes: no rng,
        // clock or OS-assigned port leaked into the fixture
        assert_eq!(encode(&fixture.legacy_repr()), bytes, "fixture bytes are not reproducible");

        // encode -> decode -> re-encode is byte-identical
        let decoded: CommitteeReprLegacy =
            try_decode(&bytes).expect("legacy repr decodes its own bytes");
        assert_eq!(decoded, repr, "legacy repr lost data through bcs");
        assert_eq!(encode(&decoded), bytes, "legacy repr re-encode diverged");

        // the shadow and the real committee describe the same value in both directions.
        // `Committee`'s PartialEq deliberately ignores bootstrap servers, so compare those too.
        let committee = fixture.committee();
        let from_repr = committee_from_legacy_repr(&repr);
        assert_eq!(from_repr, committee, "legacy repr describes a different committee");
        assert_eq!(
            from_repr.bootstrap_servers(),
            committee.bootstrap_servers(),
            "legacy repr describes different bootstrap servers"
        );
        assert_eq!(
            legacy_repr_from_committee(&committee),
            repr,
            "committee projects onto a different legacy repr"
        );

        // the two bootstrap layouts genuinely differ: the current shape length-prefixes its worker
        // list, so the same value encodes exactly one byte longer. if these ever matched, the fork
        // gate — and every assertion built on it — would be testing nothing.
        let legacy_server = repr.bootstrap_servers.values().next().expect("fixture has servers");
        let current_server = BootstrapServer::from(legacy_server.clone());
        assert_eq!(
            encode(&current_server).len(),
            encode(legacy_server).len() + 1,
            "the current bootstrap layout must differ from the legacy one"
        );
    }

    /// Pre-fork differential: the epoch-gated [`Committee`] and the derived legacy shadow encode
    /// to the same bytes, and each side reads what the other wrote.
    ///
    /// The `adiri` worker-fork epoch is a `u32::MAX` placeholder, so every epoch is pre-fork in
    /// this lane. `TN_COMMITTEE_WORKERS_FORK_EPOCH` is deliberately not used to stage that: the
    /// override's `OnceLock` is process-wide and the whole test binary shares one process.
    #[cfg(feature = "adiri")]
    #[test]
    fn committee_bcs_pre_fork_layout_matches_legacy_repr() {
        let fixture = CommitteeWireFixture::single_worker(LEGACY_FIXTURE_EPOCH, 4, 3);
        let committee = fixture.committee();
        assert!(
            !crate::forks::committee_workers_active(committee.epoch()),
            "epoch {} must be pre-fork for this differential to mean anything; is \
             TN_COMMITTEE_WORKERS_FORK_EPOCH set in the environment, or has the fork been armed?",
            committee.epoch()
        );

        let repr = fixture.legacy_repr();
        let legacy_bytes = encode(&repr);
        let gated_bytes = encode(&committee);

        // the gated encoder reproduces the pre-#554 derive byte for byte
        assert_eq!(gated_bytes, legacy_bytes, "pre-fork Committee bytes left the legacy layout");

        // legacy bytes decode through the gated `Committee`: a pack written by a pre-#554 binary
        // still loads on this build
        let from_legacy: Committee =
            try_decode(&legacy_bytes).expect("legacy bytes decode as a Committee");
        assert_eq!(from_legacy, committee);
        assert_eq!(from_legacy.bootstrap_servers(), committee.bootstrap_servers());
        assert_eq!(from_legacy.number_of_workers(), 1);

        // gated bytes decode through the shadow: a pre-#554 binary still reads what this build
        // writes for a pre-fork epoch
        let read_back: CommitteeReprLegacy =
            try_decode(&gated_bytes).expect("pre-fork Committee bytes decode as the legacy repr");
        assert_eq!(read_back, repr, "pre-fork Committee bytes are not the legacy layout");
    }

    #[test]
    fn committee_yaml_without_num_workers_defaults_to_one() {
        let mut rng = rng();
        let authorities = (0..4u8)
            .map(|i| {
                let keypair = BlsKeypair::generate(&mut rng);
                (*keypair.public(), Authority::new(*keypair.public(), Address::repeat_byte(i)))
            })
            .collect::<BTreeMap<BlsPublicKey, Authority>>();
        let bootstrap_servers = authorities
            .keys()
            .map(|key| (*key, bootstrap_with_workers(1)))
            .collect::<BTreeMap<BlsPublicKey, BootstrapServer>>();
        let committee = Committee::new(
            authorities,
            3,
            bootstrap_servers,
            std::num::NonZeroUsize::new(2).expect("2 is not 0"),
        );
        assert_eq!(committee.number_of_workers(), 2);

        // strip the field: an older committee file has no `num_workers`
        let mut yaml_value = serde_yaml::to_value(&committee).expect("YAML serialization failed");
        let map = yaml_value.as_mapping_mut().expect("committee should serialize to a mapping");
        assert!(map.remove(&serde_yaml::Value::String("num_workers".to_string())).is_some());
        let decoded: Committee = serde_yaml::from_value(yaml_value).expect("deserialize");
        assert_eq!(decoded.number_of_workers(), 1);
        assert_eq!(decoded.epoch(), 3);
        // and the count round-trips when present
        let yaml = serde_yaml::to_string(&committee).expect("serialize");
        let decoded: Committee = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(decoded.number_of_workers(), 2);
        assert_eq!(decoded.with_num_workers(std::num::NonZeroUsize::MIN).number_of_workers(), 1);
    }

    #[test]
    fn with_num_workers_keeps_derived_fields_and_accepts_default_committee() {
        // a default (empty) committee is what a missing committee file loads as: no panic
        let widened = Committee::default()
            .with_num_workers(std::num::NonZeroUsize::new(2).expect("2 is not 0"));
        assert_eq!(widened.number_of_workers(), 2);
        assert_eq!(widened.size(), 0);

        // a real committee keeps its authorities, indexes and thresholds
        let mut rng = rng();
        let authorities = (0..4u8)
            .map(|i| {
                let keypair = BlsKeypair::generate(&mut rng);
                (*keypair.public(), Authority::new(*keypair.public(), Address::repeat_byte(i)))
            })
            .collect::<BTreeMap<BlsPublicKey, Authority>>();
        let bootstrap_servers = authorities
            .keys()
            .map(|key| (*key, bootstrap_with_workers(1)))
            .collect::<BTreeMap<BlsPublicKey, BootstrapServer>>();
        let committee =
            Committee::new(authorities, 5, bootstrap_servers, std::num::NonZeroUsize::MIN);
        let widened =
            committee.with_num_workers(std::num::NonZeroUsize::new(3).expect("3 is not 0"));
        assert_eq!(widened.number_of_workers(), 3);
        assert_eq!(widened.epoch(), 5);
        assert_eq!(widened.size(), committee.size());
        assert_eq!(widened.quorum_threshold(), committee.quorum_threshold());
        assert_eq!(widened.validity_threshold(), committee.validity_threshold());
        assert_eq!(widened.bootstrap_servers().len(), 4);
        assert!(committee
            .authorities()
            .iter()
            .all(|authority| widened.authority(&authority.id()).is_some()));
    }

    #[test]
    fn committee_yaml_deserialize_with_legacy_authority_voting_power() {
        let mut rng = rng();
        let num_of_authorities = 4;

        let authorities = (0..num_of_authorities)
            .enumerate()
            .map(|(i, _)| {
                let keypair = BlsKeypair::generate(&mut rng);
                let execution_address = Address::repeat_byte(i as u8);
                let authority = Authority::new(*keypair.public(), execution_address);
                (*keypair.public(), authority)
            })
            .collect::<BTreeMap<BlsPublicKey, Authority>>();

        let bootstrap_servers = authorities
            .keys()
            .map(|key| {
                let primary_keypair = NetworkKeypair::generate_ed25519();
                let worker_keypair = NetworkKeypair::generate_ed25519();
                let bootstrap = BootstrapServer::new(
                    (Multiaddr::empty(), primary_keypair.public().clone().into()).into(),
                    vec![(Multiaddr::empty(), worker_keypair.public().clone().into()).into()],
                );
                (*key, bootstrap)
            })
            .collect::<BTreeMap<BlsPublicKey, BootstrapServer>>();

        let committee = Committee::new(authorities, 0, bootstrap_servers, super::ONE_WORKER);
        let mut yaml_value = serde_yaml::to_value(&committee).expect("YAML serialization failed");
        let committee_map =
            yaml_value.as_mapping_mut().expect("committee should serialize to a mapping");
        let authorities_key = serde_yaml::Value::String("authorities".to_string());
        let authorities_value = committee_map
            .get_mut(&authorities_key)
            .expect("committee YAML should contain authorities");
        let authorities_map =
            authorities_value.as_mapping_mut().expect("authorities should serialize as a mapping");

        for (_, authority_value) in authorities_map.iter_mut() {
            let authority_map =
                authority_value.as_mapping_mut().expect("authority should serialize as a mapping");
            authority_map.insert(
                serde_yaml::Value::String("voting_power".to_string()),
                serde_yaml::Value::from(999_u64),
            );
        }

        let legacy_yaml =
            serde_yaml::to_string(&yaml_value).expect("legacy committee YAML conversion failed");
        let reloaded: Committee =
            serde_yaml::from_str(&legacy_yaml).expect("legacy committee YAML should deserialize");

        assert_eq!(reloaded.total_voting_power(), num_of_authorities as u64);
        for authority in reloaded.authorities() {
            assert_eq!(authority.voting_power(), EQUAL_VOTING_POWER);
        }
    }

    #[test]
    fn reputation_scores_json_roundtrip() {
        let mut scores = ReputationScores::default();
        let id1 = AuthorityIdentifier::from_bytes([1u8; 32]);
        let id2 = AuthorityIdentifier::from_bytes([2u8; 32]);
        scores.scores_per_authority.insert(id1.clone(), 100);
        scores.scores_per_authority.insert(id2.clone(), 200);

        // Serialize to JSON
        let json = serde_json::to_string(&scores).expect("JSON serialization failed");

        // Verify JSON contains bs58 string keys (not array format)
        assert!(json.contains(&id1.to_string()), "JSON should contain bs58 key for id1");
        assert!(json.contains(&id2.to_string()), "JSON should contain bs58 key for id2");
        assert!(!json.contains("[["), "JSON should not be array of tuples format");

        // Deserialize back
        let parsed: ReputationScores =
            serde_json::from_str(&json).expect("JSON deserialization failed");
        assert_eq!(scores, parsed);
    }

    #[test]
    fn reputation_scores_bincode_roundtrip() {
        let mut scores = ReputationScores::default();
        let id1 = AuthorityIdentifier::from_bytes([1u8; 32]);
        let id2 = AuthorityIdentifier::from_bytes([2u8; 32]);
        scores.scores_per_authority.insert(id1, 100);
        scores.scores_per_authority.insert(id2, 200);

        // Serialize to bincode
        let bytes = bincode::serialize(&scores).expect("bincode serialization failed");

        // Deserialize back
        let parsed: ReputationScores =
            bincode::deserialize(&bytes).expect("bincode deserialization failed");
        assert_eq!(scores, parsed);
    }

    #[test]
    fn reputation_scores_json_invalid_key() {
        // JSON with invalid bs58 key (contains '0' which is not valid bs58)
        let invalid_json =
            r#"{"scores_per_authority":{"invalid0key":100},"final_of_schedule":false}"#;
        let result: Result<ReputationScores, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err(), "Should fail on invalid bs58 key");
    }

    #[test]
    fn authority_identifier_json_serialization() {
        let id = AuthorityIdentifier::from_bytes([42u8; 32]);

        // JSON should serialize as a bs58 string
        let json = serde_json::to_string(&id).expect("JSON serialization failed");

        // Should be a quoted string, not an array
        assert!(json.starts_with('"'), "JSON should be a string");
        assert!(json.ends_with('"'), "JSON should be a string");
        assert!(!json.contains('['), "JSON should not be an array");

        // The string should be the bs58 representation
        let expected = format!("\"{id}\"");
        assert_eq!(json, expected);

        // Roundtrip
        let parsed: AuthorityIdentifier =
            serde_json::from_str(&json).expect("JSON deserialization failed");
        assert_eq!(id, parsed);
    }

    #[test]
    fn authority_identifier_bincode_serialization() {
        let bytes = [42u8; 32];
        let id = AuthorityIdentifier::from_bytes(bytes);

        // Serialize to bincode
        let serialized = bincode::serialize(&id).expect("bincode serialization failed");

        // Bincode should serialize as raw 32 bytes (the array itself)
        assert_eq!(serialized.len(), 32, "bincode should be exactly 32 bytes");
        assert_eq!(serialized.as_slice(), &bytes, "bincode should be raw bytes");

        // Roundtrip
        let parsed: AuthorityIdentifier =
            bincode::deserialize(&serialized).expect("bincode deserialization failed");
        assert_eq!(id, parsed);
    }

    #[test]
    fn authority_identifier_bincode_backward_compatibility() {
        // Simulate data serialized with the OLD format (raw bytes)
        // This ensures we maintain backward compatibility
        let raw_bytes: [u8; 32] = [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ];

        // The old format was just the raw 32 bytes
        let parsed: AuthorityIdentifier =
            bincode::deserialize(&raw_bytes).expect("Should deserialize old format");

        // Verify we got the right bytes
        assert_eq!(parsed, AuthorityIdentifier::from_bytes(raw_bytes));
    }

    #[test]
    fn authority_identifier_display_fromstr_roundtrip() {
        // Test with known bytes
        let bytes = [42u8; 32];
        let id = AuthorityIdentifier::from_bytes(bytes);

        // Display should produce bs58
        let displayed = id.to_string();
        assert!(!displayed.is_empty());

        // FromStr should parse back to the same identifier
        let parsed: AuthorityIdentifier = displayed.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn authority_identifier_fromstr_invalid_bs58() {
        // Invalid bs58 character (0, O, I, l are not valid in bs58)
        let result: Result<AuthorityIdentifier, _> = "invalid0string".parse();
        assert!(matches!(result, Err(ParseAuthorityIdentifierError::InvalidBs58(_))));
    }

    #[test]
    fn authority_identifier_fromstr_wrong_length() {
        // Valid bs58 but wrong length (only 4 bytes when decoded)
        let short = bs58::encode([1u8, 2, 3, 4]).into_string();
        let result: Result<AuthorityIdentifier, _> = short.parse();
        assert!(matches!(
            result,
            Err(ParseAuthorityIdentifierError::InvalidLength { expected: 32, actual: 4 })
        ));
    }

    /// Pre-upgrade `P2pNode` YAML (no `rpc_*` keys) must still deserialize and
    /// produce `None` for both new fields. Guards the no-flag-day promise we
    /// give to the YAML config layer.
    #[test]
    fn p2p_node_yaml_legacy_deserializes_with_default_rpc() {
        use crate::{NetworkKeypair, P2pNode};

        // build a legacy YAML payload using a real generated network key. We
        // emit the bs58 encoding directly to avoid serde_yaml document markers.
        let public = NetworkKeypair::generate_ed25519().public();
        let key_bs58 = bs58::encode(public.encode_protobuf()).into_string();
        let legacy_yaml =
            format!("network_address: /ip4/127.0.0.1/udp/49584/quic-v1\nnetwork_key: {key_bs58}\n");
        let parsed: P2pNode = serde_yaml::from_str(&legacy_yaml).expect("legacy YAML deserialize");
        assert!(parsed.rpc.is_none());
    }

    /// New-format YAML (with an `rpc` key) must round-trip through serde_yaml.
    #[test]
    fn p2p_node_yaml_with_rpc_roundtrip() {
        use crate::{NetworkKeypair, P2pNode, RpcInfo};

        let original = P2pNode {
            network_address: "/ip4/127.0.0.1/udp/49584/quic-v1".parse().expect("multiaddr"),
            network_key: NetworkKeypair::generate_ed25519().public().clone().into(),
            rpc: Some(RpcInfo {
                http: "https://validator.example.com:8545/".parse().expect("http url"),
                ws: Some("wss://validator.example.com:8546/".parse().expect("ws url")),
            }),
        };
        let yaml = serde_yaml::to_string(&original).expect("serialize");
        let parsed: P2pNode = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(parsed, original);
    }

    /// `RpcInfo::validate` rejects endpoints with an inappropriate scheme.
    #[test]
    fn rpc_info_validate_rejects_wrong_scheme() {
        use crate::{RpcInfo, RpcInfoError};

        // ftp is not a valid http(s) scheme
        let bad_http = RpcInfo {
            http: "ftp://validator.example.com:8545/".parse().expect("ftp url"),
            ws: None,
        };
        assert!(matches!(bad_http.validate(), Err(RpcInfoError::InvalidHttpScheme(_))));

        // a plain http url is not a valid ws(s) scheme
        let bad_ws = RpcInfo {
            http: "https://validator.example.com:8545/".parse().expect("http url"),
            ws: Some("http://validator.example.com:8546/".parse().expect("http url")),
        };
        assert!(matches!(bad_ws.validate(), Err(RpcInfoError::InvalidWsScheme(_))));

        // well-formed endpoints pass
        let good = RpcInfo {
            http: "https://validator.example.com:8545/".parse().expect("http url"),
            ws: Some("wss://validator.example.com:8546/".parse().expect("ws url")),
        };
        assert!(good.validate().is_ok());
    }

    /// `RpcInfo::validate` rejects endpoints whose URL exceeds `MAX_RPC_URL_LEN`.
    #[test]
    fn rpc_info_validate_rejects_oversized_url() {
        use crate::{RpcInfo, RpcInfoError, MAX_RPC_URL_LEN};

        // an http URL whose length exceeds the cap is rejected, even with a valid scheme
        let bad = RpcInfo {
            http: format!("https://x.example/{}", "a".repeat(MAX_RPC_URL_LEN))
                .parse()
                .expect("http url"),
            ws: None,
        };
        assert!(bad.http.as_str().len() > MAX_RPC_URL_LEN);
        assert!(matches!(bad.validate(), Err(RpcInfoError::UrlTooLong(_))));

        // a scheme-valid URL just under the cap still passes
        let prefix = "https://x.example/";
        let good = RpcInfo {
            http: format!("{prefix}{}", "a".repeat(MAX_RPC_URL_LEN - prefix.len() - 1))
                .parse()
                .expect("http url"),
            ws: None,
        };
        assert!(good.http.as_str().len() < MAX_RPC_URL_LEN);
        assert!(good.validate().is_ok());
    }
}
