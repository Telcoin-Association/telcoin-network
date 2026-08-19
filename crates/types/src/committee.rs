//! Committee of validators reach consensus.

use crate::{
    crypto::{BlsPublicKey, NetworkPublicKey},
    Address, Multiaddr, WorkerId,
};
use serde::{Deserialize, Serialize};
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

/// On-disk representations of a primary plus its worker list.
///
/// Shared by [BootstrapServer] and `NodeP2pInfo`, which both persist the same
/// `{ primary, workers }` shape. `Current` is the shape written today. `Legacy` is the
/// pre-multi-worker single `worker` map, still accepted on read so files written before the
/// worker list existed load unchanged.
#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum PrimaryWorkersRepr {
    /// The current shape: `workers: [..]`.
    Current {
        /// The p2p info of the primary.
        primary: P2pNode,
        /// The p2p info for each worker, never empty.
        #[serde(deserialize_with = "deserialize_non_empty_workers")]
        workers: Vec<P2pNode>,
    },
    /// The legacy shape: `worker: {..}`.
    Legacy {
        /// The p2p info of the primary.
        primary: P2pNode,
        /// The p2p info of the single worker (boxed to keep the variants a similar size).
        worker: Box<P2pNode>,
    },
}

impl From<PrimaryWorkersRepr> for (P2pNode, Vec<P2pNode>) {
    fn from(value: PrimaryWorkersRepr) -> Self {
        match value {
            PrimaryWorkersRepr::Current { primary, workers } => (primary, workers),
            PrimaryWorkersRepr::Legacy { primary, worker } => (primary, vec![*worker]),
        }
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
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(from = "PrimaryWorkersRepr")]
pub struct BootstrapServer {
    /// The p2p info the primary.
    pub primary: P2pNode,
    /// The p2p info for each worker, indexed by [WorkerId].
    pub workers: Vec<P2pNode>,
}

impl From<PrimaryWorkersRepr> for BootstrapServer {
    fn from(value: PrimaryWorkersRepr) -> Self {
        let (primary, workers) = value.into();
        Self { primary, workers }
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
#[derive(Serialize, Deserialize, Debug, Eq)]
struct CommitteeInner {
    /// The authorities of epoch.
    authorities: BTreeMap<BlsPublicKey, Authority>,
    /// Keeps and index of the Authorities by their respective identifier
    /// This is a helper struct, not included in serde or equality.
    #[serde(skip)]
    authorities_by_id: BTreeMap<AuthorityIdentifier, Authority>,
    /// The epoch number of this committee
    epoch: Epoch,
    /// The quorum threshold (2f+1)
    #[serde(skip)]
    quorum_threshold: VotingPower,
    /// The validity threshold (f+1)
    #[serde(skip)]
    validity_threshold: VotingPower,
    /// The bootstrap servers to initially join a network (probably the initial committee).
    /// Note, not included in partial eq since they are not relevand to overall committee equality.
    bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
    /// The number of workers every validator in this committee runs.
    ///
    /// This is a protocol-level value: all nodes must agree on it. Its source of truth is the
    /// on-chain `WorkerConfigs` contract, read at the previous epoch's closing block when the
    /// committee for an epoch is created. Committee files without the field default to one.
    #[serde(default = "default_num_workers")]
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
        Address, Authority, AuthorityIdentifier, BlsKeypair, BlsPublicKey, BootstrapServer,
        Committee, Multiaddr, NetworkKeypair, ParseAuthorityIdentifierError, ReputationScores,
        EQUAL_VOTING_POWER,
    };
    use rand::rng;
    use std::collections::BTreeMap;

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
