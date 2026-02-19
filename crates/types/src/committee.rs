//! Committee of validators reach consensus.

use crate::{
    crypto::{BlsPublicKey, NetworkPublicKey},
    Address, Multiaddr,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Display, Formatter},
    num::NonZeroU64,
    str::FromStr,
    sync::Arc,
};

/// The epoch number.
/// Becomes the upper 32 bits of a nonce (with rounds the low bits).
pub type Epoch = u32;

/// The voting power an authority has within the committee.
pub type VotingPower = u64;
/// All authorities have equal voting power in consensus.
pub const EQUAL_VOTING_POWER: VotingPower = 1;

/// A multiaddr and network public key for a libp2p node.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct P2pNode {
    /// The network address of the node.
    pub network_address: Multiaddr,
    /// Network key of the node.
    pub network_key: NetworkPublicKey,
}

impl From<(Multiaddr, NetworkPublicKey)> for P2pNode {
    fn from(value: (Multiaddr, NetworkPublicKey)) -> Self {
        Self { network_address: value.0, network_key: value.1 }
    }
}

impl From<(NetworkPublicKey, Multiaddr)> for P2pNode {
    fn from(value: (NetworkPublicKey, Multiaddr)) -> Self {
        Self { network_address: value.1, network_key: value.0 }
    }
}

/// Bootstrap p2p server info to join the network.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct BootstrapServer {
    /// The p2p info the primary.
    pub primary: P2pNode,
    /// The p2p info the worker.
    pub worker: P2pNode,
}

impl BootstrapServer {
    pub fn new(primary_node: P2pNode, worker_node: P2pNode) -> Self {
        Self { primary: primary_node, worker: worker_node }
    }
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
    fn new(
        protocol_key: BlsPublicKey,
        _voting_power: VotingPower,
        execution_address: Address,
    ) -> Self {
        Self { inner: Arc::new(AuthorityInner { protocol_key, execution_address }) }
    }

    /// Version of new that can be called directly.  Useful for testing, if you are calling this
    /// outside of a test you are wrong (see comment on new).
    pub fn new_for_test(
        protocol_key: BlsPublicKey,
        _voting_power: VotingPower,
        execution_address: Address,
    ) -> Self {
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
#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default)]
struct CommitteeInner {
    /// The authorities of epoch.
    authorities: BTreeMap<BlsPublicKey, Authority>,
    /// Keeps and index of the Authorities by their respective identifier
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
    bootstrap_servers: BTreeMap<BlsPublicKey, BootstrapServer>,
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
    inner: Arc<RwLock<CommitteeInner>>,
}

impl Serialize for Committee {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ok = self.inner.read().serialize(serializer)?;
        Ok(ok)
    }
}

impl<'de> Deserialize<'de> for Committee {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = CommitteeInner::deserialize(deserializer)?;
        Ok(Self { inner: Arc::new(RwLock::new(inner)) })
    }
}

impl PartialEq for Committee {
    fn eq(&self, other: &Self) -> bool {
        self.inner.read().eq(&*other.inner.read())
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
    ) -> Self {
        let mut committee = CommitteeInner {
            authorities,
            epoch,
            authorities_by_id: Default::default(),
            validity_threshold: 0,
            quorum_threshold: 0,
            bootstrap_servers,
        };
        committee.load();

        // Some sanity checks to ensure that we'll not end up in invalid state
        assert_eq!(committee.authorities_by_id.len(), committee.authorities.len());

        assert_eq!(committee.validity_threshold, committee.calculate_validity_threshold().get());
        assert_eq!(committee.quorum_threshold, committee.calculate_quorum_threshold().get());

        Self { inner: Arc::new(RwLock::new(committee)) }
    }

    /// Expose new for tests.  If you are calling this outside of a test you are wrong, see comment
    /// on new.
    ///
    /// Pass an optional epoch_boundary timestamp. Defaults to u64::MAX to disable epoch
    /// transitions.
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

        Self { inner: Arc::new(RwLock::new(committee)) }
    }

    /// Updates the committee internal secondary indexes.
    pub fn load(&self) {
        self.inner.write().load()
    }

    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.inner.read().epoch
    }

    /// Provided an identifier it returns the corresponding authority
    pub fn authority(&self, identifier: &AuthorityIdentifier) -> Option<Authority> {
        self.inner.read().authorities_by_id.get(identifier).cloned()
    }

    pub fn authority_by_key(&self, key: &BlsPublicKey) -> Option<Authority> {
        self.inner.read().authorities.get(key).cloned()
    }

    pub fn authorities(&self) -> Vec<Authority> {
        // Return sorted by id (using the id keyed BTree) since this may be important to some code.
        self.inner.read().authorities_by_id.values().cloned().collect()
    }

    /// Return true if the authority for id is in the committee.
    pub fn is_authority(&self, id: &AuthorityIdentifier) -> bool {
        // Return sorted by id (using the id keyed BTree) since this may be important to some code.
        self.inner.read().authorities_by_id.contains_key(id)
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.inner.read().authorities.len()
    }

    /// Return the voting power of a specific authority.
    pub fn voting_power(&self, name: &BlsPublicKey) -> VotingPower {
        self.inner.read().authorities.get(&name.clone()).map_or_else(|| 0, |_| EQUAL_VOTING_POWER)
    }

    pub fn voting_power_by_id(&self, id: &AuthorityIdentifier) -> VotingPower {
        self.inner.read().authorities_by_id.get(id).map_or_else(|| 0, |_| EQUAL_VOTING_POWER)
    }

    /// Returns the voting power required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> VotingPower {
        self.inner.read().quorum_threshold
    }

    /// Returns the voting power required to reach availability (f+1).
    pub fn validity_threshold(&self) -> VotingPower {
        self.inner.read().validity_threshold
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
        self.inner.read().total_voting_power()
    }

    /// Return all the network addresses in the committee.
    pub fn others_primaries_by_id(
        &self,
        myself: Option<&AuthorityIdentifier>,
    ) -> Vec<(AuthorityIdentifier, BlsPublicKey)> {
        self.inner
            .read()
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
            .read()
            .authorities
            .iter()
            .filter_map(|(_, authority)| {
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
        self.inner.read().authorities.values().map(|authority| *authority.protocol_key()).collect()
    }

    /// Return the bootstrap record for key if it exists.
    pub fn get_bootstrap(&self, key: &BlsPublicKey) -> Option<BootstrapServer> {
        self.inner.read().bootstrap_servers.get(key).cloned()
    }

    /// Return the map of bootstrap servers.
    pub fn bootstrap_servers(&self) -> BTreeMap<BlsPublicKey, BootstrapServer> {
        self.inner.read().bootstrap_servers.clone()
    }

    /// Used for testing - not recommended to use for any other case.
    /// It creates a new instance with updated epoch
    pub fn advance_epoch_for_test(&self, new_epoch: Epoch) -> Committee {
        Committee::new_for_test(
            self.inner.read().authorities.clone(),
            new_epoch,
            self.inner.read().bootstrap_servers.clone(),
        )
    }

    /// Return the number of workers that are in use for this committee.
    /// This is a protocol level value, all nodes have to agree on this and be
    /// running the required number of workers.
    /// Currently 1 but may change with a future fork on an epoch boundary.
    pub fn number_of_workers(&self) -> usize {
        1
    }
}

impl std::fmt::Display for Committee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Committee E{}: {:?}",
            self.epoch(),
            self.inner
                .read()
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
}

impl CommitteeBuilder {
    /// Create a new instance of [CommitteeBuilder] for making a new [Committee].
    pub fn new(epoch: Epoch) -> Self {
        Self { epoch, authorities: BTreeMap::default(), bootstrap_server: BTreeMap::default() }
    }

    /// Add an authority and bootstrap server to the committee builder.
    pub fn add_authority_and_bootstrap(
        &mut self,
        protocol_key: BlsPublicKey,
        stake: VotingPower,
        primary_node: P2pNode,
        worker_node: P2pNode,
        execution_address: Address,
    ) {
        let authority = Authority::new(protocol_key, stake, execution_address);
        self.authorities.insert(protocol_key, authority);
        let bootstrap = BootstrapServer::new(primary_node, worker_node);
        self.bootstrap_server.insert(protocol_key, bootstrap);
    }

    /// Add an authority to the committee builder.
    pub fn add_authority(
        &mut self,
        protocol_key: BlsPublicKey,
        stake: VotingPower,
        execution_address: Address,
    ) {
        let authority = Authority::new(protocol_key, stake, execution_address);
        self.authorities.insert(protocol_key, authority);
    }

    /// Add an authority to the committee builder.
    pub fn add_bootstrap_server(
        &mut self,
        protocol_key: BlsPublicKey,
        primary_node: P2pNode,
        worker_node: P2pNode,
    ) {
        let bootstrap = BootstrapServer::new(primary_node, worker_node);
        self.bootstrap_server.insert(protocol_key, bootstrap);
    }

    pub fn build(self) -> Committee {
        Committee::new(self.authorities, self.epoch, self.bootstrap_server)
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

                let a = Authority::new(*keypair.public(), 1, execution_address);

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
                    (Multiaddr::empty(), worker_keypair.public().clone().into()).into(),
                );

                (*key, b)
            })
            .collect::<BTreeMap<BlsPublicKey, BootstrapServer>>();

        // WHEN
        let committee = Committee::new(authorities, 10, bootstrap_servers);

        // THEN
        assert_eq!(committee.inner.read().authorities_by_id.len() as u64, num_of_authorities);
        assert_eq!(committee.inner.read().authorities.len() as u64, num_of_authorities);

        for (identifier, authority) in committee.inner.read().authorities_by_id.iter() {
            assert_eq!(*identifier, authority.id());
        }

        // AND ensure thresholds are calculated correctly
        assert_eq!(committee.quorum_threshold(), 7);
        assert_eq!(committee.validity_threshold(), 4);

        let guard = committee.inner.read();
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

    #[test]
    fn committee_yaml_deserialize_with_legacy_authority_voting_power() {
        let mut rng = rng();
        let num_of_authorities = 4;

        let authorities = (0..num_of_authorities)
            .enumerate()
            .map(|(i, _)| {
                let keypair = BlsKeypair::generate(&mut rng);
                let execution_address = Address::repeat_byte(i as u8);
                let authority = Authority::new(*keypair.public(), 1, execution_address);
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
                    (Multiaddr::empty(), worker_keypair.public().clone().into()).into(),
                );
                (*key, bootstrap)
            })
            .collect::<BTreeMap<BlsPublicKey, BootstrapServer>>();

        let committee = Committee::new(authorities, 0, bootstrap_servers);
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
        reloaded.load();

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
        let expected = format!("\"{}\"", id.to_string());
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
}
