//! Committee of validators reach consensus.

use crate::{
    crypto::{BlsPublicKey, NetworkPublicKey},
    Address, Multiaddr,
};
use libp2p::{multihash::Multihash, PeerId};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IndexedRandom as _, SeedableRng};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
    num::NonZeroU64,
    sync::Arc,
};

/// The epoch number.
/// Becomes the upper 32 bits of a nonce (with rounds the low bits).
pub type Epoch = u32;

/// The voting power an authority has within the committee.
pub type VotingPower = u64;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
struct AuthorityInner {
    /// The authority's main BlsPublicKey which is used to verify the content they sign.
    protocol_key: BlsPublicKey,
    /// The voting power of this authority.
    voting_power: VotingPower,
    /// The network address of the primary.
    primary_network_address: Multiaddr,
    /// The execution address for the authority.
    /// This address will be used as the suggested fee recipient.
    execution_address: Address,
    /// Network key of the primary.
    network_key: NetworkPublicKey,
    /// The validator's hostname
    hostname: String,
}

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
        voting_power: VotingPower,
        primary_network_address: Multiaddr,
        execution_address: Address,
        network_key: NetworkPublicKey,
        hostname: String,
    ) -> Self {
        Self {
            inner: Arc::new(AuthorityInner {
                protocol_key,
                voting_power,
                primary_network_address,
                execution_address,
                network_key,
                hostname,
            }),
        }
    }

    /// Version of new that can be called directly.  Useful for testing, if you are calling this
    /// outside of a test you are wrong (see comment on new).
    pub fn new_for_test(
        protocol_key: BlsPublicKey,
        voting_power: VotingPower,
        primary_network_address: Multiaddr,
        execution_address: Address,
        network_key: NetworkPublicKey,
        hostname: String,
    ) -> Self {
        Self {
            inner: Arc::new(AuthorityInner {
                protocol_key,
                voting_power,
                primary_network_address,
                execution_address,
                network_key,
                hostname,
            }),
        }
    }

    pub fn id(&self) -> AuthorityIdentifier {
        self.inner.network_key.to_peer_id().into()
    }

    /// Return the peer id for the primary network.
    pub fn peer_id(&self) -> PeerId {
        self.inner.network_key.to_peer_id()
    }

    pub fn protocol_key(&self) -> &BlsPublicKey {
        // Skip the assert here, this is called in testing before the initialise...
        &self.inner.protocol_key
    }

    pub fn voting_power(&self) -> VotingPower {
        self.inner.voting_power
    }

    pub fn primary_network_address(&self) -> &Multiaddr {
        &self.inner.primary_network_address
    }

    pub fn execution_address(&self) -> Address {
        self.inner.execution_address
    }

    pub fn network_key(&self) -> NetworkPublicKey {
        self.inner.network_key.clone()
    }

    pub fn hostname(&self) -> &str {
        self.inner.hostname.as_str()
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

    pub fn total_voting_power(&self) -> VotingPower {
        self.authorities.values().map(|x| x.inner.voting_power).sum()
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
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Debug, Hash, Serialize, Deserialize)]
pub struct AuthorityIdentifier(Arc<PeerId>);

impl AuthorityIdentifier {
    pub fn dummy_for_test(byte: u8) -> Self {
        let data = [byte; 32];
        PeerId::from_multihash(
            Multihash::wrap(0x0, &data).expect("The digest size is never too large"),
        )
        .expect("valid multihash bytes")
        .into()
    }

    pub fn peer_id(&self) -> PeerId {
        self.into()
    }
}

impl Default for AuthorityIdentifier {
    fn default() -> Self {
        let data = [0_u8; 32];
        PeerId::from_multihash(
            Multihash::wrap(0x0, &data).expect("The digest size is never too large"),
        )
        .expect("valid multihash bytes")
        .into()
    }
}

impl Display for AuthorityIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.to_string().as_str())
    }
}

impl From<PeerId> for AuthorityIdentifier {
    fn from(value: PeerId) -> Self {
        Self(Arc::new(value))
    }
}

impl From<AuthorityIdentifier> for PeerId {
    fn from(value: AuthorityIdentifier) -> Self {
        *value.0
    }
}

impl From<&AuthorityIdentifier> for PeerId {
    fn from(value: &AuthorityIdentifier) -> Self {
        *value.0
    }
}

impl Committee {
    /// Any committee should be created via the [CommitteeBuilder] - this is intentionally
    /// a private method.
    fn new(authorities: BTreeMap<BlsPublicKey, Authority>, epoch: Epoch) -> Self {
        let mut committee = CommitteeInner {
            authorities,
            epoch,
            authorities_by_id: Default::default(),
            validity_threshold: 0,
            quorum_threshold: 0,
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
    pub fn new_for_test(authorities: BTreeMap<BlsPublicKey, Authority>, epoch: Epoch) -> Self {
        let mut committee = CommitteeInner {
            authorities,
            epoch,
            authorities_by_id: Default::default(),
            validity_threshold: 0,
            quorum_threshold: 0,
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

    /// Return the stake of a specific authority.
    pub fn voting_power(&self, name: &BlsPublicKey) -> VotingPower {
        self.inner.read().authorities.get(&name.clone()).map_or_else(|| 0, |x| x.inner.voting_power)
    }

    pub fn voting_power_by_id(&self, id: &AuthorityIdentifier) -> VotingPower {
        self.inner
            .read()
            .authorities_by_id
            .get(id)
            .map_or_else(|| 0, |authority| authority.inner.voting_power)
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> VotingPower {
        self.inner.read().quorum_threshold
    }

    /// Returns the stake required to reach availability (f+1).
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

    /// Returns a leader node as a weighted choice seeded by the provided integer
    pub fn leader(&self, seed: u64) -> Authority {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[32 - 8..].copy_from_slice(&seed.to_le_bytes());
        let mut rng = StdRng::from_seed(seed_bytes);
        let choices = self
            .inner
            .read()
            .authorities
            .values()
            .map(|authority| (authority.clone(), authority.inner.voting_power as f32))
            .collect::<Vec<_>>();
        choices
            .choose_weighted(&mut rng, |item| item.1)
            .expect("Weighted choice error: stake values incorrect!")
            .0
            .clone()
    }

    /// Return all the network addresses in the committee.
    pub fn others_primaries_by_id(
        &self,
        myself: Option<&AuthorityIdentifier>,
    ) -> Vec<(AuthorityIdentifier, Multiaddr, NetworkPublicKey)> {
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
            .map(|(_, authority)| {
                (
                    authority.id(),
                    authority.primary_network_address().clone(),
                    authority.network_key(),
                )
            })
            .collect()
    }

    /// Used for testing - not recommended to use for any other case.
    /// It creates a new instance with updated epoch
    pub fn advance_epoch_for_test(&self, new_epoch: Epoch) -> Committee {
        Committee::new_for_test(self.inner.read().authorities.clone(), new_epoch)
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
pub struct CommitteeBuilder {
    /// The epoch for the committee.
    epoch: Epoch,
    /// The map of [BlsPublicKey] for each [Authority] in the committee.
    authorities: BTreeMap<BlsPublicKey, Authority>,
}

impl CommitteeBuilder {
    /// Create a new instance of [CommitteeBuilder] for making a new [Committee].
    pub fn new(epoch: Epoch) -> Self {
        Self { epoch, authorities: BTreeMap::new() }
    }

    /// Add an authority to the committee builder.
    pub fn add_authority(
        &mut self,
        protocol_key: BlsPublicKey,
        stake: VotingPower,
        primary_network_address: Multiaddr,
        execution_address: Address,
        network_key: NetworkPublicKey,
        hostname: String,
    ) {
        let authority = Authority::new(
            protocol_key,
            stake,
            primary_network_address,
            execution_address,
            network_key,
            hostname,
        );
        self.authorities.insert(protocol_key, authority);
    }

    pub fn build(self) -> Committee {
        Committee::new(self.authorities, self.epoch)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Address, Authority, BlsKeypair, BlsPublicKey, Committee, Multiaddr, NetworkKeypair,
    };
    use rand::rng;
    use std::collections::BTreeMap;

    #[test]
    fn committee_load() {
        // GIVEN
        let mut rng = rng();
        let num_of_authorities = 10;

        let authorities = (0..num_of_authorities)
            .map(|i| {
                let keypair = BlsKeypair::generate(&mut rng);
                let network_keypair = NetworkKeypair::generate_ed25519();
                let execution_address = Address::random();

                let a = Authority::new(
                    *keypair.public(),
                    1,
                    Multiaddr::empty(),
                    execution_address,
                    network_keypair.public().clone().into(),
                    i.to_string(),
                );

                (*keypair.public(), a)
            })
            .collect::<BTreeMap<BlsPublicKey, Authority>>();

        // WHEN
        let committee = Committee::new(authorities, 10);

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
        for (public_key, authority_1) in guard.authorities.iter() {
            assert_eq!(public_key, authority_1.protocol_key());
            let authority_2 = guard.authorities_by_id.get(&authority_1.id()).unwrap();
            assert_eq!(authority_1, authority_2);
        }
    }
}
