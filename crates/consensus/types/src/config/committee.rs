// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{CommitteeUpdateError, ConfigError, Epoch, Stake};
use crate::{
    crypto::{BlsPublicKey, BlsPublicKeyBytes, NetworkPublicKey},
    Multiaddr,
};
use fastcrypto::{
    serde_helpers::ToFromByteArray,
    traits::{EncodeDecodeBase64, ToFromBytes},
};
use mem_utils::MallocSizeOf;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use reth_primitives::Address;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    fmt::{Display, Formatter},
    num::NonZeroU64,
};

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Authority {
    /// The id under which we identify this authority across Narwhal
    #[serde(skip)]
    id: AuthorityIdentifier,
    /// The authority's main BlsPublicKey which is used to verify the content they sign.
    protocol_key: BlsPublicKey,
    /// The authority's main BlsPublicKey expressed as pure bytes
    protocol_key_bytes: BlsPublicKeyBytes,
    /// The voting power of this authority.
    stake: Stake,
    /// The network address of the primary.
    primary_network_address: Multiaddr,
    /// The execution address for the authority.
    /// This address will be used as the suggested fee recipient.
    execution_address: Address,
    /// Network key of the primary.
    network_key: NetworkPublicKey,
    /// The validator's hostname
    hostname: String,
    /// There are secondary indexes that should be initialised before we are ready to use the
    /// authority - this bool protect us for premature use.
    #[serde(skip)]
    initialised: bool,
}

impl Authority {
    /// The constructor is not public by design. Everyone who wants to create authorities should do
    /// it via Committee (more specifically can use [CommitteeBuilder]). As some internal properties
    /// of Authority are initialised via the Committee, to ensure that the user will not
    /// accidentally use stale Authority data, should always derive them via the Commitee.
    fn new(
        protocol_key: BlsPublicKey,
        stake: Stake,
        primary_network_address: Multiaddr,
        execution_address: Address,
        network_key: NetworkPublicKey,
        hostname: String,
    ) -> Self {
        let protocol_key_bytes = BlsPublicKeyBytes::from(&protocol_key);

        Self {
            id: Default::default(),
            protocol_key,
            protocol_key_bytes,
            stake,
            primary_network_address,
            execution_address,
            network_key,
            hostname,
            initialised: false,
        }
    }

    fn initialise(&mut self, id: AuthorityIdentifier) {
        self.id = id;
        self.initialised = true;
    }

    pub fn id(&self) -> AuthorityIdentifier {
        assert!(self.initialised);
        self.id
    }

    pub fn protocol_key(&self) -> &BlsPublicKey {
        assert!(self.initialised);
        &self.protocol_key
    }

    pub fn protocol_key_bytes(&self) -> &BlsPublicKeyBytes {
        assert!(self.initialised);
        &self.protocol_key_bytes
    }

    pub fn stake(&self) -> Stake {
        assert!(self.initialised);
        self.stake
    }

    pub fn primary_network_address(&self) -> Multiaddr {
        assert!(self.initialised);
        self.primary_network_address.clone()
    }

    pub fn execution_address(&self) -> Address {
        assert!(self.initialised);
        self.execution_address
    }

    pub fn network_key(&self) -> NetworkPublicKey {
        assert!(self.initialised);
        self.network_key.clone()
    }

    pub fn hostname(&self) -> &str {
        assert!(self.initialised);
        self.hostname.as_str()
    }
}

/// The committee lists all validators that participate in consensus.
///
/// TODO: remove default? Added for compatibility with [ConfigTrait].
/// Probably a better way to do this.
#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Default)]
pub struct Committee {
    /// The authorities of epoch.
    authorities: BTreeMap<BlsPublicKey, Authority>,
    /// Keeps and index of the Authorities by their respective identifier
    #[serde(skip)]
    authorities_by_id: BTreeMap<AuthorityIdentifier, Authority>,
    /// The epoch number of this committee
    epoch: Epoch,
    /// The quorum threshold (2f+1)
    #[serde(skip)]
    quorum_threshold: Stake,
    /// The validity threshold (f+1)
    #[serde(skip)]
    validity_threshold: Stake,
}

// Every authority gets uniquely identified by the AuthorityIdentifier
// The type can be easily swapped without needing to change anything else in the implementation.
#[derive(
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Clone,
    Copy,
    Debug,
    Default,
    Hash,
    Serialize,
    Deserialize,
    MallocSizeOf,
)]
pub struct AuthorityIdentifier(pub u16);

impl Display for AuthorityIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.to_string().as_str())
    }
}

impl Committee {
    /// Any committee should be created via the [CommitteeBuilder] - this is intentionally
    /// a private method.
    fn new(authorities: BTreeMap<BlsPublicKey, Authority>, epoch: Epoch) -> Self {
        let mut committee = Self {
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

        // ensure all the authorities are ordered in incremented manner with their ids - just some
        // extra confirmation here.
        for (index, (_, authority)) in committee.authorities.iter().enumerate() {
            assert_eq!(index as u16, authority.id.0);
        }

        committee
    }

    fn calculate_quorum_threshold(&self) -> NonZeroU64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.total_stake();
        NonZeroU64::new(2 * total_votes / 3 + 1).unwrap()
    }

    fn calculate_validity_threshold(&self) -> NonZeroU64 {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: Stake = self.total_stake();
        NonZeroU64::new((total_votes + 2) / 3).unwrap()
    }

    /// Updates the committee internal secondary indexes.
    pub fn load(&mut self) {
        self.authorities_by_id = (0_u16..)
            .zip(self.authorities.iter_mut())
            .map(|(identifier, (_key, authority))| {
                let id = AuthorityIdentifier(identifier);
                authority.initialise(id);

                (id, authority.clone())
            })
            .collect();

        self.validity_threshold = self.calculate_validity_threshold().get();
        self.quorum_threshold = self.calculate_quorum_threshold().get();
        assert!(self.authorities_by_id.len() > 1, "committee size must be larger that 1");
    }

    /// Returns the current epoch.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Provided an identifier it returns the corresponding authority
    pub fn authority(&self, identifier: &AuthorityIdentifier) -> Option<&Authority> {
        self.authorities_by_id.get(identifier)
    }

    /// Provided an identifier it returns the corresponding authority - if is not found then it
    /// panics
    pub fn authority_safe(&self, identifier: &AuthorityIdentifier) -> &Authority {
        self.authorities_by_id.get(identifier).unwrap_or_else(|| {
            panic!("Authority with id {:?} should have been in committee", identifier)
        })
    }

    pub fn authority_by_key(&self, key: &BlsPublicKey) -> Option<&Authority> {
        self.authorities.get(key)
    }

    /// Returns the keys in the committee
    pub fn keys(&self) -> Vec<BlsPublicKey> {
        self.authorities.keys().cloned().collect::<Vec<BlsPublicKey>>()
    }

    /// Returns info from the committee needed for randomness DKG.
    pub fn randomness_dkg_info(
        &self,
    ) -> Vec<(
        AuthorityIdentifier,
        fastcrypto_tbls::ecies::PublicKey<fastcrypto::groups::bls12381::G2Element>,
        Stake,
    )> {
        self.authorities_by_id
            .iter()
            .map(|(id, authority)| {
                let pk = fastcrypto::groups::bls12381::G2Element::from_byte_array(
                    authority
                        .protocol_key()
                        .as_bytes()
                        .try_into()
                        .expect("key length should match"),
                )
                .expect("should work to convert BLS key to G2Element");
                (*id, fastcrypto_tbls::ecies::PublicKey::from(pk), authority.stake())
            })
            .collect()
    }

    pub fn authorities(&self) -> impl Iterator<Item = &Authority> {
        self.authorities.values()
    }

    pub fn authority_by_network_key(&self, network_key: &NetworkPublicKey) -> Option<&Authority> {
        self.authorities
            .iter()
            .find(|(_, authority)| authority.network_key == *network_key)
            .map(|(_, authority)| authority)
    }

    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &BlsPublicKey) -> Stake {
        self.authorities.get(&name.clone()).map_or_else(|| 0, |x| x.stake)
    }

    pub fn stake_by_id(&self, id: AuthorityIdentifier) -> Stake {
        self.authorities_by_id.get(&id).map_or_else(|| 0, |authority| authority.stake)
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        self.quorum_threshold
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        self.validity_threshold
    }

    /// Returns true if the provided stake has reached quorum (2f+1)
    pub fn reached_quorum(&self, stake: Stake) -> bool {
        stake >= self.quorum_threshold()
    }

    /// Returns true if the provided stake has reached availability (f+1)
    pub fn reached_validity(&self, stake: Stake) -> bool {
        stake >= self.validity_threshold()
    }

    pub fn total_stake(&self) -> Stake {
        self.authorities.values().map(|x| x.stake).sum()
    }

    /// Returns a leader node as a weighted choice seeded by the provided integer
    pub fn leader(&self, seed: u64) -> Authority {
        let mut seed_bytes = [0u8; 32];
        seed_bytes[32 - 8..].copy_from_slice(&seed.to_le_bytes());
        let mut rng = StdRng::from_seed(seed_bytes);
        let choices = self
            .authorities
            .values()
            .map(|authority| (authority.clone(), authority.stake as f32))
            .collect::<Vec<_>>();
        choices
            .choose_weighted(&mut rng, |item| item.1)
            .expect("Weighted choice error: stake values incorrect!")
            .0
            .clone()
    }

    /// Returns the primary address of the target primary.
    pub fn primary(&self, to: &BlsPublicKey) -> Result<Multiaddr, ConfigError> {
        self.authorities
            .get(&to.clone())
            .map(|x| x.primary_network_address.clone())
            .ok_or_else(|| ConfigError::NotInCommittee((*to).encode_base64()))
    }

    /// Returns the primary address of the target primary.
    pub fn primary_by_id(&self, to: &AuthorityIdentifier) -> Result<Multiaddr, ConfigError> {
        self.authorities_by_id
            .get(to)
            .map(|x| x.primary_network_address.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(to.0.to_string()))
    }

    pub fn network_key(&self, pk: &BlsPublicKey) -> Result<NetworkPublicKey, ConfigError> {
        self.authorities
            .get(&pk.clone())
            .map(|x| x.network_key.clone())
            .ok_or_else(|| ConfigError::NotInCommittee((*pk).encode_base64()))
    }

    /// Return all the network addresses in the committee.
    pub fn others_primaries(
        &self,
        myself: &BlsPublicKey,
    ) -> Vec<(BlsPublicKey, Multiaddr, NetworkPublicKey)> {
        self.authorities
            .iter()
            .filter(|(name, _)| *name != myself)
            .map(|(name, authority)| {
                (
                    name.clone(),
                    authority.primary_network_address.clone(),
                    authority.network_key.clone(),
                )
            })
            .collect()
    }

    /// Return all the network addresses in the committee.
    pub fn others_primaries_by_id(
        &self,
        myself: AuthorityIdentifier,
    ) -> Vec<(AuthorityIdentifier, Multiaddr, NetworkPublicKey)> {
        self.authorities
            .iter()
            .filter(|(_, authority)| authority.id() != myself)
            .map(|(_, authority)| {
                (authority.id(), authority.primary_network_address(), authority.network_key())
            })
            .collect()
    }

    fn get_all_network_addresses(&self) -> HashSet<&Multiaddr> {
        self.authorities.values().map(|authority| &authority.primary_network_address).collect()
    }

    /// Return the network addresses that are present in the current committee but that are absent
    /// from the new committee (provided as argument).
    pub fn network_diff<'a>(&'a self, other: &'a Self) -> HashSet<&'a Multiaddr> {
        self.get_all_network_addresses()
            .difference(&other.get_all_network_addresses())
            .cloned()
            .collect()
    }

    /// Update the networking information of some of the primaries. The arguments are a full vector
    /// of authorities which Public key and Stake must match the one stored in the current
    /// Committee. Any discrepancy will generate no update and return a vector of errors.
    #[allow(clippy::manual_try_fold)]
    pub fn update_primary_network_info(
        &mut self,
        mut new_info: BTreeMap<BlsPublicKey, (Stake, Multiaddr)>,
    ) -> Result<(), Vec<CommitteeUpdateError>> {
        let mut errors = None;

        let table = &self.authorities;
        let push_error_and_return = |acc, error| {
            let mut error_table = if let Err(errors) = acc { errors } else { Vec::new() };
            error_table.push(error);
            Err(error_table)
        };

        let res = table.iter().fold(Ok(BTreeMap::new()), |acc, (pk, authority)| {
            if let Some((stake, address)) = new_info.remove(pk) {
                if stake == authority.stake {
                    match acc {
                        // No error met yet, update the accumulator
                        Ok(mut bmap) => {
                            let mut res = authority.clone();
                            res.primary_network_address = address;
                            bmap.insert(pk.clone(), res);
                            Ok(bmap)
                        }
                        // in error mode, continue
                        _ => acc,
                    }
                } else {
                    // Stake does not match: create or append error
                    push_error_and_return(acc, CommitteeUpdateError::DifferentStake(pk.to_string()))
                }
            } else {
                // This key is absent from new information
                push_error_and_return(acc, CommitteeUpdateError::MissingFromUpdate(pk.to_string()))
            }
        });

        // If there are elements left in new_info, they are not in the original table
        // If new_info is empty, this is a no-op.
        let res = new_info.iter().fold(res, |acc, (pk, _)| {
            push_error_and_return(acc, CommitteeUpdateError::NotInCommittee(pk.to_string()))
        });

        match res {
            Ok(new_table) => self.authorities = new_table,
            Err(errs) => {
                errors = Some(errs);
            }
        };

        errors.map(Err).unwrap_or(Ok(()))
    }

    /// Used for testing - not recommended to use for any other case.
    /// It creates a new instance with updated epoch
    pub fn advance_epoch(&self, new_epoch: Epoch) -> Committee {
        Committee::new(self.authorities.clone(), new_epoch)
    }
}

impl std::fmt::Display for Committee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Committee E{}: {:?}",
            self.epoch(),
            self.authorities
                .keys()
                .map(|x| {
                    if let Some(k) = x.encode_base64().get(0..16) {
                        k.to_owned()
                    } else {
                        format!("Invalid key: {}", x)
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
        stake: Stake,
        primary_network_address: Multiaddr,
        execution_address: Address,
        network_key: NetworkPublicKey,
        hostname: String,
    ) {
        let authority = Authority::new(
            protocol_key.clone(),
            stake,
            primary_network_address,
            execution_address,
            network_key,
            hostname,
        );
        self.authorities.insert(protocol_key, authority);
        // self
    }

    pub fn build(self) -> Committee {
        Committee::new(self.authorities, self.epoch)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Authority, BlsKeypair, BlsPublicKey, Committee, Multiaddr, NetworkKeypair};
    use fastcrypto::traits::KeyPair as _;
    use rand::thread_rng;
    use reth_primitives::Address;
    use std::collections::BTreeMap;

    #[test]
    fn committee_load() {
        // GIVEN
        let mut rng = thread_rng();
        let num_of_authorities = 10;

        let authorities = (0..num_of_authorities)
            .map(|i| {
                let keypair = BlsKeypair::generate(&mut rng);
                let network_keypair = NetworkKeypair::generate(&mut rng);
                let execution_address = Address::random();

                let a = Authority::new(
                    keypair.public().clone(),
                    1,
                    Multiaddr::empty(),
                    execution_address,
                    network_keypair.public().clone(),
                    i.to_string(),
                );

                (keypair.public().clone(), a)
            })
            .collect::<BTreeMap<BlsPublicKey, Authority>>();

        // WHEN
        let committee = Committee::new(authorities, 10);

        // THEN
        assert_eq!(committee.authorities_by_id.len() as u64, num_of_authorities);

        for (identifier, authority) in committee.authorities_by_id.iter() {
            assert_eq!(*identifier, authority.id());
        }

        // AND ensure thresholds are calculated correctly
        assert_eq!(committee.quorum_threshold(), 7);
        assert_eq!(committee.validity_threshold(), 4);

        // AND ensure authorities are returned in the same order
        for ((id, authority_1), (public_key, authority_2)) in
            committee.authorities_by_id.iter().zip(committee.authorities)
        {
            assert_eq!(authority_1.clone(), authority_2);
            assert_eq!(*id, authority_2.id());
            assert_eq!(&public_key, authority_1.protocol_key());
        }
    }
}
