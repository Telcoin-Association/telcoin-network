//! Types for managing peers.

use crate::types::NetworkResult;
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map::IntoIter, HashMap, HashSet},
    net::IpAddr,
};
use tn_types::{BlsPublicKey, BlsPublicKeyBytes, NetworkPublicKey};
use tokio::sync::oneshot;

pub use super::penalty::Penalty;

/// Domain identity for a tracked peer.
///
/// Telcoin associates a [BlsPublicKey] with a peer once its network settings are known
/// (committee, trusted, or otherwise known peers). Until then a peer discovered through
/// libp2p - an inbound connection or a kad-dialed candidate - is known only by its libp2p
/// [PeerId]. This sum type captures that distinction so the domain key (the bls public key)
/// is used wherever it is known, and the libp2p id only where it isn't.
///
/// `Confirmed` and `Unidentified` are distinct keys: a peer first seen as `Unidentified`
/// is re-keyed onto its `Confirmed` identity once its bls key is learned.
///
/// The variants differ in size (a bls key is larger than a libp2p id), but this type is a
/// `Copy` map key resolved on every libp2p connection event; boxing would forfeit `Copy` and
/// add indirection to those lookups, so the inline layout is intentional.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum PeerIdentity {
    /// A peer whose [BlsPublicKey] is known. This is the telcoin-domain identity.
    Confirmed(BlsPublicKey),
    /// A peer known only by its libp2p [PeerId] (no bls key learned yet).
    Unidentified(PeerId),
}

/// Events for the `PeerManager`.
#[derive(Debug)]
pub(crate) enum PeerEvent {
    /// Connected with peer.
    PeerConnected(PeerId, Multiaddr),
    /// Peer was disconnected.
    PeerDisconnected(PeerId),
    /// Disconnect from the peer without exchanging peer information.
    /// This is the event for disconnecting from penalized peers.
    DisconnectPeer(PeerId),
    /// Disconnect from the peer and share peer information for discovery.
    /// This is the event for disconnecting from excess peers with otherwise trusted reputations.
    DisconnectPeerX(PeerId, PeerExchangeMap),
    /// Peer manager has identified a peer and associated ip addresses to ban.
    Banned(PeerId),
    /// Peer manager has unbanned a peer and associated ip addresses.
    Unbanned(PeerId),
    /// Authorities are missing from the peer map. This triggers kad queries.
    MissingAuthorities(Vec<BlsPublicKey>),
    /// Initiate a discovery attempt because discovery peer counts are low.
    Discovery,
}

/// The action to take after a peer's reputation or connection status changes.
///
/// Both reputation and connection status changes may require the manager to take
/// action to update the peer.
#[derive(Debug)]
pub(super) enum PeerAction {
    /// Ban the peer and the associated IP addresses.
    Ban(Vec<IpAddr>),
    /// No action needed.
    NoAction,
    /// Disconnect from peer.
    Disconnect,
    /// Disconnect a peer with peer exchange information to support discovery.
    /// This results in a temporary ban to prevent immediate reconnection attempts.
    DisconnectWithPX,
    /// Unban the peer and it's known ip addresses.
    Unban(Vec<IpAddr>),
}

impl PeerAction {
    /// Helper method if the action is to ban the peer.
    pub(super) fn is_ban(&self) -> bool {
        matches!(self, PeerAction::Ban(_))
    }
}

/// Whether a reported penalty changed the peer's score.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PenaltyOutcome {
    /// The peer policy applies the penalty to the score.
    Applied,
    /// The peer policy suppresses the penalty.
    Exempt,
}

/// Request for dialing peers.
#[derive(Debug)]
pub(crate) struct DialRequest {
    /// The peer's network id.
    pub(crate) peer_id: PeerId,
    /// The multiaddr to dial.
    pub(crate) multiaddrs: Vec<Multiaddr>,
    /// The channel to forward results and errors.
    /// Optional in case dial is the result of a peer-exchange.
    pub(crate) reply: Option<oneshot::Sender<NetworkResult<()>>>,
}

/// Types of connections between peers.
#[derive(Debug)]
pub(super) enum ConnectionType {
    /// A peer has successfully dialed this node.
    IncomingConnection {
        /// The peer's multiaddr.
        multiaddr: Multiaddr,
    },
    /// This node has successfully dialed a peer.
    OutgoingConnection {
        /// The peer's multiaddr.
        multiaddr: Multiaddr,
    },
}

/// Direction of connection between peers from the local node's perspective.
#[derive(Debug, Clone, Serialize)]
pub(super) enum ConnectionDirection {
    /// The connection was established by a peer dialing this node.
    Incoming,
    /// The connection was established by this node dialing a peer.
    Outgoing,
}

/// The most entries one [`PeerExchangeMap`] carries on the wire.
///
/// A goodbye lists the sender's connected peers, and the connection budget keeps that set at or
/// under this count. The decoder rejects a larger map from its length prefix, before it decodes
/// one entry, and the producer truncates to the same count, so an honest map always decodes
/// while a padded map costs the receiver nothing past the prefix.
pub const MAX_PEER_EXCHANGE_ENTRIES: usize = 128;

/// The most addresses one exchange entry carries on the wire.
///
/// Mirrors the per-peer store cap the producer already applies, so the decoder never holds more
/// addresses for one entry than the store would keep.
pub const MAX_PEER_EXCHANGE_ADDRS_PER_PEER: usize = super::peer::MAX_MULTIADDRS_PER_PEER;

/// Allowance for one encoded [`Multiaddr`] in the exchange size ceiling, in bytes.
///
/// An IP address with a transport and a `/p2p` suffix takes about a quarter of this; only a DNS
/// name near its 253-byte limit comes close.
const PEER_EXCHANGE_ADDR_ALLOWANCE: usize = 256;

/// Wire size of one exchange entry with every cap filled: the length-prefixed compressed BLS key,
/// the length-prefixed protobuf network key, and the address set at its cap with each address at
/// its allowance behind a two-byte length prefix.
const MAX_PEER_EXCHANGE_ENTRY_SIZE: usize = (1 + BlsPublicKey::COMPRESSED_BYTES)
    + 40
    + MAX_PEER_EXCHANGE_ADDRS_PER_PEER * (2 + PEER_EXCHANGE_ADDR_ALLOWANCE);

/// Size ceiling for the dedicated peer-exchange codec, in bytes.
///
/// Derived from the entry and address caps rather than `max_rpc_message_size`, so one goodbye
/// stream commits at most what a capped map can occupy instead of the full RPC budget. The
/// leading bytes cover the map's own length prefix.
pub(crate) const MAX_PEER_EXCHANGE_MESSAGE_SIZE: usize =
    16 + MAX_PEER_EXCHANGE_ENTRIES * MAX_PEER_EXCHANGE_ENTRY_SIZE;

/// The compressed wire bytes of a BLS public key as a peer advertised them, never decompressed.
///
/// Peer exchange keys its entries by the advertised BLS key only to tell entries apart: the
/// receiver resolves and dials the entry's network key and addresses and never uses the BLS
/// key. Decoding the key as a [`BlsPublicKey`] would decompress a G2 point for every entry an
/// unauthenticated sender chose to include, so this type keeps the 96 wire bytes as they came
/// and leaves the decompression to the one caller that needs the point
/// ([`Self::to_bls_public_key`]). It serializes exactly as a [`BlsPublicKey`] does, so the wire
/// format and the canonical map order are unchanged.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerExchangeKey([u8; BlsPublicKey::COMPRESSED_BYTES]);

impl PeerExchangeKey {
    /// The advertised compressed encoding, unverified.
    pub fn as_bytes(&self) -> &[u8; BlsPublicKey::COMPRESSED_BYTES] {
        &self.0
    }

    /// Decompress the advertised bytes into a validated key.
    ///
    /// This is the one place the point is decompressed. It costs tens of microseconds and must
    /// not run once per entry of an untrusted map.
    pub fn to_bls_public_key(self) -> Option<BlsPublicKey> {
        BlsPublicKey::from_literal_bytes(&self.0).ok()
    }
}

impl std::fmt::Debug for PeerExchangeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.iter().take(8).try_fold((), |(), byte| write!(f, "{byte:02x}"))
    }
}

impl From<BlsPublicKey> for PeerExchangeKey {
    fn from(key: BlsPublicKey) -> Self {
        Self::from(&key)
    }
}

impl From<&BlsPublicKey> for PeerExchangeKey {
    fn from(key: &BlsPublicKey) -> Self {
        Self(<[u8; BlsPublicKey::COMPRESSED_BYTES]>::from(BlsPublicKeyBytes::from(key)))
    }
}

impl Serialize for PeerExchangeKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for PeerExchangeKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(PeerExchangeKeyVisitor)
    }
}

/// Accepts exactly the compressed key length and does no curve work.
struct PeerExchangeKeyVisitor;

impl<'de> serde::de::Visitor<'de> for PeerExchangeKeyVisitor {
    type Value = PeerExchangeKey;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} bytes of a compressed bls public key", BlsPublicKey::COMPRESSED_BYTES)
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        <[u8; BlsPublicKey::COMPRESSED_BYTES]>::try_from(v)
            .map(PeerExchangeKey)
            .map_err(|_| E::invalid_length(v.len(), &self))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let bytes = std::iter::from_fn(|| seq.next_element::<u8>().transpose())
            .take(BlsPublicKey::COMPRESSED_BYTES.saturating_add(1))
            .collect::<Result<Vec<_>, _>>()?;
        self.visit_bytes(&bytes)
    }
}

/// Wrapper for a map of advertised BLS keys to a network key and a collection of [Multiaddr].
///
/// This is a convenience wrapper because PeerId doesn't implement `Deserialize`.
/// Peers exchange information to facilitate discovery.
///
/// The map decodes at most [`MAX_PEER_EXCHANGE_ENTRIES`] entries of at most
/// [`MAX_PEER_EXCHANGE_ADDRS_PER_PEER`] addresses each, and keys its entries by
/// [`PeerExchangeKey`] so no entry costs a point decompression. Both bounds are checked from the
/// length prefixes before the entries are decoded, and again while they are decoded for formats
/// that give no hint.
#[derive(Clone, Debug, PartialEq, Serialize, Default)]
pub struct PeerExchangeMap(pub HashMap<PeerExchangeKey, (NetworkPublicKey, HashSet<Multiaddr>)>);

impl IntoIterator for PeerExchangeMap {
    type Item = (PeerExchangeKey, (NetworkPublicKey, HashSet<Multiaddr>));
    type IntoIter = IntoIter<PeerExchangeKey, (NetworkPublicKey, HashSet<Multiaddr>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl From<HashMap<PeerExchangeKey, (NetworkPublicKey, HashSet<Multiaddr>)>> for PeerExchangeMap {
    fn from(value: HashMap<PeerExchangeKey, (NetworkPublicKey, HashSet<Multiaddr>)>) -> Self {
        Self(value)
    }
}

impl From<HashMap<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>> for PeerExchangeMap {
    fn from(value: HashMap<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)>) -> Self {
        Self(value.into_iter().map(|(key, info)| (PeerExchangeKey::from(key), info)).collect())
    }
}

impl<'de> Deserialize<'de> for PeerExchangeMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_newtype_struct("PeerExchangeMap", PeerExchangeMapVisitor)
    }
}

/// Mirrors the derived newtype visitor, with the bounded decoder for the inner map.
struct PeerExchangeMapVisitor;

impl<'de> serde::de::Visitor<'de> for PeerExchangeMapVisitor {
    type Value = PeerExchangeMap;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "a peer exchange map of at most {MAX_PEER_EXCHANGE_ENTRIES} entries")
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        BoundedEntries::deserialize(deserializer).map(|entries| PeerExchangeMap(entries.0))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        seq.next_element::<BoundedEntries>()?
            .map(|entries| PeerExchangeMap(entries.0))
            .ok_or_else(|| serde::de::Error::invalid_length(0, &self))
    }
}

/// The inner map, decoded with the entry cap enforced from the length prefix and the count.
struct BoundedEntries(HashMap<PeerExchangeKey, (NetworkPublicKey, HashSet<Multiaddr>)>);

impl<'de> Deserialize<'de> for BoundedEntries {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(BoundedEntriesVisitor)
    }
}

struct BoundedEntriesVisitor;

impl<'de> serde::de::Visitor<'de> for BoundedEntriesVisitor {
    type Value = BoundedEntries;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "at most {MAX_PEER_EXCHANGE_ENTRIES} peer exchange entries")
    }

    fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        // reject from the prefix before any entry is decoded
        let declared = access.size_hint().unwrap_or(0);
        if declared > MAX_PEER_EXCHANGE_ENTRIES {
            return Err(serde::de::Error::invalid_length(declared, &self));
        }
        // take one past the cap so an unhinted stream over the cap is still rejected
        let entries = std::iter::from_fn(|| {
            access.next_entry::<PeerExchangeKey, (NetworkPublicKey, BoundedAddrs)>().transpose()
        })
        .take(MAX_PEER_EXCHANGE_ENTRIES.saturating_add(1))
        .collect::<Result<Vec<_>, _>>()?;
        if entries.len() > MAX_PEER_EXCHANGE_ENTRIES {
            return Err(serde::de::Error::invalid_length(entries.len(), &self));
        }
        Ok(BoundedEntries(
            entries
                .into_iter()
                .map(|(key, (network_key, addrs))| (key, (network_key, addrs.0)))
                .collect(),
        ))
    }
}

/// One entry's address set, decoded with the per-peer cap enforced the same way.
struct BoundedAddrs(HashSet<Multiaddr>);

impl<'de> Deserialize<'de> for BoundedAddrs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedAddrsVisitor)
    }
}

struct BoundedAddrsVisitor;

impl<'de> serde::de::Visitor<'de> for BoundedAddrsVisitor {
    type Value = BoundedAddrs;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "at most {MAX_PEER_EXCHANGE_ADDRS_PER_PEER} multiaddrs")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let declared = seq.size_hint().unwrap_or(0);
        if declared > MAX_PEER_EXCHANGE_ADDRS_PER_PEER {
            return Err(serde::de::Error::invalid_length(declared, &self));
        }
        let addrs = std::iter::from_fn(|| seq.next_element::<Multiaddr>().transpose())
            .take(MAX_PEER_EXCHANGE_ADDRS_PER_PEER.saturating_add(1))
            .collect::<Result<Vec<_>, _>>()?;
        if addrs.len() > MAX_PEER_EXCHANGE_ADDRS_PER_PEER {
            return Err(serde::de::Error::invalid_length(addrs.len(), &self));
        }
        Ok(BoundedAddrs(addrs.into_iter().collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::multiaddr::Protocol;
    use rand::{rngs::StdRng, SeedableRng};
    use std::net::Ipv4Addr;
    use tn_types::{BlsKeypair, NetworkKeypair};

    fn network_key() -> NetworkPublicKey {
        NetworkKeypair::generate_ed25519().public().clone().into()
    }

    fn raw_key(index: usize) -> PeerExchangeKey {
        let tag = u16::try_from(index).expect("index fits u16").to_le_bytes();
        let bytes: Vec<u8> = tag
            .into_iter()
            .chain(std::iter::repeat(7u8))
            .take(BlsPublicKey::COMPRESSED_BYTES)
            .collect();
        PeerExchangeKey(bytes.try_into().expect("compressed length"))
    }

    fn multiaddr_for_index(index: usize) -> Multiaddr {
        Multiaddr::empty()
            .with(Protocol::Ip4(Ipv4Addr::new(203, 0, 113, 1)))
            .with(Protocol::Tcp(8000 + u16::try_from(index).expect("index fits u16")))
    }

    fn map_with_entries(count: usize) -> PeerExchangeMap {
        PeerExchangeMap::from(
            (0..count)
                .map(|index| (raw_key(index), (network_key(), HashSet::new())))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn map_with_addrs(count: usize) -> PeerExchangeMap {
        let addrs: HashSet<Multiaddr> = (0..count).map(multiaddr_for_index).collect();
        PeerExchangeMap::from(HashMap::from([(raw_key(0), (network_key(), addrs))]))
    }

    /// A key that is not a curve point still decodes as the advertised bytes, so decoding does
    /// no curve work; the one decompression point reports it as invalid.
    #[test]
    fn peer_exchange_key_decodes_without_decompression() {
        let encoded = bcs::to_bytes(&raw_key(0)).expect("encode raw key");
        let decoded: PeerExchangeKey = bcs::from_bytes(&encoded).expect("non-point decodes");
        assert_eq!(decoded, raw_key(0));
        assert!(decoded.to_bls_public_key().is_none(), "the bytes are not a point");

        // only the compressed length is accepted
        let short_len = BlsPublicKey::COMPRESSED_BYTES - 1;
        let short: Vec<u8> = std::iter::once(u8::try_from(short_len).expect("fits u8"))
            .chain(std::iter::repeat(7u8).take(short_len))
            .collect();
        assert!(bcs::from_bytes::<PeerExchangeKey>(&short).is_err());
    }

    /// The exchange key serializes exactly as the BLS key it stands for, so a map keyed by
    /// either produces the same bytes and a real key survives the round trip.
    #[test]
    fn peer_exchange_map_wire_format_matches_bls_keyed_map() {
        let mut rng = StdRng::from_seed([3; 32]);
        let bls = *BlsKeypair::generate(&mut rng).public();
        let net = network_key();
        let addrs = HashSet::from([multiaddr_for_index(0), multiaddr_for_index(1)]);

        let bls_keyed: HashMap<BlsPublicKey, (NetworkPublicKey, HashSet<Multiaddr>)> =
            HashMap::from([(bls, (net.clone(), addrs.clone()))]);
        let exchange = PeerExchangeMap::from(bls_keyed.clone());

        let from_bls = bcs::to_bytes(&bls_keyed).expect("encode bls keyed map");
        let from_exchange = bcs::to_bytes(&exchange).expect("encode exchange map");
        assert_eq!(from_bls, from_exchange);
        assert_eq!(
            bcs::to_bytes(&bls).expect("encode key"),
            bcs::to_bytes(&PeerExchangeKey::from(bls)).expect("encode exchange key")
        );

        let decoded: PeerExchangeMap = bcs::from_bytes(&from_bls).expect("decode exchange map");
        assert_eq!(decoded, exchange);
        let (key, (decoded_net, decoded_addrs)) = decoded.into_iter().next().expect("one entry");
        assert_eq!(key.to_bls_public_key(), Some(bls));
        assert_eq!(decoded_net, net);
        assert_eq!(decoded_addrs, addrs);
    }

    /// A map at the entry cap decodes; one entry over it is rejected from the length prefix.
    #[test]
    fn peer_exchange_map_rejects_entries_over_cap() {
        let at_cap = map_with_entries(MAX_PEER_EXCHANGE_ENTRIES);
        let encoded = bcs::to_bytes(&at_cap).expect("encode at-cap map");
        let decoded: PeerExchangeMap = bcs::from_bytes(&encoded).expect("at-cap map decodes");
        assert_eq!(decoded, at_cap);

        let over_cap = map_with_entries(MAX_PEER_EXCHANGE_ENTRIES + 1);
        assert_eq!(over_cap.0.len(), MAX_PEER_EXCHANGE_ENTRIES + 1);
        let encoded = bcs::to_bytes(&over_cap).expect("encode over-cap map");
        assert!(bcs::from_bytes::<PeerExchangeMap>(&encoded).is_err());
    }

    /// An entry at the per-peer address cap decodes; one address over it rejects the map.
    #[test]
    fn peer_exchange_map_rejects_addrs_over_cap() {
        let at_cap = map_with_addrs(MAX_PEER_EXCHANGE_ADDRS_PER_PEER);
        let encoded = bcs::to_bytes(&at_cap).expect("encode at-cap addrs");
        let decoded: PeerExchangeMap = bcs::from_bytes(&encoded).expect("at-cap addrs decode");
        assert_eq!(decoded, at_cap);

        let over_cap = map_with_addrs(MAX_PEER_EXCHANGE_ADDRS_PER_PEER + 1);
        let encoded = bcs::to_bytes(&over_cap).expect("encode over-cap addrs");
        assert!(bcs::from_bytes::<PeerExchangeMap>(&encoded).is_err());
    }

    /// The codec ceiling holds a map at every cap, so an honest goodbye always fits.
    #[test]
    fn peer_exchange_message_size_holds_a_map_at_every_cap() {
        let addrs: HashSet<Multiaddr> =
            (0..MAX_PEER_EXCHANGE_ADDRS_PER_PEER).map(multiaddr_for_index).collect();
        let full = PeerExchangeMap::from(
            (0..MAX_PEER_EXCHANGE_ENTRIES)
                .map(|index| (raw_key(index), (network_key(), addrs.clone())))
                .collect::<HashMap<_, _>>(),
        );
        let encoded = bcs::to_bytes(&full).expect("encode full map");
        assert!(encoded.len() <= MAX_PEER_EXCHANGE_MESSAGE_SIZE);
    }
}
