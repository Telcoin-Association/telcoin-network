//! Unit tests for the signed node record.

use crate::{
    AddressError, NetworkInfo, NetworkType, NodeRecord, RecordDomain, RecordError,
    MAX_ADVERTISED_MULTIADDRS,
};
use libp2p::Multiaddr;
use tn_types::{
    BlsKeypair, BlsPublicKey, BlsSignature, NetworkKeypair, NetworkPublicKey, RpcInfo, Signer as _,
};

/// A well-formed multiaddr for record tests; the address itself is never dialed.
fn create_multiaddr(_ip: Option<std::net::IpAddr>) -> Multiaddr {
    Multiaddr::empty()
        .with(libp2p::multiaddr::Protocol::Ip4(std::net::Ipv4Addr::LOCALHOST))
        .with(libp2p::multiaddr::Protocol::Udp(8000))
        .with(libp2p::multiaddr::Protocol::QuicV1)
}

/// The key material a validator signs a record with: its BLS keypair and the network identity
/// the record advertises. Mirrors the slice of the node's `KeyConfig` the record path uses so
/// these tests need no dependency on the node's config crate.
struct KeyConfig {
    bls: BlsKeypair,
    network: NetworkKeypair,
}

/// Parse a QUIC test endpoint, returning the address-policy error for malformed fixture input.
fn endpoint(input: &str) -> Result<Multiaddr, AddressError> {
    input.parse().map_err(|_| AddressError::InvalidTransport)
}

/// Ordered overlap records keep the previous wire layout as an exact prefix, append the
/// network-key co-signature (issue #1751), and authenticate every endpoint byte.
#[test]
fn multi_address_records_preserve_wire_and_authentication() -> Result<(), RecordError> {
    use rand::{rngs::StdRng, SeedableRng as _};
    use serde::{Deserialize, Serialize};
    use tn_types::{encode, try_decode, TimestampSec};

    /// The supported pre-migration reader's unchanged NetworkInfo wire layout.
    #[derive(Serialize, Deserialize)]
    struct PreviousInfo {
        /// Transport public key.
        pubkey: NetworkPublicKey,
        /// Ordered wire addresses, supported by the old decoder even though admission capped one.
        multiaddrs: Vec<Multiaddr>,
        /// Publication timestamp.
        timestamp: TimestampSec,
        /// Optional RPC advertisement.
        rpc: Option<RpcInfo>,
    }
    /// The supported pre-migration reader's unchanged record layout.
    #[derive(Serialize, Deserialize)]
    struct PreviousRecord {
        /// Signed network information.
        info: PreviousInfo,
        /// BLS signature of the v1 domain-bound information.
        signature: BlsSignature,
    }

    let key =
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_seed([19; 32])));
    let domain = RecordDomain::new(2017, NetworkType::Worker(1));
    let addresses = [
        "/ip4/192.0.2.10/udp/9000/quic-v1",
        "/ip6/2001:db8::10/udp/9000/quic-v1",
        "/ip4/192.0.2.20/udp/9000/quic-v1",
        "/ip6/2001:db8::20/udp/9000/quic-v1",
    ]
    .into_iter()
    .map(endpoint)
    .collect::<Result<Vec<_>, _>>()?;
    [1, 2, MAX_ADVERTISED_MULTIADDRS].into_iter().try_for_each(|count| -> Result<(), RecordError> {
        let record = NodeRecord::build_multi(
            domain,
            &key.primary_public_key(),
            key.primary_network_keypair(),
            addresses.iter().take(count).cloned().collect(),
            None,
            |data| key.request_signature_direct(data),
        )?;
        let bytes = encode(&record);
        // The co-signature extends the wire layout (issue #1751): previous readers reject the
        // trailing bytes, and the previous layout is an exact prefix of the current bytes.
        assert!(try_decode::<PreviousRecord>(&bytes).is_err());
        let prefix = encode(&(&record.info, &record.signature));
        assert!(bytes.starts_with(&prefix));
        let previous: PreviousRecord =
            try_decode(&prefix).map_err(|_| AddressError::InvalidTransport)?;
        assert_eq!(encode(&previous), prefix);
        let previous_payload = encode(&(
            b"telcoin-network/node-record/v1".as_slice(),
            2017u64,
            1u8,
            1u16,
            &previous.info,
        ));
        // Old readers authenticate the identical prefix bytes. Their one-address admission rule
        // deliberately rejects overlap records until readers have been upgraded.
        assert!(previous.signature.verify_raw(&previous_payload, &key.primary_public_key()));
        assert!(!previous.signature.verify_raw(&encode(&previous.info), &key.primary_public_key()));
        // A pre-domain BLS signature in the current layout decodes but never verifies.
        let pre_domain = NodeRecord {
            info: record.info.clone(),
            signature: key.request_signature_direct(&encode(&previous.info)),
            network_signature: record.network_signature.clone(),
        };
        assert!(NodeRecord::try_decode_compat(&encode(&pre_domain)).is_some());
        assert!(NodeRecord::decode_and_verify(
            &encode(&pre_domain),
            domain,
            &key.primary_public_key()
        )
        .is_none());
        assert_eq!(pre_domain.info.multiaddrs.len() <= 1, count == 1);
        assert!(NodeRecord::decode_and_verify(&bytes, domain, &key.primary_public_key()).is_some());
        assert!(NodeRecord::decode_and_verify(
            &bytes,
            RecordDomain::new(2017, NetworkType::Worker(0)),
            &key.primary_public_key(),
        )
        .is_none());
        let mut altered = record;
        altered.info.multiaddrs.reverse();
        if count > 1 {
            assert!(NodeRecord::decode_and_verify(
                &encode(&altered),
                domain,
                &key.primary_public_key()
            )
            .is_none());
        }
        altered.info.multiaddrs.push(endpoint("/ip4/192.0.2.99/udp/9000/quic-v1")?);
        assert!(NodeRecord::decode_and_verify(
            &encode(&altered),
            domain,
            &key.primary_public_key()
        )
        .is_none());
        Ok(())
    })
}

/// The writer and authenticated decoder reject empty, duplicate, foreign and excessive endpoints.
#[test]
fn advertised_address_policy_is_symmetric() -> Result<(), AddressError> {
    use rand::{rngs::StdRng, SeedableRng as _};
    use tn_types::{encode, now};
    let key =
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_seed([20; 32])));
    let domain = RecordDomain::new(2017, NetworkType::Primary);
    let address = endpoint("/ip4/192.0.2.1/udp/9000/quic-v1")?;
    let peer: libp2p::PeerId = key.primary_network_public_key().into();
    let suffixed = address.clone().with_p2p(peer).map_err(|_| AddressError::InvalidPeerId)?;
    let excessive = (1..=MAX_ADVERTISED_MULTIADDRS + 1)
        .map(|port| endpoint(&format!("/ip4/192.0.2.1/udp/{port}/quic-v1")))
        .collect::<Result<Vec<_>, _>>()?;
    [
        Vec::new(),
        vec![address.clone(), address],
        vec![suffixed.clone(), endpoint("/ip4/192.0.2.1/udp/9000/quic-v1")?],
        excessive,
        vec![endpoint("/dns/example.com/udp/9000/quic-v1")?],
        vec![endpoint("/ip4/0.0.0.0/udp/9000/quic-v1")?],
        vec![endpoint("/ip4/192.0.2.1/udp/0/quic-v1")?],
        vec![endpoint("/ip4/192.0.2.1/tcp/9000")?],
    ]
    .into_iter()
    .try_for_each(|multiaddrs| {
        assert!(NodeRecord::build_multi(
            domain,
            &key.primary_public_key(),
            key.primary_network_keypair(),
            multiaddrs.clone(),
            None,
            |data| key.request_signature_direct(data)
        )
        .is_err_and(|err| matches!(err, RecordError::Address(_))));
        let info = NetworkInfo {
            pubkey: key.primary_network_public_key(),
            multiaddrs,
            timestamp: now(),
            rpc: None,
        };
        let signature = key.request_signature_direct(&NodeRecord::signing_bytes(domain, &info));
        let network_signature = key
            .primary_network_keypair()
            .sign(&NodeRecord::network_signing_bytes(domain, &key.primary_public_key(), &info))
            .expect("ed25519 signing is infallible");
        let record = NodeRecord { info, signature, network_signature };
        assert!(NodeRecord::decode_and_verify(&encode(&record), domain, &key.primary_public_key())
            .is_none());
        Ok(())
    })?;
    let other_key = NetworkKeypair::generate_ed25519().public().into();
    assert!(crate::validate_advertised_addresses(&[suffixed], &other_key).is_err());
    Ok(())
}

impl KeyConfig {
    fn new_with_testing_key(bls: BlsKeypair) -> Self {
        Self { bls, network: NetworkKeypair::generate_ed25519() }
    }

    fn primary_public_key(&self) -> BlsPublicKey {
        *self.bls.public()
    }

    /// The public half of the network identity the record advertises.
    fn primary_network_public_key(&self) -> NetworkPublicKey {
        self.network.public().into()
    }

    /// The network keypair that co-signs the record's binding to the BLS key.
    fn primary_network_keypair(&self) -> &NetworkKeypair {
        &self.network
    }

    fn request_signature_direct(&self, data: &[u8]) -> BlsSignature {
        self.bls.sign(data)
    }
}

#[test]
fn test_node_record() {
    let multiaddr = create_multiaddr(None);
    let bls_keypair = BlsKeypair::generate(&mut rand::rng());
    let pubkey = *bls_keypair.public();
    let key_config = KeyConfig::new_with_testing_key(bls_keypair);
    let domain = RecordDomain::new(2017, NetworkType::Primary);

    // build a valid node record
    let node_record = NodeRecord::build(
        domain,
        &key_config.primary_public_key(),
        key_config.primary_network_keypair(),
        multiaddr,
        None,
        |data| key_config.request_signature_direct(data),
    )
    .expect("sign record");
    let (bls_pubkey, record) =
        node_record.clone().verify(domain, &pubkey).expect("valid node record");

    // assert returned values match
    assert!(record.verify(domain, &bls_pubkey).is_some());

    // assert incorrect pubkey fails
    let bad_keypair = BlsKeypair::generate(&mut rand::rng());
    assert!(node_record.verify(domain, bad_keypair.public()).is_none());
}

/// Round-trip a [NodeRecord] that includes a populated [RpcInfo]. Ensures the
/// signature covers the new field and verifies after encode/decode.
#[test]
fn test_node_record_with_rpc_roundtrip() {
    use tn_types::{decode, encode};

    let multiaddr = create_multiaddr(None);
    let bls_keypair = BlsKeypair::generate(&mut rand::rng());
    let pubkey = *bls_keypair.public();
    let key_config = KeyConfig::new_with_testing_key(bls_keypair);

    let domain = RecordDomain::new(2017, NetworkType::Primary);

    let rpc = RpcInfo {
        http: "https://a.example:8545/".parse().expect("http url"),
        ws: Some("wss://a.example:8546/".parse().expect("ws url")),
    };

    let node_record = NodeRecord::build(
        domain,
        &key_config.primary_public_key(),
        key_config.primary_network_keypair(),
        multiaddr,
        Some(rpc.clone()),
        |data| key_config.request_signature_direct(data),
    )
    .expect("sign record");

    // encode and decode round-trip preserves rpc and stays verifiable
    let bytes = encode(&node_record);
    let decoded: NodeRecord = decode(&bytes);
    assert_eq!(decoded.info.rpc.as_ref(), Some(&rpc));
    assert!(decoded.verify(domain, &pubkey).is_some());
}

/// Legacy (pre-`rpc`) bytes still decode through the compat fallback with
/// `rpc: None`, but they are UNSCOPED (their signature covers no `(chain, role)`
/// domain) so `decode_and_verify` now REJECTS them under any domain. This is the
/// intended post-fix behavior for GHSA-cc64-wfq5-56ph: only current,
/// domain-scoped records verify. Current-layout bytes verify under the matching
/// domain, and garbage is rejected by both helpers.
#[test]
fn test_legacy_record_compat_decode_and_verify() {
    use serde::{Deserialize, Serialize};
    use tn_types::{encode, now, BlsSignature, Multiaddr, NetworkPublicKey, TimestampSec};

    /// Pre-upgrade NetworkInfo shape (no `rpc` field). Field order MUST mirror
    /// the historical layout so encoded bytes match what an old peer signed.
    #[derive(Serialize, Deserialize)]
    struct OldNetworkInfo {
        pubkey: NetworkPublicKey,
        multiaddrs: Vec<Multiaddr>,
        timestamp: TimestampSec,
    }

    /// Pre-upgrade NodeRecord shape.
    #[derive(Serialize, Deserialize)]
    struct OldNodeRecord {
        info: OldNetworkInfo,
        signature: BlsSignature,
    }

    let multiaddr = create_multiaddr(None);
    let bls_keypair = BlsKeypair::generate(&mut rand::rng());
    let pubkey = *bls_keypair.public();
    let key_config = KeyConfig::new_with_testing_key(bls_keypair);
    let domain = RecordDomain::new(2017, NetworkType::Primary);

    let old_info = OldNetworkInfo {
        pubkey: key_config.primary_network_public_key(),
        multiaddrs: vec![multiaddr.clone()],
        timestamp: now(),
    };
    let signature = key_config.request_signature_direct(&encode(&old_info));
    let legacy_bytes = encode(&OldNodeRecord { info: old_info, signature });

    // compat decode falls back to the legacy layout with rpc defaulted
    let decoded = NodeRecord::try_decode_compat(&legacy_bytes).expect("legacy bytes decode");
    assert!(decoded.info.rpc.is_none());
    assert_eq!(decoded.info.multiaddrs, vec![multiaddr.clone()]);

    // GHSA-cc64-wfq5-56ph: the unscoped legacy record carries no `(chain, role)`
    // domain in its signature, so `decode_and_verify` now REJECTS it even with the
    // correct pubkey, under both the primary and any worker domain.
    assert!(NodeRecord::decode_and_verify(&legacy_bytes, domain, &pubkey).is_none());
    let worker_domain = RecordDomain::new(2017, NetworkType::Worker(0));
    assert!(NodeRecord::decode_and_verify(&legacy_bytes, worker_domain, &pubkey).is_none());

    // the wrong key is likewise rejected
    let other_keypair = BlsKeypair::generate(&mut rand::rng());
    assert!(NodeRecord::decode_and_verify(&legacy_bytes, domain, other_keypair.public()).is_none());

    // current-layout, domain-scoped bytes decode and verify under the matching domain
    let rpc = RpcInfo { http: "https://a.example:8545/".parse().expect("http url"), ws: None };
    let current = NodeRecord::build(
        domain,
        &key_config.primary_public_key(),
        key_config.primary_network_keypair(),
        multiaddr,
        Some(rpc.clone()),
        |data| key_config.request_signature_direct(data),
    )
    .expect("sign record");
    let current_bytes = encode(&current);
    let decoded = NodeRecord::try_decode_compat(&current_bytes).expect("current bytes decode");
    assert_eq!(decoded.info.rpc, Some(rpc));
    assert!(NodeRecord::decode_and_verify(&current_bytes, domain, &pubkey).is_some());

    // garbage is rejected by both helpers
    let garbage = [0xde, 0xad, 0xbe, 0xef];
    assert!(NodeRecord::try_decode_compat(&garbage).is_none());
    assert!(NodeRecord::decode_and_verify(&garbage, domain, &pubkey).is_none());
}

/// GHSA-cc64-wfq5-56ph cross-ROLE replay: a record signed for the worker(0)
/// network verifies under that same worker domain but is REJECTED under the
/// primary domain (same chain, same BLS key). Exercised on both the in-memory
/// `verify` path and the bytes `decode_and_verify` path.
#[test]
fn test_cross_role_replay_rejected() {
    use tn_types::encode;

    let multiaddr = create_multiaddr(None);
    let bls_keypair = BlsKeypair::generate(&mut rand::rng());
    let pubkey = *bls_keypair.public();
    let key_config = KeyConfig::new_with_testing_key(bls_keypair);

    let chain = 2017;
    let worker_domain = RecordDomain::new(chain, NetworkType::Worker(0));
    let primary_domain = RecordDomain::new(chain, NetworkType::Primary);

    // sign for the worker(0) network
    let record = NodeRecord::build(
        worker_domain,
        &key_config.primary_public_key(),
        key_config.primary_network_keypair(),
        multiaddr,
        None,
        |data| key_config.request_signature_direct(data),
    )
    .expect("sign record");

    // in-memory path: verifies under the SAME worker domain, rejected under primary
    assert!(record.clone().verify(worker_domain, &pubkey).is_some());
    assert!(record.clone().verify(primary_domain, &pubkey).is_none());

    // bytes path mirrors the in-memory outcome
    let bytes = encode(&record);
    assert!(NodeRecord::decode_and_verify(&bytes, worker_domain, &pubkey).is_some());
    assert!(NodeRecord::decode_and_verify(&bytes, primary_domain, &pubkey).is_none());
}

/// Sibling workers must reject each other's records even with the same chain and BLS key.
#[test]
fn test_cross_worker_replay_rejected() {
    let key_config = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut rand::rng()));
    let pubkey = key_config.primary_public_key();
    let worker_0 = RecordDomain::new(2017, NetworkType::Worker(0));
    let worker_1 = RecordDomain::new(2017, NetworkType::Worker(1));
    let record = NodeRecord::build(
        worker_0,
        &key_config.primary_public_key(),
        key_config.primary_network_keypair(),
        create_multiaddr(None),
        None,
        |data| key_config.request_signature_direct(data),
    )
    .expect("sign record");
    assert!(record.clone().verify(worker_0, &pubkey).is_some());
    assert!(record.clone().verify(worker_1, &pubkey).is_none());
    let bytes = tn_types::encode(&record);
    assert!(NodeRecord::decode_and_verify(&bytes, worker_0, &pubkey).is_some());
    assert!(NodeRecord::decode_and_verify(&bytes, worker_1, &pubkey).is_none());
}

/// GHSA-cc64-wfq5-56ph cross-CHAIN replay: a record signed for one chain
/// verifies under that chain but is REJECTED under a different chain id (same
/// role, same BLS key), on both the in-memory and bytes paths.
#[test]
fn test_cross_chain_replay_rejected() {
    use tn_types::encode;

    let multiaddr = create_multiaddr(None);
    let bls_keypair = BlsKeypair::generate(&mut rand::rng());
    let pubkey = *bls_keypair.public();
    let key_config = KeyConfig::new_with_testing_key(bls_keypair);

    // two distinct chain ids
    let chain_a = 2017;
    let chain_b = 2018;
    let domain_a = RecordDomain::new(chain_a, NetworkType::Primary);
    let domain_b = RecordDomain::new(chain_b, NetworkType::Primary);

    // sign for chain_a
    let record = NodeRecord::build(
        domain_a,
        &key_config.primary_public_key(),
        key_config.primary_network_keypair(),
        multiaddr,
        None,
        |data| key_config.request_signature_direct(data),
    )
    .expect("sign record");

    // verifies under chain_a, rejected under chain_b
    assert!(record.clone().verify(domain_a, &pubkey).is_some());
    assert!(record.clone().verify(domain_b, &pubkey).is_none());

    // bytes path mirrors the in-memory outcome
    let bytes = encode(&record);
    assert!(NodeRecord::decode_and_verify(&bytes, domain_a, &pubkey).is_some());
    assert!(NodeRecord::decode_and_verify(&bytes, domain_b, &pubkey).is_none());
}

/// Opt-out producers (validators that do not advertise RPC) should still
/// produce records that verify after encode/decode.
#[test]
fn test_node_record_without_rpc_roundtrip() {
    use tn_types::{decode, encode};

    let multiaddr = create_multiaddr(None);
    let bls_keypair = BlsKeypair::generate(&mut rand::rng());
    let pubkey = *bls_keypair.public();
    let key_config = KeyConfig::new_with_testing_key(bls_keypair);
    let domain = RecordDomain::new(2017, NetworkType::Primary);

    let node_record = NodeRecord::build(
        domain,
        &key_config.primary_public_key(),
        key_config.primary_network_keypair(),
        multiaddr,
        None,
        |data| key_config.request_signature_direct(data),
    )
    .expect("sign record");
    let bytes = encode(&node_record);
    let decoded: NodeRecord = decode(&bytes);
    assert!(decoded.info.rpc.is_none());
    assert!(decoded.verify(domain, &pubkey).is_some());
}

/// Issue #1751: a BLS key cannot publish a record that names a network key whose holder did not
/// co-sign the binding to that BLS key.
#[test]
fn network_key_must_cosign_binding() {
    use tn_types::encode;

    let victim = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut rand::rng()));
    let attacker = BlsKeypair::generate(&mut rand::rng());
    let domain = RecordDomain::new(2017, NetworkType::Primary);
    let honest = NodeRecord::build(
        domain,
        &victim.primary_public_key(),
        victim.primary_network_keypair(),
        create_multiaddr(None),
        None,
        |data| victim.request_signature_direct(data),
    )
    .expect("sign record");

    // (1) the honest record verifies under the victim's BLS key
    assert!(honest.clone().verify(domain, &victim.primary_public_key()).is_some());

    // (2) the attacker signs the victim's info with its own BLS key and copies the victim's
    // co-signature, which covers the victim's BLS key and not the attacker's
    let forged = NodeRecord {
        signature: attacker.sign(&NodeRecord::signing_bytes(domain, &honest.info)),
        ..honest.clone()
    };
    assert!(NodeRecord::decode_and_verify(&encode(&forged), domain, attacker.public()).is_none());
    assert!(forged.clone().verify(domain, attacker.public()).is_none());

    // (3) a co-signature by another network key over the right payload does not bind the
    // network key that the record names
    let other_network = NetworkKeypair::generate_ed25519();
    let wrong_cosigner = NodeRecord {
        network_signature: other_network
            .sign(&NodeRecord::network_signing_bytes(domain, attacker.public(), &forged.info))
            .expect("sign"),
        ..forged.clone()
    };
    assert!(wrong_cosigner.verify(domain, attacker.public()).is_none());

    // (4) a record without a co-signature never verifies
    let unsigned = NodeRecord { network_signature: Vec::new(), ..forged };
    assert!(unsigned.verify(domain, attacker.public()).is_none());

    // (5) when the victim's network key does co-sign the attacker's BLS key, the record
    // verifies: the check is the co-signature, not equality of the two key pairs
    let consented = NodeRecord::build(
        domain,
        attacker.public(),
        victim.primary_network_keypair(),
        create_multiaddr(None),
        None,
        |data| attacker.sign(data),
    )
    .expect("sign record");
    assert!(consented.verify(domain, attacker.public()).is_some());
}

/// Bytes in the pre-#1751 layout (the current info and the BLS signature, without the
/// co-signature) are rejected by the compat decoder and by the authenticated decoder.
#[test]
fn previous_layout_without_cosignature_is_rejected() {
    use tn_types::encode;

    let key_config = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut rand::rng()));
    let domain = RecordDomain::new(2017, NetworkType::Primary);
    let record = NodeRecord::build(
        domain,
        &key_config.primary_public_key(),
        key_config.primary_network_keypair(),
        create_multiaddr(None),
        None,
        |data| key_config.request_signature_direct(data),
    )
    .expect("sign record");
    // BCS flattens the tuple, so these are exactly the pre-#1751 wire bytes
    let previous = encode(&(&record.info, &record.signature));
    assert!(NodeRecord::try_decode_compat(&previous).is_none());
    assert!(NodeRecord::decode_and_verify(&previous, domain, &key_config.primary_public_key())
        .is_none());
}
