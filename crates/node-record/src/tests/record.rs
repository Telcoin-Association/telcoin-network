//! Unit tests for the signed node record.

use crate::{
    AddressError, NetworkInfo, NetworkType, NodeRecord, RecordDomain, MAX_ADVERTISED_MULTIADDRS,
};
use libp2p::Multiaddr;
use tn_types::{
    BlsKeypair, BlsPublicKey, BlsSignature, NetworkKeypair, NetworkPublicKey, RpcInfo, Signer as _,
};

/// A well-formed multiaddr for record tests; the address itself is never dialed.
fn create_multiaddr(_ip: Option<std::net::IpAddr>) -> Multiaddr {
    "/ip4/127.0.0.1/udp/8000/quic-v1".parse().expect("static multiaddr parses")
}

/// The key material a validator signs a record with: its BLS keypair and the network identity
/// the record advertises. Mirrors the slice of the node's `KeyConfig` the record path uses so
/// these tests need no dependency on the node's config crate.
struct KeyConfig {
    bls: BlsKeypair,
    network: NetworkPublicKey,
}

/// Parse a QUIC test endpoint, returning the address-policy error for malformed fixture input.
fn endpoint(input: &str) -> Result<Multiaddr, AddressError> {
    input.parse().map_err(|_| AddressError::InvalidTransport)
}

/// Ordered overlap records retain the existing wire layout and authenticate every endpoint byte.
#[test]
fn multi_address_records_preserve_wire_and_authentication() -> Result<(), AddressError> {
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
    [1, 2, MAX_ADVERTISED_MULTIADDRS].into_iter().try_for_each(|count| {
        let record = NodeRecord::build_multi(
            domain,
            key.primary_network_public_key(),
            addresses.iter().take(count).cloned().collect(),
            None,
            |data| key.request_signature_direct(data),
        )?;
        let bytes = encode(&record);
        let previous: PreviousRecord =
            try_decode(&bytes).map_err(|_| AddressError::InvalidTransport)?;
        assert_eq!(encode(&previous), bytes);
        let previous_payload = encode(&(
            b"telcoin-network/node-record/v1".as_slice(),
            2017u64,
            1u8,
            1u16,
            &previous.info,
        ));
        // Old readers authenticate the identical bytes. Their one-address admission rule
        // deliberately rejects overlap records until readers have been upgraded.
        assert!(previous.signature.verify_raw(&previous_payload, &key.primary_public_key()));
        assert!(!previous.signature.verify_raw(&encode(&previous.info), &key.primary_public_key()));
        let pre_domain = PreviousRecord {
            signature: key.request_signature_direct(&encode(&previous.info)),
            info: previous.info,
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
            key.primary_network_public_key(),
            multiaddrs.clone(),
            None,
            |data| key.request_signature_direct(data)
        )
        .is_err());
        let info = NetworkInfo {
            pubkey: key.primary_network_public_key(),
            multiaddrs,
            timestamp: now(),
            rpc: None,
        };
        let signature = key.request_signature_direct(&NodeRecord::signing_bytes(domain, &info));
        let record = NodeRecord { info, signature };
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
        Self { bls, network: NetworkKeypair::generate_ed25519().public().into() }
    }

    fn primary_public_key(&self) -> BlsPublicKey {
        *self.bls.public()
    }

    fn primary_network_public_key(&self) -> NetworkPublicKey {
        self.network.clone()
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
        key_config.primary_network_public_key(),
        multiaddr,
        None,
        |data| key_config.request_signature_direct(data),
    );
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
        key_config.primary_network_public_key(),
        multiaddr,
        Some(rpc.clone()),
        |data| key_config.request_signature_direct(data),
    );

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
        key_config.primary_network_public_key(),
        multiaddr,
        Some(rpc.clone()),
        |data| key_config.request_signature_direct(data),
    );
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
        key_config.primary_network_public_key(),
        multiaddr,
        None,
        |data| key_config.request_signature_direct(data),
    );

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
        key_config.primary_network_public_key(),
        create_multiaddr(None),
        None,
        |data| key_config.request_signature_direct(data),
    );
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
        key_config.primary_network_public_key(),
        multiaddr,
        None,
        |data| key_config.request_signature_direct(data),
    );

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
        key_config.primary_network_public_key(),
        multiaddr,
        None,
        |data| key_config.request_signature_direct(data),
    );
    let bytes = encode(&node_record);
    let decoded: NodeRecord = decode(&bytes);
    assert!(decoded.info.rpc.is_none());
    assert!(decoded.verify(domain, &pubkey).is_some());
}
