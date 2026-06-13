//! Unit tests for network types.rs

use super::{NodeRecord, RpcInfo};
use crate::common::create_multiaddr;
use tn_config::KeyConfig;
use tn_types::{BlsKeypair, BlsSigner};

#[test]
fn test_node_record() {
    let multiaddr = create_multiaddr(None);
    let bls_keypair = BlsKeypair::generate(&mut rand::rng());
    let pubkey = *bls_keypair.public();
    let key_config = KeyConfig::new_with_testing_key(bls_keypair);

    // build a valid node record
    let node_record =
        NodeRecord::build(key_config.primary_network_public_key(), multiaddr, None, |data| {
            key_config.request_signature_direct(data)
        });
    let (bls_pubkey, record) = node_record.clone().verify(&pubkey).expect("valid node record");

    // assert returned values match
    assert!(record.verify(&bls_pubkey).is_some());

    // assert incorrect pubkey fails
    let bad_keypair = BlsKeypair::generate(&mut rand::rng());
    assert!(node_record.verify(bad_keypair.public()).is_none());
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

    let rpc = RpcInfo {
        http: "https://a.example:8545/".parse().expect("http url"),
        ws: Some("wss://a.example:8546/".parse().expect("ws url")),
    };

    let node_record = NodeRecord::build(
        key_config.primary_network_public_key(),
        multiaddr,
        Some(rpc.clone()),
        |data| key_config.request_signature_direct(data),
    );

    // encode and decode round-trip preserves rpc and stays verifiable
    let bytes = encode(&node_record);
    let decoded: NodeRecord = decode(&bytes);
    assert_eq!(decoded.info.rpc.as_ref(), Some(&rpc));
    assert!(decoded.verify(&pubkey).is_some());
}

/// Legacy (pre-`rpc`) bytes decode through the compat fallback with `rpc: None`
/// and verify against the signature over the legacy encoding. Current-layout
/// bytes pass through unchanged, and garbage is rejected by both helpers.
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

    // the signature verifies over the legacy encoding
    let (verified_key, verified) =
        NodeRecord::decode_and_verify(&legacy_bytes, &pubkey).expect("legacy signature verifies");
    assert_eq!(verified_key, pubkey);
    assert!(verified.info.rpc.is_none());

    // the wrong key fails verification
    let other_keypair = BlsKeypair::generate(&mut rand::rng());
    assert!(NodeRecord::decode_and_verify(&legacy_bytes, other_keypair.public()).is_none());

    // current-layout bytes pass through the compat helpers unchanged
    let rpc = RpcInfo { http: "https://a.example:8545/".parse().expect("http url"), ws: None };
    let current = NodeRecord::build(
        key_config.primary_network_public_key(),
        multiaddr,
        Some(rpc.clone()),
        |data| key_config.request_signature_direct(data),
    );
    let current_bytes = encode(&current);
    let decoded = NodeRecord::try_decode_compat(&current_bytes).expect("current bytes decode");
    assert_eq!(decoded.info.rpc, Some(rpc));
    assert!(NodeRecord::decode_and_verify(&current_bytes, &pubkey).is_some());

    // garbage is rejected by both helpers
    let garbage = [0xde, 0xad, 0xbe, 0xef];
    assert!(NodeRecord::try_decode_compat(&garbage).is_none());
    assert!(NodeRecord::decode_and_verify(&garbage, &pubkey).is_none());
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

    let node_record =
        NodeRecord::build(key_config.primary_network_public_key(), multiaddr, None, |data| {
            key_config.request_signature_direct(data)
        });
    let bytes = encode(&node_record);
    let decoded: NodeRecord = decode(&bytes);
    assert!(decoded.info.rpc.is_none());
    assert!(decoded.verify(&pubkey).is_some());
}
