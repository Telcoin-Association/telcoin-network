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
