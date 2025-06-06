//! Unit tests for network types.rs

use super::NodeRecord;
use crate::common::create_multiaddr;
use tn_config::KeyConfig;
use tn_types::{BlsKeypair, BlsSigner};

#[test]
fn test_node_record() {
    let multiaddr = create_multiaddr(None);
    let bls_keypair = BlsKeypair::generate(&mut rand::rng());
    let pubkey = *bls_keypair.public();
    let key_config = KeyConfig::new_with_testing_key(bls_keypair);

    // build the node record
    let node_record = NodeRecord::build(
        key_config.primary_network_public_key(),
        multiaddr,
        "GSMA".to_string(),
        |data| key_config.request_signature_direct(data),
    );
    let (bls_pubkey, record) = node_record.clone().verify(&pubkey).expect("valid node record");

    // assert returned values match
    assert!(record.verify(&bls_pubkey).is_some());

    // assert incorrect pubkey fails
    let bad_keypair = BlsKeypair::generate(&mut rand::rng());
    assert!(node_record.verify(bad_keypair.public()).is_none());
}
