//! Unit tests for network types.rs

use super::{NetworkType, NodeRecord, RecordDomain, RpcInfo};
use crate::common::create_multiaddr;
use tn_config::KeyConfig;
use tn_types::{BlsKeypair, BlsSigner};

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
