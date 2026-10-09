//! Regression tests for local record metadata, persistence and query ordering.

use super::*;
use crate::{
    consensus::PendingKadQuery,
    types::{NodeRecord, RecordDomain},
};
use libp2p::{multiaddr::Protocol, Multiaddr};
use tn_storage::mem_db::MemDatabase;
use tn_types::{BlsKeypair, NetworkKeypair, Signer as _};

/// Construct an authenticated primary record with an explicitly controlled timestamp.
fn signed_record(key: &BlsKeypair, timestamp: tn_types::TimestampSec) -> NodeRecord {
    let domain = RecordDomain::new(2017, NetworkType::Primary);
    let address = Multiaddr::empty()
        .with(Protocol::Ip4(std::net::Ipv4Addr::LOCALHOST))
        .with(Protocol::Udp(8000))
        .with(Protocol::QuicV1);
    let mut record = NodeRecord::build(
        domain,
        NetworkKeypair::generate_ed25519().public().into(),
        address,
        None,
        |bytes| key.sign(bytes),
    );
    record.info.timestamp = timestamp;
    // Sign the actual timestamp in the fixture rather than mutate authenticated bytes.
    record.signature = key.sign(&encode(&(
        b"telcoin-network/node-record/v1".as_slice(),
        domain.chain_id(),
        0_u8,
        tn_types::WorkerId::default(),
        &record.info,
    )));
    assert!(
        NodeRecord::decode_and_verify(&encode(&record), domain, key.public()).is_some(),
        "controlled timestamp fixture must verify"
    );
    record
}

/// Restart and identical republishes preserve the original ceiling and signed bytes.
#[test]
fn persisted_freshness_survives_restart_and_republish() -> Result<(), String> {
    let db = MemDatabase::default();
    let bls = BlsKeypair::generate(&mut rand::rng());
    let key = *bls.public();
    let config = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut rand::rng()));
    let domain = RecordDomain::new(2017, NetworkType::Primary);
    let node = signed_record(&bls, u64::MAX);
    let record = Record {
        key: node_record_key(&key),
        value: encode(&node),
        publisher: Some(node.info.pubkey.clone().into()),
        expires: None,
    };
    [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(1)].into_iter().try_for_each(
        |network| {
            let mut store = KadStore::new(db.clone(), PeerId::random(), &config, network);
            let timestamp = RecordTimestamp::admit(u64::MAX, 1_000);
            store
                .put_with_timestamp(record.clone(), Some(timestamp))
                .map_err(|error| error.to_string())?;
            assert_eq!(store.record_timestamp(&record.key), Some(timestamp));
            let mut restarted = KadStore::new(db.clone(), PeerId::random(), &config, network);
            assert_eq!(restarted.record_timestamp(&record.key), Some(timestamp));
            restarted.put(record.clone()).map_err(|error| error.to_string())?;
            assert_eq!(restarted.record_timestamp(&record.key), Some(timestamp));
            let retained = restarted.get(&record.key).ok_or("missing record")?;
            assert_eq!(retained.value, record.value);
            assert!(NodeRecord::decode_and_verify(&retained.value, domain, &key).is_some());
            assert!(RecordTimestamp::admit(now(), now()).supersedes(timestamp, now()));
            Ok(())
        },
    )
}

/// Pre-upgrade rows retain their signed payload but receive repairable local metadata.
#[test]
fn legacy_future_record_is_repairable() -> Result<(), &'static str> {
    let bls = BlsKeypair::generate(&mut rand::rng());
    let key = *bls.public();
    let domain = RecordDomain::new(2017, NetworkType::Primary);
    let node = signed_record(&bls, u64::MAX);
    let record =
        Record { key: node_record_key(&key), value: encode(&node), publisher: None, expires: None };
    let legacy: KadRecord = record.clone().into();
    let decoded = StoredKadRecord::decode(&encode(&legacy)).ok_or("legacy decode failed")?;
    let timestamp = decoded.timestamp.ok_or("missing legacy metadata")?;
    assert_eq!(decoded.record.value, record.value);
    assert!(NodeRecord::decode_and_verify(&decoded.record.value, domain, &key).is_some());
    assert!(RecordTimestamp::admit(u64::MAX - 1, now()).supersedes(timestamp, now()));
    Ok(())
}

/// Query winners follow bounded local ordering instead of the largest raw timestamp.
#[test]
fn query_repairs_future_record_without_reaccepting_stale_results() -> Result<(), &'static str> {
    let bls = BlsKeypair::generate(&mut rand::rng());
    let mut query = PendingKadQuery::from(*bls.public());
    query.consider(signed_record(&bls, u64::MAX), 1_000);
    query.consider(signed_record(&bls, 1_001), 1_001);
    query.consider(signed_record(&bls, 999), 1_001);
    let (winner, timestamp) = query.into_result().ok_or("query lost its winner")?;
    assert_eq!(winner.info.timestamp, 1_001);
    assert_eq!(timestamp, RecordTimestamp::admit(1_001, 1_001));
    Ok(())
}
