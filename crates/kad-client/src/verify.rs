//! Pure record verification and newest-wins aggregation, with no swarm involved.
//!
//! [`verify_record`] mirrors the node's `ConsensusNetwork::peer_record_valid` and the key check in
//! its `process_kad_query_result` (`crates/network-libp2p/src/consensus.rs`), so a record this
//! client accepts is exactly one the node itself would accept from a peer. [`fold_newest`]
//! mirrors the node's aggregation across the copies a single lookup returns.

use libp2p::{kad, PeerId};
use tn_node_record::{
    BlsPublicKey, NetworkInfo, NodeRecord, RecordDomain, MAX_ADVERTISED_MULTIADDRS,
};

/// A [`NodeRecord`] that passed every check the node applies to records learned from peers.
#[derive(Clone, Debug)]
pub struct VerifiedRecord {
    /// The BLS key the record is published under and signed by.
    pub key: BlsPublicKey,
    /// The signed record.
    pub record: NodeRecord,
    /// How many valid copies of this key's record the lookup returned.
    ///
    /// Copies that failed verification are not counted. A count above one means the record was
    /// replicated across peers; the copy kept is the one with the greatest timestamp.
    pub copies_seen: usize,
}

impl VerifiedRecord {
    /// The libp2p peer id derived from the record's network public key.
    ///
    /// This is the identity the validator's swarm authenticates as on the wire. Verification
    /// requires it to equal the kademlia publisher of the record, so it is also the identity that
    /// published the record.
    pub fn peer_id(&self) -> PeerId {
        self.record.info.pubkey.clone().into()
    }

    /// The advertised network information.
    pub fn info(&self) -> &NetworkInfo {
        &self.record.info
    }
}

/// Why a record returned by the DHT was discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RejectReason {
    /// The record's key is not a BLS public key, or it is a different key than the one queried.
    ///
    /// The node assesses a fatal penalty on the peer that returned it
    /// (`process_kad_query_result`); the client only discards the copy.
    KeyMismatch,
    /// The value did not decode as a [`NodeRecord`], or its signature did not verify for the
    /// queried key under the client's `(chain_id, network_type)` domain.
    ///
    /// A record signed for another chain or role fails here even when the BLS key matches: the
    /// domain is folded into the signed bytes but never transmitted (GHSA-cc64-wfq5-56ph).
    SignatureOrDomain,
    /// The record advertises more than [`MAX_ADVERTISED_MULTIADDRS`] addresses
    /// (GHSA-29v6-gvv5-45gx).
    TooManyMultiaddrs {
        /// How many addresses the record carried.
        count: usize,
    },
    /// The kademlia `publisher` is absent or is not the peer id of the record's network key.
    ///
    /// The publisher is stamped by the originating node's `put_record` and carried on the wire,
    /// so a third party re-serving a stale copy under its own identity is rejected here.
    PublisherMismatch,
}

/// Validate a raw kademlia record against the key it was requested for.
///
/// Checks run in the same order as the node's `peer_record_valid`:
/// 1. the record key must decode as a BLS public key equal to `expected`;
/// 2. the value must decode as a [`NodeRecord`] whose signature verifies for `expected` under
///    `domain`;
/// 3. the record must advertise at most [`MAX_ADVERTISED_MULTIADDRS`] addresses;
/// 4. the kademlia `publisher` must equal the peer id of the record's network public key.
pub(crate) fn verify_record(
    domain: RecordDomain,
    expected: &BlsPublicKey,
    record: &kad::Record,
) -> Result<VerifiedRecord, RejectReason> {
    let key = BlsPublicKey::from_literal_bytes(record.key.as_ref())
        .ok()
        .filter(|key| key == expected)
        .ok_or(RejectReason::KeyMismatch)?;

    let (key, node_record) = NodeRecord::decode_and_verify(&record.value, domain, &key)
        .ok_or(RejectReason::SignatureOrDomain)?;

    let count = node_record.info.multiaddrs.len();
    if count > MAX_ADVERTISED_MULTIADDRS {
        return Err(RejectReason::TooManyMultiaddrs { count });
    }

    let expected_publisher: PeerId = node_record.info.pubkey.clone().into();
    if record.publisher != Some(expected_publisher) {
        return Err(RejectReason::PublisherMismatch);
    }

    Ok(VerifiedRecord { key, record: node_record, copies_seen: 1 })
}

/// Fold a newly verified copy into the best record seen so far for a key.
///
/// A strictly newer timestamp replaces the held record; ties keep the record already held, as the
/// node does. The copy count accumulates either way.
pub(crate) fn fold_newest(best: &mut Option<VerifiedRecord>, candidate: VerifiedRecord) {
    match best {
        None => *best = Some(candidate),
        Some(current) => {
            current.copies_seen += candidate.copies_seen;
            if current.record.info.timestamp < candidate.record.info.timestamp {
                current.record = candidate.record;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::Multiaddr;
    use tn_node_record::{NetworkType, RpcInfo};
    use tn_types::{
        encode, now, BlsKeypair, BlsSignature, NetworkKeypair, NetworkPublicKey, Signer as _,
        WorkerId,
    };

    /// The record signer's key material: the BLS key the record is keyed and signed by, and the
    /// network identity that publishes it.
    struct Signer {
        bls: BlsKeypair,
        network: NetworkKeypair,
    }

    impl Signer {
        fn generate() -> Self {
            Self {
                bls: BlsKeypair::generate(&mut rand::rng()),
                network: NetworkKeypair::generate_ed25519(),
            }
        }

        fn bls_key(&self) -> BlsPublicKey {
            *self.bls.public()
        }

        fn network_key(&self) -> NetworkPublicKey {
            self.network.public().into()
        }

        fn peer_id(&self) -> PeerId {
            self.network.public().to_peer_id()
        }

        /// Sign a record for `domain` exactly as the node does.
        fn node_record(&self, domain: RecordDomain, rpc: Option<RpcInfo>) -> NodeRecord {
            NodeRecord::build(domain, self.network_key(), multiaddr(), rpc, |data| {
                self.bls.sign(data)
            })
        }

        /// The kademlia record the node would put for `node_record`: keyed by the raw BLS key,
        /// stamped with its own peer id as publisher.
        fn kad_record(&self, node_record: &NodeRecord) -> kad::Record {
            kad::Record {
                key: kad::RecordKey::new(&self.bls_key()),
                value: encode(node_record),
                publisher: Some(self.peer_id()),
                expires: None,
            }
        }
    }

    fn multiaddr() -> Multiaddr {
        "/ip4/127.0.0.1/udp/49594/quic-v1".parse().expect("static multiaddr parses")
    }

    fn rpc() -> RpcInfo {
        RpcInfo {
            http: "https://a.example:8545/".parse().expect("http url"),
            ws: Some("wss://a.example:8546/".parse().expect("ws url")),
        }
    }

    /// The domain every fixture record is signed for unless a test says otherwise.
    fn worker_domain() -> RecordDomain {
        RecordDomain::new(2017, NetworkType::Worker(0))
    }

    #[test]
    fn valid_record_is_accepted() {
        let signer = Signer::generate();
        let node_record = signer.node_record(worker_domain(), Some(rpc()));
        let record = signer.kad_record(&node_record);

        let verified = verify_record(worker_domain(), &signer.bls_key(), &record)
            .expect("record signed for the queried domain and key verifies");
        assert_eq!(verified.key, signer.bls_key());
        assert_eq!(verified.peer_id(), signer.peer_id());
        assert_eq!(verified.info().pubkey, signer.network_key());
        assert_eq!(verified.info().multiaddrs, vec![multiaddr()]);
        assert_eq!(verified.info().rpc, Some(rpc()));
        assert_eq!(verified.copies_seen, 1);
    }

    /// A record signed for the worker DHT is rejected under the primary domain of the same chain,
    /// and a record signed for one chain is rejected under another: the domain is part of the
    /// signed bytes.
    #[test]
    fn wrong_domain_is_rejected_as_signature_or_domain() {
        let signer = Signer::generate();
        let node_record = signer.node_record(worker_domain(), None);
        let record = signer.kad_record(&node_record);

        let primary = RecordDomain::new(2017, NetworkType::Primary);
        assert_eq!(
            verify_record(primary, &signer.bls_key(), &record).err(),
            Some(RejectReason::SignatureOrDomain)
        );

        let other_chain = RecordDomain::new(2018, NetworkType::Worker(0));
        assert_eq!(
            verify_record(other_chain, &signer.bls_key(), &record).err(),
            Some(RejectReason::SignatureOrDomain)
        );

        let sibling_worker = RecordDomain::new(2017, NetworkType::Worker(1));
        assert_eq!(
            verify_record(sibling_worker, &signer.bls_key(), &record).err(),
            Some(RejectReason::SignatureOrDomain)
        );
    }

    /// Garbage bytes under a well-formed key fail to decode.
    #[test]
    fn undecodable_value_is_rejected_as_signature_or_domain() {
        let signer = Signer::generate();
        let node_record = signer.node_record(worker_domain(), None);
        let mut record = signer.kad_record(&node_record);
        record.value = vec![0xde, 0xad, 0xbe, 0xef];

        assert_eq!(
            verify_record(worker_domain(), &signer.bls_key(), &record).err(),
            Some(RejectReason::SignatureOrDomain)
        );
    }

    /// A record keyed by a different BLS key than the one queried is rejected before its signature
    /// is examined, and so is a key that is not a BLS public key at all.
    #[test]
    fn key_mismatch_is_rejected() {
        let signer = Signer::generate();
        let node_record = signer.node_record(worker_domain(), None);
        let record = signer.kad_record(&node_record);

        let other = BlsKeypair::generate(&mut rand::rng());
        assert_eq!(
            verify_record(worker_domain(), other.public(), &record).err(),
            Some(RejectReason::KeyMismatch)
        );

        let mut bad_key = record.clone();
        bad_key.key = kad::RecordKey::new(&[0u8; 96]);
        assert_eq!(
            verify_record(worker_domain(), &signer.bls_key(), &bad_key).err(),
            Some(RejectReason::KeyMismatch)
        );
    }

    /// The publisher must be exactly the peer id of the record's network key: neither absent nor
    /// some other peer re-serving the copy.
    #[test]
    fn publisher_none_or_other_peer_is_rejected() {
        let signer = Signer::generate();
        let node_record = signer.node_record(worker_domain(), None);

        let mut no_publisher = signer.kad_record(&node_record);
        no_publisher.publisher = None;
        assert_eq!(
            verify_record(worker_domain(), &signer.bls_key(), &no_publisher).err(),
            Some(RejectReason::PublisherMismatch)
        );

        let mut other_publisher = signer.kad_record(&node_record);
        other_publisher.publisher = Some(NetworkKeypair::generate_ed25519().public().to_peer_id());
        assert_eq!(
            verify_record(worker_domain(), &signer.bls_key(), &other_publisher).err(),
            Some(RejectReason::PublisherMismatch)
        );
    }

    /// Sign `info` for `domain` over the same payload the record crate signs: the domain label,
    /// chain id, role discriminant, worker id, and BCS-encoded info. Mirrors the private
    /// `NodeRecord::signing_bytes`; [`too_many_multiaddrs_is_rejected`] proves the mirror is
    /// exact by verifying a single-address record signed this way through the public path.
    fn sign_info(signer: &Signer, domain: RecordDomain, info: &NetworkInfo) -> BlsSignature {
        const LABEL: &[u8] = b"telcoin-network/node-record/v1";
        let (role, worker_id): (u8, WorkerId) = match domain.network_type() {
            NetworkType::Primary => (0, 0),
            NetworkType::Worker(id) => (1, id),
        };
        signer.bls.sign(&encode(&(LABEL, domain.chain_id(), role, worker_id, info)))
    }

    /// A validly signed record that advertises more than the cap is rejected after the signature
    /// check, with the offending count reported.
    #[test]
    fn too_many_multiaddrs_is_rejected() {
        let signer = Signer::generate();
        let record_with = |count: usize| {
            let info = NetworkInfo {
                pubkey: signer.network_key(),
                multiaddrs: vec![multiaddr(); count],
                timestamp: now(),
                rpc: None,
            };
            let signature = sign_info(&signer, worker_domain(), &info);
            signer.kad_record(&NodeRecord { info, signature })
        };

        // exactly at the cap: the hand-signed payload verifies, proving the signing mirror
        let at_cap = verify_record(
            worker_domain(),
            &signer.bls_key(),
            &record_with(MAX_ADVERTISED_MULTIADDRS),
        )
        .expect("a record at the cap verifies");
        assert_eq!(at_cap.info().multiaddrs.len(), MAX_ADVERTISED_MULTIADDRS);

        // one over: same signer, same domain, rejected for the count alone
        let count = MAX_ADVERTISED_MULTIADDRS + 1;
        assert_eq!(
            verify_record(worker_domain(), &signer.bls_key(), &record_with(count)).err(),
            Some(RejectReason::TooManyMultiaddrs { count })
        );
    }

    #[test]
    fn fold_newest_keeps_greater_timestamp_and_existing_on_tie() {
        let signer = Signer::generate();
        let older = signer.node_record(worker_domain(), None);
        let mut newer = older.clone();
        newer.info.timestamp = older.info.timestamp + 10;
        newer.info.rpc = Some(rpc());
        let mut same_age = older.clone();
        same_age.info.rpc = Some(rpc());

        let verified = |record: &NodeRecord| VerifiedRecord {
            key: signer.bls_key(),
            record: record.clone(),
            copies_seen: 1,
        };

        // first copy is taken as-is
        let mut best = None;
        fold_newest(&mut best, verified(&older));
        let held = best.as_ref().expect("first copy held");
        assert_eq!(held.record.info.timestamp, older.info.timestamp);
        assert_eq!(held.copies_seen, 1);

        // a tie keeps the copy already held
        fold_newest(&mut best, verified(&same_age));
        let held = best.as_ref().expect("held");
        assert_eq!(held.record.info.timestamp, older.info.timestamp);
        assert!(held.record.info.rpc.is_none(), "tie must not replace the held copy");
        assert_eq!(held.copies_seen, 2);

        // strictly newer replaces
        fold_newest(&mut best, verified(&newer));
        let held = best.as_ref().expect("held");
        assert_eq!(held.record.info.timestamp, newer.info.timestamp);
        assert_eq!(held.record.info.rpc, Some(rpc()));
        assert_eq!(held.copies_seen, 3);

        // an older copy arriving afterwards does not roll back
        fold_newest(&mut best, verified(&older));
        let held = best.as_ref().expect("held");
        assert_eq!(held.record.info.timestamp, newer.info.timestamp);
        assert_eq!(held.copies_seen, 4);
    }
}
