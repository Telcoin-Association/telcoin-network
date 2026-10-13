//! The BLS-signed node record a validator publishes to the kademlia DHT.

use crate::{validate_advertised_addresses, AddressError, NetworkType};
use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};
use tn_types::{
    encode, now, try_decode, BlsPublicKey, BlsSignature, NetworkPublicKey, RpcInfo, TimestampSec,
    WorkerId,
};

/// List of addresses for a node, signature will be the nodes BLS signature
/// over the addresses to verify they are from the node in question.
/// Used to publish this to kademlia.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeRecord {
    /// The network information contained within the record.
    pub info: NetworkInfo,
    /// Signature of the info field with the node's BLS key.
    /// This is part of a kademlia record keyed on a BLS public key
    /// that can be used for verifiction.  Intended to stop malicious
    /// nodes from poisoning the routing table.
    pub signature: BlsSignature,
}

/// Pre-`rpc` [NetworkInfo] layout.
///
/// BCS is not self-describing, so records encoded and signed by pre-upgrade
/// software fail to decode under the current schema. This mirror preserves the
/// exact historical field order so legacy bytes can still be decoded (and their
/// signatures verified over the legacy encoding).
#[derive(Serialize, Deserialize)]
struct LegacyNetworkInfo {
    /// The node's [NetworkPublicKey].
    pubkey: NetworkPublicKey,
    /// Network address for node.
    multiaddrs: Vec<Multiaddr>,
    /// The timestamps when this was published.
    timestamp: TimestampSec,
}

/// Pre-`rpc` [NodeRecord] layout. See [LegacyNetworkInfo].
#[derive(Serialize, Deserialize)]
struct LegacyNodeRecord {
    /// The network information contained within the record.
    info: LegacyNetworkInfo,
    /// Signature of the info field with the node's BLS key.
    signature: BlsSignature,
}

impl From<LegacyNodeRecord> for NodeRecord {
    fn from(legacy: LegacyNodeRecord) -> Self {
        let LegacyNodeRecord {
            info: LegacyNetworkInfo { pubkey, multiaddrs, timestamp },
            signature,
        } = legacy;
        Self { info: NetworkInfo { pubkey, multiaddrs, timestamp, rpc: None }, signature }
    }
}

/// Domain-separation label folded into every [NodeRecord] signature.
///
/// Baked into the signed payload so a signature is valid only for this exact
/// purpose and schema. The trailing version is the signed-payload schema
/// version: bumping it invalidates every prior signature and forces a
/// coordinated re-sign across the network.
const NODE_RECORD_SIGNING_LABEL: &[u8] = b"telcoin-network/node-record/v1";

/// The `(chain, role)` network a [NodeRecord] signature is bound to.
///
/// The domain is folded into the bytes a signature covers but is **never
/// transmitted**: a verifier reconstructs it from its own network identity, so a
/// record signed for one `(chain_id, NetworkType)` network can never verify on
/// another. This closes cross-role / cross-chain [NodeRecord] replay
/// (GHSA-cc64-wfq5-56ph): a validly-signed worker record no longer verifies on
/// the primary DHT (the primary reconstructs the payload with the primary role),
/// and a record signed for one chain no longer verifies on another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordDomain {
    /// The chain the record is scoped to.
    chain_id: u64,
    /// The role/worker network the record is scoped to.
    network_type: NetworkType,
}

impl RecordDomain {
    /// Build a [RecordDomain] for the given chain and role network.
    pub fn new(chain_id: u64, network_type: NetworkType) -> Self {
        Self { chain_id, network_type }
    }

    /// The chain this domain is scoped to.
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// The role/worker network this domain is scoped to.
    pub fn network_type(&self) -> NetworkType {
        self.network_type
    }

    /// Role discriminant and worker id folded into the signed payload.
    ///
    /// The discriminant (`0` primary, `1` worker) keeps the primary role
    /// distinct from `Worker(0)`, so the worker id can default to `0` for the
    /// primary without the two domains colliding.
    fn role_parts(&self) -> (u8, WorkerId) {
        match self.network_type {
            NetworkType::Primary => (0, 0),
            NetworkType::Worker(id) => (1, id),
        }
    }
}

impl NodeRecord {
    /// The exact bytes a [NodeRecord] signature covers: the domain-separation
    /// label, the `(chain_id, role, worker_id)` [RecordDomain] the record is
    /// scoped to, and the BCS encoding of the advertised [NetworkInfo].
    ///
    /// The domain is folded in but never transmitted; the verifier reconstructs
    /// it from its own network identity (see [RecordDomain]), so the signature
    /// alone decides whether a record belongs on the verifying network.
    fn signing_bytes(domain: RecordDomain, info: &NetworkInfo) -> Vec<u8> {
        let (role, worker_id) = domain.role_parts();
        encode(&(NODE_RECORD_SIGNING_LABEL, domain.chain_id, role, worker_id, info))
    }

    /// Helper method to build a [NodeRecord] signed for `domain`.
    pub fn build<F>(
        domain: RecordDomain,
        pubkey: NetworkPublicKey,
        multiaddr: Multiaddr,
        rpc: Option<RpcInfo>,
        signer: F,
    ) -> NodeRecord
    where
        F: FnOnce(&[u8]) -> BlsSignature,
    {
        let info = NetworkInfo { pubkey, multiaddrs: vec![multiaddr], timestamp: now(), rpc };
        let data = Self::signing_bytes(domain, &info);
        let signature = signer(&data);
        Self { info, signature }
    }

    /// Sign a nonempty, ordered endpoint list within the shared migration budget.
    ///
    /// Uses the existing v1 signing domain and BCS layout. Order and every endpoint byte are
    /// authenticated; the first endpoint is the operator's preferred dial candidate.
    pub fn build_multi<F>(
        domain: RecordDomain,
        pubkey: NetworkPublicKey,
        multiaddrs: Vec<Multiaddr>,
        rpc: Option<RpcInfo>,
        signer: F,
    ) -> Result<NodeRecord, AddressError>
    where
        F: FnOnce(&[u8]) -> BlsSignature,
    {
        validate_advertised_addresses(&multiaddrs, &pubkey)?;
        let info = NetworkInfo { pubkey, multiaddrs, timestamp: now(), rpc };
        let signature = signer(&Self::signing_bytes(domain, &info));
        Ok(Self { info, signature })
    }

    /// Refresh the timestamp and signature while preserving configured endpoint order.
    pub fn refresh<F>(&mut self, domain: RecordDomain, signer: F)
    where
        F: FnOnce(&[u8]) -> BlsSignature,
    {
        self.info.timestamp = now();
        self.signature = signer(&Self::signing_bytes(domain, &self.info));
    }

    /// Verify the record's signature against `domain` and `pubkey`.
    ///
    /// Fails if the record was signed for a different `(chain, role)` network,
    /// even when the BLS `pubkey` matches: the domain is part of the signed
    /// bytes (see [RecordDomain]).
    pub fn verify(
        self,
        domain: RecordDomain,
        pubkey: &BlsPublicKey,
    ) -> Option<(BlsPublicKey, NodeRecord)> {
        let data = Self::signing_bytes(domain, &self.info);
        if self.signature.verify_raw(&data, pubkey) {
            Some((*pubkey, self))
        } else {
            None
        }
    }

    /// Return a reference to the record's [NetworkInfo].
    pub fn info(&self) -> &NetworkInfo {
        &self.info
    }

    /// Decode a [NodeRecord] from bytes, falling back to the pre-`rpc` legacy
    /// layout (with `rpc: None`) for records produced by pre-upgrade software.
    ///
    /// The fallback order is deterministic because the two layouts are mutually
    /// exclusive under BCS. `NetworkInfo` ends with `rpc: Option<RpcInfo>`, encoded
    /// as a single Option tag byte (`0x00`/`0x01`). The legacy layout ends with
    /// `signature: BlsSignature`, which BCS encodes via `serialize_bytes` as a
    /// ULEB128 length prefix (`0x30` = 48) followed by the 48 signature bytes.
    ///
    /// - Legacy bytes fail the current decode: where the current decoder expects the `rpc` Option
    ///   tag, legacy bytes hold the signature's `0x30` length prefix, which is neither `0x00` nor
    ///   `0x01`, so BCS rejects it.
    /// - Current bytes fail the legacy decode: the legacy decoder reads the `rpc` Option tag
    ///   (`0x00`/`0x01`) as the signature's length prefix, yielding a 0- or 1-byte slice that
    ///   `BlsSignature` rejects as an invalid signature.
    ///
    /// Does NOT verify the signature — use [Self::decode_and_verify] when
    /// authenticity matters.
    pub fn try_decode_compat(value: &[u8]) -> Option<NodeRecord> {
        Self::try_decode_current(value)
            .or_else(|| try_decode::<LegacyNodeRecord>(value).ok().map(Into::into))
    }

    /// Decode only the current wire layout, without verifying the signature.
    ///
    /// Authenticated readers must use this strict layout or [`Self::decode_and_verify`]. The
    /// inspection-only legacy fallback must not reconstruct information before authentication.
    pub fn try_decode_current(value: &[u8]) -> Option<NodeRecord> {
        try_decode::<NodeRecord>(value).ok()
    }

    /// Decode a [NodeRecord] and verify its BLS signature against the local
    /// `domain`.
    ///
    /// Only the current domain-scoped schema is accepted. Pre-domain records —
    /// including the pre-`rpc` `LegacyNodeRecord` layout — carry signatures that
    /// do not cover the `(chain, role)` domain, which is exactly the
    /// cross-network replay this rejects (GHSA-cc64-wfq5-56ph); such records no
    /// longer verify and are dropped after upgrade.
    pub fn decode_and_verify(
        value: &[u8],
        domain: RecordDomain,
        key: &BlsPublicKey,
    ) -> Option<(BlsPublicKey, NodeRecord)> {
        let record = Self::try_decode_current(value)?;
        validate_advertised_addresses(&record.info.multiaddrs, &record.info.pubkey).ok()?;
        record.verify(domain, key)
    }
}

/// The network information needed for consensus.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkInfo {
    /// The node's [NetworkPublicKey].
    pub pubkey: NetworkPublicKey,
    /// Network address for node.
    pub multiaddrs: Vec<Multiaddr>,
    /// The timestamps when this was published.
    /// Useful for nodes to compare latest records.
    pub timestamp: TimestampSec,
    /// Optional JSON-RPC endpoint information for this node.
    ///
    /// Populated only on worker [NodeRecord]s of validators that opt-in to
    /// advertising RPC publicly. `None` on primary records and on validators
    /// that do not expose RPC publicly.
    pub rpc: Option<RpcInfo>,
}

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
