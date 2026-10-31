//! A network implementation that does nothing.
//!
//! This is useful for wiring components together that don't require network but still need to be
//! generic over it.

use core::marker::PhantomData;
use std::net::{IpAddr, SocketAddr};

use crate::{NetworkError, NetworkInfo, NetworkStatus, PeerId, PeersInfo};
use alloy_rpc_types_admin::EthProtocolInfo;
use enr::{secp256k1::SecretKey, Enr};
use reth_eth_wire_types::{EthNetworkPrimitives, ProtocolVersion};
use reth_network_peers::NodeRecord;

// TN(TN-0031): `NoopNetwork` implements only `NetworkInfo` and `PeersInfo`, the two network
// traits reth's RPC still bounds on. Upstream's `Peers`, downloader, sync, event and peers-handle
// impls went with those traits.

/// A type that implements all network trait that does nothing.
///
/// Intended for testing purposes where network is not used.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NoopNetwork<Net = EthNetworkPrimitives> {
    chain_id: u64,
    _marker: PhantomData<Net>,
}

impl<Net> NoopNetwork<Net> {
    /// Creates a new [`NoopNetwork`].
    pub const fn new() -> Self {
        Self {
            chain_id: 1, // mainnet
            _marker: PhantomData,
        }
    }

    /// Creates a new [`NoopNetwork`] from an existing one but with a new chain id.
    pub const fn with_chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = chain_id;
        self
    }
}

impl Default for NoopNetwork<EthNetworkPrimitives> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Net> NetworkInfo for NoopNetwork<Net>
where
    Net: Send + Sync,
{
    fn local_addr(&self) -> SocketAddr {
        (IpAddr::from(std::net::Ipv4Addr::UNSPECIFIED), 30303).into()
    }

    async fn network_status(&self) -> Result<NetworkStatus, NetworkError> {
        #[expect(deprecated)]
        Ok(NetworkStatus {
            client_version: "reth-test".to_string(),
            protocol_version: ProtocolVersion::V5 as u64,
            eth_protocol_info: EthProtocolInfo {
                network: 1,
                difficulty: None,
                genesis: Default::default(),
                config: Default::default(),
                head: Default::default(),
            },
            capabilities: vec![],
        })
    }

    fn chain_id(&self) -> u64 {
        self.chain_id
    }

    fn is_syncing(&self) -> bool {
        false
    }

    fn is_initially_syncing(&self) -> bool {
        false
    }
}

impl<Net> PeersInfo for NoopNetwork<Net>
where
    Net: Send + Sync,
{
    fn num_connected_peers(&self) -> usize {
        0
    }

    fn local_node_record(&self) -> NodeRecord {
        NodeRecord::new(self.local_addr(), PeerId::random())
    }

    fn local_enr(&self) -> Enr<SecretKey> {
        let sk = SecretKey::from_slice(&[0xcd; 32]).unwrap();
        Enr::builder().build(&sk).unwrap()
    }
}
