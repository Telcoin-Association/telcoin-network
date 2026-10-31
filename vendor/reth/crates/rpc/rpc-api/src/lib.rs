//! Reth RPC interface definitions
//!
//! Provides all RPC interfaces.
//!
//! ## Feature Flags
//!
//! - `client`: Enables JSON-RPC client support.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]

// TN(TN-0021): the traits of namespaces TN never serves are removed: admin, anvil, engine,
// hardhat, miner, otterscan, reth, testing, txpool and validation.
mod debug;
mod mev;
mod net;
mod rpc;
mod trace;
mod web3;

/// re-export of all server traits
pub use servers::*;

/// Aggregates all server traits.
pub mod servers {
    pub use crate::{
        debug::{DebugApiServer, DebugExecutionWitnessApiServer},
        mev::{MevFullApiServer, MevSimApiServer},
        net::NetApiServer,
        rpc::RpcApiServer,
        trace::TraceApiServer,
        web3::Web3ApiServer,
    };
    pub use reth_rpc_eth_api::{
        self as eth, EthApiServer, EthBundleApiServer, EthCallBundleApiServer, EthFilterApiServer,
        EthPubSubApiServer, L2EthApiExtServer,
    };
}

/// re-export of all client traits
#[cfg(feature = "client")]
pub use clients::*;

/// Aggregates all client traits.
#[cfg(feature = "client")]
pub mod clients {
    pub use crate::{
        debug::{DebugApiClient, DebugExecutionWitnessApiClient},
        mev::{MevFullApiClient, MevSimApiClient},
        net::NetApiClient,
        rpc::RpcApiServer,
        trace::TraceApiClient,
        web3::Web3ApiClient,
    };
    pub use reth_rpc_eth_api::{
        EthApiClient, EthBundleApiClient, EthCallBundleApiClient, EthFilterApiClient,
        L2EthApiExtServer,
    };
}
