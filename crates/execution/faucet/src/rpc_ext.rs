//! RPC extension

use super::Faucet;
use crate::FaucetConfig;
use jsonrpsee::proc_macros::rpc;
use reth::rpc::server_types::eth::EthResult;
use tn_reth::{RethEnv, WorkerTxPool};
use tn_types::{Address, TxHash};

/// Faucet that disperses 1 TEL every 24hours per requesting address.
#[rpc(server, namespace = "faucet")]
pub trait FaucetRpcExtApi {
    /// Transfer TEL to an address
    #[method(name = "transfer")]
    async fn transfer(&self, address: Address, contract: Option<Address>) -> EthResult<TxHash>;
}

/// The type that implements Faucet namespace trait.
#[derive(Debug)]
pub struct FaucetRpcExt {
    /// Type to interact with the faucet service task.
    faucet: Faucet,
}

#[async_trait::async_trait]
impl FaucetRpcExtApiServer for FaucetRpcExt {
    /// Faucet method.
    ///
    /// The faucet checks the time-based LRU cache for the recipient's address.
    /// If the address is not found, a transaction is created to transfer TEL
    /// to the recipient. Otherwise, a time is returned indicating when the
    /// recipient's request is valid.
    ///
    /// By default, addresses are removed from the cache every 24 hours.
    async fn transfer(&self, address: Address, contract: Option<Address>) -> EthResult<TxHash> {
        self.faucet.handle_request(address, contract).await
    }
}

impl FaucetRpcExt {
    /// Create new instance
    pub fn new(reth_env: RethEnv, pool: WorkerTxPool, config: FaucetConfig) -> Self {
        let faucet = Faucet::spawn(reth_env, pool, config);

        Self { faucet }
    }
}
