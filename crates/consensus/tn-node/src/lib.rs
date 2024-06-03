use consensus_metrics::RegistryService;
// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0
use dirs::DataDirPath;
use engine::{ExecutionNode, TnBuilder};
use futures::{future::try_join_all, stream::FuturesUnordered};
use narwhal_network::client::NetworkClient;
pub use narwhal_storage::{CertificateStoreCacheMetrics, NodeStorage};
use prometheus::Registry;
use reth::dirs::ChainPath;
use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_evm::execute::BlockExecutorProvider;
use tn_config::{
    read_validator_keypair_from_file, traits::ConfigTrait as _, Config, BLS_KEYFILE,
    PRIMARY_NETWORK_KEYFILE, WORKER_NETWORK_KEYFILE,
};
use tn_types::{ChainIdentifier, Committee, WorkerCache};
use tracing::info;

use crate::{dirs::TelcoinDirs as _, primary::PrimaryNode, worker::WorkerNode};

pub mod dirs;
pub mod engine;
mod error;
pub mod metrics;
pub mod primary;
pub mod worker;

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
pub async fn launch_node<DB, Evm>(
    mut builder: TnBuilder<DB>,
    evm_config: Evm,
    tn_datadir: ChainPath<DataDirPath>,
) -> eyre::Result<()>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Clone + Unpin + 'static,
    Evm: BlockExecutorProvider + Clone + 'static,
{
    // config for validator keys
    let config = builder.tn_config.clone();
    // adjust rpc instance ports
    builder.node_config.adjust_instance_ports();
    let engine = ExecutionNode::new(builder, evm_config)?;

    info!(target: "telcoin::node", "execution engine created");

    let narwhal_db_path = tn_datadir.narwhal_db_path();

    info!(target: "telcoin::cli", "opening node storage at {:?}", narwhal_db_path);

    // open storage for consensus - no metrics passed
    // TODO: pass metrics here?
    let node_storage = NodeStorage::reopen(narwhal_db_path, None);

    info!(target: "telcoin::cli", "node storage open");

    let registry_service = RegistryService::new(Registry::new());
    let network_client =
        NetworkClient::new_from_public_key(config.validator_info.primary_network_key());
    let primary = PrimaryNode::new(config.parameters.clone(), registry_service.clone());
    let (worker_id, _worker_info) = config.workers().first_worker()?;
    let worker = WorkerNode::new(*worker_id, config.parameters.clone(), registry_service);

    // TODO: find a better way to manage keys
    //
    // load keys to start the primary
    let validator_keypath = tn_datadir.validator_keys_path();
    info!(target: "telcoin::cli", "loading validator keys at {:?}", validator_keypath);
    let bls_keypair = read_validator_keypair_from_file(validator_keypath.join(BLS_KEYFILE))?;
    let network_keypair =
        read_validator_keypair_from_file(validator_keypath.join(PRIMARY_NETWORK_KEYFILE))?;

    // load committee from file
    let mut committee: Committee = Config::load_from_path(tn_datadir.committee_path())?;
    committee.load();
    info!(target: "telcoin::cli", "committee loaded");
    // TODO: make worker cache part of committee?
    let worker_cache: WorkerCache = Config::load_from_path(tn_datadir.worker_cache_path())?;
    info!(target: "telcoin::cli", "worker cache loaded");

    // TODO: this could be a separate method on `Committee` to have robust checks in place
    // - all public keys are unique
    // - thresholds / stake
    //
    // assert committee loaded correctly
    // assert!(committee.size() >= 4, "not enough validators in committee.");

    // TODO: better assertion here
    // right now, each validator should only have 1 worker
    // this assertion would incorrectly pass if 1 authority had 2 workers and another had 0
    //
    // assert worker cache loaded correctly
    assert!(
        worker_cache.all_workers().len() == committee.size(),
        "each validator within committee must have one worker"
    );

    // start the primary
    primary
        .start(
            bls_keypair,
            network_keypair,
            committee.clone(),
            ChainIdentifier::unknown(), // TODO: use ChainSpec here
            worker_cache.clone(),
            network_client.clone(),
            &node_storage,
            &engine,
        )
        .await?;

    let worker_network_keypair =
        read_validator_keypair_from_file(validator_keypath.join(WORKER_NETWORK_KEYFILE))?;

    // start the worker
    worker
        .start(
            config.primary_public_key()?.clone(), // TODO: remove result for this method
            worker_network_keypair,
            committee,
            worker_cache,
            network_client,
            &node_storage,
            None, // optional metrics
            &engine,
        )
        .await?;

    // TODO: use value from CLI
    let terminate_early = false;

    if terminate_early {
        Ok(())
    } else {
        // The pipeline has finished downloading blocks up to `--debug.tip` or
        // `--debug.max-block`. Keep other node components alive for further usage.
        futures::future::pending().await
    }
}
