//! Generate subcommand

use crate::args::clap_address_parser;
use clap::{value_parser, Args, Subcommand};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NodeInfo, TelcoinDirs};
use tn_types::{get_available_udp_port, Address, Multiaddr, Protocol};
use tracing::info;

/// Generate keypairs and save them to a file.
#[derive(Debug, Clone, Args)]
#[command(args_conflicts_with_subcommands = true)]
pub struct GenerateKeys {
    /// Generate command that creates keypairs and writes to file.
    #[command(subcommand)]
    pub node_type: NodeType,
}

///Subcommand to generate keys for validator, primary, or worker.
#[derive(Debug, Clone, Subcommand)]
pub enum NodeType {
    /// Generate all validator keys and write them to file.
    #[command(name = "validator", alias = "all")]
    ValidatorKeys(KeygenArgs),
    /// Generate all observer (non-validator) keys and write them to file.
    #[command(name = "observer")]
    ObserverKeys(KeygenArgs),
}

#[derive(Debug, Clone, Args)]
pub struct KeygenArgs {
    /// The number of workers for the primary.
    /// Currently workers MUST be 1.
    #[arg(long, value_name = "workers", global = true, default_value_t = 1, value_parser = value_parser!(u16).range(..=4))]
    pub workers: u16,

    /// Overwrite existing keys, if present.
    ///
    /// Warning: Existing keys will be lost.
    #[arg(
        long = "force",
        alias = "overwrite",
        help_heading = "Overwrite existing keys. Warning: existing keys will be lost.",
        verbatim_doc_comment
    )]
    pub force: bool,

    /// The address for suggested fee recipient.
    ///
    /// The execution layer address, derived from `secp256k1` keypair.
    /// The validator uses this address when producing batches and blocks.
    /// Validators can pass "0" to use the zero address.
    /// Address doesn't have to start with "0x", but the CLI supports the "0x" format too.
    #[arg(
        long = "address",
        alias = "execution-address",
        help_heading = "The address that should receive block rewards. Pass `0` to use the zero address.",
        env = "EXECUTION_ADDRESS",
        value_parser = clap_address_parser,
        verbatim_doc_comment
    )]
    pub address: Address,

    /// The external multiaddr for the primary p2p network. Must be quic-v1 and udp. Recommended do
    /// not include p2p protocol id - the CLI will add this.
    /// For example: /ip4/[HOST]/udp/[PORT]/quic-v1
    ///
    /// If not set will default to /ip4/127.0.0.1/udp/[PORT]/quic-v1 with an unused port for PORT.
    /// This default is only useful for tests (including a local testnet).
    ///
    /// NOTE: the node's [Protocol::P2p] is automatically added to the Multiaddr and does not need
    /// to be provided.
    #[arg(long, value_name = "MULTIADDR", env = "TN_EXTERNAL_PRIMARY_ADDR")]
    pub external_primary_addr: Option<Multiaddr>,

    /// List of external multiaddrs for the workers p2p networks, comma seperated. Must be quic-v1
    /// and udp. Recommended do not include p2p protocol id - the CLI will add this.
    /// For example: /ip4/[HOST1]/udp/[PORT1]/quic-v1,
    ///
    /// If not set each worker will default to /ip4/127.0.0.1/udp/[PORT]/quic-v1 with an unused
    /// port for PORT. This default is only useful for tests (including a local testnet).
    ///
    /// NOTE: the node's [Protocol::P2p] is automatically added to the Multiaddr and does not need
    /// to be provided.
    #[arg(
        long,
        value_name = "MULTIADDRS",
        env = "TN_EXTERNAL_WORKER_ADDRS",
        value_delimiter = ','
    )]
    pub external_worker_addrs: Option<Vec<Multiaddr>>,
}

impl KeygenArgs {
    fn update_keys<TND: TelcoinDirs>(
        &self,
        node_info: &mut NodeInfo,
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<()> {
        let key_config = KeyConfig::generate_and_save(tn_datadir, passphrase)?;
        let proof = key_config.generate_proof_of_possession_bls(&self.address)?;
        node_info.bls_public_key = key_config.primary_public_key();
        node_info.proof_of_possession = proof;
        node_info.name = format!(
            "node-{}",
            bs58::encode(&node_info.bls_public_key.to_bytes()[0..8]).into_string()
        );

        // network keypair for authority
        let network_publickey = key_config.primary_network_public_key();
        node_info.p2p_info.primary.network_key = network_publickey.clone();
        node_info.p2p_info.primary.network_address =
            if let Some(primary_addr) = &self.external_primary_addr {
                primary_addr.clone().with_p2p(network_publickey.into()).map_err(|_| {
                    eyre::eyre!("Primary address already contains a different P2P protocol")
                })?
            } else {
                let primary_udp_port = get_available_udp_port("127.0.0.1").unwrap_or(49584);
                let addr: Multiaddr =
                    format!("/ip4/127.0.0.1/udp/{primary_udp_port}/quic-v1").parse()?;
                addr.with(Protocol::P2p(network_publickey.into()))
            };

        info!(target: "tn::generate_keys", primary=?node_info.p2p_info.primary.network_address, "updating primary external network address");

        // network keypair for workers
        let network_publickey = key_config.worker_network_public_key();
        if node_info.p2p_info.workers.is_empty() {
            let default_worker = NodeInfo::default()
                .p2p_info
                .workers
                .into_iter()
                .next()
                .expect("default node info has a worker entry");
            node_info.p2p_info.workers.push(default_worker);
        }
        node_info.p2p_info.workers[0].network_key = network_publickey.clone();
        node_info.p2p_info.workers[0].network_address =
            if let Some(worker_addrs) = &self.external_worker_addrs {
                if let Some(worker_addr) = worker_addrs.first() {
                    worker_addr.clone().with_p2p(network_publickey.into()).map_err(|_| {
                        eyre::eyre!("worker address already contains a different P2P protocol")
                    })?
                } else {
                    let worker_udp_port = get_available_udp_port("127.0.0.1").unwrap_or(49584);
                    let addr: Multiaddr =
                        format!("/ip4/127.0.0.1/udp/{worker_udp_port}/quic-v1").parse()?;
                    addr.with(Protocol::P2p(network_publickey.into()))
                }
            } else {
                let worker_udp_port = get_available_udp_port("127.0.0.1").unwrap_or(49584);
                let addr: Multiaddr =
                    format!("/ip4/127.0.0.1/udp/{worker_udp_port}/quic-v1").parse()?;
                addr.with(Protocol::P2p(network_publickey.into()))
            };

        info!(target: "tn::generate_keys", worker=?node_info.p2p_info.workers[0].network_address, "updating worker external network address");

        Ok(())
    }

    /// Create all necessary information needed for validator and save to file.
    pub fn execute<TND: TelcoinDirs>(
        &self,
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<()> {
        info!(target: "tn::generate_keys", "generating keys for full validator node");
        let mut node_info = NodeInfo::default();
        if self.workers != 1 {
            return Err(eyre::eyre!("Only supports a single worker at this time!"));
        }
        /* Uncomment when multi-worker support is enabled
        if self.workers > 1 {
            node_info.p2p_info.worker_index.0 = Vec::with_capacity(self.workers as usize);
            for _ in 0..self.workers {
                node_info.p2p_info.worker_index.0.push(WorkerInfo::default());
            }
        }
        */

        self.update_keys(&mut node_info, tn_datadir, passphrase)?;

        // add execution address
        node_info.execution_address = self.address;
        Config::write_to_path(tn_datadir.node_info_path(), &node_info, ConfigFmt::YAML)?;

        Ok(())
    }
}
