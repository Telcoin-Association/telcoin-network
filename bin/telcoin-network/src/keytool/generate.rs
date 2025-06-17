//! Generate subcommand

use crate::args::clap_address_parser;
use clap::{value_parser, Args, Subcommand};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NodeInfo, TelcoinDirs};
use tn_types::{get_available_udp_port, Address, Multiaddr};
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

    /// The multiaddr for the primary p2p network.  Must be quic-v1 and udp.
    /// For example: /ip4/[HOST]/udp/[PORT]/quic-v1
    /// If not set will default to /ip4/127.0.0.1/udp/[PORT]/quic-v1 with an unused port for PORT.
    /// This default is only useful for tests (including a local testnet).
    #[arg(long, value_name = "MULTIADDR", env = "TN_PRIMARY_ADDR")]
    pub primary_addr: Option<Multiaddr>,

    /// List of multiaddrs for the workers p2p networks, comma seperated.  Must be quic-v1 and udp.
    /// For example: /ip4/[HOST1]/udp/[PORT1]/quic-v1,/ip4/[HOST2]/udp/[PORT2]/quic-v1
    /// If not set each worker will default to /ip4/127.0.0.1/udp/[PORT]/quic-v1 with an unused
    /// port for PORT. This default is only useful for tests (including a local testnet).
    #[arg(long, value_name = "MULTIADDRS", env = "TN_WORKER_ADDRS", value_delimiter = ',')]
    pub worker_addrs: Option<Vec<Multiaddr>>,
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
        node_info.p2p_info.network_key = network_publickey;
        node_info.p2p_info.network_address = if let Some(primary_addr) = &self.primary_addr {
            primary_addr.clone()
        } else {
            let primary_udp_port = get_available_udp_port("127.0.0.1").unwrap_or(49584);
            format!("/ip4/127.0.0.1/udp/{primary_udp_port}/quic-v1").parse()?
        };

        // network keypair for workers
        let network_publickey = key_config.worker_network_public_key();
        for (i, worker) in node_info.p2p_info.worker_index.0.iter_mut().enumerate() {
            worker.name = network_publickey.clone();
            worker.worker_address = if let Some(worker_addrs) = &self.worker_addrs {
                if let Some(addr) = worker_addrs.get(i) {
                    addr.clone()
                } else {
                    let udp_port = get_available_udp_port("127.0.0.1").unwrap_or(49584 + 1);
                    format!("/ip4/127.0.0.1/udp/{udp_port}/quic-v1").parse()?
                }
            } else {
                let udp_port = get_available_udp_port("127.0.0.1").unwrap_or(49584 + 1);
                format!("/ip4/127.0.0.1/udp/{udp_port}/quic-v1").parse()?
            };
        }
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
