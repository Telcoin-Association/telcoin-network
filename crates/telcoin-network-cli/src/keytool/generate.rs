//! Generate subcommand

use crate::{args::clap_address_parser, keytool::pop::PopArgs};
use clap::{value_parser, Args, Subcommand};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, KeyConfig, NodeInfo, TelcoinDirs};
use tn_types::{get_available_udp_port, Address, BlsPublicKey, Multiaddr, Protocol};
use tracing::info;

/// Sign the proof of possession for `address` from `key_config` and write the
/// PoP-derived fields (`bls_public_key`, `proof_of_possession`, `execution_address`)
/// into `node_info`. Shared by `generate validator|observer` (fresh keys) and
/// `generate pop` (existing keys).
///
/// Errors if `node_info` already records a BLS public key that differs from the one
/// in `key_config`: that means the keys on disk do not belong to this `node-info`
/// (used by `generate pop` to refuse a wrong / mixed-up datadir). The node name is
/// intentionally left untouched here - fresh generation sets it in `update_keys`
/// (honoring `--name`), and `generate pop` preserves the existing name.
pub(crate) fn set_proof_of_possession(
    node_info: &mut NodeInfo,
    key_config: &KeyConfig,
    address: Address,
) -> eyre::Result<()> {
    let primary_public_key = key_config.primary_public_key();

    // When re-signing with existing keys (`generate pop`), the keys on disk must
    // match the BLS key recorded in node-info.yaml. A mismatch means the keys in
    // this datadir do not belong to this node-info (wrong datadir / mixed-up
    // files), so refuse rather than silently rewrite the recorded identity.
    // Skipped for fresh generation, where node_info still holds the default key.
    if node_info.bls_public_key != BlsPublicKey::default()
        && node_info.bls_public_key != primary_public_key
    {
        return Err(eyre::eyre!(
            "BLS key mismatch: node-info.yaml records public key {} but the keys \
             loaded from disk are {}; the keys in this datadir do not match \
             node-info.yaml (re-run from the correct datadir, or regenerate node-info.yaml)",
            node_info.bls_public_key,
            primary_public_key,
        ));
    }

    node_info.bls_public_key = primary_public_key;
    node_info.proof_of_possession = key_config.generate_proof_of_possession_bls(&address)?;
    node_info.execution_address = address;
    // NOTE: node_info.name is intentionally NOT set here. Fresh generation sets it
    // in `update_keys` (honoring `--name`); `generate pop` preserves the existing name.
    Ok(())
}

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
    /// Re-sign the proof of possession for a new execution address using the
    /// existing BLS keys (does not generate or overwrite any keys).
    #[command(name = "pop", alias = "proof-of-possession")]
    Pop(PopArgs),
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

    /// Optional human-readable name for this node.
    ///
    /// Recorded in node-info.yaml for logging / RPC metadata only; not used for
    /// consensus or peer identity. If unset, defaults to `node-` followed by the
    /// base58 encoding of the first 8 bytes of the BLS public key.
    #[arg(long = "name")]
    pub name: Option<String>,

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
        set_proof_of_possession(node_info, &key_config, self.address)?;

        // Fresh keys: use the operator-supplied name, else derive from the new BLS key.
        // (`generate pop` never reaches here, so it preserves the existing name.)
        node_info.name = self.name.clone().unwrap_or_else(|| {
            format!(
                "node-{}",
                bs58::encode(&node_info.bls_public_key.to_bytes()[0..8]).into_string()
            )
        });

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
        node_info.p2p_info.worker.network_key = network_publickey.clone();
        node_info.p2p_info.worker.network_address =
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

        info!(target: "tn::generate_keys", worker=?node_info.p2p_info.worker.network_address, "updating worker external network address");

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

        // execution address is set inside `set_proof_of_possession` (called by `update_keys`)
        Config::write_to_path(tn_datadir.node_info_path(), &node_info, ConfigFmt::YAML)?;

        Ok(())
    }
}
