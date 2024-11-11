//! Genesis ceremony command.

use clap::Args;
use eyre::Context;

use reth::dirs::MaybePlatformPath;
use reth_chainspec::ChainSpec;
use std::{path::PathBuf, sync::Arc};
use tn_config::{Config, ConfigFmt, ConfigTrait, NetworkGenesis, TelcoinDirs as _};
use tn_node::dirs::{default_datadir_args, DataDirChainPath, DataDirPath};

use crate::args::clap_genesis_parser;
use tracing::info;

/// Add the validator to the node
#[derive(Debug, Clone, Args)]
pub struct AddValidator {
    /// The path to the data dir for all telcoin-network files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/telcoin-network/` or `$HOME/.local/share/telcoin-network/`
    /// - Windows: `{FOLDERID_RoamingAppData}/telcoin-network/`
    /// - macOS: `$HOME/Library/Application Support/telcoin-network/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t)]
    pub datadir: MaybePlatformPath<DataDirPath>,

    /// The path to the configuration file to use.
    #[arg(long, value_name = "CONFIG_FILE", verbatim_doc_comment)]
    pub config: Option<PathBuf>,

    /// The path to the genesis directory.
    ///
    /// The GENESIS_DIRECTORY contains more directories:
    /// - committee
    /// - todo
    ///
    /// Validators add their information to the directory using VCS like
    /// github. Using individual files prevents merge conflicts.
    #[arg(long, value_name = "GENESIS_DIRECTORY", verbatim_doc_comment)]
    pub genesis: Option<PathBuf>,

    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    ///
    /// Defaults to the custom
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "adiri",
        // default_value_if("dev", "true", "dev"),
        value_parser = clap_genesis_parser,
        required = false,
    )]
    pub chain: Arc<ChainSpec>,
}

impl AddValidator {
    /// Execute `AddValidator` command
    ///
    /// Process:
    /// - load [NetworkGenesis] from genesis dir
    /// - TODO: load validator information keys (assume safe for now)
    /// - add validator to network genesis
    /// - save NetworkGenesis as files in genesis dir again
    pub async fn execute(&self) -> eyre::Result<()> {
        info!(target: "genesis::add-validator", "Adding validator to committee");

        // add network name to data dir
        let data_dir: DataDirChainPath =
            self.datadir.unwrap_or_chain_default(self.chain.chain, default_datadir_args()).into();
        let config_path = self.config.clone().unwrap_or(data_dir.node_config_path());

        let config = self.load_config(config_path.clone())?;

        info!(target: "genesis::ceremony", path = ?config_path, "Configuration loaded");

        // load config file
        // load committee file
        // errors here?
        //
        // call function to "get_validator_key()"
        //  - provides flexibility down the road how we want to do this
        //  - ENV VAR at first, then EIP-2335
        //  - note: ssh-keygen / ssh-agent already do this for ed25519 keys!
        //
        // adding validator needs EL public key
        //
        // steps:
        // read validator info from config / source of info
        //
        // ensure:
        // - worker index is length 1
        // - no defaults are used
        //
        // load network_genesis from genesis dir
        //
        // add validator info to network genesis
        //
        // write network genesis to file

        let genesis_path = data_dir.genesis_path();

        // TODO: is this the right approach?
        let validator_info = config.validator_info.clone();

        let mut network_genesis = NetworkGenesis::load_from_path(&data_dir)?;

        network_genesis.add_validator(validator_info);

        network_genesis.write_to_path(genesis_path)
    }

    /// Loads the reth config with the given datadir root
    fn load_config(&self, config_path: PathBuf) -> eyre::Result<Config> {
        Config::load_from_path::<Config>(config_path.clone(), ConfigFmt::YAML)
            .wrap_err_with(|| format!("Could not load config file {:?}", config_path))
    }

    // /// Get bls key for validator
    // ///
    // /// TODO: this is not secure and only meant as a placeholder until a more secure
    // /// approach to key management is implemented. EIP-2335 & ssh-agent
    // fn get_bls_key(&self, path: &PathBuf) -> eyre::Result<BlsKeypair> {
    //     self.read_authority_keypair_from_file::<BlsKeypair, _>(path)
    // }

    // /// Get network keys for validator:
    // /// - 1 primary
    // /// - 1 worker
    // ///
    // /// TODO: this is not secure and only meant as a placeholder until a more secure
    // /// approach to key management is implemented. EIP-2335 & ssh-agent
    // fn get_network_key(&self, path: &PathBuf) -> eyre::Result<NetworkKeypair> {
    //     self.read_authority_keypair_from_file::<NetworkKeypair, _>(path)
    // }

    // /// Read from file as Base64 encoded `privkey` and return a KeyPair.
    // pub fn read_authority_keypair_from_file<KP, P>(&self, path: P) -> eyre::Result<KP>
    // where
    //     KP: KeyPairTrait,
    //     P: AsRef<Path>,
    // {
    //     let contents = std::fs::read_to_string(path)?;
    //     KP::decode_base64(contents.as_str().trim()).map_err(|e| eyre::eyre!(e))
    // }
}
