//! Telcoin Network data directories.
use reth::{
    args::DatadirArgs,
    dirs::{ChainPath, MaybePlatformPath, XdgPath},
};
use reth_chainspec::Chain;
use std::{fmt::Debug, ops::Deref, path::PathBuf, str::FromStr as _};
use tn_config::{TelcoinDirs, GENESIS_VALIDATORS_DIR};

/// The path to join for the directory that stores validator keys.
pub const VALIDATOR_KEYS_DIR: &str = "validator-keys";
/// The constant for default root directory.
/// This is a workaround for using TN default dir instead of "reth".
pub const DEFAULT_ROOT_DIR: &str = "telcoin-network";

/// Workaround for getting default DatadirArgs for reth node config.
pub fn default_datadir_args() -> DatadirArgs {
    // TODO: this is inefficient, but the only way to use "telcoin-network" as datadir instead of
    // "reth"
    DatadirArgs {
        datadir: MaybePlatformPath::from_str(DEFAULT_ROOT_DIR)
            .expect("default datadir args always work"),
        // default static path should resolve to: `DEFAULT_ROOT_DIR/<CHAIN_ID>/static_files`
        static_files_path: None,
    }
}

/// Constructs a string to be used as a path for configuration and db paths.
pub fn config_path_prefix(chain: Chain) -> String {
    chain.to_string()
}

/// Returns the path to the telcoin network data directory.
///
/// Refer to [dirs_next::data_dir] for cross-platform behavior.
pub fn data_dir() -> Option<PathBuf> {
    dirs_next::data_dir().map(|root| root.join(DEFAULT_ROOT_DIR))
}

/// Returns the path to the telcoin network database.
///
/// Refer to [dirs_next::data_dir] for cross-platform behavior.
pub fn database_path() -> Option<PathBuf> {
    data_dir().map(|root| root.join("db"))
}

/// Returns the path to the telcoin network configuration directory.
///
/// Refer to [dirs_next::config_dir] for cross-platform behavior.
pub fn config_dir() -> Option<PathBuf> {
    dirs_next::config_dir().map(|root| root.join("telcoin-network"))
}

/// Returns the path to the telcoin network cache directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
pub fn cache_dir() -> Option<PathBuf> {
    dirs_next::cache_dir().map(|root| root.join("telcoin-network"))
}

/// Returns the path to the telcoin network logs directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
pub fn logs_dir() -> Option<PathBuf> {
    cache_dir().map(|root| root.join("logs"))
}

/// Returns the path to the telcoin network genesis directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
pub fn genesis_dir() -> Option<PathBuf> {
    config_dir().map(|root| root.join("genesis"))
}

/// Returns the path to the telcoin network committee directory.
///
/// Child of `genesis_dir`.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
pub fn validators_dir() -> Option<PathBuf> {
    genesis_dir().map(|root| root.join(GENESIS_VALIDATORS_DIR))
}

#[derive(Clone, Debug)]
pub struct DataDirChainPath(ChainPath<DataDirPath>);

impl Deref for DataDirChainPath {
    type Target = ChainPath<DataDirPath>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<ChainPath<DataDirPath>> for DataDirChainPath {
    fn from(value: ChainPath<DataDirPath>) -> Self {
        Self(value)
    }
}

impl From<DataDirChainPath> for PathBuf {
    fn from(value: DataDirChainPath) -> Self {
        value.0.into()
    }
}

//impl TelcoinDirs for ChainPath<DataDirPath> {
impl TelcoinDirs for DataDirChainPath {
    fn node_config_path(&self) -> PathBuf {
        self.0.as_ref().join("telcoin-network.yaml")
    }

    fn validator_keys_path(&self) -> PathBuf {
        self.0.as_ref().join(VALIDATOR_KEYS_DIR)
    }

    fn validator_info_path(&self) -> PathBuf {
        self.0.as_ref().join("validator")
    }

    fn genesis_path(&self) -> PathBuf {
        self.0.as_ref().join("genesis")
    }

    fn committee_path(&self) -> PathBuf {
        self.genesis_path().join("committee.yaml")
    }

    fn worker_cache_path(&self) -> PathBuf {
        self.genesis_path().join("worker_cache.yaml")
    }

    fn genesis_file_path(&self) -> PathBuf {
        self.genesis_path().join("genesis.json")
    }

    fn consensus_db_path(&self) -> PathBuf {
        self.0.as_ref().join("consensus-db")
    }

    fn network_config_path(&self) -> PathBuf {
        self.0.as_ref().join("network-config")
    }
}

/// Returns the path to the telcoin network data dir.
///
/// The data dir should contain a subdirectory for each chain, and those chain directories will
/// include all information for that chain, such as the p2p secret.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct DataDirPath;

impl XdgPath for DataDirPath {
    fn resolve() -> Option<PathBuf> {
        data_dir()
    }
}

/// Returns the path to the telcoin network logs directory.
///
/// Refer to [dirs_next::cache_dir] for cross-platform behavior.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct LogsDir;

impl XdgPath for LogsDir {
    fn resolve() -> Option<PathBuf> {
        logs_dir()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth::dirs::MaybePlatformPath;
    use std::str::FromStr;

    #[test]
    fn test_maybe_data_dir_path() {
        let path = MaybePlatformPath::<DataDirPath>::default();
        let path = path.unwrap_or_chain_default(Chain::from_id(2017), default_datadir_args());
        assert!(
            path.as_ref().ends_with("telcoin-network/2017"),
            "actual default path is: {:?}",
            path
        );

        let db_path = path.db();
        assert!(db_path.ends_with("telcoin-network/2017/db"), "actual db path is: {:?}", db_path);

        let static_files_path = path.static_files();
        assert!(
            static_files_path.ends_with("telcoin-network/2017/static_files"),
            "actual static_files path is: {:?}",
            static_files_path
        );

        let path = MaybePlatformPath::<DataDirPath>::from_str("my/path/to/datadir").unwrap();
        let path = path.unwrap_or_chain_default(Chain::from_id(2017), default_datadir_args());
        assert!(path.as_ref().ends_with("my/path/to/datadir"), "{:?}", path);
    }
}
