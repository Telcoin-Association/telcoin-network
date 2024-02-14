//! reth data directories.
use reth::dirs::{ChainPath, XdgPath};
use reth_primitives::Chain;
use std::{fmt::Debug, path::PathBuf};
use tn_types::GENESIS_VALIDATORS_DIR;

/// The path to join for the directory that stores validator keys.
pub const VALIDATOR_KEYS_DIR: &str = "validator-keys";

/// Constructs a string to be used as a path for configuration and db paths.
pub fn config_path_prefix(chain: Chain) -> String {
    match chain {
        Chain::Named(name) => name.to_string(),
        Chain::Id(id) => id.to_string(),
    }
}

/// Returns the path to the telcoin network data directory.
///
/// Refer to [dirs_next::data_dir] for cross-platform behavior.
pub fn data_dir() -> Option<PathBuf> {
    dirs_next::data_dir().map(|root| root.join("telcoin-network"))
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

/// Telcoin Network specific directories.
pub trait TelcoinDirs {
    /// Return the path to `configuration` yaml file.
    fn node_config_path(&self) -> PathBuf;
    /// Return the path to the directory that holds
    /// private keys for the validator operating this node.
    fn validator_keys_path(&self) -> PathBuf;
    /// Return the path to `genesis` dir.
    fn genesis_path(&self) -> PathBuf;
    /// Return the path to the directory where individual and public validator information is
    /// collected for genesis.
    fn validator_info_path(&self) -> PathBuf;
    /// Return the path to the committee file.
    fn committee_path(&self) -> PathBuf;
    /// Return the path to the worker cache file.
    fn worker_cache_path(&self) -> PathBuf;
    /// Return the path to narwhal's node storage.
    fn narwhal_db_path(&self) -> PathBuf;
}

impl TelcoinDirs for ChainPath<DataDirPath> {
    fn node_config_path(&self) -> PathBuf {
        self.as_ref().join("telcoin-network.yaml")
    }

    fn validator_keys_path(&self) -> PathBuf {
        self.as_ref().join(VALIDATOR_KEYS_DIR)
    }

    fn validator_info_path(&self) -> PathBuf {
        self.as_ref().join("validator")
    }

    fn genesis_path(&self) -> PathBuf {
        self.as_ref().join("genesis")
    }

    fn committee_path(&self) -> PathBuf {
        self.genesis_path().join("committee.yaml")
    }

    fn worker_cache_path(&self) -> PathBuf {
        self.genesis_path().join("worker_cache.yaml")
    }

    fn narwhal_db_path(&self) -> PathBuf {
        self.as_ref().join("narwhal-db")
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
        let path = path.unwrap_or_chain_default(Chain::Id(2017));
        assert!(path.as_ref().ends_with("telcoin-network/2017"), "{:?}", path);

        let db_path = path.db_path();
        assert!(db_path.ends_with("telcoin-network/2017/db"), "{:?}", db_path);

        let path = MaybePlatformPath::<DataDirPath>::from_str("my/path/to/datadir").unwrap();
        let path = path.unwrap_or_chain_default(Chain::Id(2017));
        assert!(path.as_ref().ends_with("my/path/to/datadir"), "{:?}", path);
    }
}
