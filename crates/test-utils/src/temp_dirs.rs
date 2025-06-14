use std::path::PathBuf;
use tempfile::{tempdir, TempDir};
use tn_config::TelcoinDirs;

#[derive(Debug)]
pub struct TelcoinTempDirs(TempDir);

impl Default for TelcoinTempDirs {
    fn default() -> Self {
        Self(tempdir().expect("tempdir created"))
    }
}

impl TelcoinDirs for TelcoinTempDirs {
    fn node_config_parameters_path(&self) -> PathBuf {
        self.0.as_ref().join("parameters.yaml")
    }

    fn node_keys_path(&self) -> PathBuf {
        self.0.path().join("node-keys")
    }

    fn node_info_path(&self) -> PathBuf {
        self.0.path().join("node-info.yaml")
    }

    fn genesis_path(&self) -> PathBuf {
        self.0.path().join("genesis")
    }

    fn committee_path(&self) -> PathBuf {
        self.genesis_path().join("committee.yaml")
    }

    fn worker_cache_path(&self) -> PathBuf {
        self.genesis_path().join("worker_cache.yaml")
    }

    fn genesis_file_path(&self) -> PathBuf {
        self.genesis_path().join("genesis.yaml")
    }

    fn consensus_db_path(&self) -> PathBuf {
        self.0.path().join("consensus-db")
    }

    fn reth_db_path(&self) -> PathBuf {
        self.0.path().join("db")
    }

    fn network_config_path(&self) -> PathBuf {
        self.0.path().join("network-config")
    }
}
