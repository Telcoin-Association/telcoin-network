//! Trait for configurations to read and write to paths.

use eyre::{Context, ContextCompat};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind::NotFound, Read, Write},
    path::{Path, PathBuf},
};
use tracing::info;

/// The serialization format for the config.
#[derive(PartialEq, Debug)]
pub enum ConfigFmt {
    /// Serialize using YAML.
    YAML,
    /// Serialize using JSON.
    JSON,
}

impl ConfigFmt {
    /// Helper method to identify type.
    pub fn is_json(&self) -> bool {
        *self == Self::JSON
    }
}

/// Based on `confy` crate.
/// Problem: reth uses TOML and TN uses yaml, so must replicate the necessary code as a trait.
/// Can't use confy crate with two different `cfg`s.
pub trait ConfigTrait {
    /// Load an application configuration from a specified path.
    ///
    /// A new configuration file is created with default values if none
    /// exists.
    fn load_from_path<T: Serialize + DeserializeOwned + Default>(
        path: impl AsRef<Path>,
        fmt: ConfigFmt,
    ) -> eyre::Result<T> {
        info!(target: "tn::config", path = ?path.as_ref(), "Loading configuration");
        match File::open(&path) {
            Ok(mut file) => {
                let mut cfg_string = String::new();
                file.read_to_string(&mut cfg_string)?;

                // return deserialized data in specified format
                if fmt.is_json() {
                    serde_json::from_str(&cfg_string).with_context(|| "bad json data")
                } else {
                    serde_yaml::from_str(&cfg_string).with_context(|| "bad yaml data")
                }
            }
            Err(ref e) if e.kind() == NotFound => {
                if let Some(parent) = path.as_ref().parent() {
                    fs::create_dir_all(parent).with_context(|| "Directory creation failed")?;
                }
                let cfg = T::default();
                Self::store_path(path, &cfg, fmt)?;
                Ok(cfg)
            }
            Err(e) => eyre::bail!("Failed to open file: {e}"),
        }
    }

    /// Save changes made to a configuration object at a specified path
    ///
    /// This is an alternate version of [`store`] that allows the specification of
    /// an arbitrary path instead of a system one.  For more information on errors
    /// and behavior, see [`store`]'s documentation.
    ///
    /// [`store`]: fn.store.html
    fn store_path<T: Serialize>(
        path: impl AsRef<Path>,
        cfg: T,
        fmt: ConfigFmt,
    ) -> eyre::Result<()> {
        let path = path.as_ref();
        let config_dir =
            path.parent().with_context(|| format!("{:?} is a root or prefix", path))?;
        fs::create_dir_all(config_dir)
            .with_context(|| "directory creation failed while storing")?;

        // serialize in specified fmt
        let s = if fmt.is_json() {
            serde_json::to_string(&cfg).with_context(|| "Failed to serialize config to json")?
        } else {
            serde_yaml::to_string(&cfg).with_context(|| "Failed to serialize config to yaml")?
        };

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .with_context(|| "Failed to open configuration file using OpenOptions")?;

        f.write_all(s.as_bytes()).with_context(|| "Failed to write configuration file")?;
        Ok(())
    }
}

/// Telcoin Network specific directories.
pub trait TelcoinDirs: std::fmt::Debug + Send + Sync + 'static {
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
    /// Return the path to the chain spec file.
    fn genesis_file_path(&self) -> PathBuf;
    /// Return the path to narwhal's node storage.
    fn narwhal_db_path(&self) -> PathBuf;
}

impl TelcoinDirs for PathBuf {
    fn node_config_path(&self) -> PathBuf {
        self.join("telcoin-network.yaml")
    }

    fn validator_keys_path(&self) -> PathBuf {
        self.join("validator-keys")
    }

    fn validator_info_path(&self) -> PathBuf {
        self.join("validator")
    }

    fn genesis_path(&self) -> PathBuf {
        self.join("genesis")
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

    fn narwhal_db_path(&self) -> PathBuf {
        self.join("narwhal-db")
    }
}

impl TelcoinDirs for Path {
    fn node_config_path(&self) -> PathBuf {
        self.join("telcoin-network.yaml")
    }

    fn validator_keys_path(&self) -> PathBuf {
        self.join("validator-keys")
    }

    fn validator_info_path(&self) -> PathBuf {
        self.join("validator")
    }

    fn genesis_path(&self) -> PathBuf {
        self.join("genesis")
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

    fn narwhal_db_path(&self) -> PathBuf {
        self.join("narwhal-db")
    }
}

impl TelcoinDirs for &'static Path {
    fn node_config_path(&self) -> PathBuf {
        self.join("telcoin-network.yaml")
    }

    fn validator_keys_path(&self) -> PathBuf {
        self.join("validator-keys")
    }

    fn validator_info_path(&self) -> PathBuf {
        self.join("validator")
    }

    fn genesis_path(&self) -> PathBuf {
        self.join("genesis")
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

    fn narwhal_db_path(&self) -> PathBuf {
        self.join("narwhal-db")
    }
}
