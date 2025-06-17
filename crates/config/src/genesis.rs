//! Genesis information used when configuring a node.
use crate::TelcoinDirs;
use eyre::Context;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, ffi::OsStr, fs, path::Path, sync::Arc};
use tn_types::{
    address, test_genesis, verify_proof_of_possession_bls, Address, BlsPublicKey, BlsSignature,
    Committee, CommitteeBuilder, Genesis, GenesisAccount, Multiaddr, NetworkPublicKey, NodeP2pInfo,
    WorkerCache, WorkerIndex,
};
use tracing::{info, warn};

/// The validators directory used to create genesis
pub const GENESIS_VALIDATORS_DIR: &str = "validators";
/// Precompile info for genesis, read from current submodule commit
pub const DEPLOYMENTS_JSON: &str =
    include_str!("../../../tn-contracts/deployments/deployments.json");
pub const CONSENSUS_REGISTRY_JSON: &str =
    include_str!("../../../tn-contracts/artifacts/ConsensusRegistry.json");
pub const ERC1967PROXY_JSON: &str =
    include_str!("../../../tn-contracts/artifacts/ERC1967Proxy.json");
pub const ITS_CFG_YAML: &str =
    include_str!("../../../tn-contracts/deployments/genesis/its-config.yaml");
pub const GOVERNANCE_SAFE_ADDRESS: Address = address!("00000000000000000000000000000000000007a0");

/// The struct for starting a network at genesis.
pub struct NetworkGenesis {
    /// Execution data
    genesis: Genesis,
    /// Validator signatures
    validators: BTreeMap<BlsPublicKey, NodeInfo>,
}

impl NetworkGenesis {
    /// Create new version of [NetworkGenesis] using the adiri genesis [ChainSpec].
    pub fn new_for_test() -> Self {
        Self { genesis: test_genesis(), validators: Default::default() }
    }

    /// Return the current genesis.
    pub fn genesis(&self) -> &Genesis {
        &self.genesis
    }

    /// Add validator information to the genesis directory.
    ///
    /// Adding [ValidatorInfo] to the genesis directory allows other
    /// validators to discover peers using VCS (ie - github).
    #[cfg(test)]
    fn add_validator(&mut self, validator: NodeInfo) {
        self.validators.insert(*validator.public_key(), validator);
    }

    /// Update chain spec with executed values for genesis.
    pub fn update_genesis(&mut self, genesis: Genesis) {
        self.genesis = genesis;
    }

    /// Load a list of validators by reading files in a directory.
    fn load_validators_from_path<P>(
        telcoin_paths: &P,
    ) -> eyre::Result<Vec<(BlsPublicKey, NodeInfo)>>
    where
        P: TelcoinDirs,
    {
        let path = telcoin_paths.genesis_path();
        info!(target: "genesis::ceremony", ?path, "Loading Network Genesis");

        if !path.is_dir() {
            eyre::bail!("path must be a directory");
        }

        // Load validator information
        let mut validators = Vec::new();
        for entry in fs::read_dir(path.join(GENESIS_VALIDATORS_DIR))? {
            let entry = entry?;
            let path = entry.path();

            // Check if it's a file and has the .yaml extension and does not start with '.'
            if path.is_file()
                && path.file_name().and_then(OsStr::to_str).is_none_or(|s| !s.starts_with('.'))
            {
                let info_bytes = fs::read(&path)?;
                let validator: NodeInfo = serde_yaml::from_slice(&info_bytes)
                    .with_context(|| format!("validator failed to load from {}", path.display()))?;
                validators.push((validator.bls_public_key, validator));
            } else {
                warn!("skipping dir: {}\ndirs should not be in validators dir", path.display());
            }
        }
        Ok(validators)
    }

    /// Generate a [NetworkGenesis] by reading validators from files in a directory with genesis.
    pub fn new_from_path_and_genesis<P>(telcoin_paths: &P, genesis: Genesis) -> eyre::Result<Self>
    where
        P: TelcoinDirs,
    {
        // Load validator information
        let validators = Self::load_validators_from_path(telcoin_paths)?;
        let validators = BTreeMap::from_iter(validators);

        Ok(Self { genesis, validators })
    }

    /// Validate each validator:
    /// - verify proof of possession
    pub fn validate(&self) -> eyre::Result<()> {
        for (pubkey, validator) in self.validators.iter() {
            info!(target: "genesis::validate", "verifying validator: {}", pubkey);
            verify_proof_of_possession_bls(
                &validator.proof_of_possession,
                pubkey,
                &validator.execution_address,
            )?;
        }
        info!(target: "genesis::validate", "all validators valid for genesis");
        Ok(())
    }

    /// Create a [Committee] from the validators in [NetworkGenesis].
    pub fn create_committee(&self) -> eyre::Result<Committee> {
        let mut committee_builder = CommitteeBuilder::new(0);
        for (pubkey, validator) in self.validators.iter() {
            committee_builder.add_authority(
                *pubkey,
                1,
                validator.primary_network_address().clone(),
                validator.execution_address,
                validator.primary_network_key().clone(),
                "hostname".to_string(),
            );
        }
        Ok(committee_builder.build())
    }

    /// Create a [WorkerCache] from the validators in [NetworkGenesis].
    pub fn create_worker_cache(&self) -> eyre::Result<WorkerCache> {
        let workers = self
            .validators
            .iter()
            .map(|(pubkey, validator)| (*pubkey, validator.p2p_info.worker_index.clone()))
            .collect();

        let worker_cache = WorkerCache { epoch: 0, workers: Arc::new(workers) };

        Ok(worker_cache)
    }

    /// Return a reference to the validators.
    pub fn validators(&self) -> &BTreeMap<BlsPublicKey, NodeInfo> {
        &self.validators
    }

    /// Returns configurations for precompiles as genesis accounts
    /// Precompiles configs yamls are generated using foundry in `tn-contracts` submodule.
    ///
    /// Overrides InterchainTEL genesis balance to reflect genesis validator stake
    pub fn fetch_precompile_genesis_accounts(
        itel_address: Address,
        itel_balance: tn_types::U256,
    ) -> eyre::Result<Vec<(Address, GenesisAccount)>> {
        let yaml_content = ITS_CFG_YAML;
        let config: std::collections::HashMap<Address, GenesisAccount> =
            serde_yaml::from_str(yaml_content).expect("yaml parsing failure");
        let governance_balance = config
            .get(&GOVERNANCE_SAFE_ADDRESS)
            .expect("base fee recipient governance safe missing")
            .balance;
        let final_itel_balance = itel_balance - governance_balance;
        let mut accounts = Vec::new();
        for (address, precompile_config) in config {
            let bal = if address == itel_address {
                final_itel_balance
            } else {
                precompile_config.balance
            };
            let account = GenesisAccount::default()
                .with_nonce(precompile_config.nonce)
                .with_balance(bal)
                .with_code(precompile_config.code)
                .with_storage(precompile_config.storage);

            accounts.push((address, account));
        }

        Ok(accounts)
    }
}

/// Information needed for every validator:
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct NodeInfo {
    /// The name for the validator. The default value
    /// is the hashed value of the validator's
    /// execution address. The operator can overwrite
    /// this value since it is not used when writing to file.
    ///
    /// Keccak256(Address)
    pub name: String,
    /// [BlsPublicKey] to verify signature.
    pub bls_public_key: BlsPublicKey,
    /// Information for this validator's primary,
    /// including worker details.
    pub p2p_info: NodeP2pInfo,
    /// The address for suggested fee recipient.
    ///
    /// Validator rewards are sent to this address.
    /// Note, non-validators can also have an address but do not earn rewards (it is informational
    /// only).
    pub execution_address: Address,
    /// Proof
    pub proof_of_possession: BlsSignature,
}

impl NodeInfo {
    /// Return public key bytes.
    pub fn public_key(&self) -> &BlsPublicKey {
        &self.bls_public_key
    }

    /// Return the primary's public network key.
    pub fn primary_network_key(&self) -> &NetworkPublicKey {
        &self.p2p_info.network_key
    }

    /// Return the primary's network address.
    pub fn primary_network_address(&self) -> &Multiaddr {
        &self.p2p_info.network_address
    }

    /// Return a reference to the primary's [WorkerIndex].
    pub fn worker_index(&self) -> &WorkerIndex {
        self.p2p_info.worker_index()
    }
}

impl Default for NodeInfo {
    fn default() -> Self {
        let bls_public_key = BlsPublicKey::default();
        let name = format!("node-{}", bs58::encode(&bls_public_key.to_bytes()[0..8]).into_string());
        Self {
            name,
            bls_public_key,
            p2p_info: Default::default(),
            execution_address: Address::ZERO,
            proof_of_possession: BlsSignature::default(),
        }
    }
}

/// Fetch a file with a path relative to the CARGO MANIFEST dir and return it as a string.
///
/// Note this will ONLY work in tests or during builds, otherwise the required env variable
/// will not be set.
pub fn fetch_file_content_relative_to_manifest<P: AsRef<Path>>(relative_path: P) -> String {
    let mut file_path = std::path::PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR").expect("Missing CARGO_MANIFEST_DIR!"),
    );
    file_path.push(relative_path);

    fs::read_to_string(file_path).expect("unable to read file")
}

#[cfg(test)]
mod tests {
    use super::NetworkGenesis;
    use crate::NodeInfo;
    use rand::{rngs::StdRng, SeedableRng};
    use tn_types::{
        generate_proof_of_possession_bls, Address, BlsKeypair, Multiaddr, NetworkKeypair,
        NodeP2pInfo, WorkerIndex, WorkerInfo,
    };

    #[test]
    fn test_validate_genesis() {
        let mut network_genesis = NetworkGenesis::new_for_test();
        // create keys and information for validators
        for v in 0..4 {
            let bls_keypair = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            let network_keypair = NetworkKeypair::generate_ed25519();
            let address = Address::from_raw_public_key(&[0; 64]);
            let proof_of_possession =
                generate_proof_of_possession_bls(&bls_keypair, &address).unwrap();
            let primary_network_address = Multiaddr::empty();
            let worker_info = WorkerInfo::default();
            let worker_index = WorkerIndex(vec![worker_info]);
            let primary_info = NodeP2pInfo::new(
                network_keypair.public().clone().into(),
                primary_network_address,
                worker_index,
            );
            let name = format!("validator-{v}");
            // create validator
            let validator = NodeInfo {
                name,
                bls_public_key: *bls_keypair.public(),
                p2p_info: primary_info,
                execution_address: address,
                proof_of_possession,
            };
            // add validator
            network_genesis.add_validator(validator.clone());
        }
        // validate
        assert!(network_genesis.validate().is_ok())
    }

    #[test]
    fn test_validate_genesis_fails() {
        // this uses `adiri_genesis`
        let mut network_genesis = NetworkGenesis::new_for_test();
        // create keys and information for validators
        for v in 0..4 {
            let bls_keypair = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            let network_keypair = NetworkKeypair::generate_ed25519();
            let address = Address::from_raw_public_key(&[0; 64]);
            let wrong_address = Address::from_raw_public_key(&[1; 64]);

            // generate proof with wrong chain spec
            let proof_of_possession =
                generate_proof_of_possession_bls(&bls_keypair, &wrong_address).unwrap();
            let primary_network_address = Multiaddr::empty();
            let worker_info = WorkerInfo::default();
            let worker_index = WorkerIndex(vec![worker_info]);
            let primary_info = NodeP2pInfo::new(
                network_keypair.public().clone().into(),
                primary_network_address,
                worker_index,
            );
            let name = format!("validator-{v}");
            // create validator
            let validator = NodeInfo {
                name,
                bls_public_key: *bls_keypair.public(),
                p2p_info: primary_info,
                execution_address: address,
                proof_of_possession,
            };
            // add validator
            network_genesis.add_validator(validator.clone());
        }
        // validate should fail
        assert!(network_genesis.validate().is_err(), "proof of possession should fail")
    }
}
