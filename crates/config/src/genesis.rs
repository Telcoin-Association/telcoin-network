//! Genesis information used when configuring a node.
use crate::{Config, ConfigFmt, ConfigTrait, TelcoinDirs};
use eyre::{Context, OptionExt};
use reth_chainspec::ChainSpec;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fmt::{Display, Formatter},
    fs,
    path::Path,
    sync::Arc,
};
use tn_types::{
    adiri_genesis, keccak256, verify_proof_of_possession_bls, Address, BlsPublicKey, BlsSignature,
    Committee, CommitteeBuilder, Epoch, Genesis, GenesisAccount, Intent, IntentMessage, Multiaddr,
    NetworkPublicKey, PrimaryInfo, ProtocolSignature, Signer, WorkerCache, WorkerIndex,
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

/// Wrapper for fetching JSON keys as object or values
pub enum QueryResult {
    Map(Map<String, Value>),
    Single(Value),
}

/// The struct for starting a network at genesis.
pub struct NetworkGenesis {
    /// Execution data
    chain: ChainSpec,
    /// Validator signatures
    validators: BTreeMap<BlsPublicKey, ValidatorInfo>,
}

impl Default for NetworkGenesis {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkGenesis {
    /// Create new version of [NetworkGenesis] using the adiri genesis [ChainSpec].
    pub fn new() -> Self {
        Self { chain: adiri_genesis().into(), validators: Default::default() }
    }

    /// Return the current genesis.
    pub fn genesis(&self) -> &Genesis {
        &self.chain.genesis
    }

    /// Create new version of [NetworkGenesis] using the adiri genesis [ChainSpec].
    pub fn with_chain_spec(chain: ChainSpec) -> Self {
        Self { chain, validators: Default::default() }
    }

    /// Add validator information to the genesis directory.
    ///
    /// Adding [ValidatorInfo] to the genesis directory allows other
    /// validators to discover peers using VCS (ie - github).
    pub fn add_validator(&mut self, validator: ValidatorInfo) {
        self.validators.insert(*validator.public_key(), validator);
    }

    /// Update chain spec with executed values for genesis.
    pub fn update_chain(&mut self, new_chain: ChainSpec) {
        self.chain = new_chain;
    }

    /// Generate a [NetworkGenesis] by reading files in a directory.
    pub fn load_from_path<P>(telcoin_paths: &P) -> eyre::Result<Self>
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
                let validator: ValidatorInfo = serde_yaml::from_slice(&info_bytes)
                    .with_context(|| format!("validator failed to load from {}", path.display()))?;
                validators.push((validator.bls_public_key, validator));
            } else {
                warn!("skipping dir: {}\ndirs should not be in validators dir", path.display());
            }
        }

        // prevent mutable key type
        // The keys being used here seem to trip this because they contain a OnceCell but do not
        // appear to be actually mutable.  So it should be safe to ignore this clippy warning...
        #[allow(clippy::mutable_key_type)]
        let validators = BTreeMap::from_iter(validators);

        let tn_config: Config =
            Config::load_from_path(telcoin_paths.node_config_path(), ConfigFmt::YAML)?;

        let network_genesis = Self {
            chain: tn_config.chain_spec(),
            validators,
            // signatures,
        };

        Ok(network_genesis)
    }

    /// Write [NetworkGenesis] to path (genesis directory) as individual validator files.
    pub fn write_to_path<P>(self, path: P) -> eyre::Result<()>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        info!(target: "genesis::ceremony", ?path, "Writing Network Genesis to dir");

        fs::create_dir_all(path)?;

        // Write validator infos
        let committee_dir = path.join(GENESIS_VALIDATORS_DIR);
        fs::create_dir_all(&committee_dir)?;

        for (pubkey, validator) in self.validators {
            let validator_info = serde_yaml::to_string(&validator)?;
            let file_name = format!("{}.yaml", keccak256(pubkey));
            fs::write(committee_dir.join(file_name), validator_info)?;
        }

        Ok(())
    }

    /// Return a reference to `Self::chain`.
    pub fn chain_info(&self) -> &ChainSpec {
        &self.chain
    }

    /// Validate each validator:
    /// - verify proof of possession
    pub fn validate(&self) -> eyre::Result<()> {
        for (pubkey, validator) in self.validators.iter() {
            info!(target: "genesis::validate", "verifying validator: {}", pubkey);
            verify_proof_of_possession_bls(&validator.proof_of_possession, pubkey, &self.chain)?;
        }
        info!(target: "genesis::validate", "all validators valid for genesis");
        Ok(())
    }

    /// Create a [Committee] from the validators in [NetworkGenesis].
    pub fn create_committee(&self) -> eyre::Result<Committee> {
        // disable epochs for now
        let epoch_boundary = u64::MAX;
        let mut committee_builder = CommitteeBuilder::new(0, epoch_boundary);
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
        // The keys being used here seem to trip this because they contain a OnceCell but do not
        // appear to be actually mutable.  So it should be safe to ignore this clippy warning...
        #[allow(clippy::mutable_key_type)]
        let workers = self
            .validators
            .iter()
            .map(|(pubkey, validator)| (*pubkey, validator.primary_info.worker_index.clone()))
            .collect();

        let worker_cache = WorkerCache { epoch: 0, workers: Arc::new(workers) };

        Ok(worker_cache)
    }

    /// Return a reference to the validators.
    pub fn validators(&self) -> &BTreeMap<BlsPublicKey, ValidatorInfo> {
        &self.validators
    }

    /// Returns configurations for precompiles as genesis accounts
    /// Precompiles configs yamls are generated using foundry in tn-Contracts
    pub fn fetch_precompile_genesis_accounts() -> eyre::Result<Vec<(Address, GenesisAccount)>> {
        let yaml_content = ITS_CFG_YAML;
        let config: std::collections::HashMap<Address, GenesisAccount> =
            serde_yaml::from_str(yaml_content).expect("yaml parsing failure");
        let mut accounts = Vec::new();
        for (address, precompile_config) in config {
            let account = GenesisAccount::default()
                .with_nonce(precompile_config.nonce)
                .with_balance(precompile_config.balance)
                .with_code(precompile_config.code)
                .with_storage(precompile_config.storage);

            accounts.push((address, account));
        }

        Ok(accounts)
    }

    /// Fetches json info from the given string
    ///
    /// If a query is specified, return the corresponding nested object.
    /// Otherwise return the entire JSON
    /// With a generic this could be adjused to handle YAML also
    pub fn fetch_from_json_str(
        json_string: &str,
        query: Option<&str>,
    ) -> eyre::Result<QueryResult> {
        let json: Value = serde_json::from_str(json_string).expect("json string malformed");
        let result = match query {
            Some(path) => {
                let keys: Vec<&str> = path.split('.').collect();
                let mut current_value = &json;
                for &key in &keys {
                    current_value = current_value.get(key).ok_or_eyre("query key not found")?;
                }

                match current_value {
                    // return objects directly
                    Value::Object(map) => QueryResult::Map(map.clone()),
                    // return single entries wrapped in a Map
                    _ => QueryResult::Single(current_value.clone()),
                }
            }
            None => {
                let map = json.as_object().cloned().ok_or_eyre("json string malformed")?;
                QueryResult::Map(map)
            }
        };

        Ok(result)
    }
}

// deserialize into HashMap<Account, GenesisAccount>

/// information needed for every validator:
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct ValidatorInfo {
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
    pub primary_info: PrimaryInfo,
    /// The address for suggested fee recipient.
    ///
    /// Validator rewards are sent to this address.
    pub execution_address: Address,
    /// Proof
    pub proof_of_possession: BlsSignature,
}

impl ValidatorInfo {
    /// Create a new instance of [ValidatorInfo] using the provided data.
    pub fn new(
        name: String,
        bls_public_key: BlsPublicKey,
        primary_info: PrimaryInfo,
        execution_address: Address,
        proof_of_possession: BlsSignature,
    ) -> Self {
        Self { name, bls_public_key, primary_info, execution_address, proof_of_possession }
    }

    /// Return public key bytes.
    pub fn public_key(&self) -> &BlsPublicKey {
        &self.bls_public_key
    }

    /// Return the primary's public network key.
    pub fn primary_network_key(&self) -> &NetworkPublicKey {
        &self.primary_info.network_key
    }

    /// Return the primary's network address.
    pub fn primary_network_address(&self) -> &Multiaddr {
        &self.primary_info.network_address
    }

    /// Return a reference to the primary's [WorkerIndex].
    pub fn worker_index(&self) -> &WorkerIndex {
        self.primary_info.worker_index()
    }
}

impl Default for ValidatorInfo {
    fn default() -> Self {
        Self {
            name: "DEFAULT".to_string(),
            bls_public_key: BlsPublicKey::default(),
            primary_info: Default::default(),
            execution_address: Address::ZERO,
            proof_of_possession: BlsSignature::default(),
        }
    }
}

/// If using aggregate signatures for NetworkGenesis over chainspec.
#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
pub struct ValidatorSignatureInfo {
    pub epoch: Epoch,
    pub authority: BlsPublicKey,
    pub signature: BlsSignature,
}

impl ValidatorSignatureInfo {
    pub fn new<T>(
        epoch: Epoch,
        value: &T,
        intent: Intent,
        authority: BlsPublicKey,
        secret: &dyn Signer,
    ) -> Self
    where
        T: Serialize,
    {
        Self {
            epoch,
            authority,
            signature: BlsSignature::new_secure(&IntentMessage::new(intent, value), secret),
        }
    }
}

impl Display for ValidatorSignatureInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AuthoritySignatureInfo {{ epoch: {:?}, authority: {} }}",
            self.epoch, self.authority,
        )
    }
}

impl PartialEq for ValidatorSignatureInfo {
    fn eq(&self, other: &Self) -> bool {
        // Do not compare the signature. It's possible to have multiple
        // valid signatures for the same epoch and authority.
        self.epoch == other.epoch && self.authority == other.authority
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
    use crate::ValidatorInfo;
    use rand::{rngs::StdRng, SeedableRng};
    use std::collections::BTreeMap;
    use tn_types::{
        adiri_chain_spec, generate_proof_of_possession_bls, Address, BlsKeypair, Multiaddr,
        NetworkKeypair, PrimaryInfo, WorkerIndex, WorkerInfo,
    };

    #[test]
    fn test_validate_genesis() {
        let mut network_genesis = NetworkGenesis::new();
        // create keys and information for validators
        for v in 0..4 {
            let bls_keypair = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            let network_keypair = NetworkKeypair::generate_ed25519();
            let address = Address::from_raw_public_key(&[0; 64]);
            let proof_of_possession =
                generate_proof_of_possession_bls(&bls_keypair, &adiri_chain_spec()).unwrap();
            let primary_network_address = Multiaddr::empty();
            let worker_info = WorkerInfo::default();
            let worker_index = WorkerIndex(BTreeMap::from([(0, worker_info)]));
            let primary_info = PrimaryInfo::new(
                network_keypair.public().clone().into(),
                primary_network_address,
                network_keypair.public().clone().into(),
                worker_index,
            );
            let name = format!("validator-{v}");
            // create validator
            let validator = ValidatorInfo::new(
                name,
                *bls_keypair.public(),
                primary_info,
                address,
                proof_of_possession,
            );
            // add validator
            network_genesis.add_validator(validator.clone());
        }
        // validate
        assert!(network_genesis.validate().is_ok())
    }

    #[test]
    fn test_validate_genesis_fails() {
        // this uses `adiri_genesis`
        let mut network_genesis = NetworkGenesis::new();
        // create keys and information for validators
        for v in 0..4 {
            let bls_keypair = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            let network_keypair = NetworkKeypair::generate_ed25519();
            let address = Address::from_raw_public_key(&[0; 64]);

            // create wrong chain spec
            let mut wrong_chain = adiri_chain_spec();
            wrong_chain.genesis.timestamp = 0;

            // generate proof with wrong chain spec
            let proof_of_possession =
                generate_proof_of_possession_bls(&bls_keypair, &wrong_chain).unwrap();
            let primary_network_address = Multiaddr::empty();
            let worker_info = WorkerInfo::default();
            let worker_index = WorkerIndex(BTreeMap::from([(0, worker_info)]));
            let primary_info = PrimaryInfo::new(
                network_keypair.public().clone().into(),
                primary_network_address,
                network_keypair.public().clone().into(),
                worker_index,
            );
            let name = format!("validator-{v}");
            // create validator
            let validator = ValidatorInfo::new(
                name,
                *bls_keypair.public(),
                primary_info,
                address,
                proof_of_possession,
            );
            // add validator
            network_genesis.add_validator(validator.clone());
        }
        // validate should fail
        assert!(network_genesis.validate().is_err(), "proof of possession should fail")
    }
}
