//! Genesis information used when configuring a node.
use crate::{Config, ConfigFmt, ConfigTrait, TelcoinDirs};
use eyre::Context;
use reth_chainspec::ChainSpec;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fmt::{Display, Formatter},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use tn_types::{
    adiri_genesis, hex, keccak256, verify_proof_of_possession_bls, Address, BlsPublicKey,
    BlsSignature, Committee, CommitteeBuilder, Epoch, FromHex as _, GenesisAccount, Intent,
    IntentMessage, Multiaddr, NetworkPublicKey, PrimaryInfo, ProtocolSignature, Signer,
    WorkerCache, WorkerIndex, B256,
};
use tracing::{info, warn};
/// The validators directory used to create genesis.
pub const GENESIS_VALIDATORS_DIR: &str = "validators";

const fn from_utf8(bytes: &[u8]) -> &str {
    match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => panic!("bytes not valid utf8!"),
    }
}

// We need to embed these files as defaults so we can run the binary vs tests.
const CONSENSUS_REGISTRY_STORAGE: &str =
    from_utf8(include_bytes!("../../../tn-contracts/deployments/consensus-registry-storage.yaml"));
const CONSENSUS_REGISTRY: &str =
    from_utf8(include_bytes!("../../../tn-contracts/artifacts/ConsensusRegistry.json"));
const ERC1967_PROXY: &str =
    from_utf8(include_bytes!("../../../tn-contracts/artifacts/ERC1967Proxy.json"));

/// The struct for starting a network at genesis.
pub struct NetworkGenesis {
    // /// The committee
    // committee: Committee,
    /// Execution data
    chain: ChainSpec,
    /// Validator signatures
    validators: BTreeMap<BlsPublicKey, ValidatorInfo>,
    // // Validator signatures over checkpoint
    // signatures: BTreeMap<BlsPublicKey, ValidatorSignatureInfo>,
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

    /// Read output file from Solidity GenerateConsensusRegistryStorage utility
    /// to fetch storage configuration for ConsensusRegistry at genesis
    pub fn construct_registry_genesis_accounts(&mut self, registry_cfg_path: Option<PathBuf>) {
        let registry_storage_yaml = match registry_cfg_path {
            // Note, this is called outside of tests when creating a committee vs starting a node.
            // A panic here when given an invalid file is probably fine.
            Some(path) => fs::read_to_string(&path).unwrap_or_else(|_| {
                panic!(
                    "unable to read file supplied consensus registry file: {}",
                    path.to_string_lossy()
                )
            }),
            None => CONSENSUS_REGISTRY_STORAGE.to_string(),
        };
        let registry_storage_cfg: BTreeMap<String, String> =
            serde_yaml::from_str(&registry_storage_yaml).expect("yaml parsing failure");
        let mut registry_storage_cfg: BTreeMap<B256, B256> = registry_storage_cfg
            .into_iter()
            .map(|(k, v)| (k.parse().expect("Invalid key"), v.parse().expect("Invalid val")))
            .collect();

        let pubkey_flags = PubkeyFlags::new(self.validators.len());
        // iterate over BTreeMap to conditionally overwrite flagged values with pubkeys that are now
        // known
        let validator_info: Vec<_> = self.validators.values().cloned().collect();
        for val in registry_storage_cfg.values_mut() {
            PubkeyFlags::overwrite_if_flag(val, &pubkey_flags, &validator_info);
        }

        let registry_impl = Address::random();
        let registry_standard_json = CONSENSUS_REGISTRY;
        let registry_contract: ContractStandardJson =
            serde_json::from_str(registry_standard_json).expect("json parsing failure");
        let registry_bytecode = hex::decode(registry_contract.deployed_bytecode.object)
            .expect("invalid bytecode hexstring");
        let registry_proxy = Address::from_hex("0x07e17e17e17e17e17e17e17e17e17e17e17e17e1")
            .expect("invalid hex address");
        let proxy_standard_json = ERC1967_PROXY;
        let proxy_contract: ContractStandardJson =
            serde_json::from_str(proxy_standard_json).expect("json parsing failure");
        let proxy_bytecode = hex::decode(proxy_contract.deployed_bytecode.object)
            .expect("invalid bytecode hexstring");
        let registry_genesis_accounts = vec![
            (registry_impl, GenesisAccount::default().with_code(Some(registry_bytecode.into()))),
            (
                registry_proxy,
                GenesisAccount::default()
                    .with_code(Some(proxy_bytecode.into()))
                    .with_storage(Some(registry_storage_cfg)),
            ),
        ];

        // update chain with new genesis
        self.chain = self.chain.genesis.clone().extend_accounts(registry_genesis_accounts).into();
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
                // TODO: checking this is probably more trouble than it's worth
                // && path.extension().and_then(OsStr::to_str) == Some("yaml")

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
    ///
    /// TODO: addition validation?
    ///     - validator name isn't default
    ///     - ???
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
}

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

/// TODO: decide if this is needed or not.
///
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

struct PubkeyFlags {
    bls_a: B256,
    bls_b: B256,
    bls_c: B256,
    ed25519: B256,
    ecdsa: B256,
}

impl PubkeyFlags {
    /// Calculate flags used by Foundry util to label storage values for overwriting
    fn new(num_validators: usize) -> Vec<PubkeyFlags> {
        (1..=num_validators)
            .map(|i| PubkeyFlags {
                bls_a: keccak256(format!("VALIDATOR_{}_BLS_A", i)),
                bls_b: keccak256(format!("VALIDATOR_{}_BLS_B", i)),
                bls_c: keccak256(format!("VALIDATOR_{}_BLS_C", i)),
                ed25519: keccak256(format!("VALIDATOR_{}_ED25519", i)),
                ecdsa: keccak256(format!("VALIDATOR_{}_ECDSA", i)),
            })
            .collect()
    }

    /// Conditionally overwrites flagged placeholder values with the intended pubkey within
    /// `validator_infos` This only occurs if `val` is found to match a collision-resistant hash
    /// within `flags`
    fn overwrite_if_flag(val: &mut B256, flags: &[PubkeyFlags], validator_infos: &[ValidatorInfo]) {
        for (i, flag) in flags.iter().enumerate() {
            if *val == flag.bls_a {
                // overwrite using first 32 bytes of bls pubkey
                let bls_first_word = &validator_infos[i].bls_public_key.to_bytes()[0..32];
                val.copy_from_slice(bls_first_word);
                return;
            } else if *val == flag.bls_b {
                // overwrite using middle 32 bytes of bls pubkey
                let bls_middle_word = &validator_infos[i].bls_public_key.to_bytes()[32..64];
                val.copy_from_slice(bls_middle_word);
                return;
            } else if *val == flag.bls_c {
                // overwrite using last 32 bytes of bls pubkey
                let bls_last_word = &validator_infos[i].bls_public_key.to_bytes()[64..96];
                val.copy_from_slice(bls_last_word);
                return;
            } else if *val == flag.ecdsa {
                *val = validator_infos[i].execution_address.into_word();
                return;
            } else if *val == flag.ed25519 {
                let key: libp2p::identity::PublicKey =
                    validator_infos[i].primary_network_key().clone().into();
                let key = key.try_into_ed25519().expect("ed25519 key");
                *val = B256::from_slice(&key.to_bytes());
                return;
            }
        }
    }
}

#[derive(Deserialize)]
pub struct BytecodeObject {
    pub object: String,
}

#[derive(Deserialize)]
pub struct ContractStandardJson {
    pub bytecode: BytecodeObject,
    #[serde(rename = "deployedBytecode")]
    pub deployed_bytecode: BytecodeObject,
}

/// Fetch a file with a path relative to the CARGO MANIFEST dir and return it as a string.
///
/// Note this will ONLY work in tests or during builds, otherwise the required env variable
/// will not be set.
pub fn test_fetch_file_content_relative_to_manifest(relative_path: PathBuf) -> String {
    let mut file_path = std::path::PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR").expect("Missing CARGO_MANIFEST_DIR!"),
    );
    file_path.push(relative_path);

    fs::read_to_string(file_path).expect("unable to read file")
}

#[cfg(test)]
mod tests {
    use super::NetworkGenesis;
    use crate::{
        genesis::ContractStandardJson, test_fetch_file_content_relative_to_manifest, TelcoinDirs,
        ValidatorInfo,
    };
    use rand::{rngs::StdRng, SeedableRng};
    use std::collections::BTreeMap;
    use tempfile::tempdir;
    use tn_types::{
        adiri_chain_spec, generate_proof_of_possession_bls, hex, Address, BlsKeypair, FromHex as _,
        Multiaddr, NetworkKeypair, PrimaryInfo, WorkerIndex, WorkerInfo,
    };

    #[test]
    fn test_write_and_read_network_genesis() {
        let mut network_genesis = NetworkGenesis::new();
        let tmp_dir = tempdir().unwrap();
        // Keep tmp_dir around so the temp dir is not deleted yet.
        let paths = tmp_dir.path().to_path_buf();
        // create keys and information for validator
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
        let name = "validator1".to_string();
        // create validator
        let validator = ValidatorInfo::new(
            name,
            bls_keypair.public().clone(),
            primary_info,
            address,
            proof_of_possession,
        );
        // add validator
        network_genesis.add_validator(validator.clone());
        // save to file
        network_genesis.write_to_path(paths.genesis_path()).unwrap();
        // load network genesis
        let mut loaded_network_genesis =
            NetworkGenesis::load_from_path(&paths).expect("unable to load network genesis");

        loaded_network_genesis.construct_registry_genesis_accounts(Some(
            "../../tn-contracts/deployments/consensus-registry-storage.yaml".into(),
        ));

        let loaded_validator =
            loaded_network_genesis.validators.get(validator.public_key()).unwrap();
        assert_eq!(&validator, loaded_validator);

        let expected_registry_addr =
            Address::from_hex("0x07e17e17e17e17e17e17e17e17e17e17e17e17e1")
                .expect("failed to parse address");
        let proxy_standard_json = test_fetch_file_content_relative_to_manifest(
            "../../tn-contracts/artifacts/ERC1967Proxy.json".into(),
        );
        let proxy_contract: ContractStandardJson =
            serde_json::from_str(&proxy_standard_json).expect("failed to parse json");
        let proxy_bytecode = hex::decode(proxy_contract.deployed_bytecode.object)
            .expect("invalid bytecode hexstring");
        match loaded_network_genesis.chain.genesis.alloc.get(&expected_registry_addr) {
            Some(account) => {
                // check registry bytecode matches expected value
                match &account.code {
                    Some(code) => {
                        assert_eq!(***code, *proxy_bytecode, "wrong registry bytecode")
                    }
                    None => panic!("registry code not set"),
                }

                // check registry storage was set and is not `None`
                match &account.storage {
                    Some(storage) => assert!(!storage.is_empty()),
                    None => panic!("registry storage not set"),
                }
            }
            None => panic!("expected registry address not found in genesis"),
        }
    }

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
            let name = format!("validator-{}", v);
            // create validator
            let validator = ValidatorInfo::new(
                name,
                bls_keypair.public().clone(),
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
            let name = format!("validator-{}", v);
            // create validator
            let validator = ValidatorInfo::new(
                name,
                bls_keypair.public().clone(),
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
