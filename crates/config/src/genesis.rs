//! Genesis information used when configuring a node.
use crate::TelcoinDirs;
use eyre::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs,
    num::NonZeroUsize,
    path::Path,
};
use tn_types::{
    address, forks::committee_workers_active, test_genesis, verify_proof_of_possession_bls,
    Address, BlsPublicKey, BlsSignature, Committee, CommitteeBuilder, Genesis, GenesisAccount,
    Multiaddr, NetworkPublicKey, NodeP2pInfo, P2pNode, WorkerId,
};
use tracing::{info, warn};

/// The validators directory used to create genesis
pub const GENESIS_VALIDATORS_DIR: &str = "validators";
/// Precompile info for genesis, read from current submodule commit.
/// tn-contracts split its single deployments.json into per-network files (#123); the
/// genesis-assigned addresses live in deployments-mainnet.json, which is the genesis
/// source of truth that also generates the precompile-config.yaml read below.
pub const DEPLOYMENTS_JSON: &str =
    include_str!("../../../tn-contracts/deployments/deployments-mainnet.json");
/// The path to consensus registry json (tn-contracts submodule).
pub const CONSENSUS_REGISTRY_JSON: &str =
    include_str!("../../../tn-contracts/artifacts/ConsensusRegistry.json");
/// The path to issuance json (tn-contracts submodule).
pub const ISSUANCE_JSON: &str = include_str!("../../../tn-contracts/artifacts/Issuance.json");
/// The path to erc1967proxy json (tn-contracts submodule).
pub const ERC1967PROXY_JSON: &str =
    include_str!("../../../tn-contracts/artifacts/ERC1967Proxy.json");
/// The path to the configuration yaml for genesis accounts (tn-contracts submodule).
pub const GENESIS_ACCOUNT_STATE_YAML: &str =
    include_str!("../../../tn-contracts/deployments/genesis/precompile-config.yaml");
/// The default governance safe address.
pub const GOVERNANCE_SAFE_ADDRESS: Address = address!("00000000000000000000000000000000000007a0");
/// The default issuance address.
pub const ISSUANCE_ADDRESS: Address = address!("07a07a07a07a07a07a07a07a07a07a07a07a07a0");
/// The address for the WorkerConfigs contract.
pub const WORKER_CONFIGS_ADDRESS: Address = address!("Fee0FEe0fee0fEE0FEe0fee0FEE0fEe0feE0FEe0");
/// The path to WorkerConfigs json (tn-contracts submodule).
pub const WORKER_CONFIGS_JSON: &str =
    include_str!("../../../tn-contracts/artifacts/WorkerConfigs.json");

/// The struct for starting a network at genesis.
#[derive(Debug)]
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
    /// - every validator runs the same, non-zero number of workers, and that count is one the
    ///   epoch-0 [`Committee`] layout can hold
    pub fn validate(&self) -> eyre::Result<()> {
        for (pubkey, validator) in self.validators.iter() {
            info!(target: "genesis::validate", "verifying validator: {}", pubkey);
            verify_proof_of_possession_bls(
                &validator.proof_of_possession,
                pubkey,
                &validator.execution_address,
            )?;
        }
        self.agreed_num_workers()?;
        info!(target: "genesis::validate", "all validators valid for genesis");
        Ok(())
    }

    /// Return the worker count shared by every validator.
    ///
    /// The count is a protocol-level value, so genesis is rejected when validators disagree or
    /// when any validator advertises no worker. An empty validator set yields one worker.
    ///
    /// A count above one additionally requires [`committee_workers_active`] at epoch 0, the epoch
    /// of the committee this genesis produces. Below that fork the [`Committee`] bcs layout has no
    /// field to carry a worker count, so the encoder refuses the value outright — a genesis that
    /// slipped through here would not fail until the first consensus pack tried to write its
    /// `EpochMeta`, i.e. a panic at epoch-0 startup rather than a config error while generating
    /// genesis.
    fn agreed_num_workers(&self) -> eyre::Result<NonZeroUsize> {
        let counts = self
            .validators
            .iter()
            .map(|(pubkey, validator)| {
                NonZeroUsize::new(validator.num_workers()).map(|count| (pubkey, count)).ok_or_else(
                    || eyre::eyre!("validator {pubkey} advertises no workers in genesis"),
                )
            })
            .collect::<eyre::Result<Vec<_>>>()?;
        let first = counts.first().map_or(NonZeroUsize::MIN, |(_, count)| *count);
        // disagreement is checked before the fork gate so its message stays the same in every
        // build: a mismatched set is malformed regardless of which counts the fork allows
        if let Some((pubkey, count)) = counts.iter().find(|(_, count)| *count != first) {
            eyre::bail!(
                "validator {pubkey} advertises {count} workers but the first validator advertises \
                 {first}: all validators must run the same number of workers"
            );
        }
        // the committee built from this genesis is epoch 0, which is the epoch the gate reads
        if first.get() != 1 && !committee_workers_active(0) {
            eyre::bail!(
                "genesis validators agree on {first} workers but the committee-workers fork is not \
                 active at epoch 0: the pre-fork committee layout cannot hold a worker count, so \
                 this network must be generated with one worker per validator"
            );
        }
        Ok(first)
    }

    /// Create a [Committee] from the validators in [NetworkGenesis].
    ///
    /// The committee's worker count is the count every validator agrees on (see
    /// [Self::validate]); the bootstrap record for each validator carries all of its workers.
    pub fn create_committee(&self) -> eyre::Result<Committee> {
        let num_workers = self.agreed_num_workers()?;
        let mut committee_builder = CommitteeBuilder::new(0).with_num_workers(num_workers);
        for (pubkey, validator) in self.validators.iter() {
            committee_builder.add_authority_and_bootstrap(
                *pubkey,
                (
                    validator.primary_network_address().clone(),
                    validator.primary_network_key().clone(),
                )
                    .into(),
                validator.worker_p2p_nodes().to_vec(),
                validator.execution_address,
            );
        }
        Ok(committee_builder.build())
    }

    /// Return a reference to the validators.
    pub fn validators(&self) -> &BTreeMap<BlsPublicKey, NodeInfo> {
        &self.validators
    }

    /// Returns configurations for precompiles as genesis accounts.
    /// Precompile configs are generated using foundry in `tn-contracts` submodule.
    pub fn fetch_precompile_genesis_accounts() -> eyre::Result<Vec<(Address, GenesisAccount)>> {
        let config: HashMap<Address, GenesisAccount> =
            serde_yaml::from_str(GENESIS_ACCOUNT_STATE_YAML).expect("yaml parsing failure");
        let mut accounts = Vec::new();
        for (address, account) in config {
            let account = GenesisAccount::default()
                .with_nonce(account.nonce)
                .with_balance(account.balance)
                .with_code(account.code)
                .with_storage(account.storage);
            accounts.push((address, account));
        }
        Ok(accounts)
    }
}

/// Information needed for every validator:
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct NodeInfo {
    /// The name for the validator. The default value
    /// is the base58 encoding of the first 8 bytes of the BLS public key
    /// prepended with 'node-'. The operator can overwrite
    /// this value since it is not used when writing to file.
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
        &self.p2p_info.primary.network_key
    }

    /// Return the primary's network address.
    pub fn primary_network_address(&self) -> &Multiaddr {
        &self.p2p_info.primary.network_address
    }

    /// Return the public network key of worker `worker_id`, if this node runs that worker.
    pub fn worker_network_key(&self, worker_id: WorkerId) -> Option<&NetworkPublicKey> {
        self.p2p_info.worker(worker_id).map(|worker| &worker.network_key)
    }

    /// Return the network address of worker `worker_id`, if this node runs that worker.
    pub fn worker_network_address(&self, worker_id: WorkerId) -> Option<&Multiaddr> {
        self.p2p_info.worker(worker_id).map(|worker| &worker.network_address)
    }

    /// Return the p2p info of every worker, indexed by [WorkerId].
    pub fn worker_p2p_nodes(&self) -> &[P2pNode] {
        &self.p2p_info.workers
    }

    /// Return the number of workers this node runs.
    pub fn num_workers(&self) -> usize {
        self.p2p_info.num_workers()
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
        generate_proof_of_possession_bls_for_test, Address, BlsKeypair, Multiaddr, NetworkKeypair,
        NodeP2pInfo,
    };

    #[test]
    fn test_validate_genesis() {
        let mut network_genesis = NetworkGenesis::new_for_test();
        // create keys and information for validators
        for v in 0..4 {
            let bls_keypair = BlsKeypair::generate(&mut StdRng::from_seed([0; 32]));
            let network_keypair = NetworkKeypair::generate_ed25519();
            let worker_network_keypair = NetworkKeypair::generate_ed25519();
            let address = Address::from_raw_public_key(&[0; 64]);
            let proof_of_possession =
                generate_proof_of_possession_bls_for_test(&bls_keypair, &address).unwrap();
            let primary_network_address = Multiaddr::empty();
            let worker_network_address = Multiaddr::empty();
            let primary_info = NodeP2pInfo::new(
                (network_keypair.public().clone().into(), primary_network_address).into(),
                vec![
                    (worker_network_keypair.public().clone().into(), worker_network_address).into()
                ],
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
            let worker_network_keypair = NetworkKeypair::generate_ed25519();
            let address = Address::from_raw_public_key(&[0; 64]);
            let wrong_address = Address::from_raw_public_key(&[1; 64]);

            // generate proof with wrong chain spec
            let proof_of_possession =
                generate_proof_of_possession_bls_for_test(&bls_keypair, &wrong_address).unwrap();
            let primary_network_address = Multiaddr::empty();
            let worker_network_address = Multiaddr::empty();
            let primary_info = NodeP2pInfo::new(
                (network_keypair.public().clone().into(), primary_network_address).into(),
                vec![
                    (worker_network_keypair.public().clone().into(), worker_network_address).into()
                ],
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

    /// Build a valid validator with `num_workers` workers.
    fn validator_with_workers(index: usize, num_workers: usize) -> NodeInfo {
        let seed = u8::try_from(index).expect("validator index fits a seed byte");
        let bls_keypair = BlsKeypair::generate(&mut StdRng::from_seed([seed; 32]));
        let network_keypair = NetworkKeypair::generate_ed25519();
        let address = Address::from_raw_public_key(&[0; 64]);
        let proof_of_possession =
            generate_proof_of_possession_bls_for_test(&bls_keypair, &address).unwrap();
        let workers = (0..num_workers)
            .map(|_| {
                (NetworkKeypair::generate_ed25519().public().clone().into(), Multiaddr::empty())
                    .into()
            })
            .collect();
        NodeInfo {
            name: format!("validator-{index}"),
            bls_public_key: *bls_keypair.public(),
            p2p_info: NodeP2pInfo::new(
                (network_keypair.public().clone().into(), Multiaddr::empty()).into(),
                workers,
            ),
            execution_address: address,
            proof_of_possession,
        }
    }

    /// Default builds have the multi-worker committee layout active from genesis, so an agreed
    /// count above one is a valid genesis and reaches the committee unchanged; the adiri lane
    /// covers the pre-fork rejection below.
    #[cfg(not(feature = "adiri"))]
    #[test]
    fn test_validate_genesis_agrees_on_worker_count() {
        let mut network_genesis = NetworkGenesis::new_for_test();
        (0..4).for_each(|v| network_genesis.add_validator(validator_with_workers(v, 2)));
        assert!(network_genesis.validate().is_ok());
        let committee = network_genesis.create_committee().expect("committee");
        assert_eq!(committee.number_of_workers(), 2);
        let bootstrap = committee.bootstrap_servers();
        assert!(bootstrap.values().all(|server| server.num_workers() == 2));
    }

    /// Pre-fork the committee layout has no field for a worker count, so an agreed count above one
    /// is rejected while generating genesis instead of panicking the first time epoch 0's
    /// `EpochMeta` is encoded. A single worker still validates.
    ///
    /// The adiri fork epoch is a `u32::MAX` placeholder and its arming constraint floors it at 407,
    /// so epoch 0 is pre-fork in this lane however the constant moves.
    /// `TN_COMMITTEE_WORKERS_FORK_EPOCH` is deliberately not used to stage that: the override's
    /// `OnceLock` is process-wide and the whole test binary shares one process.
    #[cfg(feature = "adiri")]
    #[test]
    fn test_validate_genesis_rejects_multiple_workers_pre_fork() {
        let mut network_genesis = NetworkGenesis::new_for_test();
        (0..4).for_each(|v| network_genesis.add_validator(validator_with_workers(v, 2)));
        let err = network_genesis.validate().expect_err("pre-fork multi-worker genesis must fail");
        assert!(err.to_string().contains("committee-workers fork is not active"), "{err}");
        assert!(network_genesis.create_committee().is_err());

        let mut single_worker = NetworkGenesis::new_for_test();
        (0..4).for_each(|v| single_worker.add_validator(validator_with_workers(v, 1)));
        single_worker.validate().expect("single-worker genesis is valid pre-fork");
        let committee = single_worker.create_committee().expect("committee");
        assert_eq!(committee.number_of_workers(), 1);
    }

    /// Validators disagreeing is malformed in every build, so this assertion holds in both lanes:
    /// the disagreement check runs before the fork gate.
    #[test]
    fn test_validate_genesis_rejects_mismatched_worker_count() {
        let mut network_genesis = NetworkGenesis::new_for_test();
        (0..3).for_each(|v| network_genesis.add_validator(validator_with_workers(v, 2)));
        network_genesis.add_validator(validator_with_workers(3, 1));
        let err = network_genesis.validate().expect_err("mismatched worker counts must fail");
        assert!(err.to_string().contains("same number of workers"), "{err}");
        assert!(network_genesis.create_committee().is_err());
    }
}
