//! Cryptographic keys used by the node.

use crate::{TelcoinDirs, BLS_KEYFILE, PRIMARY_NETWORK_SEED_FILE, WORKER_NETWORK_SEED_FILE};
use blake2::Digest;
use rand::{rngs::StdRng, CryptoRng, RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use reth_chainspec::ChainSpec;
use std::sync::Arc;
use tn_types::{
    encode, BlsKeypair, BlsPublicKey, BlsSignature, BlsSigner, DefaultHashFunction, Intent,
    IntentMessage, IntentScope, NetworkKeypair, NetworkPublicKey, ProtocolSignature as _, Signer,
};

#[derive(Debug)]
struct KeyConfigInner {
    // DO NOT expose the private key to other code.  Tests that need this will provide a primary
    // key. Use the BlsSigner trait for signing for the primary.
    primary_keypair: BlsKeypair,
    // Derived from the primary_keypair.
    primary_network_keypair: NetworkKeypair,
    // Derived from the primary_keypair.
    worker_network_keypair: NetworkKeypair,
}

/// Basic implementation of a key manager.  This version will read a BLS key
/// from a file (which is not ideal).  It is intended to be an interface that
/// can later expand to be backed with something more secure (like an HSM).
/// It should NOT expose the BLS private key, even though it is currently read
/// from a file this will not always be the case and all code needing signatures
/// MUST go through KeyConfig.
/// NOTE: The two network keys (primary and worker) are derived from the BLS key
/// and are exposed to other code.  This is required to work with libp2p which
/// wants the actual private key.  This method of deriving the key is an attempt
/// to provide some protection to the key- even though it will exist in memory it
/// does NOT need to be stored on disk or otherwise saved.
#[derive(Debug, Clone)]
pub struct KeyConfig {
    inner: Arc<KeyConfigInner>,
}

impl KeyConfig {
    /// Read a key config file that contains the primary BLS key in Base 58 format.
    pub fn read_config<TND: TelcoinDirs>(tn_datadir: &TND) -> eyre::Result<Self> {
        // TODO: find a better way to manage keys
        //
        // load keys to start the primary
        let validator_keypath = tn_datadir.validator_keys_path();
        tracing::info!(target: "telcoin::consensus_config", "loading validator keys at {:?}", validator_keypath);
        let contents = std::fs::read_to_string(tn_datadir.validator_keys_path().join(BLS_KEYFILE))?;
        let primary_seed = std::fs::read_to_string(
            tn_datadir.validator_keys_path().join(PRIMARY_NETWORK_SEED_FILE),
        )
        .unwrap_or_else(|_| "primary network keypair".to_string());
        let worker_seed = std::fs::read_to_string(
            tn_datadir.validator_keys_path().join(WORKER_NETWORK_SEED_FILE),
        )
        .unwrap_or_else(|_| "worker network keypair".to_string());
        let bytes = bs58::decode(contents.as_str().trim()).into_vec()?;
        let primary_keypair = BlsKeypair::from_bytes(&bytes)?;
        let primary_network_keypair =
            Self::generate_network_keypair(&primary_keypair, &primary_seed);
        let worker_network_keypair = Self::generate_network_keypair(&primary_keypair, &worker_seed);
        Ok(Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair,
                worker_network_keypair,
            }),
        })
    }

    /// Generate a new random primary BLS key and save to the config file.
    /// Note, this is not very secure in that it is writing the private key to a file...
    pub fn generate_and_save<TND: TelcoinDirs>(tn_datadir: &TND) -> eyre::Result<Self> {
        let rng = ChaCha20Rng::from_entropy();
        // note: StdRng uses ChaCha12
        let primary_keypair = BlsKeypair::generate(&mut StdRng::from_rng(rng)?);
        let primary_seed = "primary network keypair";
        let worker_seed = "worker network keypair";
        let primary_network_keypair =
            Self::generate_network_keypair(&primary_keypair, primary_seed);
        let worker_network_keypair = Self::generate_network_keypair(&primary_keypair, worker_seed);
        let contents = bs58::encode(primary_keypair.to_bytes()).into_string();
        std::fs::write(tn_datadir.validator_keys_path().join(BLS_KEYFILE), contents)?;
        std::fs::write(
            tn_datadir.validator_keys_path().join(PRIMARY_NETWORK_SEED_FILE),
            primary_seed,
        )?;
        std::fs::write(
            tn_datadir.validator_keys_path().join(WORKER_NETWORK_SEED_FILE),
            worker_seed,
        )?;
        Ok(Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair,
                worker_network_keypair,
            }),
        })
    }

    /// Generate random keys with provided RNG.
    ///
    /// Useful for testing.
    pub fn with_random<R: CryptoRng + RngCore>(rng: &mut R) -> Self {
        let primary_keypair = BlsKeypair::generate(rng);
        let primary_network_keypair =
            Self::generate_network_keypair(&primary_keypair, "primary network keypair");
        let worker_network_keypair =
            Self::generate_network_keypair(&primary_keypair, "worker network keypair");
        Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair,
                worker_network_keypair,
            }),
        }
    }

    /// Create a config with a provided key- this is ONLY for testing.
    pub fn new_with_testing_key(primary_keypair: BlsKeypair) -> Self {
        let primary_network_keypair =
            Self::generate_network_keypair(&primary_keypair, "primary network keypair");
        let worker_network_keypair =
            Self::generate_network_keypair(&primary_keypair, "worker network keypair");
        Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair,
                worker_network_keypair,
            }),
        }
    }

    /// Provide the primaries public key.
    pub fn primary_public_key(&self) -> BlsPublicKey {
        *self.inner.primary_keypair.public()
    }

    /// Provide the keypair (with private key) for the network.
    /// Allows building the libp2p network.
    pub fn primary_network_keypair(&self) -> &NetworkKeypair {
        &self.inner.primary_network_keypair
    }

    /// The [NetworkPublicKey] for the primary network.
    pub fn primary_network_public_key(&self) -> NetworkPublicKey {
        self.primary_network_keypair().public().clone().into()
    }

    /// Provide the keypair (with private key) for the worker network.
    /// Allows building the libp2p worker network.
    pub fn worker_network_keypair(&self) -> &NetworkKeypair {
        &self.inner.worker_network_keypair
    }

    /// The [NetworkPublicKey] for the worker network.
    pub fn worker_network_public_key(&self) -> NetworkPublicKey {
        self.worker_network_keypair().public().into()
    }

    /// Creates a proof of that the authority account address is owned by the
    /// holder of authority protocol key, and also ensures that the authority
    /// protocol public key exists.
    ///
    /// The proof of possession is a [BlsSignature] committed over the intent message
    /// `intent || message` (See more at [IntentMessage] and [Intent]).
    /// The message is constructed as: [BlsPublicKey] || [Genesis].
    pub fn generate_proof_of_possession_bls(
        &self,
        chain_spec: &ChainSpec,
    ) -> eyre::Result<BlsSignature> {
        let mut msg = self.primary_public_key().as_ref().to_vec();
        let genesis_bytes = encode(&chain_spec.genesis);
        msg.extend_from_slice(genesis_bytes.as_slice());
        let sig = BlsSignature::new_secure(
            &IntentMessage::new(Intent::telcoin(IntentScope::ProofOfPossession), msg),
            &self.inner.primary_keypair,
        );
        Ok(sig)
    }

    /// Derive a NetworkKeypair from a BLS signature, seed string and [DefaultHashFunction].
    /// This is deterministic for a given keypair and seed_str.
    fn generate_network_keypair(primary_keypair: &BlsKeypair, seed_str: &str) -> NetworkKeypair {
        let mut hasher = DefaultHashFunction::new();
        hasher.update(primary_keypair.sign(seed_str.as_bytes()).to_bytes());
        let hash = hasher.finalize();
        NetworkKeypair::ed25519_from_bytes(hash[0..32].to_vec()).expect("invalid network key bytes")
    }
}

impl BlsSigner for KeyConfig {
    fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
        self.inner.primary_keypair.sign(msg)
    }
}
