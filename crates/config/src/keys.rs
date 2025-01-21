//! Cryptographic keys used by the node.

use crate::{
    read_validator_keypair_from_file, TelcoinDirs, BLS_KEYFILE, PRIMARY_NETWORK_KEYFILE,
    WORKER_NETWORK_KEYFILE,
};
use std::sync::Arc;
use tn_types::{
    traits::{AllowedRng, KeyPair, Signer},
    BlsKeypair, BlsPublicKey, BlsSignature, BlsSigner, NetworkKeypair, NetworkPublicKey,
};

#[derive(Debug)]
struct KeyConfigInner {
    // DO NOT expose the private key to other code.  Tests that need this will provide a primary
    // key. Use the BlsSigner trait for signing for the primary.
    primary_keypair: BlsKeypair,
    primary_network_keypair: NetworkKeypair,
    worker_network_keypair: NetworkKeypair,
}

// TODO: audit the use of this struct for leaking private keys, etc...
#[derive(Debug, Clone)]
pub struct KeyConfig {
    inner: Arc<KeyConfigInner>,
}

impl KeyConfig {
    pub fn new<TND: TelcoinDirs>(tn_datadir: &TND) -> eyre::Result<Self> {
        // TODO: find a better way to manage keys
        //
        // load keys to start the primary
        let validator_keypath = tn_datadir.validator_keys_path();
        tracing::info!(target: "telcoin::consensus_config", "loading validator keys at {:?}", validator_keypath);
        let primary_keypair =
            read_validator_keypair_from_file(validator_keypath.join(BLS_KEYFILE))?;
        let network_keypair =
            read_validator_keypair_from_file(validator_keypath.join(PRIMARY_NETWORK_KEYFILE))?;
        let worker_network_keypair =
            read_validator_keypair_from_file(validator_keypath.join(WORKER_NETWORK_KEYFILE))?;
        Ok(Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair: network_keypair,
                worker_network_keypair,
            }),
        })
    }

    /// Generate random keys with provided RNG.
    ///
    /// Useful for testing.
    pub fn with_random<R: AllowedRng>(rng: &mut R) -> Self {
        let primary_keypair = BlsKeypair::generate(rng);
        let network_keypair = NetworkKeypair::generate(rng);
        let worker_network_keypair = NetworkKeypair::generate(rng);
        Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair: network_keypair,
                worker_network_keypair,
            }),
        }
    }

    /// Generate a config with a provided primary and random network keys with provided RNG.
    ///
    /// Useful for testing ONLY.
    pub fn with_primary_random_networks<R: AllowedRng>(
        primary_keypair: BlsKeypair,
        rng: &mut R,
    ) -> Self {
        let network_keypair = NetworkKeypair::generate(rng);
        let worker_network_keypair = NetworkKeypair::generate(rng);
        Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair: network_keypair,
                worker_network_keypair,
            }),
        }
    }

    /// Provide the primaries public key.
    pub fn primary_public_key(&self) -> BlsPublicKey {
        self.inner.primary_keypair.public().clone()
    }

    /// Provide the keypair (with private key) for the network.
    /// Allows building the anemo network.
    pub fn primary_network_keypair(&self) -> &NetworkKeypair {
        &self.inner.primary_network_keypair
    }

    /// The [NetworkPublicKey] for the primary network.
    pub fn primary_network_public_key(&self) -> NetworkPublicKey {
        self.inner.primary_network_keypair.public().clone()
    }

    /// Provide the keypair (with private key) for the worker network.
    /// Allows building the anemo worker network.
    pub fn worker_network_keypair(&self) -> &NetworkKeypair {
        &self.inner.worker_network_keypair
    }

    /// The [NetworkPublicKey] for the worker network.
    pub fn worker_network_public_key(&self) -> NetworkPublicKey {
        self.inner.worker_network_keypair.public().clone()
    }
}

impl BlsSigner for KeyConfig {
    fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
        self.inner.primary_keypair.sign(msg)
    }
}
