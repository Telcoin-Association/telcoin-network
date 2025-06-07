//! Cryptographic keys used by the node.

use crate::{
    TelcoinDirs, BLS_KEYFILE, BLS_WRAPPED_KEYFILE, PRIMARY_NETWORK_SEED_FILE,
    WORKER_NETWORK_SEED_FILE,
};
use aes_gcm_siv::{aead::Aead as _, Aes256GcmSiv, Key, KeyInit, Nonce};
use pbkdf2::pbkdf2_hmac;
use rand::{rngs::StdRng, Rng as _, SeedableRng};
use sha2::Sha256;
use std::sync::Arc;
use tn_types::{
    encode, Address, BlsKeypair, BlsPublicKey, BlsSignature, BlsSigner, DefaultHashFunction,
    Intent, IntentMessage, IntentScope, NetworkKeypair, NetworkPublicKey, ProtocolSignature as _,
    Signer,
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
    /// Wrap (encrypt) a BLS key with passphrase.
    /// Returns a String that is the Base58 encoding of the encrypted bytes.
    /// bytes 0-11 are the pbkdf2 salt, 12-23 are the aes-gcm-siv nonce and 24.. are the encrypted
    /// key.
    fn wrap_bls_key(primary_keypair: &BlsKeypair, passphrase: &str) -> eyre::Result<String> {
        let mut salt = [0_u8; 12];
        rand::rng().fill(&mut salt);
        let mut nonce_bytes = [0_u8; 12];
        rand::rng().fill(&mut nonce_bytes);
        let mut passphrase_bytes = [0_u8; 32];
        pbkdf2_hmac::<Sha256>(passphrase.as_bytes(), &salt, 1_000, &mut passphrase_bytes);
        let key = Key::<Aes256GcmSiv>::from_slice(&passphrase_bytes);
        let cipher = Aes256GcmSiv::new(key);
        let nonce = Nonce::from_slice(&nonce_bytes); // 96-bits
        let ciphertext = cipher
            .encrypt(nonce, &primary_keypair.to_bytes()[..])
            .map_err(|e| eyre::eyre!("Could not encrypt BLS key: {e}"))?;
        let encrypted_data = [&salt[..], &nonce_bytes[..], &ciphertext[..]].concat();
        Ok(bs58::encode(&encrypted_data).into_string())
    }

    /// Accepts bytes that are a wrapped BLS key and unwraps with the passphrase.
    /// bytes 0-11 are the pbkdf2 salt, 12-23 are the aes-gcm-siv nonce and 24.. are the encrypted
    /// key.
    fn unwrap_bls_key(bytes: &[u8], passphrase: &str) -> eyre::Result<BlsKeypair> {
        let mut passphrase_bytes = [0_u8; 32];
        pbkdf2_hmac::<Sha256>(passphrase.as_bytes(), &bytes[0..12], 1_000, &mut passphrase_bytes);
        let nonce = Nonce::from_slice(&bytes[12..24]); // 96-bits
        let key = Key::<Aes256GcmSiv>::from_slice(&passphrase_bytes);
        let cipher = Aes256GcmSiv::new(key);
        let plaintext = cipher
            .decrypt(nonce, &bytes[24..])
            .map_err(|e| eyre::eyre!("Could not decrypt BLS key: {e}"))?;
        BlsKeypair::from_bytes(&plaintext)
    }

    /// Read a key config file that contains the primary BLS key in Base 58 format.
    pub fn read_config<TND: TelcoinDirs>(
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<Self> {
        // load keys to start the primary
        let validator_keypath = tn_datadir.node_keys_path();
        tracing::info!(target: "telcoin::consensus_config", "loading validator keys at {:?}", validator_keypath);
        let contents = if passphrase.is_some() {
            std::fs::read_to_string(tn_datadir.node_keys_path().join(BLS_WRAPPED_KEYFILE))?
        } else {
            std::fs::read_to_string(tn_datadir.node_keys_path().join(BLS_KEYFILE))?
        };
        let primary_seed =
            std::fs::read_to_string(tn_datadir.node_keys_path().join(PRIMARY_NETWORK_SEED_FILE))
                .unwrap_or_else(|_| "primary network keypair".to_string());
        let worker_seed =
            std::fs::read_to_string(tn_datadir.node_keys_path().join(WORKER_NETWORK_SEED_FILE))
                .unwrap_or_else(|_| "worker network keypair".to_string());
        let bytes = bs58::decode(contents.as_str().trim()).into_vec()?;
        let primary_keypair = if let Some(passphrase) = passphrase {
            Self::unwrap_bls_key(&bytes, &passphrase)?
        } else {
            BlsKeypair::from_bytes(&bytes)?
        };
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
    pub fn generate_and_save<TND: TelcoinDirs>(
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<Self> {
        // note: StdRng uses ChaCha12
        let primary_keypair = BlsKeypair::generate(&mut StdRng::from_os_rng());
        let primary_seed = "primary network keypair";
        let worker_seed = "worker network keypair";
        let primary_network_keypair =
            Self::generate_network_keypair(&primary_keypair, primary_seed);
        let worker_network_keypair = Self::generate_network_keypair(&primary_keypair, worker_seed);
        // Make sure we have the validator dir.
        // Don't error out if path exists.
        let _ = std::fs::create_dir(tn_datadir.node_keys_path());
        if let Some(passphrase) = passphrase {
            let contents = Self::wrap_bls_key(&primary_keypair, &passphrase)?;
            std::fs::write(tn_datadir.node_keys_path().join(BLS_WRAPPED_KEYFILE), contents)?;
        } else {
            let contents = bs58::encode(primary_keypair.to_bytes()).into_string();
            std::fs::write(tn_datadir.node_keys_path().join(BLS_KEYFILE), contents)?;
        }
        std::fs::write(tn_datadir.node_keys_path().join(PRIMARY_NETWORK_SEED_FILE), primary_seed)?;
        std::fs::write(tn_datadir.node_keys_path().join(WORKER_NETWORK_SEED_FILE), worker_seed)?;
        Ok(Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair,
                worker_network_keypair,
            }),
        })
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
        address: &Address,
    ) -> eyre::Result<BlsSignature> {
        let mut msg = self.primary_public_key().as_ref().to_vec();
        let address_bytes = encode(address);
        msg.extend_from_slice(address_bytes.as_slice());
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
        hasher.update(&primary_keypair.sign(seed_str.as_bytes()).to_bytes());
        let hash = hasher.finalize();
        NetworkKeypair::ed25519_from_bytes(hash.as_bytes()[0..32].to_vec())
            .expect("invalid network key bytes")
    }
}

impl BlsSigner for KeyConfig {
    fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
        self.inner.primary_keypair.sign(msg)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::KeyConfig;

    #[test]
    fn test_bls_passphrase() {
        let tmp_dir = TempDir::new().expect("tmp dir");
        let pp = Some("test_bls_passphrase".to_string());
        let kc = KeyConfig::generate_and_save(&tmp_dir.path().to_path_buf(), pp.clone())
            .expect("BLS key config");
        let kc2 =
            KeyConfig::read_config(&tmp_dir.path().to_path_buf(), pp.clone()).expect("load config");
        assert_eq!(kc.inner.primary_keypair.to_bytes(), kc2.inner.primary_keypair.to_bytes());
        assert!(KeyConfig::read_config(&tmp_dir.path().to_path_buf(), None).is_err());
        assert!(KeyConfig::read_config(
            &tmp_dir.path().to_path_buf(),
            Some("not_passphrase".to_string())
        )
        .is_err());
    }

    #[test]
    fn test_bls_no_passphrase() {
        let tmp_dir = TempDir::new().expect("tmp dir");
        let pp = None;
        let kc = KeyConfig::generate_and_save(&tmp_dir.path().to_path_buf(), pp.clone())
            .expect("BLS key config");
        let kc2 =
            KeyConfig::read_config(&tmp_dir.path().to_path_buf(), pp.clone()).expect("load config");
        assert_eq!(kc.inner.primary_keypair.to_bytes(), kc2.inner.primary_keypair.to_bytes());
        assert!(KeyConfig::read_config(
            &tmp_dir.path().to_path_buf(),
            Some("not_passphrase".to_string())
        )
        .is_err());
    }
}
