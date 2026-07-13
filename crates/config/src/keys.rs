//! Cryptographic keys used by the node.
//!
//! # Wrapped BLS keyfile format (`bls.kw`)
//!
//! The file is the Base58 encoding of `salt[12] | nonce[12] | AES-256-GCM-SIV ciphertext`.
//! The AES wrapping key is derived from the passphrase with PBKDF2-HMAC-SHA256. The round
//! count is not stored on disk; [`KeyConfig::read_config`] tries `{1, 1_000_000}` in turn and
//! authenticates each guess with the AEAD tag, so a wrong round count (or passphrase) can
//! never decrypt a key. A key that only opens at 1 round was written by an insecure or
//! poisoned build and is warned about loudly.

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
    construct_proof_of_possession_message, Address, BlsKeypair, BlsPublicKey, BlsSignature,
    BlsSigner, DefaultHashFunction, NetworkKeypair, NetworkPublicKey, Signer,
};

/// The work factor for PBKDF2 is implemented through an iteration count, which is based on the
/// internal hashing algorithm used. HMAC-SHA-256 is widely supported and is recommended by NIST.
/// OWASP recommends 600,000 iterations for PBKDF2-HMAC-SHA256.
///
/// This constant must NEVER be feature-gated: cargo features unify across the whole workspace,
/// so a cfg'd weak value here would silently poison release binaries that happen to be built
/// alongside test crates. Tests that need a fast KDF must use
/// [`KeyConfig::generate_and_save_insecure`], whose weakness stays contained to the single
/// file it writes.
const PBKDF2_HMAC_ROUNDS: u32 = 1_000_000;

/// The round count written by builds poisoned by the old `test-utils` feature unification bug
/// (and by the insecure test writer). Tried first when reading - it costs microseconds and
/// precisely identifies weakly wrapped keys so they can be loudly warned about.
const TEST_ONLY_INSECURE_ROUNDS: u32 = 1;

/// PBKDF2 salt length (bytes).
const SALT_LEN: usize = 12;
/// AES-256-GCM-SIV nonce length (96 bits).
const NONCE_LEN: usize = 12;

/// Emit `msg` as a warning on both `tracing` and stderr.
///
/// Node startup reads the BLS key *before* tracing is initialized (see the CLI's
/// `read_config` call site), so a tracing-only warning would vanish exactly where it matters
/// most; `eprintln!` still reaches the operator's console and container logs.
fn warn_weak_kdf(msg: &str) {
    tracing::warn!(target: "tn::config", "{msg}");
    eprintln!("WARNING: {msg}");
}

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
    /// Derive the 32-byte AES wrapping key from `passphrase` via PBKDF2-HMAC-SHA256.
    fn derive_wrapping_key(passphrase: &str, salt: &[u8], rounds: u32) -> [u8; 32] {
        let mut wrapping_key = [0_u8; 32];
        pbkdf2_hmac::<Sha256>(passphrase.as_bytes(), salt, rounds, &mut wrapping_key);
        wrapping_key
    }

    /// Wrap (encrypt) a BLS key with a passphrase using `rounds` PBKDF2-HMAC-SHA256 iterations.
    /// Returns the Base58 encoding of `salt[12] | nonce[12] | ciphertext` (see the module docs).
    fn wrap_bls_key(
        primary_keypair: &BlsKeypair,
        passphrase: &str,
        rounds: u32,
    ) -> eyre::Result<String> {
        let mut salt = [0_u8; SALT_LEN];
        rand::rng().fill(&mut salt);
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        rand::rng().fill(&mut nonce_bytes);
        let wrapping_key = Self::derive_wrapping_key(passphrase, &salt, rounds);
        let key = Key::<Aes256GcmSiv>::from_slice(&wrapping_key);
        let cipher = Aes256GcmSiv::new(key);
        let nonce = Nonce::from_slice(&nonce_bytes); // 96-bits
        let ciphertext = cipher
            .encrypt(nonce, &primary_keypair.to_bytes()[..])
            .map_err(|e| eyre::eyre!("Could not encrypt BLS key: {e}"))?;
        Ok(bs58::encode([&salt[..], &nonce_bytes[..], &ciphertext[..]].concat()).into_string())
    }

    /// One AEAD decryption attempt at a specific salt/nonce/rounds interpretation.
    ///
    /// `None` means authentication failed, i.e. this interpretation (or the passphrase) is
    /// wrong; callers may safely try another interpretation of the same bytes.
    fn try_decrypt(
        passphrase: &str,
        salt: &[u8],
        nonce: &[u8],
        ciphertext: &[u8],
        rounds: u32,
    ) -> Option<BlsKeypair> {
        let wrapping_key = Self::derive_wrapping_key(passphrase, salt, rounds);
        let key = Key::<Aes256GcmSiv>::from_slice(&wrapping_key);
        let cipher = Aes256GcmSiv::new(key);
        let plaintext = cipher.decrypt(Nonce::from_slice(nonce), ciphertext).ok()?;
        BlsKeypair::from_bytes(&plaintext).ok()
    }

    /// Accepts bytes that are a wrapped BLS key and unwraps with the passphrase.
    ///
    /// The round count is not stored in the file, so each historical value is tried in turn.
    /// Every failed attempt is an AEAD authentication failure, so falling through to the next
    /// round count can never decrypt a key wrongly.
    fn unwrap_bls_key(bytes: &[u8], passphrase: &str) -> eyre::Result<BlsKeypair> {
        if bytes.len() <= SALT_LEN + NONCE_LEN {
            return Err(eyre::eyre!("Could not decrypt BLS key: keyfile is truncated or corrupt"));
        }
        let salt = &bytes[..SALT_LEN];
        let (nonce, ciphertext) = bytes[SALT_LEN..].split_at(NONCE_LEN);
        // Weak first: it costs microseconds and precisely flags weakly-wrapped keys. AEAD
        // authentication rejects wrong guesses, so falling through can never decrypt a key
        // wrongly.
        for &rounds in &[TEST_ONLY_INSECURE_ROUNDS, PBKDF2_HMAC_ROUNDS] {
            if let Some(keypair) = Self::try_decrypt(passphrase, salt, nonce, ciphertext, rounds) {
                if rounds < PBKDF2_HMAC_ROUNDS {
                    warn_weak_kdf(&format!(
                        "BLS keyfile is protected by a weak KDF ({rounds} PBKDF2 round(s) instead \
                         of {PBKDF2_HMAC_ROUNDS}); it was written by an insecure or poisoned \
                         build. Re-wrap it with a production binary."
                    ));
                }
                return Ok(keypair);
            }
        }
        Err(eyre::eyre!(
            "Could not decrypt BLS key: wrong passphrase, or corrupted/unsupported keyfile"
        ))
    }

    /// Read a key config file that contains the primary BLS key in Base 58 format.
    pub fn read_config<TND: TelcoinDirs>(
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<Self> {
        // If we don't have a wrapped file then try an unencrypted file before failure.
        let passphrase = if std::fs::exists(tn_datadir.node_keys_path().join(BLS_WRAPPED_KEYFILE))
            .unwrap_or(false)
        {
            passphrase
        } else {
            None
        };

        // load keys to start the primary
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

    /// Returns `true` if BLS key material exists on disk for `tn_datadir`, whether
    /// stored encrypted (`bls.kw`) or in cleartext (`bls.key`).
    ///
    /// Lets callers distinguish "no keys generated yet" from "keys present but
    /// unreadable" (e.g. an incorrect passphrase) when reporting errors.
    pub fn keys_exist<TND: TelcoinDirs>(tn_datadir: &TND) -> bool {
        let keys_dir = tn_datadir.node_keys_path();
        std::fs::exists(keys_dir.join(BLS_WRAPPED_KEYFILE)).unwrap_or(false)
            || std::fs::exists(keys_dir.join(BLS_KEYFILE)).unwrap_or(false)
    }

    /// Generate a new random primary BLS key and save to the config file.
    /// Note, this is not very secure in that it is writing the private key to a file...
    pub fn generate_and_save<TND: TelcoinDirs>(
        tn_datadir: &TND,
        passphrase: Option<String>,
    ) -> eyre::Result<Self> {
        Self::generate_and_save_with_rounds(tn_datadir, passphrase, PBKDF2_HMAC_ROUNDS)
    }

    /// Generate a new random primary BLS key and save it wrapped with a caller-chosen,
    /// intentionally weak PBKDF2 round count. NEVER call this outside tests.
    ///
    /// The reader tries the weak round count when opening any wrapped key (with a loud
    /// warning), so the file this writes still loads on a production binary - the weakness
    /// stays contained to this one file and can never change how other keys are wrapped.
    #[cfg(feature = "test-utils")]
    pub fn generate_and_save_insecure<TND: TelcoinDirs>(
        tn_datadir: &TND,
        passphrase: Option<String>,
        rounds: u32,
    ) -> eyre::Result<Self> {
        if rounds == 0 {
            return Err(eyre::eyre!("invalid PBKDF2 round count: must be >= 1"));
        }
        tracing::warn!(
            target: "tn::config",
            "generating BLS key with INSECURE PBKDF2 rounds = {rounds} - test use only"
        );
        Self::generate_and_save_with_rounds(tn_datadir, passphrase, rounds)
    }

    /// Shared implementation for [`Self::generate_and_save`] and
    /// [`Self::generate_and_save_insecure`]: generate the key material and persist it, wrapped
    /// at `rounds` when a passphrase is given.
    fn generate_and_save_with_rounds<TND: TelcoinDirs>(
        tn_datadir: &TND,
        passphrase: Option<String>,
        rounds: u32,
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
            let contents = Self::wrap_bls_key(&primary_keypair, &passphrase, rounds)?;
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

    /// Creates a proof that the authority account address is owned by the
    /// holder of authority protocol key, and also ensures that the authority
    /// protocol public key exists.
    ///
    /// The proof of possession is a [BlsSignature] over [`construct_proof_of_possession_message`]:
    /// `intentPrefix || compressedBlsPubkey || address`. Using the compressed key keeps the message
    /// cheaply reconstructable by the on-chain `ConsensusRegistry`, which verifies it via the
    /// native BLS precompile.
    pub fn generate_proof_of_possession_bls(
        &self,
        address: &Address,
    ) -> eyre::Result<BlsSignature> {
        let msg = construct_proof_of_possession_message(&self.primary_public_key(), address);
        Ok(self.inner.primary_keypair.sign(&msg))
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

    fn public_key(&self) -> BlsPublicKey {
        self.primary_public_key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Re-implement the on-disk format independently of the production writer, so the reader
    /// tests pin the format contract rather than merely echoing the writer's output:
    /// `salt[12] | nonce[12] | ciphertext`, with `rounds` implied rather than stored.
    fn wrap_test_only(keypair: &BlsKeypair, passphrase: &str, rounds: u32) -> String {
        let mut salt = [0_u8; SALT_LEN];
        rand::rng().fill(&mut salt);
        let mut nonce_bytes = [0_u8; NONCE_LEN];
        rand::rng().fill(&mut nonce_bytes);
        let wrapping_key = KeyConfig::derive_wrapping_key(passphrase, &salt, rounds);
        let cipher = Aes256GcmSiv::new(Key::<Aes256GcmSiv>::from_slice(&wrapping_key));
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce_bytes), &keypair.to_bytes()[..])
            .expect("test_only encrypt");
        bs58::encode([&salt[..], &nonce_bytes[..], &ciphertext[..]].concat()).into_string()
    }

    /// Write `wrapped` as `bls.kw` under a fresh datadir so `read_config` exercises the
    /// normal read path.
    fn install_wrapped(tmp_dir: &TempDir, wrapped: &str) -> PathBuf {
        let datadir = tmp_dir.path().to_path_buf();
        std::fs::create_dir_all(datadir.node_keys_path()).expect("keys dir");
        std::fs::write(datadir.node_keys_path().join(BLS_WRAPPED_KEYFILE), wrapped)
            .expect("write bls.kw");
        datadir
    }

    fn random_keypair() -> BlsKeypair {
        BlsKeypair::generate(&mut StdRng::from_os_rng())
    }

    #[test]
    fn test_bls_passphrase() {
        let tmp_dir = TempDir::new().expect("tmp dir");
        let datadir = tmp_dir.path().to_path_buf();
        let pp = Some("test_bls_passphrase".to_string());
        // fast rounds: this test covers the wrap/unwrap plumbing, not the work factor
        // (the production round count is pinned by `test_bls_passphrase_production_rounds`)
        let kc = KeyConfig::generate_and_save_with_rounds(&datadir, pp.clone(), 1)
            .expect("BLS key config");
        let kc2 = KeyConfig::read_config(&datadir, pp).expect("load config");
        assert_eq!(kc.inner.primary_keypair.to_bytes(), kc2.inner.primary_keypair.to_bytes());
        assert!(KeyConfig::read_config(&datadir, None).is_err());
        assert!(KeyConfig::read_config(&datadir, Some("not_passphrase".to_string())).is_err());
    }

    /// The true production write path: `generate_and_save` must emit the headerless layout,
    /// wrap at `PBKDF2_HMAC_ROUNDS`, and read back correctly.
    #[test]
    fn test_bls_passphrase_production_rounds() {
        let tmp_dir = TempDir::new().expect("tmp dir");
        let datadir = tmp_dir.path().to_path_buf();
        let pp = "production_rounds";
        let kc = KeyConfig::generate_and_save(&datadir, Some(pp.to_string())).expect("key config");

        // decode the on-disk file and pin the headerless layout: salt | nonce | ct (+16-byte tag)
        let contents = std::fs::read_to_string(datadir.node_keys_path().join(BLS_WRAPPED_KEYFILE))
            .expect("read bls.kw");
        let bytes = bs58::decode(contents.trim()).into_vec().expect("base58");
        let sk_len = kc.inner.primary_keypair.to_bytes().len();
        assert_eq!(bytes.len(), SALT_LEN + NONCE_LEN + sk_len + 16);

        // The round count is not stored on disk, so pin the production work factor directly:
        // the file must open at PBKDF2_HMAC_ROUNDS and NOT at the insecure round count. This is
        // what would catch a cfg-gated weak constant sneaking back in via feature unification.
        let salt = &bytes[..SALT_LEN];
        let (nonce, ct) = bytes[SALT_LEN..].split_at(NONCE_LEN);
        assert!(
            KeyConfig::try_decrypt(pp, salt, nonce, ct, TEST_ONLY_INSECURE_ROUNDS).is_none(),
            "production keyfile must not open at the insecure round count"
        );
        assert!(
            KeyConfig::try_decrypt(pp, salt, nonce, ct, PBKDF2_HMAC_ROUNDS).is_some(),
            "production keyfile must open at PBKDF2_HMAC_ROUNDS"
        );

        let kc2 = KeyConfig::read_config(&datadir, Some(pp.to_string())).expect("load config");
        assert_eq!(kc.inner.primary_keypair.to_bytes(), kc2.inner.primary_keypair.to_bytes());
    }

    /// Weak keyfile written by a build poisoned by the old feature-unification bug
    /// (rounds = 1): the in-place upgrade path for weakly provisioned datadirs - must read
    /// OK (with a loud warning).
    #[test]
    fn test_test_only_weak_file_read() {
        let tmp_dir = TempDir::new().expect("tmp dir");
        let keypair = random_keypair();
        let expected = keypair.to_bytes();
        let wrapped = wrap_test_only(&keypair, "test_only_weak", TEST_ONLY_INSECURE_ROUNDS);
        let datadir = install_wrapped(&tmp_dir, &wrapped);
        let kc = KeyConfig::read_config(&datadir, Some("test_only_weak".to_string()))
            .expect("read test_only weak file");
        assert_eq!(kc.inner.primary_keypair.to_bytes(), expected);
    }

    /// Keyfile written by a healthy build (rounds = 1,000,000) must read OK.
    #[test]
    fn test_test_only_strong_file_read() {
        let tmp_dir = TempDir::new().expect("tmp dir");
        let keypair = random_keypair();
        let expected = keypair.to_bytes();
        let wrapped = wrap_test_only(&keypair, "test_only_strong", PBKDF2_HMAC_ROUNDS);
        let datadir = install_wrapped(&tmp_dir, &wrapped);
        let kc = KeyConfig::read_config(&datadir, Some("test_only_strong".to_string()))
            .expect("read test_only strong file");
        assert_eq!(kc.inner.primary_keypair.to_bytes(), expected);
    }

    /// Truncated / garbage keyfiles must error, not panic (regression test: the old reader
    /// sliced fixed byte ranges without length guards).
    #[test]
    fn test_truncated_wrapped_file_errors() {
        let tmp_dir = TempDir::new().expect("tmp dir");
        let datadir = install_wrapped(&tmp_dir, &bs58::encode([0_u8; 10]).into_string());
        assert!(KeyConfig::read_config(&datadir, Some("any".to_string())).is_err());
    }

    /// The test-utils-gated insecure writer rejects a zero round count and round-trips.
    #[cfg(feature = "test-utils")]
    #[test]
    fn test_generate_and_save_insecure() {
        let tmp_dir = TempDir::new().expect("tmp dir");
        let datadir = tmp_dir.path().to_path_buf();
        let pp = Some("insecure".to_string());
        assert!(KeyConfig::generate_and_save_insecure(&datadir, pp.clone(), 0).is_err());
        let kc = KeyConfig::generate_and_save_insecure(&datadir, pp.clone(), 1)
            .expect("insecure BLS key config");
        let kc2 = KeyConfig::read_config(&datadir, pp).expect("load config");
        assert_eq!(kc.inner.primary_keypair.to_bytes(), kc2.inner.primary_keypair.to_bytes());
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
        // Note this is Ok and not an Err because since we don't have a passphrase the wrong one is
        // just ignored.
        assert!(KeyConfig::read_config(
            &tmp_dir.path().to_path_buf(),
            Some("not_passphrase".to_string())
        )
        .is_ok());
    }
}
