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
// A dependency only for its `zeroize` feature, which clears the expanded AES round-key
// schedule (invertible back to the wrapping key) when the cipher objects below drop.
use aes as _;
use aes_gcm_siv::{aead::Aead as _, Aes256GcmSiv, Key, KeyInit, Nonce};
use pbkdf2::pbkdf2_hmac;
use rand::{rngs::StdRng, Rng as _, SeedableRng};
use sha2::Sha256;
use std::sync::Arc;
use tn_types::{
    construct_proof_of_possession_message, Address, BlsKeypair, BlsPublicKey, BlsSignature,
    BlsSigner, DefaultHashFunction, NetworkKeypair, NetworkPublicKey, Signer, WorkerId,
};
use zeroize::Zeroizing;

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

/// Create the node-keys directory owner-only (0700), creating parent directories as needed.
///
/// `std::fs::create_dir` applies `0o777 & !umask`, i.e. 0755 at the usual umask of 022, which
/// leaves the key directory world-traversable. A directory that already exists but is still
/// empty is tightened to 0700: it holds no key material yet, so its mode is this code's to
/// set (the CLI pre-creates the path before the key writer runs). A populated directory's
/// mode belongs to the operator and is left alone; the read path warns about it instead.
///
/// Exported for the CLI's `keytool generate` flow, which prepares the directory before
/// [`KeyConfig::generate_and_save`] runs.
pub fn create_keys_dir(path: &std::path::Path) -> std::io::Result<()> {
    // The parent chain (the datadir) holds no key material; default modes are fine there.
    path.parent().map_or(Ok(()), std::fs::create_dir_all)?;
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        builder.mode(0o700);
    }
    builder.create(path).or_else(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            tighten_empty_keys_dir(path)
        } else {
            Err(e)
        }
    })
}

/// Tighten an existing key directory to 0700 while it is still empty.
///
/// Covers the `AlreadyExists` case of [`create_keys_dir`]: an empty directory holds no
/// operator-managed key material yet, so restoring the owner-only default cannot conflict
/// with a deliberate operator choice the way re-moding a populated deployment would.
#[cfg(unix)]
fn tighten_empty_keys_dir(path: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let is_empty = std::fs::read_dir(path)?.next().is_none();
    if is_empty {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn tighten_empty_keys_dir(_path: &std::path::Path) -> std::io::Result<()> {
    Ok(())
}

/// Write `contents` to `path` readable only by the owner (0600).
///
/// `std::fs::write` applies `0o666 & !umask`, i.e. 0644 at the usual umask of 022. For a
/// keyfile that is world-readable, and in the no-passphrase configuration the file is the BLS
/// private key in the clear.
///
/// The bytes go to a fresh sibling temp file (`<name>.tmp`) created owner-only, which is then
/// renamed over `path`. The mode is set at creation so there is no window in which the key
/// exists at a looser mode, and the pre-existing file is replaced only after the new contents
/// are fully on disk - a failure part-way (a filesystem that rejects the write, a crash)
/// leaves the old keyfile intact instead of truncated. A re-run over a keyfile written by an
/// older build replaces it with an owner-only one for the same reason.
fn write_secret_file(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write as _;

    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "secret file path has no file name")
    })?;
    let mut tmp_name = file_name.to_os_string();
    tmp_name.push(".tmp");
    let tmp_path = path.with_file_name(&tmp_name);

    // A run that died between creating and renaming the temp file leaves it behind; remove it
    // so `create_new` below can insist on a fresh file whose owner-only mode it controls.
    std::fs::remove_file(&tmp_path)
        .or_else(|e| (e.kind() == std::io::ErrorKind::NotFound).then_some(()).ok_or(e))?;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }

    let mut file = options.open(&tmp_path)?;
    file.write_all(contents.as_bytes())?;
    // Flush to disk before the rename: otherwise a crash could replace the old keyfile with
    // one whose contents never landed.
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp_path, path)
}

/// The group/other-accessible mode of `path`, `None` when the mode is owner-only or `path`
/// cannot be inspected (absent file). The returned mode is masked to the permission bits.
#[cfg(unix)]
fn loose_mode(path: &std::path::Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = std::fs::metadata(path).ok()?.permissions().mode();
    (mode & 0o077 != 0).then_some(mode & 0o7777)
}

/// Warn when existing key material is accessible by anyone other than its owner.
///
/// Keys and directories written by a build predating owner-only permissions keep their loose
/// mode - a populated deployment is never re-moded behind the operator's back - so tell the
/// operator instead of silently leaving a world-readable validator key on disk. Covers the
/// key directory, the keyfile about to be read, and (with a passphrase in use) a stale
/// cleartext keyfile left beside the wrapped one by a pre-passphrase deployment.
#[cfg(unix)]
fn warn_if_key_permissions_are_loose(
    keys_dir: &std::path::Path,
    keyfile: &std::path::Path,
    stale_cleartext: Option<&std::path::Path>,
) {
    if let Some(mode) = loose_mode(keys_dir) {
        warn_weak_kdf(&format!(
            "BLS key directory {} is accessible beyond its owner (mode {mode:o}); it was \
             created by an older build. Restrict it with: chmod 700 {}",
            keys_dir.display(),
            keys_dir.display()
        ));
    }
    if let Some(mode) = loose_mode(keyfile) {
        warn_weak_kdf(&format!(
            "BLS keyfile {} is readable beyond its owner (mode {mode:o}); it was written by an \
             older build. Restrict it with: chmod 600 {}",
            keyfile.display(),
            keyfile.display()
        ));
    }
    if let Some((path, mode)) = stale_cleartext.and_then(|p| loose_mode(p).map(|m| (p, m))) {
        warn_weak_kdf(&format!(
            "stale cleartext BLS keyfile {} lies beside the wrapped keyfile and is readable \
             beyond its owner (mode {mode:o}). Delete it, or restrict it with: chmod 600 {}",
            path.display(),
            path.display()
        ));
    }
}

#[cfg(not(unix))]
fn warn_if_key_permissions_are_loose(
    _keys_dir: &std::path::Path,
    _keyfile: &std::path::Path,
    _stale_cleartext: Option<&std::path::Path>,
) {
}

#[derive(Debug)]
struct KeyConfigInner {
    // DO NOT expose the private key to other code.  Tests that need this will provide a primary
    // key. Use the BlsSigner trait for signing for the primary.
    primary_keypair: BlsKeypair,
    // Derived from the primary_keypair.
    primary_network_keypair: NetworkKeypair,
    // Seed string for worker network keypairs. Per-worker keypairs are derived on demand from
    // the primary_keypair and this seed; see `KeyConfig::worker_network_keypair`.
    worker_network_seed: String,
}

/// Basic implementation of a key manager.  This version will read a BLS key
/// from a file (which is not ideal).  It is intended to be an interface that
/// can later expand to be backed with something more secure (like an HSM).
/// It should NOT expose the BLS private key, even though it is currently read
/// from a file this will not always be the case and all code needing signatures
/// MUST go through KeyConfig.
/// NOTE: The network keys (primary and per-worker) are derived from the BLS key
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
    ///
    /// Returned in a [`Zeroizing`] so the derived key is cleared when the caller drops it
    /// rather than being left in freed memory for a core dump or swap page to pick up.
    fn derive_wrapping_key(passphrase: &str, salt: &[u8], rounds: u32) -> Zeroizing<[u8; 32]> {
        let mut wrapping_key = Zeroizing::new([0_u8; 32]);
        pbkdf2_hmac::<Sha256>(passphrase.as_bytes(), salt, rounds, wrapping_key.as_mut());
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
        let key = Key::<Aes256GcmSiv>::from_slice(&wrapping_key[..]);
        let cipher = Aes256GcmSiv::new(key);
        let nonce = Nonce::from_slice(&nonce_bytes); // 96-bits

        // The raw scalar is only needed as AEAD input; hold it in a `Zeroizing` so the copy
        // `to_bytes` hands back does not outlive the encrypt call in freed memory.
        let key_bytes = Zeroizing::new(primary_keypair.to_bytes());
        let ciphertext = cipher
            .encrypt(nonce, &key_bytes[..])
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
        let key = Key::<Aes256GcmSiv>::from_slice(&wrapping_key[..]);
        let cipher = Aes256GcmSiv::new(key);
        // This is the BLS private key in the clear. `BlsKeypair` (via blst's `#[zeroize(drop)]`)
        // clears the parsed copy, so wrap the transient decrypt buffer to close the same gap.
        let plaintext = Zeroizing::new(cipher.decrypt(Nonce::from_slice(nonce), ciphertext).ok()?);
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
        let keys_dir = tn_datadir.node_keys_path();
        // If we don't have a wrapped file then try an unencrypted file before failure.
        let passphrase = if std::fs::exists(keys_dir.join(BLS_WRAPPED_KEYFILE)).unwrap_or(false) {
            passphrase
        } else {
            None
        };

        // load keys to start the primary
        let keyfile = if passphrase.is_some() {
            keys_dir.join(BLS_WRAPPED_KEYFILE)
        } else {
            keys_dir.join(BLS_KEYFILE)
        };
        // With a passphrase in use the cleartext keyfile is never read, but one left behind by
        // a pre-passphrase deployment is still the BLS private key on disk - point at it.
        let stale_cleartext = passphrase.is_some().then(|| keys_dir.join(BLS_KEYFILE));
        warn_if_key_permissions_are_loose(&keys_dir, &keyfile, stale_cleartext.as_deref());
        // In the no-passphrase branch `contents` and `bytes` hold the raw private key (Base58
        // and decoded), so both are wrapped in `Zeroizing` rather than left in freed memory.
        // The wrapped branch carries only AEAD-protected bytes; clearing those too is free.
        let contents = Zeroizing::new(std::fs::read_to_string(&keyfile)?);
        let primary_seed =
            std::fs::read_to_string(tn_datadir.node_keys_path().join(PRIMARY_NETWORK_SEED_FILE))
                .unwrap_or_else(|_| "primary network keypair".to_string());
        let worker_seed =
            std::fs::read_to_string(tn_datadir.node_keys_path().join(WORKER_NETWORK_SEED_FILE))
                .unwrap_or_else(|_| "worker network keypair".to_string());
        let bytes = Zeroizing::new(bs58::decode(contents.as_str().trim()).into_vec()?);
        let primary_keypair = if let Some(passphrase) = passphrase {
            Self::unwrap_bls_key(&bytes, &passphrase)?
        } else {
            BlsKeypair::from_bytes(&bytes)?
        };
        let primary_network_keypair =
            Self::generate_network_keypair(&primary_keypair, &primary_seed);
        Ok(Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair,
                worker_network_seed: worker_seed,
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
        // Make sure we have the validator dir, owner-only.
        // Don't error out if path exists.
        create_keys_dir(&tn_datadir.node_keys_path())?;
        if let Some(passphrase) = passphrase {
            let contents = Self::wrap_bls_key(&primary_keypair, &passphrase, rounds)?;
            write_secret_file(&tn_datadir.node_keys_path().join(BLS_WRAPPED_KEYFILE), &contents)?;
        } else {
            // This path persists the key in cleartext by design. Wrap the scalar copy and its
            // Base58 encoding so these two buffers are cleared on drop (stack temporaries made
            // inside `to_bytes` itself are beyond the caller's reach).
            let key_bytes = Zeroizing::new(primary_keypair.to_bytes());
            let contents = Zeroizing::new(bs58::encode(&key_bytes[..]).into_string());
            write_secret_file(&tn_datadir.node_keys_path().join(BLS_KEYFILE), &contents)?;
        }
        // The seed files hold fixed public strings rather than secrets, but there is no reason
        // for anything under node-keys to be world-readable.
        write_secret_file(
            &tn_datadir.node_keys_path().join(PRIMARY_NETWORK_SEED_FILE),
            primary_seed,
        )?;
        write_secret_file(
            &tn_datadir.node_keys_path().join(WORKER_NETWORK_SEED_FILE),
            worker_seed,
        )?;
        Ok(Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair,
                worker_network_seed: worker_seed.to_string(),
            }),
        })
    }

    /// Create a config with a provided key- this is ONLY for testing.
    pub fn new_with_testing_key(primary_keypair: BlsKeypair) -> Self {
        let primary_network_keypair =
            Self::generate_network_keypair(&primary_keypair, "primary network keypair");
        Self {
            inner: Arc::new(KeyConfigInner {
                primary_keypair,
                primary_network_keypair,
                worker_network_seed: "worker network keypair".to_string(),
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

    /// Provide the keypair (with private key) for the network of `worker_id`.
    /// Allows building the libp2p worker network.
    ///
    /// Worker 0 derives from the stored seed exactly as before per-worker swarms existed. This
    /// keeps worker 0's PeerId stable for deployed nodes: that network identity is advertised
    /// on-chain and cached in peers' kad stores, so it must not change. Worker ids above 0
    /// append the id to the seed to get a distinct keypair per swarm.
    pub fn worker_network_keypair(&self, worker_id: WorkerId) -> NetworkKeypair {
        if worker_id == 0 {
            Self::generate_network_keypair(
                &self.inner.primary_keypair,
                &self.inner.worker_network_seed,
            )
        } else {
            Self::generate_network_keypair(
                &self.inner.primary_keypair,
                &format!("{} {worker_id}", self.inner.worker_network_seed),
            )
        }
    }

    /// The [NetworkPublicKey] for the network of `worker_id`.
    pub fn worker_network_public_key(&self, worker_id: WorkerId) -> NetworkPublicKey {
        self.worker_network_keypair(worker_id).public().into()
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
        let cipher = Aes256GcmSiv::new(Key::<Aes256GcmSiv>::from_slice(&wrapping_key[..]));
        // Mirror the production `wrap_bls_key`: hold the raw scalar copy in a `Zeroizing` so
        // this buffer is cleared when it drops.
        let key_bytes = Zeroizing::new(keypair.to_bytes());
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce_bytes), &key_bytes[..])
            .expect("test_only encrypt");
        bs58::encode([&salt[..], &nonce_bytes[..], &ciphertext[..]].concat()).into_string()
    }

    /// Pin the wrapping key's type so a refactor cannot silently drop back to a bare
    /// `[u8; 32]` that is left in freed memory on drop.
    ///
    /// Zeroization itself is not observable from safe Rust (reading the freed page is UB and
    /// the optimizer is free to elide a plain overwrite), so this asserts on the type that
    /// carries the guarantee rather than on the cleared bytes.
    #[test]
    fn wrapping_key_is_zeroizing() {
        let salt = [0_u8; SALT_LEN];
        let wrapping_key: Zeroizing<[u8; 32]> =
            KeyConfig::derive_wrapping_key("passphrase", &salt, TEST_ONLY_INSECURE_ROUNDS);

        // Deriving twice with the same inputs is stable, so the wrapper does not disturb PBKDF2.
        let again = KeyConfig::derive_wrapping_key("passphrase", &salt, TEST_ONLY_INSECURE_ROUNDS);
        assert_eq!(*wrapping_key, *again);
        assert_ne!(*wrapping_key, [0_u8; 32], "derivation should produce key material");
    }

    /// Pin the PBKDF2-HMAC-SHA256 derivation to fixed known-answer vectors (computed with an
    /// independent PBKDF2 implementation). This pins the construction, the hash (SHA-256),
    /// the 32-byte output length, and the passphrase encoding; the production round count is
    /// pinned separately by `test_bls_passphrase_production_rounds`. The 1-round vector pins
    /// the base construction; the 10-round vector pins the iteration composition.
    #[test]
    fn derive_wrapping_key_known_answer() {
        let salt: [u8; SALT_LEN] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let expected_one_round: [u8; 32] = [
            233, 222, 218, 7, 33, 111, 58, 49, 75, 89, 219, 116, 1, 54, 137, 62, 204, 147, 55, 220,
            243, 124, 64, 73, 24, 247, 110, 231, 235, 135, 209, 242,
        ];
        let expected_ten_rounds: [u8; 32] = [
            47, 15, 188, 110, 71, 239, 224, 19, 18, 208, 6, 195, 178, 52, 200, 65, 76, 146, 210,
            47, 214, 0, 174, 192, 253, 114, 25, 128, 200, 216, 197, 237,
        ];
        assert_eq!(*KeyConfig::derive_wrapping_key("passphrase", &salt, 1), expected_one_round);
        assert_eq!(*KeyConfig::derive_wrapping_key("passphrase", &salt, 10), expected_ten_rounds);
    }

    /// Run `f` with the process umask set to 0, restoring it afterwards.
    ///
    /// The permission tests must not be able to pass merely because the developer's or CI
    /// runner's umask happened to strip the group and other bits: at umask 0 the only thing
    /// keeping a keyfile owner-only is the mode this code asks for explicitly.
    ///
    /// The umask is process-global, so these tests must not run concurrently with anything else
    /// that creates files. Rust runs tests in threads, hence the mutex.
    #[cfg(unix)]
    static UMASK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// RAII: set the process umask to 0, restore the previous value on drop.
    ///
    /// The restore must run on unwind too: the permission tests use `expect`, and without the
    /// `Drop` a failing test would leave the whole test binary running at umask 0,
    /// contaminating every later test that creates files.
    #[cfg(unix)]
    struct PermissiveUmask {
        previous: libc::mode_t,
    }

    #[cfg(unix)]
    impl PermissiveUmask {
        /// Zero the process umask; the caller must hold [`UMASK_LOCK`].
        fn set() -> Self {
            // SAFETY: `umask` is always successful and returns the previous mask.
            Self { previous: unsafe { libc::umask(0) } }
        }
    }

    #[cfg(unix)]
    impl Drop for PermissiveUmask {
        fn drop(&mut self) {
            // SAFETY: `umask` is always successful and returns the previous mask.
            unsafe { libc::umask(self.previous) };
        }
    }

    /// Run `f` with the process umask set to 0, restoring it afterwards (unwind included).
    #[cfg(unix)]
    fn with_permissive_umask<T>(f: impl FnOnce() -> T) -> T {
        let _lock = UMASK_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _umask = PermissiveUmask::set();
        f()
    }

    /// Every file the key writer creates must be owner-only, and so must the directory.
    #[cfg(unix)]
    #[test]
    fn generated_key_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp_dir = TempDir::new().expect("tempdir");
        let datadir = tmp_dir.path().to_path_buf();

        with_permissive_umask(|| {
            // The shared impl rather than `generate_and_save`, so the test keeps the fast KDF
            // without depending on the `test-utils` feature being enabled.
            KeyConfig::generate_and_save_with_rounds(
                &datadir,
                Some("passphrase".to_string()),
                TEST_ONLY_INSECURE_ROUNDS,
            )
            .expect("generate keys");
        });

        let keys_dir = datadir.node_keys_path();
        let dir_mode = std::fs::metadata(&keys_dir).expect("keys dir").permissions().mode();
        assert_eq!(
            dir_mode & 0o077,
            0,
            "node-keys dir is accessible beyond its owner: {:o}",
            dir_mode & 0o7777
        );

        for name in [BLS_WRAPPED_KEYFILE, PRIMARY_NETWORK_SEED_FILE, WORKER_NETWORK_SEED_FILE] {
            let path = keys_dir.join(name);
            let mode = std::fs::metadata(&path).expect(name).permissions().mode();
            assert_eq!(mode & 0o077, 0, "{name} is readable beyond its owner: {:o}", mode & 0o7777);
        }
    }

    /// The no-passphrase path writes the BLS private key in the clear, so it matters most.
    #[cfg(unix)]
    #[test]
    fn cleartext_key_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp_dir = TempDir::new().expect("tempdir");
        let datadir = tmp_dir.path().to_path_buf();

        with_permissive_umask(|| {
            KeyConfig::generate_and_save_with_rounds(&datadir, None, TEST_ONLY_INSECURE_ROUNDS)
                .expect("generate keys");
        });

        let path = datadir.node_keys_path().join(BLS_KEYFILE);
        let mode = std::fs::metadata(&path).expect("bls.key").permissions().mode();
        assert_eq!(
            mode & 0o077,
            0,
            "cleartext bls.key is readable beyond its owner: {:o}",
            mode & 0o7777
        );
    }

    /// A keyfile left loose by an older build is tightened rather than silently rewritten at
    /// the old mode.
    #[cfg(unix)]
    #[test]
    fn existing_loose_key_file_is_tightened_on_write() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp_dir = TempDir::new().expect("tempdir");
        let path = tmp_dir.path().join("bls.kw");

        std::fs::write(&path, "old").expect("seed file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");

        with_permissive_umask(|| {
            write_secret_file(&path, "new").expect("rewrite");
        });

        let mode = std::fs::metadata(&path).expect("metadata").permissions().mode();
        assert_eq!(mode & 0o077, 0, "pre-existing file kept a loose mode: {:o}", mode & 0o7777);
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "new");
    }

    /// The CLI pre-creates the key directory before `generate_and_save` runs, so the
    /// `AlreadyExists` case must tighten a still-empty directory rather than keep the loose
    /// mode `create_dir_all` gave it (regression: that case used to be a no-op, which made
    /// the 0700 mode dead for every fresh `tn keytool generate` install).
    ///
    /// The loose mode is set with an explicit chmod rather than via the umask, so the test is
    /// deterministic in any environment.
    #[cfg(unix)]
    #[test]
    fn empty_keys_dir_is_tightened_on_create() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp_dir = TempDir::new().expect("tempdir");
        let dir = tmp_dir.path().join("node-keys");
        std::fs::create_dir_all(&dir).expect("pre-create keys dir");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).expect("chmod");

        create_keys_dir(&dir).expect("create over empty dir");

        let mode = std::fs::metadata(&dir).expect("metadata").permissions().mode();
        assert_eq!(
            mode & 0o077,
            0,
            "empty pre-existing keys dir kept a loose mode: {:o}",
            mode & 0o7777
        );
    }

    /// A populated key directory's mode belongs to the operator: creation must leave it
    /// alone (the read path warns instead; an existing deployment is never re-moded).
    #[cfg(unix)]
    #[test]
    fn populated_keys_dir_mode_is_left_alone() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp_dir = TempDir::new().expect("tempdir");
        let dir = tmp_dir.path().join("node-keys");
        std::fs::create_dir_all(&dir).expect("pre-create keys dir");
        std::fs::write(dir.join(BLS_WRAPPED_KEYFILE), "existing key").expect("populate");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).expect("chmod");

        create_keys_dir(&dir).expect("create over populated dir");

        let mode = std::fs::metadata(&dir).expect("metadata").permissions().mode();
        assert_eq!(mode & 0o7777, 0o755, "populated keys dir mode must be left alone");
    }

    /// A temp file left behind by a run that died between write and rename must not wedge
    /// the next write, and the write must land through the rename (no temp file remains).
    #[test]
    fn stale_temp_file_does_not_block_secret_write() {
        let tmp_dir = TempDir::new().expect("tempdir");
        let path = tmp_dir.path().join(BLS_WRAPPED_KEYFILE);
        let tmp_path = tmp_dir.path().join("bls.kw.tmp");

        std::fs::write(&tmp_path, "half-written").expect("stale tmp");
        write_secret_file(&path, "fresh").expect("write over stale tmp");

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "fresh");
        assert!(!tmp_path.exists(), "temp file must be renamed over the target");
    }

    /// A failing permission test must not leave the whole test binary at umask 0: the
    /// restore has to run on unwind, not only on return.
    #[cfg(unix)]
    #[test]
    fn umask_is_restored_when_the_closure_panics() {
        // Hold the lock across the whole check so no other permission test's umask-0 window
        // can interleave between the unwind and the reads below.
        let _lock = UMASK_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        // SAFETY: `umask` is always successful; set-then-restore reads the current value.
        let before = unsafe { libc::umask(0o022) };
        unsafe { libc::umask(before) };

        let result = std::panic::catch_unwind(|| {
            let _umask = PermissiveUmask::set();
            panic!("intentional panic: exercising the unwind path");
        });
        assert!(result.is_err(), "the closure must have panicked");

        // SAFETY: `umask` is always successful; set-then-restore reads the current value.
        let after = unsafe { libc::umask(0o022) };
        unsafe { libc::umask(after) };
        assert_eq!(before, after, "umask must be restored when the closure unwinds");
    }

    /// `loose_mode` is the single predicate behind every read-time permission warning: it
    /// must flag group/other bits, stay quiet on owner-only modes, and treat an absent path
    /// as nothing to warn about.
    #[cfg(unix)]
    #[test]
    fn loose_mode_flags_only_group_and_other_bits() {
        use std::os::unix::fs::PermissionsExt as _;

        let tmp_dir = TempDir::new().expect("tempdir");
        let file = tmp_dir.path().join("keyfile");
        std::fs::write(&file, "key").expect("write");

        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).expect("chmod");
        assert_eq!(loose_mode(&file), None, "owner-only file must not be flagged");

        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o640)).expect("chmod");
        assert_eq!(loose_mode(&file), Some(0o640), "group-readable file must be flagged");

        let dir = tmp_dir.path().join("subdir");
        std::fs::create_dir(&dir).expect("mkdir");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o750)).expect("chmod");
        assert_eq!(loose_mode(&dir), Some(0o750), "group-traversable dir must be flagged");

        assert_eq!(loose_mode(&tmp_dir.path().join("absent")), None, "absent path is quiet");
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

    /// Worker 0 must keep the legacy bare-seed derivation (its PeerId is advertised on-chain),
    /// worker 1 must get a distinct keypair, and derivation must be deterministic per id.
    #[test]
    fn test_worker_network_keypair_per_id_derivation() {
        let kc = KeyConfig::new_with_testing_key(random_keypair());
        let legacy: NetworkPublicKey = KeyConfig::generate_network_keypair(
            &kc.inner.primary_keypair,
            "worker network keypair",
        )
        .public()
        .into();
        assert_eq!(kc.worker_network_public_key(0), legacy);
        assert_ne!(kc.worker_network_public_key(1), kc.worker_network_public_key(0));
        assert_eq!(kc.worker_network_public_key(1), kc.worker_network_public_key(1));
    }
}
