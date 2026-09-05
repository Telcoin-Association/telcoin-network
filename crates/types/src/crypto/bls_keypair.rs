use rand::{CryptoRng, RngCore};

use super::{BlsPublicKey, BlsSignature, Signer};
use blst::min_sig::SecretKey as BlsPrivateKey;
use std::fmt;

/// Validator's main protocol keypair.
///
/// `Debug` is implemented by hand rather than derived: `blst`'s `SecretKey` derives a plain
/// `Debug` over its raw scalar, so a derive here would print the validator's signing key in
/// full to whatever sink the formatting lands in (a log line, an error context, a panic
/// payload). Keep the private half redacted.
pub struct BlsKeypair {
    public: BlsPublicKey,
    private: BlsPrivateKey,
}

impl fmt::Debug for BlsKeypair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlsKeypair")
            .field("public", &self.public)
            .field("private", &"[REDACTED]")
            .finish()
    }
}

pub const DST_G1: &[u8] = b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_NUL_"; // min sig
impl BlsKeypair {
    pub fn public(&self) -> &BlsPublicKey {
        &self.public
    }

    pub fn generate<R: CryptoRng + RngCore>(rng: &mut R) -> Self {
        let mut ikm = [0u8; 32];
        rng.fill_bytes(&mut ikm);
        let private = BlsPrivateKey::key_gen(&ikm, &[]).expect("ikm length should be higher");
        let pubkey = private.sk_to_pk();
        let mut bytes = [0_u8; 96];
        bytes.copy_from_slice(&pubkey.to_bytes());
        Self { public: pubkey.into(), private }
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.private.to_bytes()
    }

    pub fn from_bytes(bytes: &[u8]) -> eyre::Result<Self> {
        let private = BlsPrivateKey::from_bytes(bytes)
            .map_err(|_| eyre::eyre!("invalid bls private key bytes!"))?;
        let pubkey = private.sk_to_pk();
        Ok(Self { public: pubkey.into(), private })
    }

    pub fn copy(&self) -> Self {
        Self { public: self.public, private: self.private.clone() }
    }
}

impl Signer for BlsKeypair {
    fn sign(&self, msg: &[u8]) -> BlsSignature {
        self.private.sign(msg, DST_G1, &[]).into()
    }
}

impl Signer for BlsPrivateKey {
    fn sign(&self, msg: &[u8]) -> BlsSignature {
        self.sign(msg, DST_G1, &[]).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};

    #[test]
    fn debug_does_not_leak_the_private_key() {
        let keypair = BlsKeypair::generate(&mut StdRng::from_os_rng());
        let rendered = format!("{keypair:?}");

        assert!(rendered.contains("[REDACTED]"), "private half must be redacted: {rendered}");

        // The scalar must not appear in any encoding the derived impl (or a careless future
        // one) would have produced. `to_bytes()` is the big-endian serialized scalar, whereas
        // the derived impl prints blst's internal little-endian bytes (`blst_scalar` wraps a
        // plain `[u8; 32]`), so run every check over both byte orders.
        let assert_scalar_absent = |bytes: &[u8], order: &str| {
            assert!(!rendered.contains(&hex::encode(bytes)), "private key leaked as {order} hex");
            assert!(
                !rendered.contains(&bs58::encode(bytes).into_string()),
                "private key leaked as {order} bs58"
            );
            assert!(
                !rendered.contains(&format!("{bytes:?}")),
                "private key leaked as a {order} byte-array debug"
            );
        };
        let private = keypair.to_bytes();
        let reversed: Vec<u8> = private.iter().rev().copied().collect();
        assert_scalar_absent(&private, "big-endian");
        assert_scalar_absent(&reversed, "little-endian");
        // The derive's wrapper names cannot appear if the field is redacted.
        assert!(!rendered.contains("blst_scalar"), "raw blst scalar leaked: {rendered}");
        assert!(!rendered.contains("SecretKey"), "blst SecretKey debug leaked: {rendered}");

        // The public half is not secret and stays useful for diagnostics: pin the rendered
        // value (the bs58 Display string), not just the `public` field label.
        assert!(rendered.contains("public"), "public half should still be shown: {rendered}");
        assert!(
            rendered.contains(&keypair.public().to_string()),
            "public key value should still be shown: {rendered}"
        );
    }
}
