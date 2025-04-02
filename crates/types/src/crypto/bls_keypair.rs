use rand::{CryptoRng, RngCore};

use super::{BlsPublicKey, BlsSignature, Signer};
use blst::min_sig::SecretKey as BlsPrivateKey;

/// Validator's main protocol keypair.
#[derive(Debug)]
pub struct BlsKeypair {
    public: BlsPublicKey,
    private: BlsPrivateKey,
}

pub const DST_G1: &[u8] = b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_NUL_"; // min sig
const _DST_G2: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_"; // min pk
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
