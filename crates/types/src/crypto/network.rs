//! Crypto functions to help with new node handshake using network keys.

use super::{IntentMessage, NetworkPublicKey, NetworkSignature, ProtocolSignature};
use crate::encode;
use fastcrypto::{
    error::FastCryptoError,
    traits::{Signer, VerifyingKey as _},
};
use serde::Serialize;

impl ProtocolSignature for NetworkSignature {
    type Pubkey = NetworkPublicKey;

    fn new_secure<T>(value: &IntentMessage<T>, secret: &dyn Signer<Self>) -> Self
    where
        T: Serialize,
    {
        let message = encode(&value);
        secret.sign(&message)
    }

    fn verify_secure<T>(
        &self,
        value: &IntentMessage<T>,
        public_key: &NetworkPublicKey,
    ) -> Result<(), FastCryptoError>
    where
        T: Serialize,
    {
        let message = encode(&value);
        public_key.verify(&message, self)
    }
}
