//! Parse a BLS public key from the two textual encodings in circulation.

use tn_types::BlsPublicKey;

/// Why a string failed to parse as a [`BlsPublicKey`].
///
/// Each variant names the first check the input failed, so a caller can print a
/// message that says *what* was wrong ("96 or 192 bytes expected, got 95") rather
/// than an opaque curve error.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ParseBlsPubkeyError {
    /// The input was empty (or only whitespace).
    #[error("bls public key is empty")]
    Empty,
    /// The input had a `0x` prefix but the remainder was not hex.
    #[error("bls public key is not valid hex: {0}")]
    InvalidHex(String),
    /// The input had no `0x` prefix and was not valid base58.
    #[error("bls public key is not valid base58: {0}")]
    InvalidBase58(String),
    /// The decoded bytes have a length that can never be a G2 point encoding.
    #[error(
        "bls public key must decode to {compressed} (compressed) or {uncompressed} (uncompressed) \
         bytes, got {actual}"
    )]
    InvalidLength {
        /// The number of bytes the input decoded to.
        actual: usize,
        /// [`BlsPublicKey::COMPRESSED_BYTES`].
        compressed: usize,
        /// [`BlsPublicKey::UNCOMPRESSED_BYTES`].
        uncompressed: usize,
    },
    /// The bytes had a plausible length but do not encode a valid G2 point.
    #[error("bls public key bytes do not encode a valid BLS12-381 G2 point")]
    InvalidPoint,
}

/// Parse a [`BlsPublicKey`] from either of its textual encodings.
///
/// - `0x`/`0X`-prefixed input is hex: the form contract calldata and the `tn_*` RPC namespace use.
/// - Anything else is base58: the canonical `Display`/serde form.
///
/// The `0x` prefix is a total discriminator because bs58's alphabet excludes
/// `0`, so no base58 key can start with it. Both the 96-byte compressed and
/// 192-byte uncompressed G2 encodings are accepted; the returned key's
/// `as_ref()` is always the 96 compressed bytes that form the kad record key.
///
/// Surrounding whitespace is trimmed. Length is screened with
/// [`BlsPublicKey::is_plausible_encoding`] before the curve check so a length
/// mistake reports as a length error rather than an opaque `BLST_ERROR`.
pub fn parse_bls_pubkey(input: &str) -> Result<BlsPublicKey, ParseBlsPubkeyError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(ParseBlsPubkeyError::Empty);
    }

    let bytes = match input.strip_prefix("0x").or_else(|| input.strip_prefix("0X")) {
        Some(hex_body) => {
            hex::decode(hex_body).map_err(|e| ParseBlsPubkeyError::InvalidHex(e.to_string()))?
        }
        None => bs58::decode(input)
            .into_vec()
            .map_err(|e| ParseBlsPubkeyError::InvalidBase58(e.to_string()))?,
    };

    if !BlsPublicKey::is_plausible_encoding(&bytes) {
        return Err(ParseBlsPubkeyError::InvalidLength {
            actual: bytes.len(),
            compressed: BlsPublicKey::COMPRESSED_BYTES,
            uncompressed: BlsPublicKey::UNCOMPRESSED_BYTES,
        });
    }

    BlsPublicKey::from_literal_bytes(&bytes).map_err(|_| ParseBlsPubkeyError::InvalidPoint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use tn_types::BlsKeypair;

    fn key() -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::from_seed([7u8; 32])).public()
    }

    #[test]
    fn base58_and_hex_parse_to_the_same_key() {
        let key = key();
        let base58 = key.to_string();
        let hex = format!("0x{}", hex::encode(key.as_ref()));

        assert_eq!(parse_bls_pubkey(&base58).expect("base58"), key);
        assert_eq!(parse_bls_pubkey(&hex).expect("hex"), key);
        // upper-case prefix and surrounding whitespace are tolerated
        assert_eq!(parse_bls_pubkey(&format!("  0X{}\n", &hex[2..])).expect("0X"), key);
        assert_eq!(parse_bls_pubkey(&format!(" {base58} ")).expect("trimmed"), key);
    }

    #[test]
    fn uncompressed_hex_normalizes_to_compressed_key() {
        let key = key();
        // `BlsPublicKey` implements serde `Serialize`, so reach blst's inherent `serialize`
        // (the 192-byte uncompressed form) through the deref
        let uncompressed = (*key).serialize();
        assert_eq!(uncompressed.len(), BlsPublicKey::UNCOMPRESSED_BYTES);
        let parsed = parse_bls_pubkey(&format!("0x{}", hex::encode(uncompressed))).expect("parses");
        assert_eq!(parsed, key);
        assert_eq!(parsed.as_ref().len(), BlsPublicKey::COMPRESSED_BYTES);
    }

    #[test]
    fn empty_input_is_distinct() {
        assert_eq!(parse_bls_pubkey(""), Err(ParseBlsPubkeyError::Empty));
        assert_eq!(parse_bls_pubkey("   "), Err(ParseBlsPubkeyError::Empty));
    }

    #[test]
    fn hex_errors_are_reported_as_hex() {
        assert!(matches!(parse_bls_pubkey("0xzz"), Err(ParseBlsPubkeyError::InvalidHex(_))));
        // odd length is a hex error, not a length error
        assert!(matches!(parse_bls_pubkey("0xabc"), Err(ParseBlsPubkeyError::InvalidHex(_))));
        // a bare `0x` decodes to zero bytes, which is a length error
        assert!(matches!(
            parse_bls_pubkey("0x"),
            Err(ParseBlsPubkeyError::InvalidLength { actual: 0, .. })
        ));
    }

    #[test]
    fn base58_errors_are_reported_as_base58() {
        // `0` and `l` are not in the bs58 alphabet
        assert!(matches!(parse_bls_pubkey("0abc"), Err(ParseBlsPubkeyError::InvalidBase58(_))));
        assert!(matches!(parse_bls_pubkey("abcl"), Err(ParseBlsPubkeyError::InvalidBase58(_))));
    }

    #[test]
    fn wrong_length_is_a_length_error() {
        let short = format!("0x{}", hex::encode([1u8; 95]));
        assert_eq!(
            parse_bls_pubkey(&short),
            Err(ParseBlsPubkeyError::InvalidLength {
                actual: 95,
                compressed: 96,
                uncompressed: 192
            })
        );
        let long = bs58::encode([1u8; 97]).into_string();
        assert!(matches!(
            parse_bls_pubkey(&long),
            Err(ParseBlsPubkeyError::InvalidLength { actual: 97, .. })
        ));
    }

    #[test]
    fn right_length_but_not_a_point_is_rejected() {
        let junk = format!("0x{}", hex::encode([0xffu8; 96]));
        assert_eq!(parse_bls_pubkey(&junk), Err(ParseBlsPubkeyError::InvalidPoint));
    }
}
