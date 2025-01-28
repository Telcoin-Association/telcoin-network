//! This contains the encode/decode (serialize/deserialize) functions.
//!
//! These should be used to
//! allow for one place to examine and change. Note the normal and "key" versions, the key versions
//! have the added requirement that the produced bytes will binary sort correctly when used
//! as a DB key.  They do not need be used for anything else, bincode with the with_fixint_encoding
//! option provides this. However bincode can not handle some of our structures so we use bcs for
//! non keys.  BCS encoding however does not meet the sorting requirements for DB keys so we have
//! both encodings.  This can be experimented with by changing these functions.

pub use bcs::Error as BcsError;
use bincode::Options;
use serde::{Deserialize, Serialize};

/// Decode bytes to a type for a DB key.
///
/// This version will panic on failure, use with data that should be valid.
/// The binary format for a DB key should sort correctly (the with_fixint_encoding()).
/// This proper sorting MUST be maintained else stuff will break.
pub fn decode_key<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .deserialize(bytes)
        .expect("Invalid bytes!")
}

/// Decode bytes to a type for a DB key.
///
/// The binary format for a DB key should sort correctly (the with_fixint_encoding()).
/// This proper sorting MUST be maintained else stuff will break.
pub fn try_decode_key<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> eyre::Result<T> {
    Ok(bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .deserialize(bytes)?)
}

/// Encode an object to byte vector.
///
/// This is for use with DB keys and should produce bytes that can be
/// binary sorted correctly (the with_fixint_encoding).
pub fn encode_key<T: Serialize>(obj: &T) -> Vec<u8> {
    bincode::DefaultOptions::new()
        .with_big_endian()
        .with_fixint_encoding()
        .serialize(obj)
        .expect("Can not serialize!")
}

/// Decode bytes to a type.
///
/// This version will panic on failure, use with data that should be valid.
/// This version will be optimized without regard to binary sort order.
pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> T {
    bcs::from_bytes(bytes).expect("Invalid bytes!")
}

/// Decode bytes to a type.
///
/// This version will be optimized without regard to binary sort order.
pub fn try_decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> bcs::Result<T> {
    bcs::from_bytes(bytes)
}

/// Encode an object to a byte vector.
///
/// This version will be optimized without regard to binary sort order.
pub fn encode<T: Serialize>(obj: &T) -> Vec<u8> {
    bcs::to_bytes(obj).unwrap_or_else(|_| panic!("Serialization should not fail"))
}

/// Encode into a provided buffer.
pub fn encode_into_buffer<W, T>(write: &mut W, value: &T) -> bcs::Result<()>
where
    W: ?Sized + std::io::Write,
    T: ?Sized + Serialize,
{
    bcs::serialize_into(write, value)
}
