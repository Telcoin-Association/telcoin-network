//! This contains the encode/decode (serialize/deserialize) functions.
//!
//! These should be used to
//! allow for one place to examine and change. Note the normal and "key" versions, the key versions
//! have the added requirement that the produced bytes will binary sort correctly when used
//! as a DB key.  They do not need be used for anything else, bincode with the with_fixint_encoding
//! option provides this. However bincode can not handle some of our structures so we use bcs for
//! non keys.  BCS encoding however does not meet the sorting requirements for DB keys so we have
//! both encodings.  This can be experimented with by changing these functions.

use std::io::Read;

pub use bcs::Error as BcsError;
use bincode::Options;

/// Extracts the next element of a hand-written positional (bcs) field sequence, converting an
/// early end of input into a field-labeled `missing_field` error (bcs would otherwise surface
/// only a distal `Eof`). Shared by the epoch-gated `Header` and `CommittedSubDag` visitors.
pub(crate) fn next_seq_field<'de, A, T>(seq: &mut A, field: &'static str) -> Result<T, A::Error>
where
    A: serde::de::SeqAccess<'de>,
    T: serde::Deserialize<'de>,
{
    seq.next_element()?.ok_or_else(|| serde::de::Error::missing_field(field))
}
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Serde helpers that encode a byte vector as one byte string (`serialize_bytes`) instead of as a
/// sequence of `u8`. Use as `#[serde(with = "tn_types::byte_vec")]` on a `Vec<u8>` field.
///
/// serde has no byte specialization: a plain `Vec<u8>` serializes element by element, and `bcs`
/// writes (and reads back) each element with its own call, so a large byte vector costs a call per
/// byte. A byte string is one length prefix and one copy. The two are byte-identical in `bcs`
/// (`ULEB128(len)` then the raw bytes, with the same length limit on decode), so switching a field
/// to this leaves its encoding, and any digest over it, unchanged. Not for fixed `[u8; N]` arrays,
/// which `bcs` writes without a length prefix.
pub mod byte_vec {
    use serde::{de, Deserializer, Serializer};
    use std::fmt;

    /// Serialize `bytes` as one byte string.
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }

    /// Deserialize a byte vector written by [`serialize`], or as a sequence of `u8` by a format
    /// that presents bytes that way.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_byte_buf(ByteVecVisitor)
    }

    struct ByteVecVisitor;

    impl<'de> de::Visitor<'de> for ByteVecVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a byte vector")
        }

        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
            Ok(v.to_vec())
        }

        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
            Ok(v)
        }

        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
            // Never trust a declared length for the allocation size.
            let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(4096));
            while let Some(byte) = seq.next_element::<u8>()? {
                bytes.push(byte);
            }
            Ok(bytes)
        }
    }
}

/// A byte vector that serializes as one byte string (see [`byte_vec`]), for byte vectors held in a
/// collection, where a field attribute cannot reach.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ByteVec(pub Vec<u8>);

impl Serialize for ByteVec {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        byte_vec::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for ByteVec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        byte_vec::deserialize(deserializer).map(Self)
    }
}

/// A borrowed byte slice that serializes as one byte string (see [`byte_vec`]), for serializing
/// byte vectors held in a collection without copying them into [`ByteVec`]s.
#[derive(Clone, Copy, Debug)]
pub struct ByteSlice<'a>(pub &'a [u8]);

impl Serialize for ByteSlice<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        byte_vec::serialize(self.0, serializer)
    }
}

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

/// Decode a Read instance to a type.
///
/// This version will be optimized without regard to binary sort order.
pub fn try_decode_from_read<T: DeserializeOwned, R: Read>(read: R) -> bcs::Result<T> {
    bcs::from_reader(read)
}

/// Encode an object to a byte vector.
///
/// This version will be optimized without regard to binary sort order.
pub fn encode<T: Serialize>(obj: &T) -> Vec<u8> {
    bcs::to_bytes(obj).unwrap_or_else(|_| panic!("Serialization should not fail"))
}

/// Return the BCS-encoded byte length of `obj` without allocating the bytes.
///
/// Runs the same serializer as [`encode`] over a byte counter, so `encoded_size(x)`
/// equals `encode(x).len()` for any value that serializes, and surfaces the serializer
/// error instead of panicking. Use it where only the size is needed (e.g. streaming
/// byte-cap accounting) to avoid the throwaway allocation of encoding just to read `.len()`.
pub fn encoded_size<T: ?Sized + Serialize>(obj: &T) -> bcs::Result<usize> {
    bcs::serialized_size(obj)
}

/// Encode into a provided buffer.
pub fn encode_into_buffer<W, T>(write: &mut W, value: &T) -> bcs::Result<()>
where
    W: ?Sized + std::io::Write,
    T: ?Sized + Serialize,
{
    bcs::serialize_into(write, value)
}
