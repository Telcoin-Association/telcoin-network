//! Serialize and deserialize roaring bitmap used by certificates.

use std::fmt;

use serde::{
    de::Deserializer,
    ser::{Error as SerError, Serializer},
};
use serde_with::{DeserializeAs, SerializeAs};

/// Serde interface to RoaringBitmap according to the roaring bitmap on-disk standard.
pub struct RoaringBitmapSerde;

impl SerializeAs<roaring::RoaringBitmap> for RoaringBitmapSerde {
    fn serialize_as<S>(source: &roaring::RoaringBitmap, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut bytes = vec![];

        source
            .serialize_into(&mut bytes)
            .map_err(|e| S::Error::custom(format!("roaring bitmap serialization failed: {e:?}")))?;
        if serializer.is_human_readable() {
            serializer.serialize_str(&bs58::encode(&bytes).into_string())
        } else {
            serializer.serialize_bytes(&bytes)
        }
    }
}

impl<'de> DeserializeAs<'de, roaring::RoaringBitmap> for RoaringBitmapSerde {
    fn deserialize_as<D>(deserializer: D) -> Result<roaring::RoaringBitmap, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::*;

        struct RBVisitor;

        impl Visitor<'_> for RBVisitor {
            type Value = roaring::RoaringBitmap;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "valid roaring bitmap bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                roaring::RoaringBitmap::deserialize_from(v).map_err(|e| {
                    Error::custom(format!("roaring bitmap deserialization failed: {e:?}"))
                })
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let bytes = bs58::decode(v)
                    .into_vec()
                    .map_err(|_| Error::invalid_value(Unexpected::Str(v), &self))?;
                self.visit_bytes(&bytes)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(RBVisitor)
        } else {
            deserializer.deserialize_bytes(RBVisitor)
        }
    }
}
