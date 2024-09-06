// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use serde::{
    de::{Deserializer, Error},
    ser::{Error as SerError, Serializer},
};
use serde_with::{Bytes, DeserializeAs, SerializeAs};

// Serializes a bitmap according to the roaring bitmap on-disk standard.
/// https://github.com/RoaringBitmap/RoaringFormatSpec
pub struct NarwhalBitmap;

impl SerializeAs<roaring::RoaringBitmap> for NarwhalBitmap {
    fn serialize_as<S>(source: &roaring::RoaringBitmap, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut bytes = vec![];

        source.serialize_into(&mut bytes).map_err(|e| {
            S::Error::custom(format!("roaring bitmap serialization failed: {:?}", e))
        })?;
        Bytes::serialize_as(&bytes, serializer)
    }
}

impl<'de> DeserializeAs<'de, roaring::RoaringBitmap> for NarwhalBitmap {
    fn deserialize_as<D>(deserializer: D) -> Result<roaring::RoaringBitmap, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Bytes::deserialize_as(deserializer)?;
        roaring::RoaringBitmap::deserialize_from(&bytes[..])
            .map_err(|e| Error::custom(format!("roaring bitmap deserialization failed: {:?}", e)))
    }
}
