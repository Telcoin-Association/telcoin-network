//! Serialize and deserialize roaring bitmap used by certificates.

use std::fmt;

use serde::{
    de::Deserializer,
    ser::{Error as SerError, Serializer},
};
use serde_with::{DeserializeAs, SerializeAs};

/// Roaring's portable serialization cookie for a bitmap with no run containers; the container
/// count follows it as a separate `u32`.
const ROARING_SERIAL_COOKIE_NO_RUNCONTAINER: u32 = 12346;

/// Roaring's portable serialization cookie for a bitmap that may contain run containers; the
/// container count is packed into the cookie word's high 16 bits as `count - 1`.
const ROARING_SERIAL_COOKIE: u32 = 12347;

/// Upper bound on the container count accepted when deserializing a bitmap.
///
/// Every field using [`RoaringBitmapSerde`] stores committee authority *indices*: they are
/// produced by `enumerate()` over the committee and read back by matching against
/// `committee.iter().enumerate()`, so the values are dense from zero and bounded by the
/// committee size. Roaring partitions by the high 16 bits of each value, so a well-formed
/// bitmap here occupies exactly one container (or zero, when empty). The bound is set a little
/// above that so a committee spanning more than one container would still round-trip, while
/// capping the ~8 KiB-per-container heap that `deserialize_from` allocates from a container
/// count the sender chose.
///
/// Stated explicitly: a committee of more than 4 * 65_536 = 262_144 authorities would fail to
/// round-trip under this bound. No realistic configuration approaches that size.
///
/// Without this bound the count is attacker-controlled and the wire size does not constrain it:
/// the libp2p codec caps *serialized* bytes, and a serialized bitmap is orders of magnitude
/// smaller than the containers it expands into.
const MAX_BITMAP_CONTAINERS: u64 = 4;

/// Number of containers declared in a serialized [`roaring::RoaringBitmap`] header, read without
/// materializing the bitmap.
///
/// Roaring's portable format (roaring 0.10.12, `bitmap/serialization.rs`) opens with either
/// `ROARING_SERIAL_COOKIE_NO_RUNCONTAINER` followed by a `u32` count, or `ROARING_SERIAL_COOKIE`
/// with `count - 1` packed into the cookie word's high 16 bits; this reads only that count so a
/// caller can bound a bitmap's size before allocating its containers. Mirrors the header parsing
/// in `RoaringBitmap::deserialize_from`. Errors on a truncated header or an unrecognized cookie,
/// both of which `deserialize_from` would also reject.
///
/// This is the single source of truth for the header parse: `RoaringBitmapSerde` bounds
/// certificate bitmaps with it here, and the primary network's `MissingCertificatesRequest`
/// bounds its `skip_rounds` bitmaps with it.
pub fn roaring_container_count(serialized: &[u8]) -> Result<u64, &'static str> {
    let le_u32 = |start: usize| -> Result<u32, &'static str> {
        serialized
            .get(start..start.saturating_add(4))
            .and_then(|word| <[u8; 4]>::try_from(word).ok())
            .map(u32::from_le_bytes)
            .ok_or("roaring bitmap header truncated")
    };

    let cookie = le_u32(0)?;
    if cookie == ROARING_SERIAL_COOKIE_NO_RUNCONTAINER {
        le_u32(4).map(u64::from)
    } else if cookie & 0xFFFF == ROARING_SERIAL_COOKIE {
        Ok(u64::from(cookie >> 16) + 1)
    } else {
        Err("roaring bitmap has an unrecognized cookie")
    }
}

/// Serde interface to RoaringBitmap according to the roaring bitmap on-disk standard.
pub(crate) struct RoaringBitmapSerde;

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

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "valid roaring bitmap bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                // Reject a decompression bomb *before* `deserialize_from` materializes it. The
                // header declares how many containers to allocate, and that count is chosen by
                // whoever sent the bytes, so it must be bounded here rather than relying on the
                // transport's serialized-size cap.
                let containers = roaring_container_count(v).map_err(Error::custom)?;
                if containers > MAX_BITMAP_CONTAINERS {
                    return Err(Error::custom(format!(
                        "roaring bitmap declares too many containers: {containers} > \
                         {MAX_BITMAP_CONTAINERS}"
                    )));
                }

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

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;
    use serde::{Deserialize, Serialize};
    use serde_with::serde_as;

    /// Stand-in for the certificate fields that carry a bitmap over the wire.
    #[serde_as]
    #[derive(Debug, Serialize, Deserialize)]
    struct Wrapper {
        #[serde_as(as = "RoaringBitmapSerde")]
        bitmap: RoaringBitmap,
    }

    fn serialized(bitmap: &RoaringBitmap) -> Vec<u8> {
        let mut bytes = vec![];
        bitmap.serialize_into(&mut bytes).expect("serialize");
        bytes
    }

    /// A bitmap spanning `n` containers: roaring partitions on the high 16 bits, so stepping by
    /// 65_536 puts each value in a container of its own.
    fn spanning(n: u32) -> RoaringBitmap {
        let mut bitmap = RoaringBitmap::new();
        for i in 0..n {
            bitmap.insert(i * 65_536);
        }
        bitmap
    }

    #[test]
    fn committee_sized_bitmap_round_trips() {
        // Authority indices are dense from zero, so even a large committee stays in container 0.
        let mut bitmap = RoaringBitmap::new();
        for i in 0..1_000 {
            bitmap.insert(i);
        }
        assert_eq!(roaring_container_count(&serialized(&bitmap)), Ok(1));

        let encoded = bcs::to_bytes(&Wrapper { bitmap: bitmap.clone() }).expect("encode");
        let decoded: Wrapper = bcs::from_bytes(&encoded).expect("decode");
        assert_eq!(decoded.bitmap, bitmap);
    }

    #[test]
    fn empty_bitmap_round_trips() {
        let bitmap = RoaringBitmap::new();
        assert_eq!(roaring_container_count(&serialized(&bitmap)), Ok(0));

        let encoded = bcs::to_bytes(&Wrapper { bitmap: bitmap.clone() }).expect("encode");
        let decoded: Wrapper = bcs::from_bytes(&encoded).expect("decode");
        assert_eq!(decoded.bitmap, bitmap);
    }

    #[test]
    fn bitmap_at_the_container_limit_is_accepted() {
        let bitmap = spanning(MAX_BITMAP_CONTAINERS as u32);
        let encoded = bcs::to_bytes(&Wrapper { bitmap: bitmap.clone() }).expect("encode");
        let decoded: Wrapper = bcs::from_bytes(&encoded).expect("decode");
        assert_eq!(decoded.bitmap, bitmap);
    }

    #[test]
    fn bitmap_over_the_container_limit_is_rejected() {
        let bitmap = spanning(MAX_BITMAP_CONTAINERS as u32 + 1);
        assert_eq!(
            roaring_container_count(&serialized(&bitmap)),
            Ok(MAX_BITMAP_CONTAINERS + 1),
            "test fixture should exceed the bound"
        );

        // Encoding is deliberately unguarded (we only ever serialize our own bitmaps); the
        // rejection must happen on the way back in, which is where remote bytes arrive.
        let encoded = bcs::to_bytes(&Wrapper { bitmap }).expect("encode");
        let err = bcs::from_bytes::<Wrapper>(&encoded).expect_err("must reject");
        assert!(err.to_string().contains("too many containers"), "unexpected error: {err}");
    }

    /// The bomb this bound exists to stop: a header declaring far more containers than any
    /// committee could justify, in a payload small enough to clear the transport size cap.
    #[test]
    fn declared_container_count_is_bounded_before_allocating() {
        let mut header = Vec::new();
        header.extend_from_slice(&ROARING_SERIAL_COOKIE_NO_RUNCONTAINER.to_le_bytes());
        header.extend_from_slice(&50_000_u32.to_le_bytes());

        assert_eq!(roaring_container_count(&header), Ok(50_000));

        let json = serde_json::to_string(&Wrapper { bitmap: RoaringBitmap::new() }).expect("json");
        let poisoned = json.replace(
            &bs58::encode(serialized(&RoaringBitmap::new())).into_string(),
            &bs58::encode(&header).into_string(),
        );
        let err = serde_json::from_str::<Wrapper>(&poisoned).expect_err("must reject");
        assert!(err.to_string().contains("too many containers"), "unexpected error: {err}");
    }

    #[test]
    fn truncated_or_unrecognized_header_is_rejected() {
        assert_eq!(roaring_container_count(&[]), Err("roaring bitmap header truncated"));
        assert_eq!(roaring_container_count(&[0, 1, 2]), Err("roaring bitmap header truncated"));
        assert_eq!(
            roaring_container_count(&ROARING_SERIAL_COOKIE_NO_RUNCONTAINER.to_le_bytes()),
            Err("roaring bitmap header truncated"),
            "cookie present but the count word is missing"
        );
        assert_eq!(
            roaring_container_count(&[0xFF, 0xFF, 0xFF, 0xFF]),
            Err("roaring bitmap has an unrecognized cookie")
        );
    }

    #[test]
    fn run_container_cookie_count_is_read_correctly() {
        // count - 1 packed into the high 16 bits.
        let cookie = ROARING_SERIAL_COOKIE | (2_u32 << 16);
        assert_eq!(roaring_container_count(&cookie.to_le_bytes()), Ok(3));
    }
}
