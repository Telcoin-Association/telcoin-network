//! Bloom type.
//!
//! Adapted from <https://github.com/alloy-rs/core/blob/main/crates/primitives/src/bits/bloom.rs>
//! Which was
//! Adapted from <https://github.com/paritytech/parity-common/blob/2fb72eea96b6de4a085144ce239feb49da0cd39e/ethbloom/src/lib.rs>
//!
//! This extends it to handle much larger bloom filters on 256 bit hashes.

//use crate::{keccak256, Address, Log, LogData, B256};

use std::{error::Error, fmt};

use tn_types::B256;

/// Number of bits to set per input in Ethereum bloom filter.
const BLOOM_BITS_PER_ITEM: usize = 8;
/// Size of the bloom filter in bytes- 2M.
pub const BLOOM_SIZE_BYTES: usize = 2 * 1024 * 1024;
/// Size of the bloom filter in bits
const BLOOM_SIZE_BITS: usize = BLOOM_SIZE_BYTES * 8;

/// Mask, used in accrue
const MASK: usize = BLOOM_SIZE_BITS - 1;
/// Number of bytes per item, used in accrue
const ITEM_BYTES: usize = BLOOM_SIZE_BITS.ilog2().div_ceil(8) as usize;

// BLOOM_SIZE_BYTES must be a power of 2
#[allow(clippy::assertions_on_constants)]
const _: () = assert!(BLOOM_SIZE_BYTES.is_power_of_two());

#[derive(Clone, Debug)]
/// Implement a simple bloom filter for hashes used in digest indexes.
pub struct Bloom {
    bits: Vec<u8>,
}

impl Bloom {
    /// Create a new empty bloom filter.
    pub fn new() -> Self {
        Self { bits: vec![0_u8; BLOOM_SIZE_BYTES] }
    }

    /// Returns a reference to the underlying data.
    #[inline]
    pub const fn data(&self) -> &[u8] {
        self.bits.as_slice()
    }

    /// Returns true if this bloom filter is a possible superset of the other
    /// bloom filter, admitting false positives.
    ///
    /// Note: This method may return false positives. This is inherent to the
    /// bloom filter data structure.
    pub fn contains(&self, hash: B256) -> bool {
        let mut ptr = 0;

        for _ in 0..BLOOM_BITS_PER_ITEM {
            let mut index = 0_usize;
            for _ in 0..ITEM_BYTES {
                index = (index << 8) | hash[ptr] as usize;
                ptr += 1;
            }
            index &= MASK;
            if (self.bits[BLOOM_SIZE_BYTES - 1 - index / 8] & 1 << (index % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Accrues the input into the bloom filter.
    pub fn accrue(&mut self, hash: B256) {
        let mut ptr = 0;

        for _ in 0..BLOOM_BITS_PER_ITEM {
            let mut index = 0_usize;
            for _ in 0..ITEM_BYTES {
                index = (index << 8) | hash[ptr] as usize;
                ptr += 1;
            }
            index &= MASK;
            self.bits[BLOOM_SIZE_BYTES - 1 - index / 8] |= 1 << (index % 8);
        }
    }
}

impl Default for Bloom {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Bloom> for Vec<u8> {
    fn from(value: Bloom) -> Self {
        value.bits
    }
}

impl TryFrom<Vec<u8>> for Bloom {
    type Error = BloomError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let len = value.len();
        if len < BLOOM_SIZE_BYTES {
            return Err(BloomError::SliceTooSmall);
        }
        if len > BLOOM_SIZE_BYTES {
            return Err(BloomError::SliceTooLarge);
        }
        Ok(Self { bits: value })
    }
}

/// Bloom filter error.
#[derive(Debug)]
pub enum BloomError {
    /// Attempted to create a Bloom with too many bytes (bits).
    SliceTooLarge,
    /// Attempted to create a Bloom with too few bytes (bits).
    SliceTooSmall,
}

impl Error for BloomError {}

impl fmt::Display for BloomError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Self::SliceTooLarge => write!(f, "invalid bit slice, too large"),
            Self::SliceTooSmall => write!(f, "invalid bit slice, too small"),
        }
    }
}

#[cfg(test)]
mod tests {
    use tn_types::DefaultHashFunction;

    use super::*;

    #[test]
    fn test_digest_bloom() {
        let mut bloom: Bloom = Bloom::new();
        for i in 0..1_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            // An empty bloom should have 0 false positives...
            assert!(!bloom.contains(hash));
        }
        for i in 0..1_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            bloom.accrue(hash);
        }
        for i in 0..1_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            assert!(bloom.contains(hash));
        }
        let bits: Vec<u8> = bloom.into();

        let mut bloom: Bloom = bits.try_into().expect("bloom from old bits");
        for i in 0..1_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            assert!(bloom.contains(hash));
        }
        let mut false_positives = 0;
        for i in 1_000_000..2_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            // We have a filter with 1Mil hashes, we can get false positives.
            if bloom.contains(hash) {
                false_positives += 1;
            }
        }
        // We don't want more that .1% of these to be false positives.
        // At the time the test was written false positives where always 438.
        assert!(false_positives < 1_000, "more than 1,000 false positives with 1Mil hashes");
        for i in 1_000_000..2_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            bloom.accrue(hash);
        }
        for i in 0..1_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            assert!(bloom.contains(hash));
        }
        for i in 1_000_000..2_000_000 {
            let mut hasher = DefaultHashFunction::new();
            hasher.update(&format!("idx-{i}").into_bytes());
            let hash = B256::from_slice(hasher.finalize().as_bytes());
            assert!(bloom.contains(hash));
        }
    }
}
