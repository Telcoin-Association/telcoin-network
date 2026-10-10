//! CRC-32 helpers: the shared [`crc32`] / [`crc32_update`] (with an aarch64 PMULL fast path), and
//! functions that add and check a CRC-32 trailer on byte buffers. The trailer is always the last
//! four bytes, little endian.

/// A CRC of exactly zero is indistinguishable from the all-zero "dirty" sentinel [`zero_crc`]
/// writes, so buffers whose validity is classified by [`crc_state`] (the digest index's main
/// buckets) are stamped with [`add_crc32_nonzero`], which maps a computed CRC of 0 to this fixed
/// non-zero value. Any non-zero `u32` would do; the exact constant is irrelevant as long as it is
/// not 0.
const NONZERO_CRC_SENTINEL: u32 = 0xFFFF_FFFF;

/// The CRC-32 (IEEE, the value `crc32fast` and zlib compute) of `data`.
pub(crate) fn crc32(data: &[u8]) -> u32 {
    crc32_update(0, data)
}

/// Continue the finished CRC-32 `crc` (0 to start) over `data`: the same value as
/// `crc32fast::Hasher::new_with_initial(crc)` updated with `data`, so `crc32_update(crc32(a), b)`
/// is the CRC-32 of `a` followed by `b`.
///
/// On aarch64 with PMULL, inputs of at least `pmull::MIN_LEN` bytes take a carry-less-multiply
/// folding path (a port of `crc32fast`'s x86 pclmulqdq one): `crc32fast` itself uses a single
/// serial `crc32` instruction chain there, several times slower on large buffers. Everything else
/// (small inputs, other targets) is `crc32fast`.
pub(crate) fn crc32_update(crc: u32, data: &[u8]) -> u32 {
    #[cfg(target_arch = "aarch64")]
    if data.len() >= pmull::MIN_LEN && std::arch::is_aarch64_feature_detected!("aes") {
        // SAFETY: the `aes` feature (which carries PMULL) was detected at runtime just above.
        return unsafe { pmull::update(crc, data) };
    }
    let mut hasher = crc32fast::Hasher::new_with_initial(crc);
    hasher.update(data);
    hasher.finalize()
}

/// CRC-32 by carry-less-multiply folding on aarch64 (PMULL): a port of `crc32fast`'s x86
/// `specialized::pclmulqdq::calculate` (same constants, same steps), so it computes the same value.
/// Each `_mm_clmulepi64_si128` there becomes a `vmull_p64` here: selector `0x00` multiplies the
/// low 64-bit halves, `0x11` the high halves, and `0x10` the first operand's low half by the second
/// operand's high half.
#[cfg(target_arch = "aarch64")]
mod pmull {
    use std::arch::aarch64::{
        uint64x2_t, vdupq_n_u64, veorq_u64, vgetq_lane_u64, vld1q_u64, vld1q_u8, vmull_high_p64,
        vmull_p64, vreinterpretq_p64_u64, vreinterpretq_u64_p128, vreinterpretq_u64_u8,
        vsetq_lane_u64,
    };

    /// Below this the folding setup does not pay; `crc32fast` (as on x86) handles it.
    pub(super) const MIN_LEN: usize = 128;

    const K1: u64 = 0x1_5444_2bd4;
    const K2: u64 = 0x1_c6e4_1596;
    const K3: u64 = 0x1_7519_97d0;
    const K4: u64 = 0x0_ccaa_009e;
    const K5: u64 = 0x1_63cd_6124;
    const P_X: u64 = 0x1_DB71_0641;
    const U_PRIME: u64 = 0x1_F701_1641;

    /// Continue the finished CRC `crc` over `data` (at least [`MIN_LEN`] bytes).
    ///
    /// # Safety
    /// The CPU must support the `aes` feature (PMULL); NEON is baseline on aarch64.
    #[target_feature(enable = "neon,aes")]
    pub(super) unsafe fn update(crc: u32, mut data: &[u8]) -> u32 {
        debug_assert!(data.len() >= MIN_LEN);

        // Fold by 4: four 128-bit accumulators over 64-byte blocks.
        let mut x3 = get(&mut data);
        let mut x2 = get(&mut data);
        let mut x1 = get(&mut data);
        let mut x0 = get(&mut data);
        // The incoming CRC enters as the (inverted) first 32 bits of the message.
        x3 = veorq_u64(x3, vsetq_lane_u64::<0>(u64::from(!crc), vdupq_n_u64(0)));
        let k1k2 = vld1q_u64([K1, K2].as_ptr());
        while data.len() >= 64 {
            x3 = reduce128(x3, get(&mut data), k1k2);
            x2 = reduce128(x2, get(&mut data), k1k2);
            x1 = reduce128(x1, get(&mut data), k1k2);
            x0 = reduce128(x0, get(&mut data), k1k2);
        }
        let k3k4 = vld1q_u64([K3, K4].as_ptr());
        let mut x = reduce128(x3, x2, k3k4);
        x = reduce128(x, x1, k3k4);
        x = reduce128(x, x0, k3k4);
        // Fold by 1 over the remaining 16-byte blocks.
        while data.len() >= 16 {
            x = reduce128(x, get(&mut data), k3k4);
        }

        // 128 -> 64 bits, then Barrett to 32, on the 128-bit value as a `u128`.
        let x = u128::from(vgetq_lane_u64::<0>(x)) | (u128::from(vgetq_lane_u64::<1>(x)) << 64);
        let x = clmul(x as u64, K4) ^ (x >> 64);
        let x = clmul(x as u64 & 0xFFFF_FFFF, K5) ^ (x >> 32);
        let t1 = clmul(x as u64 & 0xFFFF_FFFF, U_PRIME);
        let t2 = clmul(t1 as u64 & 0xFFFF_FFFF, P_X);
        let c = ((x ^ t2) >> 32) as u32;

        if data.is_empty() {
            !c
        } else {
            let mut hasher = crc32fast::Hasher::new_with_initial(!c);
            hasher.update(data);
            hasher.finalize()
        }
    }

    /// `b ^ a.lo * k.lo ^ a.hi * k.hi` (carry-less).
    #[inline]
    #[target_feature(enable = "neon,aes")]
    unsafe fn reduce128(a: uint64x2_t, b: uint64x2_t, k: uint64x2_t) -> uint64x2_t {
        let lo = vmull_p64(vgetq_lane_u64::<0>(a), vgetq_lane_u64::<0>(k));
        let hi = vmull_high_p64(vreinterpretq_p64_u64(a), vreinterpretq_p64_u64(k));
        veorq_u64(b, veorq_u64(vreinterpretq_u64_p128(lo), vreinterpretq_u64_p128(hi)))
    }

    /// Carry-less 64 x 64 -> 128-bit multiply.
    #[inline]
    #[target_feature(enable = "neon,aes")]
    unsafe fn clmul(a: u64, b: u64) -> u128 {
        vmull_p64(a, b)
    }

    /// Load the next 16 bytes (unaligned) and advance `data` past them.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn get(data: &mut &[u8]) -> uint64x2_t {
        let (block, rest) = data.split_at(16);
        // SAFETY: `block` is exactly 16 readable bytes; `vld1q_u8` has no alignment requirement.
        let v = unsafe { vreinterpretq_u64_u8(vld1q_u8(block.as_ptr())) };
        *data = rest;
        v
    }
}

/// Map a computed CRC to a never-zero value so a stamped trailer can never collide with the
/// all-zero dirty sentinel. Identity for any non-zero CRC.
fn map_nonzero(crc: u32) -> u32 {
    if crc == 0 {
        NONZERO_CRC_SENTINEL
    } else {
        crc
    }
}

/// Check buffers crc32.  The last 4 bytes of the buffer are the CRC32 code and rest of the buffer
/// is checked against that.
pub(crate) fn check_crc(buffer: &[u8]) -> bool {
    let len = buffer.len();
    if len < 5 {
        return false;
    }
    let calc_crc32 = crc32(&buffer[..(len - 4)]);
    let mut buf32 = [0_u8; 4];
    buf32.copy_from_slice(&buffer[(len - 4)..]);
    let read_crc32 = u32::from_le_bytes(buf32);
    calc_crc32 == read_crc32
}

/// Add a crc32 code to buffer.  The last four bytes of buffer are overwritten by the crc32 code of
/// the rest of the buffer.
///
/// The buffer must be at least 5 bytes (>= 1 payload byte plus the 4-byte CRC) to match what
/// [`check_crc`] will accept; a shorter buffer would be stamped but could never validate, so this
/// is a no-op (and trips a debug assert) in that case.
pub(crate) fn add_crc32(buffer: &mut [u8]) {
    let len = buffer.len();
    debug_assert!(len >= 5, "add_crc32 needs at least 5 bytes (>=1 payload byte + 4 crc bytes)");
    if len < 5 {
        return;
    }
    let crc = crc32(&buffer[..(len - 4)]);
    buffer[len - 4..].copy_from_slice(&crc.to_le_bytes());
}

/// Like [`add_crc32`], but never stamps an all-zero trailer: a computed CRC of 0 is written as
/// [`NONZERO_CRC_SENTINEL`]. Use this for buffers whose state is later classified by [`crc_state`]
/// (the digest index's main buckets), where an all-zero trailer is reserved for the "dirty" marker
/// [`zero_crc`] writes. A genuine CRC of 0 stamped by plain [`add_crc32`] would be misread as
/// dirty; because the index layout is deterministic, that can wedge the open-time first-bucket CRC
/// guard into rebuilding on every open.  Verify with [`crc_state`] (not [`check_crc`]).
///
/// Like [`add_crc32`], needs at least 5 bytes (>= 1 payload byte + 4 CRC bytes); shorter buffers
/// are a no-op (and trip a debug assert).
pub(crate) fn add_crc32_nonzero(buffer: &mut [u8]) {
    let len = buffer.len();
    debug_assert!(
        len >= 5,
        "add_crc32_nonzero needs at least 5 bytes (>=1 payload byte + 4 crc bytes)"
    );
    if len < 5 {
        return;
    }
    let crc = map_nonzero(crc32(&buffer[..(len - 4)]));
    buffer[len - 4..].copy_from_slice(&crc.to_le_bytes());
}

/// Overwrite the trailing 4-byte CRC of `buffer` with zeros, marking it as "dirty" — written but
/// not yet CRC'd. This is the sentinel used by lazy-CRC mode: a modified buffer is left with a zero
/// CRC so a later pass can [`add_crc32`] only the dirty buffers, and so recovery can tell a dirty
/// buffer (zero CRC) from a corrupt one (non-zero CRC that fails to verify — see [`crc_state`]).
///
/// Like [`add_crc32`], needs at least 5 bytes (>= 1 payload byte + 4 CRC bytes); shorter buffers
/// are a no-op (and trip a debug assert).
pub(crate) fn zero_crc(buffer: &mut [u8]) {
    let len = buffer.len();
    debug_assert!(len >= 5, "zero_crc needs at least 5 bytes (>=1 payload byte + 4 crc bytes)");
    if len < 5 {
        return;
    }
    buffer[len - 4..].fill(0);
}

/// True if `buffer`'s trailing 4-byte CRC is all zero — the "dirty / not-yet-CRC'd" sentinel
/// written by [`zero_crc`]. Cheap: reads the 4 CRC bytes and computes nothing. Buffers too short to
/// hold a payload + CRC are not considered zero.
pub(crate) fn crc_is_zero(buffer: &[u8]) -> bool {
    let len = buffer.len();
    if len < 5 {
        return false;
    }
    buffer[len - 4..] == [0, 0, 0, 0]
}

/// Classification of a CRC-trailed buffer, distinguishing a deliberately un-CRC'd ("dirty") buffer
/// from genuine corruption. Produced by [`crc_state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CrcState {
    /// The trailing CRC matches the payload — intact.
    Valid,
    /// The trailing CRC is all-zero: written but not yet CRC'd (see [`zero_crc`]). Expected for
    /// buffers modified under lazy-CRC mode before a `sync()`, or after an unclean shutdown.
    Dirty,
    /// The trailing CRC is non-zero but does not match the payload — corruption.
    Corrupt,
}

/// Verify a buffer stamped by [`add_crc32_nonzero`]: recompute the payload CRC with the same
/// zero→[`NONZERO_CRC_SENTINEL`] mapping and compare it to the trailing 4 bytes. Mirrors
/// [`check_crc`] but accepts the never-zero stamping, so a payload whose real CRC is 0 (stored as
/// the sentinel) still verifies instead of reading as corrupt.
fn check_crc_nonzero(buffer: &[u8]) -> bool {
    let len = buffer.len();
    if len < 5 {
        return false;
    }
    let calc = map_nonzero(crc32(&buffer[..(len - 4)]));
    let mut buf32 = [0_u8; 4];
    buf32.copy_from_slice(&buffer[(len - 4)..]);
    calc == u32::from_le_bytes(buf32)
}

/// Classify `buffer` by its trailing 4-byte CRC: all-zero ⇒ [`CrcState::Dirty`]; otherwise
/// recompute the CRC over the payload and compare ⇒ [`CrcState::Valid`] / [`CrcState::Corrupt`].
/// Buffers too short to hold a payload + CRC are [`CrcState::Corrupt`].
///
/// Valid buffers this classifies are stamped by [`add_crc32_nonzero`] (never an all-zero trailer),
/// so the dirty (all-zero) and valid states are disjoint — the recompute uses the same never-zero
/// mapping ([`check_crc_nonzero`]) so a genuine CRC of 0 reads `Valid`, not `Dirty` or `Corrupt`.
///
/// Note this recomputes the CRC for non-dirty buffers (the digest index pays it on every lookup of
/// a stamped bucket); use [`crc_is_zero`] when you only need the cheap dirty check.
pub(crate) fn crc_state(buffer: &[u8]) -> CrcState {
    let len = buffer.len();
    if len < 5 {
        return CrcState::Corrupt;
    }
    if buffer[len - 4..] == [0, 0, 0, 0] {
        CrcState::Dirty
    } else if check_crc_nonzero(buffer) {
        CrcState::Valid
    } else {
        CrcState::Corrupt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random bytes.
    fn noise(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    /// `crc32` / `crc32_update` compute exactly `crc32fast`'s CRC-32 for every length up to 2 KiB
    /// at every start offset 0..16 (unaligned loads), and for large buffers. On aarch64 with
    /// PMULL this exercises the folding path (from `pmull::MIN_LEN` bytes); elsewhere the
    /// dispatch is `crc32fast` itself, so the comparison is trivial there.
    #[test]
    fn crc32_matches_crc32fast() {
        let buf = noise(2_048 + 16, 1);
        for offset in 0..16 {
            for len in 0..=2_048 {
                let data = &buf[offset..offset + len];
                assert_eq!(crc32(data), crc32fast::hash(data), "offset {offset}, len {len}");
            }
        }
        for (len, seed) in [(4_096, 2), ((64 << 10) + 13, 3), ((1 << 20) + 7, 4)] {
            let data = noise(len, seed);
            assert_eq!(crc32(&data), crc32fast::hash(&data), "len {len}");
        }
    }

    /// Continuing from any CRC matches `crc32fast::Hasher::new_with_initial`, and chaining updates
    /// across any split point equals the one-shot CRC.
    #[test]
    fn crc32_update_continues_like_crc32fast() {
        let data = noise(1_500, 5);
        for (i, init) in [0_u32, 1, 0xFFFF_FFFF, 0xDEAD_BEEF, 0x1234_5678].into_iter().enumerate() {
            for len in [0, 1, 15, 16, 127, 128, 129, 200, 1_024, 1_500] {
                let mut hasher = crc32fast::Hasher::new_with_initial(init);
                hasher.update(&data[..len]);
                assert_eq!(
                    crc32_update(init, &data[..len]),
                    hasher.finalize(),
                    "init {i}, len {len}"
                );
            }
        }
        let whole = crc32(&data);
        for split in 0..=data.len() {
            let (a, b) = data.split_at(split);
            assert_eq!(crc32_update(crc32(a), b), whole, "split {split}");
        }
    }

    #[test]
    fn test_crc_round_trip() {
        // Smallest valid buffer: 1 payload byte + 4 crc bytes.
        let mut buffer = [0xAB, 0, 0, 0, 0];
        add_crc32(&mut buffer);
        assert!(check_crc(&buffer));
        // Corrupting any payload byte must fail the check.
        buffer[0] ^= 0xFF;
        assert!(!check_crc(&buffer));
    }

    #[test]
    fn test_check_crc_rejects_too_short() {
        // Buffers too small to hold a payload + crc are never valid.
        assert!(!check_crc(&[0_u8; 4]));
        assert!(!check_crc(&[]));
    }

    #[test]
    fn test_zero_crc_dirty_marker() {
        // 9 bytes: 5 payload + 4 CRC.
        let mut buffer = [0xAB, 0x01, 0x02, 0x03, 0x04, 0xFF, 0xFF, 0xFF, 0xFF];
        // Zeroing the trailer marks it dirty.
        zero_crc(&mut buffer);
        assert!(crc_is_zero(&buffer));
        assert_eq!(crc_state(&buffer), CrcState::Dirty);
        // A real CRC is valid and (for this payload) non-zero.
        add_crc32(&mut buffer);
        assert!(!crc_is_zero(&buffer));
        assert_eq!(crc_state(&buffer), CrcState::Valid);
        // Corrupting a payload byte while keeping the (now stale, non-zero) CRC reads as corrupt.
        buffer[0] ^= 0xFF;
        assert_eq!(crc_state(&buffer), CrcState::Corrupt);
    }

    #[test]
    fn test_crc_is_zero_bounds() {
        // Too short to hold a payload + CRC: never "zero".
        assert!(!crc_is_zero(&[0_u8; 4]));
        assert!(!crc_is_zero(&[]));
        // Non-zero trailer.
        assert!(!crc_is_zero(&[0, 0, 0, 1, 2]));
    }

    #[test]
    fn test_map_nonzero() {
        // A computed CRC of 0 is remapped to a non-zero sentinel; every other value is unchanged.
        assert_ne!(map_nonzero(0), 0);
        assert_eq!(map_nonzero(0), NONZERO_CRC_SENTINEL);
        assert_eq!(map_nonzero(1), 1);
        assert_eq!(map_nonzero(0x1234_5678), 0x1234_5678);
        assert_eq!(map_nonzero(NONZERO_CRC_SENTINEL), NONZERO_CRC_SENTINEL);
    }

    #[test]
    fn test_add_crc32_nonzero_never_leaves_zero_trailer() {
        // A normally-stamped buffer is Valid and its trailer is not the dirty (all-zero) sentinel.
        let mut buffer = [0xAB, 0x01, 0x02, 0x03, 0x04, 0xFF, 0xFF, 0xFF, 0xFF];
        add_crc32_nonzero(&mut buffer);
        assert!(!crc_is_zero(&buffer), "a nonzero-stamped buffer must not read as dirty");
        assert_eq!(crc_state(&buffer), CrcState::Valid);
    }

    #[test]
    fn test_crc_state_dirty_then_nonzero_stamp_is_valid() {
        // Mirrors the index sync flow (`crc_dirty_buckets`): a buffer marked dirty by `zero_crc`
        // classifies Dirty, then after `add_crc32_nonzero` classifies Valid (never Dirty), and
        // corrupting a payload byte afterwards reads Corrupt.
        let mut buffer = [0xAB, 0x01, 0x02, 0x03, 0x04, 0xFF, 0xFF, 0xFF, 0xFF];
        zero_crc(&mut buffer);
        assert_eq!(crc_state(&buffer), CrcState::Dirty);
        add_crc32_nonzero(&mut buffer);
        assert!(!crc_is_zero(&buffer), "a nonzero-stamped buffer must never look dirty");
        assert_eq!(crc_state(&buffer), CrcState::Valid);
        buffer[0] ^= 0xFF;
        assert_eq!(crc_state(&buffer), CrcState::Corrupt);
    }

    #[test]
    fn test_check_crc_nonzero_round_trip() {
        // `add_crc32_nonzero` and `check_crc_nonzero` are consistent (both apply `map_nonzero`, so
        // a real crc-0 payload — infeasible to construct here — would verify identically):
        // a freshly stamped buffer verifies, and any payload change breaks verification.
        let mut buffer = [0x10, 0x20, 0x30, 0x40, 0, 0, 0, 0];
        add_crc32_nonzero(&mut buffer);
        assert!(check_crc_nonzero(&buffer));
        buffer[0] ^= 0xFF;
        assert!(!check_crc_nonzero(&buffer));
    }
}
