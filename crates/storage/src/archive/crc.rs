//! Wrapper function to add and check crc32s on byte buffers.  THe CRC codes are always the last
//! four bytes in little endian format.

/// A CRC of exactly zero is indistinguishable from the all-zero "dirty" sentinel [`zero_crc`]
/// writes, so buffers whose validity is classified by [`crc_state`] (the digest index's main
/// buckets) are stamped with [`add_crc32_nonzero`], which maps a computed CRC of 0 to this fixed
/// non-zero value. Any non-zero `u32` would do; the exact constant is irrelevant as long as it is
/// not 0.
const NONZERO_CRC_SENTINEL: u32 = 0xFFFF_FFFF;

/// Compute the crc32 of `payload`.
fn compute_crc32(payload: &[u8]) -> u32 {
    let mut crc32_hasher = crc32fast::Hasher::new();
    crc32_hasher.update(payload);
    crc32_hasher.finalize()
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
    let mut crc32_hasher = crc32fast::Hasher::new();
    crc32_hasher.update(&buffer[..(len - 4)]);
    let calc_crc32 = crc32_hasher.finalize();
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
    let mut crc32_hasher = crc32fast::Hasher::new();
    crc32_hasher.update(&buffer[..(len - 4)]);
    let crc32 = crc32_hasher.finalize();
    buffer[len - 4..].copy_from_slice(&crc32.to_le_bytes());
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
    let crc32 = map_nonzero(compute_crc32(&buffer[..(len - 4)]));
    buffer[len - 4..].copy_from_slice(&crc32.to_le_bytes());
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
    let calc = map_nonzero(compute_crc32(&buffer[..(len - 4)]));
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
/// Note this recomputes the CRC for non-dirty buffers, so it is for verification/recovery, not the
/// hot path; use [`crc_is_zero`] when you only need the cheap dirty check.
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
