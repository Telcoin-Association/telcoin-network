//! Wrapper function to add and check crc32s on byte buffers.  THe CRC codes are always the last
//! four bytes in little endian format.

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

/// Classify `buffer` by its trailing 4-byte CRC: all-zero ⇒ [`CrcState::Dirty`]; otherwise
/// recompute the CRC over the payload and compare ⇒ [`CrcState::Valid`] / [`CrcState::Corrupt`].
/// Buffers too short to hold a payload + CRC are [`CrcState::Corrupt`].
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
    } else if check_crc(buffer) {
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
}
