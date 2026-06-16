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
}
