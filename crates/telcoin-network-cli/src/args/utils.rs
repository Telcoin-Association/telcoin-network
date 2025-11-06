//! Utilities for parsing args

use eyre::OptionExt;
use std::str::FromStr;
use tn_reth::{dirs::DataDirPath, MaybePlatformPath};
use tn_types::{Address, U256};

/// Create a default path for the node.
pub fn tn_platform_path(value: &str) -> eyre::Result<MaybePlatformPath<DataDirPath>> {
    let path = if value.is_empty() { "telcoin-network" } else { value };

    Ok(MaybePlatformPath::<DataDirPath>::from_str(path)?)
}

/// Parse address from string for execution layer.
///
/// Pass "0" to return the zero address, otherwise it must be a valid H160 address.
pub fn clap_address_parser(value: &str) -> eyre::Result<Address> {
    let address = match value {
        "0" => Address::ZERO,
        _ => Address::from_str(value)?,
    };

    Ok(address)
}

/// Parse 18 decimal U256 from string for ConsensusRegistry.
pub fn clap_u256_parser_to_18_decimals(value: &str) -> eyre::Result<U256> {
    let parsed_val = U256::from_str_radix(value, 10)?
        .checked_mul(U256::from(10).checked_pow(U256::from(18)).expect("1e18 exponentiation"))
        .ok_or_eyre("U256 parsing")?;

    Ok(parsed_val)
}

/// Parse a u64 as base 10 or base 16 (hex) if prefixed with 0x.
pub fn maybe_hex(s: &str) -> eyre::Result<u64> {
    let result = if let Some(stripped) = s.strip_prefix("0x") {
        u64::from_str_radix(stripped, 16)?
    } else {
        s.parse::<u64>()?
    };
    Ok(result)
}

#[cfg(test)]
mod tests {
    use crate::args::maybe_hex;

    #[test]
    fn test_maybe_hex() {
        assert_eq!(maybe_hex("0x1e7").unwrap(), 487);
        assert_eq!(maybe_hex("487").unwrap(), 487);
        assert_eq!(maybe_hex("0x7e1").unwrap(), 2017);
        assert_eq!(maybe_hex("2017").unwrap(), 2017);
    }
}
