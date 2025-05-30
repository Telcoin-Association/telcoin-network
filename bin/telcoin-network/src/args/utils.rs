//! Utilities for parsing args

use alloy::primitives::aliases::U232;
use eyre::OptionExt;
use std::str::FromStr;
use tn_reth::{dirs::DataDirPath, MaybePlatformPath};
use tn_types::Address;

pub use tn_reth::clap_genesis_parser;

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

/// Parse 18 decimal U232 from string for ConsensusRegistry.
pub fn clap_u232_parser(value: &str) -> eyre::Result<U232> {
    let parsed_val = U232::from_str_radix(value, 10)?
        .checked_mul(U232::from(10).checked_pow(U232::from(18)).expect("1e18 exponentiation"))
        .ok_or_eyre("U232 parsing")?;

    Ok(parsed_val)
}

/// Parse a u64 as base 10 or base 16 (hex) if prefixed with 0x.
pub fn maybe_hex(s: &str) -> eyre::Result<u64> {
    let result = if s.starts_with("0x") {
        u64::from_str_radix(&s[2..], 16)?
    } else {
        u64::from_str_radix(s, 10)?
    };
    Ok(result)
}
