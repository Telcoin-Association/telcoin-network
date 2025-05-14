//! Utilities for parsing args

use alloy::primitives::aliases::U232;
use eyre::OptionExt;
use std::{str::FromStr, sync::Arc};
use tn_reth::{chain_value_parser, dirs::DataDirPath, MaybePlatformPath, RethChainSpec};
use tn_types::{adiri_chain_spec_arc, Address};

/// Create a default path for the node.
pub fn tn_platform_path(value: &str) -> eyre::Result<MaybePlatformPath<DataDirPath>> {
    let path = if value.is_empty() { "telcoin-network" } else { value };

    Ok(MaybePlatformPath::<DataDirPath>::from_str(path)?)
}

/// Defaults for chain spec clap parser.
///
/// Wrapper to intercept "adiri" chain spec. If not adiri, try reth's genesis_value_parser.
pub fn clap_genesis_parser(value: &str) -> eyre::Result<Arc<RethChainSpec>, eyre::Error> {
    let chain = match value {
        "adiri" => adiri_chain_spec_arc(),
        _ => chain_value_parser(value)?,
    };

    Ok(chain)
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
