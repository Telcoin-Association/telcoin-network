//! Utilities for parsing args

use reth::{chainspec::chain_value_parser, dirs::MaybePlatformPath};
use reth_chainspec::ChainSpec;
use std::{str::FromStr, sync::Arc};
use tn_node::dirs::DataDirPath;
use tn_types::{adiri_chain_spec_arc, Address};

/// Create a default path for the node.
pub fn tn_platform_path(value: &str) -> eyre::Result<MaybePlatformPath<DataDirPath>> {
    let path = if value.is_empty() { "telcoin-network" } else { value };

    Ok(MaybePlatformPath::<DataDirPath>::from_str(path)?)
}

/// Defaults for chain spec clap parser.
///
/// Wrapper to intercept "adiri" chain spec. If not adiri, try reth's genesis_value_parser.
pub fn clap_genesis_parser(value: &str) -> eyre::Result<Arc<ChainSpec>, eyre::Error> {
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
