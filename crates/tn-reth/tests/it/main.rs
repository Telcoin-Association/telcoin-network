//! Integration tests for tn-reth crate.

#![allow(unused_crate_dependencies)]

mod bls_precompile_props;
mod economics_props;
mod pipeline_helpers;
mod recover_drop_props;

// testnet
#[cfg(feature = "faucet")]
mod pipeline_tel_faucet_props;
#[cfg(feature = "faucet")]
mod tel_precompile_faucet_props;

// mainnet
#[cfg(not(feature = "faucet"))]
mod pipeline_tel_precompile_props;
#[cfg(not(feature = "faucet"))]
mod tel_precompile_props;

fn main() {}
