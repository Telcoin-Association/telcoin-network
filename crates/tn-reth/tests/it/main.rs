//! Integration tests for tn-reth crate.

#![allow(unused_crate_dependencies)]

mod economics_props;
#[cfg(feature = "test-utils")]
mod pipeline_helpers;
#[cfg(all(feature = "test-utils", feature = "faucet"))]
mod pipeline_tel_faucet_props;
#[cfg(all(feature = "test-utils", not(feature = "faucet")))]
mod pipeline_tel_precompile_props;
#[cfg(feature = "faucet")]
mod tel_precompile_faucet_props;
mod tel_precompile_helpers;
#[cfg(not(feature = "faucet"))]
mod tel_precompile_props;

fn main() {}
