// Vendored reth code. TN's lint levels do not apply here; see vendor/reth/VENDOR.md.
#![allow(warnings, unused_extern_crates)]
//! Invalid block hook implementations.

mod witness;

pub use witness::InvalidBlockWitnessHook;
