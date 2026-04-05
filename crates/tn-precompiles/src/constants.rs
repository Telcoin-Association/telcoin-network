//! Constants for the Telcoin precompile.

use alloy::primitives::{Address, address};
use std::sync::OnceLock;

/// The canonical address of the Telcoin precompile: `0x7e1`.
pub const TELCOIN_PRECOMPILE_ADDRESS: Address =
    address!("00000000000000000000000000000000000007e1");

/// The default governance safe address.
pub const GOVERNANCE_SAFE_ADDRESS: Address = address!("00000000000000000000000000000000000007a0");

/// The system address used for protocol-level calls.
pub const SYSTEM_ADDRESS: Address = address!("fffffffffffffffffffffffffffffffffffffffe");

/// Global basefee address, set once at startup.
static BASEFEE_ADDRESS: OnceLock<Address> = OnceLock::new();

/// Default governance safe address (fallback if basefee address not set).
const DEFAULT_BASEFEE_ADDRESS: Address = address!("00000000000000000000000000000000000007a0");

/// Returns the configured basefee collection address.
pub fn basefee_address() -> Address {
    *BASEFEE_ADDRESS.get().unwrap_or(&DEFAULT_BASEFEE_ADDRESS)
}

/// Sets the basefee collection address. Can only be called once.
pub fn set_basefee_address(address: Option<Address>) {
    if let Some(addr) = address {
        let _ = BASEFEE_ADDRESS.set(addr);
    }
}
