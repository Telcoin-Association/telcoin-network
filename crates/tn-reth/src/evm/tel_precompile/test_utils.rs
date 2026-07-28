//! TEL-specific extensions to the shared precompile test harness.
//!
//! The generic in-memory EVM harness ([`TestEnv`]) lives in
//! [`precompile_test_utils`](crate::evm::precompile_test_utils). This module adds the TEL token
//! helpers (`mint`, `get_total_supply`, `set_total_supply`) as an inherent extension of `TestEnv`,
//! plus the [`GENESIS_SUPPLY`] constant the shared constructor seeds.

use super::TELCOIN_PRECOMPILE_ADDRESS;
use crate::evm::precompile_test_utils::{TestEnv, TestResult};
use alloy::sol_types::SolCall;
use tn_types::{Address, U256};

/// Genesis total supply in whole TEL units (100 billion).
///
/// The shared [`TestEnv`] constructor seeds the `totalSupply` slot with `GENESIS_SUPPLY * 10^18`
/// wei.
pub const GENESIS_SUPPLY: u128 = 100_000_000_000; // 100B

/// TEL-token helpers layered onto the shared [`TestEnv`].
impl TestEnv {
    /// Mint tokens via the precompile.
    ///
    /// In mainnet mode, creates a timelocked pending mint to governance (ignores `recipient`).
    /// In faucet mode, directly credits `recipient`.
    pub fn mint(&mut self, caller: Address, recipient: Address, amount: U256) -> TestResult {
        let _ = recipient; // unused in mainnet mode; suppress warning in faucet mode via cfg
        #[cfg(not(feature = "faucet"))]
        let data = super::burnable::mintCall { amount }.abi_encode();
        #[cfg(feature = "faucet")]
        let data = super::faucet::mintCall { recipient, amount }.abi_encode();
        self.exec_default(caller, data)
    }

    /// Read the precompile's `totalSupply` slot.
    ///
    /// Prefers the in-memory journal state; falls back to the database.
    pub fn get_total_supply(&mut self) -> U256 {
        self.get_storage(TELCOIN_PRECOMPILE_ADDRESS, U256::from(100))
    }

    /// Override the precompile's `totalSupply` storage slot.
    ///
    /// Useful for testing checked-arithmetic boundaries (overflow/underflow).
    pub fn set_total_supply(&mut self, amount: U256) {
        self.evm
            .ctx
            .journaled_state
            .database
            .insert_account_storage(TELCOIN_PRECOMPILE_ADDRESS, U256::from(100), amount)
            .unwrap();
    }
}
