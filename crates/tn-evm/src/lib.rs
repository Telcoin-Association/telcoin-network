//! TN EVM types extracted from tn-reth for shared use.

pub mod constants;
pub mod context;
pub mod evm;
pub mod factory;
pub mod gas_penalty;
pub mod handler;
pub mod hardfork;

// Re-exports for convenience
pub use constants::{basefee_address, set_basefee_address, SYSTEM_ADDRESS};
pub use context::{TNContext, TNContextBuilder, TNEvmContext, TelcoinEvm};
pub use evm::TNEvm;
pub use factory::TNEvmFactory;
pub use gas_penalty::calculate_gas_penalty;
pub use handler::TNEvmHandler;
pub use hardfork::TelcoinHardfork;
