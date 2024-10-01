//! The worker's approach to maintaining and updating transaction pool state.

mod backup;
mod maintain;
mod metrics;

pub use maintain::{maintain_transaction_pool_future, LastCanonicalUpdate, PoolMaintenanceConfig};
