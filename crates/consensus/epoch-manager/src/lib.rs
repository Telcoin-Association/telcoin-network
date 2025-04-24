//! The epoch manager is responsible for managing all consensus-related tasks during each epoch.
//!
//! At the epoch boundary, the committee of validators changes. The epoch manager over sees the
//! transition between committees for each node. Certain tasks are shared across epochs while
//! others expire with each epoch.

mod manager;
pub use manager::*;
