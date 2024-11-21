//! Inner-node networking.
//!
//! Each component (primary, worker, engine) sends messages to the inner-node network. The
//! inner-node network routes these messages to the desired component.

mod engine;
mod local;
mod primary;
mod worker;
pub use engine::*;
pub use local::*;
pub use primary::*;
pub use worker::*;
