//! Inner-node networking.
//!
//! Each component (primary, worker, engine) sends messages to the inner-node network. The
//! inner-node network routes these messages to the desired component.

mod local;
