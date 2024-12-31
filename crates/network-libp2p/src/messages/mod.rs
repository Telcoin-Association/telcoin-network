//! Network message types.

mod inner;
mod primary;
mod worker;

pub use primary::{PrimaryRequest, PrimaryResponse};
pub use worker::{WorkerRequest, WorkerResponse};
