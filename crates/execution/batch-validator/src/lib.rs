//! Batch validation

mod error;
mod validator;

pub use validator::{BatchValidation, BatchValidator};

#[cfg(any(test, feature = "test-utils"))]
pub use validator::NoopBatchValidator;
