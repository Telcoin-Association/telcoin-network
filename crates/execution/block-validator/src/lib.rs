//! Batch validation

mod error;
mod validator;
pub use validator::{BlockValidation, BlockValidator};

#[cfg(any(test, feature = "test-utils"))]
pub use validator::NoopBlockValidator;
