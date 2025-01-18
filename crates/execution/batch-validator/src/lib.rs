// SPDX-License-Identifier: MIT or Apache-2.0
//! Batch validation

mod validator;
pub use validator::BatchValidator;

#[cfg(any(test, feature = "test-utils"))]
pub use validator::NoopBatchValidator;
