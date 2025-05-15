// SPDX-License-Identifier: MIT or Apache-2.0
//! Batch validation

#![warn(
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    rustdoc::all,
    unused_crate_dependencies
)]

mod validator;
pub use validator::BatchValidator;

#[cfg(any(test, feature = "test-utils"))]
pub use validator::NoopBatchValidator;
