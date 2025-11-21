//! Telcoin Network (TN) binary executable.
//!
//! ## Feature Flags
//!
//! - `min-error-logs`: Disables all logs below `error` level.
//! - `min-warn-logs`: Disables all logs below `warn` level.
//! - `min-info-logs`: Disables all logs below `info` level. This can speed up the node, since fewer
//!   calls to the logging component is made.
//! - `min-debug-logs`: Disables all logs below `debug` level.
//! - `min-trace-logs`: Disables all logs below `trace` level.

#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

pub mod args;
pub mod cli;
pub mod genesis;
pub mod keytool;
pub mod node;
mod open_telemetry;
pub mod version;

/// No Additional arguments
#[derive(Debug, Clone, Copy, Default, clap::Args)]
pub struct NoArgs;
