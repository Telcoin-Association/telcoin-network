//! Validate the production config and adapter with existing libp2p and serde artifacts.

extern crate self as tn_config;

#[path = "../../crates/config/src/source_admission.rs"]
mod config;
pub use config::SourceAdmissionConfig;

#[path = "../../crates/network-libp2p/src/source_admission/mod.rs"]
pub mod source_admission;
