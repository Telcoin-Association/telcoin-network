//! Test the faucet RPC is able to make use Google KMS to submit a transaction.
//!
//! Note: this requires gcloud credentials.
//!
//! Custom service account just for testing was created. The keys
//! were downloaded to a file in the root of this crate and called
//! `gcloud-credentials.json`. The path is used by an env var set
//! at the start of each test called `GOOGLE_APPLICATION_CREDENTIALS`.
//! In production, these credentials are already set and automatically
//! discovered by the `gcloud-sdk` crate.

mod faucet;

fn main() {}
