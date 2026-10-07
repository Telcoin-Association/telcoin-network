//! Forward raw transaction submissions to operator-configured validator RPC endpoints.
//!
//! A public RPC node (an observer) started with `--forward-txs` relays `eth_sendRawTransaction`
//! and `eth_sendRawTransactionSync` to a private, ordered list of validator RPC targets instead
//! of inserting the transaction into its own pool. Every other method is still served locally.
//! `target` parses the flag value into that list.

mod target;

pub(crate) use target::parse_forward_targets;
pub use target::{ForwardTarget, ForwardTargets, TxForwardConfig};
