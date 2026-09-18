//! Telcoin Network node-record API.
//!
//! A lightweight HTTP/JSON daemon that backs a public website listing validators' advertised
//! JSON-RPC endpoints. On a schedule it fetches BLS-signed `NodeRecord`s from a worker kademlia
//! DHT through [`tn_kad_client`], caches the verified records in memory, and serves them over a
//! small read-only HTTP surface. It never runs a node and never puts load on a validator's RPC to
//! obtain the records themselves: each record is self-authenticating (signed by the validator's
//! BLS key for exactly this chain and worker network), so the DHT is the only source of truth.
//!
//! The crate is a library plus a thin `node-record-api` binary so the integration test in
//! `tests/` can drive one refresh cycle against a real worker swarm; see [`refresh::run_cycle`].
//! Module map:
//!
//! - [`cli`]: flags, environment fallbacks, and startup validation into [`cli::Settings`];
//! - [`keys`]: where the set of BLS keys to look up comes from (RPC committee, committee file,
//!   static list) and how the sources combine;
//! - [`epoch`]: epoch-boundary derivation and the refresh scheduler;
//! - [`refresh`]: one refresh cycle and the loop that runs it;
//! - [`cache`]: the in-memory record cache and its eviction rules;
//! - [`api`]: the HTTP routes and JSON shapes;
//! - [`server`] / [`ratelimit`]: the hardened accept loop and edge rate limiting, copied from
//!   `bin/worker-gateway`;
//! - [`readiness`] / [`telemetry`]: the readiness rule and the Prometheus vocabulary.

pub mod api;
pub mod app;
pub mod cache;
pub mod cli;
pub mod epoch;
pub mod error;
pub mod keys;
pub mod ratelimit;
pub mod readiness;
pub mod refresh;
pub mod server;
pub mod telemetry;

// dev-dependencies exercised only by the integration test in `tests/`; named here so the lib's
// own unit-test build does not trip `unused_crate_dependencies`
#[cfg(test)]
use tn_config as _;
#[cfg(test)]
use tn_network_libp2p as _;
#[cfg(test)]
use tn_storage as _;
#[cfg(test)]
use tn_test_utils as _;
