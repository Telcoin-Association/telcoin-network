//! Shared code for the busy-node execution bench, behind the `test-utils` feature.
//!
//! - [`CaptureRecorder`]: a global metrics recorder that keeps the raw samples of chosen histograms
//!   for the bench's percentiles.
//! - [`env`]: the bench's environment: [`open_production_env`] opens a reth env as the node opens
//!   it ([`open_production_env_with_db`] with its database handle), [`clone_datadir`] copies a
//!   closed datadir cheaply and, on Linux, evicts the copy from the page cache,
//!   [`announce_temp_dir`] refuses a temp dir on a memory file system, and [`db_file_bytes`] and
//!   [`dir_bytes`] measure the database and the static files on disk.
//! - The open database's own reads, the same on either backend: [`db_counters`](fn@db_counters)
//!   reads its cumulative page and write counters as [`DbCounters`] (MDBX's page operations or
//!   tndb-b's I/O counters, by name; [`DbCounters::since`] gives what happened between two reads),
//!   and [`db_page_size`] the page size its file was created with.
//! - [`state`]: the prefilled genesis, [`PrefillSpec`]: keyed senders, funded accounts, two ERC20
//!   tokens with prefilled balances and allowances, and a swap router with prefilled pools.
//! - [`base`]: the cached base datadir: [`build_base`] builds a spec's genesis once per backend and
//!   page size into a cache dir, and [`slim_chain`] reopens a clone of it with the cached genesis
//!   header, without recomputing the prefilled genesis.
//! - [`workload`]: the transactions, [`Workload`]: a seeded mix of native transfers, ERC20
//!   transfers, swaps and contract creations over a spec's state, to hot and fresh recipients, from
//!   the senders' on-chain nonces, cut into batches by exact expected gas.
//! - [`output`]: the consensus outputs, [`OutputFactory`]: a workload's batches packed into
//!   production-shaped `ConsensusOutput`s for `tn_engine::execute_consensus_output`, chained after
//!   the env's last executed output with rising commit times.
//! - [`readers`]: the concurrent side load of a busy node: [`Readers`], reader threads calling the
//!   in-process RPC (`eth_getBalance`, `eth_call` of `balanceOf`, `eth_getTransactionReceipt`,
//!   `eth_getLogs`, `eth_getBlockByNumber`) and the batch builder's balance poll behind one on/off
//!   gate, and [`Submitter`], `eth_sendRawTransaction` of chosen raw transactions, each draining
//!   per-window latency samples and error counts.

pub mod base;
mod db_counters;
pub mod env;
pub mod output;
pub mod readers;
mod recorder;
pub mod state;
pub mod workload;

pub use base::{build_base, slim_chain, BaseMeta};
pub use db_counters::{db_counters, db_page_size, DbCounters};
pub use env::{
    announce_temp_dir, clone_datadir, db_file_bytes, dir_bytes, open_production_env,
    open_production_env_with_db,
};
pub use output::{LoadOutput, OutputConfig, OutputFactory};
pub use readers::{poll_addresses, ReadKind, ReadSamples, Readers, SubmitSamples, Submitter};
pub use recorder::CaptureRecorder;
pub use state::PrefillSpec;
pub use workload::Workload;
