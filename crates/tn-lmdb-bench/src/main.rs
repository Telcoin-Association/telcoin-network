//! The `tn-lmdb-bench` command: see the library's docs and the crate's `README.md`.

// The library uses these; the binary only calls it.
use serde as _;
use serde_json as _;
use tempfile as _;
use tn_lmdb as _;

fn main() {
    std::process::exit(tn_lmdb_bench::cli_main(std::env::args().skip(1).collect()));
}
