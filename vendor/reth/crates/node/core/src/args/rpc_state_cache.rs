use clap::{builder::RangedU64ValueParser, Args};
use reth_rpc_server_types::constants::cache::{
    DEFAULT_BLOCK_CACHE_MAX_LEN, DEFAULT_CONCURRENT_DB_REQUESTS, DEFAULT_HEADER_CACHE_MAX_LEN,
    DEFAULT_MAX_CACHED_TX_HASHES, DEFAULT_RECEIPT_CACHE_MAX_LEN,
};

/// TN(TN-0061): the most permits a tokio semaphore holds (`Semaphore::MAX_PERMITS`, spelled
/// out because this crate's tokio has no `sync` feature); a larger count panics while the RPC
/// server is built.
const MAX_PERMITS: u64 = (usize::MAX >> 3) as u64;

/// Parameters to configure RPC state cache.
#[derive(Debug, Clone, Args, PartialEq, Eq)]
#[command(next_help_heading = "RPC State Cache")]
pub struct RpcStateCacheArgs {
    /// Max number of blocks in cache.
    #[arg(
        long = "rpc-cache.max-blocks",
        default_value_t = DEFAULT_BLOCK_CACHE_MAX_LEN,
    )]
    pub max_blocks: u32,

    /// Max number receipts in cache.
    #[arg(
        long = "rpc-cache.max-receipts",
        default_value_t = DEFAULT_RECEIPT_CACHE_MAX_LEN,
    )]
    pub max_receipts: u32,

    /// Max number of headers in cache.
    #[arg(
        long = "rpc-cache.max-headers",
        alias = "rpc-cache.max-envs",
        default_value_t = DEFAULT_HEADER_CACHE_MAX_LEN,
    )]
    pub max_headers: u32,

    /// Max number of concurrent database requests.
    ///
    /// TN(TN-0061): at least 1 (with none, every cache miss waits forever) and at most what a
    /// tokio semaphore holds (more panics while the RPC server is built).
    #[arg(
        long = "rpc-cache.max-concurrent-db-requests",
        value_parser = RangedU64ValueParser::<usize>::new().range(1..=MAX_PERMITS),
        default_value_t = DEFAULT_CONCURRENT_DB_REQUESTS,
    )]
    pub max_concurrent_db_requests: usize,

    /// Maximum number of transaction hashes to cache for transaction lookups.
    #[arg(
        long = "rpc-cache.max-cached-tx-hashes",
        default_value_t = DEFAULT_MAX_CACHED_TX_HASHES,
    )]
    pub max_cached_tx_hashes: u32,
}

impl RpcStateCacheArgs {
    /// Sets the Cache sizes to zero, effectively disabling caching.
    pub const fn set_zero_lengths(&mut self) {
        self.max_blocks = 0;
        self.max_receipts = 0;
        self.max_headers = 0;
    }
}

impl Default for RpcStateCacheArgs {
    fn default() -> Self {
        Self {
            max_blocks: DEFAULT_BLOCK_CACHE_MAX_LEN,
            max_receipts: DEFAULT_RECEIPT_CACHE_MAX_LEN,
            max_headers: DEFAULT_HEADER_CACHE_MAX_LEN,
            max_concurrent_db_requests: DEFAULT_CONCURRENT_DB_REQUESTS,
            max_cached_tx_hashes: DEFAULT_MAX_CACHED_TX_HASHES,
        }
    }
}
