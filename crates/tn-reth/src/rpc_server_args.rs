//! Contains RPC server args.
//! This is a subset of args Reth's RPC (mostly taken from Reth) provides to eliminate arguments we
//! don't support.

use std::{
    ffi::OsStr,
    net::{IpAddr, Ipv4Addr},
};

use alloy::rpc::types::engine::JwtSecret;
use clap::{
    builder::{PossibleValue, RangedU64ValueParser, TypedValueParser},
    command, Arg, Args, Command,
};
use reth::{
    args::{
        types::{MaxOr, MaxU32, ZeroAsNoneU64},
        RpcStateCacheArgs,
    },
    rpc::builder::{constants, RethRpcModule, RpcModuleSelection},
};
use reth_cli_util::parse_ether_value;

/// The default IPC endpoint
#[cfg(windows)]
pub const DEFAULT_IPC_ENDPOINT: &str = r"\\.\pipe\tn.ipc";

/// The default IPC endpoint
#[cfg(not(windows))]
pub const DEFAULT_IPC_ENDPOINT: &str = "/tmp/tn.ipc";

/// Default max number of subscriptions per connection.
pub(crate) const RPC_DEFAULT_MAX_SUBS_PER_CONN: u32 = 1024;

/// Default max request size in MB.
pub(crate) const RPC_DEFAULT_MAX_REQUEST_SIZE_MB: u32 = 15;

/// Default max response size in MB.
///
/// This is only relevant for very large trace responses.
pub(crate) const RPC_DEFAULT_MAX_RESPONSE_SIZE_MB: u32 = 160;

/// Default number of incoming connections.
pub(crate) const RPC_DEFAULT_MAX_CONNECTIONS: u32 = 500;

/// Parameters for configuring the rpc more granularity via CLI
/// This is from Reth minus some options we don't support.
#[derive(Debug, Clone, Args, PartialEq, Eq)]
#[command(next_help_heading = "RPC")]
pub struct RpcServerArgs {
    /// Enable the HTTP-RPC server
    #[arg(long, default_value_if("dev", "true", "true"))]
    pub http: bool,

    /// Http server address to listen on
    #[arg(long = "http.addr", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    pub http_addr: IpAddr,

    /// Http server port to listen on
    #[arg(long = "http.port", default_value_t = constants::DEFAULT_HTTP_RPC_PORT)]
    pub http_port: u16,

    /// Disable compression for HTTP responses
    #[arg(long = "http.disable-compression", default_value_t = false)]
    pub http_disable_compression: bool,

    /// Rpc Modules to be configured for the HTTP server
    #[arg(long = "http.api", value_parser = RpcModuleSelectionValueParser::default())]
    pub http_api: Option<RpcModuleSelection>,

    /// Http Corsdomain to allow request from
    #[arg(long = "http.corsdomain")]
    pub http_corsdomain: Option<String>,

    /// Enable the WS-RPC server
    #[arg(long)]
    pub ws: bool,

    /// Ws server address to listen on
    #[arg(long = "ws.addr", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
    pub ws_addr: IpAddr,

    /// Ws server port to listen on
    #[arg(long = "ws.port", default_value_t = constants::DEFAULT_WS_RPC_PORT)]
    pub ws_port: u16,

    /// Origins from which to accept `WebSocket` requests
    #[arg(id = "ws.origins", long = "ws.origins", alias = "ws.corsdomain")]
    pub ws_allowed_origins: Option<String>,

    /// Rpc Modules to be configured for the WS server
    #[arg(long = "ws.api", value_parser = RpcModuleSelectionValueParser::default())]
    pub ws_api: Option<RpcModuleSelection>,

    /// Disable the IPC-RPC server
    #[arg(long)]
    pub ipcdisable: bool,

    /// Filename for IPC socket/pipe within the datadir
    #[arg(long, default_value_t = DEFAULT_IPC_ENDPOINT.to_string())]
    pub ipcpath: String,

    /// Hex encoded JWT secret to authenticate the regular RPC server(s), see `--http.api` and
    /// `--ws.api`.
    ///
    /// This is __not__ used for the authenticated engine-API RPC server, see
    /// `--authrpc.jwtsecret`.
    #[arg(long = "rpc.jwtsecret", value_name = "HEX", global = true, required = false)]
    pub rpc_jwtsecret: Option<JwtSecret>,

    /// Set the maximum RPC request payload size for both HTTP and WS in megabytes.
    #[arg(long = "rpc.max-request-size", alias = "rpc-max-request-size", default_value_t = RPC_DEFAULT_MAX_REQUEST_SIZE_MB.into())]
    pub rpc_max_request_size: MaxU32,

    /// Set the maximum RPC response payload size for both HTTP and WS in megabytes.
    #[arg(long = "rpc.max-response-size", alias = "rpc-max-response-size", visible_alias = "rpc.returndata.limit", default_value_t = RPC_DEFAULT_MAX_RESPONSE_SIZE_MB.into())]
    pub rpc_max_response_size: MaxU32,

    /// Set the maximum concurrent subscriptions per connection.
    #[arg(long = "rpc.max-subscriptions-per-connection", alias = "rpc-max-subscriptions-per-connection", default_value_t = RPC_DEFAULT_MAX_SUBS_PER_CONN.into())]
    pub rpc_max_subscriptions_per_connection: MaxU32,

    /// Maximum number of RPC server connections.
    #[arg(long = "rpc.max-connections", alias = "rpc-max-connections", value_name = "COUNT", default_value_t = RPC_DEFAULT_MAX_CONNECTIONS.into())]
    pub rpc_max_connections: MaxU32,

    /// Maximum number of concurrent tracing requests.
    ///
    /// By default this chooses a sensible value based on the number of available cores.
    /// Tracing requests are generally CPU bound.
    /// Choosing a value that is higher than the available CPU cores can have a negative impact on
    /// the performance of the node and affect the node's ability to maintain sync.
    #[arg(long = "rpc.max-tracing-requests", alias = "rpc-max-tracing-requests", value_name = "COUNT", default_value_t = constants::default_max_tracing_requests())]
    pub rpc_max_tracing_requests: usize,

    /// Maximum number of blocks for `trace_filter` requests.
    #[arg(long = "rpc.max-trace-filter-blocks", alias = "rpc-max-trace-filter-blocks", value_name = "COUNT", default_value_t = constants::DEFAULT_MAX_TRACE_FILTER_BLOCKS)]
    pub rpc_max_trace_filter_blocks: u64,

    /// Maximum number of blocks that could be scanned per filter request. (0 = entire chain)
    #[arg(long = "rpc.max-blocks-per-filter", alias = "rpc-max-blocks-per-filter", value_name = "COUNT", default_value_t = ZeroAsNoneU64::new(constants::DEFAULT_MAX_BLOCKS_PER_FILTER))]
    pub rpc_max_blocks_per_filter: ZeroAsNoneU64,

    /// Maximum number of logs that can be returned in a single response. (0 = no limit)
    #[arg(long = "rpc.max-logs-per-response", alias = "rpc-max-logs-per-response", value_name = "COUNT", default_value_t = ZeroAsNoneU64::new(constants::DEFAULT_MAX_LOGS_PER_RESPONSE as u64))]
    pub rpc_max_logs_per_response: ZeroAsNoneU64,

    /// Maximum gas limit for `eth_call` and call tracing RPC methods.
    #[arg(
        long = "rpc.gascap",
        alias = "rpc-gascap",
        value_name = "GAS_CAP",
        value_parser = MaxOr::new(RangedU64ValueParser::<u64>::new().range(1..)),
        default_value_t = constants::gas_oracle::RPC_DEFAULT_GAS_CAP
    )]
    pub rpc_gas_cap: u64,

    /// Maximum eth transaction fee (in ether) that can be sent via the RPC APIs (0 = no cap)
    #[arg(
        long = "rpc.txfeecap",
        alias = "rpc-txfeecap",
        value_name = "TX_FEE_CAP",
        value_parser = parse_ether_value,
        default_value = "1.0"
    )]
    pub rpc_tx_fee_cap: u128,

    /// Maximum number of blocks for `eth_simulateV1` call.
    #[arg(
        long = "rpc.max-simulate-blocks",
        value_name = "BLOCKS_COUNT",
        default_value_t = constants::DEFAULT_MAX_SIMULATE_BLOCKS
    )]
    pub rpc_max_simulate_blocks: u64,

    /// The maximum proof window for historical proof generation.
    /// This value allows for generating historical proofs up to
    /// configured number of blocks from current tip (up to `tip - window`).
    #[arg(
        long = "rpc.eth-proof-window",
        default_value_t = constants::DEFAULT_ETH_PROOF_WINDOW,
        value_parser = RangedU64ValueParser::<u64>::new().range(..=constants::MAX_ETH_PROOF_WINDOW)
    )]
    pub rpc_eth_proof_window: u64,

    /// Maximum number of concurrent getproof requests.
    #[arg(long = "rpc.proof-permits", alias = "rpc-proof-permits", value_name = "COUNT", default_value_t = constants::DEFAULT_PROOF_PERMITS)]
    pub rpc_proof_permits: usize,

    /// State cache configuration.
    #[command(flatten)]
    pub rpc_state_cache: RpcStateCacheArgs,
}

impl Default for RpcServerArgs {
    fn default() -> Self {
        Self {
            http: false,
            http_addr: Ipv4Addr::LOCALHOST.into(),
            http_port: constants::DEFAULT_HTTP_RPC_PORT,
            http_disable_compression: false,
            http_api: None,
            http_corsdomain: None,
            ws: false,
            ws_addr: Ipv4Addr::LOCALHOST.into(),
            ws_port: constants::DEFAULT_WS_RPC_PORT,
            ws_allowed_origins: None,
            ws_api: None,
            ipcdisable: false,
            ipcpath: constants::DEFAULT_IPC_ENDPOINT.to_string(),
            rpc_jwtsecret: None,
            rpc_max_request_size: RPC_DEFAULT_MAX_REQUEST_SIZE_MB.into(),
            rpc_max_response_size: RPC_DEFAULT_MAX_RESPONSE_SIZE_MB.into(),
            rpc_max_subscriptions_per_connection: RPC_DEFAULT_MAX_SUBS_PER_CONN.into(),
            rpc_max_connections: RPC_DEFAULT_MAX_CONNECTIONS.into(),
            rpc_max_tracing_requests: constants::default_max_tracing_requests(),
            rpc_max_trace_filter_blocks: constants::DEFAULT_MAX_TRACE_FILTER_BLOCKS,
            rpc_max_blocks_per_filter: constants::DEFAULT_MAX_BLOCKS_PER_FILTER.into(),
            rpc_max_logs_per_response: (constants::DEFAULT_MAX_LOGS_PER_RESPONSE as u64).into(),
            rpc_gas_cap: constants::gas_oracle::RPC_DEFAULT_GAS_CAP,
            rpc_tx_fee_cap: constants::DEFAULT_TX_FEE_CAP_WEI,
            rpc_max_simulate_blocks: constants::DEFAULT_MAX_SIMULATE_BLOCKS,
            rpc_eth_proof_window: constants::DEFAULT_ETH_PROOF_WINDOW,
            rpc_state_cache: RpcStateCacheArgs::default(),
            rpc_proof_permits: constants::DEFAULT_PROOF_PERMITS,
        }
    }
}

impl From<RpcServerArgs> for reth::args::RpcServerArgs {
    fn from(v: RpcServerArgs) -> Self {
        reth::args::RpcServerArgs {
            http: v.http,
            http_addr: v.http_addr,
            http_port: v.http_port,
            http_disable_compression: v.http_disable_compression,
            http_api: v.http_api,
            http_corsdomain: v.http_corsdomain,
            ws: v.ws,
            ws_addr: v.ws_addr,
            ws_port: v.ws_port,
            ws_allowed_origins: v.ws_allowed_origins,
            ws_api: v.ws_api,
            ipcdisable: v.ipcdisable,
            ipcpath: v.ipcpath,
            rpc_jwtsecret: v.rpc_jwtsecret,
            rpc_max_request_size: v.rpc_max_request_size,
            rpc_max_response_size: v.rpc_max_response_size,
            rpc_max_subscriptions_per_connection: v.rpc_max_subscriptions_per_connection,
            rpc_max_connections: v.rpc_max_connections,
            rpc_max_tracing_requests: v.rpc_max_tracing_requests,
            rpc_max_trace_filter_blocks: v.rpc_max_trace_filter_blocks,
            rpc_max_blocks_per_filter: v.rpc_max_blocks_per_filter,
            rpc_max_logs_per_response: v.rpc_max_logs_per_response,
            rpc_gas_cap: v.rpc_gas_cap,
            rpc_tx_fee_cap: v.rpc_tx_fee_cap,
            rpc_max_simulate_blocks: v.rpc_max_simulate_blocks,
            rpc_eth_proof_window: v.rpc_eth_proof_window,
            rpc_proof_permits: v.rpc_proof_permits,
            rpc_state_cache: v.rpc_state_cache,
            ..Default::default()
        }
    }
}

/// clap value parser for [`RpcModuleSelection`].
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
struct RpcModuleSelectionValueParser;

impl TypedValueParser for RpcModuleSelectionValueParser {
    type Value = RpcModuleSelection;

    fn parse_ref(
        &self,
        _cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let val =
            value.to_str().ok_or_else(|| clap::Error::new(clap::error::ErrorKind::InvalidUtf8))?;
        val.parse::<RpcModuleSelection>().map_err(|err| {
            let arg = arg.map(|a| a.to_string()).unwrap_or_else(|| "...".to_owned());
            let possible_values = RethRpcModule::all_variant_names().to_vec().join(",");
            let msg = format!(
                "Invalid value '{val}' for {arg}: {err}.\n    [possible values: {possible_values}]"
            );
            clap::Error::raw(clap::error::ErrorKind::InvalidValue, msg)
        })
    }

    fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
        let values = RethRpcModule::all_variant_names().iter().map(PossibleValue::new);
        Some(Box::new(values))
    }
}
