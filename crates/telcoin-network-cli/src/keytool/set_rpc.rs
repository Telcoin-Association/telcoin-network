//! `set-rpc` subcommand: set or clear the worker JSON-RPC endpoint advertised
//! in `node-info.yaml`.
//!
//! A node advertises an optional JSON-RPC endpoint to peers over kademlia so
//! wallets/dapps can discover where to submit transactions. That endpoint lives
//! in `node_info.p2p_info.worker.rpc` (type `Option<RpcInfo>`). The runtime
//! reads it at node startup, validates it, and hands it to the worker network
//! for advertisement - this command only populates the config field.
//!
//! Unlike `generate validator|observer` or `generate pop`, this is a pure,
//! unsigned config edit: it loads `node-info.yaml`, mutates the worker RPC
//! descriptor, and writes it back. No keys are read and the BLS passphrase is
//! ignored - the kademlia node record is signed at node startup, not here.
//!
//! The [`build_worker_rpc`] helper is the single source of truth for building
//! and validating a worker RPC endpoint; it is shared with
//! `generate validator|observer` so both commands apply identical validation.

use crate::args::clap_url_parser;
use clap::Args;
use eyre::WrapErr as _;
use tn_config::{Config, ConfigFmt, ConfigTrait as _, NodeInfo, TelcoinDirs};
use tn_types::RpcInfo;
use tracing::{info, warn};
use url::Url;

/// Build a validated worker `RpcInfo` from the `--http`/`--ws` (or `--rpc-http`/`--rpc-ws`) flags.
///
/// Returns `Ok(None)` when no HTTP endpoint was provided (nothing to advertise). Applies the same
/// validation node startup runs (`node.rs:624`), so a bad scheme/length fails at CLI time. Shared by
/// `set-rpc` and `generate validator|observer`.
pub(crate) fn build_worker_rpc(
    http: Option<Url>,
    ws: Option<Url>,
) -> eyre::Result<Option<RpcInfo>> {
    match http {
        Some(http) => {
            let rpc = RpcInfo { http, ws };
            rpc.validate().wrap_err("invalid worker rpc endpoint")?;
            Ok(Some(rpc))
        }
        None => Ok(None),
    }
}

/// Set or clear the worker JSON-RPC endpoint advertised in `node-info.yaml`.
///
/// `set-rpc --http URL [--ws URL]` sets the worker RPC descriptor;
/// `set-rpc --clear` removes it. The endpoint is validated with the same check
/// node startup applies, so a bad scheme fails here rather than at runtime.
#[derive(Debug, Clone, Args)]
pub struct SetRpcArgs {
    /// HTTP(S) JSON-RPC endpoint to advertise (e.g. https://validator.example.com:8545/).
    ///
    /// Required unless `--clear`.
    #[arg(
        long,
        value_name = "URL",
        value_parser = clap_url_parser,
        required_unless_present = "clear",
        conflicts_with = "clear"
    )]
    pub http: Option<Url>,

    /// Optional WebSocket JSON-RPC endpoint (e.g. wss://validator.example.com:8546/).
    #[arg(long, value_name = "URL", value_parser = clap_url_parser, conflicts_with = "clear")]
    pub ws: Option<Url>,

    /// Remove any previously-set worker RPC endpoint instead of setting one.
    #[arg(long)]
    pub clear: bool,
}

impl SetRpcArgs {
    /// Set or clear the worker RPC endpoint in `node-info.yaml`.
    pub fn execute<TND: TelcoinDirs>(&self, dir: &TND) -> eyre::Result<()> {
        // existing node-info - this is a config-only edit; error if absent so we
        // never silently create a fresh node-info with default identity.
        let mut node_info =
            Config::load_from_path::<NodeInfo>(dir.node_info_path(), ConfigFmt::YAML).map_err(
                |e| {
                    eyre::eyre!(
                        "could not load node-info.yaml at {}: {e}\n\
                         hint: generate keys first with `keytool generate validator|observer`",
                        dir.node_info_path().display()
                    )
                },
            )?;

        if self.clear {
            if node_info.p2p_info.worker.rpc.take().is_some() {
                warn!(target: "tn::keytool", "cleared existing worker RPC endpoint");
            } else {
                info!(target: "tn::keytool", "no worker RPC endpoint set; nothing to clear");
            }
        } else {
            // clap guarantees --http is present unless --clear.
            let rpc = build_worker_rpc(self.http.clone(), self.ws.clone())?
                .expect("clap requires --http unless --clear");
            if node_info.p2p_info.worker.rpc.is_some() {
                warn!(target: "tn::keytool", "overwriting existing worker RPC endpoint");
            }
            node_info.p2p_info.worker.rpc = Some(rpc);
        }

        Config::write_to_path(dir.node_info_path(), &node_info, ConfigFmt::YAML)?;
        println!("OK  node-info.yaml updated: {}", dir.node_info_path().display());
        match &node_info.p2p_info.worker.rpc {
            Some(rpc) => {
                println!("    worker rpc http: {}", rpc.http);
                if let Some(ws) = &rpc.ws {
                    println!("    worker rpc ws:   {ws}");
                }
            }
            None => println!("    worker rpc: cleared"),
        }
        Ok(())
    }
}
