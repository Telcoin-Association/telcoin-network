//! Builder for engine to mantain generics.

use super::{inner::ExecutionNodeInner, TnBuilder};
use std::collections::HashMap;
use tn_config::Config;
use tn_faucet::FaucetArgs;
use tn_reth::RethEnv;

/// A builder that handles component initialization for the execution node.
/// Separates initialization concerns from runtime behavior.
pub struct ExecutionNodeBuilder {
    tn_config: Config,

    // Reth environment
    reth_env: RethEnv,

    // Optional components
    opt_faucet_args: Option<FaucetArgs>,
}

impl ExecutionNodeBuilder {
    /// Start the builder with required components
    pub fn new(tn_builder: &TnBuilder, reth_env: RethEnv) -> Self {
        let TnBuilder { node_config: _, tn_config, opt_faucet_args, consensus_metrics: _ } =
            tn_builder;

        Self { reth_env, tn_config: tn_config.clone(), opt_faucet_args: opt_faucet_args.clone() }
    }

    /// Build the final ExecutionNodeInner
    pub fn build(self) -> eyre::Result<ExecutionNodeInner> {
        // Ensure all required components are initialized

        Ok(ExecutionNodeInner {
            reth_env: self.reth_env,
            address: *self.tn_config.execution_address(),
            opt_faucet_args: self.opt_faucet_args,
            tn_config: self.tn_config,
            workers: HashMap::default(),
        })
    }
}
