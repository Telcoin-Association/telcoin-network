//! Builder for engine to mantain generics.

use super::{inner::ExecutionNodeInner, TnBuilder};
use tn_config::Config;
use tn_exex::TnExExManagerHandle;
use tn_reth::RethEnv;

/// A builder that handles component initialization for the execution node.
/// Separates initialization concerns from runtime behavior.
#[derive(Debug)]
pub(super) struct ExecutionNodeBuilder {
    /// The protocol configuration.
    tn_config: Config,

    /// Reth environment for executing transactions.
    reth_env: RethEnv,

    /// ExEx manager handle for notifying Execution Extensions.
    exex_handle: TnExExManagerHandle,
}

impl ExecutionNodeBuilder {
    /// Start the builder with required components
    pub(super) fn new(
        tn_builder: &TnBuilder,
        reth_env: RethEnv,
        exex_handle: TnExExManagerHandle,
    ) -> Self {
        let TnBuilder { tn_config, .. } = tn_builder;

        Self { reth_env, tn_config: tn_config.clone(), exex_handle }
    }

    /// Build the final ExecutionNodeInner
    pub(super) fn build(self) -> eyre::Result<ExecutionNodeInner> {
        // Ensure all required components are initialized

        Ok(ExecutionNodeInner {
            reth_env: self.reth_env,
            address: *self.tn_config.execution_address(),
            tn_config: self.tn_config,
            workers: Vec::default(),
            exex_handle: self.exex_handle,
        })
    }
}
