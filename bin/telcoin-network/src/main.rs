use clap::Parser as _;
#[cfg(not(feature = "faucet"))]
use reth_cli_commands::node::NoArgs;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use std::sync::Arc;
#[cfg(feature = "faucet")]
use tn_faucet::FaucetArgs;
use tn_node::launch_node;

// We use jemalloc for performance reasons
#[cfg(all(feature = "jemalloc", unix))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    #[cfg(not(feature = "faucet"))]
    if let Err(err) =
        telcoin_network::cli::Cli::<NoArgs>::parse().run(|builder, _, tn_datadir| async move {
            let evm_config = EthEvmConfig::default();
            let executor =
                EthExecutorProvider::new(Arc::clone(&builder.node_config.chain), evm_config);
            launch_node(builder, executor, evm_config, &tn_datadir).await
        })
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }

    #[cfg(feature = "faucet")]
    if let Err(err) = telcoin_network::cli::Cli::<FaucetArgs>::parse().run(
        |mut builder, faucet, tn_datadir| async move {
            builder.opt_faucet_args = Some(faucet);
            let evm_config = EthEvmConfig::default();
            let executor =
                EthExecutorProvider::new(Arc::clone(&builder.node_config.chain), evm_config);
            launch_node(builder, executor, evm_config, &tn_datadir).await
        },
    ) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
