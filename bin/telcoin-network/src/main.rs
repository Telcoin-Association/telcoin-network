use clap::Parser as _;
#[cfg(not(feature = "faucet"))]
use reth::commands::node::NoArgs;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
#[cfg(feature = "faucet")]
use tn_faucet::FaucetArgs;
use tn_node::launch_node;

// We use jemalloc for performance reasons
#[cfg(all(feature = "jemalloc", unix))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() {
    #[cfg(not(feature = "faucet"))]
    if let Err(err) =
        telcoin_network::cli::Cli::<NoArgs>::parse().run(|builder, _, tn_datadir| async move {
            let executor = EthExecutorProvider::new(std::sync::Arc::clone(&builder.node_config.chain), EthEvmConfig::default());
            launch_node(builder, executor, tn_datadir).await
        })
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }

    #[cfg(feature = "faucet")]
    if let Err(err) = telcoin_network::cli::Cli::<FaucetArgs>::parse().run(
        |mut builder, faucet, tn_datadir| async move {
            builder.opt_faucet_args = Some(faucet);
            let executor = EthExecutorProvider::new(std::sync::Arc::clone(&builder.node_config.chain), EthEvmConfig::default());
            launch_node(builder, executor, tn_datadir).await
        },
    ) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
