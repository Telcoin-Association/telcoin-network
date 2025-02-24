use clap::{Args, Parser as _};
#[cfg(feature = "faucet")]
use tn_faucet::FaucetArgs;
use tn_node::launch_node;

/// No Additional arguments
#[derive(Debug, Clone, Copy, Default, Args)]
#[non_exhaustive]
pub struct NoArgs;

fn main() {
    #[cfg(not(feature = "faucet"))]
    if let Err(err) = telcoin_network::cli::Cli::<NoArgs>::parse()
        .run(|builder, _, tn_datadir| launch_node(builder, tn_datadir))
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }

    #[cfg(feature = "faucet")]
    if let Err(err) =
        telcoin_network::cli::Cli::<FaucetArgs>::parse().run(|mut builder, faucet, tn_datadir| {
            builder.opt_faucet_args = Some(faucet);
            launch_node(builder, tn_datadir)
        })
    {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
