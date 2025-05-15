//! Test utils.

mod tracing;
pub use tracing::*;

use clap::{Args, Parser};
/// A helper type to parse Args more easily.
#[derive(Parser, Debug)]
pub struct CommandParser<T: Args> {
    #[clap(flatten)]
    pub args: T,
}
