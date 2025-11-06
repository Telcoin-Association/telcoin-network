//! Create metadata for cli build.
use std::error::Error;
use vergen::EmitBuilder;

/// Metadata for current build.
fn main() -> Result<(), Box<dyn Error>> {
    // Emit the instructions
    EmitBuilder::builder()
        .git_sha(true)
        .build_timestamp()
        .cargo_features()
        .cargo_target_triple()
        .emit()?;
    Ok(())
}
