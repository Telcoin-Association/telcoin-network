//! Create metadata for cli build.
use std::error::Error;
use vergen::EmitBuilder;

/// Metadata for current build.
fn main() -> Result<(), Box<dyn Error>> {
    // Only rerun build script when git state or dependencies change
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/refs/heads/");
    println!("cargo:rerun-if-changed=../../Cargo.lock");

    // Emit the instructions
    EmitBuilder::builder()
        .git_sha(true)
        .build_timestamp()
        .cargo_features()
        .cargo_target_triple()
        .emit()?;
    Ok(())
}
