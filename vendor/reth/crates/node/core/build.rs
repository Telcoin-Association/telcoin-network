#![allow(missing_docs)]

use std::{env, error::Error, path::Path};
use vergen::{BuildBuilder, CargoBuilder, Emitter};

fn main() -> Result<(), Box<dyn Error>> {
    let mut emitter = Emitter::default();

    let build_builder = BuildBuilder::default().build_timestamp(true).build()?;

    emitter.add_instructions(&build_builder)?;

    let cargo_builder = CargoBuilder::default().features(true).target_triple(true).build()?;

    emitter.add_instructions(&cargo_builder)?;

    emitter.emit_and_set()?;

    // TN(TN-0020): describe the upstream commit this copy was taken from (`tn/upstream.toml`),
    // not TN's git state. Upstream ran vergen-git2 here, which inside TN's repository rebuilt
    // this crate, and everything that depends on it, on every TN commit, and built libgit2.
    let pin_path = Path::new(&env::var("CARGO_MANIFEST_DIR")?).join("../../../tn/upstream.toml");
    println!("cargo:rerun-if-changed={}", pin_path.display());
    let pin = std::fs::read_to_string(&pin_path)?;
    let sha = pin_value(&pin, "commit").ok_or("tn/upstream.toml has no `commit`")?;
    println!("cargo:rustc-env=VERGEN_GIT_SHA={sha}");
    let sha_short = &sha[0..7];

    // TN carries changes on top of the pin (vendor/reth/CHANGES.md).
    let version_suffix = "-tn";
    println!("cargo:rustc-env=RETH_VERSION_SUFFIX={version_suffix}");

    // Set short SHA
    println!("cargo:rustc-env=VERGEN_GIT_SHA_SHORT={}", &sha[..8]);

    // Set the build profile
    let out_dir = env::var("OUT_DIR").unwrap();
    let profile = out_dir.rsplit(std::path::MAIN_SEPARATOR).nth(3).unwrap();
    println!("cargo:rustc-env=RETH_BUILD_PROFILE={profile}");

    // Set formatted version strings
    let pkg_version = env!("CARGO_PKG_VERSION");

    // The short version information for reth.
    // - The latest version from Cargo.toml
    // - The short SHA of the latest commit.
    // Example: 0.1.0 (defa64b2)
    println!("cargo:rustc-env=RETH_SHORT_VERSION={pkg_version}{version_suffix} ({sha_short})");

    // LONG_VERSION
    // The long version information for reth.
    //
    // - The latest version from Cargo.toml + version suffix (if any)
    // - The full SHA of the latest commit
    // - The build datetime
    // - The build features
    // - The build profile
    //
    // Example:
    //
    // ```text
    // Version: 0.1.0
    // Commit SHA: defa64b2
    // Build Timestamp: 2023-05-19T01:47:19.815651705Z
    // Build Features: jemalloc
    // Build Profile: maxperf
    // ```
    println!("cargo:rustc-env=RETH_LONG_VERSION_0=Version: {pkg_version}{version_suffix}");
    println!("cargo:rustc-env=RETH_LONG_VERSION_1=Commit SHA: {sha}");
    println!(
        "cargo:rustc-env=RETH_LONG_VERSION_2=Build Timestamp: {}",
        env::var("VERGEN_BUILD_TIMESTAMP")?
    );
    println!(
        "cargo:rustc-env=RETH_LONG_VERSION_3=Build Features: {}",
        env::var("VERGEN_CARGO_FEATURES")?
    );
    println!("cargo:rustc-env=RETH_LONG_VERSION_4=Build Profile: {profile}");

    // The version information for reth formatted for P2P (devp2p).
    // - The latest version from Cargo.toml
    // - The target triple
    //
    // Example: reth/v0.1.0-alpha.1-428a6dc2f/aarch64-apple-darwin
    println!(
        "cargo:rustc-env=RETH_P2P_CLIENT_VERSION={}",
        format_args!("reth/v{pkg_version}-{sha_short}/{}", env::var("VERGEN_CARGO_TARGET_TRIPLE")?)
    );

    Ok(())
}

/// TN(TN-0020): reads `key = "value"` from the flat pin file.
fn pin_value(toml: &str, key: &str) -> Option<String> {
    toml.lines().find_map(|line| {
        let (k, v) = line.split_once('=')?;
        (k.trim() == key).then(|| v.trim().trim_matches('"').to_string())
    })
}
