#![allow(missing_docs)]

use std::{
    env,
    error::Error,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

fn main() -> Result<(), Box<dyn Error>> {
    // TN(TN-0025): the three values upstream took from vergen come from cargo and the clock, so
    // vergen and its build-time dependencies are not compiled.
    let build_timestamp = build_timestamp()?;
    let cargo_features = cargo_features();
    let target_triple = env::var("TARGET")?;
    println!("cargo:rustc-env=VERGEN_BUILD_TIMESTAMP={build_timestamp}");
    println!("cargo:rustc-env=VERGEN_CARGO_FEATURES={cargo_features}");
    println!("cargo:rustc-env=VERGEN_CARGO_TARGET_TRIPLE={target_triple}");

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
    println!("cargo:rustc-env=RETH_LONG_VERSION_2=Build Timestamp: {build_timestamp}");
    println!("cargo:rustc-env=RETH_LONG_VERSION_3=Build Features: {cargo_features}");
    println!("cargo:rustc-env=RETH_LONG_VERSION_4=Build Profile: {profile}");

    // The version information for reth formatted for P2P (devp2p).
    // - The latest version from Cargo.toml
    // - The target triple
    //
    // Example: reth/v0.1.0-alpha.1-428a6dc2f/aarch64-apple-darwin
    println!(
        "cargo:rustc-env=RETH_P2P_CLIENT_VERSION={}",
        format_args!("reth/v{pkg_version}-{sha_short}/{target_triple}")
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

/// TN(TN-0025): the build time as RFC 3339 UTC, or `SOURCE_DATE_EPOCH` for reproducible builds.
fn build_timestamp() -> Result<String, Box<dyn Error>> {
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
    let secs = match env::var("SOURCE_DATE_EPOCH") {
        Ok(epoch) => epoch.parse::<i64>()?,
        Err(_) => SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs().try_into()?,
    };
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (Howard Hinnant's `civil_from_days`).
    let z = days + 719_468;
    let (era, doe) = (z.div_euclid(146_097), z.rem_euclid(146_097));
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let (hour, min, sec) = (rem / 3600, rem % 3600 / 60, rem % 60);
    Ok(format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z"))
}

/// TN(TN-0025): the enabled features, as vergen listed them (lowercase, comma-separated).
fn cargo_features() -> String {
    let mut features: Vec<_> = env::vars()
        .filter_map(|(key, _)| key.strip_prefix("CARGO_FEATURE_").map(str::to_lowercase))
        .collect();
    features.sort();
    features.join(",")
}
