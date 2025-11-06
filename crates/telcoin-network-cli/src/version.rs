//! Version information for telcoin network.

/// The short version information for telcoin network.
///
/// - The latest version from Cargo.toml
/// - The short SHA of the latest commit.
///
/// # Example
///
/// ```text
/// 0.1.0 (defa64b2)
/// ```
pub(crate) const SHORT_VERSION: &str = {
    const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
    const GIT_SHA: &str = env!("VERGEN_GIT_SHA");
    const_str::concat!(PKG_VERSION, " (", GIT_SHA, ")")
};

/// The long version information for telcoin network.
///
/// - The latest version from Cargo.toml
/// - The long SHA of the latest commit.
/// - The build datetime
/// - The build features
///
/// # Example:
///
/// ```text
/// Version: 0.1.0
/// Commit SHA: defa64b2
/// Build Timestamp: 2023-05-19T01:47:19.815651705Z
/// ```
pub(crate) const LONG_VERSION: &str = {
    const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
    const GIT_SHA: &str = env!("VERGEN_GIT_SHA");
    const BUILD_TIMESTAMP: &str = env!("VERGEN_BUILD_TIMESTAMP");
    const CARGO_FEATURES: &str = env!("VERGEN_CARGO_FEATURES");
    const PROFILE: &str = build_profile_name();

    const_str::concat!(
        "Version: ",
        PKG_VERSION,
        "\n",
        "Commit SHA: ",
        GIT_SHA,
        "\n",
        "Build Timestamp: ",
        BUILD_TIMESTAMP,
        "\n",
        "Build Features: ",
        CARGO_FEATURES,
        "\n",
        "Build Profile: ",
        PROFILE
    )
};

/// The default extradata used for payload building.
///
/// - The latest version from Cargo.toml
/// - The OS identifier
///
/// # Example
///
/// ```text
/// telcoin-network/v{major}.{minor}.{patch}/{OS}
/// ```
pub fn default_extradata() -> String {
    format!("telcoin-network/v{}/{}", env!("CARGO_PKG_VERSION"), std::env::consts::OS)
}

const fn build_profile_name() -> &'static str {
    const OUT_DIR: &str = env!("OUT_DIR");
    const SEP: char = if const_str::contains!(OUT_DIR, "/") { '/' } else { '\\' };
    let parts = const_str::split!(OUT_DIR, SEP);
    parts[parts.len() - 4]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_extradata_less_32bytes() {
        let extradata = default_extradata();
        assert!(extradata.len() <= 32, "extradata must be less than 32 bytes: {extradata}")
    }
}
