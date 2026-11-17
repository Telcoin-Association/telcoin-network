//! The `--version` text.

/// The long version clap prints for `--version`, after the crate name:
///
/// ```text
/// tn-worker-gateway Version: 0.18.0
/// Commit SHA: 0123456789abcdef0123456789abcdef01234567
/// ```
///
/// The release tooling (`etc/release.sh`) checks this exact format, so it
/// stays these two lines with no build details. The Dockerfile passes the
/// commit as `VERGEN_GIT_SHA`; a build without it reports `unknown`. `-V`
/// prints the package version alone.
pub(crate) const LONG_VERSION: &str = {
    const GIT_SHA: &str = match option_env!("VERGEN_GIT_SHA") {
        Some(sha) => sha,
        None => "unknown",
    };
    const_str::concat!("Version: ", env!("CARGO_PKG_VERSION"), "\nCommit SHA: ", GIT_SHA)
};

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use crate::cli::Cli;

    /// `--version` renders the crate name, the package version and the commit
    /// in the two lines the release tooling checks, whether or not the build
    /// set `VERGEN_GIT_SHA`.
    #[test]
    fn long_version_prints_crate_version_and_commit() {
        let name = env!("CARGO_PKG_NAME");
        let version = env!("CARGO_PKG_VERSION");
        let sha = option_env!("VERGEN_GIT_SHA").unwrap_or("unknown");

        let rendered = Cli::command().render_long_version();
        let expected = format!("{name} Version: {version}\nCommit SHA: {sha}\n");
        assert_eq!(rendered, expected);
        assert!(!rendered.contains("Build Features"), "{rendered}");
    }
}
