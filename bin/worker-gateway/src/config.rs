//! Gateway configuration: the upstream worker list, loaded from YAML.

use std::{fmt, path::Path};

use eyre::WrapErr as _;
use serde::{Deserialize, Serialize};
use tracing::info;
use url::Url;

use crate::proxy::UpstreamOrigin;

/// One upstream worker the gateway can forward to.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct UpstreamWorker {
    /// The worker's id, matching the `worker_id` reported by the node's
    /// `GET /health/workers` readiness endpoint.
    pub(crate) worker_id: u16,
    /// Base URL of the worker's JSON-RPC HTTP endpoint
    /// (e.g. `http://127.0.0.1:8545`).
    pub(crate) rpc_url: Url,
    /// URL of the node readiness endpoint reporting this worker
    /// (e.g. `http://127.0.0.1:8551/health/workers`).
    pub(crate) readiness_url: Url,
}

/// Prints the URLs by origin only (see [`UpstreamOrigin`]): a worker URL
/// can carry a credential in its userinfo, path or query.
impl fmt::Debug for UpstreamWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstreamWorker")
            .field("worker_id", &self.worker_id)
            .field("rpc_url", &format_args!("{}", UpstreamOrigin(&self.rpc_url)))
            .field("readiness_url", &format_args!("{}", UpstreamOrigin(&self.readiness_url)))
            .finish()
    }
}

/// The set of upstream workers the gateway fans out to.
///
/// Loaded from a YAML file by [`GatewayConfig::load`]. v1 forwards every request to
/// the first ready worker in the list; the list is ordered by preference so
/// the method-aware routing follow-up can layer selection on top without a
/// config change. Its `Debug` prints each upstream through [`UpstreamWorker`]'s,
/// by origin.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct GatewayConfig {
    /// Upstream workers, in preference order.
    pub(crate) upstreams: Vec<UpstreamWorker>,
}

impl GatewayConfig {
    /// Load the upstream list from a YAML file at `path`.
    ///
    /// The file must exist; a missing file is an error (unlike a defaulted
    /// config, an empty upstream list would leave the gateway unable to serve).
    /// Malformed YAML is reported by path and position only: the parser's own
    /// message can quote a value from the file, such as an upstream URL.
    pub(crate) fn load(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let path = path.as_ref();
        info!(target: "gateway", path = %path.display(), "loading upstream configuration");
        let yaml = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("failed to read {}", path.display()))?;
        serde_yaml::from_str(&yaml).map_err(|err| match err.location() {
            Some(at) => eyre::eyre!(
                "bad yaml data in {} at line {} column {}",
                path.display(),
                at.line(),
                at.column()
            ),
            None => eyre::eyre!("bad yaml data in {}", path.display()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn loads_upstreams_from_yaml() -> eyre::Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        writeln!(
            file,
            "upstreams:\n  - worker_id: 0\n    rpc_url: \"http://127.0.0.1:8545\"\n    readiness_url: \"http://127.0.0.1:8551/health/workers\""
        )?;

        let config = GatewayConfig::load(file.path())?;

        assert_eq!(config.upstreams.len(), 1);
        let upstream = config.upstreams.first().ok_or_else(|| eyre::eyre!("missing upstream"))?;
        assert_eq!(upstream.worker_id, 0);
        // `Url` normalizes the base to include a trailing slash.
        assert_eq!(upstream.rpc_url.as_str(), "http://127.0.0.1:8545/");
        assert_eq!(upstream.readiness_url.as_str(), "http://127.0.0.1:8551/health/workers");
        Ok(())
    }

    #[test]
    fn missing_file_is_an_error() {
        let result = GatewayConfig::load("/nonexistent/worker-gateway.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn errors_name_the_path_but_never_quote_the_file() -> eyre::Result<()> {
        let missing = GatewayConfig::load("/nonexistent/worker-gateway.yaml")
            .expect_err("a missing file must fail");
        assert!(format!("{missing:?}").contains("/nonexistent/worker-gateway.yaml"), "{missing:?}");

        let mut file = tempfile::NamedTempFile::new()?;
        writeln!(
            file,
            "upstreams:\n  - worker_id: 0\n    rpc_url: \"not-a-url-SECRET\"\n    readiness_url: \"http://127.0.0.1:8551/health/workers\""
        )?;
        let malformed = GatewayConfig::load(file.path()).expect_err("an invalid url must fail");
        let report = format!("{malformed:?}");
        assert!(report.contains("bad yaml data in"), "{report}");
        assert!(report.contains(&file.path().display().to_string()), "{report}");
        assert!(report.contains("at line 3"), "{report}");
        assert!(!report.contains("SECRET"), "the error quotes the file: {report}");
        Ok(())
    }

    #[test]
    fn upstream_worker_debug_prints_origins_only() -> eyre::Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        writeln!(
            file,
            "upstreams:\n  - worker_id: 0\n    rpc_url: \"http://gw:s3cret@10.0.0.7:8545/key/abc?token=xyz\"\n    readiness_url: \"http://10.0.0.7:8551/health/workers?token=xyz\""
        )?;
        let config = GatewayConfig::load(file.path())?;
        let debug = format!("{config:?}");
        for shown in ["http://10.0.0.7:8545", "http://10.0.0.7:8551"] {
            assert!(debug.contains(shown), "`{shown}` missing from: {debug}");
        }
        for secret in ["s3cret", "key/abc", "token", "health/workers"] {
            assert!(!debug.contains(secret), "`{secret}` leaked into: {debug}");
        }
        Ok(())
    }
}
