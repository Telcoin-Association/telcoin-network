//! Reach classification and startup reporting for operator listeners (health, metrics).
//!
//! Issue #1689: every operator listener reports the address it actually bound and how far
//! that address reaches, so a deployment can confirm its private binding contract from the
//! startup log alone. A wildcard bind is reported at `warn` because it opens the listener on
//! every interface of the host, public ones included.

use std::{fmt, net::SocketAddr};

use tracing::{info, warn};

/// How far a bound listener address reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerExposure {
    /// A loopback address: reachable only from processes on this host (or in this network
    /// namespace).
    Loopback,
    /// One specific non-loopback interface address: reachable from every host that can route to
    /// that interface. This is an explicit operator choice, for example a container network
    /// address.
    Interface,
    /// An unspecified (wildcard) address: every interface of the host, public ones included.
    AllInterfaces,
}

impl ListenerExposure {
    /// Classify a bound address by its IP.
    pub fn of(addr: SocketAddr) -> Self {
        let ip = addr.ip();
        match () {
            () if ip.is_loopback() => Self::Loopback,
            () if ip.is_unspecified() => Self::AllInterfaces,
            () => Self::Interface,
        }
    }

    /// The label used in the startup log line.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Loopback => "loopback",
            Self::Interface => "interface",
            Self::AllInterfaces => "all-interfaces",
        }
    }
}

impl fmt::Display for ListenerExposure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Report the address a listener actually bound and how far it reaches.
///
/// Logs one `operator listener bound` line at `info` for every listener and adds a `warn` when
/// the bind is a wildcard. Returns the classification so callers can branch on it.
pub fn report_listener_bound(listener: &'static str, listen_on: SocketAddr) -> ListenerExposure {
    let exposure = ListenerExposure::of(listen_on);
    info!(target: "tn::listener", listener, %listen_on, exposure = %exposure, "operator listener bound");
    if exposure == ListenerExposure::AllInterfaces {
        warn!(
            target: "tn::listener",
            listener,
            %listen_on,
            "listener bound on every interface; every host that can reach this machine can read it. Bind a loopback or private interface address unless the wide bind is intended"
        );
    }
    exposure
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::{report_listener_bound, ListenerExposure};

    /// Parse a literal socket address inside a test.
    fn addr(raw: &str) -> eyre::Result<SocketAddr> {
        raw.parse::<SocketAddr>().map_err(Into::into)
    }

    /// IPv4 and IPv6 loopback both classify as loopback.
    #[test]
    fn loopback_addresses_classify_as_loopback() -> eyre::Result<()> {
        assert_eq!(ListenerExposure::of(addr("127.0.0.1:1")?), ListenerExposure::Loopback);
        assert_eq!(ListenerExposure::of(addr("[::1]:1")?), ListenerExposure::Loopback);
        Ok(())
    }

    /// IPv4 and IPv6 wildcards both classify as all-interfaces.
    #[test]
    fn unspecified_addresses_classify_as_all_interfaces() -> eyre::Result<()> {
        assert_eq!(ListenerExposure::of(addr("0.0.0.0:1")?), ListenerExposure::AllInterfaces);
        assert_eq!(ListenerExposure::of(addr("[::]:1")?), ListenerExposure::AllInterfaces);
        Ok(())
    }

    /// A specific non-loopback address classifies as one interface.
    #[test]
    fn specific_addresses_classify_as_interface() -> eyre::Result<()> {
        assert_eq!(ListenerExposure::of(addr("10.10.0.21:9101")?), ListenerExposure::Interface);
        assert_eq!(ListenerExposure::of(addr("[fd00::1]:1")?), ListenerExposure::Interface);
        Ok(())
    }

    /// The log labels are part of the operator contract and must not drift.
    #[test]
    fn labels_are_stable() {
        assert_eq!(ListenerExposure::Loopback.as_str(), "loopback");
        assert_eq!(ListenerExposure::Interface.as_str(), "interface");
        assert_eq!(ListenerExposure::AllInterfaces.as_str(), "all-interfaces");
        assert_eq!(ListenerExposure::AllInterfaces.to_string(), "all-interfaces");
    }

    /// The report returns the same classification it logged.
    #[test]
    fn report_returns_the_classification() -> eyre::Result<()> {
        assert_eq!(report_listener_bound("test", addr("127.0.0.1:1")?), ListenerExposure::Loopback);
        assert_eq!(
            report_listener_bound("test", addr("0.0.0.0:1")?),
            ListenerExposure::AllInterfaces
        );
        Ok(())
    }
}
