//! Abrupt process termination for crash-recovery tests.

use std::process::{Child, ExitStatus};

/// Send SIGKILL immediately and reap the child without running its shutdown handler.
pub(crate) fn kill_and_reap(child: &mut Child) -> std::io::Result<ExitStatus> {
    child.kill().and_then(|()| child.wait())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{os::unix::process::ExitStatusExt, process::Command};

    /// The crash primitive must report SIGKILL rather than a graceful exit or SIGTERM.
    #[test]
    fn abrupt_kill_reports_sigkill() -> std::io::Result<()> {
        let mut child = Command::new("/bin/sleep").arg("60").spawn()?;
        let status = kill_and_reap(&mut child)?;
        assert_eq!(status.signal(), Some(9));
        Ok(())
    }
}
