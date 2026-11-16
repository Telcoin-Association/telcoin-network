//! Abrupt process termination for crash-recovery tests.

use std::process::{Child, ExitStatus};

/// Own test nodes and stop them abruptly on every exit path, preserving crash-state data.
pub(crate) struct CrashGuard {
    /// Occupied slots own live children; the intentionally crashed victim leaves an empty slot.
    children: Vec<Option<Child>>,
}

impl CrashGuard {
    /// Register the children whose shutdown handlers must never rewrite captured evidence.
    pub(crate) fn new(children: Vec<Child>) -> Self {
        Self { children: children.into_iter().map(Some).collect() }
    }

    /// Remove a child while it is intentionally down across an epoch boundary.
    pub(crate) fn take(&mut self, index: usize) -> Option<Child> {
        self.children.get_mut(index).and_then(Option::take)
    }

    /// Register a restarted child in its vacant slot.
    pub(crate) fn replace(&mut self, index: usize, child: Child) -> std::io::Result<()> {
        let slot = self.children.get_mut(index).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing crash-guard slot")
        })?;
        slot.as_mut().map(kill_and_reap).transpose()?;
        *slot = Some(child);
        Ok(())
    }
}

impl Drop for CrashGuard {
    /// SIGKILL and reap every remaining node before its data directory is archived.
    fn drop(&mut self) {
        self.children.iter_mut().flatten().for_each(|child| {
            kill_and_reap(child).map(|_| ()).unwrap_or_else(|error| {
                eprintln!("failed to stop crash-capture child {}: {error}", child.id());
            });
        });
    }
}

/// Send SIGKILL immediately and reap the child without running its shutdown handler.
pub(crate) fn kill_and_reap(child: &mut Child) -> std::io::Result<ExitStatus> {
    child.kill().and_then(|()| child.wait())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{BufRead, BufReader, Read},
        os::unix::process::ExitStatusExt,
        process::{Command, Stdio},
    };

    /// The crash primitive must report SIGKILL rather than a graceful exit or SIGTERM.
    #[test]
    fn abrupt_kill_reports_sigkill() -> std::io::Result<()> {
        let mut child = Command::new("/bin/sleep").arg("60").spawn()?;
        let status = kill_and_reap(&mut child)?;
        assert_eq!(status.signal(), Some(9));
        Ok(())
    }

    /// The victim slot is empty during downtime and owns the replacement after restart.
    #[test]
    fn victim_slot_tracks_restart_ownership() -> std::io::Result<()> {
        let child = Command::new("/bin/sleep").arg("60").spawn()?;
        let mut guard = CrashGuard::new(vec![child]);
        let mut victim = guard.take(0).ok_or_else(|| std::io::Error::other("missing victim"))?;
        assert_eq!(kill_and_reap(&mut victim)?.signal(), Some(9));
        assert!(guard.take(0).is_none());
        guard.replace(0, Command::new("/bin/sleep").arg("60").spawn()?)?;
        let mut restarted =
            guard.take(0).ok_or_else(|| std::io::Error::other("missing restart"))?;
        assert_eq!(kill_and_reap(&mut restarted)?.signal(), Some(9));
        Ok(())
    }

    /// A ready child with a SIGTERM handler must be reaped without running that handler.
    #[test]
    fn guard_drop_preserves_state_without_sigterm() -> std::io::Result<()> {
        let mut child = Command::new("/bin/sh")
            .args(["-c", "trap 'printf graceful; exit 0' TERM; printf 'ready\\n'; read -r pending"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let stdout = child.stdout.take().ok_or_else(|| std::io::Error::other("missing stdout"))?;
        let guard = CrashGuard::new(vec![child]);
        let mut output = BufReader::new(stdout);
        let mut ready = String::new();
        output.read_line(&mut ready)?;
        assert_eq!(ready, "ready\n");
        drop(guard);
        let mut shutdown_output = String::new();
        output.read_to_string(&mut shutdown_output)?;
        assert!(shutdown_output.is_empty(), "SIGTERM rewrote state: {shutdown_output}");
        Ok(())
    }
}
