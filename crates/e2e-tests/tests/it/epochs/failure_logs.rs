//! Bounded node-log snapshots for epoch-sync failures, captured before teardown.

use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::Path,
};

/// Maximum bytes read from each stdout or stderr log.
const MAX_LOG_TAIL_BYTES: u64 = 8 * 1024;

/// Include both runs of the restarted validator, retaining read errors as diagnostic text.
pub(super) fn capture_failure_logs(log_dir: &Path, validator: &str) -> String {
    [1, 2]
        .into_iter()
        .flat_map(|run| {
            ["log", "stderr.log"]
                .map(move |suffix| log_dir.join(format!("node{validator}-run{run}.{suffix}")))
        })
        .map(|path| {
            let contents = File::open(&path)
                .and_then(read_tail)
                .unwrap_or_else(|error| format!("could not read log: {error}"));
            format!("{} (last at most {MAX_LOG_TAIL_BYTES} bytes):\n{contents}", path.display())
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Seek to a bounded suffix and decode partial UTF-8 without hiding the failure being reported.
fn read_tail(mut input: impl Read + Seek) -> io::Result<String> {
    let length = input.seek(SeekFrom::End(0))?;
    input.seek(SeekFrom::Start(length.saturating_sub(MAX_LOG_TAIL_BYTES)))?;
    let mut bytes = Vec::new();
    input.take(MAX_LOG_TAIL_BYTES).read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Small logs must retain the complete original error and its line breaks.
    #[test]
    fn retains_small_logs() -> io::Result<()> {
        let contents = "execution stopped\noriginal error\n";
        assert_eq!(read_tail(Cursor::new(contents))?, contents);
        Ok(())
    }

    /// Large logs must contribute their newest bytes within the per-file cap.
    #[test]
    fn reads_only_the_bounded_tail() -> io::Result<()> {
        let contents = format!("{}\nlatest execution error\n", "old entry\n".repeat(2048));
        let tail = read_tail(Cursor::new(contents))?;
        assert_eq!(u64::try_from(tail.len()).map_err(io::Error::other)?, MAX_LOG_TAIL_BYTES);
        assert!(tail.ends_with("\nlatest execution error\n"));
        Ok(())
    }

    /// A byte boundary inside a UTF-8 character must not discard the remaining log.
    #[test]
    fn decodes_a_partial_utf8_character() -> io::Result<()> {
        let contents = format!("é{}", "x".repeat(8191));
        let tail = read_tail(Cursor::new(contents))?;
        assert!(tail.starts_with('\u{fffd}'));
        assert!(tail.ends_with('x'));
        Ok(())
    }

    /// Absent logs must still identify both runs and streams without replacing the test error.
    #[test]
    fn reports_missing_log_paths() {
        let absent_dir = Path::new(file!()).join("not-a-directory");
        let report = capture_failure_logs(&absent_dir, "validator-3");
        assert_eq!(report.matches("could not read log:").count(), 4);
        assert!(report.contains("nodevalidator-3-run1.log"));
        assert!(report.contains("nodevalidator-3-run1.stderr.log"));
        assert!(report.contains("nodevalidator-3-run2.log"));
        assert!(report.contains("nodevalidator-3-run2.stderr.log"));
    }
}
