//! BLS key passphrase acquisition, shared by the stock `telcoin-network` binary
//! and external binaries that embed the TN CLI (e.g. ExEx host binaries).

use crate::cli::{Cli, Commands, PassSource};
use eyre::{bail, eyre};
use std::{ffi::OsString, fmt, fs::File, io::Read as _, path::Path};
use zeroize::Zeroizing;

/// Environment variable read (and cleared) at process start for the BLS key passphrase.
pub const BLS_PASSPHRASE_ENVVAR: &str = "TN_BLS_PASSPHRASE";

/// Environment variable systemd sets to the directory holding a unit's credentials
/// (`LoadCredential=`, `LoadCredentialEncrypted=`).
pub const CREDENTIALS_DIRECTORY_ENVVAR: &str = "CREDENTIALS_DIRECTORY";

/// Default credential name read by `--bls-passphrase-source credential`.
pub const DEFAULT_BLS_PASSPHRASE_CREDENTIAL: &str = "bls-passphrase";

/// Read the bls key passphrase from then incoming environment if set.
/// This also will remove the key once read to avoid leaks in future.
/// This is meant to be called once at the very beginning of program
/// start before any threads exists.  It will only return the passphrase
/// on the first call (it clears the env if it is set).
///
/// On Linux the original bytes of the value are overwritten with zeros before the variable is
/// removed, so `/proc/<pid>/environ` no longer shows the passphrase (the variable name stays
/// visible there, followed by zero bytes). Elsewhere the variable is only removed.
///
/// Call this exactly once, as the FIRST statement of `main`, before any
/// threads exist: it mutates the process environment, which is not thread safe.
pub fn get_bls_passphrase_from_env() -> Option<Zeroizing<String>> {
    if let Ok(passphrase) = std::env::var(BLS_PASSPHRASE_ENVVAR) {
        let passphrase = Zeroizing::new(passphrase);
        if !passphrase.is_empty() {
            // `passphrase` is now the only copy the node keeps, so wipe the original
            #[cfg(target_os = "linux")]
            scrub_env_value(BLS_PASSPHRASE_ENVVAR);
            // Clear then remove the passphrase from the env.
            // Note on safety, these calls are unsafe because they are not thread safe and we
            // only call this function once at the beginning of startup so no threads should
            // exist yet.
            unsafe {
                std::env::set_var(BLS_PASSPHRASE_ENVVAR, "");
                std::env::remove_var(BLS_PASSPHRASE_ENVVAR);
            }
            Some(passphrase)
        } else {
            None
        }
    } else {
        None
    }
}

/// Overwrite the value of the environment variable `name` with zero bytes, in place.
///
/// Removing a variable only unlinks it from libc's `environ` array. A value inherited at exec
/// stays in the environment block the kernel set up, which `/proc/<pid>/environ` exposes to
/// root and to the service user for the life of the process.
#[cfg(target_os = "linux")]
fn scrub_env_value(name: &str) {
    let name = std::ffi::CString::new(name).expect("environment variable names contain no nul");
    // SAFETY: called once at startup, before any other thread exists, so nothing reads or
    // changes the environment concurrently. linux libcs return from `getenv` either null or a
    // pointer to the nul-terminated value inside the live `NAME=value` entry; that memory is
    // writable (the initial process stack for an inherited variable, the heap for one set with
    // `setenv`) and stays valid until the variable is changed or removed, which only happens
    // after this call. `strlen` excludes the terminator, so the write stays within the value
    // and leaves the entry a valid, empty c string.
    unsafe {
        let value = libc::getenv(name.as_ptr());
        if !value.is_null() {
            std::ptr::write_bytes(value, 0, libc::strlen(value));
        }
    }
}

/// Prompt (twice) for a new passphrase when generating keys.
fn read_passphrase() -> Option<Zeroizing<String>> {
    while let Ok(pw) = rpassword::prompt_password("Enter a passphrase to ecrypt BLS key: ") {
        let pw = Zeroizing::new(pw);
        if let Ok(pw2) = rpassword::prompt_password("Re-enter BLS key passphrase to confirm: ") {
            let pw2 = Zeroizing::new(pw2);
            if pw == pw2 {
                return if pw.is_empty() {
                    println!("No passphrase set for BLS key, this is not recommended.");
                    None
                } else {
                    Some(pw)
                };
            }
        }
        println!("Passphrases do not match, retry.");
    }
    None
}

/// Read the BLS passphrase from the systemd credential `name` in `credentials_dir`.
///
/// Removes exactly one trailing `\n`, and a `\r` directly before it, the line ending an
/// editor or `echo` leaves behind. Nothing else is trimmed: unlike the stdin source, a
/// passphrase may end in whitespace on purpose. An unset or empty `credentials_dir`, a
/// missing or unreadable file, and an empty passphrase are all errors.
fn read_credential(
    credentials_dir: Option<OsString>,
    name: &str,
) -> eyre::Result<Zeroizing<String>> {
    let Some(dir) = credentials_dir.filter(|dir| !dir.is_empty()) else {
        bail!(
            "{CREDENTIALS_DIRECTORY_ENVVAR} is not set: not running under a systemd credential \
             directory (LoadCredential= or LoadCredentialEncrypted=)"
        );
    };
    let path = Path::new(&dir).join(name);
    let mut passphrase = Zeroizing::new(String::new());
    File::open(&path).and_then(|mut file| file.read_to_string(&mut passphrase)).map_err(|err| {
        eyre!("could not read BLS passphrase credential {}: {err}", path.display())
    })?;
    // pop shortens in place, so no uncleared copy of the passphrase is made
    if passphrase.ends_with('\n') {
        passphrase.pop();
        if passphrase.ends_with('\r') {
            passphrase.pop();
        }
    }
    if passphrase.is_empty() {
        bail!("BLS passphrase credential {} is empty", path.display());
    }
    Ok(passphrase)
}

/// Clap value parser for `--bls-passphrase-credential`.
///
/// Accepts only names systemd itself accepts for a credential: printable ASCII, at most 255
/// bytes, no `/` or `:`, and not `.` or `..`. This also keeps the joined path inside
/// `$CREDENTIALS_DIRECTORY`.
pub(crate) fn parse_credential_name(name: &str) -> Result<String, String> {
    let valid = !name.is_empty()
        && name.len() <= 255
        && name != "."
        && name != ".."
        && name.bytes().all(|b| (b' '..=b'~').contains(&b) && b != b'/' && b != b':');
    if valid {
        Ok(name.to_owned())
    } else {
        Err(format!("{name:?} is not a valid systemd credential name"))
    }
}

impl<Ext: clap::Args + fmt::Debug> Cli<Ext> {
    /// Resolve the BLS key passphrase for this invocation.
    ///
    /// `preloaded` is the value captured by [`get_bls_passphrase_from_env`] at
    /// process start. Applies the `--bls-passphrase-source` policy for the
    /// parsed subcommand and errors when a required passphrase is absent.
    pub fn resolve_bls_passphrase(
        &self,
        preloaded: Option<Zeroizing<String>>,
    ) -> eyre::Result<Option<Zeroizing<String>>> {
        self.resolve_bls_passphrase_with_reader(preloaded, std::io::stdin().lock())
    }

    /// Same as [`Self::resolve_bls_passphrase`], but reads stdin from an injectable
    /// reader so tests can supply synthetic input without touching real process stdin.
    fn resolve_bls_passphrase_with_reader<R: std::io::BufRead>(
        &self,
        preloaded: Option<Zeroizing<String>>,
        mut reader: R,
    ) -> eyre::Result<Option<Zeroizing<String>>> {
        let mut passphrase = preloaded;
        match self.bls_passphrase_source {
            PassSource::Env => {} // Already have the env var if provided.
            PassSource::Stdin => {
                let mut buffer = Zeroizing::new(String::new());
                if let Err(err) = reader.read_line(&mut buffer) {
                    bail!("Error reading BLS passphrase from stdin: {err:?}");
                }
                // trim in place: a trimmed copy would be a second, uncleared passphrase buffer
                let len = buffer.trim_end().len();
                buffer.truncate(len);
                passphrase = Some(buffer).filter(|s| !s.is_empty());
            }
            PassSource::Ask => match &self.command {
                Commands::Keytool(keytool) if keytool.needs_passphrase() => {
                    // Need to ask and confirm before it used to encrypt.
                    passphrase = read_passphrase();
                }
                // keytool subcommands that only edit public config (e.g. `set-rpc`)
                // never touch the BLS key, so don't prompt for a passphrase.
                Commands::Keytool(_) => {}
                Commands::Db(_) => {} // DB diagnostics are read-only and do not require keys.
                Commands::Genesis(_) => {} // Don't need the passphrase..
                Commands::Node(_) => {
                    // Simple ask once and app will error out later if this is wrong.
                    passphrase =
                        rpassword::prompt_password("Enter the BLS key passphrase to decrypt: ")
                            .ok()
                            .map(Zeroizing::new);
                }
            },
            PassSource::NoPassphrase => {
                passphrase = None;
            }
            PassSource::Credential => {
                passphrase = Some(read_credential(
                    std::env::var_os(CREDENTIALS_DIRECTORY_ENVVAR),
                    &self.bls_passphrase_credential,
                )?);
            }
        }
        // The `db` subcommand is a read-only inspection tool and `keytool set-rpc`
        // only edits public config; neither needs the BLS key passphrase.
        let command_needs_passphrase = match &self.command {
            Commands::Db(_) => false,
            Commands::Keytool(keytool) => keytool.needs_passphrase(),
            Commands::Genesis(_) | Commands::Node(_) => true,
        };
        let needs_passphrase =
            self.bls_passphrase_source.with_passphrase() && command_needs_passphrase;
        if passphrase.is_none() && needs_passphrase {
            bail!(
                "Error passphrase is required, see the option --bls-passphrase-source for options"
            );
        }
        Ok(passphrase)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use std::sync::{Mutex, MutexGuard, PoisonError};

    /// Serializes the tests in this module that read or write the process environment, which
    /// is process-global while `cargo test` runs tests on concurrent threads.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn env_lock() -> MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn cli(args: &[&str]) -> Cli {
        Cli::try_parse_args_from(args).expect("cli parses")
    }

    /// Write `contents` as the `bls-passphrase` credential in a fresh credentials directory and
    /// read it back through [`read_credential`].
    fn read_credential_file(contents: &[u8]) -> eyre::Result<Zeroizing<String>> {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("bls-passphrase"), contents).expect("write credential");
        read_credential(Some(dir.path().into()), "bls-passphrase")
    }

    #[test]
    fn test_no_passphrase_source_resolves_none() {
        let cli = cli(&["tn", "node", "--bls-passphrase-source", "no-passphrase"]);
        assert_eq!(
            cli.resolve_bls_passphrase(Some(Zeroizing::new("preload".into()))).unwrap(),
            None
        );
    }

    #[test]
    fn test_env_source_keeps_preloaded_value() {
        let cli = cli(&["tn", "node"]); // default source is env
        assert_eq!(
            cli.resolve_bls_passphrase(Some(Zeroizing::new("preload".into()))).unwrap(),
            Some(Zeroizing::new("preload".into()))
        );
    }

    #[test]
    fn test_missing_required_passphrase_errors_with_exact_message() {
        let cli = cli(&["tn", "node"]);
        let err = cli.resolve_bls_passphrase(None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error passphrase is required, see the option --bls-passphrase-source for options"
        );
    }

    #[test]
    fn test_db_subcommand_is_exempt() {
        let cli = cli(&["tn", "node", "--bls-passphrase-source", "no-passphrase"]);
        assert_eq!(cli.resolve_bls_passphrase(None).unwrap(), None);
    }

    #[test]
    fn test_stdin_empty_line_errors_when_passphrase_required() {
        let cli = cli(&["tn", "node", "--bls-passphrase-source", "stdin"]);
        let err = cli
            .resolve_bls_passphrase_with_reader(None, std::io::Cursor::new(b"\n".as_ref()))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Error passphrase is required, see the option --bls-passphrase-source for options"
        );
    }

    #[test]
    fn test_stdin_reads_provided_passphrase() {
        let cli = cli(&["tn", "node", "--bls-passphrase-source", "stdin"]);
        assert_eq!(
            cli.resolve_bls_passphrase_with_reader(
                None,
                std::io::Cursor::new(b"sekrit\n".as_ref())
            )
            .unwrap(),
            Some(Zeroizing::new("sekrit".into()))
        );
    }

    #[test]
    fn test_env_preload_reads_and_clears_var() {
        let _env = env_lock();
        std::env::set_var(BLS_PASSPHRASE_ENVVAR, "sekrit");
        assert_eq!(get_bls_passphrase_from_env(), Some(Zeroizing::new("sekrit".into())));
        assert!(std::env::var(BLS_PASSPHRASE_ENVVAR).is_err());
        assert_eq!(get_bls_passphrase_from_env(), None);
    }

    /// The original environment bytes are zeroed, not only unlinked from `environ`. Reading
    /// them back after removal relies on glibc never freeing the strings `setenv` allocates,
    /// hence the `gnu` gate.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn test_env_preload_scrubs_original_bytes() {
        let _env = env_lock();
        std::env::set_var(BLS_PASSPHRASE_ENVVAR, "sekrit");
        let name = std::ffi::CString::new(BLS_PASSPHRASE_ENVVAR).expect("name");
        // SAFETY: the env lock is held and the variable was set just above, so getenv returns
        // a live, nul-terminated value.
        let (value, len) = unsafe {
            let value = libc::getenv(name.as_ptr());
            assert!(!value.is_null());
            (value, libc::strlen(value))
        };
        assert_eq!(len, "sekrit".len());

        assert_eq!(get_bls_passphrase_from_env(), Some(Zeroizing::new("sekrit".into())));
        assert!(std::env::var(BLS_PASSPHRASE_ENVVAR).is_err());
        // SAFETY: glibc keeps every string setenv allocated for the life of the process, so the
        // removed entry's `len` value bytes are still readable.
        let scrubbed = unsafe { std::slice::from_raw_parts(value.cast::<u8>(), len) };
        assert!(scrubbed.iter().all(|byte| *byte == 0), "value bytes left in place: {scrubbed:?}");
    }

    #[test]
    fn test_credential_strips_one_trailing_newline_only() {
        assert_eq!(read_credential_file(b"sekrit\n").unwrap().as_str(), "sekrit");
        assert_eq!(read_credential_file(b"sekrit").unwrap().as_str(), "sekrit");
        assert_eq!(read_credential_file(b"sekrit\n\n").unwrap().as_str(), "sekrit\n");
    }

    #[test]
    fn test_credential_keeps_trailing_whitespace() {
        assert_eq!(read_credential_file(b"sekrit \n").unwrap().as_str(), "sekrit ");
        assert_eq!(read_credential_file(b"sekrit\t").unwrap().as_str(), "sekrit\t");
        assert_eq!(read_credential_file(b" sekrit").unwrap().as_str(), " sekrit");
    }

    #[test]
    fn test_credential_strips_crlf() {
        assert_eq!(read_credential_file(b"sekrit\r\n").unwrap().as_str(), "sekrit");
        // a lone carriage return is not a line ending and stays part of the passphrase
        assert_eq!(read_credential_file(b"sekrit\r").unwrap().as_str(), "sekrit\r");
    }

    #[test]
    fn test_credential_empty_file_errors() {
        for contents in [b"".as_ref(), b"\n", b"\r\n"] {
            let err = read_credential_file(contents).unwrap_err();
            assert!(err.to_string().contains("is empty"), "{err}");
        }
    }

    #[test]
    fn test_credential_missing_file_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = read_credential(Some(dir.path().into()), "bls-passphrase").unwrap_err();
        assert!(err.to_string().contains("could not read BLS passphrase credential"), "{err}");
    }

    #[test]
    fn test_credential_unset_directory_errors() {
        for dir in [None, Some(OsString::new())] {
            let err = read_credential(dir, "bls-passphrase").unwrap_err();
            assert!(
                err.to_string().contains("not running under a systemd credential directory"),
                "{err}"
            );
        }
    }

    /// The credential source reads `$CREDENTIALS_DIRECTORY/<NAME>`, takes the name from
    /// `TN_BLS_PASSPHRASE_CREDENTIAL`, and replaces any value preloaded from the env source.
    #[test]
    fn test_credential_source_reads_credentials_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("node-pass"), b"from credential\n").expect("write");
        let _env = env_lock();
        std::env::set_var(CREDENTIALS_DIRECTORY_ENVVAR, dir.path());
        std::env::set_var("TN_BLS_PASSPHRASE_CREDENTIAL", "node-pass");
        let cli = cli(&["tn", "node", "--bls-passphrase-source", "credential"]);
        let resolved = cli.resolve_bls_passphrase_with_reader(
            Some(Zeroizing::new("from env".into())),
            std::io::Cursor::new(b"from stdin\n".as_ref()),
        );
        std::env::remove_var("TN_BLS_PASSPHRASE_CREDENTIAL");
        std::env::remove_var(CREDENTIALS_DIRECTORY_ENVVAR);
        assert_eq!(resolved.unwrap(), Some(Zeroizing::new("from credential".into())));
        assert_eq!(cli.bls_passphrase_credential, "node-pass");
    }

    #[test]
    fn test_credential_source_without_credentials_directory_errors() {
        let _env = env_lock();
        std::env::remove_var(CREDENTIALS_DIRECTORY_ENVVAR);
        let cli = cli(&["tn", "node", "--bls-passphrase-source", "credential"]);
        let err = cli.resolve_bls_passphrase(None).unwrap_err();
        assert!(err.to_string().contains("not running under a systemd credential directory"));
    }

    #[test]
    fn test_credential_name_defaults_and_rejects_paths() {
        let _env = env_lock();
        assert_eq!(cli(&["tn", "node"]).bls_passphrase_credential, "bls-passphrase");
        assert_eq!(
            cli(&["tn", "--bls-passphrase-credential", "tn.pass", "node"])
                .bls_passphrase_credential,
            "tn.pass"
        );
        for name in ["", ".", "..", "../bls-passphrase", "/etc/shadow", "a/b", "a:b", "a\tb"] {
            let err = Cli::<crate::NoArgs>::try_parse_args_from([
                "tn",
                "node",
                "--bls-passphrase-credential",
                name,
            ])
            .unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation, "{name:?}");
        }
    }
}
