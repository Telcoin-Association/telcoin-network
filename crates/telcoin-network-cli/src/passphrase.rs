//! BLS key passphrase acquisition, shared by the stock `telcoin-network` binary
//! and external binaries that embed the TN CLI (e.g. ExEx host binaries).

use crate::cli::{Cli, Commands, PassSource};
use eyre::bail;
use std::fmt;

/// Environment variable read (and cleared) at process start for the BLS key passphrase.
pub const BLS_PASSPHRASE_ENVVAR: &str = "TN_BLS_PASSPHRASE";

/// Read the bls key passphrase from then incoming environment if set.
/// This also will remove the key once read to avoid leaks in future.
/// This is meant to be called once at the very beginning of program
/// start before any threads exists.  It will only return the passphrase
/// on the first call (it clears the env if it is set).
///
/// Call this exactly once, as the FIRST statement of `main`, before any
/// threads exist: it mutates the process environment, which is not thread safe.
pub fn get_bls_passphrase_from_env() -> Option<String> {
    if let Ok(passphrase) = std::env::var(BLS_PASSPHRASE_ENVVAR) {
        if !passphrase.is_empty() {
            // Clear then remove the passphrase from the env.
            // NOTE: This is probably not doing much but is an attempt to make the var "more
            // deleted". This will depend on the underlying platform/libc but should
            // worst case does nothing. Note on safety, these need calls need to happen
            // to avoid any leaks of the passphrase if set and they are unsafe.  They
            // are unsafe because they are not thread safe and we only call this
            // function once at the beginning of startup so no threads should exist yet.
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

/// Prompt (twice) for a new passphrase when generating keys.
fn read_passphrase() -> Option<String> {
    while let Ok(pw) = rpassword::prompt_password("Enter a passphrase to ecrypt BLS key: ") {
        if let Ok(pw2) = rpassword::prompt_password("Re-enter BLS key passphrase to confirm: ") {
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

impl<Ext: clap::Args + fmt::Debug> Cli<Ext> {
    /// Resolve the BLS key passphrase for this invocation.
    ///
    /// `preloaded` is the value captured by [`get_bls_passphrase_from_env`] at
    /// process start. Applies the `--bls-passphrase-source` policy for the
    /// parsed subcommand and errors when a required passphrase is absent.
    pub fn resolve_bls_passphrase(
        &self,
        preloaded: Option<String>,
    ) -> eyre::Result<Option<String>> {
        self.resolve_bls_passphrase_with_reader(preloaded, std::io::stdin().lock())
    }

    /// Same as [`Self::resolve_bls_passphrase`], but reads stdin from an injectable
    /// reader so tests can supply synthetic input without touching real process stdin.
    fn resolve_bls_passphrase_with_reader<R: std::io::BufRead>(
        &self,
        preloaded: Option<String>,
        mut reader: R,
    ) -> eyre::Result<Option<String>> {
        let mut passphrase = preloaded;
        match self.bls_passphrase_source {
            PassSource::Env => {} // Already have the env var if provided.
            PassSource::Stdin => {
                let mut buffer = String::new();
                if let Err(err) = reader.read_line(&mut buffer) {
                    bail!("Error reading BLS passphrase from stdin: {err:?}");
                }
                passphrase = Some(buffer.trim_end().to_string()).filter(|s| !s.is_empty());
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
                            .ok();
                }
            },
            PassSource::NoPassphrase => {
                passphrase = None;
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

    fn cli(args: &[&str]) -> Cli {
        Cli::try_parse_args_from(args).expect("cli parses")
    }

    #[test]
    fn test_no_passphrase_source_resolves_none() {
        let cli = cli(&["tn", "node", "--bls-passphrase-source", "no-passphrase"]);
        assert_eq!(cli.resolve_bls_passphrase(Some("preload".into())).unwrap(), None);
    }

    #[test]
    fn test_env_source_keeps_preloaded_value() {
        let cli = cli(&["tn", "node"]); // default source is env
        assert_eq!(
            cli.resolve_bls_passphrase(Some("preload".into())).unwrap(),
            Some("preload".into())
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
            Some("sekrit".into())
        );
    }

    /// The ONLY test allowed to touch the environment: `cargo test` runs tests
    /// concurrently in one process, and the env is process-global.
    #[test]
    fn test_env_preload_reads_and_clears_var() {
        std::env::set_var(BLS_PASSPHRASE_ENVVAR, "sekrit");
        assert_eq!(get_bls_passphrase_from_env(), Some("sekrit".into()));
        assert!(std::env::var(BLS_PASSPHRASE_ENVVAR).is_err());
        assert_eq!(get_bls_passphrase_from_env(), None);
    }
}
