//! Telcoin Network worker gateway.
//!
//! A mostly stateless reverse proxy that fronts the worker JSON-RPC endpoint:
//! each replica keeps only bounded per-process state, such as its rate-limit
//! buckets and upstream readiness, so any replica can serve any request. It
//! forwards JSON-RPC calls unchanged to a ready upstream worker, gates them on
//! a polled per-worker readiness signal (`GET /health/workers`), and exposes
//! its own liveness and readiness endpoints for orchestration. With
//! `--redirect-queries <URL>` the worker keeps transaction submissions and any
//! other call the README's "Query redirect" section routes to it, and
//! everything else goes to that endpoint, which has no readiness gate. See the
//! crate `README.md` for the configuration, routing, and readiness contracts.

mod app;
mod cli;
mod config;
mod error;
mod proxy;
mod ratelimit;
mod readiness;
mod server;
mod telemetry;

use std::io::IsTerminal as _;

use clap::Parser as _;
use tracing_subscriber::{fmt::MakeWriter, util::SubscriberInitExt as _, EnvFilter};

use crate::cli::{Cli, LogFormat};

fn main() {
    if let Err(err) = try_main() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}

/// Parse the CLI, initialize tracing, and run the gateway on a multi-thread
/// runtime until SIGTERM / ctrl-c.
fn try_main() -> eyre::Result<()> {
    let cli = Cli::parse();
    let ansi = use_ansi(std::io::stdout().is_terminal(), std::env::var_os("NO_COLOR").as_deref());
    init_tracing(&cli.log_filter, cli.log_format, ansi);
    let settings = cli.into_settings()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("worker-gateway")
        .enable_io()
        .enable_time()
        .build()?;

    runtime.block_on(app::run(settings))
}

/// Whether log lines carry colour codes: only when stdout is a terminal and
/// `NO_COLOR` is unset or empty (<https://no-color.org>). Colour only helps a
/// person at a terminal; a file, a pipe or a log collector would get the raw
/// escape codes. `NO_COLOR` still turns it off at a terminal, as
/// tracing-subscriber does when left to decide.
fn use_ansi(is_terminal: bool, no_color: Option<&std::ffi::OsStr>) -> bool {
    is_terminal && no_color.is_none_or(|value| value.is_empty())
}

/// Install the global tracing subscriber, writing to stdout (see
/// [`subscriber`]).
fn init_tracing(filter: &str, format: LogFormat, ansi: bool) {
    subscriber(filter, format, ansi, std::io::stdout).init();
}

/// Build a fmt tracing subscriber that honours the `--log-filter` directive
/// (and `RUST_LOG`), writes `format` lines to `writer`, and writes colour codes
/// only when `ansi` is set.
fn subscriber<W>(
    filter: &str,
    format: LogFormat,
    ansi: bool,
    writer: W,
) -> Box<dyn tracing::Subscriber + Send + Sync>
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    let builder = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::builder().parse_lossy(filter))
        .with_ansi(ansi)
        .with_writer(writer);
    match format {
        LogFormat::Text => Box::new(builder.finish()),
        LogFormat::Json => Box::new(builder.json().finish()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, IsTerminal as _},
        sync::{Arc, Mutex, PoisonError},
    };

    use super::*;

    /// In-memory log sink standing in for stdout redirected to a file or pipe.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Captured {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap_or_else(PoisonError::into_inner))
                .into_owned()
        }
    }

    impl io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap_or_else(PoisonError::into_inner).extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for Captured {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    /// Emit two events through a scoped subscriber and return what it wrote.
    fn capture(format: LogFormat, ansi: bool) -> String {
        let sink = Captured::default();
        let scoped = subscriber("info", format, ansi, sink.clone());
        tracing::subscriber::with_default(scoped, || {
            tracing::info!(target: "gateway", answer = 42, "first event");
            tracing::warn!(target: "gateway::proxy", "second event");
        });
        sink.text()
    }

    #[test]
    fn log_format_json_emits_one_json_object_per_line() -> eyre::Result<()> {
        let output = capture(LogFormat::Json, false);
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2, "one line per event: {output:?}");

        let first: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(first["level"], "INFO");
        assert_eq!(first["target"], "gateway");
        assert_eq!(first["fields"]["message"], "first event");
        assert_eq!(first["fields"]["answer"], 42);

        let second: serde_json::Value = serde_json::from_str(lines[1])?;
        assert_eq!(second["level"], "WARN");
        assert_eq!(second["target"], "gateway::proxy");
        assert!(second["fields"].is_object(), "{second}");
        Ok(())
    }

    #[test]
    fn log_format_text_has_no_ansi_when_not_a_terminal() -> eyre::Result<()> {
        // a file is what stdout is when the output is redirected; feed its
        // terminal check through the same decision the call site makes
        let ansi = use_ansi(tempfile::tempfile()?.is_terminal(), None);
        assert!(!ansi, "a regular file is not a terminal");

        let plain = capture(LogFormat::Text, ansi);
        assert!(plain.contains("first event"), "{plain:?}");
        assert!(!plain.contains("\x1b["), "colour codes off a terminal: {plain:?}");

        // control: the same events with colour on do carry escape codes, so the
        // check above is not passing for want of the `ansi` feature
        assert!(capture(LogFormat::Text, true).contains("\x1b["));
        Ok(())
    }

    #[test]
    fn colour_needs_a_terminal_and_no_non_empty_no_color() {
        use std::ffi::OsStr;

        assert!(use_ansi(true, None));
        assert!(use_ansi(true, Some(OsStr::new(""))), "an empty NO_COLOR counts as unset");
        assert!(!use_ansi(true, Some(OsStr::new("1"))), "NO_COLOR wins at a terminal");
        assert!(!use_ansi(false, None), "a file or pipe is never coloured");
        assert!(!use_ansi(false, Some(OsStr::new(""))));
    }
}
