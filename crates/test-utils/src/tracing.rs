//! Tracing helper to subscribe to tracing output.

///  Initializes a tracing subscriber for tests that's configurable with `RUST_LOG`. This function
/// silently fails if the subscriber could not be installed.
pub fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ACTIVE)
        .with_writer(std::io::stdout)
        .try_init();
}
