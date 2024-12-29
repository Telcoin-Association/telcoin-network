//! Tracing helper to subscribe to tracing output.

use tracing_subscriber::EnvFilter;

///  Initializes a tracing subscriber for tests that's configurable with `RUST_LOG`. This function silently fails if the subscriber could not be installed.
pub fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
}
