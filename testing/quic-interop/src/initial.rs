//! Emit a genuine encrypted QUIC Initial for the isolated networking lane.

use std::path::PathBuf;

/// Generate traffic without requiring a connection to an external host.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), tn_quic_candidate::Error> {
    let output = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| tn_quic_candidate::Error::Setup("expected output file".into()))?;
    tn_quic_candidate::write_initial(&output).await
}
