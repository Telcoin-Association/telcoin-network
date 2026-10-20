//! Lightweight tests of the production diagnostic source without the node dependency graph.

#[cfg(test)]
#[path = "../../../crates/network-libp2p/src/diagnostics.rs"]
mod diagnostics;

/// Compile and exercise every production tracing wrapper through its actual emission gate.
#[test]
fn production_macros_preserve_diagnostics() {
    let capture = diagnostics::emission_tests::Capture::default();
    tracing::subscriber::with_default(capture.clone(), || {
        diagnostics::bounded_warn!(Record, target: "network", "record warning");
        diagnostics::bounded_error!(Swarm, target: "network", "swarm error");
        diagnostics::bounded_reply_error!(target: "network", "reply failure");
        diagnostics::flush();
    });
    assert_eq!(capture.message_count("record warning"), 1);
    assert_eq!(capture.message_count("swarm error"), 1);
    assert_eq!(capture.message_count("reply failure"), 1);
}
