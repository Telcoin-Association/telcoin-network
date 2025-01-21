//! mdbx metrics

use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};

#[derive(Debug)]
pub struct MdbxMetrics {
    pub page_size: IntGauge,
    pub depth: IntGauge,
    pub branch_pages: IntGauge,
    pub leaf_pages: IntGauge,
    pub overflow_pages: IntGauge,
    pub entries: IntGauge,
}

impl MdbxMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            page_size: register_int_gauge_with_registry!(
                "mdbx_page_size",
                "Size of a database page. This is the same for all databases in the environment.",
                registry,
            )?,
            depth: register_int_gauge_with_registry!(
                "mdbx_depth",
                "Depth (height) of the B-tree.",
                registry,
            )?,
            branch_pages: register_int_gauge_with_registry!(
                "mdbx_branch_pages",
                "Number of internal (non-leaf) pages.",
                registry,
            )?,
            leaf_pages: register_int_gauge_with_registry!(
                "mdbx_leaf_pages",
                "Number of leaf pages.",
                registry,
            )?,
            overflow_pages: register_int_gauge_with_registry!(
                "mdbx_overflow_pages",
                "Number of overflow pages.",
                registry,
            )?,
            entries: register_int_gauge_with_registry!(
                "mdbx_entries",
                "Number of data items.",
                registry,
            )?,
        })
    }
}

impl Default for MdbxMetrics {
    fn default() -> Self {
        // try_new() should not fail except under certain conditions with testing (see comment
        // below). This pushes the panic or retry decision lower and supporting try_new
        // allways a user to deal with errors if desired (have a non-panic option).
        // We always want do use default_registry() when not in test.
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(_) => {
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests due to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}
