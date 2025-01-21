//! redb metrics

use prometheus::{default_registry, register_int_gauge_with_registry, IntGauge, Registry};

#[derive(Debug)]
pub struct ReDbMetrics {
    pub tree_height: IntGauge,
    pub allocated_pages: IntGauge,
    pub leaf_pages: IntGauge,
    pub branch_pages: IntGauge,
    pub stored_bytes: IntGauge,
    pub metadata_bytes: IntGauge,
    pub fragmented_bytes: IntGauge,
    pub page_size: IntGauge,
}

impl ReDbMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            tree_height: register_int_gauge_with_registry!(
                "redb_tree_height",
                "Maximum traversal distance to reach the deepest (key, value) pair, across all tables",
                registry,
            )?,
            allocated_pages: register_int_gauge_with_registry!(
                "redb_allocated_pages",
                "Number of pages allocated",
                registry,
            )?,
            leaf_pages: register_int_gauge_with_registry!(
                "redb_leaf_pages",
                "Number of leaf pages that store user data",
                registry,
            )?,
            branch_pages: register_int_gauge_with_registry!(
                "redb_branch_pages",
                "Number of branch pages in btrees that store user data",
                registry,
            )?,
            stored_bytes: register_int_gauge_with_registry!(
                "redb_stored_bytes",
                "Number of bytes consumed by keys and values that have been inserted. Does not include indexing overhead",
                registry,
            )?,
            metadata_bytes: register_int_gauge_with_registry!(
                "redb_metadata_bytes",
                "Number of bytes consumed by keys in internal branch pages, plus other metadata",
                registry,
            )?,
            fragmented_bytes: register_int_gauge_with_registry!(
                "redb_fragmented_bytes",
                "Number of bytes consumed by fragmentation, both in data pages and internal metadata tables",
                registry,
            )?,
            page_size: register_int_gauge_with_registry!(
                "redb_page_size",
                "Number of bytes per page",
                registry,
            )?,
        })
    }
}

impl Default for ReDbMetrics {
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
