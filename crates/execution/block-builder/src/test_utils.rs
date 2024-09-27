//! Types for testing only.

/// Type to track the number of builds for this block builder.
#[derive(Debug)]
pub(crate) struct MaxBuilds {
    /// The maximum number of blocks the worker should build before shutting down.
    max_builds: usize,
    /// The number of blocks this block builder has built.
    ///
    /// NOTE: this is only used when `max_blocks` is specified.
    pub(crate) num_builds: usize,
}

impl MaxBuilds {
    /// Create a new instance of `Self`.
    pub(crate) fn new(max_builds: usize) -> Self {
        // always start at 0
        Self { max_builds, num_builds: 0 }
    }

    /// Check if the task has reached the maximum number of blocks to build as specified by `max_builds`.
    ///
    /// Note: this is only used for testing and debugging purposes.
    pub(crate) fn has_reached_max(&self) -> bool {
        self.num_builds >= self.max_builds
    }
}
