// SPDX-License-Identifier: MIT OR Apache-2.0
//! ExEx manager for distributing notifications and coordinating multiple ExEx tasks.
//!
//! This module will be implemented in Phase 2.

use crate::TnExExNotification;

/// Handle for communicating with a single ExEx task.
///
/// To be implemented in Phase 2.
#[derive(Debug)]
pub struct TnExExHandle {
    // Fields will be added in Phase 2
}

/// Manager that coordinates multiple ExEx tasks.
///
/// To be implemented in Phase 2.
#[derive(Debug)]
pub struct TnExExManager {
    // Fields will be added in Phase 2
}

/// Cloneable handle for sending notifications to the manager.
///
/// To be implemented in Phase 2.
#[derive(Debug, Clone)]
pub struct TnExExManagerHandle {
    // Fields will be added in Phase 2
}

impl TnExExManagerHandle {
    /// Creates an empty handle for when no ExExes are installed.
    ///
    /// This allows the engine to use the same code path regardless of whether
    /// ExExes are configured.
    pub fn empty() -> Self {
        Self {
            // Fields will be added in Phase 2
        }
    }

    /// Returns whether this handle has any ExExes registered.
    pub fn has_exexs(&self) -> bool {
        // Phase 2: will check if there are any ExEx handles
        false
    }

    /// Sends a notification to all registered ExExes.
    ///
    /// This is a no-op for empty handles.
    pub fn send(&self, _notification: TnExExNotification) {
        // Phase 2: will send to all ExEx handles
    }
}
