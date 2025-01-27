//! Synchronize state between primaries.
//!
//! This module primarily deals with Certificate synchronization.

// - primary receives gossip for cert digest
// - request state_sync to retrieve certificate
//  - verify cert
//  - store cert
//  - share with others
//      - gossip message "available" ?
