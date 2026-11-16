// SPDX-License-Identifier: MIT or Apache-2.0

/// The `CompositeDatabase` backend that splits the workload into epoch/kad/cache sub-databases,
/// each a `LayeredDatabase`: the release before tndb's consensus DB, kept with MDBX for the
/// migration tests and the benches.
pub mod composite_db;
pub mod database;
/// `LayeredDatabase`: a write-behind memory layer whose writer thread commits to a backend.
pub mod layered_db;

pub use database::MdbxDatabase;
