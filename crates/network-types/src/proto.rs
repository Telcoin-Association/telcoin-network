// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/// The module responsible for generating anemo network utilities.
///
/// The `build.rs` file in this crate defines the services and their
/// methods. The output is generated here.
///
/// Current understanding:
/// - each service generates a server, client, and trait
///     - changes should be made to this file and build.rs
/// - traits are impl on xxHandlers and passed in to xxServer::new()
///     - so, the traits should be implemented on types that handle the server logic (handle the
///       requests)
///     - see `PrimaryToPrimary` and `PrimaryToPrimaryServer`
/// - clients are structs that actually send the request
/// - server structs are wrappers for thread safety and take an inner type that implements the
///   server's trait
///     - ie) PrimaryToPrimary
///
/// Unknowns:
/// - what are inbound request layers? `add_layer_for...`
///     - see crates/consensus/primary/src/primary.rs:L248
/// - is there more to Routes than just keeping wires from crossing?
/// - probably want to change codec to be consistent (rlp/ssz), but I think this can wait for post
///   testnet
mod anemo_build {
    // output from build
    include!(concat!(env!("OUT_DIR"), "/tn.PrimaryToPrimary.rs"));
    include!(concat!(env!("OUT_DIR"), "/tn.PrimaryToWorker.rs"));
    include!(concat!(env!("OUT_DIR"), "/tn.WorkerToPrimary.rs"));
    include!(concat!(env!("OUT_DIR"), "/tn.WorkerToWorker.rs"));
    include!(concat!(env!("OUT_DIR"), "/tn.EngineToPrimary.rs"));
    include!(concat!(env!("OUT_DIR"), "/tn.PrimaryToEngine.rs"));
}

// exports from build.rs
pub use anemo_build::{
    // engine
    engine_to_primary_client::EngineToPrimaryClient,
    engine_to_primary_server::{EngineToPrimary, EngineToPrimaryServer, MockEngineToPrimary},
    primary_to_engine_client::PrimaryToEngineClient,
    primary_to_engine_server::{MockPrimaryToEngine, PrimaryToEngine, PrimaryToEngineServer},
    // primary
    primary_to_primary_client::PrimaryToPrimaryClient,
    primary_to_primary_server::{MockPrimaryToPrimary, PrimaryToPrimary, PrimaryToPrimaryServer},
    primary_to_worker_client::PrimaryToWorkerClient,
    primary_to_worker_server::{MockPrimaryToWorker, PrimaryToWorker, PrimaryToWorkerServer},
    // worker
    worker_to_primary_client::WorkerToPrimaryClient,
    worker_to_primary_server::{MockWorkerToPrimary, WorkerToPrimary, WorkerToPrimaryServer},
    worker_to_worker_client::WorkerToWorkerClient,
    worker_to_worker_server::{MockWorkerToWorker, WorkerToWorker, WorkerToWorkerServer},
};
