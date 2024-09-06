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
/// - probably want to change codec to be consistent (rlp), but I think this can wait for post
///   testnet
mod narwhal {

    // output from build
    include!(concat!(env!("OUT_DIR"), "/narwhal.PrimaryToPrimary.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.PrimaryToWorker.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.WorkerToPrimary.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.WorkerToWorker.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.EngineToWorker.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.WorkerToEngine.rs"));
}

// exports from build
pub use narwhal::{
    // engine
    engine_to_worker_client::EngineToWorkerClient,
    engine_to_worker_server::{EngineToWorker, EngineToWorkerServer, MockEngineToWorker},
    // primary
    primary_to_primary_client::PrimaryToPrimaryClient,
    primary_to_primary_server::{MockPrimaryToPrimary, PrimaryToPrimary, PrimaryToPrimaryServer},
    primary_to_worker_client::PrimaryToWorkerClient,
    primary_to_worker_server::{MockPrimaryToWorker, PrimaryToWorker, PrimaryToWorkerServer},
    // worker
    worker_to_engine_client::WorkerToEngineClient,
    worker_to_engine_server::{MockWorkerToEngine, WorkerToEngine, WorkerToEngineServer},
    worker_to_primary_client::WorkerToPrimaryClient,
    worker_to_primary_server::{MockWorkerToPrimary, WorkerToPrimary, WorkerToPrimaryServer},
    worker_to_worker_client::WorkerToWorkerClient,
    worker_to_worker_server::{MockWorkerToWorker, WorkerToWorker, WorkerToWorkerServer},
    // Empty, Transaction as TransactionProto, ValidatorData,
    // transactions_client::TransactionsClient,
    // transactions_server::{Transactions, TransactionsServer},
};

// impl From<Transaction> for TransactionProto {
//     fn from(transaction: Transaction) -> Self {
//         TransactionProto {
//             transaction: Bytes::from(transaction),
//         }
//     }
// }

// impl From<TransactionProto> for Transaction {
//     fn from(transaction: TransactionProto) -> Self {
//         transaction.transaction.to_vec()
//     }
// }
