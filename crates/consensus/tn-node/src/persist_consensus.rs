// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Responsible for persisting all consensus output to a persistant store for later retrieval.

use narwhal_typed_store::{
    tables::{SubDags, WorkerBlocks},
    traits::{Database, DbTxMut},
    ReDB,
};
use tn_types::ConsensusOutput;
use tokio::sync::broadcast;

/// Implements writing consensus output to a peristant store.
pub struct PersistConsensus {
    db: ReDB,
}

impl PersistConsensus {
    /// Create a new PersistConsensus.  Will open a new ReDB database at path and save to that.
    pub fn new<P: AsRef<std::path::Path> + Send>(path: P) -> PersistConsensus {
        // Just pick the DB type and tables here, keep it simple.
        // Nothing else cares about this, eventually some RPC calls will need to use this module to
        // expose the DB.
        let db = ReDB::open(path).expect("Cannot open database");
        db.open_table::<WorkerBlocks>().expect("failed to open table!");
        db.open_table::<SubDags>().expect("failed to open table!");
        Self { db }
    }

    /// Spawns an async task that will pull from rx and save each ConsensusOutput to the DB.
    pub async fn start(&self, mut rx: broadcast::Receiver<ConsensusOutput>) {
        let db = self.db.clone();
        // Async spawn for simplicity but if the DB writes are slow this could bog down tokio.  Will
        // only try to do one write at a time though.
        tokio::spawn(async move {
            while let Ok(consensus_output) = rx.recv().await {
                match db.write_txn() {
                    Ok(mut txn) => {
                        if let Err(e) = txn.insert::<SubDags>(
                            &consensus_output.sub_dag.sub_dag_index,
                            &consensus_output.sub_dag,
                        ) {
                            tracing::error!(target: "engine", ?e, "error saving a committed sub dag to persistant storage!")
                        }
                        for wb in consensus_output.blocks.iter().flatten() {
                            if let Err(e) = txn.insert::<WorkerBlocks>(&wb.digest(), wb) {
                                tracing::error!(target: "engine", ?e, "error saving a worker block to persistant storage!")
                            }
                        }
                        if let Err(e) = txn.commit() {
                            tracing::error!(target: "engine", ?e, "error saving committing to persistant storage!")
                        }
                    }
                    Err(e) => {
                        tracing::error!(target: "engine", ?e, "error getting a transaction on persistant storage!")
                    }
                }
            }
        });
    }
}
