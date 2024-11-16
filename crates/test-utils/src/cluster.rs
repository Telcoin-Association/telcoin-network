// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cluster fixture to represent a local network.
use crate::{authority::AuthorityDetails, default_test_execution_node, CommitteeFixture};
use itertools::Itertools;
use reth::tasks::TaskExecutor;
use std::{collections::HashMap, time::Duration};
use tn_storage::traits::Database;
use tn_types::{Committee, WorkerId};
use tracing::info;

#[cfg(test)]
#[path = "tests/cluster_tests.rs"]
pub mod cluster_tests;

/// Test fixture that holds all information needed to run a local network.
pub struct Cluster<DB> {
    fixture: CommitteeFixture<DB>,
    authorities: HashMap<usize, AuthorityDetails<DB>>,
    pub committee: Committee,
}

impl<DB> Cluster<DB>
where
    DB: Database,
{
    /// Initialises a new cluster by the provided parameters. The cluster will
    /// create all the authorities (primaries & workers) that are defined under
    /// the committee structure, but none of them will be started.
    ///
    /// Fields passed in via Parameters will be used, expect specified ports which have to be
    /// different for each instance. If None, the default Parameters will be used.
    pub fn new<F>(executor: TaskExecutor, new_db: F) -> Self
    where
        F: Fn() -> DB,
    {
        let fixture = CommitteeFixture::builder(new_db).randomize_ports(true).build();
        let committee = fixture.committee();

        info!("###### Creating new cluster ######");
        info!("Validator keys:");
        let mut nodes = HashMap::new();

        for (id, authority_fixture) in fixture.authorities().enumerate() {
            info!("Key {id} -> {}", authority_fixture.primary_public_key());

            let authority_id = authority_fixture.id();
            let authority_execution_address = authority_fixture.execution_address();

            let engine = default_test_execution_node(
                None, // default: adiri chain
                Some(authority_execution_address),
                executor.clone(),
            )
            .expect("default test execution node");

            let consensus_config = authority_fixture.consensus_config().clone();

            let authority =
                AuthorityDetails::new(id, authority_id, consensus_config.clone(), engine);
            nodes.insert(id, authority);
        }

        Self { fixture, authorities: nodes, committee }
    }

    /// Starts a cluster by the defined number of authorities. The authorities
    /// will be started sequentially started from the one with id zero up to
    /// the provided number `authorities_number`. If none number is provided, then
    /// the maximum number of authorities will be started.
    ///
    /// If a number higher than the available ones in the committee is provided then
    /// the method will panic.
    ///
    /// The workers_per_authority dictates how many workers per authority should
    /// also be started (the same number will be started for each authority). If none
    /// is provided then the maximum number of workers will be started.
    /// If the `boot_wait_time` is provided then between node starts we'll wait for this
    /// time before the next node is started. This is useful to simulate staggered
    /// node starts. If none is provided then the nodes will be started immediately
    /// the one after the other.
    pub async fn start(
        &mut self,
        authorities_number: Option<usize>,
        workers_per_authority: Option<usize>,
        boot_wait_time: Option<Duration>,
    ) {
        let max_authorities = self.committee.size();
        let authorities = authorities_number.unwrap_or(max_authorities);

        if authorities > max_authorities {
            panic!("Provided nodes number is greater than the maximum allowed");
        }

        for id in 0..authorities {
            info!("Spinning up node: {id}");
            self.start_node(id, false, workers_per_authority)
                .await
                .expect("node started successfully for authority");

            if let Some(d) = boot_wait_time {
                // we don't want to wait after the last node has been boostraped
                if id < authorities - 1 {
                    info!(
                        "#### Will wait for {} seconds before starting the next node ####",
                        d.as_secs()
                    );
                    tokio::time::sleep(d).await;
                }
            }
        }
    }

    /// Starts the authority node by the defined id - if not already running - and
    /// the details are returned. If the node is already running then a panic
    /// is thrown instead.
    ///
    /// When the preserve_store is true, then the started authority will use the
    /// same path that has been used the last time when started (both the primary
    /// and the workers).
    ///
    /// This is basically a way to use the same storage between node restarts.
    /// When the preserve_store is false, then authority will start with an empty
    /// storage.
    ///
    /// If the `workers_per_authority` is provided then the corresponding number of
    /// workers will be started per authority. Otherwise if not provided, then maximum
    /// number of workers will be started per authority.
    pub async fn start_node(
        &mut self,
        id: usize,
        preserve_store: bool,
        workers_per_authority: Option<usize>,
    ) -> eyre::Result<()> {
        let authority = self
            .authorities
            .get_mut(&id)
            .unwrap_or_else(|| panic!("Authority with id {} not found", id));

        // start the primary
        authority.start_primary().await?;

        // start the workers
        if let Some(workers) = workers_per_authority {
            for worker_id in 0..workers {
                authority.start_worker(worker_id as WorkerId, preserve_store).await?;
            }
        } else {
            authority.start_all_workers(preserve_store).await?;
        }

        Ok(())
    }

    /// This method stops the authority (both the primary and the worker nodes)
    /// with the provided id.
    pub async fn stop_node(&self, id: usize) {
        if let Some(node) = self.authorities.get(&id) {
            node.stop_all().await;
            info!("Aborted node for id {id}");
        } else {
            info!("Node with {id} not found - nothing to stop");
        }
        // TODO: wait for the node's network port to be released?
    }

    /// Returns all the running authorities. Any authority that:
    /// * has been started ever
    /// * or has been stopped
    ///
    /// will not be returned by this method.
    pub async fn authorities(&self) -> Vec<AuthorityDetails<DB>> {
        let mut result = Vec::new();

        for authority in self.authorities.values() {
            if authority.is_running().await {
                result.push(authority.clone());
            }
        }

        result
    }

    /// Returns the authority identified by the provided id. Will panic if the
    /// authority with the id is not found. The returned authority can be freely
    /// cloned and managed without having the need to fetch again.
    pub fn authority(&self, id: usize) -> AuthorityDetails<DB> {
        self.authorities
            .get(&id)
            .unwrap_or_else(|| panic!("Authority with id {} not found", id))
            .clone()
    }

    /// This method asserts the progress of the cluster.
    /// `expected_nodes`: Nodes expected to have made progress. Any number different than that
    /// will make the assertion fail.
    /// `commit_threshold`: The acceptable threshold between the minimum and maximum reported
    /// commit value from the nodes.
    pub async fn assert_progress(
        &self,
        expected_nodes: u64,
        commit_threshold: u64,
    ) -> HashMap<usize, u64> {
        let r = self.authorities_latest_commit_round().await;
        let rounds: HashMap<usize, u64> =
            r.into_iter().map(|(key, value)| (key, value as u64)).collect();

        assert_eq!(
            rounds.len(),
            expected_nodes as usize,
            "Expected to have received commit metrics from {expected_nodes} nodes"
        );
        assert!(rounds.values().all(|v| v > &1), "All nodes are available so all should have made progress and committed at least after the first round");

        if expected_nodes == 0 {
            return HashMap::new();
        }

        let (min, max) = rounds.values().minmax().into_option().unwrap();
        assert!(max - min <= commit_threshold, "Nodes shouldn't be that behind");

        rounds
    }

    async fn authorities_latest_commit_round(&self) -> HashMap<usize, f64> {
        let mut authorities_latest_commit = HashMap::new();

        for authority in self.authorities().await {
            let primary = authority.primary().await;
            let value =
                primary.consensus_metrics().await.last_committed_round.with_label_values(&[]).get()
                    as f64;

            authorities_latest_commit.insert(primary.id, value);

            info!("[Node {}] Metric tn_primary_last_committed_round -> {value}", primary.id);
        }

        authorities_latest_commit
    }

    pub fn fixture(&self) -> &CommitteeFixture<DB> {
        &self.fixture
    }
}
