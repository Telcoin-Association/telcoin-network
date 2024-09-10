// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use consensus_metrics::{
    metered_channel::{Receiver, Sender},
    spawn_logged_monitored_task,
};
use fastcrypto::groups;
use fastcrypto_tbls::{dkg, nodes};
use futures::{FutureExt, StreamExt};
use reth_primitives::BlockNumHash;
use std::{
    collections::VecDeque, future::Future, num::NonZero, pin::Pin, task::{Context, Poll}
};
use tap::TapFallible;
use tn_types::{AuthorityIdentifier, ChainIdentifier, Committee, RandomnessPrivateKey};
use tn_types::{
    Certificate, ConditionalBroadcastReceiver, Round, SystemMessage,
};
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, info, warn};
use crate::error::StateHandlerResult;

type PkG = groups::bls12381::G2Element;
type EncG = groups::bls12381::G2Element;

#[cfg(test)]
#[path = "tests/state_handler_tests.rs"]
pub mod state_handler_tests;

// TODO: is this even necessary?
const DEFAULT_EXECUTION_RESULT_SIZE: usize = 8;

/// Type alias for the async task that updates state.
type PendingStateUpdate = oneshot::Receiver<StateHandlerResult<()>>;

/// Updates Narwhal system state based on certificates received from consensus.
pub struct StateHandler {
    /// Primary's id.
    authority_id: AuthorityIdentifier,
    /// Receives the ordered certificates from consensus.
    ///
    /// TODO: use `committedcertificates` type!
    rx_committed_certificates: Receiver<(Round, Vec<Certificate>)>,
    /// Receiver for shutdown.
    ///
    /// Shutdown is used to signal committee change.
    rx_shutdown_stream: BroadcastStream<()>,
    /// Sending half to notify the `Proposer` about a committed header.
    ///
    /// Committed headers are sequenced in the consensus DAG.
    tx_committed_own_headers: Sender<(Round, Vec<Round>)>,
    /// A channel to send system messages to the proposer.
    tx_system_messages: Sender<SystemMessage>,
    /// If set, generates Narwhal system messages for random beacon
    /// DKG and randomness generation.
    randomness_state: Option<RandomnessState>,
    /// The WAN for primary to primary communication.
    network: anemo::Network,
    /// Queue for updates received from consensus.
    updates_queue: VecDeque<CommittedCertificates>,
    /// The optional task that updates state after certificates were committed by consensus.
    ///
    /// This value is `Some` when consensus commits certificates in the DAG.
    pending_update: Option<PendingStateUpdate>,

    /// Receiver for requests for a round's `[BlockNumHash]`.
    ///
    /// - The proposer requests this? Or does it just track the watch channel itself?
    /// - Validator would request this
    ///     - I think headers can come in from a previous round so this needs to track a few rounds
    ///     - if the last seen round is less than the requested round, wait for execution to update
    rx_state_request: Receiver<(Round, oneshot::Sender<Option<BlockNumHash>>)>,
    /// LRU cache to keep the map from growing infinitely large for now.
    ///
    /// TODO: just use hashmap for epoch once epochs in place
    execution_state: lru::LruCache<Round, BlockNumHash>,
    /// TODO: can't use a watch channel here because state handler MUST receive
    /// every update for round.
    ///
    /// track canonical notifications stream?
    execution_updates: todo!(),
}

/// TODO: update
/// Container type for tracking consensus updates that affect state handler.
struct CommittedCertificates {
    ///
    /// TODO: is this the round of the certificate or current round?
    ///
    pub round: Round,
    /// Committed certificates.
    pub certificates: Vec<Certificate>,
}

// Internal state for randomness DKG and generation.
// TODO: Write a brief protocol description.
struct RandomnessState {
    party: dkg::Party<PkG, EncG>,
    processed_messages: Vec<dkg::ProcessedMessage<PkG, EncG>>,
    used_messages: Option<dkg::UsedProcessedMessages<PkG, EncG>>,
    confirmations: Vec<dkg::Confirmation<EncG>>,
    dkg_output: Option<dkg::Output<PkG, EncG>>,
}

impl RandomnessState {
    /// Returns None in case of invalid input or other failure to initialize DKG.
    /// In this case, narwhal will continue to function normally and simply not run
    /// the random beacon protocol during the current epoch.
    fn try_new(
        chain: &ChainIdentifier,
        committee: Committee,
        private_key: RandomnessPrivateKey,
        mut beacon_delta: Option<u16>,
    ) -> Option<Self> {
        // see if beacon delta is enabled
        let random_state = match beacon_delta.take() {
            Some(delta) => {
                let info = committee.randomness_dkg_info();
                let nodes = info
                    .iter()
                    .map(|(id, pk, stake)| nodes::Node::<EncG> {
                        id: id.0,
                        pk: pk.clone(),
                        weight: *stake as u16,
                    })
                    .collect();
                let nodes = match nodes::Nodes::new(nodes) {
                    Ok(nodes) => nodes,
                    Err(err) => {
                        error!("Error while initializing random beacon Nodes: {err:?}");
                        return None;
                    }
                };
                let (nodes, t) = nodes.reduce(
                    committee
                        .validity_threshold()
                        .try_into()
                        .expect("validity threshold should fit in u16"),
                    // protocol_config.random_beacon_reduction_allowed_delta(),
                    delta, // 800 per current sui default
                );
                let total_weight = nodes.n();
                let party = match dkg::Party::<PkG, EncG>::new(
                    private_key,
                    nodes,
                    t.into(),
                    fastcrypto_tbls::random_oracle::RandomOracle::new(
                        format!("dkg {:x?} {}", chain.as_bytes(), committee.epoch()).as_str(),
                    ),
                    &mut rand::thread_rng(),
                ) {
                    Ok(party) => party,
                    Err(err) => {
                        error!("Error while initializing random beacon Party: {err:?}");
                        return None;
                    }
                };
                info!("random beacon: state initialized with total_weight={total_weight}, t={t}");
                Some(Self {
                    party,
                    processed_messages: Vec::new(),
                    used_messages: None,
                    confirmations: Vec::new(),
                    dkg_output: None,
                })
            }
            None => {
                info!("random beacon: disabled");
                None
            }
        };

        random_state
    }

    async fn start_dkg(&self, tx_system_messages: &Sender<SystemMessage>) {
        let msg = self.party.create_message(&mut rand::thread_rng());
        info!("random beacon: sending DKG Message: {msg:?}");
        let _ = tx_system_messages.send(SystemMessage::DkgMessage(msg)).await;
    }

    fn add_message(&mut self, msg: dkg::Message<PkG, EncG>) {
        if self.used_messages.is_some() {
            // We've already sent a `Confirmation`, so we can't add any more messages.
            return;
        }
        match self.party.process_message(msg, &mut rand::thread_rng()) {
            Ok(processed) => {
                self.processed_messages.push(processed);
            }
            Err(err) => {
                debug!("error while processing randomness DKG message: {err:?}");
            }
        }
    }

    fn add_confirmation(&mut self, conf: dkg::Confirmation<EncG>) {
        if self.used_messages.is_none() {
            // We should never see a `Confirmation` before we've sent our `Message` because
            // DKG messages are processed in consensus order.
            return;
        }
        if self.dkg_output.is_some() {
            // Once we have completed DKG, no more `Confirmation`s are needed.
            return;
        }
        self.confirmations.push(conf)
    }

    // Generates the next SystemMessage needed to advance the random beacon protocol, if possible,
    // and sends it to the proposer.
    async fn advance(&mut self, tx_system_messages: &Sender<SystemMessage>) {
        // Once we have enough ProcessedMessages, send a Confirmation.
        if self.used_messages.is_none() && !self.processed_messages.is_empty() {
            match self.party.merge(&self.processed_messages) {
                Ok((conf, used_msgs)) => {
                    info!(
                        "random beacon: sending DKG Confirmation with {} complaints",
                        conf.complaints.len()
                    );
                    self.used_messages = Some(used_msgs);
                    let _ = tx_system_messages.send(SystemMessage::DkgConfirmation(conf)).await;
                }
                Err(fastcrypto::error::FastCryptoError::NotEnoughInputs) => (), /* wait for more */
                // bad input
                Err(e) => debug!("Error while merging randomness DKG messages: {e:?}"),
            }
        }

        // Once we have enough Confirmations, process them and update shares.
        if self.dkg_output.is_none()
            && !self.confirmations.is_empty()
            && self.used_messages.is_some()
        {
            match self.party.complete(
                self.used_messages.as_ref().expect("checked above"),
                &self.confirmations,
                self.party.t() * 2 - 1, // t==f+1, we want 2f+1
                &mut rand::thread_rng(),
            ) {
                Ok(output) => {
                    self.dkg_output = Some(output);
                    info!("random beacon: DKG complete with Output {:?}", self.dkg_output);
                }
                Err(fastcrypto::error::FastCryptoError::NotEnoughInputs) => (), /* wait for more */
                // input
                Err(e) => error!("Error while processing randomness DKG confirmations: {e:?}"),
            }
        }
    }
}

impl StateHandler {
    /// Create a new instance of Self.
    // #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        chain: &ChainIdentifier,
        authority_id: AuthorityIdentifier,
        committee: Committee,
        rx_committed_certificates: Receiver<(Round, Vec<Certificate>)>,
        rx_shutdown: ConditionalBroadcastReceiver,
        tx_committed_own_headers: Sender<(Round, Vec<Round>)>,
        tx_system_messages: Sender<SystemMessage>,
        randomness_private_key: RandomnessPrivateKey,
        network: anemo::Network,
        beacon_delta: Option<u16>,
        rx_state_request: Receiver<(Round, oneshot::Sender<Option<BlockNumHash>>)>,
    ) -> Self {
        let rx_shutdown_stream = BroadcastStream::new(rx_shutdown.receiver);
        Self {
            authority_id,
            rx_committed_certificates,
            rx_shutdown_stream,
            tx_committed_own_headers,
            tx_system_messages,
            randomness_state: RandomnessState::try_new(
                chain,
                committee,
                randomness_private_key,
                beacon_delta,
            ),
            network,
            updates_queue: Default::default(),
            pending_update: None,
            rx_state_request,
            execution_state: lru::LruCache::new(NonZero::new(DEFAULT_EXECUTION_RESULT_SIZE))
        }
    }

    fn handle_sequenced(&mut self, commit_round: Round, certificates: Vec<Certificate>) {
        // filter own blocks that were committed
        let own_rounds_committed: Vec<_> = certificates
            .iter()
            .filter_map(|cert| {
                if cert.header().author() == self.authority_id {
                    Some(cert.header().round())
                } else {
                    None
                }
            })
            .collect();
        debug!(target: "primary::state_handler", "Own committed rounds {:?} at round {:?}", own_rounds_committed, commit_round);

        let (tx, rx) = oneshot::channel();
        // spawn async task to handle sequence update
        tokio::task::spawn(async move {
            // report headers to the proposer
            let _ = self.tx_committed_own_headers.send((commit_round, own_rounds_committed)).await;
        });

        // Process committed system messages.
        if let Some(randomness_state) = self.randomness_state.as_mut() {
            for certificate in certificates {
                let header = certificate.header();
                for message in header.system_messages() {
                    match message {
                        SystemMessage::DkgMessage(msg) => randomness_state.add_message(msg.clone()),
                        SystemMessage::DkgConfirmation(conf) => {
                            randomness_state.add_confirmation(conf.clone())
                        }
                    }
                }
                // Advance the random beacon protocol if possible after each certificate.
                // TODO: Implement/audit crash recovery for random beacon.
                randomness_state.advance(&self.tx_system_messages).await;
            }
        }
    }

    async fn run(mut self) {
        info!("StateHandler on node {} has started successfully.", self.authority_id);

        // Kick off randomness DKG if enabled.
        if let Some(ref randomness_state) = self.randomness_state {
            randomness_state.start_dkg(&self.tx_system_messages).await;
        }

        loop {
            tokio::select! {
                Some((commit_round, certificates)) = self.rx_committed_certificates.recv() => {
                    self.handle_sequenced(commit_round, certificates).await;
                },

                //
                // new idea:
                // track: watch.changed()
                // - last seen BlockNumHash
                // - LRU with HashMap<Round, BlockNumHash>

                // _ = self.rx_shutdown.receiver.recv() => {
                //     // shutdown network
                //     let _ = self.network.shutdown().await.tap_err(|err|{
                //         error!("Error while shutting down network: {err}")
                //     });

                //     warn!("Network has shutdown");

                //     return;
                // }
            }
        }
    }
}

impl Future for StateHandler {
    type Output = StateHandlerResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            // check for shutdown signal
            //
            // okay to shutdown here because other primary tasks are expected to shutdown too
            // ie) no point completing the proposal if certifier is down
            if let Poll::Ready(Some(_shutdown)) = this.rx_shutdown_stream.poll_next_unpin(cx) {
                // shutdown network
                let network = this.network.clone();

                // attempt to shutdown network
                tokio::task::spawn(async move {
                    let _ = network
                        .shutdown()
                        .await
                        .tap_err(|err| error!(target: "primary::state_handler", "Error while shutting down network: {err}"));
                });

                debug!(target: "primary::state_handler", "State handler shutting down");

                return Poll::Ready(Ok(()));
            }

            // respond to any requests for execution state
            while let Poll::Ready(Some((round, reply))) = this.rx_state_request.poll_recv(cx) {
                // trying to validate header

                // spawn a task for request
                // this could take a while if waiting for execution to complete

                tokio::spawn(async move {

                })
                todo!()
            }

            // poll receiver that returns when state update is complete
            //
            // if the update isn't complete, break loop and return pending
            if let Some(mut receiver) = this.pending_update.take() {
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        todo!()
                    }
                    Poll::Pending => {
                        todo!()
                    }
                }
            }

            if let Poll::Ready(Some((commit_round, certificates))) =
                this.rx_committed_certificates.poll_recv(cx)
            {
                this.handle_sequenced(commit_round, certificates);
            };

            // TODO: remove this break
            break;
        }
        Poll::Pending
    }
}
