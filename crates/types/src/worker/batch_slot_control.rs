//! Shared batch admission snapshots and the execution-to-consensus durability bridge.

use super::{
    BatchBucket, BatchSlotAuthorization, BatchSlotError, BatchSlotPosition, BatchSlotTransition,
    BatchSlotVoteStore, BatchSlots, SignedBatchSlotRecord,
};
use crate::{BlsPublicKey, Database, B256};
use futures::StreamExt;
use parking_lot::RwLock;
use std::{
    collections::BTreeMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

/// One node-wide admission handle, shared across execution, workers and transaction pools.
#[derive(Clone, Debug, Default)]
pub struct BatchSlotControl(Arc<RwLock<ControlState>>);

/// Published state and its epoch-scoped durable history writer.
#[derive(Debug, Default)]
struct ControlState {
    /// Absence retains the legacy protocol before coordinated activation.
    session: Option<SlotSession>,
    /// Local committee identity, absent on nodes that only forward transactions.
    authority: Option<BlsPublicKey>,
    /// Local scheduling information, bounded by the committee's sender buckets.
    local: BTreeMap<BatchBucket, LocalSlot>,
}

/// A canonical snapshot together with its live persistence channel.
#[derive(Debug)]
struct SlotSession {
    /// Snapshot published only after execution and authorization history are durable.
    slots: Arc<BatchSlots>,
    /// Bounded bridge from the blocking execution thread to consensus storage.
    commits: mpsc::Sender<BatchSlotCommit>,
}

/// Scheduling metadata that never decides consensus ordering.
#[derive(Debug)]
struct LocalSlot {
    /// Canonical position to which this metadata applies.
    position: BatchSlotPosition,
    /// Local monotonic time at which this position was published.
    opened: Instant,
    /// A local worker already reserved a proposal for this position.
    proposed: bool,
    /// Pending transactions or a peer retry establish demand for this bucket.
    demanded: bool,
    /// A local timeout has already been submitted for this position.
    timed_out: bool,
}

/// A private output candidate, invisible to producers until both durability barriers finish.
#[derive(Debug)]
pub struct BatchSlotOutput {
    /// Published snapshot preceding the output, used to capture real retry authorizations.
    previous: Arc<BatchSlots>,
    /// Ordered candidate state, including slots awaiting execution finalization.
    candidate: BatchSlots,
    /// Digest of the consensus output being processed.
    output: B256,
    /// Authorizations of selected slots, retained for delayed header certification.
    closed: BTreeMap<BatchBucket, BatchSlotAuthorization>,
    /// Whether this output changes protocol state and therefore needs a durable execution anchor.
    changed: bool,
}

/// Execution's request to persist historical authorizations before publishing new slots.
#[derive(Debug)]
pub struct BatchSlotCommit {
    /// Fully executed candidate and its pre-output snapshot.
    output: BatchSlotOutput,
    /// Reply delivered only after durable publication, or with a terminal failure.
    reply: oneshot::Sender<Result<(), BatchSlotControlError>>,
}

impl BatchSlotControl {
    /// Read the currently published admission state, or legacy mode before activation.
    pub fn snapshot(&self) -> Option<Arc<BatchSlots>> {
        self.0.read().session.as_ref().map(|session| session.slots.clone())
    }

    /// Whether this node can sign retry votes and proposals in the installed committee.
    pub fn is_validator(&self) -> bool {
        self.0.read().authority.is_some()
    }

    /// Install reconstructed canonical epoch state before starting any voting workers.
    ///
    /// The caller initializes the epoch's vote store and replays canonical slot transitions
    /// before calling this method. The receiver must be served for the entire epoch.
    pub fn install(
        &self,
        slots: BatchSlots,
        authority: Option<BlsPublicKey>,
    ) -> mpsc::Receiver<BatchSlotCommit> {
        let (commits, receiver) = mpsc::channel(1);
        let mut state = self.0.write();
        state.local.clear();
        state.authority = authority;
        state.refresh_local(&slots);
        state.session = Some(SlotSession { slots: Arc::new(slots), commits });
        receiver
    }

    /// Select the legacy protocol at an epoch boundary before any workers start.
    pub fn disable(&self) {
        *self.0.write() = ControlState::default();
    }

    /// Return whether a local worker can propose this still-current, unreserved slot.
    pub fn can_propose(&self, position: BatchSlotPosition) -> bool {
        let state = self.0.read();
        state.authority.as_ref().is_some_and(|authority| {
            state.session.as_ref().is_some_and(|session| {
                session.slots.producer(position).ok() == Some(authority)
                    && state
                        .local
                        .get(&position.bucket())
                        .is_some_and(|local| local.position == position && !local.proposed)
            })
        })
    }

    /// Remember a durable local reservation so other workers do not rebuild the slot.
    pub fn proposed(&self, position: BatchSlotPosition) {
        if let Some(local) = self.0.write().local.get_mut(&position.bucket()) {
            if local.position == position {
                local.proposed = true;
                local.demanded = true;
            }
        }
    }

    /// Record pending work or an authenticated peer retry, with committee-bounded memory.
    pub fn demand(&self, bucket: BatchBucket) {
        if let Some(local) = self.0.write().local.get_mut(&bucket) {
            local.demanded = true;
        }
    }

    /// Choose a demanded retry after a growing local delay, for worker zero to submit.
    ///
    /// Time only permits signing a retry vote. An ordered quorum remains necessary to advance
    /// the view. Increasing the wait across retries lets a synchronous period outlast a delay.
    pub fn retry_position(&self) -> Option<BatchSlotPosition> {
        let state = self.0.read();
        state.authority.as_ref().and_then(|_| {
            state.local.values().find_map(|local| {
                let seconds = local.position.view().value().saturating_add(1).saturating_mul(2);
                (local.demanded
                    && !local.timed_out
                    && local.opened.elapsed() >= Duration::from_secs(seconds))
                .then_some(local.position)
            })
        })
    }

    /// Mark a timeout only after quorum dissemination succeeds, allowing transport retries.
    pub fn timeout_submitted(&self, position: BatchSlotPosition) {
        if let Some(local) = self.0.write().local.get_mut(&position.bucket()) {
            if local.position == position {
                local.timed_out = true;
            }
        }
    }

    /// Start an isolated candidate for one consensus output.
    pub fn prepare(&self, output: B256) -> Option<BatchSlotOutput> {
        self.snapshot().map(|previous| BatchSlotOutput::new(previous, output))
    }

    /// Commit an executed candidate from the engine's blocking thread.
    ///
    /// This must follow durable persistence of the entire execution output. Dropped receivers,
    /// storage failures and concurrent epoch replacement fail closed, without opening new slots.
    pub fn commit_blocking(&self, output: BatchSlotOutput) -> Result<(), BatchSlotControlError> {
        output
            .candidate
            .buckets()
            .try_for_each(|bucket| output.candidate.position(bucket).map(|_| ()))
            .map_err(BatchSlotControlError::Protocol)?;
        let commits = self
            .0
            .read()
            .session
            .as_ref()
            .map(|session| session.commits.clone())
            .ok_or(BatchSlotControlError::Unavailable)?;
        let (reply, receive) = oneshot::channel();
        commits
            .blocking_send(BatchSlotCommit { output, reply })
            .map_err(|_| BatchSlotControlError::Unavailable)?;
        receive.blocking_recv().map_err(|_| BatchSlotControlError::Unavailable)?
    }

    /// Serve execution commits until controller replacement closes the channel.
    ///
    /// This service must outlive epoch worker teardown, which can precede the final output drain.
    pub async fn serve<DB: Database>(
        &self,
        store: BatchSlotVoteStore<DB>,
        receiver: mpsc::Receiver<BatchSlotCommit>,
    ) {
        let store = &store;
        futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|request| (request, receiver))
        })
        .for_each(|request| async move {
            let authorizations: Vec<_> = request.output.closed.values().cloned().collect();
            let result = store
                .publish_authorizations(&authorizations)
                .await
                .map_err(BatchSlotControlError::Storage)
                .and_then(|()| self.publish(request.output));
            let _ = request.reply.send(result);
        })
        .await;
    }

    /// Publish a candidate only if its predecessor remains the node's current snapshot.
    fn publish(&self, output: BatchSlotOutput) -> Result<(), BatchSlotControlError> {
        let mut state = self.0.write();
        if state
            .session
            .as_ref()
            .is_some_and(|session| Arc::ptr_eq(&session.slots, &output.previous))
        {
            state.refresh_local(&output.candidate);
            if let Some(session) = state.session.as_mut() {
                session.slots = Arc::new(output.candidate);
            }
            Ok(())
        } else {
            Err(BatchSlotControlError::Replaced)
        }
    }
}

impl ControlState {
    /// Reset local scheduling only for slots whose canonical position changed.
    fn refresh_local(&mut self, slots: &BatchSlots) {
        slots.buckets().filter_map(|bucket| slots.position(bucket).ok()).for_each(|position| {
            let local = self.local.entry(position.bucket()).or_insert_with(|| LocalSlot {
                position,
                opened: Instant::now(),
                proposed: false,
                demanded: false,
                timed_out: false,
            });
            if local.position != position {
                *local = LocalSlot {
                    position,
                    opened: Instant::now(),
                    proposed: false,
                    demanded: local.demanded,
                    timed_out: false,
                };
            }
        });
    }
}

impl BatchSlotOutput {
    /// Construct the same private candidate for live execution and canonical recovery.
    fn new(previous: Arc<BatchSlots>, output: B256) -> Self {
        Self {
            candidate: (*previous).clone(),
            previous,
            output,
            closed: BTreeMap::new(),
            changed: false,
        }
    }

    /// Reconstruct one already durable output and its closed-slot authorizations.
    ///
    /// The caller must obtain the records in consensus order and the final execution hash
    /// from canonical storage. This does not authorize a speculative or merely certified output.
    pub fn recover<'a>(
        previous: BatchSlots,
        output: B256,
        records: impl IntoIterator<Item = &'a SignedBatchSlotRecord>,
        execution: B256,
    ) -> Result<(BatchSlots, Vec<BatchSlotAuthorization>), BatchSlotError> {
        let mut recovered = Self::new(Arc::new(previous), output);
        records.into_iter().try_for_each(|record| recovered.apply(record).map(|_| ()))?;
        recovered.finalize(execution)?;
        Ok((recovered.candidate, recovered.closed.into_values().collect()))
    }

    /// Apply an authenticated, fully validated record in its original consensus order.
    pub fn apply(
        &mut self,
        record: &SignedBatchSlotRecord,
    ) -> Result<BatchSlotTransition, BatchSlotError> {
        let position = record.message().position();
        let published = self.previous.position(position.bucket())?;
        if position.sequence() >= published.sequence() {
            // A retry advanced inside this output was not yet available for voting. Only
            // positions authorized by the preceding published snapshot can select a proposal.
            self.previous.vote(record)?;
        }
        let transition = self.candidate.apply(record, self.output)?;
        if transition == BatchSlotTransition::Selected {
            let bucket = record.message().position().bucket();
            let authorization = self.previous.authorization(bucket)?;
            self.closed.insert(bucket, authorization);
        }
        self.changed |= transition != BatchSlotTransition::Unchanged;
        Ok(transition)
    }

    /// Whether even a transaction-free output requires an execution block to anchor its state.
    pub fn changed(&self) -> bool {
        self.changed
    }

    /// Read the admission state preceding this output for pinned transaction validation.
    pub fn previous(&self) -> &BatchSlots {
        &self.previous
    }

    /// Open selected successors only after the output's final execution hash is known.
    pub fn finalize(&mut self, execution: B256) -> Result<(), BatchSlotError> {
        self.candidate.finalize_openings(self.output, execution)
    }
}

/// A durability bridge failed or no longer matches the current epoch and execution snapshot.
#[derive(Debug)]
pub enum BatchSlotControlError {
    /// A candidate still contains unfinalized execution anchors.
    Protocol(BatchSlotError),
    /// The epoch's history writer is absent or has stopped.
    Unavailable,
    /// Another epoch or execution output replaced the candidate's predecessor.
    Replaced,
    /// Durable consensus history could not be committed.
    Storage(super::BatchSlotVoteStoreError),
}

impl fmt::Display for BatchSlotControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "batch slot publication failed: {self:?}")
    }
}

impl std::error::Error for BatchSlotControlError {}
