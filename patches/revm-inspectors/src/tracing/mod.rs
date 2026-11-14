use crate::{
    opcode::immediate_size,
    tracing::{
        arena::PushTraceKind,
        types::{
            CallKind, CallTraceNode, RecordedMemory, StorageChange, StorageChangeReason,
            TraceMemberOrder,
        },
        utils::gas_used,
    },
};
use alloc::{boxed::Box, format, sync::Arc, vec::Vec};
use core::{
    borrow::Borrow,
    mem,
    sync::atomic::{AtomicUsize, Ordering},
};
use revm::{
    bytecode::opcode::{self, OpCode},
    context::{JournalTr, LocalContextTr},
    context_interface::{ContextError, ContextTr},
    inspector::JournalExt,
    interpreter::{
        interpreter_types::{Immediates, Jumps, LoopControl, ReturnData, RuntimeFlag},
        CallInput, CallInputs, CallOutcome, CallScheme, CreateInputs, CreateOutcome, Gas,
        InstructionResult, Interpreter, InterpreterResult,
    },
    primitives::{hardfork::SpecId, Address, Bytes, Log, B256, U256},
    Inspector, JournalEntry,
};

mod arena;
pub use arena::CallTraceArena;

mod builder;
pub use builder::{
    geth::{self, GethTraceBuilder},
    parity::{self, ParityTraceBuilder},
};

mod config;
pub use config::{
    OpcodeFilter, StackSnapshotType, TracingInspectorConfig, DEFAULT_MAX_RECORDED_BYTES,
};

mod fourbyte;
pub use fourbyte::FourByteInspector;

mod opcount;
pub use opcount::OpcodeCountInspector;

pub mod types;
use types::{CallLog, CallTrace, CallTraceStep};

mod utils;

#[cfg(feature = "std")]
mod writer;
#[cfg(feature = "std")]
pub use writer::{TraceWriter, TraceWriterConfig};

#[cfg(feature = "js-tracer")]
pub mod js;

mod mux;
pub use mux::{Error as MuxError, MuxInspector};

mod debug;
pub use debug::{DebugInspector, DebugInspectorError};

/// An inspector that collects call traces.
///
/// This [Inspector] can be hooked into revm's EVM which then calls the inspector
/// functions, such as [Inspector::call] or [Inspector::call_end].
///
/// The [TracingInspector] keeps track of everything by:
///   1. start tracking steps/calls on [Inspector::step] and [Inspector::call]
///   2. complete steps/calls on [Inspector::step_end] and [Inspector::call_end]
#[derive(Clone, Debug, Default)]
pub struct TracingInspector {
    /// Configures what and how the inspector records traces.
    config: TracingInspectorConfig,
    /// Records all call traces
    traces: CallTraceArena,
    /// Tracks active calls
    trace_stack: Vec<usize>,
    /// Tracks whether the next `step_end` should be recorded. Set in `start_step`.
    record_step_end: bool,
    /// Tracks the return value of the last call
    last_call_return_data: Option<Bytes>,
    /// Tracks the journal len in the step, used in step_end to check if the journal has changed
    last_journal_len: usize,
    /// The spec id of the EVM.
    ///
    /// This is filled during execution.
    spec_id: Option<SpecId>,
    /// Pool of reusable _empty_ step vectors to reduce allocations.
    ///
    /// All `Vec<CallTraceStep>` are always empty but may have capacity.
    reusable_step_vecs: Vec<Vec<CallTraceStep>>,
    /// TN(TN-0079, TN-0153): the bytes the trace holds (its call traces with their input, init
    /// code and output, and its recorded steps), checked against
    /// [`TracingInspectorConfig::max_recorded_bytes`]. Kept across [`Self::fuse`], and shared
    /// by the inspectors of one request through [`Self::share_recorded_bytes`], so the cap
    /// bounds the request's trace, not one transaction's.
    recorded_bytes: Arc<AtomicUsize>,
}

impl TracingInspector {
    /// Returns a new instance for the given config
    pub fn new(config: TracingInspectorConfig) -> Self {
        Self { config, ..Default::default() }
    }

    /// Resets the inspector to its initial state of [Self::new].
    /// This makes the inspector ready to be used again.
    ///
    /// Note that this method has no effect on the allocated capacity of the vector.
    #[inline]
    pub fn fuse(&mut self) {
        let Self {
            traces,
            trace_stack,
            last_call_return_data,
            last_journal_len,
            spec_id,
            record_step_end,
            // kept
            config,
            reusable_step_vecs,
            recorded_bytes: _,
        } = self;

        // if we record steps we can reuse the individual calltracestep vecs
        if config.record_steps {
            for node in &mut traces.arena {
                // move out and store the reusable steps vec
                let mut steps = mem::take(&mut node.trace.steps);
                // ensure steps are cleared
                steps.clear();
                reusable_step_vecs.push(steps);
            }
        }

        traces.clear();
        trace_stack.clear();
        last_call_return_data.take();
        spec_id.take();
        *last_journal_len = 0;
        *record_step_end = false;
    }

    /// Resets the inspector to it's initial state of [Self::new].
    #[inline]
    pub fn fused(mut self) -> Self {
        self.fuse();
        self
    }

    /// TN(TN-0079): counts this inspector's recorded bytes in `counter`, so that several
    /// inspectors (one per transaction of a block, say) share one cap.
    pub fn share_recorded_bytes(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.recorded_bytes = counter;
        self
    }

    /// TN(TN-0079, TN-0153): the bytes the trace holds so far, over every inspector sharing
    /// the counter.
    pub fn recorded_bytes(&self) -> usize {
        self.recorded_bytes.load(Ordering::Relaxed)
    }

    /// TN(TN-0079, TN-0153): counts `bytes` more; past the cap, sets the `trace too large`
    /// context error, which ends the transaction as TN-0017's interrupt does: revm returns it
    /// as `EVMError::Custom` when the frame or the transaction ends.
    #[inline]
    fn charge<CTX: ContextTr>(&self, context: &mut CTX, bytes: usize) -> bool {
        let total = self.recorded_bytes.fetch_add(bytes, Ordering::Relaxed).saturating_add(bytes);
        let Some(max) = self.config.max_recorded_bytes else { return true };
        if total <= max {
            return true;
        }
        if context.error().is_ok() {
            *context.error() = Err(ContextError::Custom(format!(
                "trace too large: more than {max} bytes of calls and opcode steps recorded; \
                 trace a smaller transaction, or for opcode steps set disableStack and leave \
                 out enableMemory or vmTrace"
            )));
        }
        false
    }

    /// TN(TN-0079): [`Self::charge`]s a step's `bytes`; past the cap, also halts the
    /// interpreter, which then cannot carry on past a pending action.
    #[inline]
    fn record_bytes<CTX: ContextTr>(
        &self,
        interp: &mut Interpreter,
        context: &mut CTX,
        bytes: usize,
    ) -> bool {
        if self.charge(context, bytes) {
            return true;
        }
        interp.bytecode.action().take();
        interp.bytecode.reset_action();
        interp.halt_fatal();
        false
    }

    /// TN(TN-0172): the struct logger's frame, as [`GethTraceBuilder::geth_traces`] builds it,
    /// with each log's [`geth::struct_log_copies`] (its memory as hex, its storage copy) added
    /// to the recorded-bytes counter as the log is built. Past the cap the conversion stops and
    /// this returns the `trace too large` message.
    pub fn try_geth_traces(
        &self,
        receipt_gas_used: u64,
        return_value: Bytes,
        opts: alloy_rpc_types_trace::geth::GethDefaultTracingOptions,
    ) -> Result<alloy_rpc_types_trace::geth::DefaultFrame, String> {
        let max = self.config.max_recorded_bytes;
        self.geth_builder()
            .geth_traces_charged(receipt_gas_used, return_value, opts, &mut |bytes| {
                let total =
                    self.recorded_bytes.fetch_add(bytes, Ordering::Relaxed).saturating_add(bytes);
                max.is_none_or(|max| total <= max)
            })
            .ok_or_else(|| {
                format!(
                    "trace too large: more than {} bytes of calls, opcode steps and struct logs; \
                     trace a smaller transaction, or set disableStorage and leave out \
                     enableMemory",
                    max.unwrap_or_default()
                )
            })
    }

    /// TN(TN-0153): the result of a call or create refused at the cap, the one TN-0017's
    /// interrupt returns. The context error [`Self::charge`] set ends the transaction before
    /// revm hands this result to the calling frame.
    fn trace_too_large(gas_limit: u64) -> InterpreterResult {
        InterpreterResult::new(
            InstructionResult::FatalExternalError,
            Bytes::new(),
            Gas::new(gas_limit),
        )
    }

    /// Returns the config of the inspector.
    pub const fn config(&self) -> &TracingInspectorConfig {
        &self.config
    }

    /// Returns a mutable reference to the config of the inspector.
    pub fn config_mut(&mut self) -> &mut TracingInspectorConfig {
        &mut self.config
    }

    /// Updates the config of the inspector.
    pub fn update_config(
        &mut self,
        f: impl FnOnce(TracingInspectorConfig) -> TracingInspectorConfig,
    ) {
        self.config = f(self.config);
    }

    /// Gets a reference to the recorded call traces.
    pub const fn traces(&self) -> &CallTraceArena {
        &self.traces
    }

    #[doc(hidden)]
    #[deprecated = "use `traces` instead"]
    pub const fn get_traces(&self) -> &CallTraceArena {
        &self.traces
    }

    /// Gets a mutable reference to the recorded call traces.
    pub fn traces_mut(&mut self) -> &mut CallTraceArena {
        &mut self.traces
    }

    #[doc(hidden)]
    #[deprecated = "use `traces_mut` instead"]
    pub fn get_traces_mut(&mut self) -> &mut CallTraceArena {
        &mut self.traces
    }

    /// Consumes the inspector and returns the recorded call traces.
    pub fn into_traces(self) -> CallTraceArena {
        self.traces
    }

    /// Manually set the gas used of the root trace.
    ///
    /// This is useful if the root trace's gasUsed should mirror the actual gas used by the
    /// transaction.
    ///
    /// This allows setting it manually by consuming the execution result's gas for example.
    #[inline]
    pub fn set_transaction_gas_used(&mut self, gas_used: u64) {
        if let Some(node) = self.traces.arena.first_mut() {
            node.trace.gas_used = gas_used;
        }
    }

    /// Manually set the gas limit of the debug root trace.
    ///
    /// This is useful if the debug root trace's gasUsed should mirror the actual gas used by the
    /// transaction.
    ///
    /// This allows setting it manually by consuming the execution result's gas for example.
    #[inline]
    pub fn set_transaction_gas_limit(&mut self, gas_limit: u64) {
        if let Some(node) = self.traces.arena.first_mut() {
            node.trace.gas_limit = gas_limit;
        }
    }

    /// Convenience function for [ParityTraceBuilder::set_transaction_gas_used] that consumes the
    /// type.
    #[inline]
    pub fn with_transaction_gas_used(mut self, gas_used: u64) -> Self {
        self.set_transaction_gas_used(gas_used);
        self
    }

    /// Work with [TracingInspector::set_transaction_gas_limit] function
    #[inline]
    pub fn with_transaction_gas_limit(mut self, gas_limit: u64) -> Self {
        self.set_transaction_gas_limit(gas_limit);
        self
    }

    /// Manually set the caller address of the root trace.
    ///
    /// This is useful for custom transaction types (e.g. account abstraction batches) where the
    /// EVM's call entry point may not reflect the actual transaction sender.
    #[inline]
    pub fn set_transaction_caller(&mut self, caller: Address) {
        if let Some(node) = self.traces.arena.first_mut() {
            node.trace.caller = caller;
        }
    }

    /// Consumes the Inspector and returns a [ParityTraceBuilder].
    #[inline]
    pub fn into_parity_builder(self) -> ParityTraceBuilder {
        ParityTraceBuilder::new(self.traces.arena, self.spec_id, self.config)
    }

    /// Consumes the Inspector and returns a [GethTraceBuilder].
    #[inline]
    pub fn into_geth_builder(self) -> GethTraceBuilder<'static> {
        GethTraceBuilder::new(self.traces.arena)
    }

    /// Returns the  [GethTraceBuilder] for the recorded traces without consuming the type.
    ///
    /// This can be useful for multiple transaction tracing (block) where this inspector can be
    /// reused for each transaction but caller must ensure that the traces are cleared before
    /// starting a new transaction: [`Self::fuse`]
    #[inline]
    pub fn geth_builder(&self) -> GethTraceBuilder<'_> {
        GethTraceBuilder::new_borrowed(&self.traces.arena)
    }

    /// Returns true if we're no longer in the context of the root call.
    fn is_deep(&self) -> bool {
        // the root call will always be the first entry in the trace stack
        !self.trace_stack.is_empty()
    }

    /// Returns how many logs we already recorded.
    fn log_count(&self) -> usize {
        self.traces.nodes().iter().map(|trace| trace.log_count()).sum()
    }

    /// Returns true if this a call to a precompile contract.
    ///
    /// Returns true if the `to` address is a precompile contract and the value is zero.
    #[inline]
    fn is_precompile_call<CTX: ContextTr<Journal: JournalExt>>(
        &self,
        context: &CTX,
        to: &Address,
        value: &U256,
    ) -> bool {
        if context.journal_ref().precompile_addresses().contains(to) {
            // only if this is _not_ the root call
            return self.is_deep() && value.is_zero();
        }
        false
    }

    /// Returns the currently active call trace.
    ///
    /// This will be the last call trace pushed to the stack: the call we entered most recently.
    #[track_caller]
    #[inline]
    fn active_trace(&self) -> Option<&CallTraceNode> {
        self.trace_stack.last().map(|idx| &self.traces.arena[*idx])
    }

    /// Returns the last trace [CallTrace] index from the stack.
    ///
    /// This will be the currently active call trace.
    ///
    /// # Panics
    ///
    /// If no [CallTrace] was pushed
    #[track_caller]
    #[inline]
    fn last_trace_idx(&self) -> usize {
        self.trace_stack.last().copied().expect("can't start step without starting a trace first")
    }

    /// Returns a mutable reference to the last trace [CallTrace] from the stack.
    #[track_caller]
    fn last_trace(&mut self) -> &mut CallTraceNode {
        let idx = self.last_trace_idx();
        &mut self.traces.arena[idx]
    }

    /// _Removes_ the last trace [CallTrace] index from the stack.
    ///
    /// # Panics
    ///
    /// If no [CallTrace] was pushed
    #[track_caller]
    #[inline]
    fn pop_trace_idx(&mut self) -> usize {
        self.trace_stack.pop().expect("more traces were filled than started")
    }

    /// Starts tracking a new trace.
    ///
    /// Invoked on [Inspector::call].
    #[allow(clippy::too_many_arguments)]
    fn start_trace_on_call<CTX: ContextTr>(
        &mut self,
        context: &mut CTX,
        address: Address,
        input_data: Bytes,
        value: U256,
        kind: CallKind,
        caller: Address,
        gas_limit: u64,
        maybe_precompile: Option<bool>,
    ) {
        // This will only be true if the inspector is configured to exclude precompiles and the call
        // is to a precompile
        let push_kind = if maybe_precompile.unwrap_or(false) {
            // We don't want to track precompiles
            PushTraceKind::PushOnly
        } else {
            PushTraceKind::PushAndAttachToParent
        };

        // find an empty steps vec or create a new one
        let steps = self.reusable_step_vecs.pop().unwrap_or_default();

        self.trace_stack.push(self.traces.push_trace(
            0,
            push_kind,
            CallTrace {
                depth: context.journal().depth(),
                address,
                kind,
                data: input_data,
                value,
                status: None,
                caller,
                maybe_precompile,
                gas_limit,
                steps,
                ..Default::default()
            },
        ));
    }

    /// Fills the current trace with the outcome of a call.
    ///
    /// Invoked on [Inspector::call_end].
    ///
    /// # Panics
    ///
    /// This expects an existing trace [Self::start_trace_on_call]
    fn fill_trace_on_call_end<CTX: ContextTr>(
        &mut self,
        context: &mut CTX,
        result: &InterpreterResult,
        created_address: Option<Address>,
    ) {
        let InterpreterResult { result, ref output, ref gas } = *result;

        // TN(TN-0153): the trace keeps the output, so it counts towards the cap; past the cap
        // the trace keeps none and the context error ends the transaction.
        let charged = self.charge(context, output.len());

        let trace_idx = self.pop_trace_idx();
        let trace = &mut self.traces.arena[trace_idx].trace;

        trace.gas_used = gas.spent();
        trace.gas_refund_counter = gas.refunded().max(0) as u64;

        trace.status = Some(result);
        trace.success = trace.status.is_some_and(|status| status.is_ok());
        trace.output = if charged { output.clone() } else { Bytes::new() };

        self.last_call_return_data = Some(output.clone());

        if let Some(address) = created_address {
            // A new contract was created via CREATE
            trace.address = address;
        }
    }

    /// Starts tracking a step
    ///
    /// Invoked on [Inspector::step]
    ///
    /// # Panics
    ///
    /// This expects an existing [CallTrace], in other words, this panics if not within the context
    /// of a call.
    #[cold]
    fn start_step<CTX: ContextTr<Journal: JournalExt>>(
        &mut self,
        interp: &mut Interpreter,
        context: &mut CTX,
    ) {
        // We always want an OpCode, even it is unknown because it could be an additional opcode
        // that not a known constant.
        let op = unsafe { OpCode::new_unchecked(interp.bytecode.opcode()) };

        let record = self.config.should_record_opcode(op);
        self.record_step_end = record;
        if !record {
            return;
        }

        let trace_idx = self.last_trace_idx();
        let node = &mut self.traces.arena[trace_idx];

        // Reuse the memory from the previous step if:
        // - there is not opcode filter -- in this case we cannot rely on the order of steps
        // - it exists and has not modified memory
        // TN(TN-0079): only a fresh copy counts towards the cap; a reused snapshot is shared.
        let mut memory_bytes = 0;
        let memory = self.config.record_memory_snapshots.then(|| {
            if self.config.record_opcodes_filter.is_none() {
                if let Some(prev) = node.trace.steps.last() {
                    if !prev.op.modifies_memory() {
                        if let Some(memory) = &prev.memory {
                            return memory.clone();
                        }
                    }
                }
            }
            let memory = RecordedMemory::new(&interp.memory.borrow().context_memory());
            memory_bytes = memory.len();
            memory
        });

        let stack = if self.config.record_stack_snapshots.is_all()
            || self.config.record_stack_snapshots.is_full()
        {
            Some(interp.stack.data().as_slice().into())
        } else {
            None
        };
        let returndata = if self.config.record_returndata_snapshots {
            interp.return_data.buffer().clone()
        } else {
            Bytes::new()
        };

        let gas_used = gas_used(
            interp.runtime_flag.spec_id(),
            interp.gas.spent(),
            interp.gas.refunded() as u64,
        );

        let mut immediate_bytes = None;
        if self.config.record_immediate_bytes {
            let size = immediate_size(&interp.bytecode);
            if size != 0 {
                immediate_bytes = Some(Bytes::copy_from_slice(
                    &interp.bytecode.read_slice(size as usize + 1)[1..],
                ));
            }
        }

        // TN(TN-0079): the step and what it copies; `step_end`'s pushed values are counted
        // here from the opcode's output count. Past the cap the step is not recorded and the
        // transaction halts.
        let pushed_bytes = if self.config.record_stack_snapshots.is_all()
            || self.config.record_stack_snapshots.is_pushes()
        {
            op.outputs() as usize * mem::size_of::<U256>()
        } else {
            0
        };
        let step_bytes = mem::size_of::<CallTraceStep>()
            + stack.as_ref().map_or(0, |stack: &Box<[U256]>| stack.len() * mem::size_of::<U256>())
            + memory_bytes
            + returndata.len()
            + immediate_bytes.as_ref().map_or(0, |bytes| bytes.len())
            + pushed_bytes;
        if !self.record_bytes(interp, context, step_bytes) {
            self.record_step_end = false;
            return;
        }
        let node = &mut self.traces.arena[trace_idx];

        self.last_journal_len = context.journal_ref().journal().len();

        let step_idx = node.trace.steps.len();
        node.trace.steps.push(CallTraceStep {
            pc: interp.bytecode.pc(),
            op,
            stack,
            memory,
            returndata,
            gas_remaining: interp.gas.remaining(),
            gas_refund_counter: interp.gas.refunded() as u64,
            gas_used,
            immediate_bytes,

            // These fields will be populated in `step_end`.
            push_stack: None,
            gas_cost: 0,
            storage_change: None,
            status: None,

            // This is never populated in `TracingInspector`.
            decoded: None,
        });

        node.ordering.push(TraceMemberOrder::Step(step_idx));
    }

    /// Fills the current trace with the output of a step.
    ///
    /// Invoked on [Inspector::step_end].
    #[cold]
    fn fill_step_on_step_end<CTX: ContextTr<Journal: JournalExt>>(
        &mut self,
        interp: &mut Interpreter,
        context: &mut CTX,
    ) {
        // No need to reset here, since it is only read here and it will be overwritten by the next
        // step.
        if !self.record_step_end {
            return;
        }

        let trace_idx = self.last_trace_idx();
        let node = &mut self.traces.arena[trace_idx];
        let step = node.trace.steps.last_mut().unwrap();

        // See comments in `start_step`.
        debug_assert!(
            step.push_stack.is_none()
                && step.gas_cost == 0
                && step.storage_change.is_none()
                && step.status.is_none()
                && step.decoded.is_none(),
            "step in step_end is already filled: {trace_idx} -> {step:#?}",
        );

        if self.config.record_stack_snapshots.is_all()
            || self.config.record_stack_snapshots.is_pushes()
        {
            // this can potentially underflow if the stack is malformed
            let start = interp.stack.len().saturating_sub(step.op.outputs() as usize);
            step.push_stack = Some(interp.stack.data()[start..].into());
        }

        let journal = context.journal_ref().journal();

        // If journal has not changed, there is no state change to be recorded.
        if self.config.record_state_diff && journal.len() != self.last_journal_len {
            let op = step.op.get();

            step.storage_change = if matches!(op, opcode::SLOAD | opcode::SSTORE) {
                let reason = match op {
                    opcode::SLOAD => StorageChangeReason::SLOAD,
                    opcode::SSTORE => StorageChangeReason::SSTORE,
                    _ => unreachable!(),
                };

                match journal.last() {
                    Some(JournalEntry::StorageChanged { address, key, had_value }) => {
                        // SAFETY: (Address,key) exists if part if StorageChange
                        let value =
                            context.journal_ref().evm_state()[address].storage[key].present_value();
                        let change =
                            StorageChange { key: *key, value, had_value: Some(*had_value), reason };
                        Some(Box::new(change))
                    }
                    Some(JournalEntry::StorageWarmed { key, address }) => {
                        // SAFETY: (Address,key) exists if part if StorageChange
                        let value =
                            context.journal_ref().evm_state()[address].storage[key].present_value();
                        let change = StorageChange { key: *key, value, had_value: None, reason };
                        Some(Box::new(change))
                    }
                    _ => None,
                }
            } else {
                None
            };
        }

        // The gas cost is the difference between the recorded gas remaining at the start of the
        // step the remaining gas here, at the end of the step.
        // TODO: Figure out why this can overflow. https://github.com/paradigmxyz/revm-inspectors/pull/38
        step.gas_cost = step.gas_remaining.saturating_sub(interp.gas.remaining());

        // set the status
        step.status = interp.bytecode.action().as_ref().and_then(|i| i.instruction_result())
    }
}

impl<CTX> Inspector<CTX> for TracingInspector
where
    CTX: ContextTr<Journal: JournalExt>,
{
    #[inline]
    fn step(&mut self, interp: &mut Interpreter, context: &mut CTX) {
        if self.config.record_steps {
            self.start_step(interp, context);
        }
    }

    #[inline]
    fn step_end(&mut self, interp: &mut Interpreter, context: &mut CTX) {
        if self.config.record_steps {
            self.fill_step_on_step_end(interp, context);
        }
    }

    fn log(&mut self, _context: &mut CTX, log: Log) {
        if self.config.record_logs {
            // index starts at 0
            let log_count = self.log_count();
            let trace = self.last_trace();
            trace.ordering.push(TraceMemberOrder::Log(trace.logs.len()));
            trace.logs.push(
                CallLog::from(log)
                    .with_position(trace.children.len() as u64)
                    .with_index(log_count as u64),
            );
        }
    }

    fn call(&mut self, context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        // determine correct `from` and `to` based on the call scheme
        let (from, to) = match inputs.scheme {
            CallScheme::DelegateCall | CallScheme::CallCode => {
                (inputs.target_address, inputs.bytecode_address)
            }
            _ => (inputs.caller, inputs.target_address),
        };

        let value = if matches!(inputs.scheme, CallScheme::DelegateCall) {
            // for delegate calls we need to use the value of the top trace
            if let Some(parent) = self.active_trace() {
                parent.trace.value
            } else {
                inputs.call_value()
            }
        } else {
            inputs.call_value()
        };

        // if calls to precompiles should be excluded, check whether this is a call to a precompile
        let maybe_precompile = self
            .config
            .exclude_precompile_calls
            .then(|| self.is_precompile_call(context, &to, &value));

        // TN(TN-0153): the call's trace and its copy of the input count towards the cap. Past
        // the cap the call is not made: it ends with `FatalExternalError`, and the context
        // error ends the transaction. The trace is still pushed, without the input, because
        // `call_end` runs for this outcome too.
        let charged = self.charge(context, mem::size_of::<CallTraceNode>() + inputs.input.len());
        let input = if charged { inputs.input_data(context) } else { Bytes::new() };
        self.start_trace_on_call(
            context,
            to,
            input,
            value,
            inputs.scheme.into(),
            from,
            inputs.gas_limit,
            maybe_precompile,
        );

        (!charged).then(|| {
            CallOutcome::new(
                Self::trace_too_large(inputs.gas_limit),
                inputs.return_memory_offset.clone(),
            )
        })
    }

    fn call_end(&mut self, context: &mut CTX, _inputs: &CallInputs, outcome: &mut CallOutcome) {
        self.fill_trace_on_call_end(context, &outcome.result, None);
    }

    fn create(&mut self, context: &mut CTX, inputs: &mut CreateInputs) -> Option<CreateOutcome> {
        let nonce = context.journal_mut().load_account(inputs.caller()).ok()?.info.nonce;
        // TN(TN-0153): as in `call`, for the init code.
        let charged =
            self.charge(context, mem::size_of::<CallTraceNode>() + inputs.init_code().len());
        self.start_trace_on_call(
            context,
            inputs.created_address(nonce),
            if charged { inputs.init_code().clone() } else { Bytes::new() },
            inputs.value(),
            inputs.scheme().into(),
            inputs.caller(),
            inputs.gas_limit(),
            Some(false),
        );
        (!charged).then(|| CreateOutcome::new(Self::trace_too_large(inputs.gas_limit()), None))
    }

    fn create_end(
        &mut self,
        context: &mut CTX,
        _inputs: &CreateInputs,
        outcome: &mut CreateOutcome,
    ) {
        self.fill_trace_on_call_end(context, &outcome.result, outcome.address);
    }

    fn selfdestruct(&mut self, contract: Address, target: Address, value: U256) {
        let node = self.last_trace();
        node.trace.selfdestruct_address = Some(contract);
        node.trace.selfdestruct_refund_target = Some(target);
        node.trace.selfdestruct_transferred_value = Some(value);
    }
}

/// Contains some contextual infos for a transaction execution that is made available to the JS
/// object.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransactionContext {
    /// Hash of the block the tx is contained within.
    ///
    /// `None` if this is a call.
    pub block_hash: Option<B256>,
    /// Index of the transaction within a block.
    ///
    /// `None` if this is a call.
    pub tx_index: Option<usize>,
    /// Hash of the transaction being traced.
    ///
    /// `None` if this is a call.
    pub tx_hash: Option<B256>,
}

impl TransactionContext {
    /// Sets the block hash.
    pub const fn with_block_hash(mut self, block_hash: B256) -> Self {
        self.block_hash = Some(block_hash);
        self
    }

    /// Sets the index of the transaction within a block.
    pub const fn with_tx_index(mut self, tx_index: usize) -> Self {
        self.tx_index = Some(tx_index);
        self
    }

    /// Sets the hash of the transaction.
    pub const fn with_tx_hash(mut self, tx_hash: B256) -> Self {
        self.tx_hash = Some(tx_hash);
        self
    }
}

impl From<alloy_rpc_types_eth::TransactionInfo> for TransactionContext {
    fn from(tx_info: alloy_rpc_types_eth::TransactionInfo) -> Self {
        Self {
            block_hash: tx_info.block_hash,
            tx_index: tx_info.index.map(|idx| idx as usize),
            tx_hash: tx_info.hash,
        }
    }
}

/// A helper extension trait that _clones_ the input data from the shared mem buffer
pub(crate) trait CallInputExt {
    fn input_data<CTX: ContextTr>(&self, ctx: &mut CTX) -> Bytes;
}

impl CallInputExt for CallInputs {
    fn input_data<CTX: ContextTr>(&self, ctx: &mut CTX) -> Bytes {
        match &self.input {
            CallInput::SharedBuffer(range) => ctx
                .local()
                .shared_memory_buffer_slice(range.clone())
                .map(|slice| Bytes::copy_from_slice(&slice))
                .unwrap_or_default(),
            CallInput::Bytes(bytes) => bytes.clone(),
        }
    }
}

/// TN(TN-0079, TN-0153): the recorded-bytes cap.
#[cfg(test)]
mod tests {
    use super::*;
    use revm::{
        context::TxEnv,
        context_interface::result::EVMError,
        database::CacheDB,
        database_interface::EmptyDB,
        primitives::TxKind,
        state::{AccountInfo, Bytecode},
        InspectEvm, MainBuilder, MainContext,
    };

    const TARGET: Address = Address::with_last_byte(0xaa);
    const CALLEE: Address = Address::with_last_byte(0xbb);

    /// Runs `inspector` over `tx` with `contracts` deployed, and hands the inspector back.
    fn inspect(
        inspector: TracingInspector,
        contracts: &[(Address, Vec<u8>)],
        tx: TxEnv,
    ) -> (TracingInspector, Result<(), String>) {
        let mut db = CacheDB::new(EmptyDB::default());
        for (address, code) in contracts {
            db.insert_account_info(
                *address,
                AccountInfo::default().with_code(Bytecode::new_raw(code.clone().into())),
            );
        }
        let mut evm = revm::Context::mainnet().with_db(db).build_mainnet_with_inspector(inspector);
        let res = match evm.inspect_tx(tx) {
            Ok(_) => Ok(()),
            Err(EVMError::Custom(msg)) => Err(msg),
            Err(err) => panic!("unexpected EVM error: {err:?}"),
        };
        (evm.inspector, res)
    }

    /// A call to [`TARGET`] running `code`, with `gas_limit`.
    fn call_target(
        inspector: TracingInspector,
        contracts: &[(Address, Vec<u8>)],
        gas_limit: u64,
    ) -> (TracingInspector, Result<(), String>) {
        inspect(
            inspector,
            contracts,
            TxEnv { gas_limit, kind: TxKind::Call(TARGET), ..Default::default() },
        )
    }

    /// Runs `inspector` over a call to a contract that loops until it runs out of gas, with a
    /// stack of `depth` words, and hands the inspector back.
    fn trace_loop(
        inspector: TracingInspector,
        depth: u8,
        gas_limit: u64,
    ) -> (TracingInspector, Result<(), String>) {
        // PUSH1 1 (depth times); JUMPDEST; PUSH1 <jumpdest>; JUMP
        let mut code = Vec::new();
        for _ in 0..depth {
            code.extend([0x60, 0x01]);
        }
        let jumpdest = code.len() as u8;
        code.extend([0x5b, 0x60, jumpdest, 0x56]);
        call_target(inspector, &[(TARGET, code)], gas_limit)
    }

    /// Code that STATICCALLs `callee` with `args` bytes of (zero) memory as calldata and all
    /// its gas, in a loop until it runs out of gas. The calls' memory is paid for once.
    fn call_loop(callee: Address, args: u16) -> Vec<u8> {
        // JUMPDEST; retLen 0; retOffset 0; argsLen; argsOffset 0; callee; GAS; STATICCALL;
        // POP; PUSH1 0; JUMP
        let mut code = vec![0x5b, 0x60, 0x00, 0x60, 0x00, 0x61];
        code.extend(args.to_be_bytes());
        code.extend([0x60, 0x00, 0x73]);
        code.extend(callee.as_slice());
        code.extend([0x5a, 0xfa, 0x50, 0x60, 0x00, 0x56]);
        code
    }

    /// Code that returns `len` bytes of (zero) memory.
    fn returns(len: u16) -> Vec<u8> {
        // PUSH2 len; PUSH1 0; RETURN
        let mut code = vec![0x61];
        code.extend(len.to_be_bytes());
        code.extend([0x60, 0x00, 0xf3]);
        code
    }

    /// Code that CREATEs contracts from `len` bytes of (zero) init code in a loop until it runs
    /// out of gas.
    fn create_loop(len: u16) -> Vec<u8> {
        // JUMPDEST; PUSH2 len; offset 0; value 0; CREATE; POP; PUSH1 0; JUMP
        let mut code = vec![0x5b, 0x61];
        code.extend(len.to_be_bytes());
        code.extend([0x60, 0x00, 0x60, 0x00, 0xf0, 0x50, 0x60, 0x00, 0x56]);
        code
    }

    /// The presets that record calls but no steps: geth's call tracer and parity's traces.
    fn call_presets(max_recorded_bytes: Option<usize>) -> [TracingInspectorConfig; 2] {
        [
            TracingInspectorConfig::from_geth_call_config(&Default::default()),
            TracingInspectorConfig::default_parity(),
        ]
        .map(|mut config| {
            config.max_recorded_bytes = max_recorded_bytes;
            config
        })
    }

    /// The bytes a trace's calls hold: their nodes, input and output.
    fn call_bytes(inspector: &TracingInspector) -> usize {
        inspector
            .traces()
            .nodes()
            .iter()
            .map(|n| mem::size_of::<CallTraceNode>() + n.trace.data.len() + n.trace.output.len())
            .sum()
    }

    fn step_count(inspector: &TracingInspector) -> usize {
        inspector.traces().nodes().iter().map(|n| n.trace.steps.len()).sum()
    }

    #[test]
    fn steps_count_their_bytes() {
        let inspector = TracingInspector::new(TracingInspectorConfig::default_geth());
        let (inspector, res) = trace_loop(inspector, 8, 100_000);
        res.unwrap();
        let steps = step_count(&inspector);
        let stack_words: usize = inspector
            .traces()
            .nodes()
            .iter()
            .flat_map(|n| n.trace.steps.iter())
            .map(|s| s.stack.as_ref().map_or(0, |stack| stack.len()))
            .sum();
        assert!(steps > 100);
        assert!(
            inspector.recorded_bytes()
                >= steps * mem::size_of::<CallTraceStep>() + stack_words * mem::size_of::<U256>(),
            "{} bytes for {steps} steps and {stack_words} stack words",
            inspector.recorded_bytes()
        );
    }

    #[test]
    fn a_trace_past_the_cap_halts_the_transaction() {
        let mut config = TracingInspectorConfig::default_geth();
        config.max_recorded_bytes = Some(64 << 10);
        // under the EIP-7825 cap of the default (Osaka) spec
        let (inspector, res) = trace_loop(TracingInspector::new(config), 16, 16_000_000);
        let err = res.unwrap_err();
        assert!(err.starts_with("trace too large: more than 65536 bytes"), "{err}");
        // the trace holds what was recorded before the cap, and at most one step past it
        assert!(inspector.recorded_bytes() < (64 << 10) + 8 * mem::size_of::<CallTraceStep>());
        assert!(step_count(&inspector) > 16);
    }

    #[test]
    fn without_a_cap_the_transaction_runs_out_of_gas() {
        let mut config = TracingInspectorConfig::default_geth();
        config.max_recorded_bytes = None;
        let (inspector, res) = trace_loop(TracingInspector::new(config), 1, 200_000);
        res.unwrap();
        assert!(inspector.recorded_bytes() > 0);
    }

    #[test]
    fn the_counter_survives_fuse_and_is_shared() {
        let mut config = TracingInspectorConfig::default_geth();
        config.max_recorded_bytes = Some(64 << 20);
        let (mut inspector, res) = trace_loop(TracingInspector::new(config), 1, 100_000);
        res.unwrap();
        let first = inspector.recorded_bytes();
        inspector.fuse();
        assert_eq!(inspector.recorded_bytes(), first);
        let (inspector, res) = trace_loop(inspector, 1, 100_000);
        res.unwrap();
        assert!(inspector.recorded_bytes() > first);

        let counter = Arc::new(AtomicUsize::new(0));
        let a = TracingInspector::new(config).share_recorded_bytes(counter.clone());
        let b = TracingInspector::new(config).share_recorded_bytes(counter.clone());
        let (a, res) = trace_loop(a, 1, 100_000);
        res.unwrap();
        let (b, res) = trace_loop(b, 1, 100_000);
        res.unwrap();
        assert_eq!(counter.load(Ordering::Relaxed), a.recorded_bytes());
        assert_eq!(a.recorded_bytes(), b.recorded_bytes());
        assert!(a.recorded_bytes() > first);

        // the shared cap trips on the second transaction
        let mut config = TracingInspectorConfig::default_geth();
        config.max_recorded_bytes = Some(first + first / 2);
        let counter = Arc::new(AtomicUsize::new(0));
        let a = TracingInspector::new(config).share_recorded_bytes(counter.clone());
        let b = TracingInspector::new(config).share_recorded_bytes(counter);
        let (_, res) = trace_loop(a, 1, 100_000);
        res.unwrap();
        let (_, res) = trace_loop(b, 1, 100_000);
        assert!(res.unwrap_err().starts_with("trace too large"));
    }

    #[test]
    fn calls_count_their_bytes() {
        // 256 bytes in and 64 out per call, under the default cap: the trace completes
        let contracts = [(TARGET, call_loop(CALLEE, 256)), (CALLEE, returns(64))];
        for config in call_presets(Some(DEFAULT_MAX_RECORDED_BYTES)) {
            let (inspector, res) = call_target(TracingInspector::new(config), &contracts, 200_000);
            res.unwrap();
            let nodes = inspector.traces().nodes();
            assert!(nodes.len() > 10);
            assert!(nodes[1..].iter().all(|n| n.trace.data.len() == 256));
            // the last call may run out of gas before it returns
            assert!(nodes.iter().filter(|n| n.trace.output.len() == 64).count() >= nodes.len() - 2);
            assert_eq!(inspector.recorded_bytes(), call_bytes(&inspector));
        }
    }

    #[test]
    fn calldata_past_the_cap_halts_the_transaction() {
        // 16 KiB of calldata per call, about 130 gas each: 300,000 gas copies 34 MB
        let contracts = [(TARGET, call_loop(CALLEE, 16 << 10))];
        for config in call_presets(Some(64 << 10)) {
            let (inspector, res) = call_target(TracingInspector::new(config), &contracts, 300_000);
            let err = res.unwrap_err();
            assert!(err.starts_with("trace too large: more than 65536 bytes"), "{err}");
            // the refused call is traced without its input, and the trace holds at most the cap
            // and the refused call's node
            let refused = inspector.traces().nodes().last().unwrap();
            assert!(refused.trace.data.is_empty());
            assert_eq!(refused.trace.status, Some(InstructionResult::FatalExternalError));
            assert!(call_bytes(&inspector) <= (64 << 10) + mem::size_of::<CallTraceNode>());
        }
    }

    #[test]
    fn output_past_the_cap_halts_the_transaction() {
        // no calldata, 16 KiB of output per call
        let contracts = [(TARGET, call_loop(CALLEE, 0)), (CALLEE, returns(16 << 10))];
        for config in call_presets(Some(64 << 10)) {
            let (inspector, res) =
                call_target(TracingInspector::new(config), &contracts, 1_000_000);
            assert!(res.unwrap_err().starts_with("trace too large"));
            // the call returned; its trace keeps no output
            let refused = inspector.traces().nodes().last().unwrap();
            assert_eq!(refused.trace.status, Some(InstructionResult::Return));
            assert!(refused.trace.output.is_empty());
            assert!(call_bytes(&inspector) <= (64 << 10) + mem::size_of::<CallTraceNode>());
        }
    }

    #[test]
    fn init_code_past_the_cap_halts_the_transaction() {
        // 16 KiB of init code per create
        let contracts = [(TARGET, create_loop(16 << 10))];
        for config in call_presets(Some(64 << 10)) {
            let (inspector, res) =
                call_target(TracingInspector::new(config), &contracts, 1_000_000);
            assert!(res.unwrap_err().starts_with("trace too large"));
            assert!(inspector.traces().nodes().len() > 2);
            let refused = inspector.traces().nodes().last().unwrap();
            assert!(refused.trace.data.is_empty());
            assert_eq!(refused.trace.status, Some(InstructionResult::FatalExternalError));
            assert!(call_bytes(&inspector) <= (64 << 10) + mem::size_of::<CallTraceNode>());
        }
    }

    #[test]
    fn transaction_input_past_the_cap_halts_before_it_runs() {
        // the transaction's own calldata is the first call's input
        for config in call_presets(Some(64 << 10)) {
            let (inspector, res) = inspect(
                TracingInspector::new(config),
                &[(TARGET, call_loop(CALLEE, 0))],
                TxEnv {
                    gas_limit: 3_000_000,
                    kind: TxKind::Call(TARGET),
                    data: vec![0; 128 << 10].into(),
                    ..Default::default()
                },
            );
            assert!(res.unwrap_err().starts_with("trace too large"));
            assert_eq!(inspector.traces().nodes().len(), 1);
        }
    }
    /// TN(TN-0172): runs `code` at [`TARGET`] under the struct logger's `opts` and a cap of
    /// `cap`, then builds its frame with [`TracingInspector::try_geth_traces`].
    fn struct_logs(
        opts: alloy_rpc_types_trace::geth::GethDefaultTracingOptions,
        cap: Option<usize>,
        code: Vec<u8>,
        gas_limit: u64,
    ) -> (TracingInspector, Result<alloy_rpc_types_trace::geth::DefaultFrame, String>) {
        let mut config = TracingInspectorConfig::from_geth_config(&opts);
        config.max_recorded_bytes = cap;
        let (inspector, res) =
            call_target(TracingInspector::new(config), &[(TARGET, code)], gas_limit);
        res.expect("the steps fit the cap");
        let frame = inspector.try_geth_traces(0, Bytes::new(), opts);
        (inspector, frame)
    }

    /// Code that stores at `slots` storage slots, then reads slot 0 in a loop until it runs out
    /// of gas.
    fn store_then_read(slots: u8) -> Vec<u8> {
        // PUSH1 1; PUSH1 slot; SSTORE (slots times); JUMPDEST; PUSH0; SLOAD; POP; PUSH2 <jd>; JUMP
        let mut code = Vec::new();
        for slot in 0..slots {
            code.extend([0x60, 0x01, 0x60, slot, 0x55]);
        }
        let jumpdest = code.len() as u16;
        code.extend([0x5b, 0x5f, 0x54, 0x50, 0x61]);
        code.extend(jumpdest.to_be_bytes());
        code.push(0x56);
        code
    }

    #[test]
    fn struct_logs_count_their_memory_copies() {
        let opts = alloy_rpc_types_trace::geth::GethDefaultTracingOptions::default()
            .enable_memory()
            .disable_stack()
            .disable_storage();
        // 4 KiB of memory once (MSTORE at 4064), then a loop that reuses its snapshot
        let mut code = vec![0x60, 0x01, 0x61, 0x0f, 0xe0, 0x52];
        let jumpdest = code.len() as u8;
        code.extend([0x5b, 0x60, jumpdest, 0x56]);

        let (inspector, frame) = struct_logs(opts, None, code.clone(), 30_000);
        let frame = frame.unwrap();
        let copies: usize = frame.struct_logs.iter().map(geth::struct_log_copies).sum();
        // each reused snapshot becomes 128 hex words again
        assert!(copies > frame.struct_logs.len() * 128 * 64, "{copies} bytes");
        assert!(inspector.recorded_bytes() > copies);

        // the steps fit in 4 MiB; their logs repeat the 4 KiB of memory at every step
        let (inspector, frame) = struct_logs(opts, Some(4 << 20), code, 30_000);
        let err = frame.unwrap_err();
        assert!(err.starts_with("trace too large: more than 4194304 bytes"), "{err}");
        assert!(inspector.recorded_bytes() < (4 << 20) + 128 * 88 + 4096);
    }

    #[test]
    fn struct_logs_count_their_storage_copies() {
        let opts =
            alloy_rpc_types_trace::geth::GethDefaultTracingOptions::default().disable_stack();
        let (_, frame) = struct_logs(opts, None, store_then_read(64), 3_000_000);
        let frame = frame.unwrap();
        let sloads = frame.struct_logs.iter().filter(|log| log.op == "SLOAD").count();
        let copies: usize = frame.struct_logs.iter().map(geth::struct_log_copies).sum();
        assert!(sloads > 1_000, "{sloads} reads");
        assert!(copies >= sloads * 64 * 64, "{copies} bytes for {sloads} reads");

        let (_, frame) = struct_logs(opts, Some(copies), store_then_read(64), 3_000_000);
        assert!(frame.unwrap_err().starts_with("trace too large"));
    }
}
