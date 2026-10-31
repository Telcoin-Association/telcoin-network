//! TN(TN-0017): cooperative interruption of JS tracers for revm-inspectors 0.34.
//!
//! Upstream reth (57825d41d) stops an abandoned or timed-out JS trace with the
//! `JsInspectorInterrupt` added in revm-inspectors 0.44.2 (paradigmxyz/revm-inspectors#536). That
//! release needs revm 43, and TN is on revm 34 with revm-inspectors 0.34, so this module ports the
//! same mechanism as a wrapper around the inspector: a shared flag, checked at every inspector
//! hook, that halts the EVM with `"JavaScript tracing interrupted"` once set. Script evaluation
//! inside a single callback is bounded by the Boa loop and recursion limits revm-inspectors
//! already sets.
//!
//! Delete this module when revm-inspectors reaches 0.44.2 and use
//! `revm_inspectors::tracing::js::JsInspectorInterrupt` with `JsInspector::with_interrupt`, as
//! upstream's `JsTraceInterrupt::attach` does.

use revm::{
    context_interface::{ContextError, ContextTr},
    interpreter::{
        interpreter_types::{InterpreterTypes, LoopControl},
        CallInputs, CallOutcome, CreateInputs, CreateOutcome, Gas, InstructionResult, Interpreter,
        InterpreterResult,
    },
    primitives::{Address, Bytes, Log, U256},
    Inspector,
};
use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// The EVM error message of an interrupted trace, the same as revm-inspectors 0.44.2's.
pub(super) const INTERRUPTED: &str = "JavaScript tracing interrupted";

/// A shared handle for interrupting a trace. Clones share the signal; interruption is permanent.
#[derive(Clone, Debug, Default)]
pub(super) struct JsInspectorInterrupt(Arc<AtomicBool>);

impl JsInspectorInterrupt {
    /// Creates a handle that has not been interrupted.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Signals interruption to every inspector sharing this handle.
    pub(super) fn interrupt(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Returns whether interruption has been requested.
    pub(super) fn is_interrupted(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    /// Returns a guard that interrupts when dropped, e.g. together with the request future.
    pub(super) fn drop_guard(&self) -> JsInspectorInterruptGuard {
        JsInspectorInterruptGuard(self.clone())
    }
}

/// Interrupts its [`JsInspectorInterrupt`] when dropped.
#[derive(Debug)]
#[must_use = "the guard interrupts tracing as soon as it is dropped"]
pub(super) struct JsInspectorInterruptGuard(JsInspectorInterrupt);

impl Drop for JsInspectorInterruptGuard {
    fn drop(&mut self) {
        self.0.interrupt();
    }
}

/// Wraps an inspector so that a set [`JsInspectorInterrupt`] stops execution at the next hook.
///
/// Without an interrupt every hook is forwarded unchanged. Derefs to the inner inspector so
/// results are read from it as before.
#[derive(Debug)]
pub(super) struct Interruptible<I> {
    inner: I,
    interrupt: Option<JsInspectorInterrupt>,
}

impl<I> Interruptible<I> {
    /// Wraps `inner`; `None` makes the wrapper transparent.
    pub(super) const fn new(inner: I, interrupt: Option<JsInspectorInterrupt>) -> Self {
        Self { inner, interrupt }
    }

    #[inline]
    fn is_interrupted(&self) -> bool {
        self.interrupt.as_ref().is_some_and(JsInspectorInterrupt::is_interrupted)
    }

    /// Records the interruption as the context error, which revm returns as
    /// `EVMError::Custom` when the frame or transaction ends.
    #[inline]
    fn check_interrupt(&self, context: &mut impl ContextTr) -> bool {
        if !self.is_interrupted() {
            return false
        }
        if context.error().is_ok() {
            *context.error() = Err(ContextError::Custom(INTERRUPTED.to_string()));
        }
        true
    }

    /// Like [`Self::check_interrupt`], and halts the interpreter. A pending action (a call, a
    /// return, a revert set by the tracer) is replaced so the EVM cannot carry on past it.
    #[inline]
    fn check_and_halt<INTR: InterpreterTypes>(
        &self,
        context: &mut impl ContextTr,
        interp: &mut Interpreter<INTR>,
    ) -> bool {
        if !self.check_interrupt(context) {
            return false
        }
        interp.bytecode.action().take();
        interp.bytecode.reset_action();
        interp.halt_fatal();
        true
    }

    fn interrupt_result(
        &self,
        context: &mut impl ContextTr,
        gas_limit: u64,
    ) -> Option<InterpreterResult> {
        self.check_interrupt(context).then(|| InterpreterResult {
            result: InstructionResult::FatalExternalError,
            output: Bytes::new(),
            gas: Gas::new(gas_limit),
        })
    }
}

impl<I> Deref for Interruptible<I> {
    type Target = I;

    fn deref(&self) -> &I {
        &self.inner
    }
}

impl<I> DerefMut for Interruptible<I> {
    fn deref_mut(&mut self) -> &mut I {
        &mut self.inner
    }
}

impl<CTX, INTR, I> Inspector<CTX, INTR> for Interruptible<I>
where
    CTX: ContextTr,
    INTR: InterpreterTypes,
    I: Inspector<CTX, INTR>,
{
    fn initialize_interp(&mut self, interp: &mut Interpreter<INTR>, context: &mut CTX) {
        self.inner.initialize_interp(interp, context);
    }

    fn step(&mut self, interp: &mut Interpreter<INTR>, context: &mut CTX) {
        if self.check_and_halt(context, interp) {
            return
        }
        self.inner.step(interp, context);
        self.check_and_halt(context, interp);
    }

    fn step_end(&mut self, interp: &mut Interpreter<INTR>, context: &mut CTX) {
        if self.check_and_halt(context, interp) {
            return
        }
        self.inner.step_end(interp, context);
        self.check_and_halt(context, interp);
    }

    fn log(&mut self, context: &mut CTX, log: Log) {
        if !self.check_interrupt(context) {
            self.inner.log(context, log);
        }
    }

    fn log_full(&mut self, interp: &mut Interpreter<INTR>, context: &mut CTX, log: Log) {
        if !self.check_and_halt(context, interp) {
            self.inner.log_full(interp, context, log);
        }
    }

    fn call(&mut self, context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
        if let Some(result) = self.interrupt_result(context, inputs.gas_limit) {
            return Some(CallOutcome::new(result, inputs.return_memory_offset.clone()))
        }
        let outcome = self.inner.call(context, inputs);
        self.interrupt_result(context, inputs.gas_limit)
            .map(|result| CallOutcome::new(result, inputs.return_memory_offset.clone()))
            .or(outcome)
    }

    fn call_end(&mut self, context: &mut CTX, inputs: &CallInputs, outcome: &mut CallOutcome) {
        if self.check_interrupt(context) {
            return
        }
        self.inner.call_end(context, inputs, outcome);
        self.check_interrupt(context);
    }

    fn create(&mut self, context: &mut CTX, inputs: &mut CreateInputs) -> Option<CreateOutcome> {
        if let Some(result) = self.interrupt_result(context, inputs.gas_limit()) {
            return Some(CreateOutcome::new(result, None))
        }
        let outcome = self.inner.create(context, inputs);
        self.interrupt_result(context, inputs.gas_limit())
            .map(|result| CreateOutcome::new(result, None))
            .or(outcome)
    }

    fn create_end(
        &mut self,
        context: &mut CTX,
        inputs: &CreateInputs,
        outcome: &mut CreateOutcome,
    ) {
        if self.check_interrupt(context) {
            return
        }
        self.inner.create_end(context, inputs, outcome);
        self.check_interrupt(context);
    }

    fn selfdestruct(&mut self, contract: Address, target: Address, value: U256) {
        if !self.is_interrupted() {
            self.inner.selfdestruct(contract, target, value);
        }
    }
}
