//! Shared utilities for e2e integration tests.
//!
//! Process management, cleanup guards, and helpers used across all test modules.

use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use std::{
    process::Child,
    sync::{Condvar, Mutex},
    time::Duration,
};
use tn_types::test_utils::init_test_tracing;
use tracing::{error, info};

/// Max number of e2e tests that can run concurrently.
/// Each test spawns 4-6 node processes; limiting concurrency prevents resource exhaustion.
const MAX_CONCURRENT_TESTS: usize = 2;

static TEST_SEMAPHORE: TestSemaphore = TestSemaphore::new(MAX_CONCURRENT_TESTS);

/// Acquire a permit to run an e2e test. Blocks until a slot is available.
/// The returned guard releases the permit on drop. Also ensure test tracing.
pub(crate) fn acquire_test_permit() -> TestSemaphoreGuard<'static> {
    init_test_tracing();
    TEST_SEMAPHORE.acquire()
}

/// Counting semaphore for limiting concurrent test execution.
struct TestSemaphore {
    state: Mutex<usize>,
    cv: Condvar,
    max: usize,
}

impl TestSemaphore {
    const fn new(max: usize) -> Self {
        Self { state: Mutex::new(0), cv: Condvar::new(), max }
    }

    fn acquire(&self) -> TestSemaphoreGuard<'_> {
        let mut count = self.state.lock().unwrap();
        while *count >= self.max {
            count = self.cv.wait(count).unwrap();
        }
        *count += 1;
        TestSemaphoreGuard { sem: self }
    }
}

pub(crate) struct TestSemaphoreGuard<'a> {
    sem: &'a TestSemaphore,
}

impl Drop for TestSemaphoreGuard<'_> {
    fn drop(&mut self) {
        let mut count = self.sem.state.lock().unwrap();
        *count -= 1;
        self.sem.cv.notify_one();
    }
}

/// RAII guard that kills child processes on drop (including during panic unwinding).
///
/// Avoids global `panic::set_hook` which causes cross-test contamination in parallel runs.
/// Sends SIGTERM to all children first (parallel graceful shutdown), then waits for each.
pub(crate) struct ProcessGuard {
    /// Owned child processes that exit on `drop`.
    children: Vec<Option<Child>>,
}

impl ProcessGuard {
    /// Create a guard wrapping existing children.
    pub(crate) fn new(children: Vec<Child>) -> Self {
        Self { children: children.into_iter().map(Some).collect() }
    }

    /// Create an empty guard.
    pub(crate) fn empty() -> Self {
        Self { children: Vec::new() }
    }

    /// Add a child to the guard. Returns the index.
    pub(crate) fn push(&mut self, child: Child) -> usize {
        let idx = self.children.len();
        self.children.push(Some(child));
        idx
    }

    /// Remove and return the child at `idx`.
    /// The caller takes responsibility for killing it — the guard will no longer track it.
    pub(crate) fn take(&mut self, idx: usize) -> Option<Child> {
        self.children.get_mut(idx).and_then(|slot| slot.take())
    }

    /// Replace the child at `idx` with a new one, returning the old child (if any).
    pub(crate) fn replace(&mut self, idx: usize, child: Child) -> Option<Child> {
        if idx >= self.children.len() {
            self.children.resize_with(idx + 1, || None);
        }
        self.children[idx].replace(child)
    }

    /// Get a mutable reference to the child at `idx`, if present.
    pub(crate) fn get_mut(&mut self, idx: usize) -> Option<&mut Child> {
        self.children.get_mut(idx).and_then(|slot| slot.as_mut())
    }

    /// Send SIGTERM to all living children without waiting.
    pub(crate) fn send_term_all(&self) {
        for child in self.children.iter().flatten() {
            send_term_by_id(child.id());
        }
    }

    /// Send SIGTERM to all, wait for each to exit (SIGKILL if needed), then clear all slots.
    /// Safe to call multiple times.
    pub(crate) fn kill_all(&mut self) {
        // Phase 1: SIGTERM all in parallel for fast graceful shutdown
        self.send_term_all();

        // Phase 2: wait for each to exit, escalate to SIGKILL if needed
        for slot in self.children.iter_mut() {
            if let Some(ref mut child) = slot {
                wait_or_kill(child);
            }
            *slot = None;
        }
    }
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        self.kill_all();
    }
}

/// Send SIGTERM to a process by PID.
fn send_term_by_id(pid: u32) {
    if let Err(e) = signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
        error!(target: "e2e-test", ?e, pid, "error sending SIGTERM");
    }
}

/// Send SIGTERM to a child process.
pub(crate) fn send_term(child: &mut Child) {
    send_term_by_id(child.id());
}

/// Gracefully shut down a child process: SIGTERM -> poll up to 6s -> SIGKILL -> wait.
pub(crate) fn kill_child(child: &mut Child) {
    send_term(child);
    wait_or_kill(child);
}

/// Poll for exit up to 5 times (1.2s each), then SIGKILL + wait.
/// Assumes SIGTERM has already been sent.
fn wait_or_kill(child: &mut Child) {
    for _ in 0..5 {
        match child.try_wait() {
            Ok(Some(_)) => {
                info!(target: "e2e-test", "child exited");
                return;
            }
            Ok(None) => {}
            Err(e) => error!(target: "e2e-test", "error waiting on child to exit: {e}"),
        }
        std::thread::sleep(Duration::from_millis(1200));
    }
    if let Err(e) = child.kill() {
        error!(target: "e2e-test", ?e, "error sending SIGKILL");
    }
    if let Err(e) = child.wait() {
        error!(target: "e2e-test", ?e, "error waiting for child after SIGKILL");
    }
}
