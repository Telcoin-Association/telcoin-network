//! Pipelined persist for the execution engine (`pipelined_persist` in `parameters.yaml`).
//!
//! With the switch on, the engine hands output N's persist to a blocking task and starts building
//! output N+1 on N's in-memory state, while persists still commit in consensus order and consensus
//! sees a block only once it is durable. This module will hold that state machine. It is empty
//! for now, and the engine always builds, persists and commits one output before it starts the
//! next.
