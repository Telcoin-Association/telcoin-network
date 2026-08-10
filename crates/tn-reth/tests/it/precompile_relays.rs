//! Hand-assembled relay contracts for reaching the TEL precompile from a nested call frame.
//!
//! A top-level transaction frame is never static and `TxEnv` carries no `is_static` to set, so a
//! `STATICCALL` into `0x7e1` can only be expressed by routing through deployed bytecode. These
//! relays are shared by the mainnet and `faucet` precompile test modules so the two builds exercise
//! byte-identical call paths.
//!
//! Both relays end by storing the inner call's success flag at memory word 0 and returning it, so a
//! test can tell "the precompile accepted this" from "the precompile refused it". The
//! `DELEGATECALL` relay in `tel_precompile_props.rs` instead `POP`s that flag and forwards the
//! precompile's returndata, which cannot express this distinction: an accepted mutation and a
//! refused one both return empty bytes, and the outer transaction succeeds either way because the
//! relay always `RETURN`s.
//!
//! # Hosting address
//!
//! `STATICCALL` does not preserve `msg.sender`, so the precompile authorizes against the relay's
//! own address. A relay at a fresh address is rejected with `"unauthorized"` whether or not any
//! static guard exists, which would make a write-protection test pass vacuously. Deploy these at an
//! address the precompile already trusts and drive the outer transaction from a different sender,
//! since revm rejects a transaction whose sender has code (EIP-3607).

/// Runtime bytecode forwarding calldata to `0x7e1` with `CALL`, returning the inner success flag.
///
/// The positive control for the `STATICCALL` write-protection tests: it differs from
/// [`STATICCALL_RELAY_BYTECODE`] only in pushing a zero `value` operand (`CALL` takes seven stack
/// operands, `STATICCALL` six) and in the call opcode itself, `0xf1` against `0xfa`.
///
/// Disassembly:
/// ```text
///   CALLDATASIZE; PUSH1 0; PUSH1 0; CALLDATACOPY   // mem[0..csize] = calldata
///   PUSH1 0; PUSH1 0; CALLDATASIZE; PUSH1 0;       // retSize, retOffset, argsSize, argsOffset
///   PUSH1 0; PUSH2 0x07e1; GAS; CALL               // value, address, gas
///   PUSH1 0; MSTORE                                // mem[0..32] = success flag
///   PUSH1 32; PUSH1 0; RETURN                      // return that word
/// ```
pub(crate) const CALL_RELAY_BYTECODE: &[u8] = &[
    0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // CALLDATACOPY(0, 0, CALLDATASIZE)
    0x60, 0x00, 0x60, 0x00, 0x36, 0x60, 0x00, // retSize, retOffset, argsSize, argsOffset
    0x60, 0x00, // value = 0
    0x61, 0x07, 0xe1, 0x5a, 0xf1, // PUSH2 0x07e1; GAS; CALL
    0x60, 0x00, 0x52, // MSTORE(0, success)
    0x60, 0x20, 0x60, 0x00, 0xf3, // RETURN(0, 32)
];

/// Runtime bytecode forwarding calldata to `0x7e1` with `STATICCALL`, returning the success flag.
///
/// Identical to [`CALL_RELAY_BYTECODE`] but for the dropped `value` operand and `0xfa` in place of
/// `0xf1`.
///
/// Disassembly:
/// ```text
///   CALLDATASIZE; PUSH1 0; PUSH1 0; CALLDATACOPY   // mem[0..csize] = calldata
///   PUSH1 0; PUSH1 0; CALLDATASIZE; PUSH1 0;       // retSize, retOffset, argsSize, argsOffset
///   PUSH2 0x07e1; GAS; STATICCALL                  // address, gas
///   PUSH1 0; MSTORE                                // mem[0..32] = success flag
///   PUSH1 32; PUSH1 0; RETURN                      // return that word
/// ```
pub(crate) const STATICCALL_RELAY_BYTECODE: &[u8] = &[
    0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // CALLDATACOPY(0, 0, CALLDATASIZE)
    0x60, 0x00, 0x60, 0x00, 0x36, 0x60, 0x00, // retSize, retOffset, argsSize, argsOffset
    0x61, 0x07, 0xe1, 0x5a, 0xfa, // PUSH2 0x07e1; GAS; STATICCALL
    0x60, 0x00, 0x52, // MSTORE(0, success)
    0x60, 0x20, 0x60, 0x00, 0xf3, // RETURN(0, 32)
];
