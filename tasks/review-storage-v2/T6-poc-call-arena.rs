// T6 PoC (revm-inspectors call-trace arena is not counted by TN-0079's cap).
// Append to patches/revm-inspectors/src/tracing/mod.rs and run: cargo test --lib review_poc
// Result on HEAD 4acc752fb: 2,080 call traces, 34,062,336 bytes of copied calldata, recorded_bytes=0 at 300K gas.
/// REVIEW PoC (scratch only): the arena's per-call input copies are not counted by TN-0079.
#[cfg(test)]
mod review_poc {
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

    /// A contract that expands memory to `args` bytes once, then loops STATICCALLing an EOA
    /// with `args` bytes of calldata until it runs out of gas.
    fn run(config: TracingInspectorConfig, args: u16, gas_limit: u64) -> (TracingInspector, Result<(), String>) {
        let target = Address::with_last_byte(0xaa);
        let mut db = CacheDB::new(EmptyDB::default());
        let mut code: Vec<u8> = Vec::new();
        // PUSH1 0; PUSH2 (args-32); MSTORE  -> expand memory to `args` bytes
        let hi = ((args - 32) >> 8) as u8;
        let lo = ((args - 32) & 0xff) as u8;
        code.extend([0x60, 0x00, 0x61, hi, lo, 0x52]);
        let jumpdest = code.len() as u8;
        code.push(0x5b); // JUMPDEST
        // STATICCALL(gas, addr, argsOff, argsLen, retOff, retLen): push retLen, retOff, argsLen, argsOff, addr, gas
        code.extend([0x60, 0x00]); // retLen 0
        code.extend([0x60, 0x00]); // retOff 0
        code.extend([0x61, (args >> 8) as u8, (args & 0xff) as u8]); // argsLen
        code.extend([0x60, 0x00]); // argsOff 0
        code.extend([0x60, 0xbb]); // EOA address 0x..bb (not a precompile)
        code.extend([0x60, 0x00]); // gas 0
        code.push(0xfa); // STATICCALL
        code.push(0x50); // POP
        code.extend([0x60, jumpdest, 0x56]); // JUMP back
        db.insert_account_info(target, AccountInfo::default().with_code(Bytecode::new_raw(code.into())));
        let mut evm = revm::Context::mainnet().with_db(db).build_mainnet_with_inspector(TracingInspector::new(config));
        let res = match evm.inspect_tx(TxEnv { gas_limit, kind: TxKind::Call(target), ..Default::default() }) {
            Ok(_) => Ok(()),
            Err(EVMError::Custom(msg)) => Err(msg),
            Err(err) => panic!("unexpected EVM error: {err:?}"),
        };
        (evm.inspector, res)
    }

    fn arena_input_bytes(i: &TracingInspector) -> (usize, usize) {
        let nodes = i.traces().nodes();
        (nodes.len(), nodes.iter().map(|n| n.trace.data.len()).sum())
    }

    #[test]
    fn call_tracer_input_copies_are_not_capped() {
        // geth callTracer: no steps, cap present (none() preset) but counts nothing
        let mut config = TracingInspectorConfig::from_geth_call_config(&Default::default());
        config.max_recorded_bytes = Some(1); // the tightest possible cap
        let (i, res) = run(config, 16 << 10, 300_000);
        res.unwrap(); // the cap never trips: no EVMError::Custom
        let (calls, bytes) = arena_input_bytes(&i);
        eprintln!("callTracer: {calls} call traces, {bytes} bytes of copied calldata, recorded_bytes={}", i.recorded_bytes());
        assert_eq!(i.recorded_bytes(), 0);
        assert!(bytes > 16 << 20, "{bytes}");
    }

    #[test]
    fn parity_default_input_copies_are_not_capped() {
        let mut config = TracingInspectorConfig::default_parity();
        config.max_recorded_bytes = Some(1);
        let (i, res) = run(config, 16 << 10, 300_000);
        res.unwrap();
        let (calls, bytes) = arena_input_bytes(&i);
        eprintln!("default_parity: {calls} call traces, {bytes} bytes of copied calldata, recorded_bytes={}", i.recorded_bytes());
        assert_eq!(i.recorded_bytes(), 0);
        assert!(bytes > 16 << 20, "{bytes}");
    }

    #[test]
    fn struct_logger_input_copies_are_not_counted() {
        // struct logger: steps ARE counted, but the per-call input copies are not
        let mut config = TracingInspectorConfig::default_geth();
        config.max_recorded_bytes = Some(usize::MAX);
        let (i, res) = run(config, 16 << 10, 300_000);
        res.unwrap();
        let (calls, bytes) = arena_input_bytes(&i);
        eprintln!("struct logger: {calls} call traces, {bytes} bytes of copied calldata, recorded_bytes={}", i.recorded_bytes());
        assert!(bytes > i.recorded_bytes(), "arena input bytes {bytes} <= recorded {}", i.recorded_bytes());
    }
}
