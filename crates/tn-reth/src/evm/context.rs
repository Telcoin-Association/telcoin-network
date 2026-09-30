//! TN-specific context for evm.
//!
//! Source code in revm.
//!
//! Type aliases and builder plumbing for the revm `Context` TN executes against:
//! [`TNEvmContext`] pins the generic parameters (mainnet `BlockEnv`/`TxEnv`/`CfgEnv` over a
//! caller-supplied database), and the [`TNContext`]/[`TNContextBuilder`] traits construct the
//! inner revm `Evm` value that `TNEvmFactory` wraps in a `TNEvm`.
//!
//! Note: the builders here install revm's stock precompiles as a placeholder; the factory
//! immediately replaces that map with the TN set (stock + TEL + BLS G1), so the stock-only map
//! never executes. TN-specific transaction semantics (`transact_system_call`,
//! `transact_pre_genesis_create`) live on `TNEvm` in the parent module, not here.

use reth_evm::precompiles::PrecompilesMap;
use reth_revm::{
    context::{Block, BlockEnv, Cfg, CfgEnv, Evm, FrameStack, JournalTr, Transaction, TxEnv},
    db::EmptyDB,
    handler::{instructions::EthInstructions, EthFrame, EthPrecompiles},
    interpreter::interpreter::EthInterpreter,
    primitives::hardfork::SpecId,
    Context, Database, Journal,
};

/// The Telcoin Network EVM context.
pub(crate) type TNEvmContext<DB> = Context<BlockEnv, TxEnv, CfgEnv, DB>;

/// Convenience type for TN mainnet's EVM.
pub(crate) type TelcoinEvm<CTX, INSP = ()> =
    Evm<CTX, INSP, EthInstructions<EthInterpreter, CTX>, PrecompilesMap, EthFrame<EthInterpreter>>;

/// Trait used to initialize Context with default mainnet types.
pub(crate) trait TNContext {
    type Context;
    /// Build the default TN context.
    fn tn() -> Self;
}

/// Trait used to initialize Context with default mainnet types.
pub(crate) trait TNContextBuilder {
    type Context;
    /// Return `Evm` for execution without inspector.
    fn _build(self) -> TelcoinEvm<Self::Context>;
    /// Return `Evm` for execution with inspector.
    fn build_with_inspector<I>(self, inspector: I) -> TelcoinEvm<Self::Context, I>;
}

impl TNContext for Context<BlockEnv, TxEnv, CfgEnv, EmptyDB, Journal<EmptyDB>, ()> {
    type Context = Self;

    fn tn() -> Self {
        Context::new(EmptyDB::new(), SpecId::default())
    }
}

impl<BLOCK, TX, CFG, DB, JOURNAL, CHAIN> TNContextBuilder
    for Context<BLOCK, TX, CFG, DB, JOURNAL, CHAIN>
where
    BLOCK: Block,
    TX: Transaction,
    CFG: Cfg,
    DB: Database,
    JOURNAL: JournalTr<Database = DB>,
{
    type Context = Self;

    fn _build(self) -> TelcoinEvm<Self::Context> {
        Evm {
            ctx: self,
            inspector: (),
            instruction: EthInstructions::default(),
            precompiles: PrecompilesMap::from(EthPrecompiles::default()),
            frame_stack: FrameStack::new(),
        }
    }

    fn build_with_inspector<I>(self, inspector: I) -> TelcoinEvm<Self::Context, I> {
        Evm {
            ctx: self,
            inspector,
            instruction: EthInstructions::default(),
            precompiles: PrecompilesMap::from(EthPrecompiles::default()),
            frame_stack: FrameStack::new(),
        }
    }
}
