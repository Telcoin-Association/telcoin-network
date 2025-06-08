//! TN-specific context for evm.

use reth_evm::precompiles::PrecompilesMap;
use reth_revm::{
    context::{Block, BlockEnv, Cfg, CfgEnv, Evm, JournalTr, Transaction, TxEnv},
    db::EmptyDB,
    handler::{instructions::EthInstructions, EthPrecompiles},
    interpreter::interpreter::EthInterpreter,
    primitives::hardfork::SpecId,
    Context, Database, Journal,
};

/// The Telcoin Network EVM context.
pub(crate) type TNEvmContext<DB> = Context<BlockEnv, TxEnv, CfgEnv, DB>;

// TODO: rename thissss
// - RethEvm
// - RevmEvm
// - TNEvm
// - TnEvm
///
///
/// convenience type
/// !!!! ~~~~
pub(crate) type MainnetEvm<CTX, INSP = ()> =
    Evm<CTX, INSP, EthInstructions<EthInterpreter, CTX>, PrecompilesMap>;

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
    fn _build(self) -> MainnetEvm<Self::Context>;
    /// Return `Evm` for execution with inspector.
    fn build_with_inspector<I>(self, inspector: I) -> MainnetEvm<Self::Context, I>;
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

    fn _build(self) -> MainnetEvm<Self::Context> {
        Evm {
            ctx: self,
            inspector: (),
            instruction: EthInstructions::default(),
            precompiles: PrecompilesMap::from(EthPrecompiles::default()),
        }
    }

    fn build_with_inspector<I>(self, inspector: I) -> MainnetEvm<Self::Context, I> {
        Evm {
            ctx: self,
            inspector,
            instruction: EthInstructions::default(),
            precompiles: PrecompilesMap::from(EthPrecompiles::default()),
        }
    }
}
