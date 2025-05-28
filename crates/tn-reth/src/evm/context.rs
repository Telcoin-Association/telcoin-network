//! TN-specific context for evm.

use reth_revm::{
    context::{BlockEnv, CfgEnv, TxEnv},
    db::EmptyDB,
    primitives::hardfork::SpecId,
    Context, Journal,
};

/// Trait used to initialize Context with default mainnet types.
pub trait TNContext {
    fn tn() -> Self;
}

impl TNContext for Context<BlockEnv, TxEnv, CfgEnv, EmptyDB, Journal<EmptyDB>, ()> {
    fn tn() -> Self {
        Context::new(EmptyDB::new(), SpecId::default())
    }
}
