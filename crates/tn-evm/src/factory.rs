//! The factory to create EVM environments.

use crate::context::{TNContext as _, TNContextBuilder as _, TNEvmContext};
use crate::evm::TNEvm;
use alloy_evm::Database;
use reth_evm::{precompiles::PrecompilesMap, EvmEnv, EvmFactory};
use reth_revm::{
    context::{
        result::{EVMError, HaltReason},
        BlockEnv, CfgEnv, TxEnv,
    },
    inspector::NoOpInspector,
    precompile::{PrecompileSpecId, Precompiles},
    primitives::hardfork::SpecId,
    Context, Inspector,
};
use tn_precompiles::add_telcoin_precompile;

/// Factory producing [`TNEvm`].
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct TNEvmFactory;

impl EvmFactory for TNEvmFactory {
    type Evm<DB: Database, I: Inspector<TNEvmContext<DB>>> = TNEvm<DB, I, Self::Precompiles>;
    type Context<DB: Database> = Context<BlockEnv, TxEnv, CfgEnv, DB>;
    type Tx = TxEnv;
    type Error<DBError: core::error::Error + Send + Sync + 'static> = EVMError<DBError>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = PrecompilesMap;

    // the `NoOpInspector` is part of the trait
    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> Self::Evm<DB, NoOpInspector> {
        let spec_id = input.cfg_env.spec;
        let chain_id = input.cfg_env.chain_id;
        TNEvm {
            inner: Context::tn()
                .with_block(input.block_env)
                .with_cfg(input.cfg_env)
                .with_db(db)
                .build_with_inspector(NoOpInspector)
                .with_precompiles({
                    let mut map = PrecompilesMap::from_static(Precompiles::new(
                        PrecompileSpecId::from_spec_id(spec_id),
                    ));
                    add_telcoin_precompile(&mut map, chain_id);
                    map
                }),
            inspect: false,
        }
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        let spec_id = input.cfg_env.spec;
        let chain_id = input.cfg_env.chain_id;
        TNEvm {
            inner: Context::tn()
                .with_block(input.block_env)
                .with_cfg(input.cfg_env)
                .with_db(db)
                .build_with_inspector(inspector)
                .with_precompiles({
                    let mut map = PrecompilesMap::from_static(Precompiles::new(
                        PrecompileSpecId::from_spec_id(spec_id),
                    ));
                    add_telcoin_precompile(&mut map, chain_id);
                    map
                }),
            inspect: true,
        }
    }
}
