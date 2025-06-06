//! The factory to create EVM environments.

use super::{
    TNBlockExecutionCtx, TNBlockExecutor, TNContext as _, TNContextBuilder as _, TNEvm,
    TNEvmContext,
};
use alloy_evm::Database;
use reth_evm::{
    block::{BlockExecutorFactory, BlockExecutorFor},
    eth::{
        receipt_builder::{AlloyReceiptBuilder, ReceiptBuilder},
        spec::{EthExecutorSpec, EthSpec},
    },
    precompiles::PrecompilesMap,
    EvmEnv, EvmFactory, FromRecoveredTx, FromTxWithEncoded,
};
use reth_revm::{
    context::{
        result::{EVMError, HaltReason},
        BlockEnv, CfgEnv, TxEnv,
    },
    inspector::NoOpInspector,
    precompile::{PrecompileSpecId, Precompiles},
    primitives::hardfork::SpecId,
    Context, Inspector, State,
};
use tn_types::{Receipt, TransactionSigned};

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
    type Precompiles = PrecompilesMap;

    // the `NoOpInspector` is part of the trait
    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> Self::Evm<DB, NoOpInspector> {
        let spec_id = input.cfg_env.spec;
        TNEvm {
            inner: Context::tn()
                .with_block(input.block_env)
                .with_cfg(input.cfg_env)
                .with_db(db)
                .build_with_inspector(NoOpInspector)
                .with_precompiles(PrecompilesMap::from_static(Precompiles::new(
                    PrecompileSpecId::from_spec_id(spec_id),
                ))),
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
        TNEvm {
            inner: Context::tn()
                .with_block(input.block_env)
                .with_cfg(input.cfg_env)
                .with_db(db)
                .build_with_inspector(inspector)
                .with_precompiles(PrecompilesMap::from_static(Precompiles::new(
                    PrecompileSpecId::from_spec_id(spec_id),
                ))),
            inspect: true,
        }
    }
}

/// Ethereum block executor factory.
#[derive(Debug, Clone, Default, Copy)]
pub struct TNBlockExecutorFactory<
    R = AlloyReceiptBuilder,
    Spec = EthSpec,
    EvmFactory = TNEvmFactory,
> {
    /// Receipt builder.
    receipt_builder: R,
    /// Chain specification.
    spec: Spec,
    /// EVM factory.
    evm_factory: EvmFactory,
}

// alloy-evm
impl<R, Spec, EvmFactory> TNBlockExecutorFactory<R, Spec, EvmFactory> {
    /// Creates a new [`TNBlockExecutorFactory`] with the given spec, [`EvmFactory`], and
    /// [`ReceiptBuilder`].
    pub const fn new(receipt_builder: R, spec: Spec, evm_factory: EvmFactory) -> Self {
        Self { receipt_builder, spec, evm_factory }
    }

    /// Exposes the receipt builder.
    pub const fn receipt_builder(&self) -> &R {
        &self.receipt_builder
    }

    /// Exposes the chain specification.
    pub const fn spec(&self) -> &Spec {
        &self.spec
    }

    /// Exposes the EVM factory.
    pub const fn evm_factory(&self) -> &EvmFactory {
        &self.evm_factory
    }
}

// alloy-evm
impl<R, Spec, EvmF> BlockExecutorFactory for TNBlockExecutorFactory<R, Spec, EvmF>
where
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt = Receipt>,
    Spec: EthExecutorSpec,
    EvmF: EvmFactory<Tx: FromRecoveredTx<TransactionSigned> + FromTxWithEncoded<TransactionSigned>>,
    Self: 'static,
{
    type EvmFactory = EvmF;
    type ExecutionCtx<'a> = TNBlockExecutionCtx;
    type Transaction = R::Transaction;
    type Receipt = R::Receipt;

    fn evm_factory(&self) -> &Self::EvmFactory {
        &self.evm_factory
    }

    fn create_executor<'a, DB, I>(
        &'a self,
        evm: EvmF::Evm<&'a mut State<DB>, I>,
        ctx: Self::ExecutionCtx<'a>,
    ) -> impl BlockExecutorFor<'a, Self, DB, I>
    where
        DB: Database + 'a,
        I: Inspector<EvmF::Context<&'a mut State<DB>>> + 'a,
    {
        TNBlockExecutor::new(evm, ctx, &self.spec, &self.receipt_builder)
    }
}
