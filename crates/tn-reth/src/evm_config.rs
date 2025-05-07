//! Provide config and implement traits to bridge protocol extensions to Reth.

use alloy_consensus::Header;
use reth_chainspec::ChainSpec;
use reth_evm::{env::EvmEnv, ConfigureEvm, ConfigureEvmEnv, NextBlockEnvAttributes};
use reth_evm_ethereum::EthEvmConfig;
use reth_revm::{
    primitives::{CfgEnvWithHandlerCfg, Env, EnvWithHandlerCfg, TxEnv},
    Database, Evm,
};
use std::{convert::Infallible, sync::Arc};
use tn_types::{Address, Bytes, TransactionSigned, U256};

/// TN-related EVM configuration.
/// Wrap and modify the Reth defaults.
#[derive(Debug, Clone)]
pub struct TnEvmConfig {
    inner: EthEvmConfig,
}

impl TnEvmConfig {
    /// Creates a new TN EVM configuration with the given chain spec.
    pub const fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { inner: EthEvmConfig::new(chain_spec) }
    }

    /// Returns the chain spec associated with this configuration.
    pub const fn chain_spec(&self) -> &Arc<ChainSpec> {
        self.inner.chain_spec()
    }

    /// Provide a custom reward beneficiary callback to handle base fees for telcoin network.
    fn set_base_fee_handler<DB: Database>(&self, evm: &mut Evm<'_, (), DB>) {
        // TODO- send the base fee to safe or contract to be managed offchain.
        let basefee_address: Option<Address> = None;
        // DO NOT use this testing default in mainnet.
        //    Some(Address::parse_checksummed("0x29615F9e735932580f699C494C11fB81296AfE8F", None)
        //    .expect("valid account"));
        evm.handler.post_execution.reward_beneficiary = Arc::new(move |ctx, gas| {
            // code lifted from revm mainnet/post_execution.rs and modified to do something with
            // base fee.
            let beneficiary = ctx.evm.env.block.coinbase;
            let effective_gas_price = ctx.evm.env.effective_gas_price();

            // transfer fee to coinbase/beneficiary.
            // Basefee amount of gas is redirected.
            let coinbase_gas_price = effective_gas_price.saturating_sub(ctx.evm.env.block.basefee);

            let coinbase_account =
                ctx.evm.inner.journaled_state.load_account(beneficiary, &mut ctx.evm.inner.db)?;

            coinbase_account.data.mark_touch();
            let gas_used = U256::from(gas.spent() - gas.refunded() as u64);
            coinbase_account.data.info.balance =
                coinbase_account.data.info.balance.saturating_add(coinbase_gas_price * gas_used);

            if let Some(basefee_address) = basefee_address {
                // Send the base fee portion to a basefee account for later processing (offchain).
                let basefee = ctx.evm.env.block.basefee;
                let basefee_account = ctx
                    .evm
                    .inner
                    .journaled_state
                    .load_account(basefee_address, &mut ctx.evm.inner.db)?;
                basefee_account.data.mark_touch();
                basefee_account.data.info.balance =
                    basefee_account.data.info.balance.saturating_add(basefee * gas_used);
            }
            Ok(())
        });
    }
}

impl ConfigureEvmEnv for TnEvmConfig {
    type Header = Header;
    type Transaction = TransactionSigned;
    type Error = Infallible;

    fn fill_tx_env(&self, tx_env: &mut TxEnv, transaction: &Self::Transaction, sender: Address) {
        self.inner.fill_tx_env(tx_env, transaction, sender)
    }

    fn fill_tx_env_system_contract_call(
        &self,
        env: &mut Env,
        caller: Address,
        contract: Address,
        data: Bytes,
    ) {
        self.inner.fill_tx_env_system_contract_call(env, caller, contract, data)
    }

    fn fill_cfg_env(&self, cfg_env: &mut CfgEnvWithHandlerCfg, header: &Self::Header) {
        self.inner.fill_cfg_env(cfg_env, header)
    }

    fn next_cfg_and_block_env(
        &self,
        parent: &Self::Header,
        attributes: NextBlockEnvAttributes,
    ) -> Result<EvmEnv, Self::Error> {
        self.inner.next_cfg_and_block_env(parent, attributes)
    }
}

impl ConfigureEvm for TnEvmConfig {
    type DefaultExternalContext<'a> = ();

    fn evm<DB: Database>(&self, db: DB) -> Evm<'_, Self::DefaultExternalContext<'_>, DB> {
        let mut evm = self.inner.evm(db);
        self.set_base_fee_handler(&mut evm);
        evm
    }

    fn evm_with_env<DB: Database>(
        &self,
        db: DB,
        env: EnvWithHandlerCfg,
    ) -> Evm<'_, Self::DefaultExternalContext<'_>, DB> {
        let mut evm = self.evm(db);
        evm.modify_spec_id(env.spec_id());
        evm.context.evm.env = env.env;
        self.set_base_fee_handler(&mut evm);
        evm
    }

    fn default_external_context<'a>(&self) -> Self::DefaultExternalContext<'a> {
        self.inner.default_external_context()
    }
}
