//! Process-level coverage for the in-protocol governance-Safe fork
//! ([`tn_types::forks::GOVERNANCE_SAFE_FORK_EPOCH`]).
//!
//! Everything this fork is made of — `governance_safe_fork_epoch`, `apply_governance_safe_fork`,
//! and the one-shot boundary trigger in `tn-reth`'s `finish` — is `#[cfg(feature = "adiri")]`.
//! Unlike the four sibling fork gates, it has no unconditional non-adiri entry point, so the
//! default e2e node binary (`make build-e2e-bin`, `--features tn-storage/test-utils`) has the
//! mechanism compiled out and `TN_GOVERNANCE_SAFE_FORK_EPOCH` cannot reach it. The only
//! invocation that runs the fork on spawned node processes is:
//!
//! ```text
//! make test-e2e-governance-safe
//! ```
//!
//! which builds the `adiri` e2e binary (`make build-e2e-bin-adiri`), points `TN_BIN_PATH` at it,
//! arms `TN_GOVERNANCE_SAFE_FORK_EPOCH`, and exports the [`ADIRI_BIN_MARKER`] marker.
//!
//! # How the lane is told apart from every other one
//!
//! `make test-e2e` runs `cargo nextest run -p e2e-tests --run-ignored ignored-only
//! --all-features`, which selects every `#[ignore]` test in the package — including this one —
//! while `TN_BIN_PATH` points at the NON-adiri binary. A cargo feature on `e2e-tests` would not
//! help, because `--all-features` would turn it on there too. So the discriminator is an
//! environment marker that only the dedicated lane exports, and the three outcomes are:
//!
//! | marker | `TN_GOVERNANCE_SAFE_FORK_EPOCH` | outcome |
//! |---|---|---|
//! | unset | dormant (`u32::MAX`) | skip, with a `warn!` naming the lane |
//! | unset | armed | **fail**: the variable is inert on a non-adiri binary |
//! | set | dormant | **fail**: the lane claims adiri but armed nothing |
//! | set | armed | run, and assert every pre- and post-fork fact hard |
//!
//! The middle two rows are what keeps a false green out: the only silent path is "nobody claimed
//! anything and nothing was armed". In particular, arming the variable on a default lane — the
//! form the Makefile comment used to advertise — is now a named failure instead of a no-op, and
//! a marker-set run whose post-fork state is missing fails on the assertions below rather than
//! passing quietly.
//!
//! # The genesis this test has to build
//!
//! The fork migrates the *live adiri* Safe deployment, and all five of its gates are fail-closed
//! against that exact pre-state. The stock e2e genesis cannot satisfy them: the ceremony's
//! precompile alloc (`tn-contracts/deployments/genesis/precompile-config.yaml`, by way of
//! `NetworkGenesis::fetch_precompile_genesis_accounts`) is the *mainnet* shape, which already
//! carries the whole canonical Safe v1.4.1 suite with governance sitting on `SafeL2` — i.e. the
//! post-fork state. Arming the fork over it would abort the epoch-closing block on the first
//! suite row and take the fleet down, testing nothing.
//!
//! So [`install_pre_fork_safe_state`] rewrites the ceremony's genesis into the live adiri shape
//! before any node starts: it drops every [`GOVERNANCE_SAFE_FORK_CANONICAL_SUITE`] row (plus the
//! Safe Singleton Factory's deployer marker) and restores the three accounts adiri actually
//! carries, read out of [`adiri_genesis`] — the committed `chain-configs/testnet/genesis.yaml`,
//! which is the same source the production pins were taken from. [`assert_pre_fork_alloc`] then
//! re-derives every pin from the rewritten alloc, so a tn-contracts bump or a genesis edit that
//! breaks the fixture is a named failure here rather than a node that dies minutes later.
//!
//! The lane also has to run at chain id `2017`: an `adiri` binary refuses to boot any other
//! chain, and the default binary refuses that one (`telcoin-network-cli::node`).

use crate::common::{
    acquire_test_permit, config_committee, start_nodes, wait_for_epoch_at_least, wait_for_rpc,
    ProcessGuard, NODE_PASSWORD,
};
use alloy::providers::{Provider, ProviderBuilder};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, GOVERNANCE_SAFE_ADDRESS};
use tn_types::{
    address, adiri_genesis,
    forks::{
        governance_safe_fork_canonical_address, governance_safe_fork_epoch_override,
        GOVERNANCE_SAFE_FORK_CANONICAL_SUITE, GOVERNANCE_SAFE_PROXY_PRE_FORK_CODE_HASH,
        SAFE_PROXY_FACTORY_PRE_FORK_CODE_HASH, SAFE_SINGLETON_PRE_FORK_CODE_HASH,
    },
    keccak256, Address, Epoch, Genesis, B256, U256,
};
use tracing::{info, warn};

/// Chain id every node on this lane runs. `adiri` builds accept no other
/// (`telcoin-network-cli::node`), which is also why no other lane may use it.
const ADIRI_CHAIN_ID: u64 = 2017;

/// Epoch duration (seconds) for this lane. 5s is the consensus minimum, the same cadence
/// `epochs.rs` and `basefee.rs` run at, so the fork boundary arrives within the test budget.
const EPOCH_DURATION: u64 = 5;

/// Environment marker exported only by `make test-e2e-governance-safe`, asserting that
/// `TN_BIN_PATH` points at a node binary built WITH `adiri` (`make build-e2e-bin-adiri`).
///
/// Its value is checked, not just its presence, so a lane that exports something other than
/// [`ADIRI_BIN_MARKER_VALUE`] fails loudly instead of being read as "adiri".
const ADIRI_BIN_MARKER: &str = "TN_E2E_ADIRI_BIN";

/// The only value [`ADIRI_BIN_MARKER`] may carry.
const ADIRI_BIN_MARKER_VALUE: &str = "1";

/// Environment variable selecting the governance-Safe fork epoch for this process and every node
/// it spawns ([`governance_safe_fork_epoch_override`]).
const GOVERNANCE_SAFE_FORK_ENV: &str = "TN_GOVERNANCE_SAFE_FORK_EPOCH";

/// Highest fork epoch this lane accepts. The test waits for the boundary in real time at
/// [`EPOCH_DURATION`], so an accidental large value has to fail fast rather than hang until the
/// nextest slow timeout.
const MAX_LANE_FORK_EPOCH: Epoch = 8;

/// Safe Singleton Factory deployer EOA, whose nonce-1 marker the fork writes so post-fork adiri
/// matches mainnet genesis leaf-for-leaf. Absent from adiri genesis (adiri never ran the
/// presigned deployment transaction), so pre-fork its nonce reads 0.
const SAFE_SINGLETON_FACTORY_DEPLOYER: Address =
    address!("0xE1CB04A0fA36DdD16a06ea828007E35e1a3cBC37");

/// `FallbackManager.FALLBACK_HANDLER_STORAGE_SLOT`, derived exactly as the Safe source does and
/// as `apply_governance_safe_fork` does. Restated here on purpose: an independent derivation of
/// the slot the fork writes is what makes the post-fork assertion a check rather than an echo.
fn fallback_handler_slot() -> U256 {
    U256::from_be_bytes(keccak256(b"fallback_manager.handler.address").0)
}

/// The `Safe` v1.4.1 slot holding `threshold`, and the one holding `ownerCount`. Neither is
/// touched by the migration; both are asserted unchanged across the boundary.
const SAFE_OWNER_COUNT_SLOT: u64 = 3;
const SAFE_THRESHOLD_SLOT: u64 = 4;

/// An address as the EVM stores it in a word.
fn address_word(address: Address) -> U256 {
    U256::from_be_bytes(address.into_word().0)
}

/// Resolve a [`GOVERNANCE_SAFE_FORK_CANONICAL_SUITE`] row by name, failing with the row name
/// rather than panicking on an index.
fn canonical_address(name: &str) -> eyre::Result<Address> {
    governance_safe_fork_canonical_address(name).ok_or_else(|| {
        eyre::eyre!("GOVERNANCE_SAFE_FORK_CANONICAL_SUITE no longer carries a `{name}` row")
    })
}

/// Decide whether this process is the governance-Safe lane, and with which fork epoch.
///
/// `Ok(None)` means "not this lane, skip"; `Err` means the environment claims something it
/// cannot deliver. See the module docs for the full truth table — the short version is that the
/// only quiet outcome is an unmarked lane with the fork left dormant.
fn lane_fork_epoch() -> eyre::Result<Option<Epoch>> {
    let marker = std::env::var(ADIRI_BIN_MARKER).ok().filter(|value| !value.is_empty());
    // the raw value, not the latched override: an unparseable or absent variable means "this
    // build's own fork point", which for the adiri binary is the dormant u32::MAX placeholder
    let armed = governance_safe_fork_epoch_override().filter(|epoch| *epoch != Epoch::MAX);

    match (marker, armed) {
        (None, None) => {
            const SKIPPED: &str = "SKIPPING the governance-Safe fork test: TN_E2E_ADIRI_BIN is \
                                   unset, so TN_BIN_PATH is the default e2e binary, which has \
                                   the whole fork mechanism compiled out (#[cfg(feature = \
                                   \"adiri\")]). Run `make test-e2e-governance-safe` for the \
                                   lane that actually executes this fork.";
            warn!(target: "governance-safe-fork-test", "{SKIPPED}");
            // also unconditionally, because `init_test_tracing` builds its filter from `RUST_LOG`
            // and drops `warn!` when that is unset — which is exactly the default-lane run this
            // message exists for. Neither line reaches a passing nextest run's output, so the
            // protection against a false green is the two hard-failure arms below, not this.
            eprintln!("{SKIPPED}");
            Ok(None)
        }
        (None, Some(fork_epoch)) => Err(eyre::eyre!(
            "{GOVERNANCE_SAFE_FORK_ENV}={fork_epoch} is armed but {ADIRI_BIN_MARKER} is unset. \
             That variable is INERT on every default lane: `make build-e2e-bin` builds without \
             `adiri`, and the whole governance-Safe mechanism is behind that feature, so nothing \
             would have executed. Run `make test-e2e-governance-safe` instead."
        )),
        (Some(_), None) => Err(eyre::eyre!(
            "{ADIRI_BIN_MARKER} is set but {GOVERNANCE_SAFE_FORK_ENV} is dormant or unset, so \
             the boundary would never fire and this lane would assert nothing. The lane must \
             export both."
        )),
        (Some(marker), Some(fork_epoch)) => {
            eyre::ensure!(
                marker == ADIRI_BIN_MARKER_VALUE,
                "{ADIRI_BIN_MARKER}={marker} is not the expected `{ADIRI_BIN_MARKER_VALUE}`; \
                 refusing to guess whether TN_BIN_PATH is an adiri binary"
            );
            let bin_path = std::env::var("TN_BIN_PATH").unwrap_or_default();
            eyre::ensure!(
                !bin_path.is_empty(),
                "{ADIRI_BIN_MARKER} is set but TN_BIN_PATH is not: the harness would fall back \
                 to an escargot build, which carries no `adiri` feature and could not run the \
                 fork. Run `make test-e2e-governance-safe`."
            );
            // the trigger is `concluding_epoch + 1 == fork_epoch`, so 0 can never fire
            eyre::ensure!(
                (1..=MAX_LANE_FORK_EPOCH).contains(&fork_epoch),
                "{GOVERNANCE_SAFE_FORK_ENV}={fork_epoch} is outside 1..={MAX_LANE_FORK_EPOCH}: \
                 0 can never fire (the boundary trigger is `concluding epoch + 1 == fork epoch`) \
                 and a larger value would wait past the test budget"
            );
            info!(
                target: "governance-safe-fork-test",
                fork_epoch, bin_path, "governance-Safe fork lane armed",
            );
            Ok(Some(fork_epoch))
        }
    }
}

/// Rewrite the ceremony's mainnet-shaped Safe alloc into the live adiri pre-fork shape.
///
/// Three moves, in order: drop every canonical suite row (the fork etches eleven of them and
/// swaps two, and every one of those gates refuses an occupied/mismatched target), drop the Safe
/// Singleton Factory's deployer marker (adiri has no such account), then restore the three
/// accounts adiri does carry — the recompiled `Safe` singleton, the recompiled
/// `SafeProxyFactory`, and the governance proxy with its slot 0 still on the L1 singleton and no
/// fallback handler.
///
/// Sourced from [`adiri_genesis`] rather than restated here so the fixture and the production
/// pins share one origin: `chain-configs/testnet/genesis.yaml` is where the pre-fork code hashes
/// in `tn_types::forks` were measured.
fn install_pre_fork_safe_state(mut genesis: Genesis) -> eyre::Result<Genesis> {
    let adiri = adiri_genesis();

    for (_, address, _) in GOVERNANCE_SAFE_FORK_CANONICAL_SUITE {
        genesis.alloc.remove(&address);
    }
    genesis.alloc.remove(&SAFE_SINGLETON_FACTORY_DEPLOYER);

    for address in [
        canonical_address("Safe")?,
        canonical_address("SafeProxyFactory")?,
        GOVERNANCE_SAFE_ADDRESS,
    ] {
        let account = adiri.alloc.get(&address).cloned().ok_or_else(|| {
            eyre::eyre!(
                "adiri genesis carries no account at {address}: \
                 chain-configs/testnet/genesis.yaml no longer holds the pre-fork governance-Safe \
                 state this lane rewrites in"
            )
        })?;
        genesis.alloc.insert(address, account);
    }

    assert_pre_fork_alloc(&genesis)?;
    Ok(genesis)
}

/// Re-derive every gate the fork will evaluate, from the alloc that is about to be written to
/// disk.
///
/// Without this the lane's failure mode on a drifted fixture is a node that dies at the boundary
/// with an `EVMCustom` message minutes into the run; with it, the drift is named before the first
/// process starts.
fn assert_pre_fork_alloc(genesis: &Genesis) -> eyre::Result<()> {
    let code_hash = |address: Address| -> B256 {
        genesis
            .alloc
            .get(&address)
            .and_then(|account| account.code.as_ref())
            .map(|code| keccak256(code))
            .unwrap_or(B256::ZERO)
    };

    let safe = canonical_address("Safe")?;
    let factory = canonical_address("SafeProxyFactory")?;
    eyre::ensure!(
        code_hash(safe) == SAFE_SINGLETON_PRE_FORK_CODE_HASH,
        "genesis `Safe` singleton at {safe} does not hash to SAFE_SINGLETON_PRE_FORK_CODE_HASH: \
         the fork's swap gate would fail closed"
    );
    eyre::ensure!(
        code_hash(factory) == SAFE_PROXY_FACTORY_PRE_FORK_CODE_HASH,
        "genesis `SafeProxyFactory` at {factory} does not hash to \
         SAFE_PROXY_FACTORY_PRE_FORK_CODE_HASH: the fork's swap gate would fail closed"
    );
    eyre::ensure!(
        code_hash(GOVERNANCE_SAFE_ADDRESS) == GOVERNANCE_SAFE_PROXY_PRE_FORK_CODE_HASH,
        "genesis governance proxy at {GOVERNANCE_SAFE_ADDRESS} does not hash to \
         GOVERNANCE_SAFE_PROXY_PRE_FORK_CODE_HASH: the fork's migration gate would fail closed"
    );

    // every OTHER suite row is an etch target, and the etch refuses an address that holds code
    for (name, address, _) in GOVERNANCE_SAFE_FORK_CANONICAL_SUITE {
        if address == safe || address == factory {
            continue;
        }
        eyre::ensure!(
            code_hash(address) == B256::ZERO,
            "genesis etch target {name} ({address}) already carries code: the fork's etch gate \
             would fail closed"
        );
    }

    let storage = genesis
        .alloc
        .get(&GOVERNANCE_SAFE_ADDRESS)
        .and_then(|account| account.storage.as_ref())
        .ok_or_else(|| eyre::eyre!("genesis governance proxy carries no storage"))?;
    let slot = |index: U256| -> U256 {
        storage
            .get(&B256::from(index))
            .map(|word| U256::from_be_bytes(word.0))
            .unwrap_or(U256::ZERO)
    };
    eyre::ensure!(
        slot(U256::ZERO) == address_word(safe),
        "genesis governance proxy slot 0 is not the pre-fork L1 `Safe` singleton: the fork's \
         slot-0 gate would fail closed"
    );
    eyre::ensure!(
        slot(fallback_handler_slot()) == U256::ZERO,
        "genesis governance proxy already has a fallback handler: this lane is supposed to start \
         from the live adiri pre-fork state, where the slot is unset"
    );
    eyre::ensure!(
        !genesis.alloc.contains_key(&SAFE_SINGLETON_FACTORY_DEPLOYER),
        "genesis still carries the Safe Singleton Factory deployer marker, so the fork's nonce \
         write would not be observable"
    );

    Ok(())
}

/// Assert the genesis block still holds the live adiri pre-fork Safe state.
///
/// Read at block 0 rather than at `latest` on purpose: genesis state is immutable, so this half
/// of the transition cannot race the boundary however slow the run is.
async fn assert_pre_fork_state<P: Provider>(provider: &P) -> eyre::Result<()> {
    let safe = canonical_address("Safe")?;
    let safe_l2 = canonical_address("SafeL2")?;

    let safe_code = provider.get_code_at(safe).number(0).await?;
    eyre::ensure!(
        keccak256(&safe_code) == SAFE_SINGLETON_PRE_FORK_CODE_HASH,
        "pre-fork `Safe` singleton is not the recompiled adiri deployment"
    );
    eyre::ensure!(
        provider.get_code_at(safe_l2).number(0).await?.is_empty(),
        "pre-fork `SafeL2` at {safe_l2} already carries code, so the post-fork assertion below \
         would not prove the fork installed it"
    );
    eyre::ensure!(
        provider.get_storage_at(GOVERNANCE_SAFE_ADDRESS, U256::ZERO).number(0).await?
            == address_word(safe),
        "pre-fork governance proxy slot 0 is not the L1 `Safe` singleton"
    );
    eyre::ensure!(
        provider.get_storage_at(GOVERNANCE_SAFE_ADDRESS, fallback_handler_slot()).number(0).await?
            == U256::ZERO,
        "pre-fork governance proxy already has a fallback handler"
    );
    eyre::ensure!(
        provider.get_transaction_count(SAFE_SINGLETON_FACTORY_DEPLOYER).number(0).await? == 0,
        "pre-fork Safe Singleton Factory deployer is not at nonce 0"
    );

    info!(target: "governance-safe-fork-test", "pre-fork state confirmed at the genesis block");
    Ok(())
}

/// Assert the boundary installed the canonical suite and migrated the proxy.
///
/// Every one of the thirteen rows is hashed against its own pinned `B256` from
/// [`GOVERNANCE_SAFE_FORK_CANONICAL_SUITE`], which is the same table `tn-reth` installs from, so
/// a row installed with the wrong bytes is a named failure rather than "some code is there".
async fn assert_post_fork_state<P: Provider>(provider: &P) -> eyre::Result<()> {
    for (name, address, expected) in GOVERNANCE_SAFE_FORK_CANONICAL_SUITE {
        let code = provider.get_code_at(address).await?;
        eyre::ensure!(
            !code.is_empty(),
            "post-fork {name} ({address}) holds no code: the fork did not install it"
        );
        eyre::ensure!(
            keccak256(&code) == expected,
            "post-fork {name} ({address}) code hashes to {} instead of the pinned {expected}",
            keccak256(&code)
        );
    }

    let safe_l2 = canonical_address("SafeL2")?;
    let handler = canonical_address("CompatibilityFallbackHandler")?;
    eyre::ensure!(
        provider.get_storage_at(GOVERNANCE_SAFE_ADDRESS, U256::ZERO).await?
            == address_word(safe_l2),
        "post-fork governance proxy slot 0 is not the `SafeL2` singleton"
    );
    eyre::ensure!(
        provider.get_storage_at(GOVERNANCE_SAFE_ADDRESS, fallback_handler_slot()).await?
            == address_word(handler),
        "post-fork governance proxy fallback-handler slot is not the \
         `CompatibilityFallbackHandler`"
    );
    eyre::ensure!(
        provider.get_transaction_count(SAFE_SINGLETON_FACTORY_DEPLOYER).await? >= 1,
        "post-fork Safe Singleton Factory deployer is still at nonce 0: the mainnet-genesis \
         marker leaf was not written"
    );

    // "owners, threshold, and the Safe nonce are untouched" is half of what the migration
    // promises, and it is preserved by omission (only changed slots enter the bundle), so it is
    // worth an explicit check: a staging bug that rebuilt the account would silently drop these.
    for (label, index) in
        [("ownerCount", SAFE_OWNER_COUNT_SLOT), ("threshold", SAFE_THRESHOLD_SLOT)]
    {
        let slot = U256::from(index);
        let before = provider.get_storage_at(GOVERNANCE_SAFE_ADDRESS, slot).number(0).await?;
        let after = provider.get_storage_at(GOVERNANCE_SAFE_ADDRESS, slot).await?;
        eyre::ensure!(
            before == after && before != U256::ZERO,
            "governance proxy {label} moved across the fork: {before} -> {after}"
        );
    }

    info!(target: "governance-safe-fork-test", "post-fork state confirmed at the chain tip");
    Ok(())
}

/// Run a local testnet across the armed governance-Safe fork boundary and assert the transition
/// on a spawned node's RPC.
///
/// See the module docs for the gating rules and for why the genesis has to be rewritten first.
/// The assertions are deliberately split across two block ids — the pre-fork half at the
/// immutable genesis block, the post-fork half at the tip once the registry reports the fork
/// epoch — so the test proves a transition rather than reading a static fixture.
#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
async fn test_governance_safe_fork_boundary() -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    let Some(fork_epoch) = lane_fork_epoch()? else {
        return Ok(());
    };

    let committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
    ];

    // keep the temp-dir prefix short: every node's IPC socket path is built under it, and a unix
    // socket path is capped at ~104 bytes
    let temp_dir = tempfile::TempDir::with_prefix("gov_safe")?;
    let temp_path = temp_dir.path();
    let shared_genesis_dir = temp_path.join("shared-genesis");

    let genesis = config_committee(
        temp_path,
        &shared_genesis_dir,
        Some(NODE_PASSWORD.to_string()),
        // registry owner is never used by this test; a distinct address keeps it from
        // entangling with the governance Safe whose state the fork rewrites
        Address::from_slice(&[0xAA; 20]),
        vec![],
        &committee,
        EPOCH_DURATION,
        Some(ADIRI_CHAIN_ID),
    )?;

    // rewrite the shared genesis, then redistribute it: copying one rewritten file is what makes
    // every node's genesis byte-identical, rather than re-running the transform per directory
    let genesis_path = shared_genesis_dir.join("genesis/genesis.yaml");
    let genesis = install_pre_fork_safe_state(genesis)?;
    Config::write_to_path(&genesis_path, &genesis, ConfigFmt::YAML)?;
    for (validator, _) in committee.iter() {
        std::fs::copy(&genesis_path, temp_path.join(validator).join("genesis/genesis.yaml"))?;
    }
    info!(
        target: "governance-safe-fork-test",
        chain_id = genesis.config.chain_id,
        accounts = genesis.alloc.len(),
        "genesis rewritten to the live adiri pre-fork Safe state",
    );

    let (procs, endpoints) = start_nodes(temp_path, &committee, "governance_safe_fork", 1)?;
    // guard kills the node processes on drop (normal return, error, or panic)
    let _guard = ProcessGuard::new(procs);

    let provider = ProviderBuilder::new().connect_http(endpoints[0].http_url.parse()?);
    wait_for_rpc(&provider).await?;

    assert_pre_fork_state(&provider).await?;

    // the boundary fires from the epoch-closing block that concludes `fork_epoch - 1`, so the
    // registry reporting `fork_epoch` means that block is canonical and its state is at the tip
    let snapshot = wait_for_epoch_at_least(&provider, fork_epoch).await?;
    info!(
        target: "governance-safe-fork-test",
        epoch = snapshot.epoch_id,
        block_height = snapshot.block_height,
        "crossed the governance-Safe fork boundary",
    );

    assert_post_fork_state(&provider).await
}
