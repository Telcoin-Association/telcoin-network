//! Forward transactions accepted by a non-committee ("observer") worker to the committee.
//!
//! Instead of pushing raw transaction bytes over the libp2p worker protocol, an observer
//! forwards each transaction to the JSON-RPC endpoint the owning validator advertised on its
//! worker record (issue #804), so the submitter gets the same RPC experience they would get
//! talking to a validator directly. Routing mirrors [`submit_txn_if_mine`]: a transaction is
//! sent to the validator whose committee slot owns the sender, so all transactions from one
//! account converge on a single validator and nonce ordering is preserved. A validator that has
//! not advertised an endpoint (or is momentarily unreachable) is skipped in favor of one that
//! has.
//!
//! Three properties worth knowing at this boundary:
//!
//! - Transport security is whatever the validator advertised. Endpoint validation accepts both `https://`
//!   and plain `http://` URLs, and the forwarder adds no encryption of its own, so a plain-HTTP
//!   advertisement carries the signed raw transaction bytes in cleartext, readable by any on-path
//!   observer. (The bytes are already-signed transactions destined for a public mempool, so
//!   confidentiality is limited to submission timing, not contents.)
//! - A considered rejection stops the fallback chain. If a validator's RPC rejects the transaction
//!   itself (bad nonce, underpriced, invalid), no further validators are tried: all validators
//!   share consensus state, so the rejection would repeat everywhere. Only endpoint-local failures
//!   (timeout, transport error, full pool, internal error) fall through to the next advertised
//!   validator, and "already known" counts as delivered.
//! - The dial target is chosen by a committee member, not by this node. The endpoint arrives inside
//!   a BLS-signed node record, so an arbitrary network peer cannot inject one, but a committee
//!   member can still advertise any host it likes and every observer will dial it unattended.
//!   [`ForwardTargetPolicy`] therefore refuses non-public hosts at the dial site by default, so a
//!   committee member cannot aim an observer's outbound HTTP at hosts inside that observer's own
//!   perimeter (issue #1092).
//!
//! [`submit_txn_if_mine`]: tn_types::BatchValidation::submit_txn_if_mine

use crate::recover_raw_transaction;
use alloy::{
    providers::{Provider as _, RootProvider},
    transports::{RpcError, TransportErrorKind},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv4Addr, Ipv6Addr},
    sync::{Arc, Mutex},
    time::Duration,
};
use tn_types::{BlsPublicKey, RpcInfo, TaskSpawner, TxnForwarder};
use tokio::time::timeout;
use tracing::{debug, warn};
use url::{Host, Url};

/// Bounds a single validator's `eth_sendRawTransaction` round-trip so one unresponsive endpoint
/// cannot stall the fallback chain; on timeout the forward tries the next validator.
const FORWARD_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds the total time spent forwarding one transaction across all its fallback validators, so a
/// whole unresponsive committee cannot make a single transaction cost `committee_size` back-to-back
/// [`FORWARD_SEND_TIMEOUT`]s. When it elapses the transaction is left unforwarded and the next one
/// proceeds.
const FORWARD_TX_BUDGET: Duration = Duration::from_secs(15);

/// JSON-RPC error codes reth returns for a validator-local, transient condition (a full pool,
/// `-32003`, or an internal/IO error, `-32603`) rather than a verdict on the transaction itself.
/// The forward falls through to the next advertised validator on these, since another validator
/// may still accept the transaction.
const TRANSIENT_RPC_CODES: [i64; 2] = [-32003, -32603];

/// Substring of reth's `eth_sendRawTransaction` error message when the transaction is already in a
/// validator's pool (`code -32000`). Treated as a successful delivery, not a failure to retry.
const ALREADY_KNOWN_MESSAGE: &str = "already known";

/// Whether the forwarder may dial an advertised endpoint whose host is not a public internet
/// address.
///
/// The endpoint is chosen by a committee member and dialed by an unattended observer process, so
/// an unconstrained target lets a committee member direct an observer's outbound HTTP at hosts
/// only that observer can reach. Production deployments run [`Self::PublicOnly`]; single-host and
/// docker-compose deployments, where validators legitimately advertise `127.0.0.1`, opt in to
/// [`Self::AllowPrivate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardTargetPolicy {
    /// Dial only hosts that could be a public internet peer. Refuses loopback, private-use,
    /// link-local, unique-local, shared-address-space, unspecified and non-unicast literals, and
    /// the reserved `localhost`/`.local` names. This is the default.
    PublicOnly,
    /// Dial whatever a committee member advertised, including addresses inside this node's own
    /// network. Only appropriate where every committee member is under the same operator.
    AllowPrivate,
}

impl ForwardTargetPolicy {
    /// Build the policy from the operator's `allow_private_forward_targets` setting, which is
    /// `false` (that is, [`Self::PublicOnly`]) unless the operator sets it.
    pub fn from_allow_private(allow_private: bool) -> Self {
        if allow_private {
            Self::AllowPrivate
        } else {
            Self::PublicOnly
        }
    }

    /// Decide whether `url` may be dialed under this policy.
    fn check(&self, url: &Url) -> Result<(), RefusedTarget> {
        match self {
            Self::AllowPrivate => Ok(()),
            Self::PublicOnly => check_public_host(url),
        }
    }
}

/// Why [`ForwardTargetPolicy::PublicOnly`] refused an advertised endpoint as a dial target.
///
/// Carried instead of a message string so the refusal reason is a closed set the log line and the
/// tests agree on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefusedTarget {
    /// The URL has no host component at all, so there is nothing to dial.
    MissingHost,
    /// Loopback: `127.0.0.0/8`, `::1`, or a name reserved to loopback by RFC 6761 (`localhost`,
    /// `*.localhost`).
    Loopback,
    /// RFC 1918 private-use: `10/8`, `172.16/12`, `192.168/16`.
    PrivateUse,
    /// Link-local: RFC 3927 `169.254.0.0/16` (which carries cloud instance metadata), RFC 4291
    /// `fe80::/10`, or an RFC 6762 multicast-DNS name (`*.local`).
    LinkLocal,
    /// RFC 4193 unique-local: `fc00::/7`.
    UniqueLocal,
    /// RFC 6598 shared address space: `100.64.0.0/10`, used for carrier-grade NAT and by several
    /// container networks.
    SharedAddressSpace,
    /// "This network": `0.0.0.0/8` (RFC 1122) or the unspecified address `::`.
    Unspecified,
    /// Multicast, broadcast or reserved: never a valid unicast dial target.
    NotUnicast,
}

impl std::fmt::Display for RefusedTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MissingHost => "no host",
            Self::Loopback => "loopback",
            Self::PrivateUse => "private-use (RFC 1918)",
            Self::LinkLocal => "link-local",
            Self::UniqueLocal => "unique-local (RFC 4193)",
            Self::SharedAddressSpace => "shared address space (RFC 6598)",
            Self::Unspecified => "unspecified",
            Self::NotUnicast => "not a unicast address",
        })
    }
}

/// Refuse `url` unless its host could be a public internet peer.
///
/// This is a literal check on the host as written: it never resolves DNS, so it costs nothing on
/// the cache-miss path and cannot be steered by a resolver the advertiser controls. The cost is
/// that a hostname resolving to a private address still passes, which is why the two reserved
/// name suffixes that are *defined* to be local ([`RefusedTarget::Loopback`] for `localhost`,
/// [`RefusedTarget::LinkLocal`] for `.local`) are refused by name. Refusing an arbitrary hostname
/// that merely happens to resolve inside the perimeter needs resolution plus address pinning in
/// the connector (otherwise a rebinding resolver defeats it) and is tracked separately.
///
/// URL host parsing normalizes the alternative IPv4 spellings (`http://2130706433/`,
/// `http://0177.0.0.1/`) into [`Host::Ipv4`] before this sees them, so those reach the same
/// predicate as dotted-quad.
fn check_public_host(url: &Url) -> Result<(), RefusedTarget> {
    url.host().ok_or(RefusedTarget::MissingHost).and_then(|host| match host {
        Host::Ipv4(addr) => check_public_ipv4(addr),
        Host::Ipv6(addr) => check_public_ipv6(addr),
        Host::Domain(name) => check_public_domain(name),
    })
}

/// Refuse an IPv4 literal that is not globally reachable.
fn check_public_ipv4(addr: Ipv4Addr) -> Result<(), RefusedTarget> {
    let [first, second, _, _] = addr.octets();
    if addr.is_loopback() {
        Err(RefusedTarget::Loopback)
    } else if addr.is_private() {
        Err(RefusedTarget::PrivateUse)
    } else if addr.is_link_local() {
        Err(RefusedTarget::LinkLocal)
    } else if first == 0 {
        // 0.0.0.0/8 "this network"; `is_unspecified` covers only 0.0.0.0 itself.
        Err(RefusedTarget::Unspecified)
    } else if first == 100 && (64..128).contains(&second) {
        // 100.64.0.0/10.
        Err(RefusedTarget::SharedAddressSpace)
    } else if first >= 224 {
        // 224.0.0.0/4 multicast, 240.0.0.0/4 reserved, and 255.255.255.255 broadcast.
        Err(RefusedTarget::NotUnicast)
    } else {
        Ok(())
    }
}

/// Refuse an IPv6 literal that is not globally reachable.
///
/// The IPv6-specific blocks are checked before the embedded-IPv4 fallback so `::1` is reported as
/// loopback rather than as the `0.0.0.1` it maps to. The fallback itself is what closes the
/// `http://[::ffff:127.0.0.1]/` spelling of a loopback address, which no IPv6 predicate catches.
fn check_public_ipv6(addr: Ipv6Addr) -> Result<(), RefusedTarget> {
    let [first, ..] = addr.segments();
    if addr.is_loopback() {
        Err(RefusedTarget::Loopback)
    } else if addr.is_unspecified() {
        Err(RefusedTarget::Unspecified)
    } else if first & 0xfe00 == 0xfc00 {
        // fc00::/7.
        Err(RefusedTarget::UniqueLocal)
    } else if first & 0xffc0 == 0xfe80 {
        // fe80::/10.
        Err(RefusedTarget::LinkLocal)
    } else if addr.is_multicast() {
        // ff00::/8.
        Err(RefusedTarget::NotUnicast)
    } else {
        embedded_ipv4(addr).map_or(Ok(()), check_public_ipv4)
    }
}

/// Extract the IPv4 address an IPv6 literal embeds, for the prefixes a translating gateway routes
/// back to that IPv4 destination.
///
/// Covers the two forms [`Ipv6Addr::to_ipv4`] recognizes, IPv4-compatible `::a.b.c.d` and
/// IPv4-mapped `::ffff:a.b.c.d`, plus two it does not:
///
/// - the RFC 6052 NAT64 well-known prefix `64:ff9b::/96`, which a NAT64 gateway translates to the
///   embedded address. This is the default egress path for IPv6-only cloud and container subnets,
///   so without it `http://[64:ff9b::a9fe:a9fe]/` reaches `169.254.169.254` on exactly the
///   deployments where `http://169.254.169.254/` is correctly refused.
/// - the RFC 3056 6to4 prefix `2002::/16`, which carries the IPv4 address in its next 32 bits.
///
/// RFC 6052 also allows network-specific prefixes at other lengths. Those use an operator's own
/// prefix and cannot be enumerated statically, so they are out of reach of a literal check; only
/// the well-known prefix is fixed enough to match.
fn embedded_ipv4(addr: Ipv6Addr) -> Option<Ipv4Addr> {
    let [first, second, third, fourth, fifth, sixth, seventh, eighth] = addr.segments();
    addr.to_ipv4().or_else(|| {
        ((first, second, third, fourth, fifth, sixth) == (0x0064, 0xff9b, 0, 0, 0, 0))
            .then(|| ipv4_from_segments(seventh, eighth))
            .or_else(|| (first == 0x2002).then(|| ipv4_from_segments(second, third)))
    })
}

/// Rebuild an IPv4 address from the two 16-bit segments that carry it inside an IPv6 literal.
fn ipv4_from_segments(high: u16, low: u16) -> Ipv4Addr {
    let [a, b] = high.to_be_bytes();
    let [c, d] = low.to_be_bytes();
    Ipv4Addr::new(a, b, c, d)
}

/// Refuse the two name suffixes that are reserved to resolve locally.
///
/// Every other hostname passes: see [`check_public_host`] for why resolution is out of scope here.
fn check_public_domain(name: &str) -> Result<(), RefusedTarget> {
    // The absolute form (`localhost.`) resolves identically to the relative one.
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    if name == "localhost" || name.ends_with(".localhost") {
        // RFC 6761 section 6.3: reserved, resolves to loopback.
        Err(RefusedTarget::Loopback)
    } else if name == "local" || name.ends_with(".local") {
        // RFC 6762: multicast DNS, resolves only on the local link.
        Err(RefusedTarget::LinkLocal)
    } else {
        Ok(())
    }
}

/// Per-endpoint state the forwarder keeps across batch seals, behind one lock.
#[derive(Default)]
struct EndpointCache {
    /// HTTP providers cached per advertised endpoint so consecutive batch seals reuse the
    /// underlying connection pool instead of paying a fresh TCP+TLS handshake per validator
    /// per seal. Entries for endpoints that are no longer advertised are evicted on each
    /// forward, so the cache stays bounded by the current committee's advertisement set.
    providers: BTreeMap<String, RootProvider>,
    /// Endpoints already refused by [`ForwardTargetPolicy`], so the refusal is logged once per
    /// advertisement instead of once per batch seal. Evicted alongside `providers`, so a
    /// committee member that re-advertises a refused endpoint is logged again.
    ///
    /// Evicting is the deliberate side of the trade: it bounds this set by the current committee's
    /// advertisement set, at the cost of letting a member that toggles its advertisement re-arm
    /// the warning. Never evicting would silence that, but would let a member rotating through
    /// fresh URLs grow the set without bound, which is the worse failure for an unattended node.
    refused: BTreeSet<String>,
}

/// Forwards observer transactions to validators over their advertised JSON-RPC endpoints.
#[derive(Clone)]
pub struct WorkerRpcForwarder {
    /// Spawner used to run the (best-effort, non-blocking) forward on a background task.
    task_spawner: TaskSpawner,
    /// Which advertised hosts this node is willing to dial.
    policy: ForwardTargetPolicy,
    /// Providers and refusals for the currently advertised endpoints.
    cache: Arc<Mutex<EndpointCache>>,
}

impl std::fmt::Debug for WorkerRpcForwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkerRpcForwarder")
    }
}

impl WorkerRpcForwarder {
    /// Create a new forwarder that runs forwards on `task_spawner` and dials only the advertised
    /// hosts `policy` admits.
    pub fn new(task_spawner: TaskSpawner, policy: ForwardTargetPolicy) -> Self {
        Self { task_spawner, policy, cache: Arc::new(Mutex::new(EndpointCache::default())) }
    }

    /// Resolve the advertised endpoints to providers, reusing cached ones where the endpoint
    /// is unchanged. An endpoint the policy refuses, or a malformed URL, is dropped here so the
    /// per-transaction loop only sees usable endpoints. The endpoint string is reparsed into the
    /// exact URL type the provider expects. `RootProvider` needs no fillers: the transactions are
    /// already signed raw bytes, and `send_raw_transaction` submits them as-is.
    ///
    /// The policy is applied on the cache-miss path only, which is where the dial target is turned
    /// into a live provider: a refused endpoint is never inserted, so it can never be served from
    /// the cache on a later seal.
    fn cached_providers(
        &self,
        validator_rpcs: &[(BlsPublicKey, RpcInfo)],
    ) -> BTreeMap<BlsPublicKey, RootProvider> {
        let mut cache = self.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let advertised: BTreeSet<String> =
            validator_rpcs.iter().map(|(_, rpc)| rpc.http.to_string()).collect();
        cache.providers.retain(|url, _| advertised.contains(url));
        cache.refused.retain(|url| advertised.contains(url));
        validator_rpcs
            .iter()
            .filter_map(|(key, rpc)| {
                let url = rpc.http.to_string();
                cache
                    .providers
                    .get(&url)
                    .cloned()
                    .or_else(|| {
                        self.policy
                            .check(&rpc.http)
                            .map_err(|reason| {
                                // Log once per advertisement, not once per seal, so an operator
                                // can tell a refused endpoint from an unreachable one without the
                                // line repeating on every batch.
                                if cache.refused.insert(url.clone()) {
                                    warn!(
                                        target: "worker::forward",
                                        validator = %key,
                                        endpoint = %rpc.http,
                                        %reason,
                                        "refusing to forward to a non-public advertised RPC \
                                         endpoint; set `allow_private_forward_targets` to permit it"
                                    );
                                }
                            })
                            .ok()
                            .and_then(|()| {
                                url.parse()
                                    .map_err(|err| {
                                        debug!(
                                            target: "worker::forward",
                                            endpoint = %rpc.http,
                                            %err,
                                            "skipping validator with an unparseable RPC endpoint"
                                        );
                                    })
                                    .ok()
                            })
                            .map(|parsed| {
                                let provider = RootProvider::new_http(parsed);
                                cache.providers.insert(url.clone(), provider.clone());
                                provider
                            })
                    })
                    .map(|provider| (*key, provider))
            })
            .collect()
    }
}

impl TxnForwarder for WorkerRpcForwarder {
    fn forward_txns(
        &self,
        transactions: Vec<Vec<u8>>,
        committee_slots: Vec<BlsPublicKey>,
        validator_rpcs: Vec<(BlsPublicKey, RpcInfo)>,
    ) {
        let committee_size = committee_slots.len() as u64;
        if transactions.is_empty() || committee_size == 0 || validator_rpcs.is_empty() {
            return;
        }

        let providers = self.cached_providers(&validator_rpcs);
        if providers.is_empty() {
            warn!(
                target: "worker::forward",
                "no usable validator RPC endpoints; dropping forwarded transactions"
            );
            return;
        }
        // Fallback order: every usable endpoint, so a transaction whose owning validator has
        // not advertised (or is unreachable) can still reach the committee.
        let fallbacks: Vec<BlsPublicKey> = providers.keys().cloned().collect();

        self.task_spawner.spawn_task("forward-txns", async move {
            for tx in &transactions {
                // Route by sender so all transactions from one account land on the same
                // validator (matches `submit_txn_if_mine`), then fall back to any endpoint.
                let owner = owning_validator(tx, committee_size, &committee_slots);
                let ordered = owner.into_iter().chain(fallbacks.iter().cloned());

                // Bound the whole fallback chain for this transaction: even if every advertised
                // validator accepts the connection but never answers, one transaction cannot cost
                // more than `FORWARD_TX_BUDGET` before the next transaction proceeds.
                let outcome = timeout(FORWARD_TX_BUDGET, async {
                    let mut tried = BTreeSet::new();
                    let mut result = ForwardOutcome::NoEndpointReached;
                    for key in ordered {
                        if !tried.insert(key) {
                            continue;
                        }
                        let Some(provider) = providers.get(&key) else {
                            continue;
                        };
                        // Bound each validator's round-trip: an endpoint that accepts the
                        // connection but never answers must not stall the whole fallback chain.
                        let disposition = timeout(
                            FORWARD_SEND_TIMEOUT,
                            provider.send_raw_transaction(tx.as_slice()),
                        )
                        .await
                        .map_or_else(
                            |_elapsed| Disposition::TryNext("send timed out".to_string()),
                            |res| res.err().map_or(Disposition::Delivered, classify_error),
                        );

                        match disposition {
                            // Delivered, or a considered rejection every validator would repeat:
                            // either way this transaction is done, so stop the fallback chain.
                            Disposition::Delivered => {
                                result = ForwardOutcome::Delivered;
                                break;
                            }
                            Disposition::Rejected(reason) => {
                                result = ForwardOutcome::Rejected(reason);
                                break;
                            }
                            // Endpoint-local problem (timeout, transport error, full pool): the
                            // transaction's fate is unknown here, so try the next validator.
                            Disposition::TryNext(reason) => {
                                debug!(
                                    target: "worker::forward",
                                    reason = %reason,
                                    "validator RPC did not accept the forwarded transaction; trying next"
                                );
                            }
                        }
                    }
                    result
                })
                .await
                .unwrap_or(ForwardOutcome::NoEndpointReached);

                match outcome {
                    ForwardOutcome::Delivered => {}
                    ForwardOutcome::Rejected(reason) => warn!(
                        target: "worker::forward",
                        reason = %reason,
                        "a validator rejected the forwarded transaction; not retrying other validators"
                    ),
                    ForwardOutcome::NoEndpointReached => warn!(
                        target: "worker::forward",
                        "could not forward transaction to any advertised validator RPC"
                    ),
                }
            }
            Ok(())
        });
    }
}

/// Return the BLS key of the committee slot that owns `tx_bytes`, matching the receiver-side
/// routing in `submit_txn_if_mine`. Returns `None` if the transaction cannot be recovered or the
/// derived slot is out of range.
fn owning_validator(
    tx_bytes: &[u8],
    committee_size: u64,
    committee_slots: &[BlsPublicKey],
) -> Option<BlsPublicKey> {
    let recovered = recover_raw_transaction(tx_bytes).ok()?;
    let sender = recovered.signer();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&sender.as_slice()[0..8]);
    let slot = (u64::from_le_bytes(bytes) % committee_size) as usize;
    committee_slots.get(slot).cloned()
}

/// What to do after one timed attempt to forward a transaction to one validator.
#[derive(Debug, PartialEq, Eq)]
enum Disposition {
    /// The validator accepted the transaction, or it was already in a validator's pool.
    Delivered,
    /// The validator returned a considered rejection of the transaction itself (bad nonce,
    /// underpriced, invalid, wrong fork). No other validator will accept it either.
    Rejected(String),
    /// This endpoint gave no verdict (timeout, transport error, or a transient full pool); the
    /// transaction may still be accepted by another validator.
    TryNext(String),
}

/// Terminal result of forwarding one transaction across the ordered validators.
#[derive(Debug, PartialEq, Eq)]
enum ForwardOutcome {
    /// A validator accepted it (or already had it).
    Delivered,
    /// A validator gave a considered rejection, so the chain was stopped without delivery.
    Rejected(String),
    /// No advertised validator gave a verdict (all timed out or were unreachable).
    NoEndpointReached,
}

/// Classify a failed `send_raw_transaction` by whether the server returned a JSON-RPC error (a
/// verdict about the transaction) or the transport failed (an endpoint problem).
fn classify_error(err: RpcError<TransportErrorKind>) -> Disposition {
    err.as_error_resp().map(|payload| (payload.code, payload.message.to_string())).map_or_else(
        || Disposition::TryNext(err.to_string()),
        |(code, message)| classify_server_error(code, message),
    )
}

/// Classify a server-side JSON-RPC rejection of a forwarded transaction.
fn classify_server_error(code: i64, message: String) -> Disposition {
    if TRANSIENT_RPC_CODES.contains(&code) {
        // A full pool or an internal error is validator-local; another validator may accept it.
        Disposition::TryNext(message)
    } else if message.to_ascii_lowercase().contains(ALREADY_KNOWN_MESSAGE) {
        // Already in a validator's pool: the transaction is delivered.
        Disposition::Delivered
    } else {
        // Every validator shares consensus state, so a considered rejection repeats everywhere.
        Disposition::Rejected(format!("code {code}: {message}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use tn_types::{BlsKeypair, TaskManager};

    /// A forwarder under the shipped default: only public hosts may be dialed.
    fn test_forwarder() -> WorkerRpcForwarder {
        test_forwarder_with(ForwardTargetPolicy::PublicOnly)
    }

    fn test_forwarder_with(policy: ForwardTargetPolicy) -> WorkerRpcForwarder {
        WorkerRpcForwarder::new(TaskManager::default().get_spawner(), policy)
    }

    fn test_key(seed: u8) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::from_seed([seed; 32])).public()
    }

    fn test_rpc(url: &str) -> eyre::Result<RpcInfo> {
        Ok(RpcInfo { http: url.parse()?, ws: None })
    }

    fn cached_urls(forwarder: &WorkerRpcForwarder) -> Vec<String> {
        forwarder
            .cache
            .lock()
            .map(|cache| cache.providers.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn refused_urls(forwarder: &WorkerRpcForwarder) -> Vec<String> {
        forwarder
            .cache
            .lock()
            .map(|cache| cache.refused.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Refuse `url` under the default policy, or fail with the URL that slipped through.
    fn refusal(url: &str) -> eyre::Result<RefusedTarget> {
        let parsed: Url = url.parse()?;
        ForwardTargetPolicy::PublicOnly
            .check(&parsed)
            .err()
            .ok_or_else(|| eyre::eyre!("{url} was accepted as a dial target but must be refused"))
    }

    #[test]
    fn cached_providers_resolves_every_advertised_endpoint() -> eyre::Result<()> {
        // Public hosts: the loopback fixtures this test used before are refused by the default
        // policy, and cache mechanics are what is under test here.
        let forwarder = test_forwarder();
        let rpcs = vec![
            (test_key(1), test_rpc("http://validator-one.example.com:8545")?),
            (test_key(2), test_rpc("http://validator-two.example.com:8546")?),
        ];

        let resolved = forwarder.cached_providers(&rpcs);

        assert_eq!(resolved.len(), 2);
        assert_eq!(cached_urls(&forwarder).len(), 2);
        Ok(())
    }

    #[test]
    fn cached_providers_reuses_cache_and_evicts_stale_endpoints() -> eyre::Result<()> {
        let forwarder = test_forwarder();
        let live = (test_key(1), test_rpc("http://validator-one.example.com:8545")?);
        let stale = (test_key(2), test_rpc("http://validator-two.example.com:8546")?);
        let first = forwarder.cached_providers(&[live.clone(), stale]);
        assert_eq!(first.len(), 2);

        // A repeat advertisement resolves from the cache without growing it; the endpoint
        // that is no longer advertised is evicted.
        let second = forwarder.cached_providers(&[live]);

        assert_eq!(second.len(), 1);
        assert_eq!(
            cached_urls(&forwarder),
            vec!["http://validator-one.example.com:8545/".to_string()]
        );
        Ok(())
    }

    #[test]
    fn cached_providers_refuses_non_public_endpoints_but_keeps_public_ones() -> eyre::Result<()> {
        let forwarder = test_forwarder();
        // The public endpoint shares the slice with the refused ones, so a reject-everything
        // regression cannot pass this assertion.
        let public = (test_key(1), test_rpc("http://validator.example.com:8545")?);
        let rpcs = vec![
            public.clone(),
            (test_key(2), test_rpc("http://169.254.169.254/latest/meta-data/")?),
            (test_key(3), test_rpc("http://127.0.0.1:8545")?),
            (test_key(4), test_rpc("http://10.0.0.1:8545")?),
            (test_key(5), test_rpc("http://192.168.1.1:8545")?),
        ];

        let resolved = forwarder.cached_providers(&rpcs);

        // Only the public validator has a provider, and only its URL is cached.
        assert_eq!(resolved.keys().cloned().collect::<Vec<_>>(), vec![public.0]);
        assert_eq!(cached_urls(&forwarder), vec!["http://validator.example.com:8545/".to_string()]);
        // Every refused endpoint is recorded once so its warning is not repeated per seal.
        assert_eq!(refused_urls(&forwarder).len(), 4);
        Ok(())
    }

    #[test]
    fn cached_providers_never_caches_a_refused_endpoint() -> eyre::Result<()> {
        let forwarder = test_forwarder();
        let rpcs = vec![(test_key(1), test_rpc("http://10.0.0.1:8545")?)];

        // A refused endpoint is never cached, so a later seal re-runs the check rather than
        // serving a provider the policy already rejected.
        assert!(forwarder.cached_providers(&rpcs).is_empty());
        assert!(forwarder.cached_providers(&rpcs).is_empty());
        assert!(cached_urls(&forwarder).is_empty());
        // ... and the refusal is remembered once, so the operator warning does not repeat.
        assert_eq!(refused_urls(&forwarder), vec!["http://10.0.0.1:8545/".to_string()]);

        // Dropping the advertisement clears the record, so a re-advertisement warns again.
        assert!(forwarder.cached_providers(&[]).is_empty());
        assert!(refused_urls(&forwarder).is_empty());
        Ok(())
    }

    #[test]
    fn cached_providers_dials_private_endpoints_under_the_local_opt_in() -> eyre::Result<()> {
        // Single-host and docker-compose deployments advertise loopback legitimately.
        let forwarder = test_forwarder_with(ForwardTargetPolicy::AllowPrivate);
        let rpcs = vec![
            (test_key(1), test_rpc("http://127.0.0.1:8545")?),
            (test_key(2), test_rpc("http://10.0.0.1:8545")?),
        ];

        let resolved = forwarder.cached_providers(&rpcs);

        assert_eq!(resolved.len(), 2);
        assert_eq!(cached_urls(&forwarder).len(), 2);
        assert!(refused_urls(&forwarder).is_empty());
        Ok(())
    }

    #[test]
    fn public_only_refuses_every_non_global_host_form() -> eyre::Result<()> {
        // IPv4 special-purpose blocks.
        assert_eq!(refusal("http://127.0.0.1:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://127.255.255.254/")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://10.0.0.1:8545")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://172.16.0.1/")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://192.168.1.1:8545")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://169.254.169.254/latest/meta-data/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://0.0.0.0:8545")?, RefusedTarget::Unspecified);
        assert_eq!(refusal("http://0.1.2.3/")?, RefusedTarget::Unspecified);
        assert_eq!(refusal("http://100.64.0.1/")?, RefusedTarget::SharedAddressSpace);
        assert_eq!(refusal("http://100.127.255.255/")?, RefusedTarget::SharedAddressSpace);
        assert_eq!(refusal("http://224.0.0.1/")?, RefusedTarget::NotUnicast);
        assert_eq!(refusal("http://255.255.255.255/")?, RefusedTarget::NotUnicast);

        // Alternative IPv4 spellings the URL host parser normalizes before the check sees them.
        assert_eq!(refusal("http://2130706433/")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://0177.0.0.1/")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://0x7f.0.0.1/")?, RefusedTarget::Loopback);

        // IPv6 special-purpose blocks, including the IPv4-mapped spelling of a loopback address
        // that no IPv6 predicate catches.
        assert_eq!(refusal("http://[::1]:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://[::ffff:127.0.0.1]:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://[::ffff:10.0.0.1]/")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://[::ffff:169.254.169.254]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[::]/")?, RefusedTarget::Unspecified);
        assert_eq!(refusal("http://[fc00::1]/")?, RefusedTarget::UniqueLocal);
        assert_eq!(refusal("http://[fd12:3456::1]:8545")?, RefusedTarget::UniqueLocal);
        assert_eq!(refusal("http://[fe80::1]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[febf::1]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[ff02::1]/")?, RefusedTarget::NotUnicast);

        // Prefixes a translating gateway routes back to an embedded IPv4 destination, which
        // `Ipv6Addr::to_ipv4` does not recognize. `64:ff9b::a9fe:a9fe` is the cloud
        // instance-metadata address reached over NAT64.
        assert_eq!(refusal("http://[64:ff9b::a9fe:a9fe]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[64:ff9b::7f00:1]:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://[64:ff9b::a00:1]/")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://[2002:a9fe:a9fe::1]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[2002:7f00:1::1]/")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://[2002:c0a8:101::1]/")?, RefusedTarget::PrivateUse);

        // Names reserved to resolve locally, in relative, absolute and subdomain form.
        assert_eq!(refusal("http://localhost:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://LocalHost:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://localhost./")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://validator.localhost:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://validator.local:8545")?, RefusedTarget::LinkLocal);
        Ok(())
    }

    #[test]
    fn public_only_admits_public_hosts() -> eyre::Result<()> {
        // Public literals and hostnames must still resolve, or the filter is a blanket reject.
        // Each entry sits just outside a refused block, so an off-by-one in a mask shows up here.
        [
            "http://8.8.8.8:8545",
            "https://1.1.1.1/",
            "http://172.32.0.1/",      // just above the 172.16/12 private block
            "http://100.128.0.1/",     // just above the 100.64/10 shared block
            "http://169.253.0.1/",     // just below the 169.254/16 link-local block
            "http://126.255.255.255/", // just below 127/8
            "http://223.255.255.255/", // just below the 224/4 multicast block
            "https://validator.example.com:8545/",
            "http://[2001:4860:4860::8888]/",
            "http://[::ffff:8.8.8.8]/", // IPv4-mapped, but the mapped address is public
            "http://[64:ff9b::808:808]/", // NAT64, but the embedded address is public
            "http://[2002:808:808::1]/", // 6to4, but the embedded address is public
            "http://[64:ff9c::a9fe:a9fe]/", // not the NAT64 well-known prefix: not unmapped
            "http://[fbff::1]/",        // just below fc00::/7
            "http://[fe7f::1]/",        // just below fe80::/10
            "http://not-localhost.example.com/",
            "http://localhosting.example.com/",
        ]
        .into_iter()
        .try_for_each(|url| {
            let parsed: Url = url.parse()?;
            assert_eq!(
                ForwardTargetPolicy::PublicOnly.check(&parsed),
                Ok(()),
                "{url} must be accepted as a dial target"
            );
            eyre::Ok(())
        })
    }

    #[test]
    fn from_allow_private_defaults_to_public_only() {
        assert_eq!(ForwardTargetPolicy::from_allow_private(false), ForwardTargetPolicy::PublicOnly);
        assert_eq!(
            ForwardTargetPolicy::from_allow_private(true),
            ForwardTargetPolicy::AllowPrivate
        );
    }

    #[test]
    fn classify_server_error_maps_reth_rejections() -> eyre::Result<()> {
        // A duplicate ("already known", code -32000) is a delivery, not a failure to retry.
        assert_eq!(
            classify_server_error(-32000, "already known".to_string()),
            Disposition::Delivered
        );
        // A full pool (code -32003) is transient: fall through to the next validator.
        assert_eq!(
            classify_server_error(-32003, "txpool is full".to_string()),
            Disposition::TryNext("txpool is full".to_string())
        );
        // An internal error (code -32603) is also validator-local: try the next validator.
        assert_eq!(
            classify_server_error(-32603, "database error".to_string()),
            Disposition::TryNext("database error".to_string())
        );
        // A considered rejection (nonce too low, code -32000) stops the fallback chain.
        assert!(matches!(
            classify_server_error(-32000, "nonce too low: next nonce 42, tx nonce 41".to_string()),
            Disposition::Rejected(_)
        ));
        Ok(())
    }
}
