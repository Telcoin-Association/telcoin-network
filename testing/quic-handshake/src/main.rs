//! Current-provider QUIC TLS profiling with unchanged libp2p authentication.

use libp2p_identity::{Keypair, PeerId};
use libp2p_tls::profile;
use rustls::{quic, ClientConfig, HandshakeKind, NamedGroup, ServerConfig};
use serde_json::json;
use std::{collections::BTreeMap, fmt, sync::Arc, time::Instant};

/// Failures invalidate a run rather than becoming timing samples.
#[derive(Debug)]
enum Error {
    /// TLS rejected the handshake.
    Tls(rustls::Error),
    /// Certificate generation failed.
    Certificate(libp2p_tls::certificate::GenError),
    /// An experiment violated a required invariant.
    Invariant(&'static str),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tls(error) => write!(f, "TLS: {error}"),
            Self::Certificate(error) => write!(f, "certificate: {error}"),
            Self::Invariant(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for Error {}
impl From<rustls::Error> for Error {
    fn from(error: rustls::Error) -> Self {
        Self::Tls(error)
    }
}
impl From<libp2p_tls::certificate::GenError> for Error {
    fn from(error: libp2p_tls::certificate::GenError) -> Self {
        Self::Certificate(error)
    }
}

/// Require a protocol invariant without imposing a timing threshold.
fn require(condition: bool, message: &'static str) -> Result<(), Error> {
    condition.then_some(()).ok_or(Error::Invariant(message))
}

/// Experiments are confined to this executable.
#[derive(Clone, Copy)]
enum Scenario {
    /// Default groups on both sides, with full handshakes.
    Default,
    /// Reverse only the server's default group order.
    ReverseServer,
    /// Reverse only the client's default group order.
    ReverseClient,
    /// Offer exactly one client group to the default server.
    Single(NamedGroup),
    /// Reuse stock session caches for honest reconnects.
    Reconnect,
    /// Offer only a group outside the default server's membership.
    Incompatible,
    /// Require a server identity other than the real server.
    WrongPeer,
}
impl Scenario {
    /// Parse a stable result label.
    fn parse(name: &str) -> Result<Self, Error> {
        match name {
            "default" => Ok(Self::Default),
            "reverse-server" => Ok(Self::ReverseServer),
            "reverse-client" => Ok(Self::ReverseClient),
            "x25519" => Ok(Self::Single(NamedGroup::X25519)),
            "p256" => Ok(Self::Single(NamedGroup::secp256r1)),
            "p384" => Ok(Self::Single(NamedGroup::secp384r1)),
            "hybrid" => Ok(Self::Single(NamedGroup::X25519MLKEM768)),
            "reconnect" => Ok(Self::Reconnect),
            "incompatible" => Ok(Self::Incompatible),
            "wrong-peer" => Ok(Self::WrongPeer),
            _ => Err(Error::Invariant("unknown scenario")),
        }
    }
}

/// Configurations preserve stock certificate and session lifetimes.
struct Experiment {
    /// Client configuration including expected server identity.
    client: Arc<ClientConfig>,
    /// Listener configuration with its session cache.
    server: Arc<ServerConfig>,
    /// Identity the listener must authenticate.
    client_peer: PeerId,
    /// Required group for a successful negotiation.
    expected_group: NamedGroup,
    /// Selected experiment.
    scenario: Scenario,
}
impl Experiment {
    /// Change only experiment groups and full-handshake cache use.
    fn new(scenario: Scenario) -> Result<Self, Error> {
        let client_key = Keypair::generate_ed25519();
        let server_key = Keypair::generate_ed25519();
        let defaults: Vec<_> = rustls::crypto::aws_lc_rs::default_provider()
            .kx_groups
            .iter()
            .map(|group| group.name())
            .collect();
        let reversed: Vec<_> = defaults.iter().copied().rev().collect();
        let first = *defaults.first().ok_or(Error::Invariant("empty provider"))?;
        let (client_groups, server_groups, expected_group) = match scenario {
            Scenario::Default | Scenario::Reconnect | Scenario::WrongPeer => (None, None, first),
            Scenario::ReverseServer => (None, Some(reversed), first),
            Scenario::ReverseClient => {
                let first = *reversed.first().ok_or(Error::Invariant("empty provider"))?;
                (Some(reversed), None, first)
            }
            Scenario::Single(group) => (Some(vec![group]), None, group),
            Scenario::Incompatible => (Some(vec![NamedGroup::MLKEM768]), None, first),
        };
        let expected_peer = if matches!(scenario, Scenario::WrongPeer) {
            Keypair::generate_ed25519().public().to_peer_id()
        } else {
            server_key.public().to_peer_id()
        };
        profile::set_groups(client_groups.clone());
        let mut client = libp2p_tls::make_client_config(&client_key, Some(expected_peer))?;
        require(
            client.crypto_provider().kx_groups.iter().map(|group| group.name()).eq(client_groups
                .as_ref()
                .unwrap_or(&defaults)
                .iter()
                .copied()),
            "client experiment groups were not applied",
        )?;
        if !matches!(scenario, Scenario::Reconnect) {
            client.resumption = rustls::client::Resumption::disabled();
        }
        profile::set_groups(server_groups.clone());
        let server = libp2p_tls::make_server_config(&server_key)?;
        require(
            server.crypto_provider().kx_groups.iter().map(|group| group.name()).eq(server_groups
                .as_ref()
                .unwrap_or(&defaults)
                .iter()
                .copied()),
            "server experiment groups were not applied",
        )?;
        profile::set_groups(None);
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
            client_peer: client_key.public().to_peer_id(),
            expected_group,
            scenario,
        })
    }

    /// Drain all flights and post-handshake tickets before the next reconnect.
    fn handshake(&self, sample: usize) -> Result<serde_json::Value, Error> {
        let start = Instant::now();
        let mut client: quic::Connection = quic::ClientConnection::new(
            self.client.clone(),
            quic::Version::V1,
            "libp2p.invalid".try_into().map_err(|_| Error::Invariant("invalid DNS name"))?,
            vec![],
        )?
        .into();
        let mut server: quic::Connection =
            quic::ServerConnection::new(self.server.clone(), quic::Version::V1, vec![])?.into();
        let mut measured = Measurement::default();
        // A finite flight bound detects a broken pump, without timing assumptions.
        let result = (0..16).try_fold(false, |done, _| {
            if done {
                Ok(true)
            } else {
                let outbound = pump(&mut client, &mut server, Some(&mut measured))?;
                let inbound = pump(&mut server, &mut client, None)?;
                Ok::<_, Error>(outbound == 0 && inbound == 0)
            }
        });
        if matches!(self.scenario, Scenario::Incompatible | Scenario::WrongPeer) {
            let error =
                result.err().ok_or(Error::Invariant("negative case negotiated successfully"))?;
            let correct = match self.scenario {
                Scenario::Incompatible => {
                    matches!(error, Error::Tls(rustls::Error::PeerIncompatible(_)))
                }
                Scenario::WrongPeer => {
                    matches!(error, Error::Tls(rustls::Error::InvalidCertificate(_)))
                }
                Scenario::Default
                | Scenario::ReverseServer
                | Scenario::ReverseClient
                | Scenario::Single(_)
                | Scenario::Reconnect => false,
            };
            require(correct, "unexpected rejection class")?;
            Ok(json!({"sample": sample, "rejected": error.to_string()}))
        } else {
            require(
                result? && !client.is_handshaking() && !server.is_handshaking(),
                "handshake did not complete",
            )?;
            let group = server.negotiated_key_exchange_group().map(|group| group.name());
            require(
                group == Some(self.expected_group)
                    && client.negotiated_key_exchange_group().map(|group| group.name()) == group,
                "negotiated group differed from required experiment",
            )?;
            let expected_kind = if matches!(self.scenario, Scenario::Reconnect) && sample > 0 {
                HandshakeKind::Resumed
            } else {
                HandshakeKind::Full
            };
            require(
                client.handshake_kind() == Some(expected_kind)
                    && server.handshake_kind() == Some(expected_kind),
                "unexpected full/resumed handshake kind",
            )?;
            let client_export = client.export_keying_material([0; 32], b"profile", None)?;
            let server_export = server.export_keying_material([0; 32], b"profile", None)?;
            require(client_export == server_export, "authenticated exporters disagree")?;
            profile::take_stats();
            let peer_start = Instant::now();
            let certificate = server
                .peer_certificates()
                .and_then(|certs| certs.first())
                .ok_or(Error::Invariant("missing authenticated client certificate"))?;
            let peer = libp2p_tls::certificate::parse(certificate)
                .map_err(|_| Error::Invariant("authenticated certificate failed revalidation"))?
                .peer_id();
            let peer_ns = peer_start.elapsed().as_nanos();
            measured.add_phases();
            require(peer == self.client_peer, "wrong authenticated client identity")?;
            Ok(json!({
                "sample": sample, "group": format!("{:?}", self.expected_group),
                "kind": format!("{expected_kind:?}"), "wall_ns": start.elapsed().as_nanos(),
                "server_first_flight_ns": measured.first_flight_ns,
                "server_read_ns": measured.server_read_ns, "server_peer_id_ns": peer_ns,
                "server_phases_count_ns": measured.phases,
            }))
        }
    }
}

/// Listener timings exclude client verification and configuration generation.
#[derive(Default)]
struct Measurement {
    /// First ClientHello processing, before a client certificate is available.
    first_flight_ns: Option<u128>,
    /// Total listener TLS input processing time, including the first flight.
    server_read_ns: u128,
    /// Verification phase call counts and elapsed nanoseconds.
    phases: BTreeMap<&'static str, (u64, u128)>,
}
impl Measurement {
    /// Merge the latest measurements after listener work.
    fn add_phases(&mut self) {
        profile::take_stats().into_iter().for_each(|(name, (count, ns))| {
            let entry = self.phases.entry(name).or_default();
            entry.0 += count;
            entry.1 += ns;
        });
    }
}

/// Transfer encryption levels separately, including post-handshake tickets.
fn pump(
    from: &mut quic::Connection,
    to: &mut quic::Connection,
    mut measured: Option<&mut Measurement>,
) -> Result<usize, Error> {
    (0..8).try_fold(0, |total, _| {
        let mut bytes = Vec::new();
        let _keys = from.write_hs(&mut bytes);
        if bytes.is_empty() {
            Ok(total)
        } else {
            profile::take_stats();
            let start = Instant::now();
            let result = to.read_hs(&bytes);
            let elapsed = start.elapsed().as_nanos();
            measured.as_mut().into_iter().for_each(|timings| {
                timings.first_flight_ns.get_or_insert(elapsed);
                timings.server_read_ns += elapsed;
                timings.add_phases();
            });
            result?;
            Ok(total + bytes.len())
        }
    })
}

/// Measure provider key exchange separately from certificates and TLS bookkeeping.
fn key_exchange(name: &str, samples: usize) -> Result<(), Error> {
    let named_group = match Scenario::parse(name)? {
        Scenario::Single(group) => Ok(group),
        Scenario::Default
        | Scenario::ReverseServer
        | Scenario::ReverseClient
        | Scenario::Reconnect
        | Scenario::Incompatible
        | Scenario::WrongPeer => Err(Error::Invariant("key exchange requires a single group")),
    }?;
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let group = provider
        .kx_groups
        .iter()
        .find(|group| group.name() == named_group)
        .ok_or(Error::Invariant("group is outside the current provider defaults"))?;
    (0..samples).try_for_each(|sample| {
        let start = Instant::now();
        let client = group.start()?;
        let client_start_ns = start.elapsed().as_nanos();
        let start = Instant::now();
        let server = group.start_and_complete(client.pub_key())?;
        let server_exchange_ns = start.elapsed().as_nanos();
        let start = Instant::now();
        let client_secret = client.complete(&server.pub_key)?;
        let client_complete_ns = start.elapsed().as_nanos();
        require(server.group == named_group, "provider used a different group")?;
        require(
            client_secret.secret_bytes() == server.secret.secret_bytes(),
            "key exchange secrets disagree",
        )?;
        println!(
            "{}",
            json!({"scenario": format!("kx-{name}"), "result": {
                "sample": sample, "group": format!("{named_group:?}"),
                "client_start_ns": client_start_ns, "server_exchange_ns": server_exchange_ns,
                "client_complete_ns": client_complete_ns,
            }})
        );
        Ok(())
    })
}

/// Emit raw samples; failures terminate the run.
fn main() -> Result<(), Error> {
    let name =
        std::env::args().nth(1).ok_or(Error::Invariant("usage: profile SCENARIO SAMPLES"))?;
    let samples = std::env::args()
        .nth(2)
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|count| (2..=100_000).contains(count))
        .ok_or(Error::Invariant("samples must be 2..100000"))?;
    name.strip_prefix("kx-").map_or_else(
        || {
            let experiment = Experiment::new(Scenario::parse(&name)?)?;
            (0..samples).try_for_each(|sample| {
                let result = experiment.handshake(sample)?;
                println!("{}", json!({"scenario": name, "result": result}));
                Ok(())
            })
        },
        |group| key_exchange(group, samples),
    )
}
