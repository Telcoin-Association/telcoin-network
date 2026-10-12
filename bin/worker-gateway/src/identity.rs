//! Client identity: the address a request is attributed to.
//!
//! The per-IP rate limit keys on [`ClientAddr`], and the proxy tells the upstream
//! about the client through `X-Forwarded-For` / `X-Forwarded-Proto`
//! ([`Forwarding`]). The [`identify`] middleware resolves both once per request
//! from the connection's peer address (`ConnectInfo`, injected by the accept
//! loop) and, only when that peer is a trusted proxy, its forwarding headers.
//!
//! The client is the peer unless the peer falls inside one of
//! `--trusted-proxies`. Then it is the right-most `X-Forwarded-For` entry that
//! is not itself a trusted proxy: every header line is read, in order, as one
//! comma-separated chain, the chain is walked from the right, trusted addresses
//! are skipped, and the walk stops at the first untrusted one. Each trusted hop
//! appends the address it received the request from, so that entry is the last
//! one a trusted party vouched for; everything to its left was written by the
//! client or by a hop nobody vouched for, and is never read. An unparsable
//! entry reached before that point, an empty or missing chain, and a chain made
//! only of trusted addresses all fall back to the peer. A request from an
//! untrusted peer is attributed to the peer whatever headers it carries, so a
//! client cannot pick its own rate-limit bucket by sending `X-Forwarded-For`.
//!
//! An IPv4-mapped IPv6 address (`::ffff:a.b.c.d`, the form a dual-stack
//! listener reports an IPv4 peer in) is treated as the IPv4 address it maps,
//! in the peer, in a forwarded entry and in a trusted range alike, so an IPv4
//! proxy matches an IPv4 range on any listener and a mapped entry cannot dodge
//! a trusted-range check. An IPv6 range matches native IPv6 addresses only.
//!
//! With `--proxy-protocol`, a connection from a trusted peer must open with a
//! PROXY protocol v2 header ([`read_proxy_header`]), and the source address it
//! names stands in for the peer: it becomes the connection's `ConnectInfo`, and
//! the rule above then runs against it, so `X-Forwarded-For` is believed only
//! when that source is itself a trusted proxy.

use std::{
    fmt, io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

use axum::{
    extract::{ConnectInfo, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use tokio::io::{AsyncRead, AsyncReadExt as _};

use crate::ratelimit::{mask_v4, mask_v6, PrefixLen};

/// De-facto standard header carrying the client IP chain.
pub(crate) const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

/// De-facto standard header carrying the client-facing scheme.
pub(crate) const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

/// The scheme reported upstream unless a trusted proxy says otherwise: the
/// gateway itself only serves plain HTTP.
const DEFAULT_PROTO: &str = "http";

/// The 12 bytes every PROXY protocol v2 header starts with.
const PROXY_V2_SIGNATURE: [u8; 12] = *b"\r\n\r\n\0\r\nQUIT\n";

/// The version a PROXY protocol header carries in the high nibble of its 13th
/// byte.
const PROXY_V2_VERSION: u8 = 2;

/// The `LOCAL` command: the proxy's own connection (a health check, say),
/// carrying no client address.
const PROXY_V2_LOCAL: u8 = 0x0;

/// The `PROXY` command: a relayed connection, carrying the client's address.
const PROXY_V2_PROXY: u8 = 0x1;

/// Address family and transport byte for TCP over IPv4.
const PROXY_V2_TCP4: u8 = 0x11;

/// Address family and transport byte for TCP over IPv6.
const PROXY_V2_TCP6: u8 = 0x21;

/// Length of the TCP over IPv4 address block: two addresses and two ports.
const PROXY_V2_TCP4_LEN: u16 = 12;

/// Length of the TCP over IPv6 address block: two addresses and two ports.
const PROXY_V2_TCP6_LEN: u16 = 36;

/// The address a request is attributed to, and so the address the per-IP rate
/// limit keys on: the peer, or the client a trusted proxy forwarded the request
/// for (see the module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ClientAddr(pub(crate) IpAddr);

/// What the upstream hop is told about a request's client, resolved together
/// with its [`ClientAddr`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Forwarding {
    /// The resolved client address.
    client: IpAddr,
    /// The connection's peer address.
    peer: IpAddr,
    /// The client-facing scheme (`http` or `https`).
    proto: &'static str,
}

impl Forwarding {
    /// The upstream `X-Forwarded-For`: the peer alone, or the forwarded client
    /// followed by the trusted peer that vouched for it. An inbound chain is
    /// never copied, so an upstream that reads the header sees only addresses
    /// the gateway resolved.
    pub(crate) fn x_forwarded_for(&self) -> HeaderValue {
        let chain = if self.client == self.peer {
            self.peer.to_string()
        } else {
            format!("{}, {}", self.client, self.peer)
        };
        // an address always renders as a valid header value; the fallback only
        // avoids a panic path
        HeaderValue::from_str(&chain).unwrap_or_else(|_| HeaderValue::from_static("unknown"))
    }

    /// The upstream `X-Forwarded-Proto`: what a trusted peer reported when it
    /// named `http` or `https`, else `http`.
    pub(crate) fn x_forwarded_proto(&self) -> HeaderValue {
        HeaderValue::from_static(self.proto)
    }
}

/// The `--trusted-proxies` ranges: the peers whose `X-Forwarded-For` and
/// `X-Forwarded-Proto` the gateway believes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TrustedProxies(Vec<Cidr>);

impl TrustedProxies {
    /// The number of configured ranges.
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether no range is configured.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether a range spans a whole address family (`/0`, or the IPv4-mapped
    /// `::ffff:0:0/96`), which makes every client that can reach the gateway a
    /// trusted proxy.
    pub(crate) fn has_catch_all(&self) -> bool {
        self.0.iter().any(|cidr| cidr.prefix.bits() == 0)
    }

    /// Whether `ip` falls inside a trusted range. An IPv4-mapped IPv6 address
    /// is matched as the IPv4 address it maps.
    pub(crate) fn contains(&self, ip: IpAddr) -> bool {
        let ip = ip.to_canonical();
        self.0.iter().any(|cidr| cidr.contains(ip))
    }

    /// Resolve a request from `peer` carrying `headers` (see the module docs).
    pub(crate) fn resolve(&self, peer: IpAddr, headers: &HeaderMap) -> Forwarding {
        let peer = peer.to_canonical();
        if !self.contains(peer) {
            return Forwarding { client: peer, peer, proto: DEFAULT_PROTO };
        }
        Forwarding {
            client: self.forwarded_client(headers).unwrap_or(peer),
            peer,
            proto: forwarded_proto(headers).unwrap_or(DEFAULT_PROTO),
        }
    }

    /// The right-most untrusted `X-Forwarded-For` entry, or `None` when the
    /// walk reaches an unparsable entry or runs out of entries first.
    fn forwarded_client(&self, headers: &HeaderMap) -> Option<IpAddr> {
        for line in headers.get_all(X_FORWARDED_FOR).iter().rev() {
            // split on raw bytes, so a byte outside visible ascii makes only its
            // own entry unparsable and the entries to its right are still read
            for entry in line.as_bytes().rsplit(|&byte| byte == b',') {
                let ip = parse_forwarded_entry(std::str::from_utf8(entry).ok()?)?;
                if !self.contains(ip) {
                    return Some(ip);
                }
            }
        }
        None
    }
}

impl FromStr for TrustedProxies {
    type Err = CidrError;

    /// Parse a comma-separated list of ranges. A blank list is empty (an unset
    /// variable templated to `""` trusts no one); an empty element inside a
    /// non-blank list is an error.
    fn from_str(list: &str) -> Result<Self, Self::Err> {
        if list.trim().is_empty() {
            return Ok(Self::default());
        }
        list.split(',').map(str::parse).collect::<Result<Vec<_>, _>>().map(Self)
    }
}

/// Why a `--trusted-proxies` element was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CidrError {
    /// An empty element between commas.
    Empty,
    /// The address part is not an IPv4 or IPv6 address.
    Address(String),
    /// The prefix length is not a number from 0 to the family's width.
    Prefix {
        /// The element as written.
        entry: String,
        /// The widest prefix the element's family allows, in bits.
        max: u8,
    },
    /// Bits are set below the prefix, so the element names a host inside a
    /// range rather than the range.
    HostBits {
        /// The element as written.
        entry: String,
        /// The range's network address, which the element probably meant.
        network: IpAddr,
        /// The prefix length, in bits.
        bits: u8,
    },
}

impl fmt::Display for CidrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "empty range between commas"),
            Self::Address(entry) => write!(f, "`{entry}` is not an IPv4 or IPv6 address or range"),
            Self::Prefix { entry, max } => {
                write!(f, "`{entry}` has an invalid prefix length; expected 0 to {max}")
            }
            Self::HostBits { entry, network, bits } => {
                write!(
                    f,
                    "`{entry}` has bits set below its prefix; did you mean `{network}/{bits}`?"
                )
            }
        }
    }
}

impl std::error::Error for CidrError {}

/// Axum middleware: resolve the request's [`ClientAddr`] and [`Forwarding`] and
/// store both in its extensions, for the rate limiter and the proxy.
///
/// It is the outermost router layer, so it runs before the rate limiter. A
/// request without `ConnectInfo` (never the case behind the accept loop) gets
/// neither: the rate limiter then skips its per-IP bucket, as it did before.
pub(crate) async fn identify(
    State(trusted): State<Arc<TrustedProxies>>,
    mut request: Request,
    next: Next,
) -> Response {
    let peer = request.extensions().get::<ConnectInfo<SocketAddr>>().map(|info| info.0.ip());
    if let Some(peer) = peer {
        let forwarding = trusted.resolve(peer, request.headers());
        request.extensions_mut().insert(ClientAddr(forwarding.client));
        request.extensions_mut().insert(forwarding);
    }
    next.run(request).await
}

/// Read the PROXY protocol v2 header a trusted proxy sends ahead of a
/// connection's first byte, returning the client address it carries, or `None`
/// for a `LOCAL` header (the proxy's own connection, which keeps the proxy's
/// address).
///
/// Only the header's own bytes are read, never past the length it declares, so
/// the HTTP request behind it stays in the stream; TLVs after the address
/// block are read and discarded. Anything else is an error and the caller
/// closes the connection: no v2 signature (plain HTTP, or a v1 text header),
/// another version, an unknown command, an address block shorter than its
/// family needs, and a `PROXY` header for anything but TCP over IPv4 or IPv6
/// (a front sending one is misconfigured, and serving the connection as the
/// front's own would merge its clients into the front's rate-limit key). The
/// caller bounds the read with the header read timeout.
pub(crate) async fn read_proxy_header<R>(stream: &mut R) -> io::Result<Option<SocketAddr>>
where
    R: AsyncRead + Unpin,
{
    let mut fixed = [0_u8; 16];
    stream.read_exact(&mut fixed).await?;
    let [signature @ .., version_command, family, len_high, len_low] = fixed;
    if signature != PROXY_V2_SIGNATURE {
        return Err(invalid_proxy_header("missing the PROXY protocol v2 signature"));
    }
    if version_command >> 4 != PROXY_V2_VERSION {
        return Err(invalid_proxy_header("unsupported PROXY protocol version"));
    }
    let len = u16::from_be_bytes([len_high, len_low]);
    let (source, block_len) = match (version_command & 0x0f, family) {
        (PROXY_V2_LOCAL, _) => (None, 0),
        (PROXY_V2_PROXY, PROXY_V2_TCP4) if len >= PROXY_V2_TCP4_LEN => {
            let source = Ipv4Addr::from(stream.read_u32().await?);
            let _destination = stream.read_u32().await?;
            let port = stream.read_u16().await?;
            let _destination_port = stream.read_u16().await?;
            (Some(SocketAddr::from((source, port))), PROXY_V2_TCP4_LEN)
        }
        (PROXY_V2_PROXY, PROXY_V2_TCP6) if len >= PROXY_V2_TCP6_LEN => {
            let source = Ipv6Addr::from(stream.read_u128().await?);
            let _destination = stream.read_u128().await?;
            let port = stream.read_u16().await?;
            let _destination_port = stream.read_u16().await?;
            (Some(SocketAddr::from((source, port))), PROXY_V2_TCP6_LEN)
        }
        (PROXY_V2_PROXY, PROXY_V2_TCP4 | PROXY_V2_TCP6) => {
            return Err(invalid_proxy_header("PROXY protocol address block is too short"));
        }
        (PROXY_V2_PROXY, _) => {
            return Err(invalid_proxy_header("PROXY protocol header is not for TCP over IP"));
        }
        _ => return Err(invalid_proxy_header("unknown PROXY protocol command")),
    };
    // skip the tlvs (and a local header's address block) up to the declared
    // length and no further: the request behind the header must stay unread
    let rest = u64::from(len.saturating_sub(block_len));
    if rest > 0 {
        let skipped =
            tokio::io::copy(&mut (&mut *stream).take(rest), &mut tokio::io::sink()).await?;
        if skipped < rest {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
    }
    Ok(source)
}

/// One trusted range: a network address with its host bits clear, and its
/// prefix length. IPv4-mapped ranges are stored as IPv4.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Cidr {
    /// The network address.
    network: IpAddr,
    /// The prefix length, validated against the network's family.
    prefix: PrefixLen,
}

impl Cidr {
    /// Whether the canonical address `ip` falls inside this range.
    fn contains(&self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => mask_v4(ip, self.prefix) == network,
            (IpAddr::V6(network), IpAddr::V6(ip)) => mask_v6(ip, self.prefix) == network,
            _ => false,
        }
    }
}

impl FromStr for Cidr {
    type Err = CidrError;

    /// Parse `address/prefix` or a bare address (a single host), trimming
    /// surrounding whitespace.
    fn from_str(element: &str) -> Result<Self, Self::Err> {
        let entry = element.trim();
        if entry.is_empty() {
            return Err(CidrError::Empty);
        }
        let (address, bits) = match entry.split_once('/') {
            Some((address, bits)) => (address, Some(bits)),
            None => (entry, None),
        };
        let address =
            address.parse::<IpAddr>().map_err(|_| CidrError::Address(entry.to_string()))?;
        let max = if address.is_ipv4() { 32 } else { 128 };
        let prefix_error = || CidrError::Prefix { entry: entry.to_string(), max };
        let bits = match bits {
            Some(bits) => bits.parse::<u8>().map_err(|_| prefix_error())?,
            None => max,
        };
        let prefix = match address {
            IpAddr::V4(_) => PrefixLen::v4(bits),
            IpAddr::V6(_) => PrefixLen::v6(bits),
        }
        .map_err(|_| prefix_error())?;
        // a mapped range at /96 or longer covers only ipv4-mapped addresses,
        // which are matched as ipv4, so store it as the ipv4 range it is
        let (address, prefix) = match address {
            IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) if prefix.bits() >= 96 => {
                    (IpAddr::V4(v4), PrefixLen::v4(prefix.bits() - 96).map_err(|_| prefix_error())?)
                }
                _ => (address, prefix),
            },
            IpAddr::V4(_) => (address, prefix),
        };
        let network = match address {
            IpAddr::V4(v4) => IpAddr::V4(mask_v4(v4, prefix)),
            IpAddr::V6(v6) => IpAddr::V6(mask_v6(v6, prefix)),
        };
        if network != address {
            return Err(CidrError::HostBits {
                entry: entry.to_string(),
                network,
                bits: prefix.bits(),
            });
        }
        Ok(Self { network, prefix })
    }
}

/// One `X-Forwarded-For` entry as a canonical address: a bare IPv4 or IPv6
/// address, or one with a port (`203.0.113.7:4711`, `[2001:db8::7]:4711`),
/// which some proxies append; the port is dropped.
fn parse_forwarded_entry(entry: &str) -> Option<IpAddr> {
    let entry = entry.trim();
    entry
        .parse::<IpAddr>()
        .or_else(|_| entry.parse::<SocketAddr>().map(|addr| addr.ip()))
        .ok()
        .map(|ip| ip.to_canonical())
}

/// A PROXY protocol header the gateway will not accept.
fn invalid_proxy_header(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

/// The scheme a trusted peer reported in `X-Forwarded-Proto`: the right-most
/// entry of its last line (the one the nearest proxy wrote) when that names
/// `http` or `https`, ignoring case; `None` for anything else.
fn forwarded_proto(headers: &HeaderMap) -> Option<&'static str> {
    let line = headers.get_all(X_FORWARDED_PROTO).iter().next_back()?.to_str().ok()?;
    let proto = line.rsplit(',').next()?.trim();
    if proto.eq_ignore_ascii_case("https") {
        Some("https")
    } else if proto.eq_ignore_ascii_case("http") {
        Some("http")
    } else {
        None
    }
}

/// A PROXY protocol v2 `PROXY` header for a TCP connection from `source` to a
/// loopback destination, with `tlvs` after the address block.
#[cfg(test)]
pub(crate) fn proxy_v2_header(source: SocketAddr, tlvs: &[u8]) -> Vec<u8> {
    let ports = [source.port().to_be_bytes(), 8545_u16.to_be_bytes()].concat();
    let (family, block) = match source {
        SocketAddr::V4(source) => (
            PROXY_V2_TCP4,
            [&source.ip().octets()[..], &Ipv4Addr::LOCALHOST.octets(), &ports].concat(),
        ),
        SocketAddr::V6(source) => (
            PROXY_V2_TCP6,
            [&source.ip().octets()[..], &Ipv6Addr::LOCALHOST.octets(), &ports].concat(),
        ),
    };
    let len = u16::try_from(block.len() + tlvs.len()).expect("header length");
    [&PROXY_V2_SIGNATURE[..], &[0x21, family], &len.to_be_bytes(), &block, tlvs].concat()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trusted(list: &str) -> TrustedProxies {
        list.parse().expect("trusted proxies")
    }

    fn ip(addr: &str) -> IpAddr {
        addr.parse().expect("ip")
    }

    /// Headers holding one `name: value` line per element of `lines`.
    fn headers(name: HeaderName, lines: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for line in lines {
            headers.append(name.clone(), HeaderValue::from_str(line).expect("header value"));
        }
        headers
    }

    /// The client resolved for a request from `peer` forwarding `xff` lines.
    fn client(list: &str, peer: &str, xff: &[&str]) -> IpAddr {
        trusted(list).resolve(ip(peer), &headers(X_FORWARDED_FOR, xff)).client
    }

    #[test]
    fn peer_in_a_trusted_cidr_yields_the_rightmost_untrusted_forwarded_entry() {
        let list = "10.0.0.0/8, 192.0.2.0/24";
        // 10.1.2.3 and 192.0.2.7 are trusted hops and are skipped; 203.0.113.5
        // is the first untrusted entry from the right; 198.51.100.9, to its
        // left, was written by the client and is never read
        let chain = ["198.51.100.9, 203.0.113.5, 192.0.2.7, 10.1.2.3"];
        assert_eq!(client(list, "10.0.0.1", &chain), ip("203.0.113.5"));
        // the right-most entry alone, when it is untrusted
        assert_eq!(client(list, "10.0.0.1", &["198.51.100.9, 203.0.113.5"]), ip("203.0.113.5"));
    }

    #[test]
    fn untrusted_peer_is_the_client_whatever_it_forwards() {
        let list = "10.0.0.0/8";
        assert_eq!(client(list, "198.51.100.1", &["203.0.113.5"]), ip("198.51.100.1"));
        assert_eq!(client("", "198.51.100.1", &["203.0.113.5"]), ip("198.51.100.1"));
        assert_eq!(client(list, "198.51.100.1", &["garbage"]), ip("198.51.100.1"));
    }

    #[test]
    fn forwarded_chain_spans_every_header_line_in_order() {
        let list = "10.0.0.0/8";
        assert_eq!(client(list, "10.0.0.1", &["203.0.113.5", "10.0.0.2"]), ip("203.0.113.5"));
        assert_eq!(
            client(list, "10.0.0.1", &["203.0.113.5", "198.51.100.9, 10.0.0.2"]),
            ip("198.51.100.9")
        );
    }

    #[test]
    fn unparsable_or_empty_chain_falls_back_to_the_peer() {
        let list = "10.0.0.0/8";
        let peer = ip("10.0.0.1");
        assert_eq!(client(list, "10.0.0.1", &[]), peer);
        assert_eq!(client(list, "10.0.0.1", &[""]), peer);
        assert_eq!(client(list, "10.0.0.1", &["203.0.113.5, "]), peer);
        assert_eq!(client(list, "10.0.0.1", &["203.0.113.5, unknown"]), peer);
        assert_eq!(client(list, "10.0.0.1", &["garbage, 10.0.0.2"]), peer);
        // an unparsable entry left of the first untrusted one is never reached
        assert_eq!(client(list, "10.0.0.1", &["garbage, 203.0.113.5"]), ip("203.0.113.5"));
        // an opaque entry reached first falls back to the peer
        let mut opaque = headers(X_FORWARDED_FOR, &["203.0.113.5"]);
        opaque.append(X_FORWARDED_FOR, HeaderValue::from_bytes(b"\xff").expect("opaque value"));
        assert_eq!(trusted(list).resolve(peer, &opaque).client, peer);
        let right = HeaderValue::from_bytes(b"203.0.113.5, \xff").expect("opaque value");
        let mut opaque = HeaderMap::new();
        opaque.append(X_FORWARDED_FOR, right);
        assert_eq!(trusted(list).resolve(peer, &opaque).client, peer);
        // an opaque entry left of the first untrusted one is never reached, even
        // on the same line (a front that appends writes the client's line too)
        let left = HeaderValue::from_bytes(b"\xff, 203.0.113.5").expect("opaque value");
        let mut opaque = HeaderMap::new();
        opaque.append(X_FORWARDED_FOR, left);
        assert_eq!(trusted(list).resolve(peer, &opaque).client, ip("203.0.113.5"));
    }

    #[test]
    fn catch_all_ranges_are_detected() {
        for list in ["0.0.0.0/0", "::/0", "::ffff:0:0/96", "10.0.0.0/8, ::/0"] {
            assert!(trusted(list).has_catch_all(), "{list}");
        }
        assert!(!trusted("10.0.0.0/8, 192.0.2.7, 2001:db8::/32").has_catch_all());
        assert!(!TrustedProxies::default().has_catch_all());
    }

    #[test]
    fn all_trusted_chain_falls_back_to_the_peer() {
        assert_eq!(client("10.0.0.0/8", "10.0.0.1", &["10.0.0.3, 10.0.0.2"]), ip("10.0.0.1"));
    }

    #[test]
    fn forwarded_entry_may_carry_a_port() {
        let list = "10.0.0.0/8";
        assert_eq!(client(list, "10.0.0.1", &["203.0.113.5:4711"]), ip("203.0.113.5"));
        assert_eq!(client(list, "10.0.0.1", &["[2001:db8::7]:4711"]), ip("2001:db8::7"));
        // a trusted hop with a port is still skipped
        assert_eq!(client(list, "10.0.0.1", &["203.0.113.5, 10.0.0.2:80"]), ip("203.0.113.5"));
    }

    #[test]
    fn ipv4_mapped_addresses_match_ipv4_ranges() {
        let list = "10.0.0.0/8";
        // a dual-stack listener's ipv4 proxy is trusted by an ipv4 range
        assert!(trusted(list).contains(ip("::ffff:10.0.0.1")));
        assert_eq!(client(list, "::ffff:10.0.0.1", &["203.0.113.5"]), ip("203.0.113.5"));
        // a mapped trusted hop is skipped, and a mapped client resolves to ipv4
        assert_eq!(
            client(list, "10.0.0.1", &["::ffff:203.0.113.5, ::ffff:10.0.0.2"]),
            ip("203.0.113.5")
        );
        // a mapped range is the ipv4 range it maps
        assert_eq!(trusted("::ffff:10.0.0.0/104"), trusted(list));
        assert_eq!(trusted("::ffff:10.0.0.1"), trusted("10.0.0.1/32"));
        // an ipv6 range matches native ipv6 only
        assert!(!trusted("::/0").contains(ip("10.0.0.1")));
        assert!(!trusted("::/0").contains(ip("::ffff:10.0.0.1")));
        assert!(trusted("::/0").contains(ip("2001:db8::1")));
    }

    #[test]
    fn ranges_match_by_prefix() {
        let list = trusted(" 10.0.0.0/8 ,192.0.2.7, 2001:db8::/32 ");
        assert_eq!(list.len(), 3);
        for inside in ["10.0.0.0", "10.255.255.255", "192.0.2.7", "2001:db8:ffff::1"] {
            assert!(list.contains(ip(inside)), "{inside}");
        }
        for outside in ["11.0.0.0", "192.0.2.8", "2001:db9::1", "::1"] {
            assert!(!list.contains(ip(outside)), "{outside}");
        }
        assert!(trusted("0.0.0.0/0").contains(ip("203.0.113.5")));
        assert_eq!(trusted("  "), TrustedProxies::default());
    }

    #[test]
    fn bad_ranges_are_rejected() {
        for (list, expected) in [
            ("10.0.0.0/33", CidrError::Prefix { entry: "10.0.0.0/33".into(), max: 32 }),
            ("::/129", CidrError::Prefix { entry: "::/129".into(), max: 128 }),
            ("10.0.0.0/", CidrError::Prefix { entry: "10.0.0.0/".into(), max: 32 }),
            ("10.0.0.0/x", CidrError::Prefix { entry: "10.0.0.0/x".into(), max: 32 }),
            ("not-an-ip", CidrError::Address("not-an-ip".into())),
            ("/8", CidrError::Address("/8".into())),
            ("10.0.0/8", CidrError::Address("10.0.0/8".into())),
            ("10.0.0.0/8,,192.0.2.0/24", CidrError::Empty),
            ("10.0.0.0/8,", CidrError::Empty),
            (
                "10.0.0.1/8",
                CidrError::HostBits {
                    entry: "10.0.0.1/8".into(),
                    network: ip("10.0.0.0"),
                    bits: 8,
                },
            ),
            (
                "2001:db8::1/32",
                CidrError::HostBits {
                    entry: "2001:db8::1/32".into(),
                    network: ip("2001:db8::"),
                    bits: 32,
                },
            ),
        ] {
            assert_eq!(list.parse::<TrustedProxies>(), Err(expected), "{list}");
        }
    }

    #[test]
    fn upstream_headers_name_the_peer_unless_it_vouched_for_a_client() {
        let render = |forwarding: Forwarding| {
            (
                forwarding.x_forwarded_for().to_str().expect("xff").to_string(),
                forwarding.x_forwarded_proto().to_str().expect("xfp").to_string(),
            )
        };
        let inbound = headers(X_FORWARDED_FOR, &["6.6.6.6, 7.7.7.7"]);
        let rendered = |list: &str, peer: &str| render(trusted(list).resolve(ip(peer), &inbound));
        // untrusted: the inbound chain is dropped, only the peer is named
        assert_eq!(rendered("", "127.0.0.1"), ("127.0.0.1".into(), "http".into()));
        // trusted: the resolved client, then the peer that vouched for it
        assert_eq!(
            rendered("127.0.0.1", "127.0.0.1"),
            ("7.7.7.7, 127.0.0.1".into(), "http".into())
        );
        // trusted but nothing to vouch for: the peer alone, not twice
        let bare = trusted("127.0.0.1").resolve(ip("127.0.0.1"), &HeaderMap::new());
        assert_eq!(render(bare), ("127.0.0.1".into(), "http".into()));
        // a mapped peer is rendered as ipv4
        assert_eq!(rendered("", "::ffff:127.0.0.1"), ("127.0.0.1".into(), "http".into()));
    }

    #[test]
    fn forwarded_proto_is_copied_only_from_a_trusted_peer() {
        let proto = |list: &str, lines: &[&str]| {
            trusted(list).resolve(ip("10.0.0.1"), &headers(X_FORWARDED_PROTO, lines)).proto
        };
        assert_eq!(proto("", &["https"]), "http");
        assert_eq!(proto("10.0.0.0/8", &["https"]), "https");
        assert_eq!(proto("10.0.0.0/8", &[" HTTPS "]), "https");
        assert_eq!(proto("10.0.0.0/8", &["http"]), "http");
        assert_eq!(proto("10.0.0.0/8", &[]), "http");
        assert_eq!(proto("10.0.0.0/8", &["wss"]), "http");
        assert_eq!(proto("10.0.0.0/8", &["http, https"]), "https");
        assert_eq!(proto("10.0.0.0/8", &["https, ftp"]), "http");
        assert_eq!(proto("10.0.0.0/8", &["http", "https"]), "https");
    }

    fn addr(addr: &str) -> SocketAddr {
        addr.parse().expect("socket addr")
    }

    /// A v2 header with the given version and command byte, family and
    /// transport byte, and address block.
    fn raw_header(version_command: u8, family: u8, block: &[u8]) -> Vec<u8> {
        let len = u16::try_from(block.len()).expect("header length");
        [&PROXY_V2_SIGNATURE[..], &[version_command, family], &len.to_be_bytes(), block].concat()
    }

    /// Run the reader over `bytes`, returning its verdict and the bytes it left
    /// unread.
    async fn read_header(bytes: &[u8]) -> (io::Result<Option<SocketAddr>>, Vec<u8>) {
        let mut reader = bytes;
        let verdict = read_proxy_header(&mut reader).await;
        (verdict, reader.to_vec())
    }

    #[tokio::test]
    async fn proxy_v2_header_yields_the_source_and_leaves_the_request_unread() {
        let request = b"POST / HTTP/1.1\r\n";
        for (source, tlvs) in [
            (addr("198.51.100.1:4711"), &[][..]),
            (addr("[2001:db8::7]:4711"), &[][..]),
            // a noop tlv after the address block is read and discarded
            (addr("198.51.100.1:4711"), &[0x04, 0x00, 0x02, 0xaa, 0xbb][..]),
        ] {
            let bytes = [proxy_v2_header(source, tlvs), request.to_vec()].concat();
            let (verdict, rest) = read_header(&bytes).await;
            assert_eq!(verdict.expect("valid header"), Some(source));
            assert_eq!(rest, request);
        }
    }

    #[tokio::test]
    async fn local_proxy_v2_header_keeps_the_peer() {
        // a local header may still carry an address block, which is skipped
        for block in [&[][..], &[0_u8; 12][..]] {
            let bytes = [raw_header(0x20, 0x00, block), b"GET".to_vec()].concat();
            let (verdict, rest) = read_header(&bytes).await;
            assert_eq!(verdict.expect("valid header"), None);
            assert_eq!(rest, b"GET");
        }
    }

    #[tokio::test]
    async fn unacceptable_proxy_headers_are_errors() {
        let valid = proxy_v2_header(addr("198.51.100.1:4711"), &[]);
        let tcp4_block = &valid[16..];
        let mut truncated_tlv =
            proxy_v2_header(addr("198.51.100.1:4711"), &[0x04, 0, 4, 1, 2, 3, 4]);
        truncated_tlv.truncate(truncated_tlv.len() - 2);
        for (case, bytes) in [
            ("plain http", b"POST / HTTP/1.1\r\nHost: gateway\r\n\r\n".to_vec()),
            ("v1 text header", b"PROXY TCP4 198.51.100.1 127.0.0.1 4711 8545\r\n".to_vec()),
            ("version 1", raw_header(0x11, PROXY_V2_TCP4, tcp4_block)),
            ("unknown command", raw_header(0x22, PROXY_V2_TCP4, tcp4_block)),
            ("udp over ipv4", raw_header(0x21, 0x12, tcp4_block)),
            ("unspecified family", raw_header(0x21, 0x00, &[])),
            ("unix stream", raw_header(0x21, 0x31, &[0; 216])),
            ("short ipv4 block", raw_header(0x21, PROXY_V2_TCP4, &tcp4_block[..8])),
            ("short ipv6 block", raw_header(0x21, PROXY_V2_TCP6, tcp4_block)),
            ("truncated block", valid[..20].to_vec()),
            ("truncated tlv", truncated_tlv),
            ("empty", Vec::new()),
        ] {
            let (verdict, _) = read_header(&bytes).await;
            assert!(verdict.is_err(), "{case}");
        }
    }
}
