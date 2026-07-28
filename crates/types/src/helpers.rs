//! Helpers for starting a node

use parking_lot::Mutex;
use secp256k1::PublicKey;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    net::{TcpListener, UdpSocket},
    sync::OnceLock,
};

/// Number of probe attempts before giving up on finding a free port.
const MAX_RETRIES: u32 = 1000;

/// Ports this process has already handed out.
///
/// This guard is **per-process only**. `cargo nextest` runs each test in its own
/// process, and each process starts with a fresh, empty `USED_PORTS`, so this set
/// cannot stop two concurrent test processes from being handed the same freed port.
/// Cross-process disjointness is provided separately by [`slot_window`], which carves
/// a fixed port band into non-overlapping windows keyed on the nextest slot. Keep both:
/// `USED_PORTS` dedups *within* a process, the slot window dedups *across* processes.
static USED_PORTS: Mutex<Vec<u16>> = Mutex::new(Vec::new());

/// Lowest port of the cross-process coordination band.
///
/// The band sits deliberately **below** the operating system's ephemeral range (Linux
/// 32768-60999, macOS/IANA 49152-65535). The kernel never auto-assigns a port from this
/// band to an unrelated process, so once a test process picks a port here nothing else
/// on the host will race it into the close-then-rebind gap.
const SLOT_BAND_START: u16 = 16384;

/// One past the highest port of the coordination band (exclusive): `16384..32768`.
const SLOT_BAND_END: u16 = 32768;

/// Ports reserved per nextest slot. `256 * 64` slots exactly fills the band, and 256
/// comfortably exceeds the few dozen ports a single e2e test allocates across its nodes.
const SLOT_WINDOW: u16 = 256;

/// Number of disjoint slot windows the band is divided into.
const SLOT_COUNT: u16 = (SLOT_BAND_END - SLOT_BAND_START) / SLOT_WINDOW;

/// Represents the type of socket to create
#[derive(Debug, Clone, Copy)]
pub enum SocketType {
    Tcp,
    Udp,
}

/// Configuration for port discovery
#[derive(Debug, Clone)]
pub struct PortConfig {
    pub host: String,
    pub socket_type: SocketType,
    pub max_retries: u32,
}

/// Error types for port operations
#[derive(Debug)]
pub enum PortError {
    IoError(std::io::Error),
    NoPortsAvailable,
}

/// Get an available port with the specified configuration.
///
/// When running under `cargo nextest` the port is drawn from this process's slot window
/// (see [`slot_window`]) so concurrent test processes never collide. Otherwise the port
/// is a kernel-assigned ephemeral one, preserving the historical behavior for plain
/// `cargo test` and non-test callers.
pub fn get_available_port(config: &PortConfig) -> Result<u16, PortError> {
    nextest_global_slot()
        .map_or_else(|| reserve_ephemeral_port(config), |slot| reserve_windowed_port(config, slot))
}

impl From<std::io::Error> for PortError {
    fn from(error: std::io::Error) -> Self {
        PortError::IoError(error)
    }
}

/// This process's nextest global slot, if it is running under `cargo nextest` and the
/// slot fits the coordination band. Memoized, so a degraded edge below announces itself
/// once per process rather than once per allocated port.
///
/// `NEXTEST_TEST_GLOBAL_SLOT` is unique among the test processes running *at the same
/// time*, which is exactly the set that must not be handed overlapping ports. Returns
/// `None` under a plain `cargo test` or any non-nextest invocation, and for slots beyond
/// the band's capacity (see [`slot_fits_band`]). A value that is present but does not
/// parse also returns `None`, announced on stderr: unlike the absent-var case it means
/// the process *is* under nextest but degrades to ephemeral ports anyway, and that must
/// stay attributable in captured output.
///
/// Child processes spawned by a test (the nodes the e2e tests launch) inherit the
/// variable; a child that ever called this allocator would compute its parent's window
/// and offset with a fresh claim set, a deterministic same-window race. No spawned-node
/// code path calls it today; if one ever does, clear `NEXTEST_*` from the child's
/// environment at the spawn site first.
fn nextest_global_slot() -> Option<u16> {
    static SLOT: OnceLock<Option<u16>> = OnceLock::new();
    *SLOT.get_or_init(|| {
        let raw = std::env::var("NEXTEST_TEST_GLOBAL_SLOT").ok()?;
        raw.parse()
            .ok()
            .or_else(|| {
                eprintln!(
                    "tn_types::helpers: NEXTEST_TEST_GLOBAL_SLOT {raw:?} did not parse as \
                     a slot number; using kernel-assigned ephemeral ports (cross-process \
                     port race possible)"
                );
                None
            })
            .filter(slot_fits_band)
    })
}

/// Whether `slot` fits the coordination band's capacity, announcing on stderr when it
/// does not.
///
/// Wrapping an out-of-capacity slot onto an in-use window would have two concurrent
/// processes probe the same ports deterministically, which is worse than the historical
/// kernel-assigned behavior. So past [`SLOT_COUNT`] concurrently running port-allocating
/// tests, the excess processes degrade to ephemeral ports (rarely-colliding) instead,
/// and say so, keeping any resulting failure attributable.
fn slot_fits_band(slot: &u16) -> bool {
    let fits = *slot < SLOT_COUNT;
    if !fits {
        eprintln!(
            "tn_types::helpers: nextest global slot {slot} exceeds the band's \
             {SLOT_COUNT}-slot capacity; using a kernel-assigned ephemeral port \
             (cross-process port race possible)"
        );
    }
    fits
}

/// The half-open `[start, end)` port window reserved for nextest global `slot`.
///
/// Windows are disjoint for distinct `slot % SLOT_COUNT`, so two test processes with
/// different global slots draw from non-overlapping ranges. [`nextest_global_slot`]
/// rejects slots beyond the band's capacity before this is reached; the modulo only
/// keeps the function total.
fn slot_window(slot: u16) -> (u16, u16) {
    let lane = slot % SLOT_COUNT;
    let start = SLOT_BAND_START + lane * SLOT_WINDOW;
    (start, start + SLOT_WINDOW)
}

/// Reserve a port from this process's slot window, probing each port in turn.
///
/// The probe is bounded by the window itself, not `config.max_retries`; that field
/// governs only the ephemeral allocator.
///
/// Falls back to a kernel-assigned ephemeral port only if the whole window is saturated,
/// so a heavily loaded host degrades to the old behavior instead of failing outright.
/// The fallback reopens the cross-process race this allocator exists to close, so it is
/// announced on stderr: with the e2e retries at 0 a resulting flake fails hard, and the
/// message ties that failure back to the exhausted window instead of leaving a mystery.
fn reserve_windowed_port(config: &PortConfig, slot: u16) -> Result<u16, PortError> {
    let (start, end) = slot_window(slot);
    let offset = run_offset();
    (0..SLOT_WINDOW)
        .map(|i| start + ((offset + i) % SLOT_WINDOW))
        .find(|&port| {
            is_unclaimed(port) && can_bind(&config.host, config.socket_type, port) && claim(port)
        })
        .map_or_else(
            || {
                eprintln!(
                    "tn_types::helpers: slot window {start}..{end} exhausted; falling back to \
                     a kernel-assigned ephemeral port (cross-process port race possible)"
                );
                reserve_ephemeral_port(config)
            },
            Ok,
        )
}

/// Per-run starting offset inside a slot window.
///
/// Slot windows are disjoint within one nextest run, but a second nextest invocation
/// running concurrently on the same host (say, e2e suites in two worktrees) reuses the
/// same slot numbers and therefore the same windows. Probing every window from its base
/// would have both runs race deterministically for the same first port, which is worse
/// than the kernel-assigned behavior this allocator replaces. Starting the probe at an
/// offset hashed from `NEXTEST_RUN_ID` (fixed within a run, different across runs) and
/// wrapping cyclically through the window keeps concurrent runs on separate stretches;
/// they can only collide after one run's allocations wrap into the other's stretch.
fn run_offset() -> u16 {
    std::env::var("NEXTEST_RUN_ID").ok().map_or(0, |id| {
        let mut hasher = DefaultHasher::new();
        id.hash(&mut hasher);
        u16::try_from(hasher.finish() % u64::from(SLOT_WINDOW)).unwrap_or(0)
    })
}

/// Reserve a kernel-assigned ephemeral port (bind `:0`, record it, hand back the number).
///
/// This is the historical allocator: it retains the close-then-rebind gap and is only
/// reached off the nextest path or when a slot window is exhausted.
fn reserve_ephemeral_port(config: &PortConfig) -> Result<u16, PortError> {
    (0..config.max_retries)
        .find_map(|_| {
            bind_ephemeral(&config.host, config.socket_type).ok().filter(|&port| claim(port))
        })
        .ok_or(PortError::NoPortsAvailable)
}

/// Whether `port` is absent from the per-process guard, without inserting it.
///
/// The windowed probe walks the same deterministic sequence on every allocation, so
/// without this pre-check each later allocation would bind-and-drop every port the
/// process already handed out ([`claim`] rejects them only after the probe). That
/// transient bind can land at the exact moment the port's consumer binds for real and
/// fail it; checking the guard first skips owned ports without touching a socket. The
/// gap between this check and [`claim`] is closed by `claim` re-checking under the lock.
fn is_unclaimed(port: u16) -> bool {
    !USED_PORTS.lock().contains(&port)
}

/// Record `port` in the per-process guard, returning `false` if it was already taken.
fn claim(port: u16) -> bool {
    let mut used_ports = USED_PORTS.lock();
    if used_ports.contains(&port) {
        false
    } else {
        used_ports.push(port);
        true
    }
}

/// Whether `port` can currently be bound on `host` for `socket_type`.
///
/// The listener is dropped immediately, so this only reports availability at the instant
/// of the probe. Within the slot band that is safe: no concurrent test process shares the
/// window and the kernel will not auto-assign the port to anything else.
fn can_bind(host: &str, socket_type: SocketType, port: u16) -> bool {
    match socket_type {
        SocketType::Tcp => TcpListener::bind((host, port)).is_ok(),
        SocketType::Udp => UdpSocket::bind((host, port)).is_ok(),
    }
}

/// Bind an ephemeral (`:0`) port and return the number the kernel assigned.
fn bind_ephemeral(host: &str, socket_type: SocketType) -> std::io::Result<u16> {
    match socket_type {
        SocketType::Tcp => Ok(TcpListener::bind((host, 0))?.local_addr()?.port()),
        SocketType::Udp => Ok(UdpSocket::bind((host, 0))?.local_addr()?.port()),
    }
}

/// Convenience function for getting a TCP port
pub fn get_available_tcp_port(host: &str) -> Option<u16> {
    let config = PortConfig {
        host: host.to_string(),
        socket_type: SocketType::Tcp,
        max_retries: MAX_RETRIES,
    };

    get_available_port(&config).ok()
}

/// Convenience function for getting a UDP port
pub fn get_available_udp_port(host: &str) -> Option<u16> {
    let config = PortConfig {
        host: host.to_string(),
        socket_type: SocketType::Udp,
        max_retries: MAX_RETRIES,
    };

    get_available_port(&config).ok()
}

/// Converts a public key into an ethereum address by hashing the encoded public key with
/// keccak256.
pub fn public_key_to_address(public: PublicKey) -> crate::Address {
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = crate::keccak256(&public.serialize_uncompressed()[1..]);
    crate::Address::from_slice(&hash[12..])
}

/// Helper to deconstruct block nonce into epoch and round.
pub fn deconstruct_nonce(nonce: u64) -> (u32, u32) {
    let epoch = (nonce >> 32) as u32; // Extract the upper 32 bits
    let round = nonce as u32; // Extract the lower 32 bits (truncates upper bits)
    (epoch, round)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_windows_are_disjoint_across_slots() {
        for a in 0..SLOT_COUNT {
            for b in (a + 1)..SLOT_COUNT {
                let (a0, a1) = slot_window(a);
                let (b0, b1) = slot_window(b);
                assert!(
                    a1 <= b0 || b1 <= a0,
                    "slot {a} window {a0}..{a1} overlaps slot {b} window {b0}..{b1}"
                );
            }
        }
    }

    #[test]
    fn slot_windows_stay_within_band() {
        for slot in 0..SLOT_COUNT {
            let (start, end) = slot_window(slot);
            assert!(start >= SLOT_BAND_START, "slot {slot} start {start} below band");
            assert!(end <= SLOT_BAND_END, "slot {slot} end {end} above band");
            assert_eq!(end - start, SLOT_WINDOW, "slot {slot} window mis-sized");
        }
    }

    #[test]
    fn slot_windows_wrap_beyond_capacity() {
        assert_eq!(slot_window(0), slot_window(SLOT_COUNT));
        assert_eq!(slot_window(1), slot_window(SLOT_COUNT + 1));
    }

    #[test]
    fn out_of_capacity_slots_are_rejected() {
        assert!(slot_fits_band(&(SLOT_COUNT - 1)), "last in-band slot should fit");
        assert!(!slot_fits_band(&SLOT_COUNT), "first out-of-band slot should be rejected");
    }

    #[test]
    fn windowed_port_comes_from_the_slot_band() {
        let config = PortConfig {
            host: "127.0.0.1".to_string(),
            socket_type: SocketType::Tcp,
            max_retries: MAX_RETRIES,
        };
        // Slot 3 is a real lane a concurrent test process may own; this reservation
        // bind-probes exactly one free port there and drops it, a bounded, one-shot
        // overlap accepted for exercising the real probe path.
        let slot = 3;
        let port = reserve_windowed_port(&config, slot).expect("a free port in the window");
        let (start, end) = slot_window(slot);
        assert!((start..end).contains(&port), "port {port} outside window {start}..{end}");
    }

    #[test]
    fn saturated_window_falls_back_to_an_ephemeral_port() {
        let config = PortConfig {
            host: "127.0.0.1".to_string(),
            socket_type: SocketType::Tcp,
            max_retries: MAX_RETRIES,
        };
        // Claim every port in this slot's window up front, so the windowed probe finds
        // nothing claimable and must fall back. Slot 7 keeps the claims disjoint from the
        // windows other tests in this process draw from, and the claim pre-filter means
        // the probe skips every claimed port without binding, so this test touches no
        // real socket in the band even if a concurrent test process owns slot 7.
        let slot = 7;
        let (start, end) = slot_window(slot);
        (start..end).for_each(|port| {
            claim(port);
        });
        let port = reserve_windowed_port(&config, slot).expect("an ephemeral fallback port");
        assert!(
            !(start..end).contains(&port),
            "port {port} should come from outside the exhausted window {start}..{end}"
        );
    }
}
