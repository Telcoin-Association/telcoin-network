//! Deterministic parameter sweep over the epoch-gated `Committee` wire layout (#554).
//!
//! Every combination of (epoch, authority count, bootstrap-server count, workers per server)
//! below is checked against a byte-length oracle and a byte-exact re-encode. The grid is
//! exhaustive by construction (an iterator product, not sampled) and every value in it — BLS
//! keys, network keys, multiaddrs, execution addresses, rpc endpoints — is derived from a
//! constant plus a slot index, so a failure reproduces byte-for-byte on any machine.
//!
//! The oracle is a length identity against an independently-encoded legacy mirror declared in
//! this file: the gated wire must be exactly the pre-#554 three-field baseline, plus the bytes
//! the open gate adds and nothing else. Both sides of that identity are measured, never
//! hardcoded — the surcharge comes from encoding the parts that moved. A round-trip alone
//! cannot see this: it is a tautology of the same gate on both ends, and would still pass if
//! the encoder unconditionally wrote (or unconditionally stripped) the multi-worker fields.
//!
//! The epoch axis brackets the fork boundary and is derived from
//! [`tn_types::forks::COMMITTEE_WORKERS_FORK_EPOCH`] itself, so arming the fork retargets this
//! sweep with no edit here. `TN_COMMITTEE_WORKERS_FORK_EPOCH` is deliberately never set: the
//! sweep asserts what this build's lane actually does, and an override would only prove that
//! the override works.
//!
//! Grid points the active layout cannot represent are not skipped. Below the fork epoch the
//! wire holds exactly one worker per bootstrap server and carries no worker count at all, so a
//! wider committee must be *unrepresentable*: those points assert the encoder fails closed
//! rather than writing a committee that decodes as single-worker on every node.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};
use tn_types::{
    encode, forks::committee_workers_active, try_decode, Address, Authority, BlsKeypair,
    BlsPublicKey, Committee, CommitteeBuilder, Epoch, NetworkKeypair, P2pNode, RpcInfo,
};

/// Legacy (pre-#554) mirror of one bootstrap server's wire layout: a primary plus a single
/// unprefixed `worker`.
///
/// Derived serde, so it is byte-identical to the `origin/main` `BootstrapServer` derive output BY
/// CONSTRUCTION — same field types, same order, same attributes. #554 left [`P2pNode`] untouched,
/// so the real type is reused for the fields themselves; only the field whose shape changed gets
/// a mirror.
#[derive(serde::Serialize)]
struct LegacyBootstrapMirror {
    /// Mirrors the pre-#554 `BootstrapServer::primary`.
    primary: P2pNode,
    /// Mirrors the pre-#554 `BootstrapServer::worker`, the only worker that shape can hold.
    worker: P2pNode,
}

/// Independently-encoded legacy (three-field) mirror of one grid point's committee content: the
/// same fields in `CommitteeInner`'s declaration order, each bootstrap server in its pre-#554
/// single-worker shape, and NO `num_workers` field — encoded through this derive, never through
/// `Committee`'s hand-written epoch-gated serializer.
///
/// Its encoded length is the sweep's byte-level baseline. `origin/main` interleaved three
/// `#[serde(skip)]` helper fields between these (`authorities_by_id`, `quorum_threshold`,
/// `validity_threshold`); skipped fields never reach the wire, so omitting them preserves both
/// the field set and its order. Names are absent too: bcs writes neither struct nor field names,
/// so only the types and their order decide the bytes.
///
/// For a multi-worker grid point this is the projection the legacy layout *would* have written —
/// the first worker of each server — and [`SweepPoint::measured_gate_delta`] accounts for
/// everything the post-fork wire adds on top of it.
#[derive(serde::Serialize)]
struct LegacyCommitteeMirror {
    /// Mirrors `CommitteeInner::authorities`, whose shape #554 left untouched.
    authorities: BTreeMap<BlsPublicKey, Authority>,
    /// Mirrors `CommitteeInner::epoch`, the only input the layout gate reads.
    epoch: Epoch,
    /// Mirrors `CommitteeInner::bootstrap_servers` in the pre-#554 single-worker shape.
    bootstrap_servers: BTreeMap<BlsPublicKey, LegacyBootstrapMirror>,
}

/// The adiri grid derives three of its four epochs from the fork constant, so arming the fork
/// retargets the sweep without an edit here. That only stays meaningful while at least one epoch
/// below the constant exists: a fork epoch of zero would make every epoch post-fork and silently
/// drop both the legacy arm and every fail-closed point from this lane. Subtracting one from the
/// constant below would also fail const-eval, but this states the requirement rather than leaving
/// it to an underflow.
#[cfg(feature = "adiri")]
const _: () = assert!(tn_types::forks::COMMITTEE_WORKERS_FORK_EPOCH != 0);

/// Epochs swept under `adiri`: the legacy floor, both sides of the fork boundary (the last
/// pre-fork epoch and the first gate-open one), and the top of the epoch range.
///
/// The constant is a dormant `u32::MAX` placeholder today, which collapses the last two entries
/// onto one another — [`sweep_epochs`] dedupes rather than this list hardcoding today's shape.
/// `u32::MAX` is itself gate-open even while the fork is dormant, since the gate is `>=`, so this
/// lane covers both arms before and after arming.
#[cfg(feature = "adiri")]
const SWEEP_EPOCHS: [Epoch; 4] = [
    0,
    tn_types::forks::COMMITTEE_WORKERS_FORK_EPOCH - 1,
    tn_types::forks::COMMITTEE_WORKERS_FORK_EPOCH,
    Epoch::MAX,
];

/// Epochs swept without `adiri`: the multi-worker layout is active from genesis there, so genesis
/// plus one later epoch pin the always-post-fork layout in the default-feature suite.
#[cfg(not(feature = "adiri"))]
const SWEEP_EPOCHS: [Epoch; 2] = [0, 5];

/// Authority counts swept. Two is the floor, not a choice: `CommitteeInner::load` asserts a
/// committee larger than one, so a single-authority committee panics before it can be encoded.
const SWEEP_AUTHORITIES: [u8; 3] = [2, 3, 4];

/// Workers per bootstrap server, which is also the committee's worker count at each grid point:
/// the single worker the legacy layout can express, and two shapes only the post-fork layout can.
const SWEEP_WORKERS_PER_SERVER: [u8; 3] = [1, 2, 3];

/// Tag byte closing every BLS scalar this sweep derives, so its authorities are visibly this
/// file's own fixtures.
const BLS_SCALAR_TAG: u8 = 0x3B;

/// Seed tag marking a sweep primary's network key, so no primary shares a key with a worker.
const PRIMARY_SEED_TAG: u8 = 0xD0;

/// Seed tag marking a sweep worker's network key.
const WORKER_SEED_TAG: u8 = 0xE0;

/// The distinct epochs of [`SWEEP_EPOCHS`].
///
/// Deduped rather than listed: while the adiri fork epoch is the `u32::MAX` placeholder, two of
/// its four entries are the same epoch, and lowering the constant separates them again with no
/// edit here.
fn sweep_epochs() -> BTreeSet<Epoch> {
    SWEEP_EPOCHS.into_iter().collect()
}

/// Deterministic BLS keypair for sweep authority slot `slot`.
///
/// Built from a fixed scalar rather than a seeded rng, so the derived public key — and with it the
/// `authorities` map order and every encoded byte — is stable across `rand` version bumps as well
/// as across runs. The leading bytes stay zero, which keeps the scalar far below the BLS12-381
/// group order and nonzero, the only two values `blst` rejects.
fn sweep_bls_keypair(slot: u8) -> BlsKeypair {
    let mut scalar = [0_u8; 32];
    scalar[30] = slot;
    scalar[31] = BLS_SCALAR_TAG;
    BlsKeypair::from_bytes(&scalar).expect("sweep bls scalar is a valid private key")
}

/// The BLS public key of sweep authority slot `slot`.
fn sweep_authority_key(slot: u8) -> BlsPublicKey {
    *sweep_bls_keypair(slot).public()
}

/// A fixed 32-byte ed25519 secret seed identifying one sweep node.
fn sweep_seed(tag: u8, authority: u8, worker: u8) -> [u8; 32] {
    let mut seed = [0_u8; 32];
    seed[0] = tag;
    seed[1] = authority;
    seed[2] = worker;
    seed
}

/// A [`P2pNode`] from a fixed ed25519 seed and port.
///
/// ed25519 secret keys *are* 32-byte seeds, so a fixed seed yields a fixed public key with no rng
/// in the path; the multiaddr comes from a literal rather than an OS-assigned port.
fn sweep_p2p_node(seed: [u8; 32], port: u16, rpc: Option<RpcInfo>) -> P2pNode {
    P2pNode {
        network_address: format!("/ip4/127.0.0.1/udp/{port}/quic-v1")
            .parse()
            .expect("sweep multiaddr parses"),
        network_key: NetworkKeypair::ed25519_from_bytes(seed)
            .expect("a 32-byte array is a valid ed25519 secret seed")
            .public()
            .clone()
            .into(),
        rpc,
    }
}

/// The primary node of sweep authority `authority`. Primaries never advertise rpc.
fn sweep_primary_node(authority: u8) -> P2pNode {
    sweep_p2p_node(sweep_seed(PRIMARY_SEED_TAG, authority, 0), 49_000 + u16::from(authority), None)
}

/// Worker `worker` of sweep authority `authority`.
///
/// The first worker of the first authority advertises an rpc endpoint and no other worker does, so
/// both arms of [`P2pNode::rpc`]'s `Option` appear on the wire at every grid point that carries a
/// bootstrap server at all.
fn sweep_worker_node(authority: u8, worker: u8) -> P2pNode {
    let rpc = (authority == 0 && worker == 0).then(|| RpcInfo {
        http: "https://sweep0.example.com:8545/".parse().expect("sweep http url"),
        ws: Some("wss://sweep0.example.com:8546/".parse().expect("sweep ws url")),
    });
    sweep_p2p_node(
        sweep_seed(WORKER_SEED_TAG, authority, worker),
        50_000 + u16::from(authority) * 8 + u16::from(worker),
        rpc,
    )
}

/// One point of the sweep grid: the committee shape to build and the epoch to build it at.
///
/// `Copy` and `Debug` so every assertion below can name the exact point that failed without
/// threading the four axes through each message.
#[derive(Clone, Copy, Debug)]
struct SweepPoint {
    /// The committee's epoch, the only input the wire-layout gate reads.
    epoch: Epoch,
    /// Number of authorities. At least two: `CommitteeInner::load` asserts a committee larger
    /// than one.
    authorities: u8,
    /// Number of bootstrap servers, attached to authority slots `0..bootstrap_servers`.
    ///
    /// Swept independently of [`Self::authorities`] because the two maps are independent on the
    /// wire: a committee may carry fewer bootstrap hints than it has authorities. Letting the two
    /// lengths differ gives the maps distinct ULEB128 length prefixes, so a layout that read one
    /// where the other belongs would not line up. Zero is included: an empty server map leaves
    /// the trailing worker count as the gate's only surcharge.
    bootstrap_servers: u8,
    /// Workers each bootstrap server advertises, which is also the committee's worker count.
    workers_per_server: u8,
}

impl SweepPoint {
    /// The committee-level worker count this point describes.
    fn num_workers(self) -> NonZeroUsize {
        NonZeroUsize::new(usize::from(self.workers_per_server))
            .expect("a grid point runs at least one worker per server")
    }

    /// The `(slot, bls key)` pairs of this point's authorities.
    fn authority_slots(self) -> impl Iterator<Item = (u8, BlsPublicKey)> {
        (0..self.authorities).map(|slot| (slot, sweep_authority_key(slot)))
    }

    /// The `(slot, bls key)` pairs that carry a bootstrap server.
    ///
    /// Servers attach to the lowest authority slots, so this is always a prefix of
    /// [`Self::authority_slots`] and every server is keyed off an authority that exists.
    fn bootstrap_slots(self) -> impl Iterator<Item = (u8, BlsPublicKey)> {
        self.authority_slots().take(usize::from(self.bootstrap_servers))
    }

    /// The worker nodes of the bootstrap server on authority slot `slot`.
    fn worker_nodes(self, slot: u8) -> Vec<P2pNode> {
        (0..self.workers_per_server).map(|worker| sweep_worker_node(slot, worker)).collect()
    }

    /// Build the [`Committee`] this point describes.
    fn committee(self) -> Committee {
        let mut builder = CommitteeBuilder::new(self.epoch).with_num_workers(self.num_workers());
        self.authority_slots().for_each(|(slot, key)| {
            let execution_address = Address::repeat_byte(slot);
            if slot < self.bootstrap_servers {
                builder.add_authority_and_bootstrap(
                    key,
                    sweep_primary_node(slot),
                    self.worker_nodes(slot),
                    execution_address,
                );
            } else {
                builder.add_authority(key, execution_address);
            }
        });
        builder.build()
    }

    /// Build the legacy mirror this point projects onto.
    ///
    /// Reaches the mirror straight from the slot-derived keys and nodes, never through
    /// [`Self::committee`], so a bug in the gated encoder cannot make the two sides of the length
    /// identity agree.
    fn legacy_mirror(self) -> LegacyCommitteeMirror {
        let authorities = self
            .authority_slots()
            .map(|(slot, key)| (key, Authority::new_for_test(key, Address::repeat_byte(slot))))
            .collect();
        let bootstrap_servers = self
            .bootstrap_slots()
            .map(|(slot, key)| {
                let mirror = LegacyBootstrapMirror {
                    primary: sweep_primary_node(slot),
                    worker: sweep_worker_node(slot, 0),
                };
                (key, mirror)
            })
            .collect();
        LegacyCommitteeMirror { authorities, epoch: self.epoch, bootstrap_servers }
    }

    /// The exact byte count an open gate adds to this point's wire, measured by encoding the parts
    /// that moved rather than by arithmetic over them.
    ///
    /// Three things appear post-fork that the legacy baseline does not carry: the ULEB128 length
    /// prefix of every bootstrap server's worker list, every worker after the first (the legacy
    /// shape holds exactly one), and the trailing committee-level `num_workers`. Each is measured
    /// from `encode` of the part itself, so a change in how bcs frames a sequence or sizes a
    /// `NonZeroUsize` moves this expectation along with the wire instead of failing against a
    /// hardcoded constant.
    fn measured_gate_delta(self) -> usize {
        let worker_lists: usize = self
            .bootstrap_slots()
            .map(|(slot, _)| {
                let workers = self.worker_nodes(slot);
                let elements: usize = workers.iter().map(|worker| encode(worker).len()).sum();
                // bcs frames a sequence as its ULEB128 length followed by the bare elements, so
                // the prefix is whatever the whole is longer than the sum of its parts
                let length_prefix = encode(&workers).len() - elements;
                let workers_after_first: usize =
                    workers.iter().skip(1).map(|worker| encode(worker).len()).sum();
                length_prefix + workers_after_first
            })
            .sum();
        encode(&self.num_workers()).len() + worker_lists
    }
}

/// Check one grid point: the fail-closed refusal where the layout cannot hold the value, and
/// otherwise the byte-length oracle against the legacy mirror plus a byte-exact re-encode.
fn assert_committee_grid_point(point: SweepPoint) {
    let committee = point.committee();
    let gate_open = committee_workers_active(point.epoch);

    // fail closed: the pre-fork layout has no field for a worker count and holds exactly one
    // worker per bootstrap server, so a wider committee must be unrepresentable rather than
    // truncated on its way to a pack. bcs directly here rather than `encode`, which panics on a
    // serializer error.
    if !gate_open && point.workers_per_server > 1 {
        assert!(
            bcs::to_bytes(&committee).is_err(),
            "grid point {point:?}: the pre-fork layout must refuse a committee it cannot hold"
        );
        return;
    }

    let bytes = encode(&committee);
    let mirror_bytes = encode(&point.legacy_mirror());

    // byte-level oracle, independent of the serde gate: the wire must be exactly the legacy
    // three-field baseline plus the measured multi-worker surcharge when (and only when) the gate
    // is open for this epoch. Holds in both cfg lanes — without `adiri` every epoch is gate-open,
    // so the expectation is baseline + surcharge everywhere.
    let expected_len = mirror_bytes.len() + if gate_open { point.measured_gate_delta() } else { 0 };
    assert_eq!(
        expected_len,
        bytes.len(),
        "grid point {point:?} wire length must equal the legacy baseline plus exactly the \
         epoch-gated multi-worker bytes"
    );

    // pre-fork the mirror describes the whole wire, not just its length: a pack written by a
    // pre-#554 binary is byte-identical to what this build writes for that epoch.
    if !gate_open {
        assert_eq!(
            mirror_bytes, bytes,
            "grid point {point:?} pre-fork bytes must be the legacy layout, not merely its length"
        );
    }

    let decoded: Committee = try_decode(&bytes).expect("a committee decodes its own wire bytes");
    assert_eq!(
        bytes,
        encode(&decoded),
        "grid point {point:?} must re-encode byte-exactly: a pack survives decode and re-append"
    );
    assert_eq!(decoded, committee, "grid point {point:?} decoded to a different committee");
    // `Committee`'s PartialEq deliberately ignores bootstrap servers, so compare those too: they
    // are the half of this layout change the equality above cannot see.
    assert_eq!(
        decoded.bootstrap_servers(),
        committee.bootstrap_servers(),
        "grid point {point:?} lost or reshaped a bootstrap server across the round trip"
    );
    assert_eq!(
        usize::from(point.workers_per_server),
        decoded.number_of_workers(),
        "grid point {point:?} worker count did not survive the round trip"
    );
}

/// The epoch grid must straddle the gate the way this build's lane requires, or the sweep passes
/// vacuously: an adiri grid with no pre-fork epoch silently drops the legacy arm and every
/// fail-closed point, and a non-adiri grid is meant to be gate-open throughout.
///
/// Also the tripwire for an ambient `TN_COMMITTEE_WORKERS_FORK_EPOCH`, which this suite never sets
/// but a test-utils build would still honor from the environment.
#[test]
fn test_committee_sweep_grid_brackets_the_fork() {
    let gate_states: BTreeSet<bool> =
        sweep_epochs().into_iter().map(committee_workers_active).collect();

    #[cfg(feature = "adiri")]
    assert_eq!(
        BTreeSet::from([false, true]),
        gate_states,
        "the adiri grid must cover both gate states; is TN_COMMITTEE_WORKERS_FORK_EPOCH set in \
         the environment, or has the fork been armed at epoch 0?"
    );

    #[cfg(not(feature = "adiri"))]
    assert_eq!(
        BTreeSet::from([true]),
        gate_states,
        "the multi-worker layout is active from genesis without `adiri`, so every swept epoch \
         must be gate-open; is TN_COMMITTEE_WORKERS_FORK_EPOCH set in the environment?"
    );
}

/// The exhaustive grid: every `(epoch, authorities, bootstrap_servers, workers_per_server)`
/// combination of the sweep axes above, via an iterator product (no sampling, no loop keywords).
///
/// Bootstrap-server counts run `0..=authorities` rather than a fixed list, since a server attaches
/// to an authority slot and a committee cannot carry more hints than it has authorities.
#[test]
fn test_committee_layout_parameter_sweep() {
    sweep_epochs().into_iter().for_each(|epoch| {
        SWEEP_AUTHORITIES.iter().for_each(|&authorities| {
            (0..=authorities).for_each(|bootstrap_servers| {
                SWEEP_WORKERS_PER_SERVER.iter().for_each(|&workers_per_server| {
                    assert_committee_grid_point(SweepPoint {
                        epoch,
                        authorities,
                        bootstrap_servers,
                        workers_per_server,
                    });
                });
            });
        });
    });
}
