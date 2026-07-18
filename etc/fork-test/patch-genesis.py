#!/usr/bin/env python3
"""Patch a freshly-generated telcoin-network genesis.yaml so the ConsensusRegistry fork can fire.

A fresh local genesis DEPLOYS THE CURRENT (post-fork) ConsensusRegistry bytecode and snapshots its
storage (see crates/tn-reth/src/lib.rs). The fork's fail-closed swap gate refuses to run unless the
deployed registry carries the pinned PRE-fork code hash (CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH in
crates/types/src/forks.rs), so on a fresh genesis the fork would abort. This script splices two
accounts from the committed testnet genesis into the freshly-generated one:

  1. ConsensusRegistry `code` (0x07E1..17e1) -> the pinned pre-fork runtime bytecode
     (keccak == CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH). ONLY `code` is replaced; the freshly
     generated `storage` is kept. migrateValidatorSets() is idempotent over the pre-populated
     sets, so it converges to the correct end state.
  2. The BlsG1 library account (0xce69..0fc7), which the pre-fork registry DELEGATECALLs during
     proof-of-possession verification. A fresh genesis does not deploy it, so pre-fork staking
     would revert without this.

Everything else is left byte-for-byte untouched (ruamel round-trip), so every node fed this one
patched file computes an identical genesis hash and consensus holds.

Usage: patch-genesis.py <source-genesis.yaml> <target-genesis.yaml>
  source = committed chain-configs/testnet/genesis.yaml (holds the pre-fork code + BlsG1 library)
  target = generated local-validators/genesis/genesis.yaml (patched in place)
"""
import sys

try:
    from ruamel.yaml import YAML
except ImportError:
    sys.exit("ruamel.yaml is required: pip install ruamel.yaml")

# ConsensusRegistry system address (tn-reth CONSENSUS_REGISTRY_ADDRESS).
REGISTRY = "0x07E17e17E17e17E17e17E17E17E17e17e17E17e1"
# BLS G1 precompile-library the pre-fork registry DELEGATECALLs for proof-of-possession.
BLS_G1 = "0xce696a47b3eb0e4d1f0ae4b16f994ea2acfd0fc7"


def norm(addr):
    """Normalize an address key for case-insensitive comparison (lowercase, no 0x)."""
    s = str(addr).lower()
    return s[2:] if s.startswith("0x") else s


def find_alloc(doc, path):
    alloc = doc.get("alloc") if hasattr(doc, "get") else None
    if alloc is None:
        sys.exit("{}: no top-level 'alloc' mapping found in genesis".format(path))
    return alloc


def find_key(alloc, addr, path):
    """Return the actual key object in `alloc` matching `addr` (case-insensitive), or exit."""
    want = norm(addr)
    for k in alloc:
        if norm(k) == want:
            return k
    sys.exit("{}: address {} not found in alloc".format(path, addr))


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: {} <source-genesis.yaml> <target-genesis.yaml>".format(sys.argv[0]))
    src_path, tgt_path = sys.argv[1], sys.argv[2]

    yaml = YAML()  # round-trip: preserves quoting, key order, and every untouched scalar verbatim
    yaml.preserve_quotes = True
    yaml.width = 1 << 30  # never line-wrap the long hex `code` scalar

    with open(src_path) as f:
        src = yaml.load(f)
    with open(tgt_path) as f:
        tgt = yaml.load(f)

    src_alloc = find_alloc(src, src_path)
    tgt_alloc = find_alloc(tgt, tgt_path)

    # 1. Replace ONLY the registry's code with the pinned pre-fork bytecode; keep target storage.
    src_reg = find_key(src_alloc, REGISTRY, src_path)
    tgt_reg = find_key(tgt_alloc, REGISTRY, tgt_path)
    src_code = src_alloc[src_reg].get("code")
    if not src_code:
        sys.exit("{}: registry account {} has no 'code'".format(src_path, REGISTRY))
    tgt_alloc[tgt_reg]["code"] = src_code

    # 2. Splice in the entire BlsG1 library account (absent from a fresh genesis). Preserve the
    #    source key's format; reth parses account addresses case-insensitively.
    src_bls = find_key(src_alloc, BLS_G1, src_path)
    tgt_alloc[src_bls] = src_alloc[src_bls]

    with open(tgt_path, "w") as f:
        yaml.dump(tgt, f)

    print(
        "patched {}: registry({}) code <- pre-fork bytecode; added BlsG1 library {}".format(
            tgt_path, REGISTRY, BLS_G1
        )
    )


if __name__ == "__main__":
    main()
