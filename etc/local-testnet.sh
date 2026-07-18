#!/bin/bash

set -e

# indicates if the network should start or just generate config files
START=false

# Account that will be funded with 1 billion TEL for testing.
# You must use --dev-funds to overide with your own account,
# must be prefixed with 0x and be a valid account.
DEV_FUNDS=""

BASEFEE_ADDRESS=""

# Owner of the ConsensusRegistry contract (can mint ConsensusNFTs).
# Defaults to DEV_FUNDS if not provided.
GOVERNANCE=""

export TN_BLS_PASSPHRASE="local"

while [ "$1" != "" ]; do
    case $1 in
        --start )
                START=true
                ;;
        --dev-funds )
                shift
                DEV_FUNDS=$1
                ;;
        --basefee-address )
                shift
                BASEFEE_ADDRESS=$1
                ;;
        --governance )
                shift
                GOVERNANCE=$1
                ;;
        * )     echo "Invalid option: $1"
                exit 1
    esac
    shift
done

# Default governance to dev-funds if not explicitly set
if [ "$GOVERNANCE" == "" ] && [ "$DEV_FUNDS" != "" ]; then
    GOVERNANCE="$DEV_FUNDS"
fi

VALIDATORS=("validator-1" "validator-2" "validator-3" "validator-4")
ADDRESSES=(
    "0x1111111111111111111111111111111111111111"
    "0x2222222222222222222222222222222222222222"
    "0x3333333333333333333333333333333333333333"
    "0x4444444444444444444444444444444444444444"
)

# Non-genesis stakeable validators used to exercise the ConsensusRegistry fork.
# They are generated as regular validators (their node-info is deliberately NOT copied into the
# shared genesis dir, so they are NOT in the genesis committee) and started WITHOUT --observer.
# Each boots in Observer *mode* (follows consensus) and auto-promotes to CvvActive at the epoch
# boundary where the registry rotates its key into the committee -- no restart needed.
# The "observer*" names preserve port/metrics continuity with the previous single-observer layout.
#   observer1 -> instance 5, RPC http://localhost:8541, metrics 127.0.0.1:9104 (staked pre-fork)
#   observer2 -> instance 6, RPC http://localhost:8540, metrics 127.0.0.1:9105 (staked post-fork)
STAKEABLE=("observer1" "observer2")
STAKEABLE_INSTANCES=(5 6)
STAKEABLE_METRICS=("127.0.0.1:9104" "127.0.0.1:9105")

# variables for pulling
LOCAL_PATH="./genesis/validators/"
REMOTE_PATH="/home/share/validators/*"

# root path for all validators
ROOTDIR="./local-validators"
GENESISDIR="genesis"
VALIDATORSDIR="${GENESISDIR}/validators"
SHARED_GENESISDIR="${ROOTDIR}/${VALIDATORSDIR}"
COMMITTEE_PATH="${ROOTDIR}/${GENESISDIR}/committee.yaml"
GENESIS_JSON_PATH="${ROOTDIR}/${GENESISDIR}/genesis.json"

# number of validators
LENGTH="${#VALIDATORS[@]}"

# Use RELEASE="debug" below and remove the --release to use a debug build
RELEASE="release"

# This fork-test harness patches genesis with Python (ruamel.yaml) and drives staking with Foundry
# `cast`. Check those up front -- only when generating fresh config -- so a missing dep fails BEFORE
# the (slow) adiri release build rather than after it.
if [ ! -d "${ROOTDIR}" ]; then
    if ! command -v python3 >/dev/null 2>&1; then
        echo "python3 is required."; exit 1
    fi
    if ! python3 -c 'import ruamel.yaml' >/dev/null 2>&1; then
        echo "python3 module 'ruamel.yaml' is required: pip install ruamel.yaml"; exit 1
    fi
    if ! command -v cast >/dev/null 2>&1; then
        echo "Foundry 'cast' is required: https://book.getfoundry.sh/getting-started/installation"; exit 1
    fi
fi

# Build with the `adiri` feature so the ConsensusRegistry fork machinery (and the
# CONSENSUS_REGISTRY_FORK_EPOCH set in crates/types/src/forks.rs) is compiled in. The adiri
# binary also requires the loaded genesis to report chainId == 2017 (0x7e1), enforced below.
cargo build -p telcoin-network --bin telcoin-network --features adiri --release
# Example of using redb for the consensus DB
#cargo build -p telcoin-network --bin telcoin-network --features redb --release

# Set to true only when config is freshly generated below, so pre-fork auto-staking runs once
# (on the first --start) and NOT on subsequent restarts against an already-staked registry.
CONFIG_GENERATED=false

if [ -d "${ROOTDIR}" ]; then
    echo "The directory ${ROOTDIR} already exists -- skipping configuration"
    echo "Remove ${ROOTDIR} if you wish create a new configuration."
    echo
else
    # Make sure we have a test account with funds if configuring.
    if [ "$DEV_FUNDS" == "" ]; then
        echo "Must use --dev-funds=[ADDRESS] to fund a test account and own the consensus registry."
        echo "For example: --dev-funds 0x1111111111111111111111111111111111111111"
        echo "This sould be an account you have the private key to allow access to TEL on the test network"
        exit 1
    fi

    # Prereqs (python3/ruamel.yaml/cast) were verified before the build above.
    CONFIG_GENERATED=true

    # make local directory for all validators
    mkdir -p $SHARED_GENESISDIR

    # Loop through all the validators and generate their keys and validator infos.
    for ((i=0; i<$LENGTH; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        ADDRESS="${ADDRESSES[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        echo "creating validator keys/info for ${VALIDATOR}"
        target/${RELEASE}/telcoin-network keytool generate validator \
            --datadir "${DATADIR}" \
            --address "${ADDRESS}"

        # cp validator info into shared genesis dir
        echo "copying validator info to shared genesis dir"
        cp "${DATADIR}/node-info.yaml" "${SHARED_GENESISDIR}/${VALIDATOR}.yaml"
        echo ""
        echo ""
    done

    # Use the validator infos to Create genesis, committee and worker cache yamls.
    # Speed up blocks for testing, use a bogus chain id
    if [ "$BASEFEE_ADDRESS" = "" ]; then
        target/${RELEASE}/telcoin-network genesis \
            --datadir "${ROOTDIR}" \
            --chain-id 0x7e1 \
            --epoch-duration-in-secs 15 \
            --dev-funded-account $DEV_FUNDS \
            --max-header-delay-ms 1000 \
            --min-header-delay-ms 1000 \
            --consensus-registry-owner $GOVERNANCE
    else
        target/${RELEASE}/telcoin-network genesis \
            --datadir "${ROOTDIR}" \
            --chain-id 0x7e1 \
            --epoch-duration-in-secs 15 \
            --dev-funded-account $DEV_FUNDS \
            --basefee-address $BASEFEE_ADDRESS \
            --max-header-delay-ms 1000 \
            --min-header-delay-ms 1000 \
            --consensus-registry-owner $GOVERNANCE
    fi

    # Patch the freshly-generated genesis so the ConsensusRegistry fork can actually fire.
    #
    # A fresh genesis deploys the CURRENT (post-fork) registry bytecode and snapshots its storage,
    # so the fork's fail-closed swap gate (which requires the pinned pre-fork code hash) would abort.
    # This splices the pinned pre-fork registry `code` (and the BlsG1 library the pre-fork registry
    # DELEGATECALLs for proof-of-possession) from the committed testnet genesis into the local one,
    # keeping the freshly-generated storage. Every node is fed this one patched copy below, so all
    # nodes compute an identical genesis hash and consensus holds.
    echo "patching genesis with pre-fork ConsensusRegistry code + BlsG1 library"
    python3 etc/fork-test/patch-genesis.py \
        chain-configs/testnet/genesis.yaml \
        "${ROOTDIR}/${GENESISDIR}/genesis.yaml"

    # Copy the generated genesis, committee and parameters to each validator.
    for ((i=0; i<$LENGTH; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        mkdir "${DATADIR}/genesis"
        # cp validator info into shared genesis dir
        echo "copying validator info to shared genesis dir"
        cp "${ROOTDIR}/${GENESISDIR}/genesis.yaml" "${DATADIR}/genesis"
        cp "${ROOTDIR}/${GENESISDIR}/committee.yaml" "${DATADIR}/genesis"
        cp "${ROOTDIR}/parameters.yaml" "${DATADIR}/"
        echo ""
        echo ""
    done

    # Generate the two non-genesis stakeable validators (observer1, observer2).
    # For each: mint a fresh operator EOA (funded from the dev account at stake time and used to
    # send stake()/activate()), then generate validator keys bound to that EOA. Their node-info is
    # deliberately NOT copied into the shared genesis dir, so they stay OUT of the genesis committee
    # and instead rotate in via on-chain staking.
    for ((s=0; s<${#STAKEABLE[@]}; s++)); do
        NODE="${STAKEABLE[$s]}"
        DATADIR="${ROOTDIR}/${NODE}"
        echo "creating operator EOA + validator keys for ${NODE}"

        # Fresh operator EOA (the execution key the operator controls). `cast wallet new` prints
        # "Address: 0x.." and "Private key: 0x..". Persist both for the staking script to consume.
        EOA_OUT=$(cast wallet new)
        EOA_ADDR=$(echo "$EOA_OUT" | grep -i 'Address:' | awk '{print $NF}')
        EOA_PK=$(echo "$EOA_OUT" | grep -i 'Private key:' | awk '{print $NF}')
        if [ -z "$EOA_ADDR" ] || [ -z "$EOA_PK" ]; then
            echo "failed to generate operator EOA for ${NODE}"
            exit 1
        fi
        {
            echo "EOA_ADDR=${EOA_ADDR}"
            echo "EOA_PK=${EOA_PK}"
        } > "${ROOTDIR}/${NODE}.eoa"

        # Validator keygen bound to the operator EOA (runs as a plain validator, not an observer).
        target/${RELEASE}/telcoin-network keytool generate validator \
            --datadir "${DATADIR}" \
            --address "${EOA_ADDR}"

        # Copy chain config (patched genesis + committee + parameters). NOTE: no node-info.yaml copy
        # into the shared genesis dir -- that is what keeps these two out of the genesis committee.
        mkdir -p "${DATADIR}/genesis"
        cp "${ROOTDIR}/${GENESISDIR}/genesis.yaml" "${DATADIR}/genesis"
        cp "${ROOTDIR}/${GENESISDIR}/committee.yaml" "${DATADIR}/genesis"
        cp "${ROOTDIR}/parameters.yaml" "${DATADIR}/"
        echo ""
    done
fi

if [ "$START" = true ]; then
    for ((i=0; i<$LENGTH; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        INSTANCE=$((i+1))
        RPC_PORT=$((8545-i))
        CONSENSUS_METRICS="127.0.0.1:910$i"

        echo "Starting ${VALIDATOR} in background, rpc endpoint http://localhost:$RPC_PORT"
        # -vvv for INFO, -vvvvv for TRACE, etc
        # start validator
        target/${RELEASE}/telcoin-network node --datadir "${DATADIR}" \
           --instance "${INSTANCE}" \
           --metrics "${CONSENSUS_METRICS}" \
           --log.stdout.format log-fmt \
           -vvv \
           --http > "${ROOTDIR}/${VALIDATOR}.log" &
    done

    # Start the two non-genesis stakeable validators WITHOUT --observer, so they can auto-promote
    # into the committee once the registry rotates their key in. RPC port = 8546 - instance
    # (instance 5 -> 8541, instance 6 -> 8540), matching the validator port scheme.
    for ((s=0; s<${#STAKEABLE[@]}; s++)); do
        NODE="${STAKEABLE[$s]}"
        DATADIR="${ROOTDIR}/${NODE}"
        INSTANCE="${STAKEABLE_INSTANCES[$s]}"
        CONSENSUS_METRICS="${STAKEABLE_METRICS[$s]}"
        RPC_PORT=$((8546-INSTANCE))

        echo "Starting ${NODE} (plain validator) in background, rpc endpoint http://localhost:${RPC_PORT}"
        target/${RELEASE}/telcoin-network node --datadir "${DATADIR}" \
           --instance "${INSTANCE}" \
           --metrics "${CONSENSUS_METRICS}" \
           --log.stdout.format log-fmt \
           -vvv \
           --http > "${ROOTDIR}/${NODE}.log" &
    done

    echo "$LENGTH genesis validators + ${#STAKEABLE[@]} stakeable validators started in background, \
    use 'killall telcoin-network' to bring the test network down"

    # Auto pre-fork staking: only on a fresh config (never re-stake an already-staked registry on
    # restart). Wait for the network to produce blocks, then stake observer1 (node 5) under the
    # PRE-fork bytecode so it rotates into the committee before the fork fires at epoch 5.
    if [ "$CONFIG_GENERATED" = true ]; then
        echo "waiting for the network to come up on http://localhost:8545 ..."
        for _ in $(seq 1 60); do
            if cast block-number --rpc-url http://localhost:8545 >/dev/null 2>&1; then
                echo "network is live -- submitting pre-fork staking for observer1 (node 5)"
                ./etc/fork-test/stake-validator.sh 5 || echo "pre-fork staking returned non-zero (see output above)"
                break
            fi
            sleep 2
        done
    fi
fi
