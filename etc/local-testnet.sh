#!/bin/bash

set -e

# indicates if the network should start or just generate config files
START=false

# Account that will be funded with 1 billion TEL for testing.
# You must use --dev-funds to overide with your own account,
# must be prefixed with 0x and be a valid account.
DEV_FUNDS=""

BASEFEE_ADDRESS=""

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
        * )     echo "Invalid option: $1"
                exit 1
    esac
    shift
done

VALIDATORS=("validator-1" "validator-2" "validator-3" "validator-4")
ADDRESSES=(
    "0x1111111111111111111111111111111111111111"
    "0x2222222222222222222222222222222222222222"
    "0x3333333333333333333333333333333333333333"
    "0x4444444444444444444444444444444444444444"
)

# variables for pulling
LOCAL_PATH="./genesis/validators/"
REMOTE_PATH="/home/share/validators/*"

# root path for all validators
ROOTDIR="./local-validators"
GENESISDIR="genesis"
VALIDATORSDIR="${GENESISDIR}/validators"
SHARED_GENESISDIR="${ROOTDIR}/${VALIDATORSDIR}"
COMMITTEE_PATH="${ROOTDIR}/${GENESISDIR}/committee.yaml"
WORKER_CACHE_PATH="${ROOTDIR}/${GENESISDIR}/worker_cache.yaml"
GENESIS_JSON_PATH="${ROOTDIR}/${GENESISDIR}/genesis.json"

# number of validators
LENGTH="${#VALIDATORS[@]}"

# Use RELEASE="debug" below and remove the --release to use a debug build
RELEASE="release"
cargo build --bin telcoin-network --release
# Example of using redb for the consensus DB
#cargo build --bin telcoin-network --features redb --release

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
            --chain-id 0x1e7 \
            --epoch-duration-in-secs 86400 \
            --dev-funded-account $DEV_FUNDS \
            --max-header-delay-ms 1000 \
            --min-header-delay-ms 1000 \
            --consensus-registry-owner $DEV_FUNDS
    else
        target/${RELEASE}/telcoin-network genesis \
            --datadir "${ROOTDIR}" \
            --chain-id 0x1e7 \
            --epoch-duration-in-secs 86400 \
            --dev-funded-account $DEV_FUNDS \
            --basefee-address $BASEFEE_ADDRESS \
            --max-header-delay-ms 1000 \
            --min-header-delay-ms 1000 \
            --consensus-registry-owner $DEV_FUNDS
    fi

    # Copy the generated genesis, committee, worker_cache and parameters to each validator.
    for ((i=0; i<$LENGTH; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        mkdir "${DATADIR}/genesis"
        # cp validator info into shared genesis dir
        echo "copying validator info to shared genesis dir"
        cp "${ROOTDIR}/${GENESISDIR}/genesis.yaml" "${DATADIR}/genesis"
        cp "${ROOTDIR}/${GENESISDIR}/committee.yaml" "${DATADIR}/genesis"
        cp "${ROOTDIR}/${GENESISDIR}/worker_cache.yaml" "${DATADIR}/genesis"
        cp "${ROOTDIR}/parameters.yaml" "${DATADIR}/"
        echo ""
        echo ""
    done

    echo "creating datadir for observer"
    DATADIR="${ROOTDIR}/observer"
    mkdir -p "${DATADIR}/genesis"
    # Generate an observers "validator info"- still needs this for it's p2p netork settings and keys.
    target/${RELEASE}/telcoin-network keytool generate observer \
        --datadir "${DATADIR}" \
        --address 0x4444444444444444444444444444444444444444
        # Copy the chain config files over to the new observer config directories.
    cp "${ROOTDIR}/${GENESISDIR}/genesis.yaml" "${DATADIR}/genesis"
    cp "${ROOTDIR}/${GENESISDIR}/committee.yaml" "${DATADIR}/genesis"
    cp "${ROOTDIR}/${GENESISDIR}/worker_cache.yaml" "${DATADIR}/genesis"
    cp "${ROOTDIR}/parameters.yaml" "${DATADIR}/"
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

    DATADIR="${ROOTDIR}/observer"
    CONSENSUS_METRICS="127.0.0.1:9104"
    echo "Starting Observer in background, rpc endpoint http://localhost:8541"
    target/${RELEASE}/telcoin-network node --datadir "${DATADIR}" \
       --observer \
       --instance 5 \
       --metrics "${CONSENSUS_METRICS}" \
       --log.stdout.format log-fmt \
       -vvv \
       --http > "${ROOTDIR}/observer.log" &

    echo "$LENGTH validators started in background, \
    use 'killall telcoin-network' to bring the test network down"
fi
