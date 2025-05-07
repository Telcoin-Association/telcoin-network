#!/bin/bash

set -e

# indicates if the network should start or just generate config files
START=false

# Account that will be funded with 1 billion TEL for testing.
# You must use --dev-funds to overide with your own account,
# must be prefixed with 0x and be a valid account.
DEV_FUNDS=""

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

    for ((i=0; i<$LENGTH; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        ADDRESS="${ADDRESSES[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"


        echo "creating datadir for ${VALIDATOR}"
        target/${RELEASE}/telcoin-network genesis init --datadir "${DATADIR}" \
            --dev-funded-account $DEV_FUNDS \
            --max-header-delay-ms 1000 \
            --min-header-delay-ms 1000
			# The min|max_header_delay_ms options above cause blocks to be generated
			# faster for testing.  Remove then to build blocks at "default" speed.
			# With these blocks are built every couple seconds, without them every ten seconds.
			# This is just to make local testing faster.
            # NOTE: this has to happen at genesis to have any effect.

        echo "creating validator keys"
        target/${RELEASE}/telcoin-network keytool generate validator \
            --datadir "${DATADIR}" \
            --address "${ADDRESS}"

        echo "creating validator info for genesis"
        target/${RELEASE}/telcoin-network genesis add-validator --datadir "${DATADIR}"

        # cp validator info into shared genesis dir
        echo "copying validator info to shared genesis dir"
        ls "${DATADIR}/${VALIDATORSDIR}"
        cp "${DATADIR}/${VALIDATORSDIR}"/* "${SHARED_GENESISDIR}"
        echo ""
        echo ""
    done

    # create committee and worker cache yamls
    target/${RELEASE}/telcoin-network genesis create-committee \
        --datadir "${ROOTDIR}" \
        --consensus-registry-owner $DEV_FUNDS

    # copy config files to each validator
    for ((i=0; i<$LENGTH; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        cp "${COMMITTEE_PATH}" "${DATADIR}/genesis"
        cp "${WORKER_CACHE_PATH}" "${DATADIR}/genesis"
        cp "${GENESIS_JSON_PATH}" "${DATADIR}/genesis"
    done

    echo "creating datadir for observer"
    DATADIR="${ROOTDIR}/observer"
    mkdir -p "${DATADIR}/genesis"
    target/${RELEASE}/telcoin-network keytool generate observer \
        --datadir "${DATADIR}" \
        --dev-funded-account $DEV_FUNDS
    cp "${COMMITTEE_PATH}" "${DATADIR}/genesis"
    cp "${WORKER_CACHE_PATH}" "${DATADIR}/genesis"
    cp "${GENESIS_JSON_PATH}" "${DATADIR}/genesis"
fi

if [ "$START" = true ]; then
    for ((i=0; i<$LENGTH; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        INSTANCE=$((i+1))
        RPC_PORT=$((8545-i))
        METRICS="127.0.0.1:909$i"
        CONSENSUS_METRICS="127.0.0.1:910$i"

        echo "Starting ${VALIDATOR} in background, rpc endpoint http://localhost:$RPC_PORT"
        # -vvv for INFO, -vvvvv for TRACE, etc
        # start validator
        target/${RELEASE}/telcoin-network node --datadir "${DATADIR}" \
           --genesis "${DATADIR}/genesis/genesis.json" \
           --disable-discovery \
           --instance "${INSTANCE}" \
           --metrics "${METRICS}" \
           --consensus-metrics "${CONSENSUS_METRICS}" \
           --log.stdout.format log-fmt \
           -vvv \
           --http > "${ROOTDIR}/${VALIDATOR}.log" &
    done

    DATADIR="${ROOTDIR}/observer"
    METRICS="127.0.0.1:9094"
    CONSENSUS_METRICS="127.0.0.1:9104"
    echo "Starting Observer in background, rpc endpoint http://localhost:8541"
    target/${RELEASE}/telcoin-network node --datadir "${DATADIR}" \
       --genesis "${DATADIR}/genesis/genesis.json" \
       --observer \
       --disable-discovery \
       --instance 5 \
       --metrics "${METRICS}" \
       --consensus-metrics "${CONSENSUS_METRICS}" \
       --log.stdout.format log-fmt \
       -vvv \
       --http > "${ROOTDIR}/observer.log" &

    echo "$LENGTH validators started in background, \
    use 'killall telcoin-network' to bring the test network down"
fi
