#!/bin/bash

set -e

# TODO: this doesn't work very well all in one terminal
#
# indicates if the network should start or just generate config files
START=false

# Account that will be funded with 1 billion TEL for testing.
# You must use --dev-funds to overide with your own account,
# must be prefixed with 0x and be a valid account.
DEV_FUNDS=""

#
while [ "$1" != "" ]; do
echo "opt: $1"
    case $1 in
        --start )
                START=true
                ;;
        --dev-funds )
                shift
                DEV_FUNDS=$1
                echo "df: $DEV_FUNDS"
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
        echo "Must use --def-funds=[ADDRESS] to fund a test account."
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
            --dev-funded-account $DEV_FUNDS
            # You can add the arguments below to the genesis init command above to build
            # blocks much faster than default.
            # NOTE: this has to happen at genesis to have any effect.
            #--max-header-delay-ms 1000
            #--min-header-delay-ms 1000

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
    target/${RELEASE}/telcoin-network genesis create-committee --datadir "${ROOTDIR}"

    # copy config files to each validator
    for ((i=0; i<$LENGTH; i++)); do
        VALIDATOR="${VALIDATORS[$i]}"
        DATADIR="${ROOTDIR}/${VALIDATOR}"
        cp "${COMMITTEE_PATH}" "${DATADIR}/genesis"
        cp "${WORKER_CACHE_PATH}" "${DATADIR}/genesis"
    done
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
           --chain adiri \
           --disable-discovery \
           --instance "${INSTANCE}" \
           --metrics "${METRICS}" \
           --consensus-metrics "${CONSENSUS_METRICS}" \
           --log.stdout.format log-fmt \
           -vvv \
           --http > "${ROOTDIR}/${VALIDATOR}.log" &
    done
    echo "$LENGTH validators started in background, \
    use 'killall telcoin-network' to bring the test network down"
fi
