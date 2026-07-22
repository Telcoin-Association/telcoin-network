.PHONY: help attest udeps check test test-cargo test-faucet coverage coverage-html fmt clippy docker-login docker-adiri docker-push docker-builder docker-builder-init up down validators pr init-submodules update-tn-contracts revert-submodule clean-logs

# full path for the Makefile
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BASE_DIR:=$(shell basename $(ROOT_DIR))

# nightly toolchain used for fmt/clippy/udeps (rustfmt and clippy use nightly-only options)
NIGHTLY:=$(shell cat $(ROOT_DIR)/rust-nightly)

.DEFAULT: help

# Default tag is latest if not specified
TAG ?= latest

help:
	@echo ;
	@echo "=== Prerequisites ===" ;
	@echo "  cargo install cargo-nextest --locked   # fast test runner" ;
	@echo "  cargo install cargo-llvm-cov           # code coverage" ;
	@echo "  cargo install sccache --locked         # compilation cache (optional)" ;
	@echo ;
	@echo "make attest" ;
	@echo "    :::> Run CI locally and submit signed attestation to Adiri testnet." ;
	@echo ;
	@echo "make udeps" ;
	@echo "    :::> Check unused dependencies in the entire project by package." ;
	@echo "    :::> Dev needs 'cargo-udeps' installed." ;
	@echo "    :::> Dev also needs the rust nightly pinned in rust-nightly and protobuf (on mac). ";
	@echo "    :::> To install run: 'cargo install cargo-udeps --locked'." ;
	@echo ;
	@echo "make check" ;
	@echo "    :::> Cargo check workspace with all features activated." ;
	@echo ;
	@echo "make test" ;
	@echo "    :::> Run all tests in workspace using cargo-nextest (faster parallel execution)." ;
	@echo ;
	@echo "make test-cargo" ;
	@echo "    :::> Run all tests using cargo test (fallback if nextest not installed)." ;
	@echo ;
	@echo "make test-faucet" ;
	@echo "    :::> Test faucet integration test in main binary." ;
	@echo ;
	@echo "make test-restarts" ;
	@echo "    :::> Test restart integration tests." ;
	@echo ;
	@echo "make coverage" ;
	@echo "    :::> Run tests with coverage using cargo-llvm-cov + nextest." ;
	@echo "    :::> Requires: cargo install cargo-llvm-cov" ;
	@echo ;
	@echo "make coverage-html" ;
	@echo "    :::> Generate HTML coverage report in target/llvm-cov/html/." ;
	@echo ;
	@echo "make fmt" ;
	@echo "    :::> cargo fmt (nightly toolchain pinned in rust-nightly)" ;
	@echo ;
	@echo "make clippy" ;
	@echo "    :::> cargo clippy for all features with fix enabled (nightly toolchain pinned in rust-nightly)." ;
	@echo ;
	@echo "make docker-login" ;
	@echo "    :::> Setup docker registry using gcloud artifacts." ;
	@echo ;
	@echo "make docker-adiri" ;
	@echo "    :::> Build telcoin-network binary and push to gcloud artifact registry with latest image tag." ;
	@echo ;
	@echo "make docker-push" ;
	@echo "    :::> Push adiri:latest image to gcloud artifact registry." ;
	@echo ;
	@echo "make docker-builder" ;
	@echo "    :::> Create docker builder for building telcoin-network binary container images." ;
	@echo ;
	@echo "make docker-builder-init" ;
	@echo "    :::> Bootstrap the docker builder for building telcoin-network binary container images." ;
	@echo ;
	@echo "make up" ;
	@echo "    :::> Launch docker compose file with 4 local validators in detached state." ;
	@echo ;
	@echo "make down" ;
	@echo "    :::> Bring the docker compose containers down and remove orphans and volumes." ;
	@echo ;
	@echo "make validators" ;
	@echo "    :::> Run 4 validators locally (outside of docker)." ;
	@echo ;
	@echo "make clean-logs LOG_FILE=path/to/file.log" ;
	@echo "    :::> Strip ANSI color codes and timestamps from a log file." ;
	@echo ;

# run CI locally and submit attestation githash to on-chain program
attest:
	./etc/test-and-attest.sh ;

# check for unused dependencies
udeps:
	find . -type f -name Cargo.toml -exec sed -rne 's/^name = "(.*)"/\1/p' {} + | xargs -I {} sh -c "echo '\n\n{}:' && cargo +$(NIGHTLY) udeps --package {}" ;

check:
	cargo check --workspace --all-features --all-targets ;

# run workspace unit tests (using nextest for faster parallel execution)
test:
	cargo nextest run --workspace --no-fail-fast ;

# run workspace unit tests with cargo test (fallback if nextest not installed)
test-cargo:
	cargo test --workspace --no-fail-fast -- --show-output ;

# run faucet integration test
test-faucet:
	cargo nextest run --package telcoin-network --features faucet --test it ;

# Build the node binary once so e2e test processes reuse it via TN_BIN_PATH instead of
# rebuilding it through escargot (~3-10s of cargo overhead per test process). Built under the
# optimized `e2e` profile (opt-level 2 with debug-assertions/overflow-checks kept on; see
# `[profile.e2e]` in .cargo/config.toml) so the reused binary runs consensus and EVM at speed.
.PHONY: build-e2e-bin
build-e2e-bin:
	cargo build --profile e2e --bin telcoin-network --features tn-storage/test-utils --target-dir $(CURDIR)/target ;

# Location of the binary built by build-e2e-bin, passed to the e2e tests via TN_BIN_PATH.
# A named cargo profile emits into target/<profile>/, so the `e2e` profile binary lands here.
E2E_BIN := $(CURDIR)/target/e2e/telcoin-network

# run restart integration tests
test-restarts: build-e2e-bin
	TN_BIN_PATH=$(E2E_BIN) cargo nextest run --run-ignored all test_restarts ;

# run e2e tests
test-e2e: build-e2e-bin
	TN_BIN_PATH=$(E2E_BIN) cargo nextest run -p e2e-tests --run-ignored ignored-only --all-features ;

# run tests with coverage (using llvm-cov + nextest)
coverage:
	cargo llvm-cov nextest --workspace --exclude tn-faucet --no-fail-fast ;

# generate HTML coverage report
coverage-html:
	cargo llvm-cov nextest --workspace --exclude tn-faucet --no-fail-fast --html ;
	@echo "Coverage report: target/llvm-cov/html/index.html"

# format using the nightly toolchain pinned in rust-nightly
fmt:
	cargo +$(NIGHTLY) fmt ;

# clippy formatter + try to fix problems (nightly toolchain pinned in rust-nightly)
clippy:
	cargo +$(NIGHTLY) clippy --workspace --all-features --fix ;

# login to gcloud artifact registry for managing docker images
docker-login:
	gcloud auth application-default login ;
	gcloud auth configure-docker us-docker.pkg.dev ;

# build and push latest adiri image for amd64 and arm64
# CARGO_FEATURES=adiri compiles in the testnet fork logic (ConsensusRegistry fork, ADIRI_DUP_BATCH_EPOCH).
docker-adiri:
	docker buildx build -f ./etc/Dockerfile --build-arg CARGO_FEATURES=adiri --platform linux/amd64,linux/arm64 --no-cache -t us-docker.pkg.dev/telcoin-network/tn-public/adiri:$(TAG) . --push ;

# push local adiri:latest to the gcloud artifact registry
docker-push:
	docker push us-docker.pkg.dev/telcoin-network/tn-public/adiri:$(TAG) ;

# docker buildx used for multiple processor image building
docker-builder:
	docker buildx use tn-builder || docker buildx create --name tn-builder --use ;

# inpect and bootstrap docker buildx for multiple processor image building
docker-builder-init:
	docker buildx inspect --bootstrap ;

# bring docker compose up
up:
	docker compose -f ./etc/compose.yaml up --build --remove-orphans --detach ;

# bring docker compose down
down:
	docker compose -f ./etc/compose.yaml down --remove-orphans -v ;

# alternative approach to run 4 local validator nodes outside of docker on local machine
validators:
	./etc/local-testnet.sh ;

# init submodules
init-submodules:
	git submodule update --init --recursive ;

# update tn-contracts submodule
update-tn-contracts:
	git submodule update --remote tn-contracts;

# revert submodule - useful for excluding updates on a particular branch
revert-submodule:
	@echo "Reverting submodule pointer in tn-contracts..."
	$(eval COMMIT_SHA := $(shell git ls-tree HEAD tn-contracts | awk '{print $$3}'))
	@echo "Checking out $(COMMIT_SHA)"
	cd tn-contracts && git checkout $(COMMIT_SHA)

# workspace tests that don't require faucet credentials
public-tests: build-e2e-bin
	TN_BIN_PATH=$(E2E_BIN) cargo nextest run --workspace --exclude tn-faucet --no-fail-fast ;
	TN_BIN_PATH=$(E2E_BIN) cargo nextest run -p e2e-tests --test it --run-ignored all test_epoch ;
	TN_BIN_PATH=$(E2E_BIN) cargo nextest run --run-ignored all test_restarts ;

# local checks to ensure PR is ready
pr:
	make fmt && \
	make clippy && \
	make public-tests

# strip ANSI color codes and timestamps from a log file
# usage: make clean-logs LOG_FILE=path/to/logfile.log
clean-logs:
	@if [ -z "$(LOG_FILE)" ]; then \
		echo "Error: LOG_FILE is required. Usage: make clean-logs LOG_FILE=/path/to/file.log"; \
		exit 1; \
	fi
	sed -i '' -E "s/[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+Z //g; s/\x1B\[([0-9]{1,3}(;[0-9]{1,3})*)?[mGK]//g" $(LOG_FILE)
