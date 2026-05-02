.PHONY: help attest udeps check test test-cargo test-faucet coverage coverage-html fmt clippy docker-login docker-adiri docker-push docker-builder docker-builder-init up down validators pr init-submodules update-tn-contracts revert-submodule clean-logs release-binaries release-image release-sign release-verify release-publish release-yubikey-init release-changelog

# full path for the Makefile
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BASE_DIR:=$(shell basename $(ROOT_DIR))

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
	@echo "    :::> Dev also needs rust nightly-2025-11-04 and protobuf (on mac). ";
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
	@echo "    :::> cargo +nightly-2026-03-20 fmt" ;
	@echo ;
	@echo "make clippy" ;
	@echo "    :::> Cargo +nightly-2026-03-20 clippy for all features with fix enabled." ;
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
	@echo "make release-binaries TAG=vX.Y.Z" ;
	@echo "    :::> Local parity build of the linux release tarball into ./dist (linux host only)." ;
	@echo ;
	@echo "make release-image TAG=vX.Y.Z" ;
	@echo "    :::> Manual fallback for the multi-arch ghcr.io push (CI does this on tag push)." ;
	@echo ;
	@echo "make release-sign TAG=vX.Y.Z SIGNER=<handle>" ;
	@echo "    :::> Countersign a draft release with this maintainer's YubiKey (cosign + libykcs11)." ;
	@echo ;
	@echo "make release-verify TAG=vX.Y.Z" ;
	@echo "    :::> Verify provenance + both maintainer signatures on a release." ;
	@echo ;
	@echo "make release-publish TAG=vX.Y.Z" ;
	@echo "    :::> Run release-verify, then flip the draft release to published." ;
	@echo ;
	@echo "make release-yubikey-init" ;
	@echo "    :::> Print one-time ykman provisioning commands for slot 9c." ;
	@echo ;
	@echo "make release-changelog" ;
	@echo "    :::> Regenerate CHANGELOG.md from conventional commits between tags (git-cliff)." ;
	@echo ;

# run CI locally and submit attestation githash to on-chain program
attest:
	./etc/test-and-attest.sh ;

# check for unused dependencies
udeps:
	find . -type f -name Cargo.toml -exec sed -rne 's/^name = "(.*)"/\1/p' {} + | xargs -I {} sh -c "echo '\n\n{}:' && cargo +nightly-2026-03-20 udeps --package {}" ;

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

# run restart integration tests
test-restarts:
	cargo nextest run --run-ignored all test_restarts ;

# run e2e tests
test-e2e:
	cargo nextest run -p e2e-tests --run-ignored ignored-only --all-features ;

# run tests with coverage (using llvm-cov + nextest)
coverage:
	cargo llvm-cov nextest --workspace --exclude tn-faucet --no-fail-fast ;

# generate HTML coverage report
coverage-html:
	cargo llvm-cov nextest --workspace --exclude tn-faucet --no-fail-fast --html ;
	@echo "Coverage report: target/llvm-cov/html/index.html"

# format using +nightly-2026-03-20 toolchain
fmt:
	cargo +nightly-2026-03-20 fmt ;

# clippy formatter + try to fix problems
clippy:
	cargo +nightly-2026-03-20 clippy --workspace --all-features --fix ;

# login to gcloud artifact registry for managing docker images
docker-login:
	gcloud auth application-default login ;
	gcloud auth configure-docker us-docker.pkg.dev ;

# build and push latest adiri image for amd64 and arm64
docker-adiri:
	docker buildx build -f ./etc/Dockerfile --platform linux/amd64,linux/arm64 --no-cache -t us-docker.pkg.dev/telcoin-network/tn-public/adiri:$(TAG) . --push ;

# push local adiri:latest to the gcloud artifact registry
docker-push:
	docker push us-docker.pkg.dev/telcoin-network/tn-public/adiri:$(TAG) ;

# docker buildx used for multiple processor image building
docker-builder:
	docker buildx create --name tn-builder --use ;

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
public-tests:
	cargo nextest run --workspace --exclude tn-faucet --no-fail-fast ;
	cargo nextest run -p e2e-tests --test it --run-ignored all test_epoch ;
	cargo nextest run --run-ignored all test_restarts ;

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

# --- public release pipeline -------------------------------------------------
# CI does the heavy lifting on tag push (.github/workflows/release.yaml).
# These targets are the local entrypoints maintainers use to countersign and
# publish. Documented in docs/RELEASING.md.

# Local parity build of the linux release tarball.
release-binaries:
	@case "$(TAG)" in v*.*.*) ;; *) echo "TAG must look like vX.Y.Z (got: $(TAG))"; exit 1;; esac
	./etc/scripts/release-binaries.sh $(TAG)

# Manual fallback for the multi-arch ghcr.io push.
release-image:
	@case "$(TAG)" in v*.*.*) ;; *) echo "TAG must look like vX.Y.Z (got: $(TAG))"; exit 1;; esac
	./etc/scripts/release-image.sh $(TAG)

# Countersign a draft release with this maintainer's YubiKey.
release-sign:
	@case "$(TAG)" in v*.*.*) ;; *) echo "Usage: make release-sign TAG=vX.Y.Z SIGNER=<handle> (got TAG=$(TAG))"; exit 1;; esac
	@if [ -z "$(SIGNER)" ]; then echo "Usage: make release-sign TAG=vX.Y.Z SIGNER=<handle>"; exit 1; fi
	./etc/scripts/release-sign.sh $(TAG) $(SIGNER)

# Verify provenance + both maintainer signatures.
release-verify:
	@case "$(TAG)" in v*.*.*) ;; *) echo "TAG must look like vX.Y.Z (got: $(TAG))"; exit 1;; esac
	./etc/scripts/release-verify.sh $(TAG)

# Pre-flight verify, then flip the draft release to published.
release-publish:
	@case "$(TAG)" in v*.*.*) ;; *) echo "TAG must look like vX.Y.Z (got: $(TAG))"; exit 1;; esac
	./etc/scripts/release-verify.sh $(TAG)
	gh release edit $(TAG) --repo telcoin-association/telcoin-network --draft=false

# Print one-time ykman provisioning commands.
release-yubikey-init:
	@./etc/scripts/release-yubikey-init.sh

# Regenerate CHANGELOG.md from conventional commits.
release-changelog:
	git cliff -o CHANGELOG.md
