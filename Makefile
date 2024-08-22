.PHONY: help attest udeps check test test-faucet fmt clippy docker-login docker-adiri docker-push docker-builder docker-builder-init up down validators

# full path for the Makefile
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
BASE_DIR:=$(shell basename $(ROOT_DIR))

.DEFAULT: help

# Default tag is latest if not specified
TAG ?= latest

help:
	@echo ;
	@echo "make attest" ;
	@echo "    :::> Run CI locally and submit signed attestation to Adiri testnet." ;
	@echo ;
	@echo "make udeps" ;
	@echo "    :::> Check unused dependencies in the entire project by package." ;
	@echo "    :::> Dev needs 'cargo-udeps' installed." ;
	@echo "    :::> Dev also needs rust nightly and protobuf (on mac). ";
	@echo "    :::> To install run: 'cargo install cargo-udeps --locked'." ;
	@echo ;
	@echo "make check" ;
	@echo "    :::> Cargo check workspace with all features activated." ;
	@echo ;
	@echo "make test" ;
	@echo "    :::> Run all tests in workspace with all features using 4 threads." ;
	@echo ;
	@echo "make test-faucet" ;
	@echo "    :::> Test faucet integration test in main binary." ;
	@echo ;
	@echo "make fmt" ;
	@echo "    :::> cargo +nightly fmt" ;
	@echo ;
	@echo "make clippy" ;
	@echo "    :::> Cargo +nightly clippy for all features with fix enabled." ;
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

# run CI locally and submit attestation githash to on-chain program
attest:
	./etc/test-and-attest.sh ;

# check for unused dependencies
udeps:
	find . -type f -name Cargo.toml -exec sed -rne 's/^name = "(.*)"/\1/p' {} + | xargs -I {} sh -c "echo '\n\n{}:' && cargo +nightly udeps --package {}" ;

check:
	cargo check --workspace --all-features ;

# run workspace unit tests
test:
	cargo test --workspace --features faucet --no-fail-fast ;

# run faucet integration test
test-faucet:
	cargo test --package telcoin-network --features faucet --test it ;

# format using +nightly toolchain
fmt:
	cargo +nightly fmt ;

# clippy formatter + try to fix problems
clippy:
	cargo +nightly clippy --workspace --all-features --fix ;

# login to gcloud artifact registry for managing docker images
docker-login:
	gcloud auth application-default login ;
	gcloud auth configure-docker us-docker.pkg.dev ;

# build and push latest adiri image for amd64 and arm64
docker-adiri:
	docker buildx build -f ./etc/Dockerfile --platform linux/amd64,linux/arm64 -t us-docker.pkg.dev/telcoin-network/tn-public/adiri:$(TAG) . --push ;

# push local adiri:latest to the gcloud artifact registry
docker-push:
	docker push us-docker.pkg.dev/telcoin-network/tn-public/adiri:latest ;

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
