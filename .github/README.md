# How TN CI Works

TN CI is rapidly evolving and relying on GitHub actions to build and test the workspace takes over 45 minutes (before fmt or clippy).

Instead of relying on cloud infrastructure, the core team is responsible for submitting an attestation transaction to the git commit hash attestation contract (currently deployed at `0xde9700e89e0999854e5bfd7357a803d8fc476bb0`).

The worker takes the HEAD commit hash and verifies an attestation was push on-chain.

## Environment
Attesting devs must have "MAINTAINER" role to update contract state.

The local `test-and-attest.sh` script requires Foundry's cast.

See https://book.getfoundry.sh/getting-started/installation for installation instructions.

Add `GITHUB_ATTESTATION_PRIVATE_KEY` to a `.env` file in the project. This is the private key (without "0x" prefix) associated with the "MAINTAINER" role address.
