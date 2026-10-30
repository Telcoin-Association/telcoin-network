# Release notes

Each Telcoin Network release has a section below, newest first.
The sections come from [`CHANGELOG.md`](https://github.com/Telcoin-Association/telcoin-network/blob/main/CHANGELOG.md), which git-cliff generates from commit messages when a release is cut.
Tags ending in `-adiri` are releases for the Adiri testnet.
Release candidates have no section of their own; their changes appear under the release they lead to.
Each section lists the changes since the previous final release of either channel, so a change first shipped in an Adiri release is not listed again under the next mainnet release.
Each tag's [GitHub release](https://github.com/Telcoin-Association/telcoin-network/releases) also lists its files, the commands to verify them, and any operational notes such as a required resync or configuration change.
To install a release, follow [Installing a release](installing-a-release.md).
To roll one out across validators, follow the [release and network update process](validator-operations.md#release-and-network-update-process).

{{#include ../../../CHANGELOG.md:releases}}
