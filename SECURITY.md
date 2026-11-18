# Security Policy

## Reporting a Vulnerability

The Telcoin Network team takes security vulnerabilities seriously. If you believe you have found a security vulnerability, please report it to us privately.

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to:
- security{{[@]}}telcoin<.>org

Please include:
- A description of the vulnerability
- Steps to reproduce
- Potential impact
- Technical details and proof of concept if possible

## Response Process

1. We will acknowledge receipt of your report within 48 hours
2. We will provide an initial assessment of the report within 5 business days
3. We will keep you informed of our progress as we investigate and resolve the issue
4. Once resolved, we will notify you and discuss public disclosure timing

## Scope

| In-Scope  | Out-of-Scope |
|-----------|--------------|
| Core protocol code (this repo) | 3rd-party forks/dApps |
| TN Smart Contracts   | Non-official integrations |

### Out of Scope
- Already reported vulnerabilities
- Vulnerabilities in dependencies (report to the dependency maintainer)
- Theoretical vulnerabilities without proof of concept
- Social engineering attacks

## Carried transport dependencies

Maintainers carrying changes to transport dependencies must complete the
[transport patch record](docs/transport-patches/README.md), including advisory
ownership and evidence for the dependency's actual Cargo source form. Dependency
vulnerabilities should still be reported to the dependency maintainer, as described
above; confidential Telcoin impact and coordination use this policy's reporting
channel. Keep private advisory details and TLS key material out of public records.

## Disclosure Policy

- All vulnerability reports and associated communications are considered confidential.
- We kindly ask that you **not publicly disclose** any details related to the vulnerability without our express written permission.
- We aim to fix critical vulnerabilities as quickly as possible.
- If you wish to receive credit for a valid vulnerability report, let us know, and we can discuss private recognition or other acknowledgments.
- We may provide pre-disclosure to key partners and node operators to ensure network stability.

## Supported Versions

There are no supported versions at this time.
The target release for supported versions is Q3 2025.

## Security Updates

Security fixes are released as promptly as possible.
Telcoin Network is still under heavy development and considered unstable.

## Bug Bounty

Coming soon.
If you have something to share and want to inquire about the status of our bug bounty program, please email security{{[@]}}telcoin<.>org

## Verifying releases

Every release is a git tag signed with a maintainer's OpenPGP key.
Its assets are a Linux x86_64 tarball, an `IMAGE_DIGEST` file that names the container image by digest, a `SHA256SUMS` file with the SHA-256 of the tarball, of `IMAGE_DIGEST` and of the network's `committee.yaml`, `genesis.yaml` and `parameters.yaml` from `chain-configs/`, and `SHA256SUMS.asc`, a detached signature over `SHA256SUMS`.
The tag and `SHA256SUMS.asc` are signed with the same key, and each signing key lives on a YubiKey.
A release that ships the edge services, `worker-gateway` and `node-record-api`, which `etc/release-components` at the tagged commit names, also carries a tarball and an `IMAGE_DIGEST-<service>` file for each.
Their images are in `ghcr.io/telcoin-association/tn-worker-gateway` and `ghcr.io/telcoin-association/tn-node-record-api`, which mainnet and Adiri releases share.
Their files are listed in a second manifest, `SHA256SUMS-edge`, signed in `SHA256SUMS-edge.asc` by the same maintainer keys under the same threshold; `SHA256SUMS` does not list them.
The public keys are kept in [`tn-release-signing`](https://github.com/Telcoin-Association/tn-release-signing), the repository of maintainer keys that every signed Telcoin project shares, which this repository carries as the `release` submodule.
The release tooling and the operator check in Installing a release always read them at the commit `main` records for `release`, never from the tag being released, and moving that pointer needs a review from a maintainer who holds a release key.
Once the `v*` tag ruleset is in place (only maintainers can create a `v*` tag, and it must be signed; nobody, maintainers included, can update or delete one), CI refuses to draft a release for a tag that is not signed by one of those keys; until then this refusal is not guaranteed, because GitHub runs the release workflow as it is at the tagged commit.
`make release-sign`, `make release-verify` and `make release-publish` apply the rule of the operator check to `SHA256SUMS.asc` and `SHA256SUMS-edge.asc`: every signature in each must be a good one by an allowlisted key, and a bad, unverifiable, expired or revoked signature refuses the release even next to a good one.
Before publishing, `make release-publish` checks the signatures, checksums and image digests again, asks the maintainer to confirm, and then checks that the release's files are still the ones it verified before it makes the release public.

To check a release yourself, follow [Installing a release](https://docs.telcoin.network/getting-started/installing-a-release.html).
Compare the fingerprints it prints with the key table in `KEYS.md` in the `release` submodule of the same `main` checkout and with the maintainer's keys on GitHub at `https://github.com/<handle>.gpg`, and treat a key whose Status is `revoked` as untrusted.
None of these comes from the release tag, so a tampered tag cannot change what you compare against.

### Maintainer release keys

The maintainer release keys, their fingerprints, the signature threshold and the rules for changing them are in [`KEYS.md`](https://github.com/Telcoin-Association/tn-release-signing/blob/main/KEYS.md) in `tn-release-signing`.
A release of this repository is checked against the keys and the `KEYS.md` at the commit `main` records for the `release` submodule, which `release/KEYS.md` shows in a checkout after `git submodule update --init -- release`.
`MIN_SIGNATURES` in `etc/release.sh` may require more signatures than `KEYS.md` states, never fewer.
A change there reaches this repository's releases when a pull request here moves the `release` pointer to a commit that carries it, as [Releasing](https://docs.telcoin.network/maintainers/releasing.html#moving-the-submodule-pointer) describes.

## Credits & Acknowledgments

We thank all security researchers who responsibly disclose vulnerabilities.
Their support is critical to keeping our protocol safe for the community.
