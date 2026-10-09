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

Every release is a git tag signed with a maintainer's OpenPGP key. Its assets are a Linux x86_64 tarball, an `IMAGE_DIGEST` file that names the container image by digest, a `SHA256SUMS` file covering both, and `SHA256SUMS.asc`, a detached signature over `SHA256SUMS`. The tag and `SHA256SUMS.asc` are signed with the same key, and each signing key lives on a YubiKey. The public keys are kept in `.github/maintainer-gpg-keys/` and the release tooling always reads them from `main`, never from the tag being released. CI refuses to draft a release for a tag that is not signed by one of those keys, and the signatures, checksums and image digest are checked again before a release is published.

To check a release yourself, follow [Installing a release](https://docs.telcoin.network/getting-started/installing-a-release.html). Compare the fingerprints it prints with the table below and with the maintainer's keys on GitHub at `https://github.com/<handle>.gpg`. Neither source comes from the release tag, so a tampered tag cannot change what you compare against.

### Maintainer release keys

The fingerprint is the primary key's. Signatures are made with a signing subkey on the YubiKey, and every check matches them to the primary key.

| Handle | Primary key fingerprint | YubiKey serial | Added | Status |
|--------|-------------------------|----------------|-------|--------|
| @grantkee | pending provisioning | pending | pending | pending provisioning |

Signatures required: 1 (MIN_SIGNATURES in etc/release.sh).

Until a key is provisioned, its file in `.github/maintainer-gpg-keys/` is a placeholder and every release check fails on it.

### Changing this table

- To add a maintainer, open one pull request that adds `.github/maintainer-gpg-keys/<handle>.asc` and a row here, and have it reviewed by someone other than the key's owner. [YubiKey signing setup](https://docs.telcoin.network/maintainers/yubikey-setup.html) covers creating and exporting the key.
- When a YubiKey is replaced, update the `.asc` file in place with the new signing subkey and change the YubiKey serial and Added date in the row. The primary fingerprint stays the same.
- When a key is revoked, keep its row, set Status to `revoked YYYY-MM-DD`, and delete its `.asc` file. Release checks read the allowlist from `main`, so the key stops being accepted as soon as that change merges.
- To require more signatures, raise `MIN_SIGNATURES` in `etc/release.sh` and update the Signatures required line in the same pull request. The threshold cannot exceed the number of keys in the table.

## Credits & Acknowledgments

We thank all security researchers who responsibly disclose vulnerabilities.
Their support is critical to keeping our protocol safe for the community.
