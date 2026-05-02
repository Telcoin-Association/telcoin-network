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

Every published release carries two independent attestations:

1. CI build provenance via `actions/attest-build-provenance` — bound by OIDC
   to `.github/workflows/release.yaml` and the tagged commit. Verified with
   `gh attestation verify`.
2. Two maintainer countersignatures produced locally with `cosign` and a
   YubiKey (PIV slot 9c, ECCP256, touch-required). Two-of-two: one
   compromised key cannot ship a release.

Operators verify a downloaded tarball or pulled image with the commands in
[`docs/INSTALL.md`](docs/INSTALL.md), or in one shot with
`make release-verify TAG=<tag>` from a clone at the tag.

### Maintainer signing certificates

Public certs live at `.github/release-keys/<handle>.pem` in this repo. Their
SHA-256 fingerprints — record them out-of-band so a tampered repo cannot
silently swap the cert it ships:

| Handle | Fingerprint (SHA-256) | First release signed |
|--------|-----------------------|----------------------|
| @grantkee   | _to be recorded on first release_ | _pending_ |
| @sstanfield | _to be recorded on first release_ | _pending_ |

When a key is rotated the new fingerprint is appended here with the date the
previous key is retired. Past releases remain verifiable using the cert that
signed them at the time — committed certs are version-controlled and never
overwritten in place.

## Credits & Acknowledgments

We thank all security researchers who responsibly disclose vulnerabilities.
Their support is critical to keeping our protocol safe for the community.
