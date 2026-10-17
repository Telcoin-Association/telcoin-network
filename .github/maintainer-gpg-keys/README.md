# Maintainer release keys

This directory is the allowlist of OpenPGP keys that may sign a Telcoin Network release tag and its `SHA256SUMS.asc`.

## Format

Each file is named `<handle>.asc`, where `<handle>` is the maintainer's GitHub login and matches `^[A-Za-z0-9-]+$`.
Its content is the output of `gpg --armor --export --export-options export-minimal <PRIMARY_FPR>`: the key as it was generated on the maintainer's YubiKey, the primary key and its subkeys.
A spare YubiKey holds a key of its own, which is appended to the same file by listing both primary fingerprints in that command.
Signatures are matched on the primary key fingerprint, and a primary fingerprint may appear in only one file.

## Readers

- `.github/workflows/release.yaml` and `make release-*` (through `etc/release.sh`) always read this directory from `main`, never from the tagged tree, so a release cannot vouch for its own signer.
- Operators read this directory from `main` too, as [Installing a release](https://docs.telcoin.network/getting-started/installing-a-release.html) shows, verify `SHA256SUMS.asc` against it, and compare the fingerprints with the table in `SECURITY.md` on `main`.
- Every reader uses the current file, so a signature by a key that has since expired, been revoked or been removed stops verifying, older releases included.

## Threshold

A release needs signatures from at least `MIN_SIGNATURES` distinct maintainers (`MIN_SIGNATURES=1` in `etc/release.sh`).
`RELEASE_SIG_THRESHOLD` raises the requirement for a run, up to the number of maintainers listed here.

## Files

| File | Maintainer | Status |
|------|------------|--------|
| `grantkee.asc` | @grantkee | Placeholder until provisioned; every check fails closed on it and names the file. |

## Rules

- When an expiry date changes or a spare YubiKey's key is added, re-export the keys and update the file in place.
- Extend a key before it expires, even a spare's that is rarely used, or the releases it signed stop verifying.
- When a key is revoked, delete it from the file, or delete the file when no other key remains, and mark its row in the `SECURITY.md` table as revoked.
- Never add a placeholder for a new maintainer; add the file only once the real key exists.

Provisioning a key on a YubiKey is covered in <https://docs.telcoin.network/maintainers/yubikey-setup.html>, and the published fingerprints are in [SECURITY.md](../../SECURITY.md#maintainer-release-keys).
