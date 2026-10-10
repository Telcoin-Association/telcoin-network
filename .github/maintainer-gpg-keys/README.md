# Maintainer release keys

This directory is the allowlist of OpenPGP keys that may sign a Telcoin Network release tag and its `SHA256SUMS.asc`.

## Format

Each file is named `<handle>.asc`, where `<handle>` is the maintainer's GitHub login and matches `^[A-Za-z0-9-]+$`.
Its content is the output of `gpg --armor --export --export-options export-minimal <PRIMARY_FPR>`: the primary key plus its current signing subkeys.
Signatures are matched on the primary key fingerprint, and a primary fingerprint may appear in only one file.

## Readers

- `.github/workflows/release.yaml` and `make release-*` (through `etc/release.sh`) always read this directory from `main`, never from the tagged tree, so a release cannot vouch for its own signer.
- Operators read this directory from `main` too, as [Installing a release](https://docs.telcoin.network/getting-started/installing-a-release.html) shows, verify `SHA256SUMS.asc` against it, and compare the fingerprints with the table in `SECURITY.md` on `main`.
- Every reader uses the current file, so a signature by a subkey that has since expired or been revoked stops verifying, older releases included.

## Threshold

A release needs signatures from at least `MIN_SIGNATURES` distinct maintainers (`MIN_SIGNATURES=1` in `etc/release.sh`).
`RELEASE_SIG_THRESHOLD` raises the requirement for a run, up to the number of maintainers listed here.

## Files

| File | Maintainer | Status |
|------|------------|--------|
| `grantkee.asc` | @grantkee | Placeholder until provisioned; every check fails closed on it and names the file. |

## Rules

- When a signing subkey is added or replaced, or an expiry date changes, re-export the key and update the file in place.
- When a signing subkey is revoked, re-export with `--export-filter 'drop-subkey=revoked -t'` added to the command under Format, and check with `gpg --show-keys` that the revoked subkey is gone.
  Plain `gpgv` still reports a good signature for a revoked subkey; the operator check and `etc/release.sh` reject it, but other tools may not.
- Extend a signing subkey before it expires, even one you no longer use, or the releases it signed stop verifying.
- When a key is revoked, delete its file and mark its row in the `SECURITY.md` table as revoked.
- Never add a placeholder for a new maintainer; add the file only once the real key exists.

Provisioning a key on a YubiKey is covered in <https://docs.telcoin.network/maintainers/yubikey-setup.html>, and the published fingerprints are in [SECURITY.md](../../SECURITY.md#maintainer-release-keys).
