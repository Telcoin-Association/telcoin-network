# Maintainer Tag-Signing GPG Keys

This directory holds the ASCII-armored OpenPGP public keys for each maintainer's tag-signing key.
The release workflow imports every `.asc` file here and rejects any pushed tag that does not carry a `Good signature` from a key whose fingerprint matches one of the imported keys.

This is the encoded form of the policy "tags must be signed by a maintainer."
Enforcement lives in `.github/workflows/release.yaml`, not in a repo setting that can be silently toggled off.

## Files

| File | Maintainer | Status |
|------|------------|--------|
| `grantkee.asc`   | @grantkee   | placeholder — replace with the real public key before the first release |
| `sstanfield.asc` | @sstanfield | placeholder — replace with the real public key before the first release |

## Provisioning a new key

One-time setup per maintainer. Requires `gpg` (GnuPG >= 2.2).

```bash
# 1. Generate a long-lived signing-capable primary key.
gpg --full-generate-key

# 2. Confirm the fingerprint.
gpg --list-secret-keys --with-fingerprint <handle>@example.com

# 3. Export the ASCII-armored public key.
gpg --armor --export <FINGERPRINT> > .github/maintainer-gpg-keys/<handle>.asc

# 4. Open a PR adding the file. Record the fingerprint in SECURITY.md.
```

The PR adding or rotating a key must be reviewed by the *other* maintainer.

## Signing a tag

```bash
git config user.signingkey <FINGERPRINT>
git tag -s v0.6.0 -m "v0.6.0"
git push origin v0.6.0
```

The workflow runs `git tag -v "$TAG"` against the imported keys.
A `Good signature` from a key not in this directory is rejected the same as no signature at all.

## Rotation policy

- **Never overwrite** an existing `.asc` in place. Past releases must remain verifiable.
- Add a new file (`<handle>-2.asc`, `<handle>-3.asc`, ...) when a key is rotated.
- Update `SECURITY.md` with the new fingerprint and the date the previous key is retired.
- A revoked or lost key is called out in `SECURITY.md` with the date and reason.
