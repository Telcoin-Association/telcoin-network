# Maintainer Release Keys

This directory holds the public certificates for each maintainer's release-signing
YubiKey. Verifiers use these certs to check the countersignatures on every
published release without trusting any external key server.

Two-of-two countersigning means an attacker who compromises one maintainer key
still cannot ship a release: the second signature will not appear on the draft.

## Files

| File | Maintainer | Status |
|------|------------|--------|
| `grantkee.pem`   | @grantkee   | placeholder — replace with real cert before first release |
| `sstanfield.pem` | @sstanfield | placeholder — replace with real cert before first release |

Each `.pem` is the **certificate** exported from the maintainer's YubiKey PIV
slot 9c (Digital Signature). The matching private key never leaves the YubiKey.

## Provisioning a new key

One-time setup per maintainer. Requires `ykman` from the Yubico Authenticator
suite.

```bash
# 1. Generate an ECCP256 keypair in PIV slot 9c.
#    Slot 9c policy = touch + PIN required for every signature.
ykman piv keys generate \
  -a ECCP256 \
  --pin-policy=once \
  --touch-policy=always \
  9c \
  pub.pem

# 2. Self-issue a certificate naming the maintainer's release identity.
ykman piv certificates generate \
  -s "CN=telcoin-network release: <handle>" \
  9c \
  pub.pem

# 3. Confirm the slot is populated.
ykman piv info

# 4. Export the cert and commit it via PR.
ykman piv certificates export 9c .github/release-keys/<handle>.pem

# 5. Print the SHA-256 fingerprint and record it in SECURITY.md.
openssl x509 -in .github/release-keys/<handle>.pem -noout -fingerprint -sha256
```

The PR adding or rotating a cert must be reviewed by the *other* maintainer.

## Rotation policy

- **Never overwrite** an existing cert in place. Past releases must remain
  verifiable with the cert that signed them.
- To rotate, add a new file (`<handle>-2.pem`, `<handle>-3.pem`, ...) and update
  `etc/scripts/release-sign.sh` and `etc/scripts/release-verify.sh` to point at
  the new file as current.
- Record the new SHA-256 fingerprint in `SECURITY.md` and note which release tag
  is the first one signed by the new key.
- A revoked or lost key must be called out in `SECURITY.md` with the date and
  reason; releases signed by that key before the loss date remain valid; any
  signature claiming a date after are not.

## Expiration

Certs are self-issued and effectively long-lived. We do not rely on cert expiry
for security — the trust anchor is the file in this directory at the commit the
release tag points at. Rotate proactively if a YubiKey is replaced or
compromised.
