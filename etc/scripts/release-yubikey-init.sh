#!/usr/bin/env bash
# One-time helper: prints the ykman commands a maintainer runs to provision
# their YubiKey for release signing. Does NOT run them — operators copy/paste
# after reading what they're about to do.

set -euo pipefail

cat <<'EOF'
=== telcoin-network release YubiKey provisioning ===

Run these one at a time and read what they do first.

# 1. Confirm ykman sees the YubiKey.
ykman info

# 2. Generate a fresh ECCP256 key in PIV slot 9c (digital signature).
#    --pin-policy=once   : PIN once per session
#    --touch-policy=always : touch required for every signature
ykman piv keys generate \
  -a ECCP256 \
  --pin-policy=once \
  --touch-policy=always \
  9c \
  pub.pem

# 3. Self-issue a certificate naming this maintainer.
#    Replace <handle> with your GitHub handle (e.g. grantkee).
ykman piv certificates generate \
  -s "CN=telcoin-network release: <handle>" \
  9c \
  pub.pem

# 4. Verify the slot is populated and read the cert back.
ykman piv info
ykman piv certificates export 9c .github/release-keys/<handle>.pem

# 5. Print the SHA-256 fingerprint and add it to SECURITY.md
#    in the same PR that adds your cert.
openssl x509 -in .github/release-keys/<handle>.pem -noout -fingerprint -sha256

# 6. Open a PR with the new <handle>.pem. The other maintainer reviews and merges.

After the PR lands, install cosign:
  brew install cosign        # macOS
  apt-get install cosign     # debian/ubuntu (or grab a release binary)

Then test signing on a throwaway draft release:
  make release-sign TAG=v0.0.0-rc1-adiri SIGNER=<handle>
EOF
