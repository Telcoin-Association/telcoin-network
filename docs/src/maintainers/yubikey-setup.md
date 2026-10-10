# YubiKey signing setup

This page is the one-time setup for a maintainer who signs releases.
When you finish it you have:

- an OpenPGP key whose signing subkey lives on a YubiKey;
- that key on the release allowlist;
- GitHub showing your signed tags as Verified;
- a laptop ready for `make release-tag` and `make release-sign`;
- registry access on xerxes for `make release-build` and `make release-publish`.

## What you need

- A YubiKey 5 with firmware 5.2.3 or later, which ed25519 keys need, and preferably a second one as a spare.
- A Mac with Homebrew.
- Two USB drives for encrypted backups.
- An email address verified on your GitHub account, for the key's user ID (check at `https://github.com/settings/emails`).
- About an hour, part of it with the Mac offline.

## Install the tools

Install them on the Mac itself: GnuPG reaches the YubiKey through the host's smart card and USB stack, which a container cannot use.

```sh
brew install gnupg pinentry-mac ykman
mkdir -p ~/.gnupg && chmod 700 ~/.gnupg
echo "pinentry-program $(brew --prefix)/bin/pinentry-mac" >> ~/.gnupg/gpg-agent.conf
echo "no-allow-external-cache" >> ~/.gnupg/gpg-agent.conf
gpgconf --kill gpg-agent
```

`no-allow-external-cache` removes the "Save in Keychain" box from pinentry-mac.
Never store the PIN in Keychain.

## Check the YubiKey

```sh
ykman info
ykman config usb --list
gpg --card-status
```

- Write down the serial number and firmware version that `ykman info` prints; the serial goes in `SECURITY.md`.
- If `ykman config usb --list` does not include OpenPGP, enable it with `ykman config usb --enable OPENPGP`.
- On a YubiKey that was used before, `ykman openpgp reset` wipes its OpenPGP keys and restores the default PINs.
- GnuPG holds the YubiKey through `scdaemon`, so run `gpgconf --kill scdaemon` before any `ykman openpgp` command.

## Set the PINs

The defaults are PIN `123456` and Admin PIN `12345678`.
Change both in the card editor:

```text
gpg --card-edit
gpg/card> admin
gpg/card> kdf-setup    # optional, and only while the PINs are still the defaults
gpg/card> passwd       # choose 1 to change the PIN (6 or more characters), 3 to change the Admin PIN (8 or more), then q
gpg/card> url https://github.com/your-github-login.gpg    # optional
gpg/card> quit
```

Store the Admin PIN in your password manager.

## Choose how to create the key

Generating the key on the YubiKey means the private key never exists anywhere else, but losing the YubiKey then means a new fingerprint, a new allowlist entry and a new key on GitHub.
Creating an offline primary key and moving only its signing subkey to the YubiKey keeps the same fingerprint across lost and spare YubiKeys and lets you extend the expiry, at the cost of the key existing briefly on the Mac, in a RAM disk, while it is offline.

Use the offline primary key.
With one maintainer and a threshold of 1, losing the signing identity blocks releases until a new key reaches the allowlist.

## Create the key (recommended)

Turn off Wi-Fi and unplug any network cable first.
Keep this terminal open until the end of [Export the public key and close the RAM disk](#export-the-public-key-and-close-the-ram-disk), because the later steps use its variables.

```sh
HANDLE=your-github-login
RAMDISK=$(hdiutil attach -nomount ram://204800 | tr -d '[:space:]')
diskutil erasevolume HFS+ tn-gpg "$RAMDISK"
gpgconf --kill scdaemon
export GNUPGHOME=/Volumes/tn-gpg/gnupg
mkdir -m 700 "$GNUPGHOME"
cp ~/.gnupg/gpg-agent.conf "$GNUPGHOME/"
gpg --quick-generate-key "Your Name <you@example.org>" ed25519 cert never
FPR=$(gpg --list-keys --with-colons | awk -F: '/^fpr:/ {print $10; exit}')
gpg --quick-add-key "$FPR" ed25519 sign 2y
SUBKEY_FPR=$(gpg --list-keys --with-colons "$FPR" | awk -F: '/^sub:/ {s=1; next} s && /^fpr:/ {print $10; exit}')
echo "primary $FPR"
echo "signing subkey $SUBKEY_FPR"
```

The RAM disk holds 100 MB and is gone after a detach or a reboot.
The `gpgconf --kill scdaemon` line runs before `GNUPGHOME` changes, so it stops the `scdaemon` of your normal keyring, which still holds the YubiKey after you set the PINs; only one `scdaemon` can use the card at a time.
The primary key can only certify other keys, never expires, and stays offline; the signing subkey expires in two years.
Use your GitHub-verified email address in the user ID.
Give the primary key a strong passphrase and store it in your password manager.

### Back up before moving anything

`keytocard` replaces the local copy of the subkey with a pointer to the card, so make the backups first.
Run this once for each USB drive, with `USB` set to where the drive is mounted:

```sh
USB=/Volumes/BACKUP1
hdiutil create -size 64m -fs HFS+ -encryption AES-256 -volname tn-gpg-backup "$USB/tn-gpg-backup.dmg"
hdiutil attach "$USB/tn-gpg-backup.dmg"
gpg --armor --export-secret-keys "$FPR" > "/Volumes/tn-gpg-backup/$FPR-secret.asc"
gpg --armor --export "$FPR" > "/Volumes/tn-gpg-backup/$FPR-public.asc"
cp "$GNUPGHOME/openpgp-revocs.d/$FPR.rev" /Volumes/tn-gpg-backup/
hdiutil detach /Volumes/tn-gpg-backup
```

`hdiutil create` asks for a password for the disk image; store it in your password manager.
Also print the revocation certificate (`$FPR.rev`) and keep the paper apart from the YubiKey.

### Move the signing subkey to the card

```text
gpg --edit-key "$FPR"
gpg> key 1
gpg> keytocard    # choose (1) Signature key
gpg> save
```

`keytocard` asks for the key's passphrase, then the Admin PIN.
Then make the YubiKey require a touch for every signature, and check the result:

```sh
gpgconf --kill scdaemon
ykman openpgp keys set-touch sig on
gpg --card-status
ykman openpgp info
```

`set-touch` asks for the Admin PIN.
The `fixed` policy is stricter than `on`: it cannot be changed again without resetting the OpenPGP application.
`gpg --card-status` should list your subkey's fingerprint as the signature key, and `ykman openpgp info` should show the signature touch policy as on.

### Export the public key and close the RAM disk

```sh
gpg --armor --export --export-options export-minimal "$FPR" > ~/"$HANDLE".asc
gpgconf --kill all
unset GNUPGHOME
hdiutil detach /Volumes/tn-gpg
gpg --import ~/"$HANDLE".asc
gpg --card-status
gpg --list-secret-keys --with-subkey-fingerprints
echo "$FPR:6:" | gpg --import-ownertrust
```

Running `gpg --card-status` after the import links the subkey to the card.
`gpg --list-secret-keys` should show `sec#`, meaning the primary key is not on this machine, and `ssb>`, meaning the subkey is on a card.
You can turn the network back on.

## Alternative: generate on the card

```text
gpg --card-edit
gpg/card> admin
gpg/card> key-attr    # choose ECC, then Curve 25519, for each of the three keys
gpg/card> generate    # decline the off-card backup and set the expiry to 2y
gpg/card> quit
```

Here the card's signature key is the primary key, so set both `FPR` and `SUBKEY_FPR` to the signature key fingerprint that `gpg --card-status` shows.
Then set the touch policy and export the public key as above, and continue with the allowlist.

## Add the key to the allowlist

1. On a branch, copy `~/$HANDLE.asc` to `.github/maintainer-gpg-keys/<handle>.asc`, replacing the placeholder file if your handle has one.
2. Fill in your row of the maintainer release keys table in `SECURITY.md`: handle, primary key fingerprint (from `gpg --show-keys --with-fingerprint ~/"$HANDLE".asc`), YubiKey serial, date added and status.
3. Open a PR; another maintainer reviews it and confirms the fingerprint with you over a separate channel, such as a call.

The key can sign releases once the PR is on `main`, because every check reads the allowlist from there.

## Configure git

If this is a new terminal, set the variables again first:

```sh
HANDLE=your-github-login
FPR=$(gpg --show-keys --with-colons ~/"$HANDLE".asc | awk -F: '/^fpr:/ {print $10; exit}')
SUBKEY_FPR=$(gpg --list-keys --with-colons "$FPR" | awk -F: '/^sub:/ {s=1; next} s && /^fpr:/ {print $10; exit}')
```

Then:

```sh
git config --global user.name "Your Name"
git config --global user.email you@example.org
git config --global user.signingkey "${SUBKEY_FPR}!"
git config --global gpg.program "$(brew --prefix)/bin/gpg"
```

The trailing `!` makes GnuPG use exactly that subkey instead of choosing one itself, which matters once a spare YubiKey adds a second signing subkey.
`user.email` must be the email in the key's user ID and verified on GitHub, or GitHub shows your tags as Unverified.

## Add the key to GitHub

```sh
gh auth refresh -h github.com -s write:gpg_key
gh gpg-key add ~/"$HANDLE".asc
gh gpg-key list
```

## Test signing

Sign and verify a tag in a throwaway repository:

```sh
cd "$(mktemp -d)" && git init -q && git commit -q --allow-empty -m init
git tag -s sigtest -m sigtest
git verify-tag sigtest
```

Then push it to a private scratch repository of your own and ask GitHub whether it verifies:

```sh
gh repo create "$HANDLE/gpg-sigtest" --private --source=. --push
git push origin sigtest
gh api "repos/$HANDLE/gpg-sigtest/git/tags/$(git rev-parse sigtest)" --jq .verification.verified
```

The last command should print `true`; if it does not, `--jq .verification.reason` says why.
Delete the scratch repository afterwards, which needs the `delete_repo` scope:

```sh
gh auth refresh -h github.com -s delete_repo
gh repo delete "$HANDLE/gpg-sigtest" --yes
```

Never test with a tag name starting with `v` on the telcoin-network repository, because pushing it starts the release workflow.

Last, make a detached signature like the one `make release-sign` makes:

```sh
echo test > t
gpg --armor --detach-sign --local-user "${SUBKEY_FPR}!" --output t.asc t
gpg --verify t.asc t
```

## PIN and touch during signing

- The first signature after you plug in the YubiKey asks for the PIN through pinentry-mac, and the card keeps it until it is unplugged, because `gpg --card-status` shows `Signature PIN ....: not forced`.
- For every signature the YubiKey blinks until you touch it, so `make release-tag` and `make release-sign` each need one touch.
- If you miss the touch window the signature fails; run the command again.
- `gpg --card-status` shows the tries left in `PIN retry counter`; each PIN allows 3.
- If the PIN is blocked, unblock it with the Admin PIN: `gpg --card-edit`, then `admin`, `passwd` and `2`.
- If the Admin PIN is blocked too, run `ykman openpgp reset`, [set the PINs](#set-the-pins) again, and put the signing subkey back on the card from a backup: open it with [Work with the backup](#work-with-the-backup), move that card's subkey as in [Move the signing subkey to the card](#move-the-signing-subkey-to-the-card), selecting it with `key N` in the order `gpg --edit-key` lists the subkeys, and close the RAM disk without writing the key back.

## Registry access on xerxes

`make release-build` and `make release-publish` push to `ghcr.io/telcoin-association/telcoin-network`, which needs a classic personal access token with only the `write:packages` scope.
Create one at `https://github.com/settings/tokens/new?scopes=write:packages&description=tn-release-xerxes` with a 90-day expiry.
That link selects `write:packages` alone; ticking the scope by hand on the token page also selects `repo`, which this token must not have.
Then, on xerxes:

```sh
make docker-login
```

`make docker-login` runs `docker login ghcr.io` with your GitHub login as the user name, and Docker prompts for the password: paste the token there.
Keep `write:packages` off every `gh` login, on xerxes and on the laptop, so the only credential that can push images is the one Docker holds.
Unless xerxes has a Docker credential helper, `docker login` stores the token in `~/.docker/config.json`, so log out after each release, as step 6 of [Releasing](releasing.md#6-verify-and-publish-xerxes) does.
The YubiKey never goes to xerxes, and the packages token never goes on the laptop.

## Work with the backup

Extending, adding or revoking a subkey needs the primary key.
Each time, take the Mac offline and open a backup in a fresh RAM disk keyring:

```sh
HANDLE=your-github-login
FPR=$(gpg --show-keys --with-colons ~/"$HANDLE".asc | awk -F: '/^fpr:/ {print $10; exit}')
USB=/Volumes/BACKUP1
RAMDISK=$(hdiutil attach -nomount ram://204800 | tr -d '[:space:]')
diskutil erasevolume HFS+ tn-gpg "$RAMDISK"
gpgconf --kill scdaemon
export GNUPGHOME=/Volumes/tn-gpg/gnupg
mkdir -m 700 "$GNUPGHOME"
cp ~/.gnupg/gpg-agent.conf "$GNUPGHOME/"
hdiutil attach "$USB/tn-gpg-backup.dmg"
gpg --import "/Volumes/tn-gpg-backup/$FPR-secret.asc"
```

After changing the key, and before any `keytocard`, write it to a file in the RAM disk and check it:

```sh
gpg --armor --export-secret-keys "$FPR" > /Volumes/tn-gpg/secret.asc
gpg --list-packets /Volumes/tn-gpg/secret.asc | grep -c -E 'gnu-(dummy|divert-to-card)'
```

The count must be `0`.
Any other number means a key in the RAM disk keyring is only a pointer to a card, so stop and leave the backups as they are.
Then copy the key to the open backup:

```sh
cp /Volumes/tn-gpg/secret.asc "/Volumes/tn-gpg-backup/$FPR-secret.asc"
gpg --armor --export "$FPR" > "/Volumes/tn-gpg-backup/$FPR-public.asc"
hdiutil detach /Volumes/tn-gpg-backup
```

Attach the second drive's disk image with `hdiutil attach /Volumes/BACKUP2/tn-gpg-backup.dmg` and run the same three commands again.
Never write the key back after `keytocard`: the RAM disk keyring then holds only a pointer to the card for that subkey, and the backups would lose its secret.

When you are done, close everything the same way as after creating the key:

```sh
gpg --armor --export --export-options export-minimal "$FPR" > ~/"$HANDLE".asc
gpgconf --kill all
unset GNUPGHOME
hdiutil detach /Volumes/tn-gpg
gpg --import ~/"$HANDLE".asc
```

If a backup is still attached, detach it with `hdiutil detach /Volumes/tn-gpg-backup` first.

GitHub does not update a key in place, so after any change, replace it there too: find its ID with `gh gpg-key list`, remove it with `gh gpg-key delete <key-id>`, and add `~/$HANDLE.asc` again with `gh gpg-key add`.

## Spare YubiKey

Give the spare its own signing subkey, so losing one card only costs that card's subkey:

1. [Set the PINs](#set-the-pins) on the spare, and open a backup with [Work with the backup](#work-with-the-backup).
2. Add a subkey with `gpg --quick-add-key "$FPR" ed25519 sign 2y`, then write the key to both backups as in [Work with the backup](#work-with-the-backup).
3. With the spare plugged in, find the new subkey's number.
   A new subkey goes last, so its number is the count of subkeys; `gpg --edit-key` counts revoked and expired subkeys too, and so does this command:

   ```sh
   N=$(gpg --list-keys --with-colons "$FPR" | awk -F: '/^sub:/ {n++} END {print n}')
   echo "key $N"
   ```

   Run `gpg --edit-key "$FPR"` and the `key` command that printed, check that the subkey created today is now marked `ssb*`, then run `keytocard`, choose `(1) Signature key`, and `save`.
   Then run `gpgconf --kill scdaemon` and `gpgconf --homedir ~/.gnupg --kill scdaemon`, so that no `scdaemon` holds the spare, and set its touch policy with `ykman openpgp keys set-touch sig on`.
4. Close the RAM disk as in [Work with the backup](#work-with-the-backup) without writing the key back again, because the keyring now holds only a pointer to the spare for the new subkey, and replace the key on GitHub.
5. Open a PR that updates `.github/maintainer-gpg-keys/<handle>.asc` in place and adds the spare's serial to your `SECURITY.md` row.
6. Store the spare offline.

To sign with the spare, plug it in, run `gpg --card-status`, and point `user.signingkey` (or `RELEASE_GPG_KEY` for one command) at its subkey fingerprint followed by `!`.

## Expiry, loss and rotation

### Extend the expiry

Set a reminder well before the subkey expires.
Once a signing subkey expires, its signatures stop counting, on older releases too: `make release-verify`, CI and the check on [Installing a release](../getting-started/installing-a-release.md) read the keys from `main` and reject them.
Those releases verify again once the subkey is extended and `<handle>.asc` on `main` is re-exported; until then, or if it is never extended, they cannot be verified.

1. Open a backup with [Work with the backup](#work-with-the-backup).
2. Run `gpg --quick-set-expire "$FPR" 2y <subkey fingerprint>`, listing every signing subkey that signed a release and is not revoked, even one you no longer use.
3. Write the key to both backups as in [Work with the backup](#work-with-the-backup), close the RAM disk, and replace the key on GitHub.
4. Open a PR that updates `.github/maintainer-gpg-keys/<handle>.asc` in place.

The YubiKey itself does not change.

### Lost or broken YubiKey

If the primary key is safe, revoke only the lost card's subkey:

1. Open a backup with [Work with the backup](#work-with-the-backup).
2. Run `gpg --edit-key "$FPR"`, select the lost card's subkey with `key N`, then `revkey` and `save`.
3. Write the key to both backups as in [Work with the backup](#work-with-the-backup).
4. Switch to the spare by pointing `user.signingkey` at its subkey, or set up a replacement YubiKey with steps 2 and 3 of [Spare YubiKey](#spare-yubikey).
5. Close the RAM disk as in [Work with the backup](#work-with-the-backup) without writing the key back again, and replace the key on GitHub.
6. Open a PR that updates `.github/maintainer-gpg-keys/<handle>.asc` in place, exported without the revoked subkey as its [README](https://github.com/Telcoin-Association/telcoin-network/blob/main/.github/maintainer-gpg-keys/README.md#rules) describes, and the serial and date in your `SECURITY.md` row.

### Primary key compromised

1. Take the revocation certificate from a backup or the printed copy, and remove the leading `:` from its `-----BEGIN PGP PUBLIC KEY BLOCK-----` line, which GnuPG adds so the file cannot be imported by accident.
2. Import it with `gpg --import "$FPR.rev"`.
3. Delete the key from GitHub with `gh gpg-key delete <key-id>`, so the GitHub cross-check no longer lists it.
4. Open a PR that deletes `.github/maintainer-gpg-keys/<handle>.asc` and marks your `SECURITY.md` row `revoked YYYY-MM-DD`.
5. Tell the other maintainers and the operators which releases were signed after the compromise date; they are no longer trusted.
6. Start this page again with a new key.

If both backups are lost but nothing suggests a compromise, the key keeps working until its subkey expires, but it can no longer be extended or given new subkeys.
Create a new key with this page, and replace your allowlist file and `SECURITY.md` row in one PR before the old subkey expires.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `gpg --card-status` finds no card, or reports a card error | Run `gpgconf --kill scdaemon` and plug the YubiKey in again; while `GNUPGHOME` points at the RAM disk, also run `gpgconf --homedir ~/.gnupg --kill scdaemon`, because only one `scdaemon` can hold the card. If that does not help, add `disable-ccid` and `pcsc-shared` on separate lines to `~/.gnupg/scdaemon.conf`, and to `$GNUPGHOME/scdaemon.conf` while it points at the RAM disk, then kill `scdaemon` again. |
| `ykman` cannot connect to the YubiKey | Run `gpgconf --kill scdaemon`, and while `GNUPGHOME` points at the RAM disk also `gpgconf --homedir ~/.gnupg --kill scdaemon`, then retry. |
| `keytocard` rejects the ed25519 subkey | The firmware is older than 5.2.3; create the subkey with `rsa4096` instead of `ed25519`. |
| git prints `gpg failed to sign the data` | Run `echo test \| gpg --clearsign --local-user "${SUBKEY_FPR}!"` to see GnuPG's own error. |
| GitHub shows a tag as Unverified | The user ID's email is not verified on your GitHub account, or the key was uploaded before its signing subkey existed; fix the email, or replace the key on GitHub. |
