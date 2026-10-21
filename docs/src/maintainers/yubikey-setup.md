# YubiKey signing setup

This page is the one-time setup for a maintainer who signs releases.
When you finish it you have:

- an OpenPGP key generated on a YubiKey, which it never leaves;
- that key on the release allowlist;
- GitHub showing your signed tags as Verified;
- a laptop ready for `make release-tag` and `make release-sign`;
- registry access on the build server, the Ubuntu or Debian Linux x86_64 host that runs `make release-build` and `make release-publish`.

## What you need

- A YubiKey 5 with firmware 5.2.3 or later, which ed25519 keys need, and preferably a second one as a spare.
- A Mac with Homebrew.
- An email address verified on your GitHub account, for the key's user ID (check at `https://github.com/settings/emails`).
- About half an hour.

## Scripted setup

[`etc/yubikey-setup.sh`](https://github.com/Telcoin-Association/telcoin-network/blob/main/etc/yubikey-setup.sh) runs the steps from [Install the tools](#install-the-tools) through [Test signing](#test-signing); `make yubikey-setup` starts it.
It checks what is already there before it asks: installed tools, `gpg-agent.conf`, the YubiKey's firmware and OpenPGP application, your git `user.name`, `user.email` and `user.signingkey`, your GitHub login from `gh` or `git config github.user`, and a key already on the YubiKey or in `~/.gnupg`.
It only asks for what is missing, and it can be run again to resume.
`gh` is optional: with an OAuth login from `gh auth login` the script adds the key on GitHub itself, and otherwise it tells you how to add the key by hand and then checks `https://github.com/<handle>.gpg`.

The script does not weaken the procedure.
PINs are typed only into the prompts of pinentry-mac and ykman, never into the script.
It generates the key on the YubiKey only after you type `yes`, and it never exports the private key.
It shows where gpg wrote the revocation certificate and waits while you store it.
It checks every step by reading the card, the keyring, the files and GitHub back.
It fills in the allowlist file, your `SECURITY.md` row and your row in the allowlist README on the current branch and stages them, but it never commits, pushes or opens the allowlist PR for you.
It skips the optional `kdf-setup` and `url` card settings; do those by hand before its PIN step if you want them.
It treats a YubiKey as new only when all three key slots, signature, encryption and authentication, are empty.
When the YubiKey holds a key that matches nothing on the Mac, the script offers to wipe the card only after it has looked for that key in the allowlist on `origin/main`, fetched first, and at `https://github.com/<handle>.gpg` when it knows your login; it refuses the wipe when it finds the key there, and names where, or when it cannot read the allowlist on `origin/main`.

`etc/yubikey-setup.sh check` reports what it finds without writing anything.
The sections below are the reference for what the script does, and the only path for a [spare YubiKey](#spare-yubikey) and for [expiry, loss and rotation](#expiry-loss-and-rotation).

## Install the tools

Install them on the Mac itself: GnuPG reaches the YubiKey through the host's smart card and USB stack, which a container cannot use.

```sh
brew install gnupg pinentry-mac ykman
mkdir -p ~/.gnupg && chmod 700 ~/.gnupg
echo "pinentry-program $(brew --prefix)/bin/pinentry-mac" >> ~/.gnupg/gpg-agent.conf
echo "no-allow-external-cache" >> ~/.gnupg/gpg-agent.conf
gpgconf --kill gpg-agent
```

`ykman --version` must print 5.6.0 or later, the first release with `ykman openpgp keys info`; `brew upgrade ykman` updates an older one, and the setup script refuses it.
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
  Before you run it, check that the fingerprint `gpg --card-status` shows as the signature key is not in `git show origin/main:.github/maintainer-gpg-keys/<handle>.asc | gpg --show-keys` or at `https://github.com/<handle>.gpg`: a release key on the allowlist exists only on its YubiKey.
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

## Where the key lives

The key is generated inside the YubiKey and never leaves it: no copy exists on the Mac or anywhere else.
Signing with it takes the YubiKey, its PIN and a touch.
The cost is recovery.
If the YubiKey is lost or broken, or its Admin PIN is blocked, the key has to be replaced: a new key on a YubiKey, a new allowlist PR and a new key on GitHub.
Releases the old key signed keep verifying until the key expires or its revocation reaches `main`; after that they cannot be verified.
The maintainers accept this because more maintainers are joining, and the signature threshold moves to 2 of n as they do.

## Create the key

Set your GitHub login for the commands on this page:

```sh
HANDLE=your-github-login
```

Then generate the key in the card editor:

```text
gpg --card-edit
gpg/card> admin
gpg/card> key-attr    # for each of the three keys choose (2) ECC, then (1) Curve 25519
gpg/card> generate    # decline the off-card backup and set the expiry to 2y; see the questions below
gpg/card> quit
```

`key-attr` asks about the signature, encryption and authentication keys in turn; choose `(2) ECC` and then `(1) Curve 25519` for each.
Curve 25519 on all three is the recommendation for every new key; RSA with 4096 bits on all three is the fallback for a YubiKey that refuses ECC (see [Troubleshooting](#troubleshooting)).

The first key on the allowlist, `grantkee.asc` with primary fingerprint `4E36D35A4AB61B589861D01AC7173F2377CA7573`, predates this recommendation: its primary key is RSA 2048 and certifies and signs, its encryption subkey is RSA 2048, its authentication subkey is Ed25519, and all three expire on 2028-10-16.
It is accepted as it is.
RSA 2048 on the YubiKey signs tags and `SHA256SUMS` like the recommended keys, and what the allowlist checks is the primary key fingerprint in a `VALIDSIG` line, not the algorithm.
`generate` then asks:

- `Make off-card backup of encryption key? (Y/n)`: answer `n`, so the encryption key is generated on the card like the others.
- `Key is valid for? (0)`: `2y`, then `y` to `Is this correct?`.
- `Real name`: your name.
- `Email address`: your GitHub-verified email address.
- `Comment`: leave it empty.
- `Change (N)ame, (C)omment, (E)mail or (O)kay/(Q)uit?`: `O`.

pinentry-mac asks for the Admin PIN, which `key-attr` and `generate` need, and then for the PIN, with which the card signs the new key's user ID and its revocation certificate.
There is no passphrase.
Generation takes under a minute, and gpg ends by printing `revocation certificate stored as` followed by the file's path.
If `generate` asks `Replace existing keys?`, the YubiKey already holds a key: answer `n`, and wipe it with `ykman openpgp reset` only if you no longer need that key.

The key that signs is the primary key, in the card's signature slot.
`generate` also puts an encryption key and an authentication key in the card's other two slots; releases do not use them.
The primary key's fingerprint is the one that goes on the allowlist:

```sh
FPR=$(gpg --card-status --with-colons | awk -F: '$1 == "fpr" {print $2}')
echo "$FPR"
```

### Store the revocation certificate

gpg wrote a revocation certificate for the key:

```sh
cat ~/.gnupg/openpgp-revocs.d/"$FPR".rev
```

Store it in your password manager, or print it and keep the paper apart from the YubiKey.
It is the only way to revoke the key once the YubiKey is lost, because a new revocation certificate can only be made with the private key, which exists only on the card.
Anyone who has it can revoke your key, so keep it as private as the Admin PIN.

### Require a touch to sign

Make the YubiKey require a touch for every signature, and check the result:

```sh
gpgconf --kill scdaemon
ykman openpgp keys set-touch sig on
gpg --card-status
gpg --list-secret-keys "$FPR"
ykman openpgp info
```

`set-touch` asks for the Admin PIN.
The `fixed` policy is stricter than `on`: it cannot be changed again without resetting the OpenPGP application.
`gpg --card-status` should list `$FPR` as the signature key.
`gpg --list-secret-keys` should show `sec>`, meaning the secret key is on a card.
`ykman openpgp info` should show the signature touch policy as on.

### Export the public key

```sh
gpg --armor --export --export-options export-minimal "$FPR" > ~/"$HANDLE".asc
gpg --show-keys ~/"$HANDLE".asc
```

`gpg --show-keys` should list one `pub` key with fingerprint `$FPR` and your user ID.
The file holds only the public key; it goes on the allowlist and on GitHub.

## Add the key to the allowlist

1. On a branch, copy `~/$HANDLE.asc` to `.github/maintainer-gpg-keys/<handle>.asc`.
2. Fill in your row of the maintainer release keys table in `SECURITY.md`: handle, primary key fingerprint (`$FPR`), YubiKey serial, date added and status `active`.
3. Set the Status of your row in the Files table of `.github/maintainer-gpg-keys/README.md` to `Active since YYYY-MM-DD.`, adding the row if your handle has none.
4. Open a PR; another maintainer reviews it and confirms the fingerprint with you over a separate channel, such as a call.

The key can sign releases once the PR is on `main`, because every check reads the allowlist from there.

## Configure git

If this is a new terminal, plug in the YubiKey and set the variables again first:

```sh
HANDLE=your-github-login
FPR=$(gpg --card-status --with-colons | awk -F: '$1 == "fpr" {print $2}')
```

Then:

```sh
git config --global user.name "Your Name"
git config --global user.email you@example.org
git config --global user.signingkey "${FPR}!"
git config --global gpg.program "$(brew --prefix)/bin/gpg"
```

The trailing `!` makes GnuPG sign with exactly that key instead of choosing a signing key itself.
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
gpg --armor --detach-sign --local-user "${FPR}!" --output t.asc t
gpg --verify t.asc t
```

## PIN and touch during signing

- The first signature after you plug in the YubiKey asks for the PIN through pinentry-mac, and the card keeps it until it is unplugged, because `gpg --card-status` shows `Signature PIN ....: not forced`.
- For every signature the YubiKey blinks until you touch it, so `make release-tag` and `make release-sign` each need one touch.
- If you miss the touch window the signature fails; run the command again.
- `gpg --card-status` shows the tries left in `PIN retry counter`; each PIN allows 3.
- If the PIN is blocked, unblock it with the Admin PIN: `gpg --card-edit`, then `admin`, `passwd` and `2`.
- A blocked Admin PIN cannot be unblocked.
  The key still signs while the PIN works, but a blocked PIN could then never be unblocked, so replace the key as [Lost or broken YubiKey](#lost-or-broken-yubikey) describes.
- If both PINs are blocked, the key can no longer sign; follow [Lost or broken YubiKey](#lost-or-broken-yubikey), and run `ykman openpgp reset` before you create a new key on that YubiKey.

## Registry access on the build server

`make release-build` and `make release-publish` push to `ghcr.io/telcoin-association/telcoin-network`, which needs a classic personal access token with only the `write:packages` scope.
Create one at `https://github.com/settings/tokens/new?scopes=write:packages&description=tn-release-build-server` with a 90-day expiry.
That link selects `write:packages` alone; ticking the scope by hand on the token page also selects `repo`, which this token must not have.
Then, on the build server:

```sh
make docker-login
```

`make docker-login` runs `docker login ghcr.io` with your GitHub login as the user name, and Docker prompts for the password: paste the token there.
Keep `write:packages` off every `gh` login, on the build server and on the laptop, so the only credential that can push images is the one Docker holds.
Unless the build server has a Docker credential helper, `docker login` stores the token in `~/.docker/config.json`, so log out after each release, as step 6 of [Releasing](releasing.md#6-verify-and-publish-build-server) does.
The YubiKey never goes to the build server, and the packages token never goes on the laptop.

## Spare YubiKey

A spare holds a second key of its own, generated on the spare, so losing one YubiKey costs only the key on it.

1. Note your first key's fingerprint, then unplug that YubiKey and plug in the spare:

   ```sh
   FPR1=$(gpg --show-keys --with-colons ~/"$HANDLE".asc | awk -F: '/^fpr:/ {print $10; exit}')
   ```

2. On the spare, [check the YubiKey](#check-the-yubikey), [set the PINs](#set-the-pins), and follow [Create the key](#create-the-key) with the same name and email through [Require a touch to sign](#require-a-touch-to-sign).
   Store the spare's revocation certificate too.
   Skip [Export the public key](#export-the-public-key), which would overwrite `~/$HANDLE.asc`; the next step exports both keys.
3. Export the spare's key on its own for GitHub, add it there as a second key, and, on a branch in your checkout, export both keys into your allowlist file:

   ```sh
   FPR2=$(gpg --card-status --with-colons | awk -F: '$1 == "fpr" {print $2}')
   gpg --armor --export --export-options export-minimal "$FPR2" > ~/"$HANDLE"-spare.asc
   gh gpg-key add ~/"$HANDLE"-spare.asc
   gpg --armor --export --export-options export-minimal "$FPR1" "$FPR2" > .github/maintainer-gpg-keys/"$HANDLE".asc
   gpg --show-keys .github/maintainer-gpg-keys/"$HANDLE".asc
   ```

   `gpg --show-keys` should list two `pub` keys, `$FPR1` and `$FPR2`.
4. Add a second row for your handle to the `SECURITY.md` table with the spare's fingerprint, serial, date added and status `active`, and open a PR.
5. Keep the spare somewhere safe, apart from the first YubiKey.

To sign with the spare, plug it in and point `user.signingkey` (or `RELEASE_GPG_KEY` for one command) at its fingerprint followed by `!`.
Signatures count per maintainer handle, so the spare does not add a second signature towards the threshold.

## Expiry, loss and rotation

### Extend the expiry

Set a reminder well before the key expires.
Once a key expires, its signatures stop counting, on older releases too: `make release-verify`, CI and the check on [Installing a release](../getting-started/installing-a-release.md) read the keys from `main` and reject them.
Those releases verify again once the key is extended and `<handle>.asc` on `main` is re-exported; until then, or if it is never extended, they cannot be verified.
Extend every key in your allowlist file, including a spare's that you rarely use.

1. Plug in the YubiKey that holds the key, set `FPR` to its fingerprint as in [Create the key](#create-the-key), and run:

   ```sh
   gpg --quick-set-expire "$FPR" 2y
   gpg --quick-set-expire "$FPR" 2y '*'
   ```

   The first command extends the primary key, which signs releases; the second extends the encryption and authentication keys to match.
   pinentry-mac asks for the PIN if the card does not hold it yet, and the YubiKey blinks for a touch for each signature gpg makes.
2. Export the key again and replace it on GitHub, which does not update a key in place:

   ```sh
   gpg --armor --export --export-options export-minimal "$FPR" > ~/"$HANDLE".asc
   gh gpg-key list
   gh gpg-key delete <key-id>
   gh gpg-key add ~/"$HANDLE".asc
   ```

   For the spare's key, write `~/$HANDLE-spare.asc` instead.
3. On a branch, copy `~/$HANDLE.asc` to `.github/maintainer-gpg-keys/<handle>.asc`, or, if the file also holds a spare's key, export both keys into it as in step 3 of [Spare YubiKey](#spare-yubikey).
4. Open a PR that updates `.github/maintainer-gpg-keys/<handle>.asc` in place.

The YubiKey itself does not change.

### Lost or broken YubiKey

This also applies to a YubiKey whose Admin PIN is blocked.
The key on it cannot be recovered, so revoke it and replace it:

1. Take the revocation certificate from your password manager or the printed copy, and remove the leading `:` from its `-----BEGIN PGP PUBLIC KEY BLOCK-----` line, which GnuPG adds so the file cannot be imported by accident.
2. Import it with `gpg --import <file>`, so your keyring marks the key revoked.
3. Delete the key from GitHub with `gh gpg-key delete <key-id>`, using the ID that `gh gpg-key list` shows, so the GitHub cross-check no longer lists it.
4. Open a PR that deletes `.github/maintainer-gpg-keys/<handle>.asc` and marks the key's row in `SECURITY.md` `revoked YYYY-MM-DD`.
   If the file also holds a spare's key, export only the spare's key into it instead of deleting it.
   If the replacement key already exists, the same PR can add it: put its export in the file and add a row for it.
5. Sign with the spare, as [Spare YubiKey](#spare-yubikey) describes, or set up a new YubiKey with this page.

Without the revocation certificate, skip steps 1 and 2 and open the PR anyway: the key stops being accepted as soon as it merges, because every check reads the allowlist from `main`.
Once the PR merges, the releases the old key signed can no longer be verified against `main`.

### Key compromised

If someone else may have signed with the key, for example because the YubiKey was stolen together with its PIN, follow [Lost or broken YubiKey](#lost-or-broken-yubikey) at once.
Then tell the other maintainers and the operators which releases were signed after the compromise date; they are no longer trusted.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| `gpg --card-status` finds no card, or reports a card error | Run `gpgconf --kill scdaemon` and plug the YubiKey in again, because only one `scdaemon` can hold the card. If that does not help, add `disable-ccid` and `pcsc-shared` on separate lines to `~/.gnupg/scdaemon.conf`, then kill `scdaemon` again. |
| `ykman` cannot connect to the YubiKey | Run `gpgconf --kill scdaemon`, then retry. |
| `key-attr` refuses ECC or Curve 25519 | The firmware is older than 5.2.3; use a newer YubiKey, or fall back to `(1) RSA` with 4096 bits for each of the three keys in `key-attr`. |
| gpg asks you to insert the card with another serial number | The key you sign with is on your other YubiKey; plug that one in, or point `user.signingkey` at the key on the YubiKey that is plugged in. |
| git prints `gpg failed to sign the data` | Run `echo test \| gpg --clearsign --local-user "${FPR}!"` to see GnuPG's own error. |
| GitHub shows a tag as Unverified | The user ID's email is not verified on your GitHub account, or GitHub holds an older export of the key, for example from before you extended it; fix the email, or replace the key on GitHub. |
