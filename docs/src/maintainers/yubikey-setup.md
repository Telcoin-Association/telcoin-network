# YubiKey signing setup

This page is the one-time setup for a maintainer who signs telcoin-network releases.
The signing key belongs to [`tn-release-signing`](https://github.com/Telcoin-Association/tn-release-signing), the repository that holds the maintainer keys every signed Telcoin project shares, and its [YubiKey signing setup](https://github.com/Telcoin-Association/tn-release-signing/blob/main/docs/yubikey-setup.md) guide covers the key from start to finish.
This repository carries `tn-release-signing` as the `release` submodule, and this page covers what is specific to telcoin-network.
When you finish both you have:

- an OpenPGP key generated on a YubiKey, which it never leaves;
- that key on the allowlist in `tn-release-signing`, at a commit that the `release` pointer on `main` records;
- GitHub showing your signed tags as Verified;
- a laptop ready for `make release-tag` and `make release-sign`;
- registry access on the build server, the Ubuntu or Debian Linux x86_64 host that runs `make release-build` and `make release-publish`.

## Scripted setup

`make yubikey-setup` runs the shared setup script as `./yubikey-setup.sh run` from `release/`, because the script works only from the root of a checkout of `tn-release-signing`.
It needs the submodule checked out, and otherwise stops with `run git submodule update --init -- release first`; run that command after each pull of `main` anyway, as [The `release` submodule](releasing.md#the-release-submodule) says.
[Scripted setup](https://github.com/Telcoin-Association/tn-release-signing/blob/main/docs/yubikey-setup.md#scripted-setup) in the shared guide says what the script checks, asks and writes, and `./yubikey-setup.sh check`, run in `release/`, reports what it finds without writing anything.

The script writes your allowlist file, `keys/<handle>.asc`, in the submodule and stages it there.
The submodule is a checkout of `tn-release-signing` at a detached `HEAD`, so create a branch in `release/` before the script stages anything.
The commit, the push and the PR that the script prints go to `tn-release-signing`, not to this repository; [Add the key to the allowlist](https://github.com/Telcoin-Association/tn-release-signing/blob/main/docs/yubikey-setup.md#add-the-key-to-the-allowlist) has the steps.
Once that PR is open, `git submodule update --init -- release` returns the submodule to the commit `main` records; until then `release-prep` refuses the checkout, because the submodule is at another commit than `main` records.

## When the key can sign

Your key can sign a telcoin-network release once a PR here moves the `release` pointer to a `tn-release-signing` commit that carries it, as [Moving the submodule pointer](releasing.md#moving-the-submodule-pointer) describes.
Until then every check here refuses it, because every check reads the keys at the pointer on `main`.
The node deployment scripts carry maintainer keys of their own, so a release signed only by a new key installs on operators' hosts once their scripts carry that key too; [Adding a maintainer or changing the threshold](releasing.md#adding-a-maintainer-or-changing-the-threshold) says which keys they carry.

## PIN and touch during signing

Each signature needs one touch of the YubiKey, and the PIN when it is the first signature since the YubiKey was plugged in.
`make release-tag` makes one signature, over the tag.
`make release-sign` makes one per manifest it signs: one, over `SHA256SUMS`, for a release that ships the node alone, and two, over `SHA256SUMS` and `SHA256SUMS-edge`, for a release that ships the edge services.
It makes every signature before it uploads any, so when the second touch is missed, gpg fails and nothing is uploaded, the first signature included; run `make release-sign` again, and it asks for both touches again.
[PIN and touch during signing](https://github.com/Telcoin-Association/tn-release-signing/blob/main/docs/yubikey-setup.md#pin-and-touch-during-signing) in the shared guide covers the prompts, the retries and a blocked PIN.

## Registry access on the build server

`make release-build` and `make release-publish` push to the release packages, which needs a classic personal access token with only the `write:packages` scope.
The release packages are the node's, `ghcr.io/telcoin-association/tn-mainnet` for mainnet tags and `ghcr.io/telcoin-association/tn-testnet` for Adiri tags, and the edge services', `ghcr.io/telcoin-association/tn-worker-gateway` and `ghcr.io/telcoin-association/tn-node-record-api`, which mainnet and Adiri tags share.
The scope lets the token write only to packages its account can write, and the [one-time setup](releasing.md#one-time-setup) gives write access on the release packages to the release maintainer account alone, so create the token on that account.
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

## Expiry, loss and rotation

[Expiry, loss and rotation](https://github.com/Telcoin-Association/tn-release-signing/blob/main/docs/yubikey-setup.md#expiry-loss-and-rotation) in the shared guide has the commands to extend a key, to replace a lost or broken YubiKey and to act on a compromised key; each ends with a PR to `tn-release-signing`.
Each change takes effect here when a PR moves the `release` pointer to it, as [Rotating or revoking a key](releasing.md#rotating-or-revoking-a-key) describes.
Once a key expires, its signatures stop counting, on older releases too: `make release-verify`, CI and the check on [Installing a release](../getting-started/installing-a-release.md) read the keys at the pointer on `main` and reject them.
