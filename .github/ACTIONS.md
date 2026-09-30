# How TN CI works

A cold build and test of the workspace on a GitHub-hosted runner takes over 45 minutes, and the e2e suites cannot run there at all within a reasonable budget.
So the full suite runs on a maintainer's machine, and CI checks that the resulting commit hash was attested on-chain.

`etc/test-and-attest.sh` (`make attest`) runs fmt, clippy, both test lanes, the two guards and the e2e suites locally, then writes the HEAD commit hash to the git attestation registry on adiri (`0xf102928273a399cda6151b8616209af019499c84`).
The `verify-on-chain` lane in `.github/workflows/pr.yaml` reads that registry back.

The lanes themselves live in **`etc/ci-lanes.sh`**, and both `test-and-attest.sh` and `pr.yaml` call it.
That is the only reason "the queue runs what was attested" is a fact rather than a hope: when the two spelled out their own commands, they had already drifted (CI ran clippy under `--all-features` only, and excluded a package deleted long ago).
Edit a lane there and both callers change together.
The e2e suites are deliberately not in that file -- anything added to it lands in the merge queue.

## What the merge queue changes

`main` is merged through a GitHub merge queue.
The queue exists because attestation cannot answer the question that keeps breaking main: an attestation covers a PR's head commit in isolation, and says nothing about `main + PR`.
Two PRs that each pass alone can still break each other, and no amount of attesting either one catches it.

So the two gates cover different things and neither replaces the other:

| | attestation (`verify-on-chain`) | merge queue (`fmt`, `clippy`, `test`, `adiri-test`, the guards) |
|---|---|---|
| commit tested | the PR head | main + this PR + every PR ahead of it in the batch |
| suite | everything, e2e included | everything CI can afford; no e2e |
| runs on | a maintainer's machine | GitHub runners |

The queue builds a temporary `gh-readonly-queue/main/...` branch and fires a `merge_group` event against it.
Three consequences worth remembering:

- **Every required check must report on `merge_group`.** A required check that only
  triggers on `pull_request` does not fail the queue, it hangs it until the queue timeout
  ejects the PR. `pr.yaml` triggers on both events, and `CI Success` is the only required
  check, on purpose: a second required name means a second workflow that has to learn the
  same lesson.
- **The maintainer and draft skips do not apply in the queue.** Whoever wrote the PR, the
  merged commit has never been compiled anywhere, so every lane runs. `CI Success` treats a
  skipped lane as a failure on `merge_group` for the same reason.
- **The attestation is checked on the pull request only, never in the queue.** GitHub
  creates the merge group's commit seconds before CI starts, so no local run could have
  covered it and nothing can attest it; a lane that demanded one would hang the queue until
  the timeout ejected the PR. It does not need one. `CI Success` is required, required
  checks must pass before a PR can be queued, so every head that reaches the queue was
  verified on its own `pull_request` run. `CI Success` accepts `verify-on-chain` as skipped
  on `merge_group` and nowhere else.

### Re-running the attestation check

`make attest` writes to adiri and touches nothing on GitHub, so nothing re-runs `verify-on-chain` by itself.
Two ways to re-run it on the same sha: request a review on the PR (`review_requested` is in the workflow's `pull_request.types` for exactly this), or re-run the failed job from the Actions tab.
Both run the script as it stands on `main` at that moment, so a change to the script or its registry address reaches an open PR on its next run.
Do not push: a new sha needs a new attestation.

### Why SQUASH

The queue's merge method is `SQUASH`, and squash is the only merge method the ruleset allows.
N queued PRs land as N single-parent commits on `main`, one per PR, subject from the PR title with its `(#N)`.
`MERGE` would land every PR commit plus one merge commit per PR; `REBASE` would land every PR commit.
Either makes `main`'s history depend on how each author organized their branch.

### Pushing to a queued PR

A push to a PR that is in the queue removes it from the queue: the queue's candidate commit was built from the old head.
The new head has no attestation, so `verify-on-chain` fails on it until a maintainer runs `make attest` again, and the ruleset dismisses the stale approval (*dismiss stale pull request approvals when new commits are pushed*).
The PR has to be re-attested, re-approved and re-queued.
That is the intended cost of a late push, and it is delivered entirely by the repository settings below; the workflow does nothing for it.

### Where the coverage gap is

The queue does not run e2e.
That coverage comes only from the attested local run, against whatever the branch was based on at the time.
This is why `test-and-attest.sh` refuses to attest a branch that is behind `origin/main`: merge or rebase first so the e2e lanes test the combination that actually lands.
`ALLOW_STALE_BASE=1` overrides it when the drift is provably irrelevant.
The nightly `durable-e2e` lane is the backstop for what still slips through.

### Repository settings this requires

The workflow changes are not enough on their own.
In order of importance:

1. **`CI Success` is the required status check** (the `main` ruleset, source GitHub
   Actions). Without it the queue would gate nothing: a PR could be queued unattested with
   no green lane. Keep *Require branches to be up to date before merging* **off**, as it is
   now: strict mode would force a re-push, and so a re-attestation, every time `main`
   moved, and the queue already tests `main + PR`. Require `CI Success` only, not
   `verify-on-chain`, which is a job inside `pr.yaml` that `CI Success` already depends on.
   GitHub matches a required check by name, so a job called `CI Success` in any workflow
   would satisfy it; keep the name unique to `pr.yaml`.
2. **Merge queue.** Merge method `SQUASH` (already set). Recommended, and not yet set: a
   build concurrency of **2** (the ruleset has 5): each group runs seven jobs, so five
   groups is about 35 concurrent jobs queueing behind the organization's runner
   concurrency. And a status check timeout of **90 minutes** (it is 60): it counts from
   the group's creation, runner backlog included, so 60 can eject a lane that took 45
   after queueing for 15.
3. **Pull request rule.** One approval is required. *Dismiss stale pull request approvals
   when new commits are pushed* (already set) is, together with (1), what makes a late push
   cost a re-approval. *Require review from Code Owners* (`require_code_owner_review`) is
   on, and `.github/CODEOWNERS` assigns `/.github/`, `/etc/`, `/Makefile` and the tool
   configuration the lanes read (`.cargo/`, `.config/`, the toolchain pins, and every
   `rustfmt.toml` and `clippy.toml`) to the four accounts in `MAINTAINERS` (the `ci-scope`
   job in `pr.yaml`), so a PR that touches any of them needs one of those four to approve
   it. GitHub reads CODEOWNERS from the PR's base branch, so a PR cannot change who has to
   review it. Other paths need the one approval, but not a code owner's. Recommended, and
   still off: *Require approval of the most recent reviewable push*, so the author of that
   push cannot approve it themselves.
4. **Repository settings** (Settings -> General -> Pull Requests). *Allow auto-merge* is not
   in the ruleset and does not look related, so it is the one that gets missed: the "Merge
   when ready" button calls the `enablePullRequestAutoMerge` GraphQL mutation even on a
   branch that has a queue, and that mutation is gated on this checkbox. With it unticked
   every attempt to queue a PR fails with *"failed enabling auto-merge for pull request"*,
   however green the PR is. Also *Allow squash merging*, with the default squash message
   set to the PR title and description.
5. **Delete the `merge-into-main` environment** (Settings -> Environments). The old
   `maintainer-verify.yaml` workflow ran in that environment; it was folded into `pr.yaml`
   and deleted, and no workflow names the environment now. The replacement lane
   deliberately has no `environment:` of its own: it reads a public RPC and uses no
   secrets, and an environment with a protection rule would park the merge queue on a
   manual approval until the queue timed out. The `main` ruleset no longer has a
   `required_deployments` rule, so nothing requires a deployment to it either. The
   environment still carries a required-reviewers protection rule, which makes it a switch
   that would hang the queue if a job ever named it again.
6. **Escape hatch.** If the `merge_group` path of `CI Success` is ever broken, no PR can
   land to fix it, because the fix itself has to pass through the queue. An admin has to
   remove the required check temporarily (or use a bypass) to land the fix, then put it
   back.

### Who can put a PR in the queue

GitHub's own answer is only "anyone with write access", and there is no finer-grained setting.
The real gate here is the attestation plus the approval: `CI Success` depends on `verify-on-chain`, required checks must pass *before* a PR can be queued, and `verify-on-chain` passes only for a commit hash already written to the registry by a holder of the MAINTAINER key.
That binds a PR that leaves the gate alone, and only such a PR.

`verify-on-chain` runs `.github/scripts/verify_commit_hash.sh` from `main` as it stands when the job starts, not from the PR, so editing the script does nothing for the PR that edits it.
The new script judges every run after it lands on `main`, on every open PR, a re-run or a review request included.
The `attest` job definition and the `CI Success` allowlist still come from the PR's merge commit, though, and the queue run uses the PR's `pr.yaml` and skips `verify-on-chain`.
So a PR that edits the job or the allowlist can turn `CI Success` green without an attestation, on the PR and in the queue.
The lanes are in the same position: the queue runs `etc/ci-lanes.sh` from the merge commit, so a PR that edits it is tested by its own edit.
Nothing inside `pr.yaml` can take these out of the PR's hands; that needs a decision at the ruleset level.

What stops such a PR is the required code-owner review (item 3 above).
Whoever approves must treat any change to a path `.github/CODEOWNERS` lists as a change to the gate itself: a green `CI Success` on such a PR does not by itself show that it was attested or tested.
That includes the tool configuration: `[profile.ci]` in `.config/nextest.toml` is read only by the CI lanes (`NEXTEST_PROFILE: ci` in `pr.yaml`), so `make attest` never exercises an edit to it.
The admin role can bypass the `main` ruleset, and with it both the approval and `CI Success`.
All four code owners hold that role, so the code-owner review is a check on everyone but them: any one of the four can land a change to the gate that no second owner has read, and so can every other account with admin on the repository (Settings -> Collaborators and teams), code owner or not.
The bypass mode is *Always allow*, which also lets an admin push to `main` with no pull request at all; *For pull requests only* would keep the escape hatch in item 6 and leave a pull request behind every bypass.

## Caches

`main` is the only writer of the cache entries the lanes restore.
`.github/workflows/cache-deps.yaml` runs there and saves two entries: `clippy-cache` (dependencies for both clippy passes under the nightly pin) and `test-cache` (dependencies for both test lanes, default and adiri features, under the stable pin). `Swatinem/rust-cache` does not save the workspace crates or their test binaries, so each PR still builds those from its own source.
The lanes in `pr.yaml` restore those and never save (`save-if: "false"`): a cache saved by a `pull_request` run is scoped to that PR's branch and one saved by a `merge_group` run lands on the queue's throwaway branch, so nothing else could ever read them, while the upload adds minutes to the critical path and eats quota that evicts the entries the queue does read.

A warm runs on a push to `main` that touches a `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `rust-nightly`, `.cargo/config.toml`, `etc/ci-lanes.sh` or the workflow itself; on a schedule twice a week (GitHub deletes an entry not accessed for seven days, and a quiet week would otherwise leave the queue cold); and by hand from the Actions tab (*Warm dependency cache* -> *Run workflow*).
When the entry already matches, the run restores it, rebuilds only the workspace crates, saves nothing, and is done in a few minutes.

All six cache steps (two in `cache-deps.yaml`, three in `pr.yaml`, one in `durable-e2e.yaml`) pin the same `Swatinem/rust-cache` release.
Three of its properties shape all of this.
They were checked against v2.9.2, and a later release can change them; the third is about a release that changes the key:

- The key is `<prefix-key>-<shared-key>-<os>-<arch>-<env hash>-<hash of relevant Cargo
  manifests, lockfiles and toolchain/config files>`, so the two entries start with
  `v1-rust-clippy-cache-Linux-x64-` and `v1-rust-test-cache-Linux-x64-`. The env hash
  covers `rustc -vV` of every toolchain that `rustup toolchain list` reports, and every
  variable whose name starts with `CARGO`, `CC`, `CFLAGS`, `CXX`, `CMAKE` or `RUST` and
  whose value is non-empty. So the `env:` block and the steps before the cache step,
  including every step that installs a toolchain, must be identical in `cache-deps.yaml`
  and `pr.yaml`; they are, and both files say so. The clippy jobs hash seven variables
  (the six in `env:` plus `RUST_NIGHTLY`, written to `GITHUB_ENV` before the cache step);
  the test jobs hash six. The toolchains hashed are the stable Rust preinstalled on the
  runner image, the stable pinned in `rust-toolchain.toml`, and in the clippy jobs the
  pinned nightly. Each cache step prints what it computed in its "Cache Configuration"
  log group: Restore Key, Cache Key, and the environment considered, which includes a
  "Rust Versions:" list of every toolchain it hashed. When a restore misses, compare that
  group between the two workflows first.
- Entries are immutable, and an exact key hit skips the save. So changing *what* a warm job
  builds (a lane added, a feature set changed) writes nothing until the key changes: bump
  `prefix-key` in both workflows, in the same pull request; the next bullet says why not
  `cache-deps.yaml` first. (Or delete the entries under Settings -> Actions -> Caches and
  re-run the warm.) Both workflows now use `prefix-key: v1-rust`. Entries under an older
  key, `v0-rust-clippy-cache-*` and `v0-rust-test-cache-*` from before `v1-rust` and
  rust-cache v2.7.7's with no `x64` after `Linux-`, are superseded generations, which the
  `prune-caches` job (below) deletes.
- How the key is computed belongs to the release, so a `Swatinem/rust-cache` release that
  changes it is a change of key like any other. Whoever reviews a rust-cache bump reads
  the release notes of every release it covers for anything about the key, hashing or
  cached paths; comparing the "Cache Key" a warm prints before and after the bump shows
  whether a release changes the key. Such a release moves every cache step together, in
  one pull request and one attestation, and the queue then builds every dependency cold,
  from that pull request's own queue run until the warm its merge triggers has finished
  on `main`; whether a cold lane fits its `timeout-minutes` is the thing to check (the
  one cold figure measured is below, with the warm timings). Moving the writer,
  `cache-deps.yaml`, first no longer keeps the queue warm: the `prune-caches` job deletes
  the old key's entries at the end of the first warm that saves the new ones, so
  `pr.yaml` would have nothing to restore until it moved too. The bump from v2.7.7 to
  v2.9.2 changed the key and moved all six steps together. Dependabot sends rust-cache
  bumps as a pull request of their own (`.github/dependabot.yaml`), so that such a
  release is reviewed on its own, without the other actions riding along.

Warm timings measured on `main`: the `--all-features` clippy pass compiles in about 20 s, all workspace test binaries build in 1 m 48 s, checkout with submodules takes about 80 s and the restore about 20 s.
After a heavy dependency bump, with only a partial cache to fall back on, clippy took 11.5 min and the test build 9.5 min.
The lane ceilings in `pr.yaml` (`timeout-minutes: 45`) are set from the second set of numbers, not the first; a lane anywhere near 45 minutes means the cache is broken.
One cold figure has been measured: the warm of 2026-08-28, whose test cache step restored nothing, built the test binaries of both lanes in about 15 minutes.
A cold test lane then still has to run the tests, which no GitHub runner has timed from cold.
Each `Cargo.lock` change on `main` writes a new generation of every entry, and GitHub would keep the previous one until it went 7 days without a restore, or evict the least recently used entries once the total passed the quota, which can take a live one with it.
The `prune-caches` job in `cache-deps.yaml` deletes those superseded generations of the three entries after every successful warm on `main`.
A family is every entry on `main` whose key starts `v<N>-rust-clippy-cache-`, `v<N>-rust-test-cache-` or `v<N>-rust-durable-e2e-cache-`, whatever its prefix-key version, architecture and hashes.
In each family the job keeps the entries created since the warm began, failing those the ones accessed since, and failing both the single most recently accessed one, and deletes the rest, so a family is never left empty.
It never touches an entry outside the three families: not CodeQL's, not another ref's, not another workflow's.
To run it by hand, from the repository root, `DRY_RUN=1 GH_REPO=Telcoin-Association/telcoin-network .github/scripts/prune_caches.sh` prints what it would delete and deletes nothing; the same without `DRY_RUN=1` deletes.
Run by hand, it counts "since the warm began" from the latest successful warm on `main`, and deleting needs a `gh` login that can delete caches, which takes write access to the repository.
Two things are still worth a look under Settings -> Actions -> Caches now and then: entries on other refs, because a warm dispatched on a branch writes gigabytes into that branch's scope and nothing prunes it, and the total against the quota (10 GB in September 2026).

One exposure to know about: `rust-toolchain.toml` pins `channel = "1.94"`, so a 1.94.x point release changes the rustc version, which is in the key, and every entry misses with no fallback until the next warm (the schedule within 3-4 days, or a manual dispatch).
Pinning `1.94.x` would make the rotation explicit and deliberate.
That is a decision to make, not one made here.

A second exposure, new with v2.9.2: the stable Rust preinstalled on the `ubuntu-latest` image is one of the toolchains hashed into every key, and this repository does not control it.
When GitHub updates the image to a new Rust release (roughly every six weeks, plus point releases, rolled out to the runners over several days), every entry misses with no fallback until the next warm, and while the rollout is in progress a writer and a reader can land on different images and compute different keys.
A missed restore whose "Rust Versions:" list shows a new stable is the sign.
When it happens, dispatch a warm by hand (*Warm dependency cache* -> *Run workflow*); until that warm has finished, the lanes build cold.
While the rollout lasts, the warm can itself land on an old image, which its own "Rust Versions:" list shows, and then it has to be dispatched again.
The alternative, making the installed set deterministic by removing the image's own toolchain before each of the six cache steps, in lockstep, was considered and not adopted; it is untested on a runner.

## Action pins

Every `uses:` in `.github/workflows/` names the action by a full 40-character commit SHA, with the release it stands for in a trailing comment on the same line (`actions/checkout@<sha> # vX.Y.Z`).
A tag such as `v4` is a pointer the action's owner can move at any time.
A moved tag runs new code in the gate on the next run, with no change in this repository and nothing for a reviewer to see.
A SHA cannot be moved, so the code that runs is the code that was reviewed when the pin was set.
Three positions made this worth doing:

- `taiki-e/install-action` runs in `cache-deps.yaml`'s `warm-test-cache` job, which writes
  the `main`-scope cache entry that every PR and queue run restores.
- `foundry-rs/foundry-toolchain` supplies the `cast` binary whose answer decides
  `verify-on-chain`; that binary is pinned too, by release and digest (below).
- `actions/deploy-pages` runs with `pages: write` and `id-token: write`.

The pinned actions and the runtime each one uses (each pin's SHA, and the release it stands for in the `# vX.Y.Z` comment beside it, are in the workflows, and only there):

| Action | Runtime |
|---|---|
| `actions/checkout` | node24 |
| `taiki-e/install-action` | composite (shell steps only) |
| `foundry-rs/foundry-toolchain` | node24 |
| `actions/upload-pages-artifact` | composite (runs `actions/upload-artifact`, node24, itself pinned by SHA) |
| `actions/deploy-pages` | node24 |

`Swatinem/rust-cache` is left out of the table: its pin moves on its own terms, and "Caches" above covers it.
The runtime column matters because GitHub removed Node 20 from the hosted runners on 2026-09-23 and now forces any node20 action onto Node 24, which it was not written for.

### How a pin moves

Dependabot (`.github/dependabot.yaml`) checks the actions weekly and opens one grouped pull request for all of them except `Swatinem/rust-cache`, with each SHA and its version comment rewritten together.
It proposes a release only once the release is 7 days old, so one that is pulled or found to be compromised in its first days never reaches one of its pull requests; a pin set by hand skips that wait.
The grouping is for the attestation: a pull request cannot enter the queue until a maintainer has run the full local suite on its head and attested it, and `taiki-e/install-action` alone was released about five times a week in September 2026.
`Swatinem/rust-cache` arrives as a pull request of its own, so that a release that changes how the cache key is computed can be moved as "Caches" above describes, without the other actions riding along.

Whoever reviews such a pull request:

- reads the release notes for every bump in it, all of them between the old release and
  the new one. A changed default is how `upload-pages-artifact` came to drop mdBook's
  `.nojekyll`: v4 changed it, and this repository went from v3 to v5 in one step (see
  the comment in `docs.yaml`).
- treats it as a change to the gate. It is under `.github/`, so it needs a code owner's
  approval, and its head needs `make attest` like any other. Its own workflow runs get a
  read-only token and no secrets; the new code first runs with more than that after it
  lands, in `cache-deps.yaml` (the `main` cache the lanes restore), `durable-e2e.yaml`
  (its own `main` cache entry, nightly) and `docs.yaml` (the Pages deployment).

A new `uses:` takes the same form, SHA plus `# vX.Y.Z` on the same line; Dependabot rewrites the comment only when it is on the line it updates.

One limit to know: Dependabot raises no security alert for an action pinned by SHA, only for one referenced by a version.
The weekly version update is therefore the only channel through which a fixed release of a pinned action arrives, and the cooldown holds it back 7 days.
A fix that cannot wait has to be pinned by hand.

### Checking a pin by hand

```sh
git ls-remote https://github.com/<owner>/<repo> 'refs/tags/<tag>' 'refs/tags/<tag>^{}'
```

For an annotated tag this prints two lines, and the `^{}` line is the commit to pin; the other is the tag object.
For a lightweight tag it prints one line, and that is the commit.

### What a pin does not cover

A pin fixes the action's own code, not what that code downloads when it runs.
For `foundry-rs/foundry-toolchain` the download is pinned separately, in the `attest` job in `pr.yaml`: the install step's `version` input names a Foundry release, and the step after it fails the job unless the `cast` on `PATH` has the SHA-256 digest in `CAST_SHA256`.
A release asset swapped under the same tag therefore fails `verify-on-chain` instead of deciding it.
The digest is that of the `linux_amd64` build, because `ubuntu-latest` is x64; a runner of another architecture needs a new one.
The action takes no digest itself, and Dependabot moves an action's SHA but never its inputs, so the release and the digest stay where they are until someone moves them, together and by hand:

1. Download the new release's `linux_amd64` tarball, check it against the release's own
   `.sha256` file, and hash the `cast` inside it. The commands are for Linux; on macOS,
   `shasum -a 256` stands in for `sha256sum`.

   ```sh
   v=vX.Y.Z   # the release to move to
   base="https://github.com/foundry-rs/foundry/releases/download/$v"
   curl -fsSLO "$base/foundry_${v}_linux_amd64.tar.gz"
   curl -fsSL "$base/foundry_${v}_linux_amd64.sha256" | sha256sum --check -
   tar -xzf "foundry_${v}_linux_amd64.tar.gz" cast
   sha256sum cast
   ```

   The binary is hashed, never run.
2. In `pr.yaml`, set `version` to the release and `CAST_SHA256` to the digest the last
   command printed, in the same pull request.
3. That pull request's own `verify-on-chain` run is the test that `foundryup` installs the
   tarball's `cast` unchanged on the runner. If it does not, the check step fails there,
   before anything lands.

What is still not pinned: `foundryup`, the program the action downloads to perform the install, which runs in the job before the check and so is trusted by it.
The runner image (`ubuntu-latest`) is not pinned either, and with it everything preinstalled on it.
The local `make attest` run uses whatever `cast` the maintainer has installed, which this pin does not reach.

One thing a pin does newly fix: `taiki-e/install-action` resolves a tool requested without a version (`tool: cargo-nextest`) from the manifest in the pinned commit, with a checksum, so the cargo-nextest version stays the same until the pin moves.

## Environment
Attesting devs must have "MAINTAINER" role to update contract state.

The local `test-and-attest.sh` script requires Foundry's cast.

See https://book.getfoundry.sh/getting-started/installation for installation instructions.

Add `GITHUB_ATTESTATION_PRIVATE_KEY` to a `.env` file in the project.
This is the private key (without "0x" prefix) associated with the "MAINTAINER" role address.

## Toolchain pins

Two channels are pinned in the repo root:

- **`rust-toolchain.toml`** — stable channel (currently `1.94`). Used for all compile/test commands. rustup auto-honors it inside the repo, so bare `cargo build`, `cargo test`, `cargo check`, and `cargo nextest` use stable 1.94.
- **`rust-nightly`** — single-line file containing the nightly date (currently `nightly-2026-03-20`). Used only for `cargo fmt` and `cargo clippy`, which require nightly-only rustfmt/clippy options (`imports_granularity`, `wrap_comments`, etc.). The Makefile, `etc/test-and-attest.sh`, and CI workflows read this file and invoke `cargo +<date>` explicitly.

To bump nightly: edit `rust-nightly`.
To bump stable: edit `rust-toolchain.toml` (and align with `Cargo.toml`'s `rust-version` and `etc/Dockerfile`'s base image tag).
Either bump rotates the dependency caches; see "Caches" above.
