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
2. **Merge queue.** Merge method `SQUASH` (already set). Start with a build concurrency of
   **2**: each group runs seven jobs, so five groups is about 35 concurrent jobs queueing
   behind the organization's runner concurrency. Raise the status check timeout from 60 to
   **90 minutes**: it counts from the group's creation, runner backlog included, so 60 can
   eject a lane that took 45 after queueing for 15.
3. **Pull request rule.** One approval is required. *Dismiss stale pull request approvals
   when new commits are pushed* (already set) is, together with (1), what makes a late push
   cost a re-approval. *Require review from Code Owners* (`require_code_owner_review`) is
   on, and `.github/CODEOWNERS` assigns `/.github/`, `/etc/` and `/Makefile` to the four
   accounts in `MAINTAINERS` (the `ci-scope` job in `pr.yaml`), so a PR that touches any of
   them needs one of those four to approve it. GitHub reads CODEOWNERS from the PR's base
   branch, so a PR cannot change who has to review it. Other paths need the one approval,
   but not a code owner's. Recommended, and still off: *Require approval of the most recent
   reviewable push*, so the author of that push cannot approve it themselves.
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

`verify-on-chain` runs `.github/scripts/verify_commit_hash.sh` from the PR's base commit, not from the PR, so editing the script does nothing for the PR that edits it.
The new script judges the PRs opened or pushed after it lands on `main`; a PR already open keeps its old base, and the old script, until it is pushed again.
The `attest` job definition and the `CI Success` allowlist still come from the PR's merge commit, though, and the queue run uses the PR's `pr.yaml` and skips `verify-on-chain`.
So a PR that edits the job or the allowlist can turn `CI Success` green without an attestation, on the PR and in the queue.
The lanes are in the same position: the queue runs `etc/ci-lanes.sh` from the merge commit, so a PR that edits it is tested by its own edit.
Nothing inside `pr.yaml` can take these out of the PR's hands; that needs a decision at the ruleset level.

What stops such a PR is the required code-owner review (item 3 above).
Whoever approves must treat any change under `.github/`, `etc/` or `Makefile` as a change to the gate itself: a green `CI Success` on such a PR does not by itself show that it was attested or tested.
The admin role can bypass the `main` ruleset, and with it both the approval and `CI Success`.

## Caches

`main` is the only writer of the cache entries the lanes restore.
`.github/workflows/cache-deps.yaml` runs there and saves two entries: `clippy-cache` (dependencies for both clippy passes under the nightly pin) and `test-cache` (dependencies for both test lanes, default and adiri features, under the stable pin). `Swatinem/rust-cache` does not save the workspace crates or their test binaries, so each PR still builds those from its own source.
The lanes in `pr.yaml` restore those and never save (`save-if: "false"`): a cache saved by a `pull_request` run is scoped to that PR's branch and one saved by a `merge_group` run lands on the queue's throwaway branch, so nothing else could ever read them, while the upload adds minutes to the critical path and eats quota that evicts the entries the queue does read.

A warm runs on a push to `main` that touches a `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `rust-nightly`, `.cargo/config.toml`, `etc/ci-lanes.sh` or the workflow itself; on a schedule twice a week (GitHub deletes an entry not accessed for seven days, and a quiet week would otherwise leave the queue cold); and by hand from the Actions tab (*Warm dependency cache* -> *Run workflow*).
When the entry already matches, the run restores it, rebuilds only the workspace crates, saves nothing, and is done in a few minutes.

Three properties of `Swatinem/rust-cache` (v2.9.2) shape all of this:

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
  `prefix-key` in both workflows, `cache-deps.yaml` first, then `pr.yaml` once `main` has
  written the new entries. (Or delete the entries under Settings -> Actions -> Caches and
  re-run the warm.) Both workflows now use `prefix-key: v1-rust`; the old
  `v0-rust-clippy-cache-*` and `v0-rust-test-cache-*` entries can be deleted after the
  first successful PR and merge-group runs restore `v1-rust-*`.
- How the key is computed belongs to the release, so a `Swatinem/rust-cache` release that
  changes it is a change of key like any other: `cache-deps.yaml` first, `pr.yaml` once
  `main` has written the new entries. Dependabot sends rust-cache bumps as a pull request
  of their own for exactly this reason (`.github/dependabot.yaml`). Whoever reviews one
  reads the release notes of every release it covers for anything about the key,
  hashing or cached paths, and if there is any, splits the bump into two pull requests:
  the first moves `cache-deps.yaml` (and `durable-e2e.yaml`, which reads only the entry
  it writes itself), the second moves `pr.yaml` after the warm on `main` has succeeded.
  Comparing the "Cache Key" a warm prints before and after the bump shows it too.

Warm timings measured on `main`: the `--all-features` clippy pass compiles in about 20 s, all workspace test binaries build in 1 m 48 s, checkout with submodules takes about 80 s and the restore about 20 s.
After a heavy dependency bump, with only a partial cache to fall back on, clippy took 11.5 min and the test build 9.5 min.
The lane ceilings in `pr.yaml` (`timeout-minutes: 45`) are set from the second set of numbers, not the first; a lane anywhere near 45 minutes means the cache is broken.
Check the total under Settings -> Actions -> Caches now and then: three entries should be there (`clippy-cache`, `test-cache`, `durable-e2e-cache`), well inside the 10 GB quota.

One exposure to know about: `rust-toolchain.toml` pins `channel = "1.94"`, so a 1.94.x point release changes the rustc version, which is in the key, and every entry misses with no fallback until the next warm (the schedule within 3-4 days, or a manual dispatch).
Pinning `1.94.x` would make the rotation explicit and deliberate.
That is a decision to make, not one made here.

A second exposure, new with v2.9.2: the stable Rust preinstalled on the `ubuntu-latest` image is one of the toolchains hashed into every key, and this repository does not control it.
When GitHub updates the image to a new Rust release (roughly every six weeks, plus point releases, rolled out to the runners over several days), every entry misses with no fallback until the next warm, and while the rollout is in progress a writer and a reader can land on different images and compute different keys.
A missed restore whose "Rust Versions:" list shows a new stable is the sign.
There are two ways out.
One is to dispatch a warm by hand when it happens.
The other is to make the installed set deterministic by removing the image's own toolchain before the cache step, in both workflows and in lockstep; that is untested on a runner.
That too is a decision to make, not one made here.

### STAGE 2: the rust-cache v2.9.2 bump is half done

The move from v2.7.7 to v2.9.2 changes the key: v2.9.2 adds the CPU architecture (`Linux-x64-` where v2.7.7 wrote `Linux-`) and hashes every installed toolchain, so neither release can restore what the other wrote.
It moves in two pull requests.
The first moved the writers: `cache-deps.yaml` and `durable-e2e.yaml` are on v2.9.2.
The three cache steps in `pr.yaml` are held on v2.7.7 and restore the entries the last v2.7.7 warm wrote.
Had all six moved together, that pull request's own merge-queue run, and every queue entry after it, would have restored nothing and built every dependency cold until `main` had warmed.

The remaining step moves the three pins in `pr.yaml` to v2.9.2 and removes every passage marked `STAGE 2:` under `.github/`, this subsection included.
It can land once `main` has warmed: the *Warm dependency cache* run that the first pull request's merge triggered has succeeded, and Settings -> Actions -> Caches lists `v1-rust-clippy-cache-Linux-x64-...` and `v1-rust-test-cache-Linux-x64-...`.
Dependabot may open the same bump by itself once `.github/dependabot.yaml` is on `main`, because `pr.yaml` is then the only file behind the latest release.
That pull request can serve as stage 2, but it carries only the pins: hold it until `main` has warmed, and remove the `STAGE 2:` passages with it.
Do not let the gap run long.
Until it closes, the warm no longer refreshes the entries `pr.yaml` reads: they stay alive only while some run restores them at least once in 7 days, and after a `Cargo.lock` change on `main` a lane falls back to the restore-key prefix and gets a partly stale entry.

Until the old entries expire (7 days without a restore), the cache list holds both generations, so the "three entries should be there" check above reads six for a while.
Once stage 2 has landed, the old `v1-rust-clippy-cache-Linux-<hash>`, `v1-rust-test-cache-Linux-<hash>` and `v0-rust-durable-e2e-cache-Linux-<hash>` entries (no `x64` after `Linux-`) can be deleted there.

## Action pins

Every `uses:` in `.github/workflows/` names the action by a full 40-character commit SHA, with the release it stands for in a trailing comment on the same line (`actions/checkout@<sha> # v7.0.1`).
A tag such as `v4` is a pointer the action's owner can move at any time.
A moved tag runs new code in the gate on the next run, with no change in this repository and nothing for a reviewer to see.
A SHA cannot be moved, so the code that runs is the code that was reviewed when the pin was set.
Three positions made this worth doing:

- `taiki-e/install-action` runs in `cache-deps.yaml`'s `warm-test-cache` job, which writes
  the `main`-scope cache entry that every PR and queue run restores.
- `foundry-rs/foundry-toolchain` supplies the `cast` binary whose answer decides
  `verify-on-chain`.
- `actions/deploy-pages` runs with `pages: write` and `id-token: write`.

The current pins (the SHAs are in the workflows, and only there):

| Action | Release | Runtime |
|---|---|---|
| `actions/checkout` | v7.0.1 | node24 |
| `taiki-e/install-action` | v2.87.22 | composite (shell steps only) |
| `foundry-rs/foundry-toolchain` | v1.9.1 | node24 |
| `actions/upload-pages-artifact` | v5.0.0 | composite (runs `actions/upload-artifact` v7.0.0, node24, itself pinned by SHA) |
| `actions/deploy-pages` | v5.0.1 | node24 |

`Swatinem/rust-cache` is left out of the table: its pin moves on its own terms, and "Caches" above covers it.
The runtime column matters because GitHub removed Node 20 from the hosted runners on 2026-09-23 and now forces any node20 action onto Node 24, which it was not written for.

### How a pin moves

Dependabot (`.github/dependabot.yaml`) checks the actions weekly and opens one grouped pull request for all of them except `Swatinem/rust-cache`, with each SHA and its version comment rewritten together.
It proposes a release only once the release is 7 days old, so one that is pulled or found to be compromised in its first days never reaches a pull request.
The grouping is for the attestation: a pull request cannot enter the queue until a maintainer has run the full local suite on its head and attested it, and `taiki-e/install-action` alone releases about five times a week.
`Swatinem/rust-cache` arrives as a pull request of its own, because a release that changes how the cache key is computed has to reach `cache-deps.yaml` before `pr.yaml`.

Whoever reviews such a pull request:

- reads the release notes for every bump in it, all of them between the old release and
  the new one. A changed default is how `upload-pages-artifact` v5 came to drop mdBook's
  `.nojekyll` (see the comment in `docs.yaml`).
- treats it as a change to the gate. It is under `.github/`, so it needs a code owner's
  approval, and its head needs `make attest` like any other. Its own workflow runs get a
  read-only token and no secrets; the new code first runs with more than that after it
  lands, in `cache-deps.yaml` (the `main` cache) and `docs.yaml` (the Pages deployment).

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
Every action in the table is on a lightweight tag today; `Swatinem/rust-cache` tags are annotated.

### What a pin does not cover

A pin fixes the action's own code, not what that code downloads when it runs.
`foundry-rs/foundry-toolchain` installs the current `stable` Foundry release on every run (its `version` input defaults to `stable`), so the `cast` behind `verify-on-chain` still changes whenever Foundry releases.
The runner image (`ubuntu-latest`) is not pinned either, and with it everything preinstalled on it.

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
