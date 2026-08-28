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
In order of urgency:

1. **Add `CI Success` as a required status check. It is missing today.** The `main` ruleset
   has no `required_status_checks` rule at all, so the queue gates nothing yet: a PR can be
   queued unattested with no green lane, and the queue merges each entry as soon as GitHub
   has built it. Add the check with *Require branches to be up to date before merging*
   **off**: strict mode would force a re-push, and so a re-attestation, every time `main`
   moved, and the queue already tests `main + PR`. Require `CI Success` only, not
   `verify-on-chain`, which is a job inside `pr.yaml` that `CI Success` already depends on.
   GitHub matches a required check by name, so a job called `CI Success` in any workflow
   would satisfy it; keep the name unique to `pr.yaml`.
2. **Merge queue.** Merge method `SQUASH` (already set). Start with a build concurrency of
   **2**: each group runs seven jobs, so five groups is about 35 concurrent jobs queueing
   behind the organization's runner concurrency. Raise the status check timeout from 60 to
   **90 minutes**: it counts from the group's creation, runner backlog included, so 60 can
   eject a lane that took 45 after queueing for 15.
3. **Pull request rule.** *Dismiss stale pull request approvals when new commits are pushed*
   (already set) is, together with (1), what makes a late push cost a re-approval.
   Recommended: *Require approval of the most recent reviewable push*, so the author of that
   push cannot approve it themselves. `require_code_owner_review` is a latent switch with no
   CODEOWNERS file to consult; leave it off.
4. **Repository settings** (Settings -> General -> Pull Requests). *Allow auto-merge* is not
   in the ruleset and does not look related, so it is the one that gets missed: the "Merge
   when ready" button calls the `enablePullRequestAutoMerge` GraphQL mutation even on a
   branch that has a queue, and that mutation is gated on this checkbox. With it unticked
   every attempt to queue a PR fails with *"failed enabling auto-merge for pull request"*,
   however green the PR is. Also *Allow squash merging*, with the default squash message
   set to the PR title and description.
5. **Delete the `merge-into-main` environment** (Settings -> Environments) and drop the
   empty `required_deployments` rule from the `main` ruleset. The old
   `maintainer-verify.yaml` workflow ran in that environment; it was folded into `pr.yaml`
   and deleted, and the replacement lane deliberately has no `environment:` of its own: it
   reads a public RPC and uses no secrets, and an environment that ever gained a protection
   rule would park the merge queue on a manual approval until the queue timed out. The
   `required_deployments` rule lists no environments, so it enforces nothing while leaving
   a live switch that would hang the queue if anyone filled it in.
6. **Escape hatch.** If the `merge_group` path of `CI Success` is ever broken, no PR can
   land to fix it, because the fix itself has to pass through the queue. An admin has to
   remove the required check temporarily (or use a bypass) to land the fix, then put it
   back.

### Who can put a PR in the queue

GitHub's own answer is only "anyone with write access", and there is no finer-grained setting.
The real gate here is the attestation plus the approval: `CI Success` depends on `verify-on-chain`, required checks must pass *before* a PR can be queued, and `verify-on-chain` passes only for a commit hash already written to the registry by a holder of the MAINTAINER key.
So a contributor cannot make their own PR queue-eligible -- a maintainer has to run `test-and-attest.sh` against that exact commit first, which is a stronger claim than a CODEOWNERS entry makes.
This is documented, not enforced by GitHub: nothing stops a maintainer from queueing an attested PR without a review, other than the approval rule.

## Caches

`main` is the only cache writer.
`.github/workflows/cache-deps.yaml` runs there and saves two entries: `clippy-cache` (both clippy passes under the nightly pin) and `test-cache` (the test binaries of both test lanes, default and adiri features, under the stable pin).
The lanes in `pr.yaml` restore those and never save (`save-if: "false"`): a cache saved by a `pull_request` run is scoped to that PR's branch and one saved by a `merge_group` run lands on the queue's throwaway branch, so nothing else could ever read them, while the upload adds minutes to the critical path and eats quota that evicts the entries the queue does read.

A warm runs on a push to `main` that touches a `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `rust-nightly`, `.cargo/config.toml`, `etc/ci-lanes.sh` or the workflow itself; on a schedule twice a week (GitHub deletes an entry not accessed for seven days, and a quiet week would otherwise leave the queue cold); and by hand from the Actions tab (*Warm dependency cache* -> *Run workflow*).
When the entry already matches, the run restores it, rebuilds only the workspace crates, saves nothing, and is done in a few minutes.

Two properties of `Swatinem/rust-cache` shape all of this:

- The key is `<prefix-key>-<shared-key>-<os>-<hash of rustc and every CARGO*/RUST*
  variable in the environment>-<hash of the lockfiles>`. So the `env:` block and the steps
  before the cache step must be identical in `cache-deps.yaml` and `pr.yaml`; they are,
  and both files say so. The clippy jobs hash seven variables (the six in `env:` plus
  `RUST_NIGHTLY`, written to `GITHUB_ENV` before the cache step); the test jobs hash six.
  Each cache step prints what it computed in its "Cache Configuration" log group (Restore
  Key, Cache Key, Environment considered). When a restore misses, compare that group
  between the two workflows first.
- Entries are immutable, and an exact key hit skips the save. So changing *what* a warm job
  builds (a lane added, a feature set changed) writes nothing until the key changes: bump
  `prefix-key` in both workflows, `cache-deps.yaml` first, then `pr.yaml` once `main` has
  written the new entries. (Or delete the entries under Settings -> Actions -> Caches and
  re-run the warm.) That is why `pr.yaml` still restores `v0-rust-*` entries while
  `cache-deps.yaml` already writes `v1-rust-*`: a follow-up flips `pr.yaml` to
  `prefix-key: v1-rust` once the first warm has landed on `main`, after which the
  `v0-rust-*` entries can be deleted.

Warm timings measured on `main`: the `--all-features` clippy pass compiles in about 20 s, all workspace test binaries build in 1 m 48 s, checkout with submodules takes about 80 s and the restore about 20 s.
After a heavy dependency bump, with only a partial cache to fall back on, clippy took 11.5 min and the test build 9.5 min.
The lane ceilings in `pr.yaml` (`timeout-minutes: 45`) are set from the second set of numbers, not the first; a lane anywhere near 45 minutes means the cache is broken.
Check the total under Settings -> Actions -> Caches now and then: three entries should be there (`clippy-cache`, `test-cache`, `durable-e2e-cache`), well inside the 10 GB quota.

One exposure to know about: `rust-toolchain.toml` pins `channel = "1.94"`, so a 1.94.x point release changes the rustc version, which is in the key, and every entry misses with no fallback until the next warm (the schedule within 3-4 days, or a manual dispatch).
Pinning `1.94.x` would make the rotation explicit and deliberate.
That is a decision to make, not one made here.

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
