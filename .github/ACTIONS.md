# How TN CI Works

GitHub Actions takes over 45 minutes to build and test the workspace, and it cannot run the
e2e suites at all within a reasonable budget. So the full suite runs on a maintainer's
machine, and CI checks that the resulting commit hash was attested on-chain.

`etc/test-and-attest.sh` (`make attest`) runs fmt, clippy, both test lanes and the e2e
suites locally, then writes the HEAD commit hash to the git attestation registry on adiri
(`0xf102928273a399cda6151b8616209af019499c84`). The `verify-on-chain` lane in
`.github/workflows/pr.yaml` reads that registry back.

The lanes themselves live in **`etc/ci-lanes.sh`**, and both `test-and-attest.sh` and
`pr.yaml` call it. That is the only reason "the queue runs what was attested" is a fact
rather than a hope: when the two spelled out their own commands, they had already drifted
(CI ran clippy under `--all-features` only, and excluded a package deleted long ago). Edit a
lane there and both callers change together. The e2e suites are deliberately not in that
file -- anything added to it lands in the merge queue.

## What the merge queue changes

`main` is merged through a GitHub merge queue. The queue exists because attestation cannot
answer the question that keeps breaking main: an attestation covers a PR's head commit in
isolation, and says nothing about `main + PR`. Two PRs that each pass alone can still break
each other, and no amount of attesting either one catches it.

So the two gates cover different things and neither replaces the other:

| | attestation (`verify-on-chain`) | merge queue (`fmt`, `clippy`, `test`, `adiri-test`, the guards) |
|---|---|---|
| commit tested | the PR head | main + this PR + every PR ahead of it in the batch |
| suite | everything, e2e included | everything CI can afford; no e2e |
| runs on | a maintainer's machine | GitHub runners |

The queue builds a temporary `gh-readonly-queue/main/...` branch and fires a `merge_group`
event against it. Three consequences worth remembering:

- **Every required check must report on `merge_group`.** A required check that only
  triggers on `pull_request` does not fail the queue, it hangs it until the queue timeout
  ejects the PR. `pr.yaml` triggers on both events, and `CI Success` is the only required
  check, on purpose: a second required name means a second workflow that has to learn the
  same lesson.
- **The maintainer and draft skips do not apply in the queue.** Whoever wrote the PR, the
  merged commit has never been compiled anywhere, so every lane runs. `CI Success` treats a
  skipped lane as a failure on `merge_group` for the same reason.
- **The merge group's commit hash cannot be attested.** GitHub creates it seconds before
  CI starts, so no local run could have covered it. `verify_attestation.sh` instead
  verifies the PR *heads* folded into the group, and how it finds them depends on the
  queue's merge method. Under `MERGE` each queue commit is a merge whose non-first parents
  are the heads. Under `SQUASH`, which is what main uses, each queue commit is a
  single-parent squash and the head shas are not in the queue branch's history at all --
  the only signal left is the `(#N)` GitHub appends to the squash subject, which the script
  parses and resolves through the API. Both paths are implemented; changing the queue's
  merge method will not quietly turn this check into a no-op.

### Where the coverage gap is

The queue does not run e2e. That coverage comes only from the attested local run, against
whatever the branch was based on at the time. This is why `test-and-attest.sh` refuses to
attest a branch that is behind `origin/main`: merge or rebase first so the e2e lanes test
the combination that actually lands. `ALLOW_STALE_BASE=1` overrides it when the drift is
provably irrelevant. The nightly `durable-e2e` lane is the backstop for what still slips
through.

### Repository settings this requires

The workflow changes are not enough on their own. In the `main` ruleset:

1. Enable **Merge queue**.
2. Tick **Allow auto-merge**, under Settings -> General -> Pull Requests. This one is not
   in the ruleset and does not look related, so it is the one that gets missed: the "Merge
   when ready" button calls the `enablePullRequestAutoMerge` GraphQL mutation even on a
   branch that has a queue, and that mutation is gated on this checkbox. With it unticked
   every attempt to queue a PR fails with *"failed enabling auto-merge for pull request"*,
   however green the PR is and however many approvals it has.
3. Add **`CI Success`** as the required status check, and nothing else. In particular do not
   list `verify-on-chain`: it is a job inside `pr.yaml` now, not a separate workflow, and
   `CI Success` already depends on it. Without at least one required check the queue has
   nothing to wait for and merges each entry as soon as it is built, which makes every
   `merge_group` lane in this workflow decorative.
4. Keep the queue's **maximum PRs to build** small (1-2) to start. Larger batches mean a
   single bad PR forces a rebuild of everything behind it, and this workspace is expensive
   to compile.
5. Set the queue's check timeout above the slowest lane, which is `clippy` at 150 minutes
   (two passes, default features and `--all-features`). A timeout below the lane budget
   ejects PRs for taking exactly as long as they were designed to take.

### Who can put a PR in the queue

GitHub's own answer is only "anyone with write access", and there is no finer-grained
setting. The real gate here is the attestation: `CI Success` depends on `verify-on-chain`,
required checks must pass *before* a PR can be queued, and `verify-on-chain` passes only for
a commit hash already written to the registry by a holder of the MAINTAINER key. So a
contributor cannot make their own PR queue-eligible -- a maintainer has to run
`test-and-attest.sh` against that exact commit first, which is a stronger claim than a
CODEOWNERS entry makes, and it is why `require_code_owner_review` is not needed in the
ruleset (there is no CODEOWNERS file for it to consult anyway).

The old `maintainer-verify.yaml` workflow was folded into `pr.yaml` and deleted, along with
the `merge-into-main` deployment environment it ran in. Nothing in CI references that
environment now, and the replacement lane deliberately has no `environment:` of its own: it
reads a public RPC and uses no secrets, and an environment that ever gained a protection
rule would park the merge queue on a manual approval until the queue timed out. Delete the
environment itself in Settings -> Environments, and drop the `required_deployments` rule
from the `main` ruleset -- it currently lists no environments, so it enforces nothing while
leaving a live switch that would hang the queue if anyone filled it in.

## Environment
Attesting devs must have "MAINTAINER" role to update contract state.

The local `test-and-attest.sh` script requires Foundry's cast.

See https://book.getfoundry.sh/getting-started/installation for installation instructions.

Add `GITHUB_ATTESTATION_PRIVATE_KEY` to a `.env` file in the project. This is the private key (without "0x" prefix) associated with the "MAINTAINER" role address.

## Toolchain pins

Two channels are pinned in the repo root:

- **`rust-toolchain.toml`** — stable channel (currently `1.94`). Used for all compile/test commands. rustup auto-honors it inside the repo, so bare `cargo build`, `cargo test`, `cargo check`, and `cargo nextest` use stable 1.94.
- **`rust-nightly`** — single-line file containing the nightly date (currently `nightly-2026-03-20`). Used only for `cargo fmt` and `cargo clippy`, which require nightly-only rustfmt/clippy options (`imports_granularity`, `wrap_comments`, etc.). The Makefile, `etc/test-and-attest.sh`, and CI workflows read this file and invoke `cargo +<date>` explicitly.

To bump nightly: edit `rust-nightly`. To bump stable: edit `rust-toolchain.toml` (and align with `Cargo.toml`'s `rust-version` and `etc/Dockerfile`'s base image tag).
