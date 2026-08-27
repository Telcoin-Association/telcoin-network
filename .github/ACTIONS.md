# How TN CI Works

GitHub Actions takes over 45 minutes to build and test the workspace, and it cannot run the
e2e suites at all within a reasonable budget. So the full suite runs on a maintainer's
machine, and CI checks that the resulting commit hash was attested on-chain.

`etc/test-and-attest.sh` (`make attest`) runs fmt, clippy, both test lanes and the e2e
suites locally, then writes the HEAD commit hash to the git attestation registry on adiri
(`0xf102928273a399cda6151b8616209af019499c84`). The `verify-on-chain` lane in
`.github/workflows/pr.yaml` reads that registry back.

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
  CI starts, so no local run could have covered it. `verify_attestation.sh` instead walks
  the group's first-parent chain and verifies the PR *heads* folded into it.

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
2. Set the required status check to **`CI Success`**, and nothing else. In particular
   remove `verify-on-chain` if it is listed there: it is a job inside `pr.yaml` now, not a
   separate workflow, and `CI Success` already depends on it.
3. Keep the queue's **maximum PRs to build** small (1-2) to start. Larger batches mean a
   single bad PR forces a rebuild of everything behind it, and this workspace is expensive
   to compile.
4. Set the queue's check timeout above the slowest lane. The compile-and-test lanes are
   capped at 90 minutes each.

The old `maintainer-verify.yaml` workflow was folded into `pr.yaml` and deleted. It carried
`environment: merge-into-main`; the replacement lane deliberately does not. That job reads a
public RPC and uses no secrets, and if the environment ever gained a protection rule it
would park the merge queue on a manual approval until the queue timed out.

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
