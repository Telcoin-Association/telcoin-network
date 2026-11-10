# Lessons

## Multi-agent reviews and API session limits (2026-10-20, sls_btree_idx review)
- Correction from the user: "DO NOT RESTART THIS Session again!" When agents stop on an API limit, resume each one with SendMessage from its checkpoint file. Never start a fresh agent or redo finished tracks.
- Ten parallel agents exhausted a session window in about 20 minutes. Each resume reloads the agent's whole context, so after a limit resume in waves of about 4-5, closest-to-done first, and start the next agent as each one finishes.
- Brief every resumed agent with what already ran (baseline results), so it doesn't re-run full suites, and with any HEAD movement (review only `git diff <old> <new>` hunks).
- Have every agent checkpoint its output file after each major step. That is what made resuming cheap.
- A sub-agent's hand-back can reach the coordinator when its parent agent died. Save it to a file and point the resumed parent at it.
- Cheap mechanical work (an e2e run) is better run directly than by resuming an agent whose context must be reloaded.
- Check package names and the base ref before launching: `state-sync` (not `tn-state-sync`); local `main` can be stale, so use `origin/main`. `timeout` does not exist on this macOS shell.

## Amending a commit with other work staged (2026-10-30, reth vendoring)
- Self-caught: `git rm` stages deletions immediately. A later `git add <file> && git commit --amend`
  folded those staged deletions into the "verbatim upstream import" commit. Fixed with
  `git reset --soft <orig>` + `git restore --staged <paths>` + re-amend.
- Rule: before any `--amend` (or commit meant to be narrow), run `git diff --cached --stat` and
  read it. Prefer `git rm --cached`-free flows (plain `rm`) for working-tree changes meant for a
  later commit.
- Verify "verbatim import" claims with tree ids: `git rev-parse <tag>:vendor/reth/<p>` must equal
  `git -C upstream rev-parse <tag>:<p>` (identical content ⇒ identical object id).

## New files silently ignored; commits not gated on checks (2026-10-31, reth backports)
- Self-caught: TN's root `.gitignore` has `debug/`, so a new module at `src/debug/x.rs` was never
  staged. `git add <dir>` reported nothing and the commit's `--stat` lacked the file.
- Rule: after staging a commit that adds files, confirm each new file is in
  `git diff --cached --stat`, and run `git status --short --ignored <dir>` when one is missing.
  Avoid directory names TN ignores (`debug/`, `target/`) for source files.
- Self-caught: a `check && ...; git commit` chain written as `check | grep; commit` committed
  even though the check printed an error (it turned out to be pre-existing).
- Rule: gate commits on the check's exit status (`cmd && git commit`), not on piped output, and
  check vendored code with `tn/cargo.sh check --workspace` (single-crate `-p` builds fail for
  unrelated feature-unification reasons; D18).

## Index-based text cuts must search after their start anchor (2026-10-31, reduction pass 2)
- Self-caught: cutting `s[a:b]` with `b = s.index(end_marker)` matched an EARLIER copy of the end
  marker (a doc line repeated on a field and on a struct), so `b < a` and the edit duplicated a
  block instead of removing one. rustc caught it as a stray brace.
- Rule: always `s.index(end_marker, a)` and assert `a < b`; prefer exact-text replacement
  (`count == 1`) over index cuts when the span is short.
- Related, caught by `graph.py diff`: removing a crate can drop a third-party feature that only it
  enabled (alloy-primitives `rand` via reth-network-types). Check every feature change before
  committing; do not rely on the build failing.

## zsh expands a leading `=` in unquoted words (2026-10-31, js-tracer)
- Self-caught: `echo ===on` between two cargo runs in one command failed with "==on not found"
  (zsh `=cmd` expansion), and the `;`-chained second run never started. The first half's PASS
  lines looked like success until the exit code was read.
- Rule: quote every echo label (`echo '== on'`), and when a command chains independent runs,
  give each its own `rc=$?` line so a missing half is visible.

## Feature-graph dumps and nested worktrees (2026-11-04, fork-safety checks)
- Self-caught: `cargo tree --no-dedupe -e normal -f '{p}|{f}'` on TN's graph ran 30+ minutes
  with no output. `{f}` prints a package's resolved features on every occurrence, deduped or not,
  so drop `--no-dedupe` and `sort -u` the lines (about a minute per target).
- Self-caught: `cargo test` inside a crate the root excludes (`patches/<crate>`) failed in a
  worktree nested under `.claude/worktrees/`: cargo's workspace search continues past an
  excluding workspace and reached the outer checkout's `Cargo.toml`. Run such crates' tests from
  a copy outside any workspace (CI's checkout is not nested, so `--manifest-path` works there).
- When an agent's report says "waiting on its own background work", check for leftover node
  processes (`pgrep -f 'e2e/telcoin-network'`) before starting timing-sensitive e2e runs.
- Repeated 2026-11-04: an unquoted `echo ======` separator in a zsh command failed again
  ("===== not found"). Use `echo '--'` or a quoted label, never a bare run of `=`.
- Run new CI-lane steps under the lane's own environment before committing them:
  `etc/ci-lanes.sh` exports `CARGO_TERM_COLOR=always`, which broke a tool parsing `cargo tree`
  output that passed when run on its own. Tools that parse cargo output pass `--color never`.

## A new CI lane must run under the workflow's env, not just the script's (2026-11-05)
- Caught by the final review, not by me: pr.yaml's `vendor-reth-test` job sets
  `NEXTEST_PROFILE: ci`, and nextest reads `.config/nextest.toml` from the workspace it runs in.
  The nested `vendor/reth` workspace had none, so every nested run would have stopped at
  "profile `ci` not found" in the merge queue. `make attest` and my local runs left the profile
  unset, and a maintainer-authored PR skips the lane on `pull_request`, so the first real run would
  have been in the queue.
- Rule: before committing a CI job, run its command with the job's `env:` block exported as
  written (`NEXTEST_PROFILE=ci`, `RUSTFLAGS`, ...), and check every config file the tool reads
  per workspace (nextest, rustfmt, clippy) exists for each workspace the job enters.
- Same review: a test that shells out to `cargo` outside the repository escapes
  `rust-toolchain.toml`; CI removes the default toolchain. Run cargo with `cwd=<repo>` and
  `--manifest-path`.

## zsh `=` runs: third occurrence (2026-11-05)
- Again `echo ======` between two `cat`s; the second file was not printed. Two earlier entries
  above say the same. Use `echo '--'` only. Never type a bare run of `=` in a shell command.
- Fourth occurrence the same day (merging main into sls_reth_vendor): `echo =======` between
  two `git diff`s, the second never printed. Written rules have not stopped this; the reliable
  fix is mechanical, a PreToolUse hook that rejects a Bash command containing an unquoted
  `echo =`. Until then: separators are `echo '--'`, typed with the quotes, every time.

## A heredoc edit and its commit on separate lines (2026-11-07, sls_reth_vendor2)
- Self-caught: `python3 - <<'EOF' ... EOF` followed on the NEXT line by `git add ... && git
  commit ...`. The script's assertion failed after it had written one of two files, and the
  commit still ran, so a commit landed without its tracker update (fixed in the next commit).
- Rule: chain the edit script and the commit with `&&` on the same logical command
  (`python3 - <<'EOF' && git add ... && git commit ...`, the heredoc body after the line), or run
  the edit in one tool call and commit in the next after reading its output.
- Also seen: a fresh git worktree means a fresh target dir, so every build script and test
  binary needs its first-launch scan again. With the endpoint scanner slow, that doubled the
  queue and new executables stalled for over an hour. Reuse one target dir; don't start a second
  full build while a long nextest listing is in progress.

## A background test run that never ends is a hang, not a slow build (2026-11-09, sls_reth_vendor2)
- User-caught: a nextest run sat for over eight hours while I waited for its notification. The
  build had finished in minutes; one test (upstream's `trace_filter_defaults_to_latest`) was
  deadlocked at 0% CPU. The nested `.config/nextest.toml` has no slow-timeout, so nothing killed
  it, and `| grep | head` buffered the output so the file stayed empty.
- Rule: when a background run passes twice its usual time, check `ps -o etime,%cpu` on the
  cargo/test processes and `find target -newer <output file>`; zero CPU with no rustc means a
  hang. Kill it and look at the test, don't keep waiting. Keep raw logs (`> file 2>&1`), not a
  grep pipe, so interim progress is visible. macOS has no `timeout`; use `gtimeout` if present.
- The cause was a TN-specific test mismatch worth checking on every trace/debug backport:
  TN-0038 moves permit acquisition into the API methods, so an upstream test that builds a
  `TraceApi` with `BlockingTaskGuard::new(1)` and calls `trace_filter` (two permits) directly
  waits forever. Upstream's handler-level permits made the same test pass there. Size the guard
  for the method's permits (two for `trace_filter`) when porting such tests.

## cargo does not forward `--config` to external subcommands (2026-11-09, sls_reth_vendor2)

`cargo --config patch.crates-io.x.path=... nextest run` applies the patch to cargo's own
resolution, but cargo-nextest's inner `cargo test` never sees the flag, so the nested reth
test lane built against the registry crate for weeks while `cargo.sh check`, `tree` and
`metadata` showed the patch in place. Tell-tale: "Adding <crate> v<newer>" at the top of a
nextest log for a crate the lockfile pins. Rules: put cargo configuration that must reach every
invocation in a `.cargo/config.toml`, not in `--config` flags; and when a carried patch is meant
to be exercised by a test lane, make a test depend on a symbol only the patch has, so a silent
fallback to the registry fails to compile.

## A vendored behaviour change needs TN's own suites, not only reth's (2026-11-10, sls_reth_vendor2)

TN-0064 changed a `debug` error code; its doc page was updated but `tn-reth`'s namespace test
still pinned the old code and nobody ran it until an unrelated tn-reth run a day later. After
any backport that changes an RPC answer, run `cargo nextest run -p tn-reth` (at least
`-E 'test(rpc)'`) in the same step as the docs update; TN pins vendored behaviour in its own
tests as well as in `docs/src/rpc-methods/`.
