# Lessons

Patterns to avoid repeating. Review at session start.

## Per-commit work: run `cargo +nightly fmt` BEFORE each commit
When making one-commit-per-change, format before committing. This workspace's
rustfmt reflows doc comments to a wide width (`wrap_comments`), so hand-wrapped
comments (~72 cols) get reflowed. If several commits share a file, you can't
split the formatting back per-commit without interactive rebase (unavailable
here), forcing a single trailing `style: rustfmt` commit. Avoid that by
formatting the touched files before staging each finding.

## Verify against the actual code, not a stale `tasks/todo.md`
A prior `tasks/todo.md` claimed findings were fixed (e.g. `unreachable!` → graceful
reorg) that were NOT present in the current tree. Always Read the real files and
confirm file:line before trusting any tracker/summary from a previous session.

## Confirm intra-doc links build under `-D warnings`
`cargo doc` only warns on broken `[Foo]` links by default; CI runs
`RUSTDOCFLAGS=-D warnings`. After any rustdoc edit, run
`RUSTDOCFLAGS="-D warnings" cargo doc -p <crate> --no-deps`. (Found a pre-existing
unpathed `[Future]` link this way; fix is `[Future](std::future::Future)`.)

## `cargo` exit codes: zsh uses `$pipestatus`, not `${PIPESTATUS[...]}`
`echo ${PIPESTATUS[0]}` prints blank in zsh after a pipe. To gate on a real exit
code, run the command without a trailing pipe (`cmd >/dev/null 2>&1; echo $?`) or
read `$pipestatus[1]`. Don't infer pass/fail from tail output alone.
