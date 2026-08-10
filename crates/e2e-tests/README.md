# E2E Tests

## Running the ignored e2e suite

The heavy e2e tests (restart and epoch tests) are `#[ignore]`d and each needs the
`telcoin-network` node binary. When `TN_BIN_PATH` is unset, the first test builds that binary
in-process via `escargot`, which under the `e2e` profile (`opt-level = 2`) is a multi-minute
compile. Under `nextest`'s
default output capture that build is buffered, so the first test looks frozen for several minutes
before it does anything.

Point the suite at a prebuilt binary to avoid the in-test build:

```
make test-e2e          # builds the binary once, then runs the suite with TN_BIN_PATH set
```

or run the raw commands yourself from the workspace root (for example from an IDE test runner):

```
cargo build --profile e2e --bin telcoin-network --features tn-storage/test-utils --target-dir "${CARGO_TARGET_DIR:-$(pwd)/target}"
TN_BIN_PATH="$(cd "${CARGO_TARGET_DIR:-$(pwd)/target}" >/dev/null && pwd)/e2e/telcoin-network" \
  cargo nextest run -p e2e-tests --run-ignored ignored-only --all-features
```

`TN_BIN_PATH` must be an absolute path: `nextest` runs each test with its working directory set
to the package (`crates/e2e-tests`), not the workspace root, so a relative path would not resolve.
The `$(cd ... && pwd)` above keeps it absolute even when `CARGO_TARGET_DIR` is a relative path,
and the explicit `--target-dir` keeps the build and `TN_BIN_PATH` on one root.
The workspace root matters for the same reason:  with `CARGO_TARGET_DIR` unset, `$(pwd)/target`
follows the working directory, so a run from `crates/e2e-tests` would build into, and read from,
a stray package-local `target` tree.
If `TN_BIN_PATH` is left unset the suite still runs; add `--no-capture` to watch the one-time build
instead of waiting on a silent first test.

## Test Log Output

The e2e integration tests (restart and epoch tests) spawn multiple validator node processes. Each node's stdout is captured to a separate log file under `test_logs/` so that failures can be debugged without sifting through interleaved output from all nodes.

### Log location

```
test_logs/<test_name>/node<instance>-run<run>.log
```

For example:
- `test_logs/restarts/node1-run1.log`
- `test_logs/epoch_boundary/node3-run1.log`

### Why

When multiple nodes run concurrently, their log output is interleaved and difficult to follow. Splitting logs by node and run makes it straightforward to trace a single node's behavior leading up to a failure.

### Notes

- The `test_logs/` directory is gitignored.
- Log files are overwritten each time a test runs (`File::create` truncates existing files).
