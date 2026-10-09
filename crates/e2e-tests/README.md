# E2E Tests

## Captured SIGKILL recovery

`make test-sigkill` runs two abrupt restarts under transaction traffic: one shortly before
epoch close and one halfway through the epoch. Both keep the validator down across the
boundary and require bounded catch-up, active CVV mode, a matching execution block hash,
and a transaction executed through the restarted validator. The kill primitive verifies
SIGKILL (signal 9) directly.

The defaults are 30-second epochs, a 2-second late-kill offset, at least 45 seconds down,
and a 180-second recovery deadline. To use fleet B's epoch duration and kill offset:

```sh
TN_TEST_MDBX_SYNC=safe-no-sync \
TN_SIGKILL_EPOCH_SECS=1200 \
TN_SIGKILL_BEFORE_CLOSE_SECS=59 \
TN_SIGKILL_DOWNTIME_SECS=300 \
TN_SIGKILL_RECOVERY_SECS=1800 \
make test-sigkill
```

The 300-second downtime includes the 59 seconds before close and approximately four
minutes after close. Restart waits for both that elapsed time and a peer crossing the
boundary. These tests use four local validators and modest transaction traffic. A passing
run does not establish the cause of the larger fleet's historical freeze in
[issue #1510](https://github.com/Telcoin-Association/telcoin-network/issues/1510).

The `SIGKILL recovery` workflow runs both scenarios in Durable and SafeNoSync by default.
Its manual inputs select timing and either sync regime. Its negative control omits the
restart and must reach the recovery timeout. The nightly Durable lane also runs both cases.
Download the run's `sigkill-recovery-*` artifact before its 15-day retention expires.

Each case's `test_logs/sigkill_*` directory contains separate node stdout/stderr for each
start, `kill.json`, `boundary.json`, `restart.json`, `samples.csv`, metrics snapshots, and
file inventories. The stdout filter enables `state-sync`, `consensus-chain`, and
`tn::observer` debug messages. On a returned error, `retained-datadir.txt` names the local
data directory. Failed CI runs archive that directory under `validation-logs/failed-data`.

For a fleet run, save the victim's Docker log and service journal before the kill and before
teardown. Record the boundary block's timestamp and the actual remote kill/restart times.
List `consensus-db/epochs` sizes and modification times every minute during the outage and
recovery. Preserve these files even when the RPC remains alive and the node keeps one PID.
The benchmark scripts and the collector live in `devnet-genesis` and
`tn-transaction-generator`; their workload schedule and fleet telemetry remain necessary
for reproducing the original load.

An operator with access to the original Prometheus endpoint can preserve the historical
window with the following read-only query. Adjust the node/network labels to the fleet's
actual labels. An empty result is missing evidence, not proof of recovery.

```sh
: "${PROMETHEUS_URL:?Set the accessible Prometheus endpoint}"
mkdir -p sigkill-prometheus
for metric in tn_primary_round tn_primary_committed_round tn_epoch_current; do
  curl --fail --silent --show-error --get "$PROMETHEUS_URL/api/v1/query_range" \
    --data-urlencode "query=$metric{network=\"bench\"}" \
    --data-urlencode 'start=2026-09-23T06:10:00Z' \
    --data-urlencode 'end=2026-09-23T07:05:00Z' \
    --data-urlencode 'step=15s' \
    --output "sigkill-prometheus/$metric.json"
done
```

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
