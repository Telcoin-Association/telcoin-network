# E2E Tests

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
