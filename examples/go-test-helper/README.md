# Go test helper

[`emulator_test.go`](emulator_test.go) is an integration test that starts the
emulator as a child process on a free port, waits for it, uses it with the
official SDK, and stops it when the test ends. Copy `startEmulator` into your
own test helpers.

```bash
OSB_BIN=/path/to/open-service-bus go test ./examples/go-test-helper
```

Without the binary (`OSB_BIN` unset and nothing on the `PATH`) the test is
skipped. Pass extra flags to `startEmulator` to declare entities
(`-queue`, `-config`, `-terraform`, ...). Each test gets its own in-memory
emulator, so tests do not share state and can run in parallel.
