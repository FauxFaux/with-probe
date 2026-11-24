# with-probe

Run a command under systemd with a Kubernetes-style HTTP liveness probe. The
wrapper starts the command, checks its health endpoint, and reports readiness
and liveness through systemd's notification socket.

```sh
with-probe --url http://localhost:8080/healthz --period 5 -- some-other-command --serve
```

- `--url` is the endpoint to probe. It must return an HTTP/1.0 `200 OK`.
- `--period` sets the interval between probe starts in whole seconds. The first
  probe starts immediately.
- `--timeout` optionally sets the HTTP request timeout in whole seconds. It
  defaults to the probe period.
- Everything after `--` is the child command and its arguments.

In your systemd service, use the invocation as `ExecStart`, set `Type=notify`,
and set `WatchdogSec` to enable the watchdog. The first successful probe triggers
`READY=1`. The wrapper then sends watchdog notifications at half the watchdog
interval while the latest successful probe is less than one period plus one
second old.

A failed probe leaves the last successful probe time unchanged. The startup
deadline is the longer of the request timeout and one period plus one second.
If no probe succeeds by that deadline, the last success becomes stale, or a
notification fails, the wrapper kills the child and exits with status 1. Any
child exit also fails the wrapper.
