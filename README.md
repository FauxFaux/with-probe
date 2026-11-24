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

A failed probe, stale probe result, or notification error causes the wrapper to
kill the child and exit with status 1. Any child exit also fails the wrapper.
