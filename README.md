# with-probe

Run a command under systemd with HTTP startup and liveness probes. The wrapper
starts the command, checks its health endpoint, and reports readiness and
liveness through systemd's notification socket.

```sh
with-probe --url http://localhost:8080/healthz -- some-other-command --serve
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--url HTTP_URL` | Required | Shared endpoint. It must respond with HTTP/1.0 `200 OK`. |
| `--timeout SECS` | 1 | Request timeout for both probe types. |
| `--startup-period SECS` | 2 | Interval between startup probe starts. |
| `--startup-failure-threshold N` | 20 | Consecutive startup failures required to exit. |
| `--liveness-period SECS` | 5 | Interval between liveness probe starts. |
| `--liveness-failure-threshold N` | 3 | Consecutive liveness failures required to exit. |

Periods, timeouts, and thresholds are positive whole numbers. Everything after
`--` is the child command and its arguments.

In your systemd service, use the invocation as `ExecStart`, set `Type=notify`,
and set `WatchdogSec` to enable the watchdog. The first startup probe starts
immediately. Its first success triggers `READY=1` and switches to the liveness
settings. Watchdog notifications continue at half the watchdog interval while
the liveness failure count stays below its threshold.

A successful probe clears the accumulated failures. When a threshold is
reached, the wrapper reports each failure in the streak, kills the child, and
exits with status 1. Notification failures and child exits also fail the
wrapper.
