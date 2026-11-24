use std::io;
use std::process::{Child, Command, ExitCode};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use sd_notify::NotifyState;
use ureq::http::{StatusCode, Version};

const CHECK_CHILD_INTERVAL: Duration = Duration::from_millis(50);

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

struct ProbeSettings {
    period: Duration,
    failure_threshold: usize,
}

fn probe(agent: &ureq::Agent, url: &str) -> Result<(), String> {
    let response = agent
        .get(url)
        .version(Version::HTTP_10)
        .header("Connection", "close")
        .call()
        .map_err(|error| error.to_string())?;
    if response.version() == Version::HTTP_10 && response.status() == StatusCode::OK {
        return Ok(());
    }
    Err(format!(
        "expected HTTP/1.0 200 OK, received {:?} {}",
        response.version(),
        response.status()
    ))
}

fn check_child(child: &mut Child) -> io::Result<()> {
    match child.try_wait()? {
        Some(status) => Err(io::Error::other(format!("child exited with {status}"))),
        None => Ok(()),
    }
}

fn supervise(
    child: &mut Child,
    url: String,
    startup: ProbeSettings,
    liveness: ProbeSettings,
    timeout: Duration,
) -> io::Result<()> {
    let watchdog_interval = sd_notify::watchdog_enabled()
        .filter(|timeout| !timeout.is_zero())
        .map_or(liveness.period, |timeout| timeout / 2);
    let (ready, readiness) = mpsc::channel();
    let worker = thread::Builder::new().spawn(move || -> io::Result<()> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .proxy(None)
            .max_idle_connections(0)
            .max_redirects(0)
            .http_status_as_error(false)
            .timeout_global(Some(timeout))
            .build()
            .into();
        let mut settings = &startup;
        let mut starting = true;
        let mut failures = Vec::new();
        loop {
            let started = Instant::now();
            match probe(&agent, &url) {
                Ok(()) => {
                    failures.clear();
                    if starting {
                        sd_notify::notify(&[NotifyState::Ready, NotifyState::Watchdog])?;
                        ready.send(Instant::now()).map_err(io::Error::other)?;
                        starting = false;
                        settings = &liveness;
                    }
                }
                Err(error) => {
                    failures.push(error);
                    if failures.len() == settings.failure_threshold {
                        let phase = if starting { "startup" } else { "liveness" };
                        return Err(io::Error::other(format!(
                            "{phase} probe failed {} times in a row:\n{}",
                            failures.len(),
                            failures.join("\n")
                        )));
                    }
                }
            }
            thread::sleep(settings.period.saturating_sub(started.elapsed()));
        }
    })?;

    let mut next_watchdog: Option<Instant> = None;
    loop {
        check_child(child)?;

        let mut wait = CHECK_CHILD_INTERVAL;
        if let Some(deadline) = next_watchdog {
            let now = Instant::now();
            if now >= deadline {
                sd_notify::notify(&[NotifyState::Watchdog])?;
                next_watchdog = Some(now + watchdog_interval);
                wait = wait.min(watchdog_interval);
            } else {
                wait = wait.min(deadline - now);
            }
        }
        match readiness.recv_timeout(wait) {
            Ok(ready_at) => next_watchdog = Some(ready_at + watchdog_interval),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                return worker
                    .join()
                    .map_err(|_| io::Error::other("HTTP probe thread panicked"))?;
            }
        }
    }
}

fn seconds(value: Option<std::ffi::OsString>) -> io::Result<Duration> {
    value
        .and_then(|arg| arg.to_str()?.parse::<f64>().ok())
        .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
        .filter(|duration| !duration.is_zero() && Instant::now().checked_add(*duration).is_some())
        .ok_or_else(|| {
            invalid("period and timeout must be positive seconds within the clock range")
        })
}

fn threshold(value: Option<std::ffi::OsString>) -> io::Result<usize> {
    value
        .and_then(|arg| arg.to_str()?.parse().ok())
        .filter(|&count| count > 0)
        .ok_or_else(|| invalid("failure threshold must be a positive whole number"))
}

fn run() -> io::Result<()> {
    const USAGE: &str = "usage: with-probe --url HTTP_URL [--timeout SECS] [--startup-period SECS] [--startup-failure-threshold N] [--liveness-period SECS] [--liveness-failure-threshold N] -- COMMAND [ARG ...]";
    let mut args = std::env::args_os().skip(1);
    let mut url = None;
    let mut startup_period = None;
    let mut startup_threshold = None;
    let mut liveness_period = None;
    let mut liveness_threshold = None;
    let mut timeout = None;
    loop {
        let flag = args.next().ok_or_else(|| invalid(USAGE))?;
        match flag.to_str() {
            Some("--url") if url.is_none() => {
                url = Some(
                    args.next()
                        .and_then(|arg| arg.into_string().ok())
                        .ok_or_else(|| invalid(USAGE))?,
                );
            }
            Some("--startup-period") if startup_period.is_none() => {
                startup_period = Some(seconds(args.next())?);
            }
            Some("--startup-failure-threshold") if startup_threshold.is_none() => {
                startup_threshold = Some(threshold(args.next())?);
            }
            Some("--liveness-period") if liveness_period.is_none() => {
                liveness_period = Some(seconds(args.next())?);
            }
            Some("--liveness-failure-threshold") if liveness_threshold.is_none() => {
                liveness_threshold = Some(threshold(args.next())?);
            }
            Some("--timeout") if timeout.is_none() => timeout = Some(seconds(args.next())?),
            Some("--") => break,
            _ => return Err(invalid(USAGE)),
        }
    }
    let url = url.ok_or_else(|| invalid(USAGE))?;
    let startup = ProbeSettings {
        period: startup_period.unwrap_or(Duration::from_secs(2)),
        failure_threshold: startup_threshold.unwrap_or(20),
    };
    let liveness = ProbeSettings {
        period: liveness_period.unwrap_or(Duration::from_secs(5)),
        failure_threshold: liveness_threshold.unwrap_or(3),
    };
    let timeout = timeout.unwrap_or(Duration::from_secs(1));
    let command = args.next().ok_or_else(|| invalid(USAGE))?;
    let mut child = Command::new(&command)
        .args(args)
        .env_remove("NOTIFY_SOCKET")
        .env_remove("WATCHDOG_PID")
        .env_remove("WATCHDOG_USEC")
        .spawn()
        .map_err(|error| io::Error::other(format!("could not start {command:?}: {error}")))?;
    let result = supervise(&mut child, url, startup, liveness, timeout);
    let _ = child.kill();
    let _ = child.wait();
    result
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("with-probe: {error}");
            ExitCode::FAILURE
        }
    }
}
