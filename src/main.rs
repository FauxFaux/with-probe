use std::io;
use std::process::{Child, Command, ExitCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use sd_notify::NotifyState;
use ureq::http::{StatusCode, Version};

const CHECK_CHILD_INTERVAL: Duration = Duration::from_millis(50);

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn probe(agent: &ureq::Agent, url: &str) -> io::Result<()> {
    let response = agent
        .get(url)
        .version(Version::HTTP_10)
        .header("Connection", "close")
        .call()
        .map_err(io::Error::other)?;
    if response.version() == Version::HTTP_10 && response.status() == StatusCode::OK {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "unexpected HTTP response {:?} {}",
        response.version(),
        response.status()
    )))
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
    interval: Duration,
    timeout: Duration,
) -> io::Result<()> {
    let clock_start = Instant::now();
    // Store elapsed milliseconds plus one, with zero reserved for startup.
    let last_success = Arc::new(AtomicU64::new(0));
    let heartbeat = Arc::clone(&last_success);
    let worker = thread::Builder::new().spawn(move || -> io::Result<()> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .proxy(None)
            .max_idle_connections(0)
            .max_redirects(0)
            .http_status_as_error(false)
            .timeout_global(Some(timeout))
            .build()
            .into();
        loop {
            let started = Instant::now();
            probe(&agent, &url)?;
            heartbeat.store(
                clock_start.elapsed().as_millis() as u64 + 1,
                Ordering::Relaxed,
            );
            thread::sleep(interval.saturating_sub(started.elapsed()));
        }
    })?;

    let max_probe_age = interval + Duration::from_secs(1);
    let watchdog_interval = sd_notify::watchdog_enabled()
        .filter(|timeout| !timeout.is_zero())
        .map_or(interval, |timeout| timeout / 2);
    let mut next_watchdog = Duration::ZERO;
    loop {
        check_child(child)?;
        if worker.is_finished() {
            return worker
                .join()
                .expect("HTTP probe thread panicked");
        }

        let last_success_ms = last_success.load(Ordering::Relaxed);
        let elapsed = clock_start.elapsed();
        if last_success_ms == 0 {
            if elapsed >= timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "HTTP probe timed out",
                ));
            }
            thread::sleep(CHECK_CHILD_INTERVAL.min(timeout - elapsed));
            continue;
        }

        let probe_age = elapsed.saturating_sub(Duration::from_millis(last_success_ms - 1));
        if probe_age >= max_probe_age {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP probe heartbeat expired",
            ));
        }

        if elapsed >= next_watchdog {
            let states: &[_] = if next_watchdog.is_zero() {
                &[NotifyState::Ready, NotifyState::Watchdog]
            } else {
                &[NotifyState::Watchdog]
            };
            sd_notify::notify(states)?;
            next_watchdog = elapsed + watchdog_interval;
        }
        let until_watchdog = next_watchdog.saturating_sub(clock_start.elapsed());
        thread::sleep(CHECK_CHILD_INTERVAL.min(until_watchdog));
    }
}

fn seconds(value: Option<std::ffi::OsString>) -> io::Result<Duration> {
    value
        .and_then(|arg| arg.to_str()?.parse().ok())
        .map(Duration::from_secs)
        .filter(|duration| !duration.is_zero() && Instant::now().checked_add(*duration).is_some())
        .ok_or_else(|| {
            invalid("period and timeout must be positive whole seconds within the clock range")
        })
}

fn run() -> io::Result<()> {
    const USAGE: &str = "usage: with-probe --url HTTP_URL --period SECONDS [--timeout SECONDS] -- COMMAND [ARG ...]";
    let mut args = std::env::args_os().skip(1);
    let mut url = None;
    let mut period = None;
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
            Some("--period") if period.is_none() => period = Some(seconds(args.next())?),
            Some("--timeout") if timeout.is_none() => timeout = Some(seconds(args.next())?),
            Some("--") => break,
            _ => return Err(invalid(USAGE)),
        }
    }
    let url = url.ok_or_else(|| invalid(USAGE))?;
    let period = period.ok_or_else(|| invalid(USAGE))?;
    let timeout = timeout.unwrap_or(period);
    let command = args.next().ok_or_else(|| invalid(USAGE))?;
    let mut child = Command::new(&command)
        .args(args)
        .env_remove("NOTIFY_SOCKET")
        .env_remove("WATCHDOG_PID")
        .env_remove("WATCHDOG_USEC")
        .spawn()
        .map_err(|error| io::Error::other(format!("could not start {command:?}: {error}")))?;
    let result = supervise(&mut child, url, period, timeout);
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
