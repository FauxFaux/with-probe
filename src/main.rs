use std::io;
use std::process::{Child, Command, ExitCode};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use sd_notify::NotifyState;
use ureq::http::{StatusCode, Version};

const CHECK_CHILD_INTERVAL: Duration = Duration::from_millis(20);

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

fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|time| !time.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "HTTP probe timed out"))
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
    let (requests, start) = mpsc::sync_channel(1);
    let (finish, results) = mpsc::sync_channel(1);
    thread::Builder::new().spawn(move || {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .proxy(None)
            .max_idle_connections(0)
            .max_redirects(0)
            .http_status_as_error(false)
            .timeout_global(Some(timeout))
            .build()
            .into();
        for () in start {
            if finish.send(probe(&agent, &url)).is_err() {
                break;
            }
        }
    })?;

    let mut ready = false;
    loop {
        check_child(child)?;
        let started = Instant::now();
        let deadline = started
            .checked_add(timeout)
            .ok_or_else(|| invalid("probe deadline overflow"))?;
        requests.send(()).map_err(io::Error::other)?;
        loop {
            let result = results.recv_timeout(remaining(deadline)?.min(CHECK_CHILD_INTERVAL));
            check_child(child)?;
            match result {
                Ok(result) => {
                    remaining(deadline)?;
                    result?;
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(error) => return Err(io::Error::other(error)),
            }
        }

        if !ready {
            sd_notify::notify(&[NotifyState::Ready, NotifyState::Watchdog])?;
            ready = true;
        } else {
            sd_notify::notify(&[NotifyState::Watchdog])?;
        }

        while let Some(delay) = interval
            .checked_sub(started.elapsed())
            .filter(|delay| !delay.is_zero())
        {
            thread::sleep(delay.min(CHECK_CHILD_INTERVAL));
            check_child(child)?;
        }
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
