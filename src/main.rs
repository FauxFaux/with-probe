use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command, ExitCode};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use sd_notify::NotifyState;

const CHECK_CHILD_INTERVAL: Duration = Duration::from_millis(20);

struct Endpoint<'a> {
    authority: &'a str,
    host: &'a str,
    port: u16,
    target: String,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

impl<'a> Endpoint<'a> {
    fn parse(url: &'a str) -> io::Result<Self> {
        let url = url
            .strip_prefix("http://")
            .filter(|url| url.bytes().all(|byte| byte.is_ascii_graphic()) && !url.contains('#'))
            .ok_or_else(|| invalid("expected a plain HTTP URL without a fragment"))?;
        let (authority, path) = url.split_at(url.find(['/', '?']).unwrap_or(url.len()));
        let (host, port) = if let Some(ipv6) = authority.strip_prefix('[') {
            let (host, suffix) = ipv6
                .split_once(']')
                .ok_or_else(|| invalid("missing closing bracket in IPv6 address"))?;
            host.parse::<std::net::Ipv6Addr>()
                .map_err(|_| invalid("invalid IPv6 address"))?;
            (host, suffix)
        } else {
            let (host, suffix) = authority.split_at(authority.find(':').unwrap_or(authority.len()));
            if host.is_empty()
                || !host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b))
            {
                return Err(invalid("invalid HTTP URL host"));
            }
            (host, suffix)
        };
        let port = if port.is_empty() {
            80
        } else {
            port.strip_prefix(':')
                .and_then(|port| port.parse().ok())
                .filter(|&port| port != 0)
                .ok_or_else(|| invalid("invalid HTTP URL port"))?
        };
        let target = if path.starts_with('/') {
            path.to_owned()
        } else {
            format!("/{path}")
        };
        Ok(Self {
            authority,
            host,
            port,
            target,
        })
    }

    fn probe(&self, deadline: Instant) -> io::Result<()> {
        let mut last_error = io::Error::other("host resolved to no addresses");
        for address in (self.host, self.port).to_socket_addrs()? {
            match TcpStream::connect_timeout(&address, remaining(deadline)?) {
                Ok(stream) => return self.check_response(stream, deadline),
                Err(error) => last_error = error,
            }
        }
        Err(last_error)
    }

    fn check_response(&self, mut stream: TcpStream, deadline: Instant) -> io::Result<()> {
        let request = format!(
            "GET {} HTTP/1.0\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.target, self.authority
        );
        let mut pending = request.as_bytes();
        while !pending.is_empty() {
            stream.set_write_timeout(Some(remaining(deadline)?))?;
            let written = stream.write(pending)?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            pending = &pending[written..];
        }

        let mut line = [0; 1024];
        for index in 0..line.len() {
            stream.set_read_timeout(Some(remaining(deadline)?))?;
            stream.read_exact(&mut line[index..=index])?;
            if line[index] == b'\n' {
                remaining(deadline)?;
                let status = &line[..=index];
                if status.strip_suffix(b"\r\n").is_some_and(|status| {
                    status == b"HTTP/1.0 200" || status.starts_with(b"HTTP/1.0 200 ")
                }) {
                    return Ok(());
                }
                return Err(io::Error::other(format!(
                    "unexpected HTTP status line {:?}",
                    String::from_utf8_lossy(status).trim_end()
                )));
            }
        }
        Err(io::Error::other("HTTP status line exceeds 1024 bytes"))
    }
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
        let endpoint = Endpoint::parse(&url).expect("URL was validated before spawn");
        for deadline in start {
            if finish.send(endpoint.probe(deadline)).is_err() {
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
        requests.send(deadline).map_err(io::Error::other)?;
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
    Endpoint::parse(&url)?;
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
