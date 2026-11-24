use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn run_probe(startup_threshold: &str, child_args: &[&str]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_with-probe"));
    command
        .args([
            "--url",
            &format!("http://{address}/healthz"),
            "--timeout",
            "0.1",
            "--startup-period",
            "0.02",
            "--startup-failure-threshold",
            startup_threshold,
            "--liveness-period",
            "0.02",
            "--liveness-failure-threshold",
            "3",
            "--",
            "python3",
        ])
        .args(child_args)
        .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests"))
        .env("TEST_HTTP_PORT", address.port().to_string())
        .env_remove("NOTIFY_SOCKET")
        .env_remove("WATCHDOG_PID")
        .env_remove("WATCHDOG_USEC")
        .stderr(Stdio::piped());

    drop(listener);
    let mut process = command.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while process.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = process.kill();
            process.wait().unwrap();
            panic!("with-probe did not exit within five seconds");
        }
        thread::sleep(Duration::from_millis(10));
    }

    let output = process.wait_with_output().unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    stderr
}

#[test]
fn exits_when_child_never_becomes_ready_in_time() {
    let stderr = run_probe("3", &["http-server.py", "1", "1"]);
    assert!(
        stderr.contains("startup probe failed 3 times in a row"),
        "{stderr}"
    );
}

#[test]
fn exits_when_child_becomes_unhealthy_after_startup() {
    let stderr = run_probe("100", &["http-server.py", "0.05", "0.5"]);
    assert!(
        stderr.contains("liveness probe failed 3 times in a row"),
        "{stderr}"
    );
    assert!(stderr.contains("503"), "{stderr}");
}

#[test]
fn reports_child_exit_status() {
    for code in ["0", "7"] {
        let stderr = run_probe("100", &["-c", &format!("raise SystemExit({code})")]);
        assert!(
            stderr.contains(&format!("child exited with exit status: {code}")),
            "{stderr}"
        );
    }
}
