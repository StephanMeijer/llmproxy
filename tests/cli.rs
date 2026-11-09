//! Runs the real binary, so process startup, flags, bind errors and signal handling are covered.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    env, fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Output},
    thread::sleep,
    time::{Duration, Instant},
};

const BIN: &str = env!("CARGO_BIN_EXE_llmproxy");
const TOKEN_VAR: &str = "LLMPROXY_CLI_TEST_TOKEN";
const CONFIG_VAR: &str = "LLMPROXY_CONFIG";
const DEFAULT_CONFIG: &str = "/etc/llmproxy/config.toml";
const POLL: Duration = Duration::from_millis(50);
const PATIENCE: Duration = Duration::from_secs(30);

#[path = "common/shutdown.rs"]
mod shutdown;

/// Kills the child if a test fails before it exits, so no server outlives the test run.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        shutdown::stop(&mut self.0);
    }
}

fn config(listen: u16, extra: &str) -> String {
    format!("listen = \"127.0.0.1:{listen}\"\nproxy_token = \"${{{TOKEN_VAR}}}\"\n{extra}")
}

fn write_config(name: &str, text: &str) -> PathBuf {
    let path = env::temp_dir().join(format!("llmproxy-cli-{}-{name}.toml", std::process::id()));
    fs::write(&path, text).unwrap();
    path
}

fn run(args: &[&str], token: Option<&str>) -> Output {
    let mut command = Command::new(BIN);
    command
        .args(args)
        .env_remove(TOKEN_VAR)
        .env_remove(CONFIG_VAR);
    if let Some(token) = token {
        command.env(TOKEN_VAR, token);
    }
    command.output().unwrap()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn http_get(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap();
    reply
}

fn wait_for_exit(server: &mut Server) -> std::process::ExitStatus {
    let deadline = Instant::now() + PATIENCE;
    loop {
        if let Some(status) = server.0.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "server did not exit");
        sleep(POLL);
    }
}

#[test]
fn check_config_accepts_a_valid_file() {
    let path = write_config("valid", &config(0, ""));
    let output = run(&["--check-config", path.to_str().unwrap()], Some("t"));
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(String::from_utf8_lossy(&output.stdout).contains(": ok"));
    fs::remove_file(path).unwrap();
}

#[test]
fn check_config_reports_an_unset_variable_without_its_name() {
    let path = write_config("unset", &config(0, ""));
    let output = run(&["--check-config", path.to_str().unwrap()], None);
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("environment variable is not set"));
    assert!(!stderr(&output).contains(TOKEN_VAR));
    fs::remove_file(path).unwrap();
}

#[test]
fn check_config_redacts_closed_secret_bearing_placeholders()
-> Result<(), Box<dyn std::error::Error>> {
    const CANARY: &str = "closed-reference-cli-canary";
    let url = format!("postgres://user:{CANARY}@127.0.0.1:1/db");
    let path = write_config(
        "closed-reference",
        &config(
            0,
            &format!("[postgres]\nurl = \"${{{url}}}\"\nnamespace = \"test\"\n"),
        ),
    );
    let output = run(
        &[
            "--check-config",
            path.to_str().ok_or("non-UTF-8 config path")?,
        ],
        Some("t"),
    );
    fs::remove_file(path)?;
    assert_eq!(output.status.code(), Some(1));
    let observed = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        stderr(&output)
    );
    assert!(!observed.contains(CANARY));
    assert!(!observed.contains(&url));
    assert!(observed.contains("environment variable is not set"));
    Ok(())
}

#[test]
fn check_config_names_a_missing_file() {
    let output = run(&["--check-config", "/nonexistent/llmproxy.toml"], Some("t"));
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("/nonexistent/llmproxy.toml"));
}

/// Starts the binary with `RUST_LOG` set to `filter` (or unset), JSON logs in `<config>.log` and
/// errors in `<config>.err`.
fn spawn(path: &Path, filter: Option<&str>) -> Server {
    let mut command = Command::new(BIN);
    command
        .arg(path)
        .env(TOKEN_VAR, "t")
        .env_remove("RUST_LOG")
        .stdout(fs::File::create(path.with_extension("log")).unwrap())
        .stderr(fs::File::create(path.with_extension("err")).unwrap());
    if let Some(filter) = filter {
        command.env("RUST_LOG", filter);
    }
    Server(command.spawn().unwrap())
}

/// Waits for the server's `message` log line and returns the address it reports. The server binds
/// port 0, so no other process can take the port between choosing and binding it.
fn bound(server: &mut Server, path: &Path, message: &str) -> SocketAddr {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let log = fs::read_to_string(path.with_extension("log")).unwrap();
        let reported = log
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|line| {
                line.pointer("/fields/message")
                    .and_then(serde_json::Value::as_str)
                    == Some(message)
            })
            .and_then(|line| line.pointer("/fields/listen")?.as_str()?.parse().ok());
        if let Some(addr) = reported {
            return addr;
        }
        let exited = server.0.try_wait().unwrap();
        assert!(
            exited.is_none(),
            "server exited ({exited:?}) before {message:?}: {}",
            fs::read_to_string(path.with_extension("err")).unwrap()
        );
        assert!(Instant::now() < deadline, "server never logged {message:?}");
        sleep(POLL);
    }
}

fn remove_run_files(path: &Path) {
    for file in [
        path.into(),
        path.with_extension("log"),
        path.with_extension("err"),
    ] {
        fs::remove_file(file).unwrap();
    }
}

fn signal(server: &Server, name: &str) {
    let sent = Command::new("kill")
        .args([&format!("-{name}"), &server.0.id().to_string()])
        .status()
        .unwrap();
    assert!(sent.success());
}

#[test]
fn serves_requests_and_exits_cleanly_on_sigterm() {
    let path = write_config("serve", &config(0, "metrics_listen = \"127.0.0.1:0\"\n"));
    let mut server = spawn(&path, None);
    let listen = bound(&mut server, &path, "listening");
    let metrics = bound(&mut server, &path, "serving metrics");

    assert!(http_get(listen, "/healthz").starts_with("HTTP/1.1 200"));
    assert!(http_get(listen, "/nope/x").starts_with("HTTP/1.1 401"));
    let scraped = http_get(metrics, "/metrics");
    assert!(
        scraped.contains(r#"llm3_http_requests_total{route="other",code="401"} 1"#),
        "{scraped}"
    );
    assert!(http_get(metrics, "/elsewhere").starts_with("HTTP/1.1 404"));

    signal(&server, "TERM");
    assert!(
        wait_for_exit(&mut server).success(),
        "SIGTERM should end in a clean exit"
    );
    remove_run_files(&path);
}

/// An upstream that answers with headers and one chunk, then keeps the stream open for longer
/// than any test runs. Returns its address.
fn stuck_upstream() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0; 4096];
        let _ = stream.read(&mut request).unwrap();
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
            .unwrap();
        sleep(Duration::from_mins(10));
    });
    addr
}

#[test]
fn sigterm_exits_after_shutdown_timeout_even_with_a_stream_open() {
    let upstream = stuck_upstream();
    let route = format!(
        "shutdown_timeout = \"1s\"\n[[route]]\nname = \"stuck\"\nupstream = \"http://{upstream}\"\n"
    );
    let path = write_config("deadline", &config(0, &route));
    let mut server = spawn(&path, None);
    let listen = bound(&mut server, &path, "listening");

    // A client is mid-stream: the first chunk arrived and the body never ends.
    let mut client = TcpStream::connect(listen).unwrap();
    client.set_read_timeout(Some(PATIENCE)).unwrap();
    write!(
        client,
        "GET /stuck/x HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer t\r\n\r\n"
    )
    .unwrap();
    let mut seen = Vec::new();
    let mut buffer = [0; 1024];
    while !String::from_utf8_lossy(&seen).contains("first") {
        let read = client.read(&mut buffer).unwrap();
        assert!(
            read > 0,
            "the proxy closed the stream before the first chunk"
        );
        seen.extend_from_slice(buffer.get(..read).unwrap());
    }

    signal(&server, "TERM");
    let signalled = Instant::now();
    assert!(
        wait_for_exit(&mut server).success(),
        "the deadline exit should still be a clean exit"
    );
    assert!(
        signalled.elapsed() < Duration::from_secs(10),
        "exited only after {:?}",
        signalled.elapsed()
    );
    let log = fs::read_to_string(path.with_extension("log")).unwrap();
    assert!(log.contains("shutdown deadline passed"), "{log}");
    remove_run_files(&path);
}

#[test]
fn serves_without_a_metrics_listener_and_exits_cleanly_on_sigint() {
    let path = write_config("no-metrics", &config(0, ""));
    let mut server = spawn(&path, None);
    let listen = bound(&mut server, &path, "listening");
    assert!(http_get(listen, "/healthz").starts_with("HTTP/1.1 200"));

    signal(&server, "INT");
    assert!(
        wait_for_exit(&mut server).success(),
        "SIGINT should end in a clean exit"
    );
    remove_run_files(&path);
}

#[test]
fn serving_warns_when_postgres_tls_is_explicitly_unverified() {
    // Given a remote database allowed without certificate checks; the pool is lazy, so nothing dials it.
    let postgres = "[postgres]\nurl = \"postgres://db.invalid/x\"\nnamespace = \"test\"\ninsecure_tls = true\n";
    let path = write_config("insecure-tls", &config(0, postgres));
    let mut server = spawn(&path, None);
    bound(&mut server, &path, "listening");
    signal(&server, "TERM");
    assert!(wait_for_exit(&mut server).success());
    let log = fs::read_to_string(path.with_extension("log")).unwrap();
    remove_run_files(&path);
    assert!(log.contains("not certificate-verified"), "{log}");
}

/// Serves one `/healthz` request with `RUST_LOG` set to `filter` (or unset) and returns the log.
fn request_log(name: &str, filter: Option<&str>) -> String {
    let path = write_config(name, &config(0, ""));
    let mut server = spawn(&path, filter);
    let listen = bound(&mut server, &path, "listening");
    assert!(http_get(listen, "/healthz").starts_with("HTTP/1.1 200"));
    signal(&server, "TERM");
    assert!(wait_for_exit(&mut server).success());
    let text = fs::read_to_string(path.with_extension("log")).unwrap();
    remove_run_files(&path);
    text
}

#[test]
fn rust_log_selects_the_log_level() {
    let request_line = |log: &str| {
        log.lines()
            .any(|line| line.contains(r#""message":"request""#))
    };
    let default = request_log("log-default", None);
    assert!(
        request_line(&default),
        "info is the default level: {default}"
    );
    let empty = request_log("log-empty", Some(""));
    assert!(
        request_line(&empty),
        "an empty RUST_LOG falls back to info: {empty}"
    );
    let quiet = request_log("log-warn", Some("warn,llmproxy::listen=info"));
    assert!(
        !request_line(&quiet),
        "RUST_LOG=warn hides per-request lines: {quiet}"
    );
}

#[test]
fn check_config_rejects_what_only_the_proxy_validates() {
    let twice = "[[route]]\nname = \"a\"\nupstream = \"http://h\"\n";
    let path = write_config("twice", &config(0, &format!("{twice}{twice}")));
    let output = run(&["--check-config", path.to_str().unwrap()], Some("t"));
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("route a: defined twice"),
        "{}",
        stderr(&output)
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn check_config_honours_pgsslmode_unless_the_url_sets_sslmode() {
    let check = |name: &str, url: &str| {
        let path = write_config(
            name,
            &config(
                0,
                &format!("[postgres]\nurl = \"{url}\"\nnamespace = \"test\"\n"),
            ),
        );
        let output = Command::new(BIN)
            .args(["--check-config", path.to_str().unwrap()])
            .env_remove(CONFIG_VAR)
            .env(TOKEN_VAR, "t")
            .env("PGSSLMODE", "verify-full")
            .output()
            .unwrap();
        fs::remove_file(path).unwrap();
        output
    };
    // The environment supplies verify-full when the URL is silent.
    let silent = check("pgsslmode-silent", "postgres://db.internal/x");
    assert!(silent.status.success(), "{}", stderr(&silent));
    // An explicit sslmode in the URL wins over the environment.
    let explicit = check(
        "pgsslmode-explicit",
        "postgres://db.internal/x?sslmode=require",
    );
    assert_eq!(explicit.status.code(), Some(1));
    assert!(
        stderr(&explicit).contains("verify-full"),
        "{}",
        stderr(&explicit)
    );
}

#[test]
fn config_path_falls_back_to_the_environment_then_the_default() {
    let path = write_config("from-env", &config(0, ""));
    let from_env = Command::new(BIN)
        .arg("--check-config")
        .env(TOKEN_VAR, "t")
        .env(CONFIG_VAR, &path)
        .output()
        .unwrap();
    assert!(from_env.status.success(), "{}", stderr(&from_env));
    assert!(String::from_utf8_lossy(&from_env.stdout).contains(path.to_str().unwrap()));
    fs::remove_file(path).unwrap();

    // Whether or not the default file exists on this machine, the output names it.
    let default = run(&["--check-config"], Some("t"));
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&default.stdout),
        stderr(&default)
    );
    assert!(said.contains(DEFAULT_CONFIG), "{said}");
}

#[test]
fn refuses_to_start_when_an_address_is_taken() {
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let taken_port = taken.local_addr().unwrap().port();

    let clients = write_config("taken-listen", &config(taken_port, ""));
    let output = run(&[clients.to_str().unwrap()], Some("t"));
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains(&format!("bind 127.0.0.1:{taken_port}")),
        "{}",
        stderr(&output)
    );

    let metrics = write_config(
        "taken-metrics",
        &config(0, &format!("metrics_listen = \"127.0.0.1:{taken_port}\"\n")),
    );
    let output = run(&[metrics.to_str().unwrap()], Some("t"));
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains(&format!("bind 127.0.0.1:{taken_port}")),
        "{}",
        stderr(&output)
    );

    for path in [clients, metrics] {
        fs::remove_file(path).unwrap();
    }
}
