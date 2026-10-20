//! Runs the real binary, so process startup, flags, bind errors and signal handling are covered.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    env, fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

const BIN: &str = env!("CARGO_BIN_EXE_llmproxy");
const TOKEN_VAR: &str = "LLMPROXY_CLI_TEST_TOKEN";
const CONFIG_VAR: &str = "LLMPROXY_CONFIG";
const DEFAULT_CONFIG: &str = "/etc/llmproxy/config.toml";
const POLL: Duration = Duration::from_millis(50);
const PATIENCE: Duration = Duration::from_secs(10);

/// Kills the child if a test fails before it exits, so no server outlives the test run.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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

fn wait_until_listening(addr: SocketAddr) {
    let deadline = Instant::now() + PATIENCE;
    while TcpStream::connect_timeout(&addr, POLL).is_err() {
        assert!(Instant::now() < deadline, "nothing listened on {addr}");
        sleep(POLL);
    }
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
fn check_config_names_the_unset_variable() {
    let path = write_config("unset", &config(0, ""));
    let output = run(&["--check-config", path.to_str().unwrap()], None);
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains(&format!("{TOKEN_VAR} is not set")));
    fs::remove_file(path).unwrap();
}

#[test]
fn check_config_names_a_missing_file() {
    let output = run(&["--check-config", "/nonexistent/llmproxy.toml"], Some("t"));
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("/nonexistent/llmproxy.toml"));
}

fn spawn(path: &PathBuf) -> Server {
    Server(
        Command::new(BIN)
            .arg(path)
            .env(TOKEN_VAR, "t")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

fn signal(server: &Server, name: &str) {
    let sent = Command::new("kill")
        .args([&format!("-{name}"), &server.0.id().to_string()])
        .status()
        .unwrap();
    assert!(sent.success());
}

fn local(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

#[test]
fn serves_requests_and_exits_cleanly_on_sigterm() {
    let (listen, metrics) = (free_port(), free_port());
    let path = write_config(
        "serve",
        &config(
            listen,
            &format!("metrics_listen = \"127.0.0.1:{metrics}\"\n"),
        ),
    );
    let mut server = spawn(&path);
    wait_until_listening(local(listen));
    wait_until_listening(local(metrics));

    assert!(http_get(local(listen), "/healthz").starts_with("HTTP/1.1 200"));
    assert!(http_get(local(listen), "/nope/x").starts_with("HTTP/1.1 401"));
    let scraped = http_get(local(metrics), "/metrics");
    assert!(
        scraped.contains(r#"llm3_http_requests_total{route="other",code="401"} 1"#),
        "{scraped}"
    );
    assert!(http_get(local(metrics), "/elsewhere").starts_with("HTTP/1.1 404"));

    signal(&server, "TERM");
    assert!(
        wait_for_exit(&mut server).success(),
        "SIGTERM should end in a clean exit"
    );
    fs::remove_file(path).unwrap();
}

#[test]
fn serves_without_a_metrics_listener_and_exits_cleanly_on_sigint() {
    let listen = free_port();
    let path = write_config("no-metrics", &config(listen, ""));
    let mut server = spawn(&path);
    wait_until_listening(local(listen));
    assert!(http_get(local(listen), "/healthz").starts_with("HTTP/1.1 200"));

    signal(&server, "INT");
    assert!(
        wait_for_exit(&mut server).success(),
        "SIGINT should end in a clean exit"
    );
    fs::remove_file(path).unwrap();
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
        &config(
            free_port(),
            &format!("metrics_listen = \"127.0.0.1:{taken_port}\"\n"),
        ),
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
