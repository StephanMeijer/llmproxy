use super::*;
use std::io::Read;

#[tokio::test]
async fn stdin_credential_is_absent_from_process_arguments_and_environment() {
    // Given a real command waiting for an explicit stdin credential.
    let op = Operator::new().await;
    let alice = op.person("Alice");
    let mut child = op
        .command(&["auth", "claude", "set", &alice, "--credential-stdin"])
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let (argv, environment) = loop {
        let argv = fs::read(format!("/proc/{}/cmdline", child.id())).unwrap();
        let environment = fs::read(format!("/proc/{}/environ", child.id())).unwrap();
        if !argv.is_empty() && !environment.is_empty() {
            break (argv, environment);
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    };
    // When a provider key is supplied only through the pipe.
    child
        .stdin
        .take()
        .unwrap()
        .write_all(ALICE_KEY.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    // Then argv, environment and both output streams contain no provider plaintext.
    assert!(output.status.success(), "{output:?}");
    for bytes in [&argv, &environment, &output.stdout, &output.stderr] {
        assert!(!String::from_utf8_lossy(bytes).contains(ALICE_KEY));
    }
    artifact("stdin-argv.bin", &argv);
    artifact("stdin-output.json", &output.stdout);
    artifact("stdin.txt", b"PASS real process /proc argv/environ and stdout/stderr inspected; fake provider key passed only through stdin; no plaintext found\n");
    op.close().await;
}

#[tokio::test]
async fn terminal_prompt_hides_credential_input() {
    // Given the real binary attached to a fresh pseudo-terminal through util-linux script.
    let op = Operator::new().await;
    let alice = op.person("Alice");
    let mut child = Command::new("script")
        .args([
            "-qefc",
            &format!("'{BIN}' auth claude set {alice}"),
            "/dev/null",
        ])
        .env("LLMPROXY_CONFIG", &op.db.path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut terminal = Vec::new();
    let mut byte = [0];
    while !terminal.ends_with(b"Anthropic API key: ") {
        assert_eq!(stdout.read(&mut byte).unwrap(), 1);
        terminal.extend(byte);
    }
    // When the credential is typed after the prompt.
    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "{BOB_KEY}").unwrap();
    stdout.read_to_end(&mut terminal).unwrap();
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    // Then the terminal transcript contains the success metadata but never echoes the key.
    assert!(output.status.success(), "{output:?} {terminal:?}");
    let transcript = String::from_utf8_lossy(&terminal);
    assert!(!transcript.contains(BOB_KEY));
    assert!(transcript.contains("\"state\":\"active\""));
    assert_eq!(
        op.ok(&["auth", "claude", "status", &alice], None)
            .get("generation")
            .unwrap(),
        1
    );
    artifact("terminal-transcript.txt", &terminal);
    artifact("terminal.txt", b"PASS real binary hidden TTY prompt via script PTY; typed fake key absent transcript; stored generation active\n");
    op.close().await;
}
