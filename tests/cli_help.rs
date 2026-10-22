#![allow(clippy::unwrap_used)]

use std::process::Command;

#[test]
fn help_exits_offline_without_reading_configuration() {
    // Given a nonexistent configuration and unreachable database.
    for flag in ["--help", "-h"] {
        // When the real binary is asked for help.
        let output = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
            .arg(flag)
            .env("LLMPROXY_CONFIG", "/nonexistent/keys-and-config")
            .env("DATABASE_URL", "postgres://canary-secret@127.0.0.1:1/db")
            .output()
            .unwrap();
        // Then it succeeds and emits command syntax without loading dependencies.
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains("llmproxy auth claude set <person-id> [--credential-stdin]"));
        assert!(text.contains("llmproxy db migrate [config]"));
        assert!(!text.contains("canary-secret"));
        println!("HELP {flag}\n{text}");
    }
}

#[test]
fn unknown_flags_fail_without_echoing_input_or_loading_config() {
    // Given secret-bearing invalid arguments.
    for args in [
        vec!["--canary-secret"],
        vec!["-x"],
        vec!["--check-config", "--canary-secret"],
        vec!["one", "two"],
    ] {
        // When the real binary parses them.
        let output = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
            .args(args)
            .env("LLMPROXY_CONFIG", "/nonexistent/canary-secret")
            .output()
            .unwrap();
        // Then it fails with a fixed error, before reading configuration.
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(
            String::from_utf8(output.stderr).unwrap(),
            "error: invalid arguments; use llmproxy --help\n"
        );
    }
}
