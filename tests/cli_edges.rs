#![allow(clippy::unwrap_used)]

use std::process::Command;

#[test]
fn operator_commands_require_explicit_postgres_configuration() {
    let path = std::env::temp_dir().join(format!(
        "operator-no-postgres-{}.toml",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&path, "listen='127.0.0.1:0'\nproxy_token='test'").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
        .args(["auth", "person", "create", "Alice"])
        .env("LLMPROXY_CONFIG", &path)
        .output()
        .unwrap();
    std::fs::remove_file(path).unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("require [postgres]")
    );
}

#[test]
fn malformed_database_and_rotation_commands_fail_before_loading_configuration() {
    for args in [
        vec!["db"],
        vec!["db", "unknown"],
        vec!["db", "migrate", "--secret-canary"],
        vec!["db", "migrate", "a", "b"],
        vec!["credentials"],
        vec!["credentials", "reencrypt"],
        vec!["credentials", "reencrypt", "--wrong", "secret-canary"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
            .args(args)
            .env("LLMPROXY_CONFIG", "/nonexistent/secret-canary")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(error.contains("usage:"));
        assert!(!error.contains("secret-canary"));
    }
}
