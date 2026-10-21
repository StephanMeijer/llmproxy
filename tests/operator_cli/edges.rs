use super::*;

#[tokio::test]
async fn token_issuance_reports_output_failure() {
    let op = Operator::new().await;
    let alice = op.person(&"A".repeat(8192));
    let output = op
        .command(&["auth", "token", "issue", &alice])
        .stdout(fs::File::options().write(true).open("/dev/full").unwrap())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
    op.close().await;
}

#[tokio::test]
async fn credential_status_distinguishes_missing_from_invalid() {
    let op = Operator::new().await;
    let alice = op.person("Alice");
    let status = op.ok(&["auth", "claude", "status", &alice], None);
    assert_eq!(status.get("state").unwrap(), "missing");
    assert!(status.get("generation").unwrap().is_null());
    op.ok(
        &["auth", "claude", "set", &alice, "--credential-stdin"],
        Some(ALICE_KEY),
    );
    sqlx::query("UPDATE provider_credentials SET state='invalid'")
        .execute(&op.db.pool)
        .await
        .unwrap();
    let status = op.ok(&["auth", "claude", "status", &alice], None);
    assert_eq!(status.get("state").unwrap(), "invalid");
    assert_eq!(status.get("generation").unwrap(), 2);
    op.close().await;
}

#[tokio::test]
async fn credential_stdin_accepts_crlf_and_rejects_oversized_input() {
    let op = Operator::new().await;
    let alice = op.person("Alice");
    let args = ["auth", "claude", "set", &alice, "--credential-stdin"];
    let status = op.ok(&args, Some(&format!("{ALICE_KEY}\r\n")));
    assert_eq!(status.get("state").unwrap(), "active");
    let oversized = "x".repeat(4097);
    let output = op.run(&args, Some(&oversized));
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&output.stderr).contains(&oversized));
    assert_eq!(
        op.ok(&["auth", "claude", "status", &alice], None)
            .get("generation")
            .unwrap(),
        1
    );
    op.close().await;
}

#[tokio::test]
async fn fingerprint_file_requires_exact_key_length() {
    let op = Operator::new().await;
    let alice = op.person("Alice");
    for length in [0, 31, 33] {
        fs::write(&op.fingerprint, vec![1; length]).unwrap();
        let output = op.run(
            &["auth", "claude", "set", &alice, "--credential-stdin"],
            Some(ALICE_KEY),
        );
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains(op.fingerprint.to_str().unwrap())
        );
    }
    assert_eq!(
        op.ok(&["auth", "claude", "status", &alice], None)
            .get("state")
            .unwrap(),
        "missing"
    );
    op.close().await;
}
