use super::*;

#[tokio::test]
async fn operator_rejects_disabled_missing_invalid_and_unavailable_inputs() {
    // Given a person with an installed credential and a separate person with the same label.
    let op = Operator::new().await;
    let alice = op.person("Alice");
    assert_ne!(alice, op.person("Alice"));
    op.ok(
        &["auth", "claude", "set", &alice, "--credential-stdin"],
        Some(ALICE_KEY),
    );
    let mut transcript = Vec::new();
    // When malformed, empty, disabled, missing-key and missing-record operations are attempted.
    for (args, input) in [
        (
            vec!["auth", "claude", "set", &alice, "--credential-stdin"],
            None,
        ),
        (
            vec!["auth", "claude", "set", &alice, "--credential-stdin"],
            Some("\n"),
        ),
        (
            vec!["auth", "claude", "set", &alice, "--credential-stdin"],
            Some("bad\rkey\n"),
        ),
        (vec!["auth", "claude", "set", &alice], None),
        (vec!["auth", "claude", "set", &alice, ALICE_KEY], None),
        (
            vec![
                "auth",
                "token",
                "revoke",
                "00000000-0000-0000-0000-000000000000",
            ],
            None,
        ),
        (vec!["auth", "person", "status", "invalid-id"], None),
    ] {
        let output = op.run(&args, input);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        transcript.extend(output.stderr);
    }
    fs::write(&op.old, [8; 32]).unwrap();
    let wrong_key = op.run(&["auth", "credentials", "reencrypt", "--to", "new"], None);
    assert_eq!(wrong_key.status.code(), Some(1));
    transcript.extend(wrong_key.stderr);
    op.ok(&["auth", "person", "disable", &alice], None);
    for (args, input) in [
        (vec!["auth", "token", "issue", &alice], None),
        (
            vec!["auth", "claude", "set", &alice, "--credential-stdin"],
            Some(BOB_KEY),
        ),
    ] {
        let output = op.run(&args, input);
        assert_eq!(output.status.code(), Some(1));
        transcript.extend(output.stderr);
    }
    assert_eq!(
        op.ok(&["auth", "person", "status", &alice], None)
            .get("enabled")
            .unwrap(),
        false
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM people")
        .fetch_one(&op.db.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
    let text = fs::read_to_string(&op.db.path).unwrap();
    let outage = text.replace(
        &op.db.config.url,
        "postgres://operator:database-secret-canary@127.0.0.1:1/missing",
    );
    fs::write(&op.db.path, outage).unwrap();
    let output = op.run(&["auth", "person", "status", &alice], None);
    assert_eq!(output.status.code(), Some(1));
    transcript.extend(output.stderr);
    // Then failures are nonzero and sanitized, and disabled people remain stored.
    let rendered = String::from_utf8_lossy(&transcript);
    for canary in [ALICE_KEY, BOB_KEY, "database-secret-canary"] {
        assert!(!rendered.contains(canary));
    }
    artifact("failure-stderr.txt", &transcript);
    artifact("failure.txt", b"PASS duplicate labels have unique UUIDs; EOF/empty/control-byte/argv/non-TTY/unknown-id/disabled/wrong-key/database-outage operations fail; sanitized stderr; people retained\n");
    op.close().await;
}
