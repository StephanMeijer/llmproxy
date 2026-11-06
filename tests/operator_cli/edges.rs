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
    // Missing file first: the later writes recreate it for `close`.
    let cases = [
        (None, "fingerprint key unavailable"),
        (Some(0), "fingerprint key must contain exactly 32 bytes"),
        (Some(31), "fingerprint key must contain exactly 32 bytes"),
        (Some(33), "fingerprint key must contain exactly 32 bytes"),
    ];
    for (length, message) in cases {
        match length {
            Some(length) => fs::write(&op.fingerprint, vec![1; length]).unwrap(),
            None => fs::remove_file(&op.fingerprint).unwrap(),
        }
        let output = op.run(
            &["auth", "claude", "set", &alice, "--credential-stdin"],
            Some(ALICE_KEY),
        );
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(message), "{stderr}");
        assert!(
            !stderr.contains(op.fingerprint.to_str().unwrap()),
            "{stderr}"
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

#[tokio::test]
async fn operator_reports_database_url_the_driver_rejects() {
    // Given a URL that passes offline validation but the driver cannot build options from.
    let op = Operator::new().await;
    let path = op.db.path.with_extension("bogus.toml");
    fs::write(
        &path,
        "listen='127.0.0.1:0'\nproxy_token='t'\n[postgres]\nurl='postgres://127.0.0.1:1/db?sslmode=bogus'\nnamespace='test'\n",
    )
    .unwrap();
    // When an operator command starts against it.
    let output = Command::new(BIN)
        .args([
            "auth",
            "person",
            "status",
            &uuid::Uuid::new_v4().to_string(),
        ])
        .env("LLMPROXY_CONFIG", &path)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    fs::remove_file(path).unwrap();
    // Then it fails before any database access with a sanitized message.
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("error:"), "{stderr}");
    assert!(!stderr.contains("bogus"), "{stderr}");
}

#[tokio::test]
async fn reencrypt_to_unknown_key_id_changes_no_rows() {
    let op = Operator::new().await;
    let alice = op.person("Alice");
    op.ok(
        &["auth", "claude", "set", &alice, "--credential-stdin"],
        Some(ALICE_KEY),
    );
    let rows = || async {
        sqlx::query_scalar::<_, String>(
            "SELECT row_to_json(c)::text FROM provider_credentials c ORDER BY person_id",
        )
        .fetch_all(&op.db.pool)
        .await
        .unwrap()
    };
    let before = rows().await;
    let output = op.run(
        &["auth", "credentials", "reencrypt", "--to", "absent"],
        None,
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(rows().await, before);
    op.close().await;
}
