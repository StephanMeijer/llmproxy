use super::*;
use llmproxy::secret::{ProxyToken, TokenHasher};
use std::num::NonZeroUsize;

#[tokio::test]
async fn operator_lifecycle_keeps_secrets_out_of_status_and_storage() {
    // Given two people in a freshly migrated PostgreSQL database.
    let op = Operator::new().await;
    let alice = op.person("Alice");
    let bob = op.person("Bob");
    let mut transcript = Vec::new();
    // When the real binary issues tokens, provisions, replaces, removes and rotates credentials.
    let issued = op.ok(&["auth", "token", "issue", &alice], None);
    let wire = issued.get("token").unwrap().as_str().unwrap();
    assert_eq!(issued.to_string().matches(wire).count(), 1);
    let token = ProxyToken::parse(wire).unwrap();
    let hash: String = sqlx::query_scalar("SELECT secret_hash FROM proxy_tokens WHERE id=$1")
        .bind(token.id())
        .fetch_one(&op.db.pool)
        .await
        .unwrap();
    assert!(
        TokenHasher::new(NonZeroUsize::MIN)
            .verify(&token, &hash)
            .await
            .unwrap()
    );
    let bob_token = op.ok(&["auth", "token", "issue", &bob], None);
    assert_ne!(
        issued.get("token_id").unwrap(),
        bob_token.get("token_id").unwrap()
    );
    for (id, key) in [(&alice, ALICE_KEY), (&bob, BOB_KEY)] {
        let installed = op.ok(
            &["auth", "claude", "set", id, "--credential-stdin"],
            Some(key),
        );
        assert_eq!(installed.get("generation").unwrap(), 1);
        transcript.push(installed);
    }
    encrypted_rows(&op, "installed-encrypted-rows.json", wire).await;
    let duplicate = op.run(
        &["auth", "claude", "set", &bob, "--credential-stdin"],
        Some(ALICE_KEY),
    );
    assert!(!duplicate.status.success());
    let replacement = op.ok(
        &["auth", "claude", "set", &alice, "--credential-stdin"],
        Some("fake-anthropic-cli-replacement-9\n"),
    );
    assert_eq!(replacement.get("generation").unwrap(), 2);
    transcript.push(replacement);
    let rotated = op.ok(&["auth", "credentials", "reencrypt", "--to", "new"], None);
    assert_eq!(rotated.get("rewritten").unwrap(), 2);
    assert_eq!(rotated.get("remaining").unwrap(), 0);
    transcript.push(rotated);
    let removed = op.ok(&["auth", "claude", "remove", &alice], None);
    assert_eq!(removed.get("state").unwrap(), "removed");
    assert_eq!(removed.get("generation").unwrap(), 4);
    transcript.push(removed);
    let revoked = op.ok(
        &[
            "auth",
            "token",
            "revoke",
            issued.get("token_id").unwrap().as_str().unwrap(),
        ],
        None,
    );
    assert_eq!(revoked.get("revoked").unwrap(), true);
    transcript.push(revoked);
    // Then status exposes public metadata, persisted tokens are verifiers and provider keys are ciphertext.
    for args in [
        ["auth", "person", "status", &alice],
        ["auth", "claude", "status", &alice],
        ["auth", "claude", "status", &bob],
    ] {
        let status = op.ok(&args, None);
        assert!(status.get("token").is_none());
        assert!(!status.to_string().contains(wire));
        transcript.push(status);
    }
    let revoked: bool =
        sqlx::query_scalar("SELECT revoked_at IS NOT NULL FROM proxy_tokens WHERE id=$1")
            .bind(token.id())
            .fetch_one(&op.db.pool)
            .await
            .unwrap();
    assert!(revoked);
    encrypted_rows(&op, "encrypted-rows.json", wire).await;
    artifact(
        "lifecycle-output.json",
        &serde_json::to_vec_pretty(&transcript).unwrap(),
    );
    artifact("lifecycle.txt", b"PASS real binary migrate/create/issue/set/status/replace/remove/revoke/reencrypt; full token occurs once and verifies Argon2id; no token in status; no fake provider key in stdout/stderr/database rows; duplicate credential rejected\n");
    op.close().await;
}

async fn encrypted_rows(op: &Operator, artifact_name: &str, wire: &str) {
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT row_to_json(c)::text FROM provider_credentials c")
            .fetch_all(&op.db.pool)
            .await
            .unwrap();
    for row in &rows {
        for key in [ALICE_KEY, BOB_KEY, "fake-anthropic-cli-replacement-9", wire] {
            assert!(!row.contains(key));
        }
    }
    artifact(artifact_name, &serde_json::to_vec_pretty(&rows).unwrap());
}
