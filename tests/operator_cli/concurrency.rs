use super::*;

#[tokio::test]
async fn parallel_issuance_and_replacement_preserve_identity_and_generations() {
    // Given one enabled person with an initial credential.
    let op = Operator::new().await;
    let alice = op.person("Alice");
    op.ok(
        &["auth", "claude", "set", &alice, "--credential-stdin"],
        Some(ALICE_KEY),
    );
    // When two independent processes issue tokens and replace the credential concurrently.
    let first = op
        .command(&["auth", "token", "issue", &alice])
        .spawn()
        .unwrap();
    let second = op
        .command(&["auth", "token", "issue", &alice])
        .spawn()
        .unwrap();
    let issued: Vec<Value> = [first, second]
        .into_iter()
        .map(|child| {
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success(), "{output:?}");
            serde_json::from_slice(&output.stdout).unwrap()
        })
        .collect();
    assert_ne!(
        issued.first().unwrap().get("token_id").unwrap(),
        issued.last().unwrap().get("token_id").unwrap()
    );
    let mut children = Vec::new();
    for key in ["fake-concurrent-first", "fake-concurrent-second"] {
        let mut child = op
            .command(&["auth", "claude", "set", &alice, "--credential-stdin"])
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(key.as_bytes())
            .unwrap();
        children.push(child);
    }
    let successful = children
        .into_iter()
        .map(|child| child.wait_with_output().unwrap())
        .filter(|output| output.status.success())
        .count();
    // Then every committed replacement advances generation and both distinct tokens persist.
    assert!((1..=2).contains(&successful));
    let state = op.ok(&["auth", "claude", "status", &alice], None);
    assert_eq!(
        state.get("generation").unwrap().as_u64().unwrap(),
        1 + u64::try_from(successful).unwrap()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM proxy_tokens")
        .fetch_one(&op.db.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
    artifact(
        "concurrency-state.json",
        &serde_json::to_vec_pretty(&state).unwrap(),
    );
    artifact("concurrency.txt", b"PASS two concurrent real token issue processes persist distinct valid tokens; concurrent replacements commit only matching generations or report conflict\n");
    op.close().await;
}
