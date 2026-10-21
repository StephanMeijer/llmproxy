use super::support::*;
use axum::http::StatusCode;
use llmproxy::secret::SecretBytes;

#[tokio::test]
async fn database_outage_returns_503_while_legacy_static_remains_available() {
    // Given a running replica whose database connections will be denied and terminated.
    let mut f = Fixture::new().await;
    let a = f.binary("", "").await;
    let name = reqwest::Url::parse(&f.db.config.url)
        .unwrap()
        .path()
        .trim_start_matches('/')
        .to_owned();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE {name} ALLOW_CONNECTIONS false"
    )))
    .execute(&f.db.admin)
    .await
    .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1")
        .bind(&name)
        .execute(&f.db.admin)
        .await
        .unwrap();
    // When personal and legacy requests run during that outage.
    let unavailable = f.request(&a, false).send().await.unwrap();
    let request = tokio::spawn(
        reqwest::Client::new()
            .get(format!("{}/static/ping", a.base))
            .bearer_auth("legacy")
            .send(),
    );
    reply(f.hit().await, StatusCode::OK, "static-outage-ok");
    // Then personal auth fails503 while legacy static remains200.
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        request.await.unwrap().unwrap().text().await.unwrap(),
        "static-outage-ok"
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE {name} ALLOW_CONNECTIONS true"
    )))
    .execute(&f.db.admin)
    .await
    .unwrap();
    artifact(
        "outage.txt",
        "PASS PostgreSQL denied connections and existing backends terminated; personal503 before upstream; legacy static200\n",
    );
    f.close(vec![a]).await;
}

#[tokio::test]
async fn invalid_key_401_affects_later_requests_but_403_does_not() {
    // Given active Alice and Bob credentials.
    let mut f = Fixture::new().await;
    let a = f.binary("", "").await;
    // When Alice's provider rejects her key and Bob receives a forbidden response.
    for (bob, status) in [
        (false, StatusCode::UNAUTHORIZED),
        (true, StatusCode::FORBIDDEN),
    ] {
        let request = tokio::spawn(f.request(&a, bob).send());
        reply(f.hit().await, status, "native-error");
        let response = request.await.unwrap().unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["request-id"], "provider-request");
        assert_eq!(response.text().await.unwrap(), "native-error");
    }
    // Then later Alice requests stop before upstream while Bob remains usable.
    assert_eq!(
        f.request(&a, false).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let request = tokio::spawn(f.request(&a, true).send());
    reply(f.hit().await, StatusCode::OK, "bob-ok");
    assert_eq!(
        request.await.unwrap().unwrap().text().await.unwrap(),
        "bob-ok"
    );
    assert!(f.hits.try_recv().is_err());
    artifact(
        "invalid-key.txt",
        "PASS upstream401 current status/error/headers preserved; later503 no hit; 403 remains active and Bob unaffected\n",
    );
    f.close(vec![a]).await;
}

#[tokio::test]
async fn old_generation_401_cannot_invalidate_replacement() {
    // Given a request with an older credential snapshot held upstream.
    let mut f = Fixture::new().await;
    let a = f.binary("", "").await;
    let request = tokio::spawn(f.request(&a, false).send());
    let hit = f.hit().await;
    let replacement = b"fake-claude-replacement-task13";
    f.credentials
        .replace(f.alice_version, &SecretBytes::new(replacement.to_vec()))
        .await
        .unwrap();
    // When that old generation receives a provider 401 after replacement.
    reply(hit, StatusCode::UNAUTHORIZED, "old-invalid");
    assert_eq!(
        request.await.unwrap().unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    // Then the next request selects the replacement and still reaches upstream.
    let request = tokio::spawn(f.request(&a, false).send());
    let hit = f.hit().await;
    assert!(hit.headers.get("x-api-key").unwrap().as_bytes() == replacement);
    reply(hit, StatusCode::OK, "replacement-ok");
    assert_eq!(
        request.await.unwrap().unwrap().text().await.unwrap(),
        "replacement-ok"
    );
    artifact(
        "generation-cas.txt",
        "PASS replacement between snapshot and upstream401 survives; subsequent request uses generation2\n",
    );
    f.close(vec![a]).await;
}

#[tokio::test]
async fn missing_removed_wrong_key_body_limit_and_route_safety_fail_before_upstream() {
    // Given valid identity and removed Alice credential.
    let f = Fixture::new().await;
    f.credentials.remove(f.alice_version).await.unwrap();
    let a = f.binary("", "max_body=16").await;
    // When requests violate each boundary.
    let large = f.request(&a, false).send().await.unwrap();
    let removed = f.request(&a, false).body("{}").send().await.unwrap();
    let wrong_method = reqwest::Client::new()
        .get(format!("{}/claude/v1/messages", a.base))
        .bearer_auth(f.bob.expose())
        .send()
        .await
        .unwrap();
    let wrong_path = reqwest::Client::new()
        .post(format!("{}/claude/v1/messages/extra", a.base))
        .bearer_auth(f.bob.expose())
        .send()
        .await
        .unwrap();
    // Then every failure occurs before upstream.
    assert_eq!(large.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(removed.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(wrong_method.status(), StatusCode::NOT_FOUND);
    assert_eq!(wrong_path.status(), StatusCode::NOT_FOUND);
    std::fs::write(f.db.path.with_extension("key"), [7; 32]).unwrap();
    let b = f.binary("", "").await;
    assert_eq!(
        f.request(&b, true).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    sqlx::query("DELETE FROM provider_credentials WHERE person_id=$1")
        .bind(f.bob_version.person)
        .execute(&f.db.pool)
        .await
        .unwrap();
    assert_eq!(
        f.request(&a, true)
            .body("{}")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    artifact(
        "boundaries.txt",
        "PASS body limit413, removed/missing/undecryptable503, nonexact route/method404; no upstream permitted\n",
    );
    f.close(vec![a, b]).await;
}

#[tokio::test]
async fn conflicting_replica_returns_503_and_legacy_static_keeps_working() {
    // Given two replicas disagreeing on a shared definition.
    let mut f = Fixture::new().await;
    let a = f
        .binary(
            "[[limit]]\nname='same'\nprefixes=['/claude/']\nmax_inflight=1\n",
            "",
        )
        .await;
    let b = f
        .binary(
            "[[limit]]\nname='same'\nprefixes=['/claude/']\nmax_inflight=2\n",
            "",
        )
        .await;
    let initial = tokio::spawn(f.request(&a, false).send());
    reply(f.hit().await, StatusCode::OK, "initial");
    initial.await.unwrap().unwrap().bytes().await.unwrap();
    // When the conflicting replica attempts Claude admission.
    assert_eq!(
        f.request(&b, false).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    // Then legacy static traffic still works without shared admission.
    let request = tokio::spawn(
        reqwest::Client::new()
            .get(format!("{}/static/ping", b.base))
            .bearer_auth("legacy")
            .send(),
    );
    let hit = f.hit().await;
    assert_eq!(hit.path, "/native/ping");
    reply(hit, StatusCode::OK, "static-ok");
    assert_eq!(
        request.await.unwrap().unwrap().text().await.unwrap(),
        "static-ok"
    );
    artifact(
        "conflict-static.txt",
        "PASS fingerprint conflict503 before upstream; legacy static200 while shared conflict exists\n",
    );
    f.close(vec![a, b]).await;
}
