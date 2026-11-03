use super::support::*;
use axum::http::StatusCode;
use llmproxy::secret::SecretBytes;
use uuid::Uuid;

#[tokio::test]
async fn invalid_fingerprint_file_fails_closed_until_it_is_fixed() {
    let mut f = Fixture::new().await;
    let pepper = f.db.path.with_extension("pepper");
    std::fs::write(&pepper, [2; 31]).unwrap();
    let a = f.binary("", "").await;
    assert_eq!(
        f.request(&a, false).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.hits.try_recv().is_err());
    // When the key file is fixed in place, the next request loads it without a restart.
    std::fs::write(&pepper, [2; 32]).unwrap();
    let request = tokio::spawn(f.request(&a, false).send());
    reply(f.hit().await, StatusCode::OK, "ok");
    assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
    f.close(vec![a]).await;
}

#[tokio::test]
async fn one_replica_shares_one_pool_and_one_listener_between_auth_and_claude() {
    // Given a replica whose database sessions are tagged so they can be counted.
    let mut f = Fixture::new().await;
    let name = format!("replica-{}", Uuid::new_v4().simple());
    f.db.config.url = format!("{}?application_name={name}", f.db.config.url);
    // A pool of one makes the count exact: a second pool would need a second session.
    f.db.config.max_connections = 1;
    let a = f.binary("", "").await;
    let client = reqwest::Client::new();
    // When personal tokens authenticate on the static route and forward to Claude, twice.
    for _ in 0..2 {
        let request = tokio::spawn(
            client
                .get(format!("{}/static/models", a.base))
                .bearer_auth(f.alice.expose())
                .send(),
        );
        reply(f.hit().await, StatusCode::OK, "ok");
        assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
        let request = tokio::spawn(f.request(&a, false).send());
        reply(f.hit().await, StatusCode::OK, "ok");
        assert_eq!(request.await.unwrap().unwrap().status(), StatusCode::OK);
    }
    // Then both paths shared one pooled session, with at most one notification session.
    let (sessions, listeners): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE query NOT LIKE 'LISTEN%'), count(*) FILTER (WHERE query LIKE 'LISTEN%') FROM pg_stat_activity WHERE application_name=$1",
    )
    .bind(&name)
    .fetch_one(&f.db.admin)
    .await
    .unwrap();
    assert_eq!(sessions, 1);
    assert!(listeners <= 1);
    f.close(vec![a]).await;
}

#[tokio::test]
async fn stored_credential_with_header_control_bytes_never_reaches_provider() {
    let mut f = Fixture::new().await;
    f.credentials
        .replace(
            f.alice_version,
            &SecretBytes::new(b"invalid\nheader".to_vec()),
        )
        .await
        .unwrap();
    let a = f.binary("", "").await;
    assert_eq!(
        f.request(&a, false).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.hits.try_recv().is_err());
    f.close(vec![a]).await;
}
