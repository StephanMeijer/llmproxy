use super::{
    fixture::Fixture,
    provider::{reply, stream},
};
use axum::{body::Bytes, http::StatusCode};
use serde_json::json;
use tokio::task::JoinSet;

#[tokio::test]
async fn database_outage_cancels_renewal_and_recovers_while_static_continues() {
    // Given a leased stream and two running replicas.
    let mut f = Fixture::new("database-outage").await;
    let a = f.binary(super::LIMIT).await;
    let b = f.binary(super::LIMIT).await;
    let mut clients = JoinSet::new();
    clients.spawn(f.request(&a, 0).send());
    let send = stream(f.hit().await);
    send.send(Bytes::from_static(b"data: held\n\n"))
        .await
        .unwrap();
    let mut held = clients.join_next().await.unwrap().unwrap().unwrap();
    assert!(held.chunk().await.unwrap().is_some());
    let remaining: f64 = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (expires_at-clock_timestamp()))::float8 FROM concurrency_leases",
    )
    .fetch_one(&f.db.pool)
    .await
    .unwrap();
    let start = std::time::Instant::now();
    // When existing database sessions are terminated and new connections denied.
    f.db.available(false).await;
    let config = b.config.clone();
    drop(b);
    let b = super::process::Replica::start(&config).await;
    assert_eq!(
        f.request(&b, 1).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    f.static_ok(&a).await;
    f.static_ok(&b).await;
    tokio::time::timeout(std::time::Duration::from_secs(30), send.closed())
        .await
        .unwrap();
    let cancelled_after = start.elapsed().as_secs_f64();
    assert!(
        cancelled_after < remaining,
        "provider stream outlived confirmed database expiry"
    );
    // Then renewal loss closes upstream before expiry and recovery admits again.
    f.db.available(true).await;
    clients.spawn(f.request(&b, 0).send());
    reply(f.hit().await, StatusCode::OK, "recovered");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .text()
            .await
            .unwrap(),
        "recovered"
    );
    f.db.count("concurrency_leases", 0).await;
    f.record(json!({"outage_personal_status":503,"startup_during_outage":true,"static_status":200,"confirmed_expiry_seconds":remaining,"renewal_cancel_seconds":cancelled_after,"recovered_status":200})).await;
}

#[tokio::test]
async fn incompatible_schema_and_failed_transaction_never_reach_provider() {
    // Given two replicas and a database trigger rejecting admission writes.
    let mut f = Fixture::new("schema-transaction").await;
    let a = f.binary(super::LIMIT).await;
    let b = f.binary(super::LIMIT).await;
    sqlx::raw_sql("CREATE FUNCTION reject_waiter() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected rollback'; END $$; CREATE TRIGGER reject_waiter BEFORE INSERT ON fifo_waiters FOR EACH ROW EXECUTE FUNCTION reject_waiter();")
        .execute(&f.db.pool).await.unwrap();
    // When admission rolls back and then the schema becomes incompatible.
    assert_eq!(
        f.request(&a, 0).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    f.db.count("fifo_waiters", 0).await;
    f.db.count("concurrency_leases", 0).await;
    sqlx::raw_sql("DROP TRIGGER reject_waiter ON fifo_waiters; ALTER TABLE proxy_tokens RENAME COLUMN secret_hash TO unavailable_hash;").execute(&f.db.pool).await.unwrap();
    assert_eq!(
        f.request(&b, 0).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    f.static_ok(&b).await;
    assert!(f.provider.hits.try_recv().is_err());
    // Then restoring the schema permits a request with no abandoned admission.
    sqlx::raw_sql("ALTER TABLE proxy_tokens RENAME COLUMN unavailable_hash TO secret_hash")
        .execute(&f.db.pool)
        .await
        .unwrap();
    let mut clients = JoinSet::new();
    clients.spawn(f.request(&b, 0).send());
    reply(f.hit().await, StatusCode::OK, "restored");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::OK
    );
    f.db.count("concurrency_leases", 0).await;
    f.record(json!({"transaction_failure_status":503,"schema_mismatch_status":503,"static_status":200,"restored_status":200})).await;
}

#[tokio::test]
async fn terminated_waiter_expires_and_live_waiter_advances() {
    // Given a holder, a waiting replica and a live waiter behind it.
    let mut f = Fixture::new("waiter-crash").await;
    let a = f.binary(super::LIMIT).await;
    let mut b = f.binary(super::LIMIT).await;
    let mut clients = JoinSet::new();
    clients.spawn(f.request(&a, 0).send());
    let send = stream(f.hit().await);
    send.send(Bytes::from_static(b"data: held\n\n"))
        .await
        .unwrap();
    let held = clients.join_next().await.unwrap().unwrap().unwrap();
    clients.spawn(f.request(&b, 0).send());
    f.db.count("fifo_waiters", 1).await;
    // When the queued process dies, its request cannot reach the provider.
    b.kill();
    assert!(clients.join_next().await.unwrap().unwrap().is_err());
    sqlx::query("UPDATE fifo_waiters SET expires_at=clock_timestamp()-interval '1 second'")
        .execute(&f.db.pool)
        .await
        .unwrap();
    clients.spawn(f.request(&a, 0).send());
    drop(held);
    tokio::time::timeout(super::WAIT, send.closed())
        .await
        .unwrap();
    reply(f.hit().await, StatusCode::OK, "advanced");
    // Then authoritative expiry allows the remaining waiter to proceed.
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .text()
            .await
            .unwrap(),
        "advanced"
    );
    f.db.count("fifo_waiters", 0).await;
    f.db.count("concurrency_leases", 0).await;
    f.record(json!({"crash":"SIGKILL","expired_waiter_reaped":true,"live_status":200}))
        .await;
}

#[tokio::test]
async fn upstream_connection_error_releases_all_shared_leases() {
    // Given two replicas whose provider listener has stopped.
    let mut f = Fixture::new("upstream-connect-error").await;
    let a = f.binary(super::LIMIT).await;
    let b = f.binary(super::LIMIT).await;
    f.provider.stop().await;
    // When each replica attempts a provider connection.
    for replica in [&a, &b] {
        assert_eq!(
            f.request(replica, 0).send().await.unwrap().status(),
            StatusCode::BAD_GATEWAY
        );
        // Then each failed send releases its permit.
        f.db.count("concurrency_leases", 0).await;
    }
    f.record(json!({"statuses":[502,502],"provider_hits":0}))
        .await;
}

#[tokio::test]
async fn wrong_key_on_startup_fails_closed_with_static_continuity() {
    // Given a healthy replica and a second starting with the wrong encryption material.
    let mut f = Fixture::new("wrong-key-startup").await;
    let a = f.binary("").await;
    std::fs::write(f.db.directory.join("old"), [9; 32]).unwrap();
    let b = f.binary("").await;
    // When personal Claude traffic arrives on the incorrectly configured replica.
    assert_eq!(
        f.request(&b, 0).send().await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.provider.hits.try_recv().is_err());
    f.static_ok(&b).await;
    // Then the healthy replica still selects the correct credential independently.
    std::fs::write(f.db.directory.join("old"), [1; 32]).unwrap();
    let mut clients = JoinSet::new();
    clients.spawn(f.request(&a, 0).send());
    reply(f.hit().await, StatusCode::OK, "healthy");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .text()
            .await
            .unwrap(),
        "healthy"
    );
    f.record(
        json!({"wrong_key_status":503,"wrong_key_static_status":200,"healthy_replica_status":200}),
    )
    .await;
}
