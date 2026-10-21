use super::*;
use llmproxy::{
    limit::shared::LeaseGuard,
    storage::{FencedLease, OwnerId},
};

#[tokio::test]
async fn renewal_serializes_with_capacity_and_checks_expiry_after_lock_wait() {
    // Given an almost-expired lease and admission holding the definition lock.
    let (db, storage, key) = setup().await;
    let lease = FencedLease::new(OwnerId(Uuid::new_v4()));
    let mut tx = storage.begin().await.unwrap();
    assert!(tx.acquire_concurrency(&key, (&lease, 1)).await.unwrap());
    tx.commit().await.unwrap();
    sqlx::query(
        "UPDATE concurrency_leases SET expires_at=clock_timestamp()+interval '200 milliseconds'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let mut holder = storage.begin().await.unwrap();
    holder.lock_limit(&key).await.unwrap();
    // When renewal waits behind admission until the database row expires.
    let renewal = tokio::spawn({
        let storage = storage.clone();
        let key = key.clone();
        async move {
            let mut tx = storage.begin().await.unwrap();
            let renewed = tx.renew_concurrency(&key, &lease).await.unwrap();
            tx.commit().await.unwrap();
            renewed
        }
    });
    eventually_count(&db.pool, "leases", 0).await;
    assert!(!renewal.is_finished());
    holder.commit().await.unwrap();
    // Then renewal cannot resurrect expired ownership after gaining the lock.
    assert!(!tokio::time::timeout(BOUND, renewal).await.unwrap().unwrap());
    storage.close().await;
    db.close().await;
}

#[tokio::test]
async fn database_rejected_renewal_retries_and_rejected_release_leaves_row_to_expiry() {
    // Given a live guard whose renewals the database rejects with a constraint violation.
    let (db, storage, key) = setup().await;
    let lease = FencedLease::new(OwnerId(Uuid::new_v4()));
    let started = tokio::time::Instant::now();
    let mut tx = storage.begin().await.unwrap();
    assert!(tx.acquire_concurrency(&key, (&lease, 1)).await.unwrap());
    tx.commit().await.unwrap();
    for statement in [
        "CREATE FUNCTION llmproxy_test_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'rejected by test' USING ERRCODE='23514'; END $$",
        "CREATE TRIGGER llmproxy_test_reject_update BEFORE UPDATE ON concurrency_leases FOR EACH ROW EXECUTE FUNCTION llmproxy_test_reject()",
    ] {
        sqlx::query(statement).execute(&db.pool).await.unwrap();
    }
    let expires_at = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT (extract(epoch FROM expires_at)*1000000)::bigint FROM concurrency_leases",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap()
    };
    let initial = expires_at().await;
    let guard = LeaseGuard::start(storage.clone(), (key.clone(), lease), started);
    // When the first renewal is rejected.
    tokio::time::sleep_until(started + Duration::from_secs(11)).await;
    assert_eq!(
        expires_at().await,
        initial,
        "rejected renewal must not extend the lease"
    );
    // Then dropping the rejection lets the retry extend it within the retry interval.
    sqlx::query("DROP TRIGGER llmproxy_test_reject_update ON concurrency_leases")
        .execute(&db.pool)
        .await
        .unwrap();
    tokio::time::timeout(BOUND, async {
        while expires_at().await <= initial {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    // And a rejected release leaves the row for database expiry instead of failing the request.
    sqlx::query("CREATE TRIGGER llmproxy_test_reject_delete BEFORE DELETE ON concurrency_leases FOR EACH ROW EXECUTE FUNCTION llmproxy_test_reject()")
        .execute(&db.pool)
        .await
        .unwrap();
    drop(guard);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM concurrency_leases")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(remaining, 1);
    sqlx::query("DROP TRIGGER llmproxy_test_reject_delete ON concurrency_leases")
        .execute(&db.pool)
        .await
        .unwrap();
    storage.close().await;
    db.close().await;
}
