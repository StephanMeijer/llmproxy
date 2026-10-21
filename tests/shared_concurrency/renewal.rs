use super::*;
use llmproxy::storage::{FencedLease, OwnerId};

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
