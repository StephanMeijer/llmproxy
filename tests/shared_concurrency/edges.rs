use super::*;
use llmproxy::{
    limit::shared::{LeaseGuard, LeaseLost},
    storage::{FencedLease, OwnerId, StorageError},
};
use tokio::time::Instant;

#[tokio::test]
async fn lease_cancellation_runs_live_work_and_rejects_expired_work() {
    let (db, storage, key) = setup().await;
    let lease = FencedLease::new(OwnerId(Uuid::new_v4()));
    let mut tx = storage.begin().await.unwrap();
    assert!(tx.acquire_concurrency(&key, (&lease, 1)).await.unwrap());
    tx.commit().await.unwrap();
    let guard = LeaseGuard::start(storage.clone(), (key.clone(), lease), Instant::now());
    assert_eq!(guard.cancellation().run(async { 42 }).await, Ok(42));
    drop(guard);
    eventually_count(&db.pool, "leases", 0).await;
    let expired = LeaseGuard::start(
        storage.clone(),
        (key, lease),
        Instant::now() - Duration::from_secs(31),
    );
    assert_eq!(
        expired
            .cancellation()
            .run(std::future::pending::<()>())
            .await,
        Err(LeaseLost)
    );
    drop(expired);
    storage.close().await;
    db.close().await;
}

#[tokio::test]
async fn repository_rejects_invalid_capacity_generation_and_definition_identity() {
    let (db, storage, key) = setup().await;
    let mut tx = storage.begin().await.unwrap();
    let mut lease = FencedLease::new(OwnerId(Uuid::new_v4()));
    assert_eq!(
        tx.acquire_concurrency(&key, (&lease, 0)).await,
        Err(StorageError::InvalidData)
    );
    lease.generation = 0;
    assert_eq!(
        tx.acquire_concurrency(&key, (&lease, 1)).await,
        Err(StorageError::InvalidData)
    );
    let other_key = LimitKey {
        name: "different".into(),
        ..key
    };
    assert_eq!(
        tx.put_limit(&other_key, &definition()).await,
        Err(StorageError::InvalidData)
    );
    tx.rollback().await.unwrap();
    storage.close().await;
    db.close().await;
}
