use super::*;
use futures_util::StreamExt;
use llmproxy::{
    limit::shared::{LeaseGuard, LeaseStreamError},
    storage::{FencedLease, OwnerId},
};
use tokio::time::Instant;

#[tokio::test]
async fn concurrent_transactions_never_exceed_capacity() {
    // Given independent pools targeting one capacity slot.
    let (db, storage, key) = setup().await;
    let second = Storage::new(&db.config).unwrap();
    let reserve = async |store: &Storage| {
        let lease = FencedLease::new(OwnerId(Uuid::new_v4()));
        let mut tx = store.begin().await.unwrap();
        let acquired = tx.acquire_concurrency(&key, (&lease, 1)).await.unwrap();
        tx.commit().await.unwrap();
        acquired
    };
    // When both transactions contend simultaneously.
    let (a, b) = tokio::join!(reserve(&storage), reserve(&second));
    // Then exactly one obtains capacity.
    assert_ne!(a, b);
    eventually_count(&db.pool, "leases", 1).await;
    storage.close().await;
    second.close().await;
    db.close().await;
}

#[tokio::test]
async fn stale_generation_and_owner_cannot_mutate_replacement() {
    // Given an acquired lease replaced by a newer ownership epoch.
    let (db, storage, key) = setup().await;
    let lease = FencedLease::new(OwnerId(Uuid::new_v4()));
    let mut tx = storage.begin().await.unwrap();
    assert!(tx.acquire_concurrency(&key, (&lease, 1)).await.unwrap());
    tx.commit().await.unwrap();
    sqlx::query("UPDATE concurrency_leases SET ownership_generation=2")
        .execute(&db.pool)
        .await
        .unwrap();
    // When the old generation or a different owner attempts renewal/release.
    let mut tx = storage.begin().await.unwrap();
    for stale in [
        lease,
        FencedLease {
            owner: OwnerId(Uuid::new_v4()),
            generation: 2,
            ..lease
        },
    ] {
        assert!(!tx.renew_concurrency(&key, &stale).await.unwrap());
        assert!(!tx.release_concurrency(&key, &stale).await.unwrap());
    }
    // Then the current epoch still owns the slot and alone can renew/release.
    let current = FencedLease {
        generation: 2,
        ..lease
    };
    assert!(tx.renew_concurrency(&key, &current).await.unwrap());
    assert!(tx.release_concurrency(&key, &current).await.unwrap());
    tx.commit().await.unwrap();
    storage.close().await;
    db.close().await;
}

#[tokio::test]
async fn database_expiry_controls_reclaim_and_new_lifetime() {
    // Given an expired row in PostgreSQL.
    let (db, storage, key) = setup().await;
    let lease = FencedLease::new(OwnerId(Uuid::new_v4()));
    let mut tx = storage.begin().await.unwrap();
    assert!(tx.acquire_concurrency(&key, (&lease, 1)).await.unwrap());
    tx.commit().await.unwrap();
    sqlx::query("UPDATE concurrency_leases SET expires_at=clock_timestamp()-interval '1 second'")
        .execute(&db.pool)
        .await
        .unwrap();
    // When fresh acquisition and stale renewal use database time.
    let mut tx = storage.begin().await.unwrap();
    assert!(!tx.renew_concurrency(&key, &lease).await.unwrap());
    let replacement = FencedLease::new(OwnerId(Uuid::new_v4()));
    assert!(
        tx.acquire_concurrency(&key, (&replacement, 1))
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    // Then the new lifetime is measured from PostgreSQL's clock, not a client timestamp.
    let seconds: f64 = sqlx::query_scalar("SELECT extract(epoch FROM (expires_at-clock_timestamp()))::float8 FROM concurrency_leases WHERE id=$1").bind(replacement.id).fetch_one(&db.pool).await.unwrap();
    assert!(seconds > 28.0 && seconds <= 30.0);
    storage.close().await;
    db.close().await;
}

#[tokio::test]
async fn completed_upstream_retains_lease_until_buffered_body_is_consumed() {
    // Given a finite upstream whose response body is not yet consumed.
    let (db, storage, key) = setup().await;
    let lease = FencedLease::new(OwnerId(Uuid::new_v4()));
    let started = Instant::now();
    let mut tx = storage.begin().await.unwrap();
    assert!(tx.acquire_concurrency(&key, (&lease, 1)).await.unwrap());
    tx.commit().await.unwrap();
    let guard = LeaseGuard::start(storage.clone(), (key, lease), started);
    let (produced, observed) = tokio::sync::oneshot::channel();
    let source = futures_util::stream::once(async move {
        produced.send(()).unwrap();
        Ok::<_, std::io::Error>("last bytes")
    });
    let mut stream = Box::pin(guard.stream(source));
    // When the source produces its last bytes before downstream polling.
    tokio::time::timeout(BOUND, observed)
        .await
        .unwrap()
        .unwrap();
    // Then capacity remains held through those bytes and releases after body EOF.
    eventually_count(&db.pool, "leases", 1).await;
    assert_eq!(stream.next().await.unwrap().unwrap(), "last bytes");
    assert!(stream.next().await.is_none());
    eventually_count(&db.pool, "leases", 0).await;
    storage.close().await;
    db.close().await;
}

#[tokio::test]
async fn old_confirmation_cancels_unpolled_upstream_and_discards_buffered_data() {
    // Given a controller whose conservative confirmation window already elapsed.
    let (db, storage, key) = setup().await;
    let lease = FencedLease::new(OwnerId(Uuid::new_v4()));
    let started = Instant::now() - Duration::from_secs(31);
    let mut tx = storage.begin().await.unwrap();
    assert!(tx.acquire_concurrency(&key, (&lease, 1)).await.unwrap());
    tx.commit().await.unwrap();
    let guard = LeaseGuard::start(storage.clone(), (key, lease), started);
    let mut cancellation = guard.cancellation();
    // When the downstream has not yet polled the response stream.
    let stream = guard.stream(futures_util::stream::repeat_with(|| {
        Ok::<_, std::io::Error>("chunk")
    }));
    tokio::time::timeout(BOUND, cancellation.cancelled())
        .await
        .unwrap();
    eventually_count(&db.pool, "leases", 0).await;
    // Then resumed downstream polling receives lease loss, never buffered data.
    let mut stream = Box::pin(stream);
    assert!(matches!(
        stream.next().await,
        Some(Err(LeaseStreamError::LeaseLost(_)))
    ));
    assert!(stream.next().await.is_none());
    storage.close().await;
    db.close().await;
}
