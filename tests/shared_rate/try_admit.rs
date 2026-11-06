use super::*;
use llmproxy::storage::{Admit, FencedLease, Lease, OwnerId};
use std::time::Duration;

/// Enqueues a new waiter with `ttl` and returns its fenced identity.
async fn enqueue(f: &Fixture, key: &llmproxy::storage::LimitKey, ttl: Duration) -> FencedLease {
    let fenced = FencedLease::new(OwnerId(uuid::Uuid::new_v4()));
    let waiter = Lease {
        id: fenced.id,
        owner: fenced.owner,
    };
    let mut tx = f.first.begin().await.unwrap();
    tx.enqueue(key, &waiter, ttl).await.unwrap();
    tx.commit().await.unwrap();
    fenced
}

async fn try_admit(
    f: &Fixture,
    key: &llmproxy::storage::LimitKey,
    fenced: &FencedLease,
    charge: llmproxy::limit::shared::SharedCharge<'_>,
) -> Admit {
    let tx = f.first.begin().await.unwrap();
    tx.try_admit(key, fenced, charge).await.unwrap()
}

#[tokio::test]
async fn try_admit_decides_head_capacity_budget_and_expiry_in_one_call() {
    // Given a limit of 2 requests a minute (rpm 1 + burst 1) and one request at a time.
    let f = Fixture::new().await;
    let definition = definition("rpm=1\nburst=1\nmax_inflight=1");
    let key = key(f.alice);
    let charge = charge(&definition, f.alice, 0);
    let mut tx = f.first.begin().await.unwrap();
    tx.put_limit(&key, &definition).await.unwrap();
    tx.commit().await.unwrap();
    let first = enqueue(&f, &key, LONG).await;
    let second = enqueue(&f, &key, LONG).await;

    // A waiter behind the head is not admitted, and spends nothing.
    assert_eq!(try_admit(&f, &key, &second, charge).await, Admit::NotHead);
    // The head is admitted: one request charged, the slot taken, its waiter row gone.
    assert_eq!(try_admit(&f, &key, &first, charge).await, Admit::Admitted);
    f.waiters(1).await;
    // The next head has budget left, but the only slot is still held.
    assert_eq!(
        try_admit(&f, &key, &second, charge).await,
        Admit::AtCapacity
    );

    let mut tx = f.first.begin().await.unwrap();
    assert!(tx.release_concurrency(&key, &first).await.unwrap());
    tx.commit().await.unwrap();
    assert_eq!(try_admit(&f, &key, &second, charge).await, Admit::Admitted);
    assert!({
        let mut tx = f.first.begin().await.unwrap();
        let released = tx.release_concurrency(&key, &second).await.unwrap();
        tx.commit().await.unwrap();
        released
    });

    // Both requests of the minute are spent: the next head learns how long to wait.
    let third = enqueue(&f, &key, LONG).await;
    let waited = try_admit(&f, &key, &third, charge).await;
    assert!(
        matches!(waited, Admit::Budget(wait) if wait > Duration::ZERO && wait <= Duration::from_mins(1)),
        "{waited:?}"
    );

    // A waiter whose own deadline passed is told so and removed, before any other check.
    let expired = enqueue(&f, &key, Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(try_admit(&f, &key, &expired, charge).await, Admit::Expired);
    f.waiters(1).await;
    f.close().await;
}

#[tokio::test]
async fn a_failed_attempt_on_a_capped_limit_leaves_no_lease_behind() {
    use llmproxy::limit::shared::AdmissionStore;
    // Given a capped limit whose definition this process has already stored.
    let f = Fixture::new().await;
    let definition = definition("rpm=60\nburst=10\nmax_inflight=1");
    let store = f.replica(false, LONG);
    let charge = charge(&definition, f.alice, 0);
    drop(store.acquire(charge).await.unwrap());
    let leases = "SELECT count(*) FROM concurrency_leases";
    super::eventually::count_eventually(&f.db.pool, leases, 0, LONG).await;
    // When the attempt fails part-way, after the waiter is queued.
    sqlx::query("ALTER TABLE rate_state RENAME TO rate_state_away")
        .execute(&f.db.pool)
        .await
        .unwrap();
    assert!(store.acquire(charge).await.is_err());
    sqlx::query("ALTER TABLE rate_state_away RENAME TO rate_state")
        .execute(&f.db.pool)
        .await
        .unwrap();
    // Then no slot stays taken: the next request is admitted at once.
    super::eventually::count_eventually(&f.db.pool, leases, 0, LONG).await;
    drop(store.acquire(charge).await.unwrap());
    f.close().await;
}
