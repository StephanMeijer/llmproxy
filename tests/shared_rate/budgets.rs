use super::*;
use llmproxy::{
    limit::shared::{AdmissionStore, PendingAdmission, SharedAdmissionError, SharedRequest},
    storage::{PreparedRates, StorageError},
};
use std::time::Duration;

#[tokio::test]
async fn aggregate_both_budgets_and_person_isolation_when_two_pools_admit() {
    let f = Fixture::new().await;
    let d = definition("rpm=2\ntpm=3");
    let a = f.replica(false, SHORT);
    let b = f.replica(true, SHORT);
    a.acquire(charge(&d, f.alice, 2)).await.unwrap();
    b.acquire(charge(&d, f.alice, 1)).await.unwrap();
    assert!(matches!(
        a.acquire(charge(&d, f.alice, 1)).await,
        Err(SharedAdmissionError::QueueTimeout(_))
    ));
    b.acquire(charge(&d, f.bob, 3)).await.unwrap();
    let balances: Vec<(String, i64)> = sqlx::query_as(
        "SELECT kind,available_units FROM rate_state WHERE person_id=$1 ORDER BY kind",
    )
    .bind(f.alice)
    .fetch_all(&f.db.pool)
    .await
    .unwrap();
    assert_eq!(balances.len(), 2);
    assert!(balances.iter().all(|(_, units)| *units < 1_000_000));
    f.waiters(0).await;
    f.close().await;
}

#[tokio::test]
async fn token_shortage_does_not_charge_available_request_budget() {
    let f = Fixture::new().await;
    let d = definition("rpm=10\ntpm=1");
    let a = f.replica(false, SHORT);
    a.acquire(charge(&d, f.alice, 1)).await.unwrap();
    let before: i64 =
        sqlx::query_scalar("SELECT available_units FROM rate_state WHERE kind='requests'")
            .fetch_one(&f.db.pool)
            .await
            .unwrap();
    assert!(matches!(
        f.replica(true, SHORT).acquire(charge(&d, f.alice, 1)).await,
        Err(SharedAdmissionError::QueueTimeout(_))
    ));
    let after: i64 =
        sqlx::query_scalar("SELECT available_units FROM rate_state WHERE kind='requests'")
            .fetch_one(&f.db.pool)
            .await
            .unwrap();
    assert_eq!(after, before);
    f.close().await;
}

#[tokio::test]
async fn configured_order_retains_earlier_charge_when_later_limit_times_out() {
    let f = Fixture::new().await;
    let ds = definitions(
        "[[limit]]\nname='first'\nprefixes=['/claude/']\nrpm=2\n[[limit]]\nname='second'\nprefixes=['/claude/']\nrpm=1",
    );
    let second = ds.get(1).unwrap();
    let store = f.replica(false, SHORT);
    store.acquire(charge(second, f.alice, 0)).await.unwrap();
    let request = SharedRequest {
        person: f.alice.0,
        path: "/claude/v1/messages",
        model: None,
        body_bytes: 0,
    };
    assert!(matches!(
        PendingAdmission::new(&ds, request)
            .acquire(&f.replica(true, SHORT))
            .await,
        Err(SharedAdmissionError::QueueTimeout(_))
    ));
    let first: i64 =
        sqlx::query_scalar("SELECT available_units FROM rate_state WHERE limit_name='first'")
            .fetch_one(&f.db.pool)
            .await
            .unwrap();
    assert_eq!(first, 60_000_000);
    let orders: Vec<(String, i64)> =
        sqlx::query_as("SELECT name,next_enqueue FROM limit_definitions ORDER BY name")
            .fetch_all(&f.db.pool)
            .await
            .unwrap();
    assert_eq!(orders, vec![("first".into(), 1), ("second".into(), 2)]);
    f.close().await;
}

#[tokio::test]
async fn conflicting_replica_fails_closed_before_enqueue() {
    let f = Fixture::new().await;
    let ds = definitions("[[limit]]\nname='budget'\nprefixes=['/claude/']\nrpm=2");
    let store = f.replica(false, LONG);
    let admitted = PendingAdmission::new(
        &ds,
        SharedRequest {
            person: f.alice.0,
            path: "/claude/v1/messages",
            model: None,
            body_bytes: 4,
        },
    )
    .acquire(&store)
    .await
    .unwrap();
    drop(admitted);
    let balance: i64 = sqlx::query_scalar("SELECT available_units FROM rate_state")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    assert_eq!(balance, 60_000_000);
    let conflict = definition("rpm=3");
    assert!(matches!(
        f.replica(true, LONG)
            .acquire(charge(&conflict, f.alice, 0))
            .await,
        Err(SharedAdmissionError::Storage(StorageError::Conflict))
    ));
    f.waiters(0).await;
    f.close().await;
}

#[tokio::test]
async fn definition_is_put_once_per_process_and_conflicts_still_fail_closed() {
    // Given a store that already admitted once, after which the stored definition diverges.
    let f = Fixture::new().await;
    let d = definition("rpm=10");
    let store = f.replica(false, LONG);
    store.acquire(charge(&d, f.alice, 0)).await.unwrap();
    sqlx::query("UPDATE limit_definitions SET config = config || '{\"probe\":1}'")
        .execute(&f.db.pool)
        .await
        .unwrap();
    // When the same store admits again, it skips put_limit and never sees the divergence.
    store.acquire(charge(&d, f.alice, 0)).await.unwrap();
    // Then a process that has not put the definition yet still fails closed.
    assert!(matches!(
        f.replica(true, LONG).acquire(charge(&d, f.alice, 0)).await,
        Err(SharedAdmissionError::Storage(StorageError::Conflict))
    ));
    f.waiters(0).await;
    f.close().await;
}

#[tokio::test]
async fn cached_definition_is_put_again_when_its_row_disappears() {
    // Given a store that cached its definition, whose row is then removed.
    let f = Fixture::new().await;
    let d = definition("rpm=10");
    let store = f.replica(false, LONG);
    store.acquire(charge(&d, f.alice, 0)).await.unwrap();
    sqlx::query("DELETE FROM rate_state")
        .execute(&f.db.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM limit_definitions")
        .execute(&f.db.pool)
        .await
        .unwrap();
    // When it admits again, then it re-puts the definition instead of failing with NotFound.
    store.acquire(charge(&d, f.alice, 0)).await.unwrap();
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM limit_definitions")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
    f.close().await;
}

#[tokio::test]
async fn prepare_rates_reports_the_longest_bucket_wait() {
    // Given both buckets drained by one admission.
    let f = Fixture::new().await;
    let d = definition("rpm=1\ntpm=60");
    f.replica(false, SHORT)
        .acquire(charge(&d, f.alice, 60))
        .await
        .unwrap();
    // When the next charge needs 30 tokens and one request.
    let mut tx = f.first.begin().await.unwrap();
    let prepared = tx
        .prepare_rates(&key(f.alice), charge(&d, f.alice, 30))
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    // Then the wait is the request bucket's full minute, not the token bucket's half.
    assert!(
        matches!(prepared, PreparedRates::Wait(wait) if wait > Duration::from_secs(59) && wait <= Duration::from_mins(1)),
        "{prepared:?}"
    );
    f.close().await;
}
