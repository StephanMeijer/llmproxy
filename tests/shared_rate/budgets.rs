use super::*;
use llmproxy::{
    limit::shared::{AdmissionStore, PendingAdmission, SharedAdmissionError, SharedRequest},
    storage::StorageError,
};

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
    assert_eq!(
        SharedAdmissionError::QueueTimeout(SHORT).retry_after(),
        Some(SHORT)
    );
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
