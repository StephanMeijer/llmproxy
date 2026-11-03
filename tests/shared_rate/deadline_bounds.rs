use super::*;
use llmproxy::{
    limit::shared::{AdmissionStore, SharedAdmissionError, SharedRateStore},
    storage::Storage,
};
use std::time::Duration;

const BUDGET: Duration = Duration::from_millis(50);
const BOUND: Duration = Duration::from_millis(200);
const TOLERANCE: Duration = Duration::from_millis(50);

#[tokio::test]
async fn definition_lock_wait_respects_end_to_end_deadline() {
    let f = Fixture::new().await;
    let d = definition("rpm=1");
    let k = key(f.alice);
    let mut setup = f.first.begin().await.unwrap();
    setup.put_limit(&k, &d).await.unwrap();
    setup.commit().await.unwrap();
    let mut lock = f.first.begin().await.unwrap();
    lock.lock_limit(&k).await.unwrap();
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(
        BOUND,
        f.replica(true, BUDGET).acquire(charge(&d, f.alice, 0)),
    )
    .await;
    let elapsed = started.elapsed();
    lock.rollback().await.unwrap();
    assert!(elapsed <= BUDGET + TOLERANCE);
    assert!(matches!(result, Ok(Err(SharedAdmissionError::QueueTimeout(wait))) if wait == BUDGET));
    f.waiters(0).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM rate_state")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    println!("held definition: budget={BUDGET:?} elapsed={elapsed:?}, no charges");
    f.close().await;
}

#[tokio::test]
async fn exhausted_pool_respects_end_to_end_deadline() {
    let mut f = Fixture::new().await;
    let d = definition("rpm=1");
    f.db.config.max_connections = 1;
    let storage = Storage::new(&f.db.config).unwrap();
    let held = storage.begin().await.unwrap();
    let store = SharedRateStore::new(storage.clone(), BUDGET);
    let started = tokio::time::Instant::now();
    let result = tokio::time::timeout(BOUND, store.acquire(charge(&d, f.alice, 0))).await;
    let elapsed = started.elapsed();
    held.rollback().await.unwrap();
    assert!(elapsed <= BUDGET + TOLERANCE);
    assert!(matches!(result, Ok(Err(SharedAdmissionError::QueueTimeout(wait))) if wait==BUDGET));
    f.waiters(0).await;
    storage.close().await;
    println!("exhausted pool: budget={BUDGET:?} elapsed={elapsed:?}");
    f.close().await;
}

#[tokio::test]
async fn listener_poll_cannot_extend_small_or_tiny_budget() {
    let f = Fixture::new().await;
    let d = definition("rpm=1\ntpm=1");
    f.replica(false, LONG)
        .acquire(charge(&d, f.alice, 1))
        .await
        .unwrap();
    for budget in [BUDGET, Duration::from_millis(1), Duration::ZERO] {
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            BOUND,
            f.replica(true, budget).acquire(charge(&d, f.alice, 1)),
        )
        .await;
        let elapsed = started.elapsed();
        assert!(elapsed <= budget + TOLERANCE);
        assert!(matches!(result,Ok(Err(SharedAdmissionError::QueueTimeout(wait))) if wait==budget));
        f.waiters(0).await;
        let sum: i64 = sqlx::query_scalar("SELECT sum(available_units)::bigint FROM rate_state")
            .fetch_one(&f.db.pool)
            .await
            .unwrap();
        assert_eq!(sum, 0);
        println!("listener/poll: budget={budget:?} elapsed={elapsed:?}, waiters=0, units=0");
    }
    f.close().await;
}

#[tokio::test]
async fn queued_deadline_returns_while_cleanup_waits_for_definition_lock() {
    let queued_budget = Duration::from_millis(500);
    let f = Fixture::new().await;
    let d = definition("rpm=1\ntpm=1");
    f.replica(false, LONG)
        .acquire(charge(&d, f.alice, 1))
        .await
        .unwrap();
    f.second.begin().await.unwrap().rollback().await.unwrap();
    let store = f.replica(true, queued_budget);
    let alice = f.alice;
    let mut tasks = tokio::task::JoinSet::new();
    let started = tokio::time::Instant::now();
    tasks.spawn(async move { store.acquire(charge(&d, alice, 1)).await });
    f.waiters(1).await;
    let mut lock = f.first.begin().await.unwrap();
    lock.lock_limit(&key(alice)).await.unwrap();
    let result = tokio::time::timeout(queued_budget + BOUND, tasks.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let elapsed = started.elapsed();
    assert!(matches!(result,Err(SharedAdmissionError::QueueTimeout(wait)) if wait==queued_budget));
    assert!(elapsed <= queued_budget + TOLERANCE);
    lock.rollback().await.unwrap();
    f.waiters(0).await;
    let sum: i64 = sqlx::query_scalar("SELECT sum(available_units)::bigint FROM rate_state")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    assert_eq!(sum, 0);
    println!(
        "queued lock: budget={queued_budget:?} elapsed={elapsed:?}, cleanup after unlock, waiters=0, units=0"
    );
    f.close().await;
}
