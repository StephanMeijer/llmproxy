use super::*;
use llmproxy::{limit::shared::AdmissionStore, storage::ChangeListener};

#[tokio::test]
async fn successful_commit_order_serializes_enqueue_and_rollback_reuses_counter() {
    let f = Fixture::new().await;
    let d = definition("rpm=1");
    let k = key(f.alice);
    let mut tx = f.first.begin().await.unwrap();
    tx.put_limit(&k, &d).await.unwrap();
    tx.commit().await.unwrap();
    let first = reservation();
    let second = reservation();
    let mut tx = f.first.begin().await.unwrap();
    assert_eq!(tx.enqueue(&k, (&first, LONG)).await.unwrap(), 1);
    let mut other = f.second.begin().await.unwrap();
    let mut enqueue = Box::pin(other.enqueue(&k, (&second, LONG)));
    assert!(tokio::time::timeout(SHORT, &mut enqueue).await.is_err());
    tx.rollback().await.unwrap();
    assert_eq!(enqueue.await.unwrap(), 1);
    other.commit().await.unwrap();
    let mut tx = f.first.begin().await.unwrap();
    assert_eq!(tx.enqueue(&k, (&first, LONG)).await.unwrap(), 2);
    tx.commit().await.unwrap();
    let mut tx = f.first.begin().await.unwrap();
    assert_eq!(tx.waiter_head(&k).await.unwrap().unwrap().id, second.id);
    tx.rollback().await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn cancelled_head_and_missed_notify_advance_next_without_charge_leak() {
    let f = Fixture::new().await;
    let d = definition("rpm=1\ntpm=1");
    let a = f.replica(false, LONG);
    let b = f.replica(true, LONG);
    a.acquire(charge(&d, f.alice, 1)).await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    let copy = d.clone();
    let alice = f.alice;
    let head = tasks.spawn(async move { a.acquire(charge(&copy, alice, 1)).await });
    f.waiters(1).await;
    let copy = d.clone();
    tasks.spawn(async move { b.acquire(charge(&copy, alice, 1)).await });
    f.waiters(2).await;
    let mut listener = ChangeListener::new(f.second.clone());
    listener.limit(&key(alice), SHORT).await.unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'LISTEN%'").execute(&f.db.pool).await.unwrap();
    head.abort();
    f.waiters(1).await;
    f.refill().await;
    let mut admitted = 0;
    while let Some(result) = tokio::time::timeout(LONG, tasks.join_next()).await.unwrap() {
        match result {
            Ok(result) => {
                result.unwrap();
                admitted += 1;
            }
            Err(error) => assert!(error.is_cancelled()),
        }
    }
    assert_eq!(admitted, 1);
    let snapshot = match listener.limit(&key(alice), SHORT).await {
        Ok(snapshot) => snapshot,
        Err(_) => listener.limit(&key(alice), SHORT).await.unwrap(),
    };
    assert!(snapshot.waiters.is_empty());
    assert!(
        snapshot
            .rates
            .iter()
            .all(|state| state.available_units == 0)
    );
    println!(
        "cancelled head + terminated LISTEN: admitted={admitted}, waiters={}, both rate balances=0 after one refill",
        snapshot.waiters.len()
    );
    drop(listener);
    f.close().await;
}

#[tokio::test]
async fn expired_head_is_reaped_before_live_waiter_admission() {
    let f = Fixture::new().await;
    let d = definition("rpm=1");
    let k = key(f.alice);
    let mut tx = f.first.begin().await.unwrap();
    tx.put_limit(&k, &d).await.unwrap();
    tx.enqueue(&k, (&reservation(), std::time::Duration::ZERO))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    f.replica(true, LONG)
        .acquire(charge(&d, f.alice, 0))
        .await
        .unwrap();
    f.waiters(0).await;
    f.close().await;
}
