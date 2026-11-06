use super::*;
use llmproxy::{limit::shared::AdmissionStore, storage::ChangeListener};
use std::time::Duration;

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
    assert_eq!(tx.enqueue(&k, &first, LONG).await.unwrap(), 1);
    let mut other = f.second.begin().await.unwrap();
    let mut enqueue = Box::pin(other.enqueue(&k, &second, LONG));
    assert!(tokio::time::timeout(SHORT, &mut enqueue).await.is_err());
    tx.rollback().await.unwrap();
    assert_eq!(enqueue.await.unwrap(), 1);
    other.commit().await.unwrap();
    let mut tx = f.first.begin().await.unwrap();
    assert_eq!(tx.enqueue(&k, &first, LONG).await.unwrap(), 2);
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
    listener.changed(SHORT).await.unwrap();
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
    f.waiters(0).await;
    let balances: Vec<i64> =
        sqlx::query_scalar("SELECT available_units FROM rate_state WHERE person_id=$1")
            .bind(alice)
            .fetch_all(&f.db.pool)
            .await
            .unwrap();
    assert_eq!(balances, [0, 0]);
    println!(
        "cancelled head + terminated LISTEN: admitted={admitted}, waiters=0, both rate balances=0 after one refill"
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
    tx.enqueue(&k, &reservation(), std::time::Duration::ZERO)
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

#[tokio::test]
async fn reaping_a_stale_waiter_commits_while_the_live_waiter_keeps_waiting() {
    // Given a drained bucket with one live waiter polling behind it.
    let f = Fixture::new().await;
    let d = definition("rpm=1");
    let k = key(f.alice);
    f.replica(false, LONG)
        .acquire(charge(&d, f.alice, 1))
        .await
        .unwrap();
    let b = f.replica(true, LONG);
    let copy = d.clone();
    let alice = f.alice;
    let waiter = tokio::spawn(async move { b.acquire(charge(&copy, alice, 1)).await });
    f.waiters(1).await;
    // When an already expired waiter appears ahead of it.
    let mut tx = f.first.begin().await.unwrap();
    tx.enqueue(&k, &reservation(), std::time::Duration::ZERO)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // Then the live waiter's next attempt reaps it and commits without being admitted.
    f.waiters(1).await;
    assert!(!waiter.is_finished());
    f.refill().await;
    tokio::time::timeout(LONG, waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.close().await;
}

#[tokio::test]
async fn a_head_waiting_for_budget_sleeps_instead_of_polling() {
    // Given a drained request bucket, one waiter at the head of the queue, and a sequence that
    // counts every reclaim statement whether or not its transaction commits.
    let f = Fixture::new().await;
    sqlx::raw_sql("CREATE SEQUENCE polls; CREATE FUNCTION count_poll() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('polls'); RETURN NULL; END $$; CREATE TRIGGER count_poll AFTER DELETE ON fifo_waiters FOR EACH STATEMENT EXECUTE FUNCTION count_poll()")
        .execute(&f.db.pool)
        .await
        .unwrap();
    let d = definition("rpm=1");
    let a = f.replica(false, Duration::from_mins(2));
    a.acquire(charge(&d, f.alice, 1)).await.unwrap();
    let (copy, alice) = (d.clone(), f.alice);
    let head = tokio::spawn(async move { a.acquire(charge(&copy, alice, 1)).await });
    f.waiters(1).await;
    let polls = || async {
        sqlx::query_scalar::<_, i64>("SELECT last_value FROM polls")
            .fetch_one(&f.db.pool)
            .await
            .unwrap()
    };
    // When it is left alone for a while.
    let before = polls().await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let after = polls().await;
    // Then it polled at most twice, where a 250 ms poll would have run ten times.
    assert!(after - before <= 2, "{} polls in 2.5 s", after - before);
    // And a budget written without any notification is still seen within the blind wait.
    sqlx::query("UPDATE rate_state SET updated_at=clock_timestamp()-interval '61 seconds'")
        .execute(&f.db.pool)
        .await
        .unwrap();
    tokio::time::timeout(LONG, head)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    f.close().await;
}
