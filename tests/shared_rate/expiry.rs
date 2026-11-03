use super::*;
use llmproxy::limit::shared::{AdmissionStore, SharedAdmissionError};

#[tokio::test]
async fn server_expired_waiter_cannot_spend_budget() {
    let f = Fixture::new().await;
    let d = definition("rpm=1");
    f.replica(false, LONG)
        .acquire(charge(&d, f.alice, 0))
        .await
        .unwrap();
    let store = f.replica(true, LONG);
    let alice = f.alice;
    let task = tokio::spawn(async move { store.acquire(charge(&d, alice, 0)).await });
    f.waiters(1).await;
    sqlx::query("UPDATE fifo_waiters SET expires_at=clock_timestamp()-interval '1 second'")
        .execute(&f.db.pool)
        .await
        .unwrap();
    let result = tokio::time::timeout(LONG, task).await.unwrap().unwrap();
    assert!(matches!(result, Err(SharedAdmissionError::QueueTimeout(_))));
    f.waiters(0).await;
    f.close().await;
}

#[tokio::test]
async fn waiter_expiring_during_capacity_acquisition_rolls_back_lease_and_charge() {
    let f = Fixture::new().await;
    sqlx::raw_sql("CREATE FUNCTION expire_admitting_waiter() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE fifo_waiters SET expires_at=clock_timestamp()-interval '1 second'; RETURN NEW; END $$; CREATE TRIGGER expire_waiter AFTER INSERT ON concurrency_leases FOR EACH ROW EXECUTE FUNCTION expire_admitting_waiter();")
        .execute(&f.db.pool).await.unwrap();
    let d = definition("rpm=1\nmax_inflight=1");
    let result = f
        .replica(false, SHORT)
        .acquire(charge(&d, f.alice, 0))
        .await;
    assert!(matches!(result, Err(SharedAdmissionError::QueueTimeout(_))));
    f.waiters(0).await;
    let leases: i64 = sqlx::query_scalar("SELECT count(*) FROM concurrency_leases")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    let charged: i64 = sqlx::query_scalar("SELECT count(*) FROM rate_state")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    assert_eq!(leases, 0);
    assert_eq!(charged, 0);
    f.close().await;
}
