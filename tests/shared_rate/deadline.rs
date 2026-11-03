use super::*;
use llmproxy::limit::shared::{
    AdmissionStore, PendingAdmission, SharedAdmissionError, SharedRequest,
};

#[tokio::test]
async fn later_limit_receives_remaining_database_deadline() {
    let f = Fixture::new().await;
    let ds = definitions(
        "[[limit]]\nname='first'\nprefixes=['/claude/']\nrpm=1\n[[limit]]\nname='second'\nprefixes=['/claude/']\nrpm=1",
    );
    let store = f.replica(false, std::time::Duration::from_secs(1));
    for d in &ds {
        store.acquire(charge(d, f.alice, 0)).await.unwrap();
    }
    let alice = f.alice;
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        PendingAdmission::new(
            &ds,
            SharedRequest {
                person: alice.0,
                path: "/claude/v1/messages",
                model: None,
                body_bytes: 0,
            },
        )
        .acquire(&store)
        .await
    });
    f.waiters(1).await;
    let first_expiry:i64 = sqlx::query_scalar("SELECT (extract(epoch FROM expires_at)*1000000)::bigint FROM fifo_waiters WHERE limit_name='first'").fetch_one(&f.db.pool).await.unwrap();
    assert!(
        tokio::time::timeout(SHORT, tasks.join_next())
            .await
            .is_err()
    );
    sqlx::query("UPDATE rate_state SET updated_at=clock_timestamp()-interval '61 seconds' WHERE limit_name='first'").execute(&f.db.pool).await.unwrap();
    let second_expiry = tokio::time::timeout(LONG,async {
        loop {
            let expiry:Option<i64> = sqlx::query_scalar("SELECT (extract(epoch FROM expires_at)*1000000)::bigint FROM fifo_waiters WHERE limit_name='second'").fetch_optional(&f.db.pool).await.unwrap();
            if let Some(expiry) = expiry {break expiry;}
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    assert!(
        second_expiry <= first_expiry + 20_000,
        "deadline extended by {}us",
        second_expiry - first_expiry
    );
    assert!(matches!(
        tasks.join_next().await.unwrap().unwrap(),
        Err(SharedAdmissionError::QueueTimeout(_))
    ));
    println!("shared max_wait database expiries first={first_expiry} second={second_expiry}");
    f.close().await;
}
