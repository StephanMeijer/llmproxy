use super::{
    fixture::Fixture,
    provider::{reply, stream},
};
use axum::{body::Bytes, http::StatusCode};
use serde_json::json;
use tokio::task::JoinSet;

#[tokio::test]
async fn committed_fifo_crosses_replicas_and_disconnect_releases_capacity() {
    // Given a held stream and alternating requests on two real replicas.
    let mut f = Fixture::new("fifo-cancellation").await;
    let a = f.binary(super::LIMIT).await;
    let b = f.binary(super::LIMIT).await;
    let mut clients = JoinSet::new();
    clients.spawn(f.request(&a, 0).send());
    let send = stream(f.hit().await);
    send.send(Bytes::from_static(b"data: held\n\n"))
        .await
        .unwrap();
    let held = clients.join_next().await.unwrap().unwrap().unwrap();
    f.db.count("concurrency_leases", 1).await;
    clients.spawn(
        f.request(&b, 0)
            .body(json!({"model":"claude-test","marker":"second"}).to_string())
            .send(),
    );
    f.db.count("fifo_waiters", 1).await;
    clients.spawn(
        f.request(&a, 0)
            .body(json!({"model":"claude-test","marker":"third"}).to_string())
            .send(),
    );
    f.db.count("fifo_waiters", 2).await;
    let orders: Vec<i64> =
        sqlx::query_scalar("SELECT enqueue_order FROM fifo_waiters ORDER BY enqueue_order")
            .fetch_all(&f.db.pool)
            .await
            .unwrap();
    assert_eq!(orders, [2, 3]);
    // When the stream disconnects, capacity advances in committed SQL order.
    drop(held);
    tokio::time::timeout(super::WAIT, send.closed())
        .await
        .unwrap();
    reply(f.hit().await, StatusCode::OK, "second");
    clients
        .join_next()
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    reply(f.hit().await, StatusCode::OK, "third");
    clients
        .join_next()
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    // Then provider order equals committed enqueue order and no permits remain.
    assert_eq!(f.order, ["alice", "second", "third"]);
    f.db.count("concurrency_leases", 0).await;
    f.db.count("fifo_waiters", 0).await;
    f.record(json!({"committed_enqueue_order":orders,"disconnect_released":true}))
        .await;
}

#[tokio::test]
async fn killed_stream_owner_is_reclaimed_only_after_database_expiry() {
    // Given a stream owning the sole permit and another replica queued behind it.
    let mut f = Fixture::new("crash-reclaim").await;
    let mut a = f.binary(super::LIMIT).await;
    let b = f.binary(super::LIMIT).await;
    let mut clients = JoinSet::new();
    clients.spawn(f.request(&a, 0).send());
    let send = stream(f.hit().await);
    send.send(Bytes::from_static(b"data: held\n\n"))
        .await
        .unwrap();
    let mut held = clients.join_next().await.unwrap().unwrap().unwrap();
    assert!(held.chunk().await.unwrap().is_some());
    clients.spawn(f.request(&b, 0).send());
    f.db.count("fifo_waiters", 1).await;
    // When SIGKILL terminates the stream owner without a release transaction.
    a.kill();
    let end = tokio::time::timeout(super::WAIT, held.chunk())
        .await
        .unwrap();
    assert!(end.is_err() || end.unwrap().is_none());
    let remaining: f64 = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (expires_at-clock_timestamp()))::float8 FROM concurrency_leases",
    )
    .fetch_one(&f.db.pool)
    .await
    .unwrap();
    assert!(remaining > 0.0);
    let original_expiry: String =
        sqlx::query_scalar("SELECT expires_at::text FROM concurrency_leases")
            .fetch_one(&f.db.pool)
            .await
            .unwrap();
    assert!(f.provider.hits.try_recv().is_err());
    let hit = f.hit().await;
    // Then the surviving replica reaches upstream after the original expiry.
    let elapsed: bool = sqlx::query_scalar("SELECT $1::text::timestamptz <= clock_timestamp()")
        .bind(&original_expiry)
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    assert!(elapsed, "survivor admitted before original lease expiry");
    reply(hit, StatusCode::OK, "reclaimed");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .text()
            .await
            .unwrap(),
        "reclaimed"
    );
    f.db.count("concurrency_leases", 0).await;
    f.record(
        json!({"crash":"SIGKILL","remaining_at_kill_seconds":remaining,"survivor_status":200}),
    )
    .await;
}

#[tokio::test]
async fn aggregate_request_and_token_budgets_timeout_without_second_provider_hit() {
    // Given shared request and token quotas exercised independently on two replicas.
    for (scenario, quota) in [
        ("aggregate-rpm", "rpm=1\nburst=0"),
        ("aggregate-tpm", "tpm=1\nburst=0"),
    ] {
        let mut f = Fixture::new(scenario).await;
        let limits = format!("[[limit]]\nname='rate'\nprefixes=['/claude/']\n{quota}\n");
        let a = f.binary(&limits).await;
        let mut b = f.binary(&limits).await;
        let config = b.config.clone();
        b.kill();
        let text = std::fs::read_to_string(&config)
            .unwrap()
            .replace("max_wait='40s'", "max_wait='1s'");
        std::fs::write(&config, text).unwrap();
        let b = super::process::Replica::start(&config).await;
        let mut clients = JoinSet::new();
        clients.spawn(f.request(&a, 0).send());
        reply(f.hit().await, StatusCode::OK, "charged");
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        // When the other replica requests from the same depleted budget.
        let denied = f.request(&b, 0).send().await.unwrap();
        // Then it times out without multiplying the quota by replica count.
        assert_eq!(denied.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(denied.headers().contains_key("retry-after"));
        assert!(f.provider.hits.try_recv().is_err());
        f.db.count("fifo_waiters", 0).await;
        f.record(json!({"second_status":429,"max_wait_seconds":1,"aggregate_provider_hits":1}))
            .await;
    }
}

#[tokio::test]
async fn cancelled_waiter_is_removed_without_provider_dispatch() {
    // Given a leased stream and a queued client on the other replica.
    let mut f = Fixture::new("queued-cancellation").await;
    let a = f.binary(super::LIMIT).await;
    let b = f.binary(super::LIMIT).await;
    let mut clients = JoinSet::new();
    clients.spawn(f.request(&a, 0).send());
    let send = stream(f.hit().await);
    send.send(Bytes::from_static(b"data: held\n\n"))
        .await
        .unwrap();
    let held = clients.join_next().await.unwrap().unwrap().unwrap();
    let queued = clients.spawn(f.request(&b, 0).send());
    f.db.count("fifo_waiters", 1).await;
    // When the waiting client disconnects before admission.
    queued.abort();
    assert!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    f.db.count("fifo_waiters", 0).await;
    // Then no upstream dispatch or extra permit occurs and capacity is reusable.
    assert!(f.provider.hits.try_recv().is_err());
    f.db.count("concurrency_leases", 1).await;
    drop(held);
    tokio::time::timeout(super::WAIT, send.closed())
        .await
        .unwrap();
    f.db.count("concurrency_leases", 0).await;
    clients.spawn(f.request(&b, 0).send());
    reply(f.hit().await, StatusCode::OK, "next");
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .text()
            .await
            .unwrap(),
        "next"
    );
    f.db.count("concurrency_leases", 0).await;
    f.record(
        json!({"cancelled_waiter_removed":true,"cancelled_provider_hits":0,"next_status":200}),
    )
    .await;
}
