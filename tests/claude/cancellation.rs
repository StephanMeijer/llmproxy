use super::support::*;
use axum::{body::Bytes, http::StatusCode};

const LIMITS: &str = "[[limit]]\nname='first'\nprefixes=['/claude/']\nmax_inflight=1\n[[limit]]\nname='second'\nprefixes=['/claude/']\nmax_inflight=1\n";

#[tokio::test]
async fn earlier_lease_loss_cancels_later_limit_wait_before_upstream() {
    // Given a seeded second definition whose capacity is held by another owner.
    let mut f = Fixture::new().await;
    let a = f.binary(LIMITS, "").await;
    let seed = tokio::spawn(f.request(&a, false).send());
    reply(f.hit().await, StatusCode::OK, "seed");
    seed.await.unwrap().unwrap().bytes().await.unwrap();
    f.state_count("concurrency_leases", 0).await;
    let key = llmproxy::storage::LimitKey {
        person: f.alice_version.person,
        name: "second".into(),
    };
    let reservation =
        llmproxy::storage::FencedLease::new(llmproxy::storage::OwnerId(uuid::Uuid::new_v4()));
    let mut tx = f.storage.begin().await.unwrap();
    assert!(
        tx.acquire_concurrency(&key, (&reservation, 1))
            .await
            .unwrap()
    );
    tx.commit().await.unwrap();
    let request = tokio::spawn(f.request(&a, false).send());
    f.state_count("fifo_waiters", 1).await;
    f.state_count("concurrency_leases", 2).await;
    // When the first admitted lease is lost while the second acquisition waits.
    sqlx::query("DELETE FROM concurrency_leases WHERE limit_name='first'")
        .execute(&f.db.pool)
        .await
        .unwrap();
    // Then waiting stops with503, no provider request, and no abandoned waiter.
    assert_eq!(
        tokio::time::timeout(WAIT, request)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.hits.try_recv().is_err());
    f.state_count("fifo_waiters", 0).await;
    let mut tx = f.storage.begin().await.unwrap();
    tx.release_concurrency(&key, &reservation).await.unwrap();
    tx.commit().await.unwrap();
    artifact(
        "admission-cancellation.txt",
        "PASS prior lease loss cancels later blocked definition, returns503 before provider, waiter removed\n",
    );
    f.close(vec![a]).await;
}

#[tokio::test]
async fn lease_loss_cancels_retry_delay_without_another_provider_hit() {
    // Given a provider429 with retry delay longer than the next lease renewal.
    let mut f = Fixture::new().await;
    let a = f.binary(LIMITS, "").await;
    let request = tokio::spawn(f.request(&a, false).send());
    let hit = f.hit().await;
    let mut response = axum::response::Response::new(axum::body::Body::from("retry"));
    *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
    response
        .headers_mut()
        .insert("retry-after", "12".parse().unwrap());
    assert!(hit.reply.send(response).is_ok());
    // When the second lease loses ownership during the retry interval.
    sqlx::query("DELETE FROM concurrency_leases WHERE limit_name='second'")
        .execute(&f.db.pool)
        .await
        .unwrap();
    // Then the request ends503 and never retries upstream.
    assert_eq!(
        tokio::time::timeout(WAIT, request)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.hits.try_recv().is_err());
    f.state_count("concurrency_leases", 0).await;
    artifact(
        "retry-cancellation.txt",
        "PASS lease loss during Retry-After wait returns503 with no second provider hit and no retained leases\n",
    );
    f.close(vec![a]).await;
}

#[tokio::test]
async fn either_lease_loss_cancels_wait_for_upstream_headers() {
    // Given two leases and an upstream holding response headers.
    for lost in ["first", "second"] {
        let mut f = Fixture::new().await;
        let a = f.binary(LIMITS, "").await;
        let request = tokio::spawn(f.request(&a, false).send());
        let mut hit = f.hit().await;
        // When either lease is fenced out before its renewal.
        sqlx::query("DELETE FROM concurrency_leases WHERE limit_name=$1")
            .bind(lost)
            .execute(&f.db.pool)
            .await
            .unwrap();
        let response = tokio::time::timeout(WAIT, request)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // Then no response from the held provider is required to stop the request.
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        tokio::time::timeout(WAIT, hit.reply.closed())
            .await
            .unwrap();
        f.state_count("concurrency_leases", 0).await;
        f.close(vec![a]).await;
    }
    artifact(
        "headers-cancellation.txt",
        "PASS loss of either first/second lease ends stalled headers503 and actively closes provider request, all leases released\n",
    );
}

#[tokio::test]
async fn lease_loss_cancels_body_under_downstream_backpressure() {
    // Given a stream whose downstream does not consume its body.
    let mut f = Fixture::new().await;
    let a = f.binary(LIMITS, "").await;
    let request = tokio::spawn(f.request(&a, false).send());
    let (send, dropped) = stream(f.hit().await);
    send.send(Bytes::from_static(b"data: first\n\n"))
        .await
        .unwrap();
    let response = request.await.unwrap().unwrap();
    let pumping = tokio::spawn(async move {
        let chunk = Bytes::from(vec![b'x'; 1024 * 1024]);
        while send.send(chunk.clone()).await.is_ok() {}
    });
    // When the second lease loses ownership while upstream/downstream are backpressured.
    sqlx::query("DELETE FROM concurrency_leases WHERE limit_name='second'")
        .execute(&f.db.pool)
        .await
        .unwrap();
    // Then the provider stream closes without another downstream poll.
    tokio::time::timeout(WAIT, dropped.notified())
        .await
        .unwrap();
    tokio::time::timeout(WAIT, pumping).await.unwrap().unwrap();
    f.state_count("concurrency_leases", 0).await;
    drop(response);
    // The stalled downstream write only ends once the client socket closes, which needs this
    // runtime polled; stopping the binary blocks it, so wait for the proxy to see the disconnect.
    tokio::time::timeout(WAIT, async {
        while !super::runtime_metrics::scrape(&f, &a, "backpressure")
            .await
            .contains("llm3_http_requests_in_flight 0\n")
        {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    artifact(
        "backpressure.txt",
        "PASS second lease loss actively drops upstream while downstream body is never polled; all leases released\n",
    );
    f.close(vec![a]).await;
}

#[tokio::test]
async fn downstream_disconnect_releases_every_lease() {
    // Given a streaming response holding two concurrency definitions.
    let mut f = Fixture::new().await;
    let a = f.binary(LIMITS, "").await;
    let request = tokio::spawn(f.request(&a, false).send());
    let (send, dropped) = stream(f.hit().await);
    send.send(Bytes::from_static(b"data: first\n\n"))
        .await
        .unwrap();
    let response = request.await.unwrap().unwrap();
    f.state_count("concurrency_leases", 2).await;
    // When the downstream drops its response mid-stream.
    drop(response);
    // Then upstream cancellation and both lease releases are observed.
    tokio::time::timeout(WAIT, dropped.notified())
        .await
        .unwrap();
    f.state_count("concurrency_leases", 0).await;
    artifact(
        "disconnect.txt",
        "PASS downstream disconnect closes provider stream and releases both owned concurrency leases\n",
    );
    f.close(vec![a]).await;
}
