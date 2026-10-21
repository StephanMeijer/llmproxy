use super::{
    runtime_metrics::{connections, counts, scrape},
    support::*,
};
use axum::body::Bytes;

async fn availability(binary: &Binary, available: bool) {
    let expected = format!("llm3_postgres_available {}\n", u8::from(available));
    let client = reqwest::Client::new();
    tokio::time::timeout(WAIT, async {
        loop {
            let text = client
                .get(&binary.metrics)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            if text.contains(&expected) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn heartbeat_observes_partition_and_recovery_without_another_authentication() {
    // Given an active native stream with a shared concurrency lease.
    let mut f = Fixture::new().await;
    let binary = f
        .binary(
            "[[limit]]\nname='heartbeat'\nprefixes=['/claude/']\nmax_inflight=1\n",
            "",
        )
        .await;
    let request = tokio::spawn(f.request(&binary, false).send());
    let (send, dropped) = stream(f.hit().await);
    send.send(Bytes::from_static(b"data: first\n\n"))
        .await
        .unwrap();
    let mut response = request.await.unwrap().unwrap();
    assert_eq!(response.chunk().await.unwrap().unwrap(), "data: first\n\n");
    // When the database disappears and recovers while only metrics are scraped.
    connections(&f, false).await;
    availability(&binary, false).await;
    let text = scrape(&f, &binary, "heartbeat-outage").await;
    counts(&text, [2, 0, 0, 0]);
    assert!(text.contains("llm3_http_requests_total{route=\"claude\",code=\"200\"} 1\n"));
    connections(&f, true).await;
    availability(&binary, true).await;
    let text = scrape(&f, &binary, "heartbeat-recovery").await;
    counts(&text, [2, 0, 0, 0]);
    // Then a successful renewal recovers availability and healthy fencing never marks an outage.
    send.send(Bytes::from_static(b"data: recovered\n\n"))
        .await
        .unwrap();
    assert_eq!(
        response.chunk().await.unwrap().unwrap(),
        "data: recovered\n\n"
    );
    sqlx::query("DELETE FROM concurrency_leases WHERE person_id=$1")
        .bind(f.alice_version.person)
        .execute(&f.db.pool)
        .await
        .unwrap();
    tokio::time::timeout(WAIT, dropped.notified())
        .await
        .unwrap();
    let text = scrape(&f, &binary, "heartbeat-fenced").await;
    assert!(text.contains("llm3_postgres_available 1\n"));
    drop(response);
    drop(send);
    f.close(vec![binary]).await;
}
