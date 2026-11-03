use super::process::{Replica, drain, first_chunk, network_bridge};
use super::*;
use tokio::time::Instant;

#[tokio::test]
async fn two_process_stream_lifecycle() {
    if std::env::var_os("LEASE_CHILD_URL").is_some() {
        process::child().await;
        return;
    }
    // Given two real processes sharing a single PostgreSQL concurrency slot.
    let (db, storage, key) = setup().await;
    let a = Replica::start(&db.config.url, key.person).await;
    let b = Replica::start(&db.config.url, key.person).await;
    let client = reqwest::Client::new();
    let held = first_chunk(&client, &a).await;
    let b_url = format!("{}/hold", b.url);
    let waiting = tokio::spawn({
        let client = client.clone();
        async move { client.get(b_url).send().await.unwrap() }
    });
    eventually_count(&db.pool, "waiters", 1).await;
    assert!(!waiting.is_finished());
    // When A holds its body beyond a renewal, B remains queued.
    let initial: f64 =
        sqlx::query_scalar("SELECT extract(epoch FROM expires_at)::float8 FROM concurrency_leases")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let renewed: bool = sqlx::query_scalar(
                "SELECT extract(epoch FROM expires_at)::float8>$1 FROM concurrency_leases",
            )
            .bind(initial)
            .fetch_one(&db.pool)
            .await
            .unwrap();
            if renewed {
                break;
            }
            assert!(!waiting.is_finished());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    eventually_count(&db.pool, "leases", 1).await;
    client
        .get(format!("{}/finish", a.url))
        .send()
        .await
        .unwrap();
    assert!(tokio::time::timeout(BOUND, drain(held)).await.unwrap());
    // Then normal completion advances B, and B's disconnect releases its slot.
    let response = tokio::time::timeout(BOUND, waiting).await.unwrap().unwrap();
    assert!(response.status().is_success());
    drop(response);
    eventually_count(&db.pool, "leases", 0).await;
    eventually_count(&db.pool, "waiters", 0).await;
    println!(
        "OBSERVED two_process lifecycle: capacity=1; renewed_after_10s=true; B_waited=true; normal_release=true; disconnect_release=true"
    );
    drop(a);
    drop(b);
    storage.close().await;
    db.close().await;
}

#[tokio::test]
async fn killed_process_recovers_only_after_database_expiry() {
    // Given A holds a stream while a different process B waits.
    let (db, storage, key) = setup().await;
    let mut a = Replica::start(&db.config.url, key.person).await;
    let b = Replica::start(&db.config.url, key.person).await;
    let client = reqwest::Client::new();
    let held = first_chunk(&client, &a).await;
    let old: Uuid = sqlx::query_scalar("SELECT id FROM concurrency_leases")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    let expiry: f64 =
        sqlx::query_scalar("SELECT extract(epoch FROM expires_at)::float8 FROM concurrency_leases")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    let waiting = tokio::spawn({
        let client = client.clone();
        let url = format!("{}/hold", b.url);
        async move { client.get(url).send().await.unwrap() }
    });
    eventually_count(&db.pool, "waiters", 1).await;
    // When A is killed without cleanup or renewal.
    a.kill();
    assert!(!tokio::time::timeout(BOUND, drain(held)).await.unwrap());
    // Then B cannot pass the still-live row and acquires after its server-time expiry.
    let started = Instant::now();
    let response = tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let live: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM concurrency_leases WHERE id=$1 AND expires_at>clock_timestamp())").bind(old).fetch_one(&db.pool).await.unwrap();
            if !live { break; }
            assert!(!waiting.is_finished());
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        waiting.await.unwrap()
    }).await.unwrap();
    let new_created: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM created_at)::float8 FROM concurrency_leases WHERE id<>$1",
    )
    .bind(old)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(new_created >= expiry);
    assert!(started.elapsed() >= Duration::from_secs(28));
    println!(
        "OBSERVED killed_owner recovery_ms={} B_created_after_authoritative_expiry=true",
        started.elapsed().as_millis()
    );
    drop(response);
    eventually_count(&db.pool, "leases", 0).await;
    drop(a);
    drop(b);
    storage.close().await;
    db.close().await;
}

#[tokio::test]
async fn database_network_partition_cancels_stream_before_confirmed_expiry() {
    // Given A reaches PostgreSQL through a controllable TCP bridge and B connects directly.
    let (db, storage, key) = setup().await;
    let (url, bridge) = network_bridge(&db.config.url).await;
    let a = Replica::start(&url, key.person).await;
    let b = Replica::start(&db.config.url, key.person).await;
    let client = reqwest::Client::new();
    let held = first_chunk(&client, &a).await;
    let old: Uuid = sqlx::query_scalar("SELECT id FROM concurrency_leases")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    let expiry: f64 =
        sqlx::query_scalar("SELECT extract(epoch FROM expires_at)::float8 FROM concurrency_leases")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    let waiting = tokio::spawn({
        let client = client.clone();
        let url = format!("{}/hold", b.url);
        async move { client.get(url).send().await.unwrap() }
    });
    eventually_count(&db.pool, "waiters", 1).await;
    // When all A's database sockets close and new connections are refused.
    bridge.abort();
    assert!(bridge.await.unwrap_err().is_cancelled());
    let started = Instant::now();
    assert!(
        !tokio::time::timeout(Duration::from_secs(30), drain(held))
            .await
            .unwrap()
    );
    // Then local streaming stops while the old row remains live; B waits for DB expiry.
    let remaining: f64 = sqlx::query_scalar("SELECT extract(epoch FROM (expires_at-clock_timestamp()))::float8 FROM concurrency_leases WHERE id=$1").bind(old).fetch_one(&db.pool).await.unwrap();
    let stopped_after = started.elapsed();
    assert!(
        remaining > 0.0,
        "stream outlived confirmed lease: {remaining}"
    );
    assert!(!waiting.is_finished());
    let response = tokio::time::timeout(BOUND, waiting).await.unwrap().unwrap();
    let new_created: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM created_at)::float8 FROM concurrency_leases WHERE id<>$1",
    )
    .bind(old)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(new_created >= expiry);
    println!(
        "OBSERVED network_partition stream_stop_ms={} remaining_db_seconds={remaining} B_created_after_authoritative_expiry=true",
        stopped_after.as_millis()
    );
    drop(response);
    eventually_count(&db.pool, "leases", 0).await;
    drop(a);
    drop(b);
    storage.close().await;
    db.close().await;
}
