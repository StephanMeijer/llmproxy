use super::*;
use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use llmproxy::limit::shared::{AdmissionStore, PendingAdmission, SharedRequest};
use std::sync::{Arc, Mutex};

async fn hit(State(hits): State<Arc<Mutex<Vec<String>>>>, Path(id): Path<String>) -> StatusCode {
    let mut hits = hits.lock().unwrap();
    hits.push(id.clone());
    if id == "retry" && hits.iter().filter(|id| id.as_str() == "retry").count() < 3 {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

#[tokio::test]
async fn manual_alternating_replicas_preserve_committed_fifo_at_http_upstream() {
    let f = Fixture::new().await;
    let d = definition("rpm=10\ntpm=10\nmax_inflight=1");
    let hits = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/{id}", get(hit))
        .with_state(hits.clone());
    let mut server = tokio::task::JoinSet::new();
    server.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let initial = f
        .replica(false, LONG)
        .acquire(charge(&d, f.alice, 1))
        .await
        .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    let mut committed = Vec::new();
    for n in 1..=3 {
        let store = f.replica(n % 2 == 1, LONG);
        let definition = d.clone();
        let alice = f.alice;
        let url = url.clone();
        tasks.spawn(async move {
            let guard = store.acquire(charge(&definition, alice, 1)).await.unwrap();
            let response = reqwest::Client::new()
                .get(format!("{url}/alice-{n}"))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            drop(guard);
        });
        f.waiters(n).await;
        let row: (uuid::Uuid,i64) = sqlx::query_as("SELECT id,enqueue_order FROM fifo_waiters WHERE person_id=$1 ORDER BY enqueue_order DESC LIMIT 1").bind(f.alice).fetch_one(&f.db.pool).await.unwrap();
        committed.push((format!("alice-{n}"), row));
    }
    let bob = f
        .replica(true, LONG)
        .acquire(charge(&d, f.bob, 1))
        .await
        .unwrap();
    assert_eq!(
        reqwest::get(format!("{url}/bob")).await.unwrap().status(),
        StatusCode::OK
    );
    drop(bob);
    assert_eq!(*hits.lock().unwrap(), vec!["bob"]);
    drop(initial);
    while let Some(result) = tokio::time::timeout(LONG, tasks.join_next()).await.unwrap() {
        result.unwrap();
    }
    assert_eq!(
        *hits.lock().unwrap(),
        vec!["bob", "alice-1", "alice-2", "alice-3"]
    );
    assert_eq!(
        committed
            .iter()
            .map(|(_, (_, order))| *order)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    println!(
        "manual FIFO committed={committed:?} HTTP hits={:?}",
        hits.lock().unwrap()
    );
    f.waiters(0).await;
    tokio::time::timeout(LONG, async {
        loop {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM concurrency_leases")
                .fetch_one(&f.db.pool)
                .await
                .unwrap();
            if count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    server.shutdown().await;
    f.close().await;
}

#[tokio::test]
async fn manual_http_retries_hold_one_committed_admission() {
    let f = Fixture::new().await;
    let ds = definitions("[[limit]]\nname='budget'\nprefixes=['/claude/']\nrpm=2");
    let hits = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/retry", listener.local_addr().unwrap());
    let app = Router::new()
        .route("/{id}", get(hit))
        .with_state(hits.clone());
    let mut server = tokio::task::JoinSet::new();
    server.spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let admitted = PendingAdmission::new(
        &ds,
        SharedRequest {
            person: f.alice.0,
            path: "/claude/v1/messages",
            model: None,
            body_bytes: 4,
        },
    )
    .acquire(&f.replica(false, LONG))
    .await
    .unwrap();
    let client = reqwest::Client::new();
    for status in [
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::OK,
    ] {
        assert_eq!(client.get(&url).send().await.unwrap().status(), status);
    }
    drop(admitted);
    let row:(i64,i64) = sqlx::query_as("SELECT next_enqueue,available_units FROM limit_definitions JOIN rate_state ON limit_definitions.name=rate_state.limit_name").fetch_one(&f.db.pool).await.unwrap();
    assert_eq!(row, (1, 60_000_000));
    println!(
        "manual retry upstream statuses=503,503,200 hits={} enqueue_count={} units={}",
        hits.lock().unwrap().len(),
        row.0,
        row.1
    );
    server.shutdown().await;
    f.close().await;
}
