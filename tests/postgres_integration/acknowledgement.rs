use super::{
    fixture::{Fixture, REPLACEMENT},
    process,
    provider::reply,
};
use axum::http::StatusCode;
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinSet,
};

struct Bridge {
    url: String,
    committed: Arc<Notify>,
    tasks: JoinSet<()>,
}

impl Bridge {
    async fn new(database_url: &str) -> Self {
        let mut url = reqwest::Url::parse(database_url).unwrap();
        let target = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(5432));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        url.set_port(Some(listener.local_addr().unwrap().port()))
            .unwrap();
        url.query_pairs_mut().append_pair("sslmode", "disable");
        let committed = Arc::new(Notify::new());
        let observed = Arc::clone(&committed);
        let armed = Arc::new(AtomicBool::new(true));
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (incoming, _) = accepted.unwrap();
                        let outgoing = TcpStream::connect(&target).await.unwrap();
                        let observed = Arc::clone(&observed);
                        let armed = Arc::clone(&armed);
                        connections.spawn(async move { forward(incoming, outgoing, (observed, armed)).await; });
                    }
                    Some(result) = connections.join_next() => result.unwrap(),
                }
            }
        });
        Self {
            url: url.to_string(),
            committed,
            tasks,
        }
    }
}

async fn forward(incoming: TcpStream, outgoing: TcpStream, fault: (Arc<Notify>, Arc<AtomicBool>)) {
    let (mut client_read, mut client_write) = incoming.into_split();
    let (mut server_read, mut server_write) = outgoing.into_split();
    let client_to_server = tokio::io::copy(&mut client_read, &mut server_write);
    let server_to_client = async {
        loop {
            let kind = server_read.read_u8().await?;
            let length = server_read.read_u32().await?;
            let body_length = usize::try_from(length.checked_sub(4).unwrap()).unwrap();
            assert!(
                body_length <= 1024 * 1024,
                "unexpected PostgreSQL frame size"
            );
            let mut body = vec![0; body_length];
            server_read.read_exact(&mut body).await?;
            if kind == b'C' && body == b"COMMIT\0" && fault.1.swap(false, Ordering::SeqCst) {
                fault.0.notify_one();
                return Ok::<(), std::io::Error>(());
            }
            client_write.write_u8(kind).await?;
            client_write.write_u32(length).await?;
            client_write.write_all(&body).await?;
        }
    };
    tokio::select! {
        result = client_to_server => { if let Err(error) = result { assert!(matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::UnexpectedEof)); } }
        result = server_to_client => { if let Err(error) = result { assert!(matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::UnexpectedEof)); } }
    }
}

#[tokio::test]
async fn lost_commit_acknowledgement_preserves_committed_credential_and_retry_safety() {
    // Given two real replicas and a bridge that drops PostgreSQL CommandComplete(COMMIT).
    let mut f = Fixture::new("commit-ack-loss").await;
    let a = f.binary("").await;
    let b = f.binary("").await;
    let mut bridge = Bridge::new(&f.db.url).await;
    let config = f.db.directory.join("fault-operator.toml");
    let text = std::fs::read_to_string(&f.config)
        .unwrap()
        .replace(&f.db.url, &bridge.url);
    std::fs::write(&config, text).unwrap();
    let person = f.people.first().unwrap().clone();
    // When the server commits replacement but the operator receives no acknowledgement.
    let output = tokio::task::spawn_blocking(move || {
        process::cli(
            &config,
            &["auth", "claude", "set", &person, "--credential-stdin"],
            Some(REPLACEMENT),
        )
    });
    tokio::time::timeout(super::WAIT, bridge.committed.notified())
        .await
        .unwrap();
    let result = output.await.unwrap();
    assert!(!result.status.success());
    for bytes in [&result.stdout, &result.stderr] {
        assert!(!String::from_utf8_lossy(bytes).contains(REPLACEMENT));
    }
    let generation: i64 =
        sqlx::query_scalar("SELECT generation FROM provider_credentials WHERE person_id=$1")
            .bind(uuid::Uuid::parse_str(f.people.first().unwrap()).unwrap())
            .fetch_one(&f.db.pool)
            .await
            .unwrap();
    assert_eq!(generation, 2);
    // Then both replicas observe the committed replacement and re-encryption retries are idempotent.
    let mut clients = JoinSet::new();
    for replica in [&a, &b] {
        clients.spawn(f.request(replica, 0).send());
        reply(f.hit().await, StatusCode::OK, "committed");
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
            "committed"
        );
    }
    let rotated = process::ok(
        &f.config,
        &["auth", "credentials", "reencrypt", "--to", "new"],
        None,
    );
    assert_eq!(rotated.get("rewritten").unwrap(), 2);
    let again = process::ok(
        &f.config,
        &["auth", "credentials", "reencrypt", "--to", "new"],
        None,
    );
    assert_eq!(again.get("rewritten").unwrap(), 0);
    bridge.tasks.shutdown().await;
    f.record(json!({"suppressed_frame":"CommandComplete(COMMIT)","operator_exit_success":false,"committed_generation":generation,"replica_statuses":[200,200],"second_rotation_rewritten":0})).await;
}
