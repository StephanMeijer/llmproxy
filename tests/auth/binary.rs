use super::support::{Fixture, LEGACY};
use std::time::Duration;

pub async fn binary_scenario() {
    let fixture = Fixture::new().await;
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    let upstream_task = tokio::spawn(async move {
        axum::serve(upstream, axum::Router::new().fallback(|| async { "ok" }))
            .await
            .unwrap();
    });
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen = reservation.local_addr().unwrap();
    drop(reservation);
    let config = format!(
        "listen='{listen}'\nproxy_token='{LEGACY}'\n[postgres]\nurl='{}'\nnamespace='test'\nacquire_timeout='1s'\nconnect_timeout='1s'\n[credential_encryption]\nactive_key_id='test'\nfingerprint_key_file='/missing'\nkeys={{test='/missing'}}\n[claude]\n[[route]]\nname='static'\nupstream='http://{upstream_addr}'\n",
        fixture.db.config.url
    );
    std::fs::write(&fixture.db.path, config).unwrap();
    let log_path = fixture.db.path.with_extension("server.log");
    let log = std::fs::File::create(&log_path).unwrap();
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_llmproxy"))
        .arg(&fixture.db.path)
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let base = format!("http://{listen}");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client.get(format!("{base}/healthz")).send().await.is_ok() {
                break;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "proxy exited before health"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    check_authenticated_paths(&client, &fixture, &base).await;
    fixture.storage.close().await;
    fixture.db.pool.close().await;
    let database_name = reqwest::Url::parse(&fixture.db.config.url)
        .unwrap()
        .path()
        .trim_start_matches('/')
        .to_owned();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE {database_name} ALLOW_CONNECTIONS false"
    )))
    .execute(&fixture.db.admin)
    .await
    .unwrap();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1")
        .bind(&database_name)
        .execute(&fixture.db.admin)
        .await
        .unwrap();
    for (path, token, expected) in [
        ("/static/models", LEGACY, 200),
        ("/static/models", fixture.alice.expose(), 503),
        ("/claude/v1/messages", fixture.alice.expose(), 503),
        ("/claude/v1/messages", LEGACY, 403),
    ] {
        let response = client
            .get(format!("{base}{path}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected);
        println!("binary database=down path={path} expected_status={expected} pass");
    }
    let response = client
        .get(format!("{base}/static/models"))
        .bearer_auth(LEGACY)
        .header("x-api-key", fixture.alice.expose())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 401);
    println!("binary database=down conflicting_headers status=401 pass");
    child.kill().unwrap();
    child.wait().unwrap();
    scan_log(&log_path, &fixture);
    upstream_task.abort();
    fixture.close().await;
}

async fn check_authenticated_paths(client: &reqwest::Client, fixture: &Fixture, base: &str) {
    for (path, token, expected, kind) in [
        ("/static/models", LEGACY, 200, None),
        ("/static/models", fixture.alice.expose(), 200, None),
        (
            "/claude/v1/messages",
            fixture.alice.expose(),
            503,
            Some("adapter_unavailable"),
        ),
        ("/claude/v1/messages", LEGACY, 403, Some("forbidden")),
        ("/unknown", LEGACY, 404, Some("not_found")),
    ] {
        let response = client
            .get(format!("{base}{path}"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected);
        if let Some(kind) = kind {
            let body: serde_json::Value =
                serde_json::from_str(&response.text().await.unwrap()).unwrap();
            assert_eq!(body.pointer("/error/type").unwrap(), kind);
        }
        println!("binary path={path} expected_status={expected} pass");
    }
}

fn scan_log(log_path: &std::path::Path, fixture: &Fixture) {
    let log = std::fs::read_to_string(log_path).unwrap();
    for token in [
        &fixture.alice,
        &fixture.bob,
        &fixture.revoked,
        &fixture.disabled,
        &fixture.invalid,
    ] {
        assert!(!log.contains(token.expose()));
    }
    println!("binary secret_scan=pass\n{log}");
    std::fs::remove_file(log_path).unwrap();
}
