use super::{
    database::Database,
    fixture::Fixture,
    provider::{reply, stream},
};
use axum::{body::Bytes, http::StatusCode};
use serde_json::json;
use std::{
    process::{Command, Output},
    time::{Duration, Instant},
};
use tokio::task::JoinSet;

struct OwnedPostgres {
    id: String,
    url: String,
}

impl OwnedPostgres {
    async fn start() -> Self {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = reservation.local_addr().unwrap().port();
        drop(reservation);
        let name = format!("llmproxy-integration-{}", uuid::Uuid::new_v4().simple());
        let output = Command::new("docker").args([
            "run", "--detach", "--name", &name, "--label", "llmproxy.test-owned=postgres-integration",
            "--env", "POSTGRES_HOST_AUTH_METHOD=trust", "--publish", &format!("127.0.0.1:{port}:5432"), "postgres:18-alpine",
        ]).output().expect("postgres_integration requires Docker and postgres:18-alpine for owned server kill/restart coverage");
        assert!(
            output.status.success(),
            "could not create owned PostgreSQL18 container"
        );
        let server = Self {
            id: String::from_utf8(output.stdout).unwrap().trim().to_owned(),
            url: format!("postgres://postgres@127.0.0.1:{port}/postgres"),
        };
        assert!(server.id.len() == 64 && server.id.bytes().all(|b| b.is_ascii_hexdigit()));
        server.ready().await;
        server
    }

    fn command(&self, operation: &[&str]) -> Output {
        Command::new("docker")
            .args(operation)
            .arg(&self.id)
            .output()
            .unwrap()
    }

    async fn ready(&self) {
        tokio::time::timeout(super::WAIT, async {
            loop {
                if let Ok(pool) = sqlx::postgres::PgPoolOptions::new()
                    .acquire_timeout(Duration::from_secs(1))
                    .connect(&self.url)
                    .await
                {
                    pool.close().await;
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("owned PostgreSQL did not become reachable");
    }

    fn kill(&self) -> Outage<'_> {
        let outage = Outage {
            server: self,
            recovered: false,
        };
        assert!(self.command(&["kill", "--signal", "KILL"]).status.success());
        outage
    }
}

impl Drop for OwnedPostgres {
    fn drop(&mut self) {
        assert!(
            self.command(&["rm", "--force", "--volumes"])
                .status
                .success(),
            "owned PostgreSQL cleanup failed"
        );
    }
}

struct Outage<'a> {
    server: &'a OwnedPostgres,
    recovered: bool,
}

impl Outage<'_> {
    async fn restart(mut self) {
        assert!(self.server.command(&["start"]).status.success());
        self.server.ready().await;
        self.recovered = true;
    }
}

impl Drop for Outage<'_> {
    fn drop(&mut self) {
        if !self.recovered {
            assert!(self.server.command(&["start"]).status.success());
            let deadline = Instant::now() + super::WAIT;
            loop {
                let ready = Command::new("docker")
                    .args(["exec", &self.server.id, "pg_isready", "-U", "postgres"])
                    .output()
                    .unwrap();
                if ready.status.success() {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "PostgreSQL panic cleanup restart timed out"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

#[tokio::test]
async fn killed_postgres_restarts_durably_and_both_live_replicas_recover() {
    // Given a test-owned PostgreSQL18 server, an active stream and a committed waiter.
    let server = OwnedPostgres::start().await;
    let database = Database::at_url(server.url.clone()).await;
    let mut f = Fixture::with_database("postgres-server-kill", database).await;
    let a = f.binary(super::LIMIT).await;
    let b = f.binary(super::LIMIT).await;
    let proxy_pids = [a.pid(), b.pid()];
    let postgres_started: String = sqlx::query_scalar("SELECT pg_postmaster_start_time()::text")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    let mut clients = JoinSet::new();
    clients.spawn(f.request(&a, 0).send());
    let send = stream(f.hit().await);
    send.send(Bytes::from_static(b"data: active\n\n"))
        .await
        .unwrap();
    let mut held = clients.join_next().await.unwrap().unwrap().unwrap();
    assert!(held.chunk().await.unwrap().is_some());
    clients.spawn(f.request(&b, 0).send());
    f.db.count("concurrency_leases", 1).await;
    f.db.count("fifo_waiters", 1).await;
    let remaining: f64 = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (expires_at-clock_timestamp()))::float8 FROM concurrency_leases",
    )
    .fetch_one(&f.db.pool)
    .await
    .unwrap();
    let started = Instant::now();
    // When the owned server receives SIGKILL after SQL confirms stream and waiter state.
    let outage = server.kill();
    for replica in [&a, &b] {
        f.static_ok(replica).await;
        assert_eq!(
            f.request(replica, 1).send().await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
    assert_eq!(
        clients
            .join_next()
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(f.provider.hits.try_recv().is_err());
    tokio::time::timeout(super::WAIT, send.closed())
        .await
        .unwrap();
    let cancelled_after = started.elapsed().as_secs_f64();
    assert!(
        cancelled_after < remaining,
        "provider survived confirmed lease expiry"
    );
    outage.restart().await;
    // Then the same durable database and both unchanged proxy processes recover and reclaim state.
    let postgres_restarted: String = sqlx::query_scalar("SELECT pg_postmaster_start_time()::text")
        .fetch_one(&f.db.pool)
        .await
        .unwrap();
    assert_ne!(postgres_started, postgres_restarted);
    assert_eq!(proxy_pids, [a.pid(), b.pid()]);
    let credentials: i64 =
        sqlx::query_scalar("SELECT count(*) FROM provider_credentials WHERE state='active'")
            .fetch_one(&f.db.pool)
            .await
            .unwrap();
    assert_eq!(credentials, 2);
    for replica in [&a, &b] {
        clients.spawn(f.request(replica, 0).send());
        reply(
            f.hit().await,
            StatusCode::OK,
            "recovered-without-proxy-restart",
        );
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
            "recovered-without-proxy-restart"
        );
        f.db.count("concurrency_leases", 0).await;
        f.db.count("fifo_waiters", 0).await;
    }
    assert_eq!(f.order, ["alice", "alice", "alice"]);
    f.record(json!({"fault":"docker kill --signal KILL on test-created container ID","postgres_restart":"same container and durable volume","postgres_started":postgres_started,"postgres_restarted":postgres_restarted,"proxy_pids_before":proxy_pids,"proxy_pids_after":[a.pid(),b.pid()],"proxy_restarts":0,"before_kill":{"leases":1,"waiters":1},"during_outage":{"static_statuses":[200,200],"personal_statuses":[503,503],"queued_status":503,"provider_hits":0},"confirmed_expiry_seconds":remaining,"provider_cancel_seconds":cancelled_after,"active_credentials_after_restart":credentials,"recovered_statuses":[200,200],"cleanup":"database Drop then owned container rm --force --volumes"})).await;
}
