use super::*;
use llmproxy::{
    config::PostgresConfig,
    storage::{ChangeListener, StorageError},
};
use std::{
    env, fs,
    net::TcpListener,
    process::{Child, Command},
};

struct Container(String);
impl Container {
    fn command(&self, action: &str) {
        assert!(
            Command::new("docker")
                .args([action, &self.0])
                .output()
                .unwrap()
                .status
                .success()
        );
    }
}
impl Drop for Container {
    fn drop(&mut self) {
        let output = Command::new("docker")
            .args(["rm", "-f", "-v", &self.0])
            .output()
            .unwrap();
        assert!(output.status.success());
    }
}

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
}

#[tokio::test]
async fn postgres_stop_restart_recovers_same_runtime_and_listener() {
    // Given an explicitly configured test database and a dedicated PostgreSQL 18 container.
    env::var("LLMPROXY_TEST_DATABASE_URL")
        .expect("set LLMPROXY_TEST_DATABASE_URL; storage integration tests never skip");
    let container = Container(format!("llmproxy-storage-{}", Uuid::new_v4().simple()));
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mapping = format!("127.0.0.1:{port}:5432");
    assert!(
        Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                &container.0,
                "-e",
                "POSTGRES_PASSWORD=test",
                "-p",
                &mapping,
                "postgres:18"
            ])
            .output()
            .unwrap()
            .status
            .success()
    );
    let output = Command::new("docker")
        .args(["port", &container.0, "5432"])
        .output()
        .unwrap();
    let address = String::from_utf8(output.stdout).unwrap();
    let config = PostgresConfig {
        url: format!("postgres://postgres:test@{}/postgres", address.trim()),
        namespace: "test".into(),
        max_connections: 2,
        connect_timeout: SHORT,
        acquire_timeout: SHORT,
    };
    let deadline = tokio::time::Instant::now() + LONG;
    loop {
        if storage::migrate(&config).await.is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "PostgreSQL did not become ready"
        );
        tokio::task::yield_now().await;
    }
    let store = Storage::new(&config).unwrap();
    let person = PersonId(Uuid::new_v4());
    let mut tx = store.begin().await.unwrap();
    tx.create_person(person, "Alice").await.unwrap();
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut listener = ChangeListener::new(store.clone());
    assert_eq!(
        listener
            .credential(person, SHORT)
            .await
            .unwrap()
            .unwrap()
            .generation,
        1
    );
    // When PostgreSQL stops while an operation owns a transaction.
    let mut tx = store.begin().await.unwrap();
    container.command("stop");
    assert!(matches!(
        tx.person(person).await,
        Err(StorageError::Unavailable)
    ));
    drop(tx);
    assert!(matches!(
        store.begin().await,
        Err(StorageError::Unavailable)
    ));
    assert!(matches!(
        listener.credential(person, SHORT).await,
        Err(StorageError::Unavailable)
    ));
    container.command("start");
    // Then the original runtime and listener recover without their restart.
    let deadline = tokio::time::Instant::now() + LONG;
    loop {
        match listener.credential(person, SHORT).await {
            Ok(Some(record)) => {
                assert_eq!(record.generation, 1);
                break;
            }
            Ok(None) => {}
            Err(error) => assert_eq!(error, StorageError::Unavailable),
        }
        assert!(tokio::time::Instant::now() < deadline, "recovery timed out");
    }
    drop(listener);
    store.close().await;
}

#[tokio::test]
async fn static_binary_starts_and_serves_while_database_is_absent() {
    // Given a configured but unreachable database and a free HTTP listener.
    env::var("LLMPROXY_TEST_DATABASE_URL")
        .expect("set LLMPROXY_TEST_DATABASE_URL; storage integration tests never skip");
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let path = env::temp_dir().join(format!("storage-static-{}.toml", Uuid::new_v4()));
    fs::write(&path,format!("listen='127.0.0.1:{port}'\nproxy_token='test'\n[postgres]\nurl='postgres://127.0.0.1:1/absent'\nnamespace='test'\n")).unwrap();
    // When the real binary starts.
    let server = Server(
        Command::new(env!("CARGO_BIN_EXE_llmproxy"))
            .arg(&path)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + LONG;
    loop {
        if let Ok(response) = client
            .get(format!("http://127.0.0.1:{port}/missing"))
            .send()
            .await
        {
            assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "static listener did not start"
        );
        tokio::task::yield_now().await;
    }
    // Then serving a static request required no database connection.
    drop(server);
    fs::remove_file(path).unwrap();
}
