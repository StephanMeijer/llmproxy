use super::database;
#[path = "../common/evidence.rs"]
mod evidence;
#[path = "../common/fake_provider.rs"]
mod provider;
#[path = "../common/shutdown.rs"]
mod shutdown;
use axum::http::StatusCode;
pub use evidence::artifact;
use llmproxy::{
    secret::{KeyRing, ProxyToken, SecretBytes, TokenHasher},
    storage::{self, CredentialVersion, Credentials, PersonId, Storage, TokenId},
};
pub use provider::{Hit, reply, stream};
use std::{
    fs,
    num::NonZeroUsize,
    path::PathBuf,
    process::{Child, Command},
    time::Duration,
};
use tokio::net::TcpListener;
use uuid::Uuid;

pub const WAIT: Duration = Duration::from_secs(20);
pub const BODY: &str = "{ \"model\": \"claude-test\", \"messages\": [{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"t1\",\"content\":\"ok\"}]}], \"tools\": [{\"name\":\"weather\",\"input_schema\":{\"type\":\"object\"}}], \"max_tokens\": 25 }\n";
pub const ALICE_KEY: &[u8] = b"fake-claude-alice-task13";
pub const BOB_KEY: &[u8] = b"fake-claude-bob-task13";

pub struct Binary {
    pub base: String,
    pub metrics: String,
    pub log: PathBuf,
    child: Child,
    config: PathBuf,
}
impl Drop for Binary {
    fn drop(&mut self) {
        shutdown::stop(&mut self.child);
        let removed = fs::remove_file(&self.config);
        if !std::thread::panicking() {
            removed.unwrap();
        }
    }
}

pub struct Fixture {
    pub db: database::Database,
    pub storage: Storage,
    pub credentials: Credentials,
    pub alice: ProxyToken,
    pub bob: ProxyToken,
    pub alice_version: CredentialVersion,
    pub bob_version: CredentialVersion,
    pub provider: provider::Provider,
    upstream: String,
    key: PathBuf,
    pepper: PathBuf,
}
impl Fixture {
    pub async fn new() -> Self {
        let db = database::Database::new().await;
        storage::migrate(&db.config).await.unwrap();
        let storage = Storage::new(&db.config).unwrap();
        let alice = ProxyToken::generate().unwrap();
        let bob = ProxyToken::generate().unwrap();
        let hasher = TokenHasher::new(NonZeroUsize::MIN);
        let mut tx = storage.begin().await.unwrap();
        let alice_id = PersonId(Uuid::new_v4());
        let bob_id = PersonId(Uuid::new_v4());
        for (id, label, token) in [(alice_id, "Alice", &alice), (bob_id, "Bob", &bob)] {
            tx.create_person(id, label).await.unwrap();
            tx.create_token(TokenId(token.id()), id, &hasher.hash(token).await.unwrap())
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        let keys = KeyRing::new(
            "test",
            std::collections::BTreeMap::from([("test".into(), SecretBytes::new(vec![1; 32]))]),
        )
        .unwrap();
        let credentials = Credentials::new(storage.clone(), keys, SecretBytes::new(vec![2; 32]));
        let alice_version = credentials
            .install(alice_id, &SecretBytes::new(ALICE_KEY.to_vec()))
            .await
            .unwrap();
        let bob_version = credentials
            .install(bob_id, &SecretBytes::new(BOB_KEY.to_vec()))
            .await
            .unwrap();
        let key = db.path.with_extension("key");
        let pepper = db.path.with_extension("pepper");
        fs::write(&key, [1; 32]).unwrap();
        fs::write(&pepper, [2; 32]).unwrap();
        let provider = provider::Provider::new(false).await;
        let upstream = format!("{}/native/", provider.url);
        Self {
            db,
            storage,
            credentials,
            alice,
            bob,
            alice_version,
            bob_version,
            provider,
            upstream,
            key,
            pepper,
        }
    }

    pub async fn binary(&self, limits: &str, settings: &str) -> Binary {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let metrics_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let metrics_addr = metrics_listener.local_addr().unwrap();
        drop(metrics_listener);
        let settings = format!("metrics_listen='{metrics_addr}'\n{settings}");
        let path = self
            .db
            .path
            .with_extension(format!("{}.toml", Uuid::new_v4()));
        let log = path.with_extension("log");
        fs::write(&path, format!("listen='{addr}'\nproxy_token='legacy'\nmax_wait='15s'\n{settings}\n[postgres]\nurl='{}'\nnamespace='test'\nmax_connections={}\nacquire_timeout='1s'\nconnect_timeout='1s'\n[credential_encryption]\nactive_key_id='test'\nfingerprint_key_file='{}'\nkeys={{test='{}'}}\n[claude]\nupstream='{}'\n[[route]]\nname='static'\nupstream='{}'\n{limits}", self.db.config.url, self.db.config.max_connections, self.pepper.display(), self.key.display(), self.upstream, self.upstream)).unwrap();
        let output = fs::File::create(&log).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
            .arg(&path)
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap();
        let binary = Binary {
            base: format!("http://{addr}"),
            metrics: format!("http://{metrics_addr}/metrics"),
            child,
            config: path,
            log,
        };
        tokio::time::timeout(WAIT, async {
            let client = reqwest::Client::new();
            loop {
                if client
                    .get(format!("{}/healthz", binary.base))
                    .send()
                    .await
                    .is_ok_and(|r| r.status() == StatusCode::OK)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        binary
    }

    pub async fn hit(&mut self) -> Hit {
        self.provider.hit().await
    }

    pub async fn stop_upstream(&mut self) {
        self.provider.stop().await;
    }

    pub fn request(&self, binary: &Binary, bob: bool) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .post(format!("{}/claude/v1/messages", binary.base))
            .bearer_auth(if bob {
                self.bob.expose()
            } else {
                self.alice.expose()
            })
            .header("content-type", "application/json")
            .header("anthropic-version", "2023-06-01")
            .timeout(WAIT)
            .body(BODY)
    }

    pub async fn state_count(&self, table: &str, count: i64) {
        let query = format!("SELECT count(*) FROM {table}");
        tokio::time::timeout(WAIT, async {
            loop {
                let actual: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
                    .fetch_one(&self.db.pool)
                    .await
                    .unwrap();
                if actual == count {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    pub async fn close(mut self, binaries: Vec<Binary>) {
        for binary in binaries {
            let log = binary.log.clone();
            drop(binary);
            let bytes = fs::read(&log).unwrap();
            for secret in [
                ALICE_KEY,
                BOB_KEY,
                self.alice.expose().as_bytes(),
                self.bob.expose().as_bytes(),
            ] {
                assert!(
                    !bytes.windows(secret.len()).any(|window| window == secret),
                    "secret leaked to binary log"
                );
            }
            fs::remove_file(log).unwrap();
        }
        self.provider.stop().await;
        self.storage.close().await;
        fs::remove_file(self.key).unwrap();
        fs::remove_file(self.pepper).unwrap();
        self.db.close().await;
    }
}
