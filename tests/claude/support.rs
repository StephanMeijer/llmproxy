use super::database;
use axum::{
    body::{Body, Bytes, to_bytes},
    extract::Request,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use llmproxy::{
    secret::{KeyRing, ProxyToken, SecretBytes, TokenHasher},
    storage::{self, CredentialVersion, Credentials, PersonId, Storage, TokenId, TokenRecord},
};
use std::{
    fs,
    num::NonZeroUsize,
    path::PathBuf,
    process::{Child, Command},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use uuid::Uuid;

pub const WAIT: Duration = Duration::from_secs(20);
pub const BODY: &str = "{ \"model\": \"claude-test\", \"messages\": [{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"t1\",\"content\":\"ok\"}]}], \"tools\": [{\"name\":\"weather\",\"input_schema\":{\"type\":\"object\"}}], \"max_tokens\": 25 }\n";
pub const ALICE_KEY: &[u8] = b"fake-claude-alice-task13";
pub const BOB_KEY: &[u8] = b"fake-claude-bob-task13";

pub struct Hit {
    pub path: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub reply: oneshot::Sender<Response>,
}

pub struct Binary {
    pub base: String,
    pub log: PathBuf,
    child: Child,
    config: PathBuf,
}
impl Drop for Binary {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
            self.child.wait().unwrap();
        }
        fs::remove_file(&self.config).unwrap();
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
    pub hits: mpsc::Receiver<Hit>,
    upstream: String,
    key: PathBuf,
    pepper: PathBuf,
    servers: JoinSet<()>,
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
            tx.create_token(&TokenRecord {
                id: TokenId(token.id()),
                person_id: id,
                secret_hash: hasher.hash(token).await.unwrap(),
                revoked: false,
                person_enabled: true,
            })
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
        let (send, hits) = mpsc::channel(16);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("http://{}/native/", listener.local_addr().unwrap());
        let mut servers = JoinSet::new();
        servers.spawn(async move {
            let app = axum::Router::new().fallback(move |req: Request| {
                let send = send.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let (reply, response) = oneshot::channel();
                    send.send(Hit {
                        path: parts.uri.to_string(),
                        headers: parts.headers,
                        body: to_bytes(body, 1024 * 1024).await.unwrap(),
                        reply,
                    })
                    .await
                    .unwrap();
                    response
                        .await
                        .unwrap_or_else(|_| Response::new(Body::empty()))
                }
            });
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            db,
            storage,
            credentials,
            alice,
            bob,
            alice_version,
            bob_version,
            hits,
            upstream,
            key,
            pepper,
            servers,
        }
    }

    pub async fn binary(&self, limits: &str, settings: &str) -> Binary {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let path = self
            .db
            .path
            .with_extension(format!("{}.toml", Uuid::new_v4()));
        let log = path.with_extension("log");
        fs::write(&path, format!("listen='{addr}'\nproxy_token='legacy'\nmax_wait='15s'\n{settings}\n[postgres]\nurl='{}'\nnamespace='test'\nacquire_timeout='1s'\nconnect_timeout='1s'\n[credential_encryption]\nactive_key_id='test'\nfingerprint_key_file='{}'\nkeys={{test='{}'}}\n[claude]\nupstream='{}'\n[[route]]\nname='static'\nupstream='{}'\n{limits}", self.db.config.url, self.pepper.display(), self.key.display(), self.upstream, self.upstream)).unwrap();
        let output = fs::File::create(&log).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_llmproxy"))
            .arg(&path)
            .stdout(output.try_clone().unwrap())
            .stderr(output)
            .spawn()
            .unwrap();
        let binary = Binary {
            base: format!("http://{addr}"),
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
        tokio::time::timeout(WAIT, self.hits.recv())
            .await
            .unwrap()
            .unwrap()
    }

    pub async fn stop_upstream(&mut self) {
        self.servers.shutdown().await;
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
        self.servers.shutdown().await;
        self.storage.close().await;
        fs::remove_file(self.key).unwrap();
        fs::remove_file(self.pepper).unwrap();
        self.db.close().await;
    }
}

pub fn reply(hit: Hit, status: StatusCode, body: &'static str) {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert("request-id", "provider-request".parse().unwrap());
    assert!(hit.reply.send(response).is_ok());
}

pub fn artifact(name: &str, value: &str) {
    if let Ok(dir) = std::env::var("LLMPROXY_CLAUDE_EVIDENCE") {
        fs::write(PathBuf::from(dir).join(name), value).unwrap();
    }
}

pub struct Dropped(pub Arc<tokio::sync::Notify>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

pub fn stream(hit: Hit) -> (mpsc::Sender<Bytes>, Arc<tokio::sync::Notify>) {
    let (send, recv) = mpsc::channel(1);
    let dropped = Arc::new(tokio::sync::Notify::new());
    let guard = Dropped(Arc::clone(&dropped));
    let body = futures_util::stream::unfold((recv, guard), |(mut recv, guard)| async move {
        recv.recv()
            .await
            .map(|bytes| (Ok::<_, std::io::Error>(bytes), (recv, guard)))
    });
    let mut response = Response::new(Body::from_stream(body));
    response
        .headers_mut()
        .insert("content-type", "text/event-stream".parse().unwrap());
    assert!(hit.reply.send(response).is_ok());
    (send, dropped)
}
