use super::*;
#[path = "../common/shutdown.rs"]
mod shutdown;
use axum::{Router, body::Body, extract::State, response::Response, routing::get};
use futures_util::StreamExt;
use llmproxy::limit::shared::{AdmissionStore, SharedCharge, SharedRateStore};
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command},
    sync::Arc,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::{JoinHandle, JoinSet},
};

pub struct Replica {
    child: Child,
    pub url: String,
    directory: PathBuf,
}

impl Replica {
    pub async fn start(db_url: &str, person: PersonId) -> Self {
        let directory = std::env::temp_dir().join(format!("lease-process-{}", Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let log = fs::File::create(directory.join("process.log")).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "scenarios::two_process_stream_lifecycle",
                "--nocapture",
            ])
            .env("LEASE_CHILD_URL", db_url)
            .env("LEASE_CHILD_PERSON", person.0.to_string())
            .env("LEASE_CHILD_DIRECTORY", &directory)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        let mut replica = Self {
            child,
            url: String::new(),
            directory,
        };
        replica.url = tokio::time::timeout(BOUND, async {
            loop {
                if let Ok(url) = fs::read_to_string(replica.directory.join("ready")) {
                    return url;
                }
                assert!(
                    replica.child.try_wait().unwrap().is_none(),
                    "child failed: {}",
                    fs::read_to_string(replica.directory.join("process.log")).unwrap()
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        replica
    }

    pub fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        shutdown::stop(&mut self.child);
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[derive(Clone)]
struct ChildState {
    store: SharedRateStore,
    person: PersonId,
    finish: Arc<Notify>,
}

pub async fn child() {
    let url = std::env::var("LEASE_CHILD_URL").unwrap();
    let person = PersonId(Uuid::parse_str(&std::env::var("LEASE_CHILD_PERSON").unwrap()).unwrap());
    let config = llmproxy::config::PostgresConfig {
        url,
        namespace: "test".into(),
        max_connections: 4,
        connect_timeout: Duration::from_secs(1),
        acquire_timeout: Duration::from_secs(1),
    };
    let storage = Storage::new(&config).unwrap();
    let state = ChildState {
        store: SharedRateStore::new(storage, Duration::from_mins(1)),
        person,
        finish: Arc::new(Notify::new()),
    };
    let app = Router::new()
        .route("/hold", get(hold))
        .route("/finish", get(finish))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let directory = PathBuf::from(std::env::var("LEASE_CHILD_DIRECTORY").unwrap());
    fs::write(
        directory.join("ready"),
        format!("http://{}", listener.local_addr().unwrap()),
    )
    .unwrap();
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            terminate.recv().await;
        })
        .await
        .unwrap();
}

async fn hold(State(state): State<ChildState>) -> Response {
    let definition = definition();
    let guard = state
        .store
        .acquire(SharedCharge {
            definition: &definition,
            person: state.person.0,
            tokens: 0,
        })
        .await
        .unwrap()
        .unwrap();
    let stream = futures_util::stream::unfold(state.finish, |finish| async move {
        tokio::select! {
            () = finish.notified() => None,
            () = tokio::time::sleep(Duration::from_millis(40)) => Some((Ok::<_, std::io::Error>("data: held\n\n"), finish)),
        }
    });
    Response::new(Body::from_stream(guard.stream(stream)))
}

async fn finish(State(state): State<ChildState>) {
    state.finish.notify_waiters();
}

pub async fn first_chunk(client: &reqwest::Client, replica: &Replica) -> reqwest::Response {
    let mut response =
        tokio::time::timeout(BOUND, client.get(format!("{}/hold", replica.url)).send())
            .await
            .unwrap()
            .unwrap();
    assert!(response.status().is_success());
    assert!(response.chunk().await.unwrap().is_some());
    response
}

pub async fn drain(response: reqwest::Response) -> bool {
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        if chunk.is_err() {
            return false;
        }
    }
    true
}

pub async fn network_bridge(db_url: &str) -> (String, JoinHandle<()>) {
    let mut url = reqwest::Url::parse(db_url).unwrap();
    let target = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(5432));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    url.set_port(Some(listener.local_addr().unwrap().port()))
        .unwrap();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut incoming, _) = accepted.unwrap();
                    let mut outgoing = TcpStream::connect(&target).await.unwrap();
                    connections.spawn(async move {
                        match tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await {
                            Ok(_) | Err(_) => {}
                        }
                    });
                }
                Some(result) = connections.join_next() => result.unwrap(),
            }
        }
    });
    (url.to_string(), task)
}
