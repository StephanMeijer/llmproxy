use super::{
    database::Database,
    process::{self, Replica},
    provider::{Hit, Provider},
};
use serde_json::{Value, json};
use std::{fs, path::PathBuf};

pub const ALICE: &str = "fake-integration-alice-canary";
pub const BOB: &str = "fake-integration-bob-canary";
pub const REPLACEMENT: &str = "fake-integration-replacement-canary";

pub struct Fixture {
    pub provider: Provider,
    pub db: Database,
    pub config: PathBuf,
    pub people: Vec<String>,
    pub tokens: Vec<String>,
    pub order: Vec<String>,
    scenario: &'static str,
}

impl Fixture {
    pub async fn new(scenario: &'static str) -> Self {
        let db = Database::new().await;
        Self::with_database(scenario, db).await
    }

    pub async fn with_database(scenario: &'static str, db: Database) -> Self {
        let provider = Provider::new().await;
        for (name, byte) in [("old", 1), ("new", 2), ("fingerprint", 3)] {
            fs::write(db.directory.join(name), [byte; 32]).unwrap();
        }
        let config = db.directory.join("operator.toml");
        let mut fixture = Self {
            provider,
            db,
            config,
            people: Vec::new(),
            tokens: Vec::new(),
            order: Vec::new(),
            scenario,
        };
        fs::write(&fixture.config, fixture.config_text("127.0.0.1:0", "")).unwrap();
        let migration = process::cli(&fixture.config, &["db", "migrate"], None);
        assert!(
            migration.status.success(),
            "migration failed (output withheld)"
        );
        for (label, key) in [("Alice", ALICE), ("Bob", BOB)] {
            let person = process::ok(&fixture.config, &["auth", "person", "create", label], None);
            let id = person
                .get("person_id")
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned();
            let issued = process::ok(&fixture.config, &["auth", "token", "issue", &id], None);
            fixture
                .tokens
                .push(issued.get("token").unwrap().as_str().unwrap().to_owned());
            let installed = process::ok(
                &fixture.config,
                &["auth", "claude", "set", &id, "--credential-stdin"],
                Some(key),
            );
            assert_eq!(installed.get("generation").unwrap(), 1);
            fixture.people.push(id);
        }
        fixture
    }

    fn config_text(&self, listen: &str, limits: &str) -> String {
        format!(
            "listen='{listen}'\nproxy_token='legacy'\nmax_wait='40s'\n[postgres]\nurl='{}'\nnamespace='{}'\nacquire_timeout='1s'\nconnect_timeout='1s'\n[credential_encryption]\nactive_key_id='old'\nfingerprint_key_file='{}'\nkeys={{old='{}',new='{}'}}\n[claude]\nupstream='{}'\n[[route]]\nname='static'\nupstream='{}'\n{limits}",
            self.db.url,
            self.db.name,
            self.db.directory.join("fingerprint").display(),
            self.db.directory.join("old").display(),
            self.db.directory.join("new").display(),
            self.provider.url,
            self.provider.url
        )
    }

    pub async fn binary(&self, limits: &str) -> Replica {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let path = self
            .db
            .directory
            .join(format!("{}.toml", uuid::Uuid::new_v4()));
        fs::write(&path, self.config_text(&address.to_string(), limits)).unwrap();
        Replica::start(&path).await
    }

    pub fn request(&self, replica: &Replica, person: usize) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .post(format!("{}/claude/v1/messages", replica.base))
            .bearer_auth(self.tokens.get(person).unwrap())
            .header("content-type", "application/json")
            .header("anthropic-version", "2023-06-01")
            .body(super::BODY)
            .timeout(super::WAIT)
    }

    pub async fn hit(&mut self) -> Hit {
        let hit = self.provider.hit().await;
        let person = if hit.key == ALICE.as_bytes() {
            "alice"
        } else if hit.key == BOB.as_bytes() {
            "bob"
        } else {
            assert!(
                hit.key == REPLACEMENT.as_bytes(),
                "unknown provider credential"
            );
            "replacement"
        };
        let body: Value = serde_json::from_slice(&hit.body).unwrap();
        let marker = body.get("marker").and_then(Value::as_str).unwrap_or(person);
        self.order.push(marker.to_owned());
        hit
    }

    pub async fn static_ok(&self, replica: &Replica) {
        let response = reqwest::Client::new()
            .get(format!("{}/static/ping", replica.base))
            .bearer_auth("legacy")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(response.text().await.unwrap(), "static-ok");
    }

    pub async fn record(&self, observed: Value) {
        let rows: Vec<String> =
            sqlx::query_scalar("SELECT row_to_json(c)::text FROM provider_credentials c")
                .fetch_all(&self.db.pool)
                .await
                .unwrap();
        for row in &rows {
            for secret in [ALICE, BOB, REPLACEMENT]
                .into_iter()
                .chain(self.tokens.iter().map(String::as_str))
            {
                assert!(!row.contains(secret), "secret persisted in plaintext");
            }
        }
        let credentials: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('state',state,'generation',generation,'key_id',key_id) FROM provider_credentials ORDER BY person_id")
            .fetch_all(&self.db.pool).await.unwrap();
        let waiters: i64 = sqlx::query_scalar("SELECT count(*) FROM fifo_waiters")
            .fetch_one(&self.db.pool)
            .await
            .unwrap();
        let leases: i64 = sqlx::query_scalar("SELECT count(*) FROM concurrency_leases")
            .fetch_one(&self.db.pool)
            .await
            .unwrap();
        let balances: Vec<Value> = sqlx::query_scalar("SELECT jsonb_build_object('kind',kind,'available_units',available_units) FROM rate_state ORDER BY kind")
            .fetch_all(&self.db.pool).await.unwrap();
        let artifact = json!({"scenario":self.scenario,"provider_hit_order":self.order,"provider_hit_count":self.order.len(),"observed":observed,"credentials":credentials,"waiters":waiters,"leases":leases,"rate_balances":balances,"plaintext_scan_passed":true});
        let directory = std::env::var_os("LLMPROXY_INTEGRATION_EVIDENCE").map_or_else(
            || PathBuf::from(".omo/evidence/postgres-integration"),
            PathBuf::from,
        );
        fs::create_dir_all(&directory).unwrap();
        fs::write(
            directory.join(format!("{}.json", self.scenario)),
            serde_json::to_vec_pretty(&artifact).unwrap(),
        )
        .unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for entry in fs::read_dir(&self.db.directory).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "log") {
                let log = fs::read_to_string(path).unwrap();
                for secret in [ALICE, BOB, REPLACEMENT]
                    .into_iter()
                    .chain(self.tokens.iter().map(String::as_str))
                {
                    assert!(!log.contains(secret), "secret leaked into process log");
                }
            }
        }
    }
}
