use super::database;
use llmproxy::{
    config::PostgresConfig,
    secret::{ProxyToken, TokenHasher},
    storage::{self, PersonId, Storage, TokenId, TokenRecord},
};
use std::{num::NonZeroUsize, time::Duration};
use uuid::Uuid;

pub const LEGACY: &str = "legacy";

pub struct Fixture {
    pub db: database::Database,
    pub storage: Storage,
    pub alice: ProxyToken,
    pub bob: ProxyToken,
    pub revoked: ProxyToken,
    pub disabled: ProxyToken,
    pub invalid: ProxyToken,
    pub alice_id: PersonId,
    pub bob_id: PersonId,
}

impl Fixture {
    pub async fn new() -> Self {
        let db = database::Database::new().await;
        storage::migrate(&db.config).await.unwrap();
        let storage = Storage::new(&db.config).unwrap();
        let alice_id = PersonId(Uuid::new_v4());
        let bob_id = PersonId(Uuid::new_v4());
        let disabled_id = PersonId(Uuid::new_v4());
        let alice = ProxyToken::generate().unwrap();
        let bob = ProxyToken::generate().unwrap();
        let revoked = ProxyToken::generate().unwrap();
        let disabled = ProxyToken::generate().unwrap();
        let hasher = TokenHasher::new(NonZeroUsize::MIN);
        let mut tx = storage.begin().await.unwrap();
        tx.create_person(alice_id, "Alice").await.unwrap();
        tx.create_person(bob_id, "Bob").await.unwrap();
        let mut person = tx.create_person(disabled_id, "Disabled").await.unwrap();
        person.enabled = false;
        tx.update_person(&person).await.unwrap();
        for (token, person_id) in [
            (&alice, alice_id),
            (&bob, bob_id),
            (&revoked, alice_id),
            (&disabled, disabled_id),
        ] {
            tx.create_token(&TokenRecord {
                id: TokenId(token.id()),
                person_id,
                secret_hash: hasher.hash(token).await.unwrap(),
                revoked: false,
                person_enabled: true,
            })
            .await
            .unwrap();
        }
        tx.revoke_token(TokenId(revoked.id())).await.unwrap();
        tx.commit().await.unwrap();
        Self {
            db,
            storage,
            alice,
            bob,
            revoked,
            disabled,
            invalid: ProxyToken::generate().unwrap(),
            alice_id,
            bob_id,
        }
    }

    pub async fn unavailable_config(&self) -> PostgresConfig {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        PostgresConfig {
            url: format!("postgres://postgres@127.0.0.1:{port}/unavailable"),
            namespace: self.db.config.namespace.clone(),
            max_connections: 1,
            connect_timeout: Duration::from_millis(40),
            acquire_timeout: Duration::from_millis(40),
        }
    }

    pub async fn close(self) {
        self.storage.close().await;
        self.db.close().await;
    }
}
