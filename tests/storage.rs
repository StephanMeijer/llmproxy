#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "migrations/support.rs"]
mod database;
#[path = "storage/outage.rs"]
mod outage;
#[path = "storage/repositories.rs"]
mod repositories;
#[path = "storage/runtime.rs"]
mod runtime;
#[path = "storage/shared_listener.rs"]
mod shared_listener;

use llmproxy::{
    config::Config,
    limit::shared::{CompiledLimits, SharedDefinition},
    secret::CredentialEnvelope,
    storage::{self, CredentialRecord, CredentialState, PersonId, Storage},
};
use std::time::Duration;
use uuid::Uuid;

const SHORT: Duration = Duration::from_millis(150);
const LONG: Duration = Duration::from_secs(5);

async fn setup() -> (database::Database, Storage, PersonId) {
    let db = database::Database::new().await;
    storage::migrate(&db.config).await.unwrap();
    let store = Storage::new(&db.config).unwrap();
    let person = PersonId(Uuid::new_v4());
    let mut tx = store.begin().await.unwrap();
    tx.create_person(person, "Alice").await.unwrap();
    tx.commit().await.unwrap();
    (db, store, person)
}

fn credential(person: PersonId) -> CredentialRecord {
    CredentialRecord {
        person,
        generation: 1,
        state: CredentialState::Active,
        envelope: Some(CredentialEnvelope {
            version: 1,
            key_id: "test-key".into(),
            nonce: [7; 12],
            ciphertext: vec![9; 32],
        }),
    }
}

fn definition() -> SharedDefinition {
    let config = Config::parse("listen='127.0.0.1:0'\nproxy_token='t'\n[[limit]]\nname='budget'\nprefixes=['/claude/']\nrpm=10\n", |_| None).unwrap();
    CompiledLimits::new(&config.limits, "test")
        .unwrap()
        .shared
        .remove(0)
}
