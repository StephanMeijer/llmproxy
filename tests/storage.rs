#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "migrations/support.rs"]
mod database;
#[path = "storage/outage.rs"]
mod outage;
#[path = "storage/repositories.rs"]
mod repositories;
#[path = "storage/runtime.rs"]
mod runtime;

use llmproxy::{
    config::Config,
    limit::shared::{CompiledLimits, SharedDefinition},
    secret::CredentialEnvelope,
    storage::{
        self, ChangeListener, CredentialRecord, CredentialState, LimitKey, PersonId, Storage,
        StorageError,
    },
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

#[tokio::test]
async fn repositories_reject_invalid_state_changes_and_report_conflicts() {
    let (db, store, person) = setup().await;
    let key = LimitKey {
        person,
        name: "budget".into(),
    };
    let mut tx = store.begin().await.unwrap();
    tx.put_limit(&key, &definition()).await.unwrap();
    let mismatched = LimitKey {
        person,
        name: "other".into(),
    };
    assert!(matches!(
        tx.put_limit(&mismatched, &definition()).await,
        Err(StorageError::InvalidData)
    ));
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    let mut record = credential(person);
    assert!(matches!(
        tx.set_credential_state(&record).await,
        Err(StorageError::InvalidData)
    ));
    record.state = CredentialState::Invalid;
    tx.set_credential_state(&record).await.unwrap();
    tx.commit().await.unwrap();
    let bob = PersonId(Uuid::new_v4());
    let conflicting = Config::parse("listen='127.0.0.1:0'\nproxy_token='t'\n[[limit]]\nname='budget'\nprefixes=['/claude/']\nrpm=11\n", |_| None).unwrap();
    let conflicting = CompiledLimits::new(&conflicting.limits, "test")
        .unwrap()
        .shared
        .remove(0);
    let mut tx = store.begin().await.unwrap();
    tx.create_person(bob, "Bob").await.unwrap();
    assert!(matches!(
        tx.put_limit(
            &LimitKey {
                person: bob,
                name: "budget".into()
            },
            &conflicting
        )
        .await,
        Err(StorageError::Conflict)
    ));
    tx.rollback().await.unwrap();
    let mut listener = ChangeListener::new(store.clone());
    listener.limit(&key, SHORT).await.unwrap();
    drop(listener);
    let messages: std::collections::BTreeSet<String> = [
        StorageError::Unavailable,
        StorageError::IncompatibleSchema,
        StorageError::UnsupportedServer,
        StorageError::MigrationFailed,
        StorageError::Conflict,
        StorageError::InvalidData,
        StorageError::NotFound,
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    assert_eq!(messages.len(), 7);
    store.close().await;
    db.close().await;
}
