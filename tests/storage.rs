#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "common/database.rs"]
mod database;
#[path = "common/database_close.rs"]
mod database_close;
#[path = "storage/invalid_credential.rs"]
mod invalid_credential;
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

/// Waits for a change hint, then rereads the credential in a fresh transaction.
struct CredentialReader {
    listener: ChangeListener,
    storage: Storage,
}

impl CredentialReader {
    fn new(storage: Storage) -> Self {
        Self {
            listener: ChangeListener::new(storage.clone()),
            storage,
        }
    }

    async fn credential(
        &mut self,
        person: PersonId,
        wait: Duration,
    ) -> Result<Option<CredentialRecord>, StorageError> {
        self.listener.changed(wait).await?;
        let mut tx = self.storage.begin().await?;
        let result = tx.credential(person).await?;
        tx.rollback().await?;
        Ok(result)
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
async fn putting_an_unchanged_definition_does_not_rewrite_its_row() {
    // Given a stored definition.
    let (db, store, person) = setup().await;
    let key = LimitKey {
        person,
        name: "budget".into(),
    };
    let row = || async {
        sqlx::query_as::<_, (String, i64)>("SELECT xmin::text, version FROM limit_definitions")
            .fetch_one(&db.pool)
            .await
            .unwrap()
    };
    let mut tx = store.begin().await.unwrap();
    let first = tx.put_limit(&key, &definition()).await.unwrap();
    tx.commit().await.unwrap();
    let before = row().await;
    // When every request puts the same definition again.
    let mut tx = store.begin().await.unwrap();
    let second = tx.put_limit(&key, &definition()).await.unwrap();
    tx.commit().await.unwrap();
    // Then it reports the same version without writing a new row version.
    assert_eq!(second, first);
    assert_eq!(row().await, before);
    store.close().await;
    db.close().await;
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
        StorageError::Encryption,
    ]
    .iter()
    .map(ToString::to_string)
    .collect();
    assert_eq!(messages.len(), 8);
    store.close().await;
    db.close().await;
}
