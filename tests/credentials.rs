#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "common/database.rs"]
mod database;
#[path = "common/database_close.rs"]
mod database_close;
#[path = "credentials/disabled.rs"]
mod disabled;
#[path = "common/eventually.rs"]
mod eventually;
#[path = "common/evidence.rs"]
mod evidence;
#[path = "credentials/lifecycle.rs"]
mod lifecycle;
#[path = "common/pg_dump.rs"]
mod pg_dump;
#[path = "credentials/rotation.rs"]
mod rotation;
#[path = "credentials/rotation_failure.rs"]
mod rotation_failure;

use evidence::artifact;
use llmproxy::{
    config::CredentialEncryptionConfig,
    secret::{KeyRing, SecretBytes},
    storage::{self, Credentials, PersonId, Storage, StorageError},
};
use std::{collections::BTreeMap, fs, path::PathBuf};
use uuid::Uuid;

const ALICE: &[u8] = b"fake-anthropic-alice-credential-6";
const BOB: &[u8] = b"fake-anthropic-bob-credential-6";

fn ring(active: &str, keys: &[(&str, u8)]) -> KeyRing {
    KeyRing::new(
        active,
        keys.iter()
            .map(|(id, byte)| ((*id).into(), SecretBytes::new(vec![*byte; 32])))
            .collect(),
    )
    .unwrap()
}

fn service(store: &Storage, keys: KeyRing) -> Credentials {
    Credentials::new(store.clone(), keys, SecretBytes::new(vec![9; 32]))
}

async fn setup() -> (database::Database, Storage, PersonId, PersonId) {
    let db = database::Database::new().await;
    storage::migrate(&db.config).await.unwrap();
    let store = Storage::new(&db.config).unwrap();
    let alice = PersonId(Uuid::from_u128(1));
    let bob = PersonId(Uuid::from_u128(2));
    let mut tx = store.begin().await.unwrap();
    tx.create_person(alice, "Alice").await.unwrap();
    tx.create_person(bob, "Bob").await.unwrap();
    tx.commit().await.unwrap();
    (db, store, alice, bob)
}

async fn key_references(db: &database::Database, key_id: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM provider_credentials WHERE key_id = $1")
        .bind(key_id)
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

fn encryption(db: &database::Database) -> CredentialEncryptionConfig {
    let old = db.path.with_extension("old.key");
    let new = db.path.with_extension("new.key");
    fs::write(&old, [1; 32]).unwrap();
    fs::write(&new, [2; 32]).unwrap();
    CredentialEncryptionConfig {
        active_key_id: "new".into(),
        fingerprint_key_file: PathBuf::from("/nonexistent/fingerprint"),
        keys: BTreeMap::from([("old".into(), old), ("new".into(), new)]),
    }
}
