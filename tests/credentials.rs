#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "migrations/support.rs"]
mod database;
#[path = "credentials/disabled.rs"]
mod disabled;
#[path = "credentials/lifecycle.rs"]
mod lifecycle;
#[path = "credentials/rotation.rs"]
mod rotation;
#[path = "credentials/rotation_failure.rs"]
mod rotation_failure;

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

fn artifact(name: &str, bytes: &[u8]) {
    if let Ok(dir) = std::env::var("LLMPROXY_CREDENTIAL_EVIDENCE") {
        fs::write(PathBuf::from(dir).join(name), bytes).unwrap();
    }
}
