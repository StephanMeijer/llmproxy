use super::*;
use llmproxy::{
    secret::{KeyRing, SecretBytes},
    storage::{Credentials, StorageError},
};

#[tokio::test]
async fn unknown_persisted_credential_state_is_rejected_by_reads_and_health_counts() {
    // Given a corrupted credential row after out-of-band schema changes.
    let (db, store, person) = setup().await;
    let mut tx = store.begin().await.unwrap();
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    sqlx::query("ALTER TABLE provider_credentials DROP CONSTRAINT provider_credentials_state_check, DROP CONSTRAINT provider_credentials_check")
        .execute(&db.pool).await.unwrap();
    sqlx::query("UPDATE provider_credentials SET state='unknown' WHERE person_id=$1")
        .bind(person)
        .execute(&db.pool)
        .await
        .unwrap();
    let keys = KeyRing::new(
        "test-key",
        [("test-key".into(), SecretBytes::new(vec![1; 32]))].into(),
    )
    .unwrap();
    let credentials = Credentials::new(store.clone(), keys, SecretBytes::new(vec![2; 32]));
    // When the credential and health snapshot cross the database parsing boundary.
    let mut tx = store.begin().await.unwrap();
    let record = tx.credential(person).await;
    tx.rollback().await.unwrap();
    let counts = credentials.counts().await;
    // Then neither path treats an unknown state as an active or absent credential.
    assert!(matches!(record, Err(StorageError::InvalidData)));
    assert!(matches!(counts, Err(StorageError::InvalidData)));
    store.close().await;
    db.close().await;
}
