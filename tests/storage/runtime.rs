use super::*;
use llmproxy::storage::{ChangeListener, StorageError};

#[tokio::test]
async fn row_lock_serializes_two_pools() {
    // Given a row locked by one pool.
    let (db, first, person) = setup().await;
    let second = Storage::new(&db.config).unwrap();
    let mut tx = first.begin().await.unwrap();
    let mut record = tx.person(person).await.unwrap();
    record.display_name = "committed".into();
    tx.update_person(&record).await.unwrap();
    let mut other = second.begin().await.unwrap();
    let mut read = Box::pin(other.person(person));
    // When another pool reads under the same lock.
    assert!(tokio::time::timeout(SHORT, &mut read).await.is_err());
    tx.commit().await.unwrap();
    // Then it unblocks only after commit and sees the committed value.
    assert_eq!(read.await.unwrap().display_name, "committed");
    other.rollback().await.unwrap();
    first.close().await;
    second.close().await;
    db.close().await;
}

#[tokio::test]
async fn wake_and_timeout_reload_committed_rows_across_pools() {
    // Given a subscriber with an initial authoritative reload.
    let (db, writer, person) = setup().await;
    let reader = Storage::new(&db.config).unwrap();
    let mut listener = ChangeListener::new(reader.clone());
    assert!(listener.credential(person, LONG).await.unwrap().is_none());
    // When an uncommitted credential triggers a transactional NOTIFY.
    let mut tx = writer.begin().await.unwrap();
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    let mut read = Box::pin(listener.credential(person, LONG));
    assert!(tokio::time::timeout(SHORT, &mut read).await.is_err());
    tx.commit().await.unwrap();
    // Then the second pool wakes and rereads the committed generation.
    assert_eq!(
        tokio::time::timeout(LONG, read)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .generation,
        1
    );
    drop(listener);
    writer.close().await;
    reader.close().await;
    db.close().await;
}

#[tokio::test]
async fn missed_notification_is_recovered_by_timeout_reread() {
    // Given a listener and a credential changed without trigger notifications.
    let (db, writer, person) = setup().await;
    let reader = Storage::new(&db.config).unwrap();
    let mut listener = ChangeListener::new(reader.clone());
    assert!(listener.credential(person, LONG).await.unwrap().is_none());
    let mut tx = db.pool.begin().await.unwrap();
    sqlx::query("SET LOCAL session_replication_role = replica")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("INSERT INTO provider_credentials(namespace,person_id,provider,state,format_version,key_id,nonce,ciphertext,fingerprint) VALUES ('test',$1,'anthropic','active',1,'k',$2,$3,$4)")
        .bind(person).bind(vec![0_u8;12]).bind(vec![1_u8;16]).bind(vec![2_u8;32]).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    // When the bounded notification wait expires.
    let record = listener.credential(person, SHORT).await.unwrap().unwrap();
    // Then durable state is recovered despite no notification.
    assert_eq!(record.generation, 1);
    drop(listener);
    writer.close().await;
    reader.close().await;
    db.close().await;
}

#[tokio::test]
async fn pool_exhaustion_and_missing_schema_return_typed_errors() {
    // Given a single-connection pool and an unmigrated schema.
    let mut db = database::Database::new().await;
    db.config.max_connections = 1;
    db.config.acquire_timeout = SHORT;
    let store = Storage::new(&db.config).unwrap();
    assert!(matches!(
        store.begin().await,
        Err(StorageError::IncompatibleSchema)
    ));
    storage::migrate(&db.config).await.unwrap();
    let tx = store.begin().await.unwrap();
    // When the only connection remains checked out.
    let result = tokio::time::timeout(LONG, store.begin()).await.unwrap();
    // Then the acquire deadline returns the public unavailable category.
    assert!(matches!(result, Err(StorageError::Unavailable)));
    tx.rollback().await.unwrap();
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn listener_reconnect_gap_forces_authoritative_reload() {
    // Given a connected subscriber.
    let (db, writer, person) = setup().await;
    let reader = Storage::new(&db.config).unwrap();
    let mut listener = ChangeListener::new(reader.clone());
    assert!(listener.credential(person, LONG).await.unwrap().is_none());
    // When its idle LISTEN backend is terminated and a mutation happens in the gap.
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'LISTEN%'")
        .execute(&db.pool).await.unwrap();
    let mut tx = writer.begin().await.unwrap();
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let first = listener.credential(person, SHORT).await;
    let record = match first {
        Err(StorageError::Unavailable) => listener.credential(person, SHORT).await,
        result => result,
    }
    .unwrap();
    // Then the same listener recovers a fresh row without process restart.
    assert_eq!(record.unwrap().generation, 1);
    drop(listener);
    writer.close().await;
    reader.close().await;
    db.close().await;
}
