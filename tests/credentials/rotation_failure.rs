use super::*;

#[tokio::test]
async fn failed_rotation_preserves_ciphertext_and_invalid_state() {
    // Given an invalid credential whose ciphertext must still rotate safely.
    let (db, store, alice, _) = setup().await;
    let credentials = service(&store, ring("old", &[("old", 1)]));
    credentials
        .install(alice, &SecretBytes::new(ALICE.to_vec()))
        .await
        .unwrap();
    let mut tx = store.begin().await.unwrap();
    let mut record = tx
        .credential(Provider::Anthropic, alice)
        .await
        .unwrap()
        .unwrap();
    record.state = storage::CredentialState::Invalid;
    tx.set_credential_state(&record).await.unwrap();
    tx.commit().await.unwrap();
    let before: (i64, Vec<u8>, Vec<u8>) =
        sqlx::query_as("SELECT generation,ciphertext,fingerprint FROM provider_credentials")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    let mut config = encryption(&db);
    let old = config.keys.remove("old").unwrap();
    // When old-key absence and wrong old-key material prevent authentication.
    assert_eq!(
        Credentials::open(store.clone(), Provider::Anthropic, &config)
            .unwrap()
            .reencrypt()
            .await
            .unwrap_err(),
        StorageError::Encryption
    );
    fs::write(&old, [7; 32]).unwrap();
    config.keys.insert("old".into(), old.clone());
    assert_eq!(
        Credentials::open(store.clone(), Provider::Anthropic, &config)
            .unwrap()
            .reencrypt()
            .await
            .unwrap_err(),
        StorageError::Encryption
    );
    let after: (i64, Vec<u8>, Vec<u8>) =
        sqlx::query_as("SELECT generation,ciphertext,fingerprint FROM provider_credentials")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    // Then restoring the correct old key rotates without reactivating the credential.
    fs::write(&old, [1; 32]).unwrap();
    assert_eq!(
        Credentials::open(store.clone(), Provider::Anthropic, &config)
            .unwrap()
            .reencrypt()
            .await
            .unwrap()
            .rewritten,
        1
    );
    let state: (String, Vec<u8>) =
        sqlx::query_as("SELECT state,fingerprint FROM provider_credentials")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(state.0, "invalid");
    assert_eq!(state.1, before.2);
    assert_eq!(key_references(&db, "old").await, 0);
    artifact("rotation-failure.txt", b"PASS: missing/wrong old key returns Unavailable without generation/ciphertext/fingerprint changes; corrected key rotates invalid row while preserving state/fingerprint\n");
    for path in config.keys.values().chain([&config.fingerprint_key_file]) {
        fs::remove_file(path).unwrap();
    }
    store.close().await;
    db.close().await;
}
