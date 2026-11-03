use super::*;

#[tokio::test]
async fn invalid_fingerprint_key_prevents_installation() {
    let (db, store, alice, _) = setup().await;
    let credentials = Credentials::new(
        store.clone(),
        ring("old", &[("old", 1)]),
        SecretBytes::new(Vec::new()),
    );
    assert_eq!(
        credentials
            .install(alice, &SecretBytes::new(ALICE.to_vec()))
            .await,
        Err(StorageError::Unavailable)
    );
    let mut tx = store.begin().await.unwrap();
    assert!(tx.credential(alice).await.unwrap().is_none());
    tx.rollback().await.unwrap();
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn active_state_requires_the_encrypted_installation_path() {
    let (db, store, alice, _) = setup().await;
    let mut tx = store.begin().await.unwrap();
    let record = storage::CredentialRecord {
        person: alice,
        generation: 1,
        state: storage::CredentialState::Active,
        envelope: None,
    };
    assert_eq!(
        tx.set_credential_state(&record).await,
        Err(StorageError::InvalidData)
    );
    tx.rollback().await.unwrap();
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn disabled_person_cannot_install_or_replace_credentials() {
    let (db, store, alice, bob) = setup().await;
    let credentials = service(&store, ring("old", &[("old", 1)]));
    let version = credentials
        .install(alice, &SecretBytes::new(ALICE.to_vec()))
        .await
        .unwrap();
    let mut tx = store.begin().await.unwrap();
    for id in [alice, bob] {
        let mut person = tx.person(id).await.unwrap();
        person.enabled = false;
        tx.update_person(&person).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(
        credentials
            .install(bob, &SecretBytes::new(BOB.to_vec()))
            .await,
        Err(StorageError::Conflict)
    );
    assert_eq!(
        credentials
            .replace(version, &SecretBytes::new(BOB.to_vec()))
            .await,
        Err(StorageError::Conflict)
    );
    store.close().await;
    db.close().await;
}
