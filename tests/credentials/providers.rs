use super::*;

#[tokio::test]
async fn one_persons_providers_have_independent_generations_and_encryption_contexts() {
    // Given Alice with an Anthropic and a ChatGPT credential.
    let (db, store, alice, _) = setup().await;
    let anthropic = service(&store, ring("old", &[("old", 1)]));
    let chatgpt = Credentials::new(
        store.clone(),
        Provider::ChatGpt,
        ring("old", &[("old", 1)]),
        SecretBytes::new(vec![9; 32]),
    );
    let first = anthropic
        .install(alice, &SecretBytes::new(ALICE.to_vec()))
        .await
        .unwrap();
    let other = chatgpt
        .install(alice, &SecretBytes::new(CHATGPT.to_vec()))
        .await
        .unwrap();
    assert_eq!((first.generation, other.generation), (1, 1));
    // When one provider's generation advances, the other's is not fenced or changed by it.
    let second = anthropic
        .replace(first, &SecretBytes::new(BOB.to_vec()))
        .await
        .unwrap();
    assert_eq!(second.generation, 2);
    assert_eq!(
        chatgpt
            .replace(second, &SecretBytes::new(BOB.to_vec()))
            .await,
        Err(StorageError::Conflict)
    );
    let snapshot = chatgpt.snapshot(alice).await.unwrap();
    assert_eq!(snapshot.version.generation, 1);
    assert_eq!(snapshot.secret.expose(), CHATGPT);
    // Then ciphertext moved across providers does not decrypt: the context names the provider.
    sqlx::query("UPDATE provider_credentials AS a SET format_version=c.format_version,key_id=c.key_id,nonce=c.nonce,ciphertext=c.ciphertext FROM provider_credentials AS c WHERE a.person_id=c.person_id AND a.provider='anthropic' AND c.provider='chatgpt'")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        anthropic.snapshot(alice).await.unwrap_err(),
        StorageError::Encryption
    );
    // And removing one provider's credential leaves the other usable.
    // The swap was itself an update, which the generation trigger counted.
    let swapped = storage::CredentialVersion {
        generation: 3,
        ..second
    };
    anthropic.remove(swapped).await.unwrap();
    assert_eq!(
        anthropic.snapshot(alice).await.unwrap_err(),
        StorageError::Unavailable
    );
    assert_eq!(
        chatgpt.snapshot(alice).await.unwrap().secret.expose(),
        CHATGPT
    );
    assert_eq!(anthropic.counts().await.unwrap().removed, 1);
    assert_eq!(chatgpt.counts().await.unwrap().active, 1);
    store.close().await;
    db.close().await;
}
