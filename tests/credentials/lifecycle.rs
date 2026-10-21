use super::*;

#[tokio::test]
async fn concurrent_replacement_has_one_winner_and_snapshot_survives_removal() {
    // Given an admitted snapshot of Alice's first generation.
    let (db, store, alice, _) = setup().await;
    let credentials = service(&store, ring("old", &[("old", 1)]));
    let version = credentials
        .install(alice, &SecretBytes::new(ALICE.to_vec()))
        .await
        .unwrap();
    let admitted = credentials.snapshot(alice).await.unwrap();
    let replacement = SecretBytes::new(BOB.to_vec());
    // When two operators replace the same generation concurrently.
    let results = tokio::join!(
        credentials.replace(version, &replacement),
        credentials.replace(version, &replacement)
    );
    let results: [_; 2] = results.into();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(StorageError::Conflict))
            .count(),
        1
    );
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    // Then exactly one generation increment occurred and removal affects only future snapshots.
    assert_eq!(winner.generation, 2);
    let removed = credentials.remove(winner).await.unwrap();
    assert_eq!(removed.generation, 3);
    assert_eq!(
        credentials.snapshot(alice).await.unwrap_err(),
        StorageError::Unavailable
    );
    assert_eq!(admitted.secret.expose(), ALICE);
    assert_eq!(admitted.version.generation, 1);
    assert_eq!(
        credentials
            .replace(version, &replacement)
            .await
            .unwrap_err(),
        StorageError::Conflict
    );
    artifact("cas-removal.txt", b"PASS: concurrent CAS has one winner at generation 2; removal generation 3; admitted generation 1 secret survives; stale replacement rejected\n");
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn duplicate_key_is_rejected_across_people_and_dump_contains_no_plaintext() {
    // Given Alice's encrypted key.
    let (db, store, alice, bob) = setup().await;
    let credentials = service(&store, ring("old", &[("old", 1)]));
    let secret = SecretBytes::new(ALICE.to_vec());
    credentials.install(alice, &secret).await.unwrap();
    // When Bob attempts to install identical key material.
    let duplicate = credentials.install(bob, &secret).await;
    // Then ownership is rejected and a real database dump contains no plaintext.
    assert_eq!(duplicate.unwrap_err(), StorageError::Conflict);
    let container = std::env::var("LLMPROXY_CREDENTIAL_CONTAINER").unwrap();
    let url = reqwest::Url::parse(&db.config.url).unwrap();
    let dump = std::process::Command::new("docker")
        .args([
            "exec",
            &container,
            "pg_dump",
            "-U",
            "postgres",
            url.path().trim_start_matches('/'),
        ])
        .output()
        .unwrap();
    assert!(dump.status.success());
    assert!(!dump.stdout.windows(ALICE.len()).any(|bytes| bytes == ALICE));
    assert!(!dump.stdout.windows(BOB.len()).any(|bytes| bytes == BOB));
    artifact("credentials.sql", &dump.stdout);
    artifact("dump-duplicate.txt", b"PASS: identical credential across people rejected; pg_dump successful; both seeded plaintext values absent\n");
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn wrong_and_missing_keys_return_redacted_unavailable_without_row_changes() {
    // Given a valid durable credential.
    let (db, store, alice, _) = setup().await;
    let correct = service(&store, ring("old", &[("old", 1)]));
    correct
        .install(alice, &SecretBytes::new(ALICE.to_vec()))
        .await
        .unwrap();
    let before: (i64, Vec<u8>) =
        sqlx::query_as("SELECT generation,ciphertext FROM provider_credentials")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    // When reading through wrong and missing key-ring snapshots.
    for keys in [ring("old", &[("old", 3)]), ring("new", &[("new", 2)])] {
        let error = service(&store, keys).snapshot(alice).await.unwrap_err();
        assert_eq!(error, StorageError::Unavailable);
        assert!(!format!("{error} {error:?}").contains(std::str::from_utf8(ALICE).unwrap()));
    }
    // Then ciphertext and generation remain intact and the correct key still decrypts.
    let after: (i64, Vec<u8>) =
        sqlx::query_as("SELECT generation,ciphertext FROM provider_credentials")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    assert_eq!(
        correct.snapshot(alice).await.unwrap().secret.expose(),
        ALICE
    );
    artifact("unavailable.txt", b"PASS: wrong and missing keys return redacted Unavailable; ciphertext/generation unchanged; correct-key recovery succeeds\n");
    store.close().await;
    db.close().await;
}
