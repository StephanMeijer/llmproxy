use super::*;
use llmproxy::storage::{
    FencedLease, Lease, LimitKey, OwnerId, RateKind, RateState, ReservationId, StorageError,
    TokenId, TokenRecord,
};

#[tokio::test]
async fn people_tokens_and_ciphertext_roundtrip_without_plaintext() {
    // Given a namespace with a person and encrypted credential material.
    let (db, store, person) = setup().await;
    let token_id = TokenId(Uuid::new_v4());
    let mut tx = store.begin().await.unwrap();
    tx.create_token(&TokenRecord {
        id: token_id,
        person_id: person,
        secret_hash: "$argon2id$test-verifier".into(),
        revoked: false,
        person_enabled: true,
    })
    .await
    .unwrap();
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // When the current records are read through a fresh transaction.
    let mut tx = store.begin().await.unwrap();
    let token = tx.token(token_id).await.unwrap();
    let stored = tx.credential(person).await.unwrap().unwrap();
    // Then only the verifier and ciphertext are returned.
    assert_eq!(token.secret_hash, "$argon2id$test-verifier");
    assert!(token.person_enabled && !token.revoked);
    assert_eq!(stored.generation, 1);
    assert_eq!(stored.envelope.unwrap().ciphertext, vec![9; 32]);
    tx.rollback().await.unwrap();
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn auth_reads_observe_disable_and_revocation_without_cache() {
    // Given an enabled person with a live token.
    let (db, store, person) = setup().await;
    let id = TokenId(Uuid::new_v4());
    let mut tx = store.begin().await.unwrap();
    tx.create_token(&TokenRecord {
        id,
        person_id: person,
        secret_hash: "$argon2id$test".into(),
        revoked: false,
        person_enabled: true,
    })
    .await
    .unwrap();
    tx.commit().await.unwrap();
    // When the person is disabled and token revoked transactionally.
    let mut tx = store.begin().await.unwrap();
    let mut record = tx.person(person).await.unwrap();
    record.enabled = false;
    record.display_name = "Renamed".into();
    tx.update_person(&record).await.unwrap();
    assert!(tx.revoke_token(id).await.unwrap());
    tx.commit().await.unwrap();
    // Then a new authentication read observes both changes.
    let mut tx = store.begin().await.unwrap();
    let record = tx.token(id).await.unwrap();
    assert!(record.revoked && !record.person_enabled);
    assert_eq!(tx.person(person).await.unwrap().display_name, "Renamed");
    tx.rollback().await.unwrap();
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn credential_replace_remove_and_stale_generation_are_atomic() {
    // Given a persisted encrypted generation.
    let (db, store, person) = setup().await;
    let mut tx = store.begin().await.unwrap();
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // When replacement, stale replacement and removal run under generation checks.
    let mut tx = store.begin().await.unwrap();
    assert_eq!(
        tx.replace_credential(&credential(person), &[3; 32])
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        tx.replace_credential(&credential(person), &[4; 32]).await,
        Err(StorageError::Conflict)
    );
    let mut record = tx.credential(person).await.unwrap().unwrap();
    record.state = CredentialState::Removed;
    assert_eq!(tx.set_credential_state(&record).await.unwrap(), 3);
    tx.commit().await.unwrap();
    // Then removal retains only the tombstone generation.
    let mut tx = store.begin().await.unwrap();
    let stored = tx.credential(person).await.unwrap().unwrap();
    assert_eq!(stored.state, CredentialState::Removed);
    assert!(stored.envelope.is_none());
    assert_eq!(stored.generation, 3);
    tx.rollback().await.unwrap();
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn limit_state_waiters_leases_and_release_roundtrip() {
    // Given a limit definition and two reservations.
    let (db, store, person) = setup().await;
    let key = LimitKey {
        person,
        name: "budget".into(),
    };
    let lease = Lease {
        id: ReservationId(Uuid::new_v4()),
        owner: OwnerId(Uuid::new_v4()),
    };
    let mut tx = store.begin().await.unwrap();
    tx.put_limit(&key, &definition()).await.unwrap();
    tx.put_rate(
        &key,
        &RateState {
            kind: RateKind::Requests,
            available_units: 42,
            elapsed_microseconds: 0,
        },
    )
    .await
    .unwrap();
    assert_eq!(tx.enqueue(&key, (&lease, LONG)).await.unwrap(), 1);
    let fenced = FencedLease {
        id: lease.id,
        owner: lease.owner,
        generation: 1,
    };
    assert!(tx.acquire_concurrency(&key, (&fenced, 1)).await.unwrap());
    tx.commit().await.unwrap();
    // When the complete authoritative state is locked and read.
    let mut tx = store.begin().await.unwrap();
    let state = tx.lock_limit(&key).await.unwrap();
    // Then exact integer balances, FIFO identity and lease ownership survive.
    assert_eq!(state.rates.first().unwrap().available_units, 42);
    assert!(state.rates.first().unwrap().elapsed_microseconds >= 0);
    assert_eq!(state.waiters.first().unwrap().enqueue_order, 1);
    assert_eq!(state.leases.first().unwrap().owner, lease.owner);
    assert!(tx.renew_concurrency(&key, &fenced).await.unwrap());
    tx.release(&key, &lease).await.unwrap();
    assert!(tx.release_concurrency(&key, &fenced).await.unwrap());
    tx.commit().await.unwrap();
    let mut tx = store.begin().await.unwrap();
    let state = tx.lock_limit(&key).await.unwrap();
    assert!(state.waiters.is_empty() && state.leases.is_empty());
    tx.rollback().await.unwrap();
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn expired_leases_cannot_revive_and_reclaim_uses_database_time() {
    // Given already expired reservations, without using the application clock.
    let (db, store, person) = setup().await;
    let key = LimitKey {
        person,
        name: "budget".into(),
    };
    let lease = Lease {
        id: ReservationId(Uuid::new_v4()),
        owner: OwnerId(Uuid::new_v4()),
    };
    let mut tx = store.begin().await.unwrap();
    tx.put_limit(&key, &definition()).await.unwrap();
    tx.enqueue(&key, (&lease, Duration::ZERO)).await.unwrap();
    let fenced = FencedLease {
        id: lease.id,
        owner: lease.owner,
        generation: 1,
    };
    assert!(tx.acquire_concurrency(&key, (&fenced, 1)).await.unwrap());
    tx.commit().await.unwrap();
    sqlx::query("UPDATE concurrency_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1")
        .bind(lease.id).execute(&db.pool).await.unwrap();
    // When renewal and cleanup execute on the server.
    let mut tx = store.begin().await.unwrap();
    assert!(!tx.renew_concurrency(&key, &fenced).await.unwrap());
    tx.reclaim(&key).await.unwrap();
    tx.commit().await.unwrap();
    // Then expired rows are gone from durable state.
    let count: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM fifo_waiters)+(SELECT count(*) FROM concurrency_leases)",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    store.close().await;
    db.close().await;
}
