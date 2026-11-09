use super::*;
use llmproxy::storage::{
    FencedLease, Lease, LimitKey, OwnerId, RateKind, RateState, ReservationId, StorageError,
    TokenId,
};

#[tokio::test]
async fn people_tokens_and_ciphertext_roundtrip_without_plaintext() {
    // Given a namespace with a person and encrypted credential material.
    let (db, store, person) = setup().await;
    let token_id = TokenId(Uuid::new_v4());
    let mut tx = store.begin().await.unwrap();
    tx.create_token(token_id, person, "$argon2id$test-verifier")
        .await
        .unwrap();
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // When the current records are read through a fresh transaction.
    let mut tx = store.begin().await.unwrap();
    let token = tx.token(token_id).await.unwrap();
    let stored = tx
        .credential(Provider::Anthropic, person)
        .await
        .unwrap()
        .unwrap();
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
    tx.create_token(id, person, "$argon2id$test").await.unwrap();
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
    let mut record = tx
        .credential(Provider::Anthropic, person)
        .await
        .unwrap()
        .unwrap();
    record.state = CredentialState::Removed;
    assert_eq!(tx.set_credential_state(&record).await.unwrap(), 3);
    tx.commit().await.unwrap();
    // Then removal retains only the tombstone generation.
    let mut tx = store.begin().await.unwrap();
    let stored = tx
        .credential(Provider::Anthropic, person)
        .await
        .unwrap()
        .unwrap();
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
        provider: Provider::Anthropic,
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
    assert_eq!(tx.enqueue(&key, &lease, LONG).await.unwrap(), 1);
    let fenced = FencedLease {
        id: lease.id,
        owner: lease.owner,
    };
    assert!(tx.acquire_concurrency(&key, &fenced, 1).await.unwrap());
    tx.commit().await.unwrap();
    // When the authoritative state is locked and read.
    let mut tx = store.begin().await.unwrap();
    let rates = tx.lock_rates(&key).await.unwrap();
    // Then exact integer balances, FIFO identity and lease ownership survive.
    assert_eq!(rates.first().unwrap().available_units, 42);
    assert!(rates.first().unwrap().elapsed_microseconds >= 0);
    assert_eq!(
        tx.waiter_head(&key).await.unwrap().unwrap().enqueue_order,
        1
    );
    assert!(tx.renew_concurrency(&key, &fenced).await.unwrap());
    tx.remove_waiter(&key, &lease).await.unwrap();
    assert!(tx.release_concurrency(&key, &fenced).await.unwrap());
    tx.commit().await.unwrap();
    let mut tx = store.begin().await.unwrap();
    assert!(tx.waiter_head(&key).await.unwrap().is_none());
    assert!(!tx.renew_concurrency(&key, &fenced).await.unwrap());
    tx.rollback().await.unwrap();
    store.close().await;
    db.close().await;
}

#[tokio::test]
async fn expired_leases_cannot_revive_and_reclaim_uses_database_time() {
    // Given already expired reservations, without using the application clock.
    let (db, store, person) = setup().await;
    let key = LimitKey {
        provider: Provider::Anthropic,
        person,
        name: "budget".into(),
    };
    let lease = Lease {
        id: ReservationId(Uuid::new_v4()),
        owner: OwnerId(Uuid::new_v4()),
    };
    let mut tx = store.begin().await.unwrap();
    tx.put_limit(&key, &definition()).await.unwrap();
    tx.enqueue(&key, &lease, Duration::ZERO).await.unwrap();
    let fenced = FencedLease {
        id: lease.id,
        owner: lease.owner,
    };
    assert!(tx.acquire_concurrency(&key, &fenced, 1).await.unwrap());
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

#[tokio::test]
async fn providers_keep_separate_definitions_rates_waiters_and_leases() {
    // Given an Anthropic and a ChatGPT limit with the same name but different configs.
    let (db, store, person) = setup().await;
    let anthropic = LimitKey {
        provider: Provider::Anthropic,
        person,
        name: "budget".into(),
    };
    let chatgpt = LimitKey {
        provider: Provider::ChatGpt,
        ..anthropic.clone()
    };
    let config = Config::parse("listen='127.0.0.1:0'\nproxy_token='t'\n[[limit]]\nname='budget'\nprefixes=['/chatgpt/']\nrpm=11\n", |_| None).unwrap();
    let chatgpt_definition =
        CompiledLimits::new(&config.limits, "test", Provider::ChatGpt, "/chatgpt/")
            .unwrap()
            .shared
            .remove(0);
    let mut tx = store.begin().await.unwrap();
    // When both are stored, the second is not a conflict with the first provider's row.
    tx.put_limit(&anthropic, &definition()).await.unwrap();
    tx.put_limit(&chatgpt, &chatgpt_definition).await.unwrap();
    // And a key naming another provider than its definition is refused.
    assert_eq!(
        tx.put_limit(&anthropic, &chatgpt_definition).await,
        Err(StorageError::InvalidData)
    );
    for (key, units) in [(&anthropic, 42), (&chatgpt, 7)] {
        tx.put_rate(
            key,
            &RateState {
                kind: RateKind::Requests,
                available_units: units,
                elapsed_microseconds: 0,
            },
        )
        .await
        .unwrap();
    }
    let waiter = Lease {
        id: ReservationId(Uuid::new_v4()),
        owner: OwnerId(Uuid::new_v4()),
    };
    tx.enqueue(&anthropic, &waiter, LONG).await.unwrap();
    let held = FencedLease::new(OwnerId(Uuid::new_v4()));
    assert!(tx.acquire_concurrency(&anthropic, &held, 1).await.unwrap());
    tx.commit().await.unwrap();
    // Then each provider sees only its own balance, queue and leases.
    let mut tx = store.begin().await.unwrap();
    assert_eq!(
        tx.lock_rates(&anthropic)
            .await
            .unwrap()
            .first()
            .unwrap()
            .available_units,
        42
    );
    assert_eq!(
        tx.lock_rates(&chatgpt)
            .await
            .unwrap()
            .first()
            .unwrap()
            .available_units,
        7
    );
    assert!(tx.waiter_head(&anthropic).await.unwrap().is_some());
    assert!(tx.waiter_head(&chatgpt).await.unwrap().is_none());
    assert!(!tx.renew_concurrency(&chatgpt, &held).await.unwrap());
    let other = FencedLease::new(OwnerId(Uuid::new_v4()));
    assert!(tx.acquire_concurrency(&chatgpt, &other, 1).await.unwrap());
    assert!(!tx.acquire_concurrency(&anthropic, &other, 1).await.unwrap());
    tx.rollback().await.unwrap();
    store.close().await;
    db.close().await;
}
