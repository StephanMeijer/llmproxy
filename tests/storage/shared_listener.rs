use super::*;
use llmproxy::storage::ChangeListener;

const WAITERS: usize = 256;

async fn setup() -> (database::Database, Storage, PersonId) {
    let mut db = database::Database::new().await;
    db.config
        .url
        .push_str("?application_name=shared-listener-test");
    storage::migrate(&db.config).await.unwrap();
    let store = Storage::new(&db.config).unwrap();
    let person = PersonId(Uuid::new_v4());
    let mut tx = store.begin().await.unwrap();
    tx.create_person(person, "Alice").await.unwrap();
    tx.commit().await.unwrap();
    (db, store, person)
}

async fn connections(db: &database::Database) -> (i64, i64) {
    sqlx::query_as("SELECT count(*), count(*) FILTER (WHERE query LIKE 'LISTEN%') FROM pg_stat_activity WHERE datname=current_database() AND application_name='shared-listener-test'")
        .fetch_one(&db.pool).await.unwrap()
}

#[tokio::test]
async fn hundreds_of_waiters_share_one_connection_per_runtime() {
    // Given two independently configured runtimes and hundreds of subscribers each.
    let (db, first, person) = setup().await;
    let second = Storage::new(&db.config).unwrap();
    let mut readers = Vec::new();
    for store in [&first, &second] {
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..WAITERS {
            let mut reader = ChangeListener::new(store.clone());
            tasks.spawn(async move {
                assert!(reader.credential(person, LONG).await.unwrap().is_none());
                reader
            });
        }
        while let Some(result) = tasks.join_next().await {
            readers.push(result.unwrap());
        }
    }
    // When all subscribers are connected, each runtime has exactly one LISTEN backend.
    let (total, listeners) = connections(&db).await;
    eprintln!(
        "waiters={} runtimes=2 connections={total} listeners={listeners} bound={}",
        readers.len(),
        2 * (db.config.max_connections + 1)
    );
    assert_eq!(listeners, 2);
    assert!(total <= i64::from(2 * (db.config.max_connections + 1)));
    // Then notification coalescing still delivers the authoritative latest state to every reader.
    let mut tx = first.begin().await.unwrap();
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for mut reader in readers {
        tasks.spawn(async move {
            assert_eq!(
                reader
                    .credential(person, LONG)
                    .await
                    .unwrap()
                    .unwrap()
                    .generation,
                1
            );
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    first.close().await;
    second.close().await;
    tokio::time::timeout(LONG, async {
        while connections(&db).await != (0, 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    db.close().await;
}

#[tokio::test]
async fn dropping_runtime_releases_shared_listener_connection() {
    // Given a runtime with a live shared listener.
    let (db, store, person) = setup().await;
    let mut reader = ChangeListener::new(store.clone());
    reader.credential(person, LONG).await.unwrap();
    assert_eq!(connections(&db).await.1, 1);
    // When the final runtime and subscriber owners are dropped without close.
    drop(reader);
    drop(store);
    // Then no listener task retains the runtime or its database connections.
    tokio::time::timeout(LONG, async {
        while connections(&db).await != (0, 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    db.close().await;
}

#[tokio::test]
async fn slow_subscriber_coalesces_notification_burst_and_rereads() {
    // Given a subscriber left idle while more notifications arrive than a bounded queue holds.
    let (db, store, person) = setup().await;
    let mut reader = ChangeListener::new(store.clone());
    reader.credential(person, LONG).await.unwrap();
    let mut tx = store.begin().await.unwrap();
    tx.insert_credential(&credential(person), &[8; 32])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for sequence in 0..WAITERS {
        sqlx::query("SELECT pg_notify('llmproxy_storage', $1)")
            .bind(sequence.to_string())
            .execute(&db.pool)
            .await
            .unwrap();
    }
    // When the slow subscriber resumes with a long wait budget.
    let record = tokio::time::timeout(LONG, reader.credential(person, LONG * 2))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Then coalescing causes an immediate authoritative read, never a lag error.
    assert_eq!(record.generation, 1);
    drop(reader);
    store.close().await;
    db.close().await;
}
