#![allow(clippy::unwrap_used, clippy::expect_used)]

use llmproxy::storage::{self, StorageError};
use sqlx::AssertSqlSafe;
use std::{fs, process::Command, time::Duration};
use uuid::Uuid;

#[path = "migrations/fixed_point.rs"]
mod fixed_point;
#[path = "migrations/support.rs"]
mod support;
use support::Database;

const BIN: &str = env!("CARGO_BIN_EXE_llmproxy");

#[tokio::test]
async fn concurrent_cli_migrations_are_idempotent() {
    let db = Database::new().await;
    let mut first = Command::new(BIN)
        .args(["db", "migrate"])
        .arg(&db.path)
        .spawn()
        .unwrap();
    let mut second = Command::new(BIN)
        .args(["db", "migrate"])
        .arg(&db.path)
        .spawn()
        .unwrap();
    assert!(first.wait().unwrap().success());
    assert!(second.wait().unwrap().success());
    assert!(
        Command::new(BIN)
            .args(["db", "migrate"])
            .arg(&db.path)
            .status()
            .unwrap()
            .success()
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE success")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 3);
    storage::check_schema(&db.pool).await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn missing_newer_and_modified_schemas_fail_closed() {
    let db = Database::new().await;
    assert_eq!(
        storage::check_schema(&db.pool).await,
        Err(StorageError::IncompatibleSchema)
    );
    storage::migrate(&db.config).await.unwrap();
    sqlx::query("UPDATE _sqlx_migrations SET version = 999 WHERE version = 2")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        storage::check_schema(&db.pool).await,
        Err(StorageError::IncompatibleSchema)
    );
    sqlx::query("UPDATE _sqlx_migrations SET version = 2, checksum = '\\x00' WHERE version = 999")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        storage::check_schema(&db.pool).await,
        Err(StorageError::IncompatibleSchema)
    );
    db.close().await;
}

#[tokio::test]
async fn database_unavailability_is_a_redacted_typed_error() {
    let db = Database::new().await;
    db.pool.close().await;
    let error = storage::check_schema(&db.pool).await.unwrap_err();
    assert_eq!(error, StorageError::Unavailable);
    assert!(!format!("{error:?} {error}").contains(&db.config.url));
    db.close().await;
}

#[tokio::test]
async fn failed_migration_rolls_back_all_its_ddl() {
    let db = Database::new().await;
    sqlx::query("CREATE TABLE provider_credentials (conflict boolean)")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(
        storage::migrate(&db.config).await,
        Err(StorageError::MigrationFailed)
    );
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('people') IS NOT NULL")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert!(!exists);
    db.close().await;
}

#[tokio::test]
async fn migration_permission_failure_is_sanitized_and_transactional() {
    let db = Database::new().await;
    let role = format!("limited_{}", Uuid::new_v4().simple());
    sqlx::query(AssertSqlSafe(format!("CREATE ROLE {role} LOGIN")))
        .execute(&db.admin)
        .await
        .unwrap();
    let mut url = reqwest::Url::parse(&db.config.url).unwrap();
    url.set_username(&role).unwrap();
    url.set_password(Some("redaction-sentinel")).unwrap();
    fs::write(&db.path, format!("listen = '127.0.0.1:0'\nproxy_token = 'test'\n[postgres]\nurl = '{url}'\nnamespace = 'test'\n")).unwrap();
    let output = Command::new(BIN)
        .args(["db", "migrate"])
        .arg(&db.path)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("migration privileges"), "{error}");
    assert!(!error.contains("redaction-sentinel"));
    assert!(!error.contains(url.as_str()));
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('people') IS NOT NULL")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert!(!exists);
    sqlx::query(AssertSqlSafe(format!("DROP ROLE {role}")))
        .execute(&db.admin)
        .await
        .unwrap();
    db.close().await;
}

#[tokio::test]
async fn schema_enforces_ownership_credentials_generations_and_fifo() {
    let db = Database::new().await;
    storage::migrate(&db.config).await.unwrap();
    let person = Uuid::new_v4();
    sqlx::query("INSERT INTO people(namespace,id,display_name) VALUES ('test',$1,'Alice')")
        .bind(person)
        .execute(&db.pool)
        .await
        .unwrap();
    let insert = "INSERT INTO provider_credentials(namespace,person_id,provider,state,format_version,key_id,nonce,ciphertext,fingerprint) VALUES ('test',$1,'anthropic','active',1,'key',decode(repeat('00',12),'hex'),decode(repeat('00',16),'hex'),decode(repeat('00',32),'hex'))";
    sqlx::query(insert)
        .bind(person)
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        sqlx::query(insert)
            .bind(person)
            .execute(&db.pool)
            .await
            .is_err()
    );
    let second = Uuid::new_v4();
    sqlx::query("INSERT INTO people(namespace,id,display_name) VALUES ('test',$1,'Bob')")
        .bind(second)
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(
        sqlx::query(insert)
            .bind(second)
            .execute(&db.pool)
            .await
            .is_err()
    );
    let generation: i64 = sqlx::query_scalar(
        "UPDATE provider_credentials SET state = 'invalid', generation = 0 RETURNING generation",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(generation, 2);
    assert!(
        sqlx::query("UPDATE provider_credentials SET format_version = NULL")
            .execute(&db.pool)
            .await
            .is_err()
    );
    assert!(sqlx::query("INSERT INTO proxy_tokens(namespace,id,person_id,secret_hash) VALUES ('other',$1,$2,'$argon2id$test')").bind(Uuid::new_v4()).bind(person).execute(&db.pool).await.is_err());
    sqlx::query("INSERT INTO limit_definitions(namespace,person_id,provider,name,config) VALUES ('test',$1,'anthropic','limit','{}')").bind(person).execute(&db.pool).await.unwrap();
    let enqueue = "INSERT INTO fifo_waiters(namespace,person_id,provider,limit_name,id,owner,expires_at) VALUES ('test',$1,'anthropic','limit',$2,$2,clock_timestamp() + interval '1 minute') RETURNING enqueue_order";
    let mut tx = db.pool.begin().await.unwrap();
    let rolled_back: i64 = sqlx::query_scalar(enqueue)
        .bind(person)
        .bind(Uuid::new_v4())
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(rolled_back, 1);
    tx.rollback().await.unwrap();
    for expected in [1_i64, 2] {
        let order: i64 = sqlx::query_scalar(enqueue)
            .bind(person)
            .bind(Uuid::new_v4())
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(order, expected);
    }
    {
        let mut tx = db.pool.begin().await.unwrap();
        let first: i64 = sqlx::query_scalar(enqueue)
            .bind(person)
            .bind(Uuid::new_v4())
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let second = sqlx::query_scalar::<_, i64>(enqueue)
            .bind(person)
            .bind(Uuid::new_v4())
            .fetch_one(&db.pool);
        tokio::pin!(second);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut second)
                .await
                .is_err()
        );
        tx.commit().await.unwrap();
        assert_eq!(second.await.unwrap(), first + 1);
    }
    db.close().await;
}
