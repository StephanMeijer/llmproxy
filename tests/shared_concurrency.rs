#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "migrations/support.rs"]
mod database;
#[path = "shared_concurrency/edges.rs"]
mod edges;
#[path = "shared_concurrency/migration.rs"]
mod migration;
#[path = "shared_concurrency/process.rs"]
mod process;
#[path = "shared_concurrency/renewal.rs"]
mod renewal;
#[path = "shared_concurrency/repository.rs"]
mod repository;
#[path = "shared_concurrency/scenarios.rs"]
mod scenarios;

use llmproxy::{
    config::Config,
    limit::shared::{CompiledLimits, SharedDefinition},
    storage::{self, LimitKey, PersonId, Storage},
};
use std::time::Duration;
use uuid::Uuid;

const BOUND: Duration = Duration::from_secs(8);

fn definition() -> SharedDefinition {
    let config = Config::parse("listen='127.0.0.1:0'\nproxy_token='t'\n[[limit]]\nname='concurrency'\nprefixes=['/claude/']\nmax_inflight=1\n", |_| None).unwrap();
    CompiledLimits::new(&config.limits, "test")
        .unwrap()
        .shared
        .remove(0)
}

async fn setup() -> (database::Database, Storage, LimitKey) {
    let db = database::Database::new().await;
    storage::migrate(&db.config).await.unwrap();
    let storage = Storage::new(&db.config).unwrap();
    let key = LimitKey {
        person: PersonId(Uuid::new_v4()),
        name: "concurrency".into(),
    };
    let mut tx = storage.begin().await.unwrap();
    tx.create_person(key.person, "Lease test").await.unwrap();
    tx.put_limit(&key, &definition()).await.unwrap();
    tx.commit().await.unwrap();
    (db, storage, key)
}

async fn eventually_count(pool: &sqlx::PgPool, table: &str, expected: i64) {
    tokio::time::timeout(BOUND, async {
        loop {
            let query = match table {
                "leases" => {
                    "SELECT count(*) FROM concurrency_leases WHERE expires_at>clock_timestamp()"
                }
                "waiters" => "SELECT count(*) FROM fifo_waiters WHERE expires_at>clock_timestamp()",
                _ => unreachable!(),
            };
            let count: i64 = sqlx::query_scalar(query).fetch_one(pool).await.unwrap();
            if count == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
