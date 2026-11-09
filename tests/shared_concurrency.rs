#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "common/database.rs"]
mod database;
#[path = "common/database_close.rs"]
mod database_close;
#[path = "shared_concurrency/edges.rs"]
mod edges;
#[path = "common/eventually.rs"]
mod eventually;
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
    limit::shared::{CompiledLimits, RENEW_EVERY, SharedDefinition},
    storage::{self, LEASE_LIFETIME, LimitKey, PersonId, Provider, Storage},
};
use std::time::Duration;
use uuid::Uuid;

const BOUND: Duration = Duration::from_secs(8);

fn definition() -> SharedDefinition {
    let config = Config::parse("listen='127.0.0.1:0'\nproxy_token='t'\n[[limit]]\nname='concurrency'\nprefixes=['/claude/']\nmax_inflight=1\n", |_| None).unwrap();
    CompiledLimits::new(&config.limits, "test", Provider::Anthropic, "/claude/")
        .unwrap()
        .shared
        .remove(0)
}

async fn setup() -> (database::Database, Storage, LimitKey) {
    let db = database::Database::new().await;
    storage::migrate(&db.config).await.unwrap();
    let storage = Storage::new(&db.config).unwrap();
    let key = LimitKey {
        provider: Provider::Anthropic,
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
    let query = match table {
        "leases" => "SELECT count(*) FROM concurrency_leases WHERE expires_at>clock_timestamp()",
        "waiters" => "SELECT count(*) FROM fifo_waiters WHERE expires_at>clock_timestamp()",
        _ => unreachable!(),
    };
    eventually::count_eventually(pool, query, expected, BOUND).await;
}
