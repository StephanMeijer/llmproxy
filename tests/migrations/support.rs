#![allow(clippy::unwrap_used, clippy::expect_used)]

use llmproxy::config::PostgresConfig;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
use std::{env, fs, path::PathBuf, time::Duration};
use uuid::Uuid;

const TIMEOUT: Duration = Duration::from_secs(5);

pub struct Database {
    pub admin: PgPool,
    pub pool: PgPool,
    name: String,
    pub config: PostgresConfig,
    pub path: PathBuf,
}

impl Database {
    pub async fn new() -> Self {
        let url = env::var("LLMPROXY_TEST_DATABASE_URL").expect(
            "set LLMPROXY_TEST_DATABASE_URL to a real PostgreSQL 16+ database with CREATEDB",
        );
        let admin = PgPool::connect(&url).await.unwrap();
        let name = format!("migration_{}", Uuid::new_v4().simple());
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(&admin)
            .await
            .unwrap();
        let mut url = reqwest::Url::parse(&url).unwrap();
        url.set_path(&name);
        let config = PostgresConfig {
            url: url.to_string(),
            namespace: "test".into(),
            max_connections: 4,
            connect_timeout: TIMEOUT,
            acquire_timeout: TIMEOUT,
        };
        let pool = PgPoolOptions::new()
            .acquire_timeout(TIMEOUT)
            .connect(&config.url)
            .await
            .unwrap();
        let path = env::temp_dir().join(format!("{name}.toml"));
        fs::write(&path, format!("listen = '127.0.0.1:0'\nproxy_token = 'test'\n[postgres]\nurl = '{}'\nnamespace = 'test'\n", config.url)).unwrap();
        Self {
            admin,
            pool,
            name,
            config,
            path,
        }
    }

    pub async fn close(self) {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE {} WITH (FORCE)",
            self.name
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        self.admin.close().await;
        fs::remove_file(self.path).unwrap();
    }
}
