#![allow(clippy::unwrap_used, clippy::expect_used)]

use llmproxy::config::PostgresConfig;
use sqlx::{AssertSqlSafe, PgPool, postgres::PgPoolOptions};
use std::{env, fs, path::PathBuf, time::Duration};
use uuid::Uuid;

const TIMEOUT: Duration = Duration::from_secs(5);

/// An isolated `PostgreSQL` 18 database, dropped (with its scratch directory) on `Drop`.
pub struct Database {
    pub admin: PgPool,
    pub pool: PgPool,
    pub name: String,
    pub config: PostgresConfig,
    /// Minimal proxy config file inside `directory`.
    pub path: PathBuf,
    pub directory: PathBuf,
    admin_url: String,
}

impl Database {
    pub async fn new() -> Self {
        let admin_url = env::var("LLMPROXY_TEST_DATABASE_URL")
            .expect("LLMPROXY_TEST_DATABASE_URL is required: PostgreSQL 18 with CREATEDB; integration tests never skip");
        Self::at_url(admin_url).await
    }

    pub async fn at_url(admin_url: String) -> Self {
        let admin = PgPool::connect(&admin_url)
            .await
            .expect("test PostgreSQL connection failed");
        let version: String = sqlx::query_scalar("SHOW server_version_num")
            .fetch_one(&admin)
            .await
            .unwrap();
        assert!(
            version.parse::<u32>().unwrap() >= 180_000,
            "PostgreSQL 18 is required"
        );
        let name = format!("test_{}", Uuid::new_v4().simple());
        let directory = env::temp_dir().join(&name);
        fs::create_dir(&directory).unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(&admin)
            .await
            .unwrap();
        let mut url = reqwest::Url::parse(&admin_url).unwrap();
        url.set_path(&name);
        let config = PostgresConfig {
            url: url.to_string(),
            namespace: "test".into(),
            max_connections: 4,
            connect_timeout: TIMEOUT,
            acquire_timeout: TIMEOUT,
            insecure_tls: false,
        };
        let pool = PgPoolOptions::new()
            .acquire_timeout(TIMEOUT)
            .connect(&config.url)
            .await
            .unwrap();
        let database = Self {
            admin,
            pool,
            name,
            config,
            path: directory.join("config.toml"),
            directory,
            admin_url,
        };
        fs::write(&database.path, format!("listen = '127.0.0.1:0'\nproxy_token = 'test'\n[postgres]\nurl = '{}'\nnamespace = 'test'\n", database.config.url)).unwrap();
        database
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let url = self.admin_url.clone();
        let name = self.name.clone();
        let cleanup = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let admin = PgPoolOptions::new()
                        .acquire_timeout(TIMEOUT)
                        .connect(&url)
                        .await
                        .unwrap();
                    sqlx::query(AssertSqlSafe(format!("DROP DATABASE {name} WITH (FORCE)")))
                        .execute(&admin)
                        .await
                        .unwrap();
                    admin.close().await;
                });
        });
        let cleaned = cleanup.join();
        let removed = fs::remove_dir_all(&self.directory);
        if !std::thread::panicking() {
            assert!(cleaned.is_ok(), "isolated database cleanup failed");
            removed.unwrap();
        }
    }
}
