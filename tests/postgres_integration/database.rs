use sqlx::{AssertSqlSafe, PgPool};
use std::{fs, path::PathBuf, time::Duration};

pub struct Database {
    pub pool: PgPool,
    pub admin: PgPool,
    pub url: String,
    pub name: String,
    pub directory: PathBuf,
    admin_url: String,
}

impl Database {
    pub async fn new() -> Self {
        let admin_url = std::env::var("LLMPROXY_TEST_DATABASE_URL")
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
        let name = format!("integration_{}", uuid::Uuid::new_v4().simple());
        let directory = std::env::temp_dir().join(&name);
        fs::create_dir(&directory).unwrap();
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(&admin)
            .await
            .unwrap();
        let mut url = reqwest::Url::parse(&admin_url).unwrap();
        url.set_path(&name);
        let url = url.to_string();
        let result = Self {
            pool: PgPool::connect_lazy(&url).unwrap(),
            admin,
            url,
            name,
            directory,
            admin_url,
        };
        result.pool.acquire().await.unwrap();
        result
    }

    pub async fn available(&self, enabled: bool) {
        sqlx::query(AssertSqlSafe(format!(
            "ALTER DATABASE {} ALLOW_CONNECTIONS {enabled}",
            self.name
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        if !enabled {
            sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1")
                .bind(&self.name)
                .execute(&self.admin)
                .await
                .unwrap();
        }
    }

    pub async fn count(&self, table: &str, expected: i64) {
        let query = format!("SELECT count(*) FROM {table}");
        tokio::time::timeout(super::WAIT, async {
            loop {
                let count: i64 = sqlx::query_scalar(AssertSqlSafe(query.as_str()))
                    .fetch_one(&self.pool)
                    .await
                    .unwrap();
                if count == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("observed SQL count never reached expected state");
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
                    let admin = sqlx::postgres::PgPoolOptions::new()
                        .acquire_timeout(Duration::from_secs(5))
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
        assert!(cleanup.join().is_ok(), "isolated database cleanup failed");
        fs::remove_dir_all(&self.directory).unwrap();
    }
}
