use sqlx::{AssertSqlSafe, PgPool};
use std::time::Duration;

/// Polls `sql` (a single `count(*)`) until it returns `expected`, failing after `within`.
pub async fn count_eventually(pool: &PgPool, sql: &str, expected: i64, within: Duration) {
    let mut last = None;
    let reached = tokio::time::timeout(within, async {
        loop {
            let actual: i64 = sqlx::query_scalar(AssertSqlSafe(sql))
                .fetch_one(pool)
                .await
                .unwrap();
            if actual == expected {
                return;
            }
            last = Some(actual);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "`{sql}` expected {expected} within {within:?}, last saw {last:?}"
    );
}
