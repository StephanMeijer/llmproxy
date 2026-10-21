use super::Database;
use uuid::Uuid;

async fn legacy_balance(balance: f64) -> Database {
    let db = Database::new().await;
    sqlx::raw_sql(include_str!("../../migrations/1_initial.sql"))
        .execute(&db.pool)
        .await
        .unwrap();
    let person = Uuid::new_v4();
    sqlx::query("INSERT INTO people(namespace,id,display_name) VALUES ('test',$1,'Alice')")
        .bind(person)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO limit_definitions(namespace,person_id,provider,name,config) VALUES ('test',$1,'anthropic','budget','{}')")
        .bind(person).execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO rate_state(namespace,person_id,provider,limit_name,kind,available) VALUES ('test',$1,'anthropic','budget','requests',$2)")
        .bind(person).bind(balance).execute(&db.pool).await.unwrap();
    db
}

#[tokio::test]
async fn migration_floors_exact_ieee_balance_without_creating_units() {
    for (balance, expected) in [
        (1_234_567_890.123_456_7_f64, 74_074_073_407_407_402_i64),
        (0.0, 0),
        (-0.0, 0),
        (f64::from_bits(1), 0),
        (f64::MIN_POSITIVE, 0),
        (1.25, 75_000_000),
        (1.0_f64.next_down(), 59_999_999),
        (1.0_f64.next_up(), 60_000_000),
        (8_589_934_590.0, 515_396_075_400_000_000),
    ] {
        let db = legacy_balance(balance).await;
        let migrated = sqlx::raw_sql(include_str!(
            "../../migrations/2_fixed_point_rate_state.sql"
        ))
        .execute(&db.pool)
        .await;
        let actual: Option<i64> = if migrated.is_ok() {
            Some(
                sqlx::query_scalar("SELECT available_units FROM rate_state")
                    .fetch_one(&db.pool)
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        db.close().await;
        assert!(migrated.is_ok(), "balance={balance:?}: {migrated:?}");
        assert_eq!(actual, Some(expected), "balance={balance:?}");
    }
}

#[tokio::test]
async fn migration_rejects_oversized_balance_and_rolls_back_conversion() {
    for balance in [8_589_934_590.0_f64.next_up(), f64::MAX] {
        let db = legacy_balance(balance).await;
        let mut tx = db.pool.begin().await.unwrap();
        let migrated = sqlx::raw_sql(include_str!(
            "../../migrations/2_fixed_point_rate_state.sql"
        ))
        .execute(&mut *tx)
        .await;
        tx.rollback().await.unwrap();
        let remains_float: bool = sqlx::query_scalar(
            "SELECT pg_typeof(available) = 'double precision'::regtype FROM rate_state",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        db.close().await;
        assert!(migrated.is_err(), "balance={balance:?}");
        assert!(remains_float);
    }
}
