use super::*;

#[tokio::test]
async fn upgrade_preserves_live_lease_with_fenced_initial_generation() {
    // Given a live reservation persisted under the pre-fencing schema.
    let db = database::Database::new().await;
    sqlx::raw_sql(include_str!("../../migrations/1_initial.sql"))
        .execute(&db.pool)
        .await
        .unwrap();
    let person = Uuid::new_v4();
    let lease = Uuid::new_v4();
    sqlx::query("INSERT INTO people(namespace,id,display_name) VALUES ('test',$1,'Upgrade')")
        .bind(person)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO limit_definitions(namespace,person_id,provider,name,config) VALUES ('test',$1,'anthropic','limit','{}')").bind(person).execute(&db.pool).await.unwrap();
    sqlx::query("INSERT INTO concurrency_leases(namespace,person_id,provider,limit_name,id,owner,expires_at) VALUES ('test',$1,'anthropic','limit',$2,$2,clock_timestamp()+interval '30 seconds')").bind(person).bind(lease).execute(&db.pool).await.unwrap();
    // When the additive ownership-generation migration runs.
    sqlx::raw_sql(include_str!(
        "../../migrations/3_concurrency_lease_generation.sql"
    ))
    .execute(&db.pool)
    .await
    .unwrap();
    // Then the existing slot survives with a valid epoch; invalid epochs are rejected.
    let stored: (Uuid, i64, bool) = sqlx::query_as(
        "SELECT id,ownership_generation,expires_at>clock_timestamp() FROM concurrency_leases",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(stored, (lease, 1, true));
    assert!(
        sqlx::query("UPDATE concurrency_leases SET ownership_generation=0")
            .execute(&db.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE concurrency_leases SET ownership_generation=NULL")
            .execute(&db.pool)
            .await
            .is_err()
    );
    db.close().await;
}
